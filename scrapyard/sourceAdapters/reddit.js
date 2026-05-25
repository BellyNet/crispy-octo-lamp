'use strict'

const path = require('path')
const { normalizeMediaEntries, sanitizeToken } = require('../mediaEntries')
const mediaFileRecords = require('../mediaFileRecords')

const DEFAULT_REDDIT_PAGE_SIZE = 100

function parseResolvedDate(date) {
  return mediaFileRecords.parseResolvedDate(date)
}

function htmlDecode(value) {
  return String(value || '')
    .replace(/&amp;/g, '&')
    .replace(/&#43;/g, '+')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
}

function uniqueUrls(values) {
  const seen = new Set()
  const output = []
  const visit = (value) => {
    if (Array.isArray(value)) {
      for (const item of value) visit(item)
      return
    }
    const normalized = htmlDecode(value).trim()
    if (!normalized || seen.has(normalized)) return
    seen.add(normalized)
    output.push(normalized)
  }
  visit(values)
  return output
}

function getEntryMediaUrls(entry) {
  return uniqueUrls([
    entry?.mediaUrl,
    entry?.jsonMediaUrl,
    entry?.mediaUrls,
    entry?.sourceUrls,
  ])
}

function dedupeMediaEntries(entries, normalizeUrl) {
  const normalize =
    typeof normalizeUrl === 'function'
      ? normalizeUrl
      : (value) => String(value || '').trim()
  const seen = new Set()
  const deduped = []

  for (const entry of entries) {
    const mediaKeys = getEntryMediaUrls(entry).map((value) => normalize(value))
    const fallbackKey =
      mediaKeys.length > 0
        ? null
        : `${normalize(entry.mediaPageUrl)}\n${entry.filename}`
    const keys =
      mediaKeys.length > 0 ? mediaKeys : [fallbackKey].filter(Boolean)
    if (keys.length === 0 || keys.some((key) => seen.has(key))) continue
    for (const key of keys) seen.add(key)
    deduped.push(entry)
  }

  return deduped
}

function getPostPageUrl(source, post) {
  return post.permalink
    ? new URL(post.permalink, source.origin).toString()
    : `${source.origin}/comments/${post.id}`
}

function filenameFromMediaUrl(mediaUrl) {
  try {
    const name = decodeURIComponent(path.basename(new URL(mediaUrl).pathname))
    return name && name !== 'data' ? name : null
  } catch {
    return null
  }
}

function getRedditPostDate(post) {
  const createdUtc = Number(post?.created_utc)
  if (Number.isFinite(createdUtc) && createdUtc > 0) {
    return new Date(createdUtc * 1000)
  }
  return parseResolvedDate(post?.created)
}

function getRedditSubreddit(post) {
  return sanitizeToken(
    post?.subreddit_name_prefixed || post?.subreddit || post?.subreddit_id
  )
}

function getRedditLinkedUrl(source, value) {
  const url = htmlDecode(value)
  if (!url) return null
  try {
    return new URL(url, source.origin).toString()
  } catch {
    return url
  }
}

function isRedditContainerUrl(source, post, value) {
  if (!value) return false
  try {
    const parsed = new URL(value, source.origin)
    const host = parsed.hostname.toLowerCase()
    if (!host.endsWith('reddit.com')) return false
    const pathname = parsed.pathname.toLowerCase()
    const postId = String(post?.id || '').toLowerCase()
    return (
      pathname.includes(`/comments/${postId}`) ||
      pathname.includes(`/gallery/${postId}`) ||
      parsed.toString() === getPostPageUrl(source, post)
    )
  } catch {
    return false
  }
}

function getRedditPostLinkedUrls(source, post) {
  return uniqueUrls([
    getRedditLinkedUrl(source, post?.url_overridden_by_dest),
    getRedditLinkedUrl(source, post?.url),
  ]).filter((url) => !isRedditContainerUrl(source, post, url))
}

function getRedditMediaPageUrls(source, post) {
  const pageUrls = [getPostPageUrl(source, post)]
  if (post?.is_gallery || post?.gallery_data) {
    pageUrls.push(`${source.origin}/gallery/${post.id}`)
  }
  return uniqueUrls(pageUrls)
}

function getRedditMediaMetadataUrls(metadata) {
  return uniqueUrls([
    metadata?.s?.u,
    metadata?.s?.gif,
    metadata?.s?.mp4,
    Array.isArray(metadata?.o) ? metadata.o.map((item) => item?.u) : [],
    Array.isArray(metadata?.p) ? metadata.p.map((item) => item?.u) : [],
  ])
}

function getRedditMediaMetadataUrl(metadata) {
  return getRedditMediaMetadataUrls(metadata)[0] || ''
}

function extensionFromMime(mime) {
  const normalized = String(mime || '').toLowerCase()
  if (normalized.includes('jpeg') || normalized.includes('jpg')) return '.jpg'
  if (normalized.includes('png')) return '.png'
  if (normalized.includes('webp')) return '.webp'
  if (normalized.includes('gif')) return '.gif'
  if (normalized.includes('mp4')) return '.mp4'
  return ''
}

function buildRedditFilename(_source, post, mediaUrl, fallbackExt, index = 0) {
  const urlName = mediaUrl ? filenameFromMediaUrl(mediaUrl) : null
  const ext = path.extname(urlName || '') || fallbackExt || ''
  const suffix = index > 0 ? `_${index + 1}` : ''
  const subreddit = getRedditSubreddit(post)
  const subredditPart = subreddit ? `${subreddit}_` : ''
  return `${subredditPart}${post.id}${suffix}${ext || '.jpg'}`
}

function createRedditEntry(source, post, mediaUrl, uploadedDate, options = {}) {
  const filename =
    options.filename ||
    buildRedditFilename(
      source,
      post,
      mediaUrl,
      options.fallbackExt,
      options.index
    )
  return {
    sourceSite: 'reddit',
    sourceService: source.service || 'submitted',
    sourceUserId: source.userId || null,
    sourceUsername: source.username || source.userId || null,
    sourceSubreddit: getRedditSubreddit(post),
    postId: String(post.id || ''),
    title: post.title || null,
    mediaPageUrl: getPostPageUrl(source, post),
    mediaPageUrls: getRedditMediaPageUrls(source, post),
    mediaUrl,
    mediaUrls: uniqueUrls([mediaUrl, options.mediaUrls]),
    sourceUrls: uniqueUrls([
      options.sourceUrls,
      getRedditPostLinkedUrls(source, post),
    ]),
    filename,
    originalName: options.originalName || filenameFromMediaUrl(mediaUrl),
    uploadedDate,
  }
}

function getNativeRedditVideoUrl(post) {
  return (
    post?.secure_media?.reddit_video?.fallback_url ||
    post?.media?.reddit_video?.fallback_url ||
    post?.preview?.reddit_video_preview?.fallback_url ||
    null
  )
}

function getNativeRedditVideoUrls(post) {
  return uniqueUrls([
    post?.secure_media?.reddit_video?.fallback_url,
    post?.secure_media?.reddit_video?.dash_url,
    post?.secure_media?.reddit_video?.hls_url,
    post?.media?.reddit_video?.fallback_url,
    post?.media?.reddit_video?.dash_url,
    post?.media?.reddit_video?.hls_url,
    post?.preview?.reddit_video_preview?.fallback_url,
    post?.preview?.reddit_video_preview?.dash_url,
    post?.preview?.reddit_video_preview?.hls_url,
  ])
}

function getRedditGalleryEntries(source, post, uploadedDate) {
  const items = Array.isArray(post?.gallery_data?.items)
    ? post.gallery_data.items
    : []
  const metadata = post?.media_metadata || {}

  return items
    .map((item, index) => {
      const mediaId = item?.media_id
      const meta = mediaId ? metadata[mediaId] : null
      if (!meta || meta.status === 'failed') return null
      const mediaUrl = getRedditMediaMetadataUrl(meta)
      if (!mediaUrl) return null
      return createRedditEntry(source, post, mediaUrl, uploadedDate, {
        filename: buildRedditFilename(
          source,
          post,
          mediaUrl,
          extensionFromMime(meta.m),
          index
        ),
        mediaUrls: getRedditMediaMetadataUrls(meta),
        originalName: mediaId,
      })
    })
    .filter(Boolean)
}

async function resolveRedgifsEntry(
  source,
  post,
  redgifsUrl,
  uploadedDate,
  deps
) {
  const resolved = await deps.redgifsClient.resolveMedia(redgifsUrl)
  if (!resolved) return null
  const { id, mediaUrl } = resolved

  const filename = buildRedditFilename(
    source,
    post,
    mediaUrl,
    path.extname(new URL(mediaUrl).pathname) || '.mp4'
  )
  const createdDate = resolved.createdDate || uploadedDate

  return {
    sourceSite: 'reddit',
    sourceService: source.service || 'submitted',
    sourceUserId: source.userId || null,
    sourceUsername: source.username || source.userId || null,
    sourceSubreddit: getRedditSubreddit(post),
    postId: String(post.id || ''),
    title: post.title || null,
    mediaPageUrl: getPostPageUrl(source, post),
    mediaPageUrls: getRedditMediaPageUrls(source, post),
    mediaUrl,
    mediaUrls: uniqueUrls([mediaUrl, resolved.mediaUrls]),
    sourceUrls: uniqueUrls([
      redgifsUrl,
      resolved.canonicalUrl,
      getRedditPostLinkedUrls(source, post),
    ]),
    filename,
    originalName: id,
    uploadedDate: parseResolvedDate(createdDate) || uploadedDate,
  }
}

async function getRedditMediaEntries(source, post, deps = {}) {
  const uploadedDate = getRedditPostDate(post)
  const entries = getRedditGalleryEntries(source, post, uploadedDate)
  const redgifsId = deps.redgifsClient?.parseRedgifsId(
    post.url_overridden_by_dest || post.url
  )
  let redgifsResolved = false
  if (redgifsId) {
    const redgifsEntry = await resolveRedgifsEntry(
      source,
      post,
      post.url_overridden_by_dest || post.url,
      uploadedDate,
      deps
    ).catch((err) => {
      deps.logger?.warn?.(
        `RedGIFs resolve failed for ${post.id}: ${err.message}`
      )
      return null
    })
    if (redgifsEntry) {
      entries.push(redgifsEntry)
      redgifsResolved = true
    }
  }

  const videoUrl = redgifsResolved ? null : getNativeRedditVideoUrl(post)
  if (videoUrl) {
    entries.push(
      createRedditEntry(source, post, videoUrl, uploadedDate, {
        fallbackExt: '.mp4',
        mediaUrls: getNativeRedditVideoUrls(post),
      })
    )
  }

  const directUrl = htmlDecode(post.url_overridden_by_dest || post.url || '')
  if (
    /^https?:\/\/(?:i|preview)\.redd\.it\//i.test(directUrl) ||
    /^https?:\/\/i\.redditmedia\.com\//i.test(directUrl)
  ) {
    entries.push(createRedditEntry(source, post, directUrl, uploadedDate))
  }

  return normalizeMediaEntries(dedupeMediaEntries(entries, deps.normalizeUrl), {
    sourceSite: source.site,
    sourceService: source.service,
    sourceUserId: source.userId,
    sourceUsername: source.username,
  })
}

function getRedditListingUrl(
  source,
  after = null,
  pageSize = DEFAULT_REDDIT_PAGE_SIZE
) {
  const url = new URL(
    `/user/${encodeURIComponent(source.username || source.userId)}/submitted/.json`,
    source.origin
  )
  url.searchParams.set('limit', String(pageSize))
  url.searchParams.set('raw_json', '1')
  if (after) url.searchParams.set('after', after)
  return url.toString()
}

async function preflightRedditSource(source, deps = {}) {
  if (typeof deps.fetchJson !== 'function') {
    throw new Error('preflightRedditSource requires fetchJson')
  }
  const apiUrl = getRedditListingUrl(
    source,
    null,
    deps.pageSize || DEFAULT_REDDIT_PAGE_SIZE
  )
  const { data, byteLength } = await deps.fetchJson(apiUrl)
  const children = Array.isArray(data?.data?.children)
    ? data.data.children.map((child) => child?.data).filter(Boolean)
    : []
  const newest = children
    .map((post) => getRedditPostDate(post))
    .filter(Boolean)
    .sort((a, b) => b.getTime() - a.getTime())[0]

  return {
    apiUrl,
    byteLength,
    postCount: children.length,
    newest,
    firstPostId: children[0]?.id ? String(children[0].id) : null,
  }
}

async function fetchRedditPosts(source, options = {}, deps = {}) {
  if (typeof deps.fetchJson !== 'function') {
    throw new Error('fetchRedditPosts requires fetchJson')
  }
  if (!deps.redgifsClient) {
    throw new Error('fetchRedditPosts requires redgifsClient')
  }

  const posts = []
  let after = null
  let page = 0
  const pageSize = deps.pageSize || DEFAULT_REDDIT_PAGE_SIZE

  while (true) {
    if (options.endPage !== null && page > options.endPage) break
    const apiUrl = getRedditListingUrl(source, after, pageSize)
    deps.logger?.log?.(`Loading reddit page ${page + 1} (${apiUrl})`)
    const { data } = await deps.fetchJson(apiUrl)
    const listing = data?.data
    const pagePosts = Array.isArray(listing?.children)
      ? listing.children.map((child) => child?.data).filter(Boolean)
      : []
    if (pagePosts.length === 0) break

    for (const post of pagePosts) {
      const mediaEntries = await getRedditMediaEntries(source, post, deps)
      posts.push({
        ...post,
        id: String(post.id || ''),
        published: getRedditPostDate(post),
        mediaEntries,
      })
      if (
        Number.isFinite(options.maxPosts) &&
        options.maxPosts > 0 &&
        posts.length >= options.maxPosts
      ) {
        return posts
      }
    }

    after = listing?.after || null
    if (!after) break
    page += 1
  }

  return posts
}

module.exports = {
  DEFAULT_REDDIT_PAGE_SIZE,
  buildRedditFilename,
  fetchRedditPosts,
  getPostPageUrl,
  getRedditMediaEntries,
  getRedditPostDate,
  getRedditSubreddit,
  preflightRedditSource,
}

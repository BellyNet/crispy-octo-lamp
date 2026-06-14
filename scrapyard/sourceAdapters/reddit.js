'use strict'

const path = require('path')
const { normalizeMediaEntries, sanitizeToken } = require('../mediaEntries')
const mediaFileRecords = require('../mediaFileRecords')

const DEFAULT_REDDIT_PAGE_SIZE = 100
const REDDIT_RSS_USER_AGENT =
  'Mozilla/5.0 (compatible; LoRATraining/1.0; +https://localhost)'
const DEFAULT_REDDIT_FALLBACK_DELAY_MS = 0
const DEFAULT_REDDIT_HTML_DELAY_MS = 6500
const DEFAULT_REDDIT_HTML_RATE_LIMIT_DELAY_MS = 30000
const DEFAULT_REDDIT_HTML_MAX_RETRIES = 1
const REDDIT_DISCOVERY_PROGRESS_EVERY_POSTS = 25
const REDDIT_DISCOVERY_PROGRESS_EVERY_MS = 5000
const OLD_REDDIT_PAGE_SIZE = 25
const DEFAULT_REDDIT_INCREMENTAL_OVERLAP_POSTS = 5
let lastOldRedditHtmlFetchAt = 0

function parseResolvedDate(date) {
  return mediaFileRecords.parseResolvedDate(date)
}

function htmlDecode(value) {
  return String(value || '')
    .replace(/&amp;/g, '&')
    .replace(/&#43;/g, '+')
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) =>
      String.fromCharCode(Number.parseInt(hex, 16))
    )
    .replace(/&#(\d+);/g, (_, code) =>
      String.fromCharCode(Number.parseInt(code, 10))
    )
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
}

function cleanRedditText(value) {
  let cleaned = String(value || '')
    .replace(/^<!\[CDATA\[([\s\S]*)\]\]>$/i, '$1')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/(?:div|p|li)>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')

  for (let pass = 0; pass < 2; pass += 1) {
    const decoded = htmlDecode(cleaned)
    if (decoded === cleaned) break
    cleaned = decoded
  }

  return cleaned
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
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
  return uniqueUrls([entry?.mediaUrl, entry?.jsonMediaUrl, entry?.mediaUrls])
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

function getRedditPostTitle(post = {}) {
  const title = cleanRedditText(post.title)
  return title || getTitleFromPermalink(post.permalink)
}

function filenameFromMediaUrl(mediaUrl) {
  try {
    const name = decodeURIComponent(path.basename(new URL(mediaUrl).pathname))
    return name && name !== 'data' ? name : null
  } catch {
    return null
  }
}

function normalizeRedditImageUrl(mediaUrl) {
  const decoded = htmlDecode(mediaUrl).trim()
  if (!decoded) return ''
  try {
    const parsed = new URL(decoded)
    const host = parsed.hostname.toLowerCase()
    if (host === 'preview.redd.it') {
      return `https://i.redd.it${parsed.pathname}`
    }
    return parsed.toString()
  } catch {
    return decoded
  }
}

function getRedditPostDate(post) {
  const createdUtc = Number(post?.created_utc)
  if (Number.isFinite(createdUtc) && createdUtc > 0) {
    return new Date(createdUtc * 1000)
  }
  return parseResolvedDate(post?.created)
}

function getRedditPostCreatedUtc(post) {
  const date = getRedditPostDate(post)
  return date ? Math.floor(date.getTime() / 1000) : null
}

function getIncrementalOverlapPosts(deps = {}) {
  return getPositiveInteger(
    deps.redditIncrementalOverlapPosts ??
      process.env.HOGHAUL_REDDIT_INCREMENTAL_OVERLAP_POSTS,
    DEFAULT_REDDIT_INCREMENTAL_OVERLAP_POSTS
  )
}

function createIncrementalPostFilter(deps = {}, options = {}) {
  const state = deps.redditSourceState
  const fullRefresh = Boolean(
    options.redditFullRefresh || deps.redditFullRefresh
  )
  if (!state?.hasFrontier || fullRefresh) {
    return {
      active: false,
      filterPage: (posts) => ({ posts, stopAfterPage: false, boundaryHits: 0 }),
    }
  }

  const overlapPosts = getIncrementalOverlapPosts(deps)
  const knownPostIds =
    state.knownPostIds instanceof Set
      ? state.knownPostIds
      : new Set(state.knownPostIds || [])
  const latestCreatedUtc = Number(state.latestCreatedUtc || 0) || null

  return {
    active: true,
    latestPostId: state.latestPostId || null,
    latestCreatedUtc,
    knownPostCount: knownPostIds.size,
    filterPage(pagePosts = []) {
      const output = []
      let boundaryHits = 0

      for (const post of pagePosts) {
        const postId = String(post?.id || '')
        const postCreatedUtc = getRedditPostCreatedUtc(post)
        const known = postId && knownPostIds.has(postId)
        const atOrBeforeFrontier =
          latestCreatedUtc &&
          postCreatedUtc &&
          postCreatedUtc <= latestCreatedUtc

        if (known) {
          boundaryHits += 1
          continue
        }

        if (atOrBeforeFrontier) {
          boundaryHits += 1
          if (boundaryHits <= overlapPosts) output.push(post)
          continue
        }

        output.push(post)
      }

      return {
        posts: output,
        stopAfterPage: boundaryHits > 0,
        boundaryHits,
      }
    },
  }
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
    title: getRedditPostTitle(post),
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
  const htmlMediaUrls = uniqueUrls(post?.htmlMediaUrls || [])
    .map((url) => normalizeRedditImageUrl(url))
    .filter(Boolean)

  if (htmlMediaUrls.length > 0) {
    return htmlMediaUrls.map((mediaUrl, index) =>
      createRedditEntry(source, post, mediaUrl, uploadedDate, {
        filename: buildRedditFilename(
          source,
          post,
          mediaUrl,
          path.extname(new URL(mediaUrl).pathname) || '.jpg',
          index
        ),
        mediaUrls: [mediaUrl],
      })
    )
  }

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
    title: getRedditPostTitle(post),
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

  const directUrl = normalizeRedditImageUrl(
    post.url_overridden_by_dest || post.url || ''
  )
  if (
    /^https?:\/\/(?:i|preview)\.redd\.it\//i.test(directUrl) ||
    /^https?:\/\/i\.redditmedia\.com\//i.test(directUrl)
  ) {
    entries.push(createRedditEntry(source, post, directUrl, uploadedDate))
  }

  const htmlMediaUrls = uniqueUrls([
    post.rssContentHtml ? extractRedditHtmlMediaUrls(post.rssContentHtml) : [],
    post.htmlMediaUrls,
  ])
  if (htmlMediaUrls.length > 0) {
    htmlMediaUrls.forEach((mediaUrl, index) => {
      entries.push(
        createRedditEntry(source, post, mediaUrl, uploadedDate, {
          index,
          sourceUrls: [post.url_overridden_by_dest, post.url],
        })
      )
    })
  }

  return normalizeMediaEntries(dedupeMediaEntries(entries, deps.normalizeUrl), {
    sourceSite: source.site,
    sourceService: source.service,
    sourceUserId: source.userId,
    sourceUsername: source.username,
  })
}

function getOldRedditOrigin(source) {
  const inputOrigin = source.origin || 'https://www.reddit.com'
  try {
    const parsed = new URL(inputOrigin)
    parsed.hostname = 'old.reddit.com'
    return parsed.origin
  } catch {
    return 'https://old.reddit.com'
  }
}

function getOldRedditListingUrl(source) {
  const oldOrigin = getOldRedditOrigin(source)
  const url = new URL(
    `/user/${encodeURIComponent(source.username || source.userId)}/submitted/`,
    oldOrigin
  )
  url.searchParams.set('over18', '1')
  return url.toString()
}

function getOldRedditHeaders(extra = {}) {
  return {
    Cookie: 'over18=1;',
    Referer: 'https://old.reddit.com/',
    ...extra,
  }
}

function getPositiveInteger(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ''), 10)
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback
}

function getRedditHtmlDelayMs(deps = {}) {
  return getPositiveInteger(
    deps.redditHtmlDelayMs ?? process.env.HOGHAUL_REDDIT_HTML_DELAY_MS,
    DEFAULT_REDDIT_HTML_DELAY_MS
  )
}

function getRedditHtmlRateLimitDelayMs(deps = {}) {
  return getPositiveInteger(
    deps.redditHtmlRateLimitDelayMs ??
      process.env.HOGHAUL_REDDIT_HTML_RATE_LIMIT_DELAY_MS,
    DEFAULT_REDDIT_HTML_RATE_LIMIT_DELAY_MS
  )
}

function getRedditHtmlMaxRetries(deps = {}) {
  return getPositiveInteger(
    deps.redditHtmlMaxRetries ?? process.env.HOGHAUL_REDDIT_HTML_MAX_RETRIES,
    DEFAULT_REDDIT_HTML_MAX_RETRIES
  )
}

async function waitForOldRedditHtmlSlot(deps = {}, details = {}) {
  const delayMs = getRedditHtmlDelayMs(deps)
  if (delayMs <= 0) return 0
  const now = Date.now()
  const waitMs = Math.max(lastOldRedditHtmlFetchAt + delayMs - now, 0)
  if (waitMs > 0) {
    const requestKind = details.requestKind || 'HTML'
    deps.logger?.status?.(
      `Reddit ${requestKind}: waiting ${(waitMs / 1000).toFixed(1)}s for the paced request slot`
    )
    deps.appendRunEvent?.('reddit_html_throttle_wait', {
      requestKind,
      waitMs,
      url: details.url || null,
    })
    await sleep(waitMs)
  }
  lastOldRedditHtmlFetchAt = Date.now()
  return waitMs
}

function isRedditRateLimitError(err) {
  return /\b(?:HTTP|Browser HTTP)\s+429\b/i.test(String(err?.message || err))
}

function isRedditAccessError(err) {
  return /\b(?:HTTP|Browser HTTP)\s+(?:403|429)\b/i.test(
    String(err?.message || err)
  )
}

async function waitForRedditRateLimitRetry(err, attempt, deps = {}) {
  const baseDelayMs = getRedditHtmlRateLimitDelayMs(deps)
  const headerDelayMs = Number(err?.retryAfterMs || 0)
  const delayMs =
    headerDelayMs > 0
      ? Math.min(headerDelayMs + 5000, 10 * 60 * 1000)
      : Math.min(baseDelayMs * Math.max(attempt, 1), 10 * 60 * 1000)
  deps.logger?.warn?.(
    `Reddit HTML 429; waiting ${Math.round(delayMs / 1000)}s before retry ${attempt}: ${err.message}`
  )
  deps.appendRunEvent?.('reddit_rate_limit_retry', {
    attempt,
    delayMs,
    error: err.message,
  })
  await sleep(delayMs)
}

async function fetchRedditHtmlWithRetry(url, requestOptions, deps = {}) {
  if (typeof deps.fetchHtml !== 'function') {
    throw new Error('fetchRedditHtmlWithRetry requires fetchHtml')
  }
  const maxRetries = getRedditHtmlMaxRetries(deps)
  const requestKind =
    deps.redditHtmlRequestKind ||
    (/\.rss(?:[?#]|$)/i.test(url)
      ? 'RSS listing'
      : /\/user\/[^/]+\/submitted/i.test(url)
        ? 'HTML listing'
        : 'gallery/post')
  let lastError = null

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    await waitForOldRedditHtmlSlot(deps, { requestKind, url })
    const startedAt = Date.now()
    deps.logger?.status?.(`Fetching Reddit ${requestKind}...`)
    deps.appendRunEvent?.('reddit_html_request_started', {
      requestKind,
      attempt: attempt + 1,
      url,
    })
    try {
      const response = await deps.fetchHtml(url, requestOptions)
      deps.appendRunEvent?.('reddit_html_request_finished', {
        requestKind,
        attempt: attempt + 1,
        url: response.url || url,
        statusCode: response.statusCode || null,
        byteLength: response.byteLength || 0,
        durationMs: Date.now() - startedAt,
      })
      return response
    } catch (err) {
      lastError = err
      deps.appendRunEvent?.('reddit_html_request_failed', {
        requestKind,
        attempt: attempt + 1,
        url,
        durationMs: Date.now() - startedAt,
        error: err.message,
      })
      if (!isRedditRateLimitError(err) || attempt >= maxRetries) throw err
      await waitForRedditRateLimitRetry(err, attempt + 1, deps)
    }
  }

  throw lastError
}

async function fetchOldRedditHtml(url, deps = {}) {
  return fetchRedditHtmlWithRetry(
    url,
    {
      headers: getOldRedditHeaders(deps.headers),
    },
    deps
  )
}

function parseHtmlAttributes(tag) {
  const attrs = {}
  for (const match of String(tag || '').matchAll(
    /([a-zA-Z0-9_-]+)="([^"]*)"/g
  )) {
    attrs[match[1]] = htmlDecode(match[2])
  }
  return attrs
}

function getPostIdFromThing(attrs) {
  const fullname = attrs['data-fullname'] || attrs.id || ''
  const match = String(fullname).match(/(?:^|_)t3_([a-z0-9]+)/i)
  return match?.[1] || String(attrs.id || '').replace(/^thing_t3_/i, '')
}

function getTitleFromPermalink(permalink) {
  const parts = String(permalink || '')
    .split('/')
    .filter(Boolean)
  const slug = parts[parts.length - 1] || ''
  if (!slug) return null
  let decodedSlug = slug
  try {
    decodedSlug = decodeURIComponent(slug)
  } catch {}
  return cleanRedditText(decodedSlug.replace(/_/g, ' ')) || null
}

function parseOldRedditListingPosts(source, html) {
  const oldOrigin = getOldRedditOrigin(source)
  const posts = []
  const thingTags = String(html || '').match(
    /<div\s+class="[^"]*\bthing\b[^"]*"[^>]*>/gi
  )

  for (const tag of thingTags || []) {
    const attrs = parseHtmlAttributes(tag)
    if (attrs['data-promoted'] === 'true') continue
    const id = getPostIdFromThing(attrs)
    if (!id) continue

    const permalink = attrs['data-permalink']
      ? new URL(attrs['data-permalink'], oldOrigin).toString()
      : `${oldOrigin}/comments/${id}/`
    const dataUrl = attrs['data-url']
      ? new URL(attrs['data-url'], oldOrigin).toString()
      : permalink
    const timestampMs = Number.parseInt(attrs['data-timestamp'] || '', 10)
    const createdUtc =
      Number.isFinite(timestampMs) && timestampMs > 0
        ? Math.floor(timestampMs / 1000)
        : null

    posts.push({
      id,
      name: `t3_${id}`,
      title:
        cleanRedditText(attrs['data-title']) ||
        getTitleFromPermalink(permalink),
      subreddit: attrs['data-subreddit'] || null,
      subreddit_name_prefixed:
        attrs['data-subreddit-prefixed'] || attrs['data-subreddit'] || null,
      created_utc: createdUtc,
      permalink,
      url: dataUrl,
      url_overridden_by_dest: dataUrl,
      is_gallery:
        attrs['data-is-gallery'] === 'true' ||
        /\/gallery\/[a-z0-9]+/i.test(dataUrl),
      over_18: attrs['data-nsfw'] === 'true',
    })
  }

  return posts
}

function parseOldRedditNextUrl(source, html) {
  const match = String(html || '').match(
    /<span class="next-button">[\s\S]*?<a href="([^"]+)"/i
  )
  if (!match?.[1]) return null
  return new URL(htmlDecode(match[1]), getOldRedditOrigin(source)).toString()
}

function extractOldRedditImageUrls(html) {
  const urls = []
  const raw = String(html || '')
  for (const match of raw.matchAll(
    /https?:\/\/(?:i|preview)\.redd\.it\/[^"'<>\\\s]+/gi
  )) {
    const normalized = normalizeRedditImageUrl(match[0])
    if (normalized) urls.push(normalized)
  }
  return uniqueUrls(urls)
}

async function enrichOldRedditHtmlPostMedia(source, post, deps = {}) {
  if (!post.is_gallery) return post
  const postUrl = new URL(post.permalink, getOldRedditOrigin(source))
  postUrl.searchParams.set('over18', '1')
  const { html } = await fetchOldRedditHtml(postUrl.toString(), {
    ...deps,
    redditHtmlRequestKind: 'gallery/post',
  })
  return {
    ...post,
    htmlMediaUrls: extractOldRedditImageUrls(html),
  }
}

async function fetchRedditPostsFromOldHtml(source, options = {}, deps = {}) {
  if (!deps.redgifsClient) {
    throw new Error('fetchRedditPostsFromOldHtml requires redgifsClient')
  }

  const posts = []
  let listingUrl = getOldRedditListingUrl(source)
  let page = 0
  const incrementalFilter = createIncrementalPostFilter(deps, options)
  if (incrementalFilter.active) {
    deps.logger?.log?.(
      `Reddit incremental frontier: ${incrementalFilter.knownPostCount} known post(s), latest ${incrementalFilter.latestPostId || 'unknown'}`
    )
  }

  while (listingUrl) {
    if (options.endPage !== null && page > options.endPage) break

    const response = await fetchOldRedditHtml(listingUrl, deps)
    const { html } = response
    const pagePosts = parseOldRedditListingPosts(source, html)
    deps.onListingPage?.({
      mode: 'old_html',
      page: page + 1,
      url: response.url || listingUrl,
      statusCode: response.statusCode || null,
      byteLength: response.byteLength || Buffer.byteLength(html || ''),
      rawPostCount: pagePosts.length,
    })
    if (pagePosts.length === 0) {
      deps.appendRunEvent?.('reddit_discovery_empty_page', {
        mode: 'old_html',
        page: page + 1,
        url: response.url || listingUrl,
        statusCode: response.statusCode || null,
        byteLength: response.byteLength || Buffer.byteLength(html || ''),
      })
      break
    }
    const filteredPage = incrementalFilter.filterPage(pagePosts)
    deps.logger?.log?.(
      `Fetched reddit HTML page ${page + 1}: ${pagePosts.length} post(s), ${filteredPage.posts.length} new candidate(s)`
    )

    const shouldCollect = page >= (Number(options.startPage) || 0)
    if (shouldCollect) {
      const remainingPosts =
        Number.isFinite(options.maxPosts) && options.maxPosts > 0
          ? Math.max(options.maxPosts - posts.length, 0)
          : filteredPage.posts.length
      const postsToProcess = filteredPage.posts.slice(0, remainingPosts)
      const galleryCount = postsToProcess.filter(
        (post) => post.is_gallery
      ).length
      const estimatedGalleryWaitMs = galleryCount * getRedditHtmlDelayMs(deps)
      if (galleryCount > 0) {
        deps.logger?.log?.(
          `Resolving ${galleryCount} Reddit gallery post(s) with paced requests; estimated throttle time ${Math.ceil(estimatedGalleryWaitMs / 1000)}s`
        )
        deps.appendRunEvent?.('reddit_gallery_hydration_started', {
          page: page + 1,
          galleryCount,
          delayMs: getRedditHtmlDelayMs(deps),
          estimatedWaitMs: estimatedGalleryWaitMs,
        })
      }
      const pageStartPostCount = posts.length
      const pageTargetPostCount = Math.max(
        pageStartPostCount + postsToProcess.length,
        1
      )
      let hydratedGalleryCount = 0
      for (const post of postsToProcess) {
        const galleryIndex = post.is_gallery ? hydratedGalleryCount + 1 : null
        const galleryStartedAt = post.is_gallery ? Date.now() : null
        if (post.is_gallery) {
          deps.logger?.status?.(
            `Resolving Reddit gallery ${galleryIndex}/${galleryCount}: ${post.id}`
          )
          deps.appendRunEvent?.('reddit_gallery_hydration_post_started', {
            page: page + 1,
            postId: String(post.id || ''),
            current: galleryIndex,
            total: galleryCount,
          })
        }
        const enrichedPost = await enrichOldRedditHtmlPostMedia(
          source,
          post,
          deps
        ).catch((err) => {
          deps.logger?.warn?.(
            `Reddit gallery page fetch failed for ${post.id}: ${err.message}`
          )
          return post
        })
        if (post.is_gallery) {
          hydratedGalleryCount += 1
          deps.appendRunEvent?.('reddit_gallery_hydration_post_finished', {
            page: page + 1,
            postId: String(post.id || ''),
            current: hydratedGalleryCount,
            total: galleryCount,
            mediaUrlCount: Array.isArray(enrichedPost.htmlMediaUrls)
              ? enrichedPost.htmlMediaUrls.length
              : 0,
            durationMs: Date.now() - galleryStartedAt,
          })
        }
        const mediaEntries = await getRedditMediaEntries(
          source,
          enrichedPost,
          deps
        )
        posts.push({
          ...enrichedPost,
          id: String(enrichedPost.id || ''),
          title: getRedditPostTitle(enrichedPost),
          published: getRedditPostDate(enrichedPost),
          mediaEntries,
        })
        emitDiscoveryProgress(deps, {
          mode: 'reddit html',
          pages: page + 1,
          posts: posts.length,
          media: countPostMedia(posts),
          current: posts.length,
          total: pageTargetPostCount,
        })
        if (
          Number.isFinite(options.maxPosts) &&
          options.maxPosts > 0 &&
          posts.length >= options.maxPosts
        ) {
          return posts
        }
      }
      if (galleryCount > 0) {
        deps.appendRunEvent?.('reddit_gallery_hydration_finished', {
          page: page + 1,
          galleryCount,
          durationEstimateMs: estimatedGalleryWaitMs,
        })
      }
    }

    if (filteredPage.stopAfterPage) {
      deps.logger?.log?.(
        `Reddit incremental frontier reached after ${filteredPage.boundaryHits} known/old post(s).`
      )
      break
    }
    listingUrl = parseOldRedditNextUrl(source, html)
    page += 1
  }

  return posts
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

function getRedditRssUrl(
  source,
  after = null,
  pageSize = DEFAULT_REDDIT_PAGE_SIZE
) {
  const url = new URL(
    `/user/${encodeURIComponent(source.username || source.userId)}/submitted/.rss`,
    source.origin
  )
  url.searchParams.set('limit', String(pageSize))
  if (after) url.searchParams.set('after', after)
  return url.toString()
}

function countPostMedia(posts) {
  return posts.reduce(
    (total, post) =>
      total + (Array.isArray(post.mediaEntries) ? post.mediaEntries.length : 0),
    0
  )
}

function emitDiscoveryProgress(deps, details = {}) {
  if (typeof deps.onDiscoveryProgress !== 'function') return
  deps.onDiscoveryProgress({
    pages: details.pages || 0,
    posts: details.posts || 0,
    media: details.media || 0,
    current: details.current || 0,
    total: details.total || 0,
    mode: details.mode || 'reddit',
  })
}

function getDiscoveryTargetPosts(options, page, pageSize, postCount) {
  if (Number.isFinite(options.maxPosts) && options.maxPosts > 0) {
    return options.maxPosts
  }
  return Math.max(pageSize * (page + 1), postCount || 1)
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function mapWithConcurrency(items, limit, mapper) {
  const concurrency = Math.max(
    Number.parseInt(String(limit || '1'), 10) || 1,
    1
  )
  const results = new Array(items.length)
  let nextIndex = 0

  async function worker() {
    while (nextIndex < items.length) {
      const index = nextIndex
      nextIndex += 1
      results[index] = await mapper(items[index], index)
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, () => worker())
  )
  return results
}

function extractXmlTag(block, tag) {
  const match = String(block || '').match(
    new RegExp(`<${tag}\\b[^>]*>([\\s\\S]*?)<\\/${tag}>`, 'i')
  )
  return match ? htmlDecode(match[1]).trim() : ''
}

function extractRssContent(block) {
  const raw = extractXmlTag(block, 'content')
  return htmlDecode(raw)
}

function extractHrefByText(html, text) {
  const needle = String(text || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const pattern = new RegExp(
    `<a\\b[^>]*href=["']([^"']+)["'][^>]*>\\s*${needle}\\s*<\\/a>`,
    'i'
  )
  const match = String(html || '').match(pattern)
  return match ? htmlDecode(match[1]) : ''
}

function extractRssSubreddit(block) {
  const match = String(block || '').match(
    /<category\b[^>]*label=["']r\/([^"']+)["']/i
  )
  return match ? htmlDecode(match[1]).trim() : ''
}

function extractPostIdFromUrl(url) {
  const match = String(url || '').match(/\/comments\/([^/?#\s]+)/i)
  if (match) return match[1]
  const galleryMatch = String(url || '').match(/\/gallery\/([^/?#\s]+)/i)
  return galleryMatch ? galleryMatch[1] : ''
}

function parseRssEntries(xml, source) {
  return [...String(xml || '').matchAll(/<entry\b[^>]*>([\s\S]*?)<\/entry>/gi)]
    .map((match) => {
      const block = match[1]
      const contentHtml = extractRssContent(block)
      const linkUrl = extractHrefByText(contentHtml, '[link]')
      const commentsUrl = extractHrefByText(contentHtml, '[comments]')
      const id = extractPostIdFromUrl(commentsUrl || linkUrl)
      if (!id) return null

      const subreddit = extractRssSubreddit(block)
      const updated = extractXmlTag(block, 'updated')
      const published = parseResolvedDate(updated)
      const permalink = commentsUrl
        ? new URL(commentsUrl).pathname
        : `/comments/${id}`

      return {
        id,
        title: cleanRedditText(extractXmlTag(block, 'title')),
        permalink,
        subreddit,
        subreddit_name_prefixed: subreddit ? `r/${subreddit}` : null,
        created_utc: published ? published.getTime() / 1000 : null,
        url: linkUrl || commentsUrl,
        url_overridden_by_dest: linkUrl || commentsUrl,
        is_gallery: /\/gallery\//i.test(linkUrl),
        rssContentHtml: contentHtml,
      }
    })
    .filter(Boolean)
}

function scoreRedditHtmlMediaUrl(url) {
  let score = 0
  try {
    const parsed = new URL(url)
    const width = Number.parseInt(parsed.searchParams.get('width') || '0', 10)
    if (Number.isFinite(width)) score += width
    if (!parsed.searchParams.has('blur')) score += 10000
    if (parsed.hostname === 'i.redd.it') score += 20000
  } catch {
    return 0
  }
  return score
}

function normalizeRedditHtmlMediaUrl(url) {
  try {
    const parsed = new URL(url)
    const host = parsed.hostname.toLowerCase()
    if (host === 'preview.redd.it') {
      parsed.hostname = 'i.redd.it'
      parsed.search = ''
      return parsed.toString()
    }
    if (host === 'i.redd.it') {
      parsed.search = ''
      return parsed.toString()
    }
  } catch {
    return url
  }
  return url
}

function getRedditHtmlMediaKey(url) {
  try {
    const parsed = new URL(url)
    return parsed.pathname.replace(/^.*-v0-/, '')
  } catch {
    return url
  }
}

function extractRedditHtmlMediaUrls(html) {
  const decoded = htmlDecode(html)
  const candidates = []
  for (const match of decoded.matchAll(
    /https:\/\/(?:i|preview)\.redd\.it\/[^\s"'<>]+/gi
  )) {
    const url = normalizeRedditHtmlMediaUrl(
      htmlDecode(match[0]).replace(/,$/, '')
    )
    if (/blur=/i.test(url)) continue
    if (/\/cms\//i.test(url)) continue
    candidates.push(url)
  }

  const bestByKey = new Map()
  for (const url of candidates) {
    const key = getRedditHtmlMediaKey(url)
    const previous = bestByKey.get(key)
    if (
      !previous ||
      scoreRedditHtmlMediaUrl(url) > scoreRedditHtmlMediaUrl(previous)
    ) {
      bestByKey.set(key, url)
    }
  }
  return [...bestByKey.values()]
}

async function preflightRedditRssSource(source, deps = {}) {
  if (typeof deps.fetchHtml !== 'function') {
    throw new Error('preflightRedditRssSource requires fetchHtml')
  }
  const rssUrl = getRedditRssUrl(
    source,
    null,
    deps.pageSize || DEFAULT_REDDIT_PAGE_SIZE
  )
  const { html, byteLength } = await deps.fetchHtml(rssUrl, {
    headers: {
      Accept: 'application/atom+xml,text/xml,application/xml',
      Referer: source.origin,
      'User-Agent': REDDIT_RSS_USER_AGENT,
    },
  })
  const children = parseRssEntries(html, source)
  const newest = children
    .map((post) => getRedditPostDate(post))
    .filter(Boolean)
    .sort((a, b) => b.getTime() - a.getTime())[0]

  return {
    apiUrl: rssUrl,
    byteLength,
    postCount: children.length,
    newest,
    firstPostId: children[0]?.id ? String(children[0].id) : null,
  }
}

async function preflightRedditSource(source, deps = {}) {
  if (typeof deps.fetchHtml === 'function') {
    const pageUrl = getOldRedditListingUrl(source)
    try {
      const { html, byteLength } = await fetchOldRedditHtml(pageUrl, deps)
      const children = parseOldRedditListingPosts(source, html)
      const newest = children
        .map((post) => getRedditPostDate(post))
        .filter(Boolean)
        .sort((a, b) => b.getTime() - a.getTime())[0]

      return {
        apiUrl: pageUrl,
        byteLength,
        postCount: children.length,
        newest,
        firstPostId: children[0]?.id ? String(children[0].id) : null,
      }
    } catch (err) {
      if (!isRedditRateLimitError(err)) throw err
      deps.logger?.warn?.(
        `Reddit HTML preflight hit 429; falling back to RSS preflight: ${err.message}`
      )
      return preflightRedditRssSource(source, deps)
    }
  }

  if (typeof deps.fetchJson !== 'function') {
    throw new Error('preflightRedditSource requires fetchHtml or fetchJson')
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
  if (typeof deps.fetchHtml === 'function') {
    const htmlDelayMs = getRedditHtmlDelayMs(deps)
    deps.logger?.log?.(
      `Reddit HTML pacing: one listing/gallery request every ${(
        htmlDelayMs / 1000
      ).toFixed(1)}s`
    )
    deps.appendRunEvent?.('reddit_html_throttle_configured', {
      delayMs: htmlDelayMs,
      scope: 'listing_and_gallery',
    })
    let oldHtmlListingPosts = 0
    try {
      const posts = await fetchRedditPostsFromOldHtml(source, options, {
        ...deps,
        onListingPage: (details) => {
          oldHtmlListingPosts += Number(details.rawPostCount || 0)
          deps.onListingPage?.(details)
        },
      })
      if (posts.length > 0 || oldHtmlListingPosts > 0) return posts
      deps.logger?.warn?.(
        'Reddit HTML discovery returned an empty listing; trying RSS discovery.'
      )
      deps.appendRunEvent?.('reddit_discovery_fallback', {
        from: 'old_html',
        to: 'rss',
        reason: 'empty_listing',
      })
      return fetchRedditPostsFromRss(source, options, deps)
    } catch (err) {
      if (!isRedditAccessError(err)) throw err
      const reason = isRedditRateLimitError(err)
        ? 'rate_limited'
        : 'access_denied'
      deps.logger?.warn?.(
        `Reddit HTML discovery failed; falling back to RSS discovery: ${err.message}`
      )
      deps.appendRunEvent?.('reddit_discovery_fallback', {
        from: 'old_html',
        to: 'rss',
        reason,
        error: err.message,
      })
      return fetchRedditPostsFromRss(source, options, deps)
    }
  }

  if (typeof deps.fetchJson !== 'function') {
    throw new Error('fetchRedditPosts requires fetchHtml or fetchJson')
  }
  if (!deps.redgifsClient) {
    throw new Error('fetchRedditPosts requires redgifsClient')
  }

  const posts = []
  let after = null
  let page = 0
  const pageSize = deps.pageSize || DEFAULT_REDDIT_PAGE_SIZE
  let lastDiscoveryProgressPostCount = 0
  let lastDiscoveryProgressAt = 0
  const postConcurrency = options.postConcurrency || 1

  const maybeEmitProgress = (force = false) => {
    const now = Date.now()
    if (
      !force &&
      posts.length > 1 &&
      posts.length - lastDiscoveryProgressPostCount <
        REDDIT_DISCOVERY_PROGRESS_EVERY_POSTS &&
      now - lastDiscoveryProgressAt < REDDIT_DISCOVERY_PROGRESS_EVERY_MS
    ) {
      return
    }
    lastDiscoveryProgressPostCount = posts.length
    lastDiscoveryProgressAt = now
    const targetPosts = getDiscoveryTargetPosts(
      options,
      page,
      pageSize,
      posts.length
    )
    emitDiscoveryProgress(deps, {
      mode: 'reddit json',
      pages: page + 1,
      posts: posts.length,
      media: countPostMedia(posts),
      current: Math.min(posts.length, targetPosts),
      total: targetPosts,
    })
  }

  while (true) {
    if (options.endPage !== null && page > options.endPage) break
    const apiUrl = getRedditListingUrl(source, after, pageSize)
    let data
    try {
      ;({ data } = await deps.fetchJson(apiUrl))
    } catch (err) {
      if (page === 0 && /\bHTTP 403\b/.test(String(err?.message || ''))) {
        return fetchRedditPostsFromRss(source, options, deps)
      }
      throw err
    }
    const listing = data?.data
    const pagePosts = Array.isArray(listing?.children)
      ? listing.children.map((child) => child?.data).filter(Boolean)
      : []
    if (pagePosts.length === 0) break

    const remainingPosts =
      Number.isFinite(options.maxPosts) && options.maxPosts > 0
        ? Math.max(options.maxPosts - posts.length, 0)
        : pagePosts.length
    const postsToProcess = pagePosts.slice(0, remainingPosts)
    const processedPagePosts = await mapWithConcurrency(
      postsToProcess,
      postConcurrency,
      async (post) => {
        const mediaEntries = await getRedditMediaEntries(source, post, deps)
        return {
          ...post,
          id: String(post.id || ''),
          title: getRedditPostTitle(post),
          published: getRedditPostDate(post),
          mediaEntries,
        }
      }
    )

    for (const post of processedPagePosts) {
      posts.push(post)
      maybeEmitProgress(false)
      if (
        Number.isFinite(options.maxPosts) &&
        options.maxPosts > 0 &&
        posts.length >= options.maxPosts
      ) {
        maybeEmitProgress(true)
        return posts
      }
    }

    maybeEmitProgress(true)

    after = listing?.after || null
    if (!after) break
    page += 1
  }

  return posts
}

async function fetchRedditPostsFromRss(source, options = {}, deps = {}) {
  if (typeof deps.fetchHtml !== 'function') {
    throw new Error('Reddit RSS fallback requires fetchHtml')
  }

  const posts = []
  let after = null
  let page = 0
  const pageSize = deps.pageSize || DEFAULT_REDDIT_PAGE_SIZE
  const parsedFallbackDelayMs = Number.parseInt(
    String(deps.fallbackDelayMs ?? ''),
    10
  )
  const fallbackDelayMs =
    Number.isFinite(parsedFallbackDelayMs) && parsedFallbackDelayMs >= 0
      ? parsedFallbackDelayMs
      : DEFAULT_REDDIT_FALLBACK_DELAY_MS
  let lastDiscoveryProgressPostCount = 0
  let lastDiscoveryProgressAt = 0
  let htmlFallbackFailureCount = 0
  let firstHtmlFallbackFailure = null
  const postConcurrency = options.postConcurrency || 1
  const incrementalFilter = createIncrementalPostFilter(deps, options)
  if (incrementalFilter.active) {
    deps.logger?.log?.(
      `Reddit incremental frontier: ${incrementalFilter.knownPostCount} known post(s), latest ${incrementalFilter.latestPostId || 'unknown'}`
    )
  }

  const maybeEmitProgress = (force = false) => {
    const now = Date.now()
    if (
      !force &&
      posts.length > 1 &&
      posts.length - lastDiscoveryProgressPostCount <
        REDDIT_DISCOVERY_PROGRESS_EVERY_POSTS &&
      now - lastDiscoveryProgressAt < REDDIT_DISCOVERY_PROGRESS_EVERY_MS
    ) {
      return
    }
    lastDiscoveryProgressPostCount = posts.length
    lastDiscoveryProgressAt = now
    const targetPosts = getDiscoveryTargetPosts(
      options,
      page,
      pageSize,
      posts.length
    )
    emitDiscoveryProgress(deps, {
      mode: 'reddit rss/html',
      pages: page + 1,
      posts: posts.length,
      media: countPostMedia(posts),
      current: Math.min(posts.length, targetPosts),
      total: targetPosts,
    })
  }

  const noteHtmlFallbackFailure = (post, err) => {
    htmlFallbackFailureCount += 1
    if (!firstHtmlFallbackFailure) {
      firstHtmlFallbackFailure = {
        id: String(post?.id || 'unknown'),
        message: String(err?.message || err || 'unknown error'),
      }
    }
  }

  const emitHtmlFallbackSummary = () => {
    if (
      htmlFallbackFailureCount === 0 ||
      typeof deps.appendRunEvent !== 'function'
    ) {
      return
    }
    deps.appendRunEvent('reddit_html_media_fallback_summary', {
      failures: htmlFallbackFailureCount,
      samplePostId: firstHtmlFallbackFailure?.id || null,
      sampleMessage: firstHtmlFallbackFailure?.message || null,
    })
  }

  while (true) {
    if (options.endPage !== null && page > options.endPage) break
    const rssUrl = getRedditRssUrl(source, after, pageSize)
    const response = await fetchRedditHtmlWithRetry(
      rssUrl,
      {
        headers: {
          Accept: 'application/atom+xml,text/xml,application/xml',
          Referer: source.origin,
          'User-Agent': REDDIT_RSS_USER_AGENT,
        },
      },
      deps
    )
    const { html } = response
    const pagePosts = parseRssEntries(html, source)
    deps.onListingPage?.({
      mode: 'rss',
      page: page + 1,
      url: response.url || rssUrl,
      statusCode: response.statusCode || null,
      byteLength: response.byteLength || Buffer.byteLength(html || ''),
      rawPostCount: pagePosts.length,
    })
    if (pagePosts.length === 0) {
      deps.appendRunEvent?.('reddit_discovery_empty_page', {
        mode: 'rss',
        page: page + 1,
        url: response.url || rssUrl,
        statusCode: response.statusCode || null,
        byteLength: response.byteLength || Buffer.byteLength(html || ''),
      })
      break
    }
    const filteredPage = incrementalFilter.filterPage(pagePosts)
    deps.logger?.log?.(
      `Fetched reddit RSS page ${page + 1}: ${pagePosts.length} post(s), ${filteredPage.posts.length} new candidate(s)`
    )

    const remainingPosts =
      Number.isFinite(options.maxPosts) && options.maxPosts > 0
        ? Math.max(options.maxPosts - posts.length, 0)
        : filteredPage.posts.length
    const postsToProcess = filteredPage.posts.slice(0, remainingPosts)
    const processedPagePosts = await mapWithConcurrency(
      postsToProcess,
      postConcurrency,
      async (post) => {
        const rssMediaUrls = extractRedditHtmlMediaUrls(post.rssContentHtml)
        if (rssMediaUrls.length > 0) {
          post.htmlMediaUrls = rssMediaUrls
        }
        if (
          rssMediaUrls.length === 0 &&
          typeof deps.fetchPostHtml === 'function' &&
          post.url &&
          (/reddit\.com\/gallery\//i.test(post.url) ||
            /reddit\.com\/r\/[^/]+\/comments\//i.test(post.url))
        ) {
          try {
            const { html: postHtml } = await deps.fetchPostHtml(post.url, {
              headers: {
                Referer: source.origin,
                'User-Agent': REDDIT_RSS_USER_AGENT,
              },
            })
            post.htmlMediaUrls = extractRedditHtmlMediaUrls(postHtml)
            if (fallbackDelayMs > 0) await sleep(fallbackDelayMs)
          } catch (err) {
            noteHtmlFallbackFailure(post, err)
          }
        }

        const mediaEntries = await getRedditMediaEntries(source, post, deps)
        return {
          ...post,
          id: String(post.id || ''),
          title: getRedditPostTitle(post),
          published: getRedditPostDate(post),
          mediaEntries,
        }
      }
    )

    for (const post of processedPagePosts) {
      posts.push({
        ...post,
        id: String(post.id || ''),
        title: getRedditPostTitle(post),
        published: getRedditPostDate(post),
        mediaEntries: post.mediaEntries || [],
      })
      maybeEmitProgress(false)
      if (
        Number.isFinite(options.maxPosts) &&
        options.maxPosts > 0 &&
        posts.length >= options.maxPosts
      ) {
        maybeEmitProgress(true)
        emitHtmlFallbackSummary()
        return posts
      }
    }
    maybeEmitProgress(true)

    if (filteredPage.stopAfterPage) {
      deps.logger?.log?.(
        `Reddit incremental frontier reached after ${filteredPage.boundaryHits} known/old post(s).`
      )
      break
    }
    after = pagePosts[pagePosts.length - 1]?.id
      ? `t3_${pagePosts[pagePosts.length - 1].id}`
      : null
    if (!after || pagePosts.length < pageSize) break
    page += 1
  }

  maybeEmitProgress(true)
  emitHtmlFallbackSummary()
  return posts
}

module.exports = {
  DEFAULT_REDDIT_PAGE_SIZE,
  OLD_REDDIT_PAGE_SIZE,
  buildRedditFilename,
  fetchRedditPosts,
  getPostPageUrl,
  getRedditMediaEntries,
  getRedditPostDate,
  getRedditPostTitle,
  getRedditSubreddit,
  preflightRedditSource,
}

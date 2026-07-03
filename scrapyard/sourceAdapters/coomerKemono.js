'use strict'

const path = require('path')
const { normalizeMediaEntries, sanitizeToken } = require('../mediaEntries')
const mediaFileRecords = require('../mediaFileRecords')
const { createBoundaryPageFilter } = require('../sourceFrontier')
const {
  PAWCHIVE_ORIGIN,
  getPawchiveMediaUrl,
  getPawchivePreviewUrl,
} = require('../pawchive')

const DEFAULT_PAGE_SIZE = 50
const DEFAULT_POST_METADATA_CONCURRENCY = 4
const MEDIA_EXTENSION_RE =
  /\.(?:jpe?g|png|webp|gif|bmp|avif|mp4|m4v|webm|mov)$/i

function parseResolvedDate(date) {
  return mediaFileRecords.parseResolvedDate(date)
}

function normalizeCreatorName(value) {
  return String(value || '')
    .trim()
    .replace(/^@+/, '')
    .toLowerCase()
}

function cleanPostText(value) {
  return decodeHtmlEntities(
    String(value || '')
      .replace(/<br\s*\/?>/gi, '\n')
      .replace(/<\/(?:div|p|li)>/gi, '\n')
      .replace(/<[^>]+>/g, ' ')
  )
    .replace(/\s+/g, ' ')
    .trim()
}

function decodeHtmlEntities(value) {
  return String(value || '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#x([0-9a-f]+);/gi, (_, code) =>
      String.fromCodePoint(Number.parseInt(code, 16))
    )
    .replace(/&#(\d+);/g, (_, code) =>
      String.fromCodePoint(Number.parseInt(code, 10))
    )
}

function getPostTitleOrCaption(post = {}) {
  const title = cleanPostText(post.title)
  const caption = getPostCaption(post)
  if (!title) return caption || null
  if (!caption || caption === title) return title
  return `${title} - ${caption}`
}

function getPostCaption(post = {}) {
  const contentWithoutDownloadAnchors = String(post.content || '').replace(
    /<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>[\s\S]*?<\/a>/gi,
    (anchor, href) => (getExternalMediaCandidate(href) ? ' ' : anchor)
  )
  return cleanPostText(contentWithoutDownloadAnchors)
    .replace(/https?:\/\/[^\s<>"']+/gi, (url) =>
      getExternalMediaCandidate(url.replace(/[),.;]+$/g, '')) ? ' ' : url
    )
    .replace(/\s+/g, ' ')
    .trim()
}

function getPostsApiUrl(source, offset = 0, pageSize = DEFAULT_PAGE_SIZE) {
  return `${source.origin}/api/v1/${source.service}/user/${encodeURIComponent(
    source.userId
  )}/posts?o=${offset}`
}

function getPostPageUrl(source, post) {
  return `${source.origin}/${source.service}/user/${source.userId}/post/${post.id}`
}

function getLegacyKemonoPostPageUrls(source, post) {
  if (source.origin !== PAWCHIVE_ORIGIN || !post?.id) return []
  const pathPart = `/${source.service}/user/${source.userId}/post/${post.id}`
  return [
    `https://kemono.cr${pathPart}`,
    `https://kemono.su${pathPart}`,
    `https://kemono.party${pathPart}`,
  ]
}

function getPostApiUrl(source, post) {
  return `${source.origin}/api/v1/${source.service}/user/${encodeURIComponent(
    source.userId
  )}/post/${encodeURIComponent(post.id)}`
}

function getPostCommentsApiUrl(source, post) {
  return `${getPostApiUrl(source, post)}/comments`
}

function getMediaUrl(source, media, post = {}) {
  const mediaPath = String(media?.path || '').trim()
  if (!mediaPath) return null
  if (/^https?:\/\//i.test(mediaPath)) return mediaPath
  if (source.origin === PAWCHIVE_ORIGIN) {
    return post.has_full === true
      ? getPawchiveMediaUrl(mediaPath)
      : getPawchivePreviewUrl(mediaPath)
  }
  return `${source.origin}/data${mediaPath.startsWith('/') ? mediaPath : `/${mediaPath}`}`
}

function getLegacyKemonoMediaUrls(source, media) {
  const mediaPath = String(media?.path || '').trim()
  if (source.origin !== PAWCHIVE_ORIGIN || !mediaPath) return []
  const dataPath = `/data${mediaPath.startsWith('/') ? mediaPath : `/${mediaPath}`}`
  return [
    `https://kemono.cr${dataPath}`,
    `https://kemono.su${dataPath}`,
    `https://kemono.party${dataPath}`,
  ]
}

function getPawchiveQualityMetadata(source, media, post = {}) {
  if (source.origin !== PAWCHIVE_ORIGIN) return {}

  const isFullResolution = post.has_full === true
  return {
    mediaQuality: isFullResolution ? 'full' : 'pawchive_preview',
    needsFullResolution: !isFullResolution,
    fullResolutionStatus: isFullResolution ? 'source_full' : 'pending',
    fullResolutionUrl: getPawchiveMediaUrl(media?.path),
  }
}

function filenameFromMediaUrl(mediaUrl) {
  try {
    const name = decodeURIComponent(path.basename(new URL(mediaUrl).pathname))
    return name && name !== 'data' ? name : null
  } catch {
    return null
  }
}

function addPawchivePreviewSuffix(filename) {
  const extension = path.extname(filename)
  const basename = extension ? filename.slice(0, -extension.length) : filename
  return `${basename}.pawchive-preview${extension}`
}

function sanitizeMediaFilename(value) {
  const parsed = path.parse(
    String(value || '')
      .replace(/[\u0000-\u001f<>:"/\\|?*]/g, '_')
      .replace(/\s+/g, ' ')
      .replace(/[. ]+$/g, '')
      .trim()
  )
  if (!parsed.base || !parsed.ext || !MEDIA_EXTENSION_RE.test(parsed.ext)) {
    return null
  }
  const stem = parsed.name.slice(0, 160).replace(/[. ]+$/g, '')
  return stem ? `${stem}${parsed.ext.toLowerCase()}` : null
}

function getExternalMediaCandidate(rawUrl) {
  let parsed
  try {
    parsed = new URL(decodeHtmlEntities(rawUrl))
  } catch {
    return null
  }
  if (!/^https?:$/.test(parsed.protocol)) return null

  const originalUrl = parsed.toString()
  const hostname = parsed.hostname.toLowerCase()
  const originalName = decodeURIComponent(path.basename(parsed.pathname))
  if (!MEDIA_EXTENSION_RE.test(originalName)) return null

  if (
    hostname === 'dropbox.com' ||
    hostname === 'www.dropbox.com' ||
    hostname.endsWith('.dropbox.com') ||
    hostname === 'dl.dropboxusercontent.com'
  ) {
    parsed.hostname = 'dl.dropboxusercontent.com'
    parsed.searchParams.delete('raw')
    parsed.searchParams.set('dl', '1')
  }

  return {
    downloadUrl: parsed.toString(),
    originalUrl,
    originalName,
  }
}

function extractPostExternalUrls(post = {}) {
  const urls = []
  const add = (value) => {
    const url = String(value || '').trim()
    if (url) urls.push(url)
  }
  const embeds = Array.isArray(post.embed) ? post.embed : [post.embed]
  for (const embed of embeds) add(embed?.url)

  const content = String(post.content || '')
  const hrefPattern = /\bhref\s*=\s*["']([^"']+)["']/gi
  let match
  while ((match = hrefPattern.exec(content)) !== null) add(match[1])

  const text = cleanPostText(content)
  const urlPattern = /https?:\/\/[^\s<>"']+/gi
  while ((match = urlPattern.exec(text)) !== null) {
    add(match[0].replace(/[),.;]+$/g, ''))
  }
  return Array.from(new Set(urls))
}

function getExternalMediaEntriesFromPost(source, post, options = {}) {
  if (source.origin !== PAWCHIVE_ORIGIN) return []

  const postPublishedAt = parseResolvedDate(post.published)
  const mediaPageUrl = getPostPageUrl(source, post)
  const mediaPageUrls = [
    mediaPageUrl,
    ...getLegacyKemonoPostPageUrls(source, post),
  ]
  const title = getPostTitleOrCaption(post)
  const normalizeUrl =
    typeof options.normalizeUrl === 'function'
      ? options.normalizeUrl
      : (value) => String(value || '').trim()
  const seen = new Set()

  return extractPostExternalUrls(post)
    .map((rawUrl) => {
      const candidate = getExternalMediaCandidate(rawUrl)
      if (!candidate) return null
      const key = normalizeUrl(candidate.downloadUrl)
      if (!key || seen.has(key)) return null
      seen.add(key)

      const filename = sanitizeMediaFilename(
        `${String(post.id || 'post')}-${candidate.originalName}`
      )
      if (!filename) return null
      return {
        postId: String(post.id || ''),
        title,
        mediaPageUrl,
        mediaPageUrls,
        mediaUrl: candidate.downloadUrl,
        mediaUrls: [candidate.downloadUrl, candidate.originalUrl],
        sourceUrls: [candidate.originalUrl],
        filename,
        originalName: candidate.originalName,
        uploadedDate: postPublishedAt,
        mediaQuality: 'external_full',
        needsFullResolution: false,
        fullResolutionStatus: 'external_source',
        pageMeta: post.pageMeta || null,
        externalMedia: true,
      }
    })
    .filter(Boolean)
}

function getMediaEntriesFromPost(source, post, options = {}) {
  const postPublishedAt = parseResolvedDate(post.published)
  const mediaPageUrl = getPostPageUrl(source, post)
  const mediaPageUrls = [
    mediaPageUrl,
    ...getLegacyKemonoPostPageUrls(source, post),
  ]
  const title = getPostTitleOrCaption(post)
  const rawEntries = []
  if (post.file?.path) rawEntries.push(post.file)
  if (Array.isArray(post.attachments)) rawEntries.push(...post.attachments)

  const normalizeUrl =
    typeof options.normalizeUrl === 'function'
      ? options.normalizeUrl
      : (value) => String(value || '').trim()
  const seen = new Set()
  const entries = rawEntries
    .map((media) => {
      const mediaUrl = getMediaUrl(source, media, post)
      const sourceFilename = mediaUrl ? filenameFromMediaUrl(mediaUrl) : null
      const filename =
        sourceFilename &&
        source.origin === PAWCHIVE_ORIGIN &&
        post.has_full !== true
          ? addPawchivePreviewSuffix(sourceFilename)
          : sourceFilename
      if (!mediaUrl || !filename) return null
      const key = normalizeUrl(mediaUrl)
      if (seen.has(key)) return null
      seen.add(key)
      return {
        postId: String(post.id || ''),
        title,
        mediaPageUrl,
        mediaPageUrls,
        mediaUrl,
        mediaUrls: [mediaUrl, ...getLegacyKemonoMediaUrls(source, media)],
        filename,
        originalName: media.name || null,
        uploadedDate: postPublishedAt,
        pageMeta: post.pageMeta || null,
        ...getPawchiveQualityMetadata(source, media, post),
      }
    })
    .filter(Boolean)

  entries.push(...getExternalMediaEntriesFromPost(source, post, options))

  return normalizeMediaEntries(entries, {
    sourceSite: source.site,
    sourceService: source.service,
    sourceUserId: source.userId,
    sourceUsername: source.username || source.rawName || source.userId,
  })
}

function normalizePawchiveComments(comments) {
  if (!Array.isArray(comments)) return []
  return comments
    .map((comment) => ({
      author: cleanPostText(comment?.commenter_name) || null,
      posted: String(comment?.published || '').trim() || null,
      text: cleanPostText(comment?.content),
    }))
    .filter((comment) => comment.text)
}

async function mapWithConcurrency(items, limit, mapper) {
  const concurrency = Math.max(
    Number.parseInt(String(limit || DEFAULT_POST_METADATA_CONCURRENCY), 10) ||
      DEFAULT_POST_METADATA_CONCURRENCY,
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

async function enrichPawchivePosts(source, posts, deps = {}) {
  if (
    source.origin !== PAWCHIVE_ORIGIN ||
    !posts.length ||
    typeof deps.fetchJson !== 'function'
  ) {
    return posts
  }

  let completed = 0
  let detailFailures = 0
  let commentFailures = 0
  const enriched = await mapWithConcurrency(
    posts,
    deps.postConcurrency || DEFAULT_POST_METADATA_CONCURRENCY,
    async (post) => {
      let detailedPost = post
      if (post.detail_fetched !== true) {
        try {
          const result = await deps.fetchJson(getPostApiUrl(source, post))
          if (result?.data && typeof result.data === 'object') {
            detailedPost = { ...post, ...result.data }
          }
        } catch {
          detailFailures += 1
        }
      }

      let pageMeta = null
      try {
        const result = await deps.fetchJson(
          getPostCommentsApiUrl(source, detailedPost)
        )
        const comments = normalizePawchiveComments(result?.data)
        pageMeta = {
          comments,
          commentCount: Array.isArray(result?.data)
            ? result.data.length
            : comments.length,
        }
      } catch {
        commentFailures += 1
      }

      completed += 1
      deps.logger?.status?.(
        `Fetching Pawchive metadata: ${completed}/${posts.length} post(s)`
      )
      return {
        ...detailedPost,
        ...(pageMeta ? { pageMeta } : {}),
      }
    }
  )

  if (detailFailures || commentFailures) {
    deps.logger?.log?.(
      `Pawchive metadata warnings: ${detailFailures} detail, ${commentFailures} comment request(s) failed`
    )
  }
  return enriched
}

async function findCreatorIdByName(source, fetchJson) {
  const { data: creators } = await fetchJson(`${source.origin}/api/v1/creators`)
  if (!Array.isArray(creators)) return null

  const normalizedName = normalizeCreatorName(source.userId)
  const hit = creators.find(
    (creator) =>
      creator?.service === source.service &&
      normalizeCreatorName(creator?.name) === normalizedName
  )

  return hit ? String(hit.id) : null
}

async function resolveKemonoCreatorIdForJson(source, deps = {}) {
  if (source.site !== 'kemono' || /^\d+$/.test(source.userId)) {
    return false
  }
  if (typeof deps.fetchJson !== 'function') {
    throw new Error('resolveKemonoCreatorIdForJson requires fetchJson')
  }

  const resolvedId = await findCreatorIdByName(source, deps.fetchJson).catch(
    () => null
  )
  if (!resolvedId) {
    throw new Error(
      `Pawchive rejected "${source.userId}" for ${source.service}. Pawchive creator URLs need the numeric creator ID, and that username was not found in /api/v1/creators.`
    )
  }

  deps.logger?.log?.(
    `Resolved Pawchive creator ${source.userId} -> ${resolvedId}`
  )
  source.userId = resolvedId
  source.rawName = sanitizeToken(resolvedId)
  return true
}

async function preflightCoomerKemonoSource(source, page = 0, deps = {}) {
  if (typeof deps.fetchJson !== 'function') {
    throw new Error('preflightCoomerKemonoSource requires fetchJson')
  }
  const pageSize = deps.pageSize || DEFAULT_PAGE_SIZE
  const offset = page * pageSize
  const apiUrl = getPostsApiUrl(source, offset, pageSize)
  const { data, byteLength } = await deps.fetchJson(apiUrl)

  if (!Array.isArray(data)) {
    throw new Error(
      `Expected ${apiUrl} to return a JSON post array, got ${typeof data}`
    )
  }

  const newest = data
    .map((post) => parseResolvedDate(post?.published))
    .filter(Boolean)
    .sort((a, b) => b.getTime() - a.getTime())[0]

  return {
    apiUrl,
    byteLength,
    postCount: data.length,
    newest,
    firstPostId: data[0]?.id ? String(data[0].id) : null,
  }
}

async function fetchCoomerKemonoPosts(source, options = {}, deps = {}) {
  if (typeof deps.fetchJson !== 'function') {
    throw new Error('fetchCoomerKemonoPosts requires fetchJson')
  }

  const pageSize = deps.pageSize || DEFAULT_PAGE_SIZE
  const posts = []
  let previewMediaSelected = 0
  let linkedMediaSelected = 0
  let commentsFetched = 0
  let page = options.startPage || 0
  let fetchedPages = 0
  const pageFilter = createBoundaryPageFilter(deps.sourceFrontier, {
    fullRefresh: deps.fullSourceRefresh,
    overlapPages: deps.sourceIncrementalOverlapPages ?? 1,
  })
  if (pageFilter.active) {
    deps.logger?.log?.(
      `${source.site} incremental frontier: ${deps.sourceFrontier.knownPostCount} confirmed post(s)`
    )
  }

  while (true) {
    if (options.endPage !== null && page > options.endPage) break
    const offset = page * pageSize
    const apiUrl = getPostsApiUrl(source, offset, pageSize)

    let pagePosts
    try {
      const pageResult = await deps.fetchJson(apiUrl)
      pagePosts = pageResult.data
    } catch (err) {
      if (source.site === 'kemono' && !/^\d+$/.test(source.userId)) {
        await resolveKemonoCreatorIdForJson(source, deps)
        continue
      }
      throw err
    }

    if (!Array.isArray(pagePosts) || pagePosts.length === 0) break
    const filteredPage = pageFilter.filterPage(pagePosts)
    const selectedPagePosts =
      Number.isFinite(options.maxPosts) && options.maxPosts > 0
        ? filteredPage.items.slice(
            0,
            Math.max(options.maxPosts - posts.length, 0)
          )
        : filteredPage.items
    const enrichedPagePosts = await enrichPawchivePosts(
      source,
      selectedPagePosts,
      {
        ...deps,
        postConcurrency: options.postConcurrency,
      }
    )
    posts.push(
      ...enrichedPagePosts.map((post) => {
        if (source.origin === PAWCHIVE_ORIGIN && post.has_full !== true) {
          previewMediaSelected +=
            (post.file?.path ? 1 : 0) +
            (Array.isArray(post.attachments)
              ? post.attachments.filter((item) => item?.path).length
              : 0)
        }
        const mediaEntries = getMediaEntriesFromPost(source, post, deps)
        linkedMediaSelected += mediaEntries.filter(
          (entry) => entry.externalMedia
        ).length
        commentsFetched += Array.isArray(post.pageMeta?.comments)
          ? post.pageMeta.comments.length
          : 0
        return {
          ...post,
          mediaEntries,
        }
      })
    )
    fetchedPages += 1
    deps.logger?.status?.(
      `Fetching ${source.site} pages: ${fetchedPages} page(s), ${posts.length} post(s), ${filteredPage.completedCount || 0} complete skipped`
    )
    if (
      Number.isFinite(options.maxPosts) &&
      options.maxPosts > 0 &&
      posts.length >= options.maxPosts
    ) {
      break
    }
    if (filteredPage.stopAfterPage) break
    if (pagePosts.length < pageSize) break
    page += 1
  }

  deps.logger?.statusDone?.(
    fetchedPages > 0
      ? `Fetched ${source.site} pages: ${fetchedPages} page(s), ${posts.length} post(s)${
          previewMediaSelected > 0
            ? `, ${previewMediaSelected} Pawchive preview media selected`
            : ''
        }${
          linkedMediaSelected > 0
            ? `, ${linkedMediaSelected} linked media selected`
            : ''
        }${commentsFetched > 0 ? `, ${commentsFetched} comments fetched` : ''}`
      : ''
  )
  return posts
}

module.exports = {
  DEFAULT_PAGE_SIZE,
  fetchCoomerKemonoPosts,
  findCreatorIdByName,
  getMediaEntriesFromPost,
  getExternalMediaEntriesFromPost,
  getMediaUrl,
  getPostApiUrl,
  getPostCommentsApiUrl,
  getPostTitleOrCaption,
  getPostPageUrl,
  getPostsApiUrl,
  normalizeCreatorName,
  preflightCoomerKemonoSource,
  resolveKemonoCreatorIdForJson,
}

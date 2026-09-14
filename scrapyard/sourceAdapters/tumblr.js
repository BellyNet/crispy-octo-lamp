'use strict'

const path = require('path')
const { normalizeMediaEntries } = require('../mediaEntries')
const mediaFileRecords = require('../mediaFileRecords')
const { createBoundaryPageFilter } = require('../sourceFrontier')

const DEFAULT_PAGE_SIZE = 50
const JSONP_PREFIX_RE = /^\s*var\s+tumblr_api_read\s*=\s*/
const HTML_BODY_FIELDS = [
  'regular-body',
  'photo-caption',
  'video-caption',
  'video-player',
  'answer',
  'chat-body',
]

function parseResolvedDate(date) {
  return mediaFileRecords.parseResolvedDate(date)
}

// The legacy read API always wraps its JSON in `var tumblr_api_read = ...;`,
// even when requested with `debug=1` (tested against a live blog); strip it.
function parseTumblrJsonpBody(body) {
  const unwrapped = String(body || '')
    .trim()
    .replace(JSONP_PREFIX_RE, '')
    .replace(/;\s*$/, '')
  return JSON.parse(unwrapped)
}

function htmlDecode(value) {
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

function cleanPostText(value) {
  return htmlDecode(
    String(value || '')
      .replace(/<br\s*\/?>/gi, '\n')
      .replace(/<\/(?:div|p|li|h1|h2|h3)>/gi, '\n')
      .replace(/<[^>]+>/g, ' ')
  )
    .replace(/[ \t]+/g, ' ')
    .replace(/ *\n */g, '\n')
    .trim()
}

function getTumblrPostsApiUrl(source, start = 0, pageSize = DEFAULT_PAGE_SIZE) {
  const url = new URL('/api/read/json', source.origin)
  url.searchParams.set('start', String(Math.max(start, 0)))
  url.searchParams.set('num', String(pageSize))
  return url.toString()
}

function getTumblrPostPageUrl(source, post = {}) {
  return post.url ? String(post.url) : `${source.origin}/post/${post.id}`
}

function filenameFromMediaUrl(mediaUrl) {
  try {
    return decodeURIComponent(path.basename(new URL(mediaUrl).pathname)) || null
  } catch {
    return null
  }
}

// Legacy-API image tags carry a `srcset` of resized derivatives; the widest
// entry consistently matches `data-orig-width` (verified against a live blog),
// so treat it as the original.
function pickLargestSrcsetUrl(srcsetAttr) {
  const candidates = String(srcsetAttr || '')
    .split(',')
    .map((part) => part.trim().match(/^(\S+)\s+(\d+)w$/))
    .filter(Boolean)
    .map((match) => ({ url: match[1], width: Number.parseInt(match[2], 10) }))
  if (candidates.length === 0) return null
  return candidates.sort((a, b) => b.width - a.width)[0].url
}

function extractMediaUrlsFromHtml(html) {
  const urls = []
  const figureRe = /<figure\b[^>]*>([\s\S]*?)<\/figure>/gi
  let figureMatch
  while ((figureMatch = figureRe.exec(html)) !== null) {
    const figureHtml = figureMatch[1]
    const videoSrcMatch = figureHtml.match(
      /<source\b[^>]*\bsrc=["']([^"']+)["']/i
    )
    if (videoSrcMatch) {
      urls.push(htmlDecode(videoSrcMatch[1]))
      continue
    }

    const imgMatch = figureHtml.match(/<img\b([^>]*)>/i)
    if (!imgMatch) continue
    const attrs = imgMatch[1]
    const srcsetMatch = attrs.match(/\bsrcset=["']([^"']+)["']/i)
    const srcMatch = attrs.match(/\bsrc=["']([^"']+)["']/i)
    const best =
      (srcsetMatch && pickLargestSrcsetUrl(htmlDecode(srcsetMatch[1]))) ||
      (srcMatch && htmlDecode(srcMatch[1]))
    if (best) urls.push(best)
  }
  return urls
}

function getPostHtml(post = {}) {
  return HTML_BODY_FIELDS.map((field) => post[field])
    .filter(Boolean)
    .join('\n')
}

function getPostCaptionText(post = {}) {
  const combined = [post['regular-title'], getPostHtml(post)]
    .filter(Boolean)
    .join('\n')
  return cleanPostText(combined) || null
}

function getPostUploadedDate(post = {}) {
  const timestamp = Number(post['unix-timestamp'])
  if (Number.isFinite(timestamp) && timestamp > 0) {
    return new Date(timestamp * 1000)
  }
  return parseResolvedDate(post['date-gmt'] || post.date)
}

// Reblogs surface someone else's images under this blog's feed; a model's
// dataset should only ever contain what the blog actually posted itself.
function isReblog(post = {}) {
  return Object.entries(post).some(
    ([key, value]) =>
      Boolean(value) && /^reblogged[-_](?:from|root)[-_]/i.test(key)
  )
}

function getMediaEntriesFromPost(source, post = {}) {
  if (isReblog(post)) return []

  const mediaPageUrl = getTumblrPostPageUrl(source, post)
  const text = getPostCaptionText(post)
  const uploadedDate = getPostUploadedDate(post)
  const mediaUrls = extractMediaUrlsFromHtml(getPostHtml(post))

  const seen = new Set()
  const entries = mediaUrls
    .map((mediaUrl) => {
      const filename = filenameFromMediaUrl(mediaUrl)
      if (!filename || seen.has(mediaUrl)) return null
      seen.add(mediaUrl)
      return {
        postId: String(post.id || ''),
        title: text,
        text,
        mediaPageUrl,
        mediaPageUrls: [mediaPageUrl],
        mediaUrl,
        mediaUrls: [mediaUrl],
        filename,
        originalName: filename,
        uploadedDate,
        pageMeta: {
          tags: Array.isArray(post.tags) ? post.tags : [],
          noteCount: Number.parseInt(post['note-count'], 10) || 0,
        },
      }
    })
    .filter(Boolean)

  return normalizeMediaEntries(entries, {
    sourceSite: source.site,
    sourceService: source.service,
    sourceUserId: source.userId,
    sourceUsername: source.rawName || source.userId,
  })
}

async function preflightTumblrSource(source, page = 0, deps = {}) {
  if (typeof deps.fetchJson !== 'function') {
    throw new Error('preflightTumblrSource requires fetchJson')
  }
  const pageSize = deps.pageSize || DEFAULT_PAGE_SIZE
  const apiUrl = getTumblrPostsApiUrl(source, page * pageSize, pageSize)
  const { data, byteLength } = await deps.fetchJson(apiUrl)
  const posts = Array.isArray(data?.posts) ? data.posts : []

  const newest = posts
    .map((post) => getPostUploadedDate(post))
    .filter(Boolean)
    .sort((a, b) => b.getTime() - a.getTime())[0]

  return {
    apiUrl,
    byteLength,
    postCount: posts.length,
    postsTotal: Number.isFinite(data?.['posts-total'])
      ? data['posts-total']
      : posts.length,
    newest,
    firstPostId: posts[0]?.id ? String(posts[0].id) : null,
  }
}

async function fetchTumblrPosts(source, options = {}, deps = {}) {
  if (typeof deps.fetchJson !== 'function') {
    throw new Error('fetchTumblrPosts requires fetchJson')
  }

  const pageSize = deps.pageSize || DEFAULT_PAGE_SIZE
  const posts = []
  let page = options.startPage || 0
  let fetchedPages = 0
  let skippedReblogCount = 0
  const pageFilter = createBoundaryPageFilter(deps.sourceFrontier, {
    fullRefresh: deps.fullSourceRefresh,
    overlapPages: deps.sourceIncrementalOverlapPages ?? 1,
  })
  if (pageFilter.active) {
    deps.logger?.log?.(
      `Tumblr incremental frontier: ${deps.sourceFrontier.knownPostCount} confirmed post(s)`
    )
  }

  while (true) {
    if (options.endPage !== null && page > options.endPage) break
    const apiUrl = getTumblrPostsApiUrl(source, page * pageSize, pageSize)
    const { data } = await deps.fetchJson(apiUrl)
    const pagePosts = Array.isArray(data?.posts) ? data.posts : []
    if (pagePosts.length === 0) break

    const filteredPage = pageFilter.filterPage(pagePosts)
    for (const post of filteredPage.items) {
      if (
        Number.isFinite(options.maxPosts) &&
        options.maxPosts > 0 &&
        posts.length >= options.maxPosts
      ) {
        break
      }
      if (isReblog(post)) {
        skippedReblogCount += 1
        continue
      }
      posts.push({
        id: String(post.id || ''),
        url: getTumblrPostPageUrl(source, post),
        title: getPostCaptionText(post),
        text: getPostCaptionText(post),
        published: getPostUploadedDate(post),
        mediaEntries: getMediaEntriesFromPost(source, post),
      })
    }

    fetchedPages += 1
    deps.logger?.status?.(
      `Fetching tumblr pages: ${fetchedPages} page(s), ${posts.length} post(s), ${filteredPage.completedCount || 0} complete skipped`
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
      ? `Fetched tumblr pages: ${fetchedPages} page(s), ${posts.length} post(s)${
          skippedReblogCount > 0
            ? `, ${skippedReblogCount} reblog(s) skipped`
            : ''
        }`
      : ''
  )
  return posts
}

module.exports = {
  DEFAULT_PAGE_SIZE,
  extractMediaUrlsFromHtml,
  fetchTumblrPosts,
  getMediaEntriesFromPost,
  getTumblrPostPageUrl,
  getTumblrPostsApiUrl,
  isReblog,
  parseTumblrJsonpBody,
  pickLargestSrcsetUrl,
  preflightTumblrSource,
}

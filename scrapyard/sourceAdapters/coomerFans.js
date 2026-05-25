'use strict'

const path = require('path')
const pLimit = require('p-limit')
const { normalizeMediaEntries, sanitizeToken } = require('../mediaEntries')
const mediaFileRecords = require('../mediaFileRecords')

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

function uniqueValues(values) {
  return Array.from(new Set((values || []).filter(Boolean)))
}

function absoluteUrl(source, href) {
  if (!href) return null
  return new URL(htmlDecode(href), source.origin).toString()
}

function extractRegexValues(text, regex, group = 1) {
  return Array.from(String(text || '').matchAll(regex))
    .map((match) => match[group])
    .filter(Boolean)
}

function filenameFromMediaUrl(mediaUrl) {
  try {
    const name = decodeURIComponent(path.basename(new URL(mediaUrl).pathname))
    return name && name !== 'data' ? name : null
  } catch {
    return null
  }
}

async function resolveCoomerFansCreator(source, deps = {}) {
  if (source.userId) return source
  if (typeof deps.fetchHtml !== 'function') {
    throw new Error('resolveCoomerFansCreator requires fetchHtml')
  }

  const searchUrl = `${source.origin}/?q=${encodeURIComponent(source.rawName)}`
  const { html } = await deps.fetchHtml(searchUrl)
  const candidates = extractRegexValues(
    html,
    /href=["']\/u\/([^/]+)\/(\d+)\/([^"']+)["']/gi,
    0
  )
    .map((href) => {
      const match = href.match(/\/u\/([^/]+)\/(\d+)\/([^"']+)/i)
      if (!match) return null
      return {
        service: match[1],
        userId: match[2],
        rawName: sanitizeToken(decodeURIComponent(match[3])),
      }
    })
    .filter(Boolean)

  const exact = candidates.find(
    (candidate) =>
      candidate.service === source.service &&
      candidate.rawName === source.rawName
  )
  const fallback = candidates.find(
    (candidate) => candidate.service === source.service
  )
  const resolved = exact || fallback
  if (!resolved) {
    throw new Error(`No CoomerFans creator found for ${source.rawName}`)
  }

  source.service = resolved.service
  source.userId = resolved.userId
  source.rawName = resolved.rawName
  source.inputUrl = `${source.origin}/u/${source.service}/${source.userId}/${source.rawName}`
  deps.logger?.log?.(
    `Resolved CoomerFans creator ${source.rawName} -> ${source.service}/${source.userId}`
  )
  return source
}

function getCoomerFansPageUrl(source, pageNumber = 1) {
  const base = `${source.origin}/u/${source.service}/${source.userId}/${source.rawName}`
  return pageNumber <= 1 ? base : `${base}?page=${pageNumber}`
}

function parseCoomerFansPostLinks(source, html) {
  return uniqueValues(
    extractRegexValues(html, /href=["'](\/p\/(\d+)\/(\d+)\/([^"']+))["']/gi, 1)
  )
    .filter((href) => href.includes(`/${source.userId}/`))
    .map((href) => {
      const match = href.match(/\/p\/(\d+)\/(\d+)\/([^/?#]+)/i)
      return {
        id: match?.[1] || path.basename(href),
        url: absoluteUrl(source, href),
      }
    })
    .filter((post) => post.id && post.url)
}

function parseCoomerFansDate(html) {
  const decodedHtml = htmlDecode(html)
  const match = decodedHtml.match(
    /Added\s+([0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9:]+\s+\+0000\s+UTC)/i
  )
  return match ? parseResolvedDate(match[1].replace(' UTC', '')) : null
}

function parseCoomerFansTitle(html) {
  const ogTitle = String(html || '').match(
    /<meta\s+property=["']og:title["']\s+content=["']([^"']+)["']/i
  )?.[1]
  if (ogTitle) return htmlDecode(ogTitle)
  return htmlDecode(String(html || '').match(/<h1[^>]*>(.*?)<\/h1>/is)?.[1])
    .replace(/<[^>]+>/g, ' ')
    .trim()
}

function parseCoomerFansMediaEntries(source, post, html) {
  const uploadedDate = parseCoomerFansDate(html)
  const title = parseCoomerFansTitle(html) || null
  const mediaUrls = uniqueValues(
    extractRegexValues(
      html,
      /https?:\/\/(?:img\d+\.coomerfans\.com|coomerfans\.com)\/(?:storage|videos?)\/[^"'<> \r\n]+/gi,
      0
    )
      .map((url) => htmlDecode(url))
      .filter((url) => !url.includes('/istorage/'))
  )

  const entries = mediaUrls
    .map((mediaUrl) => {
      const filename = filenameFromMediaUrl(mediaUrl)
      if (!filename) return null
      return {
        postId: String(post.id || ''),
        title,
        mediaPageUrl: post.url,
        mediaPageUrls: [post.url],
        mediaUrl,
        mediaUrls: [mediaUrl],
        filename,
        originalName: null,
        uploadedDate,
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

async function preflightCoomerFansSource(source, page = 0, deps = {}) {
  await resolveCoomerFansCreator(source, deps)
  const pageNumber = page + 1
  const pageUrl = getCoomerFansPageUrl(source, pageNumber)
  const { html, byteLength } = await deps.fetchHtml(pageUrl)
  const postLinks = parseCoomerFansPostLinks(source, html)

  return {
    apiUrl: pageUrl,
    byteLength,
    postCount: postLinks.length,
    newest: null,
    firstPostId: postLinks[0]?.id || null,
  }
}

async function fetchCoomerFansPosts(source, options = {}, deps = {}) {
  if (typeof deps.fetchHtml !== 'function') {
    throw new Error('fetchCoomerFansPosts requires fetchHtml')
  }

  await resolveCoomerFansCreator(source, deps)
  const posts = []
  let page = options.startPage || 0
  const postLimit = pLimit(options.postConcurrency || 1)

  while (true) {
    if (options.endPage !== null && page > options.endPage) break
    const pageNumber = page + 1
    const pageUrl = getCoomerFansPageUrl(source, pageNumber)
    deps.logger?.log?.(`Loading coomerfans page ${pageNumber} (${pageUrl})`)

    const { html } = await deps.fetchHtml(pageUrl)
    const postLinks = parseCoomerFansPostLinks(source, html)
    if (postLinks.length === 0) break

    const selectedPostLinks =
      Number.isFinite(options.maxPosts) && options.maxPosts > 0
        ? postLinks.slice(0, Math.max(options.maxPosts - posts.length, 0))
        : postLinks

    const pagePosts = await Promise.all(
      selectedPostLinks.map((post) =>
        postLimit(async () => {
          deps.logger?.log?.(`Loading coomerfans post ${post.id}`)
          const { html: postHtml } = await deps.fetchHtml(post.url)
          const mediaEntries = parseCoomerFansMediaEntries(
            source,
            post,
            postHtml
          )
          return {
            id: post.id,
            url: post.url,
            title: mediaEntries[0]?.title || null,
            published: mediaEntries[0]?.uploadedDate || null,
            mediaEntries,
          }
        })
      )
    )
    posts.push(...pagePosts)

    if (
      Number.isFinite(options.maxPosts) &&
      options.maxPosts > 0 &&
      posts.length >= options.maxPosts
    ) {
      break
    }

    if (!html.includes(`?page=${pageNumber + 1}`)) break
    page += 1
  }

  return posts
}

module.exports = {
  fetchCoomerFansPosts,
  getCoomerFansPageUrl,
  parseCoomerFansMediaEntries,
  parseCoomerFansPostLinks,
  preflightCoomerFansSource,
  resolveCoomerFansCreator,
}

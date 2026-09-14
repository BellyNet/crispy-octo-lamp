'use strict'

const path = require('path')
const pLimit = require('p-limit')
const { normalizeMediaEntries, sanitizeToken } = require('../mediaEntries')
const mediaFileRecords = require('../mediaFileRecords')
const { createBoundaryPageFilter } = require('../sourceFrontier')

const DEFAULT_COOMERFANS_RETRY_DELAY_MS = 5000
const DEFAULT_COOMERFANS_MAX_RETRIES = 2
const ONLYHAVEN_ORIGIN = 'https://cum.st'
const ONLYHAVEN_MEDIA_ORIGIN =
  process.env.ONLYHAVEN_MEDIA_ORIGIN || 'https://e1.cum.st'
const ONLYHAVEN_PAGE_SIZE = 50

function parseResolvedDate(date) {
  return mediaFileRecords.parseResolvedDate(date)
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function htmlDecode(value) {
  return String(value || '')
    .replace(/&amp;/g, '&')
    .replace(/&#x([0-9a-f]+);/gi, (_, code) =>
      String.fromCodePoint(Number.parseInt(code, 16))
    )
    .replace(/&#(\d+);/g, (_, code) =>
      String.fromCodePoint(Number.parseInt(code, 10))
    )
    .replace(/&#43;/g, '+')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
}

function cleanPostText(value) {
  return htmlDecode(
    String(value || '')
      .replace(/<br\s*\/?>/gi, '\n')
      .replace(/<\/(?:div|p|li)>/gi, '\n')
      .replace(/<[^>]+>/g, ' ')
  )
    .replace(/\s+/g, ' ')
    .trim()
}

function escapeRegExp(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function uniqueValues(values) {
  return Array.from(new Set((values || []).filter(Boolean)))
}

function absoluteUrl(source, href) {
  if (!href) return null
  return new URL(htmlDecode(href), source.origin).toString()
}

function getCoomerFansMaxRetries(deps = {}) {
  const value = Number.parseInt(
    String(
      deps.coomerFansMaxRetries ??
        process.env.HOGHAUL_COOMERFANS_MAX_RETRIES ??
        ''
    ),
    10
  )
  return Number.isFinite(value) && value >= 0
    ? value
    : DEFAULT_COOMERFANS_MAX_RETRIES
}

function getCoomerFansRetryDelayMs(deps = {}) {
  const value = Number.parseInt(
    String(
      deps.coomerFansRetryDelayMs ??
        process.env.HOGHAUL_COOMERFANS_RETRY_DELAY_MS ??
        ''
    ),
    10
  )
  return Number.isFinite(value) && value >= 0
    ? value
    : DEFAULT_COOMERFANS_RETRY_DELAY_MS
}

function isCoomerFansTransientError(err) {
  const message = String(err?.message || err || '')
  return /\bHTTP\s+(?:429|502|503|504)\b/i.test(message)
}

function isCoomerFansBrowserBlockedError(err) {
  const message = String(err?.message || err || '')
  return (
    /Checking your browser/i.test(message) ||
    /browser check did not clear/i.test(message)
  )
}

async function fetchCoomerFansHtml(url, deps = {}) {
  const maxRetries = getCoomerFansMaxRetries(deps)
  let lastError = null
  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      return await deps.fetchHtml(url)
    } catch (err) {
      lastError = err
      if (!isCoomerFansTransientError(err) || attempt >= maxRetries) throw err
      const delayMs = getCoomerFansRetryDelayMs(deps) * (attempt + 1)
      deps.logger?.warn?.(
        `CoomerFans transient HTML error; waiting ${Math.round(delayMs / 1000)}s before retry ${attempt + 1}: ${err.message}`
      )
      deps.appendRunEvent?.('coomerfans_html_retry', {
        attempt: attempt + 1,
        delayMs,
        url,
        error: err.message,
      })
      await sleep(delayMs)
    }
  }
  throw lastError
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

function isOnlyHavenSource(source = {}) {
  try {
    const host = new URL(
      source.origin || source.inputUrl || ''
    ).hostname.toLowerCase()
    return host === 'cum.st' || host.endsWith('.cum.st')
  } catch {
    return false
  }
}

function unwrapJsonPayload(result) {
  return result?.data && typeof result.data === 'object' ? result.data : result
}

function getOnlyHavenCreatorUrl(source) {
  return `${source.origin || ONLYHAVEN_ORIGIN}/creators/${source.service}/${source.userId}`
}

function getOnlyHavenProfileApiUrl(source) {
  return `${source.origin || ONLYHAVEN_ORIGIN}/api/v1/${source.service}/user/${source.userId}/profile`
}

function getOnlyHavenPostsApiUrl(source, pageNumber = 1) {
  const offset = Math.max(pageNumber - 1, 0) * ONLYHAVEN_PAGE_SIZE
  return `${source.origin || ONLYHAVEN_ORIGIN}/api/v1/${source.service}/user/${source.userId}/posts?o=${offset}&n=${ONLYHAVEN_PAGE_SIZE}`
}

function getOnlyHavenPostUrl(source, postId) {
  return `${getOnlyHavenCreatorUrl(source)}/post/${postId}`
}

function getOnlyHavenAttachmentPageUrl(source, post, attachment) {
  const postUrl = getOnlyHavenPostUrl(source, post.id)
  const position = Number.isFinite(Number(attachment?.position))
    ? String(Number(attachment.position))
    : '0'
  const hash = String(attachment?.sha256 || '')
    .trim()
    .slice(0, 12)
  const fragment = [position, hash]
    .map((part) => part.replace(/[^a-z0-9_-]+/gi, ''))
    .filter(Boolean)
    .join('-')
  return fragment ? `${postUrl}#attachment-${fragment}` : postUrl
}

function getOnlyHavenMediaUrl(attachment) {
  if (!attachment || attachment.locked || !attachment.sha256) return null
  const original = (attachment.variants || []).find((variant) =>
    /^original\./i.test(String(variant?.name || ''))
  )
  let filename = original?.name || ''
  if (!filename) {
    if (/^video\//i.test(attachment.mimeType || '')) filename = 'original.mp4'
    else if (/^image\/png/i.test(attachment.mimeType || '')) {
      filename = 'original.png'
    } else if (/^image\/webp/i.test(attachment.mimeType || '')) {
      filename = 'original.webp'
    } else if (/^image\//i.test(attachment.mimeType || '')) {
      filename = 'original.jpg'
    }
  }
  if (!filename) return null
  return `${ONLYHAVEN_MEDIA_ORIGIN}/media/${attachment.sha256}/${filename}`
}

function getOnlyHavenAttachmentFilename(post, attachment, mediaUrl) {
  const originalName = filenameFromMediaUrl(mediaUrl)
  if (!originalName) return null
  const postId = String(post?.id || '').trim()
  const position = Number.isFinite(Number(attachment?.position))
    ? String(Number(attachment.position))
    : '0'
  const hash = String(attachment?.sha256 || '')
    .trim()
    .slice(0, 12)
  const prefix = [postId, position, hash]
    .map((part) => part.replace(/[^a-z0-9_-]+/gi, ''))
    .filter(Boolean)
    .join('-')
  return prefix ? `${prefix}-${originalName}` : originalName
}

function parseOnlyHavenDate(value) {
  const seconds = Number(value || 0)
  if (!Number.isFinite(seconds) || seconds <= 0) return null
  return new Date(seconds * 1000).toISOString()
}

function getOnlyHavenPostText(post = {}) {
  return cleanPostText(post.captionHtml || post.title || '')
}

async function resolveOnlyHavenCreator(source, deps = {}) {
  if (typeof deps.fetchJson !== 'function') {
    throw new Error('resolveOnlyHavenCreator requires fetchJson')
  }

  if (source.userId) {
    const profile = unwrapJsonPayload(
      await deps.fetchJson(getOnlyHavenProfileApiUrl(source), {
        headers: { Accept: 'application/json' },
      })
    )
    source.rawName = sanitizeToken(profile?.name || source.rawName)
    source.username = profile?.name || source.username || null
    source.inputUrl = getOnlyHavenCreatorUrl(source)
    return source
  }

  const searchUrl = `${source.origin || ONLYHAVEN_ORIGIN}/api/v1/creators?q=${encodeURIComponent(source.rawName)}&service=${encodeURIComponent(source.service || 'onlyfans')}&n=5`
  const result = unwrapJsonPayload(
    await deps.fetchJson(searchUrl, {
      headers: { Accept: 'application/json' },
    })
  )
  const candidates = Array.isArray(result?.creators) ? result.creators : []
  const resolved =
    candidates.find(
      (candidate) =>
        candidate.service === source.service &&
        sanitizeToken(candidate.name) === sanitizeToken(source.rawName)
    ) ||
    candidates.find((candidate) => candidate.service === source.service) ||
    candidates[0]

  if (!resolved) {
    throw new Error(`No OnlyHaven creator found for ${source.rawName}`)
  }

  source.service = resolved.service
  source.userId = String(resolved.id)
  source.rawName = sanitizeToken(resolved.name)
  source.username = resolved.name || null
  source.inputUrl = getOnlyHavenCreatorUrl(source)
  deps.logger?.log?.(
    `Resolved OnlyHaven creator ${source.rawName} -> ${source.service}/${source.userId}`
  )
  return source
}

async function resolveCoomerFansCreator(source, deps = {}) {
  if (isOnlyHavenSource(source)) return resolveOnlyHavenCreator(source, deps)

  if (source.userId) return source
  if (typeof deps.fetchHtml !== 'function') {
    throw new Error('resolveCoomerFansCreator requires fetchHtml')
  }

  const searchUrl = `${source.origin}/?q=${encodeURIComponent(source.rawName)}`
  const { html } = await fetchCoomerFansHtml(searchUrl, deps)
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
  if (isOnlyHavenSource(source)) {
    return getOnlyHavenCreatorUrl(source)
  }

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

function parseCoomerFansCaption(html, creatorName = '') {
  const postWrap =
    String(html || '').match(
      /<div[^>]*class=["'][^"']*\bpost-wrap\b[^"']*["'][^>]*>([\s\S]*?)(?=<div[^>]*class=["'][^"']*\bpagination\b|$)/i
    )?.[1] || ''
  const postHeader = postWrap.split(
    /<div[^>]*class=["'][^"']*\bpost-body\b[^"']*["'][^>]*>/i
  )[0]
  const bodyCaption = cleanPostText(
    postHeader.match(/<p[^>]*>([\s\S]*?)<\/p>/i)?.[1]
  )
  if (bodyCaption) return bodyCaption

  const description = cleanPostText(
    String(html || '').match(
      /<meta\s+property=["']og:description["']\s+content=["']([^"']*)["']/i
    )?.[1]
  )
  if (!description) return null
  const prefix = String(creatorName || '').trim()
  return prefix
    ? description.replace(
        new RegExp(`^${escapeRegExp(prefix)}\\s*-\\s*`, 'i'),
        ''
      )
    : description
}

function parseCoomerFansTitle(html, creatorName = '') {
  const caption = parseCoomerFansCaption(html, creatorName)
  if (caption) return caption

  const heading = cleanPostText(
    String(html || '').match(/<h1[^>]*>(.*?)<\/h1>/is)?.[1]
  )
  if (heading) return heading

  const ogTitle = String(html || '').match(
    /<meta\s+property=["']og:title["']\s+content=["']([^"']+)["']/i
  )?.[1]
  const title = cleanPostText(ogTitle)
  const prefix = String(creatorName || '').trim()
  return prefix
    ? title.replace(new RegExp(`^${escapeRegExp(prefix)}\\s*\\/\\s*`, 'i'), '')
    : title
}

function parseCoomerFansMediaEntries(source, post, html) {
  const uploadedDate = parseCoomerFansDate(html)
  const title = parseCoomerFansTitle(html, source.rawName) || null
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
        text: title,
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

function parseOnlyHavenMediaEntries(source, post) {
  const title = getOnlyHavenPostText(post) || null
  const uploadedDate = parseOnlyHavenDate(post.published || post.added)
  const entries = (post.attachments || [])
    .map((attachment) => {
      const mediaUrl = getOnlyHavenMediaUrl(attachment)
      if (!mediaUrl) return null
      const mediaPageUrl = getOnlyHavenAttachmentPageUrl(
        source,
        post,
        attachment
      )
      const filename = getOnlyHavenAttachmentFilename(
        post,
        attachment,
        mediaUrl
      )
      if (!filename) return null
      return {
        postId: String(post.id || ''),
        title,
        text: title,
        mediaPageUrl,
        mediaPageUrls: [mediaPageUrl],
        mediaUrl,
        mediaUrls: [mediaUrl],
        filename,
        originalName: filenameFromMediaUrl(mediaUrl),
        uploadedDate,
        expectedBytes: Number(attachment.bytes || 0) || null,
        width: Number(attachment.width || 0) || null,
        height: Number(attachment.height || 0) || null,
        durationMs: Number(attachment.durationMs || 0) || null,
      }
    })
    .filter(Boolean)

  return normalizeMediaEntries(entries, {
    sourceSite: source.site,
    sourceService: source.service,
    sourceUserId: source.userId,
    sourceUsername: source.rawName || source.username || source.userId,
  })
}

function normalizeOnlyHavenPost(source, post) {
  const mediaEntries = parseOnlyHavenMediaEntries(source, post)
  return {
    id: String(post.id || ''),
    url: getOnlyHavenPostUrl(source, post.id),
    title: mediaEntries[0]?.title || getOnlyHavenPostText(post) || null,
    text: mediaEntries[0]?.text || getOnlyHavenPostText(post) || null,
    published:
      mediaEntries[0]?.uploadedDate || parseOnlyHavenDate(post.published),
    mediaEntries,
  }
}

async function preflightOnlyHavenSource(source, page = 0, deps = {}) {
  await resolveOnlyHavenCreator(source, deps)
  const pageNumber = page + 1
  const apiUrl = getOnlyHavenPostsApiUrl(source, pageNumber)
  const result = await deps.fetchJson(apiUrl, {
    headers: { Accept: 'application/json' },
  })
  const data = unwrapJsonPayload(result)
  const posts = Array.isArray(data?.posts) ? data.posts : []

  return {
    apiUrl,
    byteLength:
      result?.byteLength || Buffer.byteLength(JSON.stringify(data || {})),
    postCount: posts.length,
    newest: posts[0]?.published ? parseOnlyHavenDate(posts[0].published) : null,
    firstPostId: posts[0]?.id ? String(posts[0].id) : null,
  }
}

async function preflightCoomerFansSource(source, page = 0, deps = {}) {
  if (isOnlyHavenSource(source)) {
    return preflightOnlyHavenSource(source, page, deps)
  }

  await resolveCoomerFansCreator(source, deps)
  const pageNumber = page + 1
  const pageUrl = getCoomerFansPageUrl(source, pageNumber)
  const { html, byteLength } = await fetchCoomerFansHtml(pageUrl, deps)
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
  if (isOnlyHavenSource(source)) {
    if (typeof deps.fetchJson !== 'function') {
      throw new Error('fetchCoomerFansPosts requires fetchJson for OnlyHaven')
    }
  } else if (typeof deps.fetchHtml !== 'function') {
    throw new Error('fetchCoomerFansPosts requires fetchHtml')
  }

  await resolveCoomerFansCreator(source, deps)
  const posts = []
  let page = options.startPage || 0
  const postLimit = pLimit(options.postConcurrency || 1)
  const seenPostIds = new Set()
  let fetchedPages = 0
  let fetchedMedia = 0
  const pageFilter = createBoundaryPageFilter(deps.sourceFrontier, {
    fullRefresh: deps.fullSourceRefresh,
    overlapPages: deps.sourceIncrementalOverlapPages ?? 1,
  })
  if (pageFilter.active) {
    deps.logger?.log?.(
      `CoomerFans incremental frontier: ${deps.sourceFrontier.knownPostCount} confirmed post(s)`
    )
  }

  while (true) {
    if (options.endPage !== null && page > options.endPage) break
    const pageNumber = page + 1
    const pageUrl = isOnlyHavenSource(source)
      ? getOnlyHavenPostsApiUrl(source, pageNumber)
      : getCoomerFansPageUrl(source, pageNumber)

    if (isOnlyHavenSource(source)) {
      const data = unwrapJsonPayload(
        await deps.fetchJson(pageUrl, {
          headers: { Accept: 'application/json' },
        })
      )
      const pagePosts = (Array.isArray(data?.posts) ? data.posts : []).map(
        (post) => normalizeOnlyHavenPost(source, post)
      )
      if (pagePosts.length === 0) break

      const newPosts = pagePosts.filter((post) => !seenPostIds.has(post.id))
      if (newPosts.length === 0) break
      for (const post of newPosts) seenPostIds.add(post.id)
      const filteredPage = pageFilter.filterPage(newPosts)
      const selectedPosts =
        Number.isFinite(options.maxPosts) && options.maxPosts > 0
          ? filteredPage.items.slice(
              0,
              Math.max(options.maxPosts - posts.length, 0)
            )
          : filteredPage.items

      posts.push(...selectedPosts)
      fetchedPages += 1
      fetchedMedia += selectedPosts.reduce(
        (total, post) => total + (post.mediaEntries?.length || 0),
        0
      )
      deps.logger?.status?.(
        `Fetching onlyhaven pages: ${fetchedPages} page(s), ${posts.length} post(s), ${fetchedMedia} media, ${filteredPage.completedCount || 0} complete skipped`
      )

      if (
        Number.isFinite(options.maxPosts) &&
        options.maxPosts > 0 &&
        posts.length >= options.maxPosts
      ) {
        break
      }

      if (filteredPage.stopAfterPage) break
      page += 1
      continue
    }

    let html
    try {
      ;({ html } = await fetchCoomerFansHtml(pageUrl, deps))
    } catch (err) {
      if (posts.length > 0 && isCoomerFansBrowserBlockedError(err)) {
        deps.appendRunEvent?.('coomerfans_partial_browser_block', {
          page: page + 1,
          url: pageUrl,
          posts: posts.length,
          media: fetchedMedia,
          error: err.message,
        })
        deps.logger?.warn?.(
          `CoomerFans browser check blocked page ${page + 1}; keeping ${posts.length} discovered post(s).`
        )
        break
      }
      throw err
    }
    const postLinks = parseCoomerFansPostLinks(source, html)
    if (postLinks.length === 0) break

    const newPostLinks = postLinks.filter((post) => !seenPostIds.has(post.id))
    if (newPostLinks.length === 0) break
    for (const post of newPostLinks) seenPostIds.add(post.id)
    const filteredPage = pageFilter.filterPage(newPostLinks)

    const selectedPostLinks =
      Number.isFinite(options.maxPosts) && options.maxPosts > 0
        ? filteredPage.items.slice(
            0,
            Math.max(options.maxPosts - posts.length, 0)
          )
        : filteredPage.items

    let pagePosts
    try {
      pagePosts = await Promise.all(
        selectedPostLinks.map((post) =>
          postLimit(async () => {
            const { html: postHtml } = await fetchCoomerFansHtml(post.url, deps)
            const mediaEntries = parseCoomerFansMediaEntries(
              source,
              post,
              postHtml
            )
            return {
              id: post.id,
              url: post.url,
              title: mediaEntries[0]?.title || null,
              text: mediaEntries[0]?.text || null,
              published: mediaEntries[0]?.uploadedDate || null,
              mediaEntries,
            }
          })
        )
      )
    } catch (err) {
      if (posts.length > 0 && isCoomerFansBrowserBlockedError(err)) {
        deps.appendRunEvent?.('coomerfans_partial_browser_block', {
          page: page + 1,
          url: pageUrl,
          posts: posts.length,
          media: fetchedMedia,
          error: err.message,
        })
        deps.logger?.warn?.(
          `CoomerFans browser check blocked post detail on page ${page + 1}; keeping ${posts.length} discovered post(s).`
        )
        break
      }
      throw err
    }
    posts.push(...pagePosts)
    fetchedPages += 1
    fetchedMedia += pagePosts.reduce(
      (total, post) => total + (post.mediaEntries?.length || 0),
      0
    )
    deps.logger?.status?.(
      `Fetching coomerfans pages: ${fetchedPages} page(s), ${posts.length} post(s), ${fetchedMedia} media, ${filteredPage.completedCount || 0} complete skipped`
    )

    if (
      Number.isFinite(options.maxPosts) &&
      options.maxPosts > 0 &&
      posts.length >= options.maxPosts
    ) {
      break
    }

    if (filteredPage.stopAfterPage) break
    page += 1
  }

  deps.logger?.statusDone?.(
    fetchedPages > 0
      ? `Fetched ${isOnlyHavenSource(source) ? 'onlyhaven' : 'coomerfans'} pages: ${fetchedPages} page(s), ${posts.length} post(s), ${fetchedMedia} media`
      : ''
  )
  return posts
}

module.exports = {
  ONLYHAVEN_MEDIA_ORIGIN,
  ONLYHAVEN_ORIGIN,
  fetchCoomerFansPosts,
  getCoomerFansPageUrl,
  getOnlyHavenMediaUrl,
  getOnlyHavenPostsApiUrl,
  isOnlyHavenSource,
  parseOnlyHavenMediaEntries,
  parseCoomerFansCaption,
  parseCoomerFansMediaEntries,
  parseCoomerFansPostLinks,
  parseCoomerFansTitle,
  preflightCoomerFansSource,
  resolveCoomerFansCreator,
}

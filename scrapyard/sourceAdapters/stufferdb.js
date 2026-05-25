'use strict'

const path = require('path')
const { normalizeMediaEntry } = require('../mediaEntries')

function normalizeStufferDbHost(inputUrl) {
  return String(inputUrl || '')
    .trim()
    .replace(/&amp;/gi, '&')
    .replace(/https?:\/\/(?:www\.)?stufferai\.com/i, 'https://stufferdb.com')
    .replace(/https?:\/\/(?:www\.)?stufferdb\.com/i, 'https://stufferdb.com')
}

function getStufferDbMirrorUrl(inputUrl) {
  const normalized = normalizeStufferDbHost(inputUrl)
  if (!normalized) return null
  return normalized.replace(
    /^https?:\/\/(?:www\.)?stufferdb\.com/i,
    'https://stufferai.com'
  )
}

function getStufferDbFallbackUrls(inputUrl) {
  const primary = normalizeStufferDbHost(inputUrl)
  const mirror = getStufferDbMirrorUrl(primary)
  return [...new Set([primary, mirror].filter(Boolean))]
}

function normalizeStufferDbPictureUrl(inputUrl) {
  const raw = normalizeStufferDbHost(inputUrl)
  if (!raw) return raw
  if (!/stufferdb\.com/i.test(raw) || !/picture\?\//i.test(raw)) {
    return raw
  }

  return raw
    .replace(
      /(https?:\/\/(?:www\.)?stufferdb\.com)\/index(?:\.php)?\?\/picture\?\//i,
      '$1/picture?/'
    )
    .replace(/&acs=[^&]+/gi, '')
    .replace(/&slideshow=?/gi, '')
    .replace(/[?&=]+$/, '')
}

async function gotoStufferDbWithFallback(
  page,
  inputUrl,
  options = {},
  deps = {}
) {
  const urls = getStufferDbFallbackUrls(inputUrl)
  let lastError = null

  for (let index = 0; index < urls.length; index += 1) {
    const url = urls[index]
    try {
      const response = await page.goto(url, options)
      if (index > 0)
        deps.onFallback?.({ primaryUrl: urls[0], fallbackUrl: url })
      return { url, response, usedFallback: index > 0 }
    } catch (error) {
      lastError = error
      if (index === 0) {
        deps.onFallbackAttempt?.({
          primaryUrl: urls[0],
          fallbackUrl: urls[1] || null,
          error,
        })
      }
    }
  }

  throw lastError || new Error(`Could not load ${inputUrl}`)
}

function normalizeStufferDbCategoryUrl(inputUrl) {
  return normalizeStufferDbHost(inputUrl)
    .replace(/&acs=[^&]+/gi, '')
    .replace(/[?&=]+$/, '')
}

function getStufferDbCategoryId(inputUrl) {
  return String(inputUrl || '').match(/category\/?(\d+)/)?.[1] || null
}

function getStufferDbPictureId(inputUrl) {
  return String(inputUrl || '').match(/picture\?\/(\d+)/)?.[1] || null
}

async function getBreadcrumbInfo(page) {
  return await page.evaluate(() => {
    const h2 = document.querySelector('.titrePage h2')
    const anchors = [...(h2?.querySelectorAll('a') || [])].map((a) => ({
      text: a.textContent?.trim() || '',
      href: a.href || '',
    }))

    return {
      texts: anchors.map((a) => a.text).filter(Boolean),
      hrefs: anchors.map((a) => a.href).filter(Boolean),
    }
  })
}

async function collectChildCategoryUrls(browser, parentUrl, deps = {}) {
  const { createScraperPage, gotoWithTimeoutRetry, onRetry } = deps
  if (typeof createScraperPage !== 'function') {
    throw new Error('collectChildCategoryUrls requires createScraperPage')
  }
  if (typeof gotoWithTimeoutRetry !== 'function') {
    throw new Error('collectChildCategoryUrls requires gotoWithTimeoutRetry')
  }

  const page = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: true,
  })

  try {
    await gotoWithTimeoutRetry(page, parentUrl, {
      waitUntil: 'domcontentloaded',
      timeoutMs: deps.categoryPageTimeoutMs,
      retryTimeoutMs: deps.categoryPageRetryTimeoutMs,
      onRetry,
    })

    const candidateUrls = await page.evaluate(() => {
      const links = [
        ...document.querySelectorAll(
          'ul.thumbnailCategories li.album a, li.gdthumb.album a'
        ),
      ]

      return [
        ...new Set(
          links
            .map((a) => a.href || '')
            .filter((href) => href.includes('index?/category/'))
            .map((href) => href.replace(/&acs=[^&]+/gi, ''))
        ),
      ]
    })

    const parentNormalized = normalizeStufferDbCategoryUrl(parentUrl)
    return candidateUrls.filter((url) => url && url !== parentNormalized)
  } finally {
    if (!page.isClosed()) await page.close()
  }
}

async function buildCategoryRunList(browser, inputUrl, deps = {}) {
  const normalizedInput = normalizeStufferDbCategoryUrl(inputUrl)
  const childUrls = await collectChildCategoryUrls(
    browser,
    normalizedInput,
    deps
  )

  return [...new Set([normalizedInput, ...childUrls])]
}

async function fetchStufferDBTotalCount(browser, url, deps = {}) {
  const { createScraperPage, logger = console } = deps
  if (typeof createScraperPage !== 'function') {
    throw new Error('fetchStufferDBTotalCount requires createScraperPage')
  }

  const tempPage = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: true,
  })

  try {
    await gotoStufferDbWithFallback(
      tempPage,
      url,
      {
        waitUntil: 'domcontentloaded',
        timeout: deps.timeoutMs || 30000,
      },
      {
        onFallbackAttempt: deps.onFallbackAttempt,
        onFallback: deps.onFallback,
      }
    )

    await tempPage.waitForSelector('span.badge.nb_items', {
      timeout: deps.selectorTimeoutMs || 10000,
    })

    const rawText = await tempPage.$eval(
      'span.badge.nb_items',
      (el) => el.textContent || ''
    )
    logger.log?.(`🕵️ Raw badge text from ${url}:`, rawText)

    const match = rawText.match(/(\d+)/)
    const count = match ? parseInt(match[1], 10) : 0
    logger.log?.(`🔢 Parsed count: ${count}`)
    return count
  } catch (err) {
    const title = await tempPage.title().catch(() => 'unknown')
    logger.log?.(`⚠️ Could not fetch count for ${url}: ${err.message}`)
    logger.log?.(`🧙 Page title: ${title}`)
    return 0
  } finally {
    if (!tempPage.isClosed()) await tempPage.close()
  }
}

async function extractGalleryPictureUrls(page) {
  const urls = await page.$$eval('a[href^="picture?/"]', (links) =>
    links.map((l) => l.href)
  )
  const dedupedUrls = [
    ...new Set(
      urls.map((mediaPageUrl) => normalizeStufferDbPictureUrl(mediaPageUrl))
    ),
  ].filter(Boolean)

  return {
    urls: dedupedUrls,
    rawUrls: urls,
  }
}

async function extractStufferDbComments(page) {
  const commentsHostFrame = page
    .frames()
    .find((frame) => frame.url().includes('cmts.stufferdb.com/app'))

  if (!commentsHostFrame) {
    return { comments: [], commentCount: 0 }
  }

  try {
    await commentsHostFrame.waitForSelector(
      '#comments_list, .comment, .allcomments',
      {
        timeout: 5000,
      }
    )
  } catch {
    return { comments: [], commentCount: 0 }
  }

  try {
    return await commentsHostFrame.evaluate(() => {
      const countText =
        document.querySelector('.allcomments')?.textContent?.trim() || ''
      const countMatch = countText.match(/(\d+)/)
      const commentCount = countMatch ? Number.parseInt(countMatch[1], 10) : 0

      const comments = Array.from(
        document.querySelectorAll('#comments_list .comment')
      )
        .map((commentEl) => {
          const author =
            commentEl
              .querySelector('.comment-top .user-guest, .comment-top .user')
              ?.textContent?.trim() || null
          const posted =
            commentEl
              .querySelector('.comment-top .date')
              ?.textContent?.replace(/^•\s*/, '')
              .trim() || null
          const spoilerText =
            commentEl
              .querySelector('.comment-spoiler-text')
              ?.textContent?.trim() || ''
          const mainText =
            commentEl
              .querySelector('.comment-text-p, .comment-text, .comment-body')
              ?.textContent?.trim() || ''
          const text = [spoilerText, mainText].filter(Boolean).join('\n').trim()

          if (!text) return null

          return {
            author,
            posted,
            text,
          }
        })
        .filter(Boolean)

      return {
        comments,
        commentCount: Number.isFinite(commentCount)
          ? commentCount
          : comments.length,
      }
    })
  } catch {
    return { comments: [], commentCount: 0 }
  }
}

async function extractMediaPageDetails(page) {
  const uploadedDateIso = await page.evaluate(() => {
    const anchor = document.querySelector('#datepost dd a')
    if (!anchor) return null
    const text = anchor.textContent?.trim()
    const match = text.match(/\d{1,2} \w+ \d{4}/)
    if (!match) return null
    const date = new Date(match[0])
    return isNaN(date.getTime()) ? null : date.toISOString()
  })

  const pageMeta = await extractStufferDbComments(page)
  const mediaUrl = await page.evaluate(() => {
    const video = document.querySelector('video.vjs-tech[src]')
    const img = document.querySelector('#theMainImage')
    return video?.src || img?.src || null
  })

  if (!mediaUrl) {
    return {
      mediaUrl: null,
      filename: null,
      extension: null,
      uploadedDateIso,
      pageMeta,
    }
  }

  const parsed = new URL(mediaUrl)
  let filename = decodeURIComponent(
    path.basename(parsed.pathname).split('?')[0]
  )
  let extension = path.extname(filename).toLowerCase()
  if (extension === '.m4v') {
    extension = '.mp4'
    filename = filename.replace(/\.m4v$/i, '.mp4')
  }

  return {
    mediaUrl,
    filename,
    extension,
    uploadedDateIso,
    pageMeta,
  }
}

async function fetchStufferDbMediaEntry(
  page,
  mediaPageUrl,
  source = {},
  deps = {}
) {
  const normalizedMediaPageUrl = normalizeStufferDbPictureUrl(mediaPageUrl)
  const waitUntil = deps.waitUntil || 'domcontentloaded'
  const timeoutMs = deps.timeoutMs || 30000
  const retryTimeoutMs = deps.retryTimeoutMs || timeoutMs

  try {
    await gotoStufferDbWithFallback(
      page,
      normalizedMediaPageUrl,
      {
        waitUntil,
        timeout: timeoutMs,
      },
      {
        onFallbackAttempt: deps.onFallbackAttempt,
        onFallback: deps.onFallback,
      }
    )
  } catch (error) {
    if (!/Navigation timeout/i.test(error.message || '')) {
      throw error
    }

    deps.onRetry?.(error)
    if (typeof deps.sleep === 'function') {
      await deps.sleep(deps.retryDelayMs || 750)
    }
    await gotoStufferDbWithFallback(
      page,
      normalizedMediaPageUrl,
      {
        waitUntil,
        timeout: retryTimeoutMs,
      },
      {
        onFallbackAttempt: deps.onFallbackAttempt,
        onFallback: deps.onFallback,
      }
    )
  }

  const mediaDetails = await extractMediaPageDetails(page)
  return buildStufferDbMediaEntry(source, normalizedMediaPageUrl, mediaDetails)
}

function buildStufferDbMediaEntry(
  source = {},
  mediaPageUrl,
  mediaDetails = {}
) {
  const normalizedMediaPageUrl = normalizeStufferDbPictureUrl(mediaPageUrl)
  const sourceUrl = source.url
    ? normalizeStufferDbCategoryUrl(source.url)
    : null
  if (!mediaDetails.mediaUrl || !mediaDetails.filename) return null

  return normalizeMediaEntry(
    {
      sourceSite: 'stufferdb',
      sourceService: source.service || 'category',
      sourceUserId:
        source.categoryId || getStufferDbCategoryId(sourceUrl) || null,
      sourceUsername: source.modelName || source.username || null,
      postId: getStufferDbPictureId(normalizedMediaPageUrl),
      mediaPageUrl: normalizedMediaPageUrl,
      mediaPageUrls: [normalizedMediaPageUrl],
      mediaUrl: mediaDetails.mediaUrl,
      mediaUrls: [mediaDetails.mediaUrl],
      sourceUrls: [sourceUrl].filter(Boolean),
      filename: mediaDetails.filename,
      originalName: mediaDetails.filename,
      uploadedDate: mediaDetails.uploadedDateIso,
      pageMeta: mediaDetails.pageMeta,
    },
    {
      sourceSite: 'stufferdb',
      sourceService: source.service || 'category',
      sourceUserId:
        source.categoryId || getStufferDbCategoryId(sourceUrl) || null,
      sourceUsername: source.modelName || source.username || null,
    }
  )
}

module.exports = {
  buildStufferDbMediaEntry,
  buildCategoryRunList,
  collectChildCategoryUrls,
  extractGalleryPictureUrls,
  extractMediaPageDetails,
  extractStufferDbComments,
  fetchStufferDbMediaEntry,
  fetchStufferDBTotalCount,
  getStufferDbFallbackUrls,
  getBreadcrumbInfo,
  getStufferDbMirrorUrl,
  getStufferDbCategoryId,
  getStufferDbPictureId,
  gotoStufferDbWithFallback,
  normalizeStufferDbHost,
  normalizeStufferDbCategoryUrl,
  normalizeStufferDbPictureUrl,
}

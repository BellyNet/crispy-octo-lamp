'use strict'

const fs = require('fs')
const path = require('path')
const { isLikelyMediaUrl } = require('./mediaEntries')

function defaultNormalizeSeenUrl(url) {
  return String(url || '')
    .trim()
    .replace(/&acs=[^&]+/gi, '')
}

function normalizeIsoDate(value) {
  if (value instanceof Date && !isNaN(value.getTime()))
    return value.toISOString()
  if (typeof value === 'string' || typeof value === 'number') {
    const parsed = new Date(value)
    if (!isNaN(parsed.getTime())) return parsed.toISOString()
  }
  return null
}

function createMediaSeenIndex(options = {}) {
  const datasetDir = options.datasetDir
  const existsLocallyOrOnNas = options.existsLocallyOrOnNas
  const normalizeUrl = options.normalizeUrl || defaultNormalizeSeenUrl
  const warn = options.warn || ((message) => console.warn(message))
  const matchOrder = options.matchOrder || ['media_url', 'media_page_url']
  const pageMatchRequiresNoMediaUrl = Boolean(
    options.pageMatchRequiresNoMediaUrl
  )

  if (!datasetDir) {
    throw new Error('createMediaSeenIndex requires datasetDir')
  }
  if (typeof existsLocallyOrOnNas !== 'function') {
    throw new Error('createMediaSeenIndex requires existsLocallyOrOnNas')
  }

  let mediaSeenIndexCache = null

  function uniqueSeenUrls(values) {
    return Array.from(
      new Set(
        values
          .flat(Infinity)
          .map((url) => normalizeUrl(url))
          .filter(Boolean)
      )
    )
  }

  function uniqueSeenMediaUrls(values) {
    return uniqueSeenUrls(values).filter((url) => isLikelyMediaUrl(url))
  }

  function getMediaSeenIndexPath(modelLogDir) {
    return path.join(modelLogDir, 'milkmaid-seen-media-index.json')
  }

  function loadMediaSeenIndex(modelLogDir) {
    const indexPath = getMediaSeenIndexPath(modelLogDir)
    if (mediaSeenIndexCache?.indexPath === indexPath) {
      return mediaSeenIndexCache.data
    }

    let parsed = {}
    if (fs.existsSync(indexPath)) {
      try {
        parsed = JSON.parse(fs.readFileSync(indexPath, 'utf8'))
      } catch (err) {
        warn(`Could not parse media seen index at ${indexPath}: ${err.message}`)
      }
    }

    const data = {
      version: 1,
      updatedAt: parsed?.updatedAt || null,
      mediaPageUrls:
        parsed?.mediaPageUrls && typeof parsed.mediaPageUrls === 'object'
          ? parsed.mediaPageUrls
          : {},
      mediaUrls:
        parsed?.mediaUrls && typeof parsed.mediaUrls === 'object'
          ? parsed.mediaUrls
          : {},
    }

    mediaSeenIndexCache = { indexPath, data }
    return data
  }

  function saveMediaSeenIndex(modelLogDir, data) {
    const indexPath = getMediaSeenIndexPath(modelLogDir)
    data.updatedAt = new Date().toISOString()
    fs.writeFileSync(indexPath, JSON.stringify(data, null, 2) + '\n')
    mediaSeenIndexCache = { indexPath, data }
  }

  function getActiveMediaSeenRecord(entry) {
    if (!entry?.relativePath) return null
    const absolutePath = path.join(
      datasetDir,
      String(entry.relativePath).replace(/\//g, path.sep)
    )
    if (!existsLocallyOrOnNas(absolutePath)) return null
    return {
      ...entry,
      absolutePath,
    }
  }

  function getMediaEntry(index, normalizedMediaUrl) {
    const mediaEntry = getActiveMediaSeenRecord(
      index.mediaUrls[normalizedMediaUrl]
    )
    if (!mediaEntry) return null
    return { matchType: 'media_url', ...mediaEntry }
  }

  function getPageEntry(index, normalizedMediaPageUrl) {
    const pageEntry = getActiveMediaSeenRecord(
      index.mediaPageUrls[normalizedMediaPageUrl]
    )
    if (!pageEntry) return null
    return { matchType: 'media_page_url', ...pageEntry }
  }

  function getSuccessfulSeenMediaMatch(modelLogDir, mediaPageUrl, mediaUrl) {
    const index = loadMediaSeenIndex(modelLogDir)
    const mediaPageUrls = uniqueSeenUrls(
      Array.isArray(mediaPageUrl) ? mediaPageUrl : [mediaPageUrl]
    )
    const mediaUrls = uniqueSeenMediaUrls(
      Array.isArray(mediaUrl) ? mediaUrl : [mediaUrl]
    )

    for (const key of matchOrder) {
      if (key === 'media_url') {
        for (const normalizedMediaUrl of mediaUrls) {
          const match = getMediaEntry(index, normalizedMediaUrl)
          if (match) return match
        }
      }

      if (key === 'media_page_url') {
        if (pageMatchRequiresNoMediaUrl && mediaUrls.length > 0) continue
        for (const normalizedMediaPageUrl of mediaPageUrls) {
          const match = getPageEntry(index, normalizedMediaPageUrl)
          if (match) return match
        }
      }
    }

    return null
  }

  function recordSeenMedia(modelLogDir, details = {}) {
    const relativePath = String(details.relativePath || '').trim()
    if (!relativePath) return

    const index = loadMediaSeenIndex(modelLogDir)
    const mediaPageUrls = uniqueSeenUrls([
      details.mediaPageUrl,
      details.mediaPageUrls,
    ])
    const mediaUrls = uniqueSeenMediaUrls([details.mediaUrl, details.mediaUrls])
    const status = String(details.status || 'saved').trim() || 'saved'
    const recordedAt = new Date().toISOString()
    const payload = {
      relativePath,
      filename: details.filename || path.basename(relativePath),
      mediaUrl: mediaUrls[0] || null,
      mediaUrls,
      mediaPageUrl: mediaPageUrls[0] || null,
      mediaPageUrls,
      sourceSite: details.sourceSite || null,
      sourceService: details.sourceService || null,
      sourceUserId: details.sourceUserId || null,
      sourceUsername: details.sourceUsername || null,
      sourceSubreddit: details.sourceSubreddit || null,
      postId: details.postId || details.sourcePostId || null,
      uploadedDate: normalizeIsoDate(details.uploadedDate),
      status,
      recordedAt,
    }

    if (status === 'saved') {
      payload.savedAt = recordedAt
    } else if (status === 'quarantined_failed') {
      payload.failedAt = recordedAt
      payload.error = details.error || null
      payload.quarantinePath = details.quarantinePath || null
      payload.bytesDownloaded = Number.isFinite(details.bytesDownloaded)
        ? details.bytesDownloaded
        : null
      payload.expectedBytes = Number.isFinite(details.expectedBytes)
        ? details.expectedBytes
        : null
    }

    let changed = false
    for (const normalizedMediaPageUrl of mediaPageUrls) {
      if (
        index.mediaPageUrls[normalizedMediaPageUrl]?.relativePath !==
          relativePath ||
        index.mediaPageUrls[normalizedMediaPageUrl]?.status !== status
      ) {
        index.mediaPageUrls[normalizedMediaPageUrl] = payload
        changed = true
      }
    }
    for (const normalizedMediaUrl of mediaUrls) {
      if (
        index.mediaUrls[normalizedMediaUrl]?.relativePath !== relativePath ||
        index.mediaUrls[normalizedMediaUrl]?.status !== status
      ) {
        index.mediaUrls[normalizedMediaUrl] = payload
        changed = true
      }
    }

    if (changed) saveMediaSeenIndex(modelLogDir, index)
  }

  function recordSuccessfulSeenMedia(modelLogDir, details = {}) {
    recordSeenMedia(modelLogDir, {
      ...details,
      status: 'saved',
    })
  }

  function recordFailedSeenMedia(modelLogDir, details = {}) {
    recordSeenMedia(modelLogDir, {
      ...details,
      status: 'quarantined_failed',
    })
  }

  return {
    uniqueSeenUrls,
    uniqueSeenMediaUrls,
    getMediaSeenIndexPath,
    loadMediaSeenIndex,
    saveMediaSeenIndex,
    getActiveMediaSeenRecord,
    getSuccessfulSeenMediaMatch,
    recordSeenMedia,
    recordSuccessfulSeenMedia,
    recordFailedSeenMedia,
  }
}

module.exports = {
  createMediaSeenIndex,
  defaultNormalizeSeenUrl,
}

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

const SEEN_RECORD_FIELDS = [
  'relativePath',
  'filename',
  'mediaUrl',
  'mediaUrls',
  'mediaPageUrl',
  'mediaPageUrls',
  'sourceSite',
  'sourceService',
  'sourceUserId',
  'sourceUsername',
  'sourceSubreddit',
  'postId',
  'title',
  'text',
  'originalName',
  'mediaQuality',
  'needsFullResolution',
  'fullResolutionStatus',
  'fullResolutionUrl',
  'fullResolutionResolvedPath',
  'uploadedDate',
  'status',
  'error',
  'quarantinePath',
  'bytesDownloaded',
  'expectedBytes',
]

const PRESERVED_SEEN_METADATA_FIELDS = [
  'sourceSite',
  'sourceService',
  'sourceUserId',
  'sourceUsername',
  'sourceSubreddit',
  'postId',
  'title',
  'text',
  'originalName',
  'mediaQuality',
  'needsFullResolution',
  'fullResolutionStatus',
  'fullResolutionUrl',
  'fullResolutionResolvedPath',
  'uploadedDate',
]

function mergeSeenRecord(existing, payload) {
  if (!existing || typeof existing !== 'object') return payload

  const merged = {
    ...existing,
    ...payload,
    recordedAt: existing.recordedAt || payload.recordedAt,
  }
  for (const field of PRESERVED_SEEN_METADATA_FIELDS) {
    if (payload[field] == null && existing[field] != null) {
      merged[field] = existing[field]
    }
  }
  if (payload.status === 'saved') {
    merged.savedAt = existing.savedAt || payload.savedAt
    delete merged.failedAt
    delete merged.error
    delete merged.quarantinePath
    delete merged.bytesDownloaded
    delete merged.expectedBytes
  } else if (payload.status === 'quarantined_failed') {
    merged.failedAt = existing.failedAt || payload.failedAt
    delete merged.savedAt
  }
  return merged
}

function seenRecordChanged(existing, next) {
  if (!existing || typeof existing !== 'object') return true
  return SEEN_RECORD_FIELDS.some(
    (field) =>
      JSON.stringify(existing[field] ?? null) !==
      JSON.stringify(next[field] ?? null)
  )
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
  const shouldUseDeadMediaMatch =
    typeof options.shouldUseDeadMediaMatch === 'function'
      ? options.shouldUseDeadMediaMatch
      : null

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

  function compactRawValues(values) {
    return Array.from(
      new Set(
        values
          .flat(Infinity)
          .map((value) => String(value || '').trim())
          .filter(Boolean)
      )
    )
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
      deadMediaUrls:
        parsed?.deadMediaUrls && typeof parsed.deadMediaUrls === 'object'
          ? parsed.deadMediaUrls
          : {},
      deadMediaPageUrls:
        parsed?.deadMediaPageUrls &&
        typeof parsed.deadMediaPageUrls === 'object'
          ? parsed.deadMediaPageUrls
          : {},
    }

    if (migrateNormalizedKeys(data)) {
      data.updatedAt = new Date().toISOString()
      fs.writeFileSync(indexPath, JSON.stringify(data, null, 2) + '\n')
    }

    mediaSeenIndexCache = { indexPath, data }
    return data
  }

  function migrateNormalizedKeys(index) {
    let changed = false
    for (const bucketName of [
      'mediaUrls',
      'mediaPageUrls',
      'deadMediaUrls',
      'deadMediaPageUrls',
    ]) {
      const bucket = index[bucketName]
      for (const [key, entry] of Object.entries(bucket)) {
        const values =
          bucketName === 'mediaUrls' || bucketName === 'deadMediaUrls'
            ? [key, entry?.mediaUrl, entry?.mediaUrls]
            : [key, entry?.mediaPageUrl, entry?.mediaPageUrls]
        for (const normalized of uniqueSeenUrls(values)) {
          if (!bucket[normalized]) {
            bucket[normalized] = entry
            changed = true
          }
        }
      }
    }
    return changed
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

  function getDeadEntry(index, normalizedMediaUrl) {
    const mediaEntry = index.deadMediaUrls[normalizedMediaUrl]
    if (!mediaEntry) return null
    return { matchType: 'dead_media_url', ...mediaEntry }
  }

  function getDeadPageEntry(index, normalizedMediaPageUrl) {
    const pageEntry = index.deadMediaPageUrls[normalizedMediaPageUrl]
    if (!pageEntry) return null
    return { matchType: 'dead_media_page_url', ...pageEntry }
  }

  function getDeadMediaMatch(modelLogDir, mediaPageUrl, mediaUrl) {
    const index = loadMediaSeenIndex(modelLogDir)
    const rawMediaPageUrls = compactRawValues(
      Array.isArray(mediaPageUrl) ? mediaPageUrl : [mediaPageUrl]
    )
    const rawMediaUrls = compactRawValues(
      Array.isArray(mediaUrl) ? mediaUrl : [mediaUrl]
    )
    const mediaPageUrls = uniqueSeenUrls(
      rawMediaPageUrls
    )
    const mediaUrls = uniqueSeenUrls(rawMediaUrls)

    const useMatch = (match) =>
      !shouldUseDeadMediaMatch ||
      shouldUseDeadMediaMatch({
        match,
        mediaPageUrl: rawMediaPageUrls[0] || null,
        mediaPageUrls: rawMediaPageUrls,
        mediaUrl: rawMediaUrls[0] || null,
        mediaUrls: rawMediaUrls,
      }) !== false

    for (const normalizedMediaUrl of mediaUrls) {
      const match = getDeadEntry(index, normalizedMediaUrl)
      if (match && useMatch(match)) return match
    }
    if (mediaUrls.length === 0) {
      for (const normalizedMediaPageUrl of mediaPageUrls) {
        const match = getDeadPageEntry(index, normalizedMediaPageUrl)
        if (match && useMatch(match)) return match
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
      title: details.title || details.sourceTitle || null,
      text: details.text || details.sourceText || null,
      originalName: details.originalName || null,
      mediaQuality: details.mediaQuality || null,
      needsFullResolution:
        typeof details.needsFullResolution === 'boolean'
          ? details.needsFullResolution
          : null,
      fullResolutionStatus: details.fullResolutionStatus || null,
      fullResolutionUrl: details.fullResolutionUrl || null,
      fullResolutionResolvedPath: details.fullResolutionResolvedPath || null,
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
      const existing = index.mediaPageUrls[normalizedMediaPageUrl]
      const next = mergeSeenRecord(existing, payload)
      if (seenRecordChanged(existing, next)) {
        index.mediaPageUrls[normalizedMediaPageUrl] = next
        changed = true
      }
    }
    for (const normalizedMediaUrl of mediaUrls) {
      const existing = index.mediaUrls[normalizedMediaUrl]
      const next = mergeSeenRecord(existing, payload)
      if (seenRecordChanged(existing, next)) {
        index.mediaUrls[normalizedMediaUrl] = next
        changed = true
      }
    }

    if (changed) saveMediaSeenIndex(modelLogDir, index)
  }

  function recordDeadMedia(modelLogDir, details = {}) {
    const index = loadMediaSeenIndex(modelLogDir)
    const rawMediaPageUrls = compactRawValues([
      details.mediaPageUrl,
      details.mediaPageUrls,
    ])
    const rawMediaUrls = compactRawValues([details.mediaUrl, details.mediaUrls])
    const mediaPageUrls = uniqueSeenUrls([
      details.mediaPageUrl,
      details.mediaPageUrls,
    ])
    const mediaUrls = uniqueSeenUrls([details.mediaUrl, details.mediaUrls])
    if (mediaUrls.length === 0 && mediaPageUrls.length === 0) return

    const recordedAt = new Date().toISOString()
    const payload = {
      filename: details.filename || null,
      mediaUrl: mediaUrls[0] || null,
      mediaUrls,
      mediaPageUrl: mediaPageUrls[0] || null,
      mediaPageUrls,
      rawMediaUrl: rawMediaUrls[0] || null,
      rawMediaUrls,
      rawMediaPageUrl: rawMediaPageUrls[0] || null,
      rawMediaPageUrls,
      status: 'dead',
      reason: details.reason || 'gone',
      error: details.error || null,
      recordedAt,
    }

    let changed = false
    for (const normalizedMediaUrl of mediaUrls) {
      if (
        index.deadMediaUrls[normalizedMediaUrl]?.status !== 'dead' ||
        index.deadMediaUrls[normalizedMediaUrl]?.reason !== payload.reason
      ) {
        index.deadMediaUrls[normalizedMediaUrl] = payload
        changed = true
      }
    }
    if (mediaUrls.length === 0) {
      for (const normalizedMediaPageUrl of mediaPageUrls) {
        if (
          index.deadMediaPageUrls[normalizedMediaPageUrl]?.status !== 'dead' ||
          index.deadMediaPageUrls[normalizedMediaPageUrl]?.reason !==
            payload.reason
        ) {
          index.deadMediaPageUrls[normalizedMediaPageUrl] = payload
          changed = true
        }
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
    getDeadMediaMatch,
    getSuccessfulSeenMediaMatch,
    recordDeadMedia,
    recordSeenMedia,
    recordSuccessfulSeenMedia,
    recordFailedSeenMedia,
  }
}

module.exports = {
  createMediaSeenIndex,
  defaultNormalizeSeenUrl,
}

'use strict'

const path = require('path')

function sanitizeToken(value) {
  return String(value || '')
    .trim()
    .replace(/^r\//i, '')
    .replace(/^@+/, '')
    .replace(/[^a-z0-9_-]/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase()
}

function parseMediaDate(value) {
  if (value instanceof Date && !isNaN(value.getTime())) return value
  if (typeof value === 'string' || typeof value === 'number') {
    const parsed = new Date(value)
    if (!isNaN(parsed.getTime())) return parsed
  }
  return null
}

function classifyMediaFilename(filename) {
  const ext = path.extname(String(filename || '')).toLowerCase()
  if (['.mp4', '.m4v', '.webm', '.mov'].includes(ext)) {
    return { ext, kind: 'video' }
  }
  if (ext === '.gif') return { ext, kind: 'gif' }
  if (['.jpg', '.jpeg', '.png', '.webp', '.bmp', '.avif'].includes(ext)) {
    return { ext, kind: 'image' }
  }
  return { ext, kind: 'unknown' }
}

function compactUnique(
  values,
  normalizeValue = (value) => String(value || '').trim()
) {
  return Array.from(
    new Set(
      values
        .flat(Infinity)
        .map((value) => normalizeValue(value))
        .filter(Boolean)
    )
  )
}

function isLikelyMediaUrl(value) {
  const raw = String(value || '').trim()
  if (!raw) return false

  let parsed
  try {
    parsed = new URL(raw)
  } catch {
    return false
  }

  const pathname = decodeURIComponent(parsed.pathname || '').toLowerCase()
  if (
    /\.(?:jpe?g|png|webp|gif|bmp|avif|mp4|m4v|webm|mov)(?:$|[?#])/i.test(
      `${pathname}${parsed.search || ''}`
    )
  ) {
    return true
  }

  return (
    /\b(?:cdn|media|preview|thumb|image|video)\b/i.test(parsed.hostname) &&
    !/(?:\/index(?:\.php)?\?\/|\/picture\?\/|\/category\/)/i.test(raw)
  )
}

function normalizeMediaEntry(entry, options = {}) {
  if (!entry || typeof entry !== 'object') return null

  const filename = String(entry.filename || '').trim()
  const mediaUrl = String(entry.mediaUrl || '').trim()
  if (!filename || !mediaUrl) return null

  const mediaUrls = compactUnique([mediaUrl, entry.mediaUrls])
  const mediaPageUrls = compactUnique([entry.mediaPageUrl, entry.mediaPageUrls])
  const sourceUrls = compactUnique([entry.sourceUrls])
  const uploadedDate = parseMediaDate(entry.uploadedDate)
  const classification = classifyMediaFilename(filename)

  return {
    ...entry,
    sourceSite: entry.sourceSite || options.sourceSite || null,
    sourceService: entry.sourceService || options.sourceService || null,
    sourceUserId: entry.sourceUserId || options.sourceUserId || null,
    sourceUsername: entry.sourceUsername || options.sourceUsername || null,
    sourceSubreddit: sanitizeToken(entry.sourceSubreddit || entry.subreddit),
    postId: String(entry.postId || '').trim(),
    title: entry.title || null,
    mediaPageUrl: mediaPageUrls[0] || null,
    mediaPageUrls,
    mediaUrl,
    mediaUrls,
    sourceUrls,
    filename,
    originalName: entry.originalName || null,
    uploadedDate,
    extension: classification.ext,
    kind: entry.kind || classification.kind,
  }
}

function normalizeMediaEntries(entries, options = {}) {
  return (Array.isArray(entries) ? entries : [])
    .map((entry) => normalizeMediaEntry(entry, options))
    .filter(Boolean)
}

function getMediaEntryUrls(entry = {}, options = {}) {
  const urls = compactUnique(
    [entry.mediaUrl, entry.jsonMediaUrl, entry.mediaUrls],
    options.normalizeUrl
  )
  return options.filterMediaUrls === false
    ? urls
    : urls.filter((url) => isLikelyMediaUrl(url))
}

function getMediaEntryPageUrls(entry = {}, options = {}) {
  return compactUnique(
    [entry.mediaPageUrl, entry.mediaPageUrls],
    options.normalizeUrl
  )
}

function getMediaEntrySeenDetails(entry = {}, options = {}) {
  return {
    mediaUrl: entry.mediaUrl || null,
    mediaUrls: getMediaEntryUrls(entry, options),
    mediaPageUrl: entry.mediaPageUrl || null,
    mediaPageUrls: getMediaEntryPageUrls(entry, options),
  }
}

function getMediaEntrySourceDetails(entry = {}) {
  return {
    sourceSite: entry.sourceSite || null,
    sourceService: entry.sourceService || null,
    sourceUserId: entry.sourceUserId || null,
    sourceUsername: entry.sourceUsername || null,
    sourceSubreddit: entry.sourceSubreddit || null,
    postId: entry.postId || null,
  }
}

function getMediaEntryHashMetadata(entry = {}) {
  return {
    sourceSite: entry.sourceSite || null,
    sourceService: entry.sourceService || null,
    sourceUserId: entry.sourceUserId || null,
    sourceUsername: entry.sourceUsername || null,
    sourceSubreddit: entry.sourceSubreddit || null,
    sourcePostId: entry.postId || null,
    sourceMediaPageUrl: entry.mediaPageUrl || null,
  }
}

module.exports = {
  sanitizeToken,
  parseMediaDate,
  classifyMediaFilename,
  compactUnique,
  getMediaEntryHashMetadata,
  getMediaEntryPageUrls,
  getMediaEntrySeenDetails,
  getMediaEntrySourceDetails,
  getMediaEntryUrls,
  isLikelyMediaUrl,
  normalizeMediaEntry,
  normalizeMediaEntries,
}

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

function compactUnique(values) {
  return Array.from(
    new Set(
      values
        .flat(Infinity)
        .map((value) => String(value || '').trim())
        .filter(Boolean)
    )
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

module.exports = {
  sanitizeToken,
  parseMediaDate,
  classifyMediaFilename,
  normalizeMediaEntry,
  normalizeMediaEntries,
}

'use strict'

const path = require('path')

const VIDEO_EXTENSIONS = new Set(['.m4v', '.mov', '.mp4', '.webm'])

function normalizeRelativePath(value) {
  return String(value || '')
    .trim()
    .replace(/\\/g, '/')
    .replace(/^\/+/, '')
}

function inferSourceSite(...urls) {
  for (const value of urls) {
    try {
      const host = new URL(String(value || '')).hostname.toLowerCase()
      if (host.includes('stufferdb') || host.includes('stufferai')) {
        return 'stufferdb'
      }
      if (host.includes('coomerfans')) return 'coomerfans'
      if (host.includes('coomer')) return 'coomer'
      if (host.includes('kemono') || host.includes('pawchive')) return 'kemono'
      if (host.includes('reddit') || host.includes('redd.it')) return 'reddit'
      if (host) return host
    } catch {
      // Try the next URL.
    }
  }
  return null
}

function sourceMetaFromSeenRecord(record) {
  if (!record || typeof record !== 'object') return null
  const mediaPageUrl = String(record.mediaPageUrl || '').trim() || null
  const mediaUrl = String(record.mediaUrl || '').trim() || null
  if (!mediaPageUrl && !mediaUrl) return null

  return {
    site:
      record.sourceSite || inferSourceSite(mediaPageUrl, mediaUrl) || 'unknown',
    service: record.sourceService || null,
    userId: record.sourceUserId || null,
    username: record.sourceUsername || null,
    subreddit: record.sourceSubreddit || null,
    postId: record.postId || null,
    title: record.title || null,
    text: record.text || null,
    originalName: record.originalName || null,
    mediaPageUrl,
    mediaUrl,
  }
}

function buildSeenIndexByRelativePath(index, modelName) {
  const result = new Map()
  const modelPrefix = `${normalizeRelativePath(modelName)}/`

  for (const bucketName of ['mediaPageUrls', 'mediaUrls']) {
    for (const record of Object.values(index?.[bucketName] || {})) {
      let relativePath = normalizeRelativePath(record?.relativePath)
      if (!relativePath) continue
      if (modelPrefix !== '/' && relativePath.startsWith(modelPrefix)) {
        relativePath = relativePath.slice(modelPrefix.length)
      }

      const previous = result.get(relativePath)
      result.set(relativePath, {
        ...(previous || {}),
        ...record,
        mediaPageUrl: record.mediaPageUrl || previous?.mediaPageUrl || null,
        mediaUrl: record.mediaUrl || previous?.mediaUrl || null,
        title: record.title || previous?.title || null,
        text: record.text || previous?.text || null,
      })
    }
  }

  return result
}

function buildStufferSourceMeta(filename) {
  const match = String(filename || '').match(
    /^(\d{4})(\d{2})(\d{2})\d{6}-[0-9a-f]{8}(?:-[^.]+)?(\.[^.]+)$/i
  )
  if (!match) return null

  const [, year, month, day, extension] = match
  const encodedFilename = encodeURIComponent(path.basename(filename))
  const mediaUrl = VIDEO_EXTENSIONS.has(extension.toLowerCase())
    ? `https://stufferai.com/upload/${year}/${month}/${day}/${encodedFilename}`
    : `https://cdn.stufferdb.com/_data/i/upload/${year}/${month}/${day}/${encodedFilename}`

  return {
    site: 'stufferdb',
    originalName: path.basename(filename),
    mediaUrl,
  }
}

function parseRedditPostId(filename) {
  const extension = path.extname(String(filename || ''))
  const stem = path.basename(String(filename || ''), extension)
  const tokens = stem.replace(/-la$/i, '').split('_')

  for (let index = tokens.length - 1; index >= 0; index -= 1) {
    const token = tokens[index].toLowerCase()
    if (/^\d+$/.test(token)) continue
    if (/^[a-z0-9]{5,8}$/.test(token) && /\d/.test(token)) return token
  }

  return null
}

function buildRedditSourceMeta(filename) {
  const postId = parseRedditPostId(filename)
  if (!postId) return null

  return {
    site: 'reddit',
    postId,
    mediaPageUrl: `https://www.reddit.com/comments/${postId}/`,
  }
}

function normalizeRegistrySite(platform, url) {
  if (platform === 'coomer') {
    return String(url || '').includes('coomerfans.com')
      ? 'coomerfans'
      : 'coomer'
  }
  return platform || inferSourceSite(url) || 'unknown'
}

function getModelProfileSource(entry, filename) {
  const sources = entry?.sources || {}
  const isCoomerFilename = /^[0-9a-f]{6,}-[0-9a-f-]{20,}\.[^.]+$/i.test(
    path.basename(String(filename || ''))
  )
  const priority = isCoomerFilename
    ? ['coomer', 'kemono', 'reddit', 'stufferdb']
    : ['reddit', 'coomer', 'kemono', 'stufferdb']

  for (const platform of priority) {
    const candidates = Array.isArray(sources[platform]) ? sources[platform] : []
    for (const candidate of candidates) {
      const url =
        typeof candidate === 'string'
          ? candidate
          : String(candidate?.url || '').trim()
      if (!url) continue

      return {
        site: normalizeRegistrySite(platform, url),
        service:
          typeof candidate === 'object' ? candidate.service || null : null,
        userId:
          typeof candidate === 'object'
            ? candidate.userId || candidate.id || null
            : null,
        username:
          typeof candidate === 'object'
            ? candidate.username || candidate.discoveredAs || null
            : null,
        mediaPageUrl: url,
      }
    }
  }

  return null
}

function hasSourceUrl(record) {
  return Boolean(
    record?.source && (record.source.mediaPageUrl || record.source.mediaUrl)
  )
}

module.exports = {
  buildRedditSourceMeta,
  buildSeenIndexByRelativePath,
  buildStufferSourceMeta,
  getModelProfileSource,
  hasSourceUrl,
  inferSourceSite,
  normalizeRelativePath,
  parseRedditPostId,
  sourceMetaFromSeenRecord,
}

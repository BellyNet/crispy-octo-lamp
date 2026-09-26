'use strict'

const fs = require('fs')
const path = require('path')

const RETRY_FILENAME = 'reddit-full-resolution-retry.json'
const DEFAULT_MINIMUM_PIXELS = 500000
const DEFAULT_MINIMUM_LONG_EDGE = 768
const DEFAULT_RETRY_COOLDOWN_MS = 24 * 60 * 60 * 1000
const DEFAULT_RETRY_LIMIT = 32

function getRetryPath(modelLogDir) {
  return path.join(modelLogDir, RETRY_FILENAME)
}

function loadRetryState(modelLogDir) {
  const retryPath = getRetryPath(modelLogDir)
  if (!fs.existsSync(retryPath)) {
    return { version: 1, updatedAt: null, pending: {} }
  }
  try {
    const parsed = JSON.parse(fs.readFileSync(retryPath, 'utf8'))
    return {
      version: 1,
      updatedAt: parsed.updatedAt || null,
      pending:
        parsed.pending && typeof parsed.pending === 'object'
          ? parsed.pending
          : {},
    }
  } catch {
    return { version: 1, updatedAt: null, pending: {} }
  }
}

function saveRetryState(modelLogDir, state) {
  const retryPath = getRetryPath(modelLogDir)
  fs.mkdirSync(path.dirname(retryPath), { recursive: true })
  state.updatedAt = new Date().toISOString()
  const tempPath = `${retryPath}.tmp-${process.pid}-${Date.now()}`
  fs.writeFileSync(tempPath, `${JSON.stringify(state, null, 2)}\n`)
  fs.renameSync(tempPath, retryPath)
}

function recordFullResolutionRetry(modelLogDir, details = {}) {
  const relativePath = String(details.relativePath || '').replace(/\\/g, '/')
  if (!relativePath) return false
  const state = loadRetryState(modelLogDir)
  const previous = state.pending[relativePath] || {}
  state.pending[relativePath] = {
    status: 'pending_full_resolution',
    queuedAt: previous.queuedAt || new Date().toISOString(),
    lastAttemptAt: previous.lastAttemptAt || null,
    attemptCount: previous.attemptCount || 0,
    relativePath,
    previewUrl: details.previewUrl || details.mediaUrl || null,
    fullResolutionUrl: details.fullResolutionUrl || null,
    mediaPageUrl: details.mediaPageUrl || null,
    postId: details.postId || null,
    sourceService: details.sourceService || null,
    sourceUserId: details.sourceUserId || null,
    sourceUsername: details.sourceUsername || null,
    quality: details.quality || null,
  }
  saveRetryState(modelLogDir, state)
  return true
}

function listDueFullResolutionRetries(modelLogDir, source = {}, options = {}) {
  const now = Number(options.now) || Date.now()
  const cooldownMs = Number.isFinite(Number(options.cooldownMs))
    ? Math.max(0, Number(options.cooldownMs))
    : DEFAULT_RETRY_COOLDOWN_MS
  const requestedLimit =
    options.limit ?? process.env.HOGHAUL_REDDIT_PENDING_RETRY_LIMIT
  const limit = Number.isFinite(Number(requestedLimit))
    ? Math.max(0, Math.floor(Number(requestedLimit)))
    : DEFAULT_RETRY_LIMIT
  const sourceUserId = String(
    source.userId || source.username || ''
  ).toLowerCase()
  const sourceService = String(source.service || 'submitted').toLowerCase()
  if (!sourceUserId || limit === 0) return []

  return Object.values(loadRetryState(modelLogDir).pending)
    .filter((item) => {
      if (!item?.relativePath || !item.fullResolutionUrl) return false
      const itemUserId = String(
        item.sourceUserId || item.sourceUsername || ''
      ).toLowerCase()
      if (itemUserId !== sourceUserId) return false
      if (
        String(item.sourceService || 'submitted').toLowerCase() !==
        sourceService
      )
        return false
      const lastAttempt = Date.parse(item.lastAttemptAt || '')
      return !Number.isFinite(lastAttempt) || now - lastAttempt >= cooldownMs
    })
    .sort((a, b) =>
      String(a.queuedAt || '').localeCompare(String(b.queuedAt || ''))
    )
    .slice(0, limit)
}

function markFullResolutionRetryAttempt(
  modelLogDir,
  relativePath,
  now = Date.now()
) {
  const key = String(relativePath || '').replace(/\\/g, '/')
  const state = loadRetryState(modelLogDir)
  const pending = state.pending[key]
  if (!pending) return false
  pending.lastAttemptAt = new Date(now).toISOString()
  pending.attemptCount = Number(pending.attemptCount || 0) + 1
  saveRetryState(modelLogDir, state)
  return true
}

function clearFullResolutionRetry(modelLogDir, relativePath) {
  const key = String(relativePath || '').replace(/\\/g, '/')
  if (!key) return false
  const state = loadRetryState(modelLogDir)
  if (!Object.hasOwn(state.pending, key)) return false
  delete state.pending[key]
  saveRetryState(modelLogDir, state)
  return true
}

function clearMatchingFullResolutionRetries(modelLogDir, details = {}) {
  const relativePath = String(details.relativePath || '').replace(/\\/g, '/')
  const mediaUrls = new Set(
    [details.mediaUrl, details.mediaUrls, details.fullResolutionUrl]
      .flat(Infinity)
      .map(normalizeUrl)
      .filter(Boolean)
  )
  if (!relativePath && mediaUrls.size === 0) return 0

  const state = loadRetryState(modelLogDir)
  let cleared = 0
  for (const [key, pending] of Object.entries(state.pending)) {
    const pendingPath = String(pending?.relativePath || '').replace(/\\/g, '/')
    const pendingUrls = [pending?.fullResolutionUrl, pending?.previewUrl]
      .map(normalizeUrl)
      .filter(Boolean)
    const pathMatches = relativePath && pendingPath === relativePath
    const urlMatches = pendingUrls.some((url) => mediaUrls.has(url))
    if (!pathMatches && !urlMatches) continue
    delete state.pending[key]
    cleared += 1
  }
  if (cleared > 0) saveRetryState(modelLogDir, state)
  return cleared
}

function evaluatePreviewQuality(metadata = {}, options = {}) {
  const minimumPixels = positiveInt(
    options.minimumPixels,
    DEFAULT_MINIMUM_PIXELS
  )
  const minimumLongEdge = positiveInt(
    options.minimumLongEdge,
    DEFAULT_MINIMUM_LONG_EDGE
  )
  const width = Number(metadata.width || 0)
  const height = Number(metadata.height || 0)
  const pixels = width * height
  const longEdge = Math.max(width, height)
  return {
    acceptable: pixels >= minimumPixels && longEdge >= minimumLongEdge,
    width,
    height,
    pixels,
    longEdge,
    minimumPixels,
    minimumLongEdge,
  }
}

function positiveInt(value, fallback) {
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function normalizeUrl(value) {
  try {
    const parsed = new URL(String(value || '').trim())
    parsed.hash = ''
    return parsed.toString()
  } catch {
    return String(value || '').trim()
  }
}

module.exports = {
  DEFAULT_MINIMUM_LONG_EDGE,
  DEFAULT_MINIMUM_PIXELS,
  clearFullResolutionRetry,
  clearMatchingFullResolutionRetries,
  evaluatePreviewQuality,
  getRetryPath,
  listDueFullResolutionRetries,
  loadRetryState,
  markFullResolutionRetryAttempt,
  recordFullResolutionRetry,
}

'use strict'

const fs = require('fs')
const path = require('path')

function normalizeKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
}

function getSourceKey(source = {}) {
  return [
    normalizeKey(source.site),
    normalizeKey(source.service),
    normalizeKey(source.userId || source.username || source.rawName),
  ].join('/')
}

function getStatePath(modelLogDir) {
  return path.join(modelLogDir, 'source-frontier-state.json')
}

function normalizePostIds(postIds = []) {
  return [
    ...new Set(
      postIds.map((postId) => String(postId || '').trim()).filter(Boolean)
    ),
  ]
}

function loadState(modelLogDir) {
  const statePath = getStatePath(modelLogDir)
  if (!fs.existsSync(statePath)) return { version: 1, sources: {} }
  try {
    const parsed = JSON.parse(fs.readFileSync(statePath, 'utf8'))
    return {
      version: 1,
      updatedAt: parsed.updatedAt || null,
      sources:
        parsed.sources && typeof parsed.sources === 'object'
          ? parsed.sources
          : {},
    }
  } catch {
    return { version: 1, sources: {} }
  }
}

function saveState(modelLogDir, state) {
  fs.mkdirSync(modelLogDir, { recursive: true })
  state.updatedAt = new Date().toISOString()
  fs.writeFileSync(
    getStatePath(modelLogDir),
    JSON.stringify(state, null, 2) + '\n'
  )
}

function uniqueRecords(index = {}) {
  const records = new Map()
  for (const bucketName of ['mediaUrls', 'mediaPageUrls']) {
    const bucket = index[bucketName]
    if (!bucket || typeof bucket !== 'object') continue
    for (const record of Object.values(bucket)) {
      const key = [
        record?.postId || record?.sourcePostId || '',
        record?.relativePath || '',
        record?.mediaUrl || '',
        record?.mediaPageUrl || '',
      ].join('\n')
      if (key.trim()) records.set(key, record)
    }
  }
  return [...records.values()]
}

function readSeenIndex(modelLogDir) {
  const indexPath = path.join(modelLogDir, 'milkmaid-seen-media-index.json')
  if (!fs.existsSync(indexPath)) return null
  try {
    return JSON.parse(fs.readFileSync(indexPath, 'utf8'))
  } catch {
    return null
  }
}

function recordExists(record, datasetPaths) {
  if (!record?.relativePath || !datasetPaths?.toDatasetAbsolutePath) {
    return false
  }
  const absolutePath = datasetPaths.toDatasetAbsolutePath(record.relativePath)
  if (typeof datasetPaths.existsLocallyOrOnNas === 'function') {
    return datasetPaths.existsLocallyOrOnNas(absolutePath)
  }
  return fs.existsSync(absolutePath)
}

function sourceMatchesRecord(source, record = {}) {
  if (normalizeKey(record.sourceSite) !== normalizeKey(source.site)) {
    return false
  }

  const sourceService = normalizeKey(source.service)
  const recordService = normalizeKey(record.sourceService)
  if (sourceService && recordService && sourceService !== recordService) {
    return false
  }

  const sourceUserId = normalizeKey(source.userId)
  const recordUserId = normalizeKey(record.sourceUserId)
  if (sourceUserId && recordUserId) return sourceUserId === recordUserId

  const sourceNames = new Set(
    [source.username, source.rawName].map(normalizeKey).filter(Boolean)
  )
  const recordNames = [
    record.sourceUsername,
    record.sourceRawName,
    record.sourceUserId,
  ]
    .map(normalizeKey)
    .filter(Boolean)
  return (
    sourceNames.size > 0 && recordNames.some((name) => sourceNames.has(name))
  )
}

function getPostIdFromRecord(source, record = {}) {
  const direct = String(record.postId || record.sourcePostId || '').trim()
  if (direct) return direct

  const pageUrl = String(record.mediaPageUrl || '').trim()
  if (!pageUrl) return ''
  if (source.site === 'coomerfans') {
    return pageUrl.match(/\/p\/(\d+)\//i)?.[1] || ''
  }
  if (source.site === 'stufferdb') {
    return pageUrl.match(/picture\?\/(\d+)/i)?.[1] || ''
  }
  return pageUrl.match(/\/post\/([^/?#]+)/i)?.[1] || ''
}

function loadConfirmedSourceFrontier(modelLogDir, source, options = {}) {
  const index = readSeenIndex(modelLogDir)
  const confirmedPostIds = new Set()
  let confirmedRecords = 0

  for (const record of uniqueRecords(index || {})) {
    if (!sourceMatchesRecord(source, record)) continue
    if (!recordExists(record, options.datasetPaths)) continue
    const postId = getPostIdFromRecord(source, record)
    if (!postId) continue
    confirmedPostIds.add(postId)
    confirmedRecords += 1
  }
  const state = loadState(modelLogDir)
  const sourceState = state.sources?.[getSourceKey(source)] || {}
  const savedSeenPostIds = new Set(sourceState.seenPostIds || [])
  const savedCompletedPostIds = new Set(sourceState.completedPostIds || [])
  const knownPostIds = new Set([
    ...confirmedPostIds,
    ...savedSeenPostIds,
    ...savedCompletedPostIds,
  ])
  const completedPostIds = new Set([...savedCompletedPostIds])
  const skippablePostIds = new Set([
    ...savedSeenPostIds,
    ...savedCompletedPostIds,
  ])

  return {
    active: knownPostIds.size > 0,
    knownPostIds,
    knownPostCount: knownPostIds.size,
    confirmedPostIds,
    confirmedPostCount: confirmedPostIds.size,
    seenPostIds: savedSeenPostIds,
    seenPostCount: savedSeenPostIds.size,
    completedPostIds,
    completedPostCount: completedPostIds.size,
    skippablePostIds,
    skippablePostCount: skippablePostIds.size,
    confirmedRecords,
    sourceSite: source.site,
    sourceService: source.service || null,
    sourceUserId: source.userId || null,
  }
}

function createBoundaryPageFilter(frontier, options = {}) {
  const fullRefresh = Boolean(options.fullRefresh)
  const minimumKnownRatio = Math.min(
    Math.max(Number(options.minimumKnownRatio ?? 0.8), 0),
    1
  )
  const overlapPages = Math.max(
    Number.parseInt(options.overlapPages, 10) || 0,
    0
  )
  let boundaryReached = false
  let overlapPagesRemaining = overlapPages

  return {
    active: Boolean(frontier?.active) && !fullRefresh,
    filterPage(items = [], getId = (item) => item?.id) {
      if (!frontier?.active || fullRefresh) {
        return {
          items,
          knownCount: 0,
          completedCount: 0,
          knownRatio: 0,
          stopAfterPage: false,
        }
      }

      const unknownItems = []
      let knownCount = 0
      let completedCount = 0
      for (const item of items) {
        const id = String(getId(item) || '').trim()
        if (id && frontier.knownPostIds.has(id)) {
          knownCount += 1
          if (
            frontier.completedPostIds?.has(id) ||
            frontier.skippablePostIds?.has(id)
          ) {
            completedCount += 1
            continue
          }
          unknownItems.push(item)
        } else {
          unknownItems.push(item)
        }
      }
      const knownRatio = items.length > 0 ? knownCount / items.length : 0
      const denseKnownPage = knownCount > 0 && knownRatio >= minimumKnownRatio

      if (boundaryReached) {
        overlapPagesRemaining -= 1
        return {
          items: unknownItems,
          knownCount,
          completedCount,
          knownRatio,
          stopAfterPage: overlapPagesRemaining <= 0,
        }
      }

      if (denseKnownPage) {
        boundaryReached = true
        return {
          items: unknownItems,
          knownCount,
          completedCount,
          knownRatio,
          stopAfterPage: overlapPagesRemaining === 0,
        }
      }

      return {
        items: unknownItems,
        knownCount,
        completedCount,
        knownRatio,
        stopAfterPage: false,
      }
    },
  }
}

function recordCompletedSourcePosts(modelLogDir, source, postIds = []) {
  const completedPostIds = normalizePostIds(postIds)
  if (completedPostIds.length === 0) return { completedPostCount: 0 }

  const state = loadState(modelLogDir)
  const sourceKey = getSourceKey(source)
  const existing = state.sources[sourceKey] || {}
  const merged = [
    ...new Set([...(existing.completedPostIds || []), ...completedPostIds]),
  ]
  state.sources[sourceKey] = {
    ...existing,
    site: source.site,
    service: source.service || null,
    userId: source.userId || null,
    username: source.username || source.rawName || null,
    completedPostIds: merged,
    updatedAt: new Date().toISOString(),
  }
  saveState(modelLogDir, state)
  return {
    sourceKey,
    completedPostCount: merged.length,
    addedPostCount: completedPostIds.length,
  }
}

function recordSeenSourcePosts(modelLogDir, source, postIds = []) {
  const seenPostIds = normalizePostIds(postIds)
  if (seenPostIds.length === 0) return { seenPostCount: 0, addedPostCount: 0 }

  const state = loadState(modelLogDir)
  const sourceKey = getSourceKey(source)
  const existing = state.sources[sourceKey] || {}
  const existingSeen = existing.seenPostIds || []
  const merged = [...new Set([...existingSeen, ...seenPostIds])]
  const addedPostCount = merged.length - existingSeen.length
  if (addedPostCount <= 0) {
    return {
      sourceKey,
      seenPostCount: merged.length,
      addedPostCount: 0,
    }
  }

  state.sources[sourceKey] = {
    ...existing,
    site: source.site,
    service: source.service || null,
    userId: source.userId || null,
    username: source.username || source.rawName || null,
    seenPostIds: merged,
    updatedAt: new Date().toISOString(),
  }
  saveState(modelLogDir, state)
  return {
    sourceKey,
    seenPostCount: merged.length,
    addedPostCount,
  }
}

function sourceEventMatches(source, event = {}) {
  const eventSite = normalizeKey(event.sourceSite || event.site)
  if (eventSite && eventSite !== normalizeKey(source.site)) return false

  const eventService = normalizeKey(event.sourceService || event.service)
  const sourceService = normalizeKey(source.service)
  if (eventService && sourceService && eventService !== sourceService) {
    return false
  }

  const eventUserId = normalizeKey(event.sourceUserId || event.userId)
  const sourceUserId = normalizeKey(source.userId)
  return !eventUserId || !sourceUserId || eventUserId === sourceUserId
}

function getPostIdFromPageUrl(source, value) {
  let parsed
  try {
    parsed = new URL(String(value || '').trim())
  } catch {
    return ''
  }
  const service = normalizeKey(source.service)
  const userId = normalizeKey(source.userId)
  const parts = parsed.pathname.split('/').filter(Boolean).map(normalizeKey)
  const serviceIndex = parts.indexOf(service)
  if (serviceIndex < 0) return ''
  if (parts[serviceIndex + 1] !== 'user') return ''
  if (userId && parts[serviceIndex + 2] !== userId) return ''
  if (parts[serviceIndex + 3] !== 'post') return ''
  return parts[serviceIndex + 4] || ''
}

function collectEventUrls(value, urls = []) {
  if (!value) return urls
  if (typeof value === 'string') {
    if (/^https?:\/\//i.test(value)) urls.push(value)
    return urls
  }
  if (Array.isArray(value)) {
    for (const item of value) collectEventUrls(item, urls)
    return urls
  }
  if (typeof value === 'object') {
    for (const item of Object.values(value)) collectEventUrls(item, urls)
  }
  return urls
}

function extractSourcePostIdsFromRunEvent(source, event = {}) {
  const postIds = new Set()
  if (sourceEventMatches(source, event)) {
    const direct = String(event.postId || event.sourcePostId || '').trim()
    if (direct) postIds.add(direct)
  }

  for (const url of collectEventUrls(event)) {
    const postId = getPostIdFromPageUrl(source, url)
    if (postId) postIds.add(postId)
  }

  return [...postIds]
}

function backfillSeenSourcePostsFromRunEvents(modelLogDir, source) {
  if (!modelLogDir || !fs.existsSync(modelLogDir)) {
    return { seenPostCount: 0, addedPostCount: 0, scannedRunCount: 0 }
  }

  const postIds = new Set()
  let scannedRunCount = 0
  for (const entry of fs.readdirSync(modelLogDir)) {
    if (!/^hoghaul-run-.*\.jsonl$/i.test(entry)) continue
    scannedRunCount += 1
    const runPath = path.join(modelLogDir, entry)
    const lines = fs.readFileSync(runPath, 'utf8').split(/\r?\n/)
    for (const line of lines) {
      if (!line.trim()) continue
      let event
      try {
        event = JSON.parse(line)
      } catch {
        continue
      }
      for (const postId of extractSourcePostIdsFromRunEvent(source, event)) {
        postIds.add(postId)
      }
    }
  }

  const result = recordSeenSourcePosts(modelLogDir, source, [...postIds])
  return {
    ...result,
    scannedRunCount,
    extractedPostCount: postIds.size,
  }
}

function getSourceCheckpoint(modelLogDir, source) {
  const sourceState = loadState(modelLogDir).sources?.[getSourceKey(source)]
  return sourceState?.checkpoint || null
}

function recordSourceCheckpoint(modelLogDir, source, checkpoint) {
  if (!checkpoint?.id && !checkpoint?.mediaPageUrl) return null

  const state = loadState(modelLogDir)
  const sourceKey = getSourceKey(source)
  const existing = state.sources[sourceKey] || {}
  state.sources[sourceKey] = {
    ...existing,
    site: source.site,
    service: source.service || null,
    userId: source.userId || null,
    username: source.username || source.rawName || null,
    checkpoint: {
      id: checkpoint.id ? String(checkpoint.id) : null,
      mediaPageUrl: checkpoint.mediaPageUrl || null,
      recordedAt: new Date().toISOString(),
    },
    updatedAt: new Date().toISOString(),
  }
  saveState(modelLogDir, state)
  return state.sources[sourceKey].checkpoint
}

module.exports = {
  backfillSeenSourcePostsFromRunEvents,
  createBoundaryPageFilter,
  getSourceCheckpoint,
  loadConfirmedSourceFrontier,
  recordCompletedSourcePosts,
  recordSeenSourcePosts,
  recordSourceCheckpoint,
}

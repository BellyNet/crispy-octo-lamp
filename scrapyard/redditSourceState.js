'use strict'

const fs = require('fs')
const path = require('path')

function normalizeKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
}

function normalizeIsoDate(value) {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString()
  }
  if (value === undefined || value === null || value === '') return null
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString()
}

function getCreatedUtcFromValue(value) {
  const iso = normalizeIsoDate(value)
  if (!iso) return null
  return Math.floor(new Date(iso).getTime() / 1000)
}

function getPostCreatedUtc(post = {}) {
  const direct = Number(post.created_utc)
  if (Number.isFinite(direct) && direct > 0) return Math.floor(direct)
  return getCreatedUtcFromValue(post.published || post.uploadedDate)
}

function uniqueValues(values) {
  const seen = new Set()
  const output = []
  const visit = (value) => {
    if (Array.isArray(value)) {
      for (const item of value) visit(item)
      return
    }
    const normalized = String(value || '').trim()
    if (!normalized || seen.has(normalized)) return
    seen.add(normalized)
    output.push(normalized)
  }
  visit(values)
  return output
}

function getRedditSourceStatePath(modelLogDir) {
  return path.join(modelLogDir, 'reddit-source-state.json')
}

function getSourceKey(source = {}) {
  const service = normalizeKey(source.service || 'submitted') || 'submitted'
  const user = normalizeKey(source.username || source.userId || source.rawName)
  return `${service}/${user || 'unknown'}`
}

function getSourceIdentity(source = {}) {
  return {
    sourceSite: 'reddit',
    sourceService: source.service || 'submitted',
    sourceUserId: source.userId || null,
    sourceUsername: source.username || source.userId || null,
    inputUrl: source.inputUrl || null,
  }
}

function loadRedditSourceState(modelLogDir) {
  const statePath = getRedditSourceStatePath(modelLogDir)
  if (!fs.existsSync(statePath)) {
    return {
      version: 1,
      updatedAt: null,
      sources: {},
    }
  }

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
    return {
      version: 1,
      updatedAt: null,
      sources: {},
    }
  }
}

function saveRedditSourceState(modelLogDir, state) {
  const statePath = getRedditSourceStatePath(modelLogDir)
  fs.mkdirSync(path.dirname(statePath), { recursive: true })
  state.updatedAt = new Date().toISOString()
  fs.writeFileSync(statePath, JSON.stringify(state, null, 2) + '\n')
}

function getOrCreateSourceState(state, source) {
  const sourceKey = getSourceKey(source)
  if (!state.sources[sourceKey]) {
    state.sources[sourceKey] = {
      ...getSourceIdentity(source),
      sourceKey,
      latestPostId: null,
      latestCreatedUtc: null,
      latestCheckedAt: null,
      latestNewPostAt: null,
      posts: {},
    }
  }
  return state.sources[sourceKey]
}

function mergePostRecord(sourceState, record = {}) {
  const postId = String(record.postId || record.id || '').trim()
  if (!postId) return false

  const existing = sourceState.posts?.[postId] || {}
  if (!sourceState.posts) sourceState.posts = {}
  const createdUtc =
    Number(record.createdUtc || record.created_utc || 0) || null
  const mediaUrls = uniqueValues([existing.mediaUrls, record.mediaUrls])
  const mediaPageUrls = uniqueValues([
    existing.mediaPageUrls,
    record.mediaPageUrls,
    record.mediaPageUrl,
  ])
  const relativePaths = uniqueValues([
    existing.relativePaths,
    record.relativePaths,
  ])

  sourceState.posts[postId] = {
    ...existing,
    postId,
    title: record.title || existing.title || null,
    createdUtc: createdUtc || existing.createdUtc || null,
    mediaUrls,
    mediaPageUrls,
    relativePaths,
    mediaCount: Math.max(
      Number(existing.mediaCount || 0),
      Number(record.mediaCount || mediaUrls.length || 0)
    ),
    firstSeenAt: existing.firstSeenAt || new Date().toISOString(),
    lastSeenAt: new Date().toISOString(),
  }

  const postCreatedUtc = sourceState.posts[postId].createdUtc
  if (
    postCreatedUtc &&
    (!sourceState.latestCreatedUtc ||
      postCreatedUtc > sourceState.latestCreatedUtc)
  ) {
    sourceState.latestCreatedUtc = postCreatedUtc
    sourceState.latestPostId = postId
  }

  return true
}

function sourceMatchesRecord(source, record = {}) {
  if (normalizeKey(record.sourceSite) !== 'reddit') return false
  const sourceUser = normalizeKey(
    source.username || source.userId || source.rawName
  )
  if (!sourceUser) return true
  const recordUsers = [
    record.sourceUsername,
    record.sourceUserId,
    record.sourceRawName,
  ].map(normalizeKey)
  return recordUsers.includes(sourceUser)
}

function getSeenIndexPath(modelLogDir) {
  return path.join(modelLogDir, 'milkmaid-seen-media-index.json')
}

function readSeenIndex(modelLogDir) {
  const seenIndexPath = getSeenIndexPath(modelLogDir)
  if (!fs.existsSync(seenIndexPath)) return null
  try {
    return JSON.parse(fs.readFileSync(seenIndexPath, 'utf8'))
  } catch {
    return null
  }
}

function collectSeenRecords(seenIndex) {
  const records = new Map()
  for (const bucketName of ['mediaUrls', 'mediaPageUrls']) {
    const bucket = seenIndex?.[bucketName]
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

function activeRecordExists(record, datasetPaths) {
  if (!record?.relativePath || !datasetPaths?.toDatasetAbsolutePath) return true
  const absolutePath = datasetPaths.toDatasetAbsolutePath(record.relativePath)
  if (typeof datasetPaths.existsLocallyOrOnNas === 'function') {
    return datasetPaths.existsLocallyOrOnNas(absolutePath)
  }
  return fs.existsSync(absolutePath)
}

function backfillRedditSourceStateFromSeenIndex(
  modelLogDir,
  source,
  options = {}
) {
  const seenIndex = readSeenIndex(modelLogDir)
  if (!seenIndex) {
    return { changed: false, sources: 0, posts: 0, records: 0 }
  }

  const state = loadRedditSourceState(modelLogDir)
  let records = 0
  let posts = 0
  const touchedSources = new Set()

  for (const record of collectSeenRecords(seenIndex)) {
    if (normalizeKey(record?.sourceSite) !== 'reddit') continue
    if (source && !sourceMatchesRecord(source, record)) continue
    if (!activeRecordExists(record, options.datasetPaths)) continue

    const recordSource = source || {
      service: record.sourceService || 'submitted',
      userId: record.sourceUserId || record.sourceUsername,
      username: record.sourceUsername || record.sourceUserId,
    }
    const sourceState = getOrCreateSourceState(state, recordSource)
    const changed = mergePostRecord(sourceState, {
      postId: record.postId || record.sourcePostId,
      createdUtc: getCreatedUtcFromValue(record.uploadedDate),
      mediaUrls: record.mediaUrls || record.mediaUrl,
      mediaPageUrls: record.mediaPageUrls || record.mediaPageUrl,
      mediaPageUrl: record.mediaPageUrl,
      relativePaths: record.relativePath,
      mediaCount: 1,
    })
    if (changed) {
      records += 1
      touchedSources.add(sourceState.sourceKey)
    }
  }

  for (const sourceKey of touchedSources) {
    posts += Object.keys(state.sources[sourceKey]?.posts || {}).length
  }

  if (touchedSources.size > 0) saveRedditSourceState(modelLogDir, state)

  return {
    changed: touchedSources.size > 0,
    sources: touchedSources.size,
    posts,
    records,
  }
}

function createIncrementalSourceState(modelLogDir, source, options = {}) {
  const state = loadRedditSourceState(modelLogDir)
  let sourceState = state.sources[getSourceKey(source)]

  if (!sourceState || Object.keys(sourceState.posts || {}).length === 0) {
    backfillRedditSourceStateFromSeenIndex(modelLogDir, source, options)
    const refreshedState = loadRedditSourceState(modelLogDir)
    sourceState = refreshedState.sources[getSourceKey(source)]
  }

  const posts = sourceState?.posts || {}
  const knownPostIds = new Set(Object.keys(posts))
  return {
    modelLogDir,
    sourceKey: getSourceKey(source),
    sourceState: sourceState || null,
    incrementalState: {
      sourceKey: getSourceKey(source),
      hasFrontier:
        knownPostIds.size > 0 &&
        Boolean(sourceState?.latestCreatedUtc || sourceState?.latestPostId),
      latestPostId: sourceState?.latestPostId || null,
      latestCreatedUtc: sourceState?.latestCreatedUtc || null,
      knownPostIds,
      knownPostCount: knownPostIds.size,
      latestCheckedAt: sourceState?.latestCheckedAt || null,
    },
  }
}

function recordRedditSourceCheck(modelLogDir, source, details = {}) {
  const state = loadRedditSourceState(modelLogDir)
  const sourceState = getOrCreateSourceState(state, source)
  sourceState.latestCheckedAt = new Date().toISOString()
  sourceState.lastNoNewPostsAt = details.noNewPosts
    ? sourceState.latestCheckedAt
    : sourceState.lastNoNewPostsAt || null
  sourceState.sourceUserId = source.userId || sourceState.sourceUserId || null
  sourceState.sourceUsername =
    source.username || source.userId || sourceState.sourceUsername || null
  sourceState.inputUrl = source.inputUrl || sourceState.inputUrl || null

  let mergedPosts = 0
  for (const post of details.posts || []) {
    const mediaEntries = Array.isArray(post.mediaEntries)
      ? post.mediaEntries
      : []
    const merged = mergePostRecord(sourceState, {
      postId: post.id,
      title: post.title,
      createdUtc: getPostCreatedUtc(post),
      mediaUrls: mediaEntries.map((entry) => entry.mediaUrls || entry.mediaUrl),
      mediaPageUrls: mediaEntries.map(
        (entry) => entry.mediaPageUrls || entry.mediaPageUrl
      ),
      mediaCount: mediaEntries.length,
    })
    if (merged) mergedPosts += 1
  }

  if (mergedPosts > 0) sourceState.latestNewPostAt = sourceState.latestCheckedAt
  saveRedditSourceState(modelLogDir, state)
  return {
    sourceKey: sourceState.sourceKey,
    knownPostCount: Object.keys(sourceState.posts || {}).length,
    mergedPosts,
    latestPostId: sourceState.latestPostId || null,
    latestCreatedUtc: sourceState.latestCreatedUtc || null,
  }
}

module.exports = {
  backfillRedditSourceStateFromSeenIndex,
  createIncrementalSourceState,
  getRedditSourceStatePath,
  getSourceKey,
  recordRedditSourceCheck,
}

'use strict'

const fs = require('fs')
const path = require('path')
const { exec } = require('child_process')
const { createHash } = require('crypto')
const minimist = require('minimist')
const pLimit = require('p-limit')

const { bannerHoghaul } = require('../banners.js')
const mediaDates = require('../milkmaid/media-dates.js')
const { writeRepoJsonFileSync } = require('../scrapyard/repoFileWriter')
const {
  mergeNasMp4Entries,
  collectMp4RelativePaths,
  syncNasMp4IndexToMirror,
} = require('../scrapyard/nasMp4Index')
const { createDatasetPaths } = require('../scrapyard/datasetPaths')
const { createMediaSeenIndex } = require('../scrapyard/mediaSeenIndex')
const {
  classifyMediaFilename,
  normalizeMediaEntry,
  normalizeMediaEntries,
  sanitizeToken,
} = require('../scrapyard/mediaEntries')
const mediaFileRecords = require('../scrapyard/mediaFileRecords')
const { createMediaSaver } = require('../scrapyard/mediaSaver')
const { createDuplicateChecker } = require('../scrapyard/duplicateChecker')
const { createHttpClient } = require('../scrapyard/httpClient')
const { createRedgifsClient } = require('../scrapyard/redgifsClient')
const {
  createBrowserMediaDownloader: createSharedBrowserMediaDownloader,
  getDefaultBrowserProfileDir,
} = require('../scrapyard/browserMediaDownloader')
const {
  fetchCoomerKemonoPosts,
  getMediaEntriesFromPost: getCoomerKemonoMediaEntriesFromPost,
  preflightCoomerKemonoSource,
  resolveKemonoCreatorIdForJson: resolveSharedKemonoCreatorIdForJson,
} = require('../scrapyard/sourceAdapters/coomerKemono')
const {
  loadVisualHashCache,
  saveVisualHashCache,
  getVisualHashFromBuffer,
  getVisualHashFromVideoPath,
  getVisualHashDistance,
  isVisualDupe,
  addVisualHash,
  getVisualHashRecord,
  getVisualHashEntries,
} = require('../scrapyard/visualHasher')
const {
  loadBitwiseHashCache,
  saveBitwiseHashCache,
  isBitwiseDupe,
  addBitwiseHash,
  getBitwiseHashRecord,
} = require('../scrapyard/bitwiseHasher')
const { getCompletionLine } = require('../stuffinglogger')

bannerHoghaul()
installProcessTerminationHandlers()

const datasetPaths = createDatasetPaths({
  rootDir: path.join(__dirname, '..'),
  repairCanUseNasMirror: true,
})
const rootDir = datasetPaths.rootDir
const datasetDir = datasetPaths.datasetDir
const registryPath =
  process.env.HOGHAUL_REGISTRY_PATH || path.join(rootDir, 'model_aliases.json')
const API_PAGE_SIZE = 50
const REDDIT_PAGE_SIZE = 100
const API_ACCEPT_HEADER = 'text/css'
const REQUEST_TIMEOUT_MS =
  Number.parseInt(process.env.HOGHAUL_REQUEST_TIMEOUT_MS || '', 10) || 30000
const httpClient = createHttpClient({ timeoutMs: REQUEST_TIMEOUT_MS })
const requestBuffer = httpClient.requestBuffer
const redgifsClient = createRedgifsClient({ requestBuffer })

let currentRunLog = null
const sharedMediaSeenIndex = createMediaSeenIndex({
  datasetDir,
  existsLocallyOrOnNas: (filePath) => existsLocallyOrOnNas(filePath),
  normalizeUrl: (url) => normalizeSeenUrl(htmlDecode(url)),
  matchOrder: ['media_url', 'media_page_url'],
  pageMatchRequiresNoMediaUrl: true,
})
const hoghaulMediaSaver = createMediaSaver({
  datasetDir,
  source: 'hoghaul',
  mediaDates,
  getExtraMetadata: (entry) => getEntryHashMetadata(entry),
  getEventMetadata: (entry) => getEntrySourceDetails(entry),
  getSeenDetails: (entry) => getEntrySeenDetails(entry),
})
const duplicateChecker = createDuplicateChecker({
  datasetDir,
  existsLocallyOrOnNas: (filePath) => existsLocallyOrOnNas(filePath),
  getBitwiseHashRecord,
  isBitwiseDupe,
  getVisualHashRecord,
  isVisualDupe,
  getVisualHashEntries,
  getVisualHashDistance,
})
const {
  getBitwiseDuplicationRecord,
  getVisualDuplicationRecord,
  getFuzzyVisualDuplicationRecord,
  getPendingImageVisualDuplicate,
  reservePendingImageVisualClaim,
  releasePendingImageVisualClaim,
} = duplicateChecker
let successCount = 0
let duplicateCount = 0
let errorCount = 0
let queuedVideoCount = 0
let savedBytes = 0
let browserMediaDownloader = null
const MAX_FUZZY_IMAGE_VISUAL_DISTANCE = 8
let runTerminationHandled = false

function formatPercent(numerator, denominator) {
  if (!Number.isFinite(denominator) || denominator <= 0) return '0.0'
  return ((numerator / denominator) * 100).toFixed(1)
}

function sanitize(name) {
  return String(name || '')
    .replace(/[^a-z0-9_-]/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase()
}

function sortStringValues(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b)
  )
}

function sortSourceList(sources) {
  return Array.isArray(sources) ? sources : []
}

function sortModelRegistry(registry) {
  return Object.fromEntries(
    Object.entries(registry || {}).map(([canonicalName, entry]) => {
      const sources =
        entry?.sources && typeof entry.sources === 'object' ? entry.sources : {}
      return [
        canonicalName,
        {
          aliases: sortStringValues(entry?.aliases),
          sources: Object.fromEntries(
            Object.entries(sources).map(([key, value]) => [
              key,
              sortSourceList(value),
            ])
          ),
        },
      ]
    })
  )
}

function loadModelRegistry() {
  if (!fs.existsSync(registryPath)) {
    writeRepoJsonFileSync(registryPath, {})
    return {}
  }

  try {
    const raw = fs.readFileSync(registryPath, 'utf8').trim()
    return raw ? JSON.parse(raw) : {}
  } catch (err) {
    console.warn(`Could not parse ${registryPath}: ${err.message}`)
    return {}
  }
}

function saveModelRegistry(registry) {
  writeRepoJsonFileSync(registryPath, sortModelRegistry(registry))
}

function ensureModelEntryShape(entry, canonicalName) {
  const aliasSet = new Set(
    Array.isArray(entry?.aliases) ? entry.aliases.filter(Boolean) : []
  )
  if (canonicalName) aliasSet.add(canonicalName)

  return {
    aliases: Array.from(aliasSet),
    sources:
      entry?.sources && typeof entry.sources === 'object' ? entry.sources : {},
  }
}

function findCanonicalModelName(registry, rawName) {
  const normalizedRaw = sanitize(rawName)
  if (!normalizedRaw) return null

  for (const [canonicalName, entry] of Object.entries(registry)) {
    if (sanitize(canonicalName) === normalizedRaw) return canonicalName
    const aliases = Array.isArray(entry?.aliases) ? entry.aliases : []
    if (aliases.some((alias) => sanitize(alias) === normalizedRaw)) {
      return canonicalName
    }
  }

  return null
}

function getSourceRegistryKey(site) {
  return site === 'coomerfans' ? 'coomer' : site
}

function findCanonicalModelNameBySource(registry, sourceInfo) {
  const normalizedUrl = String(sourceInfo?.inputUrl || '').trim()
  const normalizedSite = String(
    getSourceRegistryKey(String(sourceInfo?.site || '').trim())
  ).trim()
  const normalizedService = String(sourceInfo?.service || '').trim()
  const normalizedUserId = String(sourceInfo?.userId || '').trim()
  const normalizedUsername = sanitize(sourceInfo?.username)

  for (const [canonicalName, entry] of Object.entries(registry || {})) {
    const sources =
      entry?.sources && typeof entry.sources === 'object' ? entry.sources : {}

    for (const [siteKey, sourceList] of Object.entries(sources)) {
      if (normalizedSite && siteKey !== normalizedSite) continue

      for (const source of Array.isArray(sourceList) ? sourceList : []) {
        const sourceUrl = String(source?.url || '').trim()
        const sourceService = String(source?.service || '').trim()
        const sourceUserId = String(source?.userId || '').trim()
        const sourceUsername = sanitize(source?.username)

        if (normalizedUrl && sourceUrl === normalizedUrl) {
          return canonicalName
        }

        if (
          normalizedService &&
          normalizedUserId &&
          sourceService === normalizedService &&
          sourceUserId === normalizedUserId
        ) {
          return canonicalName
        }

        if (
          normalizedSite === 'reddit' &&
          normalizedUsername &&
          sourceUsername === normalizedUsername
        ) {
          return canonicalName
        }
      }
    }
  }

  return null
}

function upsertHoghaulSource(entry, sourceInfo) {
  const sourceKey = getSourceRegistryKey(sourceInfo.site)
  const now = new Date().toISOString()
  if (!entry.sources) entry.sources = {}
  if (!Array.isArray(entry.sources[sourceKey])) entry.sources[sourceKey] = []

  const sourceIndex = entry.sources[sourceKey].findIndex(
    (source) =>
      source?.url === sourceInfo.inputUrl ||
      (source?.service === sourceInfo.service &&
        source?.userId === sourceInfo.userId) ||
      (sourceInfo.site === 'reddit' &&
        source?.username &&
        sanitize(source.username) === sanitize(sourceInfo.username))
  )

  const nextSource = {
    url: sourceInfo.inputUrl,
    service: sourceInfo.service,
    userId: sourceInfo.userId,
    username: sourceInfo.username || null,
    discoveredAs: sourceInfo.rawName,
    lastCheckedAt: now,
  }

  if (sourceIndex >= 0) {
    entry.sources[sourceKey][sourceIndex] = {
      ...entry.sources[sourceKey][sourceIndex],
      ...nextSource,
    }
  } else {
    entry.sources[sourceKey].push(nextSource)
  }
}

function resolveAndTrackModel(rawName, sourceInfo, canonicalOverride) {
  const registry = loadModelRegistry()
  const cleanedRawName = sanitize(rawName) || 'unknown_model'
  const cleanedOverride = sanitize(canonicalOverride)
  const existingCanonicalBySource = findCanonicalModelNameBySource(
    registry,
    sourceInfo
  )
  const existingCanonical = cleanedOverride
    ? findCanonicalModelName(registry, cleanedOverride)
    : existingCanonicalBySource ||
      findCanonicalModelName(registry, cleanedRawName)
  const canonicalName =
    existingCanonical ||
    existingCanonicalBySource ||
    cleanedOverride ||
    cleanedRawName

  registry[canonicalName] = ensureModelEntryShape(
    registry[canonicalName],
    canonicalName
  )

  const aliases = registry[canonicalName].aliases
  if (!aliases.some((alias) => sanitize(alias) === cleanedRawName)) {
    aliases.push(cleanedRawName)
  }
  registry[canonicalName].aliases = sortStringValues(aliases)

  upsertHoghaulSource(registry[canonicalName], {
    ...sourceInfo,
    rawName: cleanedRawName,
  })
  saveModelRegistry(registry)

  return canonicalName
}

function registerSourceForRun(source, inputUrl, canonicalOverride) {
  const modelName = resolveAndTrackModel(
    source.rawName,
    {
      ...source,
      inputUrl,
    },
    canonicalOverride
  )
  console.log(
    `Registered source in model_aliases.json: ${source.site}/${source.service}/${source.userId} -> ${modelName}`
  )
  return modelName
}

function createModelFolders(modelName) {
  return datasetPaths.createModelFolders(modelName)
}

function getDatasetRelativePath(filePath) {
  return datasetPaths.getDatasetRelativePath(filePath)
}

function getQuarantineMirrorPath(filePath) {
  return datasetPaths.getQuarantineMirrorPath(filePath)
}

function getNasMirrorPath(filePath) {
  return datasetPaths.getNasMirrorPath(filePath)
}

function isQuarantinedPath(filePath) {
  return datasetPaths.isQuarantinedPath(filePath)
}

function existsForRepair(filePath) {
  return datasetPaths.existsForRepair(filePath)
}

function existsAtExactPath(filePath) {
  return datasetPaths.existsAtExactPath(filePath)
}

function existsLocallyOrOnNas(filePath) {
  return datasetPaths.existsLocallyOrOnNas(filePath)
}

function parseResolvedDate(date) {
  return mediaFileRecords.parseResolvedDate(date)
}

function removeFileIfExists(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return false
  fs.unlinkSync(filePath)
  return true
}

function moveFileIntoPlace(sourcePath, destinationPath) {
  fs.mkdirSync(path.dirname(destinationPath), { recursive: true })
  try {
    fs.renameSync(sourcePath, destinationPath)
  } catch (err) {
    if (err.code !== 'EXDEV') throw err
    fs.copyFileSync(sourcePath, destinationPath)
    fs.unlinkSync(sourcePath)
  }
}

function startRunLog(modelName, inputUrl, folders, keepHistory) {
  runTerminationHandled = false
  const stamp = new Date().toISOString().replace(/[:.]/g, '-')
  const logPath = path.join(folders.logDir, `hoghaul-run-${stamp}.jsonl`)
  const summaryPath = path.join(
    folders.logDir,
    'hoghaul-run-latest-summary.json'
  )
  const modelSummaryPath = path.join(folders.base, 'hoghaul-last-run.json')
  currentRunLog = {
    stamp,
    logPath,
    summaryPath,
    modelSummaryPath,
    modelName,
    inputUrl,
    keepHistory: Boolean(keepHistory),
    startedAt: new Date().toISOString(),
    counters: {
      saved: 0,
      skipped: 0,
      duplicates: 0,
      queuedVideos: 0,
      failures: 0,
      processed: 0,
      expectedMedia: 0,
    },
    transfer: {
      savedBytes: 0,
      lazyExpectedBytes: 0,
      lazyTransferredBytes: 0,
    },
    errors: [],
  }

  removeFileIfExists(modelSummaryPath)
  fs.writeFileSync(
    modelSummaryPath,
    JSON.stringify(
      {
        startedAt: currentRunLog.startedAt,
        modelName,
        inputUrl,
        status: 'running',
      },
      null,
      2
    ) + '\n'
  )

  appendRunEvent('run_started', {
    modelName,
    inputUrl,
    logPath,
  })
}

function appendRunEvent(type, payload = {}) {
  if (!currentRunLog) return
  fs.appendFileSync(
    currentRunLog.logPath,
    JSON.stringify({
      at: new Date().toISOString(),
      type,
      ...payload,
    }) + '\n'
  )
}

function recordRunError(category, details = {}) {
  if (!currentRunLog) return
  currentRunLog.errors.push({
    at: new Date().toISOString(),
    category,
    ...details,
  })
}

function finalizeRunLog(extra = {}) {
  if (!currentRunLog) return

  const { status = 'finished', ...rest } = extra
  const finishedAt = new Date().toISOString()
  const durationMs = Math.max(
    new Date(finishedAt).getTime() -
      new Date(currentRunLog.startedAt).getTime(),
    0
  )
  const summary = {
    startedAt: currentRunLog.startedAt,
    finishedAt,
    durationMs,
    modelName: currentRunLog.modelName,
    inputUrl: currentRunLog.inputUrl,
    logPath: currentRunLog.logPath,
    counters: currentRunLog.counters,
    transfer: currentRunLog.transfer,
    errors: currentRunLog.errors,
    ...rest,
  }

  fs.writeFileSync(
    currentRunLog.summaryPath,
    JSON.stringify(summary, null, 2) + '\n'
  )
  fs.writeFileSync(
    currentRunLog.modelSummaryPath,
    JSON.stringify(
      {
        ...summary,
        status,
      },
      null,
      2
    ) + '\n'
  )

  const shouldKeepHistory =
    currentRunLog.keepHistory || currentRunLog.errors.length > 0
  if (!shouldKeepHistory) removeFileIfExists(currentRunLog.logPath)
  currentRunLog = null
}

function setExpectedMediaCount(total) {
  if (!currentRunLog) return
  currentRunLog.counters.expectedMedia =
    Number.isFinite(total) && total >= 0 ? total : 0
}

function logRunProgress(context = '') {
  if (!currentRunLog) return

  const processed = currentRunLog.counters.processed || 0
  const expected = currentRunLog.counters.expectedMedia || 0
  const saved = currentRunLog.counters.saved || 0
  const skipped = currentRunLog.counters.skipped || 0
  const duplicates = currentRunLog.counters.duplicates || 0
  const failures = currentRunLog.counters.failures || 0
  const remaining = Math.max(expected - processed, 0)
  const percent = formatPercent(processed, expected)
  const suffix = context ? ` :: ${context}` : ''

  console.log(
    `Progress: ${processed}/${expected} (${percent}%) | saved ${saved} | skipped ${skipped} | dupes ${duplicates} | failed ${failures} | remaining ${remaining}${suffix}`
  )
}

function noteMediaOutcome(kind, context = '') {
  if (!currentRunLog) return

  currentRunLog.counters.processed += 1
  if (kind === 'saved') {
    currentRunLog.counters.saved += 1
  } else if (kind === 'skipped') {
    currentRunLog.counters.skipped += 1
  } else if (kind === 'duplicate') {
    currentRunLog.counters.duplicates += 1
  } else if (kind === 'failed') {
    currentRunLog.counters.failures += 1
  }

  logRunProgress(context)
}

function finalizeAbortedRun(status, error) {
  if (!currentRunLog || runTerminationHandled) return
  runTerminationHandled = true

  const normalizedStatus = status || 'failed'
  const errorMessage =
    error instanceof Error ? error.message : String(error || '').trim()

  if (errorMessage) {
    recordRunError('run_error', {
      error: errorMessage,
      status: normalizedStatus,
    })
    appendRunEvent('run_error', {
      error: errorMessage,
      status: normalizedStatus,
    })
  }

  finalizeRunLog({
    status: normalizedStatus,
    successCount,
    duplicateCount,
    errorCount: errorMessage ? errorCount + 1 : errorCount,
    queuedVideoCount,
    savedBytes,
  })
}

function installProcessTerminationHandlers() {
  process.on('beforeExit', () => {
    finalizeAbortedRun(
      'interrupted',
      'Process exited before Hoghaul completed normally'
    )
  })

  process.on('SIGINT', () => {
    finalizeAbortedRun('interrupted', 'Received SIGINT')
    process.exit(130)
  })

  process.on('SIGTERM', () => {
    finalizeAbortedRun('interrupted', 'Received SIGTERM')
    process.exit(143)
  })

  process.on('uncaughtException', (err) => {
    finalizeAbortedRun('failed', err)
    console.error(err.stack || err.message)
    process.exit(1)
  })

  process.on('unhandledRejection', (reason) => {
    const err =
      reason instanceof Error ? reason : new Error(String(reason || ''))
    finalizeAbortedRun('failed', err)
    console.error(err.stack || err.message)
    process.exit(1)
  })
}

function normalizeSeenUrl(url) {
  return String(url || '')
    .trim()
    .replace(/&acs=[^&]+/gi, '')
}

function uniqueSeenUrls(values) {
  return sharedMediaSeenIndex.uniqueSeenUrls(values)
}

function getEntryMediaUrls(entry) {
  return uniqueSeenUrls([
    entry?.mediaUrl,
    entry?.jsonMediaUrl,
    entry?.mediaUrls,
    entry?.sourceUrls,
  ])
}

function getEntryMediaPageUrls(entry) {
  return uniqueSeenUrls([entry?.mediaPageUrl, entry?.mediaPageUrls])
}

function getEntrySeenDetails(entry) {
  return {
    mediaUrl: entry.mediaUrl,
    mediaUrls: getEntryMediaUrls(entry),
    mediaPageUrl: entry.mediaPageUrl,
    mediaPageUrls: getEntryMediaPageUrls(entry),
  }
}

function getEntrySourceDetails(entry) {
  return {
    sourceSite: entry.sourceSite || null,
    sourceService: entry.sourceService || null,
    sourceUserId: entry.sourceUserId || null,
    sourceUsername: entry.sourceUsername || null,
    sourceSubreddit: entry.sourceSubreddit || null,
    postId: entry.postId || null,
  }
}

function getEntryHashMetadata(entry = {}) {
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

function getMediaSeenIndexPath(modelLogDir) {
  return sharedMediaSeenIndex.getMediaSeenIndexPath(modelLogDir)
}

function loadMediaSeenIndex(modelLogDir) {
  return sharedMediaSeenIndex.loadMediaSeenIndex(modelLogDir)
}

function saveMediaSeenIndex(modelLogDir, data) {
  return sharedMediaSeenIndex.saveMediaSeenIndex(modelLogDir, data)
}

function getActiveMediaSeenRecord(modelLogDir, entry) {
  return sharedMediaSeenIndex.getActiveMediaSeenRecord(entry)
}

function getSuccessfulSeenMediaMatch(modelLogDir, mediaPageUrl, mediaUrl) {
  return sharedMediaSeenIndex.getSuccessfulSeenMediaMatch(
    modelLogDir,
    mediaPageUrl,
    mediaUrl
  )
}

function recordSuccessfulSeenMedia(modelLogDir, details = {}) {
  return sharedMediaSeenIndex.recordSuccessfulSeenMedia(modelLogDir, details)
}

async function closeBrowserMediaDownloader() {
  if (!browserMediaDownloader) return
  const downloader = browserMediaDownloader
  browserMediaDownloader = null
  await downloader.close()
}

async function fetchJson(url) {
  const parsed = new URL(url)
  const isReddit = parsed.hostname.toLowerCase().endsWith('reddit.com')
  return httpClient.fetchJson(url, {
    headers: {
      Accept: isReddit ? 'application/json' : API_ACCEPT_HEADER,
    },
  })
}

async function fetchHtml(url) {
  return httpClient.fetchHtml(url)
}

function parseRedgifsId(url) {
  return redgifsClient.parseRedgifsId(url)
}

async function resolveRedgifsEntry(source, post, redgifsUrl, uploadedDate) {
  const resolved = await redgifsClient.resolveMedia(redgifsUrl)
  if (!resolved) return null
  const { id, mediaUrl } = resolved

  const subreddit = getRedditSubreddit(post)
  const filename = buildRedditFilename(
    source,
    post,
    mediaUrl,
    path.extname(new URL(mediaUrl).pathname) || '.mp4'
  )
  const createdDate = resolved.createdDate || uploadedDate

  return {
    sourceSite: 'reddit',
    sourceService: source.service || 'submitted',
    sourceUserId: source.userId || null,
    sourceUsername: source.username || source.userId || null,
    sourceSubreddit: subreddit,
    postId: String(post.id || ''),
    title: post.title || null,
    mediaPageUrl: getPostPageUrl(source, post),
    mediaPageUrls: getRedditMediaPageUrls(source, post),
    mediaUrl,
    mediaUrls: uniqueSeenUrls([mediaUrl, resolved.mediaUrls]),
    sourceUrls: uniqueSeenUrls([
      redgifsUrl,
      resolved.canonicalUrl,
      getRedditPostLinkedUrls(source, post),
    ]),
    filename,
    originalName: id,
    uploadedDate: parseResolvedDate(createdDate) || uploadedDate,
  }
}

function parseSourceUrl(inputUrl) {
  const parsed = new URL(inputUrl)
  const host = parsed.hostname.toLowerCase()
  const site = host.includes('coomerfans')
    ? 'coomerfans'
    : host.includes('coomer')
      ? 'coomer'
      : host.includes('kemono')
        ? 'kemono'
        : host.endsWith('reddit.com')
          ? 'reddit'
          : null
  if (!site) throw new Error(`Unsupported Hoghaul host: ${parsed.hostname}`)

  const parts = parsed.pathname.split('/').filter(Boolean)

  if (site === 'reddit') {
    if (parts[0]?.toLowerCase() === 'user' && parts[1]) {
      const username = parts[1].replace(/^u_/, '')
      return {
        inputUrl,
        origin: 'https://www.reddit.com',
        site,
        service: 'submitted',
        userId: username,
        username,
        rawName: sanitize(username),
      }
    }

    throw new Error(
      'Expected a Reddit user URL like /user/name/submitted or /user/name'
    )
  }

  if (site === 'coomerfans') {
    if (parts[0] === 'u' && parts[1] && parts[2] && parts[3]) {
      return {
        inputUrl,
        origin: parsed.origin,
        site,
        service: parts[1],
        userId: parts[2],
        rawName: sanitize(parts[3]),
      }
    }

    const queryName = parsed.searchParams.get('q')
    if (queryName) {
      return {
        inputUrl,
        origin: parsed.origin,
        site,
        service: 'onlyfans',
        userId: null,
        rawName: sanitize(queryName),
      }
    }

    throw new Error(
      'Expected a CoomerFans URL like /u/onlyfans/id/name or /?q=name'
    )
  }

  const userIndex = parts.indexOf('user')
  const service = parts[0]
  const userId = userIndex >= 0 ? parts[userIndex + 1] : null

  if (!service || !userId) {
    throw new Error(
      'Expected a creator URL like /onlyfans/user/name or /patreon/user/id'
    )
  }

  return {
    inputUrl,
    origin: parsed.origin,
    site,
    service,
    userId,
    rawName: sanitize(userId),
  }
}

function parsePageRange(value) {
  if (!value) return { startPage: 0, endPage: null }
  const raw = String(value)
    .replace(/^--pages=/, '')
    .trim()
  if (!raw) return { startPage: 0, endPage: null }

  if (raw.includes('-')) {
    const [left, right] = raw
      .split('-')
      .map((part) => Number.parseInt(part, 10))
    const startPage = Number.isFinite(left) && left > 0 ? left - 1 : 0
    const endPage = Number.isFinite(right) && right > 0 ? right - 1 : null
    return { startPage, endPage }
  }

  const pageCount = Number.parseInt(raw, 10)
  if (!Number.isFinite(pageCount) || pageCount <= 0) {
    return { startPage: 0, endPage: null }
  }
  return { startPage: 0, endPage: pageCount - 1 }
}

function isTruthyFlag(value) {
  if (value === true) return true
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
  return ['1', 'true', 'yes'].includes(normalized)
}

function parsePositiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function printMediaSample(entries, limit = 5) {
  const sample = entries.slice(0, limit)
  if (sample.length === 0) return

  console.log('Media sample:')
  for (const entry of sample) {
    const sourceBits = [
      entry.sourceSite,
      entry.sourceSubreddit ? `r/${entry.sourceSubreddit}` : null,
      entry.postId ? `post:${entry.postId}` : null,
    ].filter(Boolean)
    const sourceText = sourceBits.length ? ` (${sourceBits.join(', ')})` : ''
    console.log(`- ${entry.filename}${sourceText}`)
  }
}

function getPostPageUrl(source, post) {
  if (source.site === 'coomerfans') {
    return (
      post.url ||
      `${source.origin}/p/${post.id}/${source.userId}/${source.service}`
    )
  }
  if (source.site === 'reddit') {
    return post.permalink
      ? new URL(post.permalink, source.origin).toString()
      : `${source.origin}/comments/${post.id}`
  }
  return `${source.origin}/${source.service}/user/${source.userId}/post/${post.id}`
}

function filenameFromMediaUrl(mediaUrl) {
  try {
    const name = decodeURIComponent(path.basename(new URL(mediaUrl).pathname))
    return name && name !== 'data' ? name : null
  } catch {
    return null
  }
}

function getRedditPostDate(post) {
  const createdUtc = Number(post?.created_utc)
  if (Number.isFinite(createdUtc) && createdUtc > 0) {
    return new Date(createdUtc * 1000)
  }
  return parseResolvedDate(post?.created)
}

function getRedditSubreddit(post) {
  return sanitizeToken(
    post?.subreddit_name_prefixed || post?.subreddit || post?.subreddit_id
  )
}

function getRedditLinkedUrl(source, value) {
  const url = htmlDecode(value)
  if (!url) return null
  try {
    return new URL(url, source.origin).toString()
  } catch {
    return url
  }
}

function isRedditContainerUrl(source, post, value) {
  if (!value) return false
  try {
    const parsed = new URL(value, source.origin)
    const host = parsed.hostname.toLowerCase()
    if (!host.endsWith('reddit.com')) return false
    const pathname = parsed.pathname.toLowerCase()
    const postId = String(post?.id || '').toLowerCase()
    return (
      pathname.includes(`/comments/${postId}`) ||
      pathname.includes(`/gallery/${postId}`) ||
      parsed.toString() === getPostPageUrl(source, post)
    )
  } catch {
    return false
  }
}

function getRedditPostLinkedUrls(source, post) {
  return uniqueSeenUrls([
    getRedditLinkedUrl(source, post?.url_overridden_by_dest),
    getRedditLinkedUrl(source, post?.url),
  ]).filter((url) => !isRedditContainerUrl(source, post, url))
}

function getRedditMediaPageUrls(source, post) {
  const pageUrls = [getPostPageUrl(source, post)]
  if (post?.is_gallery || post?.gallery_data) {
    pageUrls.push(`${source.origin}/gallery/${post.id}`)
  }
  return uniqueSeenUrls(pageUrls)
}

function getRedditMediaMetadataUrls(metadata) {
  return uniqueSeenUrls([
    metadata?.s?.u,
    metadata?.s?.gif,
    metadata?.s?.mp4,
    Array.isArray(metadata?.o) ? metadata.o.map((item) => item?.u) : [],
    Array.isArray(metadata?.p) ? metadata.p.map((item) => item?.u) : [],
  ])
}

function getRedditMediaMetadataUrl(metadata) {
  return getRedditMediaMetadataUrls(metadata)[0] || ''
}

function extensionFromMime(mime) {
  const normalized = String(mime || '').toLowerCase()
  if (normalized.includes('jpeg') || normalized.includes('jpg')) return '.jpg'
  if (normalized.includes('png')) return '.png'
  if (normalized.includes('webp')) return '.webp'
  if (normalized.includes('gif')) return '.gif'
  if (normalized.includes('mp4')) return '.mp4'
  return ''
}

function buildRedditFilename(_source, post, mediaUrl, fallbackExt, index = 0) {
  const urlName = mediaUrl ? filenameFromMediaUrl(mediaUrl) : null
  const ext = path.extname(urlName || '') || fallbackExt || ''
  const suffix = index > 0 ? `_${index + 1}` : ''
  const subreddit = getRedditSubreddit(post)
  const subredditPart = subreddit ? `${subreddit}_` : ''
  return `${subredditPart}${post.id}${suffix}${ext || '.jpg'}`
}

function createRedditEntry(source, post, mediaUrl, uploadedDate, options = {}) {
  const filename =
    options.filename ||
    buildRedditFilename(
      source,
      post,
      mediaUrl,
      options.fallbackExt,
      options.index
    )
  return {
    sourceSite: 'reddit',
    sourceService: source.service || 'submitted',
    sourceUserId: source.userId || null,
    sourceUsername: source.username || source.userId || null,
    sourceSubreddit: getRedditSubreddit(post),
    postId: String(post.id || ''),
    title: post.title || null,
    mediaPageUrl: getPostPageUrl(source, post),
    mediaPageUrls: getRedditMediaPageUrls(source, post),
    mediaUrl,
    mediaUrls: uniqueSeenUrls([mediaUrl, options.mediaUrls]),
    sourceUrls: uniqueSeenUrls([
      options.sourceUrls,
      getRedditPostLinkedUrls(source, post),
    ]),
    filename,
    originalName: options.originalName || filenameFromMediaUrl(mediaUrl),
    uploadedDate,
  }
}

function getNativeRedditVideoUrl(post) {
  return (
    post?.secure_media?.reddit_video?.fallback_url ||
    post?.media?.reddit_video?.fallback_url ||
    post?.preview?.reddit_video_preview?.fallback_url ||
    null
  )
}

function getNativeRedditVideoUrls(post) {
  return uniqueSeenUrls([
    post?.secure_media?.reddit_video?.fallback_url,
    post?.secure_media?.reddit_video?.dash_url,
    post?.secure_media?.reddit_video?.hls_url,
    post?.media?.reddit_video?.fallback_url,
    post?.media?.reddit_video?.dash_url,
    post?.media?.reddit_video?.hls_url,
    post?.preview?.reddit_video_preview?.fallback_url,
    post?.preview?.reddit_video_preview?.dash_url,
    post?.preview?.reddit_video_preview?.hls_url,
  ])
}

function getRedditGalleryEntries(source, post, uploadedDate) {
  const items = Array.isArray(post?.gallery_data?.items)
    ? post.gallery_data.items
    : []
  const metadata = post?.media_metadata || {}

  return items
    .map((item, index) => {
      const mediaId = item?.media_id
      const meta = mediaId ? metadata[mediaId] : null
      if (!meta || meta.status === 'failed') return null
      const mediaUrl = getRedditMediaMetadataUrl(meta)
      if (!mediaUrl) return null
      return createRedditEntry(source, post, mediaUrl, uploadedDate, {
        filename: buildRedditFilename(
          source,
          post,
          mediaUrl,
          extensionFromMime(meta.m),
          index
        ),
        mediaUrls: getRedditMediaMetadataUrls(meta),
        originalName: mediaId,
      })
    })
    .filter(Boolean)
}

async function getRedditMediaEntries(source, post) {
  const uploadedDate = getRedditPostDate(post)
  const entries = getRedditGalleryEntries(source, post, uploadedDate)
  const redgifsId = parseRedgifsId(post.url_overridden_by_dest || post.url)
  let redgifsResolved = false
  if (redgifsId) {
    const redgifsEntry = await resolveRedgifsEntry(
      source,
      post,
      post.url_overridden_by_dest || post.url,
      uploadedDate
    ).catch((err) => {
      console.warn(`RedGIFs resolve failed for ${post.id}: ${err.message}`)
      return null
    })
    if (redgifsEntry) {
      entries.push(redgifsEntry)
      redgifsResolved = true
    }
  }

  const videoUrl = redgifsResolved ? null : getNativeRedditVideoUrl(post)
  if (videoUrl) {
    entries.push(
      createRedditEntry(source, post, videoUrl, uploadedDate, {
        fallbackExt: '.mp4',
        mediaUrls: getNativeRedditVideoUrls(post),
      })
    )
  }

  const directUrl = htmlDecode(post.url_overridden_by_dest || post.url || '')
  if (
    /^https?:\/\/(?:i|preview)\.redd\.it\//i.test(directUrl) ||
    /^https?:\/\/i\.redditmedia\.com\//i.test(directUrl)
  ) {
    entries.push(createRedditEntry(source, post, directUrl, uploadedDate))
  }

  return dedupeMediaEntries(entries).entries
}

function getMediaEntriesFromPost(source, post) {
  if (Array.isArray(post.mediaEntries)) return post.mediaEntries
  if (source.site === 'coomer' || source.site === 'kemono') {
    return getCoomerKemonoMediaEntriesFromPost(source, post, {
      normalizeUrl: normalizeSeenUrl,
    })
  }
  return []
}

async function enrichMediaEntriesFromBrowserDom(entries, downloader) {
  if (!downloader?.extractPostMediaUrls || entries.length === 0) return entries

  const entriesByPost = new Map()
  for (const entry of entries) {
    if (!entriesByPost.has(entry.mediaPageUrl)) {
      entriesByPost.set(entry.mediaPageUrl, [])
    }
    entriesByPost.get(entry.mediaPageUrl).push(entry)
  }

  const enriched = [...entries]
  const entryIndexByKey = new Map(
    enriched.map((entry, index) => [
      `${entry.mediaPageUrl}\n${entry.filename}`,
      index,
    ])
  )

  for (const [postPageUrl, postEntries] of entriesByPost) {
    let domUrls = []
    try {
      domUrls = await downloader.extractPostMediaUrls(postPageUrl)
    } catch (err) {
      appendRunEvent('dom_media_extract_error', {
        mediaPageUrl: postPageUrl,
        error: err.message,
      })
      console.warn(`DOM media extract failed: ${postPageUrl} - ${err.message}`)
      continue
    }

    const domUrlByFilename = new Map()
    for (const domUrl of domUrls) {
      const filename = filenameFromMediaUrl(domUrl)
      if (filename && !domUrlByFilename.has(filename)) {
        domUrlByFilename.set(filename, domUrl)
      }
    }

    let replacedCount = 0
    for (const entry of postEntries) {
      const domUrl = domUrlByFilename.get(entry.filename)
      if (!domUrl || domUrl === entry.mediaUrl) continue
      const index = entryIndexByKey.get(
        `${entry.mediaPageUrl}\n${entry.filename}`
      )
      if (index === undefined) continue
      enriched[index] = {
        ...enriched[index],
        jsonMediaUrl: enriched[index].mediaUrl,
        mediaUrl: domUrl,
        mediaUrls: uniqueSeenUrls([
          enriched[index].mediaUrls,
          enriched[index].mediaUrl,
          domUrl,
        ]),
      }
      replacedCount += 1
    }

    appendRunEvent('dom_media_extracted', {
      mediaPageUrl: postPageUrl,
      domMediaCount: domUrls.length,
      replacedCount,
    })
    console.log(
      `DOM media links: ${replacedCount}/${postEntries.length} updated for ${postPageUrl}`
    )
  }

  return enriched
}

function dedupeMediaEntries(entries) {
  const seen = new Set()
  const deduped = []

  for (const entry of entries) {
    const mediaKeys = getEntryMediaUrls(entry)
    const fallbackKey =
      mediaKeys.length > 0
        ? null
        : `${normalizeSeenUrl(entry.mediaPageUrl)}\n${entry.filename}`
    const keys =
      mediaKeys.length > 0 ? mediaKeys : [fallbackKey].filter(Boolean)
    if (keys.length === 0 || keys.some((key) => seen.has(key))) continue
    for (const key of keys) seen.add(key)
    deduped.push(entry)
  }

  return {
    entries: deduped,
    duplicateCount: Math.max(entries.length - deduped.length, 0),
  }
}

function uniqueValues(values) {
  return Array.from(new Set(values.filter(Boolean)))
}

function htmlDecode(value) {
  return String(value || '')
    .replace(/&amp;/g, '&')
    .replace(/&#43;/g, '+')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
}

function absoluteUrl(source, href) {
  if (!href) return null
  return new URL(htmlDecode(href), source.origin).toString()
}

function extractRegexValues(text, regex, group = 1) {
  return Array.from(String(text || '').matchAll(regex))
    .map((match) => match[group])
    .filter(Boolean)
}

async function resolveCoomerFansCreator(source) {
  if (source.userId) return source

  const searchUrl = `${source.origin}/?q=${encodeURIComponent(source.rawName)}`
  const { html } = await fetchHtml(searchUrl)
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
        rawName: sanitize(decodeURIComponent(match[3])),
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
  console.log(
    `Resolved CoomerFans creator ${source.rawName} -> ${source.service}/${source.userId}`
  )
  return source
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

function parseCoomerFansTitle(html) {
  const ogTitle = String(html || '').match(
    /<meta\s+property=["']og:title["']\s+content=["']([^"']+)["']/i
  )?.[1]
  if (ogTitle) return htmlDecode(ogTitle)
  return htmlDecode(String(html || '').match(/<h1[^>]*>(.*?)<\/h1>/is)?.[1])
    .replace(/<[^>]+>/g, ' ')
    .trim()
}

function parseCoomerFansMediaEntries(source, post, html) {
  const uploadedDate = parseCoomerFansDate(html)
  const title = parseCoomerFansTitle(html) || null
  const mediaUrls = uniqueValues(
    extractRegexValues(
      html,
      /https?:\/\/(?:img\d+\.coomerfans\.com|coomerfans\.com)\/(?:storage|videos?)\/[^"'<> \r\n]+/gi,
      0
    )
      .map((url) => htmlDecode(url))
      .filter((url) => !url.includes('/istorage/'))
  )

  return mediaUrls
    .map((mediaUrl) => {
      const filename = filenameFromMediaUrl(mediaUrl)
      if (!filename) return null
      return {
        postId: String(post.id || ''),
        title,
        mediaPageUrl: post.url,
        mediaUrl,
        filename,
        originalName: null,
        uploadedDate,
      }
    })
    .filter(Boolean)
}

async function preflightSourceJson(source, page = 0) {
  if (source.site === 'reddit') {
    return preflightRedditSource(source)
  }
  return preflightCoomerKemonoSource(source, page, {
    fetchJson,
    pageSize: API_PAGE_SIZE,
  })
}

async function resolveKemonoCreatorIdForJson(source) {
  return resolveSharedKemonoCreatorIdForJson(source, {
    fetchJson,
    logger: console,
  })
}

async function fetchCoomerFansPosts(source, options) {
  await resolveCoomerFansCreator(source)
  const posts = []
  let page = options.startPage
  const postLimit = pLimit(options.postConcurrency || 1)

  while (true) {
    if (options.endPage !== null && page > options.endPage) break
    const pageNumber = page + 1
    const pageUrl =
      pageNumber <= 1
        ? `${source.origin}/u/${source.service}/${source.userId}/${source.rawName}`
        : `${source.origin}/u/${source.service}/${source.userId}/${source.rawName}?page=${pageNumber}`
    console.log(`Loading coomerfans page ${pageNumber} (${pageUrl})`)

    const { html } = await fetchHtml(pageUrl)
    const postLinks = parseCoomerFansPostLinks(source, html)
    if (postLinks.length === 0) break

    const selectedPostLinks =
      Number.isFinite(options.maxPosts) && options.maxPosts > 0
        ? postLinks.slice(0, Math.max(options.maxPosts - posts.length, 0))
        : postLinks

    const pagePosts = await Promise.all(
      selectedPostLinks.map((post) =>
        postLimit(async () => {
          console.log(`Loading coomerfans post ${post.id}`)
          const { html: postHtml } = await fetchHtml(post.url)
          const mediaEntries = parseCoomerFansMediaEntries(
            source,
            post,
            postHtml
          )
          return {
            id: post.id,
            url: post.url,
            title: mediaEntries[0]?.title || null,
            published: mediaEntries[0]?.uploadedDate || null,
            mediaEntries,
          }
        })
      )
    )
    posts.push(...pagePosts)

    if (
      Number.isFinite(options.maxPosts) &&
      options.maxPosts > 0 &&
      posts.length >= options.maxPosts
    ) {
      break
    }

    if (!html.includes(`?page=${pageNumber + 1}`)) break
    page += 1
  }

  return posts
}

function getRedditListingUrl(source, after = null) {
  const url = new URL(
    `/user/${encodeURIComponent(source.username || source.userId)}/submitted/.json`,
    source.origin
  )
  url.searchParams.set('limit', String(REDDIT_PAGE_SIZE))
  url.searchParams.set('raw_json', '1')
  if (after) url.searchParams.set('after', after)
  return url.toString()
}

async function preflightRedditSource(source) {
  const apiUrl = getRedditListingUrl(source)
  const { data, byteLength } = await fetchJson(apiUrl)
  const children = Array.isArray(data?.data?.children)
    ? data.data.children.map((child) => child?.data).filter(Boolean)
    : []
  const newest = children
    .map((post) => getRedditPostDate(post))
    .filter(Boolean)
    .sort((a, b) => b.getTime() - a.getTime())[0]

  return {
    apiUrl,
    byteLength,
    postCount: children.length,
    newest,
    firstPostId: children[0]?.id ? String(children[0].id) : null,
  }
}

async function fetchRedditPosts(source, options) {
  const posts = []
  let after = null
  let page = 0

  while (true) {
    if (options.endPage !== null && page > options.endPage) break
    const apiUrl = getRedditListingUrl(source, after)
    console.log(`Loading reddit page ${page + 1} (${apiUrl})`)
    const { data } = await fetchJson(apiUrl)
    const listing = data?.data
    const pagePosts = Array.isArray(listing?.children)
      ? listing.children.map((child) => child?.data).filter(Boolean)
      : []
    if (pagePosts.length === 0) break

    for (const post of pagePosts) {
      const mediaEntries = await getRedditMediaEntries(source, post)
      posts.push({
        ...post,
        id: String(post.id || ''),
        published: getRedditPostDate(post),
        mediaEntries,
      })
      if (
        Number.isFinite(options.maxPosts) &&
        options.maxPosts > 0 &&
        posts.length >= options.maxPosts
      ) {
        return posts
      }
    }

    after = listing?.after || null
    if (!after) break
    page += 1
  }

  return posts
}

async function fetchPosts(source, options) {
  if (source.site === 'coomerfans') {
    return fetchCoomerFansPosts(source, options)
  }
  if (source.site === 'reddit') {
    return fetchRedditPosts(source, options)
  }

  return fetchCoomerKemonoPosts(source, options, {
    fetchJson,
    logger: console,
    normalizeUrl: normalizeSeenUrl,
    pageSize: API_PAGE_SIZE,
  })
}

function classifyMedia(filename) {
  return classifyMediaFilename(filename)
}

async function downloadMediaBuffer(mediaUrl, entry = {}) {
  if (browserMediaDownloader) {
    return browserMediaDownloader.download(mediaUrl, entry)
  }

  const response = await requestBuffer(mediaUrl, {
    headers: {
      Accept: '*/*',
    },
  })
  return response.buffer
}

function recordDuplicate(entry, savedPath, reason, folders, extra = null) {
  duplicateCount += 1
  appendRunEvent(
    reason,
    hoghaulMediaSaver.buildDuplicateEvent({
      entry,
      savedPath,
      extra: extra && typeof extra === 'object' ? extra : {},
    })
  )
  recordSuccessfulSeenMedia(
    folders.logDir,
    hoghaulMediaSaver.buildSeenRecord(entry, {
      savedPath,
      relativePath: savedPath,
      filename: entry.filename,
    })
  )
  noteMediaOutcome(
    hoghaulMediaSaver.getOutcomeKindForReason(reason),
    `${reason}: ${entry.filename}`
  )
}

async function saveImageLikeMedia(modelName, folders, entry, kind) {
  const destination = hoghaulMediaSaver.getDestination({
    modelName,
    folders,
    filename: entry.filename,
    kind,
  })
  const { bucket, finalPath, relativePath } = destination

  appendRunEvent(
    'media_seen',
    hoghaulMediaSaver.buildMediaSeenEvent({ modelName, entry, destination })
  )

  const seenMediaMatch = getSuccessfulSeenMediaMatch(
    folders.logDir,
    getEntryMediaPageUrls(entry),
    getEntryMediaUrls(entry)
  )
  if (seenMediaMatch) {
    recordDuplicate(
      entry,
      seenMediaMatch.relativePath,
      'skip_seen_media',
      folders
    )
    console.log(`Seen already: ${entry.filename}`)
    return
  }

  if (existsLocallyOrOnNas(finalPath)) {
    recordDuplicate(entry, relativePath, `skip_existing_${kind}`, folders)
    console.log(`Exists already: ${entry.filename}`)
    return
  }

  const buffer = await downloadMediaBuffer(entry.mediaUrl, entry)
  const hash = createHash('md5').update(buffer).digest('hex')
  const bitwiseMatch = getBitwiseDuplicationRecord(hash)
  if (bitwiseMatch.isDuplicate) {
    recordDuplicate(
      entry,
      bitwiseMatch.activeRefs[0],
      'duplicate_bitwise',
      folders
    )
    console.log(`Bitwise dupe: ${entry.filename}`)
    return
  }

  let visualHash = null
  let visualClaimKey = null
  if (kind === 'image') {
    visualHash = await getVisualHashFromBuffer(buffer)
    const visualMatch = visualHash
      ? getVisualDuplicationRecord(visualHash)
      : null
    if (visualMatch?.isDuplicate) {
      recordDuplicate(
        entry,
        visualMatch.activeRefs[0],
        'duplicate_visual',
        folders
      )
      console.log(`Visual dupe: ${entry.filename}`)
      return
    }

    const fuzzyMatch = visualHash
      ? getFuzzyVisualDuplicationRecord(
          modelName,
          visualHash,
          MAX_FUZZY_IMAGE_VISUAL_DISTANCE
        )
      : null
    if (fuzzyMatch?.isDuplicate) {
      recordDuplicate(
        entry,
        fuzzyMatch.activeRefs[0],
        'duplicate_visual_fuzzy',
        folders,
        {
          visualHash,
          matchedVisualHash: fuzzyMatch.matchedHash,
          distance: fuzzyMatch.distance,
        }
      )
      console.log(
        `Fuzzy visual dupe (${fuzzyMatch.distance}): ${entry.filename}`
      )
      return
    }

    const pendingMatch = visualHash
      ? getPendingImageVisualDuplicate(
          modelName,
          visualHash,
          MAX_FUZZY_IMAGE_VISUAL_DISTANCE
        )
      : null
    if (pendingMatch?.isDuplicate) {
      recordDuplicate(
        entry,
        pendingMatch.activeRefs[0],
        'duplicate_visual_pending',
        folders,
        {
          visualHash,
          matchedVisualHash: pendingMatch.matchedHash,
          distance: pendingMatch.distance,
        }
      )
      console.log(
        `Pending visual dupe (${pendingMatch.distance}): ${entry.filename}`
      )
      return
    }

    visualClaimKey = reservePendingImageVisualClaim(
      modelName,
      relativePath,
      visualHash
    )
  }

  try {
    fs.writeFileSync(finalPath, buffer)
    const { metadata } = await hoghaulMediaSaver.finalizeImage({
      modelName,
      bucket,
      filename: entry.filename,
      buffer,
      absolutePath: finalPath,
      mediaType: kind === 'gif' ? 'gif' : 'image',
      uploadedDate: entry.uploadedDate,
      entry,
    })

    addBitwiseHash(hash, metadata)
    if (visualHash) addVisualHash(visualHash, metadata)
    saveBitwiseHashCache()
    if (visualHash) saveVisualHashCache()

    const stats = hoghaulMediaSaver.buildSavedStats({
      sizeBytes: buffer.length,
      kind,
    })
    successCount += 1
    savedBytes += stats.savedBytes
    if (currentRunLog) {
      currentRunLog.transfer.savedBytes += stats.savedBytes
      currentRunLog.transfer.lazyTransferredBytes += stats.lazyTransferredBytes
    }
    recordSuccessfulSeenMedia(
      folders.logDir,
      hoghaulMediaSaver.buildSeenRecord(entry, destination)
    )
    appendRunEvent(
      destination.savedEventType,
      hoghaulMediaSaver.buildSavedEvent({
        modelName,
        entry,
        destination,
        hash,
        visualHash,
      })
    )
    noteMediaOutcome('saved', `${destination.savedOutcome}: ${entry.filename}`)
    console.log(`Saved ${kind}: ${entry.filename}`)
  } finally {
    releasePendingImageVisualClaim(visualClaimKey)
  }
}

async function saveVideoMedia(modelName, folders, entry) {
  const destination = hoghaulMediaSaver.getDestination({
    modelName,
    filename: entry.filename,
    folders,
    kind: 'video',
  })
  const { finalPath, tmpPath, relativePath } = destination

  appendRunEvent(
    'media_seen',
    hoghaulMediaSaver.buildMediaSeenEvent({ modelName, entry, destination })
  )

  const seenMediaMatch = getSuccessfulSeenMediaMatch(
    folders.logDir,
    getEntryMediaPageUrls(entry),
    getEntryMediaUrls(entry)
  )
  if (seenMediaMatch) {
    recordDuplicate(
      entry,
      seenMediaMatch.relativePath,
      'skip_seen_media',
      folders
    )
    console.log(`Seen already: ${entry.filename}`)
    return
  }

  if (existsLocallyOrOnNas(finalPath)) {
    recordDuplicate(entry, relativePath, 'skip_lazy_existing', folders)
    console.log(`Exists already: ${entry.filename}`)
    return
  }

  queuedVideoCount += 1
  if (currentRunLog) currentRunLog.counters.queuedVideos += 1
  appendRunEvent(
    'queued_lazy_video',
    hoghaulMediaSaver.buildQueuedEvent({ modelName, entry, destination })
  )

  try {
    removeFileIfExists(tmpPath)
    const buffer = await downloadMediaBuffer(entry.mediaUrl, entry)
    fs.writeFileSync(tmpPath, buffer)
    const hash = await hashFileFromPath(tmpPath)
    const bitwiseMatch = getBitwiseDuplicationRecord(hash)
    if (bitwiseMatch.isDuplicate) {
      recordDuplicate(
        entry,
        bitwiseMatch.activeRefs[0],
        'duplicate_bitwise',
        folders
      )
      removeFileIfExists(tmpPath)
      console.log(`Bitwise dupe: ${entry.filename}`)
      return
    }

    moveFileIntoPlace(tmpPath, finalPath)
    const stat = fs.statSync(finalPath)
    const { metadata } = await hoghaulMediaSaver.finalizeVideo({
      modelName,
      bucket: 'webm',
      filename: entry.filename,
      filePath: finalPath,
      mediaType: 'video',
      sizeBytes: stat.size,
      uploadedDate: entry.uploadedDate,
      entry,
    })
    let visualHash = await getVisualHashFromVideoPath(finalPath)
    const visualMatch = visualHash
      ? getVisualDuplicationRecord(visualHash)
      : null
    if (visualMatch?.isDuplicate) {
      recordDuplicate(
        entry,
        visualMatch.activeRefs[0],
        'duplicate_visual',
        folders
      )
      removeFileIfExists(finalPath)
      console.log(`Visual dupe: ${entry.filename}`)
      return
    }

    addBitwiseHash(hash, metadata)
    if (visualHash) addVisualHash(visualHash, metadata)
    saveBitwiseHashCache()
    saveVisualHashCache()

    const stats = hoghaulMediaSaver.buildSavedStats({
      sizeBytes: stat.size,
      kind: 'video',
    })
    successCount += 1
    savedBytes += stats.savedBytes
    if (currentRunLog) {
      currentRunLog.transfer.savedBytes += stats.savedBytes
      currentRunLog.transfer.lazyTransferredBytes += stats.lazyTransferredBytes
    }
    recordSuccessfulSeenMedia(
      folders.logDir,
      hoghaulMediaSaver.buildSeenRecord(entry, destination)
    )
    appendRunEvent(
      destination.savedEventType,
      hoghaulMediaSaver.buildSavedEvent({
        modelName,
        entry,
        destination,
        hash,
        visualHash,
      })
    )
    noteMediaOutcome('saved', `${destination.savedOutcome}: ${entry.filename}`)
    console.log(`Saved video: ${entry.filename}`)
  } catch (err) {
    removeFileIfExists(tmpPath)
    errorCount += 1
    recordRunError(
      'lazy_video_error',
      hoghaulMediaSaver.buildErrorEvent({
        modelName,
        entry,
        destination,
        error: err,
      })
    )
    appendRunEvent(
      'lazy_video_error',
      hoghaulMediaSaver.buildErrorEvent({
        modelName,
        entry,
        destination,
        error: err,
      })
    )
    noteMediaOutcome('failed', `video_error: ${entry.filename}`)
    console.log(`Failed video: ${entry.filename} - ${err.message}`)
  }
}

function hashFileFromPath(filePath) {
  return new Promise((resolve, reject) => {
    const hash = createHash('md5')
    const stream = fs.createReadStream(filePath)
    stream.on('error', reject)
    stream.on('data', (chunk) => hash.update(chunk))
    stream.on('end', () => resolve(hash.digest('hex')))
  })
}

function syncToNAS(modelName) {
  return new Promise((resolve) => {
    const cmd = `robocopy "%APPDATA%\\.slopvault\\dataset\\${modelName}" "Z:\\dataset\\${modelName}" /MIR /R:2 /W:5`
    exec(cmd, (error, stdout, stderr) => {
      const code = error?.code ?? 0
      if (code > 3) {
        console.error(`NAS sync failed with code ${code}: ${stderr || stdout}`)
        resolve(false)
      } else {
        console.log('NAS sync complete.')
        resolve(true)
      }
    })
  })
}

async function run() {
  const argv = minimist(process.argv.slice(2), {
    string: [
      'pages',
      'model',
      'max-posts',
      'max-files',
      'cookie',
      'cookie-file',
      'browser-executable',
      'browser-profile',
      'browser-connect',
      'browser-validate-ms',
      'post-concurrency',
      'image-concurrency',
      'video-concurrency',
    ],
    boolean: [
      'dry-run',
      'preflight',
      'skip-nas-sync',
      'track-source',
      'keep-history',
      'browser-media',
      'browser-headless',
      'headless',
    ],
    alias: {
      model: 'm',
    },
    default: {
      'browser-media': true,
    },
  })
  const inputUrl = argv._.find((arg) => /^https?:\/\//i.test(arg))
  const dryRun =
    argv['dry-run'] === true || isTruthyFlag(process.env.npm_config_dry_run)
  const preflight =
    argv.preflight === true || isTruthyFlag(process.env.npm_config_preflight)
  const skipNasSync =
    argv['skip-nas-sync'] === true ||
    isTruthyFlag(process.env.npm_config_skip_nas_sync)
  const trackSource =
    argv['track-source'] === true ||
    isTruthyFlag(process.env.npm_config_track_source)
  const keepHistory =
    argv['keep-history'] === true ||
    isTruthyFlag(process.env.npm_config_keep_history)
  let useBrowserMedia =
    argv['browser-media'] !== false &&
    !isTruthyFlag(process.env.npm_config_no_browser_media)
  const browserHeadless =
    argv.headless === true ||
    argv['browser-headless'] === true ||
    isTruthyFlag(process.env.npm_config_headless) ||
    isTruthyFlag(process.env.HOGHAUL_BROWSER_HEADLESS)
  const browserOptions = {
    browserExecutable:
      argv['browser-executable'] ||
      process.env.npm_config_browser_executable ||
      process.env.HOGHAUL_BROWSER_EXECUTABLE,
    browserProfile:
      argv['browser-profile'] ||
      process.env.npm_config_browser_profile ||
      process.env.HOGHAUL_BROWSER_PROFILE,
    browserConnect:
      argv['browser-connect'] ||
      process.env.npm_config_browser_connect ||
      process.env.HOGHAUL_BROWSER_CONNECT,
    cookieHeader:
      argv.cookie ||
      process.env.npm_config_cookie ||
      process.env.HOGHAUL_COOKIE,
    cookieFile:
      argv['cookie-file'] ||
      process.env.npm_config_cookie_file ||
      process.env.HOGHAUL_COOKIE_FILE,
    headless: browserHeadless,
    timeoutMs: REQUEST_TIMEOUT_MS,
    validateMs:
      Number.parseInt(
        argv['browser-validate-ms'] ||
          process.env.npm_config_browser_validate_ms ||
          process.env.HOGHAUL_BROWSER_VALIDATE_MS ||
          '0',
        10
      ) || 0,
  }
  if (!inputUrl) {
    console.error(
      'Usage: npm run hoghaul -- "<coomer-kemono-or-reddit-user-url>" [--pages=1 or 1-3] [--model=name] [--preflight] [--dry-run] [--track-source] [--skip-nas-sync] [--cookie-file=cookies.json] [--browser-profile=path] [--browser-connect=http://127.0.0.1:9222] [--browser-validate-ms=60000] [--post-concurrency=8] [--image-concurrency=3] [--video-concurrency=2]'
    )
    process.exitCode = 1
    return
  }

  loadBitwiseHashCache()
  loadVisualHashCache()

  const source = parseSourceUrl(inputUrl)
  if (source.site === 'coomerfans' || source.site === 'reddit') {
    useBrowserMedia = false
  }
  const imageConcurrency = parsePositiveInteger(
    argv['image-concurrency'] ||
      process.env.npm_config_image_concurrency ||
      process.env.HOGHAUL_IMAGE_CONCURRENCY,
    source.site === 'coomerfans' ? 3 : 6
  )
  const postConcurrency = parsePositiveInteger(
    argv['post-concurrency'] ||
      process.env.npm_config_post_concurrency ||
      process.env.HOGHAUL_POST_CONCURRENCY,
    source.site === 'coomerfans' ? 8 : 1
  )
  const videoConcurrency = parsePositiveInteger(
    argv['video-concurrency'] ||
      process.env.npm_config_video_concurrency ||
      process.env.HOGHAUL_VIDEO_CONCURRENCY,
    6
  )
  const { startPage, endPage } = parsePageRange(
    argv.pages || process.env.npm_config_pages
  )
  const maxPosts = Number.parseInt(
    argv['max-posts'] || process.env.npm_config_max_posts,
    10
  )

  if (preflight) {
    let report
    try {
      report = await preflightSourceJson(source, startPage)
    } catch (err) {
      if (source.site !== 'kemono' || /^\d+$/.test(source.userId)) throw err
      await resolveKemonoCreatorIdForJson(source)
      report = await preflightSourceJson(source, startPage)
    }
    console.log(`JSON preflight OK: ${report.apiUrl}`)
    console.log(
      `Downloaded ${report.byteLength} bytes; parsed ${report.postCount} posts.`
    )
    console.log(`First post: ${report.firstPostId || 'unknown'}`)
    console.log(
      `Newest post: ${report.newest ? report.newest.toISOString() : 'unknown'}`
    )
    if (trackSource) {
      registerSourceForRun(
        source,
        inputUrl,
        argv.model || process.env.npm_config_model
      )
    }
    console.log('No API key or Authorization header was used.')
    return
  }

  const posts = await fetchPosts(source, {
    startPage,
    endPage,
    maxPosts,
    postConcurrency,
  })
  const selectedPosts =
    Number.isFinite(maxPosts) && maxPosts > 0 ? posts.slice(0, maxPosts) : posts
  const mediaEntries = normalizeMediaEntries(
    selectedPosts.flatMap((post) => getMediaEntriesFromPost(source, post)),
    {
      sourceSite: source.site,
      sourceService: source.service,
      sourceUserId: source.userId,
      sourceUsername: source.username,
    }
  )
  const sourceDeduped = dedupeMediaEntries(mediaEntries)
  let selectedMediaSourceDuplicateCount = sourceDeduped.duplicateCount
  const maxFiles = Number.parseInt(
    argv['max-files'] || process.env.npm_config_max_files,
    10
  )
  let selectedMedia =
    Number.isFinite(maxFiles) && maxFiles > 0
      ? sourceDeduped.entries.slice(0, maxFiles)
      : sourceDeduped.entries

  if (selectedPosts.length === 0) {
    throw new Error(`No posts found for ${inputUrl}`)
  }

  if (dryRun) {
    const newest = selectedPosts
      .map((post) => parseResolvedDate(post.published))
      .filter(Boolean)
      .sort((a, b) => b.getTime() - a.getTime())[0]
    const modelNamePreview = trackSource
      ? registerSourceForRun(
          source,
          inputUrl,
          argv.model || process.env.npm_config_model
        )
      : sanitize(argv.model || source.rawName)
    console.log(
      `Resolved ${source.site}/${source.service}/${source.userId} -> ${modelNamePreview}: ${selectedPosts.length} posts, ${selectedMedia.length} media files`
    )
    if (selectedMediaSourceDuplicateCount > 0) {
      console.log(
        `Dry run source media dedupe: ${selectedMediaSourceDuplicateCount} repeated media URL(s)`
      )
    }
    printMediaSample(selectedMedia)
    console.log(
      `Dry run only. Newest post: ${newest ? newest.toISOString() : 'unknown'}`
    )
    return
  }

  const modelName = registerSourceForRun(
    source,
    inputUrl,
    argv.model || process.env.npm_config_model
  )
  const folders = createModelFolders(modelName)

  console.log(
    `Resolved ${source.site}/${source.service}/${source.userId} -> ${modelName}: ${selectedPosts.length} posts, ${selectedMedia.length} media files`
  )
  if (selectedMediaSourceDuplicateCount > 0) {
    console.log(
      `Source media dedupe: skipped ${selectedMediaSourceDuplicateCount} repeated media URL(s)`
    )
  }
  console.log(
    `Download concurrency: ${imageConcurrency} image/gif, ${videoConcurrency} video`
  )
  console.log(`Post fetch concurrency: ${postConcurrency}`)

  startRunLog(modelName, inputUrl, folders, keepHistory)
  appendRunEvent('source_posts_loaded', {
    modelName,
    site: source.site,
    service: source.service,
    userId: source.userId,
    postCount: selectedPosts.length,
    mediaCount: selectedMedia.length,
    sourceDuplicateMediaCount: selectedMediaSourceDuplicateCount,
  })
  if (selectedMediaSourceDuplicateCount > 0) {
    appendRunEvent('source_media_deduped', {
      duplicateCount: selectedMediaSourceDuplicateCount,
    })
  }

  setExpectedMediaCount(selectedMedia.length)

  if (useBrowserMedia) {
    browserMediaDownloader = await createSharedBrowserMediaDownloader(source, {
      ...browserOptions,
      slopvaultRoot,
      requestBuffer,
      appendRunEvent,
    })
    appendRunEvent('browser_media_enabled', {
      browserExecutable: browserOptions.browserExecutable || null,
      browserProfile:
        browserOptions.browserProfile ||
        getDefaultBrowserProfileDir(slopvaultRoot, source.site),
      browserConnect: browserOptions.browserConnect || null,
      headless: browserOptions.headless,
      cookieFile: browserOptions.cookieFile || null,
      hasCookieHeader: Boolean(browserOptions.cookieHeader),
      validateMs: browserOptions.validateMs,
    })
    selectedMedia = await enrichMediaEntriesFromBrowserDom(
      selectedMedia,
      browserMediaDownloader
    )
    selectedMedia = normalizeMediaEntries(selectedMedia, {
      sourceSite: source.site,
      sourceService: source.service,
      sourceUserId: source.userId,
      sourceUsername: source.username,
    })
    const browserDeduped = dedupeMediaEntries(selectedMedia)
    if (browserDeduped.duplicateCount > 0) {
      selectedMedia = browserDeduped.entries
      selectedMediaSourceDuplicateCount += browserDeduped.duplicateCount
      appendRunEvent('source_media_deduped_after_browser', {
        duplicateCount: browserDeduped.duplicateCount,
        mediaCount: selectedMedia.length,
      })
      console.log(
        `Browser media dedupe: skipped ${browserDeduped.duplicateCount} repeated media URL(s)`
      )
    }
    setExpectedMediaCount(selectedMedia.length)
  } else {
    console.log(
      'Browser media mode disabled; using direct HTTP media requests.'
    )
  }

  const imageLimit = pLimit(imageConcurrency)
  const videoLimit = pLimit(videoConcurrency)
  const imageLike = []
  const videos = []

  for (const entry of selectedMedia) {
    const normalizedEntry = normalizeMediaEntry(entry, {
      sourceSite: source.site,
      sourceService: source.service,
      sourceUserId: source.userId,
      sourceUsername: source.username,
    })
    if (!normalizedEntry) {
      continue
    }
    const classification = classifyMedia(normalizedEntry.filename)
    if (classification.kind === 'unknown') {
      appendRunEvent('skip_unknown_media', {
        modelName,
        filename: normalizedEntry.filename,
        mediaUrl: normalizedEntry.mediaUrl,
        mediaPageUrl: normalizedEntry.mediaPageUrl,
        ...getEntrySourceDetails(normalizedEntry),
        extension: classification.ext,
      })
      noteMediaOutcome(
        'skipped',
        `skip_unknown_media: ${normalizedEntry.filename}`
      )
      continue
    }
    if (classification.kind === 'video') {
      videos.push(normalizedEntry)
    } else {
      imageLike.push({ ...normalizedEntry, kind: classification.kind })
    }
  }

  console.log(
    `Media buckets: ${imageLike.length} image/gif, ${videos.length} video`
  )

  console.log(`Lazy downloading videos: ${videos.length}`)
  await Promise.all(
    videos.map((entry) =>
      videoLimit(() => saveVideoMedia(modelName, folders, entry))
    )
  )

  await Promise.all(
    imageLike.map((entry) =>
      imageLimit(async () => {
        try {
          await saveImageLikeMedia(modelName, folders, entry, entry.kind)
        } catch (err) {
          errorCount += 1
          recordRunError('media_error', {
            modelName,
            filename: entry.filename,
            mediaUrl: entry.mediaUrl,
            mediaPageUrl: entry.mediaPageUrl,
            ...getEntrySourceDetails(entry),
            error: err.message,
          })
          appendRunEvent('media_error', {
            modelName,
            filename: entry.filename,
            mediaUrl: entry.mediaUrl,
            mediaPageUrl: entry.mediaPageUrl,
            ...getEntrySourceDetails(entry),
            error: err.message,
          })
          noteMediaOutcome('failed', `media_error: ${entry.filename}`)
          console.log(`Failed media: ${entry.filename} - ${err.message}`)
        }
      })
    )
  )

  appendRunEvent('run_finished', {
    successCount,
    duplicateCount,
    errorCount,
    queuedVideoCount,
    savedBytes,
    postCount: selectedPosts.length,
    mediaCount: selectedMedia.length,
    sourceDuplicateMediaCount: selectedMediaSourceDuplicateCount,
  })

  saveBitwiseHashCache()
  saveVisualHashCache()

  if (skipNasSync) {
    console.log('NAS sync skipped by --skip-nas-sync')
  } else {
    const nasSyncOk = await syncToNAS(modelName)
    if (nasSyncOk) {
      mergeNasMp4Entries(
        collectMp4RelativePaths(path.join(datasetDir, modelName), datasetDir),
        datasetDir
      )
      syncNasMp4IndexToMirror('Z:\\dataset', datasetDir)
    }
  }
  const runCounters = currentRunLog
    ? {
        processed: currentRunLog.counters.processed || 0,
        expectedMedia: currentRunLog.counters.expectedMedia || 0,
        saved: currentRunLog.counters.saved || 0,
        skipped: currentRunLog.counters.skipped || 0,
        duplicates: currentRunLog.counters.duplicates || 0,
        failures: currentRunLog.counters.failures || 0,
      }
    : null
  finalizeRunLog({
    successCount,
    duplicateCount,
    errorCount,
    queuedVideoCount,
    savedBytes,
    postCount: selectedPosts.length,
    mediaCount: selectedMedia.length,
    sourceDuplicateMediaCount: selectedMediaSourceDuplicateCount,
  })

  console.log(
    `Done: ${runCounters?.processed ?? selectedMedia.length}/${runCounters?.expectedMedia ?? selectedMedia.length} processed | saved ${runCounters?.saved ?? successCount} | skipped ${runCounters?.skipped ?? 0} | dupes ${runCounters?.duplicates ?? duplicateCount} | failed ${runCounters?.failures ?? errorCount}`
  )
  console.log(getCompletionLine())
}

run()
  .catch((err) => {
    finalizeAbortedRun('failed', err)
    console.error(`Hoghaul failed: ${err.message}`)
    process.exitCode = 1
  })
  .finally(() => {
    return closeBrowserMediaDownloader()
      .catch((err) => {
        console.warn(`Browser close warning: ${err.message}`)
      })
      .finally(() => {
        mediaDates.flushAllSidecars()
      })
  })

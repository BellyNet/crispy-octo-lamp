'use strict'

const fs = require('fs')
const path = require('path')
const { createHash } = require('crypto')
const pLimit = require('p-limit')

const { bannerHoghaul } = require('../banners.js')
const mediaDates = require('../scrapyard/mediaDates')
const { createDatasetPaths } = require('../scrapyard/datasetPaths')
const { createMediaSeenIndex } = require('../scrapyard/mediaSeenIndex')
const { syncModelToNas } = require('../scrapyard/nasSync')
const runLifecycle = require('../scrapyard/runLifecycle')
const {
  parseHoghaulSourceUrl: parseSourceUrl,
} = require('../scrapyard/sourceRouter')
const {
  sanitize,
  resolveAndTrackSourceModel,
} = require('../scrapyard/modelRegistry')
const {
  normalizeHoghaulRunOptions,
  parseHoghaulArgs,
} = require('../scrapyard/scraperOptions')
const {
  classifyMediaFilename,
  getMediaEntryHashMetadata,
  getMediaEntryPageUrls,
  getMediaEntrySeenDetails,
  getMediaEntrySourceDetails,
  getMediaEntryUrls,
  normalizeMediaEntry,
  normalizeMediaEntries,
} = require('../scrapyard/mediaEntries')
const mediaFileRecords = require('../scrapyard/mediaFileRecords')
const { createMediaSaver } = require('../scrapyard/mediaSaver')
const { createMediaSavePipeline } = require('../scrapyard/mediaSavePipeline')
const { createDuplicateChecker } = require('../scrapyard/duplicateChecker')
const {
  moveFileIntoPlace,
  removeFileIfExists,
} = require('../scrapyard/fileOps')
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
  fetchRedditPosts: fetchRedditAdapterPosts,
  preflightRedditSource: preflightRedditAdapterSource,
} = require('../scrapyard/sourceAdapters/reddit')
const {
  fetchCoomerFansPosts: fetchCoomerFansAdapterPosts,
  preflightCoomerFansSource,
} = require('../scrapyard/sourceAdapters/coomerFans')
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
const {
  getCompletionLine,
  logProgress,
  logScrollingMessage,
  resetProgressBar,
} = require('../stuffinglogger')

const datasetPaths = createDatasetPaths({
  rootDir: path.join(__dirname, '..'),
  repairCanUseNasMirror: true,
})
const rootDir = datasetPaths.rootDir
const datasetDir = datasetPaths.datasetDir
const nasDatasetDir = datasetPaths.nasDatasetDir
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
const hoghaulSavePipeline = createMediaSavePipeline({
  mediaSaver: hoghaulMediaSaver,
  appendRunEvent,
  recordSuccessfulSeenMedia,
  getSuccessfulSeenMediaMatch,
  existsLocallyOrOnNas,
  onDuplicate: () => {
    duplicateCount += 1
  },
  onSaved: ({ stats }) => {
    successCount += 1
    savedBytes += stats.savedBytes
    runLifecycle.addRunTransfer(currentRunLog, 'savedBytes', stats.savedBytes)
    runLifecycle.addRunTransfer(
      currentRunLog,
      'lazyTransferredBytes',
      stats.lazyTransferredBytes
    )
  },
  onQueued: () => {
    queuedVideoCount += 1
    runLifecycle.incrementRunCounter(currentRunLog, 'queuedVideos')
  },
  onOutcome: ({ kind, label }) => {
    noteMediaOutcome(kind, label)
  },
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
let processTerminationHandlersInstalled = false

function resetRunState() {
  currentRunLog = null
  successCount = 0
  duplicateCount = 0
  errorCount = 0
  queuedVideoCount = 0
  savedBytes = 0
  browserMediaDownloader = null
  runTerminationHandled = false
}

function registerSourceForRun(source, inputUrl, canonicalOverride) {
  const modelName = resolveAndTrackSourceModel(
    registryPath,
    source.rawName,
    {
      ...source,
      inputUrl,
    },
    canonicalOverride,
    { unknownName: 'unknown_model' }
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

function startRunLog(modelName, inputUrl, folders, keepHistory) {
  runTerminationHandled = false
  currentRunLog = runLifecycle.createRunLog({
    source: 'hoghaul',
    modelName,
    inputUrl,
    folders,
    keepHistory,
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
    removeFileIfExists,
  })
}

function appendRunEvent(type, payload = {}) {
  runLifecycle.appendRunEvent(currentRunLog, type, payload)
}

function recordRunError(category, details = {}) {
  runLifecycle.recordRunError(currentRunLog, category, details)
}

function finalizeRunLog(extra = {}) {
  currentRunLog = runLifecycle.finalizeRunLog(currentRunLog, extra, {
    removeFileIfExists,
    summaryTrailingNewline: true,
  })
}

function setExpectedMediaCount(total) {
  runLifecycle.setRunCounter(
    currentRunLog,
    'expectedMedia',
    Number.isFinite(total) && total >= 0 ? total : 0
  )
  resetProgressBar(null, 'scrape')
  logRunProgress()
}

function logRunProgress(context = '') {
  if (!currentRunLog) return

  const stats = runLifecycle.getRunProgressStats(currentRunLog)
  const bottomText = [
    `processed ${stats.processed}/${stats.expectedMedia}`,
    `saved ${stats.saved}`,
    `skipped ${stats.skipped}`,
    `dupes ${stats.duplicates}`,
    `failed ${stats.failures}`,
    `remaining ${stats.remaining}`,
  ].join(' | ')

  if (context) logScrollingMessage(context)
  logProgress(stats.processed, Math.max(stats.expectedMedia, 1), {
    bottomText,
  })
}

function noteMediaOutcome(kind, context = '') {
  if (!currentRunLog) return

  runLifecycle.noteMediaOutcome(currentRunLog, kind)

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
  if (processTerminationHandlersInstalled) return
  processTerminationHandlersInstalled = true

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
  return getMediaEntryUrls(entry, { normalizeUrl: normalizeSeenUrl })
}

function getEntryMediaPageUrls(entry) {
  return getMediaEntryPageUrls(entry, { normalizeUrl: normalizeSeenUrl })
}

function getEntrySeenDetails(entry) {
  return getMediaEntrySeenDetails(entry, { normalizeUrl: normalizeSeenUrl })
}

function getEntrySourceDetails(entry) {
  return getMediaEntrySourceDetails(entry)
}

function getEntryHashMetadata(entry = {}) {
  return getMediaEntryHashMetadata(entry)
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

function filenameFromMediaUrl(mediaUrl) {
  try {
    const name = decodeURIComponent(path.basename(new URL(mediaUrl).pathname))
    return name && name !== 'data' ? name : null
  } catch {
    return null
  }
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

function htmlDecode(value) {
  return String(value || '')
    .replace(/&amp;/g, '&')
    .replace(/&#43;/g, '+')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
}

async function preflightSourceJson(source, page = 0) {
  if (source.site === 'reddit') {
    return preflightRedditAdapterSource(source, {
      fetchJson,
      pageSize: REDDIT_PAGE_SIZE,
    })
  }
  if (source.site === 'coomerfans') {
    return preflightCoomerFansSource(source, page, {
      fetchHtml,
      logger: console,
    })
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

async function fetchPosts(source, options) {
  if (source.site === 'coomerfans') {
    return fetchCoomerFansAdapterPosts(source, options, {
      fetchHtml,
      logger: console,
    })
  }
  if (source.site === 'reddit') {
    return fetchRedditAdapterPosts(source, options, {
      fetchJson,
      logger: console,
      normalizeUrl: normalizeSeenUrl,
      pageSize: REDDIT_PAGE_SIZE,
      redgifsClient,
    })
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
  hoghaulSavePipeline.recordDuplicate({
    folders,
    entry,
    destination: {
      relativePath: savedPath,
      filename: entry.filename,
    },
    reason,
    extra: extra && typeof extra === 'object' ? extra : {},
    savedPath,
    recordSeen: true,
  })
}

async function saveImageLikeMedia(modelName, folders, entry, kind) {
  const destination = hoghaulSavePipeline.getDestination({
    modelName,
    folders,
    entry,
    kind,
  })

  hoghaulSavePipeline.recordMediaSeen({ modelName, entry, destination })

  const seenMediaMatch = hoghaulSavePipeline.getSeenMediaMatch(folders, entry)
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

  const result = await hoghaulSavePipeline.saveImageLikeMedia({
    modelName,
    folders,
    entry,
    destination,
    kind,
    downloadBuffer: downloadMediaBuffer,
    getBitwiseDuplicationRecord,
    getVisualHashFromBuffer,
    getVisualDuplicationRecord,
    getFuzzyVisualDuplicationRecord,
    getPendingImageVisualDuplicate,
    reservePendingImageVisualClaim,
    releasePendingImageVisualClaim,
    addBitwiseHash,
    addVisualHash,
    saveBitwiseHashCache,
    saveVisualHashCache,
    duplicateRecordSeen: true,
    visualChecks: kind === 'image',
    fuzzyVisualDistance: MAX_FUZZY_IMAGE_VISUAL_DISTANCE,
    pendingVisualDistance: MAX_FUZZY_IMAGE_VISUAL_DISTANCE,
    saveVisualHashCacheOnSave: true,
  })

  if (result.reason?.startsWith('skip_existing_')) {
    logScrollingMessage(`Exists already: ${entry.filename}`)
    return
  }
  if (result.reason === 'duplicate_bitwise') {
    logScrollingMessage(`Bitwise dupe: ${entry.filename}`)
    return
  }
  if (result.reason === 'duplicate_visual') {
    logScrollingMessage(`Visual dupe: ${entry.filename}`)
    return
  }
  if (result.reason === 'duplicate_visual_fuzzy') {
    logScrollingMessage(
      `Fuzzy visual dupe (${result.match.distance}): ${entry.filename}`
    )
    return
  }
  if (result.reason === 'duplicate_visual_pending') {
    logScrollingMessage(
      `Pending visual dupe (${result.match.distance}): ${entry.filename}`
    )
    return
  }

  logScrollingMessage(`Saved ${kind}: ${entry.filename}`)
}

async function saveVideoMedia(modelName, folders, entry) {
  const destination = hoghaulSavePipeline.getDestination({
    modelName,
    folders,
    entry,
    kind: 'video',
  })
  const { finalPath, tmpPath, relativePath } = destination

  hoghaulSavePipeline.recordMediaSeen({ modelName, entry, destination })

  const seenMediaMatch = hoghaulSavePipeline.getSeenMediaMatch(folders, entry)
  if (seenMediaMatch) {
    recordDuplicate(
      entry,
      seenMediaMatch.relativePath,
      'skip_seen_media',
      folders
    )
    logScrollingMessage(`Seen already: ${entry.filename}`)
    return
  }

  if (hoghaulSavePipeline.isKnownOrExisting(destination, entry)) {
    recordDuplicate(entry, relativePath, 'skip_lazy_existing', folders)
    logScrollingMessage(`Exists already: ${entry.filename}`)
    return
  }

  hoghaulSavePipeline.queueVideo({
    modelName,
    folders,
    entry,
    destination,
  })

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
      logScrollingMessage(`Bitwise dupe: ${entry.filename}`)
      return
    }

    const result = await hoghaulSavePipeline.finalizeVideoFile({
      modelName,
      folders,
      entry,
      destination,
      sourcePath: tmpPath,
      moveFileIntoPlace,
      hash,
      getVisualHashFromVideoPath,
      getVisualDuplicationRecord,
      addBitwiseHash,
      addVisualHash,
      saveBitwiseHashCache,
      saveVisualHashCache,
      removeFileIfExists,
      checkVisualDuplicate: true,
      duplicateRecordSeen: true,
    })

    if (result.reason === 'duplicate_visual') {
      logScrollingMessage(`Visual dupe: ${entry.filename}`)
      return
    }
    logScrollingMessage(`Saved video: ${entry.filename}`)
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
    logScrollingMessage(`Failed video: ${entry.filename} - ${err.message}`)
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

async function run(argvInput = process.argv.slice(2)) {
  resetRunState()
  bannerHoghaul()
  installProcessTerminationHandlers()

  const runOptions = normalizeHoghaulRunOptions(argvInput, {
    requestTimeoutMs: REQUEST_TIMEOUT_MS,
  })
  const {
    inputUrl,
    model,
    dryRun,
    preflight,
    skipNasSync,
    trackSource,
    keepHistory,
    browserOptions,
  } = runOptions
  let useBrowserMedia = runOptions.useBrowserMedia
  if (!inputUrl) {
    console.error(
      'Usage: npm run hoghaul -- "<coomer-kemono-or-reddit-user-url>" [--pages=1 or 1-3] [--model=name] [--preflight] [--dry-run] [--track-source] [--skip-nas-sync] [--cookie-file=cookies.json] [--browser-profile=path] [--browser-connect=http://127.0.0.1:9222] [--browser-validate-ms=60000] [--post-concurrency=8] [--image-concurrency=3] [--video-concurrency=2]'
    )
    return 1
  }

  loadBitwiseHashCache()
  loadVisualHashCache()

  const source = parseSourceUrl(inputUrl)
  if (source.site === 'coomerfans' || source.site === 'reddit') {
    useBrowserMedia = false
  }
  const imageConcurrency = parsePositiveInteger(
    runOptions.imageConcurrency || process.env.HOGHAUL_IMAGE_CONCURRENCY,
    source.site === 'coomerfans' ? 3 : 6
  )
  const postConcurrency = parsePositiveInteger(
    runOptions.postConcurrency || process.env.HOGHAUL_POST_CONCURRENCY,
    source.site === 'coomerfans' ? 8 : 1
  )
  const videoConcurrency = parsePositiveInteger(
    runOptions.videoConcurrency || process.env.HOGHAUL_VIDEO_CONCURRENCY,
    6
  )
  const { startPage, endPage } = parsePageRange(runOptions.pages)
  const maxPosts = Number.parseInt(runOptions.maxPosts, 10)

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
      registerSourceForRun(source, inputUrl, model)
    }
    console.log('No API key or Authorization header was used.')
    return 0
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
  const maxFiles = Number.parseInt(runOptions.maxFiles, 10)
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
      ? registerSourceForRun(source, inputUrl, model)
      : sanitize(model || source.rawName)
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
    return 0
  }

  const modelName = registerSourceForRun(source, inputUrl, model)
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
      logScrollingMessage(
        `Browser media dedupe: skipped ${browserDeduped.duplicateCount} repeated media URL(s)`
      )
    }
    setExpectedMediaCount(selectedMedia.length)
  } else {
    logScrollingMessage(
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

  logScrollingMessage(
    `Media buckets: ${imageLike.length} image/gif, ${videos.length} video`
  )

  logScrollingMessage(`Lazy downloading videos: ${videos.length}`)
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
    await syncModelToNas({ modelName, datasetDir, nasDatasetDir })
  }
  const runStats = runLifecycle.getRunProgressStats(currentRunLog, {
    processed: selectedMedia.length,
    expectedMedia: selectedMedia.length,
    saved: successCount,
    duplicates: duplicateCount,
    failures: errorCount,
  })
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

  logScrollingMessage(runLifecycle.formatRunSummaryLine(runStats))
  logRunProgress()
  console.log(getCompletionLine())
  return 0
}

async function runHoghaulCli(argvInput = process.argv.slice(2)) {
  try {
    return await run(argvInput)
  } catch (err) {
    finalizeAbortedRun('failed', err)
    console.error(`Hoghaul failed: ${err.message}`)
    return 1
  } finally {
    await closeBrowserMediaDownloader()
      .catch((err) => {
        console.warn(`Browser close warning: ${err.message}`)
      })
      .finally(() => {
        mediaDates.flushAllSidecars()
      })
  }
}

module.exports = {
  normalizeHoghaulRunOptions,
  parseHoghaulArgs,
  parseSourceUrl,
  runHoghaulScrape: run,
  runHoghaulCli,
}

if (require.main === module) {
  const { runScraperCli } = require('../scrapyard/scraperRunner')
  runScraperCli()
    .then((code) => {
      process.exitCode = code
    })
    .catch((err) => {
      console.error(`Scraper runner failed: ${err.stack || err.message}`)
      process.exitCode = 1
    })
}

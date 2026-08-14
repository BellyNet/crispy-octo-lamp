const { executablePath } = require('puppeteer')
const puppeteer = require('puppeteer')
const fs = require('fs')
const path = require('path')
const { exec, spawn } = require('child_process')
const { createHash } = require('crypto')
const https = require('https')
const http = require('http')
const pLimit = require('p-limit')
const limit = pLimit(8)
const lazyLimit = pLimit(4)
const readline = require('readline')
const ansiEscapes = require('ansi-escapes')
const chalk = require('chalk').default
const MEDIA_PAGE_CONCURRENCY = 4
const CATEGORY_PAGE_TIMEOUT_MS = 20000
const CATEGORY_PAGE_RETRY_TIMEOUT_MS = 30000
const MEDIA_PAGE_TIMEOUT_MS = 20000
const MEDIA_PAGE_RETRY_TIMEOUT_MS = 30000
const LAZY_REQUEST_TIMEOUT_MS = 30000
const LAZY_IDLE_TIMEOUT_MS = 30000

const { bannerMilkmaid } = require('../banners.js') // adjust path if needed
const mediaDates = require('../scrapyard/mediaDates')
const {
  normalizeMilkmaidRunOptions,
  parseMilkmaidArgs: parseCliArgs,
} = require('../scrapyard/scraperOptions')

// Helpers
const { createScraperPage } = require('../scrapyard/pageHelpers')
const {
  loadVisualHashCache,
  saveVisualHashCache,
  getVisualHashFromBuffer,
  getVisualHashFromVideoPath,
  isVisualDupe,
  addVisualHash,
  getVisualHashRecord,
} = require('../scrapyard/visualHasher')

const {
  loadBitwiseHashCache,
  saveBitwiseHashCache,
  isBitwiseDupe,
  addBitwiseHash,
  getBitwiseHashRecord,
} = require('../scrapyard/bitwiseHasher')

const {
  logProgress,
  logLazyProgress,
  resetProgressBar,
  getCompletionLine,
  getScrapeLine,
  getStatusHeader,
  getMilestoneLine,
  getMilestoneBucket,
  logScrollingMessage,
} = require('../stuffinglogger')
const { createDatasetPaths } = require('../scrapyard/datasetPaths')
const { createMediaSeenIndex } = require('../scrapyard/mediaSeenIndex')
const { syncModelToNas } = require('../scrapyard/nasSync')
const runLifecycle = require('../scrapyard/runLifecycle')
const mediaFileRecords = require('../scrapyard/mediaFileRecords')
const {
  sanitize,
  loadModelRegistry,
  findCanonicalModelName,
  resolveAndTrackModel: resolveAndTrackRegistryModel,
} = require('../scrapyard/modelRegistry')
const {
  getMediaEntryHashMetadata,
  getMediaEntrySeenDetails,
  getMediaEntrySourceDetails,
} = require('../scrapyard/mediaEntries')
const { createMediaSaver } = require('../scrapyard/mediaSaver')
const { createMediaSavePipeline } = require('../scrapyard/mediaSavePipeline')
const { createDuplicateChecker } = require('../scrapyard/duplicateChecker')
const {
  getSourceCheckpoint,
  recordSourceCheckpoint,
} = require('../scrapyard/sourceFrontier')
const {
  moveFileIntoPlace,
  removeFileIfExists,
} = require('../scrapyard/fileOps')
const {
  buildCategoryRunList: buildStufferDbCategoryRunList,
  collectChildCategoryUrls: collectStufferDbChildCategoryUrls,
  extractGalleryPictureUrls,
  fetchStufferDbMediaEntry,
  fetchStufferDBTotalCount: fetchStufferDbTotalCountFromAdapter,
  getBreadcrumbInfo: getStufferDbBreadcrumbInfo,
  getStufferDbCategoryId,
  getStufferDbPictureId,
  gotoStufferDbWithFallback,
  normalizeStufferDbCategoryUrl,
  normalizeStufferDbPictureUrl,
  withStufferDbNewestFirst,
} = require('../scrapyard/sourceAdapters/stufferdb')

function extractModelNameFromBreadcrumb(anchors) {
  const genericFolderNames = new Set([
    'video',
    'videos',
    'clip',
    'clips',
    'gif',
    'gifs',
    'animation',
    'animations',
    'animated',
    'movies',
    'movie',
    'media',
    'extra',
    'extras',
    'misc',
    'miscellaneous',
  ])

  const cleaned = (anchors || []).map((text) => sanitize(text)).filter(Boolean)

  if (!cleaned.length) return 'unknown_cow'

  const last = cleaned[cleaned.length - 1]
  const prev = cleaned.length > 1 ? cleaned[cleaned.length - 2] : null

  if (genericFolderNames.has(last) && prev) {
    return prev
  }

  return last
}

function askQuestion(prompt) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    })

    rl.question(prompt, (answer) => {
      rl.close()
      resolve(answer)
    })
  })
}

async function promptForModelSelection(registryPath, inferredRawName) {
  const inferredName = sanitize(inferredRawName) || 'unknown_cow'
  const registry = loadModelRegistry(registryPath)
  const inferredCanonical =
    findCanonicalModelName(registry, inferredName) || inferredName

  if (!process.stdin.isTTY || !process.stdout.isTTY) {
    return {
      aliasName: inferredName,
      canonicalName: inferredCanonical,
    }
  }

  const prompt =
    inferredName === 'unknown_cow'
      ? `\nDetected page alias: unknown_cow\nPress Enter or type "y" to keep it, or type "n" to enter alias and bucket separately: `
      : `\nDetected page alias: ${inferredName}\nResolved bucket: ${inferredCanonical}\nPress Enter or type "y" to accept, or type "n" to edit alias and bucket: `

  const rawAnswer = await askQuestion(prompt)
  const normalizedAnswer = sanitize(rawAnswer)

  if (
    !normalizedAnswer ||
    normalizedAnswer === 'y' ||
    normalizedAnswer === 'yes'
  ) {
    return {
      aliasName: inferredName,
      canonicalName: inferredCanonical,
    }
  }

  const aliasAnswer = await askQuestion(`Page alias [${inferredName}]: `)
  const canonicalAnswer = await askQuestion(
    `Save this alias under model [${inferredCanonical}]: `
  )

  return {
    aliasName: sanitize(aliasAnswer) || inferredName,
    canonicalName: sanitize(canonicalAnswer) || inferredCanonical,
  }
}

async function getBreadcrumbInfo(page) {
  return getStufferDbBreadcrumbInfo(page)
}

async function gotoWithTimeoutRetry(
  page,
  url,
  {
    waitUntil = 'domcontentloaded',
    timeoutMs,
    retryTimeoutMs,
    retryDelayMs = 750,
    onRetry = null,
  } = {}
) {
  const useStufferDbFallback =
    /https?:\/\/(?:www\.)?stuffer(?:db|ai)\.com/i.test(String(url || ''))

  try {
    if (useStufferDbFallback) {
      await gotoStufferDbWithFallback(
        page,
        url,
        {
          waitUntil,
          timeout: timeoutMs,
        },
        {
          onFallbackAttempt: (details) => {
            if (typeof onRetry === 'function') onRetry(details.error)
          },
        }
      )
    } else {
      await page.goto(url, {
        waitUntil,
        timeout: timeoutMs,
      })
    }
    return
  } catch (error) {
    if (!/Navigation timeout/i.test(error.message || '')) {
      throw error
    }

    if (typeof onRetry === 'function') {
      onRetry(error)
    }

    await new Promise((resolve) => setTimeout(resolve, retryDelayMs))
    if (useStufferDbFallback) {
      await gotoStufferDbWithFallback(page, url, {
        waitUntil,
        timeout: retryTimeoutMs ?? timeoutMs,
      })
    } else {
      await page.goto(url, {
        waitUntil,
        timeout: retryTimeoutMs ?? timeoutMs,
      })
    }
  }
}

async function collectChildCategoryUrls(browser, parentUrl) {
  return collectStufferDbChildCategoryUrls(browser, parentUrl, {
    createScraperPage,
    gotoWithTimeoutRetry,
    categoryPageTimeoutMs: CATEGORY_PAGE_TIMEOUT_MS,
    categoryPageRetryTimeoutMs: CATEGORY_PAGE_RETRY_TIMEOUT_MS,
    onRetry: (error) => {
      appendRunEvent('child_category_page_retry', {
        parentUrl,
        timeoutMs: CATEGORY_PAGE_RETRY_TIMEOUT_MS,
        reason: error.message,
      })
    },
  })
}

async function buildCategoryRunList(browser, inputUrl) {
  return buildStufferDbCategoryRunList(browser, inputUrl, {
    createScraperPage,
    gotoWithTimeoutRetry,
    categoryPageTimeoutMs: CATEGORY_PAGE_TIMEOUT_MS,
    categoryPageRetryTimeoutMs: CATEGORY_PAGE_RETRY_TIMEOUT_MS,
    onRetry: (error) => {
      appendRunEvent('child_category_page_retry', {
        parentUrl: normalizeStufferDbCategoryUrl(inputUrl),
        timeoutMs: CATEGORY_PAGE_RETRY_TIMEOUT_MS,
        reason: error.message,
      })
    },
  })
}

const sleep = (ms) => new Promise((res) => setTimeout(res, ms))
const randomDelay = () => sleep(Math.floor(Math.random() * 1200) + 300)

const knownFilenames = new Set()
const skippedFilenames = new Set()
const queuedVideos = new Set()

const lazyVideoQueue = []
let totalCount = 0,
  duplicateCount = 0,
  errorCount = 0,
  successCount = 0,
  lastDraw = 0,
  totalLazyBytes = 0,
  lazyBytesDownloaded = 0
let lazyDownloadStartedAt = 0,
  lazyActiveDownloads = 0,
  lazyCompletedDownloads = 0,
  lazyCurrentLabel = ''

const datasetPaths = createDatasetPaths({
  rootDir: path.join(__dirname, '..'),
  repairCanUseNasMirror: false,
})
const rootDir = datasetPaths.rootDir
const slopvaultRoot = datasetPaths.slopvaultRoot
const datasetDir = datasetPaths.datasetDir
const nasDatasetDir = datasetPaths.nasDatasetDir
const milkmaidMediaSaver = createMediaSaver({
  datasetDir,
  source: 'milkmaid',
  mediaDates,
  getExtraMetadata: (entry) => getMilkmaidEntryHashMetadata(entry),
  getEventMetadata: (entry) => getMilkmaidEntrySourceDetails(entry),
  getSeenDetails: (entry) => getMilkmaidEntrySeenDetails(entry),
})
const milkmaidSavePipeline = createMediaSavePipeline({
  mediaSaver: milkmaidMediaSaver,
  appendRunEvent,
  recordSuccessfulSeenMedia,
  getSuccessfulSeenMediaMatch,
  existsLocallyOrOnNas,
  knownFilenames,
  isQuarantinedPath,
  onDuplicate: () => {
    duplicateCount++
    runLifecycle.incrementRunCounter(currentRunLog, 'duplicates')
  },
  onSaved: ({ stats }) => {
    successCount++
    runLifecycle.incrementRunCounter(currentRunLog, 'saved')
    addRunSavedBytes(stats.savedBytes)
  },
  onQueued: () => {
    runLifecycle.incrementRunCounter(currentRunLog, 'queuedVideos')
  },
  onOutcome: ({ reasonCounter }) => {
    recordRunReason(reasonCounter)
  },
})
const duplicateChecker = createDuplicateChecker({
  datasetDir,
  existsLocallyOrOnNas: (filePath) => existsLocallyOrOnNas(filePath),
  getBitwiseHashRecord,
  isBitwiseDupe,
  getVisualHashRecord,
  isVisualDupe,
})
const { getBitwiseDuplicationRecord, getVisualDuplicationRecord } =
  duplicateChecker
const quarantineDatasetDir = datasetPaths.quarantineDatasetDir
const quarantineManifestPath = path.join(
  slopvaultRoot,
  'quarantine',
  'quarantine-manifest.json'
)
const permanentSkipFile = path.join(
  slopvaultRoot,
  'milkmaid-permanent-skips.json'
)
const tmpDir = path.join(rootDir, 'tmp')
let currentRunLog = null
let permanentSkipEntries = []
let permanentSkipLookup = {
  relativePaths: new Set(),
  sourceUrls: new Set(),
  mediaPageUrls: new Set(),
  filenames: new Set(),
}
const sharedMediaSeenIndex = createMediaSeenIndex({
  datasetDir,
  existsLocallyOrOnNas: (filePath) => existsLocallyOrOnNas(filePath),
  normalizeUrl: normalizeSkipUrl,
  matchOrder: ['media_page_url', 'media_url'],
  warn: (message) => console.warn(`Warning: ${message}`),
})

if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true })

function resetRunState() {
  knownFilenames.clear()
  skippedFilenames.clear()
  queuedVideos.clear()
  lazyVideoQueue.length = 0
  totalCount = 0
  duplicateCount = 0
  errorCount = 0
  successCount = 0
  lastDraw = 0
  totalLazyBytes = 0
  lazyBytesDownloaded = 0
  lazyDownloadStartedAt = 0
  lazyActiveDownloads = 0
  lazyCompletedDownloads = 0
  lazyCurrentLabel = ''
  currentRunLog = null
  permanentSkipEntries = []
  permanentSkipLookup = {
    relativePaths: new Set(),
    sourceUrls: new Set(),
    mediaPageUrls: new Set(),
    filenames: new Set(),
  }
}

function addRunSavedBytes(bytes) {
  runLifecycle.addRunTransfer(currentRunLog, 'savedBytes', bytes)
}

function addRunFailedBytes(bytes) {
  runLifecycle.addRunTransfer(currentRunLog, 'failedBytes', bytes)
}

function addRunFailedLazyVideoBytes(bytes) {
  runLifecycle.addRunTransfer(currentRunLog, 'failedLazyVideoBytes', bytes)
}

function recordRunReason(reason, delta = 1) {
  if (!reason) return
  runLifecycle.incrementRunReasonCounter(currentRunLog, reason, delta)
}

function setRunLazyExpectedBytes(bytes) {
  runLifecycle.setRunTransfer(currentRunLog, 'lazyExpectedBytes', bytes)
}

function setRunLazyTransferredBytes(bytes) {
  runLifecycle.setRunTransfer(currentRunLog, 'lazyTransferredBytes', bytes)
}

function getIncompleteDirs(modelName) {
  return datasetPaths.getIncompleteDirs(modelName)
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

function loadQuarantineManifest() {
  if (!fs.existsSync(quarantineManifestPath)) {
    return {
      version: 1,
      updatedAt: null,
      items: [],
    }
  }

  try {
    const raw = fs.readFileSync(quarantineManifestPath, 'utf8').trim()
    const parsed = raw ? JSON.parse(raw) : {}
    return {
      version: parsed?.version || 1,
      updatedAt: parsed?.updatedAt || null,
      items: Array.isArray(parsed?.items) ? parsed.items : [],
    }
  } catch (err) {
    logAndProgress(`⚠️ Could not read quarantine manifest: ${err.message}`)
    return {
      version: 1,
      updatedAt: null,
      items: [],
    }
  }
}

function saveQuarantineManifest(manifest) {
  fs.mkdirSync(path.dirname(quarantineManifestPath), { recursive: true })
  manifest.updatedAt = new Date().toISOString()
  fs.writeFileSync(quarantineManifestPath, JSON.stringify(manifest, null, 2))
}

function updateQuarantineManifestForRepair(filePath, details = {}) {
  const relativePath = getDatasetRelativePath(filePath)
  const quarantinePath = getQuarantineMirrorPath(filePath)
  const manifest = loadQuarantineManifest()
  const item = manifest.items.find(
    (entry) =>
      normalizePath(entry?.relativePath) === normalizePath(relativePath) ||
      entry?.quarantinePath === quarantinePath
  )

  if (!item) {
    return false
  }

  item.state = {
    activeDatasetExists: fs.existsSync(filePath),
    activeDatasetPath: filePath,
    quarantineExists: fs.existsSync(quarantinePath),
    repairState:
      fs.existsSync(filePath) && !fs.existsSync(quarantinePath)
        ? 'repaired'
        : fs.existsSync(filePath) && fs.existsSync(quarantinePath)
          ? 'replacement_present_pending_review'
          : !fs.existsSync(filePath) && !fs.existsSync(quarantinePath)
            ? 'missing_both'
            : 'quarantined',
  }

  item.repair = {
    ...(item.repair || {}),
    repairedAt: new Date().toISOString(),
    repairedBy: 'milkmaid',
    replacementPath: filePath,
    replacementRelativePath: relativePath,
    replacementHash: details.hash || null,
    replacementSizeBytes: details.sizeBytes ?? null,
    replacementDurationSeconds: Number.isFinite(details.durationSeconds)
      ? details.durationSeconds
      : null,
    sourceUrl: details.sourceUrl || null,
    mediaPageUrl: details.mediaPageUrl || null,
  }

  saveQuarantineManifest(manifest)
  return true
}

function updateQuarantineManifestForRepairAttempt(filePath, details = {}) {
  const relativePath = getDatasetRelativePath(filePath)
  const quarantinePath = getQuarantineMirrorPath(filePath)
  const manifest = loadQuarantineManifest()
  const item = manifest.items.find(
    (entry) =>
      normalizePath(entry?.relativePath) === normalizePath(relativePath) ||
      entry?.quarantinePath === quarantinePath
  )

  if (!item) {
    return false
  }

  item.state = {
    activeDatasetExists: fs.existsSync(filePath),
    activeDatasetPath: filePath,
    quarantineExists: fs.existsSync(quarantinePath),
    repairState:
      fs.existsSync(filePath) && !fs.existsSync(quarantinePath)
        ? 'repaired'
        : fs.existsSync(filePath) && fs.existsSync(quarantinePath)
          ? 'replacement_present_pending_review'
          : !fs.existsSync(filePath) && !fs.existsSync(quarantinePath)
            ? 'missing_both'
            : 'quarantined',
  }

  item.repair = {
    ...(item.repair || {}),
    lastAttemptAt: new Date().toISOString(),
    lastAttemptBy: 'milkmaid',
    lastAttemptOutcome: details.outcome || 'failed',
    lastAttemptError: details.error || null,
    lastAttemptSourceUrl: details.sourceUrl || null,
    lastAttemptMediaPageUrl: details.mediaPageUrl || null,
    lastAttemptBytesDownloaded: Number.isFinite(details.bytesDownloaded)
      ? details.bytesDownloaded
      : null,
    lastAttemptExpectedBytes: Number.isFinite(details.expectedBytes)
      ? details.expectedBytes
      : null,
  }

  saveQuarantineManifest(manifest)
  return true
}

function getMediaTypeForFilePath(filePath) {
  const ext = path.extname(String(filePath || '')).toLowerCase()
  if (ext === '.gif') return 'gif'
  if (['.mp4', '.m4v', '.webm'].includes(ext)) return 'video'
  return 'image'
}

function ensureQuarantineManifestEntry(filePath, details = {}) {
  const relativePath = getDatasetRelativePath(filePath)
  const quarantinePath = getQuarantineMirrorPath(filePath)
  const manifest = loadQuarantineManifest()
  const sourceType = 'dataset'
  const model = relativePath.split('/')[0] || null
  const stat = fs.existsSync(quarantinePath)
    ? fs.statSync(quarantinePath)
    : null
  const reasons =
    Array.isArray(details.reasons) && details.reasons.length
      ? details.reasons
      : [String(details.reason || 'quarantined_for_review')]
  const entry = {
    id: `${sourceType}:${relativePath}`,
    sourceType,
    mediaType: getMediaTypeForFilePath(filePath),
    model,
    relativePath,
    sourcePathAtAudit: filePath,
    quarantinePath,
    reasons,
    sizeBytes: stat?.size ?? null,
    modifiedAt: stat?.mtime?.toISOString?.() || null,
    contentHash: null,
    hashLinkage: null,
    audit: {
      runId: currentRunLog?.startedAt || new Date().toISOString(),
      mode: 'milkmaid_lazy_failure',
      movedAt: new Date().toISOString(),
      decisionBacked: false,
    },
    state: {
      activeDatasetExists: fs.existsSync(filePath),
      activeDatasetPath: filePath,
      quarantineExists: fs.existsSync(quarantinePath),
      repairState:
        fs.existsSync(filePath) && !fs.existsSync(quarantinePath)
          ? 'repaired'
          : fs.existsSync(filePath) && fs.existsSync(quarantinePath)
            ? 'replacement_present_pending_review'
            : !fs.existsSync(filePath) && !fs.existsSync(quarantinePath)
              ? 'missing_both'
              : 'quarantined',
    },
    repair: {
      lastAttemptAt: new Date().toISOString(),
      lastAttemptBy: 'milkmaid',
      lastAttemptOutcome: details.outcome || 'failed',
      lastAttemptError: details.error || null,
      lastAttemptSourceUrl: details.sourceUrl || null,
      lastAttemptMediaPageUrl: details.mediaPageUrl || null,
      lastAttemptBytesDownloaded: Number.isFinite(details.bytesDownloaded)
        ? details.bytesDownloaded
        : null,
      lastAttemptExpectedBytes: Number.isFinite(details.expectedBytes)
        ? details.expectedBytes
        : null,
    },
  }

  const index = manifest.items.findIndex(
    (item) =>
      normalizePath(item?.relativePath) === normalizePath(relativePath) ||
      item?.quarantinePath === quarantinePath
  )

  if (index >= 0) {
    manifest.items[index] = {
      ...manifest.items[index],
      ...entry,
      repair: {
        ...(manifest.items[index]?.repair || {}),
        ...(entry.repair || {}),
      },
      state: entry.state,
      reasons,
    }
  } else {
    manifest.items.push(entry)
  }

  saveQuarantineManifest(manifest)
  return true
}

function isQuarantinedPath(filePath) {
  return datasetPaths.isQuarantinedPath(filePath)
}

function normalizePath(filePath) {
  return String(filePath || '').replace(/\\/g, '/')
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

function startRunLog(modelName, inputUrl, folders) {
  currentRunLog = runLifecycle.createRunLog({
    source: 'milkmaid',
    modelName,
    inputUrl,
    folders,
    counters: {
      saved: 0,
      skipped: 0,
      duplicates: 0,
      queuedVideos: 0,
      convertedGifs: 0,
      failures: 0,
      processed: 0,
      expectedMedia: 0,
    },
    transfer: {
      savedBytes: 0,
      failedBytes: 0,
      failedLazyVideoBytes: 0,
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
  })
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

function recordSeenMedia(modelLogDir, details = {}) {
  return sharedMediaSeenIndex.recordSeenMedia(modelLogDir, details)
}

function getActiveMediaSeenRecord(_modelLogDir, entry) {
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

function recordFailedSeenMedia(modelLogDir, details = {}) {
  return sharedMediaSeenIndex.recordFailedSeenMedia(modelLogDir, details)
}

function launchReviewDashboardProcess() {
  const reviewScriptPath = path.join(rootDir, 'audit', 'review-slopvault.js')
  const port = 4700 + Math.floor(Math.random() * 200)
  const child = spawn(
    process.execPath,
    [reviewScriptPath, '--skip-audit', '--port', String(port)],
    {
      cwd: rootDir,
      detached: true,
      stdio: 'ignore',
      windowsHide: false,
    }
  )

  child.unref()
  return { port }
}

async function maybePauseForErrorReview(modelName, failures, reviewErrors) {
  if (
    !reviewErrors ||
    failures <= 0 ||
    !process.stdin.isTTY ||
    !process.stdout.isTTY ||
    process.env.MILKMAID_SKIP_ERROR_REVIEW === '1'
  ) {
    return
  }

  try {
    const { port } = launchReviewDashboardProcess()
    logAndProgress('')
    logAndProgress(
      `🩺 Opened Slopvault review dashboard for ${modelName} on port ${port} before NAS sync.`
    )
    logAndProgress(
      'Review the run errors, mark bad upstream files as permanent-skip if needed, then return here.'
    )
  } catch (err) {
    logAndProgress(`⚠️ Could not launch review dashboard: ${err.message}`)
    return
  }

  await askQuestion(
    '\nPress Enter when you are ready to continue to NAS sync: '
  )
}

function normalizeSkipUrl(url) {
  const raw = String(url || '').trim()
  if (!raw) return ''

  return normalizeStufferDbPictureUrl(raw)
    .replace(/&acs=[^&]+/gi, '')
    .replace(/&slideshow=?/gi, '')
    .replace(/[?&]$/, '')
}

function isNuisanceMediaAsset(filename, ext) {
  const lowerFilename = String(filename || '')
    .trim()
    .toLowerCase()
  const lowerExt = String(ext || '')
    .trim()
    .toLowerCase()

  if (lowerExt === '.ico') return true

  return [
    'ajax_loader',
    'ajax-loader',
    'favicon',
    'loader.gif',
    'loading.gif',
    'preloader',
  ].some((token) => lowerFilename.includes(token))
}

function buildPermanentSkipLookup(entries) {
  return {
    relativePaths: new Set(
      entries
        .map((entry) => String(entry?.relativePath || '').trim())
        .filter(Boolean)
    ),
    sourceUrls: new Set(
      entries.map((entry) => normalizeSkipUrl(entry?.sourceUrl)).filter(Boolean)
    ),
    mediaPageUrls: new Set(
      entries
        .map((entry) => normalizeSkipUrl(entry?.mediaPageUrl))
        .filter(Boolean)
    ),
    filenames: new Set(
      entries
        .map((entry) => String(entry?.filename || '').trim())
        .filter(Boolean)
    ),
  }
}

function loadPermanentSkips() {
  if (!fs.existsSync(permanentSkipFile)) {
    permanentSkipEntries = []
    permanentSkipLookup = buildPermanentSkipLookup(permanentSkipEntries)
    return
  }

  try {
    const raw = fs.readFileSync(permanentSkipFile, 'utf8').trim()
    const parsed = raw ? JSON.parse(raw) : { entries: [] }
    permanentSkipEntries = Array.isArray(parsed?.entries) ? parsed.entries : []
    permanentSkipLookup = buildPermanentSkipLookup(permanentSkipEntries)
  } catch (err) {
    console.warn(
      `⚠️ Could not parse permanent skip file at ${permanentSkipFile}: ${err.message}`
    )
    permanentSkipEntries = []
    permanentSkipLookup = buildPermanentSkipLookup(permanentSkipEntries)
  }
}

function savePermanentSkips() {
  fs.writeFileSync(
    permanentSkipFile,
    JSON.stringify(
      {
        version: 1,
        entries: permanentSkipEntries,
      },
      null,
      2
    )
  )
}

function addPermanentSkip(entry) {
  const normalizedEntry = {
    relativePath: String(entry?.relativePath || '')
      .trim()
      .replace(/\\/g, '/'),
    sourceUrl: normalizeSkipUrl(entry?.sourceUrl),
    mediaPageUrl: normalizeSkipUrl(entry?.mediaPageUrl),
    filename: String(entry?.filename || '').trim(),
    reason: String(entry?.reason || '').trim() || 'manual_skip',
    note: String(entry?.note || '').trim() || null,
    addedAt: entry?.addedAt || new Date().toISOString(),
  }

  const alreadyExists = permanentSkipEntries.some(
    (existing) =>
      (normalizedEntry.relativePath &&
        existing?.relativePath === normalizedEntry.relativePath) ||
      (normalizedEntry.sourceUrl &&
        normalizeSkipUrl(existing?.sourceUrl) === normalizedEntry.sourceUrl) ||
      (normalizedEntry.mediaPageUrl &&
        normalizeSkipUrl(existing?.mediaPageUrl) ===
          normalizedEntry.mediaPageUrl)
  )

  if (alreadyExists) return false

  permanentSkipEntries.push(normalizedEntry)
  permanentSkipLookup = buildPermanentSkipLookup(permanentSkipEntries)
  savePermanentSkips()
  return true
}

function getPermanentSkipMatch({
  relativePath,
  mediaUrl,
  mediaPageUrl,
  filename,
}) {
  const normalizedRelativePath = String(relativePath || '')
    .trim()
    .replace(/\\/g, '/')
  const normalizedMediaUrl = normalizeSkipUrl(mediaUrl)
  const normalizedMediaPageUrl = normalizeSkipUrl(mediaPageUrl)
  const normalizedFilename = String(filename || '').trim()

  return (
    permanentSkipEntries.find((entry) => {
      return (
        (normalizedRelativePath &&
          permanentSkipLookup.relativePaths.has(normalizedRelativePath) &&
          entry.relativePath === normalizedRelativePath) ||
        (normalizedMediaUrl &&
          permanentSkipLookup.sourceUrls.has(normalizedMediaUrl) &&
          normalizeSkipUrl(entry.sourceUrl) === normalizedMediaUrl) ||
        (normalizedMediaPageUrl &&
          permanentSkipLookup.mediaPageUrls.has(normalizedMediaPageUrl) &&
          normalizeSkipUrl(entry.mediaPageUrl) === normalizedMediaPageUrl) ||
        (normalizedFilename &&
          permanentSkipLookup.filenames.has(normalizedFilename) &&
          entry.filename === normalizedFilename)
      )
    }) || null
  )
}

function resolveEffectiveFileDate(date, fallbackDate = new Date()) {
  return mediaFileRecords.resolveEffectiveFileDate(date, fallbackDate)
}

function buildHashMetadata(
  modelName,
  absolutePath,
  mediaType,
  sizeBytes,
  uploadedDate
) {
  return milkmaidMediaSaver.buildHashMetadata({
    modelName,
    absolutePath,
    mediaType,
    sizeBytes,
    modifiedAt: uploadedDate,
  })
}

function downloadBufferWithProgress(mediaUrl, onProgress) {
  const proto = mediaUrl.startsWith('https') ? https : http
  return new Promise((resolve, reject) => {
    proto
      .get(mediaUrl, (res) => {
        if (res.statusCode !== 200)
          return reject(new Error(`HTTP ${res.statusCode}`))
        const totalBytes = parseInt(res.headers['content-length'] || '0', 10)
        let downloadedBytes = 0,
          chunks = [],
          start = Date.now()

        res.on('data', (chunk) => {
          downloadedBytes += chunk.length
          chunks.push(chunk)
          if (typeof onProgress === 'function' && totalBytes > 0) {
            const percent = ((downloadedBytes / totalBytes) * 100).toFixed(1)
            const speed = (
              downloadedBytes /
              1024 /
              ((Date.now() - start) / 1000)
            ).toFixed(1)
            onProgress(percent, speed, chunk)
          }
        })
        res.on('end', () => resolve(Buffer.concat(chunks)))
      })
      .on('error', reject)
  })
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

async function fetchStufferDBTotalCount(browser, url) {
  return await fetchStufferDbTotalCountFromAdapter(browser, url, {
    createScraperPage,
    logger: console,
  })
}

function getVideoDuration(filePath) {
  return new Promise((resolve, reject) => {
    const cmd = `ffprobe -v error -show_entries format=duration -of csv=p=0 "${filePath}"`
    exec(cmd, (err, stdout) => {
      if (err) return reject(err)
      const duration = parseFloat(stdout.trim())
      resolve(isNaN(duration) ? 9999 : duration)
    })
  })
}

function convertShortMp4ToGif(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const cmd = `ffmpeg -y -i "${inputPath}" -vf "fps=15,scale=480:-1:flags=lanczos" "${outputPath}"`
    exec(cmd, (err) => (err ? reject(err) : resolve()))
  })
}

function cleanupEmptyParentDirs(startPath, stopPath) {
  let current = path.dirname(startPath)
  const resolvedStop = path.resolve(stopPath)

  while (
    current &&
    path.resolve(current).startsWith(resolvedStop) &&
    path.resolve(current) !== resolvedStop
  ) {
    try {
      if (fs.readdirSync(current).length > 0) break
      fs.rmdirSync(current)
      current = path.dirname(current)
    } catch (err) {
      break
    }
  }
}

function removeQuarantineMirrorIfExists(filePath) {
  const mirrorPath = getQuarantineMirrorPath(filePath)
  if (!removeFileIfExists(mirrorPath)) return false
  cleanupEmptyParentDirs(mirrorPath, quarantineDatasetDir)
  return true
}

function moveFailedLazyVideoToQuarantine(tmpPath, finalPath) {
  const quarantinePath = getQuarantineMirrorPath(finalPath)
  fs.mkdirSync(path.dirname(quarantinePath), { recursive: true })
  removeFileIfExists(quarantinePath)
  moveFileIntoPlace(tmpPath, quarantinePath)
  return quarantinePath
}

function isStructuralLazyVideoFailure(errorMessage) {
  const text = String(errorMessage || '')
  return (
    text.includes('tail_decode_error') ||
    text.includes('ffprobe_failed') ||
    text.includes('invalid_duration')
  )
}

function getPermanentLazyVideoFailure(errorMessage) {
  const text = String(errorMessage || '').trim()
  if (isStructuralLazyVideoFailure(text)) {
    const tailDecodeFailure = text.includes('tail_decode_error')
    return {
      reason: tailDecodeFailure
        ? 'upstream_tail_decode_error'
        : 'lazy_video_error',
      note: tailDecodeFailure
        ? 'Fully downloaded but tail decode failed; skipping future reruns unless manually cleared.'
        : `Lazy video failed during validation: ${text}`,
      preservePartial: true,
    }
  }

  const statusMatch = text.match(/^HTTP\s+(\d{3})\b/i)
  const statusCode = Number(statusMatch?.[1] || 0)
  if (statusCode === 404 || statusCode === 410) {
    return {
      reason: 'upstream_media_missing',
      note: `Upstream media returned HTTP ${statusCode}; skipping future reruns unless manually cleared.`,
      preservePartial: false,
    }
  }

  return null
}

function quoteForShell(value) {
  return `"${String(value).replace(/"/g, '\\"')}"`
}

function runShellCommand(command, options = {}) {
  return new Promise((resolve) => {
    exec(command, options, (error, stdout, stderr) => {
      resolve({
        ok: !error,
        code: error?.code ?? 0,
        stdout: String(stdout || '').trim(),
        stderr: String(stderr || '').trim(),
        error: error?.message || null,
      })
    })
  })
}

async function getVideoIntegrityReport(filePath) {
  const quotedPath = quoteForShell(filePath)
  const probe = await runShellCommand(
    `ffprobe -v error -show_entries format=duration,size -show_streams -of json ${quotedPath}`
  )
  const tail = await runShellCommand(
    `ffmpeg -v error -sseof -3 -i ${quotedPath} -frames:v 1 -f null -`
  )

  let duration = null
  let size = null
  let streamCount = 0

  if (probe.ok && probe.stdout) {
    try {
      const parsed = JSON.parse(probe.stdout)
      duration = Number.parseFloat(parsed?.format?.duration || '')
      size = Number.parseInt(parsed?.format?.size || '', 10)
      streamCount = Array.isArray(parsed?.streams) ? parsed.streams.length : 0
    } catch (err) {
      // Ignore JSON parse issues here; the raw stderr/stdout are logged below.
    }
  }

  return {
    probeOk: probe.ok,
    probeCode: probe.code,
    probeError: probe.error,
    probeStdout: probe.stdout,
    probeStderr: probe.stderr,
    duration: Number.isFinite(duration) ? duration : null,
    size: Number.isFinite(size) ? size : null,
    streamCount,
    tailDecodeOk: tail.ok,
    tailDecodeCode: tail.code,
    tailDecodeError: tail.error,
    tailDecodeStderr: tail.stderr,
  }
}

async function getRemoteVideoTailReport(url) {
  const timeoutMicros = 20000000
  return runShellCommand(
    `ffmpeg -v error -rw_timeout ${timeoutMicros} -sseof -3 -i ${quoteForShell(url)} -frames:v 1 -f null -`
  )
}

function shouldTraceLazyVideo(filename, finalPath) {
  const traceMatch = String(process.env.MILKMAID_TRACE_MATCH || '')
    .trim()
    .toLowerCase()
  if (traceMatch) {
    return (
      filename.toLowerCase().includes(traceMatch) ||
      getDatasetRelativePath(finalPath).toLowerCase().includes(traceMatch)
    )
  }

  return isQuarantinedPath(finalPath)
}

let completedTotal = 0
let progressMode = 'scrape'

function setProgressMode(mode) {
  progressMode = mode
}

function resetProgressCounter(total = null) {
  completedTotal = 0
  progressMode = 'scrape'

  if (typeof total === 'number' && !Number.isNaN(total)) {
    global.totalSearchTotal = Math.max(total, 1)
  }
  syncScrapeProgressCounters()
}

function logAndProgress(message, increment = false) {
  if (increment) {
    completedTotal++
    syncScrapeProgressCounters()
  }

  logScrollingMessage(message)

  if (progressMode === 'lazy') {
    if (!shouldDrawLazyProgress()) return
    const percent = totalLazyBytes
      ? (lazyBytesDownloaded / totalLazyBytes) * 100
      : 0
    const elapsedSeconds = lazyDownloadStartedAt
      ? Math.max((Date.now() - lazyDownloadStartedAt) / 1000, 0.001)
      : 0
    const speedBytesPerSecond = elapsedSeconds
      ? lazyBytesDownloaded / elapsedSeconds
      : 0
    const remainingBytes = Math.max(totalLazyBytes - lazyBytesDownloaded, 0)
    const etaSeconds =
      speedBytesPerSecond > 0 ? remainingBytes / speedBytesPerSecond : null
    logLazyProgress(percent, lazyBytesDownloaded, totalLazyBytes, {
      speedBytesPerSecond,
      etaSeconds,
      activeCount: lazyActiveDownloads,
      completedCount: lazyCompletedDownloads,
      totalCount: lazyVideoQueue.length,
      currentLabel: lazyCurrentLabel,
    })
  } else {
    logProgress(completedTotal, global.totalSearchTotal || 1, {
      bottomText: getScrapeStatsLine(),
    })
  }
}

function shouldDrawLazyProgress() {
  return (
    lazyVideoQueue.length > 0 &&
    (lazyActiveDownloads > 0 || lazyBytesDownloaded > 0)
  )
}

function getScrapeStatsLine() {
  const stats = runLifecycle.getRunProgressStats(currentRunLog, {
    processed: completedTotal,
    expectedMedia: Number.isFinite(global.totalSearchTotal)
      ? global.totalSearchTotal
      : 1,
    saved: successCount,
    duplicates: duplicateCount,
    failures: errorCount,
  })
  const parts = [
    `${stats.saved} saved`,
    `${stats.duplicates} dupes`,
    `${stats.failures} errors`,
  ]

  if (lazyVideoQueue.length > 0) {
    parts.push(`${lazyVideoQueue.length} queued`)
  }

  return parts.join(' | ')
}

function setProgressTotal(total = null) {
  if (typeof total === 'number' && !Number.isNaN(total)) {
    global.totalSearchTotal = Math.max(total, 0)
  }
  syncScrapeProgressCounters()
}

function syncScrapeProgressCounters() {
  runLifecycle.setRunCounter(currentRunLog, 'processed', completedTotal)
  runLifecycle.setRunCounter(
    currentRunLog,
    'expectedMedia',
    Number.isFinite(global.totalSearchTotal) ? global.totalSearchTotal : 1
  )
}

function getMilkmaidEntrySeenDetails(entry = {}) {
  return getMediaEntrySeenDetails(entry)
}

function getMilkmaidEntrySourceDetails(entry = {}) {
  return getMediaEntrySourceDetails(entry)
}

function getMilkmaidEntryHashMetadata(entry = {}) {
  return getMediaEntryHashMetadata(entry)
}

function recordMilkmaidDuplicate({
  modelName,
  folders,
  entry,
  destination,
  reason,
  extra = {},
  recordSeen = false,
}) {
  return milkmaidSavePipeline.recordDuplicate({
    modelName,
    folders,
    entry,
    destination,
    reason,
    extra,
    recordSeen,
  })
}

function getMilkmaidSeenMatchReasonCounter(entry, match) {
  if (match?.matchType === 'media_page_url') return 'skipSeenMediaPage'
  if (match?.matchType === 'media_url') return 'skipSeenMediaUrl'
  return 'skipSeenMedia'
}

async function saveStufferDbImageLikeMedia({
  modelName,
  folders,
  entry,
  destination,
  kind,
}) {
  const result = await milkmaidSavePipeline.saveImageLikeMedia({
    modelName,
    folders,
    entry,
    destination,
    kind,
    downloadBuffer: downloadBufferWithProgress,
    getBitwiseDuplicationRecord,
    getVisualHashFromBuffer,
    getVisualDuplicationRecord,
    addBitwiseHash,
    addVisualHash,
    saveBitwiseHashCache,
    shouldAddBitwiseHash: ({ hash }) => !isBitwiseDupe(hash),
    checkExistingBeforeDownload: false,
    duplicateRecordSeen: true,
    visualChecks: kind === 'image',
    addVisualHashBeforeSave: true,
  })

  if (result.reason === 'duplicate_bitwise') {
    return logAndProgress(`♻️ Bitwise dupe: ${entry.filename}`, true)
  }
  if (result.reason === 'duplicate_visual') {
    return logAndProgress(`👁️ Visual dupe (global): ${entry.filename}`, true)
  }
  if (result.reason === 'skip_existing_image') {
    return logAndProgress(`♻️ Skipped (exists): ${entry.filename}`, true)
  }
  if (result.reason === 'skip_existing_gif') {
    return logAndProgress(`♻️ Skipped gif (exists): ${entry.filename}`, true)
  }
  return logAndProgress(
    kind === 'gif'
      ? `Saved gif: ${entry.filename}`
      : `✅ Saved: ${entry.filename}`,
    true
  )
}

function queueStufferDbVideoMedia({ modelName, folders, entry, destination }) {
  const destinationExists = existsLocallyOrOnNas(destination.finalPath)
  if (knownFilenames.has(entry.filename) || destinationExists) {
    recordMilkmaidDuplicate({
      modelName,
      folders,
      entry,
      destination,
      reason: 'skip_existing_video',
      extra: milkmaidSavePipeline.getExistingExtra(destination),
      recordSeen: destinationExists,
    })
    return logAndProgress(
      `⛔ Skipping mp4 - already handled: ${entry.filename}`,
      true
    )
  }

  milkmaidSavePipeline.queueVideo({
    modelName,
    folders,
    entry,
    destination,
    queue: lazyVideoQueue,
  })

  return logAndProgress(`🐌 Queued lazy video: ${entry.filename}`, true)
}

async function saveStufferDbMediaEntry({ modelName, folders, entry }) {
  if (isNuisanceMediaAsset(entry.filename, entry.extension)) {
    appendRunEvent('skip_nuisance_media', {
      modelName,
      ...getMilkmaidEntrySeenDetails(entry),
      ...getMilkmaidEntrySourceDetails(entry),
      filename: entry.filename,
      extension: entry.extension,
    })
    return logAndProgress(`🚫 Skipped nuisance asset: ${entry.filename}`, true)
  }

  const destination = milkmaidSavePipeline.getDestination({
    modelName,
    folders,
    entry,
  })

  milkmaidSavePipeline.recordMediaSeen({ modelName, entry, destination })

  const permanentSkipMatch = getPermanentSkipMatch({
    relativePath: destination.relativePath,
    mediaUrl: entry.mediaUrl,
    mediaPageUrl: entry.mediaPageUrl,
    filename: entry.filename,
  })
  if (permanentSkipMatch) {
    recordMilkmaidDuplicate({
      modelName,
      entry,
      destination,
      reason: 'skip_permanent',
      extra: {
        relativePath: destination.relativePath,
        reason: permanentSkipMatch.reason || 'manual_skip',
        note: permanentSkipMatch.note || null,
        reasonCounter: 'skipPermanent',
      },
    })
    return logAndProgress(`🛑 Permanent skip: ${entry.filename}`, true)
  }

  const seenMediaMatch = milkmaidSavePipeline.getSeenMediaMatch(folders, entry)
  if (seenMediaMatch) {
    recordMilkmaidDuplicate({
      modelName,
      entry,
      destination: {
        ...destination,
        relativePath: seenMediaMatch.relativePath,
      },
      reason: 'skip_seen_media',
      extra: {
        matchType: seenMediaMatch.matchType,
        reasonCounter: getMilkmaidSeenMatchReasonCounter(entry, seenMediaMatch),
      },
    })
    return logAndProgress(
      `⏩ Seen media skip (${seenMediaMatch.matchType}): ${entry.filename}`,
      true
    )
  }

  if (entry.kind === 'video') {
    return queueStufferDbVideoMedia({ modelName, folders, entry, destination })
  }

  return saveStufferDbImageLikeMedia({
    modelName,
    folders,
    entry,
    destination,
    kind: entry.kind === 'gif' ? 'gif' : 'image',
  })
}

function getActiveStufferDbCheckpoint(savedCheckpoint, fullSourceRefresh) {
  if (fullSourceRefresh || !savedCheckpoint) return null
  const mediaPageUrl = savedCheckpoint.mediaPageUrl
    ? normalizeStufferDbPictureUrl(savedCheckpoint.mediaPageUrl)
    : null
  const id = savedCheckpoint.id || getStufferDbPictureId(mediaPageUrl)
  if (!id || !mediaPageUrl) return null
  return {
    id: String(id),
    mediaPageUrl,
  }
}

async function scrapeGallery(browser, url, modelName, folders, options = {}) {
  const { base, images, webm } = folders
  const fullSourceRefresh = Boolean(options.fullSourceRefresh)
  const categoryId = getStufferDbCategoryId(url)
  const sourceIdentity = {
    site: 'stufferdb',
    service: 'category',
    userId: categoryId,
    rawName: modelName,
  }
  const savedCheckpoint = fullSourceRefresh
    ? null
    : getSourceCheckpoint(folders.logDir, sourceIdentity)
  const activeCheckpoint = getActiveStufferDbCheckpoint(
    savedCheckpoint,
    fullSourceRefresh
  )
  const checkpointMediaPageUrl = activeCheckpoint?.mediaPageUrl || null
  const checkpointId = activeCheckpoint?.id || null
  const checkpointActive = Boolean(activeCheckpoint)
  let categoryInspectedCount = 0
  let unscannedCount = 0
  let unresolvedCount = 0
  let coverageComplete = false
  const checkpointCandidates = []
  const visitedCategoryUrls = new Set()
  const visitedGallerySignatures = new Set()
  const seenCategoryPictureIds = new Set()
  url = withStufferDbNewestFirst(url)

  if (checkpointActive) {
    appendRunEvent('category_checkpoint_activated', {
      modelName,
      categoryId,
      checkpointId,
      mediaPageUrl: checkpointMediaPageUrl,
      validation: 'persisted_chronological_marker',
    })
  }

  const page = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: false,
  })

  process.stdout.write('\n') // Reserve one lines
  logProgress(completedTotal, global.totalSearchTotal || 1, {
    bottomText: getScrapeStatsLine(),
  })

  try {
    while (url) {
      await gotoWithTimeoutRetry(page, url, {
        waitUntil: 'domcontentloaded',
        timeoutMs: CATEGORY_PAGE_TIMEOUT_MS,
        retryTimeoutMs: CATEGORY_PAGE_RETRY_TIMEOUT_MS,
        onRetry: (error) => {
          appendRunEvent('category_page_retry', {
            modelName,
            categoryUrl: url,
            timeoutMs: CATEGORY_PAGE_RETRY_TIMEOUT_MS,
            reason: error.message,
          })
        },
      })

      const galleryLinks = await extractGalleryPictureUrls(page)
      const urls = galleryLinks.rawUrls
      const loadedCategoryUrl = normalizeStufferDbCategoryUrl(page.url())
      const loadedUrlKey = loadedCategoryUrl.toLowerCase()
      const pagePictureIds = galleryLinks.urls
        .map((mediaPageUrl) => getStufferDbPictureId(mediaPageUrl))
        .filter(Boolean)
      const gallerySignature = [
        pagePictureIds.length,
        pagePictureIds[0] || '',
        pagePictureIds[pagePictureIds.length - 1] || '',
      ].join(':')
      if (
        visitedCategoryUrls.has(loadedUrlKey) ||
        visitedGallerySignatures.has(gallerySignature)
      ) {
        errorCount++
        unresolvedCount++
        runLifecycle.incrementRunCounter(currentRunLog, 'failures')
        recordRunReason('categoryPaginationLoop')
        appendRunEvent('category_pagination_loop', {
          modelName,
          requestedUrl: url,
          loadedCategoryUrl,
          gallerySignature,
        })
        logAndProgress(
          `StufferDB pagination repeated ${loadedCategoryUrl}; stopping category.`,
          true
        )
        break
      }
      visitedCategoryUrls.add(loadedUrlKey)
      visitedGallerySignatures.add(gallerySignature)

      const dedupedUrls = galleryLinks.urls.filter((mediaPageUrl) => {
        const pictureId = getStufferDbPictureId(mediaPageUrl)
        if (!pictureId || seenCategoryPictureIds.has(pictureId)) return false
        seenCategoryPictureIds.add(pictureId)
        return true
      })
      const checkpointIndex = checkpointActive
        ? dedupedUrls.findIndex(
            (mediaPageUrl) =>
              getStufferDbPictureId(mediaPageUrl) === String(checkpointId)
          )
        : -1
      const pageUrls =
        checkpointIndex >= 0
          ? dedupedUrls.slice(0, checkpointIndex)
          : dedupedUrls
      checkpointCandidates.push(...pageUrls)
      const total = pageUrls.length
      categoryInspectedCount += total
      const confirmedSeenPages = new Map()
      if (!fullSourceRefresh) {
        for (const rawMediaPageUrl of pageUrls) {
          const mediaPageUrl = normalizeStufferDbPictureUrl(rawMediaPageUrl)
          const match = getSuccessfulSeenMediaMatch(
            folders.logDir,
            mediaPageUrl,
            null
          )
          if (match) confirmedSeenPages.set(mediaPageUrl, match)
        }
      }
      const confirmedSeenCount = confirmedSeenPages.size
      const candidateUrls = fullSourceRefresh
        ? pageUrls
        : pageUrls.filter(
            (mediaPageUrl) =>
              !confirmedSeenPages.has(
                normalizeStufferDbPictureUrl(mediaPageUrl)
              )
          )

      // If prefetch undercounted, keep the rollup large enough to cover
      // what we have already processed plus what remains visible right now.
      setProgressTotal(
        Math.max(global.totalSearchTotal || 1, completedTotal + total)
      )

      const mode = url.includes('&acs=') ? 'ACS' : 'PLAIN'
      logScrollingMessage(
        `📸 ${modelName} - [${mode}] - ${dedupedUrls.length} media links (tracking ${global.totalSearchTotal})`
      )
      appendRunEvent('category_page_loaded', {
        modelName,
        categoryUrl: url,
        mode,
        mediaLinks: dedupedUrls.length,
        inspectedMediaLinks: pageUrls.length,
        rawMediaLinks: urls.length,
        trackedTotal: global.totalSearchTotal || 0,
        confirmedSeenCount,
        checkpointId: checkpointActive ? checkpointId : null,
        checkpointReached: checkpointIndex >= 0,
      })
      if (!fullSourceRefresh) {
        logScrollingMessage(
          `StufferDB incremental page: ${confirmedSeenCount} confirmed seen, ${candidateUrls.length} new candidate(s)${checkpointIndex >= 0 ? ', checkpoint reached' : ''}`
        )
      }

      for (const [mediaPageUrl, seenMediaPageMatch] of confirmedSeenPages) {
        totalCount++
        duplicateCount++
        runLifecycle.incrementRunCounter(currentRunLog, 'duplicates')
        recordRunReason(
          getMilkmaidSeenMatchReasonCounter(
            { mediaPageUrl, mediaPageUrls: [mediaPageUrl] },
            seenMediaPageMatch
          )
        )
        appendRunEvent('skip_seen_media', {
          modelName,
          filename: null,
          mediaUrl: seenMediaPageMatch.sourceUrl || null,
          mediaPageUrl,
          matchType: seenMediaPageMatch.matchType,
          savedPath: seenMediaPageMatch.relativePath,
          preNavigation: true,
          incrementalFrontier: true,
        })
        logAndProgress(
          `Seen page skip (${seenMediaPageMatch.matchType}): ${mediaPageUrl}`,
          true
        )
      }

      const pages =
        candidateUrls.length > 0
          ? await Promise.all(
              Array.from({ length: MEDIA_PAGE_CONCURRENCY }, () =>
                createScraperPage(browser, {
                  site: 'stufferdb',
                  interceptMedia: false,
                })
              )
            )
          : []

      const pageLocks = pages.map(() => pLimit(1)) // 🧠 One lock per tab

      async function scrapeMediaOnPage(page, mediaPageUrl, i) {
        mediaPageUrl = normalizeStufferDbPictureUrl(mediaPageUrl)
        totalCount++
        let mediaUrl = null
        let filename = null
        let ext = null

        try {
          const permanentSkipPageMatch = getPermanentSkipMatch({
            mediaPageUrl,
          })
          if (permanentSkipPageMatch) {
            duplicateCount++
            runLifecycle.incrementRunCounter(currentRunLog, 'duplicates')
            recordRunReason('skipPermanent')
            appendRunEvent('skip_permanent', {
              modelName,
              filename: null,
              relativePath: permanentSkipPageMatch.relativePath || null,
              mediaUrl: permanentSkipPageMatch.sourceUrl || null,
              mediaPageUrl,
              reason: permanentSkipPageMatch.reason || 'manual_skip',
              note: permanentSkipPageMatch.note || null,
              preNavigation: true,
            })
            return logAndProgress(
              `🛑 Permanent page skip: ${mediaPageUrl}`,
              true
            )
          }

          const seenMediaPageMatch = getSuccessfulSeenMediaMatch(
            folders.logDir,
            mediaPageUrl,
            null
          )
          if (seenMediaPageMatch) {
            duplicateCount++
            runLifecycle.incrementRunCounter(currentRunLog, 'duplicates')
            recordRunReason(
              getMilkmaidSeenMatchReasonCounter(
                { mediaPageUrl, mediaPageUrls: [mediaPageUrl] },
                seenMediaPageMatch
              )
            )
            appendRunEvent('skip_seen_media', {
              modelName,
              filename: null,
              mediaUrl: seenMediaPageMatch.sourceUrl || null,
              mediaPageUrl,
              matchType: seenMediaPageMatch.matchType,
              savedPath: seenMediaPageMatch.relativePath,
              preNavigation: true,
            })
            return logAndProgress(
              `⏩ Seen page skip (${seenMediaPageMatch.matchType}): ${mediaPageUrl}`,
              true
            )
          }

          const mediaEntry = await fetchStufferDbMediaEntry(
            page,
            mediaPageUrl,
            {
              url,
              categoryId: getStufferDbCategoryId(url),
              modelName,
            },
            {
              timeoutMs: MEDIA_PAGE_TIMEOUT_MS,
              retryTimeoutMs: MEDIA_PAGE_RETRY_TIMEOUT_MS,
              sleep,
              onRetry: (error) => {
                appendRunEvent('media_page_retry', {
                  modelName,
                  mediaPageUrl,
                  attempt: 2,
                  timeoutMs: MEDIA_PAGE_RETRY_TIMEOUT_MS,
                  reason: error.message,
                })
              },
            }
          )
          if (!mediaEntry) {
            unresolvedCount++
            appendRunEvent('media_page_unresolved', {
              modelName,
              mediaPageUrl,
            })
            return
          }
          mediaUrl = mediaEntry.mediaUrl
          filename = mediaEntry.filename
          ext = mediaEntry.extension

          await saveStufferDbMediaEntry({
            modelName,
            folders,
            entry: mediaEntry,
          })
        } catch (err) {
          errorCount++
          runLifecycle.incrementRunCounter(currentRunLog, 'failures')
          recordRunReason('mediaError')
          recordRunError('media_error', {
            modelName,
            mediaPageUrl,
            mediaUrl,
            filename,
            extension: ext,
            error: err.message,
          })
          appendRunEvent('media_error', {
            modelName,
            mediaPageUrl,
            mediaUrl,
            filename,
            extension: ext,
            error: err.message,
          })
          logAndProgress(
            `❌ Error processing ${mediaPageUrl}: ${err.message}`,
            true
          )
        }

        await randomDelay()
      }

      try {
        await Promise.all(
          candidateUrls.map((mediaPageUrl, i) => {
            const workerPage = pages[i % pages.length]
            const lock = pageLocks[i % pageLocks.length]

            return limit(() =>
              lock(() => scrapeMediaOnPage(workerPage, mediaPageUrl, i))
            )
          })
        )
      } finally {
        await Promise.all(
          pages.map((workerPage) =>
            workerPage.isClosed() ? null : workerPage.close()
          )
        )
      }

      if (checkpointIndex >= 0) {
        coverageComplete = unresolvedCount === 0
        unscannedCount = Math.max(
          Number(options.categoryTotal || 0) - categoryInspectedCount,
          0
        )
        appendRunEvent('category_incremental_frontier_reached', {
          modelName,
          categoryUrl: url,
          checkpointId,
          categoryInspectedCount,
          unscannedCount,
        })
        logScrollingMessage(
          `StufferDB checkpoint ${checkpointId} reached; stopping.`
        )
        break
      }

      const nextHref = await page
        .$eval('a[rel="next"]', (el) => el?.href)
        .catch(() => null)
      if (nextHref) {
        const baseUrl = new URL(url)
        url = new URL(nextHref, baseUrl).href
      } else {
        coverageComplete = unresolvedCount === 0
        break
      }
    }
  } finally {
    if (process.stdout.isTTY) {
      readline.clearLine(process.stdout, 0)
      readline.cursorTo(process.stdout, 0)
    }
    await page.close()
  }
  return {
    categoryId,
    checkpointCandidates,
    existingCheckpoint:
      checkpointActive && checkpointMediaPageUrl
        ? {
            id: String(checkpointId),
            mediaPageUrl: checkpointMediaPageUrl,
          }
        : null,
    coverageComplete,
    unresolvedCount,
    handledCount: totalCount,
    inspectedCount: categoryInspectedCount,
    unscannedCount,
  }
}

async function scrapeDirectPicture(browser, mediaPageUrl, modelName, folders) {
  const page = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: false,
  })
  const normalizedMediaPageUrl = normalizeStufferDbPictureUrl(mediaPageUrl)
  const categoryId = getStufferDbCategoryId(normalizedMediaPageUrl)
  const sourceUrl = categoryId
    ? `https://stufferdb.com/index?/category/${categoryId}`
    : normalizedMediaPageUrl

  process.stdout.write('\n')
  logProgress(completedTotal, global.totalSearchTotal || 1, {
    bottomText: getScrapeStatsLine(),
  })

  try {
    const seenMediaPageMatch = getSuccessfulSeenMediaMatch(
      folders.logDir,
      normalizedMediaPageUrl,
      null
    )
    if (seenMediaPageMatch) {
      duplicateCount++
      runLifecycle.incrementRunCounter(currentRunLog, 'duplicates')
      appendRunEvent('skip_seen_media', {
        modelName,
        filename: null,
        mediaUrl: seenMediaPageMatch.sourceUrl || null,
        mediaPageUrl: normalizedMediaPageUrl,
        matchType: seenMediaPageMatch.matchType,
        savedPath: seenMediaPageMatch.relativePath,
        preNavigation: true,
      })
      return logAndProgress(
        `Seen page skip (${seenMediaPageMatch.matchType}): ${normalizedMediaPageUrl}`,
        true
      )
    }

    const mediaEntry = await fetchStufferDbMediaEntry(
      page,
      normalizedMediaPageUrl,
      {
        url: sourceUrl,
        categoryId,
        modelName,
      },
      {
        timeoutMs: MEDIA_PAGE_TIMEOUT_MS,
        retryTimeoutMs: MEDIA_PAGE_RETRY_TIMEOUT_MS,
        sleep,
        onRetry: (error) => {
          appendRunEvent('media_page_retry', {
            modelName,
            mediaPageUrl: normalizedMediaPageUrl,
            attempt: 2,
            timeoutMs: MEDIA_PAGE_RETRY_TIMEOUT_MS,
            reason: error.message,
          })
        },
      }
    )

    if (!mediaEntry) {
      errorCount++
      runLifecycle.incrementRunCounter(currentRunLog, 'failures')
      appendRunEvent('media_error', {
        modelName,
        mediaPageUrl: normalizedMediaPageUrl,
        error: 'No media found on page',
      })
      return logAndProgress(`No media found: ${normalizedMediaPageUrl}`, true)
    }

    await saveStufferDbMediaEntry({
      modelName,
      folders,
      entry: mediaEntry,
    })
  } finally {
    if (process.stdout.isTTY) {
      readline.clearLine(process.stdout, 0)
      readline.cursorTo(process.stdout, 0)
    }
    logScrollingMessage(getCompletionLine())
    await page.close()
  }
}

async function runMilkmaidScrape(argvInput = process.argv.slice(2)) {
  resetRunState()
  bannerMilkmaid()

  let browser = null
  let modelName = null
  let categoryRunList = []
  let combinedTotal = 0
  let effectiveCombinedTotal = 0
  const pendingCategoryCheckpoints = []

  try {
    const {
      inputUrl: initialInputUrl,
      modelOverride,
      reviewErrors,
      skipNasSync,
      keepHistory,
      fullSourceRefresh,
      sourceIncrementalOverlapPages,
    } = normalizeMilkmaidRunOptions(argvInput)
    let inputUrl = initialInputUrl
    if (
      !inputUrl ||
      (!inputUrl.includes('/category/') && !/picture\?\//i.test(inputUrl))
    ) {
      logAndProgress('⚠️  Usage: node milkmaid.js <gallery-url>')
      return 1
    }

    inputUrl = inputUrl.replace(/&acs=[^&]+/i, '')
    const isDirectPictureRun = /picture\?\//i.test(inputUrl)
    inputUrl = isDirectPictureRun
      ? normalizeStufferDbPictureUrl(inputUrl)
      : normalizeStufferDbCategoryUrl(inputUrl)

    const categoryId = getStufferDbCategoryId(inputUrl)
    if (!categoryId) {
      logAndProgress('❌ Invalid category URL')
      return 1
    }

    browser = await puppeteer.launch({
      headless: 'new',
      executablePath: executablePath(),
      args: ['--no-sandbox', '--ignore-certificate-errors'],
      ignoreHTTPSErrors: true, // ✅ add this too
    })

    const tempPage = await createScraperPage(browser, {
      site: 'stufferdb',
      interceptMedia: true,
    })
    await gotoWithTimeoutRetry(tempPage, inputUrl, {
      waitUntil: 'domcontentloaded',
      timeoutMs: CATEGORY_PAGE_TIMEOUT_MS,
      retryTimeoutMs: CATEGORY_PAGE_RETRY_TIMEOUT_MS,
      onRetry: (error) => {
        appendRunEvent('initial_page_retry', {
          inputUrl,
          timeoutMs: CATEGORY_PAGE_RETRY_TIMEOUT_MS,
          reason: error.message,
        })
      },
    })

    loadVisualHashCache()
    loadBitwiseHashCache()
    loadPermanentSkips()

    const breadcrumbInfo = await getBreadcrumbInfo(tempPage)
    const inferredRawName = extractModelNameFromBreadcrumb(breadcrumbInfo.texts)
    const aliasMapPath = path.join(__dirname, '..', 'model_aliases.json')
    const modelSelection = modelOverride
      ? {
          aliasName: modelOverride,
          canonicalName: modelOverride,
        }
      : await promptForModelSelection(aliasMapPath, inferredRawName)
    const rawName = modelSelection.aliasName
    const canonicalModelName = modelSelection.canonicalName

    if (modelOverride) {
      console.log(
        `🏷️ Using manual model override: ${modelOverride} (breadcrumb inferred ${inferredRawName || 'unknown_cow'})`
      )
    } else {
      console.log(
        `🏷️ Confirmed alias: ${rawName} -> bucket: ${canonicalModelName} (breadcrumb inferred ${inferredRawName || 'unknown_cow'})`
      )
    }

    modelName = resolveAndTrackRegistryModel(
      aliasMapPath,
      rawName,
      'stufferdb',
      inputUrl,
      canonicalModelName,
      { unknownName: 'unknown_cow' }
    )

    const folders = createModelFolders(modelName)
    startRunLog(modelName, inputUrl, folders)
    if (currentRunLog) {
      currentRunLog.keepHistory = keepHistory
    }

    if (isDirectPictureRun) {
      combinedTotal = 1
      resetProgressCounter(combinedTotal)
      appendRunEvent('direct_picture_run_built', {
        modelName,
        mediaPageUrl: inputUrl,
        inferredRawName,
        canonicalModelName,
        modelOverride: modelOverride || null,
      })
      console.log(`Direct picture scrape for ${modelName}: ${inputUrl}`)
      await scrapeDirectPicture(browser, inputUrl, modelName, folders)
    } else {
      const plainUrl = `https://stufferdb.com/index?/category/${categoryId}`
      categoryRunList = await buildCategoryRunList(browser, plainUrl)
      appendRunEvent('category_run_list_built', {
        modelName,
        categoryUrls: categoryRunList,
        inferredRawName,
        canonicalModelName,
        modelOverride: modelOverride || null,
      })

      console.log(`🗂️ Category run list for ${modelName}:`)
      for (const categoryUrl of categoryRunList) {
        console.log(`   - ${categoryUrl}`)
      }

      console.log('🔍 Prefetching total counts...')
      const categoryCounts = await Promise.all(
        categoryRunList.map((categoryUrl) =>
          fetchStufferDBTotalCount(browser, categoryUrl)
        )
      )

      combinedTotal =
        categoryCounts.reduce((sum, count) => sum + (count || 0), 0) || 1
      effectiveCombinedTotal = combinedTotal

      resetProgressCounter(combinedTotal)

      console.log(`📊 Combined media total: ${combinedTotal}`)
      console.log(`💦 Starting scrape for ${modelName}`)

      for (let i = 0; i < categoryRunList.length; i++) {
        const categoryUrl = categoryRunList[i]
        const categoryTotal = categoryCounts[i] || 0

        setProgressTotal(Math.max(completedTotal, effectiveCombinedTotal, 1))

        logScrollingMessage(`🍼 Scraping category: ${categoryUrl}`)
        logScrollingMessage(
          `📊 Category media total: ${categoryTotal || 'prefetch failed, will infer from page'}`
        )

        const categoryResult = await scrapeGallery(
          browser,
          categoryUrl,
          modelName,
          folders,
          {
            fullSourceRefresh,
            sourceIncrementalOverlapPages,
            categoryTotal,
          }
        )
        const handledCount = Number(categoryResult.handledCount || 0)
        const inspectedCount = Number(categoryResult.inspectedCount || 0)
        const emptyCompletedCategoryAdjustment =
          categoryResult.coverageComplete &&
          handledCount === 0 &&
          inspectedCount === 0
            ? Number(categoryTotal || 0)
            : 0
        effectiveCombinedTotal = Math.max(
          effectiveCombinedTotal -
            Number(categoryResult.unscannedCount || 0) -
            Math.max(inspectedCount - handledCount, 0) -
            emptyCompletedCategoryAdjustment,
          completedTotal,
          handledCount
        )
        pendingCategoryCheckpoints.push(categoryResult)
        setProgressTotal(effectiveCombinedTotal)
      }
    }

    logScrollingMessage('🧮 Scrape complete')

    totalLazyBytes = 0
    lazyBytesDownloaded = 0
    lazyActiveDownloads = 0
    lazyCompletedDownloads = 0
    lazyCurrentLabel = ''
    setRunLazyExpectedBytes(0)
    setRunLazyTransferredBytes(0)

    // Pre-fetch expected file sizes (best-effort)
    await Promise.all(
      lazyVideoQueue.map(async ({ url }) => {
        return new Promise((resolve) => {
          const proto = url.startsWith('https') ? https : http
          proto
            .get(url, { method: 'HEAD' }, (res) => {
              const size = parseInt(res.headers['content-length']) || 0
              totalLazyBytes += size
              setRunLazyExpectedBytes(totalLazyBytes)
              res.destroy()
              resolve()
            })
            .on('error', resolve)
        })
      })
    )

    if (lazyVideoQueue.length > 0) {
      logAndProgress(`Lazy videos queued: ${lazyVideoQueue.length}`)
      resetProgressBar(null, 'lazy')
      lastDraw = 0
      lazyDownloadStartedAt = Date.now()
      setProgressMode('lazy')
    }

    function drawLazyProgress() {
      if (!shouldDrawLazyProgress()) return
      const percent = totalLazyBytes
        ? (lazyBytesDownloaded / totalLazyBytes) * 100
        : 0
      const elapsedSeconds = lazyDownloadStartedAt
        ? Math.max((Date.now() - lazyDownloadStartedAt) / 1000, 0.001)
        : 0
      const speedBytesPerSecond = elapsedSeconds
        ? lazyBytesDownloaded / elapsedSeconds
        : 0
      const remainingBytes = Math.max(totalLazyBytes - lazyBytesDownloaded, 0)
      const etaSeconds =
        speedBytesPerSecond > 0 ? remainingBytes / speedBytesPerSecond : null

      logLazyProgress(percent, lazyBytesDownloaded, totalLazyBytes, {
        speedBytesPerSecond,
        etaSeconds,
        activeCount: lazyActiveDownloads,
        completedCount: lazyCompletedDownloads,
        totalCount: lazyVideoQueue.length,
        currentLabel: lazyCurrentLabel,
      })
    }

    drawLazyProgress()

    await Promise.all(
      lazyVideoQueue.map(
        (
          {
            url,
            path: finalPath,
            tmpPath,
            filename,
            uploadedDate,
            mediaPageUrl,
            mediaUrls,
            mediaPageUrls,
            sourceSite,
            sourceService,
            sourceUserId,
            sourceUsername,
            sourceSubreddit,
            postId,
            title,
            originalName,
            pageMeta,
          },
          i
        ) =>
          lazyLimit(async () => {
            const destinationExists = existsLocallyOrOnNas(finalPath)
            if (knownFilenames.has(filename) || destinationExists) {
              duplicateCount++
              runLifecycle.incrementRunCounter(currentRunLog, 'duplicates')
              const relativePath = getDatasetRelativePath(finalPath)
              recordRunReason('skipExistingVideo')
              appendRunEvent('skip_lazy_existing', {
                modelName,
                filename,
                savedPath: relativePath,
                quarantinedMirrorExists: isQuarantinedPath(finalPath),
              })
              if (destinationExists) {
                recordSuccessfulSeenMedia(
                  folders.logDir,
                  milkmaidMediaSaver.buildSeenRecord(
                    {
                      filename,
                      mediaUrl: url,
                      mediaUrls,
                      mediaPageUrl,
                      mediaPageUrls,
                      sourceSite,
                      sourceService,
                      sourceUserId,
                      sourceUsername,
                      sourceSubreddit,
                      postId,
                      uploadedDate,
                    },
                    { relativePath, filename }
                  )
                )
              }
              return logAndProgress(
                `♻️ Lazy dupe (pre-download): ${filename}`,
                true
              )
            }

            knownFilenames.add(filename) // ✅ Mark as claimed early
            lazyActiveDownloads++
            lazyCurrentLabel = filename
            drawLazyProgress()
            const quarantineMirrorPath = getQuarantineMirrorPath(finalPath)
            const traceLazyVideo = shouldTraceLazyVideo(filename, finalPath)
            const hadQuarantineMirror = fs.existsSync(quarantineMirrorPath)
            let bytesDownloadedForFile = 0
            let responseContentLength = 0
            let responseContentType = null
            let responseEtag = null
            let responseLastModified = null
            let responseEndedCleanly = false
            let responseWasAborted = false
            let responseCloseBeforeEnd = false

            if (hadQuarantineMirror && fs.existsSync(finalPath)) {
              const removed = removeFileIfExists(finalPath)
              appendRunEvent('repair_cleared_stale_dataset_copy', {
                modelName,
                filename,
                savedPath: getDatasetRelativePath(finalPath),
                removed,
              })
            }

            fs.mkdirSync(path.dirname(tmpPath), { recursive: true })
            const stream = fs.createWriteStream(tmpPath)
            let lastDraw = Date.now()

            try {
              if (traceLazyVideo) {
                appendRunEvent('lazy_video_trace_started', {
                  modelName,
                  filename,
                  url,
                  savedPath: getDatasetRelativePath(finalPath),
                  tmpPath: path.relative(rootDir, tmpPath).replace(/\\/g, '/'),
                  hadQuarantineMirror,
                })
              }

              await new Promise((resolve, reject) => {
                const proto = url.startsWith('https') ? https : http
                let req = null
                let settled = false
                let idleTimer = null

                const cleanup = () => {
                  if (idleTimer) {
                    clearTimeout(idleTimer)
                    idleTimer = null
                  }
                }

                const resetIdleTimer = () => {
                  cleanup()
                  idleTimer = setTimeout(() => {
                    if (settled) return
                    settled = true
                    req?.destroy(
                      new Error(
                        `No lazy download progress for ${LAZY_IDLE_TIMEOUT_MS}ms`
                      )
                    )
                    stream.destroy(
                      new Error(
                        `No lazy download progress for ${LAZY_IDLE_TIMEOUT_MS}ms`
                      )
                    )
                    reject(
                      new Error(
                        `No lazy download progress for ${LAZY_IDLE_TIMEOUT_MS}ms`
                      )
                    )
                  }, LAZY_IDLE_TIMEOUT_MS)
                }

                const rejectOnce = (error) => {
                  if (settled) return
                  settled = true
                  cleanup()
                  reject(error)
                }

                const resolveOnce = () => {
                  if (settled) return
                  settled = true
                  cleanup()
                  resolve()
                }

                stream.on('error', reject)
                req = proto.get(url, (res) => {
                  resetIdleTimer()

                  appendRunEvent('lazy_video_download_started', {
                    modelName,
                    filename,
                    url,
                    savedPath: getDatasetRelativePath(finalPath),
                  })

                  if (res.statusCode !== 200) {
                    res.resume()
                    return rejectOnce(new Error(`HTTP ${res.statusCode}`))
                  }

                  responseContentLength =
                    parseInt(res.headers['content-length'] || '0', 10) || 0
                  responseContentType = res.headers['content-type'] || null
                  responseEtag = res.headers.etag || null
                  responseLastModified = res.headers['last-modified'] || null

                  if (traceLazyVideo) {
                    appendRunEvent('lazy_video_trace_headers', {
                      modelName,
                      filename,
                      url,
                      statusCode: res.statusCode,
                      contentLength: responseContentLength,
                      contentType: responseContentType,
                      etag: responseEtag,
                      lastModified: responseLastModified,
                    })
                  }

                  res.on('data', (chunk) => {
                    resetIdleTimer()
                    stream.write(chunk)
                    lazyBytesDownloaded += chunk.length
                    setRunLazyTransferredBytes(lazyBytesDownloaded)
                    bytesDownloadedForFile += chunk.length

                    const now = Date.now()
                    if (now - lastDraw > 250) {
                      drawLazyProgress()
                      lastDraw = now
                    }
                  })

                  res.on('end', () => {
                    responseEndedCleanly = true
                    cleanup()
                    stream.end(resolveOnce)
                  })

                  res.on('aborted', () => {
                    responseWasAborted = true
                  })

                  res.on('close', () => {
                    if (!responseEndedCleanly) {
                      responseCloseBeforeEnd = true
                    }
                  })

                  res.on('error', (err) => {
                    stream.destroy(err)
                    rejectOnce(err)
                  })
                })
                req.setTimeout(LAZY_REQUEST_TIMEOUT_MS, () => {
                  responseWasAborted = true
                  req.destroy(
                    new Error(
                      `Lazy request timeout after ${LAZY_REQUEST_TIMEOUT_MS}ms`
                    )
                  )
                })
                req.on('error', rejectOnce)
              })

              const tmpIntegrity = await getVideoIntegrityReport(tmpPath)

              if (traceLazyVideo) {
                const remoteTail = await getRemoteVideoTailReport(url)
                appendRunEvent('lazy_video_trace_validation', {
                  modelName,
                  filename,
                  url,
                  bytesDownloaded: bytesDownloadedForFile,
                  responseContentLength,
                  responseContentType,
                  responseEtag,
                  responseLastModified,
                  responseEndedCleanly,
                  responseWasAborted,
                  responseCloseBeforeEnd,
                  tmpSize: fs.existsSync(tmpPath)
                    ? fs.statSync(tmpPath).size
                    : null,
                  probeOk: tmpIntegrity.probeOk,
                  duration: tmpIntegrity.duration,
                  streamCount: tmpIntegrity.streamCount,
                  tailDecodeOk: tmpIntegrity.tailDecodeOk,
                  tailDecodeCode: tmpIntegrity.tailDecodeCode,
                  tailDecodeError: tmpIntegrity.tailDecodeError,
                  probeError: tmpIntegrity.probeError,
                  probeStderr: tmpIntegrity.probeStderr,
                  tailDecodeStderr: tmpIntegrity.tailDecodeStderr,
                  remoteTailDecodeOk: remoteTail.ok,
                  remoteTailDecodeCode: remoteTail.code,
                  remoteTailDecodeError: remoteTail.error,
                  remoteTailDecodeStderr: remoteTail.stderr,
                })
              }

              const duration = tmpIntegrity.duration

              if (
                !tmpIntegrity.probeOk ||
                !tmpIntegrity.tailDecodeOk ||
                !Number.isFinite(duration) ||
                duration <= 0
              ) {
                throw new Error(
                  [
                    !tmpIntegrity.probeOk ? 'ffprobe_failed' : null,
                    !tmpIntegrity.tailDecodeOk ? 'tail_decode_error' : null,
                    !Number.isFinite(duration) || duration <= 0
                      ? 'invalid_duration'
                      : null,
                  ]
                    .filter(Boolean)
                    .join(',')
                )
              }
              const lazyEntry = {
                filename,
                kind: 'video',
                mediaUrl: url,
                mediaUrls,
                mediaPageUrl,
                mediaPageUrls,
                sourceSite,
                sourceService,
                sourceUserId,
                sourceUsername,
                sourceSubreddit,
                postId,
                title,
                originalName,
                uploadedDate,
                pageMeta,
              }
              const destination = milkmaidSavePipeline.getDestination({
                modelName,
                folders,
                entry: lazyEntry,
                kind: 'video',
              })
              const videoResult = await milkmaidSavePipeline.finalizeVideoFile({
                modelName,
                folders,
                entry: lazyEntry,
                destination,
                sourcePath: tmpPath,
                moveFileIntoPlace,
                hashFileFromPath,
                getVisualHashFromVideoPath,
                addBitwiseHash,
                addVisualHash,
                saveBitwiseHashCache,
                saveVisualHashCache,
              })
              const finalStat = { size: videoResult.sizeBytes }
              const finalHash = videoResult.hash
              const finalVisualHash = videoResult.visualHash
              const removedQuarantineMirror =
                removeQuarantineMirrorIfExists(finalPath)
              if (removedQuarantineMirror) {
                const repairedManifestEntry = updateQuarantineManifestForRepair(
                  finalPath,
                  {
                    hash: finalHash,
                    sizeBytes: finalStat.size,
                    durationSeconds: duration,
                    sourceUrl: url,
                    mediaPageUrl,
                  }
                )
                appendRunEvent('repair_cleared_quarantine_copy', {
                  modelName,
                  filename,
                  savedPath: getDatasetRelativePath(finalPath),
                  manifestUpdated: repairedManifestEntry,
                })
              }
              logAndProgress(`✅ Saved lazy video: ${filename}`)
            } catch (err) {
              addRunFailedBytes(bytesDownloadedForFile)
              addRunFailedLazyVideoBytes(bytesDownloadedForFile)
              const relativePath = getDatasetRelativePath(finalPath)
              const permanentFailure = getPermanentLazyVideoFailure(err.message)
              const skipReason = permanentFailure?.reason || 'lazy_video_error'
              let quarantinePath = getQuarantineMirrorPath(finalPath)
              if (fs.existsSync(tmpPath)) {
                if (permanentFailure && !permanentFailure.preservePartial) {
                  removeFileIfExists(tmpPath)
                } else {
                  quarantinePath = moveFailedLazyVideoToQuarantine(
                    tmpPath,
                    finalPath
                  )
                }
              }
              const addedPermanentSkip = permanentFailure
                ? addPermanentSkip({
                    relativePath,
                    sourceUrl: url,
                    mediaPageUrl,
                    filename,
                    reason: skipReason,
                    note: permanentFailure.note,
                  })
                : false
              if (permanentFailure) {
                duplicateCount++
                runLifecycle.incrementRunCounter(currentRunLog, 'duplicates')
                recordRunReason('skipPermanent')
              } else {
                errorCount++
                runLifecycle.incrementRunCounter(currentRunLog, 'failures')
                recordRunReason('lazyVideoError')
              }
              recordFailedSeenMedia(folders.logDir, {
                relativePath,
                filename,
                mediaUrl: url,
                mediaPageUrl,
                quarantinePath,
                error: err.message,
                bytesDownloaded: bytesDownloadedForFile,
                expectedBytes: responseContentLength,
              })
              const manifestUpdated = ensureQuarantineManifestEntry(finalPath, {
                reason: skipReason,
                reasons: [skipReason],
                outcome: permanentFailure ? 'permanent_skip' : 'failed',
                error: err.message,
                sourceUrl: url,
                mediaPageUrl,
                bytesDownloaded: bytesDownloadedForFile,
                expectedBytes: responseContentLength,
              })
              updateQuarantineManifestForRepairAttempt(finalPath, {
                outcome: permanentFailure ? 'permanent_skip' : 'failed',
                error: err.message,
                sourceUrl: url,
                mediaPageUrl,
                bytesDownloaded: bytesDownloadedForFile,
                expectedBytes: responseContentLength,
              })
              const eventDetails = {
                modelName,
                filename,
                mediaUrl: url,
                mediaPageUrl,
                savedPath: relativePath,
                error: err.message,
                bytesDownloaded: bytesDownloadedForFile,
                responseContentLength,
                responseEndedCleanly,
                responseWasAborted,
                responseCloseBeforeEnd,
                hadQuarantineMirror,
                quarantinePath,
                manifestUpdated: Boolean(manifestUpdated),
                addedPermanentSkip,
                reason: skipReason,
              }
              if (!permanentFailure) {
                recordRunError('lazy_video_error', eventDetails)
              }
              appendRunEvent(
                permanentFailure
                  ? 'lazy_video_permanent_skip'
                  : 'lazy_video_error',
                eventDetails
              )
              if (permanentFailure) {
                appendRunEvent('skip_permanent', {
                  ...eventDetails,
                  preNavigation: false,
                })
              }
              logAndProgress(
                permanentFailure
                  ? `Permanent video skip: ${filename} - ${err.message}`
                  : `❌ Lazy failed: ${filename} - ${err.message}`
              )
              if (fs.existsSync(finalPath)) fs.unlinkSync(finalPath)
              if (!permanentFailure) {
                knownFilenames.delete(filename) // allow retry in future runs
              }
            } finally {
              lazyActiveDownloads = Math.max(lazyActiveDownloads - 1, 0)
              lazyCompletedDownloads++
              if (lazyCurrentLabel === filename) {
                lazyCurrentLabel = ''
              }
              setRunLazyTransferredBytes(lazyBytesDownloaded)
              drawLazyProgress()
            }
          })
      )
    )

    if (errorCount === 0) {
      for (const categoryResult of pendingCategoryCheckpoints) {
        if (
          !categoryResult.coverageComplete ||
          categoryResult.unresolvedCount > 0
        ) {
          continue
        }

        const confirmedCandidate =
          categoryResult.checkpointCandidates.find((mediaPageUrl) =>
            getSuccessfulSeenMediaMatch(folders.logDir, mediaPageUrl, null)
          ) || categoryResult.existingCheckpoint?.mediaPageUrl
        if (!confirmedCandidate) continue

        const normalizedCandidate =
          normalizeStufferDbPictureUrl(confirmedCandidate)
        const checkpoint = recordSourceCheckpoint(
          folders.logDir,
          {
            site: 'stufferdb',
            service: 'category',
            userId: categoryResult.categoryId,
            rawName: modelName,
          },
          {
            id: getStufferDbPictureId(normalizedCandidate),
            mediaPageUrl: normalizedCandidate,
          }
        )
        appendRunEvent('category_checkpoint_recorded', {
          modelName,
          categoryId: categoryResult.categoryId,
          checkpointId: checkpoint?.id || null,
          mediaPageUrl: checkpoint?.mediaPageUrl || null,
        })
      }
    } else if (pendingCategoryCheckpoints.length > 0) {
      appendRunEvent('category_checkpoint_deferred', {
        modelName,
        reason: 'run_errors',
        errorCount,
      })
    }

    await browser.close()
    browser = null

    saveVisualHashCache()
    mediaDates.flushAllSidecars()

    await maybePauseForErrorReview(modelName, errorCount, reviewErrors)

    if (skipNasSync) {
      console.log('⏭️ NAS sync skipped by --skip-nas-sync')
    } else {
      await syncModelToNas({
        modelName,
        datasetDir,
        nasDatasetDir,
        successMessage: '✅ NAS sync complete.',
        failurePrefix: '❌ NAS sync failed with code',
      })
    }

    const finalStats = runLifecycle.getRunProgressStats(currentRunLog, {
      processed: completedTotal,
      expectedMedia: Number.isFinite(global.totalSearchTotal)
        ? global.totalSearchTotal
        : 1,
      saved: successCount,
      duplicates: duplicateCount,
      failures: errorCount,
    })
    const reasonSummaryLine = runLifecycle.formatRunReasonSummary(currentRunLog)
    appendRunEvent('run_finished', {
      successCount,
      duplicateCount,
      errorCount,
      queuedVideoCount: Number(currentRunLog?.counters?.queuedVideos || 0),
      savedBytes: Number(currentRunLog?.transfer?.savedBytes || 0),
      processedCount: finalStats.processed,
      expectedMedia: finalStats.expectedMedia,
      mediaCount: finalStats.expectedMedia,
      categoryRunList,
      combinedTotal,
      effectiveCombinedTotal,
      counters: currentRunLog?.counters || {},
      transfer: currentRunLog?.transfer || {},
    })
    console.log(
      `🎉 Done: ${finalStats.saved} saved, ${finalStats.duplicates} dupes, ${finalStats.failures} errors`
    )
    if (reasonSummaryLine) console.log(reasonSummaryLine)
    return 0
  } catch (err) {
    recordRunError('run_error', {
      modelName,
      inputUrl: currentRunLog?.inputUrl || null,
      error: err.message,
    })
    appendRunEvent('run_error', {
      modelName,
      error: err.message,
    })
    throw err
  } finally {
    if (browser) {
      await browser.close().catch(() => {})
    }

    if (currentRunLog) {
      finalizeRunLog({
        successCount,
        duplicateCount,
        errorCount,
        categoryRunList,
        combinedTotal,
      })
    }

    mediaDates.flushAllSidecars()
  }
}

async function runMilkmaidCli(argvInput = process.argv.slice(2)) {
  try {
    return await runMilkmaidScrape(argvInput)
  } catch (err) {
    console.error(`Milkmaid failed: ${err.message}`)
    return 1
  }
}

module.exports = {
  getActiveStufferDbCheckpoint,
  getPermanentLazyVideoFailure,
  normalizeMilkmaidRunOptions,
  parseCliArgs,
  runMilkmaidScrape,
  runMilkmaidCli,
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

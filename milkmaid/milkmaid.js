const { executablePath } = require('puppeteer')
const puppeteer = require('puppeteer')
const fs = require('fs')
const path = require('path')
const { exec, spawn } = require('child_process')
const { createHash } = require('crypto')
const https = require('https')
const http = require('http')
const minimist = require('minimist')
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
const mediaDates = require('./media-dates.js')
bannerMilkmaid()

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
const { writeRepoJsonFileSync } = require('../scrapyard/repoFileWriter')
const {
  mergeNasMp4Entries,
  collectMp4RelativePaths,
  syncNasMp4IndexToMirror,
} = require('../scrapyard/nasMp4Index')
const { createDatasetPaths } = require('../scrapyard/datasetPaths')
const { createMediaSeenIndex } = require('../scrapyard/mediaSeenIndex')
const mediaFileRecords = require('../scrapyard/mediaFileRecords')
const { createMediaSaver } = require('../scrapyard/mediaSaver')
const { createDuplicateChecker } = require('../scrapyard/duplicateChecker')
const {
  buildCategoryRunList: buildStufferDbCategoryRunList,
  collectChildCategoryUrls: collectStufferDbChildCategoryUrls,
  extractGalleryPictureUrls,
  extractMediaPageDetails,
  getBreadcrumbInfo: getStufferDbBreadcrumbInfo,
  getStufferDbCategoryId,
  normalizeStufferDbCategoryUrl,
  normalizeStufferDbPictureUrl,
} = require('../scrapyard/sourceAdapters/stufferdb')

function sanitize(name) {
  return String(name || '')
    .replace(/[^a-z0-9_\-]/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase()
}

function loadModelRegistry(registryPath) {
  if (!fs.existsSync(registryPath)) {
    const emptyRegistry = {}
    writeRepoJsonFileSync(registryPath, emptyRegistry)
    return emptyRegistry
  }

  try {
    const raw = fs.readFileSync(registryPath, 'utf-8').trim()
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch (err) {
    console.warn(
      `⚠️ Could not parse model registry at ${registryPath}: ${err.message}`
    )
    return {}
  }
}

function saveModelRegistry(registryPath, registry) {
  writeRepoJsonFileSync(registryPath, sortModelRegistry(registry))
}

function sortStringValues(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b)
  )
}

function sortSourceEntries(sources) {
  return [...(Array.isArray(sources) ? sources : [])].sort((a, b) => {
    const left =
      String(a?.discoveredAs || '') ||
      String(a?.userId || '') ||
      String(a?.categoryId || '') ||
      String(a?.url || '')
    const right =
      String(b?.discoveredAs || '') ||
      String(b?.userId || '') ||
      String(b?.categoryId || '') ||
      String(b?.url || '')
    return left.localeCompare(right)
  })
}

function sortSourcesObject(sources) {
  return Object.fromEntries(
    Object.entries(sources && typeof sources === 'object' ? sources : {})
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([sourceName, entries]) => [sourceName, sortSourceEntries(entries)])
  )
}

function sortModelRegistry(registry) {
  return Object.fromEntries(
    Object.entries(registry || {})
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([canonicalName, entry]) => [
        canonicalName,
        {
          aliases: sortStringValues(entry?.aliases),
          sources: sortSourcesObject(entry?.sources),
        },
      ])
  )
}

function ensureModelEntryShape(entry, canonicalName) {
  const aliasSet = new Set(
    Array.isArray(entry?.aliases) ? entry.aliases.filter(Boolean) : []
  )

  if (canonicalName) aliasSet.add(canonicalName)

  const existingSources =
    entry?.sources && typeof entry.sources === 'object' ? entry.sources : {}
  const nextSources = Object.fromEntries(
    Object.entries(existingSources).map(([sourceName, sources]) => [
      sourceName,
      Array.isArray(sources) ? [...sources] : [],
    ])
  )
  if (!Array.isArray(nextSources.stufferdb)) nextSources.stufferdb = []

  return {
    aliases: Array.from(aliasSet),
    sources: nextSources,
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

function upsertStufferSource(entry, sourceUrl, rawName) {
  const cleanedUrl = String(sourceUrl || '').replace(/&acs=[^&]+/gi, '')
  const categoryId = cleanedUrl.match(/category\/?(\d+)/)?.[1] || null
  const now = new Date().toISOString()

  if (!entry.sources) entry.sources = {}
  if (!Array.isArray(entry.sources.stufferdb)) entry.sources.stufferdb = []

  const sourceIndex = entry.sources.stufferdb.findIndex(
    (source) =>
      source?.url === cleanedUrl ||
      (categoryId && source?.categoryId === categoryId)
  )

  const nextSource = {
    url: cleanedUrl,
    categoryId,
    discoveredAs: rawName,
    lastCheckedAt: now,
  }

  if (sourceIndex >= 0) {
    entry.sources.stufferdb[sourceIndex] = {
      ...entry.sources.stufferdb[sourceIndex],
      ...nextSource,
    }
  } else {
    entry.sources.stufferdb.push(nextSource)
  }
}

function resolveAndTrackModel(
  registryPath,
  rawName,
  sourceUrl,
  canonicalOverride
) {
  const registry = loadModelRegistry(registryPath)
  const cleanedRawName = sanitize(rawName) || 'unknown_cow'
  const cleanedCanonicalOverride = sanitize(canonicalOverride)
  const existingCanonical = cleanedCanonicalOverride
    ? findCanonicalModelName(registry, cleanedCanonicalOverride)
    : findCanonicalModelName(registry, cleanedRawName)
  const canonicalName =
    existingCanonical || cleanedCanonicalOverride || cleanedRawName

  registry[canonicalName] = ensureModelEntryShape(
    registry[canonicalName],
    canonicalName
  )

  const aliases = registry[canonicalName].aliases
  if (!aliases.some((alias) => sanitize(alias) === cleanedRawName)) {
    aliases.push(cleanedRawName)
  }

  registry[canonicalName].aliases = Array.from(
    new Set(aliases.filter(Boolean))
  ).sort((a, b) => a.localeCompare(b))

  upsertStufferSource(registry[canonicalName], sourceUrl, cleanedRawName)
  saveModelRegistry(registryPath, registry)

  return canonicalName
}

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

function parseCliArgs(argv) {
  const args = minimist(argv, {
    string: ['model'],
    boolean: ['review-errors', 'skip-nas-sync', 'keep-history'],
    alias: {
      m: 'model',
    },
  })

  return {
    inputUrl: args._[0] || '',
    modelOverride: sanitize(args.model || ''),
    reviewErrors: Boolean(args['review-errors']),
    skipNasSync: Boolean(args['skip-nas-sync']),
    keepHistory: Boolean(args['keep-history']),
  }
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
  try {
    await page.goto(url, {
      waitUntil,
      timeout: timeoutMs,
    })
    return
  } catch (error) {
    if (!/Navigation timeout/i.test(error.message || '')) {
      throw error
    }

    if (typeof onRetry === 'function') {
      onRetry(error)
    }

    await new Promise((resolve) => setTimeout(resolve, retryDelayMs))
    await page.goto(url, {
      waitUntil,
      timeout: retryTimeoutMs ?? timeoutMs,
    })
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
const milkmaidMediaSaver = createMediaSaver({
  datasetDir,
  source: 'milkmaid',
  mediaDates,
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
let mediaSeenIndexCache = null

if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true })

function addRunSavedBytes(bytes) {
  if (!currentRunLog) return
  currentRunLog.transfer.savedBytes += Number(bytes) || 0
}

function addRunFailedBytes(bytes) {
  if (!currentRunLog) return
  currentRunLog.transfer.failedBytes += Number(bytes) || 0
}

function addRunFailedLazyVideoBytes(bytes) {
  if (!currentRunLog) return
  currentRunLog.transfer.failedLazyVideoBytes += Number(bytes) || 0
}

function setRunLazyExpectedBytes(bytes) {
  if (!currentRunLog) return
  currentRunLog.transfer.lazyExpectedBytes = Number(bytes) || 0
}

function setRunLazyTransferredBytes(bytes) {
  if (!currentRunLog) return
  currentRunLog.transfer.lazyTransferredBytes = Number(bytes) || 0
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
  const stamp = new Date().toISOString().replace(/[:.]/g, '-')
  const logPath = path.join(folders.logDir, `milkmaid-run-${stamp}.jsonl`)
  const summaryPath = path.join(
    folders.logDir,
    'milkmaid-run-latest-summary.json'
  )
  const modelSummaryPath = path.join(folders.base, 'milkmaid-last-run.json')
  currentRunLog = {
    stamp,
    logPath,
    summaryPath,
    modelSummaryPath,
    modelName,
    inputUrl,
    keepHistory: false,
    startedAt: new Date().toISOString(),
    counters: {
      saved: 0,
      duplicates: 0,
      queuedVideos: 0,
      convertedGifs: 0,
      failures: 0,
    },
    transfer: {
      savedBytes: 0,
      failedBytes: 0,
      failedLazyVideoBytes: 0,
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

  fs.writeFileSync(currentRunLog.summaryPath, JSON.stringify(summary, null, 2))
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
  if (!shouldKeepHistory) {
    removeFileIfExists(currentRunLog.logPath)
  }
  currentRunLog = null
}

function getMediaSeenIndexPath(modelLogDir) {
  return sharedMediaSeenIndex.getMediaSeenIndexPath(modelLogDir)
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
      console.warn(
        `⚠️ Could not parse media seen index at ${indexPath}: ${err.message}`
      )
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
  }

  mediaSeenIndexCache = { indexPath, data }
  return data
}

function saveMediaSeenIndex(modelLogDir, data) {
  const indexPath = getMediaSeenIndexPath(modelLogDir)
  data.updatedAt = new Date().toISOString()
  fs.writeFileSync(indexPath, JSON.stringify(data, null, 2) + '\n')
  mediaSeenIndexCache = { indexPath, data }
}

function recordSeenMedia(modelLogDir, details = {}) {
  const relativePath = String(details.relativePath || '').trim()
  if (!relativePath) return

  const index = loadMediaSeenIndex(modelLogDir)
  const normalizedMediaPageUrl = normalizeSkipUrl(details.mediaPageUrl)
  const normalizedMediaUrl = normalizeSkipUrl(details.mediaUrl)
  const status = String(details.status || 'saved').trim() || 'saved'
  const recordedAt = new Date().toISOString()
  const payload = {
    relativePath,
    filename: details.filename || path.basename(relativePath),
    mediaUrl: normalizedMediaUrl || null,
    mediaPageUrl: normalizedMediaPageUrl || null,
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

  if (normalizedMediaPageUrl) {
    index.mediaPageUrls[normalizedMediaPageUrl] = payload
  }
  if (normalizedMediaUrl) {
    index.mediaUrls[normalizedMediaUrl] = payload
  }

  saveMediaSeenIndex(modelLogDir, index)
}

function getActiveMediaSeenRecord(modelLogDir, entry) {
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

function getSuccessfulSeenMediaMatch(modelLogDir, mediaPageUrl, mediaUrl) {
  const index = loadMediaSeenIndex(modelLogDir)
  const normalizedMediaPageUrl = normalizeSkipUrl(mediaPageUrl)
  const normalizedMediaUrl = normalizeSkipUrl(mediaUrl)

  if (normalizedMediaPageUrl) {
    const pageEntry = getActiveMediaSeenRecord(
      modelLogDir,
      index.mediaPageUrls[normalizedMediaPageUrl]
    )
    if (pageEntry) {
      return {
        matchType: 'media_page_url',
        ...pageEntry,
      }
    }
  }

  if (normalizedMediaUrl) {
    const mediaEntry = getActiveMediaSeenRecord(
      modelLogDir,
      index.mediaUrls[normalizedMediaUrl]
    )
    if (mediaEntry) {
      return {
        matchType: 'media_url',
        ...mediaEntry,
      }
    }
  }

  return null
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
          if (onProgress && totalBytes > 0) {
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

async function extractStufferDbComments(page) {
  const commentsHostFrame = page
    .frames()
    .find((frame) => frame.url().includes('cmts.stufferdb.com/app'))

  if (!commentsHostFrame) {
    return { comments: [], commentCount: 0 }
  }

  try {
    await commentsHostFrame.waitForSelector(
      '#comments_list, .comment, .allcomments',
      {
        timeout: 5000,
      }
    )
  } catch {
    return { comments: [], commentCount: 0 }
  }

  try {
    return await commentsHostFrame.evaluate(() => {
      const countText =
        document.querySelector('.allcomments')?.textContent?.trim() || ''
      const countMatch = countText.match(/(\d+)/)
      const commentCount = countMatch ? Number.parseInt(countMatch[1], 10) : 0

      const comments = Array.from(
        document.querySelectorAll('#comments_list .comment')
      )
        .map((commentEl) => {
          const author =
            commentEl
              .querySelector('.comment-top .user-guest, .comment-top .user')
              ?.textContent?.trim() || null
          const posted =
            commentEl
              .querySelector('.comment-top .date')
              ?.textContent?.replace(/^•\s*/, '')
              .trim() || null
          const spoilerText =
            commentEl
              .querySelector('.comment-spoiler-text')
              ?.textContent?.trim() || ''
          const mainText =
            commentEl
              .querySelector('.comment-text-p, .comment-text, .comment-body')
              ?.textContent?.trim() || ''
          const text = [spoilerText, mainText].filter(Boolean).join('\n').trim()

          if (!text) return null

          return {
            author,
            posted,
            text,
          }
        })
        .filter(Boolean)

      return {
        comments,
        commentCount: Number.isFinite(commentCount)
          ? commentCount
          : comments.length,
      }
    })
  } catch {
    return { comments: [], commentCount: 0 }
  }
}

async function fetchStufferDBTotalCount(browser, url) {
  const tempPage = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: true,
  })

  try {
    await tempPage.goto(url, {
      waitUntil: 'domcontentloaded',
      timeout: 30000,
    })

    await tempPage.waitForSelector('span.badge.nb_items', {
      timeout: 10000,
    })

    const rawText = await tempPage.$eval(
      'span.badge.nb_items',
      (el) => el.textContent || ''
    )
    console.log(`🕵️ Raw badge text from ${url}:`, rawText)

    const match = rawText.match(/(\d+)/)
    const count = match ? parseInt(match[1], 10) : 0
    console.log(`🔢 Parsed count: ${count}`)
    return count
  } catch (err) {
    const title = await tempPage.title().catch(() => 'unknown')
    console.log(`⚠️ Could not fetch count for ${url}: ${err.message}`)
    console.log(`🧙 Page title: ${title}`)
    return 0
  } finally {
    if (!tempPage.isClosed()) await tempPage.close()
  }
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

function removeFileIfExists(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return false
  fs.unlinkSync(filePath)
  return true
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
}

function logAndProgress(message, increment = false) {
  if (increment) {
    completedTotal++
  }

  logScrollingMessage(message)

  if (progressMode === 'lazy') {
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

function getScrapeStatsLine() {
  const parts = [
    `${successCount} saved`,
    `${duplicateCount} dupes`,
    `${errorCount} errors`,
  ]

  if (lazyVideoQueue.length > 0) {
    parts.push(`${lazyVideoQueue.length} queued`)
  }

  return parts.join(' | ')
}

function setProgressTotal(total = null) {
  if (typeof total === 'number' && !Number.isNaN(total)) {
    global.totalSearchTotal = Math.max(total, 1)
  }
}

async function scrapeGallery(browser, url, modelName, folders) {
  const { base, images, webm } = folders

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
      const dedupedUrls = galleryLinks.urls

      const total = dedupedUrls.length

      // If prefetch undercounted, keep the rollup large enough to cover
      // what we have already processed plus what remains visible right now.
      setProgressTotal(
        Math.max(global.totalSearchTotal || 1, completedTotal + total)
      )

      const mode = url.includes('&acs=') ? 'ACS' : 'PLAIN'
      logAndProgress(
        `📸 ${modelName} - [${mode}] - ${dedupedUrls.length} media links (tracking ${global.totalSearchTotal})`
      )
      appendRunEvent('category_page_loaded', {
        modelName,
        categoryUrl: url,
        mode,
        mediaLinks: dedupedUrls.length,
        rawMediaLinks: urls.length,
        trackedTotal: global.totalSearchTotal || 0,
      })

      const pages = await Promise.all(
        Array.from({ length: MEDIA_PAGE_CONCURRENCY }, () =>
          createScraperPage(browser, {
            site: 'stufferdb',
            interceptMedia: false,
          })
        )
      )

      let pageIndex = 0

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
            currentRunLog && currentRunLog.counters.duplicates++
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
            currentRunLog && currentRunLog.counters.duplicates++
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

          try {
            await page.goto(mediaPageUrl, {
              waitUntil: 'domcontentloaded',
              timeout: MEDIA_PAGE_TIMEOUT_MS,
            })
          } catch (error) {
            if (!/Navigation timeout/i.test(error.message || '')) {
              throw error
            }

            appendRunEvent('media_page_retry', {
              modelName,
              mediaPageUrl,
              attempt: 2,
              timeoutMs: MEDIA_PAGE_RETRY_TIMEOUT_MS,
              reason: error.message,
            })

            await sleep(750)
            await page.goto(mediaPageUrl, {
              waitUntil: 'domcontentloaded',
              timeout: MEDIA_PAGE_RETRY_TIMEOUT_MS,
            })
          }

          const mediaDetails = await extractMediaPageDetails(page)
          const uploadedDateIso = mediaDetails.uploadedDateIso

          const uploadedDate = uploadedDateIso
            ? resolveEffectiveFileDate(new Date(uploadedDateIso), null)
            : null
          const pageMeta = mediaDetails.pageMeta

          mediaUrl = mediaDetails.mediaUrl
          if (!mediaUrl) return

          filename = mediaDetails.filename
          ext = mediaDetails.extension

          if (isNuisanceMediaAsset(filename, ext)) {
            appendRunEvent('skip_nuisance_media', {
              modelName,
              mediaPageUrl,
              mediaUrl,
              filename,
              extension: ext,
            })
            return logAndProgress(
              `🚫 Skipped nuisance asset: ${filename}`,
              true
            )
          }

          const bucketName =
            ext === '.gif'
              ? 'gif'
              : ['.mp4', '.webm'].includes(ext)
                ? 'webm'
                : 'images'
          const candidateRelativePath = `${modelName}/${bucketName}/${filename}`

          appendRunEvent('media_seen', {
            modelName,
            mediaPageUrl,
            mediaUrl,
            filename,
            extension: ext,
          })

          const permanentSkipMatch = getPermanentSkipMatch({
            relativePath: candidateRelativePath,
            mediaUrl,
            mediaPageUrl,
            filename,
          })
          if (permanentSkipMatch) {
            duplicateCount++
            currentRunLog && currentRunLog.counters.duplicates++
            appendRunEvent('skip_permanent', {
              modelName,
              filename,
              relativePath: candidateRelativePath,
              mediaUrl,
              mediaPageUrl,
              reason: permanentSkipMatch.reason || 'manual_skip',
              note: permanentSkipMatch.note || null,
            })
            return logAndProgress(`🛑 Permanent skip: ${filename}`, true)
          }

          const seenMediaMatch = getSuccessfulSeenMediaMatch(
            folders.logDir,
            mediaPageUrl,
            mediaUrl
          )
          if (seenMediaMatch) {
            duplicateCount++
            currentRunLog && currentRunLog.counters.duplicates++
            appendRunEvent('skip_seen_media', {
              modelName,
              filename,
              mediaUrl,
              mediaPageUrl,
              matchType: seenMediaMatch.matchType,
              savedPath: seenMediaMatch.relativePath,
            })
            return logAndProgress(
              `⏩ Seen media skip (${seenMediaMatch.matchType}): ${filename}`,
              true
            )
          }

          let buffer = null
          let hash = null
          let visualHash = null

          // Step 1: Fetch file buffer
          if (!['.mp4', '.webm', '.gif'].includes(ext)) {
            buffer = await downloadBufferWithProgress(mediaUrl)

            // Step 2: Bitwise (fast) hash
            hash = createHash('md5').update(buffer).digest('hex')
            const bitwiseMatch = getBitwiseDuplicationRecord(hash)
            if (bitwiseMatch.isDuplicate) {
              duplicateCount++
              currentRunLog && currentRunLog.counters.duplicates++
              appendRunEvent('duplicate_bitwise', {
                modelName,
                filename,
                hash,
                activeRefs: bitwiseMatch.activeRefs.slice(0, 5),
              })
              return logAndProgress(`♻️ Bitwise dupe: ${filename}`, true)
            }

            // Step 3: Visual (slow) hash
            visualHash = await getVisualHashFromBuffer(buffer)
            const visualMatch = visualHash
              ? getVisualDuplicationRecord(visualHash)
              : null
            if (visualMatch?.isDuplicate) {
              duplicateCount++
              currentRunLog && currentRunLog.counters.duplicates++
              appendRunEvent('duplicate_visual', {
                modelName,
                filename,
                visualHash,
                activeRefs: visualMatch.activeRefs.slice(0, 5),
              })
              return logAndProgress(
                `👁️ Visual dupe (global): ${filename}`,
                true
              )
            }
            if (visualHash) addVisualHash(visualHash)
          }

          if (ext === '.gif') {
            buffer = await downloadBufferWithProgress(mediaUrl)
            hash = createHash('md5').update(buffer).digest('hex')

            const gifFolder = folders.createGifFolder()
            const gifPath = path.join(gifFolder, filename)

            if (knownFilenames.has(filename) || existsLocallyOrOnNas(gifPath)) {
              duplicateCount++
              currentRunLog && currentRunLog.counters.duplicates++
              appendRunEvent('skip_existing_gif', {
                modelName,
                filename,
                savedPath: getDatasetRelativePath(gifPath),
                quarantinedMirrorExists: isQuarantinedPath(gifPath),
              })
              return logAndProgress(
                `â™»ï¸ Skipped gif (exists): ${filename}`,
                true
              )
            }

            fs.writeFileSync(gifPath, buffer)

            const recordedDate = await milkmaidMediaSaver.recordImageDates({
              modelName,
              bucket: 'gif',
              filename,
              buffer,
              uploadedDate,
              pageMeta,
            })
            const fileDate = milkmaidMediaSaver.applyRecordedTimestamp(
              gifPath,
              recordedDate,
              uploadedDate
            )

            if (!isBitwiseDupe(hash)) {
              addBitwiseHash(
                hash,
                buildHashMetadata(
                  modelName,
                  gifPath,
                  'gif',
                  buffer.length,
                  fileDate
                )
              )
              saveBitwiseHashCache()
            }

            knownFilenames.add(filename)
            successCount++
            currentRunLog && currentRunLog.counters.saved++
            addRunSavedBytes(buffer.length)
            recordSuccessfulSeenMedia(folders.logDir, {
              relativePath: getDatasetRelativePath(gifPath),
              filename,
              mediaUrl,
              mediaPageUrl,
            })
            appendRunEvent('saved_gif', {
              modelName,
              filename,
              savedPath: getDatasetRelativePath(gifPath),
              hash,
            })
            return logAndProgress(`Saved gif: ${filename}`, true)
          }

          if (['.mp4', '.webm'].includes(ext)) {
            const webmFolder = folders.createWebmFolder() // Create only when needed
            const finalPath = path.join(webmFolder, filename)

            if (
              knownFilenames.has(filename) ||
              existsLocallyOrOnNas(finalPath)
            ) {
              duplicateCount++
              currentRunLog && currentRunLog.counters.duplicates++
              appendRunEvent('skip_existing_video', {
                modelName,
                filename,
                savedPath: getDatasetRelativePath(finalPath),
                quarantinedMirrorExists: isQuarantinedPath(finalPath),
              })
              return logAndProgress(
                `⛔ Skipping mp4 – already handled: ${filename}`,
                true
              )
            }

            const tmpPath = path.join(folders.incompleteVideoDir, filename)

            lazyVideoQueue.push({
              url: mediaUrl,
              path: finalPath,
              tmpPath,
              filename,
              uploadedDate,
              mediaPageUrl,
              pageMeta,
            })
            currentRunLog && currentRunLog.counters.queuedVideos++
            appendRunEvent('queued_lazy_video', {
              modelName,
              filename,
              mediaUrl,
              savedPath: getDatasetRelativePath(finalPath),
            })

            return logAndProgress(`🐌 Queued lazy video: ${filename}`, true)
          }

          if (
            knownFilenames.has(filename) ||
            existsLocallyOrOnNas(path.join(images, filename))
          ) {
            duplicateCount++
            currentRunLog && currentRunLog.counters.duplicates++
            appendRunEvent('skip_existing_image', {
              modelName,
              filename,
              savedPath: getDatasetRelativePath(path.join(images, filename)),
              quarantinedMirrorExists: isQuarantinedPath(
                path.join(images, filename)
              ),
            })
            return logAndProgress(`♻️ Skipped (exists): ${filename}`, true)
          }

          buffer = await downloadBufferWithProgress(mediaUrl)
          hash = createHash('md5').update(buffer).digest('hex')

          const finalPath = path.join(images, filename)
          fs.writeFileSync(finalPath, buffer)

          const recordedDate = await milkmaidMediaSaver.recordImageDates({
            modelName,
            bucket: 'images',
            filename,
            buffer,
            uploadedDate,
            pageMeta,
          })
          const fileDate = milkmaidMediaSaver.applyRecordedTimestamp(
            finalPath,
            recordedDate,
            uploadedDate
          )

          if (!isBitwiseDupe(hash)) {
            addBitwiseHash(
              hash,
              buildHashMetadata(
                modelName,
                finalPath,
                'image',
                buffer.length,
                fileDate
              )
            )
            saveBitwiseHashCache()
          }

          if (visualHash) {
            addVisualHash(
              visualHash,
              buildHashMetadata(
                modelName,
                finalPath,
                'image',
                buffer.length,
                fileDate
              )
            )
          }
          knownFilenames.add(filename)
          successCount++
          currentRunLog && currentRunLog.counters.saved++
          addRunSavedBytes(buffer.length)
          recordSuccessfulSeenMedia(folders.logDir, {
            relativePath: getDatasetRelativePath(finalPath),
            filename,
            mediaUrl,
            mediaPageUrl,
          })
          appendRunEvent('saved_image', {
            modelName,
            filename,
            savedPath: getDatasetRelativePath(finalPath),
            hash,
            visualHash,
          })
          return logAndProgress(`✅ Saved: ${filename}`, true)
        } catch (err) {
          errorCount++
          currentRunLog && currentRunLog.counters.failures++
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

      await Promise.all(
        dedupedUrls.map((mediaPageUrl, i) => {
          const page = pages[i % pages.length]
          const lock = pageLocks[i % pageLocks.length]

          return limit(() =>
            lock(() => scrapeMediaOnPage(page, mediaPageUrl, i))
          )
        })
      )

      const nextHref = await page
        .$eval('a[rel="next"]', (el) => el?.href)
        .catch(() => null)
      if (nextHref) {
        const baseUrl = new URL(url)
        url = new URL(nextHref, baseUrl).href
      } else {
        break
      }
    }
  } finally {
    readline.clearLine(process.stdout, 0)
    readline.cursorTo(process.stdout, 0)
    logAndProgress(getCompletionLine())

    await page.close()
  }
}

;(async () => {
  let browser = null
  let modelName = null
  let categoryRunList = []
  let combinedTotal = 0

  try {
    const {
      inputUrl: initialInputUrl,
      modelOverride,
      reviewErrors,
      skipNasSync,
      keepHistory,
    } = parseCliArgs(process.argv.slice(2))
    let inputUrl = initialInputUrl
    if (!inputUrl || !inputUrl.includes('/category/'))
      return logAndProgress('⚠️  Usage: node milkmaid.js <gallery-url>')

    inputUrl = inputUrl.replace(/&acs=[^&]+/i, '')

    const categoryId = getStufferDbCategoryId(inputUrl)
    if (!categoryId) return logAndProgress('❌ Invalid category URL')

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
    await tempPage.goto(inputUrl, { waitUntil: 'domcontentloaded' })

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

    modelName = resolveAndTrackModel(
      aliasMapPath,
      rawName,
      inputUrl,
      canonicalModelName
    )

    const folders = createModelFolders(modelName)

    const plainUrl = `https://stufferdb.com/index?/category/${categoryId}`
    categoryRunList = await buildCategoryRunList(browser, plainUrl)
    startRunLog(modelName, inputUrl, folders)
    if (currentRunLog) {
      currentRunLog.keepHistory = keepHistory
    }
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

    resetProgressCounter(combinedTotal)

    console.log(`📊 Combined media total: ${combinedTotal}`)
    console.log(`💦 Starting scrape for ${modelName}`)

    for (let i = 0; i < categoryRunList.length; i++) {
      const categoryUrl = categoryRunList[i]
      const categoryTotal = categoryCounts[i] || 0

      setProgressTotal(Math.max(global.totalSearchTotal || 1, combinedTotal))

      logScrollingMessage(`🍼 Scraping category: ${categoryUrl}`)
      logScrollingMessage(
        `📊 Category media total: ${categoryTotal || 'prefetch failed, will infer from page'}`
      )

      await scrapeGallery(browser, categoryUrl, modelName, folders)
    }

    logAndProgress('🧮 Scrape complete')

    logAndProgress(`Lazy downloading videos: ${lazyVideoQueue.length}`)
    resetProgressBar(null, 'lazy')
    lastDraw = 0
    totalLazyBytes = 0
    lazyBytesDownloaded = 0
    lazyDownloadStartedAt = Date.now()
    lazyActiveDownloads = 0
    lazyCompletedDownloads = 0
    lazyCurrentLabel = ''
    setRunLazyExpectedBytes(0)
    setRunLazyTransferredBytes(0)
    setProgressMode('lazy')

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

    function drawLazyProgress() {
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
            pageMeta,
          },
          i
        ) =>
          lazyLimit(async () => {
            if (
              knownFilenames.has(filename) ||
              existsLocallyOrOnNas(finalPath)
            ) {
              duplicateCount++
              currentRunLog && currentRunLog.counters.duplicates++
              appendRunEvent('skip_lazy_existing', {
                modelName,
                filename,
                savedPath: getDatasetRelativePath(finalPath),
                quarantinedMirrorExists: isQuarantinedPath(finalPath),
              })
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

              moveFileIntoPlace(tmpPath, finalPath)
              const recordedDate = await milkmaidMediaSaver.recordVideoDates({
                modelName,
                bucket: 'webm',
                filename,
                filePath: finalPath,
                uploadedDate,
                pageMeta,
              })
              const fileDate = milkmaidMediaSaver.applyRecordedTimestamp(
                finalPath,
                recordedDate,
                uploadedDate
              )

              const finalStat = fs.statSync(finalPath)
              const finalHash = await hashFileFromPath(finalPath)
              addBitwiseHash(
                finalHash,
                buildHashMetadata(
                  modelName,
                  finalPath,
                  'video',
                  finalStat.size,
                  fileDate
                )
              )
              saveBitwiseHashCache()

              const finalVisualHash =
                await getVisualHashFromVideoPath(finalPath)
              if (finalVisualHash) {
                addVisualHash(
                  finalVisualHash,
                  buildHashMetadata(
                    modelName,
                    finalPath,
                    'video',
                    finalStat.size,
                    fileDate
                  )
                )
                saveVisualHashCache()
              }

              successCount++
              currentRunLog && currentRunLog.counters.saved++
              addRunSavedBytes(finalStat.size)
              recordSuccessfulSeenMedia(folders.logDir, {
                relativePath: getDatasetRelativePath(finalPath),
                filename,
                mediaUrl: url,
                mediaPageUrl,
              })
              appendRunEvent('saved_lazy_video', {
                modelName,
                filename,
                savedPath: getDatasetRelativePath(finalPath),
                hash: finalHash,
                visualHash: finalVisualHash,
              })
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
              errorCount++
              currentRunLog && currentRunLog.counters.failures++
              addRunFailedBytes(bytesDownloadedForFile)
              addRunFailedLazyVideoBytes(bytesDownloadedForFile)
              const relativePath = getDatasetRelativePath(finalPath)
              const structuralFailure = isStructuralLazyVideoFailure(
                err.message
              )
              const quarantinePath = fs.existsSync(tmpPath)
                ? moveFailedLazyVideoToQuarantine(tmpPath, finalPath)
                : getQuarantineMirrorPath(finalPath)
              const skipReason = String(err.message || '').includes(
                'tail_decode_error'
              )
                ? 'upstream_tail_decode_error'
                : 'lazy_video_error'
              const addedPermanentSkip = structuralFailure
                ? addPermanentSkip({
                    relativePath,
                    sourceUrl: url,
                    mediaPageUrl,
                    filename,
                    reason: skipReason,
                    note:
                      skipReason === 'upstream_tail_decode_error'
                        ? 'Fully downloaded but tail decode failed; skipping future reruns unless manually cleared.'
                        : `Lazy video failed during validation: ${err.message}`,
                  })
                : false
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
                outcome: 'failed',
                error: err.message,
                sourceUrl: url,
                mediaPageUrl,
                bytesDownloaded: bytesDownloadedForFile,
                expectedBytes: responseContentLength,
              })
              updateQuarantineManifestForRepairAttempt(finalPath, {
                outcome: 'failed',
                error: err.message,
                sourceUrl: url,
                mediaPageUrl,
                bytesDownloaded: bytesDownloadedForFile,
                expectedBytes: responseContentLength,
              })
              recordRunError('lazy_video_error', {
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
              })
              appendRunEvent('lazy_video_error', {
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
                manifestUpdated,
                addedPermanentSkip,
              })
              logAndProgress(`❌ Lazy failed: ${filename} - ${err.message}`)
              if (fs.existsSync(finalPath)) fs.unlinkSync(finalPath)
              if (!addedPermanentSkip) {
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

    await browser.close()
    browser = null

    saveVisualHashCache()

    await maybePauseForErrorReview(modelName, errorCount, reviewErrors)

    if (skipNasSync) {
      console.log('⏭️ NAS sync skipped by --skip-nas-sync')
    } else {
      const nasSync = await runShellCommand(
        `robocopy "%APPDATA%\\.slopvault\\dataset\\${modelName}" "Z:\\dataset\\${modelName}" /MIR /R:2 /W:5`
      )

      if (!nasSync.ok && nasSync.code > 3) {
        console.error('❌ NAS sync failed with code', nasSync.code)
      } else {
        mergeNasMp4Entries(
          collectMp4RelativePaths(path.join(datasetDir, modelName), datasetDir),
          datasetDir
        )
        syncNasMp4IndexToMirror('Z:\\dataset', datasetDir)
        console.log('✅ NAS sync complete.')
      }
    }

    console.log(
      `🎉 Done: ${successCount} saved, ${duplicateCount} dupes, ${errorCount} errors`
    )
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
})()

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
  clearPinnedFooter,
  formatBytes,
  formatDuration,
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
    fs.writeFileSync(registryPath, JSON.stringify(emptyRegistry, null, 2))
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
  fs.writeFileSync(
    registryPath,
    JSON.stringify(sortModelRegistry(registry), null, 2) + '\n'
  )
}

function sortStringValues(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b)
  )
}

function sortStufferSources(sources) {
  return [...(Array.isArray(sources) ? sources : [])].sort((a, b) => {
    const left =
      String(a?.discoveredAs || '') ||
      String(a?.categoryId || '') ||
      String(a?.url || '')
    const right =
      String(b?.discoveredAs || '') ||
      String(b?.categoryId || '') ||
      String(b?.url || '')
    return left.localeCompare(right)
  })
}

function sortModelRegistry(registry) {
  return Object.fromEntries(
    Object.entries(registry || {})
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([canonicalName, entry]) => [
        canonicalName,
        {
          aliases: sortStringValues(entry?.aliases),
          sources: {
            stufferdb: sortStufferSources(entry?.sources?.stufferdb),
          },
        },
      ])
  )
}

function ensureModelEntryShape(entry, canonicalName) {
  const aliasSet = new Set(
    Array.isArray(entry?.aliases) ? entry.aliases.filter(Boolean) : []
  )

  if (canonicalName) aliasSet.add(canonicalName)

  return {
    aliases: Array.from(aliasSet),
    sources: {
      stufferdb: Array.isArray(entry?.sources?.stufferdb)
        ? entry.sources.stufferdb
        : [],
    },
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

function resolveAndTrackModel(registryPath, rawName, sourceUrl, canonicalOverride) {
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
    boolean: ['review-errors', 'skip-nas-sync'],
    alias: {
      m: 'model',
    },
  })

  return {
    inputUrl: args._[0] || '',
    modelOverride: sanitize(args.model || ''),
    reviewErrors: Boolean(args['review-errors']),
    skipNasSync: Boolean(args['skip-nas-sync']),
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

  if (!normalizedAnswer || normalizedAnswer === 'y' || normalizedAnswer === 'yes') {
    return {
      aliasName: inferredName,
      canonicalName: inferredCanonical,
    }
  }

  const aliasAnswer = await askQuestion(
    `Page alias [${inferredName}]: `
  )
  const canonicalAnswer = await askQuestion(
    `Save this alias under model [${inferredCanonical}]: `
  )

  return {
    aliasName: sanitize(aliasAnswer) || inferredName,
    canonicalName: sanitize(canonicalAnswer) || inferredCanonical,
  }
}

async function getBreadcrumbInfo(page) {
  return await page.evaluate(() => {
    const h2 = document.querySelector('.titrePage h2')
    const anchors = [...(h2?.querySelectorAll('a') || [])].map((a) => ({
      text: a.textContent?.trim() || '',
      href: a.href || '',
    }))

    return {
      texts: anchors.map((a) => a.text).filter(Boolean),
      hrefs: anchors.map((a) => a.href).filter(Boolean),
    }
  })
}

async function collectChildCategoryUrls(browser, parentUrl) {
  const page = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: true,
  })

  try {
    await page.goto(parentUrl, {
      waitUntil: 'domcontentloaded',
      timeout: 20000,
    })

    const candidateUrls = await page.evaluate(() => {
      const links = [
        ...document.querySelectorAll(
          'ul.thumbnailCategories li.album a, li.gdthumb.album a'
        ),
      ]

      return [
        ...new Set(
          links
            .map((a) => a.href || '')
            .filter((href) => href.includes('index?/category/'))
            .map((href) => href.replace(/&acs=[^&]+/gi, ''))
        ),
      ]
    })

    const parentNormalized = parentUrl.replace(/&acs=[^&]+/gi, '')
    return candidateUrls.filter((url) => url && url !== parentNormalized)
  } finally {
    if (!page.isClosed()) await page.close()
  }
}

async function buildCategoryRunList(browser, inputUrl) {
  const normalizedInput = inputUrl.replace(/&acs=[^&]+/gi, '')
  const childUrls = await collectChildCategoryUrls(browser, normalizedInput)

  return [...new Set([normalizedInput, ...childUrls])]
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
  lazyCompletedDownloads = 0

const rootDir = path.join(__dirname, '..')
const slopvaultRoot = path.join(
  process.env.APPDATA || path.join(process.env.HOME, 'AppData', 'Roaming'),
  '.slopvault'
)
const datasetDir = path.join(
  slopvaultRoot,
  'dataset'
)
const quarantineDatasetDir = path.join(slopvaultRoot, 'quarantine', 'dataset')
const quarantineManifestPath = path.join(
  slopvaultRoot,
  'quarantine',
  'quarantine-manifest.json'
)
const permanentSkipFile = path.join(slopvaultRoot, 'milkmaid-permanent-skips.json')
const tmpDir = path.join(rootDir, 'tmp')
let currentRunLog = null
let permanentSkipEntries = []
let permanentSkipLookup = {
  relativePaths: new Set(),
  sourceUrls: new Set(),
  mediaPageUrls: new Set(),
  filenames: new Set(),
}

if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true })

function addRunSavedBytes(bytes) {
  if (!currentRunLog) return
  currentRunLog.transfer.savedBytes += Number(bytes) || 0
}

function addRunTransferredBytes(bytes, phase = 'scrape') {
  if (!currentRunLog) return

  const safeBytes = Number(bytes) || 0
  if (safeBytes <= 0) return

  currentRunLog.transfer.transferredBytes += safeBytes

  if (phase === 'lazy') {
    currentRunLog.transfer.lazyTransferredBytes += safeBytes
  } else {
    currentRunLog.transfer.scrapeTransferredBytes += safeBytes
  }
}

function setRunLazyExpectedBytes(bytes) {
  if (!currentRunLog) return
  currentRunLog.transfer.lazyExpectedBytes = Number(bytes) || 0
}

function getIncompleteDirs(modelName) {
  // Per-model scratch space so unfinished work never "bleeds" into the next run
  const base = path.join(rootDir, 'incomplete', modelName)
  const gifs = path.join(base, 'gifs')
  const videos = path.join(base, 'videos')

  if (!fs.existsSync(gifs)) fs.mkdirSync(gifs, { recursive: true })
  if (!fs.existsSync(videos)) fs.mkdirSync(videos, { recursive: true })

  return { base, gifs, videos }
}

function createModelFolders(modelName) {
  const base = path.join(datasetDir, modelName)
  const images = path.join(base, 'images')
  const logDir = path.join(base, 'log')

  // Always create images folder
  fs.mkdirSync(images, { recursive: true })
  fs.mkdirSync(logDir, { recursive: true })

  // Per-model incomplete dirs (gifs/videos) live in the project root,
  // while finished media lives in the dataset folder.
  const incomplete = getIncompleteDirs(modelName)

  return {
    base,
    images,
    logDir,
    incompleteGifDir: incomplete.gifs,
    incompleteVideoDir: incomplete.videos,
    createGifFolder: () => {
      const gifPath = path.join(base, 'gif')
      if (!fs.existsSync(gifPath)) fs.mkdirSync(gifPath, { recursive: true })
      return gifPath
    },
    createWebmFolder: () => {
      const webmPath = path.join(base, 'webm')
      if (!fs.existsSync(webmPath)) fs.mkdirSync(webmPath, { recursive: true })
      return webmPath
    },
  }
}

function getDatasetRelativePath(filePath) {
  return path.relative(datasetDir, filePath).replace(/\\/g, '/')
}

function getQuarantineMirrorPath(filePath) {
  return path.join(
    quarantineDatasetDir,
    getDatasetRelativePath(filePath).replace(/\//g, path.sep)
  )
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
    replacementDurationSeconds:
      Number.isFinite(details.durationSeconds) ? details.durationSeconds : null,
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
    lastAttemptBytesDownloaded:
      Number.isFinite(details.bytesDownloaded) ? details.bytesDownloaded : null,
    lastAttemptExpectedBytes:
      Number.isFinite(details.expectedBytes) ? details.expectedBytes : null,
  }

  saveQuarantineManifest(manifest)
  return true
}

function isQuarantinedPath(filePath) {
  return fs.existsSync(getQuarantineMirrorPath(filePath))
}

function normalizePath(filePath) {
  return String(filePath || '').replace(/\\/g, '/')
}

function existsForRepair(filePath) {
  return fs.existsSync(filePath) && !isQuarantinedPath(filePath)
}

function getRecordRefs(record) {
  return Array.isArray(record?.refs)
    ? record.refs
        .map((ref) => (typeof ref === 'string' ? ref : ref?.relativePath || ''))
        .filter(Boolean)
    : []
}

function getActiveRecordRefs(record) {
  return getRecordRefs(record).filter((relativePath) =>
    existsForRepair(path.join(datasetDir, relativePath.replace(/\//g, path.sep)))
  )
}

function getBitwiseDuplicationRecord(hash) {
  const record = getBitwiseHashRecord(hash)
  const activeRefs = getActiveRecordRefs(record)
  return {
    record,
    activeRefs,
    isDuplicate: activeRefs.length > 0 && isBitwiseDupe(hash),
  }
}

function getVisualDuplicationRecord(visualHash) {
  const record = getVisualHashRecord(visualHash)
  const activeRefs = getActiveRecordRefs(record)
  return {
    record,
    activeRefs,
    isDuplicate: activeRefs.length > 0 && isVisualDupe(visualHash),
  }
}

function startRunLog(modelName, inputUrl, folders) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-')
  const logPath = path.join(folders.logDir, `milkmaid-run-${stamp}.jsonl`)
  const summaryPath = path.join(folders.logDir, 'milkmaid-run-latest-summary.json')
  const modelSummaryPath = path.join(folders.base, 'milkmaid-last-run.json')
  currentRunLog = {
    stamp,
    logPath,
    summaryPath,
    modelSummaryPath,
    modelName,
    inputUrl,
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
      transferredBytes: 0,
      scrapeTransferredBytes: 0,
      lazyExpectedBytes: 0,
      lazyTransferredBytes: 0,
    },
    timings: {
      lazyDurationMs: 0,
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

function buildRunSummary(extra = {}) {
  if (!currentRunLog) return null

  const { status = 'finished', finishedAt = new Date().toISOString(), ...rest } = extra
  const durationMs = Math.max(
    new Date(finishedAt).getTime() - new Date(currentRunLog.startedAt).getTime(),
    0
  )
  const averageBytesPerSecond =
    durationMs > 0
      ? currentRunLog.transfer.transferredBytes / (durationMs / 1000)
      : 0
  const lazyDurationMs = currentRunLog.timings.lazyDurationMs || 0
  const lazyAverageBytesPerSecond =
    lazyDurationMs > 0
      ? currentRunLog.transfer.lazyTransferredBytes / (lazyDurationMs / 1000)
      : 0

  return {
    startedAt: currentRunLog.startedAt,
    finishedAt,
    durationMs,
    modelName: currentRunLog.modelName,
    inputUrl: currentRunLog.inputUrl,
    logPath: currentRunLog.logPath,
    counters: currentRunLog.counters,
    transfer: currentRunLog.transfer,
    timings: currentRunLog.timings,
    throughput: {
      averageBytesPerSecond,
      lazyAverageBytesPerSecond,
    },
    errors: currentRunLog.errors,
    status,
    ...rest,
  }
}

function finalizeRunLog(extra = {}) {
  if (!currentRunLog) return

  const { status = 'finished', ...rest } = extra
  const summary = buildRunSummary({
    status,
    ...rest,
  })

  fs.writeFileSync(currentRunLog.summaryPath, JSON.stringify(summary, null, 2))
  fs.writeFileSync(
    currentRunLog.modelSummaryPath,
    JSON.stringify(
      {
        ...summary,
      },
      null,
      2
    ) + '\n'
  )
  currentRunLog = null
  return summary
}

function logRunSummaryTable(summary) {
  if (!summary) return

  const rows = [
    ['Duration', formatDuration(summary.durationMs)],
    ['Downloaded', formatBytes(summary.transfer.transferredBytes)],
    ['Saved size', formatBytes(summary.transfer.savedBytes)],
    ['Run avg speed', summary.throughput.averageBytesPerSecond
      ? `${formatBytes(summary.throughput.averageBytesPerSecond)}/s`
      : 'n/a'],
    ['Lazy downloaded', formatBytes(summary.transfer.lazyTransferredBytes)],
    ['Lazy expected', summary.transfer.lazyExpectedBytes > 0
      ? formatBytes(summary.transfer.lazyExpectedBytes)
      : 'n/a'],
    ['Lazy avg speed', summary.throughput.lazyAverageBytesPerSecond
      ? `${formatBytes(summary.throughput.lazyAverageBytesPerSecond)}/s`
      : 'n/a'],
    ['Saved / Dupes / Errors', `${summary.successCount} / ${summary.duplicateCount} / ${summary.errorCount}`],
  ]

  const labelWidth = Math.max(...rows.map(([label]) => label.length))
  const valueWidth = Math.max(...rows.map(([, value]) => String(value).length))
  const top = `╔${'═'.repeat(labelWidth + 2)}╤${'═'.repeat(valueWidth + 2)}╗`
  const mid = `╟${'─'.repeat(labelWidth + 2)}┼${'─'.repeat(valueWidth + 2)}╢`
  const bottom = `╚${'═'.repeat(labelWidth + 2)}╧${'═'.repeat(valueWidth + 2)}╝`

  console.log('')
  console.log(chalk.magentaBright('╔════════════════ Milkmaid Run Summary ════════════════╗'))
  console.log(top)

  rows.forEach(([label, value], index) => {
    const paddedLabel = label.padEnd(labelWidth, ' ')
    const paddedValue = String(value).padEnd(valueWidth, ' ')
    console.log(`║ ${chalk.cyan(paddedLabel)} │ ${chalk.white(paddedValue)} ║`)
    if (index < rows.length - 1) console.log(mid)
  })

  console.log(bottom)
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

  await askQuestion('\nPress Enter when you are ready to continue to NAS sync: ')
}

function normalizeSkipUrl(url) {
  return String(url || '').trim().replace(/&acs=[^&]+/gi, '')
}

function isNuisanceMediaAsset(filename, ext) {
  const lowerFilename = String(filename || '').trim().toLowerCase()
  const lowerExt = String(ext || '').trim().toLowerCase()

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
      entries.map((entry) => String(entry?.relativePath || '').trim()).filter(Boolean)
    ),
    sourceUrls: new Set(
      entries.map((entry) => normalizeSkipUrl(entry?.sourceUrl)).filter(Boolean)
    ),
    mediaPageUrls: new Set(
      entries.map((entry) => normalizeSkipUrl(entry?.mediaPageUrl)).filter(Boolean)
    ),
    filenames: new Set(
      entries.map((entry) => String(entry?.filename || '').trim()).filter(Boolean)
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
    relativePath: String(entry?.relativePath || '').trim().replace(/\\/g, '/'),
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
        normalizeSkipUrl(existing?.mediaPageUrl) === normalizedEntry.mediaPageUrl)
  )

  if (alreadyExists) return false

  permanentSkipEntries.push(normalizedEntry)
  permanentSkipLookup = buildPermanentSkipLookup(permanentSkipEntries)
  savePermanentSkips()
  return true
}

function getPermanentSkipMatch({ relativePath, mediaUrl, mediaPageUrl, filename }) {
  const normalizedRelativePath = String(relativePath || '').trim().replace(/\\/g, '/')
  const normalizedMediaUrl = normalizeSkipUrl(mediaUrl)
  const normalizedMediaPageUrl = normalizeSkipUrl(mediaPageUrl)
  const normalizedFilename = String(filename || '').trim()

  return permanentSkipEntries.find((entry) => {
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
}

function buildHashMetadata(modelName, absolutePath, mediaType, sizeBytes, uploadedDate) {
  const relativePath = path
    .relative(datasetDir, absolutePath)
    .replace(/\\/g, '/')
  const parts = relativePath.split('/').filter(Boolean)

  return {
    root: 'dataset',
    model: modelName || parts[0] || null,
    bucket: parts[1] || null,
    relativePath,
    filename: path.basename(absolutePath),
    mediaType,
    sizeBytes:
      Number.isFinite(sizeBytes) && sizeBytes >= 0 ? sizeBytes : null,
    modifiedAt: uploadedDate?.toISOString?.() || null,
    source: 'milkmaid',
  }
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
          addRunTransferredBytes(chunk.length, 'scrape')
          if (onProgress) {
            const elapsedSeconds = Math.max((Date.now() - start) / 1000, 0.001)
            onProgress({
              chunkBytes: chunk.length,
              downloadedBytes,
              totalBytes,
              percent: totalBytes > 0 ? (downloadedBytes / totalBytes) * 100 : null,
              averageBytesPerSecond: downloadedBytes / elapsedSeconds,
            })
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
  const traceMatch = String(process.env.MILKMAID_TRACE_MATCH || '').trim().toLowerCase()
  if (traceMatch) {
    return filename.toLowerCase().includes(traceMatch) ||
      getDatasetRelativePath(finalPath).toLowerCase().includes(traceMatch)
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

function drawCurrentProgressBar() {
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
      averageSpeedBytesPerSecond: speedBytesPerSecond,
      etaSeconds,
      activeCount: lazyActiveDownloads,
      completedCount: lazyCompletedDownloads,
      totalCount: lazyVideoQueue.length,
      totalTransferredBytes: currentRunLog?.transfer?.transferredBytes || 0,
    })
    return
  }

  logProgress(completedTotal, global.totalSearchTotal || 1, {
    totalTransferredBytes: currentRunLog?.transfer?.transferredBytes || 0,
    savedCount: successCount,
    duplicateCount,
    errorCount,
  })
}

function logAndProgress(message, increment = false) {
  if (increment) {
    completedTotal++
  }

  logScrollingMessage(message)
  drawCurrentProgressBar()
}

let grandCompleted = 0

async function scrapeGallery(browser, url, modelName, folders) {
  const { base, images, webm } = folders

  const page = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: false,
  })

  process.stdout.write('\n') // Reserve one lines
  grandCompleted++
  logProgress(grandCompleted, global.totalSearchTotal, {
    totalTransferredBytes: currentRunLog?.transfer?.transferredBytes || 0,
    savedCount: successCount,
    duplicateCount,
    errorCount,
  })

  try {
    while (url) {
      await page.goto(url, { waitUntil: 'domcontentloaded' })

      const urls = await page.$$eval('a[href^="picture?/"]', (links) => [
        ...new Set(links.map((l) => l.href)),
      ])

      const total = urls.length

      // If prefetch failed or undercounted badly, trust the actual page we just loaded.
      if (
        !global.totalSearchTotal ||
        global.totalSearchTotal <= 1 ||
        total > global.totalSearchTotal
      ) {
        global.totalSearchTotal = total
      }

      const mode = url.includes('&acs=') ? 'ACS' : 'PLAIN'
      logAndProgress(
        `📸 ${modelName} - [${mode}] - ${urls.length} media links (tracking ${global.totalSearchTotal})`
      )
      appendRunEvent('category_page_loaded', {
        modelName,
        categoryUrl: url,
        mode,
        mediaLinks: urls.length,
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
        totalCount++
        let mediaUrl = null
        let filename = null
        let ext = null

        try {
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

          const uploadedDateIso = await page.evaluate(() => {
            const anchor = document.querySelector('#datepost dd a')
            if (!anchor) return null
            const text = anchor.textContent?.trim()
            const match = text.match(/\d{1,2} \w+ \d{4}/)
            if (!match) return null
            const date = new Date(match[0])
            return isNaN(date.getTime()) ? null : date.toISOString()
          })

          const uploadedDate = uploadedDateIso
            ? new Date(uploadedDateIso)
            : null

          mediaUrl = await page.evaluate(() => {
            const video = document.querySelector('video.vjs-tech[src]')
            const img = document.querySelector('#theMainImage')
            return video?.src || img?.src || null
          })

          if (!mediaUrl) return

          const parsed = new URL(mediaUrl)
          filename = decodeURIComponent(
            path.basename(parsed.pathname).split('?')[0]
          )

          ext = path.extname(filename).toLowerCase()
          if (ext === '.m4v') {
            ext = '.mp4'
            filename = filename.replace(/\.m4v$/i, '.mp4')
          }

          if (isNuisanceMediaAsset(filename, ext)) {
            appendRunEvent('skip_nuisance_media', {
              modelName,
              mediaPageUrl,
              mediaUrl,
              filename,
              extension: ext,
            })
            return logAndProgress(`🚫 Skipped nuisance asset: ${filename}`, true)
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

          let buffer = null
          let hash = null
          let visualHash = null
          let lastTransferRedrawAt = 0
          const handleTransferProgress = () => {
            const now = Date.now()
            if (now - lastTransferRedrawAt < 250) return
            lastTransferRedrawAt = now
            drawCurrentProgressBar()
          }

          // Step 1: Fetch file buffer
          if (!['.mp4', '.webm', '.gif'].includes(ext)) {
            buffer = await downloadBufferWithProgress(
              mediaUrl,
              handleTransferProgress
            )

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
            buffer = await downloadBufferWithProgress(
              mediaUrl,
              handleTransferProgress
            )
            hash = createHash('md5').update(buffer).digest('hex')

            const gifFolder = folders.createGifFolder()
            const gifPath = path.join(gifFolder, filename)

            if (knownFilenames.has(filename) || existsForRepair(gifPath)) {
              duplicateCount++
              currentRunLog && currentRunLog.counters.duplicates++
              appendRunEvent('skip_existing_gif', {
                modelName,
                filename,
                savedPath: getDatasetRelativePath(gifPath),
                quarantinedMirrorExists: isQuarantinedPath(gifPath),
              })
              return logAndProgress(`â™»ï¸ Skipped gif (exists): ${filename}`, true)
            }

            fs.writeFileSync(gifPath, buffer)
            if (uploadedDate) {
              const ts = uploadedDate.getTime() / 1000
              fs.utimesSync(gifPath, ts, ts)
            }

            await mediaDates.recordImageDates(
              path.join(datasetDir, modelName),
              'gif',
              filename,
              buffer,
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
                  uploadedDate
                )
              )
              saveBitwiseHashCache()
            }

            knownFilenames.add(filename)
            successCount++
            currentRunLog && currentRunLog.counters.saved++
            addRunSavedBytes(buffer.length)
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

            if (knownFilenames.has(filename) || existsForRepair(finalPath)) {
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
            existsForRepair(path.join(images, filename))
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

          const finalPath = path.join(images, filename)
          fs.writeFileSync(finalPath, buffer)

          if (uploadedDate) {
            const ts = uploadedDate.getTime() / 1000
            fs.utimesSync(finalPath, ts, ts)
          }

          await mediaDates.recordImageDates(path.join(datasetDir, modelName), 'images', filename, buffer, uploadedDate)

          if (!isBitwiseDupe(hash)) {
            addBitwiseHash(
              hash,
              buildHashMetadata(
                modelName,
                finalPath,
                'image',
                buffer.length,
                uploadedDate
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
                uploadedDate
              )
            )
          }
          knownFilenames.add(filename)
          successCount++
          currentRunLog && currentRunLog.counters.saved++
          addRunSavedBytes(buffer.length)
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
        urls.map((mediaPageUrl, i) => {
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
      skipNasSync
    } = parseCliArgs(process.argv.slice(2))
    let inputUrl = initialInputUrl
    if (!inputUrl || !inputUrl.includes('/category/'))
      return logAndProgress('⚠️  Usage: node milkmaid.js <gallery-url>')

    inputUrl = inputUrl.replace(/&acs=[^&]+/i, '')

    const categoryId = inputUrl.match(/category\/?(\d+)/)?.[1]
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

    console.log(`📊 Combined media total: ${combinedTotal}`)
    console.log(`💦 Starting scrape for ${modelName}`)

    for (let i = 0; i < categoryRunList.length; i++) {
      const categoryUrl = categoryRunList[i]
      const categoryTotal = categoryCounts[i] || 0

      resetProgressCounter(categoryTotal)

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
  setRunLazyExpectedBytes(0)
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
      averageSpeedBytesPerSecond: speedBytesPerSecond,
      etaSeconds,
      activeCount: lazyActiveDownloads,
      completedCount: lazyCompletedDownloads,
      totalCount: lazyVideoQueue.length,
      totalTransferredBytes: currentRunLog?.transfer?.transferredBytes || 0,
    })
  }

  drawLazyProgress()

  await Promise.all(
    lazyVideoQueue.map(
      ({ url, path: finalPath, tmpPath, filename, uploadedDate, mediaPageUrl }, i) =>
        lazyLimit(async () => {
          if (knownFilenames.has(filename) || existsForRepair(finalPath)) {
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
                    addRunTransferredBytes(chunk.length, 'lazy')
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
                tmpSize: fs.existsSync(tmpPath) ? fs.statSync(tmpPath).size : null,
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

            if (uploadedDate) {
              const ts = uploadedDate.getTime() / 1000
              fs.utimesSync(tmpPath, ts, ts)
            }

            moveFileIntoPlace(tmpPath, finalPath)

            await mediaDates.recordVideoDates(path.join(datasetDir, modelName), 'webm', filename, finalPath, uploadedDate)

            const finalStat = fs.statSync(finalPath)
            const finalHash = await hashFileFromPath(finalPath)
            addBitwiseHash(
              finalHash,
              buildHashMetadata(
                modelName,
                finalPath,
                'video',
                finalStat.size,
                uploadedDate
              )
            )
            saveBitwiseHashCache()

            const finalVisualHash = await getVisualHashFromVideoPath(finalPath)
            if (finalVisualHash) {
              addVisualHash(
                finalVisualHash,
                buildHashMetadata(
                  modelName,
                  finalPath,
                  'video',
                  finalStat.size,
                  uploadedDate
                )
              )
              saveVisualHashCache()
            }

            successCount++
            currentRunLog && currentRunLog.counters.saved++
            addRunSavedBytes(finalStat.size)
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
            const manifestUpdated =
              hadQuarantineMirror &&
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
              savedPath: getDatasetRelativePath(finalPath),
              error: err.message,
              bytesDownloaded: bytesDownloadedForFile,
              responseContentLength,
              responseEndedCleanly,
              responseWasAborted,
              responseCloseBeforeEnd,
              hadQuarantineMirror,
              manifestUpdated: Boolean(manifestUpdated),
            })
            appendRunEvent('lazy_video_error', {
              modelName,
              filename,
              mediaUrl: url,
              mediaPageUrl,
              savedPath: getDatasetRelativePath(finalPath),
              error: err.message,
              bytesDownloaded: bytesDownloadedForFile,
              responseContentLength,
              responseEndedCleanly,
              responseWasAborted,
              responseCloseBeforeEnd,
              hadQuarantineMirror,
              manifestUpdated,
            })
            logAndProgress(`❌ Lazy failed: ${filename} - ${err.message}`)
            if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
            if (fs.existsSync(finalPath)) fs.unlinkSync(finalPath)
            knownFilenames.delete(filename) // allow retry in future runs
          } finally {
            lazyActiveDownloads = Math.max(lazyActiveDownloads - 1, 0)
            lazyCompletedDownloads++
            drawLazyProgress()
          }
        })
    )
  )

  if (currentRunLog) {
    currentRunLog.timings.lazyDurationMs = Math.max(
      Date.now() - lazyDownloadStartedAt,
      0
    )
  }

    await browser.close()
    browser = null

    saveVisualHashCache()

    await maybePauseForErrorReview(modelName, errorCount, reviewErrors)
    clearPinnedFooter()

    if (skipNasSync) {
      console.log('⏭️ NAS sync skipped by --skip-nas-sync')
    } else {
      const nasSync = await runShellCommand(
        `robocopy "%APPDATA%\\.slopvault\\dataset\\${modelName}" "Z:\\dataset\\${modelName}" /MIR /R:2 /W:5`
      )

      if (!nasSync.ok && nasSync.code > 3) {
        console.error('❌ NAS sync failed with code', nasSync.code)
      } else {
        console.log('✅ NAS sync complete.')
      }
    }

    const liveSummary = buildRunSummary({
      successCount,
      duplicateCount,
      errorCount,
      categoryRunList,
      combinedTotal,
    })
    logRunSummaryTable(liveSummary)

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

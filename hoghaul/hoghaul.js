'use strict'

const fs = require('fs')
const path = require('path')
const os = require('os')
const http = require('http')
const https = require('https')
const zlib = require('zlib')
const { exec } = require('child_process')
const { createHash } = require('crypto')
const minimist = require('minimist')
const pLimit = require('p-limit')
const puppeteer = require('puppeteer-extra')
const StealthPlugin = require('puppeteer-extra-plugin-stealth')

puppeteer.use(StealthPlugin())

const { bannerHoghaul } = require('../banners.js')
const mediaDates = require('../milkmaid/media-dates.js')
const { writeRepoJsonFileSync } = require('../scrapyard/repoFileWriter')
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

const rootDir = path.join(__dirname, '..')
const slopvaultRoot = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  '.slopvault'
)
const datasetDir = path.join(slopvaultRoot, 'dataset')
const quarantineDatasetDir = path.join(slopvaultRoot, 'quarantine', 'dataset')
const registryPath = path.join(rootDir, 'model_aliases.json')
const API_PAGE_SIZE = 50
const API_ACCEPT_HEADER = 'text/css'
const REQUEST_TIMEOUT_MS =
  Number.parseInt(process.env.HOGHAUL_REQUEST_TIMEOUT_MS || '', 10) || 30000

let currentRunLog = null
let mediaSeenIndexCache = null
let successCount = 0
let duplicateCount = 0
let errorCount = 0
let queuedVideoCount = 0
let convertedGifCount = 0
let savedBytes = 0
let browserMediaDownloader = null
const MAX_FUZZY_IMAGE_VISUAL_DISTANCE = 8
const pendingImageVisualClaims = new Map()

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
  return [...(Array.isArray(sources) ? sources : [])].sort((a, b) => {
    const left =
      String(a?.service || '') + String(a?.userId || '') + String(a?.url || '')
    const right =
      String(b?.service || '') + String(b?.userId || '') + String(b?.url || '')
    return left.localeCompare(right)
  })
}

function sortModelRegistry(registry) {
  return Object.fromEntries(
    Object.entries(registry || {})
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([canonicalName, entry]) => {
        const sources =
          entry?.sources && typeof entry.sources === 'object'
            ? entry.sources
            : {}
        return [
          canonicalName,
          {
            aliases: sortStringValues(entry?.aliases),
            sources: Object.fromEntries(
              Object.entries(sources)
                .sort(([left], [right]) => left.localeCompare(right))
                .map(([key, value]) => [key, sortSourceList(value)])
            ),
          },
        ]
      })
  )
}

function loadModelRegistry() {
  if (!fs.existsSync(registryPath)) {
    writeRepoJsonFileSync(registryPath, {}, { formatWithPrettier: false })
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
  writeRepoJsonFileSync(registryPath, sortModelRegistry(registry), {
    formatWithPrettier: false,
  })
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

  for (const [canonicalName, entry] of Object.entries(registry || {})) {
    const sources =
      entry?.sources && typeof entry.sources === 'object' ? entry.sources : {}

    for (const [siteKey, sourceList] of Object.entries(sources)) {
      if (normalizedSite && siteKey !== normalizedSite) continue

      for (const source of Array.isArray(sourceList) ? sourceList : []) {
        const sourceUrl = String(source?.url || '').trim()
        const sourceService = String(source?.service || '').trim()
        const sourceUserId = String(source?.userId || '').trim()

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
        source?.userId === sourceInfo.userId)
  )

  const nextSource = {
    url: sourceInfo.inputUrl,
    service: sourceInfo.service,
    userId: sourceInfo.userId,
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

function createModelFolders(modelName) {
  const base = path.join(datasetDir, modelName)
  const images = path.join(base, 'images')
  const logDir = path.join(base, 'log')
  const incompleteVideoDir = path.join(
    rootDir,
    'incomplete',
    modelName,
    'videos'
  )

  fs.mkdirSync(images, { recursive: true })
  fs.mkdirSync(logDir, { recursive: true })
  fs.mkdirSync(incompleteVideoDir, { recursive: true })

  return {
    base,
    images,
    logDir,
    incompleteVideoDir,
    createGifFolder: () => {
      const gifPath = path.join(base, 'gif')
      fs.mkdirSync(gifPath, { recursive: true })
      return gifPath
    },
    createWebmFolder: () => {
      const webmPath = path.join(base, 'webm')
      fs.mkdirSync(webmPath, { recursive: true })
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

function isQuarantinedPath(filePath) {
  return fs.existsSync(getQuarantineMirrorPath(filePath))
}

function existsForRepair(filePath) {
  return fs.existsSync(filePath) && !isQuarantinedPath(filePath)
}

function getRecordRefs(record) {
  return Array.isArray(record?.refs)
    ? record.refs
        .map((ref) => String(ref || '').replace(/\\/g, '/'))
        .filter(Boolean)
    : []
}

function getActiveRecordRefs(record) {
  return getRecordRefs(record).filter((relativePath) =>
    existsForRepair(
      path.join(datasetDir, relativePath.replace(/\//g, path.sep))
    )
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

function isSameModelRef(modelName, relativePath) {
  return String(relativePath || '').startsWith(`${modelName}/`)
}

function getFuzzyVisualDuplicationRecord(modelName, visualHash, maxDistance) {
  if (!visualHash || !Number.isFinite(maxDistance) || maxDistance < 0) {
    return null
  }

  let bestMatch = null
  for (const entry of getVisualHashEntries()) {
    const candidateHash = String(entry?.hash || '')
    const distance = getVisualHashDistance(visualHash, candidateHash)
    if (distance === null || distance > maxDistance) continue

    const activeRefs = getActiveRecordRefs(entry).filter((relativePath) =>
      isSameModelRef(modelName, relativePath)
    )
    if (activeRefs.length === 0) continue

    if (
      !bestMatch ||
      distance < bestMatch.distance ||
      (distance === bestMatch.distance &&
        candidateHash.localeCompare(bestMatch.matchedHash) < 0)
    ) {
      bestMatch = {
        record: entry,
        activeRefs,
        distance,
        matchedHash: candidateHash,
        isDuplicate: true,
      }
    }
  }

  return bestMatch
}

function getPendingImageVisualDuplicate(modelName, visualHash, maxDistance) {
  if (!visualHash || !Number.isFinite(maxDistance) || maxDistance < 0) {
    return null
  }

  let bestMatch = null
  for (const claim of pendingImageVisualClaims.values()) {
    if (!claim || claim.modelName !== modelName) continue
    const distance = getVisualHashDistance(visualHash, claim.visualHash)
    if (distance === null || distance > maxDistance) continue
    if (
      !bestMatch ||
      distance < bestMatch.distance ||
      (distance === bestMatch.distance &&
        claim.relativePath.localeCompare(bestMatch.activeRefs[0]) < 0)
    ) {
      bestMatch = {
        activeRefs: [claim.relativePath],
        distance,
        matchedHash: claim.visualHash,
        isDuplicate: true,
      }
    }
  }

  return bestMatch
}

function reservePendingImageVisualClaim(modelName, relativePath, visualHash) {
  const claimKey = `${modelName}:${relativePath}`
  pendingImageVisualClaims.set(claimKey, {
    modelName,
    relativePath,
    visualHash,
  })
  return claimKey
}

function releasePendingImageVisualClaim(claimKey) {
  if (!claimKey) return
  pendingImageVisualClaims.delete(claimKey)
}

function buildHashMetadata(
  modelName,
  absolutePath,
  mediaType,
  sizeBytes,
  uploadedDate
) {
  return {
    root: 'dataset',
    model: modelName,
    bucket: path.basename(path.dirname(absolutePath)),
    relativePath: getDatasetRelativePath(absolutePath),
    filename: path.basename(absolutePath),
    mediaType,
    sizeBytes: Number.isFinite(sizeBytes) && sizeBytes >= 0 ? sizeBytes : null,
    modifiedAt: uploadedDate?.toISOString?.() || null,
    source: 'hoghaul',
  }
}

function parseResolvedDate(date) {
  if (date instanceof Date && !isNaN(date.getTime())) return date
  if (typeof date === 'string') {
    const parsed = new Date(date)
    if (!isNaN(parsed.getTime())) return parsed
  }
  return null
}

function resolveEffectiveFileDate(date) {
  const parsed = parseResolvedDate(date)
  return parsed || new Date()
}

function applyFileTimestamp(filePath, date) {
  const effectiveDate = resolveEffectiveFileDate(date)
  const ts = effectiveDate.getTime() / 1000
  fs.utimesSync(filePath, ts, ts)
  return effectiveDate
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
      duplicates: 0,
      queuedVideos: 0,
      convertedGifs: 0,
      failures: 0,
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

function normalizeSeenUrl(url) {
  return String(url || '')
    .trim()
    .replace(/&acs=[^&]+/gi, '')
}

function getMediaSeenIndexPath(modelLogDir) {
  return path.join(modelLogDir, 'milkmaid-seen-media-index.json')
}

function loadMediaSeenIndex(modelLogDir) {
  const indexPath = getMediaSeenIndexPath(modelLogDir)
  if (mediaSeenIndexCache?.indexPath === indexPath)
    return mediaSeenIndexCache.data

  let parsed = {}
  if (fs.existsSync(indexPath)) {
    try {
      parsed = JSON.parse(fs.readFileSync(indexPath, 'utf8'))
    } catch (err) {
      console.warn(
        `Could not parse media seen index at ${indexPath}: ${err.message}`
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

function getActiveMediaSeenRecord(modelLogDir, entry) {
  if (!entry?.relativePath) return null
  const absolutePath = path.join(
    datasetDir,
    String(entry.relativePath).replace(/\//g, path.sep)
  )
  if (!existsForRepair(absolutePath)) return null
  return {
    ...entry,
    absolutePath,
  }
}

function getSuccessfulSeenMediaMatch(modelLogDir, mediaPageUrl, mediaUrl) {
  const index = loadMediaSeenIndex(modelLogDir)
  const normalizedMediaPageUrl = normalizeSeenUrl(mediaPageUrl)
  const normalizedMediaUrl = normalizeSeenUrl(mediaUrl)

  if (normalizedMediaPageUrl) {
    const pageEntry = getActiveMediaSeenRecord(
      modelLogDir,
      index.mediaPageUrls[normalizedMediaPageUrl]
    )
    if (pageEntry) return { matchType: 'media_page_url', ...pageEntry }
  }

  if (normalizedMediaUrl) {
    const mediaEntry = getActiveMediaSeenRecord(
      modelLogDir,
      index.mediaUrls[normalizedMediaUrl]
    )
    if (mediaEntry) return { matchType: 'media_url', ...mediaEntry }
  }

  return null
}

function recordSuccessfulSeenMedia(modelLogDir, details = {}) {
  const relativePath = String(details.relativePath || '').trim()
  if (!relativePath) return

  const index = loadMediaSeenIndex(modelLogDir)
  const normalizedMediaPageUrl = normalizeSeenUrl(details.mediaPageUrl)
  const normalizedMediaUrl = normalizeSeenUrl(details.mediaUrl)
  const payload = {
    relativePath,
    filename: details.filename || path.basename(relativePath),
    mediaUrl: normalizedMediaUrl || null,
    mediaPageUrl: normalizedMediaPageUrl || null,
    savedAt: new Date().toISOString(),
  }

  let changed = false
  if (
    normalizedMediaPageUrl &&
    index.mediaPageUrls[normalizedMediaPageUrl]?.relativePath !== relativePath
  ) {
    index.mediaPageUrls[normalizedMediaPageUrl] = payload
    changed = true
  }
  if (
    normalizedMediaUrl &&
    index.mediaUrls[normalizedMediaUrl]?.relativePath !== relativePath
  ) {
    index.mediaUrls[normalizedMediaUrl] = payload
    changed = true
  }

  if (changed) saveMediaSeenIndex(modelLogDir, index)
}

function decodeBody(buffer, headers) {
  const encoding = String(headers['content-encoding'] || '').toLowerCase()
  if (encoding.includes('br')) return zlib.brotliDecompressSync(buffer)
  if (encoding.includes('gzip')) return zlib.gunzipSync(buffer)
  if (encoding.includes('deflate')) return zlib.inflateSync(buffer)
  return buffer
}

function requestBuffer(url, options = {}) {
  const {
    method = 'GET',
    headers = {},
    timeoutMs = REQUEST_TIMEOUT_MS,
    maxRedirects = 5,
    onProgress = null,
  } = options

  return new Promise((resolve, reject) => {
    const parsed = new URL(url)
    const client = parsed.protocol === 'https:' ? https : http
    const req = client.request(
      parsed,
      {
        method,
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'gzip, deflate, br',
          ...headers,
        },
      },
      (res) => {
        const statusCode = res.statusCode || 0
        const redirect = res.headers.location
        if (
          [301, 302, 303, 307, 308].includes(statusCode) &&
          redirect &&
          maxRedirects > 0
        ) {
          res.resume()
          const nextUrl = new URL(redirect, url).toString()
          requestBuffer(nextUrl, {
            method: statusCode === 303 ? 'GET' : method,
            headers,
            timeoutMs,
            maxRedirects: maxRedirects - 1,
            onProgress,
          }).then(resolve, reject)
          return
        }

        if (statusCode < 200 || statusCode >= 300) {
          const chunks = []
          res.on('data', (chunk) => chunks.push(chunk))
          res.on('end', () => {
            const body = Buffer.concat(chunks).toString('utf8').slice(0, 500)
            reject(new Error(`HTTP ${statusCode}: ${body}`))
          })
          return
        }

        if (method === 'HEAD') {
          res.resume()
          resolve({
            buffer: Buffer.alloc(0),
            headers: res.headers,
            statusCode,
            url,
          })
          return
        }

        const chunks = []
        let downloadedBytes = 0
        const totalBytes = Number.parseInt(
          res.headers['content-length'] || '0',
          10
        )
        const startedAt = Date.now()
        res.on('data', (chunk) => {
          downloadedBytes += chunk.length
          chunks.push(chunk)
          if (onProgress) {
            onProgress({
              downloadedBytes,
              totalBytes,
              chunkBytes: chunk.length,
              elapsedMs: Date.now() - startedAt,
            })
          }
        })
        res.on('end', () => {
          const raw = Buffer.concat(chunks)
          resolve({
            buffer: decodeBody(raw, res.headers),
            headers: res.headers,
            statusCode,
            url,
          })
        })
      }
    )

    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`Request timed out after ${timeoutMs}ms`))
    })
    req.on('error', (err) => {
      const message =
        err?.message ||
        err?.code ||
        `Request failed for ${new URL(url).hostname}`
      reject(new Error(message))
    })
    req.end()
  })
}

function expandWindowsEnvVars(value) {
  return String(value || '').replace(/%([^%]+)%/g, (_, name) => {
    return process.env[name] || process.env[name.toUpperCase()] || ''
  })
}

function existingPathFromCandidates(candidates) {
  for (const candidate of candidates.filter(Boolean)) {
    const expanded = expandWindowsEnvVars(candidate)
    if (expanded && fs.existsSync(expanded)) return expanded
  }
  return null
}

function getDefaultBrowserExecutablePath() {
  return existingPathFromCandidates([
    process.env.HOGHAUL_BROWSER_EXECUTABLE,
    process.env.PUPPETEER_EXECUTABLE_PATH,
    '%LOCALAPPDATA%\\Yandex\\YandexBrowser\\Application\\browser.exe',
    '%LOCALAPPDATA%\\Google\\Chrome\\Application\\chrome.exe',
    '%PROGRAMFILES%\\Google\\Chrome\\Application\\chrome.exe',
    '%PROGRAMFILES(X86)%\\Google\\Chrome\\Application\\chrome.exe',
    '%LOCALAPPDATA%\\Microsoft\\Edge\\Application\\msedge.exe',
    '%PROGRAMFILES(X86)%\\Microsoft\\Edge\\Application\\msedge.exe',
    '%PROGRAMFILES%\\Microsoft\\Edge\\Application\\msedge.exe',
  ])
}

function getDefaultBrowserProfileDir(sourceSite) {
  return path.join(slopvaultRoot, 'hoghaul-browser-profile', sourceSite)
}

function normalizeCookieDomain(hostname) {
  const lower = String(hostname || '').toLowerCase()
  const parts = lower.split('.').filter(Boolean)
  return `.${parts.slice(-2).join('.')}`
}

function parseCookieHeader(cookieHeader, sourceUrl) {
  const parsedUrl = new URL(sourceUrl)
  const domain = normalizeCookieDomain(parsedUrl.hostname)
  return String(cookieHeader || '')
    .split(';')
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const equalsIndex = part.indexOf('=')
      if (equalsIndex <= 0) return null
      return {
        name: part.slice(0, equalsIndex).trim(),
        value: part.slice(equalsIndex + 1).trim(),
        domain,
        path: '/',
      }
    })
    .filter((cookie) => cookie?.name)
}

function parseNetscapeCookieFile(raw) {
  return raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'))
    .map((line) => {
      const parts = line.split('\t')
      if (parts.length < 7) return null
      const [domain, , pathValue, secure, expires, name, value] = parts
      return {
        domain,
        path: pathValue || '/',
        secure: /^true$/i.test(secure),
        expires: Number.parseInt(expires, 10) || undefined,
        name,
        value,
      }
    })
    .filter((cookie) => cookie?.name)
}

function normalizeCookieJson(parsed, sourceUrl) {
  const parsedUrl = new URL(sourceUrl)
  const fallbackDomain = normalizeCookieDomain(parsedUrl.hostname)
  const cookies = Array.isArray(parsed)
    ? parsed
    : Array.isArray(parsed?.cookies)
      ? parsed.cookies
      : Object.entries(parsed || {}).map(([name, value]) => ({ name, value }))

  return cookies
    .map((cookie) => {
      if (!cookie?.name || cookie.value === undefined) return null
      const normalized = {
        name: String(cookie.name),
        value: String(cookie.value),
        domain: cookie.domain || fallbackDomain,
        path: cookie.path || '/',
      }
      if (cookie.expires || cookie.expirationDate) {
        normalized.expires = Math.floor(cookie.expires || cookie.expirationDate)
      }
      if (cookie.secure !== undefined)
        normalized.secure = Boolean(cookie.secure)
      if (cookie.httpOnly !== undefined)
        normalized.httpOnly = Boolean(cookie.httpOnly)
      if (cookie.sameSite) normalized.sameSite = cookie.sameSite
      return normalized
    })
    .filter(Boolean)
}

function loadCookiesFromFile(cookieFile, sourceUrl) {
  const expanded = expandWindowsEnvVars(cookieFile)
  if (!expanded || !fs.existsSync(expanded)) {
    throw new Error(`Cookie file does not exist: ${cookieFile}`)
  }

  const raw = fs.readFileSync(expanded, 'utf8').trim()
  if (!raw) return []
  if (raw.startsWith('{') || raw.startsWith('[')) {
    return normalizeCookieJson(JSON.parse(raw), sourceUrl)
  }
  return parseNetscapeCookieFile(raw)
}

function getBrowserCookieList(sourceUrl, options) {
  const cookies = []
  if (options.cookieHeader) {
    cookies.push(...parseCookieHeader(options.cookieHeader, sourceUrl))
  }
  if (options.cookieFile) {
    cookies.push(...loadCookiesFromFile(options.cookieFile, sourceUrl))
  }
  return cookies
}

async function createBrowserMediaDownloader(source, options) {
  let browser = null
  let shouldCloseBrowser = true
  if (options.browserConnect) {
    const browserWSEndpoint = /^https?:\/\//i.test(options.browserConnect)
      ? await getBrowserWebSocketEndpoint(options.browserConnect)
      : options.browserConnect
    console.log(`Browser media mode: connected browser (${browserWSEndpoint})`)
    browser = await puppeteer.connect({ browserWSEndpoint })
    shouldCloseBrowser = false
  }

  const executablePath =
    options.browserExecutable || getDefaultBrowserExecutablePath()
  const userDataDir =
    options.browserProfile || getDefaultBrowserProfileDir(source.site)
  const headless = options.headless ? 'new' : false
  if (!browser) {
    fs.mkdirSync(userDataDir, { recursive: true })

    const launchOptions = {
      headless,
      userDataDir,
      defaultViewport: null,
      args: [
        '--ignore-certificate-errors',
        '--disable-blink-features=AutomationControlled',
        '--disable-extensions',
      ],
      ignoreHTTPSErrors: true,
    }
    if (executablePath) launchOptions.executablePath = executablePath

    console.log(
      `Browser media mode: ${executablePath || 'bundled Chromium'} (${headless ? 'headless' : 'headful'})`
    )
    console.log(`Browser profile: ${userDataDir}`)

    browser = await puppeteer.launch(launchOptions)
  }
  const cookies = getBrowserCookieList(source.inputUrl, options)
  if (cookies.length) {
    const cookiePage = await browser.newPage()
    await cookiePage.setCookie(...cookies)
    await cookiePage.close()
    console.log(
      `Loaded ${cookies.length} browser cookie(s) for media requests.`
    )
  }

  const warmupPage = await browser.newPage()
  await warmupPage.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
  })
  await warmupPage
    .goto(source.inputUrl, {
      waitUntil: 'domcontentloaded',
      timeout: options.timeoutMs,
    })
    .catch((err) => {
      appendRunEvent('browser_warmup_warning', {
        url: source.inputUrl,
        error: err.message,
      })
      console.warn(`Browser warmup warning: ${err.message}`)
    })
  if (options.validateMs > 0) {
    console.log(
      `Browser validation pause: ${Math.round(options.validateMs / 1000)}s. Use the opened browser window to pass any site check.`
    )
    await new Promise((resolve) => setTimeout(resolve, options.validateMs))
  }

  async function getCookieHeaderFor(mediaUrl) {
    const cookiesForRequest = await warmupPage
      .cookies(source.inputUrl, mediaUrl)
      .catch(() => [])
    const browserCookieHeader = cookiesForRequest
      .map((cookie) => `${cookie.name}=${cookie.value}`)
      .join('; ')
    return [options.cookieHeader, browserCookieHeader]
      .filter(Boolean)
      .join('; ')
  }

  return {
    async download(mediaUrl, entry = {}) {
      const cookieHeader = await getCookieHeaderFor(mediaUrl)
      try {
        const response = await requestBuffer(mediaUrl, {
          timeoutMs: options.timeoutMs,
          headers: {
            Accept: '*/*',
            Referer: entry.mediaPageUrl || source.inputUrl,
            ...(cookieHeader ? { Cookie: cookieHeader } : {}),
          },
        })
        return response.buffer
      } catch (err) {
        appendRunEvent('browser_cookie_http_error', {
          mediaUrl,
          mediaPageUrl: entry.mediaPageUrl,
          error: err.message,
          hadCookieHeader: Boolean(cookieHeader),
        })
      }

      const page = await browser.newPage()
      try {
        await page.setExtraHTTPHeaders({
          Accept: '*/*',
          'Accept-Language': 'en-US,en;q=0.9',
          Referer: entry.mediaPageUrl || source.inputUrl,
        })
        const response = await page.goto(mediaUrl, {
          waitUntil: 'load',
          timeout: options.timeoutMs,
        })
        if (!response) throw new Error('Browser returned no response')
        const status = response.status()
        if (status < 200 || status >= 300) {
          throw new Error(`Browser HTTP ${status}`)
        }
        return await response.buffer()
      } finally {
        await page.close().catch(() => {})
      }
    },
    async extractPostMediaUrls(postPageUrl) {
      const page = await browser.newPage()
      try {
        await page.setExtraHTTPHeaders({
          'Accept-Language': 'en-US,en;q=0.9',
          Referer: source.inputUrl,
        })
        await page.goto(postPageUrl, {
          waitUntil: 'domcontentloaded',
          timeout: options.timeoutMs,
        })
        await page
          .waitForSelector(
            'a.fileThumb.image-link, a.post__attachment-link[href], video source[src]',
            { timeout: 10000 }
          )
          .catch(() => {})
        return await page.evaluate(() => {
          return Array.from(
            document.querySelectorAll(
              'a.fileThumb.image-link, a.post__attachment-link[href], video source[src]'
            )
          )
            .map((el) => {
              const value =
                el.href ||
                el.src ||
                el.getAttribute('href') ||
                el.getAttribute('src') ||
                ''
              if (!value) return null
              return new URL(value, location.href).toString()
            })
            .filter(Boolean)
        })
      } finally {
        await page.close().catch(() => {})
      }
    },
    async close() {
      await warmupPage.close().catch(() => {})
      if (shouldCloseBrowser) {
        await browser.close().catch(() => {})
      } else {
        browser.disconnect()
      }
    },
  }
}

async function getBrowserWebSocketEndpoint(connectValue) {
  const versionUrl = new URL('/json/version', connectValue).toString()
  const response = await requestBuffer(versionUrl, {
    headers: { Accept: 'application/json' },
  })
  const version = JSON.parse(response.buffer.toString('utf8'))
  if (!version.webSocketDebuggerUrl) {
    throw new Error(`No webSocketDebuggerUrl found at ${versionUrl}`)
  }
  return version.webSocketDebuggerUrl
}

async function closeBrowserMediaDownloader() {
  if (!browserMediaDownloader) return
  const downloader = browserMediaDownloader
  browserMediaDownloader = null
  await downloader.close()
}

async function fetchJson(url) {
  const response = await requestBuffer(url, {
    headers: {
      Accept: API_ACCEPT_HEADER,
    },
  })
  const body = response.buffer.toString('utf8')
  return {
    data: JSON.parse(body),
    byteLength: response.buffer.length,
    url,
  }
}

async function fetchHtml(url) {
  const response = await requestBuffer(url, {
    headers: {
      Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    },
  })
  return {
    html: response.buffer.toString('utf8'),
    byteLength: response.buffer.length,
    url,
  }
}

function normalizeCreatorName(value) {
  return String(value || '')
    .trim()
    .replace(/^@+/, '')
    .toLowerCase()
}

async function findCreatorIdByName(origin, service, creatorName) {
  const { data: creators } = await fetchJson(`${origin}/api/v1/creators`)
  if (!Array.isArray(creators)) return null

  const normalizedName = normalizeCreatorName(creatorName)
  const hit = creators.find(
    (creator) =>
      creator?.service === service &&
      normalizeCreatorName(creator?.name) === normalizedName
  )

  return hit ? String(hit.id) : null
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
        : null
  if (!site) throw new Error(`Unsupported Hoghaul host: ${parsed.hostname}`)

  const parts = parsed.pathname.split('/').filter(Boolean)

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

function getPostPageUrl(source, post) {
  if (source.site === 'coomerfans') {
    return (
      post.url ||
      `${source.origin}/p/${post.id}/${source.userId}/${source.service}`
    )
  }
  return `${source.origin}/${source.service}/user/${source.userId}/post/${post.id}`
}

function getMediaUrl(source, media) {
  const mediaPath = String(media?.path || '').trim()
  if (!mediaPath) return null
  if (/^https?:\/\//i.test(mediaPath)) return mediaPath
  return `${source.origin}/data${mediaPath.startsWith('/') ? mediaPath : `/${mediaPath}`}`
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

  const postPublishedAt = parseResolvedDate(post.published)
  const mediaPageUrl = getPostPageUrl(source, post)
  const rawEntries = []
  if (post.file?.path) rawEntries.push(post.file)
  if (Array.isArray(post.attachments)) rawEntries.push(...post.attachments)

  const seen = new Set()
  return rawEntries
    .map((media) => {
      const mediaUrl = getMediaUrl(source, media)
      const filename = mediaUrl ? filenameFromMediaUrl(mediaUrl) : null
      if (!mediaUrl || !filename) return null
      const key = normalizeSeenUrl(mediaUrl)
      if (seen.has(key)) return null
      seen.add(key)
      return {
        postId: String(post.id || ''),
        title: post.title || null,
        mediaPageUrl,
        mediaUrl,
        filename,
        originalName: media.name || null,
        uploadedDate: postPublishedAt,
      }
    })
    .filter(Boolean)
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
    const key =
      normalizeSeenUrl(entry.mediaUrl) ||
      `${normalizeSeenUrl(entry.mediaPageUrl)}\n${entry.filename}`
    if (!key || seen.has(key)) continue
    seen.add(key)
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

function getPostsApiUrl(source, offset = 0) {
  return `${source.origin}/api/v1/${source.service}/user/${encodeURIComponent(
    source.userId
  )}/posts?o=${offset}`
}

async function preflightSourceJson(source, page = 0) {
  const offset = page * API_PAGE_SIZE
  const apiUrl = getPostsApiUrl(source, offset)
  const { data, byteLength } = await fetchJson(apiUrl)

  if (!Array.isArray(data)) {
    throw new Error(
      `Expected ${apiUrl} to return a JSON post array, got ${typeof data}`
    )
  }

  const newest = data
    .map((post) => parseResolvedDate(post?.published))
    .filter(Boolean)
    .sort((a, b) => b.getTime() - a.getTime())[0]

  return {
    apiUrl,
    byteLength,
    postCount: data.length,
    newest,
    firstPostId: data[0]?.id ? String(data[0].id) : null,
  }
}

async function resolveKemonoCreatorIdForJson(source) {
  if (source.site !== 'kemono' || /^\d+$/.test(source.userId)) {
    return false
  }

  const resolvedId = await findCreatorIdByName(
    source.origin,
    source.service,
    source.userId
  ).catch(() => null)

  if (!resolvedId) {
    throw new Error(
      `Kemono rejected "${source.userId}" for ${source.service}. Kemono creator URLs usually need the numeric creator ID, and that username was not found in /api/v1/creators.`
    )
  }

  console.log(`Resolved Kemono creator ${source.userId} -> ${resolvedId}`)
  source.userId = resolvedId
  source.rawName = sanitize(resolvedId)
  return true
}

async function fetchCoomerFansPosts(source, options) {
  await resolveCoomerFansCreator(source)
  const posts = []
  let page = options.startPage

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

    for (const post of selectedPostLinks) {
      console.log(`Loading coomerfans post ${post.id}`)
      const { html: postHtml } = await fetchHtml(post.url)
      const mediaEntries = parseCoomerFansMediaEntries(source, post, postHtml)
      posts.push({
        id: post.id,
        url: post.url,
        title: mediaEntries[0]?.title || null,
        published: mediaEntries[0]?.uploadedDate || null,
        mediaEntries,
      })
    }

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

async function fetchPosts(source, options) {
  if (source.site === 'coomerfans') {
    return fetchCoomerFansPosts(source, options)
  }

  const posts = []
  let page = options.startPage

  while (true) {
    if (options.endPage !== null && page > options.endPage) break
    const offset = page * API_PAGE_SIZE
    const apiUrl = getPostsApiUrl(source, offset)
    console.log(`Loading ${source.site} page ${page + 1} (${apiUrl})`)

    let pagePosts
    try {
      const pageResult = await fetchJson(apiUrl)
      pagePosts = pageResult.data
    } catch (err) {
      if (source.site === 'kemono' && !/^\d+$/.test(source.userId)) {
        await resolveKemonoCreatorIdForJson(source)
        continue
      }
      throw err
    }

    if (!Array.isArray(pagePosts) || pagePosts.length === 0) break
    posts.push(...pagePosts)
    if (pagePosts.length < API_PAGE_SIZE) break
    page += 1
  }

  return posts
}

function classifyMedia(filename) {
  const ext = path.extname(filename).toLowerCase()
  if (['.mp4', '.m4v', '.webm', '.mov'].includes(ext))
    return { ext, kind: 'video' }
  if (ext === '.gif') return { ext, kind: 'gif' }
  if (['.jpg', '.jpeg', '.png', '.webp', '.bmp', '.avif'].includes(ext)) {
    return { ext, kind: 'image' }
  }
  return { ext, kind: 'unknown' }
}

function getVideoDuration(filePath) {
  return new Promise((resolve) => {
    const cmd = `ffprobe -v error -show_entries format=duration -of csv=p=0 "${filePath}"`
    exec(cmd, (err, stdout) => {
      if (err) return resolve(null)
      const duration = Number.parseFloat(stdout.trim())
      resolve(Number.isFinite(duration) ? duration : null)
    })
  })
}

function convertShortMp4ToGif(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const cmd = `ffmpeg -y -i "${inputPath}" -vf "fps=15,scale=480:-1:flags=lanczos" "${outputPath}"`
    exec(cmd, (err) => (err ? reject(err) : resolve()))
  })
}

async function maybeCreateShortVideoGif(
  modelName,
  folders,
  videoPath,
  filename,
  uploadedDate
) {
  const stat = fs.statSync(videoPath)
  const duration = await getVideoDuration(videoPath)
  if (
    !Number.isFinite(duration) ||
    duration > 6 ||
    stat.size >= 5 * 1024 * 1024
  ) {
    return null
  }

  const gifName = filename.replace(/\.(mp4|m4v|webm|mov)$/i, '.gif')
  const gifPath = path.join(folders.createGifFolder(), gifName)
  if (fs.existsSync(gifPath)) return null

  await convertShortMp4ToGif(videoPath, gifPath)
  const recordedDate = await mediaDates.recordVideoDates(
    path.join(datasetDir, modelName),
    'gif',
    gifName,
    gifPath,
    uploadedDate
  )
  applyFileTimestamp(
    gifPath,
    parseResolvedDate(recordedDate?.date) || uploadedDate
  )
  convertedGifCount += 1
  if (currentRunLog) currentRunLog.counters.convertedGifs += 1
  return gifName
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
  if (currentRunLog) currentRunLog.counters.duplicates += 1
  appendRunEvent(reason, {
    filename: entry.filename,
    mediaUrl: entry.mediaUrl,
    mediaPageUrl: entry.mediaPageUrl,
    savedPath,
    ...(extra && typeof extra === 'object' ? extra : {}),
  })
  recordSuccessfulSeenMedia(folders.logDir, {
    relativePath: savedPath,
    filename: entry.filename,
    mediaUrl: entry.mediaUrl,
    mediaPageUrl: null,
  })
}

async function saveImageLikeMedia(modelName, folders, entry, kind) {
  const bucket = kind === 'gif' ? 'gif' : 'images'
  const finalDir = kind === 'gif' ? folders.createGifFolder() : folders.images
  const finalPath = path.join(finalDir, entry.filename)
  const relativePath = getDatasetRelativePath(finalPath)

  appendRunEvent('media_seen', {
    modelName,
    mediaPageUrl: entry.mediaPageUrl,
    mediaUrl: entry.mediaUrl,
    filename: entry.filename,
    extension: path.extname(entry.filename).toLowerCase(),
    bucket,
    candidateRelativePath: relativePath,
  })

  const seenMediaMatch = getSuccessfulSeenMediaMatch(
    folders.logDir,
    null,
    entry.mediaUrl
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

  if (existsForRepair(finalPath)) {
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
    const recordedDate = await mediaDates.recordImageDates(
      path.join(datasetDir, modelName),
      bucket,
      entry.filename,
      buffer,
      entry.uploadedDate
    )
    const fileDate = applyFileTimestamp(
      finalPath,
      parseResolvedDate(recordedDate?.date) || entry.uploadedDate
    )
    const metadata = buildHashMetadata(
      modelName,
      finalPath,
      kind === 'gif' ? 'gif' : 'image',
      buffer.length,
      fileDate
    )

    addBitwiseHash(hash, metadata)
    if (visualHash) addVisualHash(visualHash, metadata)
    saveBitwiseHashCache()
    if (visualHash) saveVisualHashCache()

    successCount += 1
    savedBytes += buffer.length
    if (currentRunLog) {
      currentRunLog.counters.saved += 1
      currentRunLog.transfer.savedBytes += buffer.length
    }
    recordSuccessfulSeenMedia(folders.logDir, {
      relativePath,
      filename: entry.filename,
      mediaUrl: entry.mediaUrl,
      mediaPageUrl: null,
    })
    appendRunEvent(kind === 'gif' ? 'saved_gif' : 'saved_image', {
      modelName,
      filename: entry.filename,
      savedPath: relativePath,
      hash,
      visualHash,
    })
    console.log(`Saved ${kind}: ${entry.filename}`)
  } finally {
    releasePendingImageVisualClaim(visualClaimKey)
  }
}

async function saveVideoMedia(modelName, folders, entry) {
  const finalPath = path.join(folders.createWebmFolder(), entry.filename)
  const tmpPath = path.join(folders.incompleteVideoDir, entry.filename)
  const relativePath = getDatasetRelativePath(finalPath)

  appendRunEvent('media_seen', {
    modelName,
    mediaPageUrl: entry.mediaPageUrl,
    mediaUrl: entry.mediaUrl,
    filename: entry.filename,
    extension: path.extname(entry.filename).toLowerCase(),
    bucket: 'webm',
    candidateRelativePath: relativePath,
  })

  const seenMediaMatch = getSuccessfulSeenMediaMatch(
    folders.logDir,
    null,
    entry.mediaUrl
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

  if (existsForRepair(finalPath)) {
    recordDuplicate(entry, relativePath, 'skip_lazy_existing', folders)
    console.log(`Exists already: ${entry.filename}`)
    return
  }

  queuedVideoCount += 1
  if (currentRunLog) currentRunLog.counters.queuedVideos += 1
  appendRunEvent('queued_lazy_video', {
    modelName,
    filename: entry.filename,
    mediaUrl: entry.mediaUrl,
    mediaPageUrl: entry.mediaPageUrl,
    savedPath: relativePath,
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
      console.log(`Bitwise dupe: ${entry.filename}`)
      return
    }

    moveFileIntoPlace(tmpPath, finalPath)
    const recordedDate = await mediaDates.recordVideoDates(
      path.join(datasetDir, modelName),
      'webm',
      entry.filename,
      finalPath,
      entry.uploadedDate
    )
    const fileDate = applyFileTimestamp(
      finalPath,
      parseResolvedDate(recordedDate?.date) || entry.uploadedDate
    )
    const stat = fs.statSync(finalPath)
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

    const metadata = buildHashMetadata(
      modelName,
      finalPath,
      'video',
      stat.size,
      fileDate
    )
    addBitwiseHash(hash, metadata)
    if (visualHash) addVisualHash(visualHash, metadata)
    saveBitwiseHashCache()
    saveVisualHashCache()

    const gifName = await maybeCreateShortVideoGif(
      modelName,
      folders,
      finalPath,
      entry.filename,
      entry.uploadedDate
    ).catch((err) => {
      appendRunEvent('short_video_gif_error', {
        modelName,
        filename: entry.filename,
        error: err.message,
      })
      return null
    })

    successCount += 1
    savedBytes += stat.size
    if (currentRunLog) {
      currentRunLog.counters.saved += 1
      currentRunLog.transfer.savedBytes += stat.size
      currentRunLog.transfer.lazyTransferredBytes += stat.size
    }
    recordSuccessfulSeenMedia(folders.logDir, {
      relativePath,
      filename: entry.filename,
      mediaUrl: entry.mediaUrl,
      mediaPageUrl: null,
    })
    appendRunEvent('saved_lazy_video', {
      modelName,
      filename: entry.filename,
      savedPath: relativePath,
      hash,
      visualHash,
      convertedGif: gifName,
    })
    console.log(`Saved video: ${entry.filename}`)
  } catch (err) {
    removeFileIfExists(tmpPath)
    errorCount += 1
    if (currentRunLog) currentRunLog.counters.failures += 1
    recordRunError('lazy_video_error', {
      modelName,
      filename: entry.filename,
      mediaUrl: entry.mediaUrl,
      mediaPageUrl: entry.mediaPageUrl,
      savedPath: relativePath,
      error: err.message,
    })
    appendRunEvent('lazy_video_error', {
      modelName,
      filename: entry.filename,
      mediaUrl: entry.mediaUrl,
      mediaPageUrl: entry.mediaPageUrl,
      error: err.message,
    })
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
      'image-concurrency',
      'video-concurrency',
    ],
    boolean: [
      'dry-run',
      'preflight',
      'skip-nas-sync',
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
      'Usage: npm run hoghaul -- "<coomer-or-kemono-user-url>" [--pages=1 or 1-3] [--model=name] [--preflight] [--dry-run] [--skip-nas-sync] [--cookie-file=cookies.json] [--browser-profile=path] [--browser-connect=http://127.0.0.1:9222] [--browser-validate-ms=60000] [--image-concurrency=3] [--video-concurrency=2]'
    )
    process.exitCode = 1
    return
  }

  loadBitwiseHashCache()
  loadVisualHashCache()

  const source = parseSourceUrl(inputUrl)
  if (source.site === 'coomerfans') {
    useBrowserMedia = false
  }
  const imageConcurrency = parsePositiveInteger(
    argv['image-concurrency'] ||
      process.env.npm_config_image_concurrency ||
      process.env.HOGHAUL_IMAGE_CONCURRENCY,
    source.site === 'coomerfans' ? 3 : 6
  )
  const videoConcurrency = parsePositiveInteger(
    argv['video-concurrency'] ||
      process.env.npm_config_video_concurrency ||
      process.env.HOGHAUL_VIDEO_CONCURRENCY,
    3
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
    console.log('No API key or Authorization header was used.')
    return
  }

  const posts = await fetchPosts(source, { startPage, endPage, maxPosts })
  const selectedPosts =
    Number.isFinite(maxPosts) && maxPosts > 0 ? posts.slice(0, maxPosts) : posts
  const mediaEntries = selectedPosts.flatMap((post) =>
    getMediaEntriesFromPost(source, post)
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
    const modelNamePreview = sanitize(argv.model || source.rawName)
    console.log(
      `Resolved ${source.site}/${source.service}/${source.userId} -> ${modelNamePreview}: ${selectedPosts.length} posts, ${selectedMedia.length} media files`
    )
    if (selectedMediaSourceDuplicateCount > 0) {
      console.log(
        `Dry run source media dedupe: ${selectedMediaSourceDuplicateCount} repeated media URL(s)`
      )
    }
    console.log(
      `Dry run only. Newest post: ${newest ? newest.toISOString() : 'unknown'}`
    )
    return
  }

  const modelName = resolveAndTrackModel(
    source.rawName,
    {
      ...source,
      inputUrl,
    },
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

  if (useBrowserMedia) {
    browserMediaDownloader = await createBrowserMediaDownloader(
      source,
      browserOptions
    )
    appendRunEvent('browser_media_enabled', {
      browserExecutable: browserOptions.browserExecutable || null,
      browserProfile:
        browserOptions.browserProfile ||
        getDefaultBrowserProfileDir(source.site),
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
    const classification = classifyMedia(entry.filename)
    if (classification.kind === 'unknown') {
      appendRunEvent('skip_unknown_media', {
        modelName,
        filename: entry.filename,
        mediaUrl: entry.mediaUrl,
        mediaPageUrl: entry.mediaPageUrl,
        extension: classification.ext,
      })
      continue
    }
    if (classification.kind === 'video') {
      videos.push(entry)
    } else {
      imageLike.push({ ...entry, kind: classification.kind })
    }
  }

  await Promise.all(
    imageLike.map((entry) =>
      imageLimit(async () => {
        try {
          await saveImageLikeMedia(modelName, folders, entry, entry.kind)
        } catch (err) {
          errorCount += 1
          if (currentRunLog) currentRunLog.counters.failures += 1
          recordRunError('media_error', {
            modelName,
            filename: entry.filename,
            mediaUrl: entry.mediaUrl,
            mediaPageUrl: entry.mediaPageUrl,
            error: err.message,
          })
          appendRunEvent('media_error', {
            modelName,
            filename: entry.filename,
            mediaUrl: entry.mediaUrl,
            mediaPageUrl: entry.mediaPageUrl,
            error: err.message,
          })
          console.log(`Failed media: ${entry.filename} - ${err.message}`)
        }
      })
    )
  )

  console.log(`Lazy downloading videos: ${videos.length}`)
  await Promise.all(
    videos.map((entry) =>
      videoLimit(() => saveVideoMedia(modelName, folders, entry))
    )
  )

  appendRunEvent('run_finished', {
    successCount,
    duplicateCount,
    errorCount,
    queuedVideoCount,
    convertedGifCount,
    savedBytes,
    postCount: selectedPosts.length,
    mediaCount: selectedMedia.length,
    sourceDuplicateMediaCount: selectedMediaSourceDuplicateCount,
  })

  saveBitwiseHashCache()
  saveVisualHashCache()

  finalizeRunLog({
    successCount,
    duplicateCount,
    errorCount,
    queuedVideoCount,
    convertedGifCount,
    savedBytes,
    postCount: selectedPosts.length,
    mediaCount: selectedMedia.length,
    sourceDuplicateMediaCount: selectedMediaSourceDuplicateCount,
  })

  if (skipNasSync) {
    console.log('NAS sync skipped by --skip-nas-sync')
  } else {
    await syncToNAS(modelName)
  }

  console.log(
    `Done: ${successCount} saved, ${duplicateCount} dupes, ${errorCount} errors`
  )
  console.log(getCompletionLine())
}

run()
  .catch((err) => {
    recordRunError('run_error', {
      error: err.message,
    })
    appendRunEvent('run_error', {
      error: err.message,
    })
    if (currentRunLog) {
      finalizeRunLog({
        status: 'failed',
        successCount,
        duplicateCount,
        errorCount: errorCount + 1,
      })
    }
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

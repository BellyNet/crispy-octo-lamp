'use strict'

const fs = require('fs')
const path = require('path')
const minimist = require('minimist')
const sharp = require('sharp')

const { createHashStore } = require('./hashStore')
const { getRedditOriginalMediaUrl } = require('./redditFullResolutionRetry')
const config = require('./config')

const SIDECAR_FILENAME = '.media-dates.json'
const SEEN_INDEX_FILENAME = 'milkmaid-seen-media-index.json'
const RETRY_FILENAME = 'reddit-full-resolution-retry.json'
const IMAGE_EXTENSIONS = new Set(['.jpg', '.jpeg', '.png', '.webp', '.gif'])

const argv = minimist(process.argv.slice(2), {
  alias: { h: 'help', m: 'model' },
  boolean: ['help', 'apply', 'remote-verify', 'include-carousel-previews'],
  string: [
    'model',
    'dataset-root',
    'quarantine-root',
    'report-dir',
    'verify-direct-max-bytes',
    'remote-concurrency',
    'minimum-pixels',
    'minimum-long-edge',
  ],
  default: {
    apply: false,
    'remote-verify': true,
    'include-carousel-previews': false,
    'verify-direct-max-bytes': String(64 * 1024),
    'remote-concurrency': '8',
    'minimum-pixels': '500000',
    'minimum-long-edge': '768',
  },
})

if (require.main === module) {
  if (argv.help) {
    printHelp()
  } else {
    try {
      main().catch((err) => {
        console.error(
          `Fatal Reddit preview audit error: ${err.stack || err.message}`
        )
        process.exitCode = 1
      })
    } catch (err) {
      console.error(
        `Fatal Reddit preview audit error: ${err.stack || err.message}`
      )
      process.exitCode = 1
    }
  }
}

function printHelp() {
  console.log(`Usage: node scrapyard/auditRedditPreviewMedia.js [options]

Audits historical Reddit image rows that reference preview.redd.it or are
explicitly marked as needing full resolution. Dry-runs by default. In --apply
mode, local preview files are moved to a timestamped recovery archive and the
sidecar, seen-media index, and hash refs are removed so originals can retry.

Options:
  --apply                    Apply cleanup. Omit for audit-only mode.
  --model <name>             Limit the pass to one model.
  --dataset-root <path>      Override the local dataset root.
  --quarantine-root <path>   Override the recovery archive root.
  --report-dir <path>        Override the JSON report directory.
  --verify-direct-max-bytes  HEAD-check direct i.redd.it files at or below this
                              size. Default: 65536.
  --remote-concurrency <n>   Concurrent i.redd.it HEAD checks. Default: 8.
  --no-remote-verify         Skip direct-URL size verification.
  --include-carousel-previews
                             Upgrade saved preview images from multi-image Reddit posts.
  --minimum-pixels <n>       Minimum acceptable preview pixel count. Default: 500000.
  --minimum-long-edge <n>    Minimum acceptable preview long edge. Default: 768.
  -h, --help                 Show help.
`)
}

async function main() {
  const rootDir = path.join(__dirname, '..')
  const slopvaultRoot = config.slopvaultRoot
  const datasetRoot = path.resolve(
    String(argv['dataset-root'] || config.datasetDir)
  )
  const quarantineRoot = path.resolve(
    String(
      argv['quarantine-root'] ||
        path.join(slopvaultRoot, 'quarantine', 'reddit-preview-quality')
    )
  )
  const reportDir = path.resolve(
    String(
      argv['report-dir'] ||
        path.join(rootDir, 'tmp', 'reddit-preview-quality-audit')
    )
  )
  const apply = Boolean(argv.apply)
  const modelFilter = normalizeKey(argv.model)
  const remoteVerify = Boolean(argv['remote-verify'])
  const includeCarouselPreviews = Boolean(argv['include-carousel-previews'])
  const verifyDirectMaxBytes = getPositiveInteger(
    argv['verify-direct-max-bytes'],
    64 * 1024
  )
  const remoteConcurrency = getPositiveInteger(argv['remote-concurrency'], 8)
  const minimumPixels = getPositiveInteger(argv['minimum-pixels'], 500000)
  const minimumLongEdge = getPositiveInteger(argv['minimum-long-edge'], 768)
  const runTag = new Date().toISOString().replace(/[:.]/g, '-')
  const runArchiveRoot = path.join(quarantineRoot, runTag)

  if (!fs.existsSync(datasetRoot)) {
    throw new Error(`Dataset root does not exist: ${datasetRoot}`)
  }

  fs.mkdirSync(reportDir, { recursive: true })
  const audit = await collectAudit(datasetRoot, modelFilter, {
    remoteVerify,
    verifyDirectMaxBytes,
    remoteConcurrency,
    minimumPixels,
    minimumLongEdge,
    includeCarouselPreviews,
  })
  const report = {
    generatedAt: new Date().toISOString(),
    apply,
    datasetRoot,
    modelFilter: modelFilter || null,
    includeCarouselPreviews,
    remoteVerification: {
      enabled: remoteVerify,
      verifyDirectMaxBytes,
      concurrency: remoteConcurrency,
    },
    previewQuality: { minimumPixels, minimumLongEdge },
    quarantineRoot: apply ? runArchiveRoot : null,
    summary: summarizeAudit(audit),
    changes: {
      localFilesQuarantined: 0,
      sidecarRowsRemoved: 0,
      seenKeysRemoved: 0,
      bitwiseRefsRemoved: 0,
      visualRefsRemoved: 0,
      retryEntriesWritten: 0,
      backupFilesWritten: 0,
    },
    models: audit.models,
  }

  if (apply && audit.targets.length > 0) {
    fs.mkdirSync(runArchiveRoot, { recursive: true })
    applyCleanup(audit, report, datasetRoot, runArchiveRoot)
  }

  const reportPath = path.join(
    reportDir,
    apply
      ? 'reddit-preview-cleanup-latest.json'
      : 'reddit-preview-audit-latest.json'
  )
  const historyReportPath = path.join(
    reportDir,
    `${apply ? 'reddit-preview-cleanup' : 'reddit-preview-audit'}-${runTag}.json`
  )
  report.historyReportPath = historyReportPath
  writeJsonAtomic(historyReportPath, report)
  writeJsonAtomic(reportPath, report)
  printSummary(report, reportPath)
}

async function collectAudit(datasetRoot, modelFilter = '', options = {}) {
  const targets = []
  const previewQualityCandidates = []
  const directVerifyCandidates = []
  let redditRows = 0
  let tinyNonPreviewRows = 0
  const tinyNonPreviewSamples = []

  for (const dirent of fs.readdirSync(datasetRoot, { withFileTypes: true })) {
    if (!dirent.isDirectory()) continue
    const modelName = dirent.name
    if (modelFilter && normalizeKey(modelName) !== modelFilter) continue
    const modelRoot = path.join(datasetRoot, modelName)
    const sidecarPath = path.join(modelRoot, SIDECAR_FILENAME)
    if (!fs.existsSync(sidecarPath)) continue

    const sidecar = readJson(sidecarPath)
    const redditPostImageCounts = new Map()
    if (options.includeCarouselPreviews) {
      for (const [relativePath, row] of Object.entries(sidecar)) {
        if (normalizeKey(row?.source?.site) !== 'reddit') continue
        if (!IMAGE_EXTENSIONS.has(path.extname(relativePath).toLowerCase())) {
          continue
        }
        const postId = String(row?.source?.postId || '')
        if (!postId) continue
        redditPostImageCounts.set(
          postId,
          (redditPostImageCounts.get(postId) || 0) + 1
        )
      }
    }
    const modelTargets = []
    for (const [relativePath, row] of Object.entries(sidecar)) {
      if (relativePath.startsWith('__')) continue
      if (normalizeKey(row?.source?.site) !== 'reddit') continue
      if (!IMAGE_EXTENSIONS.has(path.extname(relativePath).toLowerCase())) {
        continue
      }
      redditRows += 1

      const localPath = resolveInside(modelRoot, relativePath)
      const localStat = statFile(localPath)
      if (!isRedditPreviewRow(row)) {
        if (
          options.remoteVerify !== false &&
          localStat &&
          localStat.size <= Number(options.verifyDirectMaxBytes || 64 * 1024) &&
          isDirectRedditUrl(row?.source?.mediaUrl)
        ) {
          directVerifyCandidates.push(
            createTarget({
              modelName,
              relativePath,
              modelRoot,
              row,
              localStat,
              reason: ['direct_reddit_remote_verification'],
            })
          )
        }
        if (localStat && localStat.size <= 20 * 1024) {
          tinyNonPreviewRows += 1
          if (tinyNonPreviewSamples.length < 50) {
            tinyNonPreviewSamples.push({
              modelName,
              relativePath,
              bytes: localStat.size,
              mediaUrl: row?.source?.mediaUrl || null,
            })
          }
        }
        continue
      }

      const previewTarget = createTarget({
        modelName,
        relativePath,
        modelRoot,
        row,
        localStat,
        reason: getPreviewReason(row),
      })
      if (
        options.includeCarouselPreviews &&
        (redditPostImageCounts.get(String(row?.source?.postId || '')) || 0) > 1
      ) {
        previewTarget.reason.push('carousel_preview_upgrade')
        modelTargets.push(previewTarget)
        continue
      }
      if (!localStat || isExplicitlyBlurredPreview(row?.source?.mediaUrl)) {
        if (!localStat) previewTarget.reason.push('missing_local_file')
        if (isExplicitlyBlurredPreview(row?.source?.mediaUrl)) {
          previewTarget.reason.push('explicit_blur_preview')
        }
        modelTargets.push(previewTarget)
      } else {
        previewQualityCandidates.push(previewTarget)
      }
    }

    targets.push(...modelTargets)
  }

  const previewQuality = await inspectPreviewQuality(
    previewQualityCandidates,
    Number(options.remoteConcurrency || 8),
    {
      minimumPixels: Number(options.minimumPixels || 500000),
      minimumLongEdge: Number(options.minimumLongEdge || 768),
    }
  )
  targets.push(...previewQuality.targets)
  const remoteVerification = await verifyDirectCandidates(
    directVerifyCandidates,
    Number(options.remoteConcurrency || 8)
  )
  targets.push(...remoteVerification.targets)
  const models = groupTargetsByModel(targets)

  return {
    datasetRoot,
    redditRows,
    tinyNonPreviewRows,
    tinyNonPreviewSamples,
    remoteVerification,
    previewQuality,
    targets,
    models,
  }
}

function summarizeAudit(audit) {
  const widths = {}
  for (const target of audit.targets) {
    const key = target.previewWidth ? String(target.previewWidth) : 'unknown'
    widths[key] = (widths[key] || 0) + 1
  }
  return {
    modelsScannedWithTargets: audit.models.length,
    redditImageRowsScanned: audit.redditRows,
    targetRows: audit.targets.length,
    localFiles: audit.targets.filter((target) => target.localExists).length,
    missingLocalFiles: audit.targets.filter((target) => !target.localExists)
      .length,
    localBytes: sum(audit.targets.map((target) => target.localBytes)),
    previewWidths: widths,
    carouselPreviewRowsTargeted: audit.targets.filter((target) =>
      target.reason.includes('carousel_preview_upgrade')
    ).length,
    tinyNonPreviewRows: audit.tinyNonPreviewRows,
    tinyNonPreviewSamples: audit.tinyNonPreviewSamples,
    directFilesVerified: audit.remoteVerification.checked,
    directVerificationFailures: audit.remoteVerification.failures.length,
    verifiedLargerOriginals: audit.remoteVerification.targets.length,
    directVerificationFailureSamples: audit.remoteVerification.failures.slice(
      0,
      50
    ),
    previewFilesInspected: audit.previewQuality.checked,
    decentPreviewFilesRetained: audit.previewQuality.retained,
    lowQualityPreviewFiles: audit.previewQuality.targets.length,
    previewInspectionFailures: audit.previewQuality.failures.length,
  }
}

async function inspectPreviewQuality(candidates, concurrency, options) {
  const targets = []
  const failures = []
  let checked = 0
  let retained = 0

  await runWithConcurrency(candidates, concurrency, async (candidate) => {
    try {
      const metadata = await sharp(candidate.localPath).metadata()
      const width = Number(metadata.width || 0)
      const height = Number(metadata.height || 0)
      const pixels = width * height
      const longEdge = Math.max(width, height)
      checked += 1
      const quality = { width, height, pixels, longEdge }
      if (
        pixels >= options.minimumPixels &&
        longEdge >= options.minimumLongEdge
      ) {
        retained += 1
        return
      }
      targets.push({
        ...candidate,
        reason: [...candidate.reason, 'low_quality_dimensions'],
        quality,
      })
    } catch (err) {
      failures.push({
        modelName: candidate.modelName,
        relativePath: candidate.relativePath,
        error: err.message,
      })
      targets.push({
        ...candidate,
        reason: [...candidate.reason, 'unreadable_image'],
      })
    }
  })

  targets.sort((a, b) =>
    a.datasetRelativePath.localeCompare(b.datasetRelativePath)
  )
  return { checked, retained, targets, failures }
}

function createTarget({
  modelName,
  relativePath,
  modelRoot,
  row,
  localStat,
  reason,
}) {
  return {
    modelName,
    relativePath: normalizePath(relativePath),
    datasetRelativePath: normalizePath(`${modelName}/${relativePath}`),
    localPath: resolveInside(modelRoot, relativePath),
    localExists: Boolean(localStat),
    localBytes: localStat?.size || 0,
    reason,
    previewWidth: getPreviewWidth(row?.source?.mediaUrl),
    fullResolutionUrl: getFullResolutionUrl(row),
    source: pickSourceMetadata(row?.source),
  }
}

async function verifyDirectCandidates(candidates, concurrency) {
  const targets = []
  const failures = []
  let checked = 0

  await runWithConcurrency(candidates, concurrency, async (candidate) => {
    try {
      const response = await fetchWithTimeout(candidate.source.mediaUrl, {
        method: 'HEAD',
        headers: { 'User-Agent': 'Mozilla/5.0 full-resolution-audit' },
      })
      const remoteBytes = Number(response.headers.get('content-length'))
      if (!response.ok || !Number.isFinite(remoteBytes) || remoteBytes <= 0) {
        failures.push({
          modelName: candidate.modelName,
          relativePath: candidate.relativePath,
          mediaUrl: candidate.source.mediaUrl,
          status: response.status,
          contentLength: response.headers.get('content-length'),
        })
        return
      }
      checked += 1
      const meaningfulDifference = Math.max(
        1024,
        Math.ceil(candidate.localBytes * 0.05)
      )
      if (remoteBytes <= candidate.localBytes + meaningfulDifference) return
      targets.push({
        ...candidate,
        reason: ['remote_original_larger'],
        fullResolutionUrl: candidate.source.mediaUrl,
        remoteBytes,
      })
    } catch (err) {
      failures.push({
        modelName: candidate.modelName,
        relativePath: candidate.relativePath,
        mediaUrl: candidate.source.mediaUrl,
        error: err.message,
      })
    }
  })

  targets.sort((a, b) =>
    a.datasetRelativePath.localeCompare(b.datasetRelativePath)
  )
  return { candidates: candidates.length, checked, targets, failures }
}

function groupTargetsByModel(targets) {
  const grouped = new Map()
  for (const target of targets) {
    if (!grouped.has(target.modelName)) grouped.set(target.modelName, [])
    grouped.get(target.modelName).push(target)
  }
  return [...grouped.entries()]
    .map(([modelName, modelTargets]) => ({
      modelName,
      targetCount: modelTargets.length,
      localFileCount: modelTargets.filter((item) => item.localExists).length,
      localBytes: sum(modelTargets.map((item) => item.localBytes)),
      targets: modelTargets,
    }))
    .sort((a, b) => a.modelName.localeCompare(b.modelName))
}

async function runWithConcurrency(items, concurrency, worker) {
  const queue = [...items]
  const workers = Array.from(
    { length: Math.min(Math.max(1, concurrency), queue.length) },
    async () => {
      while (queue.length > 0) {
        const item = queue.shift()
        await worker(item)
      }
    }
  )
  await Promise.all(workers)
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 10000) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)
  try {
    return await fetch(url, { ...options, signal: controller.signal })
  } finally {
    clearTimeout(timeout)
  }
}

function applyCleanup(audit, report, datasetRoot, runArchiveRoot) {
  const targetRefs = new Set(
    audit.targets.map((target) => normalizeRef(target.datasetRelativePath))
  )
  const bitwisePath = path.join(datasetRoot, 'bitwiseHashes.v2.json')
  const visualPath = path.join(datasetRoot, 'visualHashes.v2.json')
  const bitwiseStore = createHashStore({
    storePath: bitwisePath,
    kind: 'bitwise',
    algorithm: 'md5',
  })
  const visualStore = createHashStore({
    storePath: visualPath,
    kind: 'visual',
    algorithm: 'imghash-16-hex|video-3frame-imghash-16-hex',
  })

  backupFile(bitwisePath, datasetRoot, runArchiveRoot, report)
  backupFile(visualPath, datasetRoot, runArchiveRoot, report)
  bitwiseStore.load()
  visualStore.load()
  report.changes.bitwiseRefsRemoved = bitwiseStore.removeRefs((ref) =>
    targetRefs.has(normalizeRef(ref))
  )
  report.changes.visualRefsRemoved = visualStore.removeRefs((ref) =>
    targetRefs.has(normalizeRef(ref))
  )

  for (const model of audit.models) {
    const modelRoot = path.join(datasetRoot, model.modelName)
    const sidecarPath = path.join(modelRoot, SIDECAR_FILENAME)
    const seenPath = path.join(modelRoot, 'log', SEEN_INDEX_FILENAME)
    const retryPath = path.join(modelRoot, 'log', RETRY_FILENAME)
    const modelRefs = new Set(
      model.targets.map((target) => normalizeRef(target.datasetRelativePath))
    )

    backupFile(sidecarPath, datasetRoot, runArchiveRoot, report)
    backupFile(seenPath, datasetRoot, runArchiveRoot, report)
    backupFile(retryPath, datasetRoot, runArchiveRoot, report)

    const sidecar = readJson(sidecarPath)
    for (const target of model.targets) {
      if (target.localExists) {
        const archivePath = path.join(
          runArchiveRoot,
          'files',
          ...target.datasetRelativePath.split('/')
        )
        moveFileInsideRoots(
          datasetRoot,
          target.localPath,
          runArchiveRoot,
          archivePath
        )
        report.changes.localFilesQuarantined += 1
      }
      if (Object.hasOwn(sidecar, target.relativePath)) {
        delete sidecar[target.relativePath]
        report.changes.sidecarRowsRemoved += 1
      }
    }
    writeJsonAtomic(sidecarPath, sidecar, false)

    if (fs.existsSync(seenPath)) {
      const seen = readJson(seenPath)
      report.changes.seenKeysRemoved += pruneSeenIndex(seen, modelRefs)
      seen.updatedAt = new Date().toISOString()
      writeJsonAtomic(seenPath, seen)
    }

    const retry = fs.existsSync(retryPath)
      ? readJson(retryPath)
      : { version: 1, updatedAt: null, pending: {} }
    if (!retry.pending || typeof retry.pending !== 'object') retry.pending = {}
    for (const target of model.targets) {
      retry.pending[target.datasetRelativePath] = {
        status: 'pending_full_resolution',
        queuedAt: new Date().toISOString(),
        relativePath: target.datasetRelativePath,
        previewUrl: target.source.mediaUrl,
        fullResolutionUrl: target.fullResolutionUrl,
        mediaPageUrl: target.source.mediaPageUrl,
        postId: target.source.postId,
        sourceService: target.source.service,
        sourceUserId: target.source.userId,
        sourceUsername: target.source.username,
      }
      report.changes.retryEntriesWritten += 1
    }
    retry.updatedAt = new Date().toISOString()
    writeJsonAtomic(retryPath, retry)
  }

  bitwiseStore.save()
  visualStore.save()
}

function pruneSeenIndex(seen, targetRefs) {
  let removed = 0
  for (const bucketName of [
    'mediaUrls',
    'mediaPageUrls',
    'deadMediaUrls',
    'deadMediaPageUrls',
  ]) {
    const bucket = seen?.[bucketName]
    if (!bucket || typeof bucket !== 'object') continue
    for (const [key, entry] of Object.entries(bucket)) {
      if (!targetRefs.has(normalizeRef(entry?.relativePath))) continue
      delete bucket[key]
      removed += 1
    }
  }
  return removed
}

function isRedditPreviewRow(row = {}) {
  const source = row?.source || {}
  return (
    isPreviewUrl(source.mediaUrl) ||
    normalizeKey(source.mediaQuality) === 'reddit_preview' ||
    source.needsFullResolution === true ||
    row.needsFullResolution === true
  )
}

function getPreviewReason(row = {}) {
  const reasons = []
  if (isPreviewUrl(row?.source?.mediaUrl)) reasons.push('preview_redd_it_url')
  if (normalizeKey(row?.source?.mediaQuality) === 'reddit_preview') {
    reasons.push('preview_quality_metadata')
  }
  if (
    row?.source?.needsFullResolution === true ||
    row?.needsFullResolution === true
  ) {
    reasons.push('needs_full_resolution')
  }
  return reasons
}

function isPreviewUrl(value) {
  try {
    return (
      new URL(String(value || '')).hostname.toLowerCase() === 'preview.redd.it'
    )
  } catch {
    return false
  }
}

function isExplicitlyBlurredPreview(value) {
  try {
    return new URL(String(value || '')).searchParams.has('blur')
  } catch {
    return false
  }
}

function isDirectRedditUrl(value) {
  try {
    return new URL(String(value || '')).hostname.toLowerCase() === 'i.redd.it'
  } catch {
    return false
  }
}

function getFullResolutionUrl(row = {}) {
  const explicit = String(row?.source?.fullResolutionUrl || '').trim()
  if (explicit) return getRedditOriginalMediaUrl(explicit) || explicit
  return getRedditOriginalMediaUrl(row?.source?.mediaUrl)
}

function getPreviewWidth(value) {
  try {
    const width = Number(new URL(String(value || '')).searchParams.get('width'))
    return Number.isFinite(width) && width > 0 ? width : null
  } catch {
    return null
  }
}

function pickSourceMetadata(source = {}) {
  return {
    site: source.site || null,
    service: source.service || null,
    userId: source.userId || null,
    username: source.username || null,
    subreddit: source.subreddit || null,
    postId: source.postId || null,
    mediaPageUrl: source.mediaPageUrl || null,
    mediaUrl: source.mediaUrl || null,
  }
}

function moveFileInsideRoots(sourceRoot, sourcePath, targetRoot, targetPath) {
  assertInsideRoot(sourceRoot, sourcePath)
  assertInsideRoot(targetRoot, targetPath)
  if (fs.existsSync(targetPath)) {
    throw new Error(`Recovery archive target already exists: ${targetPath}`)
  }
  fs.mkdirSync(path.dirname(targetPath), { recursive: true })
  fs.renameSync(sourcePath, targetPath)
}

function backupFile(filePath, datasetRoot, runArchiveRoot, report) {
  if (!fs.existsSync(filePath)) return null
  assertInsideRoot(datasetRoot, filePath)
  const relativePath = normalizePath(path.relative(datasetRoot, filePath))
  const backupPath = path.join(
    runArchiveRoot,
    'backups',
    ...relativePath.split('/')
  )
  fs.mkdirSync(path.dirname(backupPath), { recursive: true })
  fs.copyFileSync(filePath, backupPath)
  report.changes.backupFilesWritten += 1
  return backupPath
}

function resolveInside(root, relativePath) {
  const resolved = path.resolve(root, ...normalizePath(relativePath).split('/'))
  assertInsideRoot(root, resolved)
  return resolved
}

function assertInsideRoot(root, target) {
  const relative = path.relative(path.resolve(root), path.resolve(target))
  if (relative.startsWith('..') || path.isAbsolute(relative)) {
    throw new Error(`Path escapes root ${root}: ${target}`)
  }
}

function statFile(filePath) {
  try {
    const stat = fs.statSync(filePath)
    return stat.isFile() ? stat : null
  } catch {
    return null
  }
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, ''))
}

function writeJsonAtomic(filePath, value, pretty = true) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true })
  const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}`
  const payload = pretty
    ? `${JSON.stringify(value, null, 2)}\n`
    : JSON.stringify(value)
  fs.writeFileSync(tempPath, payload)
  fs.renameSync(tempPath, filePath)
}

function normalizeKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

function normalizeRef(value) {
  return normalizePath(value).toLowerCase()
}

function sum(values) {
  return values.reduce((total, value) => total + Number(value || 0), 0)
}

function getPositiveInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function formatBytes(value) {
  const bytes = Number(value || 0)
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(2)} KiB`
  if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(2)} MiB`
  return `${(bytes / 1024 ** 3).toFixed(2)} GiB`
}

function printSummary(report, reportPath) {
  console.log(`Mode: ${report.apply ? 'apply' : 'audit'}`)
  console.log(`Models with targets: ${report.summary.modelsScannedWithTargets}`)
  console.log(
    `Reddit image rows scanned: ${report.summary.redditImageRowsScanned}`
  )
  console.log(
    `Full-resolution cleanup rows targeted: ${report.summary.targetRows}`
  )
  if (report.includeCarouselPreviews) {
    console.log(
      `Carousel preview rows targeted: ${report.summary.carouselPreviewRowsTargeted}`
    )
  }
  console.log(
    `Local files targeted: ${report.summary.localFiles} (${formatBytes(report.summary.localBytes)})`
  )
  console.log(`Missing local files: ${report.summary.missingLocalFiles}`)
  console.log(
    `Tiny non-preview rows retained for review: ${report.summary.tinyNonPreviewRows}`
  )
  if (report.remoteVerification.enabled) {
    console.log(
      `Direct i.redd.it files verified: ${report.summary.directFilesVerified}`
    )
    console.log(
      `Larger remote originals found: ${report.summary.verifiedLargerOriginals}`
    )
    console.log(
      `Remote verification failures: ${report.summary.directVerificationFailures}`
    )
  }
  console.log(
    `Decent preview files retained: ${report.summary.decentPreviewFilesRetained}`
  )
  console.log(
    `Low-quality preview files found: ${report.summary.lowQualityPreviewFiles}`
  )
  if (report.apply) {
    console.log(
      `Files moved to recovery archive: ${report.changes.localFilesQuarantined}`
    )
    console.log(`Sidecar rows removed: ${report.changes.sidecarRowsRemoved}`)
    console.log(`Seen-index keys removed: ${report.changes.seenKeysRemoved}`)
    console.log(`Bitwise refs removed: ${report.changes.bitwiseRefsRemoved}`)
    console.log(`Visual refs removed: ${report.changes.visualRefsRemoved}`)
    console.log(`Retry entries written: ${report.changes.retryEntriesWritten}`)
    console.log(`Recovery archive: ${report.quarantineRoot}`)
  }
  console.log(`Report: ${reportPath}`)
}

module.exports = {
  collectAudit,
  getFullResolutionUrl,
  isRedditPreviewRow,
  pruneSeenIndex,
  summarizeAudit,
}

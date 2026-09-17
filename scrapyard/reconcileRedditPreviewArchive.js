'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const minimist = require('minimist')
const sharp = require('sharp')

const { createHashStore } = require('./hashStore')
const { syncModelMetadataToNas } = require('./nasSync')

const argv = minimist(process.argv.slice(2), {
  alias: { h: 'help' },
  boolean: ['help', 'apply', 'sync-nas'],
  string: [
    'archive',
    'dataset-root',
    'nas-root',
    'report-dir',
    'minimum-pixels',
    'minimum-long-edge',
    'concurrency',
  ],
  default: {
    apply: false,
    'sync-nas': false,
    'minimum-pixels': '500000',
    'minimum-long-edge': '768',
    concurrency: '16',
  },
})

if (argv.help || !argv.archive) {
  printHelp()
  process.exit(argv.help ? 0 : 1)
}

main().catch((err) => {
  console.error(`Fatal Reddit preview reconciliation error: ${err.stack || err.message}`)
  process.exitCode = 1
})

function printHelp() {
  console.log(`Usage: node scrapyard/reconcileRedditPreviewArchive.js --archive <path> [options]

Restores usable Reddit preview files from a quality-cleanup recovery archive.
Dry-runs by default. Restores sidecar, seen-index, and hash refs only for files
meeting the configured dimension thresholds. With --sync-nas, corrected media
and state are copied to NAS and remaining pending NAS media are quarantined.

Options:
  --archive <path>           Recovery archive to reconcile. Required.
  --apply                    Apply local reconciliation.
  --sync-nas                 Sync corrected state and quarantine pending NAS copies.
  --dataset-root <path>      Override local dataset root.
  --nas-root <path>          Override NAS dataset root. Default: Z:\\dataset.
  --minimum-pixels <n>       Minimum acceptable pixel count. Default: 500000.
  --minimum-long-edge <n>    Minimum acceptable long edge. Default: 768.
  --concurrency <n>          Concurrent image metadata reads. Default: 16.
  --report-dir <path>        Override report output directory.
  -h, --help                 Show help.
`)
}

async function main() {
  const rootDir = path.join(__dirname, '..')
  const slopvaultRoot = path.join(
    process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
    '.slopvault'
  )
  const archiveRoot = path.resolve(String(argv.archive))
  const datasetRoot = path.resolve(
    String(argv['dataset-root'] || path.join(slopvaultRoot, 'dataset'))
  )
  const nasRoot = path.resolve(String(argv['nas-root'] || 'Z:\\dataset'))
  const reportDir = path.resolve(
    String(
      argv['report-dir'] ||
        path.join(rootDir, 'tmp', 'reddit-preview-quality-reconcile')
    )
  )
  const apply = Boolean(argv.apply)
  const syncNas = Boolean(argv['sync-nas'])
  const minimumPixels = positiveInt(argv['minimum-pixels'], 500000)
  const minimumLongEdge = positiveInt(argv['minimum-long-edge'], 768)
  const concurrency = positiveInt(argv.concurrency, 16)
  const runTag = new Date().toISOString().replace(/[:.]/g, '-')
  const reconcileRoot = path.join(archiveRoot, `reconcile-${runTag}`)
  const nasQuarantineRoot = path.join(
    path.dirname(nasRoot),
    'quarantine',
    'reddit-preview-quality',
    runTag
  )

  validateRoot(archiveRoot, 'archive')
  validateRoot(datasetRoot, 'dataset')
  if (syncNas) validateRoot(nasRoot, 'NAS dataset')
  fs.mkdirSync(reportDir, { recursive: true })

  const records = collectArchiveRecords(archiveRoot, datasetRoot, nasRoot, syncNas)
  const quality = await inspectRecords(records, concurrency, {
    minimumPixels,
    minimumLongEdge,
  })
  const report = {
    generatedAt: new Date().toISOString(),
    apply,
    syncNas,
    archiveRoot,
    datasetRoot,
    nasRoot: syncNas ? nasRoot : null,
    nasQuarantineRoot: syncNas && apply ? nasQuarantineRoot : null,
    thresholds: { minimumPixels, minimumLongEdge },
    summary: {
      archivedRows: records.length,
      decent: quality.decent.length,
      lowQuality: quality.lowQuality.length,
      missingEverywhere: quality.missing.length,
      unreadable: quality.unreadable.length,
      sourceArchive: quality.decent.filter((item) => item.qualitySource === 'archive').length,
      sourceNas: quality.decent.filter((item) => item.qualitySource === 'nas').length,
    },
    changes: {
      localFilesRestored: 0,
      sidecarRowsRestored: 0,
      seenKeysRestored: 0,
      retryEntriesCleared: 0,
      bitwiseRefsRestored: 0,
      visualRefsRestored: 0,
      nasFilesCopied: 0,
      nasFilesAlreadyCurrent: 0,
      nasFilesQuarantined: 0,
      nasStateFilesCopied: 0,
      nasBackupsWritten: 0,
      backupsWritten: 0,
    },
    models: summarizeModels(records, quality),
    lowQuality: quality.lowQuality.map(summarizeRecord),
    missing: quality.missing.map(summarizeRecord),
    unreadable: quality.unreadable.map(summarizeRecord),
  }

  if (apply) {
    fs.mkdirSync(reconcileRoot, { recursive: true })
    applyReconciliation({
      records,
      quality,
      report,
      archiveRoot,
      reconcileRoot,
      datasetRoot,
      nasRoot,
      syncNas,
      nasQuarantineRoot,
    })
  }

  const historyPath = path.join(reportDir, `reddit-preview-reconcile-${runTag}.json`)
  const latestPath = path.join(reportDir, 'reddit-preview-reconcile-latest.json')
  report.historyReportPath = historyPath
  writeJsonAtomic(historyPath, report)
  writeJsonAtomic(latestPath, report)
  printReport(report, latestPath)
}

function collectArchiveRecords(archiveRoot, datasetRoot, nasRoot, inspectNas) {
  const backupRoot = path.join(archiveRoot, 'backups')
  const records = []
  for (const entry of fs.readdirSync(backupRoot, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue
    const modelName = entry.name
    const sidecarPath = path.join(backupRoot, modelName, '.media-dates.json')
    if (!fs.existsSync(sidecarPath)) continue
    const sidecar = readJson(sidecarPath)
    for (const [relativePath, row] of Object.entries(sidecar)) {
      if (relativePath.startsWith('__') || !isPreviewRow(row)) continue
      const datasetRelativePath = normalizePath(`${modelName}/${relativePath}`)
      const archivePath = resolveInside(
        path.join(archiveRoot, 'files'),
        datasetRelativePath
      )
      const localPath = resolveInside(datasetRoot, datasetRelativePath)
      const nasPath = resolveInside(nasRoot, datasetRelativePath)
      const archiveExists = isFile(archivePath)
      const localExists = isFile(localPath)
      records.push({
        modelName,
        relativePath: normalizePath(relativePath),
        datasetRelativePath,
        row,
        archivePath,
        archiveExists,
        localPath,
        localExists,
        nasPath,
        nasExists:
          inspectNas && !archiveExists && !localExists ? isFile(nasPath) : false,
      })
    }
  }
  return records.sort((a, b) =>
    a.datasetRelativePath.localeCompare(b.datasetRelativePath)
  )
}

async function inspectRecords(records, concurrency, thresholds) {
  const decent = []
  const lowQuality = []
  const missing = []
  const unreadable = []

  await runWithConcurrency(records, concurrency, async (record) => {
    const source = record.archiveExists
      ? ['archive', record.archivePath]
      : record.localExists
        ? ['local', record.localPath]
        : record.nasExists
          ? ['nas', record.nasPath]
          : null
    if (!source) {
      missing.push(record)
      return
    }
    try {
      const metadata = await sharp(source[1]).metadata()
      const width = Number(metadata.width || 0)
      const height = Number(metadata.height || 0)
      const quality = {
        width,
        height,
        pixels: width * height,
        longEdge: Math.max(width, height),
        bytes: fs.statSync(source[1]).size,
      }
      const classified = { ...record, qualitySource: source[0], quality }
      if (
        !isBlurredUrl(record.row?.source?.mediaUrl) &&
        quality.pixels >= thresholds.minimumPixels &&
        quality.longEdge >= thresholds.minimumLongEdge
      ) {
        decent.push(classified)
      } else {
        lowQuality.push(classified)
      }
    } catch (err) {
      unreadable.push({ ...record, qualitySource: source[0], error: err.message })
    }
  })

  return { decent, lowQuality, missing, unreadable }
}

function applyReconciliation(context) {
  const {
    records,
    quality,
    report,
    archiveRoot,
    reconcileRoot,
    datasetRoot,
    nasRoot,
    syncNas,
    nasQuarantineRoot,
  } = context
  const goodRefs = new Set(
    quality.decent.map((item) => normalizeRef(item.datasetRelativePath))
  )
  const models = new Map()
  for (const record of records) {
    if (!models.has(record.modelName)) models.set(record.modelName, [])
    models.get(record.modelName).push(record)
  }

  const currentBitwisePath = path.join(datasetRoot, 'bitwiseHashes.v2.json')
  const currentVisualPath = path.join(datasetRoot, 'visualHashes.v2.json')
  backupCurrent(currentBitwisePath, datasetRoot, reconcileRoot, report)
  backupCurrent(currentVisualPath, datasetRoot, reconcileRoot, report)

  for (const [modelName, modelRecords] of models.entries()) {
    reconcileModel({
      modelName,
      modelRecords,
      quality,
      goodRefs,
      report,
      archiveRoot,
      reconcileRoot,
      datasetRoot,
    })
  }

  report.changes.bitwiseRefsRestored = restoreHashRefs({
    currentPath: currentBitwisePath,
    backupPath: path.join(archiveRoot, 'backups', 'bitwiseHashes.v2.json'),
    kind: 'bitwise',
    algorithm: 'md5',
    goodRefs,
  })
  report.changes.visualRefsRestored = restoreHashRefs({
    currentPath: currentVisualPath,
    backupPath: path.join(archiveRoot, 'backups', 'visualHashes.v2.json'),
    kind: 'visual',
    algorithm: 'imghash-16-hex|video-3frame-imghash-16-hex',
    goodRefs,
  })

  if (syncNas) {
    syncReconciledStateToNas({
      models,
      quality,
      report,
      datasetRoot,
      nasRoot,
      nasQuarantineRoot,
      currentBitwisePath,
      currentVisualPath,
    })
  }
}

function reconcileModel(context) {
  const {
    modelName,
    modelRecords,
    quality,
    goodRefs,
    report,
    archiveRoot,
    reconcileRoot,
    datasetRoot,
  } = context
  const modelRoot = path.join(datasetRoot, modelName)
  const currentSidecarPath = path.join(modelRoot, '.media-dates.json')
  const backupSidecarPath = path.join(
    archiveRoot,
    'backups',
    modelName,
    '.media-dates.json'
  )
  const currentSeenPath = path.join(modelRoot, 'log', 'milkmaid-seen-media-index.json')
  const backupSeenPath = path.join(
    archiveRoot,
    'backups',
    modelName,
    'log',
    'milkmaid-seen-media-index.json'
  )
  const retryPath = path.join(modelRoot, 'log', 'reddit-full-resolution-retry.json')

  for (const filePath of [currentSidecarPath, currentSeenPath, retryPath]) {
    backupCurrent(filePath, datasetRoot, reconcileRoot, report)
  }

  const currentSidecar = readJson(currentSidecarPath)
  const backupSidecar = readJson(backupSidecarPath)
  const modelGood = quality.decent.filter((item) => item.modelName === modelName)
  for (const item of modelGood) {
    if (!isFile(item.localPath)) {
      fs.mkdirSync(path.dirname(item.localPath), { recursive: true })
      if (item.archiveExists) {
        fs.renameSync(item.archivePath, item.localPath)
      } else if (item.nasExists) {
        fs.copyFileSync(item.nasPath, item.localPath)
      } else {
        throw new Error(`No restore source for ${item.datasetRelativePath}`)
      }
      report.changes.localFilesRestored += 1
    }
    if (!Object.hasOwn(currentSidecar, item.relativePath)) {
      currentSidecar[item.relativePath] = backupSidecar[item.relativePath]
      report.changes.sidecarRowsRestored += 1
    }
  }
  writeJsonAtomic(currentSidecarPath, currentSidecar, false)

  if (fs.existsSync(backupSeenPath)) {
    const currentSeen = fs.existsSync(currentSeenPath)
      ? readJson(currentSeenPath)
      : { version: 1, mediaUrls: {}, mediaPageUrls: {} }
    const backupSeen = readJson(backupSeenPath)
    for (const bucketName of [
      'mediaUrls',
      'mediaPageUrls',
      'deadMediaUrls',
      'deadMediaPageUrls',
    ]) {
      const sourceBucket = backupSeen[bucketName] || {}
      if (!currentSeen[bucketName]) currentSeen[bucketName] = {}
      for (const [key, entry] of Object.entries(sourceBucket)) {
        if (!goodRefs.has(normalizeRef(entry?.relativePath))) continue
        currentSeen[bucketName][key] = entry
        report.changes.seenKeysRestored += 1
      }
    }
    currentSeen.updatedAt = new Date().toISOString()
    writeJsonAtomic(currentSeenPath, currentSeen)
  }

  if (fs.existsSync(retryPath)) {
    const retry = readJson(retryPath)
    for (const item of modelGood) {
      if (retry.pending && Object.hasOwn(retry.pending, item.datasetRelativePath)) {
        delete retry.pending[item.datasetRelativePath]
        report.changes.retryEntriesCleared += 1
      }
    }
    retry.updatedAt = new Date().toISOString()
    writeJsonAtomic(retryPath, retry)
  }

  void modelRecords
}

function restoreHashRefs({ currentPath, backupPath, kind, algorithm, goodRefs }) {
  if (!fs.existsSync(backupPath)) return 0
  const current = createHashStore({ storePath: currentPath, kind, algorithm })
  const backup = createHashStore({ storePath: backupPath, kind, algorithm })
  current.load()
  backup.load()
  let restored = 0
  for (const entry of backup.getAllEntries()) {
    for (const ref of entry.refs || []) {
      if (!goodRefs.has(normalizeRef(ref))) continue
      const existing = current.get(entry.hash)
      if (existing?.refs?.includes(ref)) continue
      current.add(entry.hash, { relativePath: ref })
      restored += 1
    }
  }
  current.save()
  return restored
}

function syncReconciledStateToNas(context) {
  const {
    models,
    quality,
    report,
    datasetRoot,
    nasRoot,
    nasQuarantineRoot,
    currentBitwisePath,
    currentVisualPath,
  } = context

  for (const item of quality.decent) {
    fs.mkdirSync(path.dirname(item.nasPath), { recursive: true })
    if (filesHaveSameSize(item.localPath, item.nasPath)) {
      report.changes.nasFilesAlreadyCurrent += 1
      continue
    }
    backupNasFile(item.nasPath, nasRoot, nasQuarantineRoot, report)
    fs.copyFileSync(item.localPath, item.nasPath)
    report.changes.nasFilesCopied += 1
  }

  const modelNames = new Set(models.keys())
  for (const entry of fs.readdirSync(datasetRoot, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue
    const retryPath = path.join(
      datasetRoot,
      entry.name,
      'log',
      'reddit-full-resolution-retry.json'
    )
    if (fs.existsSync(retryPath)) modelNames.add(entry.name)
  }

  for (const modelName of modelNames) {
    const retryPath = path.join(
      datasetRoot,
      modelName,
      'log',
      'reddit-full-resolution-retry.json'
    )
    if (fs.existsSync(retryPath)) {
      const retry = readJson(retryPath)
      for (const pending of Object.values(retry.pending || {})) {
        const relativePath = normalizePath(pending?.relativePath)
        if (!relativePath) continue
        const nasPath = resolveInside(nasRoot, relativePath)
        if (!isFile(nasPath)) continue
        const quarantinePath = resolveInside(
          nasQuarantineRoot,
          normalizePath(`files/${relativePath}`)
        )
        fs.mkdirSync(path.dirname(quarantinePath), { recursive: true })
        if (fs.existsSync(quarantinePath)) {
          throw new Error(`NAS quarantine target already exists: ${quarantinePath}`)
        }
        fs.renameSync(nasPath, quarantinePath)
        report.changes.nasFilesQuarantined += 1
      }
    }

    const localSidecarPath = path.join(datasetRoot, modelName, '.media-dates.json')
    const nasSidecarPath = path.join(nasRoot, modelName, '.media-dates.json')
    backupNasFile(nasSidecarPath, nasRoot, nasQuarantineRoot, report)
    const metadataResult = syncModelMetadataToNas({
      modelName,
      datasetDir: datasetRoot,
      nasDatasetDir: nasRoot,
    })
    if (metadataResult.failed > 0) {
      throw new Error(
        `NAS metadata sync failed for ${modelName}: ${metadataResult.failures
          .map((failure) => failure.error)
          .join('; ')}`
      )
    }
    if (fs.existsSync(localSidecarPath)) {
      report.changes.nasStateFilesCopied +=
        metadataResult.copied + metadataResult.replaced
    }

    for (const relativeStatePath of [
      'log/milkmaid-seen-media-index.json',
      'log/reddit-full-resolution-retry.json',
    ]) {
      const sourcePath = resolveInside(
        path.join(datasetRoot, modelName),
        relativeStatePath
      )
      if (!fs.existsSync(sourcePath)) continue
      const targetPath = resolveInside(
        path.join(nasRoot, modelName),
        relativeStatePath
      )
      fs.mkdirSync(path.dirname(targetPath), { recursive: true })
      backupNasFile(targetPath, nasRoot, nasQuarantineRoot, report)
      fs.copyFileSync(sourcePath, targetPath)
      report.changes.nasStateFilesCopied += 1
    }
  }

  for (const sourcePath of [currentBitwisePath, currentVisualPath]) {
    if (!fs.existsSync(sourcePath)) continue
    const targetPath = path.join(nasRoot, path.basename(sourcePath))
    backupNasFile(targetPath, nasRoot, nasQuarantineRoot, report)
    fs.copyFileSync(sourcePath, targetPath)
    report.changes.nasStateFilesCopied += 1
  }
}

function backupNasFile(filePath, nasRoot, nasQuarantineRoot, report) {
  if (!isFile(filePath)) return
  const relativePath = normalizePath(path.relative(nasRoot, filePath))
  if (relativePath.startsWith('..')) {
    throw new Error(`Cannot back up NAS path outside dataset: ${filePath}`)
  }
  const backupPath = resolveInside(
    path.join(nasQuarantineRoot, 'backups'),
    relativePath
  )
  if (fs.existsSync(backupPath)) return
  fs.mkdirSync(path.dirname(backupPath), { recursive: true })
  fs.copyFileSync(filePath, backupPath)
  report.changes.nasBackupsWritten += 1
}

function filesHaveSameSize(leftPath, rightPath) {
  try {
    const left = fs.statSync(leftPath)
    const right = fs.statSync(rightPath)
    return left.isFile() && right.isFile() && left.size === right.size
  } catch {
    return false
  }
}

function backupCurrent(filePath, datasetRoot, reconcileRoot, report) {
  if (!fs.existsSync(filePath)) return
  const relativePath = normalizePath(path.relative(datasetRoot, filePath))
  if (relativePath.startsWith('..')) {
    throw new Error(`Cannot back up path outside dataset: ${filePath}`)
  }
  const targetPath = resolveInside(
    path.join(reconcileRoot, 'backups'),
    relativePath
  )
  fs.mkdirSync(path.dirname(targetPath), { recursive: true })
  fs.copyFileSync(filePath, targetPath)
  report.changes.backupsWritten += 1
}

function summarizeModels(records, quality) {
  const names = [...new Set(records.map((item) => item.modelName))].sort()
  return names.map((modelName) => ({
    modelName,
    archivedRows: records.filter((item) => item.modelName === modelName).length,
    decent: quality.decent.filter((item) => item.modelName === modelName).length,
    lowQuality: quality.lowQuality.filter((item) => item.modelName === modelName)
      .length,
    missing: quality.missing.filter((item) => item.modelName === modelName).length,
    unreadable: quality.unreadable.filter((item) => item.modelName === modelName)
      .length,
  }))
}

function summarizeRecord(item) {
  return {
    modelName: item.modelName,
    relativePath: item.relativePath,
    qualitySource: item.qualitySource || null,
    quality: item.quality || null,
    mediaUrl: item.row?.source?.mediaUrl || null,
    error: item.error || null,
  }
}

function isPreviewRow(row = {}) {
  try {
    return (
      String(row?.source?.site || '').toLowerCase() === 'reddit' &&
      (new URL(String(row?.source?.mediaUrl || '')).hostname.toLowerCase() ===
        'preview.redd.it' ||
        row?.source?.needsFullResolution === true ||
        row?.needsFullResolution === true ||
        String(row?.source?.mediaQuality || '').toLowerCase() ===
          'reddit_preview')
    )
  } catch {
    return false
  }
}

function isBlurredUrl(value) {
  try {
    return new URL(String(value || '')).searchParams.has('blur')
  } catch {
    return false
  }
}

async function runWithConcurrency(items, concurrency, worker) {
  const queue = [...items]
  const workers = Array.from(
    { length: Math.min(Math.max(1, concurrency), queue.length) },
    async () => {
      while (queue.length > 0) await worker(queue.shift())
    }
  )
  await Promise.all(workers)
}

function validateRoot(root, label) {
  if (!fs.existsSync(root)) throw new Error(`${label} root does not exist: ${root}`)
}

function isFile(filePath) {
  try {
    return fs.statSync(filePath).isFile()
  } catch {
    return false
  }
}

function resolveInside(root, relativePath) {
  const resolved = path.resolve(root, ...normalizePath(relativePath).split('/'))
  const relative = path.relative(path.resolve(root), resolved)
  if (relative.startsWith('..') || path.isAbsolute(relative)) {
    throw new Error(`Path escapes root ${root}: ${resolved}`)
  }
  return resolved
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, ''))
}

function writeJsonAtomic(filePath, value, pretty = true) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true })
  const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}`
  fs.writeFileSync(
    tempPath,
    pretty ? `${JSON.stringify(value, null, 2)}\n` : JSON.stringify(value)
  )
  fs.renameSync(tempPath, filePath)
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

function normalizeRef(value) {
  return normalizePath(value).toLowerCase()
}

function positiveInt(value, fallback) {
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function printReport(report, reportPath) {
  console.log(`Mode: ${report.apply ? 'apply' : 'audit'}`)
  console.log(`Archived preview rows: ${report.summary.archivedRows}`)
  console.log(`Decent quality: ${report.summary.decent}`)
  console.log(`Low quality: ${report.summary.lowQuality}`)
  console.log(`Missing everywhere: ${report.summary.missingEverywhere}`)
  console.log(`Unreadable: ${report.summary.unreadable}`)
  if (report.apply) {
    console.log(`Local files restored: ${report.changes.localFilesRestored}`)
    console.log(`Sidecar rows restored: ${report.changes.sidecarRowsRestored}`)
    console.log(`Seen-index keys restored: ${report.changes.seenKeysRestored}`)
    console.log(`Retry entries cleared: ${report.changes.retryEntriesCleared}`)
    console.log(`Bitwise refs restored: ${report.changes.bitwiseRefsRestored}`)
    console.log(`Visual refs restored: ${report.changes.visualRefsRestored}`)
  }
  if (report.syncNas && report.apply) {
    console.log(`NAS media copied: ${report.changes.nasFilesCopied}`)
    console.log(
      `NAS media already current: ${report.changes.nasFilesAlreadyCurrent}`
    )
    console.log(`NAS pending media quarantined: ${report.changes.nasFilesQuarantined}`)
    console.log(`NAS state files copied: ${report.changes.nasStateFilesCopied}`)
    console.log(`NAS quarantine: ${report.nasQuarantineRoot}`)
  }
  console.log(`Report: ${reportPath}`)
}

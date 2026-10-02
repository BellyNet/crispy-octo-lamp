'use strict'

const path = require('path')
const fs = require('fs')
const { exec, execFileSync } = require('child_process')
const crypto = require('crypto')

const {
  collectMp4RelativePaths,
  indexedVideoExtensions,
  mergeNasMp4Entries,
  normalizePath,
  syncNasMp4IndexToMirror,
} = require('./nasMp4Index')
const config = require('./config')
const { isSameDirectory } = require('./datasetLocation')

const MUTABLE_MODEL_METADATA_FILES = ['.media-dates.json']
const MAX_VERIFIED_SIZE_DELTA_BYTES = 32

function hasAcceptableNasSize(localSize, nasSize) {
  return (
    Number.isFinite(localSize) &&
    Number.isFinite(nasSize) &&
    Math.abs(localSize - nasSize) <= MAX_VERIFIED_SIZE_DELTA_BYTES
  )
}

function runRobocopy(command) {
  return new Promise((resolve) => {
    exec(command, { maxBuffer: 8 * 1024 * 1024 }, (error, stdout, stderr) => {
      const code = error?.code ?? 0
      resolve({
        ok: code <= 3,
        code,
        stdout,
        stderr,
      })
    })
  })
}

function filesHaveSameContent(sourcePath, targetPath) {
  if (!fs.existsSync(sourcePath) || !fs.existsSync(targetPath)) return false
  const sourceHash = crypto
    .createHash('sha256')
    .update(fs.readFileSync(sourcePath))
    .digest('hex')
  const targetHash = crypto
    .createHash('sha256')
    .update(fs.readFileSync(targetPath))
    .digest('hex')
  return sourceHash === targetHash
}

function copyMutableMetadataFile(sourcePath, targetPath) {
  if (fs.existsSync(targetPath) && filesHaveSameContent(sourcePath, targetPath)) {
    return 'unchanged'
  }

  try {
    fs.copyFileSync(sourcePath, targetPath)
    return 'copied'
  } catch (err) {
    if (process.platform !== 'win32' || !fs.existsSync(targetPath)) {
      throw err
    }

    const backupPath = `${targetPath}.codex-bak-${process.pid}-${Date.now()}`
    fs.copyFileSync(targetPath, backupPath)
    try {
      removeWindowsHiddenFile(targetPath)
      fs.copyFileSync(sourcePath, targetPath)
      fs.rmSync(backupPath, { force: true })
      return 'replaced'
    } catch (replaceErr) {
      if (!fs.existsSync(targetPath) && fs.existsSync(backupPath)) {
        fs.copyFileSync(backupPath, targetPath)
      }
      if (fs.existsSync(backupPath)) {
        fs.rmSync(backupPath, { force: true })
      }
      replaceErr.message = `${replaceErr.message}; original copy failed: ${err.message}`
      throw replaceErr
    }
  }
}

function removeWindowsHiddenFile(filePath) {
  try {
    fs.rmSync(filePath, { force: true })
  } catch (err) {
    if (process.platform !== 'win32') throw err
    execFileSync('cmd.exe', ['/c', 'del', '/f', '/a:h', filePath], {
      stdio: 'ignore',
    })
  }
}

function getMediaStem(filePath) {
  return path.basename(filePath, path.extname(filePath)).toLowerCase()
}

function isEvictableMediaPath(filePath) {
  return indexedVideoExtensions.has(
    path.extname(String(filePath || '')).toLowerCase()
  )
}

function findNasBackedMediaMatch({
  localPath,
  relativePath,
  nasDatasetDir = config.nasDatasetDir,
} = {}) {
  if (!localPath || !relativePath || !isEvictableMediaPath(localPath)) {
    return null
  }

  const localStat = fs.statSync(localPath)
  if (!localStat.isFile()) return null

  const nasPath = path.join(nasDatasetDir, relativePath)
  if (fs.existsSync(nasPath)) {
    const nasStat = fs.statSync(nasPath)
    if (nasStat.isFile()) {
      return {
        matchType: 'same-path',
        nasPath,
        nasRelativePath: normalizePath(relativePath),
        sizeMatches: hasAcceptableNasSize(localStat.size, nasStat.size),
        localSize: localStat.size,
        nasSize: nasStat.size,
      }
    }
  }

  const relativeDir = path.dirname(relativePath)
  const nasDir = path.join(nasDatasetDir, relativeDir)
  if (!fs.existsSync(nasDir)) return null

  const localStem = getMediaStem(localPath)
  const localExt = path.extname(localPath).toLowerCase()
  for (const entry of fs.readdirSync(nasDir, { withFileTypes: true })) {
    if (!entry.isFile()) continue
    const nasExt = path.extname(entry.name).toLowerCase()
    if (!indexedVideoExtensions.has(nasExt)) continue
    if (nasExt === localExt) continue
    if (getMediaStem(entry.name) !== localStem) continue

    const alternateNasPath = path.join(nasDir, entry.name)
    const nasStat = fs.statSync(alternateNasPath)
    return {
      matchType: 'same-stem',
      nasPath: alternateNasPath,
      nasRelativePath: normalizePath(
        path.relative(nasDatasetDir, alternateNasPath)
      ),
      sizeMatches: null,
      localSize: localStat.size,
      nasSize: nasStat.size,
    }
  }

  return null
}

function syncModelMetadataToNas({
  modelName,
  datasetDir,
  nasDatasetDir = config.nasDatasetDir,
} = {}) {
  if (!modelName || !datasetDir) {
    return {
      copied: 0,
      replaced: 0,
      unchanged: 0,
      skipped: MUTABLE_MODEL_METADATA_FILES.length,
      failed: 0,
      failures: [],
    }
  }

  // NAS-only dataset: the metadata is already in place.
  if (isSameDirectory(datasetDir, nasDatasetDir)) {
    return {
      copied: 0,
      replaced: 0,
      unchanged: MUTABLE_MODEL_METADATA_FILES.length,
      skipped: 0,
      failed: 0,
      failures: [],
      datasetIsNas: true,
    }
  }

  const localModelDir = path.join(datasetDir, modelName)
  const nasModelDir = path.join(nasDatasetDir, modelName)
  let copied = 0
  let replaced = 0
  let unchanged = 0
  let skipped = 0
  let failed = 0
  const failures = []

  for (const fileName of MUTABLE_MODEL_METADATA_FILES) {
    const sourcePath = path.join(localModelDir, fileName)
    if (!fs.existsSync(sourcePath)) {
      skipped += 1
      continue
    }

    fs.mkdirSync(nasModelDir, { recursive: true })
    const targetPath = path.join(nasModelDir, fileName)
    try {
      const status = copyMutableMetadataFile(sourcePath, targetPath)
      if (status === 'copied') copied += 1
      else if (status === 'replaced') replaced += 1
      else unchanged += 1
    } catch (err) {
      failed += 1
      failures.push({
        fileName,
        sourcePath,
        targetPath,
        error: err.message,
      })
    }
  }

  return { copied, replaced, unchanged, skipped, failed, failures }
}

function syncAllModelMetadataToNas({
  datasetDir,
  nasDatasetDir = config.nasDatasetDir,
} = {}) {
  if (!datasetDir || !fs.existsSync(datasetDir)) {
    return {
      models: 0,
      copied: 0,
      replaced: 0,
      unchanged: 0,
      skipped: 0,
      failed: 0,
      failures: [],
    }
  }

  let models = 0
  let copied = 0
  let replaced = 0
  let unchanged = 0
  let skipped = 0
  let failed = 0
  const failures = []
  for (const entry of fs.readdirSync(datasetDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue
    const result = syncModelMetadataToNas({
      modelName: entry.name,
      datasetDir,
      nasDatasetDir,
    })
    if (result.copied > 0 || result.replaced > 0) models += 1
    copied += result.copied
    replaced += result.replaced
    unchanged += result.unchanged
    skipped += result.skipped
    failed += result.failed
    failures.push(...result.failures)
  }
  return { models, copied, replaced, unchanged, skipped, failed, failures }
}

function evictVerifiedLocalMp4s({
  modelName,
  datasetDir,
  nasDatasetDir = config.nasDatasetDir,
} = {}) {
  // NAS-only dataset: every file would "match itself" on the NAS, and
  // deleting it would delete the only copy. Never evict.
  if (isSameDirectory(datasetDir, nasDatasetDir)) {
    return {
      scannedFiles: 0,
      verifiedFiles: 0,
      deletedFiles: 0,
      deletedBytes: 0,
      missingOnNas: 0,
      sizeMismatches: 0,
      sameStemMatches: 0,
      deletedRelativePaths: [],
      datasetIsNas: true,
    }
  }

  const localModelDir = path.join(datasetDir, modelName)
  const relativeVideoPaths = collectMp4RelativePaths(localModelDir, datasetDir)
  const verifiedRelativePaths = []
  const deleteCandidates = []
  let missingOnNas = 0
  let sizeMismatches = 0
  let sameStemMatches = 0

  for (const relativePath of relativeVideoPaths) {
    const localPath = path.join(datasetDir, relativePath)
    const match = findNasBackedMediaMatch({
      localPath,
      relativePath,
      nasDatasetDir,
    })

    if (!match) {
      missingOnNas += 1
      continue
    }

    if (match.matchType === 'same-path' && !match.sizeMatches) {
      sizeMismatches += 1
    } else if (match.matchType === 'same-stem') {
      sameStemMatches += 1
    }

    const normalizedPath = normalizePath(relativePath)
    verifiedRelativePaths.push(normalizedPath)
    deleteCandidates.push({
      localPath,
      relativePath: normalizedPath,
      nasRelativePath: match.nasRelativePath,
      matchType: match.matchType,
      sizeMatches: match.sizeMatches,
    })
  }

  if (verifiedRelativePaths.length > 0) {
    mergeNasMp4Entries(verifiedRelativePaths, datasetDir)
    syncNasMp4IndexToMirror(nasDatasetDir, datasetDir)
  }

  let deletedFiles = 0
  let deletedBytes = 0
  for (const candidate of deleteCandidates) {
    const stat = fs.statSync(candidate.localPath)
    fs.unlinkSync(candidate.localPath)
    deletedFiles += 1
    deletedBytes += stat.size
  }

  return {
    scannedFiles: relativeVideoPaths.length,
    verifiedFiles: verifiedRelativePaths.length,
    deletedFiles,
    deletedBytes,
    missingOnNas,
    sizeMismatches,
    sameStemMatches,
    deletedRelativePaths: deleteCandidates.map(
      (candidate) => candidate.relativePath
    ),
  }
}

async function syncModelToNas({
  modelName,
  datasetDir,
  nasDatasetDir = config.nasDatasetDir,
  log = console,
  successMessage = 'NAS sync complete.',
  failurePrefix = 'NAS sync failed with code',
}) {
  // NAS-only dataset: the scrape already wrote to the NAS. Nothing to copy
  // or evict. (The registry syncs through the dashboard; see registrySync.js.)
  if (isSameDirectory(datasetDir, nasDatasetDir)) {
    return { ok: true, code: 0, stdout: '', stderr: '', datasetIsNas: true }
  }

  const localModelDir = path.join(datasetDir, modelName)
  const nasModelDir = path.join(nasDatasetDir, modelName)

  // Note: per-scrape webm transcode + faststart remux used to live here,
  // but they slowed every model sync and ate ffmpeg cycles on content
  // that may never be viewed. They now run only via the nightly
  // maintenance script (nightly-maintenance.ps1) or by invoking the
  // CLIs directly (scrapyard/transcodeWebm.js, scrapyard/faststartMp4.js).
  const excludedMetadataFiles = MUTABLE_MODEL_METADATA_FILES.join(' ')
  const command = `robocopy "${localModelDir}" "${nasModelDir}" /E /XO /R:2 /W:5 /NFL /NDL /NP /NJH /XF ${excludedMetadataFiles}`
  const result = await runRobocopy(command)

  if (!result.ok) {
    log.error(
      `${failurePrefix} ${result.code}: ${result.stderr || result.stdout || ''}`
    )
    return result
  }

  const metadata = syncModelMetadataToNas({ modelName, datasetDir, nasDatasetDir })
  if (metadata.failed > 0) {
    log.error(
      `NAS metadata sync failed for ${modelName}: ${metadata.failures
        .map((failure) => `${failure.fileName}: ${failure.error}`)
        .join('; ')}`
    )
    return {
      ...result,
      ok: false,
      code: result.code > 3 ? result.code : 12,
      metadata,
    }
  }
  const cleanup = evictVerifiedLocalMp4s({
    modelName,
    datasetDir,
    nasDatasetDir,
  })
  if (cleanup.deletedFiles > 0) {
    log.log(
      `Removed ${cleanup.deletedFiles} NAS-backed local media file(s) after NAS sync.`
    )
  }
  if (cleanup.sizeMismatches > 0) {
    log.warn?.(
      `Evicted ${cleanup.sizeMismatches} local media file(s) with same-path NAS size differences by filename policy.`
    )
  }
  if (cleanup.missingOnNas > 0) {
    log.warn?.(
      `Kept ${cleanup.missingOnNas} local media file(s) that could not be found on NAS.`
    )
  }
  log.log(successMessage)
  return { ...result, cleanup, metadata }
}

module.exports = {
  MAX_VERIFIED_SIZE_DELTA_BYTES,
  hasAcceptableNasSize,
  runRobocopy,
  findNasBackedMediaMatch,
  isEvictableMediaPath,
  syncAllModelMetadataToNas,
  syncModelMetadataToNas,
  evictVerifiedLocalMp4s,
  syncModelToNas,
}

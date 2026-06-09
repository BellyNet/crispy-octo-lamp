'use strict'

const path = require('path')
const fs = require('fs')
const { exec } = require('child_process')

const {
  collectMp4RelativePaths,
  mergeNasMp4Entries,
  syncNasMp4IndexToMirror,
} = require('./nasMp4Index')

const LOCAL_REGISTRY_PATH = path.join(__dirname, '..', 'model_aliases.json')
const MUTABLE_MODEL_METADATA_FILES = ['.media-dates.json']

function runRobocopy(command) {
  return new Promise((resolve) => {
    exec(command, (error, stdout, stderr) => {
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

// Copies the local model_aliases.json to its bind-mount location on the NAS
// (one level above the dataset dir — that's the path docker-compose mounts as
// /app/model_aliases.json inside the dashboard container). Skips silently if
// either the source or the NAS share is missing so this never blocks a scrape.
function pushRegistryToNas({
  nasDatasetDir = process.env.NAS_DATASET_DIR || 'Z:\\dataset',
  log = console,
} = {}) {
  try {
    if (!fs.existsSync(LOCAL_REGISTRY_PATH))
      return { ok: false, reason: 'no-source' }
    const dest = path.join(path.dirname(nasDatasetDir), 'model_aliases.json')
    fs.copyFileSync(LOCAL_REGISTRY_PATH, dest)
    return { ok: true, dest }
  } catch (err) {
    log.warn?.(`Registry push to NAS failed: ${err.message}`)
    return { ok: false, reason: err.message }
  }
}

function syncModelMetadataToNas({
  modelName,
  datasetDir,
  nasDatasetDir = process.env.NAS_DATASET_DIR || 'Z:\\dataset',
} = {}) {
  if (!modelName || !datasetDir) {
    return { copied: 0, skipped: MUTABLE_MODEL_METADATA_FILES.length }
  }

  const localModelDir = path.join(datasetDir, modelName)
  const nasModelDir = path.join(nasDatasetDir, modelName)
  let copied = 0
  let skipped = 0

  for (const fileName of MUTABLE_MODEL_METADATA_FILES) {
    const sourcePath = path.join(localModelDir, fileName)
    if (!fs.existsSync(sourcePath)) {
      skipped += 1
      continue
    }

    fs.mkdirSync(nasModelDir, { recursive: true })
    fs.copyFileSync(sourcePath, path.join(nasModelDir, fileName))
    copied += 1
  }

  return { copied, skipped }
}

function syncAllModelMetadataToNas({
  datasetDir,
  nasDatasetDir = process.env.NAS_DATASET_DIR || 'Z:\\dataset',
} = {}) {
  if (!datasetDir || !fs.existsSync(datasetDir)) {
    return { models: 0, copied: 0, skipped: 0 }
  }

  let models = 0
  let copied = 0
  let skipped = 0
  for (const entry of fs.readdirSync(datasetDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue
    const result = syncModelMetadataToNas({
      modelName: entry.name,
      datasetDir,
      nasDatasetDir,
    })
    if (result.copied > 0) models += 1
    copied += result.copied
    skipped += result.skipped
  }
  return { models, copied, skipped }
}

async function syncModelToNas({
  modelName,
  datasetDir,
  nasDatasetDir = process.env.NAS_DATASET_DIR || 'Z:\\dataset',
  log = console,
  successMessage = 'NAS sync complete.',
  failurePrefix = 'NAS sync failed with code',
}) {
  const localModelDir = path.join(datasetDir, modelName)
  const nasModelDir = path.join(nasDatasetDir, modelName)

  // Note: per-scrape webm transcode + faststart remux used to live here,
  // but they slowed every model sync and ate ffmpeg cycles on content
  // that may never be viewed. They now run only via the nightly
  // maintenance script (nightly-maintenance.ps1) or by invoking the
  // CLIs directly (scrapyard/transcodeWebm.js, scrapyard/faststartMp4.js).
  const command = `robocopy "${localModelDir}" "${nasModelDir}" /E /XC /XN /XO /R:2 /W:5`
  const result = await runRobocopy(command)

  if (!result.ok) {
    log.error(
      `${failurePrefix} ${result.code}: ${result.stderr || result.stdout || ''}`
    )
    return result
  }

  syncModelMetadataToNas({ modelName, datasetDir, nasDatasetDir })
  mergeNasMp4Entries(
    collectMp4RelativePaths(localModelDir, datasetDir),
    datasetDir
  )
  syncNasMp4IndexToMirror(nasDatasetDir, datasetDir)
  pushRegistryToNas({ nasDatasetDir, log })
  log.log(successMessage)
  return result
}

module.exports = {
  runRobocopy,
  syncAllModelMetadataToNas,
  syncModelMetadataToNas,
  syncModelToNas,
  pushRegistryToNas,
}

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
    if (!fs.existsSync(LOCAL_REGISTRY_PATH)) return { ok: false, reason: 'no-source' }
    const dest = path.join(path.dirname(nasDatasetDir), 'model_aliases.json')
    fs.copyFileSync(LOCAL_REGISTRY_PATH, dest)
    return { ok: true, dest }
  } catch (err) {
    log.warn?.(`Registry push to NAS failed: ${err.message}`)
    return { ok: false, reason: err.message }
  }
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
  const command = `robocopy "${localModelDir}" "${nasModelDir}" /E /XC /XN /XO /R:2 /W:5`
  const result = await runRobocopy(command)

  if (!result.ok) {
    log.error(
      `${failurePrefix} ${result.code}: ${result.stderr || result.stdout || ''}`
    )
    return result
  }

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
  syncModelToNas,
  pushRegistryToNas,
}

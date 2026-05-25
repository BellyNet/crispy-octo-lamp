'use strict'

const path = require('path')
const { exec } = require('child_process')

const {
  collectMp4RelativePaths,
  mergeNasMp4Entries,
  syncNasMp4IndexToMirror,
} = require('./nasMp4Index')

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

async function syncModelToNas({
  modelName,
  datasetDir,
  nasDatasetDir = process.env.NAS_DATASET_DIR || 'Z:\\dataset',
  mode = 'mirror',
  log = console,
  successMessage = 'NAS sync complete.',
  failurePrefix = 'NAS sync failed with code',
}) {
  const localModelDir = path.join(datasetDir, modelName)
  const nasModelDir = path.join(nasDatasetDir, modelName)
  const robocopyMode = mode === 'additive' ? '/E /XC /XN /XO' : '/MIR'
  const command = `robocopy "${localModelDir}" "${nasModelDir}" ${robocopyMode} /R:2 /W:5`
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
  log.log(successMessage)
  return result
}

module.exports = {
  runRobocopy,
  syncModelToNas,
}

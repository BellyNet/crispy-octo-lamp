'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const minimist = require('minimist')
const { getRedditOriginalMediaUrl } = require('./redditFullResolutionRetry')

const argv = minimist(process.argv.slice(2), {
  boolean: ['apply'],
  string: ['dataset-root', 'report-dir'],
  default: { apply: false },
})
const datasetRoot = path.resolve(
  argv['dataset-root'] ||
    path.join(process.env.APPDATA || os.homedir(), '.slopvault', 'dataset')
)
const reportDir = path.resolve(
  argv['report-dir'] || path.join('tmp', 'reddit-original-url-repair')
)
const stamp = new Date().toISOString().replace(/[:.]/g, '-')
const report = {
  generatedAt: new Date().toISOString(),
  apply: Boolean(argv.apply),
  datasetRoot,
  modelsScanned: 0,
  modelsChanged: 0,
  retryUrlsCorrected: 0,
  retryAttemptsReset: 0,
  modelChanges: [],
  samples: [],
  errors: [],
}

if (!fs.existsSync(datasetRoot)) {
  throw new Error(`Dataset root not found: ${datasetRoot}`)
}
fs.mkdirSync(reportDir, { recursive: true })

for (const model of fs.readdirSync(datasetRoot, { withFileTypes: true })) {
  if (!model.isDirectory()) continue
  const retryPath = path.join(
    datasetRoot,
    model.name,
    'log',
    'reddit-full-resolution-retry.json'
  )
  if (!fs.existsSync(retryPath)) continue
  report.modelsScanned += 1
  try {
    const original = fs.readFileSync(retryPath, 'utf8')
    const state = JSON.parse(original)
    let changed = 0
    let reset = 0
    for (const pending of Object.values(state.pending || {})) {
      const previous = pending?.fullResolutionUrl
      const corrected = getRedditOriginalMediaUrl(previous)
      if (!corrected || corrected === previous) continue
      pending.fullResolutionUrl = corrected
      if (pending.lastAttemptAt || pending.attemptCount) reset += 1
      pending.lastAttemptAt = null
      pending.attemptCount = 0
      changed += 1
      if (report.samples.length < 20) {
        report.samples.push({
          model: model.name,
          relativePath: pending.relativePath,
          previous,
          corrected,
        })
      }
    }
    if (changed === 0) continue
    report.modelsChanged += 1
    report.retryUrlsCorrected += changed
    report.retryAttemptsReset += reset
    report.modelChanges.push({ model: model.name, retryUrlsCorrected: changed })
    if (argv.apply) {
      const backupPath = path.join(reportDir, 'backups', stamp, model.name)
      fs.mkdirSync(backupPath, { recursive: true })
      fs.writeFileSync(
        path.join(backupPath, path.basename(retryPath)),
        original
      )
      state.updatedAt = new Date().toISOString()
      const tempPath = `${retryPath}.tmp-${process.pid}`
      fs.writeFileSync(tempPath, `${JSON.stringify(state, null, 2)}\n`)
      fs.renameSync(tempPath, retryPath)
    }
  } catch (err) {
    report.errors.push({ model: model.name, error: err.message })
  }
}

const reportPath = path.join(
  reportDir,
  'reddit-original-url-repair-latest.json'
)
fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`)
console.log(
  `${argv.apply ? 'Corrected' : 'Would correct'} ${report.retryUrlsCorrected} Reddit original URLs across ${report.modelsChanged} models; reset ${report.retryAttemptsReset} attempts. Report: ${reportPath}`
)
if (report.errors.length > 0) {
  console.error(
    `${report.errors.length} model retry file(s) could not be read.`
  )
  process.exitCode = 1
}

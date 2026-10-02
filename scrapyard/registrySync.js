'use strict'

// Keeps the PC's model_aliases.json in step with the one on the NAS, which
// is the one the dashboard edits. Runs before and after every scrape on the
// PC (worker tasks and CLI scrapes), or by hand: `npm run registry:sync`.
//
// 1. Changes made on the PC since the last sync (by a scrape, or by hand with
//    the registry CLI tools) are found by diffing against the last synced
//    copy, and sent to the NAS as operations, which merge with anything the
//    dashboard changed meanwhile.
// 2. The merged registry comes back and replaces the PC's copy.
//
// Everything goes through the dashboard API: over SMB the PC can read stale
// cached copies of files the NAS changed.

const fs = require('fs')
const path = require('path')

const config = require('./config')
const { loadModelRegistry, saveModelRegistry } = require('./modelRegistry')
const { diffRegistries } = require('./registryOps')

const SNAPSHOT_PATH = path.join(
  config.slopvaultRoot,
  'model_aliases.synced.json'
)

function readJson(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'))
  } catch {
    return null
  }
}

async function syncRegistry({
  backend = require('./scrapeBackends').createRemoteBackend(),
  registryPath = config.registryPath,
  snapshotPath = SNAPSHOT_PATH,
} = {}) {
  const local = fs.existsSync(registryPath)
    ? loadModelRegistry(registryPath)
    : {}
  const base = readJson(snapshotPath)
  // Without a snapshot there's no telling which local differences are this
  // PC's own changes and which are just a stale copy (from git, or from
  // before something was deleted on the dashboard), so the NAS copy wins.
  const ops = base ? diffRegistries(base, local) : []
  const nas = ops.length
    ? (await backend.registryApply(ops)).registry
    : (await backend.registryPull()).registry

  saveModelRegistry(registryPath, nas)
  // Snapshot what is on disk now (after this PC's formatting), so the next
  // diff only sees real changes.
  fs.mkdirSync(path.dirname(snapshotPath), { recursive: true })
  fs.writeFileSync(
    snapshotPath,
    JSON.stringify(loadModelRegistry(registryPath))
  )
  return { sent: ops.length, models: Object.keys(nas).length }
}

module.exports = { syncRegistry, SNAPSHOT_PATH }

if (require.main === module) {
  syncRegistry()
    .then(({ sent, models }) => {
      console.log(
        `Registry synced with the NAS: sent ${sent} change(s); ${models} models.`
      )
    })
    .catch((err) => {
      console.error(`Registry sync failed: ${err.message}`)
      process.exitCode = 1
    })
}

'use strict'

// The dashboard's delete bin: datasetDir/.dashboard-trash/<runTimestamp>/...
//
// purgeOldTrash: the nightly pass permanently removes trash runs older than
// the retention period (DASHBOARD_TRASH_RETENTION_DAYS, default 30; 0 keeps
// everything). A run's age comes from its folder name, the time it was
// trashed, falling back to the folder's mtime.
//
// removeModelFromRegistry: a model deleted from the dashboard also leaves the
// model registry, or the next all-sources scrape would download it again.
// Its registry entry is saved in the trashed folder (registry-entry.json) so
// a restore can put it back.

const fs = require('fs')
const path = require('path')
const { updateRegistry } = require('../scrapyard/registryStore')

const TRASH_DIRNAME = '.dashboard-trash'
const DAY_MS = 24 * 60 * 60 * 1000

function retentionDays() {
  const raw = process.env.DASHBOARD_TRASH_RETENTION_DAYS
  const days = raw === undefined || raw === '' ? 30 : Number(raw)
  return Number.isFinite(days) && days >= 0 ? days : 30
}

// "2026-09-16T04-36-08-136Z" -> Date (the format trash runs are named with).
function parseRunTimestamp(name) {
  const match = String(name).match(
    /^(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})-(\d{3})Z$/
  )
  if (!match) return null
  const [, date, hh, mm, ss, ms] = match
  const time = Date.parse(`${date}T${hh}:${mm}:${ss}.${ms}Z`)
  return Number.isFinite(time) ? new Date(time) : null
}

async function purgeOldTrash({
  datasetDir,
  days = retentionDays(),
  now = Date.now(),
  log = console,
} = {}) {
  const root = path.join(datasetDir, TRASH_DIRNAME)
  const result = { removed: [], kept: 0 }
  if (!days) return result
  let entries
  try {
    entries = await fs.promises.readdir(root, { withFileTypes: true })
  } catch {
    return result
  }
  const cutoff = now - days * DAY_MS
  for (const entry of entries) {
    if (!entry.isDirectory()) continue
    const dir = path.join(root, entry.name)
    let trashedAt = parseRunTimestamp(entry.name)?.getTime()
    if (!trashedAt) {
      try {
        trashedAt = (await fs.promises.stat(dir)).mtimeMs
      } catch {
        continue
      }
    }
    if (trashedAt >= cutoff) {
      result.kept += 1
      continue
    }
    try {
      await fs.promises.rm(dir, { recursive: true, force: true })
      result.removed.push(entry.name)
    } catch (err) {
      log.warn(`  Trash: could not empty ${entry.name}: ${err.message}`)
    }
  }
  if (result.removed.length) {
    log.log(
      `  Trash:     emptied ${result.removed.length} delete run(s) older than ${days} days`
    )
  }
  return result
}

// Drops `model` from the registry; returns its entry (or null if it had none).
async function removeModelFromRegistry(model, trashedModelDir, registryPath) {
  let removed = null
  await updateRegistry((registry) => {
    if (!registry[model]) return
    removed = registry[model]
    delete registry[model]
  }, registryPath)
  if (removed && trashedModelDir) {
    fs.writeFileSync(
      path.join(trashedModelDir, 'registry-entry.json'),
      JSON.stringify({ model, entry: removed }, null, 2) + '\n'
    )
  }
  return removed
}

module.exports = {
  TRASH_DIRNAME,
  parseRunTimestamp,
  purgeOldTrash,
  removeModelFromRegistry,
  retentionDays,
}

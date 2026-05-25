'use strict'

/**
 * cleanupFlaggedFiles.js
 *
 * Reads dataset/<user>/.dashboard-flags.json (written by the dashboard's
 * flag-for-deletion toggle) and removes the flagged files.
 *
 * Modes (default is dry-run — nothing happens until you pass --apply):
 *   node scrapyard/cleanupFlaggedFiles.js                  # show what would happen
 *   node scrapyard/cleanupFlaggedFiles.js --apply          # move flagged files to .dashboard-trash/<ts>/
 *   node scrapyard/cleanupFlaggedFiles.js --apply --hard   # permanently delete (skip trash)
 *
 * Other flags:
 *   --user <name>     Only process this model. Defaults to all models.
 *   --dataset <path>  Override DATASET_DIR.
 */

const fs = require('fs')
const path = require('path')
const os = require('os')
const minimist = require('minimist')

const args = minimist(process.argv.slice(2), {
  boolean: ['apply', 'hard'],
  string:  ['user', 'dataset'],
})

const slopvaultRoot = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  '.slopvault'
)
const datasetDir = path.resolve(
  args.dataset || process.env.DATASET_DIR || path.join(slopvaultRoot, 'dataset')
)

const FLAGS_FILENAME = '.dashboard-flags.json'
const TRASH_DIRNAME  = '.dashboard-trash'

function listModelDirs() {
  if (!fs.existsSync(datasetDir)) {
    console.error(`Dataset dir not found: ${datasetDir}`)
    process.exit(1)
  }
  let dirs
  try {
    dirs = fs.readdirSync(datasetDir, { withFileTypes: true })
  } catch (err) {
    console.error(`Cannot read ${datasetDir}: ${err.message}`)
    process.exit(1)
  }
  return dirs
    .filter((e) => e.isDirectory() && !e.name.startsWith('.'))
    .map((e) => e.name)
    .sort()
}

function readFlags(userDir) {
  try {
    const data = JSON.parse(fs.readFileSync(path.join(userDir, FLAGS_FILENAME), 'utf8'))
    return data && data.flags && typeof data.flags === 'object' ? data : null
  } catch {
    return null
  }
}

function writeFlags(userDir, data) {
  const p = path.join(userDir, FLAGS_FILENAME)
  const tmp = p + '.tmp'
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2))
  fs.renameSync(tmp, p)
}

// Resolve a relative "<folder>/<filename>" key against the user's dir, and
// confirm the resulting absolute path is still inside that dir. Refuse anything
// that escapes — defends against a tampered sidecar.
function safeResolve(userDir, relKey) {
  const resolved = path.resolve(userDir, relKey)
  const base = path.resolve(userDir)
  if (resolved !== base && !resolved.startsWith(base + path.sep)) return null
  return resolved
}

function trashDestFor(filePath, runTimestamp) {
  // Mirror the dataset path under datasetDir/.dashboard-trash/<ts>/...
  const rel = path.relative(datasetDir, filePath)
  return path.join(datasetDir, TRASH_DIRNAME, runTimestamp, rel)
}

function moveToTrash(filePath, runTimestamp) {
  const dst = trashDestFor(filePath, runTimestamp)
  fs.mkdirSync(path.dirname(dst), { recursive: true })
  fs.renameSync(filePath, dst)
  return dst
}

function main() {
  const targets = args.user ? [args.user] : listModelDirs()
  const apply   = !!args.apply
  const hard    = !!args.hard
  const runTs   = new Date().toISOString().replace(/[:.]/g, '-')

  console.log('')
  console.log(`Dataset: ${datasetDir}`)
  console.log(`Mode:    ${apply ? (hard ? 'APPLY (HARD DELETE)' : `APPLY (move to ${TRASH_DIRNAME}/${runTs}/)`) : 'dry-run (no changes)'}`)
  console.log(`Models:  ${targets.length}`)
  console.log('')

  let totalFlagged = 0
  let totalActed   = 0
  let totalMissing = 0
  let totalSkipped = 0

  for (const username of targets) {
    const userDir = path.join(datasetDir, username)
    const data = readFlags(userDir)
    if (!data) continue
    const entries = Object.entries(data.flags).filter(([, v]) => v && v.flagged)
    if (!entries.length) continue

    console.log(`── ${username} ── ${entries.length} flagged`)

    let mutated = false
    for (const [relKey, meta] of entries) {
      totalFlagged++
      const filePath = safeResolve(userDir, relKey)
      if (!filePath) {
        console.log(`  ! ${relKey} — unsafe path, skipped`)
        totalSkipped++
        continue
      }

      if (!fs.existsSync(filePath)) {
        // Already gone (deleted externally). Clean up the sidecar entry too.
        console.log(`  · ${relKey} — already missing`)
        totalMissing++
        delete data.flags[relKey]
        mutated = true
        continue
      }

      const flaggedAt = meta && meta.addedAt ? `  [flagged ${meta.addedAt.slice(0, 10)}]` : ''
      if (!apply) {
        console.log(`  - ${relKey}${flaggedAt}`)
        continue
      }

      try {
        if (hard) {
          fs.unlinkSync(filePath)
          console.log(`  ✗ deleted ${relKey}${flaggedAt}`)
        } else {
          const dst = moveToTrash(filePath, runTs)
          const trashRel = path.relative(datasetDir, dst)
          console.log(`  → trash ${relKey} → ${trashRel}${flaggedAt}`)
        }
        delete data.flags[relKey]
        mutated = true
        totalActed++
      } catch (err) {
        console.log(`  ! ${relKey} — failed: ${err.message}`)
        totalSkipped++
      }
    }

    if (mutated) writeFlags(userDir, data)
  }

  console.log('')
  console.log('Summary:')
  console.log(`  Flagged:  ${totalFlagged}`)
  if (apply) console.log(`  ${hard ? 'Deleted' : 'Trashed'}: ${totalActed}`)
  console.log(`  Missing:  ${totalMissing}`)
  console.log(`  Skipped:  ${totalSkipped}`)
  if (!apply) console.log('  Re-run with --apply to act on these. Without --hard the files go to a per-run trash dir first.')
  console.log('')
}

main()

'use strict'

/**
 * backfill-exif-dates.js
 *
 * Scans all existing media in the dataset and populates .media-dates.json
 * sidecars for files that haven't been processed yet.
 *
 * Usage:
 *   node milkmaid/backfill-exif-dates.js [--user <username>] [--force]
 *
 * Options:
 *   --user <name>   Only process this user (default: all users)
 *   --force         Re-extract even if sidecar entry already exists
 *   --concurrency   Number of files to process in parallel (default: 8)
 */

const fs = require('fs')
const path = require('path')
const minimist = require('minimist')
const pLimit = require('p-limit')

const mediaDates = require('./media-dates.js')

const argv = minimist(process.argv.slice(2))
const FORCE = !!argv.force
const TARGET_USER = argv.user || null
const CONCURRENCY = parseInt(argv.concurrency, 10) || 8

const APPDATA =
  process.env.APPDATA ||
  path.join(process.env.HOME || process.env.USERPROFILE, 'AppData', 'Roaming')
const datasetDir =
  process.env.DATASET_DIR || path.join(APPDATA, '.slopvault', 'dataset')

const MEDIA_FOLDERS = ['images', 'gif', 'webm']
const IMAGE_EXTS = new Set(['.jpg', '.jpeg', '.png', '.gif', '.tiff', '.webp'])
const VIDEO_EXTS = new Set(['.mp4', '.webm', '.mov', '.avi'])

// ─── HELPERS ──────────────────────────────────────────────────────────────────

function log(msg) {
  process.stdout.write(msg + '\n')
}

function bar(done, total, width = 30) {
  const pct = total ? done / total : 0
  const filled = Math.round(pct * width)
  return `[${'█'.repeat(filled)}${' '.repeat(width - filled)}] ${done}/${total}`
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function processUser(username) {
  const userDir = path.join(datasetDir, username)

  // Collect all media files for this user
  const files = []
  for (const folder of MEDIA_FOLDERS) {
    const folderPath = path.join(userDir, folder)
    let entries
    try {
      entries = fs.readdirSync(folderPath)
    } catch {
      continue
    }
    for (const filename of entries) {
      const ext = path.extname(filename).toLowerCase()
      if (!IMAGE_EXTS.has(ext) && !VIDEO_EXTS.has(ext)) continue
      files.push({ folder, filename, filePath: path.join(folderPath, filename) })
    }
  }

  if (!files.length) {
    log(`  ${username}: no media files found`)
    return { exif: 0, video: 0, filename: 0, uploaded: 0, none: 0, skipped: 0 }
  }

  // Load existing sidecar to check what's already done
  const sidecarFile = path.join(userDir, mediaDates.SIDECAR_FILENAME)
  let sidecar = { __version: 2 }
  try {
    const raw = JSON.parse(fs.readFileSync(sidecarFile, 'utf8'))
    sidecar = raw.__version === 2 ? raw : { __version: 2 }
  } catch {}

  const tally = { exif: 0, video: 0, filename: 0, uploaded: 0, none: 0, skipped: 0 }
  let done = 0

  const limit = pLimit(CONCURRENCY)

  process.stdout.write(`  ${username}: processing ${files.length} files...\r`)

  await Promise.all(
    files.map((file) =>
      limit(async () => {
        const key = `${file.folder}/${file.filename}`

        // Skip if already in sidecar (unless --force)
        if (!FORCE && sidecar[key]) {
          tally.skipped++
          done++
          return
        }

        const ext = path.extname(file.filename).toLowerCase()
        const isVideo = VIDEO_EXTS.has(ext)
        const filenameDate = mediaDates.extractFilenameDate(file.filename)

        let exifDate = null
        let videoDate = null
        let uploadedDate = null

        if (isVideo) {
          videoDate = await mediaDates.extractVideoDateFromFile(file.filePath)
        } else {
          // Read buffer for EXIF extraction
          try {
            const buffer = fs.readFileSync(file.filePath)
            exifDate = await mediaDates.extractExifFromBuffer(buffer)
          } catch {}
        }

        // Get mtime as the upload date (set by milkmaid to platform upload date)
        try {
          const stat = fs.statSync(file.filePath)
          const mt = stat.mtime
          if (mt && mt.getFullYear() >= 1990 && mt.getFullYear() <= 2035) {
            uploadedDate = mt.toISOString()
          }
        } catch {}

        sidecar[key] = {
          exif: exifDate || null,
          video: videoDate || null,
          filename: filenameDate || null,
          uploaded: uploadedDate || null,
        }

        // Tally the best source
        if (exifDate)        tally.exif++
        else if (videoDate)  tally.video++
        else if (filenameDate) tally.filename++
        else if (uploadedDate) tally.uploaded++
        else                 tally.none++

        done++
        if (done % 50 === 0 || done === files.length) {
          process.stdout.write(`  ${username}: ${bar(done, files.length)}\r`)
        }
      })
    )
  )

  // Write sidecar
  fs.writeFileSync(sidecarFile, JSON.stringify(sidecar), 'utf8')

  const newlyProcessed = files.length - tally.skipped
  log(
    `  ${username}: done  ` +
    `exif=${tally.exif} video=${tally.video} filename=${tally.filename} ` +
    `upload=${tally.uploaded} none=${tally.none} skipped=${tally.skipped}` +
    (newlyProcessed !== files.length ? ` (${tally.skipped} already cached)` : '')
  )

  return tally
}

async function run() {
  log('\nBackfill media creation dates')
  log(`Dataset: ${datasetDir}`)
  if (FORCE) log('Mode: --force (re-extracting all entries)')
  log('')

  // Find ffprobe once at startup
  const probe = await mediaDates.findFfprobe()
  log(probe ? `ffprobe: ${probe}` : 'ffprobe: not found — video container dates skipped')
  log('')

  let users
  try {
    users = fs
      .readdirSync(datasetDir, { withFileTypes: true })
      .filter((e) => e.isDirectory() && !e.name.startsWith('.'))
      .map((e) => e.name)
      .sort()
  } catch (err) {
    log(`ERROR: Cannot read dataset dir: ${err.message}`)
    process.exit(1)
  }

  if (TARGET_USER) {
    if (!users.includes(TARGET_USER)) {
      log(`ERROR: User "${TARGET_USER}" not found in dataset`)
      process.exit(1)
    }
    users = [TARGET_USER]
  }

  log(`Users to process: ${users.length}\n`)

  const totals = { exif: 0, video: 0, filename: 0, uploaded: 0, none: 0, skipped: 0 }

  for (const username of users) {
    const t = await processUser(username)
    for (const k of Object.keys(totals)) totals[k] += t[k] || 0
  }

  log('\n─────────────────────────────────────────')
  log('Summary across all users:')
  log(`  EXIF date found:       ${totals.exif}`)
  log(`  Video container date:  ${totals.video}`)
  log(`  Filename timestamp:    ${totals.filename}`)
  log(`  Upload date only:      ${totals.uploaded}`)
  log(`  No date found:         ${totals.none}`)
  log(`  Already cached:        ${totals.skipped}`)
  log(`  Total files:           ${Object.values(totals).reduce((a, b) => a + b, 0)}`)
  log('')
}

run().catch((err) => {
  console.error(err)
  process.exit(1)
})

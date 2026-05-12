'use strict'

// One-time metadata pre-build script.
// Run with:  node dashboard/build-meta-cache.js
// Or:        npm run dashboard:prebuild
//
// Walks every model directory, extracts image dimensions (sharp) and video
// duration + date (ffprobe) for every file not already in the cache, and
// writes per-user JSON files to THUMB_DIR/meta/.  After this runs, the
// dashboard serves media pages from cache and only processes brand-new files.

const fs = require('fs')
const path = require('path')
const pLimit = require('p-limit')
const sharp = require('sharp')
const mediaDates = require('../milkmaid/media-dates.js')
const MetaCache = require('./meta-cache.js')

const APPDATA =
  process.env.APPDATA ||
  path.join(process.env.HOME || process.env.USERPROFILE, 'AppData', 'Roaming')
const slopvaultRoot = path.join(APPDATA, '.slopvault')
const datasetDir =
  process.env.DATASET_DIR || path.join(slopvaultRoot, 'dataset')
const THUMB_DIR =
  process.env.THUMB_DIR || path.join(slopvaultRoot, '.dashboard-thumbs')

const MEDIA_FOLDERS = ['images', 'gif', 'webm']
const IMAGE_EXTS = new Set(['.jpg', '.jpeg', '.png', '.gif'])
const VIDEO_EXTS = new Set(['.mp4', '.webm'])
const ALL_EXTS = new Set([...IMAGE_EXTS, ...VIDEO_EXTS])

let ffprobePath = null

async function processFile(cache, username, userDir, folder, filename) {
  const filePath = path.join(userDir, folder, filename)
  let stat
  try { stat = await fs.promises.stat(filePath) } catch { return }

  // Already cached and valid — skip
  if (cache.get(username, folder, filename, stat)) return

  const ext = path.extname(filename).toLowerCase()
  const meta = {}

  if (IMAGE_EXTS.has(ext)) {
    try {
      const m = await sharp(filePath).metadata()
      meta.width = m.width || 0
      meta.height = m.height || 0
    } catch {
      meta.width = 0
      meta.height = 0
    }
  } else if (VIDEO_EXTS.has(ext)) {
    const probed = await mediaDates.probeVideoFile(filePath).catch(() => ({}))
    meta.duration = probed.duration || 0
    if (probed.videoDate) meta.videoDate = probed.videoDate
  }

  cache.set(username, folder, filename, stat, meta)
}

async function main() {
  ffprobePath = await mediaDates.findFfprobe()
  if (!ffprobePath) console.warn('  ffprobe not found — video duration/dates will not be cached')

  let dirs
  try {
    dirs = await fs.promises.readdir(datasetDir, { withFileTypes: true })
  } catch (err) {
    console.error(`Cannot read dataset dir (${datasetDir}): ${err.message}`)
    process.exit(1)
  }

  const modelDirs = dirs.filter((e) => e.isDirectory() && !e.name.startsWith('.'))
  const cache = new MetaCache(THUMB_DIR)

  console.log(`\n  Dataset:   ${datasetDir}`)
  console.log(`  Cache dir: ${path.join(THUMB_DIR, 'meta')}`)
  console.log(`  Models:    ${modelDirs.length}\n`)

  // Count all files first so we can show accurate progress
  let totalFiles = 0
  for (const dir of modelDirs) {
    const userDir = path.join(datasetDir, dir.name)
    for (const folder of MEDIA_FOLDERS) {
      try {
        const files = await fs.promises.readdir(path.join(userDir, folder))
        totalFiles += files.filter((f) => ALL_EXTS.has(path.extname(f).toLowerCase())).length
      } catch {}
    }
  }

  console.log(`  Files:     ${totalFiles} total\n`)
  if (totalFiles === 0) { console.log('  Nothing to do.'); return }

  // Images use high concurrency (sharp is fast), videos use lower (ffprobe spawns processes)
  const imgLimit = pLimit(16)
  const vidLimit = pLimit(4)

  let processed = 0
  let hits = 0
  const startMs = Date.now()

  for (const dir of modelDirs) {
    const username = dir.name
    const userDir = path.join(datasetDir, username)

    // Eagerly load existing cache so hits are detected correctly
    cache._entry(username)

    const tasks = []
    for (const folder of MEDIA_FOLDERS) {
      let files
      try { files = await fs.promises.readdir(path.join(userDir, folder)) } catch { continue }

      for (const file of files) {
        const ext = path.extname(file).toLowerCase()
        if (!ALL_EXTS.has(ext)) continue

        const isVideo = VIDEO_EXTS.has(ext)
        const limiter = isVideo ? vidLimit : imgLimit

        tasks.push(limiter(async () => {
          const filePath = path.join(userDir, folder, file)
          let stat
          try { stat = await fs.promises.stat(filePath) } catch {
            processed++; return
          }

          if (cache.get(username, folder, file, stat)) {
            hits++
          } else {
            await processFile(cache, username, userDir, folder, file)
          }

          processed++
          if (processed % 200 === 0 || processed === totalFiles) {
            const pct = Math.round((processed / totalFiles) * 100)
            const elapsedS = ((Date.now() - startMs) / 1000).toFixed(0)
            process.stdout.write(
              `\r  Progress: ${processed}/${totalFiles} (${pct}%) — ${hits} cached — ${elapsedS}s elapsed`
            )
          }
        }))
      }
    }

    await Promise.all(tasks)
    cache.flush(username)
  }

  const totalS = ((Date.now() - startMs) / 1000).toFixed(1)
  const built = totalFiles - hits
  console.log(`\n\n  Done in ${totalS}s`)
  console.log(`  ${built} files indexed, ${hits} already cached`)
  console.log(`  Cache written to ${path.join(THUMB_DIR, 'meta')}/\n`)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})

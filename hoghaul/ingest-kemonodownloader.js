'use strict'

/**
 * ingest-kemonodownloader.js
 *
 * Ingests files downloaded by KemonoDownloader into the slopvault dataset,
 * sorting them into the correct buckets (images / webm / gif) and running
 * our standard bitwise + visual deduplication and date sidecar recording.
 *
 * Usage:
 *   node hoghaul/ingest-kemonodownloader.js --model=<name> --src=<dir>
 *   node hoghaul/ingest-kemonodownloader.js --model=<name>
 *     (uses default KemonoDownloader AppData output dir for that creator)
 *
 * Options:
 *   --model=<name>   Canonical model name (as in model_aliases.json)
 *   --src=<dir>      Source directory to ingest from (walks recursively)
 *   --dry-run        Print what would happen without moving anything
 *   --no-dedup       Skip visual hash dedup (bitwise dedup always runs)
 */

const fs = require('fs')
const path = require('path')
const { createHash } = require('crypto')
const minimist = require('minimist')

const {
  loadVisualHashCache,
  saveVisualHashCache,
  getVisualHashFromBuffer,
  isVisualDupe,
  addVisualHash,
} = require('../scrapyard/visualHasher')

const {
  loadBitwiseHashCache,
  saveBitwiseHashCache,
  isBitwiseDupe,
  addBitwiseHash,
} = require('../scrapyard/bitwiseHasher')

const mediaDates = require('../milkmaid/media-dates.js')
const { loadModelRegistry } = require('../scrapyard/modelRegistry.js')

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const argv = minimist(process.argv.slice(2))
const MODEL_ARG = argv.model || argv.m
const SRC_ARG = argv.src || argv.s
const DRY_RUN = !!argv['dry-run']
const SKIP_VISUAL = !!argv['no-dedup']

const APPDATA =
  process.env.APPDATA ||
  path.join(process.env.HOME || process.env.USERPROFILE, 'AppData', 'Roaming')

const datasetDir = path.join(APPDATA, '.slopvault', 'dataset')
const registryPath = path.join(__dirname, '..', 'model_aliases.json')

// KemonoDownloader's default save location
const KEMONO_DL_DEFAULT = path.join(APPDATA, 'Kemono Downloader')

// ─── EXTENSION → BUCKET ───────────────────────────────────────────────────────
const BUCKET = {
  '.jpg': 'images',
  '.jpeg': 'images',
  '.png': 'images',
  '.webp': 'images',
  '.gif': 'gif',
  '.mp4': 'webm',
  '.m4v': 'webm',
  '.webm': 'webm',
  '.mov': 'webm',
}

// ─── WALK ─────────────────────────────────────────────────────────────────────
function walkFiles(dir) {
  const results = []
  if (!fs.existsSync(dir)) return results
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name)
    if (entry.isDirectory()) {
      results.push(...walkFiles(full))
    } else if (entry.isFile()) {
      const ext = path.extname(entry.name).toLowerCase()
      if (BUCKET[ext]) results.push(full)
    }
  }
  return results
}

// ─── HASH METADATA ────────────────────────────────────────────────────────────
function buildHashMetadata(modelName, absolutePath, mediaType, sizeBytes) {
  const relativePath = path
    .relative(datasetDir, absolutePath)
    .replace(/\\/g, '/')
  return {
    root: 'dataset',
    model: modelName,
    bucket: path.basename(path.dirname(absolutePath)),
    relativePath,
    filename: path.basename(absolutePath),
    mediaType,
    sizeBytes: Number.isFinite(sizeBytes) ? sizeBytes : null,
    source: 'kemonodownloader',
  }
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function run() {
  if (!MODEL_ARG) {
    console.error(
      'Usage: node hoghaul/ingest-kemonodownloader.js --model=<name> [--src=<dir>]'
    )
    process.exit(1)
  }

  // Resolve canonical model name from registry
  const registry = loadModelRegistry(registryPath)
  const canonicalName =
    Object.keys(registry).find(
      (k) => k.toLowerCase() === MODEL_ARG.toLowerCase()
    ) || MODEL_ARG

  if (!registry[canonicalName]) {
    console.warn(
      `⚠️  Model "${canonicalName}" not found in registry — will create folders anyway.`
    )
  }

  // Find source directory
  let srcDir = SRC_ARG
  if (!srcDir) {
    // Try to find it under KemonoDownloader's default output dir
    // It typically creates a subfolder matching the creator name
    const candidates = fs.existsSync(KEMONO_DL_DEFAULT)
      ? fs
          .readdirSync(KEMONO_DL_DEFAULT, { withFileTypes: true })
          .filter((e) => e.isDirectory())
          .map((e) => e.name)
      : []

    // Match by model name or alias
    const aliases = [canonicalName, ...(registry[canonicalName]?.aliases || [])]
    const match = candidates.find((c) =>
      aliases.some(
        (a) =>
          a.toLowerCase() === c.toLowerCase() ||
          a.toLowerCase().replace(/[^a-z0-9]/g, '') ===
            c.toLowerCase().replace(/[^a-z0-9]/g, '')
      )
    )

    if (match) {
      srcDir = path.join(KEMONO_DL_DEFAULT, match)
      console.log(`  Auto-detected source: ${srcDir}`)
    } else {
      console.error(
        `Could not auto-detect source dir for "${canonicalName}" under:\n  ${KEMONO_DL_DEFAULT}\n\nPass --src=<path> explicitly.`
      )
      if (candidates.length) {
        console.log(`\nAvailable folders in KemonoDownloader output:`)
        candidates.forEach((c) => console.log(`  ${c}`))
      }
      process.exit(1)
    }
  }

  // Set up destination folders
  const modelDir = path.join(datasetDir, canonicalName)
  const bucketDirs = {
    images: path.join(modelDir, 'images'),
    webm: path.join(modelDir, 'webm'),
    gif: path.join(modelDir, 'gif'),
  }
  if (!DRY_RUN) {
    for (const d of Object.values(bucketDirs))
      fs.mkdirSync(d, { recursive: true })
  }

  // Load existing filenames for quick dupe check
  const knownFilenames = new Set()
  for (const [bucket, dir] of Object.entries(bucketDirs)) {
    if (fs.existsSync(dir)) {
      fs.readdirSync(dir).forEach((f) => knownFilenames.add(f))
    }
  }

  loadBitwiseHashCache()
  loadVisualHashCache()

  console.log(`\n  Ingest: ${canonicalName}`)
  console.log(`  Source: ${srcDir}`)
  console.log(`  Dest:   ${modelDir}`)
  if (DRY_RUN) console.log('  Mode:   --dry-run\n')

  const files = walkFiles(srcDir)
  console.log(`  Files found: ${files.length}\n`)

  let saved = 0
  let dupes = 0
  let skipped = 0

  for (const filePath of files) {
    const filename = path.basename(filePath)
    const ext = path.extname(filename).toLowerCase()
    const bucket = BUCKET[ext]
    if (!bucket) continue

    const destPath = path.join(bucketDirs[bucket], filename)

    // Quick filename dupe check
    if (knownFilenames.has(filename) || fs.existsSync(destPath)) {
      console.log(`  ♻️  Dupe (filename): ${filename}`)
      dupes++
      continue
    }

    const buffer = fs.readFileSync(filePath)
    const hash = createHash('md5').update(buffer).digest('hex')

    // Bitwise hash dupe check
    if (isBitwiseDupe(hash)) {
      console.log(`  ♻️  Dupe (bitwise): ${filename}`)
      dupes++
      continue
    }

    // Visual hash dupe check (images only, unless --no-dedup)
    if (!SKIP_VISUAL && ['images', 'gif'].includes(bucket)) {
      const visualHash = await getVisualHashFromBuffer(buffer)
      if (visualHash && isVisualDupe(visualHash)) {
        console.log(`  ♻️  Dupe (visual): ${filename}`)
        dupes++
        continue
      }
      if (visualHash && !DRY_RUN) {
        const stat = { size: buffer.length }
        addVisualHash(
          visualHash,
          buildHashMetadata(
            canonicalName,
            destPath,
            bucket === 'gif' ? 'image' : 'image',
            stat.size
          )
        )
      }
    }

    if (DRY_RUN) {
      console.log(`  ✅ [dry] ${bucket}/${filename}`)
      saved++
      continue
    }

    // Copy (not move — leave originals intact in KemonoDownloader output)
    fs.copyFileSync(filePath, destPath)

    // Date — try filename first, then fall back to file mtime
    const filenameDate = mediaDates.extractFilenameDate(filename)
    const stat = fs.statSync(destPath)
    const uploadedDate = filenameDate
      ? new Date(filenameDate)
      : new Date(stat.mtime)

    // Record date sidecar
    if (bucket === 'webm') {
      await mediaDates.recordVideoDates(
        modelDir,
        'webm',
        filename,
        destPath,
        uploadedDate
      )
    } else {
      await mediaDates.recordImageDates(
        modelDir,
        bucket,
        filename,
        uploadedDate
      )
    }

    // Record bitwise hash
    addBitwiseHash(
      hash,
      buildHashMetadata(
        canonicalName,
        destPath,
        bucket === 'webm' ? 'video' : 'image',
        buffer.length
      )
    )

    knownFilenames.add(filename)
    saved++
    console.log(`  ✅ ${bucket}/${filename}`)
  }

  if (!DRY_RUN) {
    saveBitwiseHashCache()
    saveVisualHashCache()
    mediaDates.flushAllSidecars()
  }

  console.log(`\n  ─────────────────────────────────`)
  console.log(`  Saved:    ${saved}`)
  console.log(`  Dupes:    ${dupes}`)
  console.log(`  Skipped:  ${skipped}`)
  console.log(`  Total:    ${files.length}`)
  if (DRY_RUN) console.log('  (dry-run — nothing written)')
}

run().catch((err) => {
  console.error(`\n❌ Fatal: ${err.message}`)
  process.exit(1)
})

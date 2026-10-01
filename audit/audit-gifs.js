#!/usr/bin/env node
'use strict'

// Finds .gif files that don't play properly in the dashboard:
//   - truncated: the file ends before the GIF trailer byte (0x3B), i.e. the
//     download was cut off. Browsers play the frames that made it and then
//     stop/loop early — the animation looks "cut off".
//   - single frame: a still saved under a .gif name.
//   - not a gif: JPEG/PNG/WebP bytes with a .gif extension.
//
//   node audit/audit-gifs.js [--dataset DIR] [--model a,b] [--json]
//   node audit/audit-gifs.js --model a --redownload            (dry run)
//   node audit/audit-gifs.js --model a --redownload --apply    (replace)
//
// --redownload re-fetches truncated files from the sidecar's source.mediaUrl,
// replaces the file only when the new copy is a complete GIF, and deletes
// the dashboard's cached grid thumb + mobile MP4 for it (under --thumb-dir,
// default $THUMB_DIR) so the dashboard regenerates them from the fixed file.

const fs = require('fs')
const path = require('path')
const minimist = require('minimist')
const sharp = require('sharp')
const { createHttpClient } = require('../scrapyard/httpClient')
const config = require('../scrapyard/config')

const argv = minimist(process.argv.slice(2), {
  boolean: ['json', 'redownload', 'apply'],
  string: ['dataset', 'model', 'thumb-dir'],
})

const datasetDir = argv.dataset || process.env.DATASET_DIR || config.datasetDir
const thumbDir = argv['thumb-dir'] || process.env.THUMB_DIR || null
const MODEL_FILTER = new Set(
  String(argv.model || '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
)
const GIF_TRAILER = 0x3b

function readSidecar(modelDir) {
  try {
    return JSON.parse(
      fs.readFileSync(path.join(modelDir, '.media-dates.json'), 'utf8')
    )
  } catch {
    return {}
  }
}

function* walkGifs(dir, rel = '') {
  let entries = []
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true })
  } catch {
    return
  }
  for (const entry of entries) {
    if (entry.name.startsWith('.')) continue
    const relPath = rel ? `${rel}/${entry.name}` : entry.name
    if (entry.isDirectory())
      yield* walkGifs(path.join(dir, entry.name), relPath)
    else if (/\.gif$/i.test(entry.name)) yield relPath
  }
}

// Last non-padding byte of a complete GIF is the 0x3B trailer. Some encoders
// pad with trailing NULs, so skip those.
function hasGifTrailer(buffer) {
  let i = buffer.length - 1
  while (i >= 0 && buffer[i] === 0x00) i -= 1
  return i >= 0 && buffer[i] === GIF_TRAILER
}

async function inspectGif(buffer) {
  const isGifHeader = buffer.subarray(0, 4).toString('latin1') === 'GIF8'
  let pages = null
  let format = null
  let error = null
  try {
    const meta = await sharp(buffer, { animated: true }).metadata()
    pages = meta.pages || 1
    format = meta.format
  } catch (err) {
    error = err.message
  }
  if (!isGifHeader) {
    return { problem: format ? `not a gif (actually ${format})` : 'not a gif' }
  }
  if (!hasGifTrailer(buffer)) {
    return { problem: 'truncated download', pages }
  }
  if (error) return { problem: `unreadable: ${error}` }
  if (pages === 1) return { problem: 'single frame', pages }
  return { problem: null, pages }
}

function cachedDerivativePaths(model, relPath) {
  if (!thumbDir) return []
  const folder = path.dirname(relPath)
  const stem = path.basename(relPath, path.extname(relPath))
  return [
    path.join(thumbDir, model, `thumb-${folder}-${stem}.jpg`),
    path.join(thumbDir, model, `mobile-${folder}-${stem}.mp4`),
  ]
}

async function redownload(http, row, filePath, oldSize) {
  if (!row.mediaUrl) return 'skipped: no source.mediaUrl in sidecar'
  let buffer
  try {
    buffer = (await http.requestBuffer(row.mediaUrl)).buffer
  } catch (err) {
    return `failed: ${err.message}`
  }
  const check = await inspectGif(buffer)
  if (check.problem) return `skipped: fresh copy is also ${check.problem}`
  if (buffer.length <= oldSize) {
    return `skipped: fresh copy not larger (${buffer.length} vs ${oldSize} bytes)`
  }
  if (!argv.apply) {
    return `would replace (${oldSize} → ${buffer.length} bytes, ${check.pages} frames)`
  }
  const tmp = `${filePath}.tmp-audit-gifs`
  fs.writeFileSync(tmp, buffer)
  fs.renameSync(tmp, filePath)
  for (const derived of cachedDerivativePaths(row.model, row.file)) {
    try {
      fs.unlinkSync(derived)
    } catch {}
  }
  return `replaced (${oldSize} → ${buffer.length} bytes, ${check.pages} frames)`
}

async function run() {
  if (!fs.existsSync(datasetDir)) {
    throw new Error(`Dataset directory not found: ${datasetDir}`)
  }
  if (argv.redownload && argv.apply && !thumbDir) {
    console.warn(
      'Warning: no --thumb-dir / $THUMB_DIR — replaced files keep their stale cached thumb + mobile MP4.'
    )
  }
  const http = argv.redownload ? createHttpClient({ timeoutMs: 60000 }) : null
  const report = []
  let totalGifs = 0

  for (const dirent of fs.readdirSync(datasetDir, { withFileTypes: true })) {
    if (!dirent.isDirectory() || dirent.name.startsWith('.')) continue
    if (MODEL_FILTER.size && !MODEL_FILTER.has(dirent.name.toLowerCase())) {
      continue
    }
    const modelDir = path.join(datasetDir, dirent.name)
    const sidecar = readSidecar(modelDir)
    for (const relPath of walkGifs(modelDir)) {
      totalGifs += 1
      const filePath = path.join(modelDir, relPath)
      let buffer
      try {
        buffer = fs.readFileSync(filePath)
      } catch (err) {
        report.push({ model: dirent.name, file: relPath, problem: err.message })
        continue
      }
      const { problem, pages } = await inspectGif(buffer)
      if (!problem) continue
      const source = sidecar[relPath]?.source || {}
      const row = {
        model: dirent.name,
        file: relPath,
        site: source.site || 'unknown',
        problem,
        framesDecoded: pages,
        bytes: buffer.length,
        mediaUrl: source.mediaUrl || null,
      }
      if (argv.redownload && problem === 'truncated download') {
        row.redownload = await redownload(http, row, filePath, buffer.length)
      }
      report.push(row)
    }
  }

  if (argv.json) {
    console.log(JSON.stringify(report, null, 2))
    return
  }

  console.log(
    `Scanned ${totalGifs} .gif file(s) in ${datasetDir}; ${report.length} with problems.`
  )
  const byKey = new Map()
  for (const row of report) {
    const key = `${row.problem.replace(/:.*/, '')} [${row.site}]`
    byKey.set(key, (byKey.get(key) || 0) + 1)
  }
  for (const [key, count] of byKey) console.log(`  ${key}: ${count}`)
  for (const row of report.slice(0, 50)) {
    console.log(
      `  ${row.model}/${row.file} [${row.site}] ${row.problem}` +
        (row.framesDecoded ? ` (${row.framesDecoded} frames readable)` : '') +
        (row.redownload ? ` → ${row.redownload}` : '')
    )
  }
  if (report.length > 50)
    console.log(`  … ${report.length - 50} more (use --json)`)
  if (argv.redownload && !argv.apply) {
    console.log('Dry run. Re-run with --apply to replace truncated files.')
  }
}

run().catch((err) => {
  console.error(`Fatal: ${err.message}`)
  process.exit(1)
})

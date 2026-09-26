#!/usr/bin/env node
'use strict'

// Lists .gif files that aren't actually animated (a single frame), grouped
// by model and origin site. These show up in the dashboard as "GIFs" but
// never move — typically a scraper saved a still preview (e.g. Reddit's
// preview.redd.it `format=png` render) under a .gif name.
//
//   node audit/audit-static-gifs.js [--dataset DIR] [--model name[,name]] [--json]

const fs = require('fs')
const path = require('path')
const minimist = require('minimist')
const sharp = require('sharp')

const argv = minimist(process.argv.slice(2), {
  boolean: ['json'],
  string: ['dataset', 'model'],
})

const datasetDir =
  argv.dataset ||
  process.env.DATASET_DIR ||
  path.join(process.env.APPDATA || process.cwd(), '.slopvault', 'dataset')
const MODEL_FILTER = new Set(
  String(argv.model || '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
)

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

async function frameCount(filePath) {
  try {
    const meta = await sharp(filePath, { animated: true }).metadata()
    return { pages: meta.pages || 1, format: meta.format }
  } catch (err) {
    return { pages: null, format: null, error: err.message }
  }
}

async function run() {
  if (!fs.existsSync(datasetDir)) {
    throw new Error(`Dataset directory not found: ${datasetDir}`)
  }
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
      const { pages, format, error } = await frameCount(
        path.join(modelDir, relPath)
      )
      // format !== 'gif' means the bytes are really a JPEG/PNG/WebP with a
      // .gif name — also a still.
      if (pages === 1 || (format && format !== 'gif') || error) {
        report.push({
          model: dirent.name,
          file: relPath,
          site: sidecar[relPath]?.source?.site || 'unknown',
          reason: error
            ? `unreadable: ${error}`
            : format !== 'gif'
              ? `actually ${format}`
              : 'single frame',
          mediaUrl: sidecar[relPath]?.source?.mediaUrl || null,
        })
      }
    }
  }

  if (argv.json) {
    console.log(JSON.stringify(report, null, 2))
    return
  }

  console.log(
    `Scanned ${totalGifs} .gif file(s) in ${datasetDir}; ${report.length} not animated.`
  )
  const bySite = new Map()
  for (const row of report)
    bySite.set(row.site, (bySite.get(row.site) || 0) + 1)
  for (const [site, count] of bySite) console.log(`  ${site}: ${count}`)
  for (const row of report.slice(0, 50)) {
    console.log(
      `  ${row.model}/${row.file} [${row.site}] ${row.reason}${row.mediaUrl ? ` ← ${row.mediaUrl}` : ''}`
    )
  }
  if (report.length > 50)
    console.log(`  … ${report.length - 50} more (use --json)`)
}

run().catch((err) => {
  console.error(`Fatal: ${err.message}`)
  process.exit(1)
})

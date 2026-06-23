'use strict'

const fs = require('fs')
const path = require('path')
const minimist = require('minimist')

const {
  SIDECAR_FILENAME,
  getRedditTitleFromPermalink,
} = require('./mediaDates')

const argv = minimist(process.argv.slice(2), {
  boolean: ['apply'],
  string: ['dataset', 'model'],
})

const datasetDir =
  argv.dataset ||
  process.env.DATASET_DIR ||
  path.join(process.env.APPDATA || process.cwd(), '.slopvault', 'dataset')
const APPLY = Boolean(argv.apply)
const MODEL_FILTER = new Set(
  String(argv.model || argv.models || '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
)

function shouldProcessModel(modelName) {
  return MODEL_FILTER.size === 0 || MODEL_FILTER.has(modelName.toLowerCase())
}

function loadSidecar(modelDir) {
  const sidecarPath = path.join(modelDir, SIDECAR_FILENAME)
  if (!fs.existsSync(sidecarPath)) return null
  return {
    path: sidecarPath,
    data: JSON.parse(fs.readFileSync(sidecarPath, 'utf8')),
  }
}

function getTitleCandidates(source) {
  return [
    source?.mediaPageUrl,
    source?.sourceMediaPageUrl,
    ...(Array.isArray(source?.mediaPageUrls) ? source.mediaPageUrls : []),
  ]
}

function repairSidecar(modelName, sidecar) {
  let scanned = 0
  let repaired = 0
  const examples = []

  for (const [relativePath, record] of Object.entries(sidecar.data)) {
    if (relativePath.startsWith('__')) continue
    const source = record?.source
    if (!source || typeof source !== 'object') continue
    if (String(source.site || '').toLowerCase() !== 'reddit') continue
    scanned += 1
    if (source.title) continue

    const title = getTitleCandidates(source)
      .map(getRedditTitleFromPermalink)
      .find(Boolean)
    if (!title) continue

    source.title = title
    repaired += 1
    if (examples.length < 5) {
      examples.push({ modelName, relativePath, title })
    }
  }

  return { scanned, repaired, examples }
}

function run() {
  if (!fs.existsSync(datasetDir)) {
    throw new Error(`Dataset directory not found: ${datasetDir}`)
  }

  const totals = {
    models: 0,
    scanned: 0,
    repaired: 0,
    examples: [],
  }

  for (const dirent of fs.readdirSync(datasetDir, { withFileTypes: true })) {
    if (!dirent.isDirectory() || !shouldProcessModel(dirent.name)) continue
    const modelDir = path.join(datasetDir, dirent.name)
    const sidecar = loadSidecar(modelDir)
    if (!sidecar) continue

    const result = repairSidecar(dirent.name, sidecar)
    if (result.scanned === 0) continue
    totals.models += 1
    totals.scanned += result.scanned
    totals.repaired += result.repaired
    totals.examples.push(...result.examples)

    if (APPLY && result.repaired > 0) {
      fs.writeFileSync(sidecar.path, JSON.stringify(sidecar.data, null, 2))
      fs.appendFileSync(sidecar.path, '\n')
    }
  }

  console.log(
    `${APPLY ? 'Repaired' : 'Would repair'} ${totals.repaired} missing Reddit title(s) across ${totals.models} model(s); scanned ${totals.scanned} Reddit metadata record(s).`
  )
  if (!APPLY) console.log('Dry run only. Re-run with --apply to write changes.')
  for (const example of totals.examples.slice(0, 10)) {
    console.log(
      `  ${example.modelName}/${example.relativePath}: ${example.title}`
    )
  }
}

run()

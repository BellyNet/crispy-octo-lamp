#!/usr/bin/env node
'use strict'

const fs = require('fs')
const path = require('path')
const minimist = require('minimist')

const { SIDECAR_FILENAME } = require('./mediaDates')
const { createHttpClient } = require('./httpClient')
const { parseCoomerFansTitle } = require('./sourceAdapters/coomerFans')

const argv = minimist(process.argv.slice(2), {
  boolean: ['apply'],
  string: [
    'dataset',
    'delay-ms',
    'fetch-timeout-ms',
    'limit',
    'model',
    'concurrency',
  ],
})

const datasetDir =
  argv.dataset ||
  process.env.DATASET_DIR ||
  path.join(process.env.APPDATA || process.cwd(), '.slopvault', 'dataset')
const APPLY = Boolean(argv.apply)
const FETCH_DELAY_MS = Math.max(
  Number.parseInt(String(argv['delay-ms'] || ''), 10) || 250,
  0
)
const FETCH_TIMEOUT_MS = Math.max(
  Number.parseInt(String(argv['fetch-timeout-ms'] || ''), 10) || 15000,
  1000
)
const LIMIT = Number.parseInt(String(argv.limit || ''), 10) || Infinity
const CONCURRENCY = Math.max(
  Number.parseInt(String(argv.concurrency || ''), 10) || 2,
  1
)
const MODEL_FILTER = new Set(
  String(argv.model || argv.models || '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
)

const http = createHttpClient({
  timeoutMs: FETCH_TIMEOUT_MS,
  userAgent:
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
})

const pageCache = new Map()
let lastFetchAt = 0
let fetchSlot = Promise.resolve()

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function cleanText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim()
}

function isBlank(value) {
  return cleanText(value) === ''
}

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

function writeSidecarAtomic(sidecarPath, data) {
  const tmp = `${sidecarPath}.tmp-coomerfans-title-repair`
  fs.writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`)
  fs.renameSync(tmp, sidecarPath)
}

function normalizePostPageUrl(value) {
  try {
    const url = new URL(value)
    if (!url.hostname.includes('coomerfans.com')) return null
    url.hash = ''
    url.search = ''
    return url.toString()
  } catch {
    return null
  }
}

function getPostPageUrl(source) {
  const candidates = [
    source?.mediaPageUrl,
    source?.sourceMediaPageUrl,
    ...(Array.isArray(source?.mediaPageUrls) ? source.mediaPageUrls : []),
  ]
  return candidates.map(normalizePostPageUrl).find(Boolean) || null
}

function getCreatorName(source, modelName) {
  return cleanText(source?.username || source?.rawName || modelName)
}

function collectTargets() {
  const pages = new Map()
  const sidecars = new Map()
  const totals = {
    models: 0,
    scanned: 0,
    missingRecords: 0,
    missingWithPage: 0,
    noPage: 0,
  }

  for (const dirent of fs.readdirSync(datasetDir, { withFileTypes: true })) {
    if (!dirent.isDirectory() || !shouldProcessModel(dirent.name)) continue
    const modelDir = path.join(datasetDir, dirent.name)
    const sidecar = loadSidecar(modelDir)
    if (!sidecar) continue
    let modelHasCoomerFans = false

    for (const [relativePath, record] of Object.entries(sidecar.data)) {
      if (relativePath.startsWith('__')) continue
      const source = record?.source
      if (!source || String(source.site || '').toLowerCase() !== 'coomerfans') {
        continue
      }

      modelHasCoomerFans = true
      totals.scanned += 1
      if (!isBlank(source.title) && !isBlank(source.text)) continue

      totals.missingRecords += 1
      const pageUrl = getPostPageUrl(source)
      if (!pageUrl) {
        totals.noPage += 1
        continue
      }
      totals.missingWithPage += 1
      sidecars.set(sidecar.path, sidecar.data)
      if (!pages.has(pageUrl)) {
        pages.set(pageUrl, {
          url: pageUrl,
          modelName: dirent.name,
          creatorName: getCreatorName(source, dirent.name),
          refs: [],
        })
      }
      pages.get(pageUrl).refs.push({
        sidecarPath: sidecar.path,
        sidecarData: sidecar.data,
        relativePath,
        source,
        modelName: dirent.name,
      })
    }

    if (modelHasCoomerFans) totals.models += 1
  }

  const targets = Array.from(pages.values()).slice(0, LIMIT)
  return { targets, sidecars, totals }
}

async function waitForFetchSlot() {
  if (FETCH_DELAY_MS <= 0) return
  let release
  const nextSlot = new Promise((resolve) => {
    release = resolve
  })
  const previousSlot = fetchSlot
  fetchSlot = nextSlot
  await previousSlot
  const waitMs = Math.max(lastFetchAt + FETCH_DELAY_MS - Date.now(), 0)
  if (waitMs > 0) await sleep(waitMs)
  lastFetchAt = Date.now()
  release()
}

function isRetryableError(err) {
  const message = String(err?.message || '')
  return (
    /HTTP (?:429|500|502|503|504)\b/.test(message) ||
    /timed out|ECONNRESET|ETIMEDOUT|ENOTFOUND/i.test(message)
  )
}

async function fetchTitle(page) {
  if (pageCache.has(page.url)) return pageCache.get(page.url)

  let result = { title: null, error: null }
  for (let attempt = 0; attempt < 3; attempt += 1) {
    await waitForFetchSlot()
    try {
      const response = await http.fetchHtml(page.url, {
        timeoutMs: FETCH_TIMEOUT_MS,
        headers: {
          Referer: 'https://coomerfans.com/',
        },
      })
      result = {
        title: cleanText(parseCoomerFansTitle(response.html, page.creatorName)),
        error: null,
      }
      break
    } catch (err) {
      result = { title: null, error: err }
      if (attempt === 2 || !isRetryableError(err)) break
      await sleep(3000 * (attempt + 1))
    }
  }

  pageCache.set(page.url, result)
  return result
}

function applyTitleToRefs(page, title) {
  let repaired = 0
  const dirty = new Map()
  const examples = []

  for (const ref of page.refs) {
    let changed = false
    if (isBlank(ref.source.title)) {
      ref.source.title = title
      changed = true
    }
    if (isBlank(ref.source.text)) {
      ref.source.text = title
      changed = true
    }
    if (!changed) continue

    repaired += 1
    dirty.set(ref.sidecarPath, ref.sidecarData)
    if (examples.length < 3) {
      examples.push({
        modelName: ref.modelName,
        relativePath: ref.relativePath,
        title,
      })
    }
  }

  return { repaired, dirty, examples }
}

async function run() {
  if (!fs.existsSync(datasetDir)) {
    throw new Error(`Dataset directory not found: ${datasetDir}`)
  }

  const { targets, totals } = collectTargets()
  console.log('repairCoomerFansMetadataTitles - walking', datasetDir)
  console.log('mode:', APPLY ? 'APPLY (writing sidecars)' : 'dry-run')
  if (MODEL_FILTER.size) {
    console.log('model filter:', Array.from(MODEL_FILTER).join(', '))
  }
  if (LIMIT !== Infinity) console.log('limit:', LIMIT, 'unique page(s)')
  console.log('concurrency:', CONCURRENCY, 'delay-ms:', FETCH_DELAY_MS)
  console.log(
    `CoomerFans records: scanned=${totals.scanned} missing=${totals.missingRecords} withPage=${totals.missingWithPage} noPage=${totals.noPage} models=${totals.models}`
  )
  console.log(`Unique CoomerFans post page(s) to fetch: ${targets.length}`)

  if (!targets.length) {
    if (!APPLY) console.log('Dry run only. Nothing to fetch.')
    return
  }

  const queue = targets.slice()
  const dirty = new Map()
  const examples = []
  const failedExamples = []
  let done = 0
  let pagesWithTitle = 0
  let pagesWithoutTitle = 0
  let failed = 0
  let repaired = 0

  async function worker() {
    while (queue.length) {
      const page = queue.shift()
      if (!page) return
      const result = await fetchTitle(page)
      if (result.title) {
        pagesWithTitle += 1
        const applied = applyTitleToRefs(page, result.title)
        repaired += applied.repaired
        for (const [sidecarPath, data] of applied.dirty) {
          dirty.set(sidecarPath, data)
        }
        examples.push(...applied.examples)
      } else if (result.error) {
        failed += 1
        if (failedExamples.length < 5) {
          failedExamples.push({
            url: page.url,
            error: result.error.message,
          })
        }
      } else {
        pagesWithoutTitle += 1
      }

      done += 1
      if (done % 25 === 0 || done === targets.length) {
        process.stdout.write(
          `\r  ${done}/${targets.length} pages - title=${pagesWithTitle} blank=${pagesWithoutTitle} fail=${failed} repaired=${repaired}   `
        )
      }
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()))
  console.log()

  if (APPLY) {
    console.log(`Writing ${dirty.size} updated sidecar(s)...`)
    for (const [sidecarPath, data] of dirty) {
      writeSidecarAtomic(sidecarPath, data)
    }
  }

  console.log(
    `${APPLY ? 'Repaired' : 'Would repair'} ${repaired} CoomerFans title/text record(s) from ${pagesWithTitle} page(s).`
  )
  console.log(
    `Pages without title=${pagesWithoutTitle}; failed=${failed}; sidecars=${dirty.size}.`
  )
  if (!APPLY) console.log('Dry run only. Re-run with --apply to write changes.')

  for (const example of examples.slice(0, 10)) {
    console.log(
      `  ${example.modelName}/${example.relativePath}: ${example.title}`
    )
  }
  for (const example of failedExamples) {
    console.log(`  fail ${example.url}: ${example.error}`)
  }
}

run().catch((err) => {
  console.error(`Fatal: ${err.message}`)
  process.exit(1)
})

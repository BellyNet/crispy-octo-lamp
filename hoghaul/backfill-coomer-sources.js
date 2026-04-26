'use strict'

/**
 * backfill-coomer-sources.js
 *
 * Searches Coomer.st for every model in the shared registry that doesn't
 * already have a coomer source, and adds any matches it finds.
 *
 * Coomer's public API returns all creators as a JSON array. We fetch it once
 * and match locally against every alias in the registry.
 *
 * Usage:
 *   node hoghaul/backfill-coomer-sources.js [--dry-run] [--force]
 *
 * Options:
 *   --dry-run   Print matches but don't write to the registry
 *   --force     Re-check models that already have a coomer source
 */

const https = require('https')
const path = require('path')
const minimist = require('minimist')

const {
  sanitize,
  loadModelRegistry,
  resolveAndTrackModel,
} = require('../scrapyard/modelRegistry.js')

const argv = minimist(process.argv.slice(2))
const DRY_RUN = !!argv['dry-run']
const FORCE   = !!argv.force

const registryPath = path.join(__dirname, '..', 'model_aliases.json')
const COOMER_HOST  = 'coomer.st'

// ─── FETCH ────────────────────────────────────────────────────────────────────
function httpsGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'hoghaul-backfill/1.0' } }, (res) => {
      let body = ''
      res.on('data', (c) => (body += c))
      res.on('end', () => resolve({ status: res.statusCode, body }))
    }).on('error', reject)
  })
}

async function fetchAllCreators() {
  console.log(`  Fetching creator list from ${COOMER_HOST}...`)
  // Coomer paginates at 50 per page; keep fetching until we get an empty page
  const all = []
  let offset = 0
  while (true) {
    const url = `https://${COOMER_HOST}/api/v1/creators.txt?o=${offset}`
    const { status, body } = await httpsGet(url)
    if (status !== 200) throw new Error(`Coomer API returned HTTP ${status}`)
    const page = JSON.parse(body)
    if (!Array.isArray(page) || page.length === 0) break
    all.push(...page)
    offset += page.length
    process.stdout.write(`  Fetched ${all.length} creators so far...\r`)
  }
  console.log(`  Fetched ${all.length} total creators.          `)
  return all
}

// ─── MATCH ────────────────────────────────────────────────────────────────────

/**
 * Build a lookup map: sanitized_name → [{ service, id, name, url }]
 * One creator can appear on multiple services (OF + Fansly), so we keep all.
 */
function buildCreatorIndex(creators) {
  const index = new Map()
  for (const c of creators) {
    // API returns: { id, name, service, ... }
    const key = sanitize(c.name || c.id || '')
    if (!key) continue
    const url = `https://${COOMER_HOST}/${c.service}/user/${c.id}`
    const entry = { service: c.service, id: c.id, name: c.name, url }
    if (!index.has(key)) index.set(key, [])
    index.get(key).push(entry)
  }
  return index
}

function findMatches(canonicalName, aliases, creatorIndex) {
  const candidates = new Set([sanitize(canonicalName), ...aliases.map(sanitize)])
  const matches = []
  for (const key of candidates) {
    if (creatorIndex.has(key)) {
      matches.push(...creatorIndex.get(key))
    }
  }
  // Deduplicate by URL
  const seen = new Set()
  return matches.filter((m) => {
    if (seen.has(m.url)) return false
    seen.add(m.url)
    return true
  })
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function run() {
  console.log('\nBackfill Coomer sources into model registry')
  console.log(`Registry: ${registryPath}`)
  if (DRY_RUN) console.log('Mode: --dry-run (no writes)')
  if (FORCE)   console.log('Mode: --force (re-checking models with existing coomer sources)')
  console.log('')

  const registry = loadModelRegistry(registryPath)
  const modelNames = Object.keys(registry)
  console.log(`Models in registry: ${modelNames.length}`)

  // Skip models that already have coomer sources unless --force
  const toCheck = modelNames.filter((name) => {
    if (FORCE) return true
    const sources = registry[name]?.sources?.coomer
    return !Array.isArray(sources) || sources.length === 0
  })
  console.log(`Models to check: ${toCheck.length}\n`)

  if (toCheck.length === 0) {
    console.log('Nothing to do. Use --force to re-check all models.')
    return
  }

  const creators = await fetchAllCreators()
  const creatorIndex = buildCreatorIndex(creators)
  console.log('')

  let matched = 0
  let skipped = 0

  for (const canonicalName of toCheck) {
    const entry = registry[canonicalName]
    const aliases = Array.isArray(entry?.aliases) ? entry.aliases : []
    const matches = findMatches(canonicalName, aliases, creatorIndex)

    if (matches.length === 0) {
      skipped++
      continue
    }

    matched++
    for (const m of matches) {
      console.log(`  ✅ ${canonicalName}  →  ${m.url}  (${m.service}, found as "${m.name}")`)
      if (!DRY_RUN) {
        resolveAndTrackModel(registryPath, canonicalName, 'coomer', m.url)
      }
    }
  }

  console.log('\n─────────────────────────────────────────')
  console.log(`Matched: ${matched}  |  No match: ${skipped}  |  Checked: ${toCheck.length}`)
  if (DRY_RUN) console.log('(dry-run — registry not modified)')
  console.log('')
}

run().catch((err) => {
  console.error(err)
  process.exit(1)
})

'use strict'

/**
 * backfill-coomer-sources.js
 *
 * For every model in the shared registry that doesn't already have a coomer
 * source, tries a direct lookup on each Coomer-supported service using every
 * known alias. Records any hits under sources.coomer in the registry.
 *
 * Usage:
 *   node hoghaul/backfill-coomer-sources.js [--dry-run] [--force] [--delay=ms]
 *
 * Options:
 *   --dry-run     Print matches but don't write to the registry
 *   --force       Re-check models that already have a coomer source
 *   --delay=300   Milliseconds between requests (default: 300)
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
const FORCE = !!argv.force
const DELAY = parseInt(argv.delay ?? 300, 10)

const registryPath = path.join(__dirname, '..', 'model_aliases.json')
const COOMER_HOST = 'coomerfans.com'

// All services Coomer aggregates from
const SERVICES = [
  'onlyfans',
  'fansly',
  'patreon',
  'candfans',
  'subscribestar',
  'gumroad',
  'afdian',
  'boosty',
]

// ─── HTTP ─────────────────────────────────────────────────────────────────────
function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          Accept: 'text/css', // required by Coomer to bypass DDG caching (see their 403 body)
          Referer: `https://${COOMER_HOST}/`,
        },
      },
      (res) => {
        if (
          res.statusCode >= 300 &&
          res.statusCode < 400 &&
          res.headers.location
        ) {
          return httpsGet(res.headers.location).then(resolve).catch(reject)
        }
        let body = ''
        res.on('data', (c) => (body += c))
        res.on('end', () => resolve({ status: res.statusCode, body }))
      }
    )
    req.on('error', reject)
    req.setTimeout(8000, () => {
      req.destroy()
      reject(new Error('timeout'))
    })
  })
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

/**
 * Check if a specific username exists on a specific Coomer service.
 * Returns the creator object on hit, null on miss, throws on error.
 */
async function lookupCreator(service, username) {
  // The /profile suffix is required — the bare /user/{id} endpoint returns 404
  const apiUrl = `https://${COOMER_HOST}/api/v1/${service}/user/${encodeURIComponent(username)}/profile`
  const { status, body } = await httpsGet(apiUrl)
  if (status === 200) {
    try {
      return JSON.parse(body)
    } catch {
      return { id: username, service }
    }
  }
  if (status === 404) return null
  throw new Error(`HTTP ${status}`)
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function run() {
  console.log('\nBackfill Coomer sources into model registry')
  console.log(`Registry:  ${registryPath}`)
  console.log(`Services:  ${SERVICES.join(', ')}`)
  if (DRY_RUN) console.log('Mode: --dry-run (no writes)')
  if (FORCE) console.log('Mode: --force (re-checking all models)')
  console.log(`Delay: ${DELAY}ms between requests\n`)

  const registry = loadModelRegistry(registryPath)
  const modelNames = Object.keys(registry)
  console.log(`Models in registry: ${modelNames.length}`)

  const toCheck = modelNames.filter((name) => {
    if (FORCE) return true
    const srcs = registry[name]?.sources?.coomer
    return !Array.isArray(srcs) || srcs.length === 0
  })
  console.log(`Models to check:    ${toCheck.length}\n`)

  if (toCheck.length === 0) {
    console.log('Nothing to do. Use --force to re-check all models.')
    return
  }

  let matched = 0
  let skipped = 0
  let errors = 0

  for (let i = 0; i < toCheck.length; i++) {
    const canonicalName = toCheck[i]
    const entry = registry[canonicalName]
    const aliases = Array.isArray(entry?.aliases)
      ? entry.aliases
      : [canonicalName]

    // Deduplicated sanitized names to try as usernames
    const usernames = [
      ...new Set([canonicalName, ...aliases].map(sanitize).filter(Boolean)),
    ]

    process.stdout.write(`  [${i + 1}/${toCheck.length}] ${canonicalName}...`)

    const hits = [] // { service, username, url, creator }

    for (const username of usernames) {
      for (const service of SERVICES) {
        try {
          const creator = await lookupCreator(service, username)
          if (creator) {
            const url = `https://${COOMER_HOST}/${service}/user/${username}`
            hits.push({
              service,
              username,
              url,
              name: creator.name || username,
            })
          }
          await sleep(DELAY)
        } catch (err) {
          // Non-fatal — skip this service/username combo
          errors++
          await sleep(DELAY * 2)
        }
      }
    }

    if (hits.length === 0) {
      process.stdout.write(' not found\n')
      skipped++
      continue
    }

    matched++
    process.stdout.write('\n')
    for (const hit of hits) {
      console.log(`    ✅ ${hit.url}  (${hit.service}, id="${hit.name}")`)
      if (!DRY_RUN) {
        resolveAndTrackModel(registryPath, canonicalName, 'coomer', hit.url)
      }
    }
  }

  console.log('\n─────────────────────────────────────────')
  console.log(`Matched:  ${matched}`)
  console.log(`No match: ${skipped}`)
  console.log(`Errors:   ${errors}`)
  console.log(`Checked:  ${toCheck.length}`)
  if (DRY_RUN) console.log('(dry-run — registry not modified)')
  console.log('')
}

run().catch((err) => {
  console.error(err)
  process.exit(1)
})

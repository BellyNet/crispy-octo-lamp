'use strict'

/**
 * batch-probe-sources.js
 *
 * Non-interactive batch probe: for every model in model_aliases.json that is
 * missing a coomer or kemono source, probes all known usernames (canonical +
 * aliases) against both APIs across all known services.
 *
 * All hits are auto-saved to the registry.  A full report is written to
 * batch-probe-report.txt so you can spot-check cases where the platform's
 * display name differs from the canonical model name — those are the only ones
 * that need manual attention.
 *
 * Usage:
 *   node hoghaul/batch-probe-sources.js [--delay=ms] [--force] [--dry-run]
 *
 * Options:
 *   --force     Re-probe models that already have sources
 *   --delay=300 Milliseconds between API requests (default: 300)
 *   --dry-run   Probe and report without saving to registry
 */

const fs = require('fs')
const https = require('https')
const path = require('path')
const minimist = require('minimist')

const {
  sanitize,
  loadModelRegistry,
  resolveAndTrackModel,
} = require('../scrapyard/modelRegistry.js')

const argv = minimist(process.argv.slice(2))
const FORCE = !!argv.force
const DRY_RUN = !!argv['dry-run']
const DELAY = parseInt(argv.delay ?? 300, 10)

const registryPath = path.join(__dirname, '..', 'model_aliases.json')
const reportPath = path.join(__dirname, '..', 'batch-probe-report.txt')

// ─── PLATFORM CONFIG ──────────────────────────────────────────────────────────
const PLATFORMS = {
  coomer: {
    host: 'coomerfans.com',
    label: 'Coomer',
    services: [
      'onlyfans',
      'fansly',
      'patreon',
      'candfans',
      'subscribestar',
      'gumroad',
      'afdian',
      'boosty',
    ],
    profileUrl: (service, username) =>
      `https://coomerfans.com/api/v1/${service}/user/${encodeURIComponent(username)}/profile`,
    userUrl: (service, username) =>
      `https://coomerfans.com/${service}/user/${username}`,
  },
  kemono: {
    host: 'kemono.cr',
    label: 'Kemono',
    services: [
      'patreon',
      'fanbox',
      'gumroad',
      'discord',
      'fantia',
      'afdian',
      'boosty',
      'dlsite',
      'subscribestar',
    ],
    profileUrl: (service, username) =>
      `https://kemono.cr/api/v1/${service}/user/${encodeURIComponent(username)}/profile`,
    userUrl: (service, username) =>
      `https://kemono.cr/${service}/user/${username}`,
  },
}

// ─── HTTP ─────────────────────────────────────────────────────────────────────
function httpsGet(host, url) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          Accept: 'text/css',
          Referer: `https://${host}/`,
        },
      },
      (res) => {
        if (
          res.statusCode >= 300 &&
          res.statusCode < 400 &&
          res.headers.location
        ) {
          return httpsGet(host, res.headers.location)
            .then(resolve)
            .catch(reject)
        }
        let body = ''
        res.on('data', (c) => (body += c))
        res.on('end', () => resolve({ status: res.statusCode, body }))
      }
    )
    req.on('error', reject)
    req.setTimeout(10000, () => {
      req.destroy()
      reject(new Error('timeout'))
    })
  })
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// ─── API LOOKUP ───────────────────────────────────────────────────────────────
async function lookupCreator(platform, service, username) {
  const cfg = PLATFORMS[platform]
  try {
    const { status, body } = await httpsGet(
      cfg.host,
      cfg.profileUrl(service, username)
    )
    if (status === 200) {
      try {
        return JSON.parse(body)
      } catch {
        return { id: username, service }
      }
    }
    if (status === 404) return null
    throw new Error(`HTTP ${status}`)
  } catch (err) {
    throw err
  }
}

async function probeUsername(platform, username) {
  const cfg = PLATFORMS[platform]
  const hits = []
  for (const service of cfg.services) {
    try {
      const creator = await lookupCreator(platform, service, username)
      if (creator) {
        hits.push({
          platform,
          service,
          username,
          url: cfg.userUrl(service, username),
          displayName: creator.name || username,
        })
      }
      await sleep(DELAY)
    } catch {
      await sleep(DELAY * 2)
    }
  }
  return hits
}

// ─── DISPLAY NAME MATCH ───────────────────────────────────────────────────────
// Returns true if the platform's display name closely matches the canonical
// name or any of its aliases (case-insensitive, alphanumeric only).
function isDisplayNameMatch(displayName, canonicalName, aliases) {
  const norm = (s) =>
    String(s)
      .toLowerCase()
      .replace(/[^a-z0-9]/g, '')
  const normDisplay = norm(displayName)
  return [canonicalName, ...aliases].some((a) => norm(a) === normDisplay)
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function run() {
  const registry = loadModelRegistry(registryPath)

  const toProcess = Object.entries(registry).filter(([, entry]) => {
    if (FORCE) return true
    const hasCoomer =
      Array.isArray(entry?.sources?.coomer) && entry.sources.coomer.length > 0
    const hasKemono =
      Array.isArray(entry?.sources?.kemono) && entry.sources.kemono.length > 0
    return !hasCoomer || !hasKemono
  })

  console.log('\n  ╔═══════════════════════════════════════════╗')
  console.log('  ║   Batch Source Probe (coomer + kemono)    ║')
  console.log('  ╚═══════════════════════════════════════════╝\n')
  console.log(`  Registry:  ${registryPath}`)
  console.log(`  Report:    ${reportPath}`)
  console.log(`  Delay:     ${DELAY}ms between API requests`)
  if (DRY_RUN) console.log('  Mode:      --dry-run (nothing will be saved)')
  if (FORCE) console.log('  Mode:      --force (re-probing all models)')
  console.log(`\n  Models to probe: ${toProcess.length}\n`)

  const reportLines = [
    `Batch Source Probe Report`,
    `Generated: ${new Date().toISOString()}`,
    `Models probed: ${toProcess.length}`,
    DRY_RUN ? `Mode: DRY RUN` : `Mode: LIVE (hits auto-saved)`,
    '',
    '═'.repeat(72),
    '',
  ]

  let totalHits = 0
  let autoSaved = 0
  let needsReview = 0
  let noHits = 0

  for (let i = 0; i < toProcess.length; i++) {
    const [canonicalName, entry] = toProcess[i]
    const aliases = Array.isArray(entry?.aliases) ? entry.aliases : []

    const hasCoomer =
      Array.isArray(entry?.sources?.coomer) && entry.sources.coomer.length > 0
    const hasKemono =
      Array.isArray(entry?.sources?.kemono) && entry.sources.kemono.length > 0

    const missingPlatforms = []
    if (!hasCoomer) missingPlatforms.push('coomer')
    if (!hasKemono) missingPlatforms.push('kemono')

    // Build deduplicated list of usernames to probe with
    const usernames = [
      ...new Set(
        [canonicalName, ...aliases].map((a) => sanitize(a)).filter(Boolean)
      ),
    ]

    process.stdout.write(
      `  [${String(i + 1).padStart(3)}/${toProcess.length}] ${canonicalName.padEnd(36)} `
    )

    const modelLines = [`Model: ${canonicalName}`]
    if (aliases.length) modelLines.push(`  Aliases: ${aliases.join(', ')}`)
    if (hasCoomer)
      modelLines.push(
        `  Coomer:  already set — ${entry.sources.coomer[0]?.url || entry.sources.coomer[0]}`
      )
    if (hasKemono)
      modelLines.push(
        `  Kemono:  already set — ${entry.sources.kemono[0]?.url || entry.sources.kemono[0]}`
      )

    const modelHits = []

    for (const platform of missingPlatforms) {
      for (const username of usernames) {
        const hits = await probeUsername(platform, username)
        modelHits.push(...hits)
      }
    }

    if (modelHits.length === 0) {
      process.stdout.write('no hits\n')
      modelLines.push('  No hits found.')
      noHits++
    } else {
      process.stdout.write(`${modelHits.length} hit(s)\n`)
      totalHits += modelHits.length

      // Deduplicate by URL (multiple alias probes may find the same account)
      const seen = new Set()
      const dedupedHits = modelHits.filter((h) => {
        if (seen.has(h.url)) return false
        seen.add(h.url)
        return true
      })

      // Group hits by platform — take the first hit per platform for auto-save
      const byPlatform = {}
      for (const hit of dedupedHits) {
        if (!byPlatform[hit.platform]) byPlatform[hit.platform] = []
        byPlatform[hit.platform].push(hit)
      }

      for (const [platform, hits] of Object.entries(byPlatform)) {
        // Pick first hit per platform to save (usually the canonical-name match)
        const chosen = hits[0]
        const nameMatch = isDisplayNameMatch(
          chosen.displayName,
          canonicalName,
          aliases
        )
        const tag = nameMatch ? '[AUTO-SAVED]' : '[REVIEW]    '

        if (nameMatch) {
          autoSaved++
          if (!DRY_RUN) {
            resolveAndTrackModel(
              registryPath,
              canonicalName,
              platform,
              chosen.url
            )
          }
        } else {
          needsReview++
        }

        const line = `  ${tag} ${platform}/${chosen.service.padEnd(14)} username="${chosen.username}"  display="${chosen.displayName}"  → ${chosen.url}`
        modelLines.push(line)
        process.stdout.write(`             ${line.trimStart()}\n`)

        // List any additional hits for this platform as alternates
        for (const alt of hits.slice(1)) {
          const altMatch = isDisplayNameMatch(
            alt.displayName,
            canonicalName,
            aliases
          )
          const altTag = altMatch ? '[ALT/AUTO]  ' : '[ALT/REVIEW]'
          const altLine = `  ${altTag} ${platform}/${alt.service.padEnd(14)} username="${alt.username}"  display="${alt.displayName}"  → ${alt.url}`
          modelLines.push(altLine)
        }
      }
    }

    modelLines.push('')
    reportLines.push(...modelLines)
  }

  // ── Write report ─────────────────────────────────────────────────────────────
  reportLines.push('═'.repeat(72))
  reportLines.push('')
  reportLines.push(`Summary`)
  reportLines.push(`  Models probed:      ${toProcess.length}`)
  reportLines.push(`  Total hits:         ${totalHits}`)
  reportLines.push(
    `  Auto-saved:         ${autoSaved}${DRY_RUN ? ' (dry-run, not written)' : ''}`
  )
  reportLines.push(`  Needs review:       ${needsReview}`)
  reportLines.push(`  No hits:            ${noHits}`)
  reportLines.push('')

  fs.writeFileSync(reportPath, reportLines.join('\n'), 'utf8')

  console.log('\n  ─────────────────────────────────────────────')
  console.log(`  Models probed:  ${toProcess.length}`)
  console.log(`  Total hits:     ${totalHits}`)
  console.log(`  Auto-saved:     ${autoSaved}${DRY_RUN ? ' (dry-run)' : ''}`)
  console.log(`  Needs review:   ${needsReview}`)
  console.log(`  No hits:        ${noHits}`)
  console.log(`\n  Report written to: ${reportPath}`)
  if (DRY_RUN) console.log('  (dry-run — registry unchanged)')
}

run().catch((err) => {
  console.error(`\n  ❌ Fatal: ${err.message}`)
  process.exit(1)
})

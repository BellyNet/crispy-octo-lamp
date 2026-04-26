'use strict'

/**
 * backfill-coomer-sources-interactive.js
 *
 * Interactive terminal tool for linking models that the auto-backfill missed.
 * For each model without a Coomer source, re-tries all aliases via the API,
 * then drops into a prompt where you can test username variations or paste
 * a full Coomer URL directly.
 *
 * Usage:
 *   node hoghaul/backfill-coomer-sources-interactive.js [--delay=ms] [--force]
 *
 * Options:
 *   --force     Re-review models that already have a Coomer source
 *   --delay=300 Milliseconds between API requests (default: 300)
 */

const fs       = require('fs')
const https    = require('https')
const path     = require('path')
const readline = require('readline')
const { execFile } = require('child_process')
const minimist = require('minimist')

const {
  sanitize,
  loadModelRegistry,
  resolveAndTrackModel,
} = require('../scrapyard/modelRegistry.js')

const argv    = minimist(process.argv.slice(2))
const FORCE   = !!argv.force
const DELAY   = parseInt(argv.delay ?? 300, 10)

const registryPath = path.join(__dirname, '..', 'model_aliases.json')
const COOMER_HOST  = 'coomer.st'
const SERVICES     = ['onlyfans', 'fansly', 'patreon', 'candfans', 'subscribestar', 'gumroad', 'afdian', 'boosty']

// ─── HTTP ─────────────────────────────────────────────────────────────────────
function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/css',
        'Referer': `https://${COOMER_HOST}/`,
      },
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return httpsGet(res.headers.location).then(resolve).catch(reject)
      }
      let body = ''
      res.on('data', (c) => (body += c))
      res.on('end', () => resolve({ status: res.statusCode, body }))
    })
    req.on('error', reject)
    req.setTimeout(8000, () => { req.destroy(); reject(new Error('timeout')) })
  })
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

async function lookupCreator(service, username) {
  const apiUrl = `https://${COOMER_HOST}/api/v1/${service}/user/${encodeURIComponent(username)}/profile`
  const { status, body } = await httpsGet(apiUrl)
  if (status === 200) {
    try { return JSON.parse(body) } catch { return { id: username, service } }
  }
  if (status === 404) return null
  throw new Error(`HTTP ${status}`)
}

/**
 * Try a username against all services. Returns array of hit objects.
 */
async function probeUsername(username) {
  const hits = []
  for (const service of SERVICES) {
    try {
      const creator = await lookupCreator(service, username)
      if (creator) {
        hits.push({
          service,
          username,
          url: `https://${COOMER_HOST}/${service}/user/${username}`,
          name: creator.name || username,
        })
      }
      await sleep(DELAY)
    } catch {
      await sleep(DELAY * 2)
    }
  }
  return hits
}

/**
 * Try all sanitized aliases for a model. Returns all hits found.
 */
async function autoProbeModel(canonicalName, entry) {
  const aliases = Array.isArray(entry?.aliases) ? entry.aliases : [canonicalName]
  const usernames = [...new Set([canonicalName, ...aliases].map(sanitize).filter(Boolean))]
  const allHits = []
  for (const username of usernames) {
    const hits = await probeUsername(username)
    allHits.push(...hits)
  }
  return allHits
}

/**
 * Parse a pasted Coomer URL into { service, username } or null.
 */
function parseCoomerUrl(input) {
  const str = String(input || '').trim()
  // https://coomer.st/{service}/user/{username}[/...]
  const m = str.match(/^https?:\/\/(?:www\.)?coomer\.(?:st|party)\/([^/]+)\/user\/([^/?#\s]+)/i)
  if (!m) return null
  const service  = m[1].toLowerCase()
  const username = m[2]
  if (!SERVICES.includes(service)) return null
  return { service, username, url: `https://${COOMER_HOST}/${service}/user/${username}` }
}

// ─── BROWSER ──────────────────────────────────────────────────────────────────
const YANDEX_CANDIDATES = [
  process.env.YANDEX_BROWSER_PATH,
  'C:\\Users\\jagsr\\AppData\\Local\\Yandex\\YandexBrowser\\Application\\browser.exe',
  'C:\\Program Files\\Yandex\\YandexBrowser\\Application\\browser.exe',
].filter(Boolean)

function getYandexPath() {
  for (const p of YANDEX_CANDIDATES) {
    if (fs.existsSync(p)) return p
  }
  return YANDEX_CANDIDATES[0]
}

function openInBrowser(url) {
  const browserPath = getYandexPath()
  execFile(browserPath, [url], (err) => {
    if (err) console.log(`  ⚠️  Could not open browser: ${err.message}\n  URL: ${url}`)
  })
}

// ─── PROMPT ───────────────────────────────────────────────────────────────────
function createRl() {
  return readline.createInterface({ input: process.stdin, output: process.stdout })
}

function ask(rl, q) {
  return new Promise((resolve) => rl.question(q, resolve))
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function run() {
  const registry = loadModelRegistry(registryPath)

  const toProcess = Object.entries(registry).filter(([, entry]) => {
    if (FORCE) return true
    const srcs = entry?.sources?.coomer
    return !Array.isArray(srcs) || srcs.length === 0
  })

  console.log(`\nInteractive Coomer source backfill`)
  console.log(`Registry:  ${registryPath}`)
  console.log(`Delay:     ${DELAY}ms between requests`)
  if (FORCE) console.log('Mode:      --force (reviewing all models)')
  console.log(`\nModels without Coomer source: ${toProcess.length}\n`)

  if (toProcess.length === 0) {
    console.log('Nothing to do. Use --force to re-review all models.')
    return
  }

  const rl = createRl()

  try {
    for (let i = 0; i < toProcess.length; i++) {
      const [canonicalName, entry] = toProcess[i]
      const aliases = Array.isArray(entry?.aliases) ? entry.aliases : [canonicalName]

      console.log('='.repeat(70))
      console.log(`[${i + 1}/${toProcess.length}] ${canonicalName}`)
      console.log(`Aliases:   ${aliases.join(', ')}`)

      // ── Auto-probe ────────────────────────────────────────────────────────
      process.stdout.write('  Auto-checking aliases via API...')
      const autoHits = await autoProbeModel(canonicalName, entry)
      process.stdout.write(` ${autoHits.length} hit(s)\n`)

      let saved = false

      for (const hit of autoHits) {
        console.log(`\n  ✅ Found: ${hit.url}  (${hit.service}, id="${hit.name}")`)
        openInBrowser(hit.url)

        while (true) {
          const ans = (await ask(rl, '  Accept this hit? [y=yes / s=skip / q=quit]: ')).trim().toLowerCase()
          if (ans === 'y') {
            resolveAndTrackModel(registryPath, canonicalName, 'coomer', hit.url)
            console.log(`  💾 Saved.`)
            saved = true
            break
          } else if (ans === 's') {
            console.log(`  ⏭️  Skipped.`)
            break
          } else if (ans === 'q') {
            console.log('\n💾 Quitting. Progress already saved as you went.')
            rl.close()
            return
          }
        }
      }

      if (saved) continue

      // ── Manual loop ───────────────────────────────────────────────────────
      const searchUrl = `https://${COOMER_HOST}/search?q=${encodeURIComponent(canonicalName)}`
      console.log(`\n  Opening Coomer search: ${searchUrl}`)
      openInBrowser(searchUrl)

      let currentUrl = null

      while (true) {
        console.log(`
  Commands:
    <username>   Test a username against all Coomer services
    <url>        Paste a full coomer.st URL to validate and save
    o            Reopen current URL in browser
    s            Skip this model
    q            Save and quit`)

        if (currentUrl) console.log(`  Current URL: ${currentUrl}`)
        const raw = (await ask(rl, '\n  > ')).trim()
        if (!raw) continue

        const lower = raw.toLowerCase()

        if (lower === 'q') {
          console.log('\n💾 Quitting. Progress already saved as you went.')
          rl.close()
          return
        }

        if (lower === 's') {
          console.log(`  ⏭️  Skipped ${canonicalName}`)
          break
        }

        if (lower === 'o') {
          if (!currentUrl) { console.log('  No current URL to open.'); continue }
          openInBrowser(currentUrl)
          continue
        }

        // Try to parse as full Coomer URL first
        const parsed = parseCoomerUrl(raw)

        if (parsed) {
          // Validate via API
          process.stdout.write(`  Validating ${parsed.url} ...`)
          try {
            const creator = await lookupCreator(parsed.service, parsed.username)
            if (creator) {
              const displayName = creator.name || parsed.username
              process.stdout.write(` found (id="${displayName}")\n`)
              currentUrl = parsed.url
              openInBrowser(currentUrl)
              const confirm = (await ask(rl, `  Save ${parsed.url} for ${canonicalName}? [y/n]: `)).trim().toLowerCase()
              if (confirm === 'y') {
                resolveAndTrackModel(registryPath, canonicalName, 'coomer', parsed.url)
                console.log(`  💾 Saved.`)
                saved = true
                break
              }
            } else {
              process.stdout.write(` not found on Coomer (404)\n`)
            }
          } catch (err) {
            process.stdout.write(` error: ${err.message}\n`)
          }
          continue
        }

        // Treat as username — probe all services
        const username = sanitize(raw) || raw
        process.stdout.write(`  Checking "${username}" across all services...`)
        const hits = await probeUsername(username)
        process.stdout.write(` ${hits.length} hit(s)\n`)

        for (const hit of hits) {
          console.log(`\n  ✅ ${hit.url}  (${hit.service}, id="${hit.name}")`)
          openInBrowser(hit.url)
          currentUrl = hit.url

          while (true) {
            const ans = (await ask(rl, '  Accept this hit? [y=yes / s=skip / q=quit]: ')).trim().toLowerCase()
            if (ans === 'y') {
              resolveAndTrackModel(registryPath, canonicalName, 'coomer', hit.url)
              console.log(`  💾 Saved.`)
              saved = true
              break
            } else if (ans === 's') {
              console.log(`  ⏭️  Skipped this hit.`)
              break
            } else if (ans === 'q') {
              console.log('\n💾 Quitting. Progress already saved as you went.')
              rl.close()
              return
            }
          }

          if (saved) break
        }

        if (saved) break

        if (hits.length === 0) {
          console.log(`  No Coomer profiles found for "${username}".`)
        }
      }
    }

    console.log('\n🎉 Done. All unsourced models have been reviewed.')
  } finally {
    rl.close()
  }
}

run().catch((err) => {
  console.error(`\n❌ Fatal error: ${err.message}`)
  process.exit(1)
})

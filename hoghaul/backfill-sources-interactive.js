'use strict'

/**
 * backfill-sources-interactive.js
 *
 * Unified interactive backfill for all three source types:
 *   - coomerfans.com  (OnlyFans, Fansly, Patreon, …)
 *   - kemono.cr  (Patreon, Fanbox, Gumroad, Discord, Fantia, …)
 *   - stufferdb  (manual URL paste only)
 *
 * For each model missing any source, auto-probes all known aliases against
 * both APIs, then drops into a prompt for manual lookup / URL paste.
 *
 * Usage:
 *   node hoghaul/backfill-sources-interactive.js [--delay=ms] [--force]
 *
 * Options:
 *   --force     Re-review models that already have all sources
 *   --delay=300 Milliseconds between API requests (default: 300)
 */

const fs = require('fs')
const https = require('https')
const path = require('path')
const readline = require('readline')
const { execFile } = require('child_process')
const minimist = require('minimist')

const {
  sanitize,
  loadModelRegistry,
  resolveAndTrackModel,
} = require('../scrapyard/modelRegistry.js')

const argv = minimist(process.argv.slice(2))
const FORCE = !!argv.force
const AUTO = !!argv.auto   // auto-save exact matches, skip everything else
const DELAY = parseInt(argv.delay ?? 300, 10)

const registryPath = path.join(__dirname, '..', 'model_aliases.json')

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
    // URL format: https://coomerfans.com/u/{service}/{id}/{username}
    urlPattern:
      /^https?:\/\/(?:www\.)?coomerfans\.com\/u\/([^/]+)\/(\d+)\/([^/?#\s]+)/i,
    searchUrl: (name) =>
      `https://coomerfans.com/?q=${encodeURIComponent(name)}`,
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
    urlPattern:
      /^https?:\/\/(?:www\.)?kemono\.(?:cr|su|party)\/([^/]+)\/user\/([^/?#\s]+)/i,
    searchUrl: (name) =>
      `https://kemono.cr/artists?q=${encodeURIComponent(name)}`,
    profileUrl: (service, username) =>
      `https://kemono.cr/api/v1/${service}/user/${encodeURIComponent(username)}/profile`,
    userUrl: (service, username) =>
      `https://kemono.cr/${service}/user/${username}`,
  },
}

const STUFFERDB_PATTERN = /^https?:\/\/(?:bbw\.)?stufferdb\.com\/[^\s]+/i

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

// ─── API LOOKUPS ──────────────────────────────────────────────────────────────

/**
 * Search coomerfans.com for a username. Returns all non-free profile hits
 * found in the HTML, each with { service, id, username, url, platform }.
 */
async function searchCoomer(query) {
  const cfg = PLATFORMS.coomer
  const url = `https://coomerfans.com/?q=${encodeURIComponent(query)}`
  const { status, body } = await httpsGet(cfg.host, url)
  if (status !== 200) return []

  const hits = []
  const linkRe = /href="\/u\/([^/"]+)\/(\d+)\/([^/"]+)"/gi
  let m
  while ((m = linkRe.exec(body)) !== null) {
    const service = m[1].toLowerCase()
    const id = m[2]
    const username = m[3]
    if (!cfg.services.includes(service)) continue
    if (/free/i.test(username)) continue
    const hitUrl = `https://coomerfans.com/u/${service}/${id}/${username}`
    if (!hits.some((h) => h.url === hitUrl)) {
      hits.push({ platform: 'coomer', service, id, username, url: hitUrl, name: username })
    }
  }
  return hits
}

async function lookupKemono(service, username) {
  const cfg = PLATFORMS.kemono
  const apiUrl = `https://kemono.cr/api/v1/${service}/user/${encodeURIComponent(username)}/profile`
  const { status, body } = await httpsGet(cfg.host, apiUrl)
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

async function probeUsername(platform, username) {
  if (platform === 'coomer') {
    return searchCoomer(username)
  }
  // kemono — direct profile lookup per service
  const cfg = PLATFORMS.kemono
  const hits = []
  for (const service of cfg.services) {
    try {
      const creator = await lookupKemono(service, username)
      if (creator) {
        hits.push({
          platform,
          service,
          username,
          url: `https://kemono.cr/${service}/user/${username}`,
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

async function autoProbeModel(canonicalName, entry) {
  const aliases = Array.isArray(entry?.aliases)
    ? entry.aliases
    : [canonicalName]
  const usernames = [
    ...new Set([canonicalName, ...aliases].map(sanitize).filter(Boolean)),
  ]
  const allHits = { coomer: [], kemono: [] }
  for (const username of usernames) {
    for (const platform of ['coomer', 'kemono']) {
      const hits = await probeUsername(platform, username)
      allHits[platform].push(...hits)
    }
  }
  return allHits
}

// ─── URL PARSING ──────────────────────────────────────────────────────────────
function parseSourceUrl(input) {
  const str = String(input || '').trim()

  // CoomerFans: /u/{service}/{id}/{username}
  const coomerM = str.match(PLATFORMS.coomer.urlPattern)
  if (coomerM) {
    const service = coomerM[1].toLowerCase()
    const id = coomerM[2]
    const username = coomerM[3]
    if (PLATFORMS.coomer.services.includes(service)) {
      return {
        platform: 'coomer',
        service,
        id,
        username,
        url: `https://coomerfans.com/u/${service}/${id}/${username}`,
      }
    }
  }

  // Kemono: /{service}/user/{username}
  const kemonoM = str.match(PLATFORMS.kemono.urlPattern)
  if (kemonoM) {
    const service = kemonoM[1].toLowerCase()
    const username = kemonoM[2]
    if (PLATFORMS.kemono.services.includes(service)) {
      return {
        platform: 'kemono',
        service,
        username,
        url: `https://kemono.cr/${service}/user/${username}`,
      }
    }
  }

  if (STUFFERDB_PATTERN.test(str)) {
    return { platform: 'stufferdb', url: str }
  }

  return null
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
    if (err)
      console.log(`  ⚠️  Could not open browser: ${err.message}\n  URL: ${url}`)
  })
}

// ─── PROMPT ───────────────────────────────────────────────────────────────────
function ask(rl, q) {
  return new Promise((resolve) => rl.question(q, resolve))
}

function hasSource(entry, platform) {
  const srcs = entry?.sources?.[platform]
  return Array.isArray(srcs) && srcs.length > 0
}

function sourceLabel(entry, platform) {
  if (!hasSource(entry, platform)) return '❌ missing'
  const srcs = entry.sources[platform]
  const url = srcs[0]?.url || srcs[0]
  return `✅ ${url}`
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function run() {
  const registry = loadModelRegistry(registryPath)

  const toProcess = Object.entries(registry).filter(([, entry]) => {
    if (FORCE) return true
    return (
      !hasSource(entry, 'coomer') ||
      !hasSource(entry, 'kemono') ||
      !hasSource(entry, 'stufferdb')
    )
  })

  console.log('\n  ╔══════════════════════════════════════════╗')
  console.log('  ║   Unified Source Backfill (Interactive)  ║')
  console.log('  ╚══════════════════════════════════════════╝\n')
  console.log(`  Registry: ${registryPath}`)
  console.log(`  Delay:    ${DELAY}ms between API requests`)
  if (FORCE) console.log('  Mode:     --force (reviewing all models)')
  if (AUTO) console.log('  Mode:     --auto (save exact matches, skip rest)')
  console.log(`\n  Models needing sources: ${toProcess.length}\n`)

  if (toProcess.length === 0) {
    console.log('  Nothing to do. Use --force to re-review all models.')
    return
  }

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  })

  try {
    for (let i = 0; i < toProcess.length; i++) {
      const [canonicalName, entry] = toProcess[i]
      const aliases = Array.isArray(entry?.aliases)
        ? entry.aliases
        : [canonicalName]

      console.log('\n' + '═'.repeat(68))
      console.log(`[${i + 1}/${toProcess.length}] ${canonicalName}`)
      console.log(`  Aliases:   ${aliases.join(', ')}`)
      console.log(`  Coomer:    ${sourceLabel(entry, 'coomer')}`)
      console.log(`  Kemono:    ${sourceLabel(entry, 'kemono')}`)
      console.log(`  StufferDB: ${sourceLabel(entry, 'stufferdb')}`)

      // ── Auto-probe coomer + kemono ─────────────────────────────────────────
      const missing = []
      if (!hasSource(entry, 'coomer')) missing.push('coomer')
      if (!hasSource(entry, 'kemono')) missing.push('kemono')

      let autoHits = { coomer: [], kemono: [] }
      if (missing.length > 0) {
        process.stdout.write(
          `\n  Auto-probing ${missing.join(' + ')} for all aliases...`
        )
        autoHits = await autoProbeModel(canonicalName, entry)
        const total = autoHits.coomer.length + autoHits.kemono.length
        process.stdout.write(` ${total} hit(s)\n`)
      }

      let savedCoomer = hasSource(entry, 'coomer')
      let savedKemono = hasSource(entry, 'kemono')
      let savedStufferdb = hasSource(entry, 'stufferdb')

      // Sanitized alias set for exact-match detection
      const aliasSet = new Set(
        [canonicalName, ...(entry.aliases || [])].map((a) => sanitize(a).toLowerCase()).filter(Boolean)
      )

      // ── Accept/reject auto hits ────────────────────────────────────────────
      for (const platform of ['coomer', 'kemono']) {
        if (hasSource(entry, platform)) continue
        for (const hit of autoHits[platform]) {
          const isExact = aliasSet.has(hit.username.toLowerCase())

          if (AUTO) {
            if (isExact) {
              resolveAndTrackModel(registryPath, canonicalName, platform, hit.url)
              console.log(`\n  💾 [auto] ${hit.url}  (${hit.service})`)
              if (platform === 'coomer') savedCoomer = true
              if (platform === 'kemono') savedKemono = true
            }
            // non-exact hits silently skipped in auto mode
            if (
              (platform === 'coomer' && savedCoomer) ||
              (platform === 'kemono' && savedKemono)
            )
              break
            continue
          }

          console.log(
            `\n  ✅ [${hit.platform}] ${hit.url}  (${hit.service}, id="${hit.name}")`
          )
          openInBrowser(hit.url)
          while (true) {
            const ans = (await ask(rl, '  Accept? [y=yes / s=skip / q=quit]: '))
              .trim()
              .toLowerCase()
            if (ans === 'y') {
              resolveAndTrackModel(registryPath, canonicalName, platform, hit.url)
              console.log(`  💾 Saved.`)
              if (platform === 'coomer') savedCoomer = true
              if (platform === 'kemono') savedKemono = true
              break
            } else if (ans === 's') {
              console.log(`  ⏭️  Skipped.`)
              break
            } else if (ans === 'q') {
              console.log('\n  💾 Quitting. Progress saved as you went.')
              rl.close()
              return
            }
          }
          if (
            (platform === 'coomer' && savedCoomer) ||
            (platform === 'kemono' && savedKemono)
          )
            break
        }
      }

      // ── Manual loop if still missing anything (skipped in --auto mode) ──────
      if (AUTO) continue

      const stillMissing = []
      if (!savedCoomer) stillMissing.push('coomer')
      if (!savedKemono) stillMissing.push('kemono')
      if (!savedStufferdb) stillMissing.push('stufferdb')

      if (stillMissing.length > 0) {
        // Open search pages for what's still missing
        if (!savedCoomer) {
          const url = PLATFORMS.coomer.searchUrl(canonicalName)
          console.log(`\n  Opening Coomer search: ${url}`)
          openInBrowser(url)
        }
        if (!savedKemono) {
          const url = PLATFORMS.kemono.searchUrl(canonicalName)
          console.log(`  Opening Kemono search: ${url}`)
          openInBrowser(url)
        }

        let currentUrl = null

        while (true) {
          const still = []
          if (!savedCoomer) still.push('coomer')
          if (!savedKemono) still.push('kemono')
          if (!savedStufferdb) still.push('stufferdb')
          if (still.length === 0) break

          console.log(`\n  Still missing: ${still.join(', ')}`)
          console.log(`  Commands:
    <url>        paste coomer/kemono/stufferdb URL to validate + save
    c <username> probe Coomer for a specific username
    k <username> probe Kemono for a specific username
    o            reopen current URL in browser
    s            skip this model (move to next)
    q            quit`)

          if (currentUrl) console.log(`  Current URL: ${currentUrl}`)
          const raw = (await ask(rl, '\n  > ')).trim()
          if (!raw) continue

          const lower = raw.toLowerCase()

          if (lower === 'q') {
            console.log('\n  💾 Quitting. Progress saved as you went.')
            rl.close()
            return
          }

          if (lower === 's') {
            console.log(`  ⏭️  Skipping ${canonicalName}`)
            break
          }

          if (lower === 'o') {
            if (!currentUrl) {
              console.log('  No current URL to open.')
              continue
            }
            openInBrowser(currentUrl)
            continue
          }

          // c <username> — probe coomer
          if (lower.startsWith('c ')) {
            const username =
              sanitize(raw.slice(2).trim()) || raw.slice(2).trim()
            process.stdout.write(`  Checking Coomer "${username}"...`)
            const hits = await probeUsername('coomer', username)
            process.stdout.write(` ${hits.length} hit(s)\n`)
            for (const hit of hits) {
              console.log(
                `\n  ✅ ${hit.url}  (${hit.service}, id="${hit.name}")`
              )
              openInBrowser(hit.url)
              currentUrl = hit.url
              while (true) {
                const ans = (await ask(rl, '  Accept? [y/s/q]: '))
                  .trim()
                  .toLowerCase()
                if (ans === 'y') {
                  resolveAndTrackModel(
                    registryPath,
                    canonicalName,
                    'coomer',
                    hit.url
                  )
                  console.log('  💾 Saved.')
                  savedCoomer = true
                  break
                } else if (ans === 's') {
                  break
                } else if (ans === 'q') {
                  console.log('\n  💾 Quitting.')
                  rl.close()
                  return
                }
              }
              if (savedCoomer) break
            }
            if (hits.length === 0)
              console.log(`  No Coomer profiles found for "${username}".`)
            continue
          }

          // k <username> — probe kemono
          if (lower.startsWith('k ')) {
            const username =
              sanitize(raw.slice(2).trim()) || raw.slice(2).trim()
            process.stdout.write(`  Checking Kemono "${username}"...`)
            const hits = await probeUsername('kemono', username)
            process.stdout.write(` ${hits.length} hit(s)\n`)
            for (const hit of hits) {
              console.log(
                `\n  ✅ ${hit.url}  (${hit.service}, id="${hit.name}")`
              )
              openInBrowser(hit.url)
              currentUrl = hit.url
              while (true) {
                const ans = (await ask(rl, '  Accept? [y/s/q]: '))
                  .trim()
                  .toLowerCase()
                if (ans === 'y') {
                  resolveAndTrackModel(
                    registryPath,
                    canonicalName,
                    'kemono',
                    hit.url
                  )
                  console.log('  💾 Saved.')
                  savedKemono = true
                  break
                } else if (ans === 's') {
                  break
                } else if (ans === 'q') {
                  console.log('\n  💾 Quitting.')
                  rl.close()
                  return
                }
              }
              if (savedKemono) break
            }
            if (hits.length === 0)
              console.log(`  No Kemono profiles found for "${username}".`)
            continue
          }

          // URL paste — parse and validate
          const parsed = parseSourceUrl(raw)
          if (!parsed) {
            console.log(
              '  Unrecognized input. Paste a coomer/kemono/stufferdb URL, or use c/k commands.'
            )
            continue
          }

          // StufferDB — no API validation, just save
          if (parsed.platform === 'stufferdb') {
            if (savedStufferdb) {
              console.log('  StufferDB already saved for this model.')
              continue
            }
            const confirm = (
              await ask(rl, `  Save ${parsed.url} as StufferDB source? [y/n]: `)
            )
              .trim()
              .toLowerCase()
            if (confirm === 'y') {
              resolveAndTrackModel(
                registryPath,
                canonicalName,
                'stufferdb',
                parsed.url
              )
              console.log('  💾 Saved.')
              savedStufferdb = true
            }
            continue
          }

          // Coomer or Kemono — validate then save
          const platform = parsed.platform
          if (
            (platform === 'coomer' && savedCoomer) ||
            (platform === 'kemono' && savedKemono)
          ) {
            console.log(`  ${platform} already saved for this model.`)
            continue
          }

          process.stdout.write(`  Validating ${parsed.url} ...`)
          try {
            const host =
              platform === 'coomer' ? 'coomerfans.com' : 'kemono.cr'
            let found = false
            if (platform === 'kemono') {
              const creator = await lookupKemono(
                parsed.service,
                parsed.username
              )
              found = !!creator
              if (found)
                process.stdout.write(` found (id="${creator.name || parsed.username}")\n`)
              else process.stdout.write(' not found (404)\n')
            } else {
              // CoomerFans — verify the page exists
              const { status } = await httpsGet(host, parsed.url)
              found = status === 200
              process.stdout.write(found ? ' found\n' : ` HTTP ${status}\n`)
            }

            if (found) {
              currentUrl = parsed.url
              openInBrowser(currentUrl)
              const confirm = (
                await ask(
                  rl,
                  `  Save ${parsed.url} for ${canonicalName}? [y/n]: `
                )
              )
                .trim()
                .toLowerCase()
              if (confirm === 'y') {
                resolveAndTrackModel(
                  registryPath,
                  canonicalName,
                  platform,
                  parsed.url
                )
                console.log('  💾 Saved.')
                if (platform === 'coomer') savedCoomer = true
                if (platform === 'kemono') savedKemono = true
              }
            }
          } catch (err) {
            process.stdout.write(` error: ${err.message}\n`)
          }
        }
      }
    }

    console.log('\n  🎉 Done. All models reviewed.')
  } finally {
    rl.close()
  }
}

run().catch((err) => {
  console.error(`\n  ❌ Fatal: ${err.message}`)
  process.exit(1)
})

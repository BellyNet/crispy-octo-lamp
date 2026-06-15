'use strict'

/**
 * backfill-sources-interactive.js
 *
 * Unified interactive backfill for all three source types:
 *   - coomerfans.com  (OnlyFans, Fansly, Patreon, …)
 *   - pawchive.st (Patreon, Fanbox, Gumroad, Discord, Fantia, ...)
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
 *   --auto      Run registry-wide exact username matching without prompts
 *   --retry-auto Ignore remembered no-match results and try them again
 *   --models=x  Limit auto-match and review to comma-separated models
 *   --no-open   Print search/profile URLs without opening a browser
 *   --delay=300 Milliseconds between API requests (default: 300)
 *   --reddit-delay=6500 Milliseconds between Reddit profile probes
 */

const fs = require('fs')
const https = require('https')
const http2 = require('http2')
const path = require('path')
const readline = require('readline')
const { execFile } = require('child_process')
const minimist = require('minimist')

const {
  sanitize,
  loadModelRegistry,
  resolveAndTrackModel,
} = require('../scrapyard/modelRegistry.js')
const {
  PAWCHIVE_HOST,
  PAWCHIVE_ORIGIN,
  getPawchiveProfileUrl,
  getPawchiveUserUrl,
} = require('../scrapyard/pawchive')

const argv = minimist(process.argv.slice(2))
const FORCE = !!argv.force
const AUTO = !!argv.auto // auto-save canonical/alias username matches, skip everything else
const RETRY_AUTO = !!argv['retry-auto']
const OPEN_BROWSER = argv.open !== false
const DELAY = parseInt(argv.delay ?? 300, 10)
const MODEL_FILTER = new Set(
  String(argv.models || argv.model || '')
    .split(',')
    .map(sanitize)
    .filter(Boolean)
)

const registryPath = path.join(__dirname, '..', 'model_aliases.json')
const permanentSkipPath = path.join(
  __dirname,
  'source-backfill-permanent-skips.json'
)
const autoAttemptPath = path.join(
  __dirname,
  'source-backfill-auto-attempts.json'
)

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
    host: PAWCHIVE_HOST,
    label: 'Pawchive',
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
      /^https?:\/\/(?:www\.)?(?:pawchive\.st|kemono\.(?:cr|su|party))\/([^/]+)\/user\/([^/?#\s]+)/i,
    searchUrl: (name) =>
      `${PAWCHIVE_ORIGIN}/artists?q=${encodeURIComponent(name)}`,
    profileUrl: getPawchiveProfileUrl,
    userUrl: getPawchiveUserUrl,
  },
  reddit: {
    host: 'www.reddit.com',
    label: 'Reddit',
    urlPattern:
      /^https?:\/\/(?:www\.)?reddit\.com\/(?:user|u)\/([^/?#\s]+)(?:\/submitted)?\/?/i,
    searchUrl: (name) =>
      `https://www.reddit.com/search/?q=${encodeURIComponent(name)}&type=users`,
    userUrl: (username) =>
      `https://www.reddit.com/user/${encodeURIComponent(username)}/submitted/`,
    probeUrl: (username) =>
      `https://old.reddit.com/user/${encodeURIComponent(username)}/submitted/?over18=1`,
  },
}

const STUFFERDB_PATTERN = /^https?:\/\/(?:bbw\.)?stufferdb\.com\/[^\s]+/i
const SOURCE_PLATFORMS = ['coomer', 'kemono', 'reddit', 'stufferdb']
const REDDIT_PROBE_USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36'
const REDDIT_PROBE_DELAY_MS = parseNonNegativeInteger(
  argv['reddit-delay'],
  6500
)
const REDDIT_PROBE_RETRY_DELAY_MS = parseNonNegativeInteger(
  argv['reddit-retry-delay'],
  30000
)
const REDDIT_PROBE_MAX_RETRIES = 2
let lastRedditProbeAt = 0

// ─── HTTP ─────────────────────────────────────────────────────────────────────
function httpsGet(host, url, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          Accept: 'text/css',
          Referer: `https://${host}/`,
          ...headers,
        },
      },
      (res) => {
        if (
          res.statusCode >= 300 &&
          res.statusCode < 400 &&
          res.headers.location
        ) {
          const nextUrl = new URL(res.headers.location, url).toString()
          return httpsGet(host, nextUrl, headers).then(resolve).catch(reject)
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

function http2Get(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url)
    const client = http2.connect(parsed.origin)
    let settled = false

    const finish = (callback) => {
      if (settled) return
      settled = true
      client.close()
      callback()
    }

    client.on('error', (err) => finish(() => reject(err)))

    const requestHeaders = {
      ':method': 'GET',
      ':path': `${parsed.pathname}${parsed.search}`,
      ':authority': parsed.host,
    }
    for (const [name, value] of Object.entries(headers)) {
      requestHeaders[name.toLowerCase()] = value
    }

    const req = client.request(requestHeaders)
    let responseHeaders = {}
    let body = ''
    req.setEncoding('utf8')
    req.on('response', (value) => {
      responseHeaders = value
    })
    req.on('data', (chunk) => {
      body += chunk
    })
    req.on('end', () =>
      finish(() =>
        resolve({
          status: responseHeaders[':status'],
          body,
        })
      )
    )
    req.on('error', (err) => finish(() => reject(err)))
    req.setTimeout(10000, () => {
      req.close(http2.constants.NGHTTP2_CANCEL)
      finish(() => reject(new Error('timeout')))
    })
    req.end()
  })
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

function parseNonNegativeInteger(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ''), 10)
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback
}

function isSelectedModel(canonicalName) {
  return MODEL_FILTER.size === 0 || MODEL_FILTER.has(sanitize(canonicalName))
}

function loadPermanentSkips() {
  if (!fs.existsSync(permanentSkipPath)) {
    return { version: 1, updatedAt: null, skips: {} }
  }
  try {
    const parsed = JSON.parse(fs.readFileSync(permanentSkipPath, 'utf8'))
    return {
      version: 1,
      updatedAt: parsed?.updatedAt || null,
      skips:
        parsed?.skips && typeof parsed.skips === 'object' ? parsed.skips : {},
    }
  } catch (err) {
    console.warn(
      `  Warning: could not parse permanent skip file ${permanentSkipPath}: ${err.message}`
    )
    return { version: 1, updatedAt: null, skips: {} }
  }
}

function savePermanentSkips(state) {
  state.updatedAt = new Date().toISOString()
  fs.writeFileSync(permanentSkipPath, JSON.stringify(state, null, 2) + '\n')
}

function loadAutoAttempts() {
  if (!fs.existsSync(autoAttemptPath)) {
    return { version: 1, updatedAt: null, attempts: {} }
  }
  try {
    const parsed = JSON.parse(fs.readFileSync(autoAttemptPath, 'utf8'))
    return {
      version: 1,
      updatedAt: parsed?.updatedAt || null,
      attempts:
        parsed?.attempts && typeof parsed.attempts === 'object'
          ? parsed.attempts
          : {},
    }
  } catch (err) {
    console.warn(
      `  Warning: could not parse auto-attempt file ${autoAttemptPath}: ${err.message}`
    )
    return { version: 1, updatedAt: null, attempts: {} }
  }
}

function saveAutoAttempts(state) {
  state.updatedAt = new Date().toISOString()
  fs.writeFileSync(autoAttemptPath, JSON.stringify(state, null, 2) + '\n')
}

function getAutoAttemptSignature(canonicalName, entry, missingPlatforms) {
  return JSON.stringify({
    model: sanitize(canonicalName),
    aliases: getModelUsernameSeeds(canonicalName, entry)
      .map((value) => value.toLowerCase())
      .sort(),
    missingPlatforms: [...missingPlatforms].sort(),
  })
}

function getRememberedAutoAttempt(state, canonicalName, platform, signature) {
  const modelKey = sanitize(canonicalName)
  const attempt = state.attempts?.[modelKey]?.[platform]
  return ['no_match', 'ambiguous'].includes(attempt?.result) &&
    attempt.signature === signature
    ? attempt
    : null
}

function rememberAutoResult(
  state,
  canonicalName,
  platform,
  signature,
  usernames,
  result,
  candidates = []
) {
  const modelKey = sanitize(canonicalName)
  if (!state.attempts[modelKey]) state.attempts[modelKey] = {}
  state.attempts[modelKey][platform] = {
    result,
    attemptedAt: new Date().toISOString(),
    candidates,
    signature,
    usernames,
  }
}

function clearAutoAttempt(state, canonicalName, platform) {
  const modelKey = sanitize(canonicalName)
  if (!state.attempts?.[modelKey]?.[platform]) return
  delete state.attempts[modelKey][platform]
  if (Object.keys(state.attempts[modelKey]).length === 0) {
    delete state.attempts[modelKey]
  }
}

function getPermanentSkipEntry(state, canonicalName, platform) {
  const modelKey = sanitize(canonicalName)
  const platformKey = String(platform || '')
    .trim()
    .toLowerCase()
  return state?.skips?.[modelKey]?.[platformKey] || null
}

function isPermanentlySkipped(state, canonicalName, platform) {
  return Boolean(getPermanentSkipEntry(state, canonicalName, platform))
}

function markPermanentSkip(
  state,
  canonicalName,
  platform,
  reason = 'no_content'
) {
  const modelKey = sanitize(canonicalName)
  const platformKey = String(platform || '')
    .trim()
    .toLowerCase()
  if (!modelKey || !SOURCE_PLATFORMS.includes(platformKey)) return false
  if (!state.skips[modelKey]) state.skips[modelKey] = {}
  state.skips[modelKey][platformKey] = {
    reason: String(reason || '').trim() || 'no_content',
    skippedAt: new Date().toISOString(),
  }
  savePermanentSkips(state)
  return true
}

function parseSkipPlatforms(input) {
  const normalized = String(input || '')
    .trim()
    .toLowerCase()
  if (!normalized || normalized === 'all') return SOURCE_PLATFORMS
  return normalized
    .split(',')
    .map((part) => part.trim())
    .filter((part) => SOURCE_PLATFORMS.includes(part))
}

function getCoomerSearchResultTotal(html) {
  const text = String(html || '').replace(/\s+/g, ' ')
  const match = text.match(/Names of Models\s*-\s*.*?\bTotal\s+(\d+)/i)
  return match ? Number.parseInt(match[1], 10) : null
}

// ─── API LOOKUPS ──────────────────────────────────────────────────────────────

/**
 * Search coomerfans.com for a username. Returns the first non-free profile hit
 * found in the HTML, each with { service, id, username, url, platform }.
 */
async function searchCoomer(query) {
  const cfg = PLATFORMS.coomer
  const url = `https://coomerfans.com/?q=${encodeURIComponent(query)}`
  const { status, body } = await httpsGet(cfg.host, url)
  if (status !== 200) return []

  const resultTotal = getCoomerSearchResultTotal(body)
  if (resultTotal === 0 || /Nothing was found for your request/i.test(body)) {
    return []
  }

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
      hits.push({
        platform: 'coomer',
        service,
        id,
        username,
        url: hitUrl,
        name: username,
      })
    }
  }
  return hits.slice(0, 10)
}

async function lookupKemono(service, username) {
  const cfg = PLATFORMS.kemono
  const creatorId = /^\d+$/.test(String(username || ''))
    ? String(username)
    : await resolvePawchiveCreatorId(service, username)
  if (!creatorId) return null

  const apiUrl = cfg.profileUrl(service, creatorId)
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

let pawchiveCreatorsPromise = null

function normalizeCreatorName(value) {
  return String(value || '')
    .trim()
    .replace(/^@+/, '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')
}

async function loadPawchiveCreators() {
  if (!pawchiveCreatorsPromise) {
    const startedAt = Date.now()
    process.stdout.write('  Loading Pawchive creator catalog...')
    pawchiveCreatorsPromise = httpsGet(
      PAWCHIVE_HOST,
      `${PAWCHIVE_ORIGIN}/api/v1/creators`,
      { Accept: 'application/json' }
    )
      .then(({ status, body }) => {
        if (status !== 200) throw new Error(`HTTP ${status}`)
        const creators = JSON.parse(body)
        if (!Array.isArray(creators)) {
          throw new Error('Pawchive creators response was not an array')
        }
        process.stdout.write(
          ` ${creators.length.toLocaleString()} creators (${(
            (Date.now() - startedAt) /
            1000
          ).toFixed(1)}s)\n`
        )
        return creators
      })
      .catch((err) => {
        pawchiveCreatorsPromise = null
        process.stdout.write(` failed: ${err.message}\n`)
        throw err
      })
  }
  return pawchiveCreatorsPromise
}

async function findPawchiveCreators(username, service = null) {
  const normalizedName = normalizeCreatorName(username)
  if (!normalizedName) return []
  const creators = await loadPawchiveCreators()
  return creators.filter(
    (item) =>
      (!service || item?.service === service) &&
      PLATFORMS.kemono.services.includes(item?.service) &&
      normalizeCreatorName(item?.name) === normalizedName
  )
}

async function resolvePawchiveCreatorId(service, username) {
  const creators = await findPawchiveCreators(username, service)
  return creators[0]?.id ? String(creators[0].id) : null
}

async function lookupReddit(username) {
  const cfg = PLATFORMS.reddit
  const cleanedUsername = String(username || '').replace(/^u_/, '')
  if (!cleanedUsername) return null

  for (let attempt = 0; attempt <= REDDIT_PROBE_MAX_RETRIES; attempt += 1) {
    const waitMs = Math.max(
      lastRedditProbeAt + REDDIT_PROBE_DELAY_MS - Date.now(),
      0
    )
    if (waitMs > 0) await sleep(waitMs)
    lastRedditProbeAt = Date.now()

    const response = await http2Get(cfg.probeUrl(cleanedUsername), {
      Accept: 'text/html,application/xhtml+xml',
      'Accept-Language': 'en-US,en;q=0.9',
      Cookie: 'over18=1;',
      Referer: 'https://old.reddit.com/',
      'User-Agent': REDDIT_PROBE_USER_AGENT,
    })
    if (response.status === 200) {
      return {
        username: cleanedUsername,
        url: cfg.userUrl(cleanedUsername),
      }
    }
    if (response.status === 404) return null
    if (
      ![403, 429].includes(response.status) ||
      attempt >= REDDIT_PROBE_MAX_RETRIES
    ) {
      throw new Error(`HTTP ${response.status}`)
    }
    const retryDelay = REDDIT_PROBE_RETRY_DELAY_MS * (attempt + 1)
    process.stdout.write(
      ` retrying HTTP ${response.status} in ${Math.round(retryDelay / 1000)}s...`
    )
    await sleep(retryDelay)
  }

  return null
}

async function probeUsername(platform, username) {
  if (platform === 'coomer') {
    return searchCoomer(username)
  }
  if (platform === 'reddit') {
    const hit = await lookupReddit(username)
    return hit
      ? [
          {
            platform,
            service: 'submitted',
            username: hit.username,
            url: hit.url,
            name: hit.username,
          },
        ]
      : []
  }
  // kemono — direct profile lookup per service
  if (!/^\d+$/.test(String(username || ''))) {
    const creators = await findPawchiveCreators(username)
    return creators.map((creator) => ({
      platform,
      service: creator.service,
      id: String(creator.id),
      username: creator.name || username,
      url: PLATFORMS.kemono.userUrl(creator.service, creator.id),
      name: creator.name || username,
    }))
  }

  // Numeric creator IDs do not identify their service, so try each one.
  const cfg = PLATFORMS.kemono
  const hits = []
  for (const service of cfg.services) {
    try {
      const creator = await lookupKemono(service, username)
      if (creator) {
        const creatorId = String(creator.id || username)
        hits.push({
          platform,
          service,
          id: creatorId,
          username: creator.name || username,
          url: cfg.userUrl(service, creatorId),
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

function getModelUsernameSeeds(canonicalName, entry) {
  const aliases = Array.isArray(entry?.aliases) ? entry.aliases : []
  return [...new Set([canonicalName, ...aliases].map(sanitize).filter(Boolean))]
}

function isModelUsernameMatch(canonicalName, entry, hit) {
  const username = normalizeCreatorName(hit?.username)
  if (!username) return false

  for (const seed of getModelUsernameSeeds(canonicalName, entry)) {
    const normalizedSeed = normalizeCreatorName(seed)
    if (username === normalizedSeed) return true
  }

  return false
}

function addUniqueHit(targets, hit) {
  if (!hit?.url || targets.some((existing) => existing.url === hit.url)) return
  targets.push(hit)
}

async function autoProbeMatchingUsernames(canonicalName, entry, platforms) {
  const usernames = getModelUsernameSeeds(canonicalName, entry)
  const allHits = { coomer: [], kemono: [], reddit: [] }
  const errors = { coomer: [], kemono: [], reddit: [] }
  if (!usernames.length) return { allHits, errors }

  if (
    platforms.includes('kemono') &&
    usernames.some((username) => !/^\d+$/.test(username))
  ) {
    try {
      await loadPawchiveCreators()
    } catch {
      // The individual probe reports the error and can retry.
    }
  }

  for (const platform of platforms) {
    for (const username of usernames) {
      const startedAt = Date.now()
      process.stdout.write(`    ${PLATFORMS[platform].label} "${username}"...`)
      try {
        const hits = await probeUsername(platform, username)
        for (const hit of hits) {
          if (isModelUsernameMatch(canonicalName, entry, hit)) {
            addUniqueHit(allHits[platform], hit)
          }
        }
        process.stdout.write(
          ` ${hits.length} hit(s) (${((Date.now() - startedAt) / 1000).toFixed(
            1
          )}s)\n`
        )
      } catch (err) {
        process.stdout.write(` error: ${err.message}\n`)
        errors[platform].push({
          username,
          message: err.message,
        })
        await sleep(DELAY * 2)
      }
    }
  }

  return { allHits, errors }
}

async function autoSaveDirectUsernameMatches(
  registry,
  skipState,
  attemptState
) {
  let saved = 0
  let probed = 0
  let skipped = 0
  let noMatches = 0
  let ambiguous = 0
  let failed = 0
  const candidates = Object.entries(registry).filter(
    ([canonicalName, entry]) =>
      isSelectedModel(canonicalName) &&
      getMissingSources(canonicalName, entry, skipState, false).length > 0
  )

  for (let index = 0; index < candidates.length; index += 1) {
    const [canonicalName, entry] = candidates[index]
    const missing = getMissingSources(canonicalName, entry, skipState, false)
    const signature = getAutoAttemptSignature(canonicalName, entry, missing)
    const usernames = getModelUsernameSeeds(canonicalName, entry)
    const remembered = RETRY_AUTO
      ? []
      : missing.filter((platform) =>
          getRememberedAutoAttempt(
            attemptState,
            canonicalName,
            platform,
            signature
          )
        )
    const platformsToProbe = missing.filter(
      (platform) => !remembered.includes(platform)
    )

    console.log(
      `  [${index + 1}/${candidates.length}] ${canonicalName} | missing: ${missing
        .map((platform) => PLATFORMS[platform].label)
        .join(', ')}`
    )

    if (remembered.length > 0) {
      console.log(
        `    remembered no match: ${remembered
          .map((platform) => {
            const attempt = getRememberedAutoAttempt(
              attemptState,
              canonicalName,
              platform,
              signature
            )
            const attemptedAt = attempt?.attemptedAt
              ? ` on ${attempt.attemptedAt}`
              : ''
            const result =
              attempt?.result === 'ambiguous' ? 'ambiguous' : 'no match'
            return `${PLATFORMS[platform].label} (${result})${attemptedAt}`
          })
          .join(', ')}`
      )
    }
    if (platformsToProbe.length === 0) {
      console.log('    status: skipped; aliases and missing sources unchanged')
      skipped += 1
      continue
    }

    console.log(
      `    probing: ${platformsToProbe
        .map((platform) => PLATFORMS[platform].label)
        .join(', ')}`
    )
    probed += 1
    const { allHits, errors } = await autoProbeMatchingUsernames(
      canonicalName,
      entry,
      platformsToProbe
    )

    for (const platform of platformsToProbe) {
      const hits = allHits[platform]
      if (hits.length === 1) {
        const hit = hits[0]
        resolveAndTrackModel(registryPath, canonicalName, platform, hit.url)
        clearAutoAttempt(attemptState, canonicalName, platform)
        console.log(
          `    matched ${PLATFORMS[platform].label}: ${hit.url} (${hit.username})`
        )
        saved += 1
        continue
      }

      if (hits.length > 1) {
        rememberAutoResult(
          attemptState,
          canonicalName,
          platform,
          signature,
          usernames,
          'ambiguous',
          hits.map((hit) => hit.url)
        )
        console.log(
          `    ${PLATFORMS[platform].label}: ${hits.length} exact matches; remembered as ambiguous for manual review`
        )
        ambiguous += 1
        continue
      }

      if (errors[platform].length > 0) {
        console.log(
          `    ${PLATFORMS[platform].label}: probe failed; will retry next run`
        )
        failed += 1
        continue
      }

      rememberAutoResult(
        attemptState,
        canonicalName,
        platform,
        signature,
        usernames,
        'no_match'
      )
      console.log(
        `    ${PLATFORMS[platform].label}: no match; remembered for future runs`
      )
      noMatches += 1
    }
    saveAutoAttempts(attemptState)
  }

  return {
    ambiguous,
    checked: candidates.length,
    failed,
    noMatches,
    probed,
    saved,
    skipped,
  }
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

  // Pawchive or legacy Kemono: /{service}/user/{creator-id}
  const kemonoM = str.match(PLATFORMS.kemono.urlPattern)
  if (kemonoM) {
    const service = kemonoM[1].toLowerCase()
    const username = kemonoM[2]
    if (PLATFORMS.kemono.services.includes(service)) {
      return {
        platform: 'kemono',
        service,
        username,
        url: PLATFORMS.kemono.userUrl(service, username),
      }
    }
  }

  const redditM = str.match(PLATFORMS.reddit.urlPattern)
  if (redditM) {
    const username = redditM[1].replace(/^u_/, '')
    return {
      platform: 'reddit',
      service: 'submitted',
      username,
      url: PLATFORMS.reddit.userUrl(username),
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
  if (!OPEN_BROWSER) {
    console.log(`  Browser opening disabled: ${url}`)
    return
  }
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

function needsSource(canonicalName, entry, platform, skipState) {
  return (
    !hasSource(entry, platform) &&
    !isPermanentlySkipped(skipState, canonicalName, platform)
  )
}

function getMissingSources(canonicalName, entry, skipState, includeStufferdb) {
  return SOURCE_PLATFORMS.filter((platform) => {
    if (!includeStufferdb && platform === 'stufferdb') return false
    return needsSource(canonicalName, entry, platform, skipState)
  })
}

function sourceLabel(entry, platform) {
  if (!hasSource(entry, platform)) return '❌ missing'
  const srcs = entry.sources[platform]
  const url = srcs[0]?.url || srcs[0]
  return `✅ ${url}`
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
function sourceStatusLabel(canonicalName, entry, platform, skipState) {
  const permanentSkip = getPermanentSkipEntry(
    skipState,
    canonicalName,
    platform
  )
  if (permanentSkip) return `permanent skip (${permanentSkip.reason})`
  return sourceLabel(entry, platform)
}

async function promptPermanentSkip(
  rl,
  skipState,
  canonicalName,
  defaultPlatform
) {
  const prompt = defaultPlatform
    ? `  Permanently skip ${defaultPlatform} for ${canonicalName}? [y/N]: `
    : `  Platform(s) to permanently skip for ${canonicalName} (coomer, kemono, reddit, stufferdb, all): `
  const answer = (await ask(rl, prompt)).trim().toLowerCase()
  if (defaultPlatform) {
    if (answer !== 'y' && answer !== 'yes') return []
    markPermanentSkip(skipState, canonicalName, defaultPlatform)
    return [defaultPlatform]
  }

  const platforms = parseSkipPlatforms(answer)
  if (!platforms.length) {
    console.log('  No valid platform selected.')
    return []
  }
  for (const platform of platforms) {
    markPermanentSkip(skipState, canonicalName, platform)
  }
  return platforms
}

async function run() {
  let registry = loadModelRegistry(registryPath)
  const permanentSkips = loadPermanentSkips()
  const autoAttempts = loadAutoAttempts()

  console.log('\n  Running matching username source auto-backfill...')
  if (RETRY_AUTO) {
    console.log('  Retry mode: ignoring remembered no-match results.')
  }
  const autoSummary = await autoSaveDirectUsernameMatches(
    registry,
    permanentSkips,
    autoAttempts
  )
  if (autoSummary.saved > 0) {
    registry = loadModelRegistry(registryPath)
  }
  console.log(
    `  Auto-backfill complete: ${autoSummary.checked} checked, ${autoSummary.probed} probed, ${autoSummary.skipped} remembered skips, ${autoSummary.noMatches} new no-matches, ${autoSummary.ambiguous} ambiguous, ${autoSummary.failed} failed, ${autoSummary.saved} added.`
  )
  if (AUTO) return

  const toProcess = Object.entries(registry).filter(
    ([canonicalName, entry]) => {
      if (!isSelectedModel(canonicalName)) return false
      if (FORCE) return true
      return (
        getMissingSources(canonicalName, entry, permanentSkips, true).length > 0
      )
    }
  )

  console.log('\n  ╔══════════════════════════════════════════╗')
  console.log('  ║   Unified Source Backfill (Interactive)  ║')
  console.log('  ╚══════════════════════════════════════════╝\n')
  console.log(`  Registry: ${registryPath}`)
  console.log(`  Delay:    ${DELAY}ms between API requests`)
  if (FORCE) console.log('  Mode:     --force (reviewing all models)')
  if (AUTO)
    console.log(
      '  Mode:     --auto (save canonical/alias username matches, skip rest)'
    )
  if (RETRY_AUTO)
    console.log('  Mode:     --retry-auto (retrying remembered no-matches)')
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
      console.log(
        `  Coomer:    ${sourceStatusLabel(canonicalName, entry, 'coomer', permanentSkips)}`
      )
      console.log(
        `  Pawchive:  ${sourceStatusLabel(canonicalName, entry, 'kemono', permanentSkips)}`
      )
      console.log(
        `  Reddit:    ${sourceStatusLabel(canonicalName, entry, 'reddit', permanentSkips)}`
      )
      console.log(
        `  StufferDB: ${sourceStatusLabel(canonicalName, entry, 'stufferdb', permanentSkips)}`
      )

      // ── Auto-probe coomer + kemono ─────────────────────────────────────────
      const autoHits = { coomer: [], kemono: [], reddit: [] }

      let savedCoomer =
        hasSource(entry, 'coomer') ||
        isPermanentlySkipped(permanentSkips, canonicalName, 'coomer')
      let savedKemono =
        hasSource(entry, 'kemono') ||
        isPermanentlySkipped(permanentSkips, canonicalName, 'kemono')
      let savedReddit =
        hasSource(entry, 'reddit') ||
        isPermanentlySkipped(permanentSkips, canonicalName, 'reddit')
      let savedStufferdb =
        hasSource(entry, 'stufferdb') ||
        isPermanentlySkipped(permanentSkips, canonicalName, 'stufferdb')
      const markPlatformHandled = (platform) => {
        if (platform === 'coomer') savedCoomer = true
        if (platform === 'kemono') savedKemono = true
        if (platform === 'reddit') savedReddit = true
        if (platform === 'stufferdb') savedStufferdb = true
      }

      // ── Accept/reject auto hits ────────────────────────────────────────────
      let skipAutoHits = false
      for (const platform of ['coomer', 'kemono', 'reddit']) {
        if (skipAutoHits) break
        if (hasSource(entry, platform)) continue
        for (const hit of autoHits[platform]) {
          if (skipAutoHits) break
          const isExact = isModelUsernameMatch(canonicalName, entry, hit)

          if (AUTO) {
            if (isExact) {
              resolveAndTrackModel(
                registryPath,
                canonicalName,
                platform,
                hit.url
              )
              console.log(`\n  💾 [auto] ${hit.url}  (${hit.service})`)
              if (platform === 'coomer') savedCoomer = true
              if (platform === 'kemono') savedKemono = true
              if (platform === 'reddit') savedReddit = true
            }
            // non-exact hits silently skipped in auto mode
            if (
              (platform === 'coomer' && savedCoomer) ||
              (platform === 'kemono' && savedKemono) ||
              (platform === 'reddit' && savedReddit)
            )
              break
            continue
          }

          console.log(
            `\n  ✅ [${hit.platform}] ${hit.url}  (${hit.service}, id="${hit.name}")`
          )
          openInBrowser(hit.url)
          while (true) {
            const ans = (
              await ask(
                rl,
                '  Accept? [y=yes / s=skip / p=permanent skip source / m=manual / q=quit / <url>=paste URL]: '
              )
            ).trim()
            const ansLower = ans.toLowerCase()
            if (ansLower === 'y') {
              resolveAndTrackModel(
                registryPath,
                canonicalName,
                platform,
                hit.url
              )
              console.log(`  💾 Saved.`)
              if (platform === 'coomer') savedCoomer = true
              if (platform === 'kemono') savedKemono = true
              if (platform === 'reddit') savedReddit = true
              break
            } else if (ansLower === 's') {
              console.log(`  ⏭️  Skipped.`)
              break
            } else if (ansLower === 'p') {
              const skipped = await promptPermanentSkip(
                rl,
                permanentSkips,
                canonicalName,
                platform
              )
              skipped.forEach(markPlatformHandled)
              break
            } else if (ansLower === 'm') {
              console.log(`  ↩️  Jumping to manual entry.`)
              skipAutoHits = true
              break
            } else if (ansLower === 'q') {
              console.log('\n  💾 Quitting. Progress saved as you went.')
              rl.close()
              return
            } else {
              // Check if it's a URL paste
              const parsed = parseSourceUrl(ans)
              if (!parsed) {
                console.log(
                  '  ❓ Unrecognized. Use y/s/p/m/q or paste a coomer/pawchive/reddit/stufferdb URL.'
                )
                continue
              }
              const urlPlatform = parsed.platform
              if (
                (urlPlatform === 'coomer' && savedCoomer) ||
                (urlPlatform === 'kemono' && savedKemono) ||
                (urlPlatform === 'reddit' && savedReddit) ||
                (urlPlatform === 'stufferdb' && savedStufferdb)
              ) {
                console.log(`  ${urlPlatform} already saved for this model.`)
                continue
              }
              process.stdout.write(`  Validating ${parsed.url} ...`)
              try {
                let found = false
                if (urlPlatform === 'stufferdb') {
                  found = true // no API to validate
                  process.stdout.write(' (no validation)\n')
                } else if (urlPlatform === 'reddit') {
                  const creator = await lookupReddit(parsed.username)
                  found = !!creator
                  process.stdout.write(
                    found ? ` found\n` : ' not found (404)\n'
                  )
                } else if (urlPlatform === 'kemono') {
                  const creator = await lookupKemono(
                    parsed.service,
                    parsed.username
                  )
                  found = !!creator
                  if (creator?.id) {
                    parsed.username = String(creator.id)
                    parsed.url = PLATFORMS.kemono.userUrl(
                      parsed.service,
                      parsed.username
                    )
                  }
                  process.stdout.write(
                    found ? ` found\n` : ' not found (404)\n'
                  )
                } else {
                  const { status } = await httpsGet(
                    'coomerfans.com',
                    parsed.url
                  )
                  found = status === 200
                  process.stdout.write(found ? ' found\n' : ` HTTP ${status}\n`)
                }
                if (found) {
                  openInBrowser(parsed.url)
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
                      urlPlatform,
                      parsed.url
                    )
                    console.log('  💾 Saved.')
                    if (urlPlatform === 'coomer') savedCoomer = true
                    if (urlPlatform === 'kemono') savedKemono = true
                    if (urlPlatform === 'reddit') savedReddit = true
                    if (urlPlatform === 'stufferdb') savedStufferdb = true
                    // If the pasted URL covers the platform currently being reviewed, move on
                    if (urlPlatform === platform) break
                  }
                }
              } catch (err) {
                process.stdout.write(` error: ${err.message}\n`)
              }
            }
          }
          if (skipAutoHits) break
          if (
            (platform === 'coomer' && savedCoomer) ||
            (platform === 'kemono' && savedKemono) ||
            (platform === 'reddit' && savedReddit)
          )
            break
        }
      }

      // ── Manual loop if still missing anything (skipped in --auto mode) ──────
      if (AUTO) continue

      const stillMissing = []
      if (!savedCoomer) stillMissing.push('coomer')
      if (!savedKemono) stillMissing.push('kemono')
      if (!savedReddit) stillMissing.push('reddit')
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
          console.log(`  Opening Pawchive search: ${url}`)
          openInBrowser(url)
        }
        if (!savedReddit) {
          const url = PLATFORMS.reddit.searchUrl(canonicalName)
          console.log(`  Opening Reddit user search: ${url}`)
          openInBrowser(url)
        }

        let currentUrl = null

        while (true) {
          const still = []
          if (!savedCoomer) still.push('coomer')
          if (!savedKemono) still.push('kemono')
          if (!savedReddit) still.push('reddit')
          if (!savedStufferdb) still.push('stufferdb')
          if (still.length === 0) break

          console.log(
            `\n  Still missing: ${still
              .map((platform) =>
                platform === 'stufferdb'
                  ? 'StufferDB'
                  : PLATFORMS[platform].label
              )
              .join(', ')}`
          )
          console.log(`  Commands:
    <url>        paste coomer/pawchive/reddit/stufferdb URL to validate + save
    c <username> probe Coomer for a specific username
    k <username> probe Pawchive for a specific username
    r <username> probe Reddit for a specific username
    o            reopen current URL in browser
    s            skip this model (move to next)
    p <source>   permanently skip source for this model (or p all)
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

          if (lower === 'p' || lower.startsWith('p ')) {
            const requested = lower === 'p' ? '' : raw.slice(2).trim()
            const skipped =
              requested.length > 0
                ? parseSkipPlatforms(requested)
                : await promptPermanentSkip(
                    rl,
                    permanentSkips,
                    canonicalName,
                    null
                  )
            if (requested.length > 0) {
              for (const platform of skipped) {
                markPermanentSkip(permanentSkips, canonicalName, platform)
              }
            }
            skipped.forEach(markPlatformHandled)
            if (skipped.length) {
              console.log(`  Permanently skipped: ${skipped.join(', ')}`)
            }
            continue
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
                const ans = (await ask(rl, '  Accept? [y/s/p/q]: '))
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
                } else if (ans === 'p') {
                  const skipped = await promptPermanentSkip(
                    rl,
                    permanentSkips,
                    canonicalName,
                    'coomer'
                  )
                  skipped.forEach(markPlatformHandled)
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
            process.stdout.write(`  Checking Pawchive "${username}"...`)
            const hits = await probeUsername('kemono', username)
            process.stdout.write(` ${hits.length} hit(s)\n`)
            for (const hit of hits) {
              console.log(
                `\n  ✅ ${hit.url}  (${hit.service}, id="${hit.name}")`
              )
              openInBrowser(hit.url)
              currentUrl = hit.url
              while (true) {
                const ans = (await ask(rl, '  Accept? [y/s/p/q]: '))
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
                } else if (ans === 'p') {
                  const skipped = await promptPermanentSkip(
                    rl,
                    permanentSkips,
                    canonicalName,
                    'kemono'
                  )
                  skipped.forEach(markPlatformHandled)
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
              console.log(`  No Pawchive profiles found for "${username}".`)
            continue
          }

          // URL paste — parse and validate
          if (lower.startsWith('r ')) {
            const username =
              sanitize(raw.slice(2).trim()) || raw.slice(2).trim()
            process.stdout.write(`  Checking Reddit "${username}"...`)
            const hits = await probeUsername('reddit', username).catch(
              (err) => {
                process.stdout.write(` error: ${err.message}\n`)
                return []
              }
            )
            if (hits.length > 0) {
              process.stdout.write(` ${hits.length} hit(s)\n`)
            }
            for (const hit of hits) {
              console.log(`\n  Found: ${hit.url}  (${hit.service})`)
              openInBrowser(hit.url)
              currentUrl = hit.url
              while (true) {
                const ans = (await ask(rl, '  Accept? [y/s/p/q]: '))
                  .trim()
                  .toLowerCase()
                if (ans === 'y') {
                  resolveAndTrackModel(
                    registryPath,
                    canonicalName,
                    'reddit',
                    hit.url
                  )
                  console.log('  Saved.')
                  savedReddit = true
                  break
                } else if (ans === 's') {
                  break
                } else if (ans === 'p') {
                  const skipped = await promptPermanentSkip(
                    rl,
                    permanentSkips,
                    canonicalName,
                    'reddit'
                  )
                  skipped.forEach(markPlatformHandled)
                  break
                } else if (ans === 'q') {
                  console.log('\n  Quitting.')
                  rl.close()
                  return
                }
              }
              if (savedReddit) break
            }
            if (hits.length === 0)
              console.log(`  No Reddit profile found for "${username}".`)
            continue
          }

          const parsed = parseSourceUrl(raw)
          if (!parsed) {
            console.log(
              '  Unrecognized input. Paste a coomer/pawchive/reddit/stufferdb URL, or use c/k/r/p commands.'
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

          // Coomer or Pawchive - validate then save
          const platform = parsed.platform
          if (
            (platform === 'coomer' && savedCoomer) ||
            (platform === 'kemono' && savedKemono) ||
            (platform === 'reddit' && savedReddit)
          ) {
            console.log(`  ${platform} already saved for this model.`)
            continue
          }

          process.stdout.write(`  Validating ${parsed.url} ...`)
          try {
            const host =
              platform === 'coomer'
                ? 'coomerfans.com'
                : platform === 'reddit'
                  ? 'www.reddit.com'
                  : PAWCHIVE_HOST
            let found = false
            if (platform === 'reddit') {
              const creator = await lookupReddit(parsed.username)
              found = !!creator
              if (found)
                process.stdout.write(` found (user="${parsed.username}")\n`)
              else process.stdout.write(' not found (404)\n')
            } else if (platform === 'kemono') {
              const creator = await lookupKemono(
                parsed.service,
                parsed.username
              )
              found = !!creator
              if (creator?.id) {
                parsed.username = String(creator.id)
                parsed.url = PLATFORMS.kemono.userUrl(
                  parsed.service,
                  parsed.username
                )
              }
              if (found)
                process.stdout.write(
                  ` found (id="${creator.name || parsed.username}")\n`
                )
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
                if (platform === 'reddit') savedReddit = true
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

module.exports = { lookupReddit }

if (require.main === module) {
  run().catch((err) => {
    if (/readline was closed/i.test(err.message)) {
      console.log('\n  Input closed. Progress saved as you went.')
      process.exit(0)
    }
    console.error(`\n  ❌ Fatal: ${err.message}`)
    process.exit(1)
  })
}

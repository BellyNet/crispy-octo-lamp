'use strict'

const fs = require('fs')
const path = require('path')
const minimist = require('minimist')

const {
  SIDECAR_FILENAME,
  getRedditTitleFromPermalink,
  isGenericRedditPageTitle,
  resolveBestDateRecord,
} = require('./mediaDates')
const { parseRssEntries } = require('./sourceAdapters/reddit')

const argv = minimist(process.argv.slice(2), {
  boolean: ['apply', 'fetch-missing', 'broken-only'],
  string: ['dataset', 'delay-ms', 'fetch-timeout-ms', 'model', 'rss-max-pages'],
})

const datasetDir =
  argv.dataset ||
  process.env.DATASET_DIR ||
  path.join(process.env.APPDATA || process.cwd(), '.slopvault', 'dataset')
const APPLY = Boolean(argv.apply)
const FETCH_MISSING = Boolean(argv['fetch-missing'])
// --broken-only: fix just the records whose caption is missing or is a
// Reddit chrome-page title ("Reddit - The heart of the internet"), resolving
// real titles from each user's submitted RSS (100 posts per request) instead
// of one lookup per post. Leaves dates and slug/truncated titles alone.
const BROKEN_ONLY = Boolean(argv['broken-only'])
const RSS_MAX_PAGES = Math.max(
  Number.parseInt(String(argv['rss-max-pages'] || ''), 10) || 10,
  1
)
const FETCH_DELAY_MS = Math.max(
  Number.parseInt(String(argv['delay-ms'] || ''), 10) || 650,
  0
)
const FETCH_TIMEOUT_MS = Math.max(
  Number.parseInt(String(argv['fetch-timeout-ms'] || ''), 10) || 15000,
  1000
)
const MODEL_FILTER = new Set(
  String(argv.model || argv.models || '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
)
const redditTitleCache = new Map()
let lastFetchAt = 0

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

function getPermalinkFallbackTitle(source) {
  return getTitleCandidates(source)
    .map(getRedditTitleFromPermalink)
    .find(Boolean)
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function cleanText(value) {
  return String(value || '')
    .replace(/&amp;/g, '&')
    .replace(/&#43;/g, '+')
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) =>
      String.fromCharCode(Number.parseInt(hex, 16))
    )
    .replace(/&#(\d+);/g, (_, code) =>
      String.fromCharCode(Number.parseInt(code, 10))
    )
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/\s+/g, ' ')
    .trim()
}

function extractPostId(source) {
  if (source?.postId) return String(source.postId)
  for (const candidate of getTitleCandidates(source)) {
    const match = String(candidate || '').match(/\/comments\/([^/?#\s]+)/i)
    if (match) return match[1]
  }
  return ''
}

function getTitleFromHtml(html) {
  const raw = String(html || '')
  const attrTitle = raw.match(/\bdata-title=["']([^"']+)["']/i)?.[1]
  const cleanAttrTitle = cleanText(attrTitle)
  if (cleanAttrTitle && !isGenericRedditPageTitle(cleanAttrTitle)) {
    return cleanAttrTitle
  }

  const linkTitle = raw.match(
    /<a\b[^>]*class=["'][^"']*\btitle\b[^"']*["'][^>]*>([\s\S]*?)<\/a>/i
  )?.[1]
  const cleanLinkTitle = cleanText(linkTitle)
  if (cleanLinkTitle && !isGenericRedditPageTitle(cleanLinkTitle)) {
    return cleanLinkTitle
  }

  const ogTitle = raw.match(
    /<meta\b[^>]*(?:property|name)=["']og:title["'][^>]*content=["']([^"']+)["'][^>]*>/i
  )?.[1]
  const cleanOgTitle = cleanText(ogTitle)
  if (cleanOgTitle && !isGenericRedditPageTitle(cleanOgTitle)) {
    return cleanOgTitle
  }

  const rawTitle = raw.match(/<title\b[^>]*>([\s\S]*?)<\/title>/i)?.[1]
  if (!rawTitle) return null
  const cleaned = cleanText(rawTitle)
  if (!cleaned || /^blocked$/i.test(cleaned)) return null
  const title = cleanText(
    cleaned
      .replace(/\s*:\s*reddit(?:\.com)?\s*$/i, '')
      .replace(/\s+:\s+[^:]+$/, '')
      .replace(/^\s*u\/[^:]+:\s*/i, '')
  )
  return title && !isGenericRedditPageTitle(title) ? title : null
}

function looksLikeTruncatedRedditTitle(title) {
  if (!title) return false
  const value = String(title).trim()
  return value.length >= 60 && /(\.\.|…)$/.test(value)
}

function looksLikePermalinkFallbackTitle(source, title) {
  const fallback = getPermalinkFallbackTitle(source)
  return Boolean(title && fallback && cleanText(title) === fallback)
}

async function waitForFetchSlot() {
  if (FETCH_DELAY_MS <= 0) return
  const waitMs = Math.max(lastFetchAt + FETCH_DELAY_MS - Date.now(), 0)
  if (waitMs > 0) await sleep(waitMs)
  lastFetchAt = Date.now()
}

// Reddit's /api/info.json returns full post objects (untruncated title,
// created_utc) for up to 100 ids per request — one call instead of 100
// HTML page fetches, and JSON isn't served the login/verification wall
// that HTML post pages increasingly are. Results land in redditInfoCache;
// ids it can't resolve fall through to the per-post HTML path.
const redditInfoCache = new Map() // postId → { title, createdUtc } | null
const REDDIT_INFO_BATCH = 100

async function prefetchRedditInfo(postIds) {
  const pending = [...new Set(postIds.filter(Boolean))].filter(
    (id) => !redditInfoCache.has(id)
  )
  for (let i = 0; i < pending.length; i += REDDIT_INFO_BATCH) {
    const batch = pending.slice(i, i + REDDIT_INFO_BATCH)
    await waitForFetchSlot()
    try {
      const response = await fetch(
        `https://www.reddit.com/api/info.json?raw_json=1&id=${batch
          .map((id) => `t3_${encodeURIComponent(id)}`)
          .join(',')}`,
        {
          headers: {
            Accept: 'application/json',
            'User-Agent':
              'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
          },
          signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
        }
      )
      if (!response.ok) {
        console.log(
          `  api/info batch ${i / REDDIT_INFO_BATCH + 1}: HTTP ${response.status} — falling back to per-post HTML`
        )
        continue
      }
      const body = await response.json()
      for (const child of body?.data?.children || []) {
        const post = child?.data
        if (!post?.id) continue
        const title = cleanText(post.title)
        const createdUtc = Number(post.created_utc)
        redditInfoCache.set(String(post.id), {
          title: title && !isGenericRedditPageTitle(title) ? title : null,
          createdUtc:
            Number.isFinite(createdUtc) && createdUtc > 0 ? createdUtc : null,
        })
      }
    } catch (err) {
      console.log(
        `  api/info batch ${i / REDDIT_INFO_BATCH + 1} failed: ${err.message} — falling back to per-post HTML`
      )
    }
  }
}

async function fetchRedditTitle(postId) {
  if (!postId) return null
  const info = redditInfoCache.get(postId)
  if (info?.title) return info.title
  if (redditTitleCache.has(postId)) return redditTitleCache.get(postId)

  let title = null
  for (let attempt = 0; attempt < 3; attempt += 1) {
    await waitForFetchSlot()
    let response
    let html
    try {
      response = await fetch(
        `https://old.reddit.com/comments/${encodeURIComponent(postId)}/?over18=1`,
        {
          headers: {
            Accept: 'text/html,application/xhtml+xml',
            'Accept-Language': 'en-US,en;q=0.9',
            Cookie: 'over18=1;',
            Referer: 'https://old.reddit.com/',
            'User-Agent':
              'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
          },
          redirect: 'follow',
          signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
        }
      )
      html = await response.text()
    } catch (err) {
      if (attempt === 2) break
      await sleep(30000 * (attempt + 1))
      continue
    }

    if (response.ok) {
      title =
        getTitleFromHtml(html) || getRedditTitleFromPermalink(response.url)
      break
    }

    if (![429, 500, 502, 503, 504].includes(response.status) || attempt === 2) {
      break
    }
    const retryAfterSeconds = Number.parseInt(
      response.headers.get('retry-after') || '',
      10
    )
    await sleep(
      Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0
        ? retryAfterSeconds * 1000
        : 30000 * (attempt + 1)
    )
  }

  redditTitleCache.set(postId, title)
  return title
}

function needsTitleFetch(source, title) {
  return (
    !title ||
    isGenericRedditPageTitle(title) ||
    looksLikeTruncatedRedditTitle(title) ||
    looksLikePermalinkFallbackTitle(source, title)
  )
}

async function repairSidecar(modelName, sidecar) {
  let scanned = 0
  let repaired = 0
  let fetched = 0
  let datesFixed = 0
  const examples = []

  if (FETCH_MISSING) {
    // Batch-resolve every Reddit post in this sidecar up front: titles
    // that need repair, plus created_utc for the posted-date check below.
    const ids = []
    for (const [relativePath, record] of Object.entries(sidecar.data)) {
      if (relativePath.startsWith('__')) continue
      const source = record?.source
      if (String(source?.site || '').toLowerCase() !== 'reddit') continue
      ids.push(extractPostId(source))
    }
    await prefetchRedditInfo(ids)
  }

  for (const [relativePath, record] of Object.entries(sidecar.data)) {
    if (relativePath.startsWith('__')) continue
    const source = record?.source
    if (!source || typeof source !== 'object') continue
    if (String(source.site || '').toLowerCase() !== 'reddit') continue
    scanned += 1
    const existingTitle = [source.title, source.text, source.sourceText]
      .map((value) => String(value || '').trim())
      .find(Boolean)
    let title = existingTitle || getPermalinkFallbackTitle(source)
    let fetchedTitle = null

    // A stored chrome-page title is never usable, not even as a fallback.
    if (isGenericRedditPageTitle(title)) {
      title = getPermalinkFallbackTitle(source) || null
    }

    let changed = false
    // Posted date: Reddit's created_utc is authoritative. Older scrapes
    // (RSS <updated>, remux timestamps) can carry a later date.
    const info = FETCH_MISSING
      ? redditInfoCache.get(extractPostId(source))
      : null
    if (info?.createdUtc) {
      const createdIso = new Date(info.createdUtc * 1000).toISOString()
      if (record.uploaded !== createdIso) {
        record.uploaded = createdIso
        record.resolved = resolveBestDateRecord(record)
        changed = true
        datesFixed += 1
      }
    }

    if (FETCH_MISSING && needsTitleFetch(source, title)) {
      const postId = extractPostId(source)
      const beforeSize = redditTitleCache.size
      fetchedTitle = await fetchRedditTitle(postId)
      title = fetchedTitle || title
      if (redditTitleCache.size > beforeSize) fetched += 1
      if (fetched > 0 && fetched % 25 === 0) {
        console.log(
          `  fetched ${fetched} unique Reddit post title lookup(s)...`
        )
      }
    }
    if (!title) {
      if (changed) repaired += 1
      continue
    }

    const shouldReplaceTitle =
      !String(source.title || '').trim() ||
      isGenericRedditPageTitle(source.title) ||
      (fetchedTitle &&
        (looksLikeTruncatedRedditTitle(source.title) ||
          looksLikePermalinkFallbackTitle(source, source.title)))
    if (shouldReplaceTitle && cleanText(source.title) !== title) {
      source.title = title
      changed = true
    }
    const shouldReplaceText =
      !String(source.text || '').trim() ||
      isGenericRedditPageTitle(source.text) ||
      (fetchedTitle &&
        (looksLikeTruncatedRedditTitle(source.text) ||
          looksLikePermalinkFallbackTitle(source, source.text)))
    if (shouldReplaceText && cleanText(source.text) !== title) {
      source.text = title
      changed = true
    }
    if (!changed) continue

    repaired += 1
    if (examples.length < 5) {
      examples.push({ modelName, relativePath, title })
    }
  }

  return { scanned, repaired, fetched, datesFixed, examples }
}

// ─── --broken-only: titles from each user's submitted RSS ─────────────────────
// Reddit serves post HTML a "Welcome to Reddit" login wall and 403s the JSON
// APIs for unauthenticated clients, but the per-user submitted RSS still
// carries real titles, 100 posts per request. Its rate limit is tight
// (often one request per window), so pacing follows x-ratelimit-* headers.
const REDDIT_RSS_USER_AGENT =
  'Mozilla/5.0 (compatible; LoRATraining/1.0; +https://localhost)'
const REDDIT_RSS_PAGE_SIZE = 100
const userFeedCache = new Map() // lowercased username → feed state
let rssNextRequestAt = 0

async function fetchRedditRss(url) {
  for (let attempt = 0; attempt < 6; attempt += 1) {
    const waitMs = Math.max(rssNextRequestAt - Date.now(), 0)
    if (waitMs > 0) await sleep(waitMs)
    let response
    try {
      response = await fetch(url, {
        headers: {
          Accept: 'application/atom+xml,text/xml,application/xml',
          'User-Agent': REDDIT_RSS_USER_AGENT,
        },
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
      })
    } catch (err) {
      console.log(`    RSS fetch failed (${err.message}); retrying in 30s`)
      rssNextRequestAt = Date.now() + 30000
      continue
    }
    const remaining = Number.parseFloat(
      response.headers.get('x-ratelimit-remaining') || ''
    )
    const resetSeconds = Number.parseInt(
      response.headers.get('x-ratelimit-reset') || '',
      10
    )
    const resetMs =
      ((Number.isFinite(resetSeconds) && resetSeconds > 0 ? resetSeconds : 60) +
        1) *
      1000
    const exhausted =
      response.status === 429 || (Number.isFinite(remaining) && remaining < 1)
    rssNextRequestAt = Date.now() + (exhausted ? resetMs : FETCH_DELAY_MS)
    if (response.status === 429) {
      console.log(`    rate-limited; waiting ${Math.round(resetMs / 1000)}s`)
      continue
    }
    return {
      status: response.status,
      xml: response.ok ? await response.text() : '',
    }
  }
  return { status: 429, xml: '' }
}

// Pages through u/<username>/submitted/.rss until every needed post id is
// seen and the feed reaches back past the oldest needed timestamp, or the
// feed ends, or RSS_MAX_PAGES is hit. Reddit listings stop at ~1000 posts.
async function loadUserFeed(username, neededIds, neededTimes) {
  const key = username.toLowerCase()
  let feed = userFeedCache.get(key)
  if (!feed) {
    feed = {
      byId: new Map(), // post id → title | null
      byTime: new Map(), // created second → [post id]
      oldest: Infinity,
      after: null,
      pages: 0,
      done: false,
      status: null,
    }
    userFeedCache.set(key, feed)
  }
  const minTime = neededTimes.length ? Math.min(...neededTimes) : Infinity
  const isSatisfied = () =>
    neededIds.every((id) => feed.byId.has(id)) &&
    (minTime === Infinity || feed.oldest <= minTime)

  while (!feed.done && feed.pages < RSS_MAX_PAGES && !isSatisfied()) {
    const url = new URL(
      `https://www.reddit.com/user/${encodeURIComponent(username)}/submitted/.rss`
    )
    url.searchParams.set('limit', String(REDDIT_RSS_PAGE_SIZE))
    if (feed.after) url.searchParams.set('after', feed.after)
    const { status, xml } = await fetchRedditRss(url.toString())
    feed.pages += 1
    feed.status = status
    const entries = parseRssEntries(xml)
    const fresh = entries.filter((entry) => !feed.byId.has(entry.id))
    if (fresh.length === 0) {
      feed.done = true
      break
    }
    for (const entry of fresh) {
      const title = cleanText(entry.title)
      feed.byId.set(
        entry.id,
        title && !isGenericRedditPageTitle(title) ? title : null
      )
      if (Number.isFinite(entry.created_utc)) {
        const second = Math.round(entry.created_utc)
        feed.byTime.set(second, [...(feed.byTime.get(second) || []), entry.id])
        feed.oldest = Math.min(feed.oldest, second)
      }
    }
    feed.after = `t3_${entries[entries.length - 1].id}`
    // A short page means the listing ended; skip the empty-page round trip.
    if (entries.length < REDDIT_RSS_PAGE_SIZE * 0.9) feed.done = true
  }
  return feed
}

function getExistingTitle(source) {
  return [source.title, source.text, source.sourceText]
    .map((value) => String(value || '').trim())
    .find(Boolean)
}

function isBrokenCaption(value) {
  return !String(value || '').trim() || isGenericRedditPageTitle(value)
}

async function repairBrokenSidecar(modelName, sidecar) {
  let scanned = 0
  const broken = []
  for (const [relativePath, record] of Object.entries(sidecar.data)) {
    if (relativePath.startsWith('__')) continue
    const source = record?.source
    if (!source || typeof source !== 'object') continue
    if (String(source.site || '').toLowerCase() !== 'reddit') continue
    scanned += 1
    if (!isBrokenCaption(getExistingTitle(source))) continue
    broken.push({ relativePath, record, source })
  }

  const via = { rss: 0, rssTime: 0, slug: 0, unresolved: 0 }
  const result = { scanned, repaired: 0, fetched: 0, datesFixed: 0, via }
  result.examples = []
  if (broken.length === 0) return result

  // Records with a post id match by id; the rest (hash-named files that
  // only kept a username + upload time) match only when their upload time
  // equals exactly one feed post's creation second.
  const needsByUser = new Map()
  for (const item of broken) {
    const username = item.source.username || item.source.userId
    if (!username || !FETCH_MISSING) continue
    const needs = needsByUser.get(username) || { ids: [], times: [] }
    const postId = extractPostId(item.source)
    const uploadedMs = Date.parse(item.record.uploaded || '')
    if (postId) needs.ids.push(postId)
    else if (Number.isFinite(uploadedMs)) {
      needs.times.push(Math.round(uploadedMs / 1000))
    }
    needsByUser.set(username, needs)
  }
  for (const [username, needs] of needsByUser) {
    const feed = await loadUserFeed(username, needs.ids, needs.times)
    const found = needs.ids.filter((id) => feed.byId.get(id)).length
    console.log(
      `  ${modelName} (u/${username}): ${found}/${needs.ids.length} post id(s) found in ${feed.pages} RSS page(s)` +
        (needs.times.length
          ? `, ${needs.times.length} id-less record(s)`
          : '') +
        (feed.status && feed.status !== 200 ? ` [HTTP ${feed.status}]` : '')
    )
  }

  for (const { relativePath, record, source } of broken) {
    const username = source.username || source.userId
    const feed = username ? userFeedCache.get(username.toLowerCase()) : null
    const postId = extractPostId(source)
    let title = null
    let method = null
    if (feed && postId) {
      title = feed.byId.get(postId) || null
      method = 'rss'
    } else if (feed) {
      const uploadedMs = Date.parse(record.uploaded || '')
      const ids = Number.isFinite(uploadedMs)
        ? feed.byTime.get(Math.round(uploadedMs / 1000))
        : null
      if (ids?.length === 1) title = feed.byId.get(ids[0]) || null
      method = 'rssTime'
    }
    if (!title) {
      title = getPermalinkFallbackTitle(source) || null
      method = 'slug'
    }
    if (!title) {
      via.unresolved += 1
      continue
    }

    let changed = false
    if (isBrokenCaption(source.title) && cleanText(source.title) !== title) {
      source.title = title
      changed = true
    }
    if (isBrokenCaption(source.text) && cleanText(source.text) !== title) {
      source.text = title
      changed = true
    }
    if (!changed) continue
    via[method] += 1
    result.repaired += 1
    if (result.examples.length < 5) {
      result.examples.push({
        modelName,
        relativePath,
        title: `[${method}] ${title}`,
      })
    }
  }
  return result
}

async function run() {
  if (!fs.existsSync(datasetDir)) {
    throw new Error(`Dataset directory not found: ${datasetDir}`)
  }

  const totals = {
    models: 0,
    scanned: 0,
    repaired: 0,
    fetched: 0,
    datesFixed: 0,
    via: { rss: 0, rssTime: 0, slug: 0, unresolved: 0 },
    examples: [],
  }

  for (const dirent of fs.readdirSync(datasetDir, { withFileTypes: true })) {
    if (!dirent.isDirectory() || !shouldProcessModel(dirent.name)) continue
    const modelDir = path.join(datasetDir, dirent.name)
    const sidecar = loadSidecar(modelDir)
    if (!sidecar) continue

    const result = BROKEN_ONLY
      ? await repairBrokenSidecar(dirent.name, sidecar)
      : await repairSidecar(dirent.name, sidecar)
    if (result.scanned === 0) continue
    totals.models += 1
    totals.scanned += result.scanned
    totals.repaired += result.repaired
    totals.fetched += result.fetched
    totals.datesFixed += result.datesFixed
    totals.examples.push(...result.examples)
    for (const [method, count] of Object.entries(result.via || {})) {
      totals.via[method] += count
    }

    if (APPLY && result.repaired > 0) {
      const tmp = `${sidecar.path}.tmp-reddit-title-repair`
      fs.writeFileSync(tmp, `${JSON.stringify(sidecar.data, null, 2)}\n`)
      fs.renameSync(tmp, sidecar.path)
    }
  }

  console.log(
    `${APPLY ? 'Repaired' : 'Would repair'} ${totals.repaired} Reddit title/text/date record(s) across ${totals.models} model(s); scanned ${totals.scanned} Reddit metadata record(s).`
  )
  if (BROKEN_ONLY) {
    const { rss, rssTime, slug, unresolved } = totals.via
    console.log(
      `Broken captions: ${rss} from RSS by post id, ${rssTime} from RSS by exact upload time, ${slug} from permalink slug, ${unresolved} left blank (no post id/slug match).`
    )
  } else if (FETCH_MISSING) {
    console.log(
      `Resolved ${redditInfoCache.size} Reddit post(s) via api/info; fetched ${totals.fetched} post page(s) via HTML fallback.`
    )
    console.log(
      `${APPLY ? 'Corrected' : 'Would correct'} ${totals.datesFixed} Reddit posted date(s) from created_utc.`
    )
  }
  if (!APPLY) console.log('Dry run only. Re-run with --apply to write changes.')
  for (const example of totals.examples.slice(0, 10)) {
    console.log(
      `  ${example.modelName}/${example.relativePath}: ${example.title}`
    )
  }
}

run().catch((err) => {
  console.error(`Fatal: ${err.message}`)
  process.exit(1)
})

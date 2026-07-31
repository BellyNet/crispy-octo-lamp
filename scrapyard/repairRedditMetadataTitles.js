'use strict'

const fs = require('fs')
const path = require('path')
const minimist = require('minimist')

const {
  SIDECAR_FILENAME,
  getRedditTitleFromPermalink,
} = require('./mediaDates')

const argv = minimist(process.argv.slice(2), {
  boolean: ['apply', 'fetch-missing'],
  string: ['dataset', 'delay-ms', 'fetch-timeout-ms', 'model'],
})

const datasetDir =
  argv.dataset ||
  process.env.DATASET_DIR ||
  path.join(process.env.APPDATA || process.cwd(), '.slopvault', 'dataset')
const APPLY = Boolean(argv.apply)
const FETCH_MISSING = Boolean(argv['fetch-missing'])
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

function isGenericRedditPageTitle(title) {
  return /^(?:welcome to reddit|reddit - dive into anything)$/i.test(
    String(title || '').trim()
  )
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

async function fetchRedditTitle(postId) {
  if (!postId) return null
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

async function repairSidecar(modelName, sidecar) {
  let scanned = 0
  let repaired = 0
  let fetched = 0
  const examples = []

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

    if (
      FETCH_MISSING &&
      (!title ||
        isGenericRedditPageTitle(title) ||
        looksLikeTruncatedRedditTitle(title) ||
        looksLikePermalinkFallbackTitle(source, title))
    ) {
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
    if (!title) continue

    let changed = false
    const shouldReplaceTitle =
      !String(source.title || '').trim() ||
      (fetchedTitle &&
        (isGenericRedditPageTitle(source.title) ||
          looksLikeTruncatedRedditTitle(source.title) ||
          looksLikePermalinkFallbackTitle(source, source.title)))
    if (shouldReplaceTitle && cleanText(source.title) !== title) {
      source.title = title
      changed = true
    }
    const shouldReplaceText =
      !String(source.text || '').trim() ||
      (fetchedTitle &&
        (isGenericRedditPageTitle(source.text) ||
          looksLikeTruncatedRedditTitle(source.text) ||
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

  return { scanned, repaired, fetched, examples }
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
    examples: [],
  }

  for (const dirent of fs.readdirSync(datasetDir, { withFileTypes: true })) {
    if (!dirent.isDirectory() || !shouldProcessModel(dirent.name)) continue
    const modelDir = path.join(datasetDir, dirent.name)
    const sidecar = loadSidecar(modelDir)
    if (!sidecar) continue

    const result = await repairSidecar(dirent.name, sidecar)
    if (result.scanned === 0) continue
    totals.models += 1
    totals.scanned += result.scanned
    totals.repaired += result.repaired
    totals.fetched += result.fetched
    totals.examples.push(...result.examples)

    if (APPLY && result.repaired > 0) {
      fs.writeFileSync(sidecar.path, JSON.stringify(sidecar.data, null, 2))
      fs.appendFileSync(sidecar.path, '\n')
    }
  }

  console.log(
    `${APPLY ? 'Repaired' : 'Would repair'} ${totals.repaired} missing Reddit title/text record(s) across ${totals.models} model(s); scanned ${totals.scanned} Reddit metadata record(s).`
  )
  if (FETCH_MISSING) {
    console.log(
      `Fetched ${totals.fetched} unique Reddit post page(s) for title lookup.`
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

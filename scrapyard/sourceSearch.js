'use strict'

// Finds candidate sources for a creator name (or recognizes a pasted URL) for
// the dashboard's Sources page. Ported from the old PC scrape dashboard.
//
// For a name it:
//   - widens the search with aliases/usernames of matching registry models,
//   - probes Pawchive, Coomer/CoomerFans and Tumblr for matching creators,
//   - looks the name up on StufferDB (site search, then DuckDuckGo),
//   - suggests the Reddit profile URL (unverified; Reddit blocks probing),
//   - and adds manual search links for every site.

const config = require('./config')
const {
  loadModelRegistry,
  sanitize,
  findCanonicalModelNameBySource,
} = require('./modelRegistry')
const { parseSourceUrl, findSourceForParsed } = require('./sourceRouter')
const { SOURCES } = require('./sources')

const MAX_TERMS = 6

function normalizeUsernameSearchInput(value) {
  return sanitize(
    String(value || '')
      .trim()
      .replace(/^@+/, '')
      .replace(/^u\//i, '')
      .replace(/^user\//i, '')
  )
}

function normalizeLooseSearch(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')
}

function editDistanceWithinOne(left, right) {
  if (left === right) return true
  if (Math.abs(left.length - right.length) > 1) return false
  let i = 0
  let j = 0
  let edits = 0
  while (i < left.length && j < right.length) {
    if (left[i] === right[j]) {
      i += 1
      j += 1
      continue
    }
    edits += 1
    if (edits > 1) return false
    if (left.length > right.length) i += 1
    else if (right.length > left.length) j += 1
    else {
      i += 1
      j += 1
    }
  }
  return edits + (left.length - i) + (right.length - j) <= 1
}

// The query plus names already known for matching models.
function collectSearchTerms(query, registry) {
  const username = normalizeUsernameSearchInput(query)
  if (!username) return []
  const loose = normalizeLooseSearch(username)
  const terms = new Set([username])
  for (const [modelName, entry] of Object.entries(registry)) {
    const names = [modelName, ...(entry?.aliases || [])]
    const matched = names.some((name) => {
      const candidate = normalizeLooseSearch(name)
      return (
        candidate === loose ||
        (candidate.length > 3 &&
          loose.length > 3 &&
          (candidate.includes(loose) || loose.includes(candidate))) ||
        editDistanceWithinOne(candidate, loose)
      )
    })
    if (!matched) continue
    for (const name of names) {
      const term = normalizeUsernameSearchInput(name)
      if (term) terms.add(term)
    }
    for (const list of Object.values(entry?.sources || {})) {
      for (const source of Array.isArray(list) ? list : []) {
        for (const value of [source?.username, source?.discoveredAs]) {
          const term = normalizeUsernameSearchInput(value)
          if (term && !/^\d+$/.test(term)) terms.add(term)
        }
      }
    }
  }
  // Number-only names are platform IDs, not usernames to search for.
  return [...terms]
    .filter((term, index) => index === 0 || !/^\d+$/.test(term))
    .slice(0, MAX_TERMS)
}

function findSourceOwner(registry, parsed) {
  if (!parsed) return null
  return findCanonicalModelNameBySource(registry, {
    site: parsed.site || parsed.sourceType,
    service: parsed.service,
    userId: parsed.userId,
    username: parsed.username,
    inputUrl: parsed.inputUrl || parsed.url,
    url: parsed.url,
  })
}

function candidateFor(registry, url, extra = {}) {
  const parsed = parseSourceUrl(url)
  if (!parsed) return null
  const source = findSourceForParsed(parsed)
  return {
    type: 'source',
    url: parsed.url,
    sourceId: source?.id || parsed.sourceType,
    label: source?.label || parsed.sourceType,
    requiresBrowser: Boolean(source?.requiresBrowser),
    name: parsed.username || parsed.rawName || extra.name || null,
    owner: findSourceOwner(registry, parsed),
    verified: true,
    ...extra,
  }
}

// ── StufferDB lookup ─────────────────────────────────────────────────────────

function decodeHtmlEntities(value) {
  return String(value || '')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#x([0-9a-f]+);/gi, (_m, hex) =>
      String.fromCodePoint(Number.parseInt(hex, 16))
    )
    .replace(/&#(\d+);/g, (_m, code) =>
      String.fromCodePoint(Number.parseInt(code, 10))
    )
}

function stripHtml(value) {
  return decodeHtmlEntities(String(value || '').replace(/<[^>]*>/g, ' '))
    .replace(/\s+/g, ' ')
    .trim()
}

function normalizeStufferDbCategoryUrl(href, baseUrl) {
  const decoded = decodeHtmlEntities(href).trim()
  if (!decoded) return null
  try {
    const redirect = new URL(decoded, baseUrl)
    const target =
      redirect.searchParams.get('uddg') || redirect.searchParams.get('u')
    if (target) return normalizeStufferDbCategoryUrl(target, baseUrl)
  } catch {}
  try {
    const parsed = new URL(decoded, baseUrl)
    if (!parsed.hostname.toLowerCase().includes('stufferdb')) return null
    const match = parsed
      .toString()
      .match(/\/index(?:\.php)?\?\/category\/(\d+)/i)
    return match ? `https://stufferdb.com/index?/category/${match[1]}` : null
  } catch {
    return null
  }
}

function extractStufferDbCategories(html, term, baseUrl) {
  const looseTerm = normalizeLooseSearch(term)
  const rows = []
  const seen = new Set()
  for (const match of html.matchAll(
    /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi
  )) {
    const url = normalizeStufferDbCategoryUrl(match[1], baseUrl)
    if (!url || seen.has(url)) continue
    seen.add(url)
    const title = stripHtml(match[2])
    const looseTitle = normalizeLooseSearch(title)
    const nearby = stripHtml(
      html.slice(match.index || 0, (match.index || 0) + 500)
    )
    const count = nearby.match(/\[(\d+)\]/)
    rows.push({
      url,
      title,
      exact:
        Boolean(looseTitle) &&
        (looseTitle === looseTerm || looseTitle.includes(looseTerm)),
      mediaCount: count ? Number.parseInt(count[1], 10) : null,
    })
  }
  return rows.sort((a, b) => (a.exact === b.exact ? 0 : a.exact ? -1 : 1))
}

async function fetchText(url) {
  const response = await fetch(url, {
    redirect: 'follow',
    signal: AbortSignal.timeout(15000),
    headers: {
      accept: 'text/html,application/xhtml+xml',
      'user-agent':
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    },
  })
  if (!response.ok) throw new Error(`HTTP ${response.status}`)
  return { text: await response.text(), finalUrl: response.url }
}

async function findStufferDbCategory(term) {
  const attempts = [
    `https://stufferdb.com/search.php?q=${encodeURIComponent(term)}`,
    `https://html.duckduckgo.com/html/?q=${encodeURIComponent(`site:stufferdb.com ${term}`)}`,
  ]
  for (const url of attempts) {
    try {
      const page = await fetchText(url)
      const rows = extractStufferDbCategories(
        page.text,
        term,
        page.finalUrl || url
      )
      // Only categories titled like the name; the first result alone is
      // often an unrelated tag.
      const best = rows.find((row) => row.exact)
      if (best) return best
    } catch {}
  }
  return null
}

// ── Search ───────────────────────────────────────────────────────────────────

async function searchSources(
  rawQuery,
  { registryPath = config.registryPath, probeUsername } = {}
) {
  const query = String(rawQuery || '').trim()
  const registry = loadModelRegistry(registryPath)
  if (!query) return { terms: [], candidates: [], searchLinks: [] }

  // A pasted URL is the answer itself.
  const direct = candidateFor(registry, query, { origin: 'url' })
  if (direct) return { terms: [], candidates: [direct], searchLinks: [] }

  const terms = collectSearchTerms(query, registry)
  const candidates = []
  const add = (candidate) => {
    if (
      candidate &&
      !candidates.some((existing) => existing.url === candidate.url)
    ) {
      candidates.push(candidate)
    }
  }

  const probe =
    probeUsername ||
    require('../hoghaul/backfill-sources-interactive').probeUsername
  await Promise.all(
    terms.flatMap((term) =>
      ['kemono', 'coomer', 'tumblr'].map(async (platform) => {
        let hits = []
        try {
          hits = await probe(platform, term)
        } catch {}
        for (const hit of hits || []) {
          add(
            candidateFor(registry, hit.url, {
              origin: 'probe',
              name: hit.name || term,
            })
          )
        }
      })
    )
  )
  for (const term of terms) {
    const category = await findStufferDbCategory(term)
    if (category) {
      add(
        candidateFor(registry, category.url, {
          origin: 'stufferdb-search',
          name: category.title || term,
          mediaCount: category.mediaCount,
        })
      )
    }
  }
  for (const term of terms) {
    add(
      candidateFor(registry, `https://www.reddit.com/user/${term}/submitted/`, {
        origin: 'guess',
        verified: false,
      })
    )
  }

  const searchLinks = SOURCES.filter((source) => source.searchUrl).flatMap(
    (source) =>
      terms.slice(0, 2).map((term) => ({
        label: `${source.label}: "${term}"`,
        url: source.searchUrl(term),
      }))
  )
  return { terms, candidates, searchLinks }
}

module.exports = { searchSources, findSourceOwner, collectSearchTerms }

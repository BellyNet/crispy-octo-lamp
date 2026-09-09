'use strict'

const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const { spawn, spawnSync } = require('child_process')
const express = require('express')

const {
  sanitize,
  loadModelRegistry,
  saveModelRegistry,
  ensureModelEntryShape,
  findCanonicalModelName,
  findCanonicalModelNameBySource,
  upsertStufferdbSource,
  upsertCoomerSource,
  upsertRedditSource,
  upsertSourceInfo,
} = require('../scrapyard/modelRegistry')
const { parseSourceUrl } = require('../scrapyard/sourceRouter')
const {
  readFreshModelRunSummary,
  summarizeSourceRunSummary,
} = require('../scrapyard/scraperRunner')
const {
  collectOversizedVideoTargets,
} = require('../scrapyard/run-scrape-interactive')
const {
  PLATFORMS,
  probeUsername,
} = require('../hoghaul/backfill-sources-interactive')

const rootDir = path.join(__dirname, '..')
const registryPath =
  process.env.MODEL_REGISTRY_PATH || path.join(rootDir, 'model_aliases.json')
const runScrapeScript = path.join(rootDir, 'scrapyard', 'run-scrape.js')
const app = express()

const PORT = Number.parseInt(process.env.SCRAPE_DASHBOARD_PORT, 10) || 3430
const PASSWORD =
  process.env.SCRAPE_DASHBOARD_PASSWORD || process.env.DASHBOARD_PASSWORD || ''
const AUTH_COOKIE = 'scrape_dashboard_auth'
const AUTH_TOKEN = PASSWORD
  ? crypto.createHash('sha256').update(PASSWORD).digest('hex')
  : ''

const APPDATA =
  process.env.APPDATA ||
  path.join(process.env.HOME || process.env.USERPROFILE, 'AppData', 'Roaming')
const datasetDir =
  process.env.DATASET_DIR || path.join(APPDATA, '.slopvault', 'dataset')
const allSourceReportPath = path.join(
  rootDir,
  'tmp',
  'update-all-sources',
  'update-all-sources-latest.json'
)
const quarantineManifestPath = path.join(
  APPDATA,
  '.slopvault',
  'quarantine',
  'quarantine-manifest.json'
)
const historyDir = path.join(__dirname, 'data')
const runHistoryPath = path.join(historyDir, 'run-history.json')

const SOURCE_KEYS = ['reddit', 'kemono', 'coomer', 'stufferdb']
const HISTORY_VERSION = 2
const JOB_LOG_LIMIT = 2500
const jobs = new Map()
const queue = []
const evidenceCache = new Map()
const AUDIT_CACHE_MS = 60_000
let auditCache = null
let activeJob = null
let nextJobId = 1

function parseCookies(req) {
  return Object.fromEntries(
    String(req.headers.cookie || '')
      .split(';')
      .map((part) => part.trim().split('='))
      .filter(([name, value]) => name && value)
  )
}

function requireAuth(req, res, next) {
  if (!PASSWORD) return next()
  if (parseCookies(req)[AUTH_COOKIE] === AUTH_TOKEN) return next()
  if (req.path === '/login.html' || req.path === '/auth') return next()
  if (req.path.startsWith('/api/')) {
    return res.status(401).json({ error: 'Authentication required' })
  }
  res.redirect('/login.html')
}

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
    if (left.length > right.length) {
      i += 1
    } else if (right.length > left.length) {
      j += 1
    } else {
      i += 1
      j += 1
    }
  }
  return edits + (left.length - i) + (right.length - j) <= 1
}

function getPlatformLabel(platform) {
  if (platform === 'kemono') return 'Pawchive'
  if (platform === 'coomer') return 'CoomerFans'
  if (platform === 'reddit') return 'Reddit'
  if (platform === 'stufferdb') return 'StufferDB'
  return platform || 'Unknown'
}

function getStufferDbSearchUrl(username) {
  const query = `site:stufferdb.com ${username}`
  return `https://duckduckgo.com/?q=${encodeURIComponent(query)}`
}

function getStufferDbDirectSearchUrl(username) {
  return `https://stufferdb.com/search.php?q=${encodeURIComponent(username)}`
}

function getManualSourceSearchUrl(platform, username) {
  if (platform === 'stufferdb') return getStufferDbDirectSearchUrl(username)
  return PLATFORMS[platform]?.searchUrl
    ? PLATFORMS[platform].searchUrl(username)
    : ''
}

function decodeHtmlEntities(value) {
  return String(value || '')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#x([0-9a-f]+);/gi, (_match, hex) =>
      String.fromCodePoint(Number.parseInt(hex, 16))
    )
    .replace(/&#(\d+);/g, (_match, code) =>
      String.fromCodePoint(Number.parseInt(code, 10))
    )
}

function normalizeStufferDbCategoryUrl(href, baseUrl) {
  const decoded = decodeHtmlEntities(href).trim()
  if (!decoded) return null

  try {
    const maybeRedirect = new URL(decoded, baseUrl)
    const duckDuckGoTarget =
      maybeRedirect.searchParams.get('uddg') ||
      maybeRedirect.searchParams.get('u')
    if (duckDuckGoTarget) {
      return normalizeStufferDbCategoryUrl(duckDuckGoTarget, baseUrl)
    }
  } catch {
    // Fall through to normal URL parsing below.
  }

  try {
    const parsed = new URL(decoded, baseUrl)
    if (!parsed.hostname.toLowerCase().includes('stufferdb')) return null
    const categoryMatch = parsed
      .toString()
      .match(/\/index(?:\.php)?\?\/category\/(\d+)/i)
    if (!categoryMatch) return null
    return `https://stufferdb.com/index?/category/${categoryMatch[1]}`
  } catch {
    return null
  }
}

function stripHtml(value) {
  return decodeHtmlEntities(String(value || '').replace(/<[^>]*>/g, ' '))
    .replace(/\s+/g, ' ')
    .trim()
}

function extractStufferDbCategoryCandidates(html, searchTerm, baseUrl) {
  const looseTerm = normalizeLooseSearch(searchTerm)
  const rows = []
  const anchorRe = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi

  for (const match of html.matchAll(anchorRe)) {
    const url = normalizeStufferDbCategoryUrl(match[1], baseUrl)
    if (!url) continue
    const title = stripHtml(match[2])
    const looseTitle = normalizeLooseSearch(title)
    const exact =
      Boolean(looseTitle) &&
      (looseTitle === looseTerm || looseTitle.includes(looseTerm))
    const nearby = stripHtml(
      html.slice(match.index || 0, (match.index || 0) + 500)
    )
    const countMatch = nearby.match(/\[(\d+)\]/)
    rows.push({
      url,
      title,
      exact,
      mediaCount: countMatch ? Number.parseInt(countMatch[1], 10) : null,
    })
  }

  const seen = new Set()
  return rows
    .filter((row) => {
      if (seen.has(row.url)) return false
      seen.add(row.url)
      return true
    })
    .sort((left, right) => {
      if (left.exact !== right.exact) return left.exact ? -1 : 1
      return 0
    })
}

async function fetchText(url) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 15_000)
  try {
    const response = await fetch(url, {
      redirect: 'follow',
      signal: controller.signal,
      headers: {
        accept: 'text/html,application/xhtml+xml',
        'user-agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
    })
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }
    return { text: await response.text(), finalUrl: response.url }
  } finally {
    clearTimeout(timeout)
  }
}

async function resolveFirstStufferDbCategory(searchTerm) {
  const directUrl = getStufferDbDirectSearchUrl(searchTerm)
  try {
    const direct = await fetchText(directUrl)
    const directMatches = extractStufferDbCategoryCandidates(
      direct.text,
      searchTerm,
      direct.finalUrl || directUrl
    )
    const directMatch =
      directMatches.find((match) => match.exact) || directMatches[0] || null
    if (directMatch) {
      return {
        ...directMatch,
        searchUrl: directUrl,
        source: directMatch.exact
          ? 'stufferdb-search-match'
          : 'stufferdb-first-result',
      }
    }
  } catch {
    // Keep discovery usable even if StufferDB direct search is temporarily down.
  }

  const duckDuckGoUrl = getStufferDbSearchUrl(searchTerm)
  try {
    const duckDuckGo = await fetchText(
      `https://html.duckduckgo.com/html/?q=${encodeURIComponent(
        `site:stufferdb.com ${searchTerm}`
      )}`
    )
    const duckDuckGoMatches = extractStufferDbCategoryCandidates(
      duckDuckGo.text,
      searchTerm,
      duckDuckGo.finalUrl || duckDuckGoUrl
    )
    const duckDuckGoMatch =
      duckDuckGoMatches.find((match) => match.exact) ||
      duckDuckGoMatches[0] ||
      null
    if (duckDuckGoMatch) {
      return {
        ...duckDuckGoMatch,
        searchUrl: duckDuckGoUrl,
        source: 'stufferdb-duckduckgo-result',
      }
    }
  } catch {
    // The manual search card below is the fallback.
  }

  return null
}

function sourceListFor(entry, sourceKey) {
  return Array.isArray(entry?.sources?.[sourceKey])
    ? entry.sources[sourceKey].filter((source) => source?.url)
    : []
}

function inactiveSourceListFor(entry, sourceKey) {
  return Array.isArray(entry?.inactiveSources?.[sourceKey])
    ? entry.inactiveSources[sourceKey].filter((source) => source?.url)
    : []
}

function countModelSources(entry) {
  return SOURCE_KEYS.reduce(
    (count, key) => count + sourceListFor(entry, key).length,
    0
  )
}

function getModels() {
  const registry = loadModelRegistry(registryPath)
  return Object.entries(registry)
    .map(([name, entry]) => {
      const modelDir = path.join(datasetDir, name)
      const sources = Object.fromEntries(
        SOURCE_KEYS.map((key) => [key, sourceListFor(entry, key)])
      )
      return {
        name,
        aliases: Array.isArray(entry?.aliases) ? entry.aliases : [],
        sources,
        sourceCount: Object.values(sources).reduce(
          (count, list) => count + list.length,
          0
        ),
        hasLocalMedia: fs.existsSync(modelDir),
      }
    })
    .sort((left, right) =>
      left.name.localeCompare(right.name, undefined, { sensitivity: 'base' })
    )
}

function getKnownModel(value) {
  const registry = loadModelRegistry(registryPath)
  const requested = sanitize(value)
  return findCanonicalModelName(registry, requested) || requested
}

function collectSourceSearchTerms(query) {
  const username = normalizeUsernameSearchInput(query)
  if (!username) return []

  const registry = loadModelRegistry(registryPath)
  const looseUsername = normalizeLooseSearch(username)
  const terms = new Set([username])

  for (const [modelName, entry] of Object.entries(registry)) {
    const candidates = [
      modelName,
      ...(Array.isArray(entry?.aliases) ? entry.aliases : []),
    ]
    const matched = candidates.some((candidate) => {
      const looseCandidate = normalizeLooseSearch(candidate)
      return (
        looseCandidate === looseUsername ||
        looseCandidate.includes(looseUsername) ||
        looseUsername.includes(looseCandidate) ||
        editDistanceWithinOne(looseCandidate, looseUsername)
      )
    })
    if (!matched) continue

    for (const candidate of candidates) {
      const term = normalizeUsernameSearchInput(candidate)
      if (term) terms.add(term)
    }
    for (const list of Object.values(entry?.sources || {})) {
      for (const source of Array.isArray(list) ? list : []) {
        for (const value of [
          source?.username,
          source?.userId,
          source?.discoveredAs,
        ]) {
          const term = normalizeUsernameSearchInput(value)
          if (term && !/^\d+$/.test(term)) terms.add(term)
        }
      }
    }
  }

  return [...terms].slice(0, 8)
}

function findSourceOwner(parsed) {
  if (!parsed) return null
  const registry = loadModelRegistry(registryPath)
  return findCanonicalModelNameBySource(registry, {
    site: parsed.site || parsed.sourceType,
    service: parsed.service,
    userId: parsed.userId,
    username: parsed.username,
    inputUrl: parsed.inputUrl || parsed.url,
    url: parsed.url,
  })
}

function removeSourceFromModel(modelName, sourceUrl) {
  const registry = loadModelRegistry(registryPath)
  const canonical = findCanonicalModelName(registry, sanitize(modelName))
  if (!canonical) return { removed: false, model: sanitize(modelName) }

  const entry = registry[canonical]
  const sources = entry?.sources || {}
  const targetUrl = normalizeHistoryUrl(sourceUrl)
  if (!targetUrl) return { removed: false, model: canonical }

  let removedSource = null
  for (const [sourceKey, list] of Object.entries(sources)) {
    if (!Array.isArray(list)) continue
    const nextList = list.filter((source) => {
      const keep = normalizeHistoryUrl(source?.url) !== targetUrl
      if (!keep) {
        removedSource = {
          sourceKey,
          url: source?.url || sourceUrl,
        }
      }
      return keep
    })
    sources[sourceKey] = nextList
  }

  if (!removedSource) return { removed: false, model: canonical }
  saveModelRegistry(registryPath, registry)
  auditCache = null
  return {
    removed: true,
    model: canonical,
    source: removedSource,
    remainingSourceCount: countModelSources(registry[canonical]),
  }
}

function sourceStateKey(model, url) {
  return `${sanitize(model)}|${normalizeHistoryUrl(url)}`
}

function normalizeInactiveSourceRecord(record, modelName = '') {
  const parsed = parseSourceUrl(record?.url)
  const url = parsed?.url || String(record?.url || '').trim()
  const model = sanitize(record?.model || modelName)
  if (!url || !model) return null
  return {
    sourceType: 'reddit',
    model,
    url,
    service: record?.service || parsed?.service || 'submitted',
    userId: record?.userId || parsed?.userId || parsed?.username || null,
    username: record?.username || parsed?.username || parsed?.userId || null,
    discoveredAs: record?.discoveredAs || record?.username || parsed?.username,
    inactiveAt: record?.inactiveAt || new Date().toISOString(),
    inactiveReason: record?.inactiveReason || record?.reason || 'deleted',
    note: record?.note || '',
  }
}

function getInactiveRedditSources() {
  const registry = loadModelRegistry(registryPath)
  const sources = []
  for (const [model, entry] of Object.entries(registry)) {
    for (const source of inactiveSourceListFor(entry, 'reddit')) {
      const normalized = normalizeInactiveSourceRecord(source, model)
      if (normalized) sources.push(normalized)
    }
  }
  return sources.sort(
    (left, right) => new Date(right.inactiveAt) - new Date(left.inactiveAt)
  )
}

function getInactiveSourceMap(inactiveSources = getInactiveRedditSources()) {
  const map = new Map()
  for (const record of inactiveSources) {
    const normalized = normalizeInactiveSourceRecord(record, record?.model)
    if (!normalized) continue
    map.set(sourceStateKey(normalized.model, normalized.url), normalized)
  }
  return map
}

function getActiveRedditSourceStates() {
  const registry = loadModelRegistry(registryPath)
  const states = []
  for (const [model, entry] of Object.entries(registry)) {
    for (const source of sourceListFor(entry, 'reddit')) {
      if (!source?.accountStatus) continue
      states.push({
        model,
        sourceType: 'reddit',
        url: source.url,
        service: source.service || 'submitted',
        userId: source.userId || source.username || null,
        username: source.username || source.userId || null,
        discoveredAs: source.discoveredAs || source.username || source.userId,
        accountStatus: source.accountStatus,
        accountStatusAt: source.accountStatusAt || null,
        accountStatusReason: source.accountStatusReason || '',
      })
    }
  }
  return states.sort(
    (left, right) =>
      new Date(right.accountStatusAt || 0) - new Date(left.accountStatusAt || 0)
  )
}

function markRedditSourceSuspended(modelName, sourceUrl, reason = '') {
  const parsed = parseSourceUrl(sourceUrl)
  if (!parsed || parsed.sourceType !== 'reddit') {
    throw new Error('Only Reddit sources can be marked suspended.')
  }

  const registry = loadModelRegistry(registryPath)
  const canonical = findCanonicalModelName(registry, sanitize(modelName))
  if (!canonical) throw new Error('model not found')
  registry[canonical] = ensureModelEntryShape(registry[canonical], canonical)
  const sources = sourceListFor(registry[canonical], 'reddit')
  const targetUrl = normalizeHistoryUrl(parsed.url)
  const sourceIndex = sources.findIndex(
    (source) => normalizeHistoryUrl(source?.url) === targetUrl
  )
  if (sourceIndex < 0) throw new Error('reddit source not found')

  sources[sourceIndex] = {
    ...sources[sourceIndex],
    accountStatus: 'suspended',
    accountStatusAt: new Date().toISOString(),
    accountStatusReason: reason || 'marked_suspended',
  }
  registry[canonical].sources.reddit = sources
  saveModelRegistry(registryPath, registry)
  auditCache = null
  return {
    model: canonical,
    source: sources[sourceIndex],
    active: true,
    accountStatus: 'suspended',
    stale: false,
  }
}

function markRedditSourceActive(modelName, sourceUrl) {
  const parsed = parseSourceUrl(sourceUrl)
  if (!parsed || parsed.sourceType !== 'reddit') {
    throw new Error('Only Reddit sources can be marked active.')
  }

  const registry = loadModelRegistry(registryPath)
  const canonical = findCanonicalModelName(registry, sanitize(modelName))
  if (!canonical) throw new Error('model not found')
  registry[canonical] = ensureModelEntryShape(registry[canonical], canonical)
  const sources = sourceListFor(registry[canonical], 'reddit')
  const targetUrl = normalizeHistoryUrl(parsed.url)
  const sourceIndex = sources.findIndex(
    (source) => normalizeHistoryUrl(source?.url) === targetUrl
  )
  if (sourceIndex < 0) throw new Error('reddit source not found')

  const nextSource = { ...sources[sourceIndex] }
  delete nextSource.accountStatus
  delete nextSource.accountStatusAt
  delete nextSource.accountStatusReason
  sources[sourceIndex] = nextSource
  registry[canonical].sources.reddit = sources
  saveModelRegistry(registryPath, registry)
  auditCache = null
  return {
    model: canonical,
    source: nextSource,
    active: true,
    accountStatus: 'active',
    stale: false,
  }
}

function markRedditSourceDeleted(modelName, sourceUrl, reason = '') {
  const parsed = parseSourceUrl(sourceUrl)
  if (!parsed || parsed.sourceType !== 'reddit') {
    throw new Error('Only Reddit sources can be marked deleted.')
  }

  const registry = loadModelRegistry(registryPath)
  const canonical = findCanonicalModelName(registry, sanitize(modelName))
  if (!canonical) throw new Error('model not found')
  registry[canonical] = ensureModelEntryShape(registry[canonical], canonical)
  const targetUrl = normalizeHistoryUrl(parsed.url)
  let removedSource = null

  for (const [sourceKey, list] of Object.entries(registry[canonical].sources)) {
    if (!Array.isArray(list)) continue
    registry[canonical].sources[sourceKey] = list.filter((source) => {
      const keep = normalizeHistoryUrl(source?.url) !== targetUrl
      if (!keep) removedSource = { sourceKey, ...source }
      return keep
    })
  }

  if (!registry[canonical].inactiveSources) {
    registry[canonical].inactiveSources = {}
  }
  if (!Array.isArray(registry[canonical].inactiveSources.reddit)) {
    registry[canonical].inactiveSources.reddit = []
  }
  const inactiveRecord = normalizeInactiveSourceRecord(
    {
      ...(removedSource || {}),
      model: canonical,
      url: parsed.url,
      service: parsed.service,
      userId: parsed.userId,
      username: parsed.username,
      inactiveAt: new Date().toISOString(),
      inactiveReason: reason || 'deleted',
    },
    canonical
  )
  const inactiveIndex = registry[canonical].inactiveSources.reddit.findIndex(
    (source) => normalizeHistoryUrl(source?.url) === targetUrl
  )
  if (inactiveIndex >= 0) {
    registry[canonical].inactiveSources.reddit[inactiveIndex] = {
      ...registry[canonical].inactiveSources.reddit[inactiveIndex],
      ...inactiveRecord,
    }
  } else {
    registry[canonical].inactiveSources.reddit.push(inactiveRecord)
  }

  saveModelRegistry(registryPath, registry)
  auditCache = null
  const remainingSourceCount = countModelSources(registry[canonical])
  return {
    removed: Boolean(removedSource),
    model: canonical,
    source: inactiveRecord,
    remainingSourceCount,
    inactive: true,
    stale: remainingSourceCount === 0,
  }
}

function registerParsedSourceForSelectedModel(parsed, requestedModel) {
  const registry = loadModelRegistry(registryPath)
  const cleanedModel = sanitize(requestedModel)
  if (!cleanedModel) throw new Error('model is required')
  const canonical =
    findCanonicalModelName(registry, cleanedModel) || cleanedModel
  const rawName = sanitize(parsed.rawName || parsed.username || canonical)

  registry[canonical] = ensureModelEntryShape(registry[canonical], canonical)
  if (
    rawName &&
    !registry[canonical].aliases.some((alias) => sanitize(alias) === rawName)
  ) {
    registry[canonical].aliases.push(rawName)
  }

  if (parsed.sourceType === 'stufferdb') {
    upsertStufferdbSource(registry[canonical], parsed.url, rawName || canonical)
  } else if (parsed.sourceType === 'coomerfans') {
    upsertCoomerSource(registry[canonical], parsed.url, rawName || canonical)
  } else if (parsed.sourceType === 'reddit') {
    upsertRedditSource(registry[canonical], parsed.url, rawName || canonical)
  } else {
    upsertSourceInfo(
      registry[canonical],
      {
        site: parsed.site || parsed.sourceType,
        service: parsed.service,
        userId: parsed.userId,
        username: parsed.username,
        inputUrl: parsed.url,
      },
      rawName || canonical
    )
  }
  saveModelRegistry(registryPath, registry)
  auditCache = null
  return canonical
}

function toCandidate(hit, overrides = {}) {
  const parsed = parseSourceUrl(hit.url)
  const platform = hit.platform || parsed?.sourceType || 'unknown'
  return {
    id: `${platform}:${hit.url}`,
    type: 'source',
    platform,
    label: getPlatformLabel(platform),
    service: hit.service || parsed?.service || null,
    userId: hit.id || hit.userId || parsed?.userId || null,
    username: hit.username || hit.name || parsed?.username || null,
    name: hit.name || hit.username || parsed?.rawName || null,
    url: parsed?.url || hit.url,
    parseable: Boolean(parsed),
    existingModel: parsed ? findSourceOwner(parsed) : null,
    verified: hit.verified !== false,
    ...overrides,
  }
}

async function searchSourceCandidates(rawQuery) {
  const query = String(rawQuery || '').trim()
  if (!query) return []

  const parsed = parseSourceUrl(query)
  if (parsed) {
    return [
      toCandidate(
        {
          platform: parsed.sourceType,
          service: parsed.service,
          userId: parsed.userId,
          username: parsed.username || parsed.rawName,
          url: parsed.url,
          name: parsed.rawName || parsed.username,
        },
        { verified: true, source: 'direct-url' }
      ),
    ]
  }

  const terms = collectSourceSearchTerms(query)
  const username = terms[0]
  if (!username) return []

  const candidates = []

  for (const term of terms) {
    const redditCandidate = toCandidate(
      {
        platform: 'reddit',
        service: 'submitted',
        username: term,
        url: PLATFORMS.reddit.userUrl(term),
        name: term,
      },
      { verified: false, source: 'username' }
    )
    if (
      !candidates.some((candidate) => candidate.url === redditCandidate.url)
    ) {
      candidates.push(redditCandidate)
    }
  }

  for (const term of terms) {
    for (const platform of ['coomer', 'kemono']) {
      const hits = await probeUsername(platform, term)
      for (const hit of hits) {
        if (!candidates.some((candidate) => candidate.url === hit.url)) {
          candidates.push(
            toCandidate(hit, {
              source: 'probe',
              searchTerm: term,
            })
          )
        }
      }
    }
  }

  for (const term of terms) {
    const resolved = await resolveFirstStufferDbCategory(term)
    if (!resolved) continue
    if (candidates.some((candidate) => candidate.url === resolved.url)) continue
    candidates.push(
      toCandidate(
        {
          platform: 'stufferdb',
          username: resolved.title || term,
          url: resolved.url,
          name: resolved.title || term,
        },
        {
          source: resolved.source,
          searchTerm: term,
          searchUrl: resolved.searchUrl,
          stufferdbMediaCount: resolved.mediaCount,
          verified: resolved.source === 'stufferdb-search-match',
        }
      )
    )
  }

  for (const term of terms) {
    for (const platform of ['coomer', 'kemono', 'stufferdb']) {
      const url = getManualSourceSearchUrl(platform, term)
      if (!url) continue
      candidates.push({
        id: `${platform}-search:${term}`,
        type: 'manual-search',
        platform,
        label: getPlatformLabel(platform),
        service: null,
        userId: null,
        username: term,
        name: term,
        url,
        parseable: false,
        existingModel: null,
        verified: false,
        source: 'manual-search',
      })
    }
  }

  return candidates
}

function appendOption(args, flag, value) {
  if (value === undefined || value === null || value === '') return
  args.push(flag, String(value))
}

function appendBoolean(args, flag, value) {
  if (value) args.push(flag)
}

function appendScrapeOptions(args, options = {}) {
  appendBoolean(args, '--skip-nas-sync', options.skipNasSync !== false)
  appendBoolean(args, '--dry-run', Boolean(options.dryRun))
  appendBoolean(args, '--keep-history', Boolean(options.keepHistory))
  appendBoolean(args, '--stop-on-error', Boolean(options.stopOnError))
  appendBoolean(
    args,
    '--full-source-refresh',
    Boolean(options.fullSourceRefresh)
  )
  appendBoolean(args, '--browser-visible', Boolean(options.browserVisible))
  appendBoolean(
    args,
    '--download-oversized',
    Boolean(options.downloadOversized)
  )
  appendOption(args, '--pages', options.pages)
  appendOption(args, '--max-posts', options.maxPosts)
  appendOption(args, '--max-files', options.maxFiles)
  appendOption(args, '--post-concurrency', options.postConcurrency)
  appendOption(args, '--image-concurrency', options.imageConcurrency)
  appendOption(args, '--video-concurrency', options.videoConcurrency)
  appendOption(
    args,
    '--reddit-fallback-delay-ms',
    options.redditFallbackDelayMs
  )
  appendOption(
    args,
    '--source-incremental-overlap-pages',
    options.sourceIncrementalOverlapPages
  )
}

function buildScrapeArgs(sourceUrl, modelName, options = {}) {
  const args = [runScrapeScript, sourceUrl]
  appendOption(args, '--model', modelName)
  appendScrapeOptions(args, options)
  appendBoolean(
    args,
    '--reddit-browser-media',
    Boolean(options.redditBrowserMedia)
  )
  return args
}

function buildAllScrapeArgs(options = {}) {
  const args = [runScrapeScript, 'update', 'all']
  appendScrapeOptions(args, options)
  return args
}

function stripAnsi(value) {
  return String(value || '').replace(/\x1B\[[0-?]*[ -/]*[@-~]/g, '')
}

function updateJobProgressFromLog(job, text) {
  if (!job || job.mode !== 'all') return
  const modelMatch = String(text).match(
    /^MODEL\s+(\d+)\/(\d+):\s+(.+?)\s+\|\s+sources\s+(\d+)\s*$/i
  )
  if (modelMatch) {
    job.liveProgress = {
      model: modelMatch[3],
      modelIndex: Number(modelMatch[1]),
      modelTotal: Number(modelMatch[2]),
      sourceIndex: 0,
      sourceTotal: Number(modelMatch[4]),
      sourceLabel: null,
      url: null,
      updatedAt: new Date().toISOString(),
    }
    return
  }

  const sourceMatch = String(text).match(
    /^--\s+SOURCE\s+(\d+)\/(\d+):\s+(.+?)\s+->\s+(.+?)\s*$/i
  )
  if (sourceMatch) {
    job.liveProgress = {
      ...(job.liveProgress || {}),
      model: sourceMatch[3],
      sourceIndex: Number(sourceMatch[1]),
      sourceTotal: Number(sourceMatch[2]),
      sourceLabel: sourceMatch[4],
      updatedAt: new Date().toISOString(),
    }
    return
  }

  if (job.liveProgress && /^\s+https?:\/\//i.test(String(text))) {
    job.liveProgress = {
      ...job.liveProgress,
      url: String(text).trim(),
      updatedAt: new Date().toISOString(),
    }
  }
}

function appendJobLog(job, text, stream = 'stdout') {
  const clean = stripAnsi(text)
  if (!clean) return
  const chunks = clean.split(/\r?\n/)
  for (const chunk of chunks) {
    if (!chunk) continue
    updateJobProgressFromLog(job, chunk)
    job.log.push({
      at: new Date().toISOString(),
      stream,
      text: chunk,
    })
  }
  if (job.log.length > JOB_LOG_LIMIT) {
    job.log.splice(0, job.log.length - JOB_LOG_LIMIT)
  }
}

function readJsonFileIfFresh(filePath, startedAt) {
  try {
    const stat = fs.statSync(filePath)
    if (startedAt && stat.mtimeMs + 1000 < Date.parse(startedAt)) return null
    return JSON.parse(fs.readFileSync(filePath, 'utf8'))
  } catch {
    return null
  }
}

function readLatestAllSourceReportForJob(job) {
  try {
    const stat = fs.statSync(allSourceReportPath)
    if (job?.startedAt && stat.mtimeMs + 1000 < Date.parse(job.startedAt)) {
      return null
    }
    if (job && job.allSourceReportMtimeMs === stat.mtimeMs) {
      return job.allSourceReport || null
    }
    const report = JSON.parse(fs.readFileSync(allSourceReportPath, 'utf8'))
    if (job) {
      job.allSourceReport = report
      job.allSourceReportMtimeMs = stat.mtimeMs
    }
    return report
  } catch {
    return null
  }
}

function emptyTotals() {
  return {
    modelsAttempted: 0,
    cleanModels: 0,
    sources: 0,
    sourceFailures: 0,
    saved: 0,
    skipped: 0,
    duplicates: 0,
    errors: 0,
    processed: 0,
    expectedMedia: 0,
    savedBytes: 0,
    downloadBytes: 0,
    duplicateDownloadBytes: 0,
    durationMs: 0,
  }
}

function addRunToTotals(totals, run) {
  const summary = run?.summary || {}
  totals.sources += 1
  if (run?.ok === false) totals.sourceFailures += 1
  totals.saved += Number(summary.saved || 0)
  totals.skipped += Number(summary.skipped || 0)
  totals.duplicates += Number(summary.duplicates || 0)
  totals.errors += Number(summary.errors || 0)
  totals.processed += Number(summary.processed || 0)
  totals.expectedMedia += Number(summary.expectedMedia || 0)
  totals.savedBytes += Number(summary.savedBytes || 0)
  totals.downloadBytes += Number(summary.downloadBytes || 0)
  totals.duplicateDownloadBytes += Number(summary.duplicateDownloadBytes || 0)
  totals.durationMs += Number(summary.durationMs || 0)
}

function compactEventUrl(event) {
  return event?.mediaPageUrl || event?.url || event?.mediaUrl || ''
}

function readRunEvidence(logPath) {
  if (!logPath) return { duplicates: [], errors: [] }
  try {
    const stat = fs.statSync(logPath)
    const cached = evidenceCache.get(logPath)
    if (cached?.mtimeMs === stat.mtimeMs) return cached.evidence

    const evidence = { duplicates: [], errors: [] }
    const lines = fs.readFileSync(logPath, 'utf8').split(/\r?\n/)
    for (const line of lines) {
      if (!line) continue
      let event
      try {
        event = JSON.parse(line)
      } catch {
        continue
      }

      if (
        String(event.type || '').startsWith('duplicate') &&
        evidence.duplicates.length < 5
      ) {
        evidence.duplicates.push({
          type: event.type,
          filename: event.filename || '',
          savedPath: event.savedPath || event.relativePath || '',
          postId: event.postId || '',
          title: event.title || '',
          url: compactEventUrl(event),
        })
      }

      if (
        /(error|failed|unavailable)/i.test(String(event.type || '')) &&
        evidence.errors.length < 5
      ) {
        evidence.errors.push({
          type: event.type || 'error',
          message: event.error || event.message || event.reason || '',
          filename: event.filename || '',
          postId: event.postId || '',
          url: compactEventUrl(event),
        })
      }
    }

    evidenceCache.set(logPath, { mtimeMs: stat.mtimeMs, evidence })
    if (evidenceCache.size > 200) {
      evidenceCache.delete(evidenceCache.keys().next().value)
    }
    return evidence
  } catch {
    return { duplicates: [], errors: [] }
  }
}

function normalizeHistoryUrl(value) {
  return String(value || '')
    .trim()
    .replace(/\/+$/, '')
    .toLowerCase()
}

function makeHistoryId(parts) {
  return crypto
    .createHash('sha1')
    .update(parts.filter(Boolean).join('|'))
    .digest('hex')
    .slice(0, 16)
}

function sourceWorked(source) {
  if (!source || source.ok === false) return false
  if (source.status === 'failed' || source.status === 'source_unavailable') {
    return false
  }
  return true
}

function sourceNeedsRepair(source) {
  if (!source) return false
  if (source.ok === false) return true
  if (source.status === 'failed' || source.status === 'source_unavailable') {
    return true
  }
  return false
}

function redditUsernameFromUrl(value) {
  const match = String(value || '').match(/reddit\.com\/user\/([^/]+)/i)
  return match ? decodeURIComponent(match[1]).toLowerCase() : ''
}

function getSourceSavedEvidence(modelName, source) {
  const empty = { exactSavedMedia: 0, modelSourceSavedMedia: 0 }
  if (!modelName || !source?.sourceType) return empty

  const logDir = path.join(datasetDir, modelName, 'log')
  let indexFiles = []
  try {
    indexFiles = fs
      .readdirSync(logDir)
      .filter((name) => /seen-media-index\.json$/i.test(name))
      .map((name) => path.join(logDir, name))
  } catch {
    return empty
  }

  const sourceType = String(source.sourceType || '').toLowerCase()
  const redditUsername = redditUsernameFromUrl(source.url)
  const evidence = { ...empty }

  for (const filePath of indexFiles) {
    const index = readJsonFileIfFresh(filePath)
    const entries = Object.values(index?.mediaPageUrls || {})
    for (const entry of entries) {
      const entrySite = String(entry?.sourceSite || '').toLowerCase()
      if (entrySite !== sourceType) continue
      if (entry.relativePath || entry.filename) {
        evidence.modelSourceSavedMedia += 1
      }
      if (sourceType !== 'reddit') continue
      const entryUser = String(
        entry.sourceUsername || entry.sourceUserId || ''
      ).toLowerCase()
      if (
        redditUsername &&
        entryUser === redditUsername &&
        (entry.relativePath || entry.filename)
      ) {
        evidence.exactSavedMedia += 1
      }
    }
  }

  return evidence
}

function classifySourceProblem(source) {
  if (
    source.sourceType === 'reddit' &&
    source.status === 'source_unavailable' &&
    Number(source.errors || 0) === 0
  ) {
    return 'deleted_or_empty_reddit'
  }
  if (source.sourceType === 'reddit') return 'reddit_error'
  return 'source_error'
}

function summarizeHistorySource(run, index, total) {
  const summary = run?.summary || {}
  return {
    sourceIndex: index + 1,
    sourceTotal: total,
    sourceKey: run?.sourceKey || run?.sourceType || null,
    sourceType: run?.sourceType || null,
    label: run?.label || getPlatformLabel(run?.sourceType),
    url: run?.url || '',
    ok: run?.ok !== false,
    code: run?.code ?? null,
    status: summary.status || (run?.ok === false ? 'failed' : 'finished'),
    saved: Number(summary.saved || 0),
    skipped: Number(summary.skipped || 0),
    duplicates: Number(summary.duplicates || 0),
    errors: Number(summary.errors || 0),
    processed: Number(summary.processed || 0),
    expectedMedia: Number(summary.expectedMedia || 0),
    durationMs: Number(summary.durationMs || 0),
    finishedAt: summary.finishedAt || null,
    logPath: summary.logPath || null,
    failure: summary.failure || null,
    evidence: readRunEvidence(summary.logPath),
  }
}

function summarizeHistoryModel(result, index, totalModels) {
  const totals = emptyTotals()
  const sourceTotal = Number(
    result?.sources?.length || result?.runs?.length || 0
  )
  const sources = (result?.runs || []).map((run, runIndex) => {
    addRunToTotals(totals, run)
    return summarizeHistorySource(run, runIndex, sourceTotal)
  })
  const failed =
    sources.some((source) => sourceNeedsRepair(source)) ||
    result?.nasSync?.ok === false
  return {
    modelIndex: index + 1,
    modelTotal: totalModels,
    model: result?.model || 'Unknown model',
    ok: !failed,
    sourceCount: sourceTotal,
    totals,
    nasSync: result?.nasSync || null,
    startedAt: result?.startedAt || null,
    finishedAt: result?.finishedAt || null,
    sources,
  }
}

function totalsFromReport(report, models) {
  const reportTotals = report?.totals || {}
  return {
    ...emptyTotals(),
    modelsAttempted: Number(reportTotals.modelsAttempted || models.length),
    cleanModels: Number(
      reportTotals.cleanModels || models.filter((model) => model.ok).length
    ),
    sources: Number(
      reportTotals.runs ||
        models.reduce((sum, model) => sum + model.sources.length, 0)
    ),
    sourceFailures: Number(reportTotals.failures || 0),
    saved: Number(reportTotals.saved || 0),
    skipped: Number(reportTotals.skipped || 0),
    duplicates: Number(reportTotals.duplicates || 0),
    errors: Number(reportTotals.errors || 0),
    processed: Number(reportTotals.processed || 0),
    expectedMedia: Number(reportTotals.expectedMedia || 0),
    savedBytes: Number(reportTotals.savedBytes || 0),
    downloadBytes: Number(reportTotals.downloadBytes || 0),
    duplicateDownloadBytes: Number(reportTotals.duplicateDownloadBytes || 0),
    durationMs: Number(reportTotals.durationMs || 0),
    nasSyncFailures: Number(reportTotals.nasSyncFailures || 0),
    localMp4sDeleted: Number(reportTotals.localMp4sDeleted || 0),
    localBytesReclaimed: Number(reportTotals.localBytesReclaimed || 0),
    totalModels: Number(report?.selectedModels || models.length),
    totalSources: Number(report?.selectedSources || 0),
  }
}

function snapshotFromAllSourceReport(report, overrides = {}) {
  const results = Array.isArray(report?.results) ? report.results : []
  const models = results.map((result, index) =>
    summarizeHistoryModel(
      result,
      index,
      Number(report?.selectedModels || results.length)
    )
  )
  const failed = Number(report?.totals?.failures || 0) > 0
  const startedAt = overrides.startedAt || report?.startedAt || null
  const finishedAt = overrides.finishedAt || report?.finishedAt || null
  const status =
    overrides.status ||
    (finishedAt ? (failed ? 'failed' : 'completed') : 'interrupted')
  const id = makeHistoryId([
    'all',
    startedAt,
    finishedAt,
    String(results.length),
    String(report?.totals?.saved || 0),
  ])
  return {
    id,
    mode: 'all',
    status,
    model: 'ALL SOURCES',
    createdAt:
      overrides.createdAt || report?.startedAt || report?.generatedAt || null,
    startedAt,
    finishedAt,
    options: overrides.options || {},
    reportPath: allSourceReportPath,
    totals: totalsFromReport(report, models),
    analysis: analyzeTotals(status, totalsFromReport(report, models)),
    models,
  }
}

function snapshotFromSourceJob(job) {
  const totals = emptyTotals()
  const sources = (job.runs || []).map((run, index) => {
    addRunToTotals(totals, run)
    return summarizeHistorySource(run, index, job.sources.length)
  })
  totals.modelsAttempted = job.startedAt ? 1 : 0
  totals.cleanModels =
    job.status === 'completed' && totals.sourceFailures === 0 ? 1 : 0
  const id = makeHistoryId([
    'sources',
    job.model,
    job.startedAt,
    job.finishedAt,
    String(job.id),
  ])
  return {
    id,
    jobId: job.id,
    mode: job.mode,
    status: job.status,
    model: job.model,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    options: job.options || {},
    totals,
    analysis: analyzeTotals(job.status, totals),
    models: [
      {
        modelIndex: 1,
        modelTotal: 1,
        model: job.model,
        ok: !sources.some((source) => sourceNeedsRepair(source)),
        sourceCount: sources.length,
        totals,
        startedAt: job.startedAt,
        finishedAt: job.finishedAt,
        sources,
      },
    ],
  }
}

function snapshotFromJob(job) {
  if (job.mode === 'all') {
    const report = readLatestAllSourceReportForJob(job) || job.allSourceReport
    if (report) {
      return snapshotFromAllSourceReport(report, {
        createdAt: job.createdAt,
        startedAt: job.startedAt,
        finishedAt: job.finishedAt,
        status: job.status,
        options: job.options || {},
      })
    }
  }
  return snapshotFromSourceJob(job)
}

function readRunHistory() {
  try {
    const history = JSON.parse(fs.readFileSync(runHistoryPath, 'utf8'))
    return {
      version: HISTORY_VERSION,
      updatedAt: history.updatedAt || null,
      runs: Array.isArray(history.runs) ? history.runs : [],
    }
  } catch {
    return {
      version: HISTORY_VERSION,
      updatedAt: null,
      runs: [],
    }
  }
}

function writeRunHistory(history) {
  fs.mkdirSync(historyDir, { recursive: true })
  const payload = {
    version: HISTORY_VERSION,
    updatedAt: new Date().toISOString(),
    runs: Array.isArray(history.runs) ? history.runs : [],
  }
  const tempPath = `${runHistoryPath}.tmp`
  fs.writeFileSync(tempPath, `${JSON.stringify(payload, null, 2)}\n`)
  fs.renameSync(tempPath, runHistoryPath)
}

function upsertHistorySnapshot(snapshot) {
  if (!snapshot?.startedAt) return null
  const history = readRunHistory()
  const existing = history.runs.find((run) => run.id === snapshot.id)
  if (existing && JSON.stringify(existing) === JSON.stringify(snapshot)) {
    return existing
  }
  const withoutExisting = history.runs.filter((run) => run.id !== snapshot.id)
  withoutExisting.push(snapshot)
  withoutExisting.sort(
    (left, right) =>
      new Date(right.startedAt || right.createdAt || 0) -
      new Date(left.startedAt || left.createdAt || 0)
  )
  history.runs = withoutExisting
  writeRunHistory(history)
  auditCache = null
  return snapshot
}

function syncLatestAllSourceReportToHistory() {
  const report = readJsonFileIfFresh(allSourceReportPath)
  if (!report?.startedAt) return null
  const snapshot = snapshotFromAllSourceReport(report)
  upsertHistorySnapshot(snapshot)
  return snapshot
}

function buildSourceAlerts(runs, inactiveSourceMap = new Map()) {
  const sortedRuns = [...runs].sort(
    (left, right) =>
      new Date(right.startedAt || right.createdAt || 0) -
      new Date(left.startedAt || left.createdAt || 0)
  )
  const latest = sortedRuns[0] || null
  if (!latest) return []

  const priorWorked = new Map()
  for (const run of sortedRuns.slice(1)) {
    for (const model of run.models || []) {
      for (const source of model.sources || []) {
        if (!sourceWorked(source)) continue
        const key = normalizeHistoryUrl(source.url)
        if (!key || priorWorked.has(key)) continue
        priorWorked.set(key, {
          runId: run.id,
          model: model.model,
          label: source.label,
          status: source.status,
          finishedAt: source.finishedAt || run.finishedAt || run.startedAt,
        })
      }
    }
  }

  const alerts = []
  for (const model of latest.models || []) {
    for (const source of model.sources || []) {
      if (!sourceNeedsRepair(source)) continue
      if (
        inactiveSourceMap.has(sourceStateKey(model.model, source.url)) ||
        inactiveSourceMap.has(
          sourceStateKey(model.model, normalizeHistoryUrl(source.url))
        )
      ) {
        continue
      }
      const previous = priorWorked.get(normalizeHistoryUrl(source.url)) || null
      const savedEvidence = getSourceSavedEvidence(model.model, source)
      const alertType = previous
        ? 'regression'
        : savedEvidence.exactSavedMedia > 0
          ? 'saved_media_now_failing'
          : classifySourceProblem(source)
      alerts.push({
        alertType,
        model: model.model,
        sourceKey: source.sourceKey,
        sourceType: source.sourceType,
        label: source.label,
        url: source.url,
        status: source.status,
        ok: source.ok,
        saved: source.saved,
        skipped: source.skipped,
        duplicates: source.duplicates,
        errors: source.errors,
        processed: source.processed,
        savedBefore: savedEvidence.exactSavedMedia > 0,
        savedMediaCount: savedEvidence.exactSavedMedia,
        modelSourceSavedMediaCount: savedEvidence.modelSourceSavedMedia,
        failure: source.failure,
        evidence: source.evidence,
        latestRunId: latest.id,
        latestStartedAt: latest.startedAt,
        previousWorkedAt: previous?.finishedAt || null,
        previousRunId: previous?.runId || null,
      })
    }
  }

  return alerts.sort((left, right) => {
    const rank = {
      regression: 0,
      saved_media_now_failing: 1,
      deleted_or_empty_reddit: 2,
      reddit_error: 3,
    }
    return (rank[left.alertType] ?? 3) - (rank[right.alertType] ?? 3)
  })
}

function buildStaleModelQueue(inactiveSources = []) {
  const registry = loadModelRegistry(registryPath)
  const inactiveByModel = new Map()
  for (const record of inactiveSources) {
    const model = sanitize(record?.model)
    if (!model) continue
    if (!inactiveByModel.has(model)) inactiveByModel.set(model, [])
    inactiveByModel.get(model).push(record)
  }

  return Object.entries(registry)
    .map(([model, entry]) => {
      const sourceCount = countModelSources(entry)
      if (sourceCount > 0) return null
      const modelDir = path.join(datasetDir, model)
      return {
        model,
        aliases: Array.isArray(entry?.aliases) ? entry.aliases : [],
        sourceCount,
        hasLocalMedia: fs.existsSync(modelDir),
        inactiveSources: inactiveByModel.get(model) || [],
      }
    })
    .filter(Boolean)
    .sort((left, right) =>
      left.model.localeCompare(right.model, undefined, {
        sensitivity: 'base',
      })
    )
}

function getLatestSourceStatusMap(latestRun) {
  const map = new Map()
  for (const model of latestRun?.models || []) {
    for (const source of model.sources || []) {
      map.set(sourceStateKey(model.model, source.url), {
        ok: sourceWorked(source),
        status: source.status || '',
        saved: Number(source.saved || 0),
        errors: Number(source.errors || 0),
        finishedAt: source.finishedAt || latestRun.finishedAt || null,
      })
    }
  }
  return map
}

function collectLatestMediaFailures(latestRun) {
  const failures = []
  for (const model of latestRun?.models || []) {
    for (const source of model.sources || []) {
      const errors = source.evidence?.errors || []
      for (const error of errors) {
        if (
          !/media_error|lazy_video_error|gif_conversion_error/i.test(
            String(error.type || '')
          )
        ) {
          continue
        }
        failures.push({
          model: model.model,
          label: source.label,
          sourceType: source.sourceType,
          sourceUrl: source.url,
          status: source.status,
          type: error.type,
          message: error.message || '',
          filename: error.filename || '',
          postId: error.postId || '',
          url: error.url || '',
          logPath: source.logPath || null,
        })
      }
    }
  }
  return failures
}

function readQuarantineSummary() {
  try {
    const manifest = JSON.parse(fs.readFileSync(quarantineManifestPath, 'utf8'))
    const items = Array.isArray(manifest.items) ? manifest.items : []
    const countsByState = {}
    const countsByReason = {}
    const countsByMediaType = {}
    const reviewItems = []

    for (const item of items) {
      const state =
        item?.state?.repairState ||
        item?.repairState ||
        item?.status ||
        'unknown'
      const mediaType = item?.mediaType || 'unknown'
      countsByState[state] = (countsByState[state] || 0) + 1
      countsByMediaType[mediaType] = (countsByMediaType[mediaType] || 0) + 1
      for (const reason of item?.reasons || ['unknown']) {
        countsByReason[reason] = (countsByReason[reason] || 0) + 1
      }
      if (state !== 'repaired' && reviewItems.length < 100) {
        reviewItems.push({
          id: item.id,
          model: item.model,
          mediaType,
          relativePath: item.relativePath,
          quarantinePath: item.quarantinePath,
          state,
          reasons: item.reasons || [],
          sizeBytes: Number(item.sizeBytes || 0),
          lastAttemptAt: item.repair?.lastAttemptAt || null,
          lastAttemptOutcome: item.repair?.lastAttemptOutcome || null,
          lastAttemptError: item.repair?.lastAttemptError || null,
        })
      }
    }

    return {
      path: quarantineManifestPath,
      total: items.length,
      countsByState,
      countsByReason,
      countsByMediaType,
      needsReview: items.filter(
        (item) =>
          (item?.state?.repairState ||
            item?.repairState ||
            item?.status ||
            'unknown') !== 'repaired'
      ).length,
      items: reviewItems,
    }
  } catch (err) {
    return {
      path: quarantineManifestPath,
      total: 0,
      countsByState: {},
      countsByReason: {},
      countsByMediaType: {},
      needsReview: 0,
      items: [],
      error: err.message,
    }
  }
}

function buildAuditQueues(history) {
  const runs = history?.runs || []
  const inactiveRedditSources = getInactiveRedditSources()
  const activeRedditStates = getActiveRedditSourceStates()
  const newestStartedAt = [...runs]
    .map((run) => run.startedAt || run.createdAt || '')
    .sort()
    .pop()
  const inactiveNewestAt = [...inactiveRedditSources]
    .map((source) => source.inactiveAt || '')
    .sort()
    .pop()
  const activeStateNewestAt = [...activeRedditStates]
    .map((source) => source.accountStatusAt || '')
    .sort()
    .pop()
  if (
    auditCache &&
    Date.now() - auditCache.createdAtMs < AUDIT_CACHE_MS &&
    auditCache.newestStartedAt === newestStartedAt &&
    auditCache.inactiveNewestAt === inactiveNewestAt &&
    auditCache.activeStateNewestAt === activeStateNewestAt
  ) {
    return auditCache.queues
  }

  const latestRun = [...runs].sort(
    (left, right) =>
      new Date(right.startedAt || right.createdAt || 0) -
      new Date(left.startedAt || left.createdAt || 0)
  )[0]
  const latestSourceStatuses = getLatestSourceStatusMap(latestRun)
  let oversizedVideos = []
  try {
    oversizedVideos = collectOversizedVideoTargets({ datasetDir })
  } catch (err) {
    oversizedVideos = [{ error: err.message }]
  }
  const mediaFailures = collectLatestMediaFailures(latestRun)
  const quarantine = readQuarantineSummary()
  const inactiveSourceMap = getInactiveSourceMap(inactiveRedditSources)
  const suspendedRedditSources = activeRedditStates
    .filter((source) => source.accountStatus === 'suspended')
    .map((source) => {
      const latestStatus =
        latestSourceStatuses.get(sourceStateKey(source.model, source.url)) ||
        null
      return {
        ...source,
        latestStatus,
        recovered: Boolean(latestStatus?.ok),
      }
    })
  const queues = {
    sources: buildSourceAlerts(runs, inactiveSourceMap),
    suspendedRedditSources,
    inactiveRedditSources,
    staleModels: buildStaleModelQueue(inactiveRedditSources),
    oversizedVideos,
    mediaFailures,
    quarantine,
  }
  auditCache = {
    createdAtMs: Date.now(),
    newestStartedAt,
    inactiveNewestAt,
    activeStateNewestAt,
    queues,
  }
  return queues
}

function sourceRunView(run, index, total) {
  const summary = run?.summary || {}
  return {
    sourceIndex: index + 1,
    sourceTotal: total,
    label: getPlatformLabel(run?.sourceType) || run?.label || 'Source',
    url: run?.url || '',
    ok: run?.ok !== false,
    status: summary.status || (run?.ok === false ? 'failed' : 'pending'),
    saved: Number(summary.saved || 0),
    skipped: Number(summary.skipped || 0),
    duplicates: Number(summary.duplicates || 0),
    errors: Number(summary.errors || 0),
    processed: Number(summary.processed || 0),
    expectedMedia: Number(summary.expectedMedia || 0),
    failure: summary.failure || null,
    evidence: readRunEvidence(summary.logPath),
  }
}

function summarizeSourceJob(job) {
  const totals = emptyTotals()
  totals.modelsAttempted = job.startedAt ? 1 : 0
  const sources = (job.runs || []).map((run, index) => {
    addRunToTotals(totals, run)
    return sourceRunView(run, index, job.sources.length)
  })
  if (job.finishedAt && totals.sourceFailures === 0) totals.cleanModels = 1
  const activeIndex =
    job.status === 'running' || job.status === 'queued'
      ? Math.min(Number(job.activeSourceIndex || 0), job.sources.length - 1)
      : null
  return {
    kind: 'sources',
    model: job.model,
    current:
      activeIndex !== null && job.sources[activeIndex]
        ? {
            model: job.model,
            sourceIndex: activeIndex + 1,
            sourceTotal: job.sources.length,
            url: job.sources[activeIndex],
          }
        : null,
    totals,
    sources,
    analysis: analyzeTotals(job.status, totals),
  }
}

function allSourceRunView(run, index, total) {
  const summary = run?.summary || {}
  return {
    sourceIndex: index + 1,
    sourceTotal: total,
    label: run?.label || getPlatformLabel(run?.sourceType),
    url: run?.url || '',
    ok: run?.ok !== false,
    status: summary.status || (run?.ok === false ? 'failed' : 'finished'),
    saved: Number(summary.saved || 0),
    skipped: Number(summary.skipped || 0),
    duplicates: Number(summary.duplicates || 0),
    errors: Number(summary.errors || 0),
    processed: Number(summary.processed || 0),
    expectedMedia: Number(summary.expectedMedia || 0),
    failure: summary.failure || null,
    evidence: readRunEvidence(summary.logPath),
  }
}

function allSourceModelView(result, index, totalModels) {
  const totals = emptyTotals()
  const sourceTotal = Number(
    result?.sources?.length || result?.runs?.length || 0
  )
  const sources = (result?.runs || []).map((run, runIndex) => {
    addRunToTotals(totals, run)
    return allSourceRunView(run, runIndex, sourceTotal)
  })
  const failed =
    sources.some((source) => !source.ok) || result?.nasSync?.ok === false
  return {
    modelIndex: index + 1,
    modelTotal: totalModels,
    model: result?.model || 'Unknown model',
    ok: !failed,
    sourceCount: sourceTotal,
    sources,
    totals,
  }
}

function summarizeAllSourceJob(job) {
  const report = readLatestAllSourceReportForJob(job)
  const results = Array.isArray(report?.results) ? report.results : []
  const models = results.map((result, index) =>
    allSourceModelView(
      result,
      index,
      Number(report?.selectedModels || results.length)
    )
  )
  const reportTotals = report?.totals || {}
  const totals = {
    ...emptyTotals(),
    modelsAttempted: Number(reportTotals.modelsAttempted || results.length),
    cleanModels: Number(
      reportTotals.cleanModels || models.filter((model) => model.ok).length
    ),
    sources: Number(
      reportTotals.runs ||
        models.reduce((sum, model) => sum + model.sources.length, 0)
    ),
    sourceFailures: Number(reportTotals.failures || 0),
    saved: Number(reportTotals.saved || 0),
    skipped: Number(reportTotals.skipped || 0),
    duplicates: Number(reportTotals.duplicates || 0),
    errors: Number(reportTotals.errors || 0),
    processed: Number(reportTotals.processed || 0),
    expectedMedia: Number(reportTotals.expectedMedia || 0),
    savedBytes: Number(reportTotals.savedBytes || 0),
    downloadBytes: Number(reportTotals.downloadBytes || 0),
    duplicateDownloadBytes: Number(reportTotals.duplicateDownloadBytes || 0),
    durationMs: Number(reportTotals.durationMs || 0),
    totalModels: Number(report?.selectedModels || 0),
    totalSources: Number(report?.selectedSources || 0),
  }
  const latestModel = models[models.length - 1] || null
  const latestSource =
    latestModel?.sources[latestModel.sources.length - 1] || null
  const liveProgress =
    job.liveProgress && job.status === 'running'
      ? {
          model: job.liveProgress.model,
          modelIndex: job.liveProgress.modelIndex || 0,
          modelTotal:
            job.liveProgress.modelTotal || Number(report?.selectedModels || 0),
          sourceIndex: job.liveProgress.sourceIndex || 0,
          sourceTotal: job.liveProgress.sourceTotal || 0,
          sourceLabel: job.liveProgress.sourceLabel || null,
          url: job.liveProgress.url || null,
          updatedAt: job.liveProgress.updatedAt || null,
        }
      : null
  return {
    kind: 'all',
    reportPath: report ? allSourceReportPath : null,
    current:
      liveProgress ||
      (job.status === 'running' && latestModel
        ? {
            model: latestModel.model,
            modelIndex: latestModel.modelIndex,
            modelTotal: latestModel.modelTotal,
            sourceIndex: latestSource?.sourceIndex || 0,
            sourceTotal: latestModel.sourceCount,
          }
        : null),
    totals,
    latestModel,
    recentModels: models.slice(-8).reverse(),
    failedModels: models.filter((model) => !model.ok).slice(-20),
    analysis: analyzeTotals(job.status, totals),
  }
}

function analyzeTotals(status, totals) {
  if (status === 'queued') return 'Queued and waiting for the active run.'
  if (status === 'running') {
    return `Running: saved ${totals.saved}, skipped ${totals.skipped}, duplicates ${totals.duplicates}.`
  }
  if (status === 'canceled') {
    return `Canceled after ${totals.sources} source run${totals.sources === 1 ? '' : 's'}.`
  }
  if (status === 'interrupted') {
    return `Latest report is partial: ${totals.modelsAttempted} model${totals.modelsAttempted === 1 ? '' : 's'} and ${totals.sources} source run${totals.sources === 1 ? '' : 's'} recorded.`
  }
  if (totals.sourceFailures || totals.errors) {
    return `Finished with ${totals.sourceFailures} source failure${totals.sourceFailures === 1 ? '' : 's'} and ${totals.errors} media error${totals.errors === 1 ? '' : 's'}.`
  }
  if (status === 'completed') {
    return `Completed cleanly: saved ${totals.saved}, skipped ${totals.skipped}, duplicates ${totals.duplicates}.`
  }
  return `Status: ${status}.`
}

function summarizeJobForDashboard(job) {
  return job.mode === 'all'
    ? summarizeAllSourceJob(job)
    : summarizeSourceJob(job)
}

function publicJob(job, options = {}) {
  const payload = {
    id: job.id,
    mode: job.mode,
    status: job.status,
    model: job.model,
    sources: job.sources,
    options: job.options,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    activeSourceIndex: job.activeSourceIndex,
    exitCode: job.exitCode,
    error: job.error,
    runs: job.runs,
    summary: summarizeJobForDashboard(job),
  }
  if (options.includeLog) payload.log = job.log
  return payload
}

function latestAllSourceReportJob() {
  const report = readJsonFileIfFresh(allSourceReportPath)
  if (!report) return null
  const failed = Number(report.totals?.failures || 0) > 0
  return {
    id: 0,
    mode: 'all',
    status: report.finishedAt
      ? failed
        ? 'failed'
        : 'completed'
      : 'interrupted',
    model: 'LATEST ALL-SOURCE REPORT',
    sources: [],
    options: {},
    createdAt: report.startedAt || report.generatedAt || null,
    startedAt: report.startedAt || report.generatedAt || null,
    finishedAt: report.finishedAt || null,
    activeSourceIndex: null,
    exitCode: failed ? 1 : 0,
    error: null,
    runs: [],
    log: [],
    allSourceReport: report,
  }
}

function killProcessTree(pid) {
  if (!pid) return
  if (process.platform === 'win32') {
    spawnSync('taskkill', ['/PID', String(pid), '/T', '/F'], {
      stdio: 'ignore',
    })
    return
  }
  try {
    process.kill(-pid, 'SIGTERM')
  } catch {
    try {
      process.kill(pid, 'SIGTERM')
    } catch {}
  }
}

function runChildForSource(job, sourceUrl, index) {
  return new Promise((resolve) => {
    const parsed = parseSourceUrl(sourceUrl)
    const startedAtMs = Date.now()
    const args = buildScrapeArgs(sourceUrl, job.model, job.options)
    appendJobLog(
      job,
      `[${index + 1}/${job.sources.length}] ${job.model} -> ${getPlatformLabel(
        parsed?.sourceType
      )}: ${sourceUrl}`
    )
    appendJobLog(
      job,
      `node ${args.map((arg) => JSON.stringify(arg)).join(' ')}`
    )

    const child = spawn(process.execPath, args, {
      cwd: rootDir,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    })
    job.child = child
    child.stdout.on('data', (chunk) => appendJobLog(job, chunk, 'stdout'))
    child.stderr.on('data', (chunk) => appendJobLog(job, chunk, 'stderr'))
    child.on('error', (err) => {
      appendJobLog(job, `Failed to start scraper: ${err.message}`, 'stderr')
      resolve({ code: 1, parsed, startedAtMs, error: err.message })
    })
    child.on('exit', (code, signal) => {
      resolve({
        code: code ?? (signal ? 130 : 1),
        signal,
        parsed,
        startedAtMs,
      })
    })
  })
}

function runChildForAllSources(job) {
  return new Promise((resolve) => {
    const args = buildAllScrapeArgs(job.options)
    appendJobLog(job, 'Running all registered source updates')
    appendJobLog(
      job,
      `node ${args.map((arg) => JSON.stringify(arg)).join(' ')}`
    )

    const child = spawn(process.execPath, args, {
      cwd: rootDir,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    })
    job.child = child
    child.stdout.on('data', (chunk) => appendJobLog(job, chunk, 'stdout'))
    child.stderr.on('data', (chunk) => appendJobLog(job, chunk, 'stderr'))
    child.on('error', (err) => {
      appendJobLog(
        job,
        `Failed to start all-source scrape: ${err.message}`,
        'stderr'
      )
      resolve({ code: 1, error: err.message })
    })
    child.on('exit', (code, signal) => {
      resolve({ code: code ?? (signal ? 130 : 1), signal })
    })
  })
}

async function runJob(job) {
  activeJob = job
  job.status = 'running'
  job.startedAt = new Date().toISOString()
  job.activeSourceIndex = 0
  appendJobLog(
    job,
    `Started ${job.mode === 'all' ? 'all-source' : 'scrape'} job for ${job.model}`
  )

  try {
    if (job.mode === 'all') {
      const result = await runChildForAllSources(job)
      job.child = null
      job.runs.push({
        ok: result.code === 0,
        code: result.code,
        signal: result.signal || null,
        scraper: 'all',
        sourceType: 'all',
        url: 'all registered sources',
        summary: null,
      })
      if (result.code !== 0) {
        appendJobLog(
          job,
          `All-source scrape exited with status ${result.code}`,
          'stderr'
        )
      }
      job.allSourceReport = readLatestAllSourceReportForJob(job)
    } else {
      for (let index = 0; index < job.sources.length; index += 1) {
        if (job.status === 'canceling') break
        job.activeSourceIndex = index
        const sourceUrl = job.sources[index]
        const result = await runChildForSource(job, sourceUrl, index)
        job.child = null
        const summary = result.parsed
          ? readFreshModelRunSummary(job.model, result.parsed.scraper, {
              inputUrl: result.parsed.inputUrl || result.parsed.url,
              startedAfterMs: result.startedAtMs,
            })
          : null
        const run = {
          ok: result.code === 0,
          code: result.code,
          signal: result.signal || null,
          scraper: result.parsed?.scraper || null,
          sourceType: result.parsed?.sourceType || null,
          url: sourceUrl,
          summary: summarizeSourceRunSummary(summary, {
            ok: result.code === 0,
            sourceType: result.parsed?.sourceType || null,
          }),
        }
        job.runs.push(run)
        if (result.code !== 0) {
          appendJobLog(
            job,
            `Source exited with status ${result.code}`,
            'stderr'
          )
          if (job.options.stopOnError) break
        }
      }
    }

    const failed = job.runs.some((run) => !run.ok)
    job.status =
      job.status === 'canceling' ? 'canceled' : failed ? 'failed' : 'completed'
    job.exitCode = failed ? 1 : 0
  } catch (err) {
    job.status = 'failed'
    job.exitCode = 1
    job.error = err.stack || err.message
    appendJobLog(job, job.error, 'stderr')
  } finally {
    job.finishedAt = new Date().toISOString()
    appendJobLog(job, `Job ${job.status}`)
    try {
      upsertHistorySnapshot(snapshotFromJob(job))
    } catch (err) {
      appendJobLog(
        job,
        `Failed to update run history: ${err.message}`,
        'stderr'
      )
    }
    activeJob = null
    runNextJob()
  }
}

function runNextJob() {
  if (activeJob || !queue.length) return
  const job = queue.shift()
  runJob(job)
}

app.use(express.urlencoded({ extended: false }))
app.use(express.json({ limit: '128kb' }))

app.post('/auth', (req, res) => {
  if (!PASSWORD || req.body.password === PASSWORD) {
    res.setHeader(
      'Set-Cookie',
      `${AUTH_COOKIE}=${AUTH_TOKEN}; Path=/; HttpOnly; SameSite=Strict; Max-Age=31536000`
    )
    return res.redirect('/')
  }
  res.redirect('/login.html?error=1')
})

app.use(requireAuth)
app.use(express.static(__dirname))

app.get('/', (_req, res) => {
  res.sendFile('index.html', { root: __dirname })
})

app.get('/api/models', (_req, res) => {
  res.json({
    registryPath,
    datasetDir,
    models: getModels(),
  })
})

app.get('/api/source-search', async (req, res) => {
  try {
    const candidates = await searchSourceCandidates(req.query.q)
    res.json({ candidates })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

app.post('/api/models/:model/sources', (req, res) => {
  const model = getKnownModel(req.params.model || req.body.model)
  const parsed = parseSourceUrl(req.body.url)
  if (!model) return res.status(400).json({ error: 'model is required' })
  if (!parsed) return res.status(400).json({ error: 'Unsupported source URL' })

  try {
    const savedModel = registerParsedSourceForSelectedModel(parsed, model)
    res.json({
      ok: true,
      model: savedModel,
      source: {
        platform: parsed.sourceType,
        label: getPlatformLabel(parsed.sourceType),
        url: parsed.url,
      },
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

app.delete('/api/models/:model/sources', (req, res) => {
  const model = getKnownModel(req.params.model || req.body.model)
  const url = String(req.body.url || '').trim()
  if (!model) return res.status(400).json({ error: 'model is required' })
  if (!url) return res.status(400).json({ error: 'source url is required' })

  try {
    res.json({ ok: true, ...removeSourceFromModel(model, url) })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

app.post('/api/models/:model/sources/reddit-state', (req, res) => {
  const model = getKnownModel(req.params.model || req.body.model)
  const url = String(req.body.url || '').trim()
  const state = String(req.body.state || '')
    .trim()
    .toLowerCase()
  const reason = String(req.body.reason || '').trim()
  if (!model) return res.status(400).json({ error: 'model is required' })
  if (!url) return res.status(400).json({ error: 'source url is required' })

  try {
    if (state === 'suspended') {
      return res.json({
        ok: true,
        ...markRedditSourceSuspended(model, url, reason),
      })
    }
    if (state === 'deleted') {
      return res.json({
        ok: true,
        ...markRedditSourceDeleted(model, url, reason),
      })
    }
    if (state === 'active') {
      return res.json({
        ok: true,
        ...markRedditSourceActive(model, url),
      })
    }
    res
      .status(400)
      .json({ error: 'state must be suspended, deleted, or active' })
  } catch (err) {
    res.status(400).json({ error: err.message })
  }
})

app.post('/api/models/:model/sources/ban', (req, res) => {
  const model = getKnownModel(req.params.model || req.body.model)
  const url = String(req.body.url || '').trim()
  const reason = String(req.body.reason || '').trim()
  if (!model) return res.status(400).json({ error: 'model is required' })
  if (!url) return res.status(400).json({ error: 'source url is required' })

  try {
    res.json({
      ok: true,
      ...markRedditSourceSuspended(model, url, reason || 'marked_suspended'),
    })
  } catch (err) {
    res.status(400).json({ error: err.message })
  }
})

app.get('/api/jobs', (_req, res) => {
  const visibleJobs = Array.from(jobs.values()).sort(
    (left, right) => right.id - left.id
  )
  if (!visibleJobs.length) {
    const latestJob = latestAllSourceReportJob()
    if (latestJob) visibleJobs.push(latestJob)
  }
  res.json({
    activeJobId: activeJob?.id || null,
    queuedJobIds: queue.map((job) => job.id),
    jobs: visibleJobs.slice(0, 30).map(publicJob),
  })
})

app.post('/api/jobs', (req, res) => {
  const mode = req.body.mode === 'all' ? 'all' : 'sources'
  const model = mode === 'all' ? 'ALL SOURCES' : getKnownModel(req.body.model)
  const sources = Array.isArray(req.body.sources)
    ? req.body.sources.map((url) => String(url || '').trim()).filter(Boolean)
    : []
  if (mode !== 'all' && !model) {
    return res.status(400).json({ error: 'model is required' })
  }
  if (mode !== 'all' && !sources.length) {
    return res.status(400).json({ error: 'at least one source is required' })
  }
  for (const source of mode === 'all' ? [] : sources) {
    if (!parseSourceUrl(source)) {
      return res
        .status(400)
        .json({ error: `Unsupported source URL: ${source}` })
    }
  }

  const job = {
    id: nextJobId++,
    mode,
    status: 'queued',
    model,
    sources: mode === 'all' ? [] : sources,
    options: {
      ...(req.body.options || {}),
      skipNasSync: req.body.options?.skipNasSync !== false,
    },
    createdAt: new Date().toISOString(),
    startedAt: null,
    finishedAt: null,
    activeSourceIndex: null,
    exitCode: null,
    error: null,
    runs: [],
    log: [],
    child: null,
    liveProgress: null,
  }
  jobs.set(job.id, job)
  queue.push(job)
  runNextJob()
  res.json({ ok: true, job: publicJob(job) })
})

app.get('/api/jobs/:id', (req, res) => {
  const job = jobs.get(Number(req.params.id))
  if (!job) return res.status(404).json({ error: 'job not found' })
  res.json({ job: publicJob(job, { includeLog: true }) })
})

app.get('/api/history', (_req, res) => {
  try {
    syncLatestAllSourceReportToHistory()
    const history = readRunHistory()
    const runs = history.runs.slice(0, 50)
    const auditQueues = buildAuditQueues(history)
    res.json({
      historyPath: runHistoryPath,
      updatedAt: history.updatedAt,
      latestRun: runs[0] || null,
      sourceAlerts: auditQueues.sources,
      auditQueues,
      runs,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

app.post('/api/jobs/:id/cancel', (req, res) => {
  const job = jobs.get(Number(req.params.id))
  if (!job) return res.status(404).json({ error: 'job not found' })
  if (job.status === 'queued') {
    const index = queue.indexOf(job)
    if (index >= 0) queue.splice(index, 1)
    job.status = 'canceled'
    job.finishedAt = new Date().toISOString()
    appendJobLog(job, 'Canceled before start')
    return res.json({ ok: true, job: publicJob(job) })
  }
  if (job.status === 'running' && job.child) {
    job.status = 'canceling'
    appendJobLog(job, 'Cancel requested', 'stderr')
    killProcessTree(job.child.pid)
  }
  res.json({ ok: true, job: publicJob(job) })
})

app.listen(PORT, () => {
  console.log(`Scrape Dashboard: http://localhost:${PORT}`)
  console.log(`Registry: ${registryPath}`)
  console.log(`Dataset: ${datasetDir}`)
})

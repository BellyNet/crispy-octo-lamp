'use strict'

/**
 * Match legacy coomerfans.com registry sources against OnlyHaven/cum.st.
 *
 * This script is intentionally report-only. It scans model_aliases.json for
 * sources.coomer entries on coomerfans.com, searches OnlyHaven by creator name
 * and service, then writes exact/ambiguous/not_found/error classifications.
 *
 * Usage:
 *   node hoghaul/match-coomerfans-onlyhaven.js [--model=name] [--limit=n]
 *
 * Options:
 *   --model, --models   Comma-separated canonical model filters.
 *   --limit             Stop after this many legacy sources.
 *   --delay-ms=300      Delay between OnlyHaven requests.
 *   --out=path          Override JSON report path.
 *   --registry=path     Override model_aliases.json path.
 */

const fs = require('fs')
const path = require('path')
const minimist = require('minimist')

const { createHttpClient } = require('../scrapyard/httpClient')
const { loadModelRegistry, sanitize } = require('../scrapyard/modelRegistry')

const ONLYHAVEN_ORIGIN = 'https://cum.st'
const DEFAULT_DELAY_MS = 300
const DEFAULT_RESULT_LIMIT = 5

const argv = minimist(process.argv.slice(2), {
  string: ['model', 'models', 'out', 'registry', 'delay-ms', 'limit', 'n'],
})

const rootDir = path.join(__dirname, '..')
const registryPath = path.resolve(
  String(argv.registry || path.join(rootDir, 'model_aliases.json'))
)
const delayMs = Math.max(
  0,
  Number.parseInt(String(argv['delay-ms'] || DEFAULT_DELAY_MS), 10) ||
    DEFAULT_DELAY_MS
)
const candidateLimit = Math.max(
  1,
  Number.parseInt(String(argv.n || DEFAULT_RESULT_LIMIT), 10) ||
    DEFAULT_RESULT_LIMIT
)
const sourceLimit = Math.max(
  0,
  Number.parseInt(String(argv.limit || 0), 10) || 0
)

function nowStamp() {
  return new Date().toISOString().replace(/[:.]/g, '-')
}

const outPath = path.resolve(
  String(
    argv.out ||
      path.join(rootDir, 'tmp', `onlyhaven-coomerfans-match-${nowStamp()}.json`)
  )
)

function sleep(ms) {
  if (!ms) return Promise.resolve()
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function parseListArg(value) {
  const values = Array.isArray(value) ? value : [value]
  return values
    .filter((item) => item !== undefined && item !== null)
    .flatMap((item) => String(item).split(','))
    .map((item) => item.trim())
    .filter(Boolean)
}

const modelFilters = new Set(
  [...parseListArg(argv.model), ...parseListArg(argv.models)].map((name) =>
    sanitize(name)
  )
)

function normalizeCreatorName(value) {
  return String(value || '')
    .trim()
    .replace(/^@+/, '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')
}

function unwrapJsonPayload(result) {
  if (result && Object.prototype.hasOwnProperty.call(result, 'data')) {
    return result.data
  }
  return result
}

function parseLegacyCoomerfansUrl(rawUrl) {
  if (!rawUrl) return null
  let parsed
  try {
    parsed = new URL(rawUrl)
  } catch {
    return null
  }

  const host = parsed.hostname.replace(/^www\./i, '').toLowerCase()
  if (host !== 'coomerfans.com') return null

  const parts = parsed.pathname.split('/').filter(Boolean)
  if (parts[0] === 'u' && parts.length >= 4) {
    return {
      service: parts[1].toLowerCase(),
      userId: parts[2],
      username: decodeURIComponent(parts.slice(3).join('/')),
    }
  }

  if (parts.length >= 3 && parts[1] === 'user') {
    return {
      service: parts[0].toLowerCase(),
      userId: null,
      username: decodeURIComponent(parts.slice(2).join('/')),
    }
  }

  return null
}

function getSourceUrl(source) {
  if (typeof source === 'string') return source
  return source?.url || ''
}

function getLegacySources(registry) {
  const sources = []
  for (const [model, entry] of Object.entries(registry || {})) {
    if (modelFilters.size && !modelFilters.has(sanitize(model))) continue
    const coomerSources = Array.isArray(entry?.sources?.coomer)
      ? entry.sources.coomer
      : []
    coomerSources.forEach((source, sourceIndex) => {
      const url = getSourceUrl(source)
      const parsed = parseLegacyCoomerfansUrl(url)
      if (!parsed) return
      sources.push({
        model,
        aliases: Array.isArray(entry?.aliases) ? entry.aliases : [],
        sourceIndex,
        source,
        url,
        parsed,
      })
    })
  }
  return sourceLimit > 0 ? sources.slice(0, sourceLimit) : sources
}

function getSearchNames(item) {
  const names = [
    item.parsed.username,
    item.source?.username,
    item.source?.discoveredAs,
    item.model,
    ...item.aliases,
  ]
  const seen = new Set()
  return names
    .map((name) => String(name || '').trim())
    .filter(Boolean)
    .filter((name) => {
      const key = normalizeCreatorName(name)
      if (!key || seen.has(key)) return false
      seen.add(key)
      return true
    })
}

function slimCandidate(candidate) {
  return {
    id: candidate?.id === undefined ? null : String(candidate.id),
    service: candidate?.service || null,
    name: candidate?.name || null,
    displayName: candidate?.displayName || null,
    postCount: candidate?.postCount ?? null,
    videoCount: candidate?.videoCount ?? null,
    imageCount: candidate?.imageCount ?? null,
    url:
      candidate?.service && candidate?.id
        ? `${ONLYHAVEN_ORIGIN}/creators/${candidate.service}/${candidate.id}`
        : null,
  }
}

async function searchOnlyHaven(http, service, query) {
  const url = `${ONLYHAVEN_ORIGIN}/api/v1/creators?q=${encodeURIComponent(
    query
  )}&service=${encodeURIComponent(service)}&n=${candidateLimit}`
  const data = unwrapJsonPayload(
    await http.fetchJson(url, {
      headers: {
        Accept: 'application/json',
        Referer: `${ONLYHAVEN_ORIGIN}/`,
      },
    })
  )
  const creators = Array.isArray(data?.creators) ? data.creators : []
  return {
    total: Number.isFinite(data?.total) ? data.total : creators.length,
    creators,
  }
}

async function matchOne(http, item) {
  const service = item.parsed.service
  const expectedName = normalizeCreatorName(item.parsed.username)
  const searchNames = getSearchNames(item)
  const searched = []
  const candidatesByKey = new Map()

  for (const query of searchNames) {
    const result = await searchOnlyHaven(http, service, query)
    searched.push({ query, total: result.total })
    for (const candidate of result.creators) {
      const key = `${candidate?.service || ''}:${candidate?.id || ''}`
      if (!candidatesByKey.has(key)) candidatesByKey.set(key, candidate)
    }

    const hasExact = result.creators.some(
      (candidate) =>
        candidate?.service === service &&
        normalizeCreatorName(candidate?.name) === expectedName
    )
    if (hasExact) break
  }

  const candidates = Array.from(candidatesByKey.values())
  const exactCandidates = candidates.filter(
    (candidate) =>
      candidate?.service === service &&
      normalizeCreatorName(candidate?.name) === expectedName
  )
  const sameServiceCandidates = candidates.filter(
    (candidate) => candidate?.service === service
  )

  const base = {
    model: item.model,
    sourceIndex: item.sourceIndex,
    oldUrl: item.url,
    oldService: service,
    oldUserId: item.parsed.userId,
    oldUsername: item.parsed.username,
    searched,
    candidates: sameServiceCandidates.map(slimCandidate),
  }

  if (exactCandidates.length === 1) {
    const match = slimCandidate(exactCandidates[0])
    return {
      ...base,
      status: 'exact',
      match,
      suggestedUrl: match.url,
    }
  }

  if (exactCandidates.length > 1 || sameServiceCandidates.length > 0) {
    return {
      ...base,
      status: 'ambiguous',
      reason:
        exactCandidates.length > 1
          ? 'multiple exact creator-name matches'
          : 'same-service candidates did not exactly match the legacy username',
    }
  }

  return {
    ...base,
    status: 'not_found',
    reason: 'OnlyHaven returned no same-service candidates for searched names',
  }
}

function summarize(results) {
  const summary = {
    scanned: results.length,
    exact: 0,
    ambiguous: 0,
    not_found: 0,
    error: 0,
  }
  for (const result of results) {
    if (Object.prototype.hasOwnProperty.call(summary, result.status)) {
      summary[result.status] += 1
    }
  }
  return summary
}

async function main() {
  const registry = loadModelRegistry(registryPath)
  const legacySources = getLegacySources(registry)
  const http = createHttpClient({ timeoutMs: 20000 })
  const results = []

  console.log(
    `Matching ${legacySources.length} legacy coomerfans source(s) against OnlyHaven...`
  )

  for (let index = 0; index < legacySources.length; index += 1) {
    const item = legacySources[index]
    const label = `${item.model} ${item.parsed.service}/${item.parsed.username}`
    process.stdout.write(`[${index + 1}/${legacySources.length}] ${label} ... `)
    try {
      const result = await matchOne(http, item)
      results.push(result)
      console.log(result.status)
    } catch (err) {
      results.push({
        model: item.model,
        sourceIndex: item.sourceIndex,
        oldUrl: item.url,
        oldService: item.parsed.service,
        oldUserId: item.parsed.userId,
        oldUsername: item.parsed.username,
        status: 'error',
        error: err?.message || String(err),
      })
      console.log(`error: ${err?.message || err}`)
    }
    if (index < legacySources.length - 1) await sleep(delayMs)
  }

  const report = {
    generatedAt: new Date().toISOString(),
    registryPath,
    onlyHavenOrigin: ONLYHAVEN_ORIGIN,
    filters: {
      models: Array.from(modelFilters),
      limit: sourceLimit || null,
      delayMs,
      candidateLimit,
    },
    summary: summarize(results),
    results,
  }

  fs.mkdirSync(path.dirname(outPath), { recursive: true })
  fs.writeFileSync(outPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8')

  console.log(`\nSummary: ${JSON.stringify(report.summary)}`)
  console.log(`Report: ${outPath}`)
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err)
  process.exitCode = 1
})

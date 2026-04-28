'use strict'

/**
 * modelRegistry.js — single source of truth for all model names and sources.
 *
 * Registry format:
 *   {
 *     "canonical_name": {
 *       "aliases": ["alias1", "alias2"],
 *       "sources": {
 *         "stufferdb": [{ url, categoryId, discoveredAs, lastCheckedAt }],
 *         "coomer":    [{ url, service, discoveredAs, lastCheckedAt }],
 *         // any future platform follows the same pattern
 *       }
 *     }
 *   }
 *
 * Each scraper calls resolveAndTrackModel() with its own platform name.
 * A future "update all" runner can iterate sources per platform and re-scrape.
 */

const fs = require('fs')

// ─── SANITIZE ─────────────────────────────────────────────────────────────────
function sanitize(name) {
  return String(name || '')
    .replace(/[^a-z0-9_\-]/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase()
}

// ─── LOAD / SAVE ──────────────────────────────────────────────────────────────
function loadModelRegistry(registryPath) {
  if (!fs.existsSync(registryPath)) {
    fs.writeFileSync(registryPath, JSON.stringify({}, null, 2))
    return {}
  }
  try {
    const raw = fs.readFileSync(registryPath, 'utf-8').trim()
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' ? parsed : {}
  } catch (err) {
    console.warn(
      `⚠️ Could not parse model registry at ${registryPath}: ${err.message}`
    )
    return {}
  }
}

function saveModelRegistry(registryPath, registry) {
  fs.writeFileSync(
    registryPath,
    JSON.stringify(sortModelRegistry(registry), null, 2) + '\n'
  )
}

// ─── SORT ─────────────────────────────────────────────────────────────────────
function sortStringValues(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b)
  )
}

function sortPlatformSources(sources) {
  if (!Array.isArray(sources)) return []
  return [...sources].sort((a, b) => {
    const left = String(a?.discoveredAs || a?.url || '')
    const right = String(b?.discoveredAs || b?.url || '')
    return left.localeCompare(right)
  })
}

function sortModelRegistry(registry) {
  return Object.fromEntries(
    Object.entries(registry || {})
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([canonicalName, entry]) => [
        canonicalName,
        {
          aliases: sortStringValues(entry?.aliases),
          sources: Object.fromEntries(
            Object.entries(entry?.sources || {})
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([platform, srcs]) => [platform, sortPlatformSources(srcs)])
          ),
        },
      ])
  )
}

// ─── SHAPE ────────────────────────────────────────────────────────────────────
function ensureModelEntryShape(entry, canonicalName) {
  const aliasSet = new Set(
    Array.isArray(entry?.aliases) ? entry.aliases.filter(Boolean) : []
  )
  if (canonicalName) aliasSet.add(canonicalName)
  return {
    aliases: Array.from(aliasSet),
    sources:
      entry?.sources && typeof entry.sources === 'object' ? entry.sources : {},
  }
}

// ─── LOOKUP ───────────────────────────────────────────────────────────────────
function findCanonicalModelName(registry, rawName) {
  const normalizedRaw = sanitize(rawName)
  if (!normalizedRaw) return null
  for (const [canonicalName, entry] of Object.entries(registry)) {
    if (sanitize(canonicalName) === normalizedRaw) return canonicalName
    const aliases = Array.isArray(entry?.aliases) ? entry.aliases : []
    if (aliases.some((alias) => sanitize(alias) === normalizedRaw))
      return canonicalName
  }
  return null
}

// ─── PLATFORM UPSERTS ─────────────────────────────────────────────────────────

function upsertStufferdbSource(entry, sourceUrl, rawName) {
  const cleanedUrl = String(sourceUrl || '').replace(/&acs=[^&]+/gi, '')
  const categoryId = cleanedUrl.match(/category\/?(\d+)/)?.[1] || null
  const now = new Date().toISOString()

  if (!Array.isArray(entry.sources.stufferdb)) entry.sources.stufferdb = []

  const idx = entry.sources.stufferdb.findIndex(
    (s) => s?.url === cleanedUrl || (categoryId && s?.categoryId === categoryId)
  )
  const next = {
    url: cleanedUrl,
    categoryId,
    discoveredAs: rawName,
    lastCheckedAt: now,
  }

  if (idx >= 0) {
    entry.sources.stufferdb[idx] = { ...entry.sources.stufferdb[idx], ...next }
  } else {
    entry.sources.stufferdb.push(next)
  }
}

function upsertCoomerSource(entry, sourceUrl, rawName) {
  const url = String(sourceUrl || '').trim()
  const now = new Date().toISOString()

  // e.g. https://coomer.st/onlyfans/user/username → service = 'onlyfans'
  const service = url.replace(/^https?:\/\/[^/]+\//, '').split('/')[0] || null

  if (!Array.isArray(entry.sources.coomer)) entry.sources.coomer = []

  const idx = entry.sources.coomer.findIndex((s) => s?.url === url)
  const next = { url, service, discoveredAs: rawName, lastCheckedAt: now }

  if (idx >= 0) {
    entry.sources.coomer[idx] = { ...entry.sources.coomer[idx], ...next }
  } else {
    entry.sources.coomer.push(next)
  }
}

// Generic fallback for future platforms — stores url + discoveredAs
function upsertGenericSource(entry, platform, sourceUrl, rawName) {
  const url = String(sourceUrl || '').trim()
  const now = new Date().toISOString()

  if (!Array.isArray(entry.sources[platform])) entry.sources[platform] = []

  const idx = entry.sources[platform].findIndex((s) => s?.url === url)
  const next = { url, discoveredAs: rawName, lastCheckedAt: now }

  if (idx >= 0) {
    entry.sources[platform][idx] = { ...entry.sources[platform][idx], ...next }
  } else {
    entry.sources[platform].push(next)
  }
}

// ─── RESOLVE & TRACK ──────────────────────────────────────────────────────────

/**
 * Finds or creates the canonical name for rawName, records the alias, and
 * upserts the source URL under the given platform. Returns the canonical name.
 *
 * platform: 'stufferdb' | 'coomer' | any future string
 * sourceUrl: the full URL for this model on that platform (pass null to skip source upsert)
 */
function resolveAndTrackModel(
  registryPath,
  rawName,
  platform,
  sourceUrl,
  canonicalOverride
) {
  const registry = loadModelRegistry(registryPath)
  const cleanedRawName = sanitize(rawName) || 'unknown_model'
  const cleanedCanonicalOverride = sanitize(canonicalOverride)
  const existingCanonical = cleanedCanonicalOverride
    ? findCanonicalModelName(registry, cleanedCanonicalOverride)
    : findCanonicalModelName(registry, cleanedRawName)
  const canonicalName =
    existingCanonical || cleanedCanonicalOverride || cleanedRawName

  registry[canonicalName] = ensureModelEntryShape(
    registry[canonicalName],
    canonicalName
  )

  const aliases = registry[canonicalName].aliases
  if (!aliases.some((alias) => sanitize(alias) === cleanedRawName)) {
    aliases.push(cleanedRawName)
  }
  registry[canonicalName].aliases = sortStringValues(aliases)

  if (!registry[canonicalName].sources) registry[canonicalName].sources = {}

  if (sourceUrl) {
    if (platform === 'stufferdb') {
      upsertStufferdbSource(registry[canonicalName], sourceUrl, cleanedRawName)
    } else if (platform === 'coomer') {
      upsertCoomerSource(registry[canonicalName], sourceUrl, cleanedRawName)
    } else {
      upsertGenericSource(
        registry[canonicalName],
        platform,
        sourceUrl,
        cleanedRawName
      )
    }
  }

  saveModelRegistry(registryPath, registry)
  return canonicalName
}

module.exports = {
  sanitize,
  loadModelRegistry,
  saveModelRegistry,
  sortModelRegistry,
  findCanonicalModelName,
  ensureModelEntryShape,
  resolveAndTrackModel,
  upsertStufferdbSource,
  upsertCoomerSource,
  upsertGenericSource,
}

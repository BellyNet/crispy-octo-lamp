'use strict'

// Builds the payload for GET /api/discover — generated stats and
// similarity-based "you might like" recommendations. No chat, no LLM calls:
// everything here is arithmetic over local visit counts, model stats, and
// CLIP embeddings computed by dashboard/embed/compute_embeddings.py.
//
// An earlier version of this replaced the recommendation row with CLIP
// zero-shot body-type categories (thick/curvy, bbw, ssbbw, ...). That was
// reverted — testing showed the category text prompts embed at 0.82-0.92
// cosine similarity to each other regardless of wording, model size, or
// classification method, so CLIP just can't separate them for this content.
// Image-to-image similarity (this file) doesn't hit that problem.

const fs = require('fs')
const path = require('path')

const RECENT_WINDOW_DAYS = 14
const ADDED_WINDOW_DAYS = 7
const SEED_COUNT = 5
const RECOMMEND_COUNT = 12
const TOP_LIST_COUNT = 8

function loadJsonSafe(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    return fallback
  }
}

function cosineSim(a, b) {
  let dot = 0
  let na = 0
  let nb = 0
  const n = Math.min(a.length, b.length)
  for (let i = 0; i < n; i++) {
    dot += a[i] * b[i]
    na += a[i] * a[i]
    nb += b[i] * b[i]
  }
  if (na === 0 || nb === 0) return 0
  return dot / (Math.sqrt(na) * Math.sqrt(nb))
}

// modelNames: live model dir names (source of truth — a fresh datasetDir
//   listing — so stale/removed models in visits.json or embeddings.json
//   never leak into the output).
// statsByName: modelStatsCache-shaped map { name: { latestAddedMs, fileCount,
//   totalBytes, ... } }
// visitsData: shape from VisitTracker#getVisits()
// thumbDir: to locate embeddings.json
function buildDiscoverPayload({ modelNames, statsByName, visitsData, thumbDir }) {
  const modelNameSet = new Set(modelNames)
  const rawEmbeddings = loadJsonSafe(path.join(thumbDir, 'embeddings.json'), {})
  const embeddings = Object.fromEntries(
    Object.entries(rawEmbeddings).filter(([name]) => modelNameSet.has(name))
  )
  const hasEmbeddings = Object.keys(embeddings).length > 0
  const embeddingsUpdatedAt = hasEmbeddings
    ? Object.values(embeddings)
        .map((e) => e.computedAt)
        .filter(Boolean)
        .sort()
        .pop() || null
    : null

  const now = Date.now()
  const recentCutoff = now - RECENT_WINDOW_DAYS * 24 * 60 * 60 * 1000

  const visitEntries = modelNames
    .map((name) => ({ name, v: visitsData[name] }))
    .filter((e) => e.v)
  const hasVisits = visitEntries.length > 0

  const mostVisited = [...visitEntries]
    .sort((a, b) => (b.v.totalCount || 0) - (a.v.totalCount || 0))
    .slice(0, TOP_LIST_COUNT)
    .map((e) => ({
      name: e.name,
      totalCount: e.v.totalCount,
      lastVisitedAt: e.v.lastVisitedAt,
    }))

  const trending = visitEntries
    .map((e) => ({
      name: e.name,
      recentCount: (e.v.recent || []).filter(
        (ts) => new Date(ts).getTime() >= recentCutoff
      ).length,
      totalCount: e.v.totalCount || 0,
    }))
    .filter((e) => e.recentCount > 0)
    .sort((a, b) => b.recentCount - a.recentCount || b.totalCount - a.totalCount)
    .slice(0, TOP_LIST_COUNT)

  const totalModels = modelNames.length
  const totalFiles = modelNames.reduce(
    (s, n) => s + (statsByName[n]?.fileCount || 0),
    0
  )
  const totalBytes = modelNames.reduce(
    (s, n) => s + (statsByName[n]?.totalBytes || 0),
    0
  )
  const addedCutoff = now - ADDED_WINDOW_DAYS * 24 * 60 * 60 * 1000
  const recentlyAddedCount = modelNames.filter(
    (n) => (statsByName[n]?.latestAddedMs || 0) >= addedCutoff
  ).length

  const recentlyAdded = [...modelNames]
    .sort(
      (a, b) => (statsByName[b]?.latestAddedMs || 0) - (statsByName[a]?.latestAddedMs || 0)
    )
    .slice(0, RECOMMEND_COUNT)
    .map((name) => ({ name, reason: 'Recently added', score: 0 }))

  let recommended = recentlyAdded
  let coldStart = true

  if (hasEmbeddings && mostVisited.length > 0) {
    const seeds = mostVisited
      .map((m) => m.name)
      .slice(0, SEED_COUNT)
      .filter((n) => embeddings[n])
    if (seeds.length > 0) {
      const excluded = new Set(mostVisited.map((m) => m.name))
      const best = new Map() // candidateName -> { score, seed }
      for (const seed of seeds) {
        const seedVec = embeddings[seed].vector
        for (const name of Object.keys(embeddings)) {
          if (excluded.has(name) || name === seed) continue
          const score = cosineSim(seedVec, embeddings[name].vector)
          const prev = best.get(name)
          if (!prev || score > prev.score) best.set(name, { score, seed })
        }
      }
      const ranked = [...best.entries()]
        .sort((a, b) => b[1].score - a[1].score)
        .slice(0, RECOMMEND_COUNT)
        .map(([name, { score, seed }]) => ({
          name,
          reason: `Similar to ${seed}`,
          score,
        }))
      if (ranked.length > 0) {
        recommended = ranked
        coldStart = false
      }
    }
  }

  return {
    stats: { totalModels, totalFiles, totalBytes, recentlyAddedCount },
    mostVisited,
    trending,
    recommended,
    meta: { hasVisits, hasEmbeddings, coldStart, embeddingsUpdatedAt },
  }
}

module.exports = { buildDiscoverPayload, cosineSim }

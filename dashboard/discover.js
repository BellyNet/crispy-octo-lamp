'use strict'

// Builds the payload for GET /api/discover — generated stats and a mixed
// "discover" carousel. No chat, no LLM calls, no CLIP: everything here is
// plain arithmetic over local visit counts and model stats.
//
// This replaces an earlier CLIP-embedding image-similarity recommender
// (dashboard/embed/compute_embeddings.py's output). Pulled 2026-09 after
// checking the actual numbers on the live dataset: cosine similarity
// between totally unrelated models already sat at 0.85-0.95 for the middle
// 80% of all pairs — CLIP doesn't spread this content domain out much — and
// on top of that, one single model won the "best match" slot for 158 of
// 159 models (99.4%), a hubness collapse that meant nearly everyone got
// funneled toward the same one or two "recommendations" regardless of what
// they'd actually visited. (An even earlier version tried CLIP zero-shot
// body-type categories and hit a related wall: category prompts embedded
// at 0.82-0.92 similarity to each other regardless of wording.) The
// categories below don't need an embedding space at all and can't collapse
// the same way — each is a plain sort over a single field.

const RECENT_WINDOW_DAYS = 14
const ADDED_WINDOW_DAYS = 7
const TOP_LIST_COUNT = 8
const DISCOVER_CATEGORY_CAP = 5
const DISCOVER_TOTAL_COUNT = 12

function daysAgo(iso) {
  if (!iso) return null
  return Math.floor((Date.now() - new Date(iso).getTime()) / (24 * 60 * 60 * 1000))
}

// modelNames: live model dir names (source of truth — a fresh datasetDir
//   listing — so stale/removed models in visits.json never leak into the
//   output).
// statsByName: modelStatsCache-shaped map { name: { latestAddedMs, fileCount,
//   totalBytes, ... } }
// visitsData: shape from VisitTracker#getVisits()
function buildDiscoverPayload({ modelNames, statsByName, visitsData }) {
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

  // ── Discover carousel ───────────────────────────────────────────────────
  // Three plain categories, mixed together and deduped (`used`) so a model
  // only ever shows up once, in whichever category is most specific about
  // it. Order matters: more specific/informative categories claim a model
  // before the generic "recently updated" catch-all gets a chance to.
  const visitedSet = new Set(visitEntries.map((e) => e.name))
  const used = new Set()

  // 1) Active again: a model you've visited before that has picked up new
  // files recently — "you know this one, and it's active again."
  const activeAgain = modelNames
    .filter(
      (n) => visitedSet.has(n) && (statsByName[n]?.latestAddedMs || 0) >= recentCutoff
    )
    .sort(
      (a, b) => (statsByName[b]?.latestAddedMs || 0) - (statsByName[a]?.latestAddedMs || 0)
    )
    .slice(0, DISCOVER_CATEGORY_CAP)
    .map((name) => {
      used.add(name)
      return { name, reason: 'Active again — new content', score: 0 }
    })

  // 2) Haven't visited in a while: needs actual visit history, just stale —
  // a model you've never opened isn't "overdue," it's just unexplored
  // (that's what the recently-updated catch-all below is for instead).
  const dueForRevisit = [...visitEntries]
    .filter((e) => !used.has(e.name))
    .sort((a, b) => new Date(a.v.lastVisitedAt) - new Date(b.v.lastVisitedAt))
    .slice(0, DISCOVER_CATEGORY_CAP)
    .map((e) => {
      used.add(e.name)
      const days = daysAgo(e.v.lastVisitedAt)
      return {
        name: e.name,
        reason: days == null ? "Haven't visited in a while" : `Last visited ${days}d ago`,
        score: 0,
      }
    })

  // 3) Recently updated: plain newest-content-first, filling whatever's
  // left. The only category that works with zero visit history, so this
  // is also the cold-start fallback (a brand-new install shows only this).
  const remaining = Math.max(0, DISCOVER_TOTAL_COUNT - used.size)
  const recentlyUpdated = [...modelNames]
    .filter((n) => !used.has(n))
    .sort(
      (a, b) => (statsByName[b]?.latestAddedMs || 0) - (statsByName[a]?.latestAddedMs || 0)
    )
    .slice(0, remaining)
    .map((name) => {
      used.add(name)
      return { name, reason: 'Recently updated', score: 0 }
    })

  const recommended = [...activeAgain, ...dueForRevisit, ...recentlyUpdated]

  return {
    stats: { totalModels, totalFiles, totalBytes, recentlyAddedCount },
    mostVisited,
    trending,
    recommended,
    meta: { hasVisits },
  }
}

module.exports = { buildDiscoverPayload }

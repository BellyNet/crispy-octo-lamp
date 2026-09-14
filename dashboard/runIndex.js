'use strict'

// Persistent "which scrape run does this file belong to" index —
// THUMB_DIR/run-index.json. This is purely a derived view over the current
// dataset (never original data), so it's safe to fully recompute on every
// refresh rather than only extending it — see rebuild() below for why that
// matters more than the "only look at new files" incremental approach this
// used to take.
//
// This is what lets /api/recent-media just read a JSON file instead of
// fanning out scanModel() across every model on every request.

const fs = require('fs')
const path = require('path')

const RUN_GAP_MS = 3 * 60 * 60 * 1000 // 3 hours — see rebuild()'s comment.

class RunIndex {
  constructor(thumbDir) {
    this.file = path.join(thumbDir, 'run-index.json')
    this.runs = this._load() // newest-first
  }

  _load() {
    try {
      const raw = JSON.parse(fs.readFileSync(this.file, 'utf8'))
      if (raw && Array.isArray(raw.runs)) return raw.runs
    } catch {}
    return []
  }

  _save() {
    const tmp = this.file + '.tmp'
    fs.writeFileSync(tmp, JSON.stringify({ version: 2, runs: this.runs }))
    fs.renameSync(tmp, this.file)
  }

  // items: flat array of already-formatted response items — see
  // refreshRunIndex() in server.js for the shape, including the optional
  // invocationKey used below.
  //
  // Buckets by addedMs gaps (RUN_GAP_MS): real data (500 items, 2026-09-01)
  // showed gaps of 10-80 min WITHIN one continuous automated multi-model
  // scrape session vs. 1,500-11,000+ min (1-8 days) BETWEEN separate
  // sessions — a clean bimodal split. But that same 10-80 min range also
  // covers several distinct one-off single-model scrapes run back-to-back
  // by hand (e.g. working through a queue via scrape:interactive) — a pure
  // time gap can't tell those apart from one continuous automated batch,
  // since both produce the same-looking gaps.
  //
  // So items additionally carry invocationKey when available: the
  // startedAt (ms) of the specific milkmaid/hoghaul invocation recorded in
  // that model's own -last-run.json, if the item's addedMs falls inside
  // that invocation's window (see findInvocationKey() in server.js). Two
  // adjacent items with different known invocationKeys always split into
  // separate runs, regardless of how small the time gap is — that's a
  // real "this came from a different scrape command" signal the gap
  // heuristic can't produce on its own. Items without a resolvable
  // invocationKey (most historical data — each -last-run.json only ever
  // holds the model's MOST RECENT invocation, so older batches lose this
  // signal once it's overwritten by a newer scrape) fall back to the gap
  // heuristic exactly as before.
  //
  // Always rebuilding fully (not just appending new items to permanently-
  // fixed old buckets) means a fix to this logic — or a model's -last-run
  // window simply becoming available/fresher — is reflected everywhere
  // next refresh, not just at the front. The bucketing itself is cheap
  // (pure in-memory comparisons, no I/O per item), so there's no
  // meaningful cost to recomputing it for the whole dataset every time;
  // the actual expensive part (scanModel per model) is unaffected either
  // way since it's cached separately.
  rebuild(items) {
    const sorted = [...items].sort((a, b) => (b.addedMs || 0) - (a.addedMs || 0))

    const buckets = []
    let current = null
    for (const item of sorted) {
      const startsNewBucket =
        !current ||
        current.startedAtMs - item.addedMs > RUN_GAP_MS ||
        hasConflictingInvocation(current.items, item)
      if (startsNewBucket) {
        current = { startedAtMs: item.addedMs, endedAtMs: item.addedMs, items: [] }
        buckets.push(current)
      }
      current.startedAtMs = Math.min(current.startedAtMs, item.addedMs)
      current.endedAtMs = Math.max(current.endedAtMs, item.addedMs)
      current.items.push(item)
    }

    this.runs = buckets.map((b) => ({
      runId: `r${b.startedAtMs}`,
      startedAt: new Date(b.startedAtMs).toISOString(),
      endedAt: new Date(b.endedAtMs).toISOString(),
      items: b.items,
    }))
    this._save()
    return { totalItems: sorted.length, runs: this.runs.length }
  }

  getRun(runIndex) {
    return this.runs[runIndex] || null
  }

  runsAvailable() {
    return this.runs.length
  }

  // Lightweight per-run stats for the admin "all runs" overview table —
  // deliberately excludes each run's full item list (that's what getRun()
  // is for) so this stays cheap to send even with 100+ runs.
  getRunSummaries() {
    return this.runs.map((run, runIndex) => {
      const models = new Set()
      let totalBytes = 0
      for (const item of run.items) {
        models.add(item.username)
        totalBytes += item.size || 0
      }
      return {
        runIndex,
        startedAt: run.startedAt,
        endedAt: run.endedAt,
        fileCount: run.items.length,
        modelCount: models.size,
        totalBytes,
      }
    })
  }
}

// True if `item` carries a known invocationKey that conflicts with a
// DIFFERENT known invocationKey already present in this bucket. By
// construction (this is the only place a bucket grows) a bucket only ever
// accumulates items sharing one invocation key at most, so checking
// against any single conflicting item already in it is sufficient.
function hasConflictingInvocation(bucketItems, item) {
  if (item.invocationKey == null) return false
  return bucketItems.some(
    (it) => it.invocationKey != null && it.invocationKey !== item.invocationKey
  )
}

module.exports = RunIndex

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

const RUN_GAP_MS = 15 * 60 * 1000 // 15 minutes — see rebuild()'s comment.

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
  // refreshRunIndex() in server.js for the shape.
  //
  // Each scrape invocation walks the model roster alphabetically, so
  // sorted newest-first (as below), model names trend backward through the
  // alphabet WITHIN one run (run started at "aaa", so its latest-added
  // file is from whichever model is furthest along the alphabet it got
  // to). Crossing into the previous, older run flips that: the next item
  // (older in time) jumps back UP the alphabet, because that earlier run
  // also started at "aaa" and by the time it reached "z" was already done
  // — so its LATEST file is from a late-alphabet model, higher than
  // whatever early-alphabet model the newer run had just gotten to.
  //
  // So a run boundary is: the model name goes UP instead of continuing
  // down (or staying put, for same-model files) AND the time gap clears
  // RUN_GAP_MS. Requiring both avoids two false-positive directions: a
  // same-model gap alone (one model's own files, slow to scrape, more
  // than RUN_GAP_MS apart — no alphabetic jump, so no split), and a
  // same-run alphabetic bounce with no real gap (irrelevant in practice
  // since a bounce that isn't a run boundary implies near-simultaneous
  // timestamps anyway).
  //
  // Always rebuilding fully (not just appending new items to permanently-
  // fixed old buckets) means a fix to this logic is reflected everywhere
  // next refresh, not just at the front. The bucketing itself is cheap
  // (pure in-memory comparisons, no I/O per item), so there's no
  // meaningful cost to recomputing it for the whole dataset every time;
  // the actual expensive part (scanModel per model) is unaffected either
  // way since it's cached separately.
  rebuild(items) {
    const sorted = [...items].sort((a, b) => (b.addedMs || 0) - (a.addedMs || 0))

    const buckets = []
    let current = null
    let prevItem = null
    for (const item of sorted) {
      const gapMs = prevItem ? prevItem.addedMs - item.addedMs : 0
      const alphabeticBreak = !!prevItem && item.username > prevItem.username
      const startsNewBucket = !current || (alphabeticBreak && gapMs >= RUN_GAP_MS)
      if (startsNewBucket) {
        current = { startedAtMs: item.addedMs, endedAtMs: item.addedMs, items: [] }
        buckets.push(current)
      }
      current.startedAtMs = Math.min(current.startedAtMs, item.addedMs)
      current.endedAtMs = Math.max(current.endedAtMs, item.addedMs)
      current.items.push(item)
      prevItem = item
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

module.exports = RunIndex

'use strict'

// Persistent "which scrape run does this file belong to" index —
// THUMB_DIR/run-index.json. Built incrementally: a file's addedMs (disk
// birthtime) never changes once written, so once a file is bucketed into a
// run, that assignment is permanent and never needs recomputing. Each
// refresh only has to look at files that aren't in the index yet — new
// scrape output is always newer than everything already indexed (addedMs
// is "when it landed on disk", not any backdated post date), so new items
// only ever extend or prepend at the front, never touch history.
//
// This is what lets /api/recent-media just read a JSON file instead of
// fanning out scanModel() across every model on every request.

const fs = require('fs')
const path = require('path')

const RUN_GAP_MS = 3 * 60 * 60 * 1000 // 3 hours — see server.js's old
// /api/recent-media comment for the real-data gap analysis this is based on.

class RunIndex {
  constructor(thumbDir) {
    this.file = path.join(thumbDir, 'run-index.json')
    this.runs = this._load() // newest-first
    this.indexedKeys = new Set()
    for (const run of this.runs) {
      for (const item of run.items) {
        this.indexedKeys.add(itemKey(item))
      }
    }
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
    fs.writeFileSync(tmp, JSON.stringify({ version: 1, runs: this.runs }))
    fs.renameSync(tmp, this.file)
  }

  // items: flat array of already-formatted response items — see
  // buildRunIndexItem() in server.js for the shape. Skips anything already
  // indexed, so calling this repeatedly with the full current dataset is
  // cheap once the backlog is caught up (the common case: a nightly pass
  // adds a few hundred new items out of tens of thousands total).
  addItems(items) {
    const fresh = items.filter((it) => !this.indexedKeys.has(itemKey(it)))
    if (!fresh.length) return { added: 0, newRuns: 0 }

    fresh.sort((a, b) => (b.addedMs || 0) - (a.addedMs || 0))

    // Bucket the new items among themselves first (handles catch-up after
    // being offline a while, where "new" might span several real runs).
    const newBuckets = []
    let current = null
    for (const item of fresh) {
      if (!current || current.startedAtMs - item.addedMs > RUN_GAP_MS) {
        current = { startedAtMs: item.addedMs, endedAtMs: item.addedMs, items: [] }
        newBuckets.push(current)
      }
      current.startedAtMs = Math.min(current.startedAtMs, item.addedMs)
      current.endedAtMs = Math.max(current.endedAtMs, item.addedMs)
      current.items.push(item)
    }

    // If the oldest new bucket is close enough to the existing latest run,
    // merge into it instead of creating an adjacent near-duplicate run
    // (e.g. a manual rebuild fired shortly after the nightly pass already
    // indexed most of the same batch).
    const oldestNewBucket = newBuckets[newBuckets.length - 1]
    const existingLatest = this.runs[0]
    if (
      existingLatest &&
      oldestNewBucket &&
      oldestNewBucket.startedAtMs - Date.parse(existingLatest.endedAt) <= RUN_GAP_MS
    ) {
      existingLatest.items.push(...oldestNewBucket.items)
      existingLatest.endedAt = new Date(
        Math.max(Date.parse(existingLatest.endedAt), oldestNewBucket.endedAtMs)
      ).toISOString()
      newBuckets.pop()
    }

    const newRuns = newBuckets.map((b) => ({
      runId: `r${b.startedAtMs}`,
      startedAt: new Date(b.startedAtMs).toISOString(),
      endedAt: new Date(b.endedAtMs).toISOString(),
      items: b.items,
    }))

    this.runs = [...newRuns, ...this.runs]
    for (const it of fresh) this.indexedKeys.add(itemKey(it))
    this._save()
    return { added: fresh.length, newRuns: newRuns.length }
  }

  getRun(runIndex) {
    return this.runs[runIndex] || null
  }

  runsAvailable() {
    return this.runs.length
  }
}

function itemKey(item) {
  return `${item.username}/${item.folder}/${item.filename}`
}

module.exports = RunIndex

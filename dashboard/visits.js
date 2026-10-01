'use strict'

// Persistent visit tracker — THUMB_DIR/visits.json
// Shape: { [username]: { totalCount, lastVisitedAt, recent: [iso, ...] } }
// `recent` caps at MAX_RECENT so the file never grows unbounded; it's enough
// to compute a "trending in last N days" window without storing full history.

const fs = require('fs')
const path = require('path')

const MAX_RECENT = 50

class VisitTracker {
  constructor(thumbDir) {
    this.file = path.join(thumbDir, 'visits.json')
    this.data = this._load()
  }

  _load() {
    try {
      const raw = JSON.parse(fs.readFileSync(this.file, 'utf8'))
      if (raw && typeof raw === 'object') return raw
    } catch {}
    return {}
  }

  recordVisit(username) {
    if (!username) return
    const now = new Date().toISOString()
    const entry = this.data[username] || {
      totalCount: 0,
      lastVisitedAt: null,
      recent: [],
    }
    entry.totalCount += 1
    entry.lastVisitedAt = now
    entry.recent.push(now)
    if (entry.recent.length > MAX_RECENT) {
      entry.recent = entry.recent.slice(-MAX_RECENT)
    }
    this.data[username] = entry
    try {
      fs.writeFileSync(this.file, JSON.stringify(this.data))
    } catch {}
  }

  getVisits() {
    return this.data
  }
}

module.exports = VisitTracker

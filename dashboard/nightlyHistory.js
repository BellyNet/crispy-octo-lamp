'use strict'

// Persistent nightly-pass run history — THUMB_DIR/nightly-history.json.
// Same lightweight pattern as visits.js: plain fs.writeFileSync, no deps.
// Capped at MAX_ENTRIES so the file never grows unbounded.

const fs = require('fs')
const path = require('path')

const MAX_ENTRIES = 30

class NightlyHistory {
  constructor(thumbDir) {
    this.file = path.join(thumbDir, 'nightly-history.json')
    this.entries = this._load()
  }

  _load() {
    try {
      const raw = JSON.parse(fs.readFileSync(this.file, 'utf8'))
      if (Array.isArray(raw)) return raw
    } catch {}
    return []
  }

  recordRun(entry) {
    this.entries.unshift(entry)
    if (this.entries.length > MAX_ENTRIES) {
      this.entries = this.entries.slice(0, MAX_ENTRIES)
    }
    try {
      fs.writeFileSync(this.file, JSON.stringify(this.entries))
    } catch {}
  }

  getHistory() {
    return this.entries
  }
}

module.exports = NightlyHistory

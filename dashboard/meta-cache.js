'use strict'

// Persistent per-user metadata cache stored in THUMB_DIR/meta/{username}.json
// Keys: "folder/filename"  Values: { size, mtimeMs, width?, height?, duration?, videoDate? }
// Validated by size + mtimeMs — stale if the file changed on disk.

const fs = require('fs')
const path = require('path')

class MetaCache {
  constructor(thumbDir) {
    this.metaDir = path.join(thumbDir, 'meta')
    this._users = new Map() // username → { data: {}, dirty: false }
    fs.mkdirSync(this.metaDir, { recursive: true })
  }

  _file(username) {
    return path.join(this.metaDir, `${username}.json`)
  }

  _entry(username) {
    if (!this._users.has(username)) {
      let data = {}
      try { data = JSON.parse(fs.readFileSync(this._file(username), 'utf8')) } catch {}
      this._users.set(username, { data, dirty: false })
    }
    return this._users.get(username)
  }

  // Returns the cached metadata if the file hasn't changed, otherwise null.
  get(username, folder, filename, stat) {
    const { data } = this._entry(username)
    const e = data[`${folder}/${filename}`]
    if (!e) return null
    if (e.size !== stat.size || e.mtimeMs !== stat.mtimeMs) return null
    return e
  }

  // Stores metadata for a file. Call flush() to persist.
  set(username, folder, filename, stat, meta) {
    const entry = this._entry(username)
    entry.data[`${folder}/${filename}`] = {
      size: stat.size,
      mtimeMs: stat.mtimeMs,
      ...meta,
    }
    entry.dirty = true
  }

  // Write dirty caches to disk.
  flush(username) {
    const entry = this._users.get(username)
    if (!entry?.dirty) return
    try {
      fs.writeFileSync(this._file(username), JSON.stringify(entry.data))
      entry.dirty = false
    } catch {}
  }

  flushAll() {
    for (const username of this._users.keys()) this.flush(username)
  }
}

module.exports = MetaCache

const fs = require('fs')

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

function sortRefs(refs) {
  return [...refs].sort((a, b) => a.localeCompare(b))
}

function sanitizeRef(metadata) {
  if (typeof metadata === 'string') {
    const relativePath = normalizePath(metadata)
    return relativePath || null
  }

  if (!metadata || typeof metadata !== 'object') return null

  const relativePath = normalizePath(metadata.relativePath || '')
  return relativePath || null
}

function normalizeEntry(hash, entry) {
  const refs = Array.isArray(entry?.refs)
    ? entry.refs.map((ref) => sanitizeRef(ref)).filter(Boolean)
    : []

  return {
    hash,
    refs,
  }
}

function parseEntries(parsed) {
  const entries = new Map()

  if (Array.isArray(parsed)) {
    for (const hash of parsed) {
      if (!hash) continue
      entries.set(String(hash), {
        hash: String(hash),
        refs: [],
      })
    }
    return entries
  }

  const rawEntries = Array.isArray(parsed?.entries)
    ? parsed.entries
    : parsed?.entries && typeof parsed.entries === 'object'
      ? Object.values(parsed.entries)
      : []

  for (const rawEntry of rawEntries) {
    const hash = String(rawEntry?.hash || '')
    if (!hash) continue
    entries.set(hash, normalizeEntry(hash, rawEntry))
  }

  return entries
}

function createHashStore({ storePath, kind, algorithm }) {
  let entries = new Map()

  function load() {
    if (!fs.existsSync(storePath)) {
      entries = new Map()
      return
    }

    try {
      const raw = fs.readFileSync(storePath, 'utf-8').replace(/^\uFEFF/, '')
      const parsed = raw.trim() ? JSON.parse(raw) : []
      entries = parseEntries(parsed)
    } catch (err) {
      console.warn(`Failed to load ${kind} hash cache: ${err.message}`)
      entries = new Map()
    }
  }

  function save() {
    const payload = {
      version: 2,
      kind,
      algorithm,
      entryCount: entries.size,
      entries: [...entries.values()]
        .map((entry) => ({
          hash: entry.hash,
          refs: sortRefs(entry.refs || []),
        }))
        .sort((a, b) => a.hash.localeCompare(b.hash)),
    }

    fs.writeFileSync(storePath, JSON.stringify(payload, null, 2))
  }

  function has(hash) {
    return entries.has(hash)
  }

  function get(hash) {
    return entries.get(hash) || null
  }

  function add(hash, metadata) {
    if (!hash) return null

    const existing = entries.get(hash) || {
      hash,
      refs: [],
    }

    const ref = sanitizeRef(metadata)
    if (ref) {
      if (!existing.refs.includes(ref)) existing.refs.push(ref)
    }

    entries.set(hash, existing)
    return existing
  }

  function getAllEntries() {
    return [...entries.values()]
  }

  return {
    load,
    save,
    has,
    get,
    add,
    getAllEntries,
  }
}

module.exports = {
  createHashStore,
}

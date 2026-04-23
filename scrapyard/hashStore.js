const fs = require('fs')
const path = require('path')

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

function sortRefs(refs) {
  return [...refs].sort((a, b) =>
    `${a.root || ''}:${a.relativePath || ''}`.localeCompare(
      `${b.root || ''}:${b.relativePath || ''}`
    )
  )
}

function sanitizeRef(metadata, now) {
  if (!metadata || typeof metadata !== 'object') return null

  const relativePath = normalizePath(metadata.relativePath || '')
  if (!relativePath) return null

  const segments = relativePath.split('/').filter(Boolean)

  return {
    root: metadata.root || 'dataset',
    model: metadata.model || segments[0] || null,
    bucket: metadata.bucket || segments[1] || null,
    relativePath,
    filename: metadata.filename || path.basename(relativePath),
    mediaType: metadata.mediaType || null,
    sizeBytes: Number.isFinite(metadata.sizeBytes) ? metadata.sizeBytes : null,
    modifiedAt: metadata.modifiedAt || null,
    source: metadata.source || null,
    firstSeenAt: metadata.firstSeenAt || now,
    lastSeenAt: metadata.lastSeenAt || now,
  }
}

function normalizeEntry(hash, entry, now) {
  const refs = Array.isArray(entry?.refs)
    ? entry.refs.map((ref) => sanitizeRef(ref, now)).filter(Boolean)
    : []

  return {
    hash,
    firstSeenAt: entry?.firstSeenAt || now,
    lastSeenAt: entry?.lastSeenAt || now,
    refs,
  }
}

function parseEntries(parsed, now) {
  const entries = new Map()

  if (Array.isArray(parsed)) {
    for (const hash of parsed) {
      if (!hash) continue
      entries.set(String(hash), {
        hash: String(hash),
        firstSeenAt: now,
        lastSeenAt: now,
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
    entries.set(hash, normalizeEntry(hash, rawEntry, now))
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
      entries = parseEntries(parsed, new Date().toISOString())
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
      updatedAt: new Date().toISOString(),
      entryCount: entries.size,
      entries: [...entries.values()]
        .map((entry) => ({
          hash: entry.hash,
          firstSeenAt: entry.firstSeenAt || null,
          lastSeenAt: entry.lastSeenAt || null,
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

    const now = new Date().toISOString()
    const existing = entries.get(hash) || {
      hash,
      firstSeenAt: now,
      lastSeenAt: now,
      refs: [],
    }

    existing.lastSeenAt = now

    const ref = sanitizeRef(metadata, now)
    if (ref) {
      const refKey = `${ref.root}:${ref.relativePath}`
      const matchIndex = existing.refs.findIndex(
        (candidate) =>
          `${candidate.root || 'dataset'}:${candidate.relativePath || ''}` ===
          refKey
      )

      if (matchIndex >= 0) {
        existing.refs[matchIndex] = {
          ...existing.refs[matchIndex],
          ...ref,
          firstSeenAt: existing.refs[matchIndex].firstSeenAt || ref.firstSeenAt,
          lastSeenAt: now,
        }
      } else {
        existing.refs.push(ref)
      }
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

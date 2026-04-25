'use strict'

/**
 * media-dates.js
 *
 * Extracts creation dates from media files and persists results in a per-user
 * sidecar: dataset/<user>/.media-dates.json
 *
 * Sidecar format:
 *   {
 *     "__version": 3,
 *     "images/filename.jpg": {
 *       "video":    "ISO" | null,   // ffprobe container creation_time (videos only)
 *       "filename": "ISO" | null,   // 14-digit timestamp prefix in filename
 *       "uploaded": "ISO" | null    // mtime set by milkmaid = platform upload date
 *     }
 *   }
 *
 * Resolution priority (first non-null wins):
 *   video → filename → uploaded
 */

const fs = require('fs')
const path = require('path')
const { execFile } = require('child_process')
const { promisify } = require('util')

const execFileAsync = promisify(execFile)

const SIDECAR_VERSION = 3
const SIDECAR_FILENAME = '.media-dates.json'

// ─── FFPROBE ──────────────────────────────────────────────────────────────────
let ffprobePath = null

const FFPROBE_CANDIDATES = [
  'ffprobe',
  '/e/Apps/ffmpeg-2025-07-17-git-bc8d06d541-full_build/bin/ffprobe',
  '/e/Apps/ffmpeg/bin/ffprobe',
  'C:\\ffmpeg\\bin\\ffprobe.exe',
  'C:\\Program Files\\ffmpeg\\bin\\ffprobe.exe',
]

async function findFfprobe() {
  for (const p of FFPROBE_CANDIDATES) {
    try {
      await execFileAsync(p, ['-version'], { timeout: 3000 })
      ffprobePath = p
      return p
    } catch {}
  }
  return null
}

async function ensureFfprobe() {
  if (ffprobePath !== null) return ffprobePath
  return findFfprobe()
}

// ─── DATE VALIDATION ──────────────────────────────────────────────────────────
function isSane(date) {
  if (!date || !(date instanceof Date) || isNaN(date.getTime())) return false
  const y = date.getFullYear()
  return y >= 1990 && y <= 2035
}

function toISO(date) {
  return isSane(date) ? date.toISOString() : null
}

// ─── EXTRACTION: VIDEOS (from saved file via ffprobe) ─────────────────────────
async function extractVideoDateFromFile(filePath) {
  const probe = await ensureFfprobe()
  if (!probe) return null
  try {
    const { stdout } = await execFileAsync(
      probe,
      ['-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', filePath],
      { timeout: 15000 }
    )
    const info = JSON.parse(stdout)
    const candidates = []

    const ft = info?.format?.tags || {}
    if (ft.creation_time) candidates.push(ft.creation_time)
    if (ft['com.apple.quicktime.creationdate']) candidates.push(ft['com.apple.quicktime.creationdate'])
    if (ft.date) candidates.push(ft.date)

    for (const stream of info?.streams || []) {
      if (stream?.tags?.creation_time) candidates.push(stream.tags.creation_time)
    }

    for (const raw of candidates) {
      const d = new Date(raw)
      if (isSane(d)) return toISO(d)
    }
    return null
  } catch {
    return null
  }
}

// ─── EXTRACTION: FILENAME TIMESTAMP ───────────────────────────────────────────
function extractFilenameDate(filename) {
  const m = filename.match(/^(\d{14})/)
  if (!m) return null
  const d = m[1]
  const date = new Date(
    `${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}T${d.slice(8,10)}:${d.slice(10,12)}:${d.slice(12,14)}`
  )
  return isSane(date) ? toISO(date) : null
}

// ─── SIDECAR READ / WRITE ─────────────────────────────────────────────────────
const _sidecars = new Map()

function sidecarPath(userDir) {
  return path.join(userDir, SIDECAR_FILENAME)
}

function loadSidecar(userDir) {
  if (_sidecars.has(userDir)) return _sidecars.get(userDir)
  let data = { __version: SIDECAR_VERSION }
  try {
    const raw = JSON.parse(fs.readFileSync(sidecarPath(userDir), 'utf8'))
    data = raw.__version === SIDECAR_VERSION ? raw : { __version: SIDECAR_VERSION }
  } catch {}
  const entry = { data, dirty: false, flushTimer: null }
  _sidecars.set(userDir, entry)
  return entry
}

function flushSidecar(userDir) {
  const entry = _sidecars.get(userDir)
  if (!entry || !entry.dirty) return
  fs.writeFileSync(sidecarPath(userDir), JSON.stringify(entry.data), 'utf8')
  entry.dirty = false
}

function scheduleSidecarFlush(userDir) {
  const entry = _sidecars.get(userDir)
  if (!entry) return
  entry.dirty = true
  clearTimeout(entry.flushTimer)
  entry.flushTimer = setTimeout(() => flushSidecar(userDir), 1500)
}

// ─── PUBLIC API ───────────────────────────────────────────────────────────────

async function recordImageDates(userDir, folder, filename, uploadedDate) {
  const entry = loadSidecar(userDir)
  const key = `${folder}/${filename}`
  if (entry.data[key]) return

  entry.data[key] = {
    video: null,
    filename: extractFilenameDate(filename) || null,
    uploaded: uploadedDate ? toISO(uploadedDate) : null,
  }
  scheduleSidecarFlush(userDir)
}

async function recordVideoDates(userDir, folder, filename, filePath, uploadedDate) {
  const entry = loadSidecar(userDir)
  const key = `${folder}/${filename}`
  if (entry.data[key]) return

  const [video, filenameDate] = await Promise.all([
    extractVideoDateFromFile(filePath),
    Promise.resolve(extractFilenameDate(filename)),
  ])

  entry.data[key] = {
    video: video || null,
    filename: filenameDate || null,
    uploaded: uploadedDate ? toISO(uploadedDate) : null,
  }
  scheduleSidecarFlush(userDir)
}

function resolveDateFromSidecar(userDir, folder, filename) {
  const entry = loadSidecar(userDir)
  const record = entry.data[`${folder}/${filename}`]
  if (!record) return null

  if (record.video)    return { date: record.video,    source: 'mp4' }
  if (record.filename) return { date: record.filename, source: 'filename' }
  if (record.uploaded) return { date: record.uploaded, source: 'uploaded' }
  return { date: null, source: null }
}

function flushAllSidecars() {
  for (const [userDir] of _sidecars) flushSidecar(userDir)
}

module.exports = {
  recordImageDates,
  recordVideoDates,
  resolveDateFromSidecar,
  extractVideoDateFromFile,
  extractFilenameDate,
  flushAllSidecars,
  findFfprobe,
  SIDECAR_FILENAME,
}

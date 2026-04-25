'use strict'

/**
 * media-dates.js
 *
 * Extracts the best available creation date from media files/buffers and
 * persists results in a per-user sidecar: dataset/<user>/.media-dates.json
 *
 * Sidecar format:
 *   {
 *     "__version": 2,
 *     "images/filename.jpg": {
 *       "exif":     "ISO" | null,   // EXIF DateTimeOriginal / GPS / XMP / IPTC
 *       "video":    "ISO" | null,   // ffprobe container creation_time
 *       "filename": "ISO" | null,   // 14-digit timestamp prefix in filename
 *       "uploaded": "ISO" | null    // mtime set by milkmaid = platform upload date
 *     }
 *   }
 *
 * Resolution priority (first non-null wins):
 *   exif → video → filename → uploaded
 */

const fs = require('fs')
const path = require('path')
const { execFile } = require('child_process')
const { promisify } = require('util')

const execFileAsync = promisify(execFile)

const SIDECAR_VERSION = 2
const SIDECAR_FILENAME = '.media-dates.json'

// ─── EXIFR (ESM — loaded via dynamic import once) ─────────────────────────────
let _exifr = null
async function getExifr() {
  if (!_exifr) {
    const mod = await import('exifr')
    _exifr = mod.default
  }
  return _exifr
}

// ─── FFPROBE ──────────────────────────────────────────────────────────────────
let ffprobePath = null // null = not found, string = found path

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

// ─── EXTRACTION: IMAGES (from buffer) ─────────────────────────────────────────
/**
 * Extract the best available creation date from an image buffer.
 * Tries (in order): DateTimeOriginal → GPS timestamp → DateTimeDigitized →
 *   XMP/IPTC CreateDate → DateTime
 * Returns an ISO string or null.
 */
async function extractExifFromBuffer(buffer) {
  try {
    const exifr = await getExifr()
    const tags = await exifr.parse(buffer, {
      pick: [
        'DateTimeOriginal',
        'DateTimeDigitized',
        'DateTime',
        'CreateDate',      // XMP xmp:CreateDate / IPTC
        'DateCreated',     // IPTC
        'GPSDateStamp',
        'GPSTimeStamp',
      ],
      translateValues: false,
    })
    if (!tags) return null

    // Best: DateTimeOriginal — when the shutter was pressed
    if (isSane(tags.DateTimeOriginal)) return toISO(tags.DateTimeOriginal)

    // GPS timestamp — atomic clock, very accurate when present
    if (tags.GPSDateStamp && tags.GPSTimeStamp) {
      const [y, m, d] = tags.GPSDateStamp.split(':').map(Number)
      const [h, min, sec] = tags.GPSTimeStamp
      const gps = new Date(Date.UTC(y, m - 1, d, h, min, Math.floor(sec)))
      if (isSane(gps)) return toISO(gps)
    }

    // DateTimeDigitized — when the digital image was created (good for scans)
    if (isSane(tags.DateTimeDigitized)) return toISO(tags.DateTimeDigitized)

    // XMP/IPTC CreateDate / DateCreated
    if (isSane(tags.CreateDate)) return toISO(tags.CreateDate)
    if (isSane(tags.DateCreated)) return toISO(tags.DateCreated)

    // DateTime — EXIF "file change date", least reliable but better than nothing
    if (isSane(tags.DateTime)) return toISO(tags.DateTime)

    return null
  } catch {
    return null
  }
}

// ─── EXTRACTION: VIDEOS (from saved file via ffprobe) ─────────────────────────
/**
 * Extract creation date from a saved video file using ffprobe.
 * Checks format tags, per-stream tags, and Apple QuickTime fields.
 * Returns an ISO string or null.
 */
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

    // Format-level tags (most common)
    const ft = info?.format?.tags || {}
    if (ft.creation_time) candidates.push(ft.creation_time)
    if (ft['com.apple.quicktime.creationdate']) candidates.push(ft['com.apple.quicktime.creationdate'])
    if (ft.date) candidates.push(ft.date)

    // Per-stream tags (sometimes more precise)
    for (const stream of info?.streams || []) {
      const st = stream?.tags || {}
      if (st.creation_time) candidates.push(st.creation_time)
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
/**
 * Parse the 14-digit timestamp prefix that milkmaid writes into filenames
 * for sources that provide it (e.g. StufferDB timestamped URLs).
 * Returns an ISO string or null.
 */
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
// In-memory sidecar cache keyed by absolute user dir path.
// Each entry: { data: { ...sidecar }, dirty: bool, flushTimer: null }
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

/**
 * Called by milkmaid after saving an image or GIF buffer.
 * Records all extractable dates into the sidecar.
 *
 * @param {string} userDir  Absolute path to the user's dataset folder
 * @param {string} folder   'images' | 'gif'
 * @param {string} filename Basename of the saved file
 * @param {Buffer} buffer   The raw file buffer (still in memory)
 * @param {Date|null} uploadedDate  The platform upload date milkmaid scraped
 */
async function recordImageDates(userDir, folder, filename, buffer, uploadedDate) {
  const entry = loadSidecar(userDir)
  const key = `${folder}/${filename}`
  if (entry.data[key]) return // already recorded (e.g. re-run)

  const [exif, filenameDate] = await Promise.all([
    extractExifFromBuffer(buffer),
    Promise.resolve(extractFilenameDate(filename)),
  ])

  entry.data[key] = {
    exif: exif || null,
    video: null,
    filename: filenameDate || null,
    uploaded: uploadedDate ? toISO(uploadedDate) : null,
  }
  scheduleSidecarFlush(userDir)
}

/**
 * Called by milkmaid after a video file is fully written to its final path.
 *
 * @param {string} userDir  Absolute path to the user's dataset folder
 * @param {string} folder   'webm'
 * @param {string} filename Basename of the saved file
 * @param {string} filePath Absolute path to the saved video file
 * @param {Date|null} uploadedDate
 */
async function recordVideoDates(userDir, folder, filename, filePath, uploadedDate) {
  const entry = loadSidecar(userDir)
  const key = `${folder}/${filename}`
  if (entry.data[key]) return

  const [video, filenameDate] = await Promise.all([
    extractVideoDateFromFile(filePath),
    Promise.resolve(extractFilenameDate(filename)),
  ])

  entry.data[key] = {
    exif: null,
    video: video || null,
    filename: filenameDate || null,
    uploaded: uploadedDate ? toISO(uploadedDate) : null,
  }
  scheduleSidecarFlush(userDir)
}

/**
 * Read the sidecar for a user and resolve the best available date for a file.
 * Returns { date: ISOstring|null, source: string|null }.
 * Priority: exif → video → filename → uploaded
 */
function resolveDateFromSidecar(userDir, folder, filename) {
  const entry = loadSidecar(userDir)
  const record = entry.data[`${folder}/${filename}`]
  if (!record) return null

  if (record.exif)     return { date: record.exif,     source: 'exif' }
  if (record.video)    return { date: record.video,    source: 'mp4' }
  if (record.filename) return { date: record.filename, source: 'filename' }
  if (record.uploaded) return { date: record.uploaded, source: 'uploaded' }
  return { date: null, source: null }
}

/**
 * Flush all dirty sidecars immediately (call before process exit).
 */
function flushAllSidecars() {
  for (const [userDir] of _sidecars) flushSidecar(userDir)
}

module.exports = {
  recordImageDates,
  recordVideoDates,
  resolveDateFromSidecar,
  extractExifFromBuffer,
  extractVideoDateFromFile,
  extractFilenameDate,
  flushAllSidecars,
  findFfprobe,
  SIDECAR_FILENAME,
}

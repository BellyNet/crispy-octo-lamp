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
 *       "uploaded": "ISO" | null,   // mtime set by milkmaid = platform upload date
 *       "comments": [
 *         {
 *           "author": "string" | null,
 *           "posted": "string" | null,
 *           "text": "string"
 *         }
 *       ],
 *       "commentCount": number | null
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
async function probeVideoFile(filePath) {
  const probe = await ensureFfprobe()
  if (!probe) return { duration: null, videoDate: null }
  try {
    const { stdout } = await execFileAsync(
      probe,
      [
        '-v',
        'quiet',
        '-print_format',
        'json',
        '-show_format',
        '-show_streams',
        filePath,
      ],
      { timeout: 15000 }
    )
    const info = JSON.parse(stdout)

    let duration = Number.parseFloat(info?.format?.duration)
    if (!(Number.isFinite(duration) && duration > 0)) {
      for (const stream of info?.streams || []) {
        const streamDuration = Number.parseFloat(stream?.duration)
        if (Number.isFinite(streamDuration) && streamDuration > 0) {
          duration = streamDuration
          break
        }
      }
    }
    if (!(Number.isFinite(duration) && duration > 0)) duration = null

    const candidates = []

    const ft = info?.format?.tags || {}
    if (ft.creation_time) candidates.push(ft.creation_time)
    if (ft['com.apple.quicktime.creationdate'])
      candidates.push(ft['com.apple.quicktime.creationdate'])
    if (ft.date) candidates.push(ft.date)

    for (const stream of info?.streams || []) {
      if (stream?.tags?.creation_time)
        candidates.push(stream.tags.creation_time)
    }

    let videoDate = null
    for (const raw of candidates) {
      const d = new Date(raw)
      if (isSane(d)) {
        videoDate = toISO(d)
        break
      }
    }
    return { duration, videoDate }
  } catch {
    return { duration: null, videoDate: null }
  }
}

async function extractVideoDateFromFile(filePath) {
  return (await probeVideoFile(filePath)).videoDate
}

let exifrModule = null

function getExifr() {
  if (exifrModule) return exifrModule
  try {
    exifrModule = require('exifr')
  } catch {
    exifrModule = false
  }
  return exifrModule
}

async function extractImageDateFromFile(filePath) {
  const exifr = getExifr()
  if (!exifr) return null
  try {
    const data = await exifr.parse(filePath, {
      pick: ['DateTimeOriginal', 'CreateDate', 'DateTimeDigitized'],
      translateValues: true,
    })
    const raw =
      data?.DateTimeOriginal || data?.CreateDate || data?.DateTimeDigitized
    if (!raw) return null
    const date = raw instanceof Date ? raw : new Date(raw)
    return isSane(date) ? toISO(date) : null
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
    `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}T${d.slice(8, 10)}:${d.slice(10, 12)}:${d.slice(12, 14)}`
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
    if (raw && typeof raw === 'object') {
      data = raw
      data.__version = SIDECAR_VERSION
    }
  } catch {}
  const entry = { data, dirty: false, flushTimer: null }
  _sidecars.set(userDir, entry)
  return entry
}

function normalizeComments(comments) {
  if (!Array.isArray(comments)) return []
  return comments
    .map((comment) => ({
      author:
        typeof comment?.author === 'string' && comment.author.trim()
          ? comment.author.trim()
          : null,
      posted:
        typeof comment?.posted === 'string' && comment.posted.trim()
          ? comment.posted.trim()
          : null,
      text:
        typeof comment?.text === 'string' && comment.text.trim()
          ? comment.text.trim()
          : null,
    }))
    .filter((comment) => comment.text)
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

function resolveBestDateRecord(record) {
  if (!record || typeof record !== 'object') {
    return { date: null, source: null }
  }
  if (record.video) return { date: record.video, source: 'mp4' }
  if (record.filename) return { date: record.filename, source: 'filename' }
  if (record.uploaded) return { date: record.uploaded, source: 'uploaded' }
  return { date: null, source: null }
}

async function recordImageDates(
  userDir,
  folder,
  filename,
  buffer,
  uploadedDate,
  pageMeta = null
) {
  const entry = loadSidecar(userDir)
  const key = `${folder}/${filename}`
  const existingRecord =
    entry.data[key] && typeof entry.data[key] === 'object'
      ? entry.data[key]
      : null
  const comments = normalizeComments(pageMeta?.comments)

  const nextRecord = {
    ...(existingRecord || {}),
    video: existingRecord?.video || null,
    filename: existingRecord?.filename || extractFilenameDate(filename) || null,
    uploaded:
      existingRecord?.uploaded || (uploadedDate ? toISO(uploadedDate) : null),
    comments: comments.length ? comments : existingRecord?.comments || [],
    commentCount:
      typeof pageMeta?.commentCount === 'number'
        ? pageMeta.commentCount
        : Array.isArray(existingRecord?.comments)
          ? existingRecord.comments.length
          : comments.length || null,
  }
  entry.data[key] = nextRecord
  scheduleSidecarFlush(userDir)
  return resolveBestDateRecord(nextRecord)
}

async function recordVideoDates(
  userDir,
  folder,
  filename,
  filePath,
  uploadedDate,
  pageMeta = null
) {
  const entry = loadSidecar(userDir)
  const key = `${folder}/${filename}`
  const existingRecord =
    entry.data[key] && typeof entry.data[key] === 'object'
      ? entry.data[key]
      : null

  const [video, filenameDate] = await Promise.all([
    extractVideoDateFromFile(filePath),
    Promise.resolve(extractFilenameDate(filename)),
  ])
  const comments = normalizeComments(pageMeta?.comments)

  const nextRecord = {
    ...(existingRecord || {}),
    video: existingRecord?.video || video || null,
    filename: existingRecord?.filename || filenameDate || null,
    uploaded:
      existingRecord?.uploaded || (uploadedDate ? toISO(uploadedDate) : null),
    comments: comments.length ? comments : existingRecord?.comments || [],
    commentCount:
      typeof pageMeta?.commentCount === 'number'
        ? pageMeta.commentCount
        : Array.isArray(existingRecord?.comments)
          ? existingRecord.comments.length
          : comments.length || null,
  }
  entry.data[key] = nextRecord
  scheduleSidecarFlush(userDir)
  return resolveBestDateRecord(nextRecord)
}

function resolveDateFromSidecar(userDir, folder, filename) {
  const entry = loadSidecar(userDir)
  const record = entry.data[`${folder}/${filename}`]
  if (!record) return null

  return resolveBestDateRecord(record)
}

function flushAllSidecars() {
  for (const [userDir] of _sidecars) flushSidecar(userDir)
}

module.exports = {
  recordImageDates,
  recordVideoDates,
  resolveDateFromSidecar,
  resolveBestDateRecord,
  extractVideoDateFromFile,
  probeVideoFile,
  extractImageDateFromFile,
  extractFilenameDate,
  flushAllSidecars,
  findFfprobe,
  SIDECAR_FILENAME,
}

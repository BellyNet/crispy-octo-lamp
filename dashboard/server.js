'use strict'

const express = require('express')
const compression = require('compression')
const path = require('path')
const fs = require('fs')
const crypto = require('crypto')
const { execFile } = require('child_process')
const { promisify } = require('util')
const pLimit = require('p-limit')
const sharp = require('sharp')

const execFileAsync = promisify(execFile)
const mediaDates = require('../milkmaid/media-dates.js')
const { loadModelRegistry } = require('../scrapyard/modelRegistry.js')
const MetaCache = require('./meta-cache.js')

const registryPath = path.join(__dirname, '..', 'model_aliases.json')

const app = express()
const PORT = process.env.DASHBOARD_PORT || 3420
const PASSWORD = process.env.DASHBOARD_PASSWORD || 'gitgut'
const AUTH_COOKIE = 'dashboard_auth'
const AUTH_TOKEN = crypto.createHash('sha256').update(PASSWORD).digest('hex')

const APPDATA =
  process.env.APPDATA ||
  path.join(process.env.HOME || process.env.USERPROFILE, 'AppData', 'Roaming')
const slopvaultRoot = path.join(APPDATA, '.slopvault')
const datasetDir =
  process.env.DATASET_DIR || path.join(slopvaultRoot, 'dataset')
const THUMB_DIR =
  process.env.THUMB_DIR || path.join(slopvaultRoot, '.dashboard-thumbs')

const MEDIA_FOLDERS = ['images', 'gif', 'webm']
const MEDIA_EXTS = new Set(['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.webm'])

fs.mkdirSync(THUMB_DIR, { recursive: true })
const metaCache = new MetaCache(THUMB_DIR)

const RESPONSE_CACHE_DIR = path.join(THUMB_DIR, 'response-cache')
fs.mkdirSync(RESPONSE_CACHE_DIR, { recursive: true })

// Bump when the response shape changes meaningfully (new fields, changed date
// resolution rules, etc.). On-disk caches with an older version are ignored,
// forcing a rebuild — used by mismatched-cache callers below.
const RESPONSE_CACHE_VERSION = 4

// ─── DELETION FLAGS ────────────────────────────────────────────────────────
// Per-model sidecar lists files the user has flagged for deletion via the
// dashboard. Lives at dataset/<user>/.dashboard-flags.json so the cleanup
// script in the main repo can read it without going through the API.
// Shape: { "<folder>/<filename>": { flagged: true, addedAt: ISO } }
const FLAGS_FILENAME = '.dashboard-flags.json'
function flagsPathFor(userDir) {
  return path.join(userDir, FLAGS_FILENAME)
}
function readFlagsForUser(userDir) {
  try {
    const data = JSON.parse(fs.readFileSync(flagsPathFor(userDir), 'utf8'))
    return data && typeof data === 'object' && data.flags ? data : { flags: {} }
  } catch { return { flags: {} } }
}
function writeFlagsForUser(userDir, data) {
  const p = flagsPathFor(userDir)
  const tmp = p + '.tmp'
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2))
  fs.renameSync(tmp, p)
}

function loadResponseCacheFromDisk(username) {
  try {
    const data = JSON.parse(fs.readFileSync(path.join(RESPONSE_CACHE_DIR, `${username}.json`), 'utf8'))
    if (data.version !== RESPONSE_CACHE_VERSION) return null
    if (data.fingerprint && Array.isArray(data.response)) return data
  } catch {}
  return null
}

function saveResponseCacheToDisk(username, response, fingerprint) {
  setImmediate(() => {
    try {
      fs.writeFileSync(
        path.join(RESPONSE_CACHE_DIR, `${username}.json`),
        JSON.stringify({ version: RESPONSE_CACHE_VERSION, fingerprint, response })
      )
    } catch {}
  })
}

// ─── FFMPEG ───────────────────────────────────────────────────────────────────
let ffprobePath = null
let ffmpegPath = null

async function findFfTools() {
  const ffprobeFound = await mediaDates.findFfprobe()
  if (ffprobeFound) {
    ffprobePath = ffprobeFound
    // ffmpeg lives alongside ffprobe
    const guess = ffprobeFound.replace(/ffprobe(\.exe)?$/i, (m) =>
      m.replace('ffprobe', 'ffmpeg')
    )
    try {
      await execFileAsync(guess, ['-version'], { timeout: 3000 })
      ffmpegPath = guess
      console.log(`  ffmpeg:  ${guess}`)
    } catch {}
  }
  if (!ffmpegPath)
    console.log('  ffmpeg: not found — video previews unavailable')
}

// ─── ANIMATED GIF PREVIEWS ────────────────────────────────────────────────────
// Generates a short looping GIF (≈2 s at 6 fps, 280 px wide) from a video.
// Stored in THUMB_DIR and served statically — no concurrent video decoding.

// Single-ffprobe getDuration — used only by the GIF generator. The scan path
// uses mediaDates.probeVideoFile which returns duration + date in one call.
async function getDuration(videoPath) {
  if (!ffprobePath) return null
  const { duration } = await mediaDates.probeVideoFile(videoPath)
  return duration
}

async function generatePreviewGif(videoPath, gifPath) {
  if (!ffmpegPath) return false
  const duration = await getDuration(videoPath)
  if (!duration) return false

  // Skip the opener: at least 15 s or 25% of the video, whichever is larger,
  // capped at 90 s so we don't skip half a long clip.
  // Then sample 4 evenly-spaced points across the remaining run time.
  const skipSec = Math.min(Math.max(15, duration * 0.25), 90)
  const usable = duration - skipSec - 2.5          // leave room for the 2.5 s clip
  const seekPoints = usable > 0
    ? [0, 0.33, 0.66, 1].map((p) => skipSec + p * usable)
    : [duration * 0.5]                             // very short video — just use midpoint

  for (const seek of seekPoints) {
    const tmp = gifPath + '.tmp.gif'
    try {
      await execFileAsync(
        ffmpegPath,
        [
          '-ss', seek.toFixed(2),
          '-t',  '2.5',
          '-i',  videoPath,
          '-vf', 'fps=6,scale=280:-2:flags=lanczos',
          '-loop', '0',
          '-y',  tmp,
        ],
        { timeout: 30000 }
      )
      const stat = fs.statSync(tmp)
      if (stat.size > 5000) {
        fs.renameSync(tmp, gifPath)
        return true
      }
      fs.unlinkSync(tmp)
    } catch {
      try { fs.unlinkSync(tmp) } catch {}
    }
  }
  return false
}

// ─── REGISTRY SOURCES ────────────────────────────────────────────────────────
// Build a map of username → { coomer, kemono, stufferdb }
// from model_aliases.json so the /api/users route can include source links.
// Called on every /api/users request — loadModelRegistry does a fresh fs.readFileSync
// each time, so changes to the bind-mounted file are picked up immediately.
const SOURCE_PLATFORMS = ['coomer', 'kemono', 'stufferdb']

// Cached source map — rebuilt only when model_aliases.json mtime changes.
// loadModelRegistry was previously called on every /api/users request, doing a
// fresh fs.readFileSync + JSON.parse each time.
let _sourceMapCache = { mtimeMs: -1, map: {} }
function buildSourceMap() {
  let mtimeMs = 0
  try { mtimeMs = fs.statSync(registryPath).mtimeMs } catch {}
  if (_sourceMapCache.mtimeMs === mtimeMs) return _sourceMapCache.map

  let map = {}
  try {
    const registry = loadModelRegistry(registryPath)
    for (const [canonical, entry] of Object.entries(registry)) {
      const names = [canonical, ...(entry.aliases || [])]
      const byPlatform = {}
      for (const p of SOURCE_PLATFORMS) {
        byPlatform[p] = (entry.sources?.[p] || []).map((s) => s.url).filter(Boolean)
      }
      for (const name of names) {
        if (!map[name]) map[name] = Object.fromEntries(SOURCE_PLATFORMS.map((p) => [p, []]))
        for (const p of SOURCE_PLATFORMS) {
          for (const u of byPlatform[p])
            if (!map[name][p].includes(u)) map[name][p].push(u)
        }
      }
    }
  } catch {
    map = {}
  }
  _sourceMapCache = { mtimeMs, map }
  return map
}

// ─── HELPERS ──────────────────────────────────────────────────────────────────

function getMediaType(folder, filename) {
  const ext = path.extname(filename).toLowerCase()
  if (ext === '.gif') return 'gif'
  if (ext === '.mp4' || ext === '.webm') return 'video'
  return 'image'
}

function safeSubPath(base, ...parts) {
  const resolved = path.resolve(path.join(base, ...parts))
  const baseResolved = path.resolve(base)
  if (
    resolved !== baseResolved &&
    !resolved.startsWith(baseResolved + path.sep)
  )
    return null
  return resolved
}

function isSane(date) {
  if (!date || isNaN(date.getTime())) return false
  const y = date.getFullYear()
  return y >= 1990 && y <= 2035
}

// ─── DATE RESOLUTION ──────────────────────────────────────────────────────────

// opts.cachedVideoDate: pre-resolved video date from the metadata cache (skips ffprobe)
// opts.cachedImageDate: pre-resolved EXIF date from the metadata cache (skips exifr)
// opts.stat: pre-fetched stat (skips a second stat call for the filesystem fallback)
async function resolveDateForFile(userDir, folder, filename, filePath, opts = {}) {
  const fromSidecar = mediaDates.resolveDateFromSidecar(
    userDir,
    folder,
    filename
  )
  if (fromSidecar && fromSidecar.date) return fromSidecar

  const ext = path.extname(filename).toLowerCase()
  let result = null

  if (['.mp4', '.webm', '.mov'].includes(ext)) {
    const videoDate = 'cachedVideoDate' in opts
      ? opts.cachedVideoDate
      : await mediaDates.extractVideoDateFromFile(filePath)
    if (videoDate) result = { date: videoDate, source: 'mp4' }
  }

  // EXIF for images — runs after the cached path so the scanner can pass
  // the already-extracted date in without re-reading the file.
  if (!result && ['.jpg', '.jpeg'].includes(ext)) {
    const imageDate = 'cachedImageDate' in opts
      ? opts.cachedImageDate
      : await mediaDates.extractImageDateFromFile(filePath)
    if (imageDate) result = { date: imageDate, source: 'exif' }
  }

  if (!result) {
    const filenameDate = mediaDates.extractFilenameDate(filename)
    if (filenameDate) result = { date: filenameDate, source: 'filename' }
  }

  if (!result) {
    try {
      const st = opts.stat || await fs.promises.stat(filePath)
      if (isSane(st.mtime))
        result = { date: st.mtime.toISOString(), source: 'uploaded' }
      else if (isSane(st.birthtime))
        result = { date: st.birthtime.toISOString(), source: 'filesystem' }
    } catch {}
  }

  return result || { date: null, source: null }
}

// ─── AUTH ─────────────────────────────────────────────────────────────────────

function parseCookies(req) {
  const cookies = {}
  for (const part of (req.headers.cookie || '').split(';')) {
    const [k, ...v] = part.split('=')
    if (k) {
      const raw = v.join('=').trim()
      try { cookies[k.trim()] = decodeURIComponent(raw) } catch { cookies[k.trim()] = raw }
    }
  }
  return cookies
}

// gzip JSON responses — /api/users/:name/media for a 4000-item model is
// ~1.5 MB raw and compresses to ~250 KB. Skip media files and pre-generated
// thumbnails since those are already image-compressed.
app.use(compression({
  filter: (req, res) => {
    if (req.path.startsWith('/media/') ||
        req.path.startsWith('/thumb/') ||
        req.path.startsWith('/thumbnail/')) return false
    return compression.filter(req, res)
  },
}))

app.use(express.urlencoded({ extended: false }))
app.use(express.json({ limit: '32kb' }))

app.post('/auth', (req, res) => {
  if (req.body.password === PASSWORD) {
    res.setHeader(
      'Set-Cookie',
      `${AUTH_COOKIE}=${AUTH_TOKEN}; Path=/; HttpOnly; SameSite=Strict; Max-Age=31536000`
    )
    return res.redirect('/')
  }
  res.redirect('/login.html?error=1')
})

app.use((req, res, next) => {
  if (parseCookies(req)[AUTH_COOKIE] === AUTH_TOKEN) return next()
  if (req.path === '/login.html') return next()
  res.redirect('/login.html')
})

// ─── UNIFIED SCAN PIPELINE ───────────────────────────────────────────────────
// One walk produces both: per-model stats (for the home grid) and the full
// media response (served by /api/users/:name/media). Stats live in memory for
// every model; responses are LRU-bounded so memory stays sane on large datasets.
// Disk caches back both layers so the next process restart is near-instant.

const IMAGE_EXTS_IN = new Set(['.jpg', '.jpeg', '.png', '.gif'])
const VIDEO_EXTS_IN = new Set(['.mp4', '.webm'])

let modelStatsCache = {}
// { username: { earliestMs, latestMs, latestAddedMs, fileCount, yearCounts,
//               coverPool: [{type, folder, filename, url}, ...] } }

// LRU for the heavy per-model response. Cold models live on disk only and are
// rehydrated on demand by scanModel().
class LRU {
  constructor(max) { this.max = max; this.map = new Map() }
  get(key) {
    if (!this.map.has(key)) return undefined
    const v = this.map.get(key)
    this.map.delete(key); this.map.set(key, v) // bump to MRU
    return v
  }
  set(key, value) {
    if (this.map.has(key)) this.map.delete(key)
    else if (this.map.size >= this.max) this.map.delete(this.map.keys().next().value)
    this.map.set(key, value)
  }
  delete(key) { return this.map.delete(key) }
  has(key) { return this.map.has(key) }
  get size() { return this.map.size }
}
const RESPONSE_CACHE_MAX = parseInt(process.env.RESPONSE_CACHE_MAX, 10) || 32
const mediaResponseCache = new LRU(RESPONSE_CACHE_MAX)

// Per-model fingerprint snapshot — drives change detection on the cheap tick.
const fingerprintCache = new Map()

// Scan concurrency — tunable from env so the NAS can be turned down if I/O saturates.
const imgLimit = pLimit(parseInt(process.env.SCAN_IMG_CONCURRENCY, 10) || 16)
const vidLimit = pLimit(parseInt(process.env.SCAN_VID_CONCURRENCY, 10) || 4)
const modelLimit = pLimit(parseInt(process.env.SCAN_MODEL_CONCURRENCY, 10) || 8)

function computeStatsFromResponse(allMedia) {
  let earliestMs = Infinity, latestMs = 0, latestAddedMs = 0
  const yearCounts = {}
  for (const m of allMedia) {
    if (m.addedMs > latestAddedMs) latestAddedMs = m.addedMs
    if (m.mediaDateMs) {
      if (m.mediaDateMs < earliestMs) earliestMs = m.mediaDateMs
      if (m.mediaDateMs > latestMs)   latestMs   = m.mediaDateMs
    }
    const dateMs = m.mediaDateMs || m.addedMs
    if (dateMs > 0) {
      const yr = new Date(dateMs).getFullYear()
      if (yr >= 1990 && yr <= 2035) yearCounts[yr] = (yearCounts[yr] || 0) + 1
    }
  }
  return {
    earliestMs: earliestMs === Infinity ? 0 : earliestMs,
    latestMs, latestAddedMs,
    fileCount: allMedia.length,
    yearCounts,
  }
}

// Sample up to N candidate items from a response — used to pick the daily cover
// without holding the full response in memory after scan.
function buildCoverPool(allMedia, max = 16) {
  if (!allMedia.length) return []
  // Prefer stills (images/gifs) — videos as home covers stream the whole MP4
  // and decode audio in every <video> element on the page, which is a disaster
  // on mobile. We still include them as a fallback for models with no stills,
  // and ship the GIF preview URL so the client can render them as a thumbnail
  // instead of a live <video> element.
  const stills = allMedia.filter((m) => m.type === 'image' || m.type === 'gif')
  const source = stills.length ? stills : allMedia
  const pool = []
  const step = Math.max(1, Math.floor(source.length / max))
  for (let i = 0; i < source.length && pool.length < max; i += step) {
    const m = source[i]
    pool.push({
      type:       m.type,
      folder:     m.folder,
      filename:   m.filename,
      url:        m.url,
      previewUrl: m.previewUrl || null,
      thumbUrl:   m.thumbUrl   || null,
    })
  }
  return pool
}

// Deterministic per-day cover pick: same model + same date → same cover.
// Cover rotates at the date boundary without any explicit nightly job.
function pickCoverFor(username, pool) {
  if (!pool || !pool.length) return null
  const seed = `${username}|${new Date().toISOString().slice(0, 10)}`
  let h = 0
  for (let i = 0; i < seed.length; i++) h = ((h << 5) - h + seed.charCodeAt(i)) | 0
  return pool[Math.abs(h) % pool.length]
}

// LoRA-style caption sidecar: same stem as the image but `.txt` next to it.
// e.g. images/foo.jpg → images/foo.txt
const CAPTION_MAX_BYTES = 64 * 1024
function captionPathFor(userDir, folder, filename) {
  const stem = filename.slice(0, filename.length - path.extname(filename).length)
  return path.join(userDir, folder, `${stem}.txt`)
}
function hasCaptionFile(userDir, folder, filename) {
  try { return fs.statSync(captionPathFor(userDir, folder, filename)).isFile() }
  catch { return false }
}

// Build the full response record for a single file. Reuses metaCache when valid.
async function processFileForResponse(username, userDir, item) {
  const ext = path.extname(item.filename).toLowerCase()
  const isVideo = VIDEO_EXTS_IN.has(ext)
  const stat = await fs.promises.stat(item.filePath).catch(() => null)
  if (!stat) return null

  let width = 0, height = 0, duration = 0, videoDate, imageDate
  let metaUpdated = false
  const hit = metaCache.get(username, item.folder, item.filename, stat)
  if (hit) {
    width = hit.width || 0
    height = hit.height || 0
    duration = hit.duration || 0
    videoDate = hit.videoDate
    imageDate = hit.imageDate
    // Backfill EXIF dates for entries cached before EXIF support landed.
    // Sentinel: `null` means "tried and got nothing", so we don't retry next scan.
    if (!isVideo && ['.jpg', '.jpeg'].includes(ext) && imageDate === undefined) {
      const exifDate = await mediaDates.extractImageDateFromFile(item.filePath).catch(() => null)
      imageDate = exifDate || null
      metaCache.set(username, item.folder, item.filename, stat, {
        width, height, imageDate,
      })
      metaUpdated = true
    }
  } else {
    if (isVideo) {
      const probed = await mediaDates.probeVideoFile(item.filePath).catch(() => ({}))
      duration = probed.duration || 0
      videoDate = probed.videoDate || undefined
    } else if (IMAGE_EXTS_IN.has(ext)) {
      // sharp metadata and exif parse can run in parallel — both read the file
      // header. exifr returns null for formats with no EXIF (most PNGs, GIFs).
      const [m, exifDate] = await Promise.all([
        sharp(item.filePath).metadata().catch(() => ({})),
        mediaDates.extractImageDateFromFile(item.filePath).catch(() => null),
      ])
      width = m.width || 0; height = m.height || 0
      imageDate = exifDate || null
    }
    metaCache.set(username, item.folder, item.filename, stat, isVideo
      ? { duration, ...(videoDate !== undefined && { videoDate }) }
      : { width, height, imageDate })
    metaUpdated = true
  }

  const type = getMediaType(item.folder, item.filename)
  const dateMeta = await resolveDateForFile(
    userDir, item.folder, item.filename, item.filePath,
    { cachedVideoDate: videoDate, cachedImageDate: imageDate, stat }
  )
  return {
    record: {
      filename: item.filename,
      folder:   item.folder,
      type,
      url:      `/media/${encodeURIComponent(username)}/${item.folder}/${encodeURIComponent(item.filename)}`,
      date:     dateMeta.date,
      source:   dateMeta.source,
      mediaDateMs: (dateMeta.source && dateMeta.source !== 'filesystem' && dateMeta.date)
        ? new Date(dateMeta.date).getTime() : 0,
      addedMs:  stat.birthtime.getTime() > 0 ? stat.birthtime.getTime() : stat.mtime.getTime(),
      size:     stat.size,
      duration,
      width,
      height,
      previewUrl: type === 'video'
        ? `/thumbnail/${encodeURIComponent(username)}/${encodeURIComponent(item.filename)}`
        : null,
      // Unified thumbnail URL used by every card in the UI. Videos route to
      // the GIF preview; images and gifs route to the /thumb JPEG endpoint.
      thumbUrl: type === 'video'
        ? `/thumbnail/${encodeURIComponent(username)}/${encodeURIComponent(item.filename)}`
        : `/thumb/${encodeURIComponent(username)}/${item.folder}/${encodeURIComponent(item.filename)}`,
      // True iff a same-stem .txt sidecar exists next to this file (LoRA
      // training caption). The full text is fetched on-demand from /api/caption
      // so the per-model response doesn't balloon for large datasets.
      hasCaption: type !== 'video' && hasCaptionFile(userDir, item.folder, item.filename),
    },
    metaUpdated,
  }
}

// Returns { stats, response, source: 'memory' | 'disk' | 'scan' }.
async function scanModel(username, { force = false } = {}) {
  const userDir = path.join(datasetDir, username)
  const fingerprint = await getMediaFingerprint(userDir)
  fingerprintCache.set(username, fingerprint)

  if (!force) {
    const cached = mediaResponseCache.get(username)
    if (cached && fingerprintMatches(cached.fingerprint, fingerprint)) {
      return { stats: modelStatsCache[username], response: cached.response, source: 'memory' }
    }
    const disk = loadResponseCacheFromDisk(username)
    if (disk && fingerprintMatches(disk.fingerprint, fingerprint)) {
      mediaResponseCache.set(username, { response: disk.response, fingerprint })
      const stats = computeStatsFromResponse(disk.response)
      stats.coverPool = buildCoverPool(disk.response)
      modelStatsCache[username] = stats
      return { stats, response: disk.response, source: 'disk' }
    }
  }

  const rawFiles = []
  for (const folder of MEDIA_FOLDERS) {
    let files
    try { files = await fs.promises.readdir(path.join(userDir, folder)) } catch { continue }
    for (const file of files) {
      if (!MEDIA_EXTS.has(path.extname(file).toLowerCase())) continue
      rawFiles.push({ filename: file, folder, filePath: path.join(userDir, folder, file) })
    }
  }

  let metaUpdated = false
  const allMedia = (await Promise.all(rawFiles.map((item) => {
    const ext = path.extname(item.filename).toLowerCase()
    const isVideo = VIDEO_EXTS_IN.has(ext)
    return (isVideo ? vidLimit : imgLimit)(async () => {
      const out = await processFileForResponse(username, userDir, item)
      if (!out) return null
      if (out.metaUpdated) metaUpdated = true
      return out.record
    })
  }))).filter(Boolean)

  // Stamp `flagged` onto each item from the user's sidecar. Read once, applied
  // in O(n) — far cheaper than re-reading the sidecar per file.
  const flagsData = readFlagsForUser(userDir)
  for (const m of allMedia) {
    m.flagged = !!(flagsData.flags && flagsData.flags[`${m.folder}/${m.filename}`])
  }

  allMedia.sort((a, b) => (a.mediaDateMs || a.addedMs) - (b.mediaDateMs || b.addedMs))
  mediaResponseCache.set(username, { response: allMedia, fingerprint })
  saveResponseCacheToDisk(username, allMedia, fingerprint)
  if (metaUpdated) metaCache.flush(username)

  const stats = computeStatsFromResponse(allMedia)
  stats.coverPool = buildCoverPool(allMedia)
  modelStatsCache[username] = stats
  return { stats, response: allMedia, source: 'scan' }
}

// ─── SCAN ORCHESTRATION ──────────────────────────────────────────────────────

const scanState = {
  inProgress:   false,
  trigger:      null,            // 'startup' | 'tick' | 'nightly' | 'manual'
  startedAt:    null,
  completedAt:  null,
  modelsTotal:  0,
  modelsDone:   0,
  errors:       0,
  lastTickAt:   null,
  lastFullScanAt: null,
}

async function scanAll({ force = false, trigger = 'periodic' } = {}) {
  if (scanState.inProgress) {
    return { skipped: true, reason: 'already running' }
  }
  let dirs
  try { dirs = await fs.promises.readdir(datasetDir, { withFileTypes: true }) }
  catch { return { error: 'dataset dir unreadable' } }
  const modelDirs = dirs.filter((e) => e.isDirectory() && !e.name.startsWith('.'))

  scanState.inProgress  = true
  scanState.trigger     = trigger
  scanState.startedAt   = new Date().toISOString()
  scanState.completedAt = null
  scanState.modelsTotal = modelDirs.length
  scanState.modelsDone  = 0
  scanState.errors      = 0
  const t0 = Date.now()
  console.log(`  Scan:      ${trigger}, ${modelDirs.length} models${force ? ' (force)' : ''}…`)

  const seen = new Set()
  await Promise.all(modelDirs.map((d) => modelLimit(async () => {
    seen.add(d.name)
    try { await scanModel(d.name, { force }) }
    catch { scanState.errors++ }
    scanState.modelsDone++
  })))

  // Drop state for models that no longer exist on disk.
  for (const username of Object.keys(modelStatsCache)) {
    if (!seen.has(username)) {
      delete modelStatsCache[username]
      mediaResponseCache.delete(username)
      fingerprintCache.delete(username)
    }
  }

  scanState.completedAt    = new Date().toISOString()
  scanState.lastFullScanAt = scanState.completedAt
  scanState.inProgress     = false
  console.log(`  Scan:      done in ${((Date.now() - t0) / 1000).toFixed(1)}s`)
  return { ok: true }
}

// Cheap tick: stat 4 paths per model to compute its fingerprint, then rescan
// only the models whose fingerprint changed. ~400 stat calls for 100 models —
// fast enough to run every minute even on a NAS.
async function fingerprintTick() {
  if (scanState.inProgress) return
  let dirs
  try { dirs = await fs.promises.readdir(datasetDir, { withFileTypes: true }) }
  catch { return }
  const modelDirs = dirs.filter((e) => e.isDirectory() && !e.name.startsWith('.'))

  const seen = new Set()
  const changed = []
  await Promise.all(modelDirs.map((d) => modelLimit(async () => {
    seen.add(d.name)
    const fp = await getMediaFingerprint(path.join(datasetDir, d.name))
    const prev = fingerprintCache.get(d.name)
    if (!prev || !fingerprintMatches(prev, fp)) changed.push(d.name)
  })))
  scanState.lastTickAt = new Date().toISOString()

  // Detect removed models (existed last tick, gone now).
  for (const u of [...fingerprintCache.keys()]) {
    if (!seen.has(u)) {
      delete modelStatsCache[u]
      mediaResponseCache.delete(u)
      fingerprintCache.delete(u)
    }
  }

  if (!changed.length) return
  console.log(`  Tick:      ${changed.length} changed model(s) — rescanning…`)
  await Promise.all(changed.map((u) => modelLimit(() => scanModel(u).catch(() => {}))))
}

// ─── FEATURED MODEL ───────────────────────────────────────────────────────────
// Rotates one featured model per day. No model repeats until all have been shown.
// State persisted in THUMB_DIR/featured.json so it survives restarts.

const FEATURED_FILE = path.join(THUMB_DIR, 'featured.json')
let _featuredCache = { date: null, model: null }

function shuffleArray(arr) {
  const a = [...arr]
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]]
  }
  return a
}

function getFeaturedModel(allModelNames) {
  const today = new Date().toISOString().slice(0, 10) // YYYY-MM-DD
  if (_featuredCache.date === today) return _featuredCache.model

  let state = { date: null, model: null, queue: [] }
  try { state = JSON.parse(fs.readFileSync(FEATURED_FILE, 'utf8')) } catch {}

  if (state.date !== today) {
    // Filter queue to only models that still exist
    state.queue = (state.queue || []).filter((m) => allModelNames.includes(m))
    if (state.queue.length === 0) {
      // Start a new rotation — shuffle all models, exclude today's outgoing model
      const pool = allModelNames.filter((m) => m !== state.model)
      state.queue = shuffleArray(pool.length > 0 ? pool : allModelNames)
    }
    state.model = state.queue.shift()
    state.date = today
    try { fs.writeFileSync(FEATURED_FILE, JSON.stringify(state, null, 2)) } catch {}
  }

  _featuredCache = { date: today, model: state.model }
  return state.model
}

// ─── ROUTES ───────────────────────────────────────────────────────────────────

const IMMUTABLE_CACHE = 'public, max-age=31536000, immutable'

app.use(express.static(__dirname))
app.get('/', (_req, res) => res.sendFile('index.html', { root: __dirname }))

// Users list — returns [{ name, sources, featured }, ...]
// Sets Cache-Control: no-cache so the browser revalidates on every fetch; the
// weak ETag Express attaches to res.json lets us return 304 when modelStatsCache
// hasn't changed. The 5-min client refresh becomes a no-op on quiet networks.
app.get('/api/users', async (_req, res) => {
  try {
    const entries = await fs.promises.readdir(datasetDir, {
      withFileTypes: true,
    })
    const sourceMap = buildSourceMap()
    const empty = Object.fromEntries(SOURCE_PLATFORMS.map((p) => [p, []]))
    const users = entries
      .filter((e) => e.isDirectory() && !e.name.startsWith('.'))
      .map((e) => ({
        name: e.name,
        sources: sourceMap[e.name] || { ...empty },
      }))
      .sort((a, b) =>
        a.name.localeCompare(b.name, undefined, { sensitivity: 'base' })
      )

    // Only consider models with at least one media file — otherwise the
    // featured card hits /api/users/:name/cover, gets a 404, and renders blank.
    const featuredCandidates = users
      .map((u) => u.name)
      .filter((n) => (modelStatsCache[n]?.fileCount || 0) > 0)
    const featuredModel = featuredCandidates.length
      ? getFeaturedModel(featuredCandidates)
      : null
    res.setHeader('Cache-Control', 'no-cache')
    res.json(users.map((u) => {
      const s = modelStatsCache[u.name] || {}
      const cover = pickCoverFor(u.name, s.coverPool)
      return {
        ...u,
        featured:      u.name === featuredModel,
        cover:         cover ? {
          type:       cover.type,
          url:        cover.url,
          previewUrl: cover.previewUrl || null,
          thumbUrl:   cover.thumbUrl   || null,
        } : null,
        earliestMs:    s.earliestMs    || 0,
        latestMs:      s.latestMs      || 0,
        latestAddedMs: s.latestAddedMs || 0,
        fileCount:     s.fileCount     || 0,
        yearCounts:    s.yearCounts    || {},
      }
    }))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

async function getMediaFingerprint(userDir) {
  const fp = {}
  for (const folder of MEDIA_FOLDERS) {
    try { fp[folder] = (await fs.promises.stat(path.join(userDir, folder))).mtimeMs }
    catch { fp[folder] = 0 }
  }
  try { fp._sidecar = (await fs.promises.stat(path.join(userDir, '.media-dates.json'))).mtimeMs }
  catch { fp._sidecar = 0 }
  // Flag sidecar — when the user toggles a flag this mtime changes, so the
  // next fingerprint tick rescans and picks up the new `flagged` values.
  try { fp._flags = (await fs.promises.stat(path.join(userDir, FLAGS_FILENAME))).mtimeMs }
  catch { fp._flags = 0 }
  return fp
}

function fingerprintMatches(a, b) {
  const keys = new Set([...Object.keys(a), ...Object.keys(b)])
  return [...keys].every((k) => a[k] === b[k])
}

// Media list for a user — delegates to the unified scanner, which short-circuits
// to the in-memory LRU, then the on-disk cache, before walking files.
app.get('/api/users/:username/media', async (req, res) => {
  const username = req.params.username
  const userDir = safeSubPath(datasetDir, username)
  if (!userDir) return res.status(403).json({ error: 'Forbidden' })
  try {
    const { response } = await scanModel(username)
    res.setHeader('Cache-Control', 'no-cache')
    res.json(response)
    // After responding, kick off background JPEG thumb generation for every
    // image/gif in this model. By the time the user scrolls past the first
    // ~200 cards, the rest are likely already cached on disk.
    setImmediate(() => warmGridThumbs(username, response))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// Toggle a deletion flag on a specific file. Body: { folder, filename, flagged }
// Updates the sidecar AND the in-memory + disk response caches in place so the
// UI sees the change without waiting for a full rescan.
app.post('/api/users/:username/flag', async (req, res) => {
  const username = req.params.username
  const userDir = safeSubPath(datasetDir, username)
  if (!userDir) return res.status(403).json({ error: 'Forbidden' })

  const { folder, filename } = req.body || {}
  const flagged = !!(req.body && req.body.flagged)
  if (!folder || !filename || !MEDIA_FOLDERS.includes(folder)) {
    return res.status(400).json({ error: 'folder + filename required' })
  }
  // Confirm the file actually exists — refuse to flag arbitrary paths.
  const filePath = safeSubPath(datasetDir, username, folder, filename)
  if (!filePath || !fs.existsSync(filePath)) {
    return res.status(404).json({ error: 'File not found' })
  }

  // 1. Update the sidecar (source of truth).
  const data = readFlagsForUser(userDir)
  data.flags = data.flags || {}
  const key = `${folder}/${filename}`
  if (flagged) {
    if (!data.flags[key]) data.flags[key] = { flagged: true, addedAt: new Date().toISOString() }
  } else {
    delete data.flags[key]
  }
  try { writeFlagsForUser(userDir, data) }
  catch (err) { return res.status(500).json({ error: err.message }) }

  // 2. Patch the response caches in place so the next read reflects the change
  //    without a full rescan. The sidecar mtime is also in the fingerprint, so
  //    even if these mutations are missed somehow, the next tick will resync.
  const patchItem = (response) => {
    const it = response && response.find((m) => m.folder === folder && m.filename === filename)
    if (it) it.flagged = flagged
  }
  const memHit = mediaResponseCache.get(username)
  if (memHit) patchItem(memHit.response)
  try {
    const diskFile = path.join(RESPONSE_CACHE_DIR, `${username}.json`)
    if (fs.existsSync(diskFile)) {
      const disk = JSON.parse(fs.readFileSync(diskFile, 'utf8'))
      if (Array.isArray(disk.response)) {
        patchItem(disk.response)
        fs.writeFileSync(diskFile, JSON.stringify(disk))
      }
    }
  } catch {}

  res.json({ ok: true, flagged })
})

async function warmGridThumbs(username, items) {
  const tasks = []
  for (const item of items) {
    if (item.type === 'video') continue
    const dst = thumbDiskPath(username, item.folder, item.filename)
    if (fs.existsSync(dst)) continue
    const src = safeSubPath(datasetDir, username, item.folder, item.filename)
    if (!src || !fs.existsSync(src)) continue
    tasks.push(thumbLimit(() => generateThumb(src, dst).catch(() => {})))
  }
  if (!tasks.length) return
  console.log(`  Thumbs:    warming ${tasks.length} grid thumbs for ${username}…`)
  await Promise.all(tasks)
  console.log(`  Thumbs:    ${tasks.length} grid thumbs ready for ${username} ✓`)
}

// Caption sidecar — returns the text of <stem>.txt next to the image. Used
// by the lightbox to show LoRA captions alongside the image. Capped at
// CAPTION_MAX_BYTES so a runaway file can't OOM the server.
app.get('/api/caption/:username/:folder/:filename', async (req, res) => {
  const { username, folder, filename } = req.params
  if (!MEDIA_FOLDERS.includes(folder)) return res.status(403).send('Forbidden')
  const userDir = safeSubPath(datasetDir, username)
  if (!userDir) return res.status(403).send('Forbidden')
  const capPath = captionPathFor(userDir, folder, filename)
  // Guard: ensure the resolved caption path stays inside the user dir, in case
  // an unusual filename containing path separators slipped through.
  if (!capPath.startsWith(path.resolve(userDir) + path.sep)) {
    return res.status(403).send('Forbidden')
  }
  let stat
  try { stat = await fs.promises.stat(capPath) }
  catch { return res.status(404).json({ error: 'No caption' }) }
  if (!stat.isFile()) return res.status(404).json({ error: 'No caption' })

  // ETag based on mtime+size so edits in-place are picked up.
  const etag = `W/"${stat.size.toString(36)}-${stat.mtimeMs.toString(36)}"`
  if (req.headers['if-none-match'] === etag) {
    res.status(304).end()
    return
  }
  try {
    const buf = await fs.promises.readFile(capPath, { encoding: 'utf8' })
    const text = buf.length > CAPTION_MAX_BYTES ? buf.slice(0, CAPTION_MAX_BYTES) : buf
    res.setHeader('ETag', etag)
    res.setHeader('Cache-Control', 'no-cache')
    res.json({ text, truncated: buf.length > CAPTION_MAX_BYTES })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// Small JPEG thumbnail for images and GIFs — lazy-generated, cached to disk
// under THUMB_DIR/<user>/thumb-<folder>-<filename>.jpg. Used by the home grid
// covers and the per-model media grid so cards never download full-resolution
// source files. ~640px max dim, mozjpeg quality 80 — typically 10–40 KB each.
const THUMB_MAX_DIM = parseInt(process.env.THUMB_MAX_DIM, 10) || 640
const THUMB_QUALITY = parseInt(process.env.THUMB_QUALITY, 10) || 80
const thumbLimit = pLimit(parseInt(process.env.THUMB_CONCURRENCY, 10) || 4)
const _thumbInflight = new Map() // dedupe concurrent requests for the same file

function thumbDiskPath(username, folder, filename) {
  const stem = path.basename(filename, path.extname(filename))
  return path.join(THUMB_DIR, username, `thumb-${folder}-${stem}.jpg`)
}

async function generateThumb(srcPath, dstPath) {
  await fs.promises.mkdir(path.dirname(dstPath), { recursive: true })
  const tmp = dstPath + '.tmp.jpg'
  try {
    await sharp(srcPath, { failOn: 'none', animated: false })
      .rotate() // honor EXIF orientation
      .resize(THUMB_MAX_DIM, THUMB_MAX_DIM, { fit: 'inside', withoutEnlargement: true })
      .jpeg({ quality: THUMB_QUALITY, mozjpeg: true })
      .toFile(tmp)
    await fs.promises.rename(tmp, dstPath)
    return true
  } catch {
    try { await fs.promises.unlink(tmp) } catch {}
    return false
  }
}

app.get('/thumb/:username/:folder/:filename', async (req, res) => {
  const { username, folder, filename } = req.params
  if (!MEDIA_FOLDERS.includes(folder)) return res.status(403).send('Forbidden')
  const srcPath = safeSubPath(datasetDir, username, folder, filename)
  if (!srcPath) return res.status(403).send('Forbidden')

  const dstPath = thumbDiskPath(username, folder, filename)
  const dstBase = path.basename(dstPath)
  const dstDir  = path.dirname(dstPath)

  const sendCached = () => {
    res.setHeader('Cache-Control', IMMUTABLE_CACHE)
    res.sendFile(dstBase, { root: dstDir }, (err) => {
      if (err && !res.headersSent) res.status(404).send('Not found')
    })
  }
  if (fs.existsSync(dstPath)) return sendCached()
  if (!fs.existsSync(srcPath)) return res.status(404).send('Not found')

  // Dedupe — a popular thumbnail request shouldn't trigger N parallel sharp jobs.
  const key = `${username}/${folder}/${filename}`
  let job = _thumbInflight.get(key)
  if (!job) {
    job = thumbLimit(() => generateThumb(srcPath, dstPath))
      .finally(() => _thumbInflight.delete(key))
    _thumbInflight.set(key, job)
  }
  const ok = await job
  if (!ok) return res.status(500).send('Thumb generation failed')
  sendCached()
})

// Video preview GIF (generated on demand, cached to disk)
app.get('/thumbnail/:username/:filename', async (req, res) => {
  const { username, filename } = req.params
  if (!ffmpegPath) return res.status(503).send('ffmpeg not available')

  const userThumbDir = path.join(THUMB_DIR, username)
  fs.mkdirSync(userThumbDir, { recursive: true })
  const gifPath = path.join(
    userThumbDir,
    path.basename(filename, path.extname(filename)) + '.gif'
  )

  // Serve from cache
  if (fs.existsSync(gifPath)) {
    res.setHeader('Cache-Control', IMMUTABLE_CACHE)
    return res.sendFile(path.basename(gifPath), { root: userThumbDir }, (err) => {
      if (err && !res.headersSent) res.status(404).send('Not found')
    })
  }

  // Find the actual video file
  const videoPath = safeSubPath(datasetDir, username, 'webm', filename)
  if (!videoPath || !fs.existsSync(videoPath))
    return res.status(404).send('Not found')

  const ok = await generatePreviewGif(videoPath, gifPath)
  if (!ok) return res.status(404).send('Could not generate preview')

  res.setHeader('Cache-Control', IMMUTABLE_CACHE)
  res.sendFile(path.basename(gifPath), { root: userThumbDir }, (err) => {
    if (err && !res.headersSent) res.status(500).send('Error')
  })
})

// Cover image — returns the daily-pinned cover. Deterministic per (username, date),
// so it doesn't change on every page load and survives restarts without state.
// Kept for backwards compat; new clients should use the `cover` field embedded
// directly in /api/users to skip this round trip entirely.
app.get('/api/users/:username/cover', (req, res) => {
  const { username } = req.params
  const stats = modelStatsCache[username]
  if (!stats || !stats.coverPool || !stats.coverPool.length) {
    return res.status(404).json({ error: 'No media' })
  }
  const pick = pickCoverFor(username, stats.coverPool)
  if (!pick) return res.status(404).json({ error: 'No media' })
  res.setHeader('Cache-Control', 'public, max-age=3600')
  res.json({
    type:       pick.type,
    url:        pick.url,
    previewUrl: pick.previewUrl || null,
    thumbUrl:   pick.thumbUrl   || null,
  })
})

// ─── SCAN ENDPOINTS ──────────────────────────────────────────────────────────
app.get('/api/scan-status', (_req, res) => {
  res.json({ ...scanState, responseCacheSize: mediaResponseCache.size })
})

app.post('/api/rescan', async (_req, res) => {
  if (scanState.inProgress) {
    return res.status(202).json({ skipped: true, reason: 'already running', state: scanState })
  }
  // Kick off without awaiting — client polls /api/scan-status for progress.
  scanAll({ force: true, trigger: 'manual' }).catch((err) =>
    console.warn('  Manual scan error:', err.message)
  )
  res.json({ ok: true, state: scanState })
})

// Serve media files
app.get('/media/:username/:folder/:filename', (req, res) => {
  const { username, folder, filename } = req.params
  if (!MEDIA_FOLDERS.includes(folder)) return res.status(403).send('Forbidden')
  const filePath = safeSubPath(datasetDir, username, folder, filename)
  if (!filePath) return res.status(403).send('Forbidden')
  res.setHeader('Cache-Control', IMMUTABLE_CACHE)
  const relPath = path.relative(datasetDir, filePath)
  res.sendFile(relPath, { root: datasetDir }, (err) => {
    if (err && !res.headersSent) res.status(404).send('Not found')
  })
})

// ─── PREVIEW PREWARM ──────────────────────────────────────────────────────────
// Walks all model webm folders and pre-generates any missing animated GIF previews.
// Runs in the background after the server starts — doesn't block requests.
async function prewarmThumbnails() {
  if (!ffmpegPath) return

  let dirs
  try {
    dirs = await fs.promises.readdir(datasetDir, { withFileTypes: true })
  } catch {
    return
  }
  const modelDirs = dirs.filter(
    (e) => e.isDirectory() && !e.name.startsWith('.')
  )

  const allVideos = []
  for (const dir of modelDirs) {
    const webmDir = path.join(datasetDir, dir.name, 'webm')
    let files
    try {
      files = await fs.promises.readdir(webmDir)
    } catch {
      continue
    }
    for (const file of files) {
      if (!['.mp4', '.webm'].includes(path.extname(file).toLowerCase()))
        continue
      allVideos.push({
        username: dir.name,
        filename: file,
        videoPath: path.join(webmDir, file),
      })
    }
  }

  const missing = allVideos.filter(({ username, filename }) => {
    const gifPath = path.join(
      THUMB_DIR,
      username,
      path.basename(filename, path.extname(filename)) + '.gif'
    )
    return !fs.existsSync(gifPath)
  })

  if (missing.length === 0) {
    console.log(`  Previews:  all ${allVideos.length} cached ✓`)
    return
  }

  console.log(
    `  Previews:  generating ${missing.length} GIFs (${allVideos.length - missing.length} cached)…`
  )

  const concurrency = pLimit(2) // GIF encoding is CPU-heavy — keep concurrency low
  let done = 0
  await Promise.all(
    missing.map(({ username, filename, videoPath }) =>
      concurrency(async () => {
        const userThumbDir = path.join(THUMB_DIR, username)
        fs.mkdirSync(userThumbDir, { recursive: true })
        const gifPath = path.join(
          userThumbDir,
          path.basename(filename, path.extname(filename)) + '.gif'
        )
        await generatePreviewGif(videoPath, gifPath)
        done++
        if (done % 10 === 0 || done === missing.length) {
          process.stdout.write(`\r  Previews:  ${done}/${missing.length} done`)
        }
      })
    )
  )

  console.log(`\r  Previews:  ${missing.length} GIFs generated ✓          `)
}

// Walks the whole dataset and pre-generates any missing JPEG thumb for every
// image and gif in every model. Runs in the background — never blocks startup
// or responses. After the first nightly run, mobile visits to any model are
// served entirely from cached thumbnails. Set DISABLE_GRID_THUMB_PREWARM=1
// to skip if disk space is tight.
async function prewarmAllGridThumbs() {
  if (process.env.DISABLE_GRID_THUMB_PREWARM === '1') {
    console.log('  Thumbs:    grid prewarm disabled (DISABLE_GRID_THUMB_PREWARM=1)')
    return
  }
  let dirs
  try { dirs = await fs.promises.readdir(datasetDir, { withFileTypes: true }) }
  catch { return }
  const modelDirs = dirs.filter((e) => e.isDirectory() && !e.name.startsWith('.'))

  // Collect all (src, dst) pairs first so the progress line is meaningful.
  const tasks = []
  for (const d of modelDirs) {
    const username = d.name
    for (const folder of ['images', 'gif']) {
      let files
      try { files = await fs.promises.readdir(path.join(datasetDir, username, folder)) }
      catch { continue }
      for (const file of files) {
        const ext = path.extname(file).toLowerCase()
        if (!IMAGE_EXTS_IN.has(ext)) continue
        const dst = thumbDiskPath(username, folder, file)
        if (fs.existsSync(dst)) continue
        tasks.push({ src: path.join(datasetDir, username, folder, file), dst })
      }
    }
  }

  if (!tasks.length) {
    console.log('  Thumbs:    all grid thumbs cached ✓')
    return
  }

  console.log(`  Thumbs:    generating ${tasks.length} grid thumbs across ${modelDirs.length} models…`)
  const t0 = Date.now()
  let done = 0
  await Promise.all(tasks.map((t) => thumbLimit(async () => {
    await generateThumb(t.src, t.dst).catch(() => {})
    done++
    if (done % 100 === 0 || done === tasks.length) {
      process.stdout.write(`\r  Thumbs:    ${done}/${tasks.length} done`)
    }
  })))
  console.log(`\r  Thumbs:    ${tasks.length} grid thumbs generated in ${((Date.now() - t0) / 1000).toFixed(0)}s ✓        `)
}

// Walks modelStatsCache and pre-generates any missing cover JPEG thumbs so
// the first home-grid render hits cached files. Cheap — ~16 covers per model.
async function prewarmCoverThumbs() {
  const tasks = []
  for (const [username, stats] of Object.entries(modelStatsCache)) {
    for (const c of stats.coverPool || []) {
      if (c.type === 'video') continue // video covers reuse the GIF preview
      const dst = thumbDiskPath(username, c.folder, c.filename)
      if (fs.existsSync(dst)) continue
      const src = safeSubPath(datasetDir, username, c.folder, c.filename)
      if (!src || !fs.existsSync(src)) continue
      tasks.push({ src, dst, username })
    }
  }
  if (!tasks.length) {
    console.log('  Thumbs:    all cover thumbs cached ✓')
    return
  }
  console.log(`  Thumbs:    generating ${tasks.length} cover thumbs…`)
  let done = 0
  await Promise.all(tasks.map((t) => thumbLimit(async () => {
    await generateThumb(t.src, t.dst)
    done++
    if (done % 25 === 0 || done === tasks.length) {
      process.stdout.write(`\r  Thumbs:    ${done}/${tasks.length} done`)
    }
  })))
  console.log(`\r  Thumbs:    ${tasks.length} cover thumbs generated ✓        `)
}

// ─── INFO ENDPOINT ───────────────────────────────────────────────────────────
const SERVER_START = new Date().toISOString()

// Registry "last changed" tracker. mtime alone is unreliable: robocopy /XO
// can skip a file if its dest is newer, and git checkout sometimes leaves
// the local mtime older than the remote copy. Hash-based detection survives
// both. State is persisted so the timestamp stays meaningful across restarts.
const REGISTRY_STATE_FILE = path.join(THUMB_DIR, 'registry-state.json')
let _registryState = { hash: null, lastChanged: null }
try { _registryState = JSON.parse(fs.readFileSync(REGISTRY_STATE_FILE, 'utf8')) } catch {}
let _registryLastCheckMs = 0

function refreshRegistryState() {
  // Throttle hashing — model_aliases.json is small but /api/info gets polled
  // every 5 minutes by every open tab. 30s is plenty fresh.
  const now = Date.now()
  if (now - _registryLastCheckMs < 30 * 1000 && _registryState.lastChanged) {
    return _registryState.lastChanged
  }
  _registryLastCheckMs = now

  try {
    const content = fs.readFileSync(registryPath)
    const hash = crypto.createHash('sha256').update(content).digest('hex')
    if (_registryState.hash !== hash) {
      // Content actually changed. Prefer the file's mtime when it looks
      // sensible (newer than our previous record); otherwise stamp "now".
      const stat = fs.statSync(registryPath)
      const prevMs = _registryState.lastChanged
        ? new Date(_registryState.lastChanged).getTime()
        : 0
      const useMtime = stat.mtimeMs > prevMs && stat.mtimeMs <= now + 5000
      _registryState = {
        hash,
        lastChanged: (useMtime ? stat.mtime : new Date(now)).toISOString(),
      }
      try { fs.writeFileSync(REGISTRY_STATE_FILE, JSON.stringify(_registryState)) } catch {}
    }
  } catch {}
  return _registryState.lastChanged
}

app.get('/api/info', (_req, res) => {
  res.setHeader('Cache-Control', 'no-cache')
  res.json({
    startedAt: SERVER_START,
    registryUpdatedAt: refreshRegistryState(),
    scan: { lastTickAt: scanState.lastTickAt, lastFullScanAt: scanState.lastFullScanAt },
  })
})

// ─── STARTUP ──────────────────────────────────────────────────────────────────

const TICK_INTERVAL_MS = parseInt(process.env.SCAN_TICK_MS, 10) || 60 * 1000     // 60 s
const NIGHTLY_HOUR     = parseInt(process.env.SCAN_NIGHTLY_HOUR, 10) || 4         // 04:00 local

function msUntilNextHour(targetHour) {
  const now = new Date()
  const next = new Date(now)
  next.setHours(targetHour, 0, 0, 0)
  if (next <= now) next.setDate(next.getDate() + 1)
  return next - now
}

async function start() {
  await findFfTools()

  console.log(`\n  Dataset:   ${datasetDir}`)
  console.log(`  Previews:  ${THUMB_DIR}`)
  console.log(`  Registry:  ${registryPath}\n`)

  // Initial scan: short-circuits to disk cache when fingerprints match, so
  // subsequent restarts open near-instantly. First run rebuilds everything.
  await scanAll({ trigger: 'startup' })
    .catch((err) => console.warn('  Startup scan error:', err.message))

  app.listen(PORT, () => {
    console.log(`\n  Dataset Dashboard → http://localhost:${PORT}`)
    console.log(`  Tick:      every ${TICK_INTERVAL_MS / 1000}s — nightly full scan at ${NIGHTLY_HOUR}:00`)
    console.log(`  Response cache: LRU(${RESPONSE_CACHE_MAX}) in memory + disk\n`)
  })

  // Cover thumbs first — these block the home grid being fast. Cheap (sharp).
  prewarmCoverThumbs()
    .catch((err) => console.warn('  Cover thumb prewarm error:', err.message))
    // Grid thumbs second — once covers are done, walk every image/gif in
    // every model and pre-generate the small JPEG. After this finishes,
    // mobile visits to any model are served entirely from disk cache.
    .then(() => prewarmAllGridThumbs())
    .catch((err) => console.warn('  Grid thumb prewarm error:', err.message))

  // GIF generation runs after the server is up — it's CPU-heavy and not needed
  // for page loads (the media endpoint works without previews being ready).
  prewarmThumbnails().catch((err) => console.warn('  Preview prewarm error:', err.message))

  // Cheap fingerprint tick: every minute, stat each model's folder mtimes and
  // rescan only the ones that changed. Picks up new files within ~1 tick.
  const scheduleNextTick = () => {
    setTimeout(async () => {
      try { await fingerprintTick() }
      catch (err) { console.warn('  Tick error:', err.message) }
      scheduleNextTick()
    }, TICK_INTERVAL_MS)
  }
  scheduleNextTick()

  // Nightly safety net at NIGHTLY_HOUR — force-rebuilds everything, also
  // refreshes the deterministic daily cover seed and regenerates any new GIF
  // previews. Catches anything the fingerprint tick missed (e.g. server was
  // offline during a sync).
  const scheduleNightly = () => {
    setTimeout(async () => {
      try { await scanAll({ force: true, trigger: 'nightly' }) }
      catch (err) { console.warn('  Nightly scan error:', err.message) }
      try { await prewarmCoverThumbs() }
      catch (err) { console.warn('  Cover thumb prewarm (nightly) error:', err.message) }
      try { await prewarmAllGridThumbs() }
      catch (err) { console.warn('  Grid thumb prewarm (nightly) error:', err.message) }
      try { await prewarmThumbnails() }
      catch (err) { console.warn('  Preview prewarm (nightly) error:', err.message) }
      scheduleNightly()
    }, msUntilNextHour(NIGHTLY_HOUR))
  }
  scheduleNightly()
}

start()

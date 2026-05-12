'use strict'

const express = require('express')
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

function loadResponseCacheFromDisk(username) {
  try {
    const data = JSON.parse(fs.readFileSync(path.join(RESPONSE_CACHE_DIR, `${username}.json`), 'utf8'))
    if (data.fingerprint && Array.isArray(data.response)) return data
  } catch {}
  return null
}

function saveResponseCacheToDisk(username, response, fingerprint) {
  setImmediate(() => {
    try {
      fs.writeFileSync(
        path.join(RESPONSE_CACHE_DIR, `${username}.json`),
        JSON.stringify({ fingerprint, response })
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

app.use(express.urlencoded({ extended: false }))

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
  const pool = []
  const step = Math.max(1, Math.floor(allMedia.length / max))
  for (let i = 0; i < allMedia.length && pool.length < max; i += step) {
    const m = allMedia[i]
    pool.push({ type: m.type, folder: m.folder, filename: m.filename, url: m.url })
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

// Build the full response record for a single file. Reuses metaCache when valid.
async function processFileForResponse(username, userDir, item) {
  const ext = path.extname(item.filename).toLowerCase()
  const isVideo = VIDEO_EXTS_IN.has(ext)
  const stat = await fs.promises.stat(item.filePath).catch(() => null)
  if (!stat) return null

  let width = 0, height = 0, duration = 0, videoDate
  let metaUpdated = false
  const hit = metaCache.get(username, item.folder, item.filename, stat)
  if (hit) {
    width = hit.width || 0
    height = hit.height || 0
    duration = hit.duration || 0
    videoDate = hit.videoDate
  } else {
    if (isVideo) {
      const probed = await mediaDates.probeVideoFile(item.filePath).catch(() => ({}))
      duration = probed.duration || 0
      videoDate = probed.videoDate || undefined
    } else if (IMAGE_EXTS_IN.has(ext)) {
      try {
        const m = await sharp(item.filePath).metadata()
        width = m.width || 0; height = m.height || 0
      } catch {}
    }
    metaCache.set(username, item.folder, item.filename, stat, isVideo
      ? { duration, ...(videoDate !== undefined && { videoDate }) }
      : { width, height })
    metaUpdated = true
  }

  const type = getMediaType(item.folder, item.filename)
  const dateMeta = await resolveDateForFile(
    userDir, item.folder, item.filename, item.filePath,
    { cachedVideoDate: videoDate, stat }
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
        cover:         cover ? { type: cover.type, url: cover.url } : null,
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
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
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
  res.json({ type: pick.type, url: pick.url })
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

// ─── INFO ENDPOINT ───────────────────────────────────────────────────────────
const SERVER_START = new Date().toISOString()

app.get('/api/info', (_req, res) => {
  let registryMtime = null
  try {
    registryMtime = fs.statSync(registryPath).mtime.toISOString()
  } catch {}
  res.json({
    startedAt: SERVER_START,
    registryUpdatedAt: registryMtime,
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
      try { await prewarmThumbnails() }
      catch (err) { console.warn('  Preview prewarm (nightly) error:', err.message) }
      scheduleNightly()
    }, msUntilNextHour(NIGHTLY_HOUR))
  }
  scheduleNightly()
}

start()

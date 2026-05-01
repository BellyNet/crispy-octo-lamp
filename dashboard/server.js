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

async function getDuration(videoPath) {
  if (!ffprobePath) return null
  try {
    const { stdout } = await execFileAsync(
      ffprobePath,
      ['-v', 'quiet', '-print_format', 'json', '-show_format', videoPath],
      { timeout: 10000 }
    )
    const d = parseFloat(JSON.parse(stdout)?.format?.duration)
    return isFinite(d) && d > 0 ? d : null
  } catch {
    return null
  }
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

function buildSourceMap() {
  try {
    const registry = loadModelRegistry(registryPath)
    const map = {}
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
    return map
  } catch {
    return {}
  }
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

async function resolveDateForFile(userDir, folder, filename, filePath) {
  const fromSidecar = mediaDates.resolveDateFromSidecar(
    userDir,
    folder,
    filename
  )
  if (fromSidecar && fromSidecar.date) return fromSidecar

  const ext = path.extname(filename).toLowerCase()
  let result = null

  if (['.mp4', '.webm', '.mov'].includes(ext)) {
    const videoDate = await mediaDates.extractVideoDateFromFile(filePath)
    if (videoDate) result = { date: videoDate, source: 'mp4' }
  }

  if (!result) {
    const filenameDate = mediaDates.extractFilenameDate(filename)
    if (filenameDate) result = { date: filenameDate, source: 'filename' }
  }

  if (!result) {
    try {
      const stat = await fs.promises.stat(filePath)
      if (isSane(stat.mtime))
        result = { date: stat.mtime.toISOString(), source: 'uploaded' }
      else if (isSane(stat.birthtime))
        result = { date: stat.birthtime.toISOString(), source: 'filesystem' }
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

// ─── MODEL STATS CACHE ────────────────────────────────────────────────────────
// Fast scan: sidecar + filename dates only (no ffprobe), stat for addedMs.
// Runs in the background; /api/users returns zeros until first scan completes.

let modelStatsCache = {} // { username: { earliestMs, latestMs, latestAddedMs, fileCount } }

async function buildModelStats() {
  let dirs
  try {
    dirs = await fs.promises.readdir(datasetDir, { withFileTypes: true })
  } catch { return }

  const modelDirs = dirs.filter((e) => e.isDirectory() && !e.name.startsWith('.'))
  const limit = pLimit(32) // high concurrency — mostly fast filename + stat ops
  const result = {}

  await Promise.all(modelDirs.map((dir) => limit(async () => {
    const username = dir.name
    const userDir  = path.join(datasetDir, username)
    let earliestMs = Infinity, latestMs = 0, latestAddedMs = 0, fileCount = 0
    const yearCounts = {} // { year: fileCount }

    for (const folder of MEDIA_FOLDERS) {
      const folderPath = path.join(userDir, folder)
      let files
      try { files = await fs.promises.readdir(folderPath) } catch { continue }

      for (const file of files) {
        if (!MEDIA_EXTS.has(path.extname(file).toLowerCase())) continue
        fileCount++

        // Fast date: sidecar first, then filename pattern
        let dateMs = 0
        let dateIsReal = false
        const sidecar = mediaDates.resolveDateFromSidecar(userDir, folder, file)
        if (sidecar?.date) {
          dateMs = new Date(sidecar.date).getTime()
          dateIsReal = true
        } else {
          const fn = mediaDates.extractFilenameDate(file)
          if (fn) { dateMs = new Date(fn).getTime(); dateIsReal = true }
        }

        // Filesystem stat — used for addedMs and as date fallback
        try {
          const st = await fs.promises.stat(path.join(folderPath, file))
          const addedMs = st.birthtime.getTime() > 0 ? st.birthtime.getTime() : st.mtime.getTime()
          if (addedMs > latestAddedMs) latestAddedMs = addedMs
          // Fall back to filesystem date for yearCounts only if no real date found
          if (!dateIsReal && isSane(new Date(addedMs))) dateMs = addedMs
        } catch {}

        if (dateMs > 0 && isSane(new Date(dateMs))) {
          // Only update earliest/latest from real media dates, not filesystem fallback
          if (dateIsReal) {
            if (dateMs < earliestMs) earliestMs = dateMs
            if (dateMs > latestMs)   latestMs   = dateMs
          }
          const yr = new Date(dateMs).getFullYear()
          yearCounts[yr] = (yearCounts[yr] || 0) + 1
        }
      }
    }

    result[username] = {
      earliestMs:    earliestMs === Infinity ? 0 : earliestMs,
      latestMs,
      latestAddedMs,
      fileCount,
      yearCounts,
    }
  })))

  modelStatsCache = result
  console.log(`  Stats:     indexed ${modelDirs.length} models ✓`)
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

app.use(express.static(__dirname))
app.get('/', (_req, res) => res.sendFile('index.html', { root: __dirname }))

// Users list — returns [{ name, sources, featured }, ...]
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

    const featuredModel = getFeaturedModel(users.map((u) => u.name))
    res.json(users.map((u) => {
      const s = modelStatsCache[u.name] || {}
      return {
        ...u,
        featured:      u.name === featuredModel,
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

// Image dimension cache: filePath → { width, height }
const _dimCache = new Map()
async function getImageDimensions(filePath) {
  if (_dimCache.has(filePath)) return _dimCache.get(filePath)
  try {
    const meta = await sharp(filePath).metadata()
    const dims = { width: meta.width || 0, height: meta.height || 0 }
    _dimCache.set(filePath, dims)
    return dims
  } catch {
    return { width: 0, height: 0 }
  }
}

// Media list for a user
app.get('/api/users/:username/media', async (req, res) => {
  const username = req.params.username
  const userDir = safeSubPath(datasetDir, username)
  if (!userDir) return res.status(403).json({ error: 'Forbidden' })

  const rawFiles = []
  for (const folder of MEDIA_FOLDERS) {
    const folderPath = path.join(userDir, folder)
    let files
    try {
      files = await fs.promises.readdir(folderPath)
    } catch {
      continue
    }
    for (const file of files) {
      if (!MEDIA_EXTS.has(path.extname(file).toLowerCase())) continue
      rawFiles.push({
        filename: file,
        folder,
        filePath: path.join(folderPath, file),
        type: getMediaType(folder, file),
        url: `/media/${encodeURIComponent(username)}/${folder}/${encodeURIComponent(file)}`,
      })
    }
  }

  const limit = pLimit(16)
  const allMedia = await Promise.all(
    rawFiles.map((item) =>
      limit(async () => {
        const isVideo = item.type === 'video'
        const [meta, stat, duration, dims] = await Promise.all([
          resolveDateForFile(userDir, item.folder, item.filename, item.filePath),
          fs.promises.stat(item.filePath).catch(() => null),
          isVideo ? getDuration(item.filePath) : Promise.resolve(null),
          isVideo ? Promise.resolve({ width: 0, height: 0 }) : getImageDimensions(item.filePath),
        ])
        return {
          filename: item.filename,
          folder: item.folder,
          type: item.type,
          url: item.url,
          date: meta.date,
          source: meta.source,
          // mediaDateMs: real media date only (EXIF / sidecar / filename / mp4 metadata).
          // Never set from filesystem fallback — used for "media date" sorts so that
          // recently-downloaded files don't appear as "newest" content.
          mediaDateMs: (meta.source && meta.source !== 'filesystem' && meta.date)
            ? new Date(meta.date).getTime() : 0,
          addedMs: stat
            ? (stat.birthtime.getTime() > 0 ? stat.birthtime.getTime() : stat.mtime.getTime())
            : 0,
          size: stat ? stat.size : 0,
          duration: duration || 0,
          width: dims.width,
          height: dims.height,
          previewUrl: isVideo
            ? `/thumbnail/${encodeURIComponent(username)}/${encodeURIComponent(item.filename)}`
            : null,
        }
      })
    )
  )

  // Default order: real media date asc, falling back to added-to-disk date
  allMedia.sort((a, b) => (a.mediaDateMs || a.addedMs) - (b.mediaDateMs || b.addedMs))
  res.json(allMedia)
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

  res.sendFile(path.basename(gifPath), { root: userThumbDir }, (err) => {
    if (err && !res.headersSent) res.status(500).send('Error')
  })
})

// Cover image — random media pick for home-view cards; returns { type, url }
app.get('/api/users/:username/cover', async (req, res) => {
  const { username } = req.params
  const userDir = safeSubPath(datasetDir, username)
  if (!userDir) return res.status(403).send('Forbidden')

  const candidates = []
  for (const [folder, exts] of [
    ['images', ['.jpg', '.jpeg', '.png', '.webp']],
    ['gif',    ['.gif']],
    ['webm',   ['.mp4', '.webm']],
  ]) {
    const folderPath = path.join(userDir, folder)
    let files
    try { files = await fs.promises.readdir(folderPath) } catch { continue }
    for (const f of files) {
      if (exts.includes(path.extname(f).toLowerCase())) {
        candidates.push({
          type: folder === 'webm' ? 'video' : folder === 'gif' ? 'gif' : 'image',
          folder,
          filename: f,
        })
      }
    }
  }

  if (!candidates.length) return res.status(404).json({ error: 'No media' })
  const pick = candidates[Math.floor(Math.random() * candidates.length)]
  res.json({
    type: pick.type,
    url: `/media/${encodeURIComponent(username)}/${pick.folder}/${encodeURIComponent(pick.filename)}`,
  })
})

// Serve media files
app.get('/media/:username/:folder/:filename', (req, res) => {
  const { username, folder, filename } = req.params
  if (!MEDIA_FOLDERS.includes(folder)) return res.status(403).send('Forbidden')
  const filePath = safeSubPath(datasetDir, username, folder, filename)
  if (!filePath) return res.status(403).send('Forbidden')
  const relPath = path.relative(datasetDir, filePath)
  res.sendFile(relPath, { root: datasetDir }, (err) => {
    if (err && !res.headersSent) res.status(404).send('Not found')
  })
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

  if (fs.existsSync(gifPath)) {
    return res.sendFile(path.basename(gifPath), { root: userThumbDir }, (err) => {
      if (err && !res.headersSent) res.status(404).send('Not found')
    })
  }

  const videoPath = safeSubPath(datasetDir, username, 'webm', filename)
  if (!videoPath || !fs.existsSync(videoPath))
    return res.status(404).send('Not found')

  const ok = await generatePreviewGif(videoPath, gifPath)
  if (!ok) return res.status(404).send('Could not generate preview')

  res.sendFile(path.basename(gifPath), { root: userThumbDir }, (err) => {
    if (err && !res.headersSent) res.status(500).send('Error')
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
  })
})

// ─── STARTUP ──────────────────────────────────────────────────────────────────

const PREWARM_INTERVAL_MS = 30 * 60 * 1000 // 30 minutes

async function start() {
  await findFfTools()
  app.listen(PORT, () => {
    console.log(`\n  Dataset Dashboard → http://localhost:${PORT}`)
    console.log(`  Dataset:   ${datasetDir}`)
    console.log(`  Previews:  ${THUMB_DIR}`)
    console.log(`  Registry:  ${registryPath}`)
    console.log(`  Prewarm:   every ${PREWARM_INTERVAL_MS / 60000} min\n`)
  })

  // Initial background tasks
  buildModelStats().catch((err) => console.warn('  Stats scan error:', err.message))
  prewarmThumbnails().catch((err) => console.warn('  Preview prewarm error:', err.message))

  // Periodic refresh — picks up new models / videos synced to the NAS
  setInterval(() => {
    buildModelStats().catch((err) => console.warn('  Stats scan error:', err.message))
    prewarmThumbnails().catch((err) => console.warn('  Preview prewarm (periodic) error:', err.message))
  }, PREWARM_INTERVAL_MS)
}

start()

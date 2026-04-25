'use strict'

const express = require('express')
const path = require('path')
const fs = require('fs')
const https = require('https')
const { execFile } = require('child_process')
const { promisify } = require('util')
const pLimit = require('p-limit')

const execFileAsync = promisify(execFile)
const mediaDates = require('../milkmaid/media-dates.js')

const app = express()
const PORT = process.env.DASHBOARD_PORT || 3420

const APPDATA =
  process.env.APPDATA ||
  path.join(process.env.HOME || process.env.USERPROFILE, 'AppData', 'Roaming')
const slopvaultRoot = path.join(APPDATA, '.slopvault')
const datasetDir = process.env.DATASET_DIR || path.join(slopvaultRoot, 'dataset')
const THUMB_DIR = path.join(slopvaultRoot, '.dashboard-thumbs')

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
    const guess = ffprobeFound.replace(/ffprobe(\.exe)?$/i, (m) => m.replace('ffprobe', 'ffmpeg'))
    try {
      await execFileAsync(guess, ['-version'], { timeout: 3000 })
      ffmpegPath = guess
      console.log(`  ffmpeg:  ${guess}`)
    } catch {}
  }
  if (!ffmpegPath) console.log('  ffmpeg: not found — video thumbnails unavailable')
}

// ─── VIDEO THUMBNAILS ─────────────────────────────────────────────────────────
// Tries frames at 20%, 35%, 50% of duration. A mostly-black frame compresses to
// a tiny JPEG, so we use file size as a proxy for "useful frame" and skip it.

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
  } catch { return null }
}

async function extractFrame(videoPath, seekSec, outPath) {
  if (!ffmpegPath) return false
  try {
    await execFileAsync(ffmpegPath, [
      '-ss', seekSec.toFixed(2),
      '-i', videoPath,
      '-vframes', '1',
      '-q:v', '4',
      '-vf', 'scale=360:-2',
      '-y', outPath,
    ], { timeout: 20000 })
    const stat = fs.statSync(outPath)
    return stat.size > 8000 // < 8 KB → near-black frame, skip
  } catch { return false }
}

async function generateThumbnail(videoPath, thumbPath) {
  const duration = await getDuration(videoPath)
  if (!duration) return false

  const seekPoints = [0.20, 0.35, 0.50, 0.65].map((p) => duration * p)

  for (const seek of seekPoints) {
    const tmp = thumbPath + '.tmp.jpg'
    const ok = await extractFrame(videoPath, seek, tmp)
    if (ok) {
      fs.renameSync(tmp, thumbPath)
      return true
    }
    try { fs.unlinkSync(tmp) } catch {}
  }
  return false
}

// ─── WIKI ─────────────────────────────────────────────────────────────────────
// Uses the MediaWiki API (wikitext) to avoid parsing HTML.
const wikiCache = new Map() // username → { data, fetchedAt }
const WIKI_TTL_MS = 60 * 60 * 1000 // 1 hour

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'dashboard/1.0' } }, (res) => {
      let body = ''
      res.on('data', (c) => (body += c))
      res.on('end', () => resolve({ status: res.statusCode, body }))
    }).on('error', reject)
  })
}

// Convert username → candidate wiki page titles to try
function wikiTitleCandidates(username) {
  const capitalize = (s) => s.charAt(0).toUpperCase() + s.slice(1)
  const words = username.replace(/-/g, '_').split('_').filter(Boolean)
  const titleCase = words.map(capitalize).join('_')
  const firstOnly = capitalize(username.replace(/_/g, ''))
  const raw = capitalize(username)
  return [...new Set([titleCase, raw, firstOnly])]
}

// Parse infobox template fields from MediaWiki wikitext
function parseWikitext(wikitext) {
  const info = {}

  // Extract template block {{Model ... }} or {{Infobox ... }}
  const tmplMatch = wikitext.match(/\{\{[^\n]*?\n([\s\S]*?)\}\}/m)
  if (tmplMatch) {
    const lines = tmplMatch[1].split('\n')
    for (const line of lines) {
      const m = line.match(/^\s*\|\s*([^=]+?)\s*=\s*(.*)/)
      if (m) info[m[1].toLowerCase().trim()] = m[2].replace(/\[\[([^\]|]+)(?:\|[^\]]*)?\]\]/g, '$1').trim()
    }
  }

  // Pull lead paragraph (first real prose after the template)
  const afterTemplate = wikitext.replace(/\{\{[\s\S]*?\}\}/g, '').replace(/==.*?==/g, '').trim()
  const prose = afterTemplate
    .split('\n')
    .map((l) => l.replace(/\[\[([^\]|]+)(?:\|[^\]]*)?\]\]/g, '$1').replace(/'{2,}/g, '').trim())
    .filter((l) => l.length > 40)
  if (prose.length) info._bio = prose[0]

  return info
}

// Field aliases across different wiki infobox templates
const FIELD_MAP = {
  height:      ['height'],
  weight:      ['weight', 'current weight'],
  cup:         ['cup', 'cup size', 'bra size'],
  birthdate:   ['birthdate', 'birth date', 'born', 'dob'],
  origin:      ['origin', 'nationality', 'country', 'location', 'from'],
  weightclass: ['weightclass', 'weight class', 'type', 'classification'],
  bodytype:    ['bodytype', 'body type', 'shape'],
  status:      ['status', 'activity status'],
  onlyfans:    ['onlyfans', 'of'],
  instagram:   ['instagram', 'ig'],
  twitter:     ['twitter'],
  curvage:     ['curvage'],
  youtube:     ['youtube'],
}

function normaliseWikiInfo(raw) {
  const out = {}
  for (const [key, aliases] of Object.entries(FIELD_MAP)) {
    for (const alias of aliases) {
      if (raw[alias] && raw[alias] !== '') { out[key] = raw[alias]; break }
    }
  }
  if (raw._bio) out.bio = raw._bio
  return out
}

async function fetchWikiInfo(username) {
  const cached = wikiCache.get(username)
  if (cached && Date.now() - cached.fetchedAt < WIKI_TTL_MS) return cached.data

  const candidates = wikiTitleCandidates(username)
  for (const title of candidates) {
    const url = `https://bbw.wiki/api.php?action=parse&page=${encodeURIComponent(title)}&prop=wikitext&format=json&formatversion=2`
    try {
      const { status, body } = await httpsGet(url)
      if (status !== 200) continue
      const json = JSON.parse(body)
      if (json.error) continue
      const wikitext = json.parse?.wikitext
      if (!wikitext) continue
      const raw = parseWikitext(wikitext)
      const data = { found: true, title, ...normaliseWikiInfo(raw) }
      wikiCache.set(username, { data, fetchedAt: Date.now() })
      return data
    } catch {}
  }

  const data = { found: false }
  wikiCache.set(username, { data, fetchedAt: Date.now() })
  return data
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
  if (resolved !== baseResolved && !resolved.startsWith(baseResolved + path.sep)) return null
  return resolved
}

function isSane(date) {
  if (!date || isNaN(date.getTime())) return false
  const y = date.getFullYear()
  return y >= 1990 && y <= 2035
}

// ─── DATE RESOLUTION ──────────────────────────────────────────────────────────

async function resolveDateForFile(userDir, folder, filename, filePath) {
  const fromSidecar = mediaDates.resolveDateFromSidecar(userDir, folder, filename)
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
      if (isSane(stat.mtime)) result = { date: stat.mtime.toISOString(), source: 'uploaded' }
      else if (isSane(stat.birthtime)) result = { date: stat.birthtime.toISOString(), source: 'filesystem' }
    } catch {}
  }

  return result || { date: null, source: null }
}

// ─── ROUTES ───────────────────────────────────────────────────────────────────

app.use(express.static(__dirname))
app.get('/', (_req, res) => res.sendFile('index.html', { root: __dirname }))

// Users list
app.get('/api/users', async (_req, res) => {
  try {
    const entries = await fs.promises.readdir(datasetDir, { withFileTypes: true })
    const users = entries
      .filter((e) => e.isDirectory() && !e.name.startsWith('.'))
      .map((e) => e.name)
      .sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }))
    res.json(users)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// Wiki info for a user
app.get('/api/wiki/:username', async (req, res) => {
  try {
    const data = await fetchWikiInfo(req.params.username)
    res.json(data)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// Media list for a user
app.get('/api/users/:username/media', async (req, res) => {
  const username = req.params.username
  const userDir = safeSubPath(datasetDir, username)
  if (!userDir) return res.status(403).json({ error: 'Forbidden' })

  const rawFiles = []
  for (const folder of MEDIA_FOLDERS) {
    const folderPath = path.join(userDir, folder)
    let files
    try { files = await fs.promises.readdir(folderPath) } catch { continue }
    for (const file of files) {
      if (!MEDIA_EXTS.has(path.extname(file).toLowerCase())) continue
      rawFiles.push({
        filename: file, folder,
        filePath: path.join(folderPath, file),
        type: getMediaType(folder, file),
        url: `/media/${encodeURIComponent(username)}/${folder}/${encodeURIComponent(file)}`,
      })
    }
  }

  const limit = pLimit(16)
  const allMedia = await Promise.all(
    rawFiles.map((item) => limit(async () => {
      const meta = await resolveDateForFile(userDir, item.folder, item.filename, item.filePath)
      const isVideo = item.type === 'video'
      return {
        filename: item.filename, folder: item.folder,
        type: item.type, url: item.url,
        date: meta.date, source: meta.source,
        dateMs: meta.date ? new Date(meta.date).getTime() : 0,
        thumbnailUrl: isVideo
          ? `/thumbnail/${encodeURIComponent(username)}/${encodeURIComponent(item.filename)}`
          : null,
      }
    }))
  )

  allMedia.sort((a, b) => a.dateMs - b.dateMs)
  res.json(allMedia)
})

// Video thumbnail (generated on demand, cached to disk)
app.get('/thumbnail/:username/:filename', async (req, res) => {
  const { username, filename } = req.params
  if (!ffmpegPath) return res.status(503).send('ffmpeg not available')

  const userThumbDir = path.join(THUMB_DIR, username)
  fs.mkdirSync(userThumbDir, { recursive: true })
  const thumbPath = path.join(userThumbDir, path.basename(filename, path.extname(filename)) + '.jpg')

  // Serve from cache
  if (fs.existsSync(thumbPath)) {
    return res.sendFile(path.basename(thumbPath), { root: userThumbDir }, (err) => {
      if (err && !res.headersSent) res.status(404).send('Not found')
    })
  }

  // Find the actual video file (could be in webm folder)
  const videoPath = safeSubPath(datasetDir, username, 'webm', filename)
  if (!videoPath || !fs.existsSync(videoPath)) return res.status(404).send('Not found')

  const ok = await generateThumbnail(videoPath, thumbPath)
  if (!ok) return res.status(404).send('Could not generate thumbnail')

  res.sendFile(path.basename(thumbPath), { root: userThumbDir }, (err) => {
    if (err && !res.headersSent) res.status(500).send('Error')
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

// ─── STARTUP ──────────────────────────────────────────────────────────────────

async function start() {
  await findFfTools()
  app.listen(PORT, () => {
    console.log(`\n  Dataset Dashboard → http://localhost:${PORT}`)
    console.log(`  Dataset:   ${datasetDir}`)
    console.log(`  Thumbs:    ${THUMB_DIR}\n`)
  })
}

start()

'use strict'

// Remux non-faststart .mp4 / .m4v files in place so the moov atom sits at
// the start of the file. Without faststart the browser has to range-fetch
// the tail of every video before it can start playback — a 200-500 ms hit
// per click even on a fast LAN, very noticeable on iPhone.
//
// This is *not* re-encoding. ffmpeg streams the existing codec data with
// -c copy and rewrites the box order with -movflags +faststart. Typical
// per-file cost is a fraction of a second; on a 300 MB clip from the NAS
// it's bound by disk I/O, not CPU.
//
// Same shape as transcodeWebm.js — utility + CLI + per-user walker that
// nasSync.syncModelToNas calls on every per-model sync, so newly scraped
// non-faststart files get fixed before they reach the dashboard.
//
//   node scrapyard/faststartMp4.js              # all users (local dataset)
//   node scrapyard/faststartMp4.js <username>   # one user
//   DATASET_DIR='Z:\dataset' node scrapyard/faststartMp4.js  # NAS pass

const fs = require('fs')
const path = require('path')
const { execFile } = require('child_process')
const { promisify } = require('util')

const { findFfprobe } = require('./mediaDates')

const execFileAsync = promisify(execFile)

const VIDEO_EXTS = new Set(['.mp4', '.m4v'])
const MEDIA_FOLDERS_TO_SCAN = ['images', 'gif', 'webm']
const HEADER_BYTES = 65536 // moov/mdat marker is always in the first ~64 KB

let ffmpegPath = null

async function ensureFfmpeg() {
  if (ffmpegPath) return ffmpegPath
  const probe = await findFfprobe()
  if (!probe) return null
  const guess = probe.replace(/ffprobe(\.exe)?$/i, (m) =>
    m.replace('ffprobe', 'ffmpeg')
  )
  try {
    await execFileAsync(guess, ['-version'], { timeout: 3000 })
    ffmpegPath = guess
    return guess
  } catch {
    return null
  }
}

// Returns true if `moov` appears before `mdat` in the first 64 KB of the
// file (faststart), false if `mdat` appears first (non-faststart), null if
// neither marker shows up (rare — unrecognized container or truncated).
async function isFaststart(filePath) {
  let fd
  try {
    fd = await fs.promises.open(filePath, 'r')
    const buf = Buffer.alloc(HEADER_BYTES)
    const { bytesRead } = await fd.read(buf, 0, HEADER_BYTES, 0)
    const head = buf.slice(0, bytesRead).toString('latin1')
    const moov = head.indexOf('moov')
    const mdat = head.indexOf('mdat')
    if (moov === -1 && mdat === -1) return null
    if (moov === -1) return false
    if (mdat === -1) return true
    return moov < mdat
  } catch {
    return null
  } finally {
    if (fd) await fd.close().catch(() => {})
  }
}

// ffmpeg -c copy -movflags +faststart writes a new file with the moov
// atom relocated to the start. We write to a temp path and only rename
// over the original on success — never destructive on a failed run.
async function remuxFaststart(filePath, { log = console } = {}) {
  const ff = await ensureFfmpeg()
  if (!ff) return { ok: false, srcPath: filePath, reason: 'no-ffmpeg' }
  if (!VIDEO_EXTS.has(path.extname(filePath).toLowerCase()))
    return { ok: false, srcPath: filePath, reason: 'not-video' }
  if (!fs.existsSync(filePath))
    return { ok: false, srcPath: filePath, reason: 'missing-source' }

  const tmpPath = filePath + '.faststart.tmp.mp4'
  try {
    await execFileAsync(
      ff,
      [
        '-y', '-hide_banner', '-loglevel', 'warning',
        '-i', filePath,
        '-c', 'copy',
        '-movflags', '+faststart',
        tmpPath,
      ],
      { timeout: 10 * 60 * 1000, maxBuffer: 4 << 20 }
    )
  } catch (err) {
    try { fs.unlinkSync(tmpPath) } catch {}
    return { ok: false, srcPath: filePath, reason: 'ffmpeg-failed', err: err.message }
  }

  let outStat
  try { outStat = fs.statSync(tmpPath) } catch {
    return { ok: false, srcPath: filePath, reason: 'no-output' }
  }
  // Refuse anything wildly different from the source size — a sanity gate
  // against catastrophically truncated outputs.
  const srcSize = fs.statSync(filePath).size
  if (outStat.size < Math.min(1024, srcSize * 0.5)) {
    try { fs.unlinkSync(tmpPath) } catch {}
    return { ok: false, srcPath: filePath, reason: 'output-too-small' }
  }

  try { fs.renameSync(tmpPath, filePath) } catch (err) {
    return { ok: false, srcPath: filePath, reason: 'rename-failed', err: err.message }
  }
  return { ok: true, srcPath: filePath }
}

// Walks every media folder under dataset/<user>/ for .mp4/.m4v files,
// detects non-faststart ones, and remuxes them. Called from nasSync per
// model and from the CLI for the whole dataset.
async function faststartInUserDir(userDir, { log = console } = {}) {
  const candidates = []
  for (const folder of MEDIA_FOLDERS_TO_SCAN) {
    let files
    try {
      files = await fs.promises.readdir(path.join(userDir, folder))
    } catch {
      continue
    }
    for (const f of files) {
      if (VIDEO_EXTS.has(path.extname(f).toLowerCase()))
        candidates.push(path.join(userDir, folder, f))
    }
  }
  if (!candidates.length) return []

  // Filter to just non-faststart so we don't shell out for files already
  // optimized. The byte check is cheap (one 64 KB read).
  const targets = []
  for (const p of candidates) {
    const fs_ok = await isFaststart(p)
    if (fs_ok === false) targets.push(p)
  }
  if (!targets.length) return []

  const ff = await ensureFfmpeg()
  if (!ff) {
    log.warn?.(
      `ffmpeg not found — skipping ${targets.length} non-faststart file(s) in ${userDir}`
    )
    return []
  }

  const userName = path.basename(userDir)
  const results = []
  for (const filePath of targets) {
    const rel = path.relative(userDir, filePath).replace(/\\/g, '/')
    log.log?.(`  Faststart ${userName}/${rel}…`)
    const r = await remuxFaststart(filePath, { log })
    results.push(r)
    if (r.ok) log.log?.(`    → ok`)
    else log.warn?.(`    → failed: ${r.reason}${r.err ? ` (${r.err})` : ''}`)
  }
  return results
}

module.exports = {
  ensureFfmpeg,
  isFaststart,
  remuxFaststart,
  faststartInUserDir,
}

// ─── CLI ──────────────────────────────────────────────────────────────────────
if (require.main === module) {
  ;(async () => {
    require('dotenv').config({ path: path.join(__dirname, '..', '.env') })
    const APPDATA =
      process.env.APPDATA ||
      path.join(process.env.HOME || process.env.USERPROFILE, 'AppData', 'Roaming')
    // DATASET_DIR wins over .env-supplied LOCAL_DATASET_DIR so the same
    // script can sweep the NAS mount via `DATASET_DIR=Z:\\dataset node …`.
    const datasetDir =
      process.env.DATASET_DIR ||
      process.env.LOCAL_DATASET_DIR ||
      path.join(APPDATA, '.slopvault', 'dataset')

    if (!fs.existsSync(datasetDir)) {
      console.error(`Dataset dir not found: ${datasetDir}`)
      process.exit(1)
    }

    const only = process.argv[2]
    let userDirs
    if (only) {
      userDirs = [path.join(datasetDir, only)]
    } else {
      const entries = await fs.promises.readdir(datasetDir, { withFileTypes: true })
      userDirs = entries
        .filter((e) => e.isDirectory() && !e.name.startsWith('.'))
        .map((e) => path.join(datasetDir, e.name))
    }

    console.log(`Scanning ${userDirs.length} user dir(s) under ${datasetDir}\n`)
    let total = 0
    let ok = 0
    let failed = 0
    for (const ud of userDirs) {
      const results = await faststartInUserDir(ud)
      total += results.length
      ok += results.filter((r) => r.ok).length
      failed += results.filter((r) => !r.ok).length
    }
    console.log(
      `\nDone. ${ok}/${total} remuxed${failed ? `, ${failed} failed` : ''}`
    )
  })().catch((err) => {
    console.error(err)
    process.exit(1)
  })
}

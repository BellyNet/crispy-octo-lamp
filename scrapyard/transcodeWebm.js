'use strict'

// Convert .webm videos in the dataset to iOS-friendly .mp4 (H.264 + AAC).
// iPhone Safari can't play VP9/Opus/WebM under any circumstances, so any
// .webm in the dataset is invisible to iOS visitors of the dashboard. This
// utility is invoked from the per-model NAS sync (so newly scraped videos
// get converted before they reach the NAS) and from a standalone CLI for
// one-shot cleanup of existing .webm files:
//
//   node scrapyard/transcodeWebm.js                # all users
//   node scrapyard/transcodeWebm.js <username>     # one user
//
// The source .webm is moved to dataset/<user>/.webm-backup/<filename> after
// a successful transcode so nothing is destroyed. Failed transcodes leave
// the original in place.

const fs = require('fs')
const path = require('path')
const { execFile } = require('child_process')
const { promisify } = require('util')

const { findFfprobe } = require('./mediaDates')

const execFileAsync = promisify(execFile)

let ffmpegPath = null
let ffprobeForTranscode = null

// ffmpeg ships in the same bin/ as ffprobe — derive its path from whatever
// findFfprobe() resolves to so we share the dashboard's discovery without
// re-listing every install location.
async function ensureFfmpeg() {
  if (ffmpegPath) return ffmpegPath
  const probe = await findFfprobe()
  if (!probe) return null
  ffprobeForTranscode = probe
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

async function hasAudioStream(srcPath) {
  if (!ffprobeForTranscode) return false
  try {
    const { stdout } = await execFileAsync(
      ffprobeForTranscode,
      [
        '-v', 'error',
        '-select_streams', 'a:0',
        '-show_entries', 'stream=codec_name',
        '-of', 'csv=p=0',
        srcPath,
      ],
      { timeout: 10000 }
    )
    return stdout.trim().length > 0
  } catch {
    return false
  }
}

// Convert one .webm → .mp4 in the same folder. Returns a result object
// describing what happened; never throws so the caller can keep going.
async function transcodeWebmFile(srcPath, { backupDir = null, log = console } = {}) {
  const ff = await ensureFfmpeg()
  if (!ff) return { ok: false, srcPath, reason: 'no-ffmpeg' }
  if (!srcPath.toLowerCase().endsWith('.webm'))
    return { ok: false, srcPath, reason: 'not-webm' }
  if (!fs.existsSync(srcPath))
    return { ok: false, srcPath, reason: 'missing-source' }

  const dstPath = srcPath.replace(/\.webm$/i, '.mp4')
  if (fs.existsSync(dstPath))
    return { ok: true, srcPath, dstPath, skipped: true }

  const tmpPath = dstPath + '.tmp.mp4'
  const hasAudio = await hasAudioStream(srcPath)

  // CRF 20 + medium preset is a good size/quality balance for personal
  // archive content. yuv420p + faststart make the result playable on
  // every Safari version we care about. The scale filter rounds each
  // dimension down to the nearest even number — libx264 with yuv420p
  // requires both dimensions to be divisible by 2 (chroma subsampling),
  // and a fair number of mobile-captured .webm clips come in at odd
  // heights like 480x853 that would otherwise fail to encode.
  const args = [
    '-y', '-hide_banner', '-loglevel', 'warning',
    '-i', srcPath,
    '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
    '-c:v', 'libx264',
    '-crf', '20',
    '-preset', 'medium',
    '-pix_fmt', 'yuv420p',
    '-movflags', '+faststart',
    ...(hasAudio ? ['-c:a', 'aac', '-b:a', '128k'] : ['-an']),
    tmpPath,
  ]

  try {
    await execFileAsync(ff, args, { timeout: 30 * 60 * 1000, maxBuffer: 8 << 20 })
  } catch (err) {
    try { fs.unlinkSync(tmpPath) } catch {}
    return { ok: false, srcPath, reason: 'ffmpeg-failed', err: err.message }
  }

  let outStat
  try { outStat = fs.statSync(tmpPath) } catch {
    return { ok: false, srcPath, reason: 'no-output' }
  }
  if (outStat.size < 1024) {
    try { fs.unlinkSync(tmpPath) } catch {}
    return { ok: false, srcPath, reason: 'output-too-small' }
  }

  try { fs.renameSync(tmpPath, dstPath) } catch (err) {
    return { ok: false, srcPath, reason: 'rename-failed', err: err.message }
  }

  let backupPath = null
  if (backupDir) {
    try {
      fs.mkdirSync(backupDir, { recursive: true })
      backupPath = path.join(backupDir, path.basename(srcPath))
      fs.renameSync(srcPath, backupPath)
    } catch (err) {
      // Leave the .webm next to the new .mp4 if the backup move fails —
      // the dashboard will still serve the .mp4; the user can clean up
      // the orphan later.
      log.warn?.(`Backup move failed for ${srcPath}: ${err.message}`)
      backupPath = null
    }
  }

  return { ok: true, srcPath, dstPath, backupPath, hasAudio }
}

// Walks every media subfolder under dataset/<user>/ for .webm files and
// transcodes each in place. Scrapers occasionally drop .webm into
// images/ or gif/ instead of webm/, so a one-folder walk would miss them.
// Used by both the NAS sync hook (per-model) and the CLI (all users).
const MEDIA_FOLDERS_TO_SCAN = ['images', 'gif', 'webm']

async function transcodeWebmInUserDir(userDir, { log = console } = {}) {
  const targets = []
  for (const folder of MEDIA_FOLDERS_TO_SCAN) {
    let files
    try {
      files = await fs.promises.readdir(path.join(userDir, folder))
    } catch {
      continue
    }
    for (const f of files) {
      if (f.toLowerCase().endsWith('.webm'))
        targets.push({ folder, srcPath: path.join(userDir, folder, f) })
    }
  }
  if (!targets.length) return []

  const ff = await ensureFfmpeg()
  if (!ff) {
    log.warn?.(`ffmpeg not found — skipping ${targets.length} .webm file(s) in ${userDir}`)
    return []
  }

  const backupDir = path.join(userDir, '.webm-backup')
  const results = []
  const userName = path.basename(userDir)
  for (const { folder, srcPath } of targets) {
    log.log?.(`  Transcoding ${userName}/${folder}/${path.basename(srcPath)}…`)
    const r = await transcodeWebmFile(srcPath, { backupDir, log })
    results.push(r)
    if (r.ok && r.skipped) log.log?.(`    → .mp4 already exists, skipped`)
    else if (r.ok) log.log?.(`    → ok${r.backupPath ? ' (backed up)' : ''}`)
    else log.warn?.(`    → failed: ${r.reason}${r.err ? ` (${r.err})` : ''}`)
  }
  return results
}

module.exports = {
  ensureFfmpeg,
  transcodeWebmFile,
  transcodeWebmInUserDir,
}

// ─── CLI ──────────────────────────────────────────────────────────────────────
if (require.main === module) {
  ;(async () => {
    require('dotenv').config({ path: path.join(__dirname, '..', '.env') })
    const APPDATA =
      process.env.APPDATA ||
      path.join(process.env.HOME || process.env.USERPROFILE, 'AppData', 'Roaming')
    // DATASET_DIR wins over the .env-supplied LOCAL_DATASET_DIR so callers
    // can target the NAS mount (e.g. `DATASET_DIR=Z:\\dataset node …`) for
    // a one-shot pass over files that only exist on the NAS side.
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
      const results = await transcodeWebmInUserDir(ud)
      total += results.length
      ok += results.filter((r) => r.ok).length
      failed += results.filter((r) => !r.ok).length
    }
    console.log(`\nDone. ${ok}/${total} succeeded${failed ? `, ${failed} failed` : ''}`)
  })().catch((err) => {
    console.error(err)
    process.exit(1)
  })
}

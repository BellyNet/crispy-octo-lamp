'use strict'

// ─── IMPORTS ──────────────────────────────────────────────────────────────────
const https = require('https')
const path = require('path')
const fs = require('fs')
const readline = require('readline')
const ansiEscapes = require('ansi-escapes')
const { createHash } = require('crypto')
const { execFile } = require('child_process')
const { promisify } = require('util')

const execFileAsync = promisify(execFile)

const {
  logProgress,
  logGifConversion,
  logLazyDownload,
  getCompletionLine,
} = require('../stuffinglogger')

const {
  loadVisualHashCache,
  saveVisualHashCache,
  getVisualHashFromBuffer,
  isVisualDupe,
  addVisualHash,
} = require('../scrapyard/visualHasher')

const {
  loadBitwiseHashCache,
  saveBitwiseHashCache,
  isBitwiseDupe,
  addBitwiseHash,
} = require('../scrapyard/bitwiseHasher')

const mediaDates = require('../milkmaid/media-dates.js')
const { bannerHoghaul } = require('../banners.js')
const { resolveAndTrackModel } = require('../scrapyard/modelRegistry.js')

loadBitwiseHashCache()
bannerHoghaul()

const pLimit = require('p-limit')
const limit = pLimit(8)
const lazyLimit = pLimit(4)

const scrapeStart = Date.now()

// ─── PATHS ────────────────────────────────────────────────────────────────────
const rootDir = path.join(__dirname, '..')
const datasetDir = path.join(
  process.env.APPDATA ||
    path.join(
      process.env.HOME || process.env.USERPROFILE,
      'AppData',
      'Roaming'
    ),
  '.slopvault',
  'dataset'
)
const tmpDir = path.join(__dirname, 'tmp')
const incompleteDir = path.join(__dirname, 'incomplete')
const incompleteGifDir = path.join(incompleteDir, 'gifs')
const incompleteVideoDir = path.join(incompleteDir, 'videos')
const skippedImagesLog = path.join(__dirname, 'skipped_images.txt')
const aliasMapPath = path.join(rootDir, 'model_aliases.json')

for (const d of [tmpDir, incompleteGifDir, incompleteVideoDir]) {
  fs.mkdirSync(d, { recursive: true })
}

// ─── PROGRESS ─────────────────────────────────────────────────────────────────
let completedTotal = 0
let taskCompleted = false

function logAndProgress(message) {
  if (!taskCompleted) {
    taskCompleted = true
    completedTotal++
  }
  process.stdout.write(ansiEscapes.cursorTo(0, process.stdout.rows - 1))
  readline.clearLine(process.stdout, 0)
  console.log(message)
  logProgress(completedTotal, global.totalSearchTotal || 1)
}

let lazyCompleted = 0
let lazyBytesDownloaded = 0
let lastLazyDraw = 0

function logLazyProgress(queueLen) {
  process.stdout.write(ansiEscapes.cursorTo(0, process.stdout.rows - 1))
  readline.clearLine(process.stdout, 0)
  process.stdout.write(`🐷 Lazy stuffing: ${lazyCompleted} / ${queueLen}\n`)
}

// ─── RUN LOG ──────────────────────────────────────────────────────────────────
let currentRunLog = null

function startRunLog(modelName, inputUrl, logDir) {
  fs.mkdirSync(logDir, { recursive: true })
  const stamp = new Date().toISOString().replace(/[:.]/g, '-')
  const logPath = path.join(logDir, `hoghaul-run-${stamp}.jsonl`)
  const modelSummaryPath = path.join(
    datasetDir,
    modelName,
    'hoghaul-last-run.json'
  )

  currentRunLog = {
    stamp,
    logPath,
    modelName,
    inputUrl,
    startedAt: new Date().toISOString(),
    counters: {
      saved: 0,
      duplicates: 0,
      queuedVideos: 0,
      convertedGifs: 0,
      failures: 0,
    },
    errors: [],
  }

  try {
    fs.writeFileSync(
      modelSummaryPath,
      JSON.stringify(
        {
          startedAt: currentRunLog.startedAt,
          modelName,
          inputUrl,
          status: 'running',
        },
        null,
        2
      ) + '\n'
    )
  } catch {}

  appendRunEvent('run_started', { modelName, inputUrl, logPath })
}

function appendRunEvent(type, payload = {}) {
  if (!currentRunLog) return
  try {
    fs.appendFileSync(
      currentRunLog.logPath,
      JSON.stringify({ at: new Date().toISOString(), type, ...payload }) + '\n'
    )
  } catch {}
}

function recordRunError(category, details = {}) {
  if (!currentRunLog) return
  currentRunLog.errors.push({
    at: new Date().toISOString(),
    category,
    ...details,
  })
}

function finalizeRunLog(extra = {}) {
  if (!currentRunLog) return
  const summary = {
    startedAt: currentRunLog.startedAt,
    finishedAt: new Date().toISOString(),
    modelName: currentRunLog.modelName,
    inputUrl: currentRunLog.inputUrl,
    logPath: currentRunLog.logPath,
    counters: currentRunLog.counters,
    errors: currentRunLog.errors,
    ...extra,
  }
  const summaryPath = path.join(
    path.dirname(currentRunLog.logPath),
    'hoghaul-run-latest-summary.json'
  )
  const modelSummaryPath = path.join(
    datasetDir,
    currentRunLog.modelName,
    'hoghaul-last-run.json'
  )
  try {
    fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2) + '\n')
  } catch {}
  try {
    fs.writeFileSync(modelSummaryPath, JSON.stringify(summary, null, 2) + '\n')
  } catch {}
  appendRunEvent('run_finished', summary)
  currentRunLog = null
}

// ─── HASH METADATA ────────────────────────────────────────────────────────────
function buildHashMetadata(
  modelName,
  absolutePath,
  mediaType,
  sizeBytes,
  uploadedDate
) {
  const relativePath = path
    .relative(datasetDir, absolutePath)
    .replace(/\\/g, '/')
  const parts = relativePath.split('/').filter(Boolean)
  return {
    root: 'dataset',
    model: modelName || parts[0] || null,
    bucket: parts[1] || null,
    relativePath,
    filename: path.basename(absolutePath),
    mediaType,
    sizeBytes: Number.isFinite(sizeBytes) && sizeBytes >= 0 ? sizeBytes : null,
    modifiedAt: uploadedDate?.toISOString?.() || null,
    source: 'hoghaul',
  }
}

// ─── FFTOOLS ──────────────────────────────────────────────────────────────────
let ffprobePath = null
let ffmpegPath = null

async function findFfTools() {
  const found = await mediaDates.findFfprobe()
  if (found) {
    ffprobePath = found
    const guess = found.replace(/ffprobe(\.exe)?$/i, (m) =>
      m.replace('ffprobe', 'ffmpeg')
    )
    try {
      await execFileAsync(guess, ['-version'], { timeout: 3000 })
      ffmpegPath = guess
    } catch {}
  }
  if (!ffmpegPath) {
    for (const p of ['ffmpeg', 'C:\\ffmpeg\\bin\\ffmpeg.exe']) {
      try {
        await execFileAsync(p, ['-version'], { timeout: 3000 })
        ffmpegPath = p
        break
      } catch {}
    }
  }
  console.log(
    ffprobePath ? `  ffprobe: ${ffprobePath}` : '  ffprobe: not found'
  )
  console.log(ffmpegPath ? `  ffmpeg:  ${ffmpegPath}` : '  ffmpeg:  not found')
}

function convertGifToMp4(inputPath, outputPath) {
  if (!ffmpegPath) return Promise.reject(new Error('ffmpeg not found'))
  return execFileAsync(
    ffmpegPath,
    [
      '-y',
      '-i',
      inputPath,
      '-movflags',
      'faststart',
      '-pix_fmt',
      'yuv420p',
      '-vf',
      'scale=trunc(iw/2)*2:trunc(ih/2)*2',
      outputPath,
    ],
    { timeout: 120000 }
  )
}

function convertShortMp4ToGif(inputPath, outputPath) {
  if (!ffmpegPath) return Promise.reject(new Error('ffmpeg not found'))
  return execFileAsync(
    ffmpegPath,
    [
      '-y',
      '-i',
      inputPath,
      '-vf',
      'fps=15,scale=480:-1:flags=lanczos',
      outputPath,
    ],
    { timeout: 60000 }
  )
}

async function getVideoDuration(filePath) {
  if (!ffprobePath) return 9999
  try {
    const { stdout } = await execFileAsync(
      ffprobePath,
      [
        '-v',
        'error',
        '-show_entries',
        'format=duration',
        '-of',
        'csv=p=0',
        filePath,
      ],
      { timeout: 10000 }
    )
    const d = parseFloat(stdout.trim())
    return isNaN(d) ? 9999 : d
  } catch {
    return 9999
  }
}

async function getGifFrameCount(buffer) {
  const probe = ffprobePath || 'ffprobe'
  const tmp = path.join(
    tmpDir,
    `__framecheck_${Date.now()}_${Math.random().toString(36).slice(2)}.gif`
  )
  fs.writeFileSync(tmp, buffer)
  try {
    const { stdout } = await execFileAsync(
      probe,
      [
        '-v',
        'error',
        '-count_frames',
        '-select_streams',
        'v:0',
        '-show_entries',
        'stream=nb_read_frames',
        '-of',
        'csv=p=0',
        tmp,
      ],
      { timeout: 15000 }
    )
    const n = parseInt(stdout.trim(), 10)
    return isNaN(n) ? 1 : n
  } catch {
    return 1
  } finally {
    try {
      fs.unlinkSync(tmp)
    } catch {}
  }
}

async function getVideoIntegrityReport(filePath) {
  if (!ffprobePath || !ffmpegPath)
    return { ok: false, reason: 'fftools_missing' }
  try {
    const { stdout } = await execFileAsync(
      ffprobePath,
      [
        '-v',
        'error',
        '-show_entries',
        'format=duration,size',
        '-show_streams',
        '-of',
        'json',
        filePath,
      ],
      { timeout: 15000 }
    )
    const parsed = JSON.parse(stdout)
    const duration = parseFloat(parsed?.format?.duration)
    const size = parseInt(parsed?.format?.size, 10)
    const streamCount = Array.isArray(parsed?.streams)
      ? parsed.streams.length
      : 0

    if (!Number.isFinite(duration) || duration <= 0 || streamCount === 0) {
      return { ok: false, duration, size, streamCount, reason: 'invalid_probe' }
    }

    // Tail-decode the last 3 seconds — catches truncated downloads
    await execFileAsync(
      ffmpegPath,
      [
        '-v',
        'error',
        '-sseof',
        '-3',
        '-i',
        filePath,
        '-frames:v',
        '1',
        '-f',
        'null',
        '-',
      ],
      { timeout: 20000 }
    )

    return { ok: true, duration, size, streamCount }
  } catch (err) {
    return { ok: false, reason: err.message }
  }
}

// ─── HELPERS ──────────────────────────────────────────────────────────────────
function sanitize(name) {
  return name.replace(/[^a-z0-9_\-]/gi, '_').toLowerCase()
}

function createModelFolders(modelName) {
  const base = path.join(datasetDir, modelName)
  for (const sub of ['images', 'webm', 'gif', 'logs']) {
    fs.mkdirSync(path.join(base, sub), { recursive: true })
  }
  return {
    base,
    images: path.join(base, 'images'),
    webm: path.join(base, 'webm'),
    gif: path.join(base, 'gif'),
    logDir: path.join(base, 'logs'),
  }
}

function downloadBufferWithProgress(url, onProgress, timeoutMs = 15000) {
  const proto = url.startsWith('https') ? https : require('http')
  return new Promise((resolve, reject) => {
    const req = proto.get(url, (res) => {
      if (res.statusCode !== 200) {
        res.resume()
        return reject(new Error(`HTTP ${res.statusCode}`))
      }
      const chunks = []
      let downloadedBytes = 0
      const totalBytes = parseInt(res.headers['content-length'] || '0', 10)
      const start = Date.now()
      res.on('data', (chunk) => {
        downloadedBytes += chunk.length
        chunks.push(chunk)
        if (onProgress && totalBytes > 0) {
          const percent = ((downloadedBytes / totalBytes) * 100).toFixed(1)
          const speed = (
            downloadedBytes /
            1024 /
            ((Date.now() - start) / 1000)
          ).toFixed(1)
          onProgress(percent, speed, chunk)
        }
      })
      res.on('end', () => resolve(Buffer.concat(chunks)))
    })
    req.setTimeout(timeoutMs, () => {
      req.destroy()
      reject(new Error('Download timed out'))
    })
    req.on('error', reject)
  })
}

function normalizeUrl(url) {
  if (!url) return null
  return url.startsWith('http') ? url : `https:${url}`
}

// ─── COOMER API ───────────────────────────────────────────────────────────────
const COOMER_HOST = 'coomerfans.com'
const COOMER_CDN = `https://${COOMER_HOST}`

function coomerApiGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          Accept: 'text/css',
          Referer: `https://${COOMER_HOST}/`,
        },
      },
      (res) => {
        if (
          res.statusCode >= 300 &&
          res.statusCode < 400 &&
          res.headers.location
        ) {
          return coomerApiGet(res.headers.location).then(resolve).catch(reject)
        }
        let body = ''
        res.on('data', (c) => (body += c))
        res.on('end', () => resolve({ status: res.statusCode, body }))
      }
    )
    req.on('error', reject)
    req.setTimeout(15000, () => {
      req.destroy()
      reject(new Error('API timeout'))
    })
  })
}

/**
 * Fetch one page of posts from the Coomer API.
 * Returns an array of post objects or [] on 404/empty.
 */
async function fetchPostsPage(service, username, offset) {
  const url = `https://${COOMER_HOST}/api/v1/${service}/user/${encodeURIComponent(username)}/posts?limit=50&o=${offset}`
  const { status, body } = await coomerApiGet(url)
  if (status === 200) {
    try {
      return JSON.parse(body)
    } catch {
      return []
    }
  }
  if (status === 404) return []
  throw new Error(`Coomer API HTTP ${status} at offset ${offset}`)
}

/**
 * Extract { service, username } from a Coomer user URL.
 * e.g. https://coomerfans.com/onlyfans/user/someuser → { service: 'onlyfans', username: 'someuser' }
 */
function parseCoomerUrl(userUrl) {
  const m = String(userUrl).match(
    /coomer\.(?:st|party)\/([^/]+)\/user\/([^/?#]+)/i
  )
  if (!m) throw new Error(`Cannot parse Coomer URL: ${userUrl}`)
  return { service: m[1].toLowerCase(), username: decodeURIComponent(m[2]) }
}

// ─── KNOWN FILENAMES ──────────────────────────────────────────────────────────
const knownFilenames = new Set()
const gifsToConvert = []
const lazyVideoQueue = []

fs.readdirSync(datasetDir).forEach((model) => {
  const modelPath = path.join(datasetDir, model)
  if (!fs.lstatSync(modelPath).isDirectory()) return
  for (const sub of ['images', 'webm', 'gif']) {
    const subPath = path.join(modelPath, sub)
    try {
      fs.readdirSync(subPath).forEach((f) => knownFilenames.add(f))
    } catch {}
  }
})

// ─── MAIN SCRAPE ──────────────────────────────────────────────────────────────
async function scrapeCoomerUser(userUrl, startPage = 0, endPage = null) {
  let newestDateSeen = null

  try {
    await findFfTools()

    // Extract service + username directly from the URL — no browser needed
    const { service, username } = parseCoomerUrl(userUrl)
    const rawName = sanitize(username)

    // ── Unified model registry (shared with milkmaid) ─────────────────────────
    // resolveAndTrackModel finds/creates the canonical name, records the alias,
    // and upserts the Coomer URL under sources.coomer — all in model_aliases.json
    const modelName = resolveAndTrackModel(
      aliasMapPath,
      rawName,
      'coomer',
      userUrl
    )

    const folders = createModelFolders(modelName)
    startRunLog(modelName, userUrl, folders.logDir)

    const hasPageRange = endPage !== null
    const totalPages = hasPageRange ? endPage - startPage + 1 : null
    const totalExpectedPosts = hasPageRange ? totalPages * 50 : 50
    global.totalSearchTotal = totalExpectedPosts

    let pageNum = startPage

    // ── Page loop (Coomer API — no browser) ───────────────────────────────────
    while (true) {
      if (endPage !== null && pageNum > endPage) {
        logAndProgress(`🧮 Reached end of page range (${startPage}–${endPage})`)
        break
      }

      // Polite delay between API pages
      if (pageNum > startPage) await new Promise((res) => setTimeout(res, 800))

      const posts = await fetchPostsPage(service, username, pageNum * 50)

      if (!posts.length) {
        logAndProgress(`📭 No posts on page ${pageNum}, stopping.`)
        break
      }

      if (pageNum === startPage) process.stdout.write('\n')
      logProgress(completedTotal, totalExpectedPosts)

      const pageLabel = totalPages
        ? `${pageNum - startPage + 1}/${totalPages}`
        : `${pageNum - startPage + 1}`
      logAndProgress(`📦 Page ${pageLabel} (${posts.length} posts)`)

      await Promise.all(
        posts.map((post) =>
          limit(() => {
            taskCompleted = false
            return processPost(
              post,
              folders,
              modelName,
              knownFilenames,
              gifsToConvert,
              lazyVideoQueue,
              (updatedDate) => {
                if (
                  !newestDateSeen ||
                  (updatedDate && updatedDate > newestDateSeen)
                ) {
                  newestDateSeen = updatedDate
                }
              }
            )
          })
        )
      )

      pageNum++
    }

    // ── GIF conversion ────────────────────────────────────────────────────────
    // Pick up any gifs left in the incomplete dir from interrupted runs
    for (const f of fs
      .readdirSync(incompleteGifDir)
      .filter((f) => f.endsWith('.gif'))) {
      const tmpPath = path.join(incompleteGifDir, f)
      const mp4Path = path.join(folders.webm, f.replace(/\.gif$/, '.mp4'))
      if (!gifsToConvert.find((g) => g.tmpPath === tmpPath)) {
        gifsToConvert.push({
          tmpPath,
          mp4Path,
          filename: f,
          uploadedDate: null,
          visualHash: null,
        })
      }
    }

    for (const {
      tmpPath,
      mp4Path,
      filename,
      uploadedDate,
      visualHash: gifVisualHash,
    } of gifsToConvert) {
      try {
        if (fs.existsSync(mp4Path)) {
          if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
          continue
        }

        logAndProgress(logGifConversion(filename))
        await convertGifToMp4(tmpPath, mp4Path)

        if (uploadedDate) {
          const ts = uploadedDate.getTime() / 1000
          fs.utimesSync(mp4Path, ts, ts)
        }

        const mp4Name = path.basename(mp4Path)
        await mediaDates.recordVideoDates(
          path.join(datasetDir, modelName),
          'webm',
          mp4Name,
          mp4Path,
          uploadedDate
        )

        const mp4Stat = fs.statSync(mp4Path)
        const mp4Hash = createHash('md5')
          .update(fs.readFileSync(mp4Path))
          .digest('hex')
        if (!isBitwiseDupe(mp4Hash)) {
          addBitwiseHash(
            mp4Hash,
            buildHashMetadata(
              modelName,
              mp4Path,
              'video',
              mp4Stat.size,
              uploadedDate
            )
          )
          saveBitwiseHashCache()
        }

        if (gifVisualHash && !isVisualDupe(gifVisualHash)) {
          addVisualHash(
            gifVisualHash,
            buildHashMetadata(
              modelName,
              mp4Path,
              'video',
              mp4Stat.size,
              uploadedDate
            )
          )
          saveVisualHashCache()
        }

        knownFilenames.add(mp4Name)
        currentRunLog && currentRunLog.counters.convertedGifs++
        appendRunEvent('converted_gif_to_mp4', {
          modelName,
          filename,
          mp4: path.relative(datasetDir, mp4Path).replace(/\\/g, '/'),
        })

        if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
        logAndProgress(`🎞️ Converted: ${filename}`)
      } catch (err) {
        currentRunLog && currentRunLog.counters.failures++
        recordRunError('gif_conversion_error', {
          modelName,
          filename,
          error: err.message,
        })
        appendRunEvent('gif_conversion_error', {
          modelName,
          filename,
          error: err.message,
        })
        logAndProgress(`❌ Conversion failed for ${filename}: ${err.message}`)
      }
    }

    // ── Lazy videos ───────────────────────────────────────────────────────────
    await Promise.all(
      lazyVideoQueue.map((entry, i) =>
        lazyLimit(async () => {
          const {
            url,
            path: finalPath,
            filename,
            uploadedDate,
            isImage = false,
          } = entry

          if (knownFilenames.has(filename) || fs.existsSync(finalPath)) {
            currentRunLog && currentRunLog.counters.duplicates++
            return logAndProgress(`♻️ Lazy dupe (pre-download): ${filename}`)
          }

          logAndProgress(logLazyDownload(i))
          logAndProgress(`⏳ (${i + 1}/${lazyVideoQueue.length})`)

          let hash = null
          try {
            const buffer = await downloadBufferWithProgress(
              url,
              (percent, speed, chunk) => {
                lazyBytesDownloaded += chunk.length
                const now = Date.now()
                if (now - lastLazyDraw > 250) {
                  logLazyProgress(lazyVideoQueue.length)
                  lastLazyDraw = now
                }
              }
            )

            hash = createHash('md5').update(buffer).digest('hex')
            if (isBitwiseDupe(hash)) {
              currentRunLog && currentRunLog.counters.duplicates++
              return logAndProgress(`♻️ Lazy dupe: ${filename}`)
            }

            if (isImage) {
              fs.writeFileSync(finalPath, buffer)
              if (uploadedDate) {
                const ts = uploadedDate.getTime() / 1000
                fs.utimesSync(finalPath, ts, ts)
              }
              await mediaDates.recordImageDates(
                path.join(datasetDir, modelName),
                'images',
                filename,
                uploadedDate
              )
              const stat = fs.statSync(finalPath)
              addBitwiseHash(
                hash,
                buildHashMetadata(
                  modelName,
                  finalPath,
                  'image',
                  stat.size,
                  uploadedDate
                )
              )
              saveBitwiseHashCache()
              const visualHash = await getVisualHashFromBuffer(buffer)
              if (visualHash && !isVisualDupe(visualHash)) {
                addVisualHash(
                  visualHash,
                  buildHashMetadata(
                    modelName,
                    finalPath,
                    'image',
                    stat.size,
                    uploadedDate
                  )
                )
                saveVisualHashCache()
              }
              knownFilenames.add(filename)
              currentRunLog && currentRunLog.counters.saved++
              lazyCompleted++
              logLazyProgress(lazyVideoQueue.length)
              appendRunEvent('saved_deferred_image', { modelName, filename })
              logAndProgress(`🖼️ Saved deferred image: ${filename}`)
              return
            }

            // Video — write to tmp, validate integrity, then move into place
            const tmpVideoPath = path.join(incompleteVideoDir, filename)
            fs.writeFileSync(tmpVideoPath, buffer)

            const integrity = await getVideoIntegrityReport(tmpVideoPath)
            if (!integrity.ok) {
              try {
                fs.unlinkSync(tmpVideoPath)
              } catch {}
              throw new Error(`Integrity check failed: ${integrity.reason}`)
            }

            if (uploadedDate) {
              const ts = uploadedDate.getTime() / 1000
              fs.utimesSync(tmpVideoPath, ts, ts)
            }

            fs.renameSync(tmpVideoPath, finalPath)

            await mediaDates.recordVideoDates(
              path.join(datasetDir, modelName),
              'webm',
              filename,
              finalPath,
              uploadedDate
            )

            const finalStat = fs.statSync(finalPath)
            addBitwiseHash(
              hash,
              buildHashMetadata(
                modelName,
                finalPath,
                'video',
                finalStat.size,
                uploadedDate
              )
            )
            saveBitwiseHashCache()

            // Short video → also produce a GIF preview
            try {
              if (integrity.duration <= 6 && finalStat.size < 5 * 1024 * 1024) {
                const gifName = filename.replace(/\.(mp4|m4v|webm)$/i, '.gif')
                const gifPath = path.join(folders.gif, gifName)
                if (!fs.existsSync(gifPath)) {
                  await convertShortMp4ToGif(finalPath, gifPath)
                  if (uploadedDate) {
                    const ts = uploadedDate.getTime() / 1000
                    fs.utimesSync(gifPath, ts, ts)
                  }
                  await mediaDates.recordImageDates(
                    path.join(datasetDir, modelName),
                    'gif',
                    gifName,
                    uploadedDate
                  )
                  logAndProgress(`🎁 Converted to gif: ${gifName}`)
                }
              }
            } catch (err) {
              console.warn(
                `⚠️ Couldn't convert ${filename} to gif: ${err.message}`
              )
            }

            knownFilenames.add(filename)
            currentRunLog && currentRunLog.counters.saved++
            lazyCompleted++
            logLazyProgress(lazyVideoQueue.length)
            appendRunEvent('saved_lazy_video', { modelName, filename })
            logAndProgress(`✅ Saved lazy video: ${filename}`)
          } catch (err) {
            currentRunLog && currentRunLog.counters.failures++
            recordRunError('lazy_video_error', {
              modelName,
              filename,
              error: err.message,
            })
            appendRunEvent('lazy_video_error', {
              modelName,
              filename,
              error: err.message,
            })
            logAndProgress(`❌ Lazy failed: ${filename} — ${err.message}`)
            knownFilenames.delete(filename)
            const tmpVideoPath = path.join(incompleteVideoDir, filename)
            if (fs.existsSync(tmpVideoPath))
              try {
                fs.unlinkSync(tmpVideoPath)
              } catch {}
          }
        })
      )
    )

    saveBitwiseHashCache()
    saveVisualHashCache()

    const { counters } = currentRunLog || {
      counters: { saved: 0, duplicates: 0, failures: 0 },
    }
    const durationMs = Date.now() - scrapeStart
    const mins = Math.floor(durationMs / 60000)
    const secs = Math.floor((durationMs % 60000) / 1000)
    console.log(`\n⏱️  Total scrape time: ${mins}m ${secs}s`)
    console.log(
      `🎉 Done: ${counters.saved} saved, ${counters.duplicates} dupes, ${counters.failures} errors`
    )
    console.log('\n' + getCompletionLine())
  } catch (err) {
    recordRunError('run_error', { error: err.message })
    appendRunEvent('run_error', { error: err.message })
    throw err
  } finally {
    finalizeRunLog()
    mediaDates.flushAllSidecars()
  }
}

// ─── PROCESS POST ─────────────────────────────────────────────────────────────
async function safeDownload(url) {
  try {
    return await downloadBufferWithProgress(url)
  } catch (err) {
    logAndProgress(`❌ Failed download, retrying: ${url} — ${err.message}`)
    await new Promise((r) => setTimeout(r, 1000))
    return await downloadBufferWithProgress(url)
  }
}

/**
 * Process a single post from the Coomer API.
 * post = { id, published, file: { name, path }, attachments: [{ name, path }] }
 * Media CDN URL = https://coomerfans.com/data{path}
 */
async function processPost(
  post,
  folders,
  modelName,
  knownFilenames,
  gifsToConvert,
  lazyVideoQueue,
  updateNewestDate
) {
  try {
    // ── Date from API field (reliable, no DOM needed) ──────────────────────
    let uploadedDate = null
    if (post.published) {
      const d = new Date(post.published)
      if (!isNaN(d.getTime())) uploadedDate = d
    }
    updateNewestDate(uploadedDate)

    // ── Collect media items from file + attachments ────────────────────────
    const mediaItems = []
    if (post.file?.path) {
      mediaItems.push({ name: post.file.name || null, cdnPath: post.file.path })
    }
    for (const att of post.attachments || []) {
      if (att?.path)
        mediaItems.push({ name: att.name || null, cdnPath: att.path })
    }

    for (const item of mediaItems) {
      // CDN URL: https://coomerfans.com/data{/path/to/file.ext}
      const url = `${COOMER_CDN}/data${item.cdnPath}`

      // Prefer the original filename from the API; fall back to CDN path basename
      const filename =
        item.name || decodeURIComponent(path.basename(item.cdnPath))

      if (!filename || /avatar|profile/i.test(filename)) continue

      const ext = path.extname(filename).toLowerCase()
      const timestamp = uploadedDate ? uploadedDate.getTime() / 1000 : null

      if (knownFilenames.has(filename)) {
        currentRunLog && currentRunLog.counters.duplicates++
        appendRunEvent('skip_existing', { modelName, filename })
        continue
      }

      // Queue videos for lazy download
      if (['.mp4', '.webm', '.m4v'].includes(ext)) {
        logAndProgress(`🐌 Queued lazy video: ${filename}`)
        lazyVideoQueue.push({
          url,
          path: path.join(folders.webm, filename),
          filename,
          uploadedDate,
        })
        currentRunLog && currentRunLog.counters.queuedVideos++
        appendRunEvent('queued_lazy_video', { modelName, filename })
        continue
      }

      let buffer = null
      try {
        buffer = await safeDownload(url)
      } catch (err) {
        // With API-sourced CDN URLs there's no DOM fallback — log and skip
        currentRunLog && currentRunLog.counters.failures++
        appendRunEvent('media_error', {
          modelName,
          filename,
          url,
          error: err.message,
        })
        fs.appendFileSync(skippedImagesLog, `${url}\n`)
        logAndProgress(
          `❌ Download failed after retries: ${filename} — ${err.message}`
        )
        continue
      }

      // Defer large images to the lazy queue
      if (buffer.length > 5 * 1024 * 1024) {
        logAndProgress(`🍖 Deferring large image to lazy queue: ${filename}`)
        lazyVideoQueue.push({
          url,
          path: path.join(folders.images, filename),
          filename,
          uploadedDate,
          isImage: true,
        })
        appendRunEvent('queued_large_image', { modelName, filename })
        continue
      }

      // ── GIF ────────────────────────────────────────────────────────────────
      if (ext === '.gif') {
        const frameCount = await getGifFrameCount(buffer)
        if (frameCount > 1) {
          // Animated — save to gif folder, queue for MP4 conversion
          const gifPath = path.join(folders.gif, filename)
          fs.writeFileSync(gifPath, buffer)
          if (timestamp) fs.utimesSync(gifPath, timestamp, timestamp)

          // Compute visual hash now so it travels with the conversion entry
          const visualHash = await getVisualHashFromBuffer(buffer)

          await mediaDates.recordImageDates(
            path.join(datasetDir, modelName),
            'gif',
            filename,
            uploadedDate
          )
          gifsToConvert.push({
            tmpPath: gifPath,
            mp4Path: path.join(
              folders.webm,
              filename.replace(/\.gif$/, '.mp4')
            ),
            filename,
            uploadedDate,
            visualHash:
              visualHash && !isVisualDupe(visualHash) ? visualHash : null,
          })
          appendRunEvent('queued_gif_conversion', { modelName, filename })
        } else {
          // Static single-frame GIF — treat as image
          const stillPath = path.join(folders.images, filename)
          fs.writeFileSync(stillPath, buffer)
          if (timestamp) fs.utimesSync(stillPath, timestamp, timestamp)

          const hash = createHash('md5').update(buffer).digest('hex')
          const stat = fs.statSync(stillPath)
          if (!isBitwiseDupe(hash)) {
            addBitwiseHash(
              hash,
              buildHashMetadata(
                modelName,
                stillPath,
                'image',
                stat.size,
                uploadedDate
              )
            )
            saveBitwiseHashCache()
          }
          const visualHash = await getVisualHashFromBuffer(buffer)
          if (visualHash && !isVisualDupe(visualHash)) {
            addVisualHash(
              visualHash,
              buildHashMetadata(
                modelName,
                stillPath,
                'image',
                stat.size,
                uploadedDate
              )
            )
            saveVisualHashCache()
          }
          await mediaDates.recordImageDates(
            path.join(datasetDir, modelName),
            'images',
            filename,
            uploadedDate
          )
          knownFilenames.add(filename)
          currentRunLog && currentRunLog.counters.saved++
          appendRunEvent('saved_still_gif', { modelName, filename })
        }
        continue
      }

      // ── Standard image ─────────────────────────────────────────────────────
      const hash = createHash('md5').update(buffer).digest('hex')
      if (isBitwiseDupe(hash)) {
        currentRunLog && currentRunLog.counters.duplicates++
        appendRunEvent('duplicate_bitwise', { modelName, filename })
        continue
      }

      const visualHash = await getVisualHashFromBuffer(buffer)
      if (visualHash && isVisualDupe(visualHash)) {
        currentRunLog && currentRunLog.counters.duplicates++
        appendRunEvent('duplicate_visual', { modelName, filename })
        continue
      }

      const outPath = path.join(folders.images, filename)
      fs.writeFileSync(outPath, buffer)
      if (timestamp) fs.utimesSync(outPath, timestamp, timestamp)

      await mediaDates.recordImageDates(
        path.join(datasetDir, modelName),
        'images',
        filename,
        uploadedDate
      )

      const stat = fs.statSync(outPath)
      addBitwiseHash(
        hash,
        buildHashMetadata(modelName, outPath, 'image', stat.size, uploadedDate)
      )
      saveBitwiseHashCache()
      if (visualHash) {
        addVisualHash(
          visualHash,
          buildHashMetadata(
            modelName,
            outPath,
            'image',
            stat.size,
            uploadedDate
          )
        )
        saveVisualHashCache()
      }

      knownFilenames.add(filename)
      currentRunLog && currentRunLog.counters.saved++
      appendRunEvent('saved_image', { modelName, filename })
      logAndProgress(`✅ Saved ${filename}`)
    }
  } catch (err) {
    currentRunLog && currentRunLog.counters.failures++
    recordRunError('post_error', { postId: post?.id, error: err.message })
    appendRunEvent('post_error', { postId: post?.id, error: err.message })
    logAndProgress(`❌ Error processing post ${post?.id}: ${err.message}`)
  }
}

// ─── CLI ──────────────────────────────────────────────────────────────────────
const argv = process.argv.slice(2)
const target = argv.find((arg) => arg.includes('coomer.'))

let startPage = 0
let endPage = null

let pageArgRaw =
  process.env.npm_config_pages || argv.find((a) => a.startsWith('--pages='))
if (pageArgRaw) {
  pageArgRaw = pageArgRaw.replace('--pages=', '')
  if (pageArgRaw.includes('-')) {
    const [start, end] = pageArgRaw.split('-').map((n) => parseInt(n, 10))
    startPage = isNaN(start) ? 0 : start
    endPage = isNaN(end) ? null : end
  } else {
    const num = parseInt(pageArgRaw, 10)
    endPage = isNaN(num) ? null : num - 1
  }
}

if (!target) {
  console.error('Usage: node hoghaul.js <coomer-url> [--pages=N or N-M]')
  process.exit(1)
}

loadVisualHashCache()

scrapeCoomerUser(target, startPage, endPage)

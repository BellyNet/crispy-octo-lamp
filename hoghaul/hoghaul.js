// hoghaul.js — Coomer scraper (Milkmaid-matching)

const puppeteer = require('puppeteer-extra')
const StealthPlugin = require('puppeteer-extra-plugin-stealth')
puppeteer.use(StealthPlugin())
const path = require('path')
const fs = require('fs')
const readline = require('readline')
const ansiEscapes = require('ansi-escapes')
const { createHash } = require('crypto')
const { exec } = require('child_process')
const {
  logProgress,
  logLazyDownload,
  logGifConversion,
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

loadBitwiseHashCache()

const pLimit = require('p-limit')

const limit = pLimit(8)
const lazyLimit = pLimit(4)

const scrapeStart = Date.now()

const { bannerHoghaul } = require('../banners.js') // adjust path if needed
bannerHoghaul()

const { createScraperPage } = require('../scrapyard/pageHelpers')

const rootDir = path.join(__dirname, '..')
const datasetDir = path.join(
  process.env.APPDATA || path.join(process.env.HOME, 'AppData', 'Roaming'),
  '.slopvault',
  'dataset'
)
const tmpDir = path.join(__dirname, 'tmp')
const incompleteDir = path.join(__dirname, 'incomplete')
const incompleteGifDir = path.join(incompleteDir, 'gifs')

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

if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true })
if (!fs.existsSync(incompleteGifDir))
  fs.mkdirSync(incompleteGifDir, { recursive: true })

let globalPostIndex = 0

function sanitize(name) {
  return name.replace(/[^a-z0-9_\-]/gi, '_').toLowerCase()
}

function createModelFolders(modelName) {
  const base = path.join(datasetDir, modelName)
  const folders = ['images', 'webm', 'gif']
  for (const folder of folders)
    fs.mkdirSync(path.join(base, folder), { recursive: true })
  return {
    base,
    images: path.join(base, 'images'),
    webm: path.join(base, 'webm'),
    gif: path.join(base, 'gif'),
  }
}

function downloadBufferWithProgress(url, onProgress, timeoutMs = 15000) {
  const proto = url.startsWith('https') ? require('https') : require('http')

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

      res.on('end', () => {
        const buffer = Buffer.concat(chunks)
        resolve(buffer)
      })
    })

    req.setTimeout(timeoutMs, () => {
      req.destroy()
      reject(new Error('Download timed out'))
    })

    req.on('error', reject)
  })
}

function convertGifToMp4(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const cmd = `ffmpeg -y -i "${inputPath}" -movflags faststart -pix_fmt yuv420p -vf "scale=trunc(iw/2)*2:trunc(ih/2)*2" "${outputPath}"`
    exec(cmd, (err) => (err ? reject(err) : resolve()))
  })
}

function convertShortMp4ToGif(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const cmd = `ffmpeg -y -i "${inputPath}" -vf "fps=15,scale=480:-1:flags=lanczos" "${outputPath}"`
    exec(cmd, (err) => (err ? reject(err) : resolve()))
  })
}

function getVideoDuration(filePath) {
  return new Promise((resolve, reject) => {
    const cmd = `ffprobe -v error -show_entries format=duration -of csv=p=0 "${filePath}"`
    exec(cmd, (err, stdout) => {
      if (err) return reject(err)
      const duration = parseFloat(stdout.trim())
      resolve(isNaN(duration) ? 9999 : duration)
    })
  })
}

function getGifFrameCount(buffer) {
  const tmp = path.join(tmpDir, `__framecheck_${Date.now()}.gif`)
  fs.writeFileSync(tmp, buffer)
  return new Promise((resolve) => {
    exec(
      `ffprobe -v error -count_frames -select_streams v:0 -show_entries stream=nb_read_frames -of csv=p=0 "${tmp}"`,
      (err, stdout) => {
        fs.unlinkSync(tmp)
        const frameCount = parseInt(stdout.trim(), 10)
        resolve(isNaN(frameCount) ? 1 : frameCount)
      }
    )
  })
}

function normalizeUrl(url) {
  if (!url) return null
  return url.startsWith('http') ? url : `https:${url}`
}

const knownHashes = new Set()
const knownFilenames = new Set()
const gifsToConvert = []
const lazyVideoQueue = []

fs.readdirSync(datasetDir).forEach((folder) => {
  const folderPath = path.join(datasetDir, folder)
  if (fs.lstatSync(folderPath).isDirectory()) {
    fs.readdirSync(folderPath).forEach((f) => knownFilenames.add(f))
  }
})

let lazyCompleted = 0
let lazyBytesDownloaded = 0
let lastDraw = 0

function logLazyProgress() {
  process.stdout.write(ansiEscapes.cursorTo(0, process.stdout.rows - 1))
  readline.clearLine(process.stdout, 0)
  process.stdout.write(
    `🐷 Lazy stuffing: ${lazyCompleted} / ${lazyVideoQueue.length}\n`
  )
}

async function scrapeCoomerUser(userUrl, startPage = 0, endPage = null) {
  const browser = await puppeteer.launch({
    headless: 'new',
    defaultViewport: null,
    args: ['--ignore-certificate-errors'],
    ignoreHTTPSErrors: true, // ✅ ← This is key
  })

  let newestDateSeen = null // ← 💥 this is what you're missing

  const page = await createScraperPage(browser, {
    site: 'coomer',
    interceptMedia: true, // for post list/gallery pages
  })

  const urlParts = new URL(userUrl).pathname.split('/')
  const username = urlParts[urlParts.indexOf('user') + 1]

  if (!username) {
    throw new Error('❌ Failed to extract username from Coomer URL.')
  }

  const rawName = sanitize(username)
  const aliasMapPath = path.join(__dirname, '..', 'model_aliases.json')
  let aliasMap = {}

  if (fs.existsSync(aliasMapPath)) {
    aliasMap = JSON.parse(fs.readFileSync(aliasMapPath, 'utf-8'))
  } else {
    fs.writeFileSync(aliasMapPath, JSON.stringify({}, null, 2))
  }

  const modelName = aliasMap[rawName] || rawName
  if (!aliasMap[rawName]) {
    aliasMap[rawName] = rawName
    fs.writeFileSync('model_aliases.json', JSON.stringify(aliasMap, null, 2))
  }
  const folders = createModelFolders(modelName)

  const totalPages = endPage - startPage + 1
  const totalExpectedPosts = totalPages * 50
  global.totalSearchTotal = totalExpectedPosts

  const postLinks = new Set()
  let pageNum = startPage

  while (true) {
    if (endPage !== null && pageNum > endPage) {
      logAndProgress(`🧮 Reached end of page range (${startPage}-${endPage})`)
      break
    }

    await new Promise((res) => setTimeout(res, 1500)) // 1.5s pause

    const url = `${userUrl}?o=${pageNum * 50}`
    await page.goto(url, { waitUntil: 'networkidle2' })
    const hasPosts = await page
      .waitForSelector('article.post-card a.fancy-link', { timeout: 10000 })
      .then(() => true)
      .catch(() => false)

    if (!hasPosts) {
      logAndProgress(`📭 No posts found on page ${pageNum}, stopping.`)
      break
    }

    const links = await page.$$eval('article.post-card a.fancy-link', (els) =>
      els.map((el) => el.href)
    )

    if (!links.length) break

    if (pageNum === startPage) process.stdout.write('\n') // reserve space
    logProgress(globalPostIndex, totalExpectedPosts)

    // const pageNumDisplay = pageNum - startPage + 1
    // logAndProgress(`📦 Page ${pageNumDisplay}/${totalPages}`)

    await Promise.all(
      links.map((link) =>
        limit(() => {
          taskCompleted = false // start of this post

          return processPost(
            link,
            browser,
            folders,
            knownHashes,
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

  // convert gifs
  for (const gif of fs
    .readdirSync(incompleteGifDir)
    .filter((f) => f.endsWith('.gif'))) {
    const tmpPath = path.join(incompleteGifDir, gif)
    const mp4Path = path.join(folders.webm, gif.replace(/\.gif$/, '.mp4'))
    gifsToConvert.push({ tmpPath, mp4Path, filename: gif })
  }

  for (const { tmpPath, mp4Path, filename, uploadedDate } of gifsToConvert) {
    try {
      if (fs.existsSync(mp4Path)) continue
      await convertGifToMp4(tmpPath, mp4Path)

      if (uploadedDate) {
        const ts = uploadedDate.getTime() / 1000
        fs.utimesSync(mp4Path, ts, ts) // ✅ timestamp converted mp4
      }

      knownHashes.add(
        createHash('md5').update(fs.readFileSync(mp4Path)).digest('hex')
      )
      addBitwiseHash(hash)
      saveBitwiseHashCache()

      knownFilenames.add(path.basename(mp4Path))
      logAndProgress(`🎞️ Converted: ${filename}`)
      if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
    } catch (err) {
      logAndProgress(`❌ Conversion failed for ${filename}`)
    }
  }

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

        let hash = null

        if (knownFilenames.has(filename) || fs.existsSync(finalPath)) {
          return logAndProgress(`♻️ Lazy dupe (pre-download): ${filename}`)
        }

        logAndProgress(logLazyDownload(i))
        logAndProgress(`⏳ (${i + 1}/${lazyVideoQueue.length})`)
        try {
          const buffer = await downloadBufferWithProgress(
            url,
            (percent, speed, chunk) => {
              lazyBytesDownloaded += chunk.length
              const now = Date.now()
              if (now - lastDraw > 250) {
                logLazyProgress()
                lastDraw = now
              }
            }
          )

          hash = createHash('md5').update(buffer).digest('hex')
          if (isBitwiseDupe(hash))
            return logAndProgress(`♻️ Lazy dupe: ${filename}`)

          fs.writeFileSync(finalPath, buffer)
          if (uploadedDate) {
            const ts = uploadedDate.getTime() / 1000
            fs.utimesSync(finalPath, ts, ts)
          }

          // after writing finalPath
          if (fs.existsSync(finalPath)) {
            try {
              const { size } = fs.statSync(finalPath)
              const isSmallFile = size < 5 * 1024 * 1024 // < 5MB

              const duration = await getVideoDuration(finalPath)
              if (duration <= 6 && isSmallFile) {
                const gifName = filename.replace(/\.(mp4|m4v)$/i, '.gif')
                const gifPath = path.join(folders.gif, gifName)

                if (!fs.existsSync(gifPath)) {
                  await convertShortMp4ToGif(finalPath, gifPath)
                  if (uploadedDate) {
                    const ts = uploadedDate.getTime() / 1000
                    fs.utimesSync(gifPath, ts, ts) // ✅ set date on new .gif
                  }
                  lazyCompleted++
                  logLazyProgress()

                  logAndProgress(`🎁 Converted to gif: ${gifName}`)
                }
              }

              if (isImage) {
                fs.writeFileSync(finalPath, buffer)
                if (uploadedDate) {
                  const ts = uploadedDate.getTime() / 1000
                  fs.utimesSync(finalPath, ts, ts)
                }

                const hash = createHash('md5').update(buffer).digest('hex')
                if (!isBitwiseDupe(hash)) {
                  addBitwiseHash(hash)
                  saveBitwiseHashCache()
                }

                knownFilenames.add(filename)

                lazyCompleted++
                logLazyProgress()

                logAndProgress(`🖼️ Saved deferred image: ${filename}`)
                return
              }
            } catch (err) {
              console.warn(
                `⚠️ Couldn’t convert ${filename} to gif: ${err.message}`
              )
            }
          }

          if (!isBitwiseDupe(hash)) {
            addBitwiseHash(hash)
            saveBitwiseHashCache()
          }

          knownFilenames.add(filename)

          lazyCompleted++
          logLazyProgress()

          logAndProgress(`✅ Saved lazy video: ${filename}`)
        } catch (err) {
          logAndProgress(`❌ Lazy failed: ${filename} - ${err.message}`)
          if (hash) knownHashes.delete(hash)
          knownFilenames.delete(filename)
        }
      })
    )
  )

  const logPath = path.join(folders.base, 'log.txt')
  fs.writeFileSync(
    logPath,
    `Model: ${modelName}\nTotal: ${completedTotal}\nSaved: ${knownFilenames.size}\nDupes: N/A\nErrors: 0`
  )

  saveBitwiseHashCache()
  saveVisualHashCache()

  const durationMs = Date.now() - scrapeStart
  const mins = Math.floor(durationMs / 60000)
  const secs = Math.floor((durationMs % 60000) / 1000)
  console.log(`\n⏱️ Total scrape time: ${mins}m ${secs}s`)
  console.log('\n' + getCompletionLine())

  await browser.close()
}

async function safeDownload(url) {
  try {
    return await downloadBufferWithProgress(url)
  } catch (err) {
    logAndProgress(`❌ Failed download, retrying: ${url} — ${err.message}`)
    await new Promise((r) => setTimeout(r, 1000))
    return await downloadBufferWithProgress(url)
  }
}

async function processPost(
  link,
  browser,
  folders,
  knownHashes,
  knownFilenames,
  gifsToConvert,
  lazyVideoQueue,
  updateNewestDate
) {
  const page = await createScraperPage(browser, {
    site: 'coomer',
  })

  try {
    await page.goto(link, { waitUntil: 'networkidle2', timeout: 10000 })

    let uploadedDate = null
    try {
      await page.waitForSelector('time.timestamp', { timeout: 10000 })
      const timeText = await page.$eval('time.timestamp', (el) =>
        el.getAttribute('datetime')
      )
      uploadedDate = timeText ? new Date(timeText) : null
    } catch {
      logAndProgress(`⏳ No timestamp for: ${link}`)
    }
    updateNewestDate(uploadedDate)

    const tMediaStart = Date.now()
    const mediaUrls = await page.evaluate(() => {
      const urls = []
      document
        .querySelectorAll(
          'a.fileThumb.image-link, video source[src], a.post__attachment-link[href]'
        )
        .forEach((el) => {
          const u =
            el.href ||
            el.src ||
            el.getAttribute('src') ||
            el.getAttribute('href')
          if (u && u.startsWith('http')) urls.push(u)
        })
      return urls
    })

    for (const mediaUrl of mediaUrls) {
      let buffer = null
      let url = normalizeUrl(mediaUrl)
      if (!url || typeof url !== 'string') continue

      // 🧼 Strip Coomer ?f= proxy image resizing
      const parsed = new URL(url)
      parsed.search = ''
      url = parsed.toString()

      if (url.includes('?f=')) {
        logAndProgress(`⚠️ Still has ?f=: ${url}`)
      }

      let filename
      try {
        filename = decodeURIComponent(
          path.basename(new URL(url).pathname).split('?')[0]
        )
        if (/avatar|profile/i.test(filename)) continue
      } catch {
        continue
      }

      const ext = path.extname(filename).toLowerCase()
      const timestamp = uploadedDate ? uploadedDate.getTime() / 1000 : null

      if (knownFilenames.has(filename)) continue

      // 🎥 Skip video download for lazy
      if (['.mp4', '.webm', '.m4v'].includes(ext)) {
        logAndProgress(`🐌 Queued lazy video: ${filename}`)
        lazyVideoQueue.push({
          url,
          path: path.join(folders.webm, filename),
          filename,
          uploadedDate,
        })
        continue
      }

      try {
        buffer = await safeDownload(url)
      } catch (err) {
        logAndProgress(`🚨 Full-res failed after retries: ${filename}`)

        // fallback block starts here
        const fallbackUrl = await page.evaluate((filenameGuess) => {
          const imgs = Array.from(document.querySelectorAll('img'))
          const match = imgs.find((img) => {
            const src = img?.src || img?.getAttribute('src') || ''
            return src.includes(filenameGuess.slice(0, 10))
          })
          return match?.src?.startsWith('http')
            ? match.src
            : match?.src
              ? `https:${match.src}`
              : null
        }, filename)

        if (fallbackUrl) {
          try {
            const fallbackBuffer = await downloadBufferWithProgress(fallbackUrl)
            const fallbackPath = path.join(folders.images, filename)
            fs.writeFileSync(fallbackPath, fallbackBuffer)
            knownFilenames.add(filename)
            logAndProgress(`🧷 Saved fallback image: ${filename}`)
          } catch (e) {
            logAndProgress(`❌ Fallback image failed: ${e.message}`)
            fs.appendFileSync('skipped_images.txt', `${url}\n`)
          }
        } else {
          logAndProgress(`❌ No fallback found in DOM for ${filename}`)
          fs.appendFileSync('skipped_images.txt', `${url}\n`)
        }

        continue // ⛔ Skip the rest of this media loop
      }

      if (buffer.length > 5 * 1024 * 1024) {
        logAndProgress(`🍖 Deferring large image to lazy queue: ${filename}`)
        lazyVideoQueue.push({
          url,
          path: path.join(folders.images, filename),
          filename,
          uploadedDate,
          isImage: true, // 👈 you'll need to check this later
        })
        continue
      }

      // 🌀 GIF handling
      if (ext === '.gif') {
        const frameCount = await getGifFrameCount(buffer)
        const gifPath = path.join(folders.gif, filename)
        if (frameCount > 1) {
          fs.writeFileSync(gifPath, buffer)
          if (timestamp) fs.utimesSync(gifPath, timestamp, timestamp)
          gifsToConvert.push({
            tmpPath: gifPath,
            mp4Path: path.join(
              folders.webm,
              filename.replace(/\.gif$/, '.mp4')
            ),
            filename,
            uploadedDate,
          })
        } else {
          const stillPath = path.join(folders.images, filename)
          fs.writeFileSync(stillPath, buffer)
          if (timestamp) fs.utimesSync(stillPath, timestamp, timestamp)
          knownFilenames.add(filename)
        }
        continue
      }

      const hash = createHash('md5').update(buffer).digest('hex')
      if (isBitwiseDupe(hash)) continue

      const visualHash = await getVisualHashFromBuffer(buffer)
      if (visualHash && isVisualDupe(visualHash)) continue
      if (visualHash) addVisualHash(visualHash)

      const outPath = path.join(folders.images, filename)
      fs.writeFileSync(outPath, buffer)
      if (timestamp) fs.utimesSync(outPath, timestamp, timestamp)
      if (!isBitwiseDupe(hash)) {
        addBitwiseHash(hash)
        saveBitwiseHashCache()
        logAndProgress(`✅ updated global hash `)
      }
      knownFilenames.add(filename)

      logAndProgress(`✅ Saved ${filename}`)
    }
  } catch (err) {
    logAndProgress(`❌ Error scraping post ${link}: ${err.message}`)
  } finally {
    if (!page.isClosed()) await page.close()
  }
}

const argv = process.argv.slice(2)
const target = argv.find((arg) => arg.includes('coomer.su'))

let startPage = 0
let endPage = null

// Support --pages=1 or --pages=1-3 from both env and argv
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

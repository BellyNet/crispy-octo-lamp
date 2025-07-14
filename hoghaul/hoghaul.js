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
const pLimit = require('p-limit')

const limit = pLimit(2)
const lazyLimit = pLimit(2)

const { bannerHoghaul } = require('../banners.js') // adjust path if needed
bannerHoghaul()

const rootDir = path.join(__dirname, '..')
const datasetDir = path.join(
  process.env.APPDATA || path.join(process.env.HOME, 'AppData', 'Roaming'),
  '.slopvault',
  'dataset'
)
const tmpDir = path.join(__dirname, 'tmp')
const incompleteDir = path.join(__dirname, 'incomplete')
const incompleteGifDir = path.join(incompleteDir, 'gifs')
const incompleteVideoDir = path.join(incompleteDir, 'videos')
const lastCheckedPath = path.join(datasetDir, 'lastChecked.json')

if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true })
if (!fs.existsSync(incompleteGifDir))
  fs.mkdirSync(incompleteGifDir, { recursive: true })
if (!fs.existsSync(incompleteVideoDir))
  fs.mkdirSync(incompleteVideoDir, { recursive: true })

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

function downloadBufferWithProgress(url, onProgress) {
  const proto = url.startsWith('https') ? require('https') : require('http')
  return new Promise((resolve, reject) => {
    proto
      .get(url, (res) => {
        if (res.statusCode !== 200)
          return reject(new Error(`HTTP ${res.statusCode}`))
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
      .on('error', reject)
  })
}

function convertGifToMp4(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const cmd = `ffmpeg -y -i "${inputPath}" -movflags faststart -pix_fmt yuv420p -vf "scale=trunc(iw/2)*2:trunc(ih/2)*2" "${outputPath}"`
    exec(cmd, (err) => (err ? reject(err) : resolve()))
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

let lastCheckedMap = {}
if (fs.existsSync(lastCheckedPath)) {
  try {
    lastCheckedMap = JSON.parse(fs.readFileSync(lastCheckedPath, 'utf-8'))
  } catch (e) {
    console.warn('⚠️ Failed to load lastChecked cache:', e.message)
  }
}

let totalLazyBytes = 0
let lazyBytesDownloaded = 0
let lastDraw = 0

function logLazyProgress() {
  const percent = totalLazyBytes
    ? ((lazyBytesDownloaded / totalLazyBytes) * 100).toFixed(1)
    : '??'
  process.stdout.write(ansiEscapes.cursorTo(0, process.stdout.rows - 1))
  readline.clearLine(process.stdout, 0)
  process.stdout.write(
    `🐷 Lazy stuffing: ${percent}% (${(lazyBytesDownloaded / 1024 / 1024).toFixed(2)} MB)\n`
  )
}

async function scrapeCoomerUser(userUrl, startPage = 0, endPage = null) {
  const browser = await puppeteer.launch({
    headless: 'new',
    defaultViewport: null,
  })

  const page = await browser.newPage()
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
  )
  await page.setViewport({ width: 1280, height: 800 })
  const randomDelay = () => Math.random() * 2000 + 1000
  await new Promise((res) => setTimeout(res, randomDelay()))

  await page.goto(userUrl, { waitUntil: 'domcontentloaded' })
  await page.waitForSelector('a.user-header__profile span:last-of-type', {
    timeout: 0,
  })

  const username = await page.$eval(
    'a.user-header__profile span:last-of-type',
    (el) => el.textContent.trim()
  )

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
  const modelHashPath = path.join(folders.base, 'hashes.json')
  if (fs.existsSync(modelHashPath)) {
    try {
      JSON.parse(fs.readFileSync(modelHashPath)).forEach((h) =>
        knownHashes.add(h)
      )
    } catch (e) {
      console.warn(
        `⚠️ Failed to load hash cache for ${modelName}: ${e.message}`
      )
    }
  }

  const lastChecked = lastCheckedMap[rawName]
    ? new Date(lastCheckedMap[rawName])
    : null
  let newestDateSeen = lastChecked

  const postLinks = new Set()
  let pageNum = startPage

  while (true) {
    if (endPage !== null && pageNum > endPage) {
      console.log(`🧮 Reached end of page range (${startPage}-${endPage})`)
      break
    }

    await new Promise((res) => setTimeout(res, 1500)) // 1.5s pause

    pageNumDisplay = pageNum - startPage + 1
    console.log(`📦 Page ${pageNumDisplay}/${endPage - startPage + 1}`)

    const url = `${userUrl}?o=${pageNum * 50}`
    await page.goto(url, { waitUntil: 'networkidle2' })
    await page.waitForSelector('article.post-card a.fancy-link', {
      timeout: 10000,
    })

    const links = await page.$$eval('article.post-card a.fancy-link', (els) =>
      els.map((el) => el.href)
    )

    if (!links.length) break

    // Process this page’s links immediately
    let postIndex = 0
    await Promise.all(
      links.map((link) =>
        limit(() => {
          postIndex++
          console.log(
            `📸 Page ${pageNumDisplay} — (${++pagePostIndex}/${links.length}) — Global ${++globalPostIndex}/${totalExpectedPosts}`
          )

          return processPost(
            link,
            browser,
            folders,
            knownHashes,
            knownFilenames,
            gifsToConvert,
            lazyVideoQueue,
            (updatedDate) => {
              // update newestDateSeen if needed
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

    // Check for disabled “next” button
    const nextDisabled = await page.$(
      'a.pagination-button-disabled.pagination-button-after-current'
    )
    if (nextDisabled) break

    pageNum++
  }

  const urls = Array.from(postLinks)
  let completed = 0
  process.stdout.write('\n')
  logProgress(0, urls.length)

  await Promise.all(
    urls.map((link) =>
      limit(async () => {
        let taskCompleted = false
        const logAndProgress = (msg) => {
          if (!taskCompleted) {
            completed++
            taskCompleted = true
          }
          process.stdout.write(ansiEscapes.cursorTo(0, process.stdout.rows - 1))
          readline.clearLine(process.stdout, 0)
          console.log(msg)
          logProgress(completed, urls.length)
        }

        const page = await browser.newPage()
        try {
          await page.goto(link, { waitUntil: 'domcontentloaded' })

          let timeText = null
          try {
            await page.waitForSelector('time.timestamp', { timeout: 3000 })
            timeText = await page.$eval('time.timestamp', (el) =>
              el.getAttribute('datetime')
            )
          } catch (err) {
            console.warn(`⏳ No timestamp on ${link}`)
          }
          const uploadedDate = timeText ? new Date(timeText) : null

          if (
            !newestDateSeen ||
            (uploadedDate && uploadedDate > newestDateSeen)
          ) {
            newestDateSeen = uploadedDate
          }

          const mediaUrls = await page.$$eval(
            'a.fileThumb.image-link, video source, a.post__attachment-link[href]',
            (elements) =>
              elements
                .map(
                  (el) =>
                    el.href ||
                    el.src ||
                    el.getAttribute('src') ||
                    el.getAttribute('href')
                )
                .filter((url) => {
                  if (!url) return false
                  const normalized = url.startsWith('http')
                    ? url
                    : `https:${url}`
                  return (
                    normalized.includes('/data/') || // full-res images
                    normalized.endsWith('.mp4') || // video sources
                    normalized.endsWith('.m4v') ||
                    (!normalized.includes('/thumbnail/') &&
                      !normalized.includes('/icons/') &&
                      !normalized.includes('/static/') &&
                      !normalized.includes('/user/') &&
                      !normalized.endsWith('.svg'))
                  )
                })
          )

          for (const mediaUrl of mediaUrls) {
            let url = normalizeUrl(mediaUrl)
            if (url.includes('?f=')) {
              url = url.split('?')[0]
            }

            if (!url)
              return logAndProgress`⚠️ Failed to extract filename from URL: ${url}`

            let filename
            try {
              filename = decodeURIComponent(
                path.basename(new URL(url).pathname).split('?')[0]
              )
              if (/avatar|profile/i.test(filename)) {
                logAndProgress(`🚫 Skipping avatar image: ${filename}`)
                continue
              }
            } catch (e) {
              return logAndProgress(
                `⚠️ Failed to extract filename from URL: ${url}`
              )
            }

            const ext = path.extname(filename).toLowerCase()

            const timestamp = uploadedDate
              ? uploadedDate.getTime() / 1000
              : null

            if (knownFilenames.has(filename)) {
              logAndProgress(`🔁 Existing filename: ${filename}`)
              continue
            }

            const buffer = await downloadBufferWithProgress(url)
            const hash = createHash('md5').update(buffer).digest('hex')
            if (knownHashes.has(hash)) {
              logAndProgress(`♻️ Visual dupe: ${filename}`)
              continue
            }

            if (ext === '.gif') {
              const frameCount = await getGifFrameCount(buffer)
              if (frameCount > 1) {
                const tmpPath = path.join(incompleteGifDir, filename)
                fs.writeFileSync(tmpPath, buffer)
                if (timestamp) fs.utimesSync(tmpPath, timestamp, timestamp)
                const gifSavePath = path.join(folders.gif, filename)
                fs.writeFileSync(gifSavePath, buffer)
                if (timestamp) fs.utimesSync(gifSavePath, timestamp, timestamp)
                gifsToConvert.push({
                  tmpPath,
                  mp4Path: path.join(
                    folders.webm,
                    filename.replace(/\.gif$/, '.mp4')
                  ),
                  filename,
                })
                logAndProgress(`📥 Queued gif for conversion: ${filename}`)
              } else {
                const outPath = path.join(folders.images, filename)
                fs.writeFileSync(outPath, buffer)
                if (timestamp) fs.utimesSync(outPath, timestamp, timestamp)
                logAndProgress(`🖼️ Saved still gif: ${filename}`)
                knownHashes.add(hash)
                knownFilenames.add(filename)
              }
            } else if (['.mp4', '.m4v'].includes(ext)) {
              const tmpPath = path.join(incompleteVideoDir, filename)
              fs.writeFileSync(tmpPath, buffer)
              if (timestamp) fs.utimesSync(tmpPath, timestamp, timestamp)
              lazyVideoQueue.push({
                url,
                path: path.join(folders.webm, filename),
                tmpPath,
                filename,
              })
              logAndProgress(`🐌 Queued lazy video: ${filename}`)
            } else {
              const outPath = path.join(folders.images, filename)
              fs.writeFileSync(outPath, buffer)
              if (timestamp) fs.utimesSync(outPath, timestamp, timestamp)
              logAndProgress(`✅ Saved: ${filename}`)
              knownHashes.add(hash)
              knownFilenames.add(filename)
            }
          }
        } catch (err) {
          console.error(`❌ Error on ${link}: ${err.message}`)
        } finally {
          if (!page.isClosed()) await page.close()
        }
      })
    )
  )

  // convert gifs
  for (const gif of fs
    .readdirSync(incompleteGifDir)
    .filter((f) => f.endsWith('.gif'))) {
    const tmpPath = path.join(incompleteGifDir, gif)
    const mp4Path = path.join(folders.webm, gif.replace(/\.gif$/, '.mp4'))
    gifsToConvert.push({ tmpPath, mp4Path, filename: gif })
  }

  for (const { tmpPath, mp4Path, filename } of gifsToConvert) {
    try {
      if (fs.existsSync(mp4Path)) continue
      await convertGifToMp4(tmpPath, mp4Path)
      knownHashes.add(
        createHash('md5').update(fs.readFileSync(mp4Path)).digest('hex')
      )
      knownFilenames.add(path.basename(mp4Path))
      console.log(`🎞️ Converted: ${filename}`)
      if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
    } catch (err) {
      console.error(`❌ Conversion failed for ${filename}`)
    }
  }

  // Pre-fetch expected file sizes (best-effort)
  await Promise.all(
    lazyVideoQueue.map(async ({ url }) => {
      return new Promise((resolve) => {
        const proto = url.startsWith('https')
          ? require('https')
          : require('http')
        proto
          .get(url, { method: 'HEAD' }, (res) => {
            const size = parseInt(res.headers['content-length']) || 0
            totalLazyBytes += size
            res.destroy()
            resolve()
          })
          .on('error', resolve)
      })
    })
  )

  await Promise.all(
    lazyVideoQueue.map(({ url, path: finalPath, tmpPath, filename }, i) =>
      lazyLimit(async () => {
        if (knownFilenames.has(filename) || fs.existsSync(finalPath)) {
          return console.log(`♻️ Lazy dupe (pre-download): ${filename}`)
        }

        console.log(logLazyDownload(i))
        console.log(`⏳ (${i + 1}/${lazyVideoQueue.length})`)
        try {
          const buffer =
            tmpPath && fs.existsSync(tmpPath)
              ? fs.readFileSync(tmpPath)
              : await downloadBufferWithProgress(
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
          const hash = createHash('md5').update(buffer).digest('hex')
          if (knownHashes.has(hash))
            return console.log(`♻️ Lazy dupe: ${filename}`)
          fs.writeFileSync(finalPath, buffer)
          knownHashes.add(hash)
          knownFilenames.add(filename)
          console.log(`✅ Saved lazy video: ${filename}`)
          if (tmpPath && fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
        } catch (err) {
          console.warn(`❌ Lazy failed: ${filename} - ${err.message}`)
        }
      })
    )
  )

  lastCheckedMap[rawName] = newestDateSeen?.toISOString()
  fs.writeFileSync(lastCheckedPath, JSON.stringify(lastCheckedMap, null, 2))

  const logPath = path.join(folders.base, 'log.txt')
  fs.writeFileSync(
    logPath,
    `Model: ${modelName}\nTotal: ${urls.length}\nSaved: ${knownFilenames.size}\nDupes: ${urls.length - knownFilenames.size}\nErrors: 0`
  )
  fs.writeFileSync(
    path.join(folders.base, 'hashes.json'),
    JSON.stringify([...knownHashes], null, 2)
  )

  console.log('\n' + getCompletionLine())
  console.log('🧼 Scrape complete. Leaving browser open for inspection.')
  await browser.close()
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
  const page = await browser.newPage()
  let taskCompleted = false
  const logAndProgress = (msg) => {
    if (!taskCompleted) {
      taskCompleted = true
    }
    console.log(msg)
  }

  try {
    await page.goto(link, { waitUntil: 'domcontentloaded' })

    let timeText = null
    try {
      await page.waitForSelector('time.timestamp', { timeout: 3000 })
      timeText = await page.$eval('time.timestamp', (el) =>
        el.getAttribute('datetime')
      )
    } catch (err) {
      console.warn(`⏳ No timestamp on ${link}`)
    }
    const uploadedDate = timeText ? new Date(timeText) : null
    updateNewestDate(uploadedDate)

    const mediaUrls = await page.evaluate(() => {
      const urls = []

      // Full-size image or file
      const anchors = document.querySelectorAll('a.fileThumb.image-link')
      anchors.forEach((a) => {
        if (a?.href?.startsWith('http')) urls.push(a.href)
      })

      // Embedded video (direct .mp4)
      const sources = document.querySelectorAll('video source[src]')
      sources.forEach((s) => {
        if (s?.src?.startsWith('http')) urls.push(s.src)
      })

      // Direct attachments
      const links = document.querySelectorAll('a.post__attachment-link[href]')
      links.forEach((l) => {
        if (l?.href?.startsWith('http')) urls.push(l.href)
      })

      return urls
    })

    for (const mediaUrl of mediaUrls) {
      const url = normalizeUrl(mediaUrl)
      if (!url)
        return logAndProgress(`⚠️ Failed to extract filename from URL: ${url}`)

      let filename
      try {
        filename = decodeURIComponent(
          path.basename(new URL(url).pathname).split('?')[0]
        )

        if (/avatar|profile/i.test(filename)) {
          logAndProgress(`🚫 Skipping avatar image: ${filename}`)
          continue
        }
      } catch (e) {
        return logAndProgress(`⚠️ Failed to extract filename from URL: ${url}`)
      }

      const ext = path.extname(filename).toLowerCase()
      const timestamp = uploadedDate ? uploadedDate.getTime() / 1000 : null

      if (knownFilenames.has(filename)) {
        logAndProgress(`🔁 Existing filename: ${filename}`)
        continue
      }

      const buffer = await downloadBufferWithProgress(url)
      const hash = createHash('md5').update(buffer).digest('hex')
      if (knownHashes.has(hash)) {
        logAndProgress(`♻️ Visual dupe: ${filename}`)
        continue
      }

      if (ext === '.gif') {
        const frameCount = await getGifFrameCount(buffer)
        if (frameCount > 1) {
          const tmpPath = path.join(incompleteGifDir, filename)
          fs.writeFileSync(tmpPath, buffer)
          if (timestamp) fs.utimesSync(tmpPath, timestamp, timestamp)
          const gifSavePath = path.join(folders.gif, filename)
          fs.writeFileSync(gifSavePath, buffer)
          if (timestamp) fs.utimesSync(gifSavePath, timestamp, timestamp)
          gifsToConvert.push({
            tmpPath,
            mp4Path: path.join(
              folders.webm,
              filename.replace(/\.gif$/, '.mp4')
            ),
            filename,
          })
          logAndProgress(`📥 Queued gif for conversion: ${filename}`)
        } else {
          const outPath = path.join(folders.images, filename)
          fs.writeFileSync(outPath, buffer)
          if (timestamp) fs.utimesSync(outPath, timestamp, timestamp)
          logAndProgress(`🖼️ Saved still gif: ${filename}`)
          knownHashes.add(hash)
          knownFilenames.add(filename)
        }
      } else if (['.mp4', '.m4v'].includes(ext)) {
        const tmpPath = path.join(incompleteVideoDir, filename)
        fs.writeFileSync(tmpPath, buffer)
        if (timestamp) fs.utimesSync(tmpPath, timestamp, timestamp)
        lazyVideoQueue.push({
          url,
          path: path.join(folders.webm, filename),
          tmpPath,
          filename,
        })
        logAndProgress(`🐌 Queued lazy video: ${filename}`)
      } else {
        const outPath = path.join(folders.images, filename)
        fs.writeFileSync(outPath, buffer)
        if (timestamp) fs.utimesSync(outPath, timestamp, timestamp)
        logAndProgress(`✅ Saved: ${filename}`)
        knownHashes.add(hash)
        knownFilenames.add(filename)
      }
    }
  } catch (err) {
    console.error(`❌ Error on ${link}: ${err.message}`)
  } finally {
    if (!page.isClosed()) await page.close()
  }
}

const args = process.argv.slice(2)
const target = args.find((arg) => arg.includes('coomer.su'))

let startPage = 0
let endPage = null

const pageArg = args.find((arg) => arg.startsWith('--pages='))
if (pageArg) {
  const [, value] = pageArg.split('=')
  if (value.includes('-')) {
    const [start, end] = value.split('-').map((n) => parseInt(n, 10))
    startPage = isNaN(start) ? 0 : start
    endPage = isNaN(end) ? null : end
  } else {
    const num = parseInt(value, 10)
    endPage = isNaN(num) ? null : num
  }
}

if (!target) {
  console.error('Usage: node hoghaul.js <coomer-url> [--pages=N or N-M]')
  process.exit(1)
}

scrapeCoomerUser(target, startPage, endPage)

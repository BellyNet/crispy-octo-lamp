const { executablePath } = require('puppeteer')
const puppeteer = require('puppeteer')
const fs = require('fs')
const path = require('path')
const { exec } = require('child_process')
const { createHash } = require('crypto')
const https = require('https')
const http = require('http')
const pLimit = require('p-limit')
const limit = pLimit(8)
const lazyLimit = pLimit(4)
const readline = require('readline')
const ansiEscapes = require('ansi-escapes')
const chalk = require('chalk').default

const { bannerMilkmaid } = require('../banners.js') // adjust path if needed
bannerMilkmaid()

// Helpers
const { createScraperPage } = require('../scrapyard/pageHelpers')
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

const {
  logProgress,
  logLazyDownload,
  logGifConversion,
  getCompletionLine,
} = require('../stuffinglogger')

function sanitize(name) {
  return name.replace(/[^a-z0-9_\-]/gi, '_').toLowerCase()
}

const sleep = (ms) => new Promise((res) => setTimeout(res, ms))
const randomDelay = () => sleep(Math.floor(Math.random() * 1200) + 300)

const knownFilenames = new Set()
const skippedFilenames = new Set()
const queuedVideos = new Set()

const gifsToConvert = []
const lazyVideoQueue = []
let totalCount = 0,
  duplicateCount = 0,
  errorCount = 0,
  successCount = 0

const rootDir = path.join(__dirname, '..')
const datasetDir = path.join(
  process.env.APPDATA || path.join(process.env.HOME, 'AppData', 'Roaming'),
  '.slopvault',
  'dataset'
)
const tmpDir = path.join(rootDir, 'tmp')

if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true })

const incompleteDir = path.join(rootDir, 'incomplete')
const incompleteGifDir = path.join(incompleteDir, 'gifs')
const incompleteVideoDir = path.join(incompleteDir, 'videos')
if (!fs.existsSync(incompleteGifDir))
  fs.mkdirSync(incompleteGifDir, { recursive: true })
if (!fs.existsSync(incompleteVideoDir))
  fs.mkdirSync(incompleteVideoDir, { recursive: true })

function createModelFolders(modelName) {
  const base = path.join(datasetDir, modelName)
  const images = path.join(base, 'images')

  // Always create images folder
  fs.mkdirSync(images, { recursive: true })

  return {
    base,
    images,
    createGifFolder: () => {
      const gifPath = path.join(base, 'gif')
      if (!fs.existsSync(gifPath)) fs.mkdirSync(gifPath, { recursive: true })
      return gifPath
    },
    createWebmFolder: () => {
      const webmPath = path.join(base, 'webm')
      if (!fs.existsSync(webmPath)) fs.mkdirSync(webmPath, { recursive: true })
      return webmPath
    },
  }
}

function downloadBufferWithProgress(mediaUrl, onProgress) {
  const proto = mediaUrl.startsWith('https') ? https : http
  return new Promise((resolve, reject) => {
    proto
      .get(mediaUrl, (res) => {
        if (res.statusCode !== 200)
          return reject(new Error(`HTTP ${res.statusCode}`))
        const totalBytes = parseInt(res.headers['content-length'] || '0', 10)
        let downloadedBytes = 0,
          chunks = [],
          start = Date.now()

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
    logAndProgress(`🔥 Converting`)
    exec(cmd, (err) => (err ? reject(err) : resolve()))
  })
}

function getGifFrameCount(buffer) {
  return new Promise((resolve) => {
    const tmp = path.join(tmpDir, `__framecheck_${Date.now()}.gif`)
    fs.writeFileSync(tmp, buffer)
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

async function fetchStufferDBTotalCount(browser, url) {
  const tempPage = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: true,
  })
  try {
    await tempPage.goto(url, { waitUntil: 'networkidle2', timeout: 20000 })

    // Log raw text from the span
    const rawText = await tempPage.$eval(
      'span.badge.nb_items',
      (el) => el.textContent
    )
    console.log(`🕵️ Raw badge text from ${url}:`, rawText)

    const match = rawText.match(/(\d+)/)
    const count = match ? parseInt(match[1]) : 0
    console.log(`🔢 Parsed count: ${count}`)
    return count
  } catch (err) {
    const title = await tempPage.title()
    console.log(`⚠️ Could not fetch count for ${url}: ${err.message}`)
    console.log(`🧙 Page title: ${title}`)
    return 0
  } finally {
    if (!tempPage.isClosed()) await tempPage.close()
  }
}

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

let grandCompleted = 0

async function scrapeGallery(browser, url, modelName, folders) {
  const { base, images, webm } = folders

  const page = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: false,
  })

  process.stdout.write('\n') // Reserve one lines
  grandCompleted++
  logProgress(grandCompleted, global.totalSearchTotal)

  try {
    while (url) {
      await page.goto(url, { waitUntil: 'domcontentloaded' })

      const urls = await page.$$eval('a[href^="picture?/"]', (links) => [
        ...new Set(links.map((l) => l.href)),
      ])

      const total = urls.length

      const mode = url.includes('&acs=') ? 'ACS' : 'PLAIN'
      logAndProgress(`📸 ${modelName} - [${mode}] - ${urls.length} media links`)

      const pages = await Promise.all(
        Array.from({ length: 8 }, () =>
          createScraperPage(browser, {
            site: 'stufferdb',
            interceptMedia: false,
          })
        )
      )

      let pageIndex = 0

      const pageLocks = pages.map(() => pLimit(1)) // 🧠 One lock per tab

      async function scrapeMediaOnPage(page, mediaPageUrl, i) {
        taskCompleted = false
        totalCount++

        try {
          await page.goto(mediaPageUrl, {
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          })

          const uploadedDateIso = await page.evaluate(() => {
            const anchor = document.querySelector('#datepost dd a')
            if (!anchor) return null
            const text = anchor.textContent?.trim()
            const match = text.match(/\d{1,2} \w+ \d{4}/)
            if (!match) return null
            const date = new Date(match[0])
            return isNaN(date.getTime()) ? null : date.toISOString()
          })

          const uploadedDate = uploadedDateIso
            ? new Date(uploadedDateIso)
            : null

          const mediaUrl = await page.evaluate(() => {
            const video = document.querySelector('video.vjs-tech[src]')
            const img = document.querySelector('#theMainImage')
            return video?.src || img?.src || null
          })

          if (!mediaUrl) return

          const parsed = new URL(mediaUrl)
          let filename = decodeURIComponent(
            path.basename(parsed.pathname).split('?')[0]
          )

          let ext = path.extname(filename).toLowerCase()
          if (ext === '.m4v') {
            ext = '.mp4'
            filename = filename.replace(/\.m4v$/i, '.mp4')
          }

          let buffer = null
          let hash = null
          let visualHash = null

          // Step 1: Fetch file buffer
          if (!['.mp4', '.webm', '.gif'].includes(ext)) {
            buffer = await downloadBufferWithProgress(mediaUrl)

            // Step 2: Bitwise (fast) hash
            hash = createHash('md5').update(buffer).digest('hex')
            if (isBitwiseDupe(hash)) {
              duplicateCount++
              return logAndProgress(`♻️ Bitwise dupe: ${filename}`)
            }

            // Step 3: Visual (slow) hash

            visualHash = await getVisualHashFromBuffer(buffer)
            if (visualHash && isVisualDupe(visualHash)) {
              duplicateCount++
              return logAndProgress(`👁️ Visual dupe (global): ${filename}`)
            }
            if (visualHash) addVisualHash(visualHash)
          }

          if (ext === '.gif') {
            buffer = await downloadBufferWithProgress(mediaUrl)
            hash = createHash('md5').update(buffer).digest('hex')

            const frameCount = await getGifFrameCount(buffer)
            if (frameCount > 1) {
              // Animated GIF → Save and queue conversion
              const mp4Name = filename.replace(/\.gif$/, '.mp4')

              if (knownFilenames.has(mp4Name)) {
                duplicateCount++
                return logAndProgress(
                  `♻️ Already converted gif > mp4: ${mp4Name}`
                )
              }

              const tmpPath = path.join(incompleteGifDir, filename)
              fs.writeFileSync(tmpPath, buffer)

              // Create gif folder only now
              const gifFolder = folders.createGifFolder()
              const gifSavePath = path.join(gifFolder, filename)
              fs.writeFileSync(gifSavePath, buffer)

              if (uploadedDate) {
                const ts = uploadedDate.getTime() / 1000
                fs.utimesSync(tmpPath, ts, ts)
                fs.utimesSync(gifSavePath, ts, ts)
              }

              // Create webm folder only when queuing conversion
              const webmFolder = folders.createWebmFolder()
              gifsToConvert.push({
                tmpPath,
                mp4Path: path.join(webmFolder, mp4Name),
                filename,
              })

              return logAndProgress(logGifConversion(completedTotal))
            } else {
              // Static GIF → treat as image
              const stillPath = path.join(folders.images, filename)
              fs.writeFileSync(stillPath, buffer)
              if (uploadedDate) {
                const ts = uploadedDate.getTime() / 1000
                fs.utimesSync(stillPath, ts, ts)
              }

              knownFilenames.add(filename)
              if (visualHash) addVisualHash(visualHash)
              if (!isBitwiseDupe(hash)) {
                addBitwiseHash(hash)
                saveBitwiseHashCache()
              }

              successCount++
              return logAndProgress(`🖼️ Saved still gif: ${filename}`)
            }
          }

          if (ext === '.mp4') {
            const webmFolder = folders.createWebmFolder() // Create only when needed
            const finalPath = path.join(webmFolder, filename)

            if (knownFilenames.has(filename) || fs.existsSync(finalPath)) {
              duplicateCount++
              return logAndProgress(
                `⛔ Skipping mp4 – already handled: ${filename}`
              )
            }

            const tmpPath = path.join(incompleteVideoDir, filename)

            lazyVideoQueue.push({
              url: mediaUrl,
              path: finalPath,
              tmpPath,
              filename,
              uploadedDate,
            })

            return logAndProgress(`🐌 Queued lazy video: ${filename}`)
          }

          if (knownFilenames.has(filename)) {
            duplicateCount++
            return logAndProgress(`🔁 Existing filename: ${filename}`)
          }

          buffer = await downloadBufferWithProgress(mediaUrl)
          hash = createHash('md5').update(buffer).digest('hex')

          const finalPath = path.join(images, filename)
          fs.writeFileSync(finalPath, buffer)
          if (uploadedDate) {
            const ts = uploadedDate.getTime() / 1000
            fs.utimesSync(finalPath, ts, ts)
          }

          if (!isBitwiseDupe(hash)) {
            addBitwiseHash(hash)
            saveBitwiseHashCache()
          }

          if (visualHash) addVisualHash(visualHash)
          knownFilenames.add(filename)
          successCount++
          return logAndProgress(`✅ Saved: ${filename}`)
        } catch (err) {
          errorCount++
          logAndProgress(`❌ Error processing ${mediaPageUrl}: ${err.message}`)
        }

        await randomDelay()
      }

      await Promise.all(
        urls.map((mediaPageUrl, i) => {
          const page = pages[i % pages.length]
          const lock = pageLocks[i % pageLocks.length]

          return limit(() =>
            lock(() => scrapeMediaOnPage(page, mediaPageUrl, i))
          )
        })
      )

      const nextHref = await page
        .$eval('a[rel="next"]', (el) => el?.href)
        .catch(() => null)
      if (nextHref) {
        const baseUrl = new URL(url)
        url = new URL(nextHref, baseUrl).href
      } else {
        break
      }
    }
  } finally {
    readline.clearLine(process.stdout, 0)
    readline.cursorTo(process.stdout, 0)
    logAndProgress(getCompletionLine())

    await page.close()
  }
}

;(async () => {
  let inputUrl = process.argv[2]
  if (!inputUrl || !inputUrl.includes('/category/'))
    return logAndProgress('⚠️  Usage: node milkmaid.js <gallery-url>')

  inputUrl = inputUrl.replace(/&acs=[^&]+/i, '')

  const categoryId = inputUrl.match(/category\/?(\d+)/)?.[1]
  if (!categoryId) return logAndProgress('❌ Invalid category URL')

  const browser = await puppeteer.launch({
    headless: 'new',
    executablePath: executablePath(),
    args: ['--no-sandbox', '--ignore-certificate-errors'],
    ignoreHTTPSErrors: true, // ✅ add this too
  })

  const tempPage = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: true,
  })
  await tempPage.goto(inputUrl, { waitUntil: 'domcontentloaded' })

  loadVisualHashCache()
  loadBitwiseHashCache()

  const tmpModelName = await tempPage
    .evaluate(() => {
      const h2 = document.querySelector('.titrePage h2')
      const anchors = h2?.querySelectorAll('a')
      return anchors?.[anchors.length - 1]?.textContent?.trim() || 'unknown_cow'
    })
    .then((n) => n.replace(/\W+/g, '_').toLowerCase())

  const rawName = sanitize(tmpModelName)
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

  const plainUrl = `https://stufferdb.com/index?/category/${categoryId}`

  logAndProgress('🔍 Prefetching total counts...')
  const plainCount = await Promise.all([
    fetchStufferDBTotalCount(browser, plainUrl),
  ])

  global.totalSearchTotal = plainCount
  logAndProgress(`📊 Combined media total: ${global.totalSearchTotal}`)

  logAndProgress(`💦 Starting scrape for ${modelName}`)

  await scrapeGallery(browser, plainUrl, modelName, folders)

  logAndProgress('🧮 Scrape complete')

  const leftoverGifs = fs
    .readdirSync(incompleteGifDir)
    .filter((f) => f.endsWith('.gif'))
  for (const gif of leftoverGifs) {
    const tmpPath = path.join(incompleteGifDir, gif)
    const webmFolder = folders.createWebmFolder()
    const mp4Path = path.join(webmFolder, gif.replace(/\.gif$/, '.mp4'))
    gifsToConvert.push({ tmpPath, mp4Path, filename: gif })
  }

  logAndProgress(`🚜 Converting gifs: ${gifsToConvert.length}`)
  const filteredGifs = gifsToConvert.filter(({ mp4Path }) => {
    const mp4Name = path.basename(mp4Path)
    const isKnown = knownFilenames.has(mp4Name) || skippedFilenames.has(mp4Name)
    if (isKnown) {
      logAndProgress(
        `🚫 Skipping gif conversion (already known or failed): ${mp4Name}`
      )
    }
    return !isKnown
  })

  for (const { tmpPath, mp4Path, filename } of filteredGifs) {
    try {
      if (fs.existsSync(mp4Path)) {
        logAndProgress(`♻️ Already exists: ${mp4Path}`)
        continue
      }

      logAndProgress(`🔥 Converting GIF → MP4: ${filename}`)
      await convertGifToMp4(tmpPath, mp4Path)

      // Preserve timestamp if available
      const uploadedDate = gifsToConvert.find(
        (g) => g.filename === filename
      )?.uploadedDate
      if (uploadedDate) {
        const ts = uploadedDate.getTime() / 1000
        fs.utimesSync(mp4Path, ts, ts)
      }

      knownFilenames.add(path.basename(mp4Path))

      // Clean up the original GIF from tmp folder
      if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)

      logAndProgress(`✅ Converted GIF to MP4: ${filename}`)
    } catch (err) {
      logAndProgress(`❌ Conversion failed for ${filename}: ${err.message}`)
      skippedFilenames.add(path.basename(mp4Path))
    }
  }

  logAndProgress(`🐢 Lazy downloading videos: ${lazyVideoQueue.length}`)
  let lastDraw = 0
  let totalLazyBytes = 0
  let lazyBytesDownloaded = 0

  // Pre-fetch expected file sizes (best-effort)
  await Promise.all(
    lazyVideoQueue.map(async ({ url }) => {
      return new Promise((resolve) => {
        const proto = url.startsWith('https') ? https : http
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

  function logLazyProgress() {
    const percent = totalLazyBytes
      ? ((lazyBytesDownloaded / totalLazyBytes) * 100).toFixed(1)
      : '??'
    process.stdout.write(ansiEscapes.cursorTo(0, process.stdout.rows - 1))
    readline.clearLine(process.stdout, 0)
    process.stdout.write(
      `🐷 Lazy stuffing: ${percent}% (${(lazyBytesDownloaded / 1024 / 1024).toFixed(2)} MB)`
    )
  }

  await Promise.all(
    lazyVideoQueue.map(({ url, path: finalPath, filename, uploadedDate }, i) =>
      lazyLimit(async () => {
        if (knownFilenames.has(filename) || fs.existsSync(finalPath)) {
          duplicateCount++
          return logAndProgress(`♻️ Lazy dupe (pre-download): ${filename}`)
        }

        knownFilenames.add(filename) // ✅ Mark as claimed early

        logAndProgress(`🚀 STARTING lazy task #${i}: ${filename}`)
        logAndProgress(logLazyDownload(i))
        logAndProgress(`⏳ (${i + 1}/${lazyVideoQueue.length})`)

        const stream = fs.createWriteStream(finalPath)
        let lastDraw = Date.now()

        try {
          await new Promise((resolve, reject) => {
            const proto = url.startsWith('https') ? https : http
            proto
              .get(url, (res) => {
                if (res.statusCode !== 200)
                  return reject(new Error(`HTTP ${res.statusCode}`))

                res.on('data', (chunk) => {
                  stream.write(chunk)
                  lazyBytesDownloaded += chunk.length

                  const now = Date.now()
                  if (now - lastDraw > 250) {
                    const percent = totalLazyBytes
                      ? ((lazyBytesDownloaded / totalLazyBytes) * 100).toFixed(
                          1
                        )
                      : '??'
                    const mb = (lazyBytesDownloaded / 1024 / 1024).toFixed(1)
                    logLazyProgress()
                    lastDraw = now
                  }
                })

                res.on('end', () => {
                  stream.end()
                  resolve()
                })

                res.on('error', reject)
              })
              .on('error', reject)
          })

          if (uploadedDate) {
            const ts = uploadedDate.getTime() / 1000
            fs.utimesSync(finalPath, ts, ts)
          }

          successCount++
          logAndProgress(`✅ Saved lazy video: ${filename}`)

          const duration = await getVideoDuration(finalPath)
          const isSmallFile = fs.statSync(finalPath).size < 5 * 1024 * 1024 // <5MB
          if (duration <= 6 && isSmallFile) {
            const gifFolder = folders.createGifFolder()
            const gifName = filename.replace(/\.(mp4|m4v)$/i, '.gif')
            const gifPath = path.join(gifFolder, gifName)

            if (!fs.existsSync(gifPath)) {
              await convertShortMp4ToGif(finalPath, gifPath)
              if (uploadedDate) {
                const ts = uploadedDate.getTime() / 1000
                fs.utimesSync(gifPath, ts, ts)
              }
              logAndProgress(`🎁 Converted short mp4 to gif: ${gifName}`)
            }
          }
        } catch (err) {
          errorCount++
          logAndProgress(`❌ Lazy failed: ${filename} - ${err.message}`)
          if (fs.existsSync(finalPath)) fs.unlinkSync(finalPath)
          knownFilenames.delete(filename) // allow retry in future runs
        }
      })
    )
  )

  await browser.close()

  saveVisualHashCache()

  exec(
    `robocopy "%APPDATA%\\.slopvault\\dataset\\${modelName}" "Z:\\dataset\\${modelName}" /MIR /R:2 /W:5`,
    (err) => {
      if (err && err.code > 3) {
        console.error('❌ NAS sync failed with code', err.code)
      } else {
        console.log('✅ NAS sync complete.')
      }
    }
  )

  console.log(
    `🎉 Done: ${successCount} saved, ${duplicateCount} dupes, ${errorCount} errors`
  )
})()

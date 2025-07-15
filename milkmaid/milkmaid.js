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

const knownHashes = new Set()
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
const lastCheckedPath = path.join(datasetDir, 'lastChecked.json')

if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true })
let lastCheckedMap = {}
if (fs.existsSync(lastCheckedPath)) {
  try {
    lastCheckedMap = JSON.parse(fs.readFileSync(lastCheckedPath, 'utf-8'))
  } catch (e) {
    console.warn('⚠️ Failed to load lastChecked cache:', e.message)
  }
}

const incompleteDir = path.join(rootDir, 'incomplete')
const incompleteGifDir = path.join(incompleteDir, 'gifs')
const incompleteVideoDir = path.join(incompleteDir, 'videos')
if (!fs.existsSync(incompleteGifDir))
  fs.mkdirSync(incompleteGifDir, { recursive: true })
if (!fs.existsSync(incompleteVideoDir))
  fs.mkdirSync(incompleteVideoDir, { recursive: true })

function createModelFolders(modelName) {
  const base = path.join(datasetDir, modelName)

  const folders = ['images', 'webm', 'gif', 'tags', 'captions']
  for (const folder of folders)
    fs.mkdirSync(path.join(base, folder), { recursive: true })
  return {
    base,
    images: path.join(base, 'images'),
    webm: path.join(base, 'webm'),
    gif: path.join(base, 'gif'),
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
    console.log(`🔥 Converting`)
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
    console.warn(`⚠️ Could not fetch count for ${url}: ${err.message}`)
    console.warn(`🧙 Page title: ${title}`)
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
  logProgress(completedTotal, global.totalSearchTotal || total)
}

async function scrapeGallery(browser, url, modelName, folders, lastChecked) {
  const { base, images, webm } = folders
  let newestDateSeen = lastChecked

  const page = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: false,
  })

  process.stdout.write('\n') // Reserve one lines
  logProgress(0, global.totalSearchTotal || total)

  try {
    while (url) {
      await page.goto(url, { waitUntil: 'domcontentloaded' })

      const urls = await page.$$eval('a[href^="picture?/"]', (links) => [
        ...new Set(links.map((l) => l.href)),
      ])

      const total = urls.length

      const mode = url.includes('&acs=') ? 'ACS' : 'PLAIN'
      logAndProgress(`📸 ${modelName} - [${mode}] - ${urls.length} media links`)

      await Promise.all(
        urls.map(
          async (mediaPageUrl) =>
            await limit(async () => {
              taskCompleted = false

              totalCount++
              const workerPage = await createScraperPage(browser, {
                site: 'stufferdb',
                interceptMedia: false,
              })

              try {
                await workerPage.goto(mediaPageUrl, {
                  waitUntil: 'domcontentloaded',
                  timeout: 20000,
                })

                const uploadedDateIso = await workerPage.evaluate(() => {
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

                if (
                  lastChecked &&
                  uploadedDate &&
                  uploadedDate <= lastChecked
                ) {
                  return logAndProgress(
                    `⏩ Skipping old media from ${uploadedDate.toISOString().split('T')[0]}`
                  )
                }

                if (
                  !newestDateSeen ||
                  (uploadedDate && uploadedDate > newestDateSeen)
                ) {
                  newestDateSeen = uploadedDate
                }

                const mediaUrl = await workerPage.evaluate(() => {
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
                buffer = await downloadBufferWithProgress(mediaUrl)

                // Step 2: Bitwise (fast) hash
                hash = createHash('md5').update(buffer).digest('hex')
                if (knownHashes.has(hash)) {
                  duplicateCount++
                  return logAndProgress(`♻️ Bitwise dupe: ${filename}`)
                }

                // Step 3: Visual (slow) hash
                if (!['.mp4', '.webm', '.gif'].includes(ext)) {
                  const visualHash = await getVisualHashFromBuffer(buffer)
                  if (visualHash && isVisualDupe(visualHash)) {
                    duplicateCount++
                    return logAndProgress(
                      `👁️ Visual dupe (global): ${filename}`
                    )
                  }
                  if (visualHash) addVisualHash(visualHash)
                }

                if (ext === '.gif') {
                  buffer = await downloadBufferWithProgress(mediaUrl)
                  hash = createHash('md5').update(buffer).digest('hex')
                  const frameCount = await getGifFrameCount(buffer)
                  if (frameCount > 1) {
                    const mp4Name = filename.replace(/\.gif$/, '.mp4')
                    if (knownFilenames.has(mp4Name)) {
                      duplicateCount++
                      return logAndProgress(
                        `♻️ Already converted gif > mp4: ${mp4Name}`
                      )
                    }
                    const tmpPath = path.join(incompleteGifDir, filename)
                    fs.writeFileSync(tmpPath, buffer)
                    const gifSavePath = path.join(folders.gif, filename)
                    fs.writeFileSync(gifSavePath, buffer)
                    if (uploadedDate) {
                      const ts = uploadedDate.getTime() / 1000
                      fs.utimesSync(tmpPath, ts, ts)
                      fs.utimesSync(gifSavePath, ts, ts)
                    }
                    gifsToConvert.push({
                      tmpPath,
                      mp4Path: path.join(webm, mp4Name),
                      filename,
                    })
                    return logAndProgress(logGifConversion(completedTotal))
                  } else {
                    const stillPath = path.join(images, filename)
                    fs.writeFileSync(stillPath, buffer)
                    if (uploadedDate) {
                      const ts = uploadedDate.getTime() / 1000
                      fs.utimesSync(stillPath, ts, ts)
                    }
                    knownHashes.add(hash)
                    knownFilenames.add(filename)
                    if (visualHash) addVisualHash(visualHash)

                    successCount++
                    return logAndProgress(`🖼️ Saved still gif: ${filename}`)
                  }
                }

                if (ext === '.mp4') {
                  const finalPath = path.join(webm, filename)
                  if (
                    knownFilenames.has(filename) ||
                    fs.existsSync(finalPath)
                  ) {
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

                // Add both hashes to global sets
                knownHashes.add(hash)
                if (visualHash) addVisualHash(visualHash)
                knownFilenames.add(filename)
                successCount++
                return logAndProgress(`✅ Saved: ${filename}`)
              } catch (err) {
                errorCount++
                console.error(
                  `❌ Error processing ${mediaPageUrl}: ${err.message}`
                )
              } finally {
                if (!workerPage.isClosed()) await workerPage.close()
              }

              await randomDelay()
            })
        )
      )

      const nextHref = await page
        .$eval('a[rel="next"]', (el) => el?.href)
        .catch(() => null)
      if (nextHref) {
        const baseUrl = new URL(url)
        url = new URL(nextHref, baseUrl).href
        // console.log(`➡️ Next page found: ${url}`)
      } else {
        // console.log(`🏁 No more pages.`)
        break
      }
    }
  } finally {
    readline.clearLine(process.stdout, 0)
    readline.cursorTo(process.stdout, 0)
    console.log(getCompletionLine())

    await page.close()
  }

  return newestDateSeen
}

;(async () => {
  let inputUrl = process.argv[2]
  if (!inputUrl || !inputUrl.includes('/category/'))
    return console.error('⚠️  Usage: node milkmaid.js <gallery-url>')

  inputUrl = inputUrl.replace(/&acs=[^&]+/i, '')

  const categoryId = inputUrl.match(/category\/?(\d+)/)?.[1]
  if (!categoryId) return console.error('❌ Invalid category URL')

  const browser = await puppeteer.launch({
    headless: 'new',
    executablePath: executablePath(),
    args: ['--no-sandbox'],
  })

  const tempPage = await createScraperPage(browser, {
    site: 'stufferdb',
    interceptMedia: true,
  })
  await tempPage.goto(inputUrl, { waitUntil: 'domcontentloaded' })

  loadVisualHashCache()

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

  const plainUrl = `https://stufferdb.com/index?/category/${categoryId}`
  const acsUrl = `${plainUrl}&acs=${modelName}`

  console.log('🔍 Prefetching total counts...')
  const [acsCount, plainCount] = await Promise.all([
    fetchStufferDBTotalCount(browser, acsUrl),
    fetchStufferDBTotalCount(browser, plainUrl),
  ])

  global.totalSearchTotal = acsCount + plainCount
  console.log(`📊 Combined media total: ${global.totalSearchTotal}`)

  console.log(`💦 Starting scrape for ${modelName}`)

  let newest1 = await scrapeGallery(
    browser,
    acsUrl,
    modelName,
    folders,
    lastChecked
  )
  console.log('✅ ACS scrape finished')

  console.log('🔁 Now starting PLAIN scrape')
  let newest2 = await scrapeGallery(
    browser,
    plainUrl,
    modelName,
    folders,
    lastChecked
  )

  console.log('🧮 Both scrapes complete')

  const newest = [newest1, newest2].filter(Boolean).sort().pop()
  if (newest) {
    lastCheckedMap[rawName] = newest.toISOString()
    fs.writeFileSync(lastCheckedPath, JSON.stringify(lastCheckedMap, null, 2))
  }

  const leftoverGifs = fs
    .readdirSync(incompleteGifDir)
    .filter((f) => f.endsWith('.gif'))
  for (const gif of leftoverGifs) {
    const tmpPath = path.join(incompleteGifDir, gif)
    const mp4Path = path.join(folders.webm, gif.replace(/\.gif$/, '.mp4'))
    gifsToConvert.push({ tmpPath, mp4Path, filename: gif })
  }

  console.log(`🚜 Converting gifs: ${gifsToConvert.length}`)
  const filteredGifs = gifsToConvert.filter(({ mp4Path }) => {
    const mp4Name = path.basename(mp4Path)
    const isKnown = knownFilenames.has(mp4Name) || skippedFilenames.has(mp4Name)
    if (isKnown) {
      console.log(
        `🚫 Skipping gif conversion (already known or failed): ${mp4Name}`
      )
    }
    return !isKnown
  })

  for (const { tmpPath, mp4Path, filename } of filteredGifs) {
    try {
      if (fs.existsSync(mp4Path)) {
        console.log(`⚠️ MP4 already exists, skipping conversion: ${mp4Path}`)
        continue
      }

      await convertGifToMp4(tmpPath, mp4Path)

      knownHashes.add(
        createHash('md5').update(fs.readFileSync(mp4Path)).digest('hex')
      )
      knownFilenames.add(path.basename(mp4Path))

      successCount++
      console.log(`🎞️ Converted: ${mp4Path}`)

      if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
    } catch (err) {
      console.error(`❌ Conversion failed for ${filename}`)
      console.error(err)
    }
  }

  console.log(`🐢 Lazy downloading videos: ${lazyVideoQueue.length}`)
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
      `🐷 Lazy stuffing: ${percent}% (${(lazyBytesDownloaded / 1024 / 1024).toFixed(2)} MB)\n`
    )
  }

  await Promise.all(
    lazyVideoQueue.map(
      ({ url, path: finalPath, tmpPath, filename, uploadedDate }, i) =>
        lazyLimit(async () => {
          if (knownFilenames.has(filename) || fs.existsSync(finalPath)) {
            duplicateCount++
            return console.log(`♻️ Lazy dupe (pre-download): ${filename}`)
          }

          let lastProgressLine = ''
          let lastDraw = Date.now()

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
                        console.log(
                          `⬇️ ${percent}% @ ${speed} KB/s (${chunk.length} bytes)`
                        )

                        lastDraw = now
                      }
                    }
                  )

            const hash = createHash('md5').update(buffer).digest('hex')
            if (knownHashes.has(hash)) {
              duplicateCount++
              return console.log(`♻️ Lazy dupe: ${filename}`)
            }

            fs.writeFileSync(finalPath, buffer)
            if (uploadedDate) {
              const ts = uploadedDate.getTime() / 1000
              fs.utimesSync(finalPath, ts, ts)
            }
            knownHashes.add(hash)
            knownFilenames.add(filename)
            console.log(`✅ Saved lazy video: ${filename}`)
            if (tmpPath && fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
          } catch (err) {
            console.warn(`❌ Lazy failed: ${filename} - ${err.message}`)
          }
          await sleep(3000)
        })
    )
  )

  // const modelHashPathUpdate = path.join(folders.base, 'hashes.json')
  fs.writeFileSync(modelHashPath, JSON.stringify([...knownHashes]))
  const logPath = path.join(folders.base, 'log.txt')
  fs.writeFileSync(
    logPath,
    `Model: ${modelName}\nTotal: ${totalCount}\nSaved: ${successCount}\nDupes: ${duplicateCount}\nErrors: ${errorCount}`
  )
  await browser.close()

  saveVisualHashCache()
  fs.writeFileSync(modelHashPath, JSON.stringify([...knownHashes]))

  console.log(
    `🎉 Done: ${successCount} saved, ${duplicateCount} dupes, ${errorCount} errors`
  )
})()

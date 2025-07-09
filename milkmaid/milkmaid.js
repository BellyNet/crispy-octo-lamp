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

const { bannerMilkmaid } = require('../banners.js') // adjust path if needed
bannerMilkmaid()
const {
  logProgress,
  logLazyDownload,
  logGifConversion,
  getCompletionLine,
} = require('../stuffinglogger')

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
const datasetDir = path.join(rootDir, 'dataset')
const tmpDir = path.join(rootDir, 'tmp')
const lastCheckedPath = path.join(rootDir, 'lastChecked.json')

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

  const folders = ['images', 'webm', 'tags', 'captions', 'dupes']
  for (const folder of folders)
    fs.mkdirSync(path.join(base, folder), { recursive: true })
  return {
    base,
    images: path.join(base, 'images'),
    webm: path.join(base, 'webm'),
    dupes: path.join(base, 'dupes'),
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
            onProgress(percent, speed)
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

async function scrapeGallery(browser, url, modelName, folders, lastChecked) {
  const { base, images, webm, dupes } = folders
  let newestDateSeen = lastChecked

  const page = await browser.newPage()
  await page.setUserAgent('Mozilla/5.0 ... Safari/537.36')
  await page.setViewport({ width: 1280, height: 800 })

  try {
    while (url) {
      await page.goto(url, { waitUntil: 'domcontentloaded' })

      const urls = await page.$$eval('a[href^="picture?/"]', (links) => [
        ...new Set(links.map((l) => l.href)),
      ])

      let completed = 0
      const total = urls.length

      console.log(
        `📸 ${modelName} - ${url.includes('&acs=') ? '[ACS]' : '[PLAIN]'} - ${urls.length} media links`
      )

      process.stdout.write('\n') // Reserve one lines
      logProgress(0, total)

      function logAndProgress(message) {
        // Move to piggy bar and clear it
        process.stdout.write(ansiEscapes.cursorTo(0, process.stdout.rows - 1))
        readline.clearLine(process.stdout, 0)

        // Now print the message — THIS pushes everything up by 1
        console.log(message)

        // Redraw the piggy bar at the new bottom
        logProgress(completed++, total)
      }

      await Promise.all(
        urls.map(
          async (mediaPageUrl) =>
            await limit(async () => {
              totalCount++
              const workerPage = await browser.newPage()
              await workerPage.setUserAgent('Mozilla/5.0 ... Safari/537.36')
              await workerPage.setRequestInterception(true)

              workerPage.on('request', (req) => {
                const type = req.resourceType()
                if (['media', 'other'].includes(type)) {
                  req.abort()
                } else {
                  req.continue()
                }
              })

              await workerPage.setViewport({ width: 1280, height: 800 })

              try {
                await workerPage.goto(mediaPageUrl, {
                  waitUntil: 'domcontentloaded',
                  timeout: 20000,
                })

                const uploadedDate = await workerPage.evaluate(() => {
                  const meta = document.querySelector('.imageInfo small')
                  const text = meta?.textContent || ''
                  const match = text.match(/\d{2}\/\d{2}\/\d{4}/)
                  if (!match) return null
                  const [day, month, year] = match[0].split('/')
                  return new Date(`${year}-${month}-${day}`)
                })

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
                const filename = decodeURIComponent(
                  path.basename(parsed.pathname).split('?')[0]
                )
                const ext = path.extname(filename).toLowerCase()
                const tmpPath = path.join(tmpDir, filename)
                const buffer = await downloadBufferWithProgress(mediaUrl)
                const hash = createHash('md5').update(buffer).digest('hex')

                if (knownHashes.has(hash)) {
                  fs.writeFileSync(path.join(dupes, filename), buffer)
                  duplicateCount++
                  return logAndProgress(`♻️ Visual dupe: ${filename}`)
                }

                if (ext === '.gif') {
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
                    gifsToConvert.push({
                      tmpPath,
                      mp4Path: path.join(webm, mp4Name),
                      filename,
                    })
                    return logAndProgress(logGifConversion(completed))
                  } else {
                    const stillPath = path.join(images, filename)
                    fs.writeFileSync(stillPath, buffer)
                    knownHashes.add(hash)
                    knownFilenames.add(filename)
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
                  fs.writeFileSync(tmpPath, buffer)
                  lazyVideoQueue.push({
                    url: mediaUrl,
                    path: finalPath,
                    tmpPath,
                    filename,
                  })
                  return logAndProgress(logLazyDownload(completed))
                }

                if (knownFilenames.has(filename)) {
                  duplicateCount++
                  return logAndProgress(`🔁 Existing filename: ${filename}`)
                }

                const finalPath = path.join(images, filename)
                fs.writeFileSync(finalPath, buffer)
                knownHashes.add(hash)
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
        console.log(`➡️ Next page found: ${url}`)
      } else {
        console.log(`🏁 No more pages.`)
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
  const inputUrl = process.argv[2]
  if (!inputUrl || !inputUrl.includes('/category/'))
    return console.error('⚠️  Usage: node milkmaid.js <gallery-url>')

  const categoryId = inputUrl.match(/category\/?(\d+)/)?.[1]
  if (!categoryId) return console.error('❌ Invalid category URL')

  const browser = await puppeteer.launch({
    headless: 'new',
    executablePath: executablePath(),
    args: ['--no-sandbox'],
  })

  const tempPage = await browser.newPage()
  await tempPage.setUserAgent('Mozilla/5.0 ... Safari/537.36')
  await tempPage.setViewport({ width: 1280, height: 800 })
  await tempPage.goto(inputUrl, { waitUntil: 'domcontentloaded' })

  const modelName = await tempPage
    .evaluate(() => {
      const h2 = document.querySelector('.titrePage h2')
      const anchors = h2?.querySelectorAll('a')
      return anchors?.[anchors.length - 1]?.textContent?.trim() || 'unknown_cow'
    })
    .then((n) => n.replace(/\W+/g, '_').toLowerCase())

  const folders = createModelFolders(modelName)

  const hashFile = path.join(folders.base, 'hashes.json')
  if (fs.existsSync(hashFile)) {
    try {
      JSON.parse(fs.readFileSync(hashFile)).forEach((h) => knownHashes.add(h))
    } catch (e) {
      console.warn('⚠️ Failed to load hash cache:', e.message)
    }
  }

  for (const folder of [folders.images, folders.webm, folders.dupes]) {
    if (fs.existsSync(folder)) {
      fs.readdirSync(folder).forEach((f) => knownFilenames.add(f))
    }
  }

  const lastChecked = lastCheckedMap[modelName]
    ? new Date(lastCheckedMap[modelName])
    : null

  const acsUrl = `https://stufferdb.com/index?/category/${categoryId}&acs=${modelName}`
  const plainUrl = `https://stufferdb.com/index?/category/${categoryId}`

  console.log(`💦 Starting scrape for ${modelName}`)

  let newest1 = await scrapeGallery(
    browser,
    acsUrl,
    modelName,
    folders,
    lastChecked
  )

  console.log(`🔄 ACS scrape done. Now checking plain category URL...`)

  let newest2 = await scrapeGallery(
    browser,
    plainUrl,
    modelName,
    folders,
    lastChecked
  )

  const newest = [newest1, newest2].filter(Boolean).sort().pop()
  if (newest) {
    lastCheckedMap[modelName] = newest.toISOString()
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
  await Promise.all(
    lazyVideoQueue.map(({ url, path: finalPath, tmpPath, filename }, i) =>
      lazyLimit(async () => {
        console.log(
          `⏳ (${i + 1}/${lazyVideoQueue.length}) Downloading: ${filename}`
        )
        try {
          const buffer =
            tmpPath && fs.existsSync(tmpPath)
              ? fs.readFileSync(tmpPath)
              : await downloadBufferWithProgress(url)

          const hash = createHash('md5').update(buffer).digest('hex')
          if (knownHashes.has(hash)) {
            fs.writeFileSync(path.join(folders.dupes, filename), buffer)
            duplicateCount++
            return console.log(`♻️ Lazy dupe: ${filename}`)
          }

          fs.writeFileSync(finalPath, buffer)
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

  fs.writeFileSync(hashFile, JSON.stringify([...knownHashes]))
  const logPath = path.join(folders.base, 'log.txt')
  fs.writeFileSync(
    logPath,
    `Model: ${modelName}\nTotal: ${totalCount}\nSaved: ${successCount}\nDupes: ${duplicateCount}\nErrors: ${errorCount}`
  )
  await browser.close()

  console.log(
    `🎉 Done: ${successCount} saved, ${duplicateCount} dupes, ${errorCount} errors`
  )
})()

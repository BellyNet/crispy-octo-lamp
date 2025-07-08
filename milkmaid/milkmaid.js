const { executablePath } = require('puppeteer')
const puppeteer = require('puppeteer')
const fs = require('fs')
const path = require('path')
const { exec } = require('child_process')
const { createHash } = require('crypto')
const https = require('https')
const http = require('http')
const pLimit = require('p-limit')
const limit = pLimit(4)
const lazyLimit = pLimit(2)

let knownHashes = new Set()
const fatLabels = [
  'fat',
  'plush',
  'softbelly',
  'overfed',
  'stuffed',
  'heavy',
  'overhang',
  'round',
  'fullfigure',
  'bloated',
]

const gifsToConvert = []
const lazyVideoQueue = []
let totalCount = 0,
  duplicateCount = 0,
  errorCount = 0,
  successCount = 0
let imageCounter = 1

const sleep = (ms) => new Promise((res) => setTimeout(res, ms))
const randomDelay = () => sleep(Math.floor(Math.random() * 1200) + 300)

function createModelFolders(modelName) {
  const base = path.join(__dirname, 'dataset', modelName)
  const folders = ['images', 'webm', 'tags', 'captions']
  for (const folder of folders)
    fs.mkdirSync(path.join(base, folder), { recursive: true })
  return {
    base,
    images: path.join(base, 'images'),
    webm: path.join(base, 'webm'),
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
    exec(cmd, (err) => (err ? reject(err) : resolve()))
  })
}

;(async () => {
  const startUrl = process.argv[2]
  if (!startUrl)
    return console.error('⚠️  Usage: node milkmaid.js <gallery-url>')

  const browser = await puppeteer.launch({
    headless: 'new',
    executablePath: executablePath(),
    args: ['--no-sandbox'],
  })

  const page = await browser.newPage()
  await page.setUserAgent('Mozilla/5.0 ... Safari/537.36')
  await page.setViewport({ width: 1280, height: 800 })
  await page.goto(startUrl, { waitUntil: 'domcontentloaded' })

  const modelName = await page
    .evaluate(() => {
      const h2 = document.querySelector('.titrePage h2')
      const anchors = h2?.querySelectorAll('a')
      return anchors?.[anchors.length - 1]?.textContent?.trim() || 'unknown_cow'
    })
    .then((n) => n.replace(/\W+/g, '_').toLowerCase())

  const {
    base,
    images: imageFolder,
    webm: webmFolder,
  } = createModelFolders(modelName)

  const hashFile = path.join(base, 'hashes.json')
  if (fs.existsSync(hashFile)) {
    try {
      knownHashes = new Set(JSON.parse(fs.readFileSync(hashFile, 'utf-8')))
    } catch (e) {
      console.warn('⚠️ Failed to load hash cache:', e.message)
    }
  }

  const urls = await page.$$eval('a[href^="picture?/"]', (links) => [
    ...new Set(links.map((l) => l.href)),
  ])
  console.log(`🔍 Found ${urls.length} full-res page links`)

  await Promise.all(
    urls.map((mediaPageUrl) =>
      limit(async () => {
        totalCount++
        const workerPage = await browser.newPage()
        await workerPage.setUserAgent('Mozilla/5.0 ... Safari/537.36')
        await workerPage.setViewport({ width: 1280, height: 800 })

        try {
          await workerPage.goto(mediaPageUrl, {
            waitUntil: 'domcontentloaded',
            timeout: 20000,
          })
          const mediaUrl = await workerPage.evaluate(() => {
            const video = document.querySelector('video.vjs-tech[src]')
            const img = document.querySelector('#theMainImage')
            return video?.src || img?.src || null
          })
          await workerPage.close()
          if (!mediaUrl) return console.log(`💤 No media on ${mediaPageUrl}`)

          const ext = path
            .extname(new URL(mediaUrl).pathname)
            .split('?')[0]
            .toLowerCase()
          const label = fatLabels[Math.floor(Math.random() * fatLabels.length)]
          const padded = String(imageCounter++).padStart(3, '0')
          const filename = `${modelName}_${label}-${padded}${ext}`
          const tmpPath = path.join(__dirname, 'tmp', filename)
          const finalPath = path.join(
            ext === '.gif' || ext === '.mp4' ? webmFolder : imageFolder,
            filename
          )

          fs.mkdirSync(path.dirname(tmpPath), { recursive: true })

          if (ext === '.gif') {
            gifsToConvert.push({
              url: mediaUrl,
              tmpPath,
              mp4Path: finalPath.replace(/\.gif$/, '.mp4'),
            })
            console.log(`🕓 Queued gif: ${filename}`)
          } else if (ext === '.mp4') {
            lazyVideoQueue.push({ url: mediaUrl, path: finalPath, filename })
            console.log(`🕓 Queued lazy video: ${filename}`)
          } else {
            const buffer = await downloadBufferWithProgress(mediaUrl)
            const hash = createHash('md5').update(buffer).digest('hex')
            if (knownHashes.has(hash)) {
              duplicateCount++
              return console.log(`🔁 Skipping duplicate: ${filename}`)
            }
            fs.writeFileSync(tmpPath, buffer)
            fs.renameSync(tmpPath, finalPath)
            knownHashes.add(hash)
            successCount++
            console.log(`✅ Saved: ${finalPath}`)
          }
        } catch (err) {
          errorCount++
          console.error(`❌ Error processing ${mediaPageUrl}: ${err.message}`)
        }
        await randomDelay()
      })
    )
  )

  console.log(`🚜 Starting gif-to-mp4 conversions: ${gifsToConvert.length}`)
  for (const { url, tmpPath, mp4Path } of gifsToConvert) {
    try {
      const buffer = await downloadBufferWithProgress(url)
      const hash = createHash('md5').update(buffer).digest('hex')
      if (knownHashes.has(hash)) {
        duplicateCount++
        console.log(`🔁 Skipping duplicate gif: ${mp4Path}`)
        continue
      }
      fs.writeFileSync(tmpPath, buffer)
      await convertGifToMp4(tmpPath, mp4Path)
      fs.unlinkSync(tmpPath)
      knownHashes.add(hash)
      console.log(`🎞️ Converted: ${mp4Path}`)
    } catch (err) {
      console.error(`❌ Conversion failed for ${tmpPath}: ${err.message}`)
    }
  }

  console.log(`🐢 Processing lazy video queue: ${lazyVideoQueue.length}`)
  await Promise.all(
    lazyVideoQueue.map(({ url, path: finalPath, filename }, i) =>
      lazyLimit(async () => {
        console.log(
          `⏳ (${i + 1}/${lazyVideoQueue.length}) Downloading: ${filename}`
        )
        try {
          const buffer = await downloadBufferWithProgress(
            url,
            (percent, speed) => {
              process.stdout.write(`  ↪ ${percent}% @ ${speed} KB/s       \r`)
            }
          )
          const hash = createHash('md5').update(buffer).digest('hex')
          if (knownHashes.has(hash)) {
            duplicateCount++
            process.stdout.write(' '.repeat(40) + '\r')
            console.log(`🔁 Skipping duplicate: ${filename}`)
            return
          }
          fs.writeFileSync(finalPath, buffer)
          knownHashes.add(hash)
          process.stdout.write(' '.repeat(40) + '\r')
          console.log(`✅ Saved: ${filename}`)
        } catch (err) {
          console.warn(
            `❌ Lazy download failed for ${filename}: ${err.message}`
          )
        }
        await sleep(3000)
      })
    )
  )

  const logPath = path.join(base, 'log.txt')
  const logContent = `
Model: ${modelName}
-------------------------------
Total scanned:        ${totalCount}
Duplicates skipped:   ${duplicateCount}
Downloaded:           ${successCount}
Errors:               ${errorCount}
GIFs queued:          ${gifsToConvert.length}
Lazy videos queued:   ${lazyVideoQueue.length}
`.trim()
  fs.writeFileSync(logPath, logContent)
  fs.writeFileSync(hashFile, JSON.stringify([...knownHashes]))
  await browser.close()
  console.log(
    `🎉 Gallery complete: ${imageCounter - 1} images milked from ${modelName}`
  )
})()

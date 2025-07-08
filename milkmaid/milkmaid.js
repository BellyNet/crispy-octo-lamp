const puppeteer = require('puppeteer')
const fs = require('fs')
const path = require('path')
const { exec } = require('child_process')
const { createHash } = require('crypto')
const https = require('https')
const http = require('http')

const knownHashes = new Set()
const fatLabels = [
  'fat',
  'luscious',
  'waddler',
  'milkme',
  'snackload',
  'stuffed',
  'overflowing',
]
const gifsToConvert = []
let totalCount = 0
let duplicateCount = 0
let errorCount = 0
let successCount = 0
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const randomDelay = () => sleep(Math.floor(Math.random() * 1200) + 300)

function createModelFolders(modelName) {
  const base = path.join(__dirname, 'dataset', modelName)
  const folders = ['images', 'webm', 'tags', 'captions']
  for (const folder of folders) {
    fs.mkdirSync(path.join(base, folder), { recursive: true })
  }
  return {
    images: path.join(base, 'images'),
    webm: path.join(base, 'webm'),
  }
}

function downloadBuffer(mediaUrl) {
  const proto = mediaUrl.startsWith('https') ? https : http
  return new Promise((resolve, reject) => {
    proto
      .get(mediaUrl, (res) => {
        if (res.statusCode !== 200)
          return reject(new Error(`HTTP ${res.statusCode}`))
        const chunks = []
        res.on('data', (chunk) => chunks.push(chunk))
        res.on('end', () => resolve(Buffer.concat(chunks)))
      })
      .on('error', reject)
  })
}

function convertGifToMp4(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const cmd = `ffmpeg -y -i "${inputPath}" -movflags faststart -pix_fmt yuv420p -vf "scale=trunc(iw/2)*2:trunc(ih/2)*2" "${outputPath}"`
    exec(cmd, (err) => {
      if (err) return reject(err)
      resolve()
    })
  })
}

;(async () => {
  const startUrl = process.argv[2]
  if (!startUrl)
    return console.error('⚠️  Usage: node milkmaid.js <gallery-url>')

  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--incognito', '--no-sandbox'],
  })

  const context = await browser.createIncognitoBrowserContext()
  const page = await context.newPage()
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36'
  )
  await page.setViewport({ width: 1280, height: 800 })

  const subPage = await context.newPage()
  await subPage.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36'
  )
  await subPage.setViewport({ width: 1280, height: 800 })

  await page.goto(startUrl, { waitUntil: 'networkidle2' })

  const modelName = await page
    .evaluate(() => {
      const h2 = document.querySelector('.titrePage h2')
      const anchors = h2?.querySelectorAll('a')
      const lastAnchor = anchors?.[anchors.length - 1]
      return lastAnchor?.textContent?.trim() || 'unknown_cow'
    })
    .then((name) => name.replace(/\W+/g, '_').toLowerCase())

  const { images: imageFolder, webm: webmFolder } =
    createModelFolders(modelName)

  const urls = await page.$$eval('a[href^="picture?/"]', (links) => [
    ...new Set(links.map((link) => link.href)),
  ])

  console.log(`🔍 Found ${urls.length} full-res page links`)

  let imageCounter = 1

  for (const mediaPageUrl of urls) {
    totalCount++

    console.log(`🥛 Milking: ${mediaPageUrl}`)
    await subPage.goto(mediaPageUrl, { waitUntil: 'networkidle2', timeout: 0 })

    // do your milking...

    await randomDelay()

    // Try to click the play button if it's there (to reveal mp4 URL)
    try {
      const playBtn = await subPage.$('button.vjs-big-play-button')
      if (playBtn) {
        await playBtn.click()
        await subPage.waitForSelector('video.vjs-tech[src]', { timeout: 5000 })
      }
    } catch (err) {
      console.warn(
        '⚠️ No video play button or timed out waiting for video to load'
      )
    }

    // Now grab either the image or video
    const mediaUrl = await subPage.evaluate(() => {
      const video = document.querySelector('video.vjs-tech[src]')
      if (video?.src) return video.src

      const img = document.querySelector('#theMainImage')
      if (img?.src?.match(/\.(gif|jpg|jpeg|png)$/i)) return img.src

      return null
    })

    if (!mediaUrl) {
      console.log(`💤 No media on ${mediaPageUrl}`)
      await randomDelay()
      await subPage.close()
      continue
    }

    try {
      const ext = path
        .extname(new URL(mediaUrl).pathname)
        .split('?')[0]
        .toLowerCase()
      const label = fatLabels[Math.floor(Math.random() * fatLabels.length)]
      const padded = String(imageCounter).padStart(3, '0')
      const filename = `${modelName}_${label}-${padded}${ext}`
      const tmpPath = path.join(__dirname, 'tmp', filename)
      const finalPath = path.join(
        ext === '.gif' ? webmFolder : imageFolder,
        filename
      )

      fs.mkdirSync(path.dirname(tmpPath), { recursive: true })
      const buffer = await downloadBuffer(mediaUrl)
      const hash = createHash('md5').update(buffer).digest('hex')

      if (knownHashes.has(hash)) {
        duplicateCount++
        console.log(`🔁 Skipping duplicate: ${filename}`)
      } else {
        knownHashes.add(hash)
        fs.writeFileSync(tmpPath, buffer)

        if (ext === '.gif' && buffer.includes(Buffer.from('NETSCAPE2.0'))) {
          const mp4Path = path.join(
            webmFolder,
            `${modelName}_${label}-${padded}.mp4`
          )
          gifsToConvert.push({ tmpPath, mp4Path })
          console.log(`🧃 Queued gif for conversion: ${tmpPath}`)
        } else {
          fs.renameSync(tmpPath, finalPath)
          console.log(`✅ Saved: ${finalPath}`)
        }

        successCount++
        imageCounter++
      }
    } catch (err) {
      errorCount++
      console.error(`❌ Download error: ${err.message}`)
    } finally {
      await subPage.close()
    }
  }

  console.log(
    `🚜 Starting gif-to-mp4 conversions: ${gifsToConvert.length} total`
  )
  for (const { tmpPath, mp4Path } of gifsToConvert) {
    try {
      await convertGifToMp4(tmpPath, mp4Path)
      fs.unlinkSync(tmpPath)
      console.log(`🎞️ Converted: ${mp4Path}`)
    } catch (err) {
      console.error(`❌ Conversion failed for ${tmpPath}: ${err.message}`)
    }
  }

  const logPath = path.join(imageFolder, '..', 'log.txt')
  const logContent = `
                Model: ${modelName}
                -------------------------------
                Total scanned:       ${totalCount}
                Duplicates skipped:  ${duplicateCount}
                Downloaded:          ${successCount}
                Errors:              ${errorCount}
                  `.trim()

  fs.writeFileSync(logPath, logContent)
  console.log(`📄 Log saved to: ${logPath}`)

  await browser.close()
  console.log(
    `🎉 Gallery complete: ${imageCounter - 1} images milked from ${modelName}`
  )
})()

// milkmaid.js - Extended gif logic to detect single-frame gifs

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
let knownFilenames = new Set()
const gifsToConvert = []
const lazyVideoQueue = []
let totalCount = 0,
  duplicateCount = 0,
  errorCount = 0,
  successCount = 0

const sleep = (ms) => new Promise((res) => setTimeout(res, ms))
const randomDelay = () => sleep(Math.floor(Math.random() * 1200) + 300)

function createModelFolders(modelName) {
  const base = path.join(__dirname, 'dataset', modelName)
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
              downloadedBytes / 1024 / ((Date.now() - start) / 1000)
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

function getGifFrameCount(buffer) {
  return new Promise((resolve) => {
    const tmp = path.join(__dirname, 'tmp', `__framecheck_${Date.now()}.gif`)
    fs.writeFileSync(tmp, buffer)
    exec(`ffprobe -v error -count_frames -select_streams v:0 -show_entries stream=nb_read_frames -of csv=p=0 "${tmp}"`, (err, stdout) => {
      fs.unlinkSync(tmp)
      const frameCount = parseInt(stdout.trim(), 10)
      resolve(isNaN(frameCount) ? 1 : frameCount)
    })
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

  const modelName = await page.evaluate(() => {
    const h2 = document.querySelector('.titrePage h2')
    const anchors = h2?.querySelectorAll('a')
    return anchors?.[anchors.length - 1]?.textContent?.trim() || 'unknown_cow'
  }).then((n) => n.replace(/\W+/g, '_').toLowerCase())

  const { base, images: imageFolder, webm: webmFolder, dupes: dupeFolder } = createModelFolders(modelName)

  const hashFile = path.join(base, 'hashes.json')
  if (fs.existsSync(hashFile)) {
    try {
      knownHashes = new Set(JSON.parse(fs.readFileSync(hashFile, 'utf-8')))
    } catch (e) {
      console.warn('⚠️ Failed to load hash cache:', e.message)
    }
  }

  const folders = [imageFolder, webmFolder, dupeFolder]
  folders.forEach((f) => {
    if (fs.existsSync(f)) {
      fs.readdirSync(f).forEach((file) => knownFilenames.add(file))
    }
  })

  const urls = await page.$$eval('a[href^="picture?/"]', (links) => [...new Set(links.map((l) => l.href))])
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

          const parsed = new URL(mediaUrl)
          const filename = decodeURIComponent(path.basename(parsed.pathname).split('?')[0])
          const ext = path.extname(filename).toLowerCase()

          if (knownFilenames.has(filename)) {
            duplicateCount++
            return console.log(`🔁 Skipping existing filename: ${filename}`)
          }

          const tmpPath = path.join(__dirname, 'tmp', filename)
          const buffer = await downloadBufferWithProgress(mediaUrl)
          const hash = createHash('md5').update(buffer).digest('hex')
          if (knownHashes.has(hash)) {
            fs.writeFileSync(path.join(dupeFolder, filename), buffer)
            duplicateCount++
            return console.log(`♻️ Visual dupe saved to dupes/: ${filename}`)
          }

          if (ext === '.gif') {
            const frameCount = await getGifFrameCount(buffer)
            if (frameCount > 1) {
              const mp4Path = path.join(webmFolder, filename.replace(/\.gif$/, '.mp4'))
              fs.writeFileSync(tmpPath, buffer)
              gifsToConvert.push({ url: mediaUrl, tmpPath, mp4Path, filename })
              return console.log(`🕓 Queued animated gif: ${filename}`)
            } else {
              const stillPath = path.join(imageFolder, filename)
              fs.writeFileSync(stillPath, buffer)
              knownHashes.add(hash)
              knownFilenames.add(filename)
              successCount++
              return console.log(`🖼️ Saved still gif: ${filename}`)
            }
          }

          if (ext === '.mp4') {
            const finalPath = path.join(webmFolder, filename)
            lazyVideoQueue.push({ url: mediaUrl, path: finalPath, filename })
            return console.log(`🕓 Queued lazy video: ${filename}`)
          }

          const finalPath = path.join(imageFolder, filename)
          fs.writeFileSync(finalPath, buffer)
          knownHashes.add(hash)
          knownFilenames.add(filename)
          successCount++
          console.log(`✅ Saved: ${finalPath}`)
        } catch (err) {
          errorCount++
          console.error(`❌ Error processing ${mediaPageUrl}: ${err.message}`)
        }
        await randomDelay()
      })
    )
  )

  console.log(`🚜 Converting gifs: ${gifsToConvert.length}`)
  for (const { tmpPath, mp4Path, filename } of gifsToConvert) {
    try {
      await convertGifToMp4(tmpPath, mp4Path)
      fs.unlinkSync(tmpPath)
      knownHashes.add(createHash('md5').update(fs.readFileSync(mp4Path)).digest('hex'))
      knownFilenames.add(path.basename(mp4Path))
      successCount++
      console.log(`🎞️ Converted: ${mp4Path}`)
    } catch (err) {
      console.error(`❌ Conversion failed for ${filename}: ${err.message}`)
    }
  }

  console.log(`🐢 Lazy downloading videos: ${lazyVideoQueue.length}`)
  await Promise.all(
    lazyVideoQueue.map(({ url, path: finalPath, filename }, i) =>
      lazyLimit(async () => {
        console.log(`⏳ (${i + 1}/${lazyVideoQueue.length}) Downloading: ${filename}`)
        try {
          const buffer = await downloadBufferWithProgress(url, (percent, speed) => {
            process.stdout.write(`  ↪ ${percent}% @ ${speed} KB/s       \r`)
          })
          const hash = createHash('md5').update(buffer).digest('hex')
          if (knownHashes.has(hash)) {
            fs.writeFileSync(path.join(dupeFolder, filename), buffer)
            duplicateCount++
            process.stdout.write(' '.repeat(40) + '\r')
            return console.log(`♻️ Lazy dupe saved: ${filename}`)
          }
          fs.writeFileSync(finalPath, buffer)
          knownHashes.add(hash)
          knownFilenames.add(filename)
          process.stdout.write(' '.repeat(40) + '\r')
          console.log(`✅ Saved lazy video: ${filename}`)
        } catch (err) {
          console.warn(`❌ Lazy video failed: ${filename} - ${err.message}`)
        }
        await sleep(3000)
      })
    )
  )

  fs.writeFileSync(hashFile, JSON.stringify([...knownHashes]))
  const logPath = path.join(base, 'log.txt')
  fs.writeFileSync(logPath, `Model: ${modelName}\nTotal: ${totalCount}\nSaved: ${successCount}\nDupes: ${duplicateCount}\nErrors: ${errorCount}`)
  await browser.close()
  console.log(`🎉 Gallery complete: ${successCount} saved, ${duplicateCount} dupes, ${errorCount} errors`)
})()
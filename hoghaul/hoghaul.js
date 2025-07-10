// hoghaul.js - Coomer scraper matching Milkmaid's behavior

const puppeteer = require('puppeteer')
const path = require('path')
const fs = require('fs')
const readline = require('readline')
const ansiEscapes = require('ansi-escapes')
const { createHash } = require('crypto')
const {
  logProgress,
  logLazyDownload,
  logGifConversion,
  getCompletionLine,
} = require('../stuffinglogger')
const pLimit = require('p-limit')

const limit = pLimit(6)
const lazyLimit = pLimit(4)

const rootDir = path.join(__dirname, '..')
const datasetDir = path.join(rootDir, 'dataset')
const tmpDir = path.join(__dirname, 'tmp')
const incompleteDir = path.join(__dirname, 'incomplete')
const incompleteGifDir = path.join(incompleteDir, 'gifs')
const incompleteVideoDir = path.join(incompleteDir, 'videos')
const lastCheckedPath = path.join(datasetDir, 'lastChecked.json')
const hashFile = path.join(datasetDir, 'hashes.json')

if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true })
if (!fs.existsSync(incompleteGifDir))
  fs.mkdirSync(incompleteGifDir, { recursive: true })
if (!fs.existsSync(incompleteVideoDir))
  fs.mkdirSync(incompleteVideoDir, { recursive: true })

function sanitize(name) {
  return name.replace(/[^a-z0-9_\-]/gi, '_').toLowerCase()
}

function createModelFolders(modelName) {
  const base = path.join(datasetDir, modelName)
  const folders = ['images', 'webm']
  for (const folder of folders)
    fs.mkdirSync(path.join(base, folder), { recursive: true })
  return {
    base,
    images: path.join(base, 'images'),
    webm: path.join(base, 'webm'),
  }
}

function downloadBufferWithProgress(url, onProgress) {
  const proto = url.startsWith('https') ? require('https') : require('http')
  return new Promise((resolve, reject) => {
    proto
      .get(url, (res) => {
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
    require('child_process').exec(cmd, (err) => (err ? reject(err) : resolve()))
  })
}

function getGifFrameCount(buffer) {
  return new Promise((resolve) => {
    const tmp = path.join(tmpDir, `__framecheck_${Date.now()}.gif`)
    fs.writeFileSync(tmp, buffer)
    require('child_process').exec(
      `ffprobe -v error -count_frames -select_streams v:0 -show_entries stream=nb_read_frames -of csv=p=0 "${tmp}"`,
      (err, stdout) => {
        fs.unlinkSync(tmp)
        const frameCount = parseInt(stdout.trim(), 10)
        resolve(isNaN(frameCount) ? 1 : frameCount)
      }
    )
  })
}

const knownHashes = new Set()
const knownFilenames = new Set()
const gifsToConvert = []
const lazyVideoQueue = []

if (fs.existsSync(hashFile)) {
  try {
    JSON.parse(fs.readFileSync(hashFile)).forEach((h) => knownHashes.add(h))
  } catch (e) {
    console.warn('⚠️ Failed to load hash cache:', e.message)
  }
}

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

// scrape logic begins here

async function scrapeCoomerUser(userUrl) {
  const browser = await puppeteer.launch({ headless: 'new' })
  const page = await browser.newPage()
  await page.goto(userUrl, { waitUntil: 'domcontentloaded' })

  const username = await page.$eval('div.site-section-user header h1', (el) =>
    el.textContent.trim()
  )

  const modelName = sanitize(username)
  const folders = createModelFolders(modelName)
  const lastChecked = lastCheckedMap[userUrl]
    ? new Date(lastCheckedMap[userUrl])
    : null
  let newestDateSeen = lastChecked

  const postLinks = new Set()
  let pageNum = 0

  while (true) {
    await page.goto(`${userUrl}?o=${pageNum * 50}`, {
      waitUntil: 'domcontentloaded',
    })
    const links = await page.$$eval('article.post-card a.fancy-link', (els) =>
      els.map((el) => el.href)
    )
    if (!links.length) break
    links.forEach((link) => postLinks.add(link))
    const hasNext = await page.$('a[rel="next"]')
    if (!hasNext) break
    pageNum++
  }

  const urls = Array.from(postLinks)
  let completed = 0
  process.stdout.write('test')
  logProgress(0, urls.length)

  await Promise.all(
    urls.map((link) =>
      limit(async () => {
        let taskCompleted = false
        function logAndProgress(msg) {
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

          const timeText = await page.$eval(
            'header.post__card-header time',
            (el) => el.getAttribute('datetime')
          )
          const uploadedDate = timeText ? new Date(timeText) : null

          if (lastChecked && uploadedDate && uploadedDate <= lastChecked) {
            return logAndProgress(
              `⏩ Skipping old post from ${uploadedDate.toISOString().split('T')[0]}`
            )
          }
          if (
            !newestDateSeen ||
            (uploadedDate && uploadedDate > newestDateSeen)
          ) {
            newestDateSeen = uploadedDate
          }

          const mediaUrls = await page.$$eval(
            'div.post__files img, div.post__files video source',
            (els) => els.map((el) => el.src || el.getAttribute('src'))
          )

          for (const mediaUrl of mediaUrls) {
            const url = mediaUrl.startsWith('http')
              ? mediaUrl
              : `https:${mediaUrl}`
            const filename = decodeURIComponent(
              path.basename(new URL(url).pathname).split('?')[0]
            )
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

  await Promise.all(
    lazyVideoQueue.map(({ url, path: finalPath, tmpPath, filename }, i) =>
      lazyLimit(async () => {
        console.log(logLazyDownload(i))
        try {
          const buffer =
            tmpPath && fs.existsSync(tmpPath)
              ? fs.readFileSync(tmpPath)
              : await downloadBufferWithProgress(url)
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

  fs.writeFileSync(hashFile, JSON.stringify([...knownHashes]))
  lastCheckedMap[userUrl] = newestDateSeen?.toISOString()
  fs.writeFileSync(lastCheckedPath, JSON.stringify(lastCheckedMap, null, 2))

  console.log(getCompletionLine())
  await browser.close()
}

// Run it
const target = process.argv[2]
if (!target || !target.includes('coomer.su')) {
  console.error(
    'Usage: node hoghaul.js https://coomer.su/onlyfans/user/<username>'
  )
  process.exit(1)
}
scrapeCoomerUser(target)

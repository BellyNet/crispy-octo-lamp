const puppeteer = require('puppeteer')
const fs = require('fs')
const path = require('path')
const { exec } = require('child_process')
const { createHash } = require('crypto')
const https = require('https')
const http = require('http')

const knownHashes = new Set()
const urls = [
  'https://stufferdb.com/picture?/616220/category/23051',
  'https://stufferdb.com/picture?/616200/category/23051',
  'https://stufferdb.com/picture?/616201/category/23051',
  'https://stufferdb.com/picture?/616202/category/23051',
  'https://stufferdb.com/picture?/616138/category/23051',
  'https://stufferdb.com/picture?/616129/category/23051',
  'https://stufferdb.com/picture?/616128/category/23051',
  'https://stufferdb.com/picture?/616095/category/23051',
  'https://stufferdb.com/picture?/616056/category/23051',
  'https://stufferdb.com/picture?/614834/category/23051',
  'https://stufferdb.com/picture?/614829/category/23051',
  'https://stufferdb.com/picture?/614807/category/23051',
  'https://stufferdb.com/picture?/614589/category/23051',
  'https://stufferdb.com/picture?/614584/category/23051',
  'https://stufferdb.com/picture?/614580/category/23051',
  'https://stufferdb.com/picture?/614496/category/23051',
  'https://stufferdb.com/picture?/614479/category/23051',
  'https://stufferdb.com/picture?/614474/category/23051',
  'https://stufferdb.com/picture?/614317/category/23051',
  'https://stufferdb.com/picture?/614318/category/23051',
  'https://stufferdb.com/picture?/614437/category/23051',
  'https://stufferdb.com/picture?/614438/category/23051',
  'https://stufferdb.com/picture?/614221/category/23051',
  'https://stufferdb.com/picture?/614222/category/23051',
  'https://stufferdb.com/picture?/614315/category/23051',
  'https://stufferdb.com/picture?/614316/category/23051',
]

// 💅 Create folders for each milkmaid
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

// 🎬 Convert animated gifs to smooth mp4 for thicc cinematic glory
async function convertGifToMp4(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const cmd = `ffmpeg -y -i "${inputPath}" -movflags faststart -pix_fmt yuv420p -vf "scale=trunc(iw/2)*2:trunc(ih/2)*2" "${outputPath}"`
    exec(cmd, (err) => {
      if (err) return reject(err)
      resolve()
    })
  })
}

;(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    args: ['--incognito', '--no-sandbox'],
  })
  const context = await browser.createIncognitoBrowserContext()
  const page = await context.newPage()
  let imageCounter = 1

  for (const url of urls) {
    try {
      console.log(`🥛 Approaching dairy barn: ${url}`)
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 0 })
      await page.waitForTimeout(5000)

      const username = await page.evaluate(() => {
        const catSection = document.querySelector('#Categories ul')
        if (!catSection) return 'unknown_cow'
        const firstLi = catSection.querySelector('li')
        const anchors = firstLi ? firstLi.querySelectorAll('a') : []
        return anchors.length
          ? anchors[anchors.length - 1].textContent.trim()
          : 'unknown_cow'
      })

      const { images: imageFolder, webm: webmFolder } =
        createModelFolders(username)

      await page.waitForSelector('#theMainImage, video source', {
        timeout: 10000,
      })

      const mediaUrl = await page.evaluate(() => {
        const img = document.querySelector('#theMainImage')
        if (img?.src?.match(/\.(gif|jpg|jpeg|png)$/i)) return img.src
        const vid = document.querySelector('video source')
        if (vid?.src) return vid.src
        return null
      })

      if (!mediaUrl) throw new Error('No media found on this chubby graze')

      const ext = path
        .extname(new URL(mediaUrl).pathname)
        .split('?')[0]
        .toLowerCase()
      const padded = imageCounter.toString().padStart(3, '0')

      // Random milkfat-flavored label
      const fatLabels = [
        'plump',
        'luscious',
        'waddler',
        'milkme',
        'snackload',
        'stuffed',
        'overflowing',
      ]
      const label = fatLabels[Math.floor(Math.random() * fatLabels.length)]
      const fileName = `${username}_${label}-${padded}${ext}`

      const tmpPath = path.join(__dirname, 'tmp', fileName)
      const targetPath = path.join(imageFolder, fileName)
      fs.mkdirSync(path.dirname(tmpPath), { recursive: true })

      const buffer = await new Promise((resolve, reject) => {
        const proto = mediaUrl.startsWith('https') ? https : http
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

      const hash = createHash('md5').update(buffer).digest('hex')
      if (knownHashes.has(hash)) {
        console.log(`🔁 Skipping duplicate jug shot: ${fileName}`)
        continue
      }
      knownHashes.add(hash)

      fs.writeFileSync(tmpPath, buffer)

      if (ext === '.gif') {
        const isAnimated = buffer.includes(Buffer.from('NETSCAPE2.0'))
        if (isAnimated) {
          const mp4Path = path.join(
            webmFolder,
            `${username}_${label}-${padded}.mp4`
          )
          console.log(`🎥 Milking motion: converting .gif to .mp4`)
          await convertGifToMp4(tmpPath, mp4Path)
          fs.unlinkSync(tmpPath)
        } else {
          fs.renameSync(tmpPath, targetPath)
          console.log(`🧊 Chilled .gif added to pantry: ${fileName}`)
        }
      } else {
        fs.renameSync(tmpPath, targetPath)
        console.log(`✅ Fat frame secured: ${fileName}`)
      }

      imageCounter++
    } catch (err) {
      console.error(`💩 Cow tripped at ${url}: ${err.message}`)
    }
  }

  await browser.close()
  console.log(`🐄 All thicc milking complete. Dataset full, udders heavy.`)
})()

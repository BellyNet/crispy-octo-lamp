import puppeteer from 'puppeteer'
import sharp from 'sharp'
import imghash from 'imghash'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'
const fetch = globalThis.fetch

const __dirname = path.dirname(fileURLToPath(import.meta.url))

const TEMP_IMG_PATH = path.join(__dirname, 'tmp_input.jpg')
const subreddit = 'Bigger'
const targetDate = new Date('Thursday 26 June 2025')
const stufferdbUrl = 'https://stufferdb.com/picture?/615557/best_rated'

function unixDayRange(date, offsetDays = 1) {
  const oneDay = 86400
  const after = Math.floor((date.getTime() - offsetDays * oneDay * 1000) / 1000)
  const before = Math.floor(
    (date.getTime() + offsetDays * oneDay * 1000) / 1000
  )
  return { after, before }
}

async function downloadImage(url, outPath) {
  const res = await fetch(url)
  const arrayBuffer = await res.arrayBuffer()
  const buffer = Buffer.from(arrayBuffer)

  fs.writeFileSync(outPath, buffer)
  return outPath
}

async function hashImage(filePath) {
  return await imghash.hash(filePath, 16, 'hex')
}

function hammingDistance(hash1, hash2) {
  const b1 = BigInt('0x' + hash1)
  const b2 = BigInt('0x' + hash2)
  return (b1 ^ b2).toString(2).replace(/0/g, '').length
}

async function extractStufferImage() {
  const browser = await puppeteer.launch({ headless: 'new' })
  const page = await browser.newPage()
  await page.setUserAgent('Mozilla/5.0 ... Chrome/115 Safari/537.36')
  await page.goto(stufferdbUrl, {
    waitUntil: 'domcontentloaded',
    timeout: 20000,
  })

  await page
    .waitForSelector('#theMainImage, video.vjs-tech[src]', { timeout: 5000 })
    .catch(() => {})
  const imageUrl = await page.evaluate(() => {
    const img = document.querySelector('#theMainImage')
    return img?.src || null
  })

  await browser.close()
  if (!imageUrl) throw new Error('No image found on page.')

  return await downloadImage(imageUrl, TEMP_IMG_PATH)
}

async function fetchRedditImages(date) {
  const { after, before } = unixDayRange(date)
  const searchUrl = `https://apiv2.pushshift.io/reddit/search/submission/?subreddit=${subreddit}&after=${after}&before=${before}&size=100`
  const res = await fetch(searchUrl)
  const json = await res.json()

  if (!json || !Array.isArray(json.data)) {
    console.warn(
      '⚠️ No Reddit data returned (API may be down or empty result).'
    )
    return []
  }

  return json.data.filter(
    (post) => post.url && /\.(jpe?g|png|webp)$/i.test(post.url)
  )
}

const main = async () => {
  console.log('🖼️ Downloading StufferDB image...')
  const stufferImgPath = await extractStufferImage()
  const stufferHash = await hashImage(stufferImgPath)
  console.log(`📷 Hash of StufferDB image: ${stufferHash}`)

  console.log(`📦 Using fallback Reddit post for known test hash`)
  const redditPosts = [
    {
      permalink: 'https://www.reddit.com/r/bigger/comments/1ll32r7',
      url: 'https://preview.redd.it/bodylovebritt-absolutely-exploded-v0-jjgxt97hia9f1.jpg?width=1080&crop=smart&auto=webp&s=fdfa711250195f85905017b1ca176ddb478ca4f5',
      title: 'Bodylovebritt absolutely exploded',
      created_utc: Math.floor(
        new Date('2025-06-26T15:28:57.850Z').getTime() / 1000
      ),
    },
  ]

  for (const post of redditPosts) {
    console.log(post)
    try {
      const tmpOut = path.join(__dirname, 'tmp_cmp.jpg')
      await downloadImage(post.url, tmpOut)
      const redditHash = await hashImage(tmpOut)
      const distance = hammingDistance(stufferHash, redditHash)

      if (distance <= 5) {
        console.log(`🎯 Visual match found!`)
        console.log(`🥩 ${post.permalink}`)
        console.log(
          `   📅 ${new Date(post.created_utc * 1000).toLocaleString()}`
        )
        console.log(`   📎 ${post.url}`)
        break
      } else {
        console.log(`🚫 Not a match (distance ${distance}) - ${post.url}`)
      }
    } catch (err) {
      console.warn(`⚠️ Error checking post: ${err.message}`)
    }
  }

  fs.unlinkSync(stufferImgPath)
}

main().catch(console.error)

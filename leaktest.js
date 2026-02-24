import puppeteer from 'puppeteer-extra'
import StealthPlugin from 'puppeteer-extra-plugin-stealth'
puppeteer.use(StealthPlugin())

const url = 'https://leakedzone.com/bbw_demetra/video/4860973'

const browser = await puppeteer.launch({ headless: false })
const [page] = await browser.pages()

await page.goto(url, { waitUntil: 'domcontentloaded' })

// Step 1: Click initial play icon
await page.waitForSelector('.play-icon', { timeout: 5000 })
await page.click('.play-icon')
console.log('👆 Clicked .play-icon')

// Step 2: Close popup if any
await new Promise((r) => setTimeout(r, 1500))
const pages = await browser.pages()
if (pages.length > 1) {
  const popup = pages.find((p) => p !== page)
  await popup.close()
  console.log('🧼 Closed popup window')
}

// Step 3: Close overlay ad buttons
await new Promise((r) => setTimeout(r, 2000))
await page.evaluate(() => {
  document.querySelectorAll('.close-ad').forEach((el) => el.click())
})
console.log('✖️ Closed overlay ads')

// Step 4: Wait and click the [aria-label="Skip"] ad button
console.log('⏳ Waiting for inline skip...')
await new Promise((r) => setTimeout(r, 5000))
const skipClicked = await page.evaluate(() => {
  const btn = document.querySelector('[aria-label="Skip"]')
  if (btn) {
    btn.click()
    return true
  }
  return false
})
console.log(skipClicked ? '⏭️ Skipped inline ad!' : '❌ Skip not found')

// Step 5: Wait for JWPlayer to switch source
await new Promise((r) => setTimeout(r, 3000))

// Step 6: Extract .m3u8 URL from jwplayer
const videoUrl = await page.evaluate(() => {
  try {
    return jwplayer().getPlaylist()?.[0]?.file || null
  } catch (err) {
    return null
  }
})

const cookies = await page.cookies()
const cookieHeader = cookies.map((c) => `${c.name}=${c.value}`).join('; ')
const userAgent = await page.evaluate(() => navigator.userAgent)

console.log('\n🐽 COPY/PASTE THIS FFMPEG COMMAND QUICKLY:\n')
console.log(
  `ffmpeg -headers "Referer: https://leakedzone.com\\r\\nUser-Agent: ${userAgent}\\r\\nCookie: ${cookieHeader}" -i "${videoUrl}" -c copy output.mp4\n`
)

if (!videoUrl) {
  console.log('❌ No JWPlayer URL found.')
  await browser.close()
  process.exit(1)
}
console.log('📺 JWPlayer URL:', videoUrl)

// Step 7: Fetch the .m3u8 playlist INSIDE the browser
const m3u8Text = await page.evaluate(async (url) => {
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        Referer: 'https://leakedzone.com',
      },
    })
    if (!res.ok) return `FETCH ERROR: ${res.status}`
    return await res.text()
  } catch (err) {
    return `EXCEPTION: ${err.message}`
  }
}, videoUrl)

console.log('\n📄 M3U8 Preview:\n', m3u8Text.slice(0, 800), '...\n')

await browser.close()

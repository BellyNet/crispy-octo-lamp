// /scrapyard/pageHelpers.js
const path = require('path')

const defaultUA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'

async function createScraperPage(browser, options = {}) {
  const {
    site = null,
    interceptMedia = false, // block media for non-download pages
    stealth = true, // default true
    userAgent = defaultUA,
    injectHelpers = true,
    waitForReady = true, // NEW: controls built-in wait
  } = options

  const page = await browser.newPage()
  await page.setViewport({ width: 1280, height: 800 })
  await page.setUserAgent(userAgent)

  if (interceptMedia) {
    await page.setRequestInterception(true)
    page.on('request', (req) => {
      const type = req.resourceType()
      if (['media', 'other'].includes(type)) {
        req.abort()
      } else {
        req.continue()
      }
    })
  }

  if (injectHelpers && site) {
    await page.addScriptTag({
      path: path.join(__dirname, 'mediaExtractors.js'),
    })
    // could inject more helpers here later
  }

  // ⏳ Optional safety wait for body/content to load (handles CF/DDOS screens)
  if (waitForReady) {
    try {
      await page.waitForSelector('body', { timeout: 15000 })
    } catch {
      console.warn(
        '⚠️ Timed out waiting for <body> to appear (may be Cloudflare)'
      )
    }
  }

  return page
}

module.exports = { createScraperPage }

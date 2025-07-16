const path = require('path')

async function createScraperPage(browser, options = {}) {
  const {
    site = null,
    interceptMedia = false, // block media for non-download pages
    stealth = true, // default true
    userAgent = null,
    injectHelpers = true,
    waitForReady = true, // ⏳ wait for <body> to load
  } = options

  const page = await browser.newPage()
  await page.setViewport({ width: 1280, height: 800 })

  const realisticAgents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
  ]
  const selectedUA =
    userAgent ||
    realisticAgents[Math.floor(Math.random() * realisticAgents.length)]
  await page.setUserAgent(selectedUA)

  await page.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'no-cache',
    Pragma: 'no-cache',
  })

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
  }

  if (waitForReady) {
    try {
      await page.waitForSelector('body', { timeout: 15000 })
    } catch {
      console.warn(
        '⚠️ Timed out waiting for <body> to appear (may be Cloudflare or anti-bot)'
      )
    }
  }

  return page
}

module.exports = { createScraperPage }

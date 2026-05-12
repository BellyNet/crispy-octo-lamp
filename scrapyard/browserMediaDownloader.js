'use strict'

const fs = require('fs')
const path = require('path')
const puppeteer = require('puppeteer-extra')
const StealthPlugin = require('puppeteer-extra-plugin-stealth')

puppeteer.use(StealthPlugin())

function expandWindowsEnvVars(value) {
  return String(value || '').replace(/%([^%]+)%/g, (_, name) => {
    return process.env[name] || process.env[name.toUpperCase()] || ''
  })
}

function existingPathFromCandidates(candidates) {
  for (const candidate of candidates.filter(Boolean)) {
    const expanded = expandWindowsEnvVars(candidate)
    if (expanded && fs.existsSync(expanded)) return expanded
  }
  return null
}

function getDefaultBrowserExecutablePath() {
  return existingPathFromCandidates([
    process.env.HOGHAUL_BROWSER_EXECUTABLE,
    process.env.PUPPETEER_EXECUTABLE_PATH,
    '%LOCALAPPDATA%\\Yandex\\YandexBrowser\\Application\\browser.exe',
    '%LOCALAPPDATA%\\Google\\Chrome\\Application\\chrome.exe',
    '%PROGRAMFILES%\\Google\\Chrome\\Application\\chrome.exe',
    '%PROGRAMFILES(X86)%\\Google\\Chrome\\Application\\chrome.exe',
    '%LOCALAPPDATA%\\Microsoft\\Edge\\Application\\msedge.exe',
    '%PROGRAMFILES(X86)%\\Microsoft\\Edge\\Application\\msedge.exe',
    '%PROGRAMFILES%\\Microsoft\\Edge\\Application\\msedge.exe',
  ])
}

function getDefaultBrowserProfileDir(slopvaultRoot, sourceSite) {
  return path.join(slopvaultRoot, 'hoghaul-browser-profile', sourceSite)
}

function normalizeCookieDomain(hostname) {
  const lower = String(hostname || '').toLowerCase()
  const parts = lower.split('.').filter(Boolean)
  return `.${parts.slice(-2).join('.')}`
}

function parseCookieHeader(cookieHeader, sourceUrl) {
  const parsedUrl = new URL(sourceUrl)
  const domain = normalizeCookieDomain(parsedUrl.hostname)
  return String(cookieHeader || '')
    .split(';')
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const equalsIndex = part.indexOf('=')
      if (equalsIndex <= 0) return null
      return {
        name: part.slice(0, equalsIndex).trim(),
        value: part.slice(equalsIndex + 1).trim(),
        domain,
        path: '/',
      }
    })
    .filter((cookie) => cookie?.name)
}

function parseNetscapeCookieFile(raw) {
  return raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'))
    .map((line) => {
      const parts = line.split('\t')
      if (parts.length < 7) return null
      const [domain, , pathValue, secure, expires, name, value] = parts
      return {
        domain,
        path: pathValue || '/',
        secure: /^true$/i.test(secure),
        expires: Number.parseInt(expires, 10) || undefined,
        name,
        value,
      }
    })
    .filter((cookie) => cookie?.name)
}

function normalizeCookieJson(parsed, sourceUrl) {
  const parsedUrl = new URL(sourceUrl)
  const fallbackDomain = normalizeCookieDomain(parsedUrl.hostname)
  const cookies = Array.isArray(parsed)
    ? parsed
    : Array.isArray(parsed?.cookies)
      ? parsed.cookies
      : Object.entries(parsed || {}).map(([name, value]) => ({ name, value }))

  return cookies
    .map((cookie) => {
      if (!cookie?.name || cookie.value === undefined) return null
      const normalized = {
        name: String(cookie.name),
        value: String(cookie.value),
        domain: cookie.domain || fallbackDomain,
        path: cookie.path || '/',
      }
      if (cookie.expires || cookie.expirationDate) {
        normalized.expires = Math.floor(cookie.expires || cookie.expirationDate)
      }
      if (cookie.secure !== undefined) {
        normalized.secure = Boolean(cookie.secure)
      }
      if (cookie.httpOnly !== undefined) {
        normalized.httpOnly = Boolean(cookie.httpOnly)
      }
      if (cookie.sameSite) normalized.sameSite = cookie.sameSite
      return normalized
    })
    .filter(Boolean)
}

function loadCookiesFromFile(cookieFile, sourceUrl) {
  const expanded = expandWindowsEnvVars(cookieFile)
  if (!expanded || !fs.existsSync(expanded)) {
    throw new Error(`Cookie file does not exist: ${cookieFile}`)
  }

  const raw = fs.readFileSync(expanded, 'utf8').trim()
  if (!raw) return []
  if (raw.startsWith('{') || raw.startsWith('[')) {
    return normalizeCookieJson(JSON.parse(raw), sourceUrl)
  }
  return parseNetscapeCookieFile(raw)
}

function getBrowserCookieList(sourceUrl, options) {
  const cookies = []
  if (options.cookieHeader) {
    cookies.push(...parseCookieHeader(options.cookieHeader, sourceUrl))
  }
  if (options.cookieFile) {
    cookies.push(...loadCookiesFromFile(options.cookieFile, sourceUrl))
  }
  return cookies
}

async function getBrowserWebSocketEndpoint(connectValue, requestBuffer) {
  const versionUrl = new URL('/json/version', connectValue).toString()
  const response = await requestBuffer(versionUrl, {
    headers: { Accept: 'application/json' },
  })
  const version = JSON.parse(response.buffer.toString('utf8'))
  if (!version.webSocketDebuggerUrl) {
    throw new Error(`No webSocketDebuggerUrl found at ${versionUrl}`)
  }
  return version.webSocketDebuggerUrl
}

async function createBrowserMediaDownloader(source, options = {}) {
  const {
    requestBuffer,
    slopvaultRoot,
    appendRunEvent = () => {},
    logger = console,
  } = options
  if (typeof requestBuffer !== 'function') {
    throw new Error('createBrowserMediaDownloader requires requestBuffer')
  }
  if (!slopvaultRoot) {
    throw new Error('createBrowserMediaDownloader requires slopvaultRoot')
  }

  let browser = null
  let shouldCloseBrowser = true
  if (options.browserConnect) {
    const browserWSEndpoint = /^https?:\/\//i.test(options.browserConnect)
      ? await getBrowserWebSocketEndpoint(options.browserConnect, requestBuffer)
      : options.browserConnect
    logger.log(`Browser media mode: connected browser (${browserWSEndpoint})`)
    browser = await puppeteer.connect({ browserWSEndpoint })
    shouldCloseBrowser = false
  }

  const executablePath =
    options.browserExecutable || getDefaultBrowserExecutablePath()
  const userDataDir =
    options.browserProfile ||
    getDefaultBrowserProfileDir(slopvaultRoot, source.site)
  const headless = options.headless ? 'new' : false
  if (!browser) {
    fs.mkdirSync(userDataDir, { recursive: true })

    const launchOptions = {
      headless,
      userDataDir,
      defaultViewport: null,
      args: [
        '--ignore-certificate-errors',
        '--disable-blink-features=AutomationControlled',
        '--disable-extensions',
      ],
      ignoreHTTPSErrors: true,
    }
    if (executablePath) launchOptions.executablePath = executablePath

    logger.log(
      `Browser media mode: ${executablePath || 'bundled Chromium'} (${headless ? 'headless' : 'headful'})`
    )
    logger.log(`Browser profile: ${userDataDir}`)

    browser = await puppeteer.launch(launchOptions)
  }
  const cookies = getBrowserCookieList(source.inputUrl, options)
  if (cookies.length) {
    const cookiePage = await browser.newPage()
    await cookiePage.setCookie(...cookies)
    await cookiePage.close()
    logger.log(`Loaded ${cookies.length} browser cookie(s) for media requests.`)
  }

  const warmupPage = await browser.newPage()
  await warmupPage.setExtraHTTPHeaders({
    'Accept-Language': 'en-US,en;q=0.9',
  })
  await warmupPage
    .goto(source.inputUrl, {
      waitUntil: 'domcontentloaded',
      timeout: options.timeoutMs,
    })
    .catch((err) => {
      appendRunEvent('browser_warmup_warning', {
        url: source.inputUrl,
        error: err.message,
      })
      logger.warn(`Browser warmup warning: ${err.message}`)
    })
  if (options.validateMs > 0) {
    logger.log(
      `Browser validation pause: ${Math.round(options.validateMs / 1000)}s. Use the opened browser window to pass any site check.`
    )
    await new Promise((resolve) => setTimeout(resolve, options.validateMs))
  }

  async function getCookieHeaderFor(mediaUrl) {
    const cookiesForRequest = await warmupPage
      .cookies(source.inputUrl, mediaUrl)
      .catch(() => [])
    const browserCookieHeader = cookiesForRequest
      .map((cookie) => `${cookie.name}=${cookie.value}`)
      .join('; ')
    return [options.cookieHeader, browserCookieHeader]
      .filter(Boolean)
      .join('; ')
  }

  return {
    async download(mediaUrl, entry = {}) {
      const cookieHeader = await getCookieHeaderFor(mediaUrl)
      try {
        const response = await requestBuffer(mediaUrl, {
          timeoutMs: options.timeoutMs,
          headers: {
            Accept: '*/*',
            Referer: entry.mediaPageUrl || source.inputUrl,
            ...(cookieHeader ? { Cookie: cookieHeader } : {}),
          },
        })
        return response.buffer
      } catch (err) {
        appendRunEvent('browser_cookie_http_error', {
          mediaUrl,
          mediaPageUrl: entry.mediaPageUrl,
          error: err.message,
          hadCookieHeader: Boolean(cookieHeader),
        })
      }

      const page = await browser.newPage()
      try {
        await page.setExtraHTTPHeaders({
          Accept: '*/*',
          'Accept-Language': 'en-US,en;q=0.9',
          Referer: entry.mediaPageUrl || source.inputUrl,
        })
        const response = await page.goto(mediaUrl, {
          waitUntil: 'load',
          timeout: options.timeoutMs,
        })
        if (!response) throw new Error('Browser returned no response')
        const status = response.status()
        if (status < 200 || status >= 300) {
          throw new Error(`Browser HTTP ${status}`)
        }
        return await response.buffer()
      } finally {
        await page.close().catch(() => {})
      }
    },
    async extractPostMediaUrls(postPageUrl) {
      const page = await browser.newPage()
      try {
        await page.setExtraHTTPHeaders({
          'Accept-Language': 'en-US,en;q=0.9',
          Referer: source.inputUrl,
        })
        await page.goto(postPageUrl, {
          waitUntil: 'domcontentloaded',
          timeout: options.timeoutMs,
        })
        await page
          .waitForSelector(
            'a.fileThumb.image-link, a.post__attachment-link[href], video source[src]',
            { timeout: 10000 }
          )
          .catch(() => {})
        return await page.evaluate(() => {
          return Array.from(
            document.querySelectorAll(
              'a.fileThumb.image-link, a.post__attachment-link[href], video source[src]'
            )
          )
            .map((el) => {
              const value =
                el.href ||
                el.src ||
                el.getAttribute('href') ||
                el.getAttribute('src') ||
                ''
              if (!value) return null
              return new URL(value, location.href).toString()
            })
            .filter(Boolean)
        })
      } finally {
        await page.close().catch(() => {})
      }
    },
    async close() {
      await warmupPage.close().catch(() => {})
      if (shouldCloseBrowser) {
        await browser.close().catch(() => {})
      } else {
        browser.disconnect()
      }
    },
  }
}

module.exports = {
  createBrowserMediaDownloader,
  getDefaultBrowserExecutablePath,
  getDefaultBrowserProfileDir,
  getBrowserCookieList,
  getBrowserWebSocketEndpoint,
}

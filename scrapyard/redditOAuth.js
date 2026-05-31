const fs = require('fs')
const http = require('http')
const https = require('https')
const os = require('os')
const path = require('path')
const crypto = require('crypto')

const DEFAULT_SCOPES = ['identity', 'read']
const DEFAULT_REDIRECT_URI = 'http://127.0.0.1:8765/reddit/callback'
const DEFAULT_USER_AGENT = 'windows:lora-training:v1.0 (by /u/local)'
const CONFIG_PATH = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  '.slopvault',
  'reddit-oauth.json'
)

let cachedToken = null

function getConfigPath() {
  return process.env.REDDIT_OAUTH_CONFIG || CONFIG_PATH
}

function readJsonIfExists(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'))
  } catch {
    return null
  }
}

function loadRedditOAuthConfig() {
  const fileConfig = readJsonIfExists(getConfigPath()) || {}
  const config = {
    ...fileConfig,
    clientId: process.env.REDDIT_CLIENT_ID || fileConfig.clientId,
    clientSecret:
      process.env.REDDIT_CLIENT_SECRET || fileConfig.clientSecret || '',
    refreshToken: process.env.REDDIT_REFRESH_TOKEN || fileConfig.refreshToken,
    accessToken: process.env.REDDIT_ACCESS_TOKEN || fileConfig.accessToken,
    userAgent:
      process.env.REDDIT_USER_AGENT ||
      fileConfig.userAgent ||
      DEFAULT_USER_AGENT,
    redirectUri:
      process.env.REDDIT_REDIRECT_URI ||
      fileConfig.redirectUri ||
      DEFAULT_REDIRECT_URI,
  }

  if (!config.clientId && !config.accessToken) return null
  return config
}

function saveRedditOAuthConfig(config) {
  const configPath = getConfigPath()
  fs.mkdirSync(path.dirname(configPath), { recursive: true })
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2) + '\n')
  return configPath
}

function formEncode(values) {
  return new URLSearchParams(values).toString()
}

function requestJson(url, options = {}) {
  const body = options.body || ''
  const parsed = new URL(url)
  return new Promise((resolve, reject) => {
    const req = https.request(
      parsed,
      {
        method: options.method || 'GET',
        headers: {
          'User-Agent': options.userAgent || DEFAULT_USER_AGENT,
          Accept: 'application/json',
          ...(body
            ? {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Content-Length': Buffer.byteLength(body),
              }
            : {}),
          ...(options.headers || {}),
        },
      },
      (res) => {
        let data = ''
        res.setEncoding('utf8')
        res.on('data', (chunk) => {
          data += chunk
        })
        res.on('end', () => {
          let parsedBody = null
          try {
            parsedBody = data ? JSON.parse(data) : null
          } catch {
            parsedBody = { raw: data }
          }
          if (res.statusCode < 200 || res.statusCode >= 300) {
            const message =
              parsedBody?.error_description ||
              parsedBody?.message ||
              parsedBody?.error ||
              data.slice(0, 200) ||
              `HTTP ${res.statusCode}`
            reject(new Error(`Reddit OAuth HTTP ${res.statusCode}: ${message}`))
            return
          }
          resolve(parsedBody)
        })
      }
    )
    req.on('error', reject)
    if (body) req.write(body)
    req.end()
  })
}

function getBasicAuthHeader(clientId, clientSecret = '') {
  return `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`
}

async function exchangeAuthorizationCode(options = {}) {
  const {
    clientId,
    clientSecret = '',
    code,
    redirectUri = DEFAULT_REDIRECT_URI,
    userAgent = DEFAULT_USER_AGENT,
  } = options
  if (!clientId) throw new Error('clientId is required')
  if (!code) throw new Error('code is required')

  return requestJson('https://www.reddit.com/api/v1/access_token', {
    method: 'POST',
    userAgent,
    headers: {
      Authorization: getBasicAuthHeader(clientId, clientSecret),
    },
    body: formEncode({
      grant_type: 'authorization_code',
      code,
      redirect_uri: redirectUri,
    }),
  })
}

async function refreshAccessToken(config) {
  if (!config?.clientId || !config?.refreshToken) return null
  const token = await requestJson(
    'https://www.reddit.com/api/v1/access_token',
    {
      method: 'POST',
      userAgent: config.userAgent,
      headers: {
        Authorization: getBasicAuthHeader(
          config.clientId,
          config.clientSecret || ''
        ),
      },
      body: formEncode({
        grant_type: 'refresh_token',
        refresh_token: config.refreshToken,
      }),
    }
  )
  return {
    accessToken: token.access_token,
    expiresAt:
      Date.now() + Math.max(Number(token.expires_in || 3600) - 60, 60) * 1000,
    userAgent: config.userAgent,
  }
}

async function getRedditOAuthAccess() {
  const config = loadRedditOAuthConfig()
  if (!config) return null

  if (cachedToken?.accessToken && cachedToken.expiresAt > Date.now()) {
    return cachedToken
  }

  if (config.accessToken && !config.refreshToken) {
    cachedToken = {
      accessToken: config.accessToken,
      expiresAt: Date.now() + 30 * 60 * 1000,
      userAgent: config.userAgent,
    }
    return cachedToken
  }

  cachedToken = await refreshAccessToken(config)
  return cachedToken
}

function toOAuthRedditUrl(url) {
  const parsed = new URL(url)
  parsed.protocol = 'https:'
  parsed.hostname = 'oauth.reddit.com'
  return parsed.toString()
}

function buildAuthorizationUrl(options = {}) {
  const {
    clientId,
    redirectUri = DEFAULT_REDIRECT_URI,
    state = crypto.randomBytes(16).toString('hex'),
    scopes = DEFAULT_SCOPES,
  } = options
  if (!clientId) throw new Error('clientId is required')
  const url = new URL('https://www.reddit.com/api/v1/authorize')
  url.searchParams.set('client_id', clientId)
  url.searchParams.set('response_type', 'code')
  url.searchParams.set('state', state)
  url.searchParams.set('redirect_uri', redirectUri)
  url.searchParams.set('duration', 'permanent')
  url.searchParams.set('scope', scopes.join(' '))
  return { url: url.toString(), state }
}

function waitForAuthorizationCode(options = {}) {
  const redirectUri = new URL(options.redirectUri || DEFAULT_REDIRECT_URI)
  const expectedPath = redirectUri.pathname
  const expectedState = options.state
  const port = Number(redirectUri.port || 80)
  const host = redirectUri.hostname

  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      const requestUrl = new URL(req.url, `http://${req.headers.host}`)
      if (requestUrl.pathname !== expectedPath) {
        res.writeHead(404)
        res.end('Not found')
        return
      }

      const error = requestUrl.searchParams.get('error')
      const code = requestUrl.searchParams.get('code')
      const state = requestUrl.searchParams.get('state')
      if (error) {
        res.writeHead(400, { 'Content-Type': 'text/plain' })
        res.end(`Reddit OAuth failed: ${error}`)
        server.close()
        reject(new Error(`Reddit OAuth failed: ${error}`))
        return
      }
      if (!code || state !== expectedState) {
        res.writeHead(400, { 'Content-Type': 'text/plain' })
        res.end('Reddit OAuth failed: invalid callback.')
        server.close()
        reject(new Error('Invalid Reddit OAuth callback'))
        return
      }

      res.writeHead(200, { 'Content-Type': 'text/plain' })
      res.end('Reddit OAuth is connected. You can close this tab.')
      server.close()
      resolve(code)
    })

    server.on('error', reject)
    server.listen(port, host)
  })
}

module.exports = {
  DEFAULT_REDIRECT_URI,
  DEFAULT_SCOPES,
  DEFAULT_USER_AGENT,
  buildAuthorizationUrl,
  exchangeAuthorizationCode,
  getConfigPath,
  getRedditOAuthAccess,
  loadRedditOAuthConfig,
  saveRedditOAuthConfig,
  toOAuthRedditUrl,
  waitForAuthorizationCode,
}

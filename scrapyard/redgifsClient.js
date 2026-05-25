'use strict'

const DEFAULT_AUTH_URL = 'https://api.redgifs.com/v2/auth/temporary'
const DEFAULT_GIF_API_BASE = 'https://api.redgifs.com/v2/gifs'
const DEFAULT_TOKEN_TTL_MS = 12 * 60 * 60 * 1000

function sanitizeRedgifsId(value) {
  return String(value || '')
    .trim()
    .replace(/[^a-z0-9]/gi, '')
    .toLowerCase()
}

function createRedgifsClient(options = {}) {
  const requestBuffer = options.requestBuffer
  if (typeof requestBuffer !== 'function') {
    throw new Error('createRedgifsClient requires requestBuffer')
  }

  const authUrl = options.authUrl || DEFAULT_AUTH_URL
  const gifApiBase = options.gifApiBase || DEFAULT_GIF_API_BASE
  const tokenTtlMs = options.tokenTtlMs || DEFAULT_TOKEN_TTL_MS
  let auth = null

  async function getToken() {
    if (auth?.token && auth.expiresAt > Date.now() + 60000) {
      return auth.token
    }

    const response = await requestBuffer(authUrl, {
      headers: {
        Accept: 'application/json',
      },
    })
    const data = JSON.parse(response.buffer.toString('utf8'))
    if (!data?.token) {
      throw new Error('RedGIFs temporary auth returned no token')
    }

    auth = {
      token: data.token,
      expiresAt: Date.now() + tokenTtlMs,
    }
    return auth.token
  }

  async function fetchJson(url) {
    const token = await getToken()
    const response = await requestBuffer(url, {
      headers: {
        Accept: 'application/json',
        Authorization: `Bearer ${token}`,
      },
    })
    return JSON.parse(response.buffer.toString('utf8'))
  }

  function parseRedgifsId(url) {
    try {
      const parsed = new URL(url)
      const host = parsed.hostname.toLowerCase()
      if (!host.includes('redgifs.com')) return null
      const parts = parsed.pathname.split('/').filter(Boolean)
      const markerIndex = parts.findIndex((part) =>
        ['watch', 'ifr', 'iframe'].includes(part.toLowerCase())
      )
      const rawId =
        markerIndex >= 0 ? parts[markerIndex + 1] : parts[parts.length - 1]
      return sanitizeRedgifsId(rawId)
    } catch {
      return null
    }
  }

  async function getGif(id) {
    const data = await fetchJson(`${gifApiBase}/${encodeURIComponent(id)}`)
    return data?.gif || null
  }

  async function resolveMedia(redgifsUrl) {
    const id = parseRedgifsId(redgifsUrl)
    if (!id) return null

    const gif = await getGif(id)
    const mediaUrl = gif?.urls?.hd || gif?.urls?.sd
    if (!mediaUrl) return null

    const createDateSeconds = Number(gif.createDate)
    const createdDate =
      Number.isFinite(createDateSeconds) && createDateSeconds > 0
        ? new Date(createDateSeconds * 1000)
        : null

    return {
      id,
      gif,
      mediaUrl,
      mediaUrls: [gif?.urls?.hd, gif?.urls?.sd].filter(Boolean),
      canonicalUrl: `https://www.redgifs.com/watch/${id}`,
      createdDate,
    }
  }

  return {
    getToken,
    fetchJson,
    parseRedgifsId,
    getGif,
    resolveMedia,
  }
}

module.exports = {
  createRedgifsClient,
  sanitizeRedgifsId,
}

'use strict'

const http = require('http')
const https = require('https')
const zlib = require('zlib')

const DEFAULT_USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36'
const DEFAULT_TIMEOUT_MS = 30000

function decodeBody(buffer, headers = {}) {
  const encoding = String(headers['content-encoding'] || '').toLowerCase()
  if (encoding.includes('br')) return zlib.brotliDecompressSync(buffer)
  if (encoding.includes('gzip')) return zlib.gunzipSync(buffer)
  if (encoding.includes('deflate')) return zlib.inflateSync(buffer)
  return buffer
}

function createHttpClient(options = {}) {
  const defaultTimeoutMs =
    Number.parseInt(options.timeoutMs || '', 10) || DEFAULT_TIMEOUT_MS
  const userAgent = options.userAgent || DEFAULT_USER_AGENT
  const acceptLanguage = options.acceptLanguage || 'en-US,en;q=0.9'

  function requestBuffer(url, requestOptions = {}) {
    const {
      method = 'GET',
      headers = {},
      timeoutMs = defaultTimeoutMs,
      maxRedirects = 5,
      onProgress = null,
    } = requestOptions

    return new Promise((resolve, reject) => {
      const parsed = new URL(url)
      const client = parsed.protocol === 'https:' ? https : http
      const req = client.request(
        parsed,
        {
          method,
          headers: {
            'User-Agent': userAgent,
            'Accept-Language': acceptLanguage,
            'Accept-Encoding': 'gzip, deflate, br',
            ...headers,
          },
        },
        (res) => {
          const statusCode = res.statusCode || 0
          const redirect = res.headers.location
          if (
            [301, 302, 303, 307, 308].includes(statusCode) &&
            redirect &&
            maxRedirects > 0
          ) {
            res.resume()
            const nextUrl = new URL(redirect, url).toString()
            requestBuffer(nextUrl, {
              method: statusCode === 303 ? 'GET' : method,
              headers,
              timeoutMs,
              maxRedirects: maxRedirects - 1,
              onProgress,
            }).then(resolve, reject)
            return
          }

          if (statusCode < 200 || statusCode >= 300) {
            const chunks = []
            res.on('data', (chunk) => chunks.push(chunk))
            res.on('end', () => {
              const raw = Buffer.concat(chunks)
              let body
              try {
                body = decodeBody(raw, res.headers).toString('utf8')
              } catch {
                body = raw.toString('utf8')
              }
              body = body.replace(/\s+/g, ' ').trim().slice(0, 500)
              reject(new Error(`HTTP ${statusCode}: ${body}`))
            })
            return
          }

          if (method === 'HEAD') {
            res.resume()
            resolve({
              buffer: Buffer.alloc(0),
              headers: res.headers,
              statusCode,
              url,
            })
            return
          }

          const chunks = []
          let downloadedBytes = 0
          const totalBytes = Number.parseInt(
            res.headers['content-length'] || '0',
            10
          )
          const startedAt = Date.now()
          res.on('data', (chunk) => {
            downloadedBytes += chunk.length
            chunks.push(chunk)
            if (onProgress) {
              onProgress({
                downloadedBytes,
                totalBytes,
                chunkBytes: chunk.length,
                elapsedMs: Date.now() - startedAt,
              })
            }
          })
          res.on('end', () => {
            const raw = Buffer.concat(chunks)
            resolve({
              buffer: decodeBody(raw, res.headers),
              headers: res.headers,
              statusCode,
              url,
            })
          })
        }
      )

      req.setTimeout(timeoutMs, () => {
        req.destroy(new Error(`Request timed out after ${timeoutMs}ms`))
      })
      req.on('error', (err) => {
        const message =
          err?.message ||
          err?.code ||
          `Request failed for ${new URL(url).hostname}`
        reject(new Error(message))
      })
      req.end()
    })
  }

  async function fetchJson(url, requestOptions = {}) {
    const response = await requestBuffer(url, {
      ...requestOptions,
      headers: {
        Accept: 'application/json',
        ...(requestOptions.headers || {}),
      },
    })
    const body = response.buffer.toString('utf8')
    return {
      data: JSON.parse(body),
      byteLength: response.buffer.length,
      url,
      headers: response.headers,
      statusCode: response.statusCode,
    }
  }

  async function fetchHtml(url, requestOptions = {}) {
    const response = await requestBuffer(url, {
      ...requestOptions,
      headers: {
        Accept:
          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        ...(requestOptions.headers || {}),
      },
    })
    return {
      html: response.buffer.toString('utf8'),
      byteLength: response.buffer.length,
      url,
      headers: response.headers,
      statusCode: response.statusCode,
    }
  }

  return {
    requestBuffer,
    fetchJson,
    fetchHtml,
  }
}

module.exports = {
  createHttpClient,
  decodeBody,
}

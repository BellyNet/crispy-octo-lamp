'use strict'

const path = require('path')

const { sanitize } = require('./modelRegistry')
const {
  PAWCHIVE_ORIGIN,
  getPawchiveUserUrl,
  isPawchiveOrKemonoHost,
} = require('./pawchive')

const TUMBLR_RESERVED_SUBDOMAINS = new Set([
  'www',
  'api',
  'assets',
  'embed',
  'engine',
  't',
  'render',
])

function normalizeSourceUrlInput(inputUrl) {
  const raw = String(inputUrl || '').trim()
  const markdownMatch = raw.match(/^\[[^\]]+\]\((https?:\/\/[^)]+)\)$/i)
  if (markdownMatch) return markdownMatch[1].trim()
  const angleMatch = raw.match(/^<\s*(https?:\/\/[^>]+)\s*>$/i)
  if (angleMatch) return angleMatch[1].trim()
  return raw
}

function isTumblrHost(host) {
  return host === 'tumblr.com' || host.endsWith('.tumblr.com')
}

function parseHoghaulSourceUrl(inputUrl) {
  const parsed = new URL(normalizeSourceUrlInput(inputUrl))
  const host = parsed.hostname.toLowerCase()
  const site = host.includes('coomerfans')
    ? 'coomerfans'
    : host === 'cum.st' || host.endsWith('.cum.st')
      ? 'coomerfans'
      : host.includes('coomer')
        ? 'coomer'
        : isPawchiveOrKemonoHost(host)
          ? 'kemono'
          : host.endsWith('reddit.com')
            ? 'reddit'
            : isTumblrHost(host)
              ? 'tumblr'
              : null
  if (!site) throw new Error(`Unsupported Hoghaul host: ${parsed.hostname}`)

  const parts = parsed.pathname.split('/').filter(Boolean)

  if (site === 'reddit') {
    const userIndex = parts.findIndex((part) =>
      /^(?:user|u)$/i.test(String(part || ''))
    )
    const username = userIndex >= 0 ? parts[userIndex + 1] : null
    if (username) {
      const cleanUsername = username.replace(/^u_/, '')
      return {
        inputUrl: `https://www.reddit.com/user/${cleanUsername}/submitted/`,
        origin: 'https://www.reddit.com',
        site,
        service: 'submitted',
        userId: cleanUsername,
        username: cleanUsername,
        rawName: sanitize(cleanUsername),
      }
    }

    throw new Error(
      'Expected a Reddit user URL like /user/name/submitted or /user/name'
    )
  }

  if (site === 'coomerfans') {
    if (host === 'cum.st' || host.endsWith('.cum.st')) {
      if (parts[0] === 'creators' && parts[1] && parts[2]) {
        return {
          inputUrl: `${parsed.origin}/creators/${parts[1]}/${parts[2]}`,
          origin: parsed.origin,
          site,
          service: parts[1],
          userId: parts[2],
          rawName: sanitize(parts[2]),
        }
      }

      const queryName =
        parsed.searchParams.get('q') || parsed.searchParams.get('search')
      if (queryName) {
        return {
          inputUrl: parsed.toString(),
          origin: parsed.origin,
          site,
          service: 'onlyfans',
          userId: null,
          rawName: sanitize(queryName),
        }
      }

      throw new Error(
        'Expected an OnlyHaven URL like /creators/onlyfans/id or /creators?q=name'
      )
    }

    if (parts[0] === 'u' && parts[1] && parts[2] && parts[3]) {
      return {
        inputUrl: parsed.toString(),
        origin: parsed.origin,
        site,
        service: parts[1],
        userId: parts[2],
        rawName: sanitize(parts[3]),
      }
    }

    const queryName = parsed.searchParams.get('q')
    if (queryName) {
      return {
        inputUrl: parsed.toString(),
        origin: parsed.origin,
        site,
        service: 'onlyfans',
        userId: null,
        rawName: sanitize(queryName),
      }
    }

    throw new Error(
      'Expected a CoomerFans URL like /u/onlyfans/id/name or /?q=name'
    )
  }

  if (site === 'tumblr') {
    const subdomain = host.replace(/\.tumblr\.com$/i, '')
    let blogName = null
    if (host !== 'tumblr.com' && !TUMBLR_RESERVED_SUBDOMAINS.has(subdomain)) {
      blogName = subdomain
    } else if (parts[0] === 'blog' && parts[1] === 'view' && parts[2]) {
      blogName = parts[2]
    } else if (parts[0] && parts[0] !== 'blog') {
      blogName = parts[0]
    }

    if (!blogName) {
      throw new Error(
        'Expected a Tumblr blog URL like https://<blog>.tumblr.com or https://www.tumblr.com/<blog>'
      )
    }

    const cleanBlogName = blogName.toLowerCase()
    return {
      inputUrl: `https://${cleanBlogName}.tumblr.com/`,
      origin: `https://${cleanBlogName}.tumblr.com`,
      site,
      service: 'blog',
      userId: cleanBlogName,
      username: cleanBlogName,
      rawName: sanitize(cleanBlogName),
    }
  }

  const userIndex = parts.indexOf('user')
  const service = parts[0]
  const userId = userIndex >= 0 ? parts[userIndex + 1] : null

  if (!service || !userId) {
    throw new Error(
      'Expected a creator URL like /onlyfans/user/name or /patreon/user/id'
    )
  }

  return {
    inputUrl:
      site === 'kemono'
        ? getPawchiveUserUrl(service, userId)
        : parsed.toString(),
    origin: site === 'kemono' ? PAWCHIVE_ORIGIN : parsed.origin,
    site,
    service,
    userId,
    rawName: sanitize(userId),
  }
}

function parseSourceUrl(inputUrl) {
  try {
    const normalizedInputUrl = normalizeSourceUrlInput(inputUrl)
    const parsed = new URL(normalizedInputUrl)
    const host = parsed.hostname.toLowerCase()

    if (host.includes('stufferdb') || host.includes('stufferai')) {
      parsed.hostname = 'stufferdb.com'
      const normalized = parsed.toString()
      if (!/\/(?:category|picture)(?:[/?#]|$)/i.test(normalized)) {
        return null
      }
      return {
        scraper: 'milkmaid',
        sourceType: 'stufferdb',
        url: normalized,
        rawName: null,
      }
    }

    if (
      host === 'reddit.com' ||
      host.endsWith('.reddit.com') ||
      host === 'cum.st' ||
      host.endsWith('.cum.st') ||
      host.includes('coomer') ||
      isPawchiveOrKemonoHost(host) ||
      isTumblrHost(host)
    ) {
      const source = parseHoghaulSourceUrl(normalizedInputUrl)
      return {
        ...source,
        scraper: 'hoghaul',
        sourceType: source.site,
        url: source.inputUrl,
      }
    }
  } catch {
    return null
  }

  return null
}

function getScraperScript(parsedSource) {
  if (parsedSource?.scraper === 'milkmaid') {
    return path.join('milkmaid', 'milkmaid.js')
  }
  if (parsedSource?.scraper === 'hoghaul') {
    return path.join('hoghaul', 'hoghaul.js')
  }
  return null
}

function describeSource(parsedSource) {
  if (!parsedSource) return 'unknown'
  return `${parsedSource.sourceType} via ${parsedSource.scraper}`
}

module.exports = {
  parseSourceUrl,
  parseHoghaulSourceUrl,
  normalizeSourceUrlInput,
  getScraperScript,
  describeSource,
}

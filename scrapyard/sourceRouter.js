'use strict'

const path = require('path')

const { sanitize } = require('./modelRegistry')

function parseHoghaulSourceUrl(inputUrl) {
  const parsed = new URL(String(inputUrl || '').trim())
  const host = parsed.hostname.toLowerCase()
  const site = host.includes('coomerfans')
    ? 'coomerfans'
    : host.includes('coomer')
      ? 'coomer'
      : host.includes('kemono')
        ? 'kemono'
        : host.endsWith('reddit.com')
          ? 'reddit'
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

  const userIndex = parts.indexOf('user')
  const service = parts[0]
  const userId = userIndex >= 0 ? parts[userIndex + 1] : null

  if (!service || !userId) {
    throw new Error(
      'Expected a creator URL like /onlyfans/user/name or /patreon/user/id'
    )
  }

  return {
    inputUrl: parsed.toString(),
    origin: parsed.origin,
    site,
    service,
    userId,
    rawName: sanitize(userId),
  }
}

function parseSourceUrl(inputUrl) {
  try {
    const parsed = new URL(String(inputUrl || '').trim())
    const host = parsed.hostname.toLowerCase()

    if (host.includes('stufferdb') || host.includes('stufferai')) {
      parsed.hostname = 'stufferdb.com'
      return {
        scraper: 'milkmaid',
        sourceType: 'stufferdb',
        url: parsed.toString(),
        rawName: null,
      }
    }

    if (
      host === 'reddit.com' ||
      host.endsWith('.reddit.com') ||
      host.includes('coomer') ||
      host.includes('kemono')
    ) {
      const source = parseHoghaulSourceUrl(inputUrl)
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
  getScraperScript,
  describeSource,
}

'use strict'

const path = require('path')

function parseSourceUrl(inputUrl) {
  try {
    const parsed = new URL(String(inputUrl || '').trim())
    const host = parsed.hostname.toLowerCase()
    const parts = parsed.pathname.split('/').filter(Boolean)

    if (host.includes('stufferdb')) {
      return {
        scraper: 'milkmaid',
        sourceType: 'stufferdb',
        url: parsed.toString(),
        rawName: null,
      }
    }

    if (host === 'reddit.com' || host.endsWith('.reddit.com')) {
      const userIndex = parts.findIndex((part) =>
        /^(?:user|u)$/i.test(String(part || ''))
      )
      const username = userIndex >= 0 ? parts[userIndex + 1] : null
      if (username) {
        return {
          scraper: 'hoghaul',
          sourceType: 'reddit',
          url: `https://www.reddit.com/user/${username}/submitted/`,
          rawName: username,
        }
      }
    }

    if (host.includes('coomerfans')) {
      if (parts[0] === 'u' && parts.length >= 4) {
        return {
          scraper: 'hoghaul',
          sourceType: 'coomer',
          url: parsed.toString(),
          rawName: parts[3],
        }
      }
      if (parsed.searchParams.get('q')) {
        return {
          scraper: 'hoghaul',
          sourceType: 'coomer',
          url: parsed.toString(),
          rawName: parsed.searchParams.get('q'),
        }
      }
    }

    if (host.includes('coomer')) {
      return {
        scraper: 'hoghaul',
        sourceType: 'coomer',
        url: parsed.toString(),
        rawName: parts[2] || parts[parts.length - 1] || null,
      }
    }

    if (host.includes('kemono')) {
      return {
        scraper: 'hoghaul',
        sourceType: 'kemono',
        url: parsed.toString(),
        rawName: parts[2] || parts[parts.length - 1] || null,
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
  getScraperScript,
  describeSource,
}

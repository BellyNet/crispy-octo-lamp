'use strict'

const path = require('path')

const {
  normalizeSourceUrlInput,
  findSourceByHost,
  findSourceForParsed,
} = require('./sources')

// Parses a creator URL for the hoghaul engine. Throws a descriptive Error for
// unsupported hosts or URLs that aren't a creator page.
function parseHoghaulSourceUrl(inputUrl) {
  const parsed = new URL(normalizeSourceUrlInput(inputUrl))
  const host = parsed.hostname.toLowerCase()
  const source = findSourceByHost(host)
  if (!source || source.engine !== 'hoghaul') {
    throw new Error(`Unsupported Hoghaul host: ${parsed.hostname}`)
  }
  return source.parseUrl(parsed, host)
}

// Returns the parsed source with `scraper` (engine) and `sourceType` set, or
// null when the URL isn't a supported creator page.
function parseSourceUrl(inputUrl) {
  try {
    const parsed = new URL(normalizeSourceUrlInput(inputUrl))
    const host = parsed.hostname.toLowerCase()
    const source = findSourceByHost(host)
    if (!source) return null

    const fields = source.parseUrl(parsed, host)
    if (!fields) return null
    if (source.engine === 'milkmaid') {
      return { scraper: 'milkmaid', ...fields }
    }
    return {
      ...fields,
      scraper: source.engine,
      sourceType: fields.site,
      url: fields.inputUrl,
    }
  } catch {
    return null
  }
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
  findSourceForParsed,
}

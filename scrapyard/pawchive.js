'use strict'

const PAWCHIVE_ORIGIN = 'https://pawchive.pw'
const PAWCHIVE_HOST = 'pawchive.pw'
const PAWCHIVE_MEDIA_ORIGIN = 'https://file.pawchive.pw'
const PAWCHIVE_MEDIA_HOST = 'file.pawchive.pw'
const PAWCHIVE_PREVIEW_ORIGIN = 'https://img.pawchive.pw'
const PAWCHIVE_PREVIEW_HOST = 'img.pawchive.pw'
const LEGACY_PAWCHIVE_HOSTS = new Set(['pawchive.st', 'img.pawchive.st'])

function isHostOrSubdomain(host, parentHost) {
  return host === parentHost || host.endsWith(`.${parentHost}`)
}

function isPawchiveHost(hostname) {
  const host = String(hostname || '').toLowerCase()
  return (
    isHostOrSubdomain(host, PAWCHIVE_HOST) ||
    isHostOrSubdomain(host, PAWCHIVE_MEDIA_HOST) ||
    isHostOrSubdomain(host, PAWCHIVE_PREVIEW_HOST) ||
    LEGACY_PAWCHIVE_HOSTS.has(host)
  )
}

function isPawchiveOrKemonoHost(hostname) {
  const host = String(hostname || '').toLowerCase()
  return isPawchiveHost(host) || host.includes('kemono')
}

function isPawchiveFileDataUrl(url) {
  try {
    const parsed = new URL(String(url || '').trim())
    return (
      parsed.hostname.toLowerCase() === PAWCHIVE_MEDIA_HOST &&
      /^\/data\//i.test(parsed.pathname)
    )
  } catch {
    return false
  }
}

function hasStalePawchiveDataHost(value) {
  return /https?:\/\/(?:img\.)?pawchive\.st\/data\//i.test(
    String(value || '')
  ) || /https?:\/\/img\.pawchive\.pw\/data\//i.test(String(value || ''))
}

function isNormalizedPawchiveDataKey(value) {
  return /^(?:pawchive|kemono)-data:/i.test(String(value || '').trim())
}

function shouldUsePawchiveDeadMediaMatch(details = {}) {
  const candidateUrls = [details.mediaUrl, details.mediaUrls]
    .flat(Infinity)
    .map((url) => String(url || '').trim())
    .filter(Boolean)
  if (!candidateUrls.some((url) => isPawchiveFileDataUrl(url))) return true

  const match = details.match || {}
  const matchedValues = [
    match.mediaUrl,
    match.mediaUrls,
    match.error,
    match.reason,
  ].flat(Infinity)

  if (matchedValues.some((value) => hasStalePawchiveDataHost(value))) {
    return false
  }

  const rawMatchedUrls = [match.rawMediaUrl, match.rawMediaUrls]
    .flat(Infinity)
    .map((url) => String(url || '').trim())
    .filter(Boolean)
  if (rawMatchedUrls.some((url) => isPawchiveFileDataUrl(url))) return true

  if (
    rawMatchedUrls.length === 0 &&
    [match.mediaUrl, match.mediaUrls]
      .flat(Infinity)
      .some((url) => isNormalizedPawchiveDataKey(url))
  ) {
    return false
  }

  return true
}

function getPawchiveUserUrl(service, userId) {
  return `${PAWCHIVE_ORIGIN}/${encodeURIComponent(service)}/user/${encodeURIComponent(userId)}`
}

function getPawchiveProfileUrl(service, userId) {
  return `${PAWCHIVE_ORIGIN}/api/v1/${encodeURIComponent(service)}/user/${encodeURIComponent(userId)}/profile`
}

function getPawchiveMediaUrl(mediaPath, filename = null) {
  const normalizedPath = String(mediaPath || '').replace(/^\/+/, '')
  if (!normalizedPath) return null
  const url = new URL(`${PAWCHIVE_MEDIA_ORIGIN}/data/${normalizedPath}`)
  const normalizedFilename = String(filename || '').trim()
  if (normalizedFilename) url.searchParams.set('f', normalizedFilename)
  return url.toString()
}

function getPawchivePreviewUrl(mediaPath) {
  const normalizedPath = String(mediaPath || '').replace(/^\/+/, '')
  return normalizedPath
    ? `${PAWCHIVE_PREVIEW_ORIGIN}/thumbnail/data/${normalizedPath}`
    : null
}

module.exports = {
  PAWCHIVE_HOST,
  PAWCHIVE_MEDIA_HOST,
  PAWCHIVE_MEDIA_ORIGIN,
  PAWCHIVE_ORIGIN,
  PAWCHIVE_PREVIEW_HOST,
  PAWCHIVE_PREVIEW_ORIGIN,
  getPawchiveMediaUrl,
  getPawchivePreviewUrl,
  getPawchiveProfileUrl,
  getPawchiveUserUrl,
  isPawchiveFileDataUrl,
  isPawchiveHost,
  isPawchiveOrKemonoHost,
  shouldUsePawchiveDeadMediaMatch,
}

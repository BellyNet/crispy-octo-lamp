'use strict'

const PAWCHIVE_ORIGIN = 'https://pawchive.pw'
const PAWCHIVE_HOST = 'pawchive.pw'
const PAWCHIVE_MEDIA_ORIGIN = 'https://img.pawchive.pw'
const PAWCHIVE_MEDIA_HOST = 'img.pawchive.pw'
const LEGACY_PAWCHIVE_HOSTS = new Set(['pawchive.st', 'img.pawchive.st'])

function isHostOrSubdomain(host, parentHost) {
  return host === parentHost || host.endsWith(`.${parentHost}`)
}

function isPawchiveHost(hostname) {
  const host = String(hostname || '').toLowerCase()
  return (
    isHostOrSubdomain(host, PAWCHIVE_HOST) ||
    isHostOrSubdomain(host, PAWCHIVE_MEDIA_HOST) ||
    LEGACY_PAWCHIVE_HOSTS.has(host)
  )
}

function isPawchiveOrKemonoHost(hostname) {
  const host = String(hostname || '').toLowerCase()
  return isPawchiveHost(host) || host.includes('kemono')
}

function getPawchiveUserUrl(service, userId) {
  return `${PAWCHIVE_ORIGIN}/${encodeURIComponent(service)}/user/${encodeURIComponent(userId)}`
}

function getPawchiveProfileUrl(service, userId) {
  return `${PAWCHIVE_ORIGIN}/api/v1/${encodeURIComponent(service)}/user/${encodeURIComponent(userId)}/profile`
}

function getPawchiveMediaUrl(mediaPath) {
  const normalizedPath = String(mediaPath || '').replace(/^\/+/, '')
  return normalizedPath
    ? `${PAWCHIVE_MEDIA_ORIGIN}/data/${normalizedPath}`
    : null
}

function getPawchivePreviewUrl(mediaPath) {
  const normalizedPath = String(mediaPath || '').replace(/^\/+/, '')
  return normalizedPath
    ? `${PAWCHIVE_MEDIA_ORIGIN}/thumbnail/data/${normalizedPath}`
    : null
}

module.exports = {
  PAWCHIVE_HOST,
  PAWCHIVE_MEDIA_HOST,
  PAWCHIVE_MEDIA_ORIGIN,
  PAWCHIVE_ORIGIN,
  getPawchiveMediaUrl,
  getPawchivePreviewUrl,
  getPawchiveProfileUrl,
  getPawchiveUserUrl,
  isPawchiveHost,
  isPawchiveOrKemonoHost,
}

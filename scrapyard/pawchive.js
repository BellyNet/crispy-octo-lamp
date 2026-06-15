'use strict'

const PAWCHIVE_ORIGIN = 'https://pawchive.st'
const PAWCHIVE_HOST = 'pawchive.st'
const PAWCHIVE_MEDIA_ORIGIN = 'https://img.pawchive.st'

function isPawchiveOrKemonoHost(hostname) {
  const host = String(hostname || '').toLowerCase()
  return (
    host === PAWCHIVE_HOST ||
    host.endsWith(`.${PAWCHIVE_HOST}`) ||
    host.includes('kemono')
  )
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
  PAWCHIVE_MEDIA_ORIGIN,
  PAWCHIVE_ORIGIN,
  getPawchiveMediaUrl,
  getPawchivePreviewUrl,
  getPawchiveProfileUrl,
  getPawchiveUserUrl,
  isPawchiveOrKemonoHost,
}

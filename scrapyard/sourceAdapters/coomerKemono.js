'use strict'

const path = require('path')
const { normalizeMediaEntries, sanitizeToken } = require('../mediaEntries')
const mediaFileRecords = require('../mediaFileRecords')

const DEFAULT_PAGE_SIZE = 50

function parseResolvedDate(date) {
  return mediaFileRecords.parseResolvedDate(date)
}

function normalizeCreatorName(value) {
  return String(value || '')
    .trim()
    .replace(/^@+/, '')
    .toLowerCase()
}

function getPostsApiUrl(source, offset = 0, pageSize = DEFAULT_PAGE_SIZE) {
  return `${source.origin}/api/v1/${source.service}/user/${encodeURIComponent(
    source.userId
  )}/posts?o=${offset}`
}

function getPostPageUrl(source, post) {
  return `${source.origin}/${source.service}/user/${source.userId}/post/${post.id}`
}

function getMediaUrl(source, media) {
  const mediaPath = String(media?.path || '').trim()
  if (!mediaPath) return null
  if (/^https?:\/\//i.test(mediaPath)) return mediaPath
  return `${source.origin}/data${mediaPath.startsWith('/') ? mediaPath : `/${mediaPath}`}`
}

function filenameFromMediaUrl(mediaUrl) {
  try {
    const name = decodeURIComponent(path.basename(new URL(mediaUrl).pathname))
    return name && name !== 'data' ? name : null
  } catch {
    return null
  }
}

function getMediaEntriesFromPost(source, post, options = {}) {
  const postPublishedAt = parseResolvedDate(post.published)
  const mediaPageUrl = getPostPageUrl(source, post)
  const rawEntries = []
  if (post.file?.path) rawEntries.push(post.file)
  if (Array.isArray(post.attachments)) rawEntries.push(...post.attachments)

  const normalizeUrl =
    typeof options.normalizeUrl === 'function'
      ? options.normalizeUrl
      : (value) => String(value || '').trim()
  const seen = new Set()
  const entries = rawEntries
    .map((media) => {
      const mediaUrl = getMediaUrl(source, media)
      const filename = mediaUrl ? filenameFromMediaUrl(mediaUrl) : null
      if (!mediaUrl || !filename) return null
      const key = normalizeUrl(mediaUrl)
      if (seen.has(key)) return null
      seen.add(key)
      return {
        postId: String(post.id || ''),
        title: post.title || null,
        mediaPageUrl,
        mediaPageUrls: [mediaPageUrl],
        mediaUrl,
        mediaUrls: [mediaUrl],
        filename,
        originalName: media.name || null,
        uploadedDate: postPublishedAt,
      }
    })
    .filter(Boolean)

  return normalizeMediaEntries(entries, {
    sourceSite: source.site,
    sourceService: source.service,
    sourceUserId: source.userId,
    sourceUsername: source.username || source.rawName || source.userId,
  })
}

async function findCreatorIdByName(source, fetchJson) {
  const { data: creators } = await fetchJson(`${source.origin}/api/v1/creators`)
  if (!Array.isArray(creators)) return null

  const normalizedName = normalizeCreatorName(source.userId)
  const hit = creators.find(
    (creator) =>
      creator?.service === source.service &&
      normalizeCreatorName(creator?.name) === normalizedName
  )

  return hit ? String(hit.id) : null
}

async function resolveKemonoCreatorIdForJson(source, deps = {}) {
  if (source.site !== 'kemono' || /^\d+$/.test(source.userId)) {
    return false
  }
  if (typeof deps.fetchJson !== 'function') {
    throw new Error('resolveKemonoCreatorIdForJson requires fetchJson')
  }

  const resolvedId = await findCreatorIdByName(source, deps.fetchJson).catch(
    () => null
  )
  if (!resolvedId) {
    throw new Error(
      `Kemono rejected "${source.userId}" for ${source.service}. Kemono creator URLs usually need the numeric creator ID, and that username was not found in /api/v1/creators.`
    )
  }

  deps.logger?.log?.(
    `Resolved Kemono creator ${source.userId} -> ${resolvedId}`
  )
  source.userId = resolvedId
  source.rawName = sanitizeToken(resolvedId)
  return true
}

async function preflightCoomerKemonoSource(source, page = 0, deps = {}) {
  if (typeof deps.fetchJson !== 'function') {
    throw new Error('preflightCoomerKemonoSource requires fetchJson')
  }
  const pageSize = deps.pageSize || DEFAULT_PAGE_SIZE
  const offset = page * pageSize
  const apiUrl = getPostsApiUrl(source, offset, pageSize)
  const { data, byteLength } = await deps.fetchJson(apiUrl)

  if (!Array.isArray(data)) {
    throw new Error(
      `Expected ${apiUrl} to return a JSON post array, got ${typeof data}`
    )
  }

  const newest = data
    .map((post) => parseResolvedDate(post?.published))
    .filter(Boolean)
    .sort((a, b) => b.getTime() - a.getTime())[0]

  return {
    apiUrl,
    byteLength,
    postCount: data.length,
    newest,
    firstPostId: data[0]?.id ? String(data[0].id) : null,
  }
}

async function fetchCoomerKemonoPosts(source, options = {}, deps = {}) {
  if (typeof deps.fetchJson !== 'function') {
    throw new Error('fetchCoomerKemonoPosts requires fetchJson')
  }

  const pageSize = deps.pageSize || DEFAULT_PAGE_SIZE
  const posts = []
  let page = options.startPage || 0

  while (true) {
    if (options.endPage !== null && page > options.endPage) break
    const offset = page * pageSize
    const apiUrl = getPostsApiUrl(source, offset, pageSize)

    let pagePosts
    try {
      const pageResult = await deps.fetchJson(apiUrl)
      pagePosts = pageResult.data
    } catch (err) {
      if (source.site === 'kemono' && !/^\d+$/.test(source.userId)) {
        await resolveKemonoCreatorIdForJson(source, deps)
        continue
      }
      throw err
    }

    if (!Array.isArray(pagePosts) || pagePosts.length === 0) break
    const selectedPagePosts =
      Number.isFinite(options.maxPosts) && options.maxPosts > 0
        ? pagePosts.slice(0, Math.max(options.maxPosts - posts.length, 0))
        : pagePosts
    posts.push(
      ...selectedPagePosts.map((post) => ({
        ...post,
        mediaEntries: getMediaEntriesFromPost(source, post, deps),
      }))
    )
    if (
      Number.isFinite(options.maxPosts) &&
      options.maxPosts > 0 &&
      posts.length >= options.maxPosts
    ) {
      break
    }
    if (pagePosts.length < pageSize) break
    page += 1
  }

  return posts
}

module.exports = {
  DEFAULT_PAGE_SIZE,
  fetchCoomerKemonoPosts,
  findCreatorIdByName,
  getMediaEntriesFromPost,
  getMediaUrl,
  getPostPageUrl,
  getPostsApiUrl,
  normalizeCreatorName,
  preflightCoomerKemonoSource,
  resolveKemonoCreatorIdForJson,
}

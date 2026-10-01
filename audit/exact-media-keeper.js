'use strict'

const fs = require('fs')
const path = require('path')
const config = require('../scrapyard/config')

const localRoot = config.datasetDir
const nasRoot = config.nasDatasetDir
const modelCache = new Map()

function readJson(filePath) {
  try { return JSON.parse(fs.readFileSync(filePath, 'utf8')) } catch { return null }
}

function modelData(modelName) {
  if (modelCache.has(modelName)) return modelCache.get(modelName)
  const localSidecar = readJson(path.join(localRoot, modelName, '.media-dates.json'))
  const nasSidecar = readJson(path.join(nasRoot, modelName, '.media-dates.json'))
  const localSeen = readJson(path.join(localRoot, modelName, 'log', 'milkmaid-seen-media-index.json'))
  const nasSeen = readJson(path.join(nasRoot, modelName, 'log', 'milkmaid-seen-media-index.json'))
  const seenCounts = new Map()
  for (const seen of [localSeen, nasSeen]) {
    // Local/NAS are mirrors; use the largest count, not their sum.
    const counts = new Map()
    for (const section of ['mediaPageUrls', 'mediaUrls']) {
      for (const item of Object.values(seen?.[section] || {})) {
        if (!item?.relativePath) continue
        counts.set(item.relativePath, (counts.get(item.relativePath) || 0) + 1)
      }
    }
    for (const [relativePath, count] of counts) {
      seenCounts.set(relativePath, Math.max(seenCounts.get(relativePath) || 0, count))
    }
  }
  const data = { localSidecar, nasSidecar, seenCounts }
  modelCache.set(modelName, data)
  return data
}

function rank(file) {
  const { localSidecar, nasSidecar, seenCounts } = modelData(file.modelName)
  const key = `${file.bucket}/${file.filename}`
  const row = localSidecar?.[key] || nasSidecar?.[key] || null
  const source = row?.source || {}
  const seenReferences = seenCounts.get(file.relativePath) || 0
  const score =
    (row ? 100 : 0) +
    (source.mediaPageUrl ? 30 : 0) +
    (source.postId ? 20 : 0) +
    (source.mediaUrl ? 12 : 0) +
    (source.title || source.text ? 10 : 0) +
    (row?.uploaded ? 8 : 0) +
    (row?.resolved?.date ? 8 : 0) +
    (row?.video || row?.image ? 6 : 0) +
    (Array.isArray(row?.comments) && row.comments.length ? 3 : 0) +
    Math.min(seenReferences, 20) * 5 +
    (file.availableOnNas ? 5 : 0)
  const historyDate = Date.parse(row?.resolved?.date || row?.uploaded || row?.filename || '')
  return {
    score,
    seenReferences,
    hasSidecar: Boolean(row),
    hasSourcePage: Boolean(source.mediaPageUrl),
    hasPostId: Boolean(source.postId),
    historyDate: Number.isFinite(historyDate) ? historyDate : null,
  }
}

function chooseKeeper(files) {
  return [...files].map((file) => ({ file, rank: rank(file) })).sort((a, b) =>
    b.rank.score - a.rank.score ||
    (a.rank.historyDate ?? Infinity) - (b.rank.historyDate ?? Infinity) ||
    a.file.relativePath.localeCompare(b.file.relativePath))[0]
}

module.exports = { chooseKeeper, rank, localRoot, nasRoot }

'use strict'

const fs = require('fs')
const path = require('path')

function parseResolvedDate(date) {
  if (date instanceof Date && !isNaN(date.getTime())) return date
  if (typeof date === 'string' || typeof date === 'number') {
    const parsed = new Date(date)
    if (!isNaN(parsed.getTime())) return parsed
  }
  return null
}

function resolveEffectiveFileDate(date, fallbackDate = new Date()) {
  return (
    parseResolvedDate(date) || parseResolvedDate(fallbackDate) || new Date()
  )
}

function applyFileTimestamp(filePath, date, fallbackDate) {
  const effectiveDate = resolveEffectiveFileDate(date, fallbackDate)
  const ts = effectiveDate.getTime() / 1000
  fs.utimesSync(filePath, ts, ts)
  return effectiveDate
}

function buildHashMetadata({
  datasetDir,
  source,
  modelName,
  absolutePath,
  mediaType,
  sizeBytes,
  modifiedAt,
  extra = {},
}) {
  const relativePath = path
    .relative(datasetDir, absolutePath)
    .replace(/\\/g, '/')
  const parts = relativePath.split('/').filter(Boolean)

  return {
    root: 'dataset',
    model: modelName || parts[0] || null,
    bucket: path.basename(path.dirname(absolutePath)) || parts[1] || null,
    relativePath,
    filename: path.basename(absolutePath),
    mediaType,
    sizeBytes: Number.isFinite(sizeBytes) && sizeBytes >= 0 ? sizeBytes : null,
    modifiedAt: parseResolvedDate(modifiedAt)?.toISOString?.() || null,
    source,
    ...extra,
  }
}

module.exports = {
  parseResolvedDate,
  resolveEffectiveFileDate,
  applyFileTimestamp,
  buildHashMetadata,
}

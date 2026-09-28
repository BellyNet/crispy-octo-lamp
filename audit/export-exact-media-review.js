'use strict'

const fs = require('fs')
const path = require('path')

const inputPath = path.resolve(
  process.argv[2] || path.join(__dirname, '..', 'tmp', 'exact-media-duplicates-full-20260928.json')
)
const outputPath = path.resolve(
  process.argv[3] || path.join(__dirname, '..', 'tmp', 'exact-media-review-latest.json')
)
const liveOnly = process.argv.includes('--live')

const audit = JSON.parse(fs.readFileSync(inputPath, 'utf8'))
if (
  audit.mode !== 'exact_bytes_md5_size_prefilter' ||
  audit.summary?.scanErrors !== 0 ||
  audit.summary?.hashErrors !== 0
) {
  throw new Error('The exact-media audit must finish without scan or hash errors.')
}

function toFile(record) {
  return {
    relativePath: record.relativePath,
    modelName: record.modelName,
    bucket: record.bucket,
    filename: record.filename,
    availableOnNas: record.locations.some((location) => location.rootType === 'nas'),
  }
}

const sameModelGroups = []
const crossModelGroups = []
let excludedTrashRecords = 0
let removedSinceAuditRecords = 0
let sameModelHashGroups = 0
for (const group of audit.duplicateGroups) {
  // The raw audit also walks .dashboard-trash, which is a recovery archive,
  // not a model. Never offer archived files as live cleanup candidates.
  const liveRecords = group.records.filter((record) => {
    const validModelPath = !record.modelName.startsWith('.') &&
      ['images', 'gif', 'webm'].includes(record.bucket)
    if (!validModelPath) { excludedTrashRecords++; return false }
    const live = !liveOnly || record.locations.some((location) => fs.existsSync(location.absolutePath))
    if (!live) removedSinceAuditRecords++
    return live
  })
  if (liveRecords.length < 2) continue
  const byModel = new Map()
  for (const record of liveRecords) {
    const key = record.modelName.toLowerCase()
    if (!byModel.has(key)) byModel.set(key, [])
    byModel.get(key).push(record)
  }
  if ([...byModel.values()].some((records) => records.length > 1)) sameModelHashGroups++
  for (const records of byModel.values()) {
    if (records.length < 2) continue
    sameModelGroups.push({
      id: `${group.md5}:${records[0].modelName.toLowerCase()}`,
      modelName: records[0].modelName,
      mediaType: group.mediaType,
      sizeBytes: group.sizeBytes,
      redundantBytes: (records.length - 1) * group.sizeBytes,
      files: records.map(toFile),
    })
  }
  if (byModel.size > 1) {
    const models = [...byModel.values()].map((records) => records[0].modelName).sort()
    crossModelGroups.push({
      id: group.md5,
      mediaType: group.mediaType,
      sizeBytes: group.sizeBytes,
      models,
      crossModelOnly: ![...byModel.values()].some((records) => records.length > 1),
      files: liveRecords.map(toFile),
    })
  }
}

sameModelGroups.sort(
  (a, b) => b.redundantBytes - a.redundantBytes || a.modelName.localeCompare(b.modelName)
)
crossModelGroups.sort(
  (a, b) => b.sizeBytes - a.sizeBytes || a.id.localeCompare(b.id)
)

const summary = {
  ...audit.summary,
  sameModelDuplicateGroups: sameModelHashGroups,
  sameModelRedundantCopies: sameModelGroups.reduce((sum, group) => sum + group.files.length - 1, 0),
  conservativeReclaimableBytes: sameModelGroups.reduce((sum, group) => sum + group.redundantBytes, 0),
  crossModelGroups: crossModelGroups.length,
  excludedTrashRecords,
  removedSinceAuditRecords,
  sameModelReviewGroups: sameModelGroups.length,
}
if (summary.sameModelRedundantCopies > audit.summary.sameModelRedundantCopies ||
    summary.conservativeReclaimableBytes > audit.summary.conservativeReclaimableBytes ||
    summary.crossModelGroups > audit.summary.crossModelGroups) {
  throw new Error('Filtered review totals exceed the exact-media audit.')
}

const review = {
  version: 2,
  generatedAt: new Date().toISOString(),
  auditedAt: audit.generatedAt,
  liveOnly,
  readOnly: true,
  summary,
  sameModelGroups,
  crossModelGroups,
}
fs.mkdirSync(path.dirname(outputPath), { recursive: true })
fs.writeFileSync(outputPath, JSON.stringify(review))
console.log(
  `Exported ${sameModelGroups.length} same-model review groups and ${crossModelGroups.length} cross-model groups to ${outputPath}`
)

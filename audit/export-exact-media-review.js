'use strict'

const fs = require('fs')
const path = require('path')

const inputPath = path.resolve(
  process.argv[2] || path.join(__dirname, '..', 'tmp', 'exact-media-duplicates-full-20260928.json')
)
const outputPath = path.resolve(
  process.argv[3] || path.join(__dirname, '..', 'tmp', 'exact-media-review-latest.json')
)

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
for (const group of audit.duplicateGroups) {
  const byModel = new Map()
  for (const record of group.records) {
    const key = record.modelName.toLowerCase()
    if (!byModel.has(key)) byModel.set(key, [])
    byModel.get(key).push(record)
  }
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
  if (group.crossModel) {
    crossModelGroups.push({
      id: group.md5,
      mediaType: group.mediaType,
      sizeBytes: group.sizeBytes,
      models: group.models,
      crossModelOnly: !group.hasSameModelDuplicates,
      files: group.records.map(toFile),
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
  sameModelReviewGroups: sameModelGroups.length,
}
if (
  sameModelGroups.reduce((sum, group) => sum + group.redundantBytes, 0) !==
    summary.conservativeReclaimableBytes ||
  sameModelGroups.reduce((sum, group) => sum + group.files.length - 1, 0) !==
    summary.sameModelRedundantCopies ||
  crossModelGroups.length !== summary.crossModelGroups
) {
  throw new Error('Review export totals do not match the exact-media audit.')
}

const review = {
  version: 1,
  generatedAt: new Date().toISOString(),
  auditedAt: audit.generatedAt,
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

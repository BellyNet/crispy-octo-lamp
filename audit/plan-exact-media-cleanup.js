'use strict'

// Read-only plan for the live duplicate review. Cross-model files are only
// selected when the dashboard has an explicit keeper decision for that hash.
const fs = require('fs')
const path = require('path')
const { chooseKeeper, rank } = require('./exact-media-keeper')

const reportPath = path.resolve(process.argv[2] || path.join(__dirname, '..', 'tmp', 'exact-media-review-latest.json'))
const decisionsPath = path.resolve(process.argv[3] || 'Z:\\dashboard-cache\\exact-duplicate-decisions.json')
const outputPath = path.resolve(process.argv[4] || path.join(__dirname, '..', 'tmp', 'exact-media-cleanup-plan-latest.json'))
const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'))
if (report.version !== 2 || !report.readOnly || report.summary.scanErrors !== 0 || report.summary.hashErrors !== 0) {
  throw new Error('A clean, filtered exact-media review report is required')
}
let decisions = { auditedAt: report.auditedAt, groups: {} }
if (fs.existsSync(decisionsPath)) decisions = JSON.parse(fs.readFileSync(decisionsPath, 'utf8'))
if (decisions.auditedAt !== report.auditedAt) throw new Error('Dashboard choices belong to a different audit')

const crossByHash = new Map(report.crossModelGroups.map((group) => [group.id, group]))
const sameByHash = new Map()
for (const group of report.sameModelGroups) {
  const hash = group.id.split(':')[0]
  if (!sameByHash.has(hash)) sameByHash.set(hash, [])
  sameByHash.get(hash).push(group)
}
const hashes = new Set([...crossByHash.keys(), ...sameByHash.keys()])
const operations = []
let undecidedCrossGroups = 0
for (const hash of hashes) {
  const cross = crossByHash.get(hash)
  const same = sameByHash.get(hash) || []
  const models = cross?.models || same.map((group) => group.modelName)
  const keepModel = cross ? decisions.groups?.[hash]?.keepModel : null
  const validCrossDecision = cross && models.includes(keepModel)
  if (cross && !validCrossDecision) undecidedCrossGroups++
  const files = cross?.files || same.flatMap((group) => group.files)
  const byModel = new Map()
  for (const file of files) {
    if (!byModel.has(file.modelName)) byModel.set(file.modelName, [])
    byModel.get(file.modelName).push(file)
  }
  // Prefer the path with the richest sidecar and seen-URL history.
  const preferred = (list) => chooseKeeper(list).file
  const keepers = validCrossDecision
    ? [preferred(byModel.get(keepModel))]
    : [...byModel.values()].map(preferred)
  const keepPaths = new Set(keepers.map((file) => file.relativePath))
  for (const file of files) {
    if (keepPaths.has(file.relativePath)) continue
    if (cross && !validCrossDecision && byModel.get(file.modelName).length === 1) continue
    const keeper = validCrossDecision ? keepers[0] : keepers.find((item) => item.modelName === file.modelName)
    operations.push({ hash, sizeBytes: cross?.sizeBytes || same[0].sizeBytes,
      from: file.relativePath, to: keeper.relativePath,
      fromHistory: rank(file), keeperHistory: rank(keeper),
      crossModel: file.modelName !== keeper.modelName })
  }
}
const plan = {
  version: 1,
  generatedAt: new Date().toISOString(),
  auditedAt: report.auditedAt,
  readOnly: true,
  summary: {
    logicalCopiesToRemove: operations.length,
    sameModelCopiesToRemove: operations.filter((item) => !item.crossModel).length,
    chosenCrossModelCopiesToRemove: operations.filter((item) => item.crossModel).length,
    undecidedCrossGroups,
    potentialBytes: operations.reduce((sum, item) => sum + item.sizeBytes, 0),
  },
  operations,
}
fs.mkdirSync(path.dirname(outputPath), { recursive: true })
fs.writeFileSync(outputPath, JSON.stringify(plan, null, 2) + '\n')
console.log(JSON.stringify({ outputPath, summary: plan.summary }, null, 2))

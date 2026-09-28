'use strict'

const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const { localRoot, nasRoot } = require('./exact-media-keeper')

const backupDirs = process.argv.slice(2)
if (!backupDirs.length) throw new Error('Pass one or more cleanup backup directories')
const operations = []
const expectedProvenance = new Set()
const remaining = []
for (const backupDir of backupDirs) {
  const plan = JSON.parse(fs.readFileSync(path.join(backupDir, 'plan.json')))
  const result = JSON.parse(fs.readFileSync(path.join(backupDir, 'result.json')))
  if (!['complete', 'partial_purge'].includes(result.status)) throw new Error(`Unfinished cleanup: ${backupDir}`)
  operations.push(...plan.operations)
  const provenance = JSON.parse(fs.readFileSync(path.join(backupDir, 'removed-source-provenance.json')))
  for (const item of provenance) if (item.metadata) expectedProvenance.add(item.from)
  remaining.push(...result.failures)
}
const fromPaths = new Set(operations.map((item) => item.from))
const models = new Set(operations.map((item) => item.from.split('/')[0]))
const errors = []
let archivedProvenance = 0
for (const root of [localRoot, nasRoot]) {
  for (const operation of operations) {
    const from = path.join(root, ...operation.from.split('/'))
    const to = path.join(root, ...operation.to.split('/'))
    if (fs.existsSync(from)) errors.push(`Live redundant path: ${from}`)
    if (root === nasRoot && !fs.existsSync(to)) errors.push(`Missing NAS keeper: ${to}`)
  }
  for (const model of models) {
    const sidecarPath = path.join(root, model, '.media-dates.json')
    const sidecar = JSON.parse(fs.readFileSync(sidecarPath))
    for (const operation of operations.filter((item) => item.from.split('/')[0] === model)) {
      const from = operation.from.split('/').slice(1).join('/')
      const to = operation.to.split('/').slice(1).join('/')
      if (sidecar[from]) errors.push(`Stale sidecar row: ${root}/${operation.from}`)
      if (expectedProvenance.has(operation.from)) {
        if (!sidecar[to]?.exactDuplicateSources?.some((item) => item.relativePath === operation.from)) {
          errors.push(`Missing archived provenance: ${root}/${operation.from}`)
        } else archivedProvenance++
      }
    }
    const seenPath = path.join(root, model, 'log', 'milkmaid-seen-media-index.json')
    if (fs.existsSync(seenPath)) {
      const seen = JSON.parse(fs.readFileSync(seenPath))
      for (const section of ['mediaPageUrls', 'mediaUrls', 'deadMediaUrls', 'deadMediaPageUrls']) {
        for (const item of Object.values(seen[section] || {})) {
          if (fromPaths.has(item?.relativePath)) errors.push(`Stale seen ref: ${root}/${model}/${section}`)
        }
      }
    }
  }
  for (const fileName of ['bitwiseHashes.v2.json', 'visualHashes.v2.json']) {
    const store = JSON.parse(fs.readFileSync(path.join(root, fileName)))
    for (const entry of store.entries) {
      for (const ref of entry.refs || []) if (fromPaths.has(ref)) errors.push(`Stale ${fileName} ref: ${ref}`)
    }
  }
  const nasIndex = JSON.parse(fs.readFileSync(path.join(root, 'nas-mp4-index.v1.json')))
  for (const ref of nasIndex.entries) if (fromPaths.has(ref)) errors.push(`Stale NAS index ref: ${ref}`)
}
for (const model of models) {
  for (const relative of ['.media-dates.json', path.join('log', 'milkmaid-seen-media-index.json')]) {
    const local = path.join(localRoot, model, relative)
    const nas = path.join(nasRoot, model, relative)
    if (fs.existsSync(local) && fs.existsSync(nas)) {
      const a = crypto.createHash('sha256').update(fs.readFileSync(local)).digest('hex')
      const b = crypto.createHash('sha256').update(fs.readFileSync(nas)).digest('hex')
      if (a !== b) errors.push(`Local/NAS metadata mismatch: ${model}/${relative}`)
    }
  }
}
const output = {
  verifiedAt: new Date().toISOString(),
  logicalPathsRemoved: operations.length,
  models: models.size,
  archivedProvenanceRecords: expectedProvenance.size,
  archivedProvenanceCopiesVerified: archivedProvenance,
  stagedPurgeFailures: remaining.length,
  errors: errors.slice(0, 100),
  errorCount: errors.length,
}
console.log(JSON.stringify(output, null, 2))
if (errors.length) process.exitCode = 1

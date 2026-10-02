'use strict'

// Remove same-model exact-byte copies from the live dataset. All source and
// keeper files are rehashed before any move. Media is first staged in a hidden
// quarantine, metadata/index backups and a move journal are saved, and only
// then are staged bytes purged. Cross-model decisions are never applied here.
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const { localRoot, nasRoot } = require('./exact-media-keeper')
const { syncModelMetadataToNas } = require('../scrapyard/nasSync')

const args = process.argv.slice(2)
const apply = args.includes('--apply')
const modelArg = args.find((arg) => arg.startsWith('--model='))
const modelFilter = modelArg ? modelArg.slice('--model='.length) : null
const planPath = path.join(__dirname, '..', 'tmp', 'exact-media-cleanup-plan-latest.json')
const auditPath = path.join(__dirname, '..', 'tmp', 'exact-media-duplicates-full-20260928.json')
const plan = JSON.parse(fs.readFileSync(planPath, 'utf8'))
const audit = JSON.parse(fs.readFileSync(auditPath, 'utf8'))
if (plan.auditedAt !== audit.generatedAt || audit.summary.scanErrors || audit.summary.hashErrors || audit.summary.mirrorConflicts) {
  throw new Error('Cleanup plan does not match a clean exact-byte audit')
}
const operations = plan.operations.filter((operation) =>
  !operation.crossModel && (!modelFilter || operation.from.split('/')[0] === modelFilter))
if (!operations.length) throw new Error('No same-model operations selected')
const groupByHash = new Map(audit.duplicateGroups.map((group) => [group.md5, group]))
const fileByPath = new Map()
for (const operation of operations) {
  const group = groupByHash.get(operation.hash)
  if (!group || group.sizeBytes !== operation.sizeBytes) throw new Error(`Stale hash group: ${operation.hash}`)
  for (const record of group.records) fileByPath.set(`${operation.hash}:${record.relativePath}`, record)
  if (!fileByPath.has(`${operation.hash}:${operation.from}`) ||
      !fileByPath.has(`${operation.hash}:${operation.to}`)) throw new Error(`Path absent from audit: ${operation.from}`)
  if (operation.from === operation.to || operation.from.split('/')[0] !== operation.to.split('/')[0] ||
      operation.from.startsWith('.') || operation.to.startsWith('.')) throw new Error(`Unsafe operation: ${operation.from}`)
}
const stamp = new Date().toISOString().replace(/[:.]/g, '-')
const backupDir = path.join(__dirname, '..', 'tmp', `exact-media-cleanup-backup-${stamp}`)
const reportPath = path.join(backupDir, 'result.json')
require('../scrapyard/datasetLocation').assertSeparateFromNas('apply-same-model-exact-cleanup', localRoot, nasRoot)
const roots = [{ type: 'local', root: localRoot }, { type: 'nas', root: nasRoot }]
const models = [...new Set(operations.map((operation) => operation.from.split('/')[0]))]
const fromPaths = new Set(operations.map((operation) => operation.from))
const replacementByPath = new Map(operations.map((operation) => [operation.from, operation.to]))
const physical = []
const keepPhysical = []
for (const operation of operations) {
  const fromRecord = fileByPath.get(`${operation.hash}:${operation.from}`)
  const toRecord = fileByPath.get(`${operation.hash}:${operation.to}`)
  for (const location of fromRecord.locations) physical.push({
    role: 'remove', relativePath: operation.from, hash: operation.hash,
    sizeBytes: operation.sizeBytes, absolutePath: location.absolutePath, rootType: location.rootType,
  })
  for (const location of toRecord.locations) keepPhysical.push({
    role: 'keep', relativePath: operation.to, hash: operation.hash,
    sizeBytes: operation.sizeBytes, absolutePath: location.absolutePath, rootType: location.rootType,
  })
}
const uniqueKeep = [...new Map(keepPhysical.map((item) => [item.absolutePath.toLowerCase(), item])).values()]
const summary = {
  mode: apply ? 'apply' : 'dry-run', models: models.length, logicalPaths: operations.length,
  physicalCopies: physical.length, physicalBytes: physical.reduce((sum, item) => sum + item.sizeBytes, 0),
  logicalBytes: operations.reduce((sum, item) => sum + item.sizeBytes, 0),
  keeperCopiesToVerify: uniqueKeep.length, backupDir: apply ? backupDir : null,
}
console.log(JSON.stringify(summary, null, 2))
if (!apply) process.exit(0)

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true })
  fs.writeFileSync(filePath, JSON.stringify(value) + '\n')
}
function readJson(filePath) { return JSON.parse(fs.readFileSync(filePath, 'utf8')) }
function sameFile(a, b) { return fs.readFileSync(a).equals(fs.readFileSync(b)) }
function backup(filePath, type) {
  if (!fs.existsSync(filePath)) return null
  const root = type === 'nas' ? nasRoot : localRoot
  const relative = path.relative(root, filePath)
  if (relative.startsWith('..') || path.isAbsolute(relative)) throw new Error(`Backup path outside root: ${filePath}`)
  const target = path.join(backupDir, 'metadata', type, relative)
  fs.mkdirSync(path.dirname(target), { recursive: true })
  fs.copyFileSync(filePath, target)
  return { original: filePath, backup: target }
}
function replaceJson(filePath, data) {
  // In-place writing works for Windows hidden sidecars. The original is in
  // backupDir before this call, so a failed write can be restored.
  fs.writeFileSync(filePath, JSON.stringify(data))
  if (!sameFileJson(filePath, data)) throw new Error(`Metadata verification failed: ${filePath}`)
}
function sameFileJson(filePath, data) {
  return JSON.stringify(readJson(filePath)) === JSON.stringify(data)
}
async function hashFile(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('md5')
    const stream = fs.createReadStream(filePath)
    stream.on('data', (chunk) => hash.update(chunk))
    stream.on('error', reject)
    stream.on('end', () => resolve(hash.digest('hex')))
  })
}
async function verifyFiles() {
  const all = [...physical, ...uniqueKeep]
  for (let index = 0; index < all.length; index++) {
    const item = all[index]
    const stat = fs.statSync(item.absolutePath)
    if (!stat.isFile() || stat.size !== item.sizeBytes || await hashFile(item.absolutePath) !== item.hash) {
      throw new Error(`File changed since audit: ${item.absolutePath}`)
    }
    if ((index + 1) % 100 === 0 || index + 1 === all.length) {
      console.log(`Rehashed ${index + 1}/${all.length} physical source/keeper files`)
    }
  }
}
function validateMetadataMirrors() {
  for (const model of models) {
    for (const relative of ['.media-dates.json', path.join('log', 'milkmaid-seen-media-index.json')]) {
      const a = path.join(localRoot, model, relative)
      const b = path.join(nasRoot, model, relative)
      if (fs.existsSync(a) !== fs.existsSync(b) || (fs.existsSync(a) && !sameFile(a, b))) {
        throw new Error(`Local/NAS metadata differs for ${model}/${relative}`)
      }
    }
  }
}
function backupMetadata() {
  const backups = []
  for (const model of models) {
    for (const relative of ['.media-dates.json', path.join('log', 'milkmaid-seen-media-index.json')]) {
      for (const root of roots) {
        const item = backup(path.join(root.root, model, relative), root.type)
        if (item) backups.push(item)
      }
    }
  }
  for (const fileName of ['bitwiseHashes.v2.json', 'visualHashes.v2.json', 'nas-mp4-index.v1.json']) {
    for (const root of roots) {
      const item = backup(path.join(root.root, fileName), root.type)
      if (item) backups.push(item)
    }
  }
  writeJson(path.join(backupDir, 'metadata-backups.json'), backups)
  return backups
}
function updateSidecarsAndSeen() {
  const provenance = []
  let seenRemapped = 0
  for (const model of models) {
    const modelOps = operations.filter((operation) => operation.from.split('/')[0] === model)
    const sidecarPath = path.join(localRoot, model, '.media-dates.json')
    const sidecar = readJson(sidecarPath)
    for (const operation of modelOps) {
      const fromKey = operation.from.split('/').slice(1).join('/')
      const toKey = operation.to.split('/').slice(1).join('/')
      const old = sidecar[fromKey] ? structuredClone(sidecar[fromKey]) : null
      const hadKeeper = Boolean(sidecar[toKey])
      const keeper = sidecar[toKey] || (old ? structuredClone(old) : null)
      provenance.push({ from: operation.from, to: operation.to, hash: operation.hash, metadata: old })
      if (keeper && old && hadKeeper) {
        if (!Array.isArray(keeper.exactDuplicateSources)) keeper.exactDuplicateSources = []
        keeper.exactDuplicateSources.push({ relativePath: operation.from, metadata: old })
      }
      if (keeper) sidecar[toKey] = keeper
      delete sidecar[fromKey]
    }
    replaceJson(sidecarPath, sidecar)
    const metadataSync = syncModelMetadataToNas({ modelName: model, datasetDir: localRoot, nasDatasetDir: nasRoot })
    if (metadataSync.failed || !sameFile(sidecarPath, path.join(nasRoot, model, '.media-dates.json'))) {
      throw new Error(`Sidecar NAS sync failed for ${model}: ${JSON.stringify(metadataSync.failures)}`)
    }
    const seenPath = path.join(localRoot, model, 'log', 'milkmaid-seen-media-index.json')
    if (fs.existsSync(seenPath)) {
      const seen = readJson(seenPath)
      const replacements = new Map(modelOps.map((operation) => [operation.from, operation.to]))
      for (const section of ['mediaPageUrls', 'mediaUrls', 'deadMediaUrls', 'deadMediaPageUrls']) {
        for (const entry of Object.values(seen[section] || {})) {
          const target = replacements.get(entry?.relativePath)
          if (!target) continue
          entry.relativePath = target
          entry.filename = path.basename(target)
          seenRemapped++
        }
      }
      seen.updatedAt = new Date().toISOString()
      replaceJson(seenPath, seen)
      fs.copyFileSync(seenPath, path.join(nasRoot, model, 'log', 'milkmaid-seen-media-index.json'))
      if (!sameFile(seenPath, path.join(nasRoot, model, 'log', 'milkmaid-seen-media-index.json'))) {
        throw new Error(`Seen index NAS sync failed for ${model}`)
      }
    }
  }
  writeJson(path.join(backupDir, 'removed-source-provenance.json'), provenance)
  return { provenanceRows: provenance.filter((item) => item.metadata).length, seenRemapped }
}
function updateIndex(fileName, mutate) {
  const local = path.join(localRoot, fileName)
  const nas = path.join(nasRoot, fileName)
  const value = readJson(local)
  mutate(value)
  replaceJson(local, value)
  fs.copyFileSync(local, nas)
  if (!sameFile(local, nas)) throw new Error(`NAS index sync failed: ${fileName}`)
}
function updateGlobalIndexes() {
  for (const fileName of ['bitwiseHashes.v2.json', 'visualHashes.v2.json']) {
    updateIndex(fileName, (store) => {
      store.entries = store.entries.map((entry) => {
        const nextRefs = (entry.refs || []).filter((ref) => !fromPaths.has(ref))
        // A keeper already has its bitwise ref from the verified backfill.
        if (fileName.startsWith('visual')) {
          for (const ref of entry.refs || []) {
            const keeper = replacementByPath.get(ref)
            if (keeper && !nextRefs.includes(keeper)) nextRefs.push(keeper)
          }
        }
        return { ...entry, refs: [...new Set(nextRefs)].sort() }
      }).filter((entry) => entry.refs.length > 0)
      store.entryCount = store.entries.length
    })
  }
  updateIndex('nas-mp4-index.v1.json', (store) => {
    store.entries = store.entries.filter((entry) => !fromPaths.has(entry))
    store.entryCount = store.entries.length
    store.updatedAt = new Date().toISOString()
  })
}
function stageMedia() {
  const moves = physical.map((item) => {
    const root = item.rootType === 'nas' ? nasRoot : localRoot
    const quarantine = path.join(root, '.exact-duplicate-quarantine', stamp, ...item.relativePath.split('/'))
    if (!path.resolve(quarantine).startsWith(path.resolve(root) + path.sep)) throw new Error('Quarantine escaped dataset root')
    return { from: item.absolutePath, to: quarantine, sizeBytes: item.sizeBytes, relativePath: item.relativePath }
  })
  writeJson(path.join(backupDir, 'move-journal.json'), moves)
  for (let index = 0; index < moves.length; index++) {
    const move = moves[index]
    fs.mkdirSync(path.dirname(move.to), { recursive: true })
    if (fs.existsSync(move.to)) throw new Error(`Quarantine collision: ${move.to}`)
    fs.renameSync(move.from, move.to)
    if ((index + 1) % 100 === 0 || index + 1 === moves.length) console.log(`Staged ${index + 1}/${moves.length} physical duplicates`)
  }
  return moves
}
function rollback(backups, moves) {
  for (const item of backups) fs.copyFileSync(item.backup, item.original)
  for (const move of [...moves].reverse()) {
    if (fs.existsSync(move.to) && !fs.existsSync(move.from)) {
      fs.mkdirSync(path.dirname(move.from), { recursive: true })
      fs.renameSync(move.to, move.from)
    }
  }
}
async function main() {
  fs.mkdirSync(backupDir, { recursive: true })
  writeJson(path.join(backupDir, 'plan.json'), { auditedAt: plan.auditedAt, operations })
  validateMetadataMirrors()
  console.log('Rehashing every source and keeper copy before staging...')
  await verifyFiles()
  const backups = backupMetadata()
  let moves = []
  let metadata
  try {
    moves = stageMedia()
    metadata = updateSidecarsAndSeen()
    updateGlobalIndexes()
    for (const operation of operations) {
      for (const root of roots) {
        if (fs.existsSync(path.join(root.root, ...operation.from.split('/')))) {
          throw new Error(`Removed path still exists in live dataset: ${operation.from}`)
        }
      }
      if (![localRoot, nasRoot].some((root) => fs.existsSync(path.join(root, ...operation.to.split('/'))))) {
        throw new Error(`Keeper disappeared: ${operation.to}`)
      }
    }
  } catch (error) {
    console.error(`Cleanup failed before purge: ${error.message}; restoring staged media and metadata`)
    rollback(backups, moves.length ? moves : readJson(path.join(backupDir, 'move-journal.json')))
    throw error
  }
  let purged = 0
  let purgedBytes = 0
  const failures = []
  for (const move of moves) {
    try { fs.unlinkSync(move.to); purged++; purgedBytes += move.sizeBytes }
    catch (error) { failures.push({ path: move.to, error: error.message }) }
  }
  const result = { ...summary, status: failures.length ? 'partial_purge' : 'complete',
    purged, purgedBytes, failures, ...metadata, completedAt: new Date().toISOString() }
  writeJson(reportPath, result)
  console.log(JSON.stringify(result, null, 2))
  if (failures.length) process.exitCode = 1
}
main().catch((error) => { console.error(error.stack || error.message); process.exitCode = 1 })

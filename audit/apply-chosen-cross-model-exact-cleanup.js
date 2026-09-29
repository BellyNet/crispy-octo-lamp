'use strict'

// Apply only explicit cross-model keeper choices from an exact-byte audit.
// Rehash every source and keeper, journal reversible moves, and back up every
// touched index before removing any live path. Run only while scrapers are idle.
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const { execFileSync } = require('child_process')
const rootsFromKeeper = require('./exact-media-keeper')

const args = process.argv.slice(2)
const apply = args.includes('--apply')
const option = (name, fallback) => args.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3) || fallback
const localRoot = path.resolve(option('local-root', rootsFromKeeper.localRoot))
const nasRoot = path.resolve(option('nas-root', rootsFromKeeper.nasRoot))
const planPath = path.resolve(option('plan', path.join(__dirname, '..', 'tmp', 'exact-media-cleanup-plan-latest.json')))
const auditPath = path.resolve(option('audit', path.join(__dirname, '..', 'tmp', 'exact-media-duplicates-full-20260928.json')))
const decisionsPath = path.resolve(option('decisions', 'Z:\\dashboard-cache\\exact-duplicate-decisions.json'))
const reviewPath = option('review', null)
const plan = JSON.parse(fs.readFileSync(planPath, 'utf8'))
const audit = JSON.parse(fs.readFileSync(auditPath, 'utf8'))
if (plan.auditedAt !== audit.generatedAt || audit.summary.scanErrors || audit.summary.hashErrors || audit.summary.mirrorConflicts) {
  throw new Error('Cleanup plan does not match a clean exact-byte audit')
}
if (!plan.decisionsDigest) throw new Error('Cleanup plan has no dashboard decision snapshot')
const operations = plan.operations.filter((operation) => operation.crossModel)
if (!operations.length) throw new Error('No chosen cross-model operations selected')
function validateDecisions() {
  const raw = fs.readFileSync(decisionsPath)
  const digest = crypto.createHash('sha256').update(raw).digest('hex')
  if (digest !== plan.decisionsDigest) throw new Error('Dashboard decisions changed since planning')
  const decisions = JSON.parse(raw)
  if (decisions.auditedAt !== plan.auditedAt) throw new Error('Dashboard decisions belong to another audit')
  for (const operation of operations) {
    if (decisions.groups?.[operation.hash]?.keepModel !== operation.to.split('/')[0]) {
      throw new Error(`Keeper choice changed for ${operation.hash}`)
    }
  }
}
validateDecisions()
const groupByHash = new Map(audit.duplicateGroups.map((group) => [group.md5, group]))
const fileByPath = new Map()
for (const operation of operations) {
  const group = groupByHash.get(operation.hash)
  if (!group || group.sizeBytes !== operation.sizeBytes) throw new Error(`Stale hash group: ${operation.hash}`)
  for (const record of group.records) fileByPath.set(`${operation.hash}:${record.relativePath}`, record)
  if (!fileByPath.has(`${operation.hash}:${operation.from}`) ||
      !fileByPath.has(`${operation.hash}:${operation.to}`)) throw new Error(`Path absent from audit: ${operation.from}`)
  for (const relativePath of [operation.from, operation.to]) {
    const segments = relativePath.split('/')
    if (segments.length !== 3 || segments.some((part) => !part || part === '.' || part === '..' || /[\\:]/.test(part)) ||
        !['images', 'gif', 'webm'].includes(segments[1])) {
      throw new Error(`Unsafe media path: ${relativePath}`)
    }
  }
  if (operation.from === operation.to || operation.from.split('/')[0] === operation.to.split('/')[0] ||
      operation.from.startsWith('.') || operation.to.startsWith('.')) throw new Error(`Unsafe operation: ${operation.from}`)
}
const stamp = new Date().toISOString().replace(/[:.]/g, '-')
const backupDir = path.join(path.resolve(option('backup-root', path.join(__dirname, '..', 'tmp'))), `exact-media-cross-model-cleanup-backup-${stamp}`)
const reportPath = path.join(backupDir, 'result.json')
const roots = [{ type: 'local', root: localRoot }, { type: 'nas', root: nasRoot }]
const models = [...new Set(operations.flatMap((operation) => [operation.from.split('/')[0], operation.to.split('/')[0]]))]
const sourceModels = [...new Set(operations.map((operation) => operation.from.split('/')[0]))]
const fromPaths = new Set(operations.map((operation) => operation.from))
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
for (const item of [...physical, ...uniqueKeep]) {
  const root = item.rootType === 'nas' ? nasRoot : localRoot
  const expected = path.resolve(root, ...item.relativePath.split('/'))
  if (!expected.toLowerCase().startsWith(path.resolve(root).toLowerCase() + path.sep) ||
      path.resolve(item.absolutePath).toLowerCase() !== expected.toLowerCase()) {
    throw new Error(`Audit location escaped its dataset root: ${item.absolutePath}`)
  }
}
const summary = {
  mode: apply ? 'apply' : 'dry-run', models: models.length, sourceModels: sourceModels.length, logicalPaths: operations.length,
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
function validateMetadataPaths() {
  for (const model of models) {
    for (const relative of ['.media-dates.json', path.join('log', 'milkmaid-seen-media-index.json')]) {
      for (const root of roots) {
        const filePath = path.join(root.root, model, relative)
        if (fs.existsSync(filePath)) readJson(filePath)
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
  for (const root of roots) {
    for (const model of sourceModels) {
      const modelOps = operations.filter((operation) => operation.from.split('/')[0] === model)
      const sidecarPath = path.join(root.root, model, '.media-dates.json')
      if (fs.existsSync(sidecarPath)) {
        const sidecar = readJson(sidecarPath)
        for (const operation of modelOps) {
          const fromKey = operation.from.split('/').slice(1).join('/')
          const old = sidecar[fromKey] ? structuredClone(sidecar[fromKey]) : null
          if (old) {
            provenance.push({ rootType: root.type, from: operation.from, to: operation.to,
              hash: operation.hash, metadata: old })
          }
          delete sidecar[fromKey]
        }
        replaceJson(sidecarPath, sidecar)
      }
      const seenPath = path.join(root.root, model, 'log', 'milkmaid-seen-media-index.json')
      if (fs.existsSync(seenPath)) {
        const seen = readJson(seenPath)
        const replacements = new Map(modelOps.map((operation) => [operation.from, operation.to]))
        for (const section of ['mediaPageUrls', 'mediaUrls', 'deadMediaUrls', 'deadMediaPageUrls']) {
          for (const entry of Object.values(seen[section] || {})) {
            const target = replacements.get(entry?.relativePath)
            if (target) {
              entry.relativePath = target
              entry.filename = path.basename(target)
              seenRemapped++
            }
            const resolutionTarget = replacements.get(entry?.fullResolutionResolvedPath)
            if (resolutionTarget) entry.fullResolutionResolvedPath = resolutionTarget
          }
        }
        seen.updatedAt = new Date().toISOString()
        replaceJson(seenPath, seen)
      }
    }
  }
  writeJson(path.join(backupDir, 'removed-source-provenance.json'), provenance)
  return { provenanceRows: provenance.filter((item) => item.metadata).length, seenRemapped }
}
function updateIndex(fileName, mutate) {
  for (const root of roots) {
    const filePath = path.join(root.root, fileName)
    if (!fs.existsSync(filePath)) continue
    const value = readJson(filePath)
    mutate(value)
    replaceJson(filePath, value)
  }
}
function updateGlobalIndexes() {
  for (const fileName of ['bitwiseHashes.v2.json', 'visualHashes.v2.json']) {
    updateIndex(fileName, (store) => {
      store.entries = store.entries.map((entry) => {
        const nextRefs = (entry.refs || []).filter((ref) => !fromPaths.has(ref))
        // The keeper's existing hash ref remains; never invent a cross-model ref.
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
function refreshDashboardReview() {
  if (!reviewPath) return { updated: false }
  const target = path.resolve(reviewPath)
  const working = `${target}.${process.pid}.tmp`
  const exporter = path.join(__dirname, 'export-exact-media-review.js')
  execFileSync(process.execPath, [exporter, auditPath, working, '--live'], { stdio: 'pipe' })
  const review = readJson(working)
  const current = readJson(decisionsPath)
  const validIds = new Set(review.crossModelGroups.map((group) => group.id))
  const groups = Object.fromEntries(Object.entries(current.groups || {}).filter(([id]) => validIds.has(id)))
  const dashboardBackupDir = path.join(backupDir, 'dashboard')
  fs.mkdirSync(dashboardBackupDir, { recursive: true })
  const savedReview = fs.existsSync(target) ? path.join(dashboardBackupDir, 'review.json') : null
  const savedDecisions = fs.existsSync(decisionsPath) ? path.join(dashboardBackupDir, 'decisions.json') : null
  if (savedReview) fs.copyFileSync(target, savedReview)
  if (savedDecisions) fs.copyFileSync(decisionsPath, savedDecisions)
  try {
    fs.renameSync(working, target)
    const temp = `${decisionsPath}.${process.pid}.tmp`
    writeJson(temp, { ...current, auditedAt: review.auditedAt, groups })
    fs.renameSync(temp, decisionsPath)
  } catch (error) {
    if (savedReview) fs.copyFileSync(savedReview, target)
    if (savedDecisions) fs.copyFileSync(savedDecisions, decisionsPath)
    throw error
  }
  return { updated: true, remainingCrossGroups: review.crossModelGroups.length,
    remainingChoices: Object.keys(groups).length }
}
async function main() {
  fs.mkdirSync(backupDir, { recursive: true })
  writeJson(path.join(backupDir, 'plan.json'), { auditedAt: plan.auditedAt, operations })
  validateDecisions()
  validateMetadataPaths()
  console.log('Rehashing every source and keeper copy before staging...')
  await verifyFiles()
  validateDecisions()
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
  let dashboardReview = null
  try { dashboardReview = refreshDashboardReview() }
  catch (error) { failures.push({ path: reviewPath, error: `Dashboard review refresh failed: ${error.message}` }) }
  const result = { ...summary, status: failures.length ? 'partial_purge' : 'complete',
    purged, purgedBytes, failures, ...metadata, dashboardReview, completedAt: new Date().toISOString() }
  writeJson(reportPath, result)
  console.log(JSON.stringify(result, null, 2))
  if (failures.length) process.exitCode = 1
}
main().catch((error) => { console.error(error.stack || error.message); process.exitCode = 1 })

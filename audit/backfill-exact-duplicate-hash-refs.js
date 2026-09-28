'use strict'

// Restore missing bitwise index references found by the read-only exact audit.
// Dry-run by default. --apply hashes every added file again and backs up the
// existing index before an atomic replacement.
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const os = require('os')

const apply = process.argv.includes('--apply')
const reportPath = path.resolve(__dirname, '..', 'tmp', 'exact-media-duplicates-full-20260928.json')
const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'))
if (report.mode !== 'exact_bytes_md5_size_prefilter' ||
    report.summary.scanErrors !== 0 || report.summary.hashErrors !== 0 ||
    report.summary.mirrorConflicts !== 0) throw new Error('Audit is not safe for hash backfill')

const datasetRoot = path.join(process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'), '.slopvault', 'dataset')
const storePath = path.join(datasetRoot, 'bitwiseHashes.v2.json')
const store = JSON.parse(fs.readFileSync(storePath, 'utf8'))
if (store.version !== 2 || !Array.isArray(store.entries)) throw new Error('Unexpected hash index format')
const byHash = new Map(store.entries.map((entry) => [entry.hash, entry]))
const candidates = []
for (const group of report.duplicateGroups) {
  for (const record of group.records) {
    if (record.modelName.startsWith('.') || !['images', 'gif', 'webm'].includes(record.bucket)) continue
    const entry = byHash.get(group.md5)
    if (entry?.refs?.includes(record.relativePath)) continue
    const location = record.locations.find((item) => item.rootType === 'local') || record.locations[0]
    if (!location) continue
    candidates.push({ hash: group.md5, path: record.relativePath, absolutePath: location.absolutePath, size: group.sizeBytes })
  }
}
console.log(`${apply ? 'Applying' : 'Dry run:'} ${candidates.length} missing live-file hash references`)
if (!apply) process.exit(0)

async function hashFile(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('md5')
    const stream = fs.createReadStream(filePath)
    stream.on('data', (chunk) => hash.update(chunk))
    stream.on('error', reject)
    stream.on('end', () => resolve(hash.digest('hex')))
  })
}

async function main() {
  let added = 0
  const rejected = []
  for (const candidate of candidates) {
    try {
      const stat = fs.statSync(candidate.absolutePath)
      if (stat.size !== candidate.size || await hashFile(candidate.absolutePath) !== candidate.hash) {
        rejected.push(candidate.path)
        continue
      }
      let entry = byHash.get(candidate.hash)
      if (!entry) {
        entry = { hash: candidate.hash, refs: [] }
        byHash.set(candidate.hash, entry)
      }
      if (!entry.refs.includes(candidate.path)) { entry.refs.push(candidate.path); added++ }
      if (added % 100 === 0) console.log(`Validated ${added} missing references...`)
    } catch (error) {
      rejected.push(`${candidate.path}: ${error.message}`)
    }
  }
  if (rejected.length) throw new Error(`Rejected ${rejected.length} refs; no index written. First: ${rejected[0]}`)
  const stamp = new Date().toISOString().replace(/[:.]/g, '-')
  const backup = `${storePath}.bak-exact-dupes-${stamp}`
  fs.copyFileSync(storePath, backup)
  const payload = { ...store, entryCount: byHash.size,
    entries: [...byHash.values()].map((entry) => ({ ...entry, refs: [...new Set(entry.refs)].sort() }))
      .sort((a, b) => a.hash.localeCompare(b.hash)) }
  const temp = `${storePath}.${process.pid}.tmp`
  fs.writeFileSync(temp, JSON.stringify(payload))
  fs.renameSync(temp, storePath)
  console.log(`Added ${added} hash refs; backup: ${backup}`)
}

main().catch((error) => { console.error(error.stack || error.message); process.exitCode = 1 })

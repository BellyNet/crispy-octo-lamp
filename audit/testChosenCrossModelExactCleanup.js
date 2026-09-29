'use strict'

const assert = require('assert')
const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { execFileSync } = require('child_process')

const fixture = fs.mkdtempSync(path.join(os.tmpdir(), 'cross-model-exact-test-'))
const localRoot = path.join(fixture, 'local')
const nasRoot = path.join(fixture, 'nas')
const hash = crypto.createHash('md5').update('identical media bytes').digest('hex')
const from = 'alpha/images/one.jpg'
const to = 'beta/images/two.jpg'
const bytes = Buffer.from('identical media bytes')
const put = (root, relative, value) => {
  const target = path.join(root, ...relative.split('/'))
  fs.mkdirSync(path.dirname(target), { recursive: true })
  fs.writeFileSync(target, value)
  return target
}
const json = (root, relative, value) => put(root, relative, JSON.stringify(value))
const auditPath = path.join(fixture, 'audit.json')
const planPath = path.join(fixture, 'plan.json')
const decisionsPath = path.join(fixture, 'decisions.json')
const reviewPath = path.join(fixture, 'review.json')
try {
  const sourcePaths = [put(localRoot, from, bytes), put(nasRoot, from, bytes)]
  const keeperPaths = [put(localRoot, to, bytes), put(nasRoot, to, bytes)]
  for (const root of [localRoot, nasRoot]) {
    json(root, 'alpha/.media-dates.json', { 'images/one.jpg': { source: { title: 'source' } }, untouched: { value: root } })
    json(root, 'beta/.media-dates.json', { 'images/two.jpg': { source: { title: 'keeper' } } })
    json(root, 'alpha/log/milkmaid-seen-media-index.json', {
      mediaUrls: { url: { relativePath: from, filename: 'one.jpg', fullResolutionResolvedPath: from } },
      mediaPageUrls: {}, deadMediaUrls: {}, deadMediaPageUrls: {},
    })
    json(root, 'bitwiseHashes.v2.json', { entries: [{ hash, refs: [from, to] }], entryCount: 1 })
    json(root, 'visualHashes.v2.json', { entries: [{ hash: 'visual', refs: [from, to] }], entryCount: 1 })
    json(root, 'nas-mp4-index.v1.json', { entries: [from, to], entryCount: 2 })
  }
  const audit = {
    generatedAt: 'fixture-audit',
    mode: 'exact_bytes_md5_size_prefilter',
    summary: { scanErrors: 0, hashErrors: 0, mirrorConflicts: 0,
      sameModelRedundantCopies: 0, conservativeReclaimableBytes: 0, crossModelGroups: 1 },
    duplicateGroups: [{ md5: hash, sizeBytes: bytes.length, records: [
      { relativePath: from, modelName: 'alpha', bucket: 'images', filename: 'one.jpg', locations: [
        { rootType: 'local', absolutePath: sourcePaths[0] },
        { rootType: 'nas', absolutePath: sourcePaths[1] },
      ] },
      { relativePath: to, modelName: 'beta', bucket: 'images', filename: 'two.jpg', locations: [
        { rootType: 'local', absolutePath: keeperPaths[0] },
        { rootType: 'nas', absolutePath: keeperPaths[1] },
      ] },
    ] }],
  }
  fs.writeFileSync(auditPath, JSON.stringify(audit))
  const decisions = { auditedAt: audit.generatedAt, groups: { [hash]: { keepModel: 'beta' } } }
  const decisionsRaw = JSON.stringify(decisions)
  fs.writeFileSync(decisionsPath, decisionsRaw)
  const plan = {
    auditedAt: audit.generatedAt,
    decisionsDigest: crypto.createHash('sha256').update(decisionsRaw).digest('hex'),
    operations: [{ hash, sizeBytes: bytes.length, from, to, crossModel: true }],
  }
  fs.writeFileSync(planPath, JSON.stringify(plan))
  const script = path.join(__dirname, 'apply-chosen-cross-model-exact-cleanup.js')
  const args = [script, `--local-root=${localRoot}`, `--nas-root=${nasRoot}`,
    `--plan=${planPath}`, `--audit=${auditPath}`, `--decisions=${decisionsPath}`,
    `--backup-root=${fixture}`, `--review=${reviewPath}`]
  const dryRun = JSON.parse(execFileSync(process.execPath, args, { encoding: 'utf8' }))
  assert.strictEqual(dryRun.logicalPaths, 1)
  assert(sourcePaths.every((file) => fs.existsSync(file)))
  fs.writeFileSync(decisionsPath, JSON.stringify({ auditedAt: audit.generatedAt,
    groups: { [hash]: { keepModel: 'alpha' } } }))
  assert.throws(() => execFileSync(process.execPath, [...args, '--apply'], { stdio: 'pipe' }))
  assert(sourcePaths.every((file) => fs.existsSync(file)))
  fs.writeFileSync(decisionsPath, decisionsRaw)
  fs.writeFileSync(keeperPaths[0], 'changed keeper bytes')
  assert.throws(() => execFileSync(process.execPath, [...args, '--apply'], { stdio: 'pipe' }))
  assert(sourcePaths.every((file) => fs.existsSync(file)))
  fs.writeFileSync(keeperPaths[0], bytes)
  const output = execFileSync(process.execPath, [...args, '--apply'], { encoding: 'utf8' })
  assert(output.includes('"status": "complete"'))
  assert(sourcePaths.every((file) => !fs.existsSync(file)))
  assert(keeperPaths.every((file) => fs.existsSync(file)))
  assert.strictEqual(JSON.parse(fs.readFileSync(reviewPath)).crossModelGroups.length, 0)
  assert.deepStrictEqual(JSON.parse(fs.readFileSync(decisionsPath)).groups, {})
  for (const root of [localRoot, nasRoot]) {
    const sidecar = JSON.parse(fs.readFileSync(path.join(root, 'alpha/.media-dates.json')))
    assert(!sidecar['images/one.jpg'])
    assert(sidecar.untouched)
    const seen = JSON.parse(fs.readFileSync(path.join(root, 'alpha/log/milkmaid-seen-media-index.json')))
    assert.strictEqual(seen.mediaUrls.url.relativePath, to)
    assert.strictEqual(seen.mediaUrls.url.fullResolutionResolvedPath, to)
    const hashes = JSON.parse(fs.readFileSync(path.join(root, 'bitwiseHashes.v2.json')))
    assert.deepStrictEqual(hashes.entries[0].refs, [to])
  }
  console.log('Chosen cross-model exact cleanup fixture passed.')
} finally {
  const resolved = path.resolve(fixture)
  if (resolved.startsWith(path.resolve(os.tmpdir()) + path.sep) &&
      path.basename(resolved).startsWith('cross-model-exact-test-')) {
    fs.rmSync(resolved, { recursive: true, force: true })
  }
}

'use strict'

// Fixture test: with the dataset on the NAS (one folder, possibly reached by a
// different path), sync must not copy and eviction must never delete. A
// control case confirms eviction still works when there are two copies.
const assert = require('assert')
const fs = require('fs')
const os = require('os')
const path = require('path')

const { isSameDirectory } = require('./datasetLocation')
const {
  evictVerifiedLocalMp4s,
  syncModelMetadataToNas,
  syncModelToNas,
} = require('./nasSync')

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dataset-location-'))
function makeDataset(dir) {
  const webm = path.join(dir, 'model_a', 'webm')
  fs.mkdirSync(webm, { recursive: true })
  fs.writeFileSync(path.join(webm, 'clip.mp4'), Buffer.alloc(2048, 7))
  fs.writeFileSync(path.join(dir, 'model_a', '.media-dates.json'), '{"a":1}')
  return path.join(webm, 'clip.mp4')
}

;(async () => {
  // One folder, reached by the same path and by an alias (junction/symlink).
  const nas = path.join(root, 'nas')
  const clip = makeDataset(nas)
  const alias = path.join(root, 'alias')
  fs.symlinkSync(nas, alias, process.platform === 'win32' ? 'junction' : 'dir')

  assert.strictEqual(isSameDirectory(nas, nas), true)
  assert.strictEqual(isSameDirectory(nas, `${nas}${path.sep}`), true)
  assert.strictEqual(isSameDirectory(alias, nas), true)
  assert.strictEqual(
    fs.readdirSync(nas).some((name) => name.startsWith('.same-dir-probe')),
    false,
    'probe marker must be cleaned up'
  )

  for (const datasetDir of [nas, alias]) {
    const evicted = evictVerifiedLocalMp4s({
      modelName: 'model_a',
      datasetDir,
      nasDatasetDir: nas,
    })
    assert.strictEqual(evicted.deletedFiles, 0)
    assert.ok(fs.existsSync(clip), 'eviction must not delete the only copy')

    const metadata = syncModelMetadataToNas({
      modelName: 'model_a',
      datasetDir,
      nasDatasetDir: nas,
    })
    assert.strictEqual(metadata.failed, 0)
    assert.strictEqual(metadata.copied + metadata.replaced, 0)

    const sync = await syncModelToNas({
      modelName: 'model_a',
      datasetDir,
      nasDatasetDir: nas,
      log: { log() {}, warn() {}, error() {} },
    })
    assert.strictEqual(sync.ok, true)
    assert.strictEqual(sync.datasetIsNas, true)
    assert.ok(fs.existsSync(clip), 'sync must not delete the only copy')
    assert.strictEqual(fs.statSync(clip).size, 2048)
  }

  // Control: two real copies. Eviction deletes the local one, keeps the NAS one.
  const local = path.join(root, 'local')
  const localClip = makeDataset(local)
  assert.strictEqual(isSameDirectory(local, nas), false)
  const evicted = evictVerifiedLocalMp4s({
    modelName: 'model_a',
    datasetDir: local,
    nasDatasetDir: nas,
  })
  assert.strictEqual(evicted.deletedFiles, 1)
  assert.ok(!fs.existsSync(localClip))
  assert.ok(fs.existsSync(clip))

  fs.rmSync(root, { recursive: true, force: true })
  console.log('Dataset location fixture passed.')
})().catch((err) => {
  console.error(err)
  process.exitCode = 1
})

'use strict'

// Fixture test for the delete bin: 30-day purge and registry removal.
const assert = require('assert')
const fs = require('fs')
const os = require('os')
const path = require('path')

const {
  parseRunTimestamp,
  purgeOldTrash,
  removeModelFromRegistry,
} = require('./trashBin')
const {
  saveModelRegistry,
  loadModelRegistry,
} = require('../scrapyard/modelRegistry')

;(async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'trash-bin-'))
  const datasetDir = path.join(root, 'dataset')
  const trash = path.join(datasetDir, '.dashboard-trash')
  const now = Date.parse('2026-10-02T12:00:00.000Z')

  assert.strictEqual(
    parseRunTimestamp('2026-09-16T04-36-08-136Z').toISOString(),
    '2026-09-16T04:36:08.136Z'
  )
  assert.strictEqual(parseRunTimestamp('not-a-run'), null)

  for (const name of [
    '2026-08-01T00-00-00-000Z', // 62 days old: purged
    '2026-09-01T11-59-59-999Z', // just over 30 days: purged
    '2026-09-03T00-00-00-000Z', // 29.5 days: kept
    '2026-10-02T11-00-00-000Z', // today: kept
  ]) {
    fs.mkdirSync(path.join(trash, name, 'model', 'images'), { recursive: true })
    fs.writeFileSync(path.join(trash, name, 'model', 'images', 'a.jpg'), 'x')
  }
  const quiet = { log() {}, warn() {} }
  const result = await purgeOldTrash({ datasetDir, days: 30, now, log: quiet })
  assert.deepStrictEqual(result.removed.sort(), [
    '2026-08-01T00-00-00-000Z',
    '2026-09-01T11-59-59-999Z',
  ])
  assert.strictEqual(result.kept, 2)
  assert.deepStrictEqual(fs.readdirSync(trash).sort(), [
    '2026-09-03T00-00-00-000Z',
    '2026-10-02T11-00-00-000Z',
  ])
  // 0 days keeps everything; a missing bin is fine.
  assert.strictEqual(
    (await purgeOldTrash({ datasetDir, days: 0, now: now + 1e12, log: quiet }))
      .removed.length,
    0
  )
  assert.deepStrictEqual(
    await purgeOldTrash({
      datasetDir: path.join(root, 'nope'),
      days: 30,
      now,
      log: quiet,
    }),
    { removed: [], kept: 0 }
  )

  // Deleting a model drops it from the registry and keeps its entry.
  const registryPath = path.join(root, 'model_aliases.json')
  saveModelRegistry(registryPath, {
    gone: {
      aliases: ['gone'],
      sources: {
        reddit: [{ url: 'https://www.reddit.com/user/gone/submitted/' }],
      },
    },
    stays: { aliases: ['stays'], sources: {} },
  })
  const trashedDir = path.join(trash, '2026-10-02T11-00-00-000Z', 'gone')
  fs.mkdirSync(trashedDir, { recursive: true })
  const removed = await removeModelFromRegistry(
    'gone',
    trashedDir,
    registryPath
  )
  assert.ok(removed.sources.reddit)
  assert.deepStrictEqual(Object.keys(loadModelRegistry(registryPath)), [
    'stays',
  ])
  const saved = JSON.parse(
    fs.readFileSync(path.join(trashedDir, 'registry-entry.json'), 'utf8')
  )
  assert.strictEqual(saved.model, 'gone')
  assert.strictEqual(
    await removeModelFromRegistry('never-registered', null, registryPath),
    null
  )

  fs.rmSync(root, { recursive: true, force: true })
  console.log('Trash bin fixture passed.')
})().catch((err) => {
  console.error(err)
  process.exitCode = 1
})

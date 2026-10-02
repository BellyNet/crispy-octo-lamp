'use strict'

// Fixture test for registry operations and the PC <-> NAS registry sync.
const assert = require('assert')
const fs = require('fs')
const os = require('os')
const path = require('path')

const {
  loadModelRegistry,
  saveModelRegistry,
  sortModelRegistry,
} = require('./modelRegistry')
const { diffRegistries, applyRegistryOps } = require('./registryOps')
const { syncRegistry } = require('./registrySync')
const { applyOpsToRegistry, updateRegistry } = require('./registryStore')

const clone = (value) => JSON.parse(JSON.stringify(value))
const normalized = (registry) =>
  JSON.parse(JSON.stringify(sortModelRegistry(registry)))

const real = loadModelRegistry(path.join(__dirname, '..', 'model_aliases.json'))
const models = Object.keys(real)
assert.ok(models.length > 10, 'needs the real registry for a realistic test')

// 1. Round trip: diff then apply reproduces the edited registry.
let seed = 11
const random = () => {
  seed = (seed * 1103515245 + 12345) % 2147483648
  return seed / 2147483648
}
const pick = (list) => list[Math.floor(random() * list.length)]
for (let round = 0; round < 25; round += 1) {
  const before = clone(real)
  const after = clone(real)
  for (let i = 0; i < 8; i += 1) {
    const model = pick(models)
    const entry = after[model]
    const kind = Math.floor(random() * 6)
    const keys = Object.keys(entry.sources || {}).filter(
      (key) => entry.sources[key]?.length
    )
    if (kind === 0) {
      ;(entry.sources.tumblr ||= []).push({
        url: `https://new${round}${i}.tumblr.com/`,
        lastCheckedAt: 'x',
      })
    } else if (kind === 1 && keys.length) {
      const key = pick(keys)
      entry.sources[key].pop()
    } else if (kind === 2 && keys.length) {
      const key = pick(keys)
      entry.sources[key][0] = {
        ...entry.sources[key][0],
        lastCheckedAt: `round-${round}`,
      }
    } else if (kind === 3) {
      entry.aliases.push(`alias_${round}_${i}`)
    } else if (kind === 4) {
      after[`newmodel_${round}_${i}`] = {
        aliases: [`newmodel_${round}_${i}`],
        sources: {
          reddit: [
            { url: `https://www.reddit.com/user/n${round}${i}/submitted/` },
          ],
        },
      }
    } else if (keys.length) {
      // Move a source to inactive, like Reddit auto-archiving.
      const key = pick(keys)
      const [moved] = entry.sources[key].splice(0, 1)
      entry.inactiveSources ||= {}
      ;(entry.inactiveSources[key] ||= []).push({
        ...moved,
        inactiveReason: 'test',
      })
    }
  }
  const ops = diffRegistries(before, after)
  const rebuilt = applyRegistryOps(clone(before), ops)
  assert.deepStrictEqual(
    normalized(rebuilt),
    normalized(after),
    `round ${round}`
  )
  assert.deepStrictEqual(diffRegistries(after, after), [])
}

// 2. Concurrent edits on both sides both survive.
{
  const base = clone(real)
  const pc = clone(base)
  const nas = clone(base)
  const [m1, m2] = models
  pc[m1].aliases.push('pc_alias')
  ;(pc[m1].sources.reddit ||= []).push({
    url: 'https://www.reddit.com/user/pc_added/submitted/',
  })
  ;(nas[m1].sources.kemono ||= []).push({
    url: 'https://pawchive.pw/patreon/user/999',
  })
  nas.dashboard_new_model = { aliases: ['dashboard_new_model'], sources: {} }
  delete pc[m2]
  const merged = applyRegistryOps(clone(nas), diffRegistries(base, pc))
  assert.ok(merged[m1].aliases.includes('pc_alias'))
  assert.ok(merged[m1].sources.reddit.some((s) => s.url.includes('pc_added')))
  assert.ok(merged[m1].sources.kemono.some((s) => s.url.endsWith('/999')))
  assert.ok(merged.dashboard_new_model)
  assert.ok(!merged[m2], 'deletions sync too')
}

// 3. syncRegistry end to end against a NAS file through a stand-in backend.
;(async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'registry-sync-'))
  const pcPath = path.join(root, 'pc', 'model_aliases.json')
  const nasPath = path.join(root, 'nas', 'model_aliases.json')
  const snapshotPath = path.join(root, 'pc', 'synced.json')
  fs.mkdirSync(path.dirname(pcPath), { recursive: true })
  fs.mkdirSync(path.dirname(nasPath), { recursive: true })
  saveModelRegistry(pcPath, clone(real))
  saveModelRegistry(nasPath, clone(real))
  const backend = {
    registryPull: async () => ({ registry: loadModelRegistry(nasPath) }),
    registryApply: async (ops) => ({
      registry: await applyOpsToRegistry(ops, nasPath),
    }),
  }

  // First sync: nothing differs.
  assert.strictEqual(
    (await syncRegistry({ backend, registryPath: pcPath, snapshotPath })).sent,
    0
  )

  // A scrape on the PC adds a source; meanwhile the dashboard adds another.
  const [m1] = models
  const pcReg = loadModelRegistry(pcPath)
  ;(pcReg[m1].sources.tumblr ||= []).push({ url: 'https://pcside.tumblr.com/' })
  saveModelRegistry(pcPath, pcReg)
  await applyOpsToRegistry(
    [
      {
        op: 'upsertSource',
        model: m1,
        list: 'sources',
        key: 'kemono',
        source: { url: 'https://pawchive.pw/fanbox/user/42' },
      },
    ],
    nasPath
  )

  const result = await syncRegistry({
    backend,
    registryPath: pcPath,
    snapshotPath,
  })
  assert.strictEqual(result.sent, 1)
  for (const file of [pcPath, nasPath]) {
    const reg = loadModelRegistry(file)
    assert.ok(
      reg[m1].sources.tumblr.some(
        (s) => s.url === 'https://pcside.tumblr.com/'
      ),
      file
    )
    assert.ok(
      reg[m1].sources.kemono.some((s) => s.url.endsWith('/42')),
      file
    )
  }
  assert.strictEqual(
    (await syncRegistry({ backend, registryPath: pcPath, snapshotPath })).sent,
    0
  )

  // A model deleted on the dashboard stays deleted: the PC's later edit to
  // it (a scrape updating its sources) must not recreate it.
  const victim = models[2]
  await updateRegistry((registry) => {
    delete registry[victim]
  }, nasPath)
  const pcEdit = loadModelRegistry(pcPath)
  ;(pcEdit[victim].sources.tumblr ||= []).push({
    url: 'https://late-edit.tumblr.com/',
  })
  saveModelRegistry(pcPath, pcEdit)
  await syncRegistry({ backend, registryPath: pcPath, snapshotPath })
  assert.ok(!loadModelRegistry(nasPath)[victim], 'deleted model not recreated')
  assert.ok(!loadModelRegistry(pcPath)[victim], 'and gone from the PC copy')

  // Without a snapshot, a stale PC copy (e.g. the git version) is replaced
  // by the NAS copy instead of being pushed as changes.
  const stale = clone(real)
  stale.zombie_model = { aliases: ['zombie_model'], sources: {} }
  saveModelRegistry(pcPath, stale)
  fs.rmSync(snapshotPath)
  const noSnapshot = await syncRegistry({
    backend,
    registryPath: pcPath,
    snapshotPath,
  })
  assert.strictEqual(noSnapshot.sent, 0)
  assert.ok(!loadModelRegistry(nasPath).zombie_model)
  assert.ok(!loadModelRegistry(pcPath).zombie_model)
  assert.ok(!loadModelRegistry(pcPath)[victim])

  fs.rmSync(root, { recursive: true, force: true })
  console.log('Registry sync fixture passed.')
})().catch((err) => {
  console.error(err)
  process.exitCode = 1
})

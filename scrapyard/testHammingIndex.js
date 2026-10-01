'use strict'

// Fixture test: the packed Hamming index must return exactly what the
// original full scan through getVisualHashDistance() returns.
const assert = require('assert')
const { createHammingIndex } = require('./hammingIndex')
const { getVisualHashDistance } = require('./visualHasher')
const { createDuplicateChecker } = require('./duplicateChecker')

let seed = 7
function random() {
  seed = (seed * 1103515245 + 12345) % 2147483648
  return seed / 2147483648
}
function randomHex(length) {
  let out = ''
  for (let i = 0; i < length; i += 1)
    out += Math.floor(random() * 16).toString(16)
  return out
}
function flipBits(hash, count) {
  const chars = hash.split('')
  for (let i = 0; i < count; i += 1) {
    const at = Math.floor(random() * chars.length)
    chars[at] = (
      parseInt(chars[at], 16) ^
      (1 << Math.floor(random() * 4))
    ).toString(16)
  }
  return chars.join('')
}

const entries = []
for (let i = 0; i < 400; i += 1) {
  const model = `model${i % 7}`
  const base = randomHex(64)
  entries.push({ hash: base, refs: [`${model}/images/${i}.jpg`] })
  entries.push({ hash: flipBits(base, 3), refs: [`${model}/images/${i}b.jpg`] })
}
// Odd shapes the distance function treats specially.
entries.push({
  hash: 'ABCDEF0123456789'.repeat(4),
  refs: ['model1/images/upper.jpg'],
})
entries.push({
  hash: `${randomHex(64)}|${randomHex(64)}`,
  refs: ['model1/webm/v.mp4'],
})
entries.push({ hash: randomHex(20), refs: ['model2/images/short.jpg'] })

const index = createHammingIndex()
for (const entry of entries) index.add(entry.hash)
const byHash = new Map(entries.map((entry) => [entry.hash, entry]))

const queries = []
for (let i = 0; i < 300; i += 1) {
  const entry = entries[Math.floor(random() * entries.length)]
  queries.push(flipBits(entry.hash.split('|')[0], Math.floor(random() * 12)))
}
queries.push(
  'abcdef0123456789'.repeat(4),
  'nothex',
  '',
  `${randomHex(64)}|x`,
  randomHex(20)
)

for (const query of queries) {
  for (const maxDistance of [0, 4, 8]) {
    const expected = entries
      .map((entry) => ({
        hash: entry.hash,
        distance: getVisualHashDistance(query, entry.hash),
      }))
      .filter(
        (match) => match.distance !== null && match.distance <= maxDistance
      )
      .sort((a, b) => a.hash.localeCompare(b.hash))
    const actual = index
      .findWithin(query, maxDistance)
      .sort((a, b) => a.hash.localeCompare(b.hash))
    assert.deepStrictEqual(
      actual,
      expected,
      `index mismatch for ${query} @ ${maxDistance}`
    )
  }
}

// The duplicate checker picks the same best match through either path.
const shared = {
  datasetDir: 'dataset',
  existsLocallyOrOnNas: () => true,
  getVisualHashRecord: (hash) => byHash.get(hash) || null,
  isVisualDupe: (hash) => byHash.has(hash),
}
const scanChecker = createDuplicateChecker({
  ...shared,
  getVisualHashEntries: () => entries,
  getVisualHashDistance,
})
const indexChecker = createDuplicateChecker({
  ...shared,
  findVisualHashesWithin: (hash, maxDistance) =>
    index.findWithin(hash, maxDistance).map(({ hash: match, distance }) => ({
      entry: byHash.get(match),
      distance,
    })),
})
for (const query of queries) {
  for (const model of ['model0', 'model1', 'model3']) {
    assert.deepStrictEqual(
      indexChecker.getFuzzyVisualDuplicationRecord(model, query, 8),
      scanChecker.getFuzzyVisualDuplicationRecord(model, query, 8),
      `checker mismatch for ${model} ${query}`
    )
  }
}

console.log('Hamming index fixture passed.')

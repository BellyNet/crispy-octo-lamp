'use strict'

const assert = require('assert')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { refreshExactDuplicateReview } = require('./exactDuplicateNightly')

async function main() {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), 'exact-nightly-test-'))
  try {
    const datasetDir = path.join(fixture, 'dataset')
    const thumbDir = path.join(fixture, 'thumbs')
    fs.mkdirSync(thumbDir)
    for (const model of ['alpha', 'beta']) {
      const filePath = path.join(datasetDir, model, 'images', 'same.jpg')
      fs.mkdirSync(path.dirname(filePath), { recursive: true })
      fs.writeFileSync(filePath, 'same bytes')
    }
    await refreshExactDuplicateReview({ datasetDir, thumbDir })
    const reviewPath = path.join(thumbDir, 'exact-media-review-latest.json')
    const decisionsPath = path.join(thumbDir, 'exact-duplicate-decisions.json')
    let review = JSON.parse(fs.readFileSync(reviewPath, 'utf8'))
    assert.strictEqual(review.crossModelGroups.length, 1)
    const id = review.crossModelGroups[0].id
    fs.writeFileSync(decisionsPath, JSON.stringify({ auditedAt: review.auditedAt,
      groups: { [id]: { keepModel: 'alpha' } } }))
    await refreshExactDuplicateReview({ datasetDir, thumbDir })
    let decisions = JSON.parse(fs.readFileSync(decisionsPath, 'utf8'))
    review = JSON.parse(fs.readFileSync(reviewPath, 'utf8'))
    assert.strictEqual(decisions.auditedAt, review.auditedAt)
    assert.strictEqual(decisions.groups[id].keepModel, 'alpha')
    fs.unlinkSync(path.join(datasetDir, 'beta', 'images', 'same.jpg'))
    await refreshExactDuplicateReview({ datasetDir, thumbDir })
    review = JSON.parse(fs.readFileSync(reviewPath, 'utf8'))
    decisions = JSON.parse(fs.readFileSync(decisionsPath, 'utf8'))
    assert.strictEqual(review.crossModelGroups.length, 0)
    assert.deepStrictEqual(decisions.groups, {})
    console.log('Nightly exact duplicate audit fixture passed.')
  } finally {
    const resolved = path.resolve(fixture)
    if (resolved.startsWith(path.resolve(os.tmpdir()) + path.sep) &&
        path.basename(resolved).startsWith('exact-nightly-test-')) {
      fs.rmSync(resolved, { recursive: true, force: true })
    }
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1 })

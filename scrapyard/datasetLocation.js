'use strict'

// Is the dataset the scrapers write to the same folder as the NAS dataset?
//
// Once the dataset lives on the NAS, every "copy the local copy to the NAS"
// and "delete the local copy because the NAS has it" step must be skipped:
// run against a single folder, eviction would see each file "on the NAS" and
// delete the only copy. Different spellings can name one folder (Z:\dataset,
// \\nas\Vault69\dataset), so a text comparison isn't enough; when the paths
// differ, a marker file written in one is looked for in the other.

const crypto = require('crypto')
const fs = require('fs')
const path = require('path')

const config = require('./config')

const sameDirectoryCache = new Map()

function normalizeDir(value) {
  const resolved = path.resolve(String(value || ''))
  return process.platform === 'win32'
    ? resolved.toLowerCase().replace(/[\\/]+$/, '')
    : resolved.replace(/\/+$/, '')
}

function isSameDirectory(left, right) {
  if (!left || !right) return false
  const a = normalizeDir(left)
  const b = normalizeDir(right)
  if (a === b) return true

  const key = `${a}\n${b}`
  if (sameDirectoryCache.has(key)) return sameDirectoryCache.get(key)

  const marker = `.same-dir-probe-${process.pid}-${crypto.randomBytes(6).toString('hex')}`
  let same = false
  try {
    fs.writeFileSync(path.join(a, marker), '')
    same = fs.existsSync(path.join(b, marker))
  } catch {
    // Unwritable or missing folder: it can't be the dataset being evicted from.
    same = false
  } finally {
    try {
      fs.unlinkSync(path.join(a, marker))
    } catch {}
  }
  sameDirectoryCache.set(key, same)
  return same
}

// True when the dataset and the NAS dataset are one folder (NAS-only mode).
function datasetIsNas(
  datasetDir = config.datasetDir,
  nasDatasetDir = config.nasDatasetDir
) {
  return isSameDirectory(datasetDir, nasDatasetDir)
}

// For tools that only make sense with a separate local copy: stop before
// touching anything.
function assertSeparateFromNas(
  toolName,
  datasetDir = config.datasetDir,
  nasDatasetDir = config.nasDatasetDir
) {
  if (isSameDirectory(datasetDir, nasDatasetDir)) {
    throw new Error(
      `${toolName} works on a local copy of the dataset alongside the NAS, but the dataset now lives on the NAS (${datasetDir}). Nothing to do; not running.`
    )
  }
}

module.exports = { isSameDirectory, datasetIsNas, assertSeparateFromNas }

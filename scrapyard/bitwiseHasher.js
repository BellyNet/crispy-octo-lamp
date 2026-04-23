const path = require('path')
const fs = require('fs')
const os = require('os')
const { createHashStore } = require('./hashStore')

const datasetDir = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  '.slopvault',
  'dataset'
)

const bitwiseHashPath = path.join(datasetDir, 'bitwiseHashes.json')
const bitwiseHashStore = createHashStore({
  storePath: bitwiseHashPath,
  kind: 'bitwise',
  algorithm: 'md5',
})

function loadBitwiseHashCache() {
  bitwiseHashStore.load()
}

function saveBitwiseHashCache() {
  fs.mkdirSync(path.dirname(bitwiseHashPath), { recursive: true })
  bitwiseHashStore.save()
}

function isBitwiseDupe(hash) {
  return bitwiseHashStore.has(hash)
}

function addBitwiseHash(hash, metadata = null) {
  return bitwiseHashStore.add(hash, metadata)
}

function getBitwiseHashRecord(hash) {
  return bitwiseHashStore.get(hash)
}

function getBitwiseHashEntries() {
  return bitwiseHashStore.getAllEntries()
}

module.exports = {
  loadBitwiseHashCache,
  saveBitwiseHashCache,
  isBitwiseDupe,
  addBitwiseHash,
  getBitwiseHashRecord,
  getBitwiseHashEntries,
}

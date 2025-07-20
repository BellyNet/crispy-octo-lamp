const path = require('path')
const fs = require('fs')
const os = require('os')

const datasetDir = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  '.slopvault',
  'dataset'
)

const bitwiseHashPath = path.join(datasetDir, 'bitwiseHashes.json')
let bitwiseHashSet = new Set()

function loadBitwiseHashCache() {
  if (fs.existsSync(bitwiseHashPath)) {
    try {
      const data = JSON.parse(fs.readFileSync(bitwiseHashPath, 'utf-8'))
      bitwiseHashSet = new Set(data)
    } catch (err) {
      console.warn('⚠️ Failed to load bitwise hash cache:', err.message)
    }
  }
}

function saveBitwiseHashCache() {
  fs.writeFileSync(
    bitwiseHashPath,
    JSON.stringify([...bitwiseHashSet], null, 2)
  )
}

function isBitwiseDupe(hash) {
  return bitwiseHashSet.has(hash)
}

function addBitwiseHash(hash) {
  bitwiseHashSet.add(hash)
}

module.exports = {
  loadBitwiseHashCache,
  saveBitwiseHashCache,
  isBitwiseDupe,
  addBitwiseHash,
}

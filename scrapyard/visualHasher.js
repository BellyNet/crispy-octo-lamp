const path = require('path')
const fs = require('fs')
const os = require('os')
const imghash = require('imghash')
const sharp = require('sharp')
const { createHash } = require('crypto')
const { createHashStore } = require('./hashStore')

const tmpDir = path.join(os.tmpdir(), 'thicc_visual_hash')

if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true })

const datasetDir = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  '.slopvault',
  'dataset'
)
const visualHashPath = path.join(datasetDir, 'visualHashes.json')
const visualHashStore = createHashStore({
  storePath: visualHashPath,
  kind: 'visual',
  algorithm: 'imghash-16-hex',
})

function loadVisualHashCache() {
  visualHashStore.load()
}

function saveVisualHashCache() {
  fs.mkdirSync(path.dirname(visualHashPath), { recursive: true })
  visualHashStore.save()
}

async function getVisualHashFromBuffer(buffer) {
  const hash = createHash('md5').update(buffer).digest('hex')
  const tmpPath = path.join(tmpDir, `vh_${hash}.jpg`)

  try {
    await sharp(buffer).resize(512).jpeg({ quality: 95 }).toFile(tmpPath)
    const visualHash = await imghash.hash(tmpPath, 16, 'hex')
    fs.unlinkSync(tmpPath)
    return visualHash
  } catch (err) {
    console.warn(`Failed visual hash: ${err.message}`)
    if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
    return null
  }
}

function isVisualDupe(visualHash) {
  return visualHashStore.has(visualHash)
}

function addVisualHash(visualHash, metadata = null) {
  return visualHashStore.add(visualHash, metadata)
}

function getVisualHashRecord(visualHash) {
  return visualHashStore.get(visualHash)
}

function getVisualHashEntries() {
  return visualHashStore.getAllEntries()
}

module.exports = {
  loadVisualHashCache,
  saveVisualHashCache,
  getVisualHashFromBuffer,
  isVisualDupe,
  addVisualHash,
  getVisualHashRecord,
  getVisualHashEntries,
}

const path = require('path')
const fs = require('fs')
const os = require('os')
const imghash = require('imghash')
const sharp = require('sharp')
const { createHash } = require('crypto')

const tmpDir = path.join(os.tmpdir(), 'thicc_visual_hash')

if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true })

const datasetDir = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  '.slopvault',
  'dataset'
)
const visualHashPath = path.join(datasetDir, 'visualHashes.json')

let visualHashSet = new Set()

function loadVisualHashCache() {
  if (fs.existsSync(visualHashPath)) {
    try {
      const data = JSON.parse(fs.readFileSync(visualHashPath, 'utf-8'))
      visualHashSet = new Set(data)
    } catch (err) {
      console.warn('⚠️ Failed to load visual hash cache:', err.message)
    }
  }
}

function saveVisualHashCache() {
  fs.writeFileSync(visualHashPath, JSON.stringify([...visualHashSet], null, 2))
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
    console.warn('⚠️ Failed visual hash:', err.message)
    if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
    return null
  }
}

function isVisualDupe(visualHash) {
  return visualHashSet.has(visualHash)
}

function addVisualHash(visualHash) {
  visualHashSet.add(visualHash)
}

module.exports = {
  loadVisualHashCache,
  saveVisualHashCache,
  getVisualHashFromBuffer,
  isVisualDupe,
  addVisualHash,
}

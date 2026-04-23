const path = require('path')
const fs = require('fs')
const os = require('os')
const { spawn } = require('child_process')
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
const visualHashPath = path.join(datasetDir, 'visualHashes.v2.json')
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
  const ffmpegInputPath = path.join(tmpDir, `vh_${hash}_src.jpg`)
  const ffmpegOutputPath = path.join(tmpDir, `vh_${hash}_ffmpeg.png`)

  try {
    await sharp(buffer).resize(512).jpeg({ quality: 95 }).toFile(tmpPath)
    const visualHash = await imghash.hash(tmpPath, 16, 'hex')
    fs.unlinkSync(tmpPath)
    return visualHash
  } catch (err) {
    try {
      if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath)
      fs.writeFileSync(ffmpegInputPath, buffer)
      await normalizeImageWithFfmpeg(ffmpegInputPath, ffmpegOutputPath)
      const visualHash = await imghash.hash(ffmpegOutputPath, 16, 'hex')
      fs.unlinkSync(ffmpegInputPath)
      fs.unlinkSync(ffmpegOutputPath)
      return visualHash
    } catch (fallbackErr) {
      console.warn(
        `Failed visual hash: ${err.message}; ffmpeg fallback failed: ${fallbackErr.message}`
      )
      if (fs.existsSync(ffmpegInputPath)) fs.unlinkSync(ffmpegInputPath)
      if (fs.existsSync(ffmpegOutputPath)) fs.unlinkSync(ffmpegOutputPath)
      return null
    }
  }
}

function normalizeImageWithFfmpeg(inputPath, outputPath) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      'ffmpeg',
      [
        '-y',
        '-loglevel',
        'error',
        '-i',
        inputPath,
        '-frames:v',
        '1',
        outputPath,
      ],
      {
        stdio: ['ignore', 'ignore', 'pipe'],
      }
    )

    let stderr = ''
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString()
    })

    child.on('error', reject)
    child.on('exit', (code) => {
      if (code === 0 && fs.existsSync(outputPath)) return resolve()
      reject(new Error(stderr.trim() || `ffmpeg exited with code ${code}`))
    })
  })
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

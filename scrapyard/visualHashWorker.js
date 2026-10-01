'use strict'

const fs = require('fs')
const path = require('path')
const { spawn } = require('child_process')
const imghash = require('imghash')
const sharp = require('sharp')

function unlinkIfExists(filePath) {
  try {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath)
  } catch {}
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

// Returns the image's 16x16 imghash as hex, or null when neither sharp nor
// ffmpeg can decode it. `scratchPrefix` names the temporary files.
async function computeImageHash(inputPath, scratchPrefix) {
  const tmpPath = `${scratchPrefix}.jpg`
  const ffmpegOutputPath = `${scratchPrefix}.png`

  try {
    await sharp(inputPath).resize(512).jpeg({ quality: 95 }).toFile(tmpPath)
    return await imghash.hash(tmpPath, 16, 'hex')
  } catch {
    try {
      await normalizeImageWithFfmpeg(inputPath, ffmpegOutputPath)
      return await imghash.hash(ffmpegOutputPath, 16, 'hex')
    } catch {
      return null
    }
  } finally {
    unlinkIfExists(tmpPath)
    unlinkIfExists(ffmpegOutputPath)
  }
}

async function hashImage(inputPath, outputPath) {
  const hash = await computeImageHash(inputPath, outputPath)
  fs.writeFileSync(outputPath, JSON.stringify({ hash }) + '\n')
  return 0
}

// Long-lived mode used by visualHasher: one process answers many
// { id, inputPath } requests over IPC, so each image no longer pays Node
// startup. Exits when the parent disconnects.
function serve() {
  process.on('message', async (request) => {
    const { id, inputPath } = request || {}
    let hash = null
    try {
      hash = await computeImageHash(inputPath, `${inputPath}.${id}`)
    } catch {}
    if (process.connected) process.send({ id, hash })
  })
  process.on('disconnect', () => process.exit(0))
}

async function main(argv = process.argv.slice(2)) {
  const [mode, inputPath, outputPath] = argv
  if (mode === 'serve') {
    serve()
    return null
  }
  if (mode !== 'image' || !inputPath || !outputPath) return 2
  fs.mkdirSync(path.dirname(outputPath), { recursive: true })
  return hashImage(inputPath, outputPath)
}

if (require.main === module) {
  main()
    .then((code) => {
      if (code !== null) process.exitCode = code
    })
    .catch(() => {
      process.exitCode = 1
    })
}

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

async function hashImage(inputPath, outputPath) {
  const tmpPath = `${outputPath}.jpg`
  const ffmpegOutputPath = `${outputPath}.png`

  try {
    await sharp(inputPath).resize(512).jpeg({ quality: 95 }).toFile(tmpPath)
    const hash = await imghash.hash(tmpPath, 16, 'hex')
    fs.writeFileSync(outputPath, JSON.stringify({ hash }) + '\n')
    return 0
  } catch {
    try {
      await normalizeImageWithFfmpeg(inputPath, ffmpegOutputPath)
      const hash = await imghash.hash(ffmpegOutputPath, 16, 'hex')
      fs.writeFileSync(outputPath, JSON.stringify({ hash }) + '\n')
      return 0
    } catch {
      fs.writeFileSync(outputPath, JSON.stringify({ hash: null }) + '\n')
      return 0
    }
  } finally {
    unlinkIfExists(tmpPath)
    unlinkIfExists(ffmpegOutputPath)
  }
}

async function main(argv = process.argv.slice(2)) {
  const [mode, inputPath, outputPath] = argv
  if (mode !== 'image' || !inputPath || !outputPath) return 2
  fs.mkdirSync(path.dirname(outputPath), { recursive: true })
  return hashImage(inputPath, outputPath)
}

if (require.main === module) {
  main()
    .then((code) => {
      process.exitCode = code
    })
    .catch(() => {
      process.exitCode = 1
    })
}

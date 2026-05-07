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
  algorithm: 'imghash-16-hex|video-3frame-imghash-16-hex',
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
        `Failed visual hash: ${compactErrorMessage(err)}; ffmpeg fallback failed: ${compactErrorMessage(fallbackErr)}`
      )
      if (fs.existsSync(ffmpegInputPath)) fs.unlinkSync(ffmpegInputPath)
      if (fs.existsSync(ffmpegOutputPath)) fs.unlinkSync(ffmpegOutputPath)
      return null
    }
  }
}

function compactErrorMessage(error) {
  return String(error?.message || error || '')
    .replace(/\s+/g, ' ')
    .trim()
}

async function getVideoFrameHashesFromPath(videoPath, options = {}) {
  const hash = createHash('md5').update(videoPath).digest('hex')
  const outputPrefix = path.join(tmpDir, `vh_video_${hash}`)
  const streamInfo = await probePrimaryVideoStream(videoPath)
  const duration =
    streamInfo?.durationSeconds ?? (await probeVideoDuration(videoPath))
  const timestamps = Array.isArray(options.timestamps)
    ? options.timestamps
    : buildVideoFrameTimestamps(duration)
  const frameHashes = []

  for (let index = 0; index < timestamps.length; index += 1) {
    const timestamp = timestamps[index]
    const outputPath = `${outputPrefix}_${index}.png`
    try {
      await extractVideoFrameWithFfmpeg(videoPath, outputPath, timestamp)
      const visualHash = await imghash.hash(outputPath, 16, 'hex')
      if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath)
      frameHashes.push(visualHash)
    } catch (err) {
      if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath)
      try {
        const visualHash = await getVideoFrameHashFromRawYuv(
          videoPath,
          timestamp,
          streamInfo,
          outputPrefix,
          index
        )
        if (visualHash) frameHashes.push(visualHash)
      } catch {
        // Leave this frame empty and keep trying other timestamps.
      }
    }
  }

  const signatureParts = frameHashes
    .map((value) => String(value || '').trim())
    .filter(Boolean)

  return signatureParts
}

async function getVisualHashFromVideoPath(videoPath, options = {}) {
  const signatureParts = await getVideoFrameHashesFromPath(videoPath, options)

  if (signatureParts.length < 2) {
    return null
  }

  return signatureParts.join('|')
}

function buildVideoFrameTimestamps(durationSeconds) {
  const base = [0.2, 0.5, 0.8]
  if (Number.isFinite(durationSeconds) && durationSeconds > 0) {
    return base
      .map((ratio) =>
        Math.max(0, Math.min(durationSeconds - 0.1, durationSeconds * ratio))
      )
      .concat([1, 0, 3])
      .filter(
        (value, index, list) =>
          Number.isFinite(value) && value >= 0 && list.indexOf(value) === index
      )
  }

  return [1, 0, 3]
}

function probeVideoDuration(videoPath) {
  return new Promise((resolve) => {
    const child = spawn(
      'ffprobe',
      [
        '-v',
        'error',
        '-show_entries',
        'format=duration',
        '-of',
        'default=noprint_wrappers=1:nokey=1',
        videoPath,
      ],
      {
        stdio: ['ignore', 'pipe', 'ignore'],
      }
    )

    let stdout = ''
    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })

    child.on('error', () => resolve(null))
    child.on('exit', (code) => {
      if (code !== 0) return resolve(null)
      const duration = Number.parseFloat(stdout.trim())
      resolve(Number.isFinite(duration) ? duration : null)
    })
  })
}

function probePrimaryVideoStream(videoPath) {
  return new Promise((resolve) => {
    const child = spawn(
      'ffprobe',
      [
        '-v',
        'error',
        '-select_streams',
        'v:0',
        '-show_entries',
        'stream=width,height,pix_fmt,duration',
        '-of',
        'json',
        videoPath,
      ],
      {
        stdio: ['ignore', 'pipe', 'ignore'],
      }
    )

    let stdout = ''
    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })

    child.on('error', () => resolve(null))
    child.on('exit', (code) => {
      if (code !== 0) return resolve(null)
      try {
        const parsed = JSON.parse(stdout || '{}')
        const stream = Array.isArray(parsed?.streams) ? parsed.streams[0] : null
        const width = Number(stream?.width)
        const height = Number(stream?.height)
        const durationSeconds = Number.parseFloat(
          String(stream?.duration || '')
        )
        resolve({
          width: Number.isFinite(width) ? width : null,
          height: Number.isFinite(height) ? height : null,
          pixelFormat: String(stream?.pix_fmt || '').trim() || null,
          durationSeconds: Number.isFinite(durationSeconds)
            ? durationSeconds
            : null,
        })
      } catch {
        resolve(null)
      }
    })
  })
}

function extractVideoFrameWithFfmpeg(inputPath, outputPath, timestampSeconds) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      'ffmpeg',
      [
        '-y',
        '-loglevel',
        'error',
        '-ss',
        String(timestampSeconds),
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

function extractVideoFrameRawYuv420p(inputPath, outputPath, timestampSeconds) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      'ffmpeg',
      [
        '-y',
        '-loglevel',
        'error',
        '-ss',
        String(timestampSeconds),
        '-i',
        inputPath,
        '-an',
        '-sn',
        '-dn',
        '-map',
        '0:v:0',
        '-frames:v',
        '1',
        '-f',
        'rawvideo',
        '-pix_fmt',
        'yuv420p',
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
      if (
        code === 0 &&
        fs.existsSync(outputPath) &&
        fs.statSync(outputPath).size > 0
      ) {
        return resolve()
      }
      reject(new Error(stderr.trim() || `ffmpeg exited with code ${code}`))
    })
  })
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

async function getVideoFrameHashFromRawYuv(
  videoPath,
  timestampSeconds,
  streamInfo,
  outputPrefix,
  frameIndex
) {
  if (!streamInfo?.width || !streamInfo?.height) return null
  if (streamInfo.pixelFormat && streamInfo.pixelFormat !== 'yuv420p')
    return null

  const rawPath = `${outputPrefix}_${frameIndex}.yuv`
  const pngPath = `${outputPrefix}_${frameIndex}_raw.png`

  try {
    await extractVideoFrameRawYuv420p(videoPath, rawPath, timestampSeconds)
    const rawBuffer = fs.readFileSync(rawPath)
    const rgbBuffer = yuv420pToRgb(
      rawBuffer,
      streamInfo.width,
      streamInfo.height
    )
    await sharp(rgbBuffer, {
      raw: {
        width: streamInfo.width,
        height: streamInfo.height,
        channels: 3,
      },
    })
      .png()
      .toFile(pngPath)
    return await imghash.hash(pngPath, 16, 'hex')
  } finally {
    if (fs.existsSync(rawPath)) fs.unlinkSync(rawPath)
    if (fs.existsSync(pngPath)) fs.unlinkSync(pngPath)
  }
}

function yuv420pToRgb(buffer, width, height) {
  const frameSize = width * height
  const chromaWidth = Math.floor(width / 2)
  const chromaHeight = Math.floor(height / 2)
  const chromaSize = chromaWidth * chromaHeight
  const expectedSize = frameSize + chromaSize * 2

  if (buffer.length < expectedSize) {
    throw new Error(
      `Raw YUV buffer too small (${buffer.length} < ${expectedSize}) for ${width}x${height}`
    )
  }

  const output = Buffer.allocUnsafe(frameSize * 3)
  const yPlane = buffer.subarray(0, frameSize)
  const uPlane = buffer.subarray(frameSize, frameSize + chromaSize)
  const vPlane = buffer.subarray(frameSize + chromaSize, expectedSize)

  for (let y = 0; y < height; y += 1) {
    const uvRow = Math.floor(y / 2)
    for (let x = 0; x < width; x += 1) {
      const uvCol = Math.floor(x / 2)
      const yIndex = y * width + x
      const uvIndex = uvRow * chromaWidth + uvCol

      const yy = yPlane[yIndex]
      const uu = uPlane[uvIndex]
      const vv = vPlane[uvIndex]

      const c = yy - 16
      const d = uu - 128
      const e = vv - 128

      const r = clampRgb((298 * c + 409 * e + 128) >> 8)
      const g = clampRgb((298 * c - 100 * d - 208 * e + 128) >> 8)
      const b = clampRgb((298 * c + 516 * d + 128) >> 8)

      const outIndex = yIndex * 3
      output[outIndex] = r
      output[outIndex + 1] = g
      output[outIndex + 2] = b
    }
  }

  return output
}

function clampRgb(value) {
  if (value < 0) return 0
  if (value > 255) return 255
  return value
}

function getVisualHashDistance(left, right) {
  const normalizedLeft = String(left || '')
    .trim()
    .toLowerCase()
  const normalizedRight = String(right || '')
    .trim()
    .toLowerCase()

  if (!normalizedLeft || !normalizedRight) return null
  if (normalizedLeft.includes('|') || normalizedRight.includes('|')) return null
  if (normalizedLeft.length !== normalizedRight.length) return null

  let distance = 0
  for (let index = 0; index < normalizedLeft.length; index += 1) {
    const leftNibble = Number.parseInt(normalizedLeft[index], 16)
    const rightNibble = Number.parseInt(normalizedRight[index], 16)
    if (!Number.isFinite(leftNibble) || !Number.isFinite(rightNibble)) {
      return null
    }
    distance += (leftNibble ^ rightNibble).toString(2).replace(/0/g, '').length
  }

  return distance
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

function removeVisualRefs(matchRef) {
  return visualHashStore.removeRefs(matchRef)
}

module.exports = {
  loadVisualHashCache,
  saveVisualHashCache,
  getVisualHashFromBuffer,
  getVideoFrameHashesFromPath,
  getVisualHashFromVideoPath,
  getVisualHashDistance,
  isVisualDupe,
  addVisualHash,
  getVisualHashRecord,
  getVisualHashEntries,
  removeVisualRefs,
}

const fs = require('fs')
const path = require('path')
const os = require('os')
const { spawn } = require('child_process')
const minimist = require('minimist')

const { getVideoFrameHashesFromPath } = require('./visualHasher')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
  },
  default: {
    'duration-tolerance': 1.0,
    'min-shared-frames': 2,
    'max-candidates': 40,
    'same-model-only': false,
  },
  boolean: ['help', 'same-model-only'],
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const appData =
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming')
const slopvaultRoot = path.join(appData, '.slopvault')
const datasetRoot = path.join(slopvaultRoot, 'dataset')
const quarantineRoot = path.join(slopvaultRoot, 'quarantine', 'dataset')
const cachePath = path.resolve(
  __dirname,
  '..',
  'tmp',
  'video-metadata-cache.json'
)
const defaultManifestPath = path.resolve(
  __dirname,
  '..',
  'audit',
  'manifests',
  'slopvault-manifest-latest.json'
)
const manifestPath = path.resolve(String(argv.manifest || defaultManifestPath))

main().catch((err) => {
  console.error(`Fatal video visual match error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  const metadataCache = loadMetadataCache(cachePath)
  const findings = loadRunErrorTargets(manifestPath)
  const targets = findings.filter(
    (item) =>
      item.relativePath &&
      item.relativePath.includes('/webm/') &&
      hasTailDecodeReason(item)
  )

  const datasetVideos = collectDatasetVideos(datasetRoot).map((filePath) => ({
    filePath,
    relativePath: path.relative(datasetRoot, filePath).replace(/\\/g, '/'),
    model: path.relative(datasetRoot, filePath).split(path.sep)[0] || null,
  }))

  const results = []
  let frameHashComputations = 0
  let metadataCacheWrites = 0

  for (const target of targets) {
    const localPath = resolveTargetPath(target.relativePath)
    if (!localPath) continue

    const targetMetadataResult = await getVideoMetadata(
      localPath,
      target.relativePath,
      metadataCache
    )
    metadataCacheWrites += targetMetadataResult.wasUpdated ? 1 : 0

    const targetFrameHashes = await getVideoFrameHashesFromPath(localPath)
    frameHashComputations += 1
    const targetDistinctHashes = [...new Set(targetFrameHashes)]

    if (targetDistinctHashes.length < 2) {
      results.push({
        model: target.model,
        relativePath: target.relativePath,
        error: target.errorDetails || target.reasons || null,
        mediaUrl: target.mediaUrl || null,
        mediaPageUrl: target.mediaPageUrl || null,
        durationSeconds: targetMetadataResult.metadata.durationSeconds,
        width: targetMetadataResult.metadata.width,
        height: targetMetadataResult.metadata.height,
        targetDistinctFrameHashes: targetDistinctHashes,
        skippedReason: 'low_information_frames',
        matches: [],
      })
      continue
    }

    const candidatePool = shortlistCandidates(
      target,
      targetMetadataResult.metadata,
      datasetVideos,
      metadataCache
    )

    const candidates = []
    for (const candidate of candidatePool) {
      const candidateMetadataResult = await getVideoMetadata(
        candidate.filePath,
        candidate.relativePath,
        metadataCache
      )
      metadataCacheWrites += candidateMetadataResult.wasUpdated ? 1 : 0

      if (
        !Number.isFinite(candidateMetadataResult.metadata.durationSeconds) ||
        Math.abs(
          candidateMetadataResult.metadata.durationSeconds -
            targetMetadataResult.metadata.durationSeconds
        ) > Number(argv['duration-tolerance'])
      ) {
        continue
      }

      const candidateFrameHashes = await getVideoFrameHashesFromPath(candidate.filePath)
      frameHashComputations += 1
      const candidateDistinctHashes = [...new Set(candidateFrameHashes)]

      if (candidateDistinctHashes.length < 2) {
        continue
      }

      const sharedFrames = targetDistinctHashes.filter((hash) =>
        candidateDistinctHashes.includes(hash)
      )
      if (sharedFrames.length < Number(argv['min-shared-frames'])) {
        continue
      }

      candidates.push({
        relativePath: candidate.relativePath,
        model: candidate.model,
        durationSeconds: candidateMetadataResult.metadata.durationSeconds,
        width: candidateMetadataResult.metadata.width,
        height: candidateMetadataResult.metadata.height,
        sizeBytes: candidateMetadataResult.metadata.sizeBytes,
        sharedFrameCount: sharedFrames.length,
        sharedFrames,
        frameHashes: candidateDistinctHashes,
        durationDeltaSeconds: Math.abs(
          candidateMetadataResult.metadata.durationSeconds -
            targetMetadataResult.metadata.durationSeconds
        ),
      })
    }

    candidates.sort(compareCandidates)

    results.push({
      model: target.model,
      relativePath: target.relativePath,
      error: target.errorDetails || target.reasons || null,
      mediaUrl: target.mediaUrl || null,
      mediaPageUrl: target.mediaPageUrl || null,
      durationSeconds: targetMetadataResult.metadata.durationSeconds,
      width: targetMetadataResult.metadata.width,
      height: targetMetadataResult.metadata.height,
      targetDistinctFrameHashes: targetDistinctHashes,
      matches: candidates,
    })
  }

  saveMetadataCache(cachePath, metadataCache)

  const matched = results.filter((item) => item.matches.length > 0)
  const unmatched = results.filter(
    (item) => item.matches.length === 0 && !item.skippedReason
  )
  const skipped = results.filter((item) => item.skippedReason)

  console.log(
    JSON.stringify(
      {
        manifestPath,
        targetCount: results.length,
        matchedCount: matched.length,
        unmatchedCount: unmatched.length,
        skippedCount: skipped.length,
        frameHashComputations,
        metadataCacheWrites,
        matched,
        unmatched,
        skipped,
      },
      null,
      2
    )
  )
}

function printHelp() {
  console.log(`Usage: node scrapyard/matchErroredVideoVisuals.js [options]

Options:
  --manifest <path>         Manifest to read unresolved run_error findings from.
  --duration-tolerance <s>  Max duration delta allowed for candidate matches.
  --min-shared-frames <n>   Minimum shared sampled frame hashes required.
  --max-candidates <n>      Max candidates to inspect per target after metadata filtering.
  --same-model-only         Only compare against videos from the same model.
  -h, --help                Show help.
`)
}

function loadRunErrorTargets(filePath) {
  const manifest = JSON.parse(fs.readFileSync(filePath, 'utf8'))
  return Array.isArray(manifest?.audit?.findings) ? manifest.audit.findings : []
}

function hasTailDecodeReason(item) {
  if (String(item.errorDetails || '').includes('tail_decode_error')) return true
  return Array.isArray(item.reasons)
    ? item.reasons.some((reason) => String(reason).includes('tail_decode_error'))
    : false
}

function resolveTargetPath(relativePath) {
  const quarantinePath = path.join(quarantineRoot, ...relativePath.split('/'))
  if (fs.existsSync(quarantinePath)) return quarantinePath

  const datasetPath = path.join(datasetRoot, ...relativePath.split('/'))
  if (fs.existsSync(datasetPath)) return datasetPath

  return null
}

function collectDatasetVideos(root) {
  const files = []
  const stack = [root]
  const exts = new Set(['.mp4', '.webm', '.m4v', '.mov'])

  while (stack.length) {
    const current = stack.pop()
    const entries = fs.readdirSync(current, { withFileTypes: true })

    for (const entry of entries) {
      const fullPath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        if (entry.name === 'log') continue
        stack.push(fullPath)
      } else if (
        entry.isFile() &&
        exts.has(path.extname(entry.name).toLowerCase())
      ) {
        files.push(fullPath)
      }
    }
  }

  return files.sort((a, b) => a.localeCompare(b))
}

function loadMetadataCache(filePath) {
  try {
    if (!fs.existsSync(filePath)) {
      return { version: 1, items: {} }
    }

    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'))
    return {
      version: 1,
      items: parsed?.items && typeof parsed.items === 'object' ? parsed.items : {},
    }
  } catch (err) {
    return { version: 1, items: {} }
  }
}

function saveMetadataCache(filePath, cache) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true })
  fs.writeFileSync(filePath, JSON.stringify(cache, null, 2))
}

async function getVideoMetadata(filePath, relativePath, cache) {
  const stat = fs.statSync(filePath)
  const cacheKey = normalizePath(relativePath || filePath)
  const cacheEntry = cache.items[cacheKey]
  const fileState = {
    sizeBytes: stat.size,
    modifiedAtMs: stat.mtimeMs,
  }

  if (
    cacheEntry &&
    cacheEntry.sizeBytes === fileState.sizeBytes &&
    cacheEntry.modifiedAtMs === fileState.modifiedAtMs
  ) {
    return {
      metadata: cacheEntry,
      wasUpdated: false,
    }
  }

  const probed = await probeVideoMetadata(filePath)
  const metadata = {
    ...fileState,
    durationSeconds: probed.durationSeconds,
    width: probed.width,
    height: probed.height,
    codecName: probed.codecName,
  }

  cache.items[cacheKey] = metadata
  return {
    metadata,
    wasUpdated: true,
  }
}

function shortlistCandidates(target, targetMetadata, datasetVideos, cache) {
  const tolerance = Number(argv['duration-tolerance'])
  const targetModel = target.model || target.relativePath.split('/')[0] || null
  const sameModelOnly = Boolean(argv['same-model-only'])

  const candidates = datasetVideos.filter((candidate) => {
    if (candidate.relativePath === target.relativePath) return false
    if (sameModelOnly && candidate.model !== targetModel) return false

    const cached = cache.items[normalizePath(candidate.relativePath)]
    if (!cached || !Number.isFinite(cached.durationSeconds)) return true

    return (
      Math.abs(cached.durationSeconds - targetMetadata.durationSeconds) <= tolerance
    )
  })

  candidates.sort((left, right) => {
    const leftCached = cache.items[normalizePath(left.relativePath)] || {}
    const rightCached = cache.items[normalizePath(right.relativePath)] || {}
    const leftSameModel = left.model === targetModel ? 0 : 1
    const rightSameModel = right.model === targetModel ? 0 : 1
    if (leftSameModel !== rightSameModel) return leftSameModel - rightSameModel

    const leftDelta = Number.isFinite(leftCached.durationSeconds)
      ? Math.abs(leftCached.durationSeconds - targetMetadata.durationSeconds)
      : Number.POSITIVE_INFINITY
    const rightDelta = Number.isFinite(rightCached.durationSeconds)
      ? Math.abs(rightCached.durationSeconds - targetMetadata.durationSeconds)
      : Number.POSITIVE_INFINITY
    if (leftDelta !== rightDelta) return leftDelta - rightDelta

    return left.relativePath.localeCompare(right.relativePath)
  })

  return candidates.slice(0, Number(argv['max-candidates']))
}

function compareCandidates(left, right) {
  if (left.sharedFrameCount !== right.sharedFrameCount) {
    return right.sharedFrameCount - left.sharedFrameCount
  }
  if (left.durationDeltaSeconds !== right.durationDeltaSeconds) {
    return left.durationDeltaSeconds - right.durationDeltaSeconds
  }

  const leftPixels = (left.width || 0) * (left.height || 0)
  const rightPixels = (right.width || 0) * (right.height || 0)
  if (leftPixels !== rightPixels) {
    return rightPixels - leftPixels
  }

  if ((left.sizeBytes || 0) !== (right.sizeBytes || 0)) {
    return (right.sizeBytes || 0) - (left.sizeBytes || 0)
  }

  return left.relativePath.localeCompare(right.relativePath)
}

function probeVideoMetadata(filePath) {
  return new Promise((resolve) => {
    const child = spawn(
      'ffprobe',
      [
        '-v',
        'error',
        '-show_entries',
        'format=duration:stream=codec_name,width,height,codec_type',
        '-of',
        'json',
        filePath,
      ],
      {
        stdio: ['ignore', 'pipe', 'ignore'],
      }
    )

    let stdout = ''
    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })

    child.on('error', () =>
      resolve({
        durationSeconds: null,
        width: null,
        height: null,
        codecName: null,
      })
    )
    child.on('exit', () => {
      try {
        const parsed = JSON.parse(stdout || '{}')
        const videoStream = Array.isArray(parsed.streams)
          ? parsed.streams.find((stream) => stream.codec_type === 'video')
          : null
        const duration = Number.parseFloat(parsed?.format?.duration)
        resolve({
          durationSeconds: Number.isFinite(duration) ? duration : null,
          width: Number.isFinite(Number(videoStream?.width))
            ? Number(videoStream.width)
            : null,
          height: Number.isFinite(Number(videoStream?.height))
            ? Number(videoStream.height)
            : null,
          codecName: videoStream?.codec_name || null,
        })
      } catch (err) {
        resolve({
          durationSeconds: null,
          width: null,
          height: null,
          codecName: null,
        })
      }
    })
  })
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

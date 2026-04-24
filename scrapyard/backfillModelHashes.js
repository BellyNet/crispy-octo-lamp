const fs = require('fs')
const path = require('path')
const os = require('os')
const { createHash } = require('crypto')
const minimist = require('minimist')

const {
  loadBitwiseHashCache,
  saveBitwiseHashCache,
  addBitwiseHash,
} = require('./bitwiseHasher')
const {
  loadVisualHashCache,
  saveVisualHashCache,
  getVisualHashFromBuffer,
  getVisualHashFromVideoPath,
  addVisualHash,
} = require('./visualHasher')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  boolean: ['help', 'dry-run'],
  default: {
    'dry-run': false,
    'include-video-visuals': false,
  },
})

if (argv.help || !argv.model) {
  printHelp()
  process.exit(argv.help ? 0 : 1)
}

const datasetRoot = path.resolve(
  String(
    argv['dataset-root'] ||
      path.join(
        process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
        '.slopvault',
        'dataset'
      )
  )
)
const modelName = String(argv.model)
const modelRoot = path.join(datasetRoot, modelName)
const dryRun = Boolean(argv['dry-run'])

const imageExts = new Set(['.jpg', '.jpeg', '.png', '.webp'])
const gifExts = new Set(['.gif'])
const videoExts = new Set(['.mp4', '.webm', '.m4v', '.mov'])

main().catch((err) => {
  console.error(`Fatal backfill error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  if (!fs.existsSync(modelRoot)) {
    throw new Error(`Model folder not found: ${modelRoot}`)
  }

  loadBitwiseHashCache()
  loadVisualHashCache()

  let bitwiseCount = 0
  let visualCount = 0
  let scannedCount = 0
  const scannedByBucket = {}
  const bitwiseByBucket = {}
  const visualByBucket = {}

  for (const filePath of collectFiles(modelRoot)) {
    const ext = path.extname(filePath).toLowerCase()
    if (!imageExts.has(ext) && !gifExts.has(ext) && !videoExts.has(ext))
      continue

    scannedCount += 1
    const buffer = fs.readFileSync(filePath)
    const stat = fs.statSync(filePath)
    const relativePath = path
      .relative(datasetRoot, filePath)
      .replace(/\\/g, '/')
    const metadata = {
      root: 'dataset',
      model: modelName,
      bucket: relativePath.split('/')[1] || null,
      relativePath,
      filename: path.basename(filePath),
      mediaType: getMediaType(ext),
      sizeBytes: stat.size,
      modifiedAt: stat.mtime.toISOString(),
      source: 'backfill-model-hashes',
    }
    const bucketName = metadata.bucket || 'unknown'

    scannedByBucket[bucketName] = (scannedByBucket[bucketName] || 0) + 1

    const bitwiseHash = createHash('md5').update(buffer).digest('hex')
    addBitwiseHash(bitwiseHash, metadata)
    bitwiseCount += 1
    bitwiseByBucket[bucketName] = (bitwiseByBucket[bucketName] || 0) + 1

    if (imageExts.has(ext)) {
      const visualHash = await getVisualHashFromBuffer(buffer)
      if (visualHash) {
        addVisualHash(visualHash, metadata)
        visualCount += 1
        visualByBucket[bucketName] = (visualByBucket[bucketName] || 0) + 1
      }
    } else if (argv['include-video-visuals'] && videoExts.has(ext)) {
      const visualHash = await getVisualHashFromVideoPath(filePath)
      if (visualHash) {
        addVisualHash(visualHash, metadata)
        visualCount += 1
        visualByBucket[bucketName] = (visualByBucket[bucketName] || 0) + 1
      }
    }
  }

  if (!dryRun) {
    saveBitwiseHashCache()
    saveVisualHashCache()
  }

  console.log(`Model: ${modelName}`)
  console.log(`Dataset root: ${datasetRoot}`)
  console.log(`Scanned files: ${scannedCount}`)
  console.log(`Scanned by bucket: ${JSON.stringify(scannedByBucket)}`)
  console.log(`Bitwise updates: ${bitwiseCount}`)
  console.log(`Bitwise by bucket: ${JSON.stringify(bitwiseByBucket)}`)
  console.log(`Visual updates: ${visualCount}`)
  console.log(`Visual by bucket: ${JSON.stringify(visualByBucket)}`)
  console.log(
    dryRun
      ? 'Dry run only. No cache files were written.'
      : 'Structured hash caches saved.'
  )
}

function printHelp() {
  console.log(`Usage: node scrapyard/backfillModelHashes.js --model <name> [options]

Options:
  --dataset-root <path>  Override dataset root.
  --dry-run              Scan and report without saving cache changes.
  --include-video-visuals  Extract one representative frame for videos too.
  -h, --help             Show help.

Notes:
  This backfills structured hash metadata for one model at a time.
  Bitwise hashes cover images, GIFs, and videos.
  Visual hashes cover non-GIF images and optionally videos via one sampled frame.
`)
}

function getMediaType(ext) {
  if (videoExts.has(ext)) return 'video'
  if (gifExts.has(ext)) return 'gif'
  return 'image'
}

function collectFiles(root) {
  const files = []
  const stack = [root]

  while (stack.length) {
    const current = stack.pop()
    const entries = fs.readdirSync(current, { withFileTypes: true })

    for (const entry of entries) {
      const fullPath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        stack.push(fullPath)
      } else if (entry.isFile()) {
        files.push(fullPath)
      }
    }
  }

  return files.sort((a, b) => a.localeCompare(b))
}

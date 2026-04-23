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

  for (const filePath of collectFiles(modelRoot)) {
    const ext = path.extname(filePath).toLowerCase()
    if (!imageExts.has(ext) && !gifExts.has(ext)) continue

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
      mediaType: gifExts.has(ext) ? 'gif' : 'image',
      sizeBytes: stat.size,
      modifiedAt: stat.mtime.toISOString(),
      source: 'backfill-model-hashes',
    }

    const bitwiseHash = createHash('md5').update(buffer).digest('hex')
    addBitwiseHash(bitwiseHash, metadata)
    bitwiseCount += 1

    if (imageExts.has(ext)) {
      const visualHash = await getVisualHashFromBuffer(buffer)
      if (visualHash) {
        addVisualHash(visualHash, metadata)
        visualCount += 1
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
  console.log(`Bitwise updates: ${bitwiseCount}`)
  console.log(`Visual updates: ${visualCount}`)
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
  -h, --help             Show help.

Notes:
  This backfills structured hash metadata for one model at a time.
  Current scope mirrors Milkmaid image handling: bitwise for images/GIFs,
  visual for non-GIF images only.
`)
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

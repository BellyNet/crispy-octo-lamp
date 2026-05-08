const fs = require('fs')
const path = require('path')
const os = require('os')
const { createHash } = require('crypto')
const minimist = require('minimist')

const {
  loadNasMp4Index,
  getDefaultDatasetRoot,
  normalizePath,
} = require('./nasMp4Index')
const {
  loadBitwiseHashCache,
  saveBitwiseHashCache,
  addBitwiseHash,
  getBitwiseHashEntries,
} = require('./bitwiseHasher')
const {
  loadVisualHashCache,
  saveVisualHashCache,
  addVisualHash,
  getVisualHashEntries,
  getVisualHashFromVideoPath,
} = require('./visualHasher')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
  },
  boolean: ['help', 'apply', 'bitwise-only', 'visual-only'],
  default: {
    apply: false,
  },
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const datasetRoot = path.resolve(String(argv['dataset-root'] || getDefaultDatasetRoot()))
const nasRoot = path.resolve(String(argv['nas-root'] || process.env.NAS_DATASET_DIR || 'Z:\\dataset'))
const reportDir = path.resolve(
  String(argv['report-dir'] || path.join(process.cwd(), 'tmp', 'restore-nas-mp4-hashes'))
)
const reportPath = path.join(reportDir, 'restore-nas-mp4-hashes-latest.json')
const inputReportPath = argv['input-report']
  ? path.resolve(String(argv['input-report']))
  : null
const progressEvery = Math.max(Number.parseInt(String(argv['progress-every'] || '25'), 10) || 25, 1)

main().catch((error) => {
  console.error(error.message || String(error))
  process.exit(1)
})

async function main() {
  fs.mkdirSync(reportDir, { recursive: true })

  const mode = resolveMode()
  const indexEntries = getTargetEntries(datasetRoot, inputReportPath)

  loadBitwiseHashCache()
  loadVisualHashCache()

  const existingBitwiseRefs = buildRefSet(getBitwiseHashEntries())
  const existingVisualRefs = buildRefSet(getVisualHashEntries())

  const stats = {
    generatedAt: new Date().toISOString(),
    datasetRoot,
    nasRoot,
    apply: Boolean(argv.apply),
    scannedCount: 0,
    missingFileCount: 0,
    bitwiseRefsAdded: 0,
    visualRefsAdded: 0,
    visualHashMisses: 0,
    restoredPaths: [],
    missingFiles: [],
  }

  for (const relativePath of indexEntries) {
    stats.scannedCount += 1
    const absoluteNasPath = path.join(nasRoot, relativePath.replace(/\//g, path.sep))
    if (!fs.existsSync(absoluteNasPath)) {
      stats.missingFileCount += 1
      stats.missingFiles.push(relativePath)
      continue
    }

    const needBitwise = mode !== 'visual' && !existingBitwiseRefs.has(relativePath)
    const needVisual = mode !== 'bitwise' && !existingVisualRefs.has(relativePath)
    if (!needBitwise && !needVisual) {
      continue
    }

    const restored = {
      relativePath,
      bitwiseAdded: false,
      visualAdded: false,
    }

    if (needBitwise) {
      const bitwiseHash = hashFileMd5(absoluteNasPath)
      if (argv.apply) {
        addBitwiseHash(bitwiseHash, { relativePath })
      }
      existingBitwiseRefs.add(relativePath)
      stats.bitwiseRefsAdded += 1
      restored.bitwiseAdded = true
    }

    if (needVisual) {
      const visualHash = await getVisualHashFromVideoPath(absoluteNasPath)
      if (visualHash) {
        if (argv.apply) {
          addVisualHash(visualHash, { relativePath })
        }
        existingVisualRefs.add(relativePath)
        stats.visualRefsAdded += 1
        restored.visualAdded = true
      } else {
        stats.visualHashMisses += 1
      }
    }

    if (restored.bitwiseAdded || restored.visualAdded) {
      stats.restoredPaths.push(restored)
    }

    if (argv.apply && stats.scannedCount % progressEvery === 0) {
      saveBitwiseHashCache()
      saveVisualHashCache()
      fs.writeFileSync(reportPath, JSON.stringify(stats, null, 2) + '\n')
      console.log(
        `[restore-nas-mp4-hashes] ${stats.scannedCount}/${indexEntries.length} scanned | bitwise +${stats.bitwiseRefsAdded} | visual +${stats.visualRefsAdded} | misses ${stats.visualHashMisses}`
      )
    }
  }

  if (argv.apply) {
    saveBitwiseHashCache()
    saveVisualHashCache()
  }

  fs.writeFileSync(reportPath, JSON.stringify(stats, null, 2) + '\n')

  console.log(`Dataset root: ${datasetRoot}`)
  console.log(`NAS root: ${nasRoot}`)
  if (inputReportPath) console.log(`Input report: ${inputReportPath}`)
  console.log(`Mode: ${argv.apply ? 'apply' : 'dry-run'} (${mode})`)
  console.log(`Scanned MP4 entries: ${stats.scannedCount}`)
  console.log(`Missing NAS files: ${stats.missingFileCount}`)
  console.log(`Bitwise refs added: ${stats.bitwiseRefsAdded}`)
  console.log(`Visual refs added: ${stats.visualRefsAdded}`)
  console.log(`Visual hash misses: ${stats.visualHashMisses}`)
  console.log(`Report: ${reportPath}`)
}

function printHelp() {
  console.log(`Usage: node scrapyard/restoreNasMp4Hashes.js [options]

Options:
  --apply                Restore missing MP4 hash refs into local hash stores.
  --dataset-root <path>  Override local dataset root.
  --nas-root <path>      Override NAS dataset root.
  --input-report <path>  Limit work to deletedRelativePaths from a cleanup report.
  --bitwise-only         Restore only bitwise refs.
  --visual-only          Restore only visual refs.
  --progress-every <n>   Save caches and print progress every n files. Default: 25.
  --report-dir <path>    Override report directory.
  -h, --help             Show help.

Notes:
  Scans the local NAS MP4 index and restores missing bitwise/visual refs
  for NAS-backed .mp4 files that no longer exist locally.
`)
}

function resolveMode() {
  if (argv['bitwise-only'] && argv['visual-only']) {
    throw new Error('Use only one of --bitwise-only or --visual-only.')
  }
  if (argv['bitwise-only']) return 'bitwise'
  if (argv['visual-only']) return 'visual'
  return 'both'
}

function getTargetEntries(datasetRoot, reportPath) {
  if (reportPath) {
    const parsed = JSON.parse(fs.readFileSync(reportPath, 'utf8'))
    return Array.from(
      new Set(
        (parsed.deletedRelativePaths || [])
          .map((entry) => normalizePath(entry))
          .filter((entry) => entry.toLowerCase().endsWith('.mp4'))
      )
    ).sort((a, b) => a.localeCompare(b))
  }

  return [...loadNasMp4Index(datasetRoot, { forceReload: true })]
    .map((entry) => normalizePath(entry))
    .filter((entry) => entry.toLowerCase().endsWith('.mp4'))
    .sort((a, b) => a.localeCompare(b))
}

function buildRefSet(entries) {
  const refs = new Set()
  for (const entry of entries || []) {
    for (const ref of Array.isArray(entry?.refs) ? entry.refs : []) {
      const normalized = normalizePath(ref)
      if (normalized) refs.add(normalized)
    }
  }
  return refs
}

function hashFileMd5(filePath) {
  const digest = createHash('md5')
  const handle = fs.openSync(filePath, 'r')
  try {
    const buffer = Buffer.allocUnsafe(1024 * 1024)
    while (true) {
      const bytesRead = fs.readSync(handle, buffer, 0, buffer.length, null)
      if (bytesRead <= 0) break
      digest.update(buffer.subarray(0, bytesRead))
    }
  } finally {
    fs.closeSync(handle)
  }
  return digest.digest('hex')
}

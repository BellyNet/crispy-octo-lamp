const fs = require('fs')
const path = require('path')
const os = require('os')
const minimist = require('minimist')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  string: ['model', 'json-out'],
})

if (argv.help) {
  printHelp()
  process.exit(0)
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
const bitwisePath = path.join(datasetRoot, 'bitwiseHashes.v2.json')
const visualPath = path.join(datasetRoot, 'visualHashes.v2.json')
const targetModel = argv.model ? String(argv.model) : null
const jsonOutPath = argv['json-out']
  ? path.resolve(String(argv['json-out']))
  : null

const imageExts = new Set(['.jpg', '.jpeg', '.png', '.webp'])
const gifExts = new Set(['.gif'])
const videoExts = new Set(['.mp4', '.webm', '.m4v', '.mov'])

try {
  main()
} catch (err) {
  console.error(`Fatal validation error: ${err.stack || err.message}`)
  process.exitCode = 1
}

function main() {
  const bitwiseStore = loadStore(bitwisePath, 'bitwise')
  const visualStore = loadStore(visualPath, 'visual')
  const models = targetModel
    ? [targetModel]
    : fs
        .readdirSync(datasetRoot, { withFileTypes: true })
        .filter((entry) => entry.isDirectory())
        .map((entry) => entry.name)
        .sort((a, b) => a.localeCompare(b))

  const modelReports = models.map((modelName) =>
    buildModelReport(modelName, bitwiseStore, visualStore)
  )

  const summary = {
    modelCount: modelReports.length,
    fullyMatched: modelReports.filter(
      (report) =>
        report.bitwise.missingCount === 0 &&
        report.visual.missingCount === 0 &&
        report.bitwise.extraCount === 0 &&
        report.visual.extraCount === 0
    ).length,
    bitwiseModelsMissing: modelReports.filter(
      (report) => report.bitwise.missingCount > 0
    ).length,
    visualModelsMissing: modelReports.filter(
      (report) => report.visual.missingCount > 0
    ).length,
  }

  const payload = {
    generatedAt: new Date().toISOString(),
    datasetRoot,
    bitwisePath,
    visualPath,
    summary,
    models: modelReports,
  }

  if (jsonOutPath) {
    fs.writeFileSync(jsonOutPath, JSON.stringify(payload, null, 2))
    console.log(`Wrote validation report: ${jsonOutPath}`)
  }

  console.log(`Validated ${summary.modelCount} model(s)`)
  console.log(`Fully matched: ${summary.fullyMatched}`)
  console.log(`Bitwise missing models: ${summary.bitwiseModelsMissing}`)
  console.log(`Visual missing models: ${summary.visualModelsMissing}`)
  console.log('')

  for (const report of modelReports) {
    const status =
      report.bitwise.missingCount === 0 &&
      report.visual.missingCount === 0 &&
      report.bitwise.extraCount === 0 &&
      report.visual.extraCount === 0
        ? 'OK'
        : 'NEEDS_BACKFILL'

    console.log(
      [
        `[${status}]`,
        report.model,
        `files=${report.actual.totalFiles}`,
        `bitwiseMissing=${report.bitwise.missingCount}`,
        `visualMissing=${report.visual.missingCount}`,
      ].join(' ')
    )

    if (report.bitwise.missingSample.length) {
      console.log(
        `  bitwise missing sample: ${report.bitwise.missingSample.join(', ')}`
      )
    }
    if (report.visual.missingSample.length) {
      console.log(
        `  visual missing sample: ${report.visual.missingSample.join(', ')}`
      )
    }
  }
}

function printHelp() {
  console.log(`Usage: node scrapyard/validateModelHashes.js [options]

Options:
  --model <name>         Validate one model only.
  --dataset-root <path>  Override dataset root.
  --json-out <path>      Also save a JSON report.
  -h, --help             Show help.
`)
}

function loadStore(filePath, kind) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`${kind} hash store not found: ${filePath}`)
  }

  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'))
  const refsByModel = new Map()

  for (const entry of parsed.entries || []) {
    for (const ref of entry.refs || []) {
      if (!ref?.model || !ref?.relativePath) continue
      if (!refsByModel.has(ref.model)) {
        refsByModel.set(ref.model, new Set())
      }
      refsByModel.get(ref.model).add(normalizePath(ref.relativePath))
    }
  }

  return {
    filePath,
    refsByModel,
  }
}

function buildModelReport(modelName, bitwiseStore, visualStore) {
  const modelRoot = path.join(datasetRoot, modelName)
  const files = collectModelFiles(modelRoot)

  const actualBitwisePaths = new Set(files.map((file) => file.relativePath))
  const actualVisualPaths = new Set(
    files
      .filter((file) => file.mediaType === 'image')
      .map((file) => file.relativePath)
  )

  const bitwiseRefs = bitwiseStore.refsByModel.get(modelName) || new Set()
  const visualRefs = visualStore.refsByModel.get(modelName) || new Set()

  return {
    model: modelName,
    actual: {
      totalFiles: files.length,
      byBucket: countBy(files, (file) => file.bucket),
      bitwiseExpected: actualBitwisePaths.size,
      visualExpected: actualVisualPaths.size,
    },
    bitwise: comparePathSets(actualBitwisePaths, bitwiseRefs),
    visual: comparePathSets(actualVisualPaths, visualRefs),
  }
}

function collectModelFiles(modelRoot) {
  if (!fs.existsSync(modelRoot)) return []

  const files = []
  const stack = [modelRoot]

  while (stack.length) {
    const current = stack.pop()
    const entries = fs.readdirSync(current, { withFileTypes: true })

    for (const entry of entries) {
      const fullPath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        stack.push(fullPath)
        continue
      }

      if (!entry.isFile()) continue

      const ext = path.extname(entry.name).toLowerCase()
      const mediaType = getMediaType(ext)
      if (!mediaType) continue

      const relativePath = normalizePath(path.relative(datasetRoot, fullPath))
      files.push({
        absolutePath: fullPath,
        relativePath,
        bucket: relativePath.split('/')[1] || 'unknown',
        mediaType,
      })
    }
  }

  return files.sort((a, b) => a.relativePath.localeCompare(b.relativePath))
}

function getMediaType(ext) {
  if (imageExts.has(ext)) return 'image'
  if (gifExts.has(ext)) return 'gif'
  if (videoExts.has(ext)) return 'video'
  return null
}

function comparePathSets(actualSet, refSet) {
  const missing = [...actualSet].filter((value) => !refSet.has(value)).sort()
  const extra = [...refSet].filter((value) => !actualSet.has(value)).sort()

  return {
    actualCount: actualSet.size,
    refCount: refSet.size,
    missingCount: missing.length,
    extraCount: extra.length,
    missingSample: missing.slice(0, 5),
    extraSample: extra.slice(0, 5),
  }
}

function countBy(items, getKey) {
  const counts = {}
  for (const item of items) {
    const key = getKey(item) || 'unknown'
    counts[key] = (counts[key] || 0) + 1
  }
  return counts
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

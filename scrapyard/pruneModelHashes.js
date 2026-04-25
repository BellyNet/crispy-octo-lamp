const fs = require('fs')
const path = require('path')
const os = require('os')
const minimist = require('minimist')

const {
  loadBitwiseHashCache,
  saveBitwiseHashCache,
  removeBitwiseRefs,
} = require('./bitwiseHasher')
const {
  loadVisualHashCache,
  saveVisualHashCache,
  removeVisualRefs,
} = require('./visualHasher')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  boolean: ['help', 'dry-run'],
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

try {
  main()
} catch (err) {
  console.error(`Fatal prune error: ${err.stack || err.message}`)
  process.exitCode = 1
}

function printHelp() {
  console.log(`Usage: node scrapyard/pruneModelHashes.js --model <name> [options]

Options:
  --dataset-root <path>  Override dataset root.
  --dry-run              Report removals without saving.
  -h, --help             Show help.
`)
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

function main() {
  if (!fs.existsSync(modelRoot)) {
    throw new Error(`Model folder not found: ${modelRoot}`)
  }

  const actualPaths = collectModelMediaPaths(modelRoot)

  loadBitwiseHashCache()
  loadVisualHashCache()

  const shouldRemove = (ref) => {
    const relativePath =
      typeof ref === 'string'
        ? normalizePath(ref)
        : normalizePath(ref?.relativePath || '')
    if (!relativePath.startsWith(`${modelName}/`)) return false
    return !actualPaths.has(relativePath)
  }

  const bitwiseRemoved = removeBitwiseRefs(shouldRemove)
  const visualRemoved = removeVisualRefs(shouldRemove)

  if (!dryRun) {
    saveBitwiseHashCache()
    saveVisualHashCache()
  }

  console.log(`Model: ${modelName}`)
  console.log(`Actual dataset media files: ${actualPaths.size}`)
  console.log(`Bitwise refs removed: ${bitwiseRemoved}`)
  console.log(`Visual refs removed: ${visualRemoved}`)
  console.log(dryRun ? 'Dry run only.' : 'Hash stores pruned and saved.')
}

function collectModelMediaPaths(root) {
  const files = new Set()
  const stack = [root]
  const mediaExts = new Set([
    '.jpg',
    '.jpeg',
    '.png',
    '.webp',
    '.gif',
    '.mp4',
    '.webm',
    '.m4v',
    '.mov',
  ])

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
      if (!mediaExts.has(ext)) continue

      files.add(normalizePath(path.relative(datasetRoot, fullPath)))
    }
  }

  return files
}

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
  },
  boolean: ['help', 'apply'],
  default: {
    apply: false,
  },
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
const mirrorRoot = argv['mirror-root']
  ? path.resolve(String(argv['mirror-root']))
  : null
const reportDir = path.resolve(
  String(
    argv['report-dir'] || path.join(process.cwd(), 'tmp', 'cleanup-gif-mp4')
  )
)
const reportPath = path.join(reportDir, 'remove-gif-derived-mp4s-latest.json')

main()

function main() {
  ensureDir(reportDir)
  const matches = collectMatches(datasetRoot)
  const verifiedMatches = mirrorRoot
    ? matches.filter((match) =>
        fs.existsSync(path.join(mirrorRoot, match.mp4RelativePath))
      )
    : matches
  const skippedMissingMirrorPaths = mirrorRoot
    ? matches
        .filter(
          (match) =>
            !fs.existsSync(path.join(mirrorRoot, match.mp4RelativePath))
        )
        .map((match) => match.mp4RelativePath)
    : []
  const deletedRelativePaths = verifiedMatches.map(
    (match) => match.mp4RelativePath
  )
  const affectedModels = Array.from(
    new Set(matches.map((match) => match.model))
  ).sort((a, b) => a.localeCompare(b))

  const report = {
    generatedAt: new Date().toISOString(),
    datasetRoot,
    mirrorRoot,
    apply: Boolean(argv.apply),
    matchedCount: matches.length,
    verifiedCount: verifiedMatches.length,
    skippedMissingMirrorCount: skippedMissingMirrorPaths.length,
    affectedModelCount: affectedModels.length,
    affectedModels,
    deletedRelativePaths,
    skippedMissingMirrorPaths,
    hashCleanup: {
      bitwiseRefsRemoved: 0,
      visualRefsRemoved: 0,
    },
  }

  if (argv.apply && verifiedMatches.length > 0) {
    for (const match of verifiedMatches) {
      if (fs.existsSync(match.mp4AbsolutePath)) {
        fs.unlinkSync(match.mp4AbsolutePath)
      }
    }

    const deletedSet = new Set(deletedRelativePaths)
    loadBitwiseHashCache()
    loadVisualHashCache()
    report.hashCleanup.bitwiseRefsRemoved = removeBitwiseRefs((ref) =>
      deletedSet.has(getRelativePath(ref))
    )
    report.hashCleanup.visualRefsRemoved = removeVisualRefs((ref) =>
      deletedSet.has(getRelativePath(ref))
    )
    saveBitwiseHashCache()
    saveVisualHashCache()
  }

  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2) + '\n')

  console.log(`Dataset root: ${datasetRoot}`)
  console.log(`Matched GIF-derived MP4s: ${matches.length}`)
  if (mirrorRoot) {
    console.log(`Verified on mirror: ${verifiedMatches.length}`)
    console.log(`Missing on mirror: ${skippedMissingMirrorPaths.length}`)
  }
  console.log(`Affected models: ${affectedModels.length}`)
  console.log(argv.apply ? 'Mode: apply' : 'Mode: dry-run')
  if (argv.apply) {
    console.log(
      `Bitwise refs removed: ${report.hashCleanup.bitwiseRefsRemoved}`
    )
    console.log(`Visual refs removed: ${report.hashCleanup.visualRefsRemoved}`)
  }
  console.log(`Report: ${reportPath}`)
}

function printHelp() {
  console.log(`Usage: node scrapyard/removeGifDerivedMp4s.js [options]

Options:
  --apply                Delete matched MP4 files and prune hash refs.
  --dataset-root <path>  Override dataset root.
  --mirror-root <path>   Only delete MP4s that also exist under this mirror root.
  --report-dir <path>    Override report directory.
  -h, --help             Show help.

Match rule:
  Deletes webm/<stem>.mp4 only when the same model also has gif/<stem>.gif.
`)
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function collectMatches(root) {
  const matches = []
  const models = fs
    .readdirSync(root, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b))

  for (const model of models) {
    const gifDir = path.join(root, model, 'gif')
    const webmDir = path.join(root, model, 'webm')
    if (!fs.existsSync(gifDir) || !fs.existsSync(webmDir)) continue

    const gifStems = new Set(
      fs
        .readdirSync(gifDir)
        .filter((name) => name.toLowerCase().endsWith('.gif'))
        .map((name) => name.slice(0, -4).toLowerCase())
    )

    for (const name of fs.readdirSync(webmDir)) {
      if (!name.toLowerCase().endsWith('.mp4')) continue
      const stem = name.slice(0, -4).toLowerCase()
      if (!gifStems.has(stem)) continue

      const mp4AbsolutePath = path.join(webmDir, name)
      matches.push({
        model,
        mp4AbsolutePath,
        mp4RelativePath: normalizePath(path.relative(root, mp4AbsolutePath)),
      })
    }
  }

  return matches
}

function getRelativePath(ref) {
  if (typeof ref === 'string') {
    return normalizePath(ref)
  }

  return normalizePath(ref?.relativePath || '')
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

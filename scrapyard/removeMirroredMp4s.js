const fs = require('fs')
const path = require('path')
const os = require('os')
const minimist = require('minimist')
const { findNasBackedMediaMatch, isEvictableMediaPath } = require('./nasSync')

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
    argv['report-dir'] ||
      path.join(process.cwd(), 'tmp', 'cleanup-mirrored-mp4')
  )
)
const reportPath = path.join(reportDir, 'remove-mirrored-mp4s-latest.json')

main()

function main() {
  if (!mirrorRoot) {
    console.error('Missing required --mirror-root for mirrored media cleanup.')
    process.exit(1)
  }

  ensureDir(reportDir)
  const matches = collectMatches(datasetRoot)
  const verifiedMatches = matches.filter((match) => match.nasMatch)
  const skippedMissingMirrorPaths = matches
    .filter((match) => !match.nasMatch)
    .map((match) => match.relativePath)
  const deletedRelativePaths = verifiedMatches.map(
    (match) => match.relativePath
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
    matchSummary: summarizeMatches(verifiedMatches),
    hashCleanup: {
      bitwiseRefsRemoved: 0,
      visualRefsRemoved: 0,
      refsPreserved: true,
    },
  }

  if (argv.apply && verifiedMatches.length > 0) {
    for (const match of verifiedMatches) {
      if (fs.existsSync(match.absolutePath)) {
        fs.unlinkSync(match.absolutePath)
      }
    }
  }

  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2) + '\n')

  console.log(`Dataset root: ${datasetRoot}`)
  console.log(`Matched mirrored media candidates: ${matches.length}`)
  console.log(`Verified on mirror: ${verifiedMatches.length}`)
  console.log(`Missing on mirror: ${skippedMissingMirrorPaths.length}`)
  console.log(
    `Same-stem matches: ${report.matchSummary.sameStem}; same-path size mismatches: ${report.matchSummary.samePathSizeMismatch}`
  )
  console.log(`Affected models: ${affectedModels.length}`)
  console.log(argv.apply ? 'Mode: apply' : 'Mode: dry-run')
  if (argv.apply) {
    console.log('Hash refs preserved for NAS-backed media.')
  }
  console.log(`Report: ${reportPath}`)
}

function printHelp() {
  console.log(`Usage: node scrapyard/removeMirroredMp4s.js [options]

Options:
  --apply                Delete local media files that also exist under the mirror root.
  --dataset-root <path>  Override dataset root.
  --mirror-root <path>   Required mirror root used to verify safe deletion.
  --report-dir <path>    Override report directory.
  -h, --help             Show help.

Match rule:
  Deletes local .mp4/.m4v/.mov/.webm/.gif when the same relative path exists
  under the mirror root, or when the same model/folder has the same filename
  stem with another media extension.
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
    const modelDir = path.join(root, model)
    const mediaFiles = findMediaFiles(modelDir)

    for (const absolutePath of mediaFiles) {
      const relativePath = normalizePath(path.relative(root, absolutePath))
      matches.push({
        model,
        absolutePath,
        relativePath,
        nasMatch: findNasBackedMediaMatch({
          localPath: absolutePath,
          relativePath,
          nasDatasetDir: mirrorRoot,
        }),
      })
    }
  }

  return matches
}

function findMediaFiles(dirPath) {
  const results = []
  const entries = fs.readdirSync(dirPath, { withFileTypes: true })

  for (const entry of entries) {
    const absolutePath = path.join(dirPath, entry.name)
    if (entry.isDirectory()) {
      results.push(...findMediaFiles(absolutePath))
      continue
    }

    if (entry.isFile() && isEvictableMediaPath(entry.name)) {
      results.push(absolutePath)
    }
  }

  return results
}

function summarizeMatches(matches) {
  return matches.reduce(
    (summary, match) => {
      const nasMatch = match.nasMatch
      if (nasMatch?.matchType === 'same-stem') summary.sameStem += 1
      if (nasMatch?.matchType === 'same-path') summary.samePath += 1
      if (
        nasMatch?.matchType === 'same-path' &&
        nasMatch.sizeMatches === false
      ) {
        summary.samePathSizeMismatch += 1
      }
      summary.bytes += fs.statSync(match.absolutePath).size
      return summary
    },
    {
      samePath: 0,
      sameStem: 0,
      samePathSizeMismatch: 0,
      bytes: 0,
    }
  )
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

const fs = require('fs')
const path = require('path')
const os = require('os')
const minimist = require('minimist')

const {
  loadVisualHashCache,
  saveVisualHashCache,
  getVisualHashFromVideoPath,
  addVisualHash,
  getVisualHashRecord,
  getVisualHashEntries,
} = require('./visualHasher')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
  },
  boolean: ['help', 'write-cache'],
  default: {
    'write-cache': true,
  },
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
  loadVisualHashCache()

  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'))
  const findings = Array.isArray(manifest?.audit?.findings)
    ? manifest.audit.findings
    : []
  const targets = findings.filter(
    (item) =>
      item.reviewType === 'run_error' &&
      (
        String(item.errorDetails || '').includes('tail_decode_error') ||
        (Array.isArray(item.reasons) && item.reasons.some((reason) => String(reason).includes('tail_decode_error')))
      ) &&
      item.relativePath &&
      item.relativePath.includes('/webm/')
  )

  const targetRecords = []
  for (const target of targets) {
    const localPath = resolveTargetPath(target.relativePath)
    if (!localPath) continue

    const visualHash = await getVisualHashFromVideoPath(localPath)
    targetRecords.push({
      ...target,
      localPath,
      visualHash,
    })
  }

  const datasetVideos = collectDatasetVideos(datasetRoot)
  const knownRefs = new Set(
    getVisualHashEntries().flatMap((entry) => (Array.isArray(entry.refs) ? entry.refs : []))
  )
  let newlyHashed = 0

  for (const filePath of datasetVideos) {
    const relativePath = path.relative(datasetRoot, filePath).replace(/\\/g, '/')
    if (knownRefs.has(relativePath)) continue

    const visualHash = await getVisualHashFromVideoPath(filePath)
    if (!visualHash) continue

    addVisualHash(visualHash, { relativePath })
    knownRefs.add(relativePath)
    newlyHashed += 1
  }

  if (argv['write-cache'] && newlyHashed > 0) {
    saveVisualHashCache()
  }

  const matches = targetRecords.map((item) => {
    const record = item.visualHash ? getVisualHashRecord(item.visualHash) : null
    const refs = Array.isArray(record?.refs) ? record.refs : []
    const otherRefs = refs.filter((ref) => ref !== item.relativePath)
    return {
      model: item.model,
      relativePath: item.relativePath,
      error: item.errorDetails || item.reasons || null,
      visualHash: item.visualHash,
      mediaUrl: item.mediaUrl || null,
      mediaPageUrl: item.mediaPageUrl || null,
      matches: otherRefs,
    }
  })

  const matched = matches.filter((item) => item.matches.length > 0)
  const unmatched = matches.filter((item) => item.matches.length === 0)

  console.log(
    JSON.stringify(
      {
        manifestPath,
        targetCount: matches.length,
        matchedCount: matched.length,
        unmatchedCount: unmatched.length,
        newlyHashedDatasetVideos: newlyHashed,
        matched,
      },
      null,
      2
    )
  )
}

function printHelp() {
  console.log(`Usage: node scrapyard/matchErroredVideoVisuals.js [options]

Options:
  --manifest <path>   Manifest to read unresolved run_error findings from.
  --write-cache       Save newly generated video visual hashes into visualHashes.v2.json.
  -h, --help          Show help.
`)
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
        stack.push(fullPath)
      } else if (entry.isFile() && exts.has(path.extname(entry.name).toLowerCase())) {
        files.push(fullPath)
      }
    }
  }

  return files.sort((a, b) => a.localeCompare(b))
}

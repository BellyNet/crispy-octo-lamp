'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
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
const { removeNasMp4Entries, normalizePath } = require('./nasMp4Index')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
  },
  boolean: ['help', 'apply'],
  string: ['audit', 'dataset-root', 'report-dir'],
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
const auditPath = path.resolve(
  String(
    argv.audit ||
      path.join(process.cwd(), 'reports', 'missing-media-audit-latest.json')
  )
)
const reportDir = path.resolve(
  String(argv['report-dir'] || path.join(process.cwd(), 'reports'))
)
const apply = Boolean(argv.apply)

try {
  main()
} catch (err) {
  console.error(`Fatal missing-media prune error: ${err.stack || err.message}`)
  process.exitCode = 1
}

function main() {
  if (!fs.existsSync(auditPath)) {
    throw new Error(`Missing audit report: ${auditPath}`)
  }

  fs.mkdirSync(reportDir, { recursive: true })

  const audit = JSON.parse(
    fs.readFileSync(auditPath, 'utf8').replace(/^\uFEFF/, '')
  )
  const missingRefs = Array.isArray(audit.missingRefs) ? audit.missingRefs : []
  const targets = missingRefs.filter(
    (entry) => !entry.existsLocal && !entry.existsNas
  )
  const hashTargets = new Set(
    targets
      .filter(
        (entry) =>
          entry.sources?.includes('bitwise') ||
          entry.sources?.includes('visual')
      )
      .map((entry) => normalizePath(entry.relativePath))
      .filter(Boolean)
  )
  const bitwiseTargets = new Set(
    targets
      .filter((entry) => entry.sources?.includes('bitwise'))
      .map((entry) => normalizePath(entry.relativePath))
      .filter(Boolean)
  )
  const visualTargets = new Set(
    targets
      .filter((entry) => entry.sources?.includes('visual'))
      .map((entry) => normalizePath(entry.relativePath))
      .filter(Boolean)
  )
  const seenTargets = new Set(
    targets
      .filter((entry) => entry.sources?.includes('seen'))
      .map((entry) => normalizePath(entry.relativePath))
      .filter(Boolean)
  )
  const nasIndexTargets = new Set(
    targets
      .filter((entry) => entry.sources?.includes('nas_mp4_index'))
      .map((entry) => normalizePath(entry.relativePath))
      .filter(Boolean)
  )

  const backupTag = new Date().toISOString().replace(/[:.]/g, '-')
  const backups = []

  loadBitwiseHashCache()
  loadVisualHashCache()

  const bitwiseRefsRemoved = removeBitwiseRefs((ref) =>
    bitwiseTargets.has(normalizePath(ref))
  )
  const visualRefsRemoved = removeVisualRefs((ref) =>
    visualTargets.has(normalizePath(ref))
  )

  const seenResult = pruneSeenIndexes(seenTargets, apply, backups, backupTag)

  if (apply) {
    backupFile(
      path.join(datasetRoot, 'bitwiseHashes.v2.json'),
      backups,
      backupTag
    )
    backupFile(
      path.join(datasetRoot, 'visualHashes.v2.json'),
      backups,
      backupTag
    )
    backupFile(
      path.join(datasetRoot, 'nas-mp4-index.v1.json'),
      backups,
      backupTag
    )
    saveBitwiseHashCache()
    saveVisualHashCache()
    if (nasIndexTargets.size > 0) {
      removeNasMp4Entries(nasIndexTargets, datasetRoot)
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply,
    datasetRoot,
    auditPath,
    targetRefs: targets.length,
    hashTargetRefs: hashTargets.size,
    bitwiseTargetRefs: bitwiseTargets.size,
    visualTargetRefs: visualTargets.size,
    seenTargetRefs: seenTargets.size,
    nasIndexTargetRefs: nasIndexTargets.size,
    bitwiseRefsRemoved,
    visualRefsRemoved,
    seenRecordsRemoved: seenResult.recordsRemoved,
    seenKeysRemoved: seenResult.keysRemoved,
    seenIndexesTouched: seenResult.indexesTouched,
    nasIndexRefsRemoved: apply ? nasIndexTargets.size : 0,
    backups,
  }

  const reportPath = path.join(
    reportDir,
    'prune-missing-media-refs-latest.json'
  )
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2) + '\n')

  console.log(`Audit: ${auditPath}`)
  console.log(`Mode: ${apply ? 'apply' : 'dry-run'}`)
  console.log(`Missing target refs: ${report.targetRefs}`)
  console.log(`Bitwise refs removed: ${bitwiseRefsRemoved}`)
  console.log(`Visual refs removed: ${visualRefsRemoved}`)
  console.log(`Seen records removed: ${seenResult.recordsRemoved}`)
  console.log(`Seen keys removed: ${seenResult.keysRemoved}`)
  console.log(
    `NAS MP4 index refs ${apply ? 'removed' : 'targeted'}: ${nasIndexTargets.size}`
  )
  console.log(`Report: ${reportPath}`)
}

function pruneSeenIndexes(targets, shouldApply, backups, backupTag) {
  const result = {
    recordsRemoved: 0,
    keysRemoved: 0,
    indexesTouched: 0,
  }
  if (targets.size === 0 || !fs.existsSync(datasetRoot)) return result

  for (const model of listModels()) {
    const indexPath = path.join(
      datasetRoot,
      model,
      'log',
      'milkmaid-seen-media-index.json'
    )
    if (!fs.existsSync(indexPath)) continue

    let parsed = null
    try {
      parsed = JSON.parse(
        fs.readFileSync(indexPath, 'utf8').replace(/^\uFEFF/, '')
      )
    } catch {
      continue
    }

    const removedPaths = new Set()
    let keysRemoved = 0
    for (const bucket of ['mediaUrls', 'mediaPageUrls']) {
      const values = parsed[bucket]
      if (!values || typeof values !== 'object') continue
      for (const [key, entry] of Object.entries(values)) {
        const relativePath = normalizePath(entry?.relativePath)
        if (!targets.has(relativePath)) continue
        delete values[key]
        keysRemoved += 1
        removedPaths.add(relativePath)
      }
    }

    if (keysRemoved === 0) continue
    result.recordsRemoved += removedPaths.size
    result.keysRemoved += keysRemoved
    result.indexesTouched += 1

    if (shouldApply) {
      backupFile(indexPath, backups, backupTag)
      parsed.updatedAt = new Date().toISOString()
      fs.writeFileSync(indexPath, JSON.stringify(parsed, null, 2) + '\n')
    }
  }

  return result
}

function listModels() {
  return fs
    .readdirSync(datasetRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b))
}

function backupFile(filePath, backups, backupTag) {
  if (!fs.existsSync(filePath)) return
  const backupPath = `${filePath}.bak-${backupTag}`
  fs.copyFileSync(filePath, backupPath)
  backups.push({
    filePath,
    backupPath,
  })
}

function printHelp() {
  console.log(`Usage: node scrapyard/pruneMissingMediaRefs.js [options]

Options:
  --apply                Save changes. Default is dry-run.
  --audit <path>         Audit JSON from npm run audit:missing-media.
  --dataset-root <path>  Override dataset root.
  --report-dir <path>    Override report directory.
  -h, --help             Show help.

Notes:
  Removes only refs that the audit found missing from both local dataset and
  actual NAS mirror. Historical run logs are never modified.
`)
}

'use strict'

const fs = require('fs')
const path = require('path')
const os = require('os')
const minimist = require('minimist')
const { mergeNasMp4Entries, normalizePath } = require('./nasMp4Index')

const slopvaultRoot = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  '.slopvault'
)
const datasetDir = path.join(slopvaultRoot, 'dataset')
const nasDatasetDir = path.resolve(
  String(process.env.NAS_DATASET_DIR || 'Z:\\dataset')
)

function normalizeModelList(value) {
  if (!value) return []
  return String(value)
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean)
}

function getAllModelNames() {
  if (!fs.existsSync(datasetDir)) return []
  return fs
    .readdirSync(datasetDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b))
}

function listFilesRecursive(rootDir) {
  const pending = [rootDir]
  const files = []

  while (pending.length > 0) {
    const current = pending.pop()
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const absolutePath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        pending.push(absolutePath)
      } else if (entry.isFile()) {
        files.push(absolutePath)
      }
    }
  }

  return files
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes < 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let value = bytes
  let unitIndex = 0
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex += 1
  }
  return `${value.toFixed(value >= 100 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`
}

function tryRemoveEmptyParents(startDir, stopDir) {
  let current = startDir
  const resolvedStop = path.resolve(stopDir)

  while (current && path.resolve(current).startsWith(resolvedStop)) {
    if (path.resolve(current) === resolvedStop) break
    if (!fs.existsSync(current)) {
      current = path.dirname(current)
      continue
    }
    if (fs.readdirSync(current).length > 0) break
    fs.rmdirSync(current)
    current = path.dirname(current)
  }
}

function main() {
  const argv = minimist(process.argv.slice(2), {
    string: ['model', 'models', 'bucket', 'mode'],
    boolean: ['apply', 'dry-run'],
    default: {
      bucket: 'webm',
      'dry-run': true,
    },
  })

  const getOption = (name) => {
    const directValue = argv[name]
    if (directValue !== undefined) return directValue
    const envName = `npm_config_${String(name).replace(/-/g, '_')}`
    return process.env[envName]
  }

  const isTruthy = (value) =>
    value === true ||
    value === 'true' ||
    value === '1' ||
    value === 1 ||
    value === 'yes'

  const requestedModels = [
    ...normalizeModelList(getOption('model')),
    ...normalizeModelList(getOption('models')),
  ]
  const modelNames =
    requestedModels.length > 0 ? requestedModels : getAllModelNames()
  const bucket = String(getOption('bucket') || 'webm').trim()
  const mode = String(getOption('mode') || '').trim().toLowerCase()
  const shouldApply = isTruthy(getOption('apply')) || mode === 'apply'
  const dryRun = !shouldApply

  let scannedFiles = 0
  let eligibleFiles = 0
  let deletedFiles = 0
  let skippedMissingNas = 0
  let skippedSizeMismatch = 0
  let reclaimableBytes = 0
  const confirmedNasRelativePaths = []

  console.log(
    `${dryRun ? 'Dry run' : 'Applying'} local eviction for bucket "${bucket}" against NAS root ${nasDatasetDir}`
  )

  for (const modelName of modelNames) {
    const localBucketDir = path.join(datasetDir, modelName, bucket)
    if (!fs.existsSync(localBucketDir)) continue

    let modelEligible = 0
    let modelMissingNas = 0
    let modelMismatch = 0
    let modelBytes = 0

    for (const localFile of listFilesRecursive(localBucketDir)) {
      scannedFiles += 1
      const relativePath = path.relative(datasetDir, localFile)
      const nasFile = path.join(nasDatasetDir, relativePath)
      const localStat = fs.statSync(localFile)

      if (!fs.existsSync(nasFile)) {
        skippedMissingNas += 1
        modelMissingNas += 1
        continue
      }

      const nasStat = fs.statSync(nasFile)
      if (localStat.size !== nasStat.size) {
        skippedSizeMismatch += 1
        modelMismatch += 1
        continue
      }

      eligibleFiles += 1
      modelEligible += 1
      reclaimableBytes += localStat.size
      modelBytes += localStat.size
      confirmedNasRelativePaths.push(normalizePath(relativePath))

      if (shouldApply) {
        fs.unlinkSync(localFile)
        deletedFiles += 1
        tryRemoveEmptyParents(path.dirname(localFile), localBucketDir)
      }
    }

    console.log(
      ` - ${modelName}: eligible ${modelEligible}, missingNAS ${modelMissingNas}, sizeMismatch ${modelMismatch}, ${dryRun ? 'reclaimable' : 'reclaimed'} ${formatBytes(modelBytes)}`
    )
  }

  console.log(
    `Scanned ${scannedFiles} files; eligible ${eligibleFiles}; missing on NAS ${skippedMissingNas}; size mismatch ${skippedSizeMismatch}.`
  )
  console.log(
    `${dryRun ? 'Potentially reclaimable' : 'Reclaimed'}: ${formatBytes(reclaimableBytes)}`
  )
  if (shouldApply && confirmedNasRelativePaths.length > 0) {
    const indexPath = mergeNasMp4Entries(confirmedNasRelativePaths, datasetDir)
    console.log(
      `Updated NAS MP4 index with ${confirmedNasRelativePaths.length} confirmed path(s): ${indexPath}`
    )
  }
  if (dryRun) {
    console.log(
      'No files were deleted. Re-run with --apply=true, --mode=apply, or npm run evict:nas-media:apply to evict matching local files.'
    )
  }
}

main()

const fs = require('fs')
const path = require('path')
const os = require('os')
const { spawn } = require('child_process')
const minimist = require('minimist')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  string: ['model', 'report-dir'],
  boolean: ['dry-run', 'help', 'all'],
  default: {
    'dry-run': false,
    limit: 0,
  },
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const rootDir = path.join(__dirname, '..')
const slopvaultRoot = path.resolve(
  String(
    argv['slopvault-root'] ||
      path.join(
        process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
        '.slopvault'
      )
  )
)
const datasetRoot = path.join(slopvaultRoot, 'dataset')
const quarantineRoot = path.join(slopvaultRoot, 'quarantine')
const quarantineDatasetRoot = path.join(quarantineRoot, 'dataset')
const quarantineManifestPath = path.join(
  quarantineRoot,
  'quarantine-manifest.json'
)
const salvageOutputRoot = path.join(quarantineRoot, 'salvaged')
const targetModel = argv.model ? String(argv.model).trim().toLowerCase() : ''
const limit = Math.max(parseInt(argv.limit, 10) || 0, 0)
const dryRun = Boolean(argv['dry-run'])
const runStamp = new Date().toISOString().replace(/[:.]/g, '-')
const reportDir = path.resolve(
  String(argv['report-dir'] || path.join(rootDir, 'tmp', 'session-repair'))
)
const summaryPath = path.join(reportDir, `session-repair-${runStamp}.json`)
const latestSummaryPath = path.join(reportDir, 'session-repair-latest.json')
const checkpointPath = path.join(reportDir, 'session-repair-checkpoint.json')

main().catch((err) => {
  console.error(`Fatal session repair error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  ensureDir(reportDir)

  if (!fs.existsSync(quarantineManifestPath)) {
    throw new Error(`Quarantine manifest not found: ${quarantineManifestPath}`)
  }

  const manifest = loadManifest()
  const checkpoint = loadCheckpoint()
  const candidates = selectTailDecodeCandidates(manifest.items, checkpoint)
  const selectedCandidates = limit > 0 ? candidates.slice(0, limit) : candidates

  const summary = {
    generatedAt: new Date().toISOString(),
    dryRun,
    slopvaultRoot,
    datasetRoot,
    quarantineManifestPath,
    reportDir,
    checkpointPath,
    selectionMode: argv.all ? 'all' : 'touched_since_last_completed_run',
    lastCompletedRunAt: checkpoint?.completedAt || null,
    targetModel: targetModel || null,
    limit: limit || null,
    candidateCount: selectedCandidates.length,
    candidates: selectedCandidates.map((item) => ({
      model: item.model,
      relativePath: item.relativePath,
      quarantinePath: item.quarantinePath,
      existingSalvagePath: buildSalvageOutputPath(item.quarantinePath),
      state: item.state || null,
      reasons: Array.isArray(item.reasons) ? item.reasons : [],
    })),
    salvageRun: null,
    promotions: [],
    affectedModels: [],
    modelMaintenance: [],
    unresolved: null,
  }

  if (dryRun) {
    summary.unresolved = buildUnresolvedSummary(manifest.items)
    writeSummary(summary)
    printDryRunSummary(summary)
    return
  }

  printLiveStart(summary)
  const salvageSummary = await runSalvageBatch()
  summary.salvageRun = salvageSummary

  const promotedModels = new Set()
  for (const result of salvageSummary.results || []) {
    const promotion = promoteSalvageResult(result, manifest)
    summary.promotions.push(promotion)
    if (promotion.status === 'promoted') {
      promotedModels.add(promotion.model)
    }
  }

  saveManifest(manifest)

  const affectedModels = [...promotedModels].sort((a, b) => a.localeCompare(b))
  summary.affectedModels = affectedModels

  for (const modelName of affectedModels) {
    const maintenance = await runModelMaintenance(modelName)
    summary.modelMaintenance.push(maintenance)
  }

  summary.unresolved = buildUnresolvedSummary(manifest.items)

  writeSummary(summary)
  writeCheckpoint(summary)
  printLiveSummary(summary)
}

function printHelp() {
  console.log(`Usage: node audit/run-session-repair.js [options]

Options:
  --model <name>          Limit to one model or matching model names.
  --all                   Ignore the last-completed-run checkpoint and scan all matching models.
  --limit <n>             Limit number of tail-decode candidates processed.
  --dry-run               Preview what would happen without writing changes.
  --report-dir <path>     Override report output directory.
  --slopvault-root <path> Override Slopvault root.
  -h, --help              Show help.

What it does:
  1. Batch-salvages quarantined tail-decode videos.
  2. Promotes successful salvages back into dataset paths.
  3. Clears resolved quarantine copies.
  4. Prunes, backfills, and validates hashes for affected models.
  5. Writes a summary report with unresolved quarantine state.
`)
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function loadManifest() {
  const raw = fs.readFileSync(quarantineManifestPath, 'utf8').trim()
  const parsed = raw ? JSON.parse(raw) : {}
  return {
    version: parsed?.version || 1,
    updatedAt: parsed?.updatedAt || null,
    items: Array.isArray(parsed?.items) ? parsed.items : [],
  }
}

function saveManifest(manifest) {
  manifest.updatedAt = new Date().toISOString()
  fs.writeFileSync(quarantineManifestPath, JSON.stringify(manifest, null, 2))
}

function loadCheckpoint() {
  if (!fs.existsSync(checkpointPath)) return null

  try {
    const raw = fs.readFileSync(checkpointPath, 'utf8').trim()
    return raw ? JSON.parse(raw) : null
  } catch (err) {
    return null
  }
}

function selectTailDecodeCandidates(items, checkpoint) {
  const tailCandidates = items
    .filter((item) => item?.sourceType === 'dataset')
    .filter((item) => Array.isArray(item?.reasons))
    .filter((item) => item.reasons.includes('tail_decode_error'))
    .filter((item) => item?.state?.quarantineExists)
    .filter((item) => item.quarantinePath && fs.existsSync(item.quarantinePath))
    .filter((item) =>
      targetModel
        ? String(item.model || '')
            .toLowerCase()
            .includes(targetModel)
        : true
    )

  const lastCompletedAt = parseTimestamp(checkpoint?.completedAt)
  const shouldUseCheckpoint = !argv.all && lastCompletedAt

  const selected = shouldUseCheckpoint
    ? filterCandidatesByTouchedModels(tailCandidates, lastCompletedAt)
    : tailCandidates

  return selected.sort((a, b) =>
    String(a.relativePath || '').localeCompare(String(b.relativePath || ''))
  )
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

function buildSalvageOutputPath(quarantinePath) {
  const normalized = normalizePath(quarantinePath)
  const datasetMarker = '/.slopvault/'
  const markerIndex = normalized.toLowerCase().indexOf(datasetMarker)
  let relative = path.basename(quarantinePath)

  if (markerIndex >= 0) {
    relative = normalized.slice(markerIndex + datasetMarker.length)
  }

  const parsed = path.parse(relative)
  return path.join(outputRoot(), parsed.dir, `${parsed.name}.salvaged.mp4`)
}

function outputRoot() {
  return salvageOutputRoot
}

function parseTimestamp(value) {
  const ms = new Date(value || '').getTime()
  return Number.isFinite(ms) && ms > 0 ? ms : null
}

function getFileMtimeMs(filePath) {
  try {
    return fs.existsSync(filePath) ? fs.statSync(filePath).mtimeMs : null
  } catch (err) {
    return null
  }
}

function getCandidateTouchedAtMs(item) {
  const timestamps = [
    parseTimestamp(item?.repair?.lastAttemptAt),
    parseTimestamp(item?.repair?.repairedAt),
    parseTimestamp(item?.audit?.movedAt),
    parseTimestamp(item?.modifiedAt),
    getFileMtimeMs(item?.quarantinePath),
    getFileMtimeMs(buildSalvageOutputPath(item?.quarantinePath || '')),
  ].filter((value) => Number.isFinite(value) && value > 0)

  return timestamps.length ? Math.max(...timestamps) : 0
}

function filterCandidatesByTouchedModels(items, lastCompletedAt) {
  const touchedModels = new Set(
    items
      .filter((item) => getCandidateTouchedAtMs(item) > lastCompletedAt)
      .map((item) => String(item.model || '').trim())
      .filter(Boolean)
  )

  if (touchedModels.size === 0) return []
  return items.filter((item) =>
    touchedModels.has(String(item.model || '').trim())
  )
}

async function runSalvageBatch() {
  const args = [path.join(rootDir, 'audit', 'salvage-quarantine-tail-videos.js')]
  if (argv.model) {
    args.push('--model', String(argv.model))
  }
  if (limit > 0) {
    args.push('--limit', String(limit))
  }
  args.push('--output-dir', reportDir)

  console.log('')
  console.log('Starting tail-decode salvage batch...')
  await runNode(args, { streamOutput: true })

  const latestJsonPath = path.join(reportDir, 'salvage-tail-videos-latest.json')
  if (!fs.existsSync(latestJsonPath)) {
    throw new Error(`Expected salvage summary not found: ${latestJsonPath}`)
  }

  return JSON.parse(fs.readFileSync(latestJsonPath, 'utf8'))
}

function promoteSalvageResult(result, manifest) {
  const modelName = String(result.model || '').trim()
  const relativePath = normalizePath(result.relativePath)
  const manifestItem = manifest.items.find(
    (item) =>
      normalizePath(item?.relativePath) === relativePath ||
      normalizePath(item?.quarantinePath) ===
        normalizePath(result.quarantinePath)
  )

  if (
    result.status !== 'salvaged' ||
    !result.outputTailDecodeOk ||
    !result.outputPath
  ) {
    return {
      model: modelName,
      relativePath,
      status:
        result.status === 'failed'
          ? 'salvage_failed'
          : `skipped_${result.status}`,
      quarantinePath: result.quarantinePath || null,
      outputPath: result.outputPath || null,
    }
  }

  const datasetPath = path.join(
    datasetRoot,
    relativePath.replace(/\//g, path.sep)
  )
  const quarantinePath =
    result.quarantinePath ||
    manifestItem?.quarantinePath ||
    path.join(quarantineDatasetRoot, relativePath.replace(/\//g, path.sep))
  const outputPath = path.resolve(String(result.outputPath))

  if (!fs.existsSync(outputPath)) {
    return {
      model: modelName,
      relativePath,
      status: 'missing_salvage_output',
      quarantinePath,
      outputPath,
    }
  }

  if (fs.existsSync(datasetPath)) {
    return {
      model: modelName,
      relativePath,
      status: 'dataset_conflict',
      quarantinePath,
      datasetPath,
      outputPath,
    }
  }

  ensureDir(path.dirname(datasetPath))
  fs.renameSync(outputPath, datasetPath)

  const sidecarPath = `${outputPath}.json`
  removeFileIfExists(sidecarPath)
  removeFileIfExists(quarantinePath)
  cleanupEmptyParentDirs(path.dirname(outputPath), salvageOutputRoot)
  cleanupEmptyParentDirs(path.dirname(quarantinePath), quarantineRoot)

  const stat = fs.statSync(datasetPath)
  const item = manifestItem || {
    id: `dataset:${relativePath}`,
    sourceType: 'dataset',
    model: modelName,
    relativePath,
    reasons: ['tail_decode_error'],
  }

  item.sourceType = 'dataset'
  item.model = modelName
  item.relativePath = relativePath
  item.quarantinePath = quarantinePath
  item.sizeBytes = stat.size
  item.modifiedAt = stat.mtime.toISOString()
  item.state = {
    activeDatasetExists: true,
    activeDatasetPath: datasetPath,
    quarantineExists: false,
    repairState: 'repaired',
  }
  item.repair = {
    ...(item.repair || {}),
    repairedAt: new Date().toISOString(),
    repairedBy: 'session-repair',
    replacementPath: datasetPath,
    replacementRelativePath: relativePath,
    replacementSizeBytes: stat.size,
    replacementDurationSeconds: result.outputDurationSeconds || null,
    sourceSalvagePath: outputPath,
    quarantineCleared: true,
    lastAttemptOutcome: 'salvaged_and_promoted',
  }

  if (!manifestItem) {
    manifest.items.push(item)
  }

  return {
    model: modelName,
    relativePath,
    status: 'promoted',
    datasetPath,
    quarantinePath,
    promotedSizeBytes: stat.size,
    trimmedSeconds: result.trimmedSeconds ?? null,
  }
}

function removeFileIfExists(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return false
  fs.unlinkSync(filePath)
  return true
}

function cleanupEmptyParentDirs(startDir, stopDir) {
  let current = startDir
  const normalizedStop = path.resolve(stopDir)

  while (current && path.resolve(current).startsWith(normalizedStop)) {
    if (path.resolve(current) === normalizedStop) break
    if (!fs.existsSync(current)) {
      current = path.dirname(current)
      continue
    }
    if (fs.readdirSync(current).length > 0) break
    fs.rmdirSync(current)
    current = path.dirname(current)
  }
}

async function runModelMaintenance(modelName) {
  const modelReportDir = path.join(reportDir, 'models')
  ensureDir(modelReportDir)
  const validateJsonPath = path.join(modelReportDir, `${modelName}-validate.json`)

  console.log('')
  console.log(`Refreshing hashes for ${modelName}...`)
  const prune = await runNode([
    path.join(rootDir, 'scrapyard', 'pruneModelHashes.js'),
    '--model',
    modelName,
  ], { streamOutput: true })
  const backfill = await runNode([
    path.join(rootDir, 'scrapyard', 'backfillModelHashes.js'),
    '--model',
    modelName,
    '--include-video-visuals',
  ], { streamOutput: true })
  const validate = await runNode([
    path.join(rootDir, 'scrapyard', 'validateModelHashes.js'),
    '--model',
    modelName,
    '--json-out',
    validateJsonPath,
  ], { streamOutput: true })

  let validationReport = null
  if (fs.existsSync(validateJsonPath)) {
    validationReport = JSON.parse(fs.readFileSync(validateJsonPath, 'utf8'))
  }

  const modelSummary = validationReport?.models?.[0] || null
  const fullyMatched = Boolean(
    modelSummary &&
      modelSummary.bitwise?.missingCount === 0 &&
      modelSummary.visual?.missingCount === 0 &&
      modelSummary.videoVisual?.missingCount === 0 &&
      modelSummary.bitwise?.extraCount === 0 &&
      modelSummary.visual?.extraCount === 0 &&
      modelSummary.videoVisual?.extraCount === 0
  )

  return {
    model: modelName,
    fullyMatched,
    validateJsonPath,
    commands: {
      prune: prune.command,
      backfill: backfill.command,
      validate: validate.command,
    },
    summaries: {
      prune: prune.stdout,
      backfill: backfill.stdout,
      validate: validate.stdout,
    },
    report: modelSummary,
  }
}

function buildUnresolvedSummary(items) {
  const filtered = targetModel
    ? items.filter((item) =>
        String(item.model || '')
          .toLowerCase()
          .includes(targetModel)
      )
    : items

  const unresolvedTailDecode = filtered.filter(
    (item) =>
      Array.isArray(item?.reasons) &&
      item.reasons.includes('tail_decode_error') &&
      item?.state?.quarantineExists
  )

  const states = {
    total: filtered.length,
    quarantined: 0,
    repaired: 0,
    replacementPresentPendingReview: 0,
    missingBoth: 0,
  }

  for (const item of filtered) {
    const state = String(item?.state?.repairState || '')
    if (state === 'quarantined') states.quarantined += 1
    else if (state === 'repaired') states.repaired += 1
    else if (state === 'replacement_present_pending_review') {
      states.replacementPresentPendingReview += 1
    } else if (state === 'missing_both') {
      states.missingBoth += 1
    }
  }

  return {
    stateCounts: states,
    unresolvedTailDecodeCount: unresolvedTailDecode.length,
    unresolvedTailDecodeSample: unresolvedTailDecode
      .slice(0, 20)
      .map((item) => ({
        model: item.model,
        relativePath: item.relativePath,
        quarantinePath: item.quarantinePath,
        repairState: item?.state?.repairState || null,
      })),
  }
}

function writeSummary(summary) {
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2))
  fs.writeFileSync(latestSummaryPath, JSON.stringify(summary, null, 2))
}

function writeCheckpoint(summary) {
  const checkpoint = {
    completedAt: summary.generatedAt,
    reportPath: summaryPath,
    latestReportPath: latestSummaryPath,
    targetModel: summary.targetModel,
    selectionMode: summary.selectionMode,
    affectedModels: summary.affectedModels,
    candidateCount: summary.candidateCount,
  }
  fs.writeFileSync(checkpointPath, JSON.stringify(checkpoint, null, 2))
}

function printDryRunSummary(summary) {
  console.log(`Session repair dry run`)
  console.log(`Selection mode: ${summary.selectionMode}`)
  console.log(`Last completed run: ${summary.lastCompletedRunAt || 'none'}`)
  console.log(`Candidates: ${summary.candidateCount}`)
  console.log(`Report: ${latestSummaryPath}`)
  console.log(
    `Unresolved tail-decode quarantines: ${summary.unresolved.unresolvedTailDecodeCount}`
  )
}

function printLiveStart(summary) {
  console.log(`Session repair starting`)
  console.log(`Selection mode: ${summary.selectionMode}`)
  console.log(`Last completed run: ${summary.lastCompletedRunAt || 'none'}`)
  console.log(`Candidates queued: ${summary.candidateCount}`)
  console.log(`Report: ${latestSummaryPath}`)
}

function printLiveSummary(summary) {
  console.log(`Session repair complete`)
  console.log(`Selection mode: ${summary.selectionMode}`)
  console.log(`Last completed run: ${summary.lastCompletedRunAt || 'none'}`)
  console.log(`Candidates scanned: ${summary.candidateCount}`)
  console.log(
    `Promoted salvages: ${
      summary.promotions.filter((item) => item.status === 'promoted').length
    }`
  )
  console.log(`Affected models: ${summary.affectedModels.length}`)
  console.log(
    `Unresolved tail-decode quarantines: ${summary.unresolved.unresolvedTailDecodeCount}`
  )
  console.log(`Summary: ${latestSummaryPath}`)
}

function runNode(args, options = {}) {
  return new Promise((resolve, reject) => {
    const command = [process.execPath, ...args].join(' ')
    const child = spawn(process.execPath, args, {
      cwd: rootDir,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    })

    let stdout = ''
    let stderr = ''

    child.stdout.on('data', (chunk) => {
      const text = chunk.toString()
      stdout += text
      if (options.streamOutput) process.stdout.write(text)
    })
    child.stderr.on('data', (chunk) => {
      const text = chunk.toString()
      stderr += text
      if (options.streamOutput) process.stderr.write(text)
    })
    child.on('error', reject)
    child.on('exit', (code) => {
      if (code !== 0) {
        const err = new Error(
          `Command failed (${code}): ${command}\n${stderr || stdout}`.trim()
        )
        err.code = code
        return reject(err)
      }
      resolve({
        command,
        stdout: stdout.trim(),
        stderr: stderr.trim(),
      })
    })
  })
}

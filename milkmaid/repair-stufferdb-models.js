const fs = require('fs')
const path = require('path')
const os = require('os')
const minimist = require('minimist')
const { spawn } = require('child_process')
const {
  upsertErrorsSource,
  latestJsonPath: errorsToCheckLatestJsonPath,
} = require('../scrapyard/errorsToCheck')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  string: [
    'model',
    'models',
    'start-from',
    'registry',
    'log-dir',
    'dataset-root',
    'nas-dataset-root',
    'errors-file',
  ],
  boolean: [
    'stop-on-error',
    'scrape',
    'skip-nas-sync',
    'only-errors',
    'sync-pending-only',
  ],
  default: {
    limit: 0,
    scrape: false,
    'stop-on-error': false,
    'skip-nas-sync': false,
    'only-errors': false,
    'sync-pending-only': false,
  },
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const rootDir = path.join(__dirname, '..')
const appDataRoot =
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming')
const registryPath = path.resolve(
  String(argv.registry || path.join(rootDir, 'model_aliases.json'))
)
const datasetRoot = path.resolve(
  String(argv['dataset-root'] || path.join(appDataRoot, '.slopvault', 'dataset'))
)
const logDir = path.resolve(
  String(argv['log-dir'] || path.join(rootDir, 'tmp', 'repair-stufferdb'))
)
const latestReportPath = path.join(logDir, 'repair-stufferdb-latest.json')
const latestTextPath = path.join(logDir, 'repair-stufferdb-latest.txt')
const statePath = path.join(logDir, 'repair-stufferdb-state.json')
const nasDatasetRoot = path.resolve(
  String(argv['nas-dataset-root'] || process.env.NAS_DATASET_DIR || 'Z:\\dataset')
)
const errorsFilePath = path.resolve(
  String(argv['errors-file'] || errorsToCheckLatestJsonPath)
)
const limit = Math.max(parseInt(argv.limit, 10) || 0, 0)
const singleModel = argv.model ? String(argv.model).trim() : null
const explicitModels = String(argv.models || '')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean)
const startFrom = argv['start-from'] ? String(argv['start-from']).trim() : null
const rawArgv = process.argv.slice(2)
const shouldScrape =
  rawArgv.includes('--scrape') ||
  parseBooleanEnv(process.env.npm_config_scrape)
const skipNasSync =
  rawArgv.includes('--skip-nas-sync') ||
  parseBooleanEnv(process.env.npm_config_skip_nas_sync)
const onlyErrors =
  rawArgv.includes('--only-errors') ||
  parseBooleanEnv(process.env.npm_config_only_errors)
const syncPendingOnly =
  rawArgv.includes('--sync-pending-only') ||
  parseBooleanEnv(process.env.npm_config_sync_pending_only)

main().catch((err) => {
  console.error(`Fatal repair-runner error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  ensureDir(logDir)
  const registry = loadRegistry()
  const queue = buildQueue(registry)
  const selectedQueue = applySelection(queue)
  const state = loadState()

  const report = {
    generatedAt: new Date().toISOString(),
    mode: syncPendingOnly
      ? 'nas_sync_only'
      : shouldScrape
        ? 'update_and_repair'
        : 'local_dataset_repair',
    registryPath,
    datasetRoot,
    nasDatasetRoot,
    totalModelsInDataset: queue.length,
    selectedModels: selectedQueue.length,
    scrapeEnabled: shouldScrape,
    skipNasSync,
    stopOnError: Boolean(argv['stop-on-error']),
    pendingNasSyncModelsAtStart: [...state.pendingNasSyncModels].sort((a, b) =>
      a.localeCompare(b)
    ),
    totals: buildEmptyTotals(),
    results: [],
    nasSync: null,
  }

  console.log(
    `Repair queue: ${selectedQueue.length} model(s) selected from ${queue.length} dataset folders`
  )
  if (onlyErrors) {
    console.log(`Error-targeted mode is enabled via ${errorsFilePath}`)
  }
  if (syncPendingOnly) {
    console.log(
      `Sync-only mode is enabled for ${report.pendingNasSyncModelsAtStart.length} pending model(s).`
    )
  }
  console.log(
    syncPendingOnly
      ? 'Local repair steps are disabled; only pending NAS sync work will run.'
      : shouldScrape
      ? 'Scrape refresh is enabled for models with StufferDB sources.'
      : 'Scrape refresh is disabled; checking local dataset state only.'
  )

  if (!syncPendingOnly) {
    for (let index = 0; index < selectedQueue.length; index += 1) {
      const item = selectedQueue[index]
      console.log('')
      console.log(`[${index + 1}/${selectedQueue.length}] Repairing ${item.model}`)

      const result = await runModelRepair(item)
      report.results.push(result)
      report.totals = calculateTotals(report.results)
      writeReport(report)

      if (!skipNasSync) {
        queuePendingNasSyncModel(state, item.model)
      }

      if (
        argv['stop-on-error'] &&
        (result.scrape.status === 'failed' ||
          !result.hashPrune.ok ||
          !result.hashBackfill.ok ||
          !result.validation.ok)
      ) {
        console.log('Stopping on first error because --stop-on-error was set.')
        break
      }
    }
  }

  report.nasSync = skipNasSync
    ? {
        attempted: [],
        synced: [],
        failed: [],
        pendingAfterRun: [...state.pendingNasSyncModels].sort((a, b) =>
          a.localeCompare(b)
        ),
        skippedBecause: 'skip_nas_sync',
      }
    : await syncPendingModelsToNas(state)

  writeReport(report)
  console.log('')
  console.log(`Latest report: ${latestReportPath}`)
}

function printHelp() {
  console.log(`Usage: node milkmaid/repair-stufferdb-models.js [options]

Options:
  --model <name>           Repair one model only.
  --models <a,b,c>         Repair a comma-separated set of models.
  --start-from <name>      Start from this model name.
  --limit <n>              Only process the first n selected models.
  --scrape                 Re-run milkmaid before local repair.
  --skip-nas-sync          Skip NAS sync and leave models queued for a later run.
  --dataset-root <path>    Override local dataset root.
  --nas-dataset-root <p>   Override NAS dataset root.
  --registry <path>        Override model_aliases.json path.
  --log-dir <path>         Override repair runner report directory.
  --stop-on-error          Stop the batch when one model fails.
  --only-errors            Only repair models currently listed in errors-to-check.
  --errors-file <path>     Override errors-to-check JSON path.
  --sync-pending-only      Skip repair work and only flush pending NAS sync models.
  -h, --help               Show help.

Default behavior:
  - walks local model folders under the dataset root
  - prunes, backfills, and validates hashes for each model
  - clears stale milkmaid error artifacts when a model is clean
  - syncs repaired models to the NAS

Add --scrape when you want this to also run StufferDB update scraping first.
`)
}

function buildEmptyTotals() {
  return {
    filesSaved: 0,
    sourceItemsHandled: 0,
    duplicates: 0,
    runErrors: 0,
    modelsProcessed: 0,
    modelsClean: 0,
    modelsNeedingAttention: 0,
  }
}

function parseBooleanEnv(envValue) {
  if (typeof envValue === 'boolean') return envValue
  if (envValue == null || envValue === '') return false
  const normalized = String(envValue).trim().toLowerCase()
  return !['0', 'false', 'no', 'off'].includes(normalized)
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function loadRegistry() {
  if (!fs.existsSync(registryPath)) return {}
  try {
    return JSON.parse(fs.readFileSync(registryPath, 'utf8'))
  } catch {
    return {}
  }
}

function buildQueue(registry) {
  if (!fs.existsSync(datasetRoot)) return []

  return fs
    .readdirSync(datasetRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => {
      const model = entry.name
      const sources = Array.isArray(registry?.[model]?.sources?.stufferdb)
        ? registry[model].sources.stufferdb
            .map((source) => String(source?.url || '').trim())
            .filter(Boolean)
        : []

      return {
        model,
        modelPath: path.join(datasetRoot, model),
        sources,
      }
    })
    .sort((left, right) => left.model.localeCompare(right.model))
}

function applySelection(queue) {
  let next = queue
  const errorModels = onlyErrors ? loadErroredModels(errorsFilePath) : null

  if (errorModels) {
    next = next.filter((item) => errorModels.has(item.model))
  }

  if (singleModel) {
    next = next.filter((item) => item.model === singleModel)
  }

  if (explicitModels.length) {
    const wanted = new Set(explicitModels)
    next = next.filter((item) => wanted.has(item.model))
  }

  if (startFrom) {
    next = next.filter((item) => item.model.localeCompare(startFrom) >= 0)
  }

  if (limit > 0) {
    next = next.slice(0, limit)
  }

  return next
}

function loadErroredModels(filePath) {
  if (!fs.existsSync(filePath)) return new Set()

  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'))
    const sections = Array.isArray(parsed?.sections) ? parsed.sections : []
    const repairSection = sections.find(
      (section) => String(section?.source || '').trim() === 'repair'
    )
    const items = Array.isArray(repairSection?.items) ? repairSection.items : []

    return new Set(
      items
        .map((item) => String(item?.model || '').trim())
        .filter(Boolean)
    )
  } catch {
    return new Set()
  }
}

async function runModelRepair(item) {
  const startedAt = new Date().toISOString()
  const result = {
    model: item.model,
    modelPath: item.modelPath,
    startedAt,
    sourceUrls: item.sources,
    scrapeRuns: [],
    scrape: buildSkippedScrapeSummary(
      shouldScrape ? 'no_sources' : 'disabled',
      item.sources
    ),
    hashPrune: null,
    hashBackfill: null,
    validation: null,
    finishedAt: null,
    lastRunSummaryPath: getLastRunSummaryPath(item.model),
    lastRunSummary: null,
    localRunErrors: [],
    errorArtifactsCleared: [],
  }

  if (shouldScrape && item.sources.length > 0) {
    for (const sourceUrl of item.sources) {
      const scrapeRun = await runCommand(
        process.execPath,
        [
          path.join(rootDir, 'milkmaid', 'milkmaid.js'),
          sourceUrl,
          '--model',
          item.model,
          '--skip-nas-sync',
        ],
        { cwd: rootDir, label: `milkmaid:${item.model}` }
      )
      result.scrapeRuns.push(scrapeRun)
      if (!scrapeRun.ok && argv['stop-on-error']) {
        break
      }
    }

    result.scrape = summarizeScrapeRuns(result.scrapeRuns)
  }

  result.hashPrune = await runCommand(
    process.execPath,
    [path.join(rootDir, 'scrapyard', 'pruneModelHashes.js'), '--model', item.model],
    { cwd: rootDir, label: `prune:${item.model}` }
  )

  result.hashBackfill = await runCommand(
    process.execPath,
    [
      path.join(rootDir, 'scrapyard', 'backfillModelHashes.js'),
      '--model',
      item.model,
      '--include-video-visuals',
    ],
    { cwd: rootDir, label: `backfill:${item.model}` }
  )

  const validationPath = path.join(logDir, `${item.model}.validate.json`)
  result.validation = await runCommand(
    process.execPath,
    [
      path.join(rootDir, 'scrapyard', 'validateModelHashes.js'),
      '--model',
      item.model,
      '--json-out',
      validationPath,
    ],
    { cwd: rootDir, label: `validate:${item.model}` }
  )
  result.validation = hydrateValidationResult(result.validation, validationPath)

  result.lastRunSummary = readJsonIfExists(result.lastRunSummaryPath)
  result.localRunErrors = readLocalRunErrors(item.model)

  if (shouldClearErrorArtifacts(result)) {
    result.errorArtifactsCleared = clearResolvedErrorArtifacts(item.model)
  }

  result.finishedAt = new Date().toISOString()
  result.needsAttention = modelNeedsAttention(result)
  return result
}

function buildSkippedScrapeSummary(reason, sourceUrls) {
  return {
    ok: true,
    skipped: true,
    status: reason === 'disabled' ? 'skipped' : 'skipped_no_sources',
    reason,
    runs: 0,
    failures: 0,
    labels: [],
    commands: [],
    sourceCount: Array.isArray(sourceUrls) ? sourceUrls.length : 0,
  }
}

function summarizeScrapeRuns(scrapeRuns) {
  const failures = scrapeRuns.filter((run) => !run.ok)
  return {
    ok: failures.length === 0,
    skipped: false,
    status: failures.length === 0 ? 'ok' : 'failed',
    reason: null,
    runs: scrapeRuns.length,
    failures: failures.length,
    labels: scrapeRuns.map((run) => run.label),
    commands: scrapeRuns.map((run) => run.command),
  }
}

function hydrateValidationResult(validationResult, validationPath) {
  const hydrated = { ...validationResult }
  if (!fs.existsSync(validationPath)) {
    hydrated.clean = false
    hydrated.summary = null
    return hydrated
  }

  try {
    const payload = JSON.parse(fs.readFileSync(validationPath, 'utf8'))
    const modelReport = Array.isArray(payload.models) ? payload.models[0] : null
    const counts = {
      bitwiseMissing: Number(modelReport?.bitwise?.missingCount || 0),
      bitwiseExtra: Number(modelReport?.bitwise?.extraCount || 0),
      visualMissing: Number(modelReport?.visual?.missingCount || 0),
      visualExtra: Number(modelReport?.visual?.extraCount || 0),
      videoVisualMissing: Number(modelReport?.videoVisual?.missingCount || 0),
      videoVisualExtra: Number(modelReport?.videoVisual?.extraCount || 0),
    }
    hydrated.summary = counts
    hydrated.clean = Object.values(counts).every((count) => count === 0)
    hydrated.ok = hydrated.ok && hydrated.clean
  } catch (err) {
    hydrated.clean = false
    hydrated.summary = { parseError: err.message }
    hydrated.ok = false
  }

  return hydrated
}

function getLastRunSummaryPath(modelName) {
  return path.join(datasetRoot, modelName, 'milkmaid-last-run.json')
}

function readJsonIfExists(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'))
  } catch (err) {
    return { parseError: err.message }
  }
}

function getModelLogDir(modelName) {
  return path.join(datasetRoot, modelName, 'log')
}

function readLocalRunErrors(modelName) {
  const logDirPath = getModelLogDir(modelName)
  const latestJsonPath = path.join(logDirPath, 'milkmaid-run-errors-latest.json')
  const latestHtmlPath = path.join(logDirPath, 'milkmaid-run-errors-latest.html')
  const payload = readJsonIfExists(latestJsonPath)

  return [
    payload
      ? {
          kind: 'latest_json',
          path: latestJsonPath,
          errorCount: Number(payload?.errorCount || payload?.errors?.length || 0),
        }
      : null,
    fs.existsSync(latestHtmlPath)
      ? {
          kind: 'latest_html',
          path: latestHtmlPath,
        }
      : null,
  ].filter(Boolean)
}

function shouldClearErrorArtifacts(result) {
  const scrapeFailed = result.scrape.status === 'failed'
  return Boolean(result.validation?.clean && !scrapeFailed)
}

function clearResolvedErrorArtifacts(modelName) {
  const logDirPath = getModelLogDir(modelName)
  if (!fs.existsSync(logDirPath)) return []

  const cleared = []
  for (const fileName of fs.readdirSync(logDirPath)) {
    if (!fileName.startsWith('milkmaid-run-errors-')) continue
    const filePath = path.join(logDirPath, fileName)
    try {
      fs.unlinkSync(filePath)
      cleared.push(filePath)
    } catch {
      // Leave stubborn files alone; they will still be reported by localRunErrors.
    }
  }
  return cleared
}

function modelNeedsAttention(result) {
  const scrapeFailed = result.scrape.status === 'failed'
  const validationFailed = !result.validation?.ok
  const currentRunErrorCount = shouldScrape
    ? getRunErrorCount(result.lastRunSummary)
    : 0
  const lingeringLogErrors =
    !result.validation?.clean && result.localRunErrors.some((item) => item.kind === 'latest_json')

  return scrapeFailed || validationFailed || currentRunErrorCount > 0 || lingeringLogErrors
}

function getRunErrorCount(summary) {
  if (!summary || summary.parseError) return 0
  return Number(summary.errorCount || 0)
}

function runCommand(command, args, { cwd, label }) {
  return new Promise((resolve) => {
    const child = spawn(command, args, {
      cwd,
      stdio: 'inherit',
      windowsHide: true,
    })

    child.on('error', (err) => {
      resolve({
        ok: false,
        code: null,
        label,
        command: [command, ...args].join(' '),
        error: err.message,
      })
    })

    child.on('exit', (code) => {
      resolve({
        ok: code === 0,
        code,
        label,
        command: [command, ...args].join(' '),
      })
    })
  })
}

function writeReport(report) {
  report.totals = calculateTotals(report.results)
  const payload = JSON.stringify(report, null, 2)
  fs.writeFileSync(latestReportPath, payload)

  const lines = [
    `Generated: ${report.generatedAt}`,
    `Mode: ${report.mode}`,
    `Dataset: ${report.datasetRoot}`,
    `Selected models: ${report.selectedModels}`,
    `Totals: saved=${report.totals.filesSaved} sourceItems=${report.totals.sourceItemsHandled} dupes=${report.totals.duplicates} runErrors=${report.totals.runErrors} clean=${report.totals.modelsClean} attention=${report.totals.modelsNeedingAttention}`,
    '',
  ]

  for (const item of report.results) {
    lines.push(
      [
        item.model,
        `scrape=${item.scrape.status}`,
        `prune=${item.hashPrune?.ok ? 'ok' : 'fail'}`,
        `backfill=${item.hashBackfill?.ok ? 'ok' : 'fail'}`,
        `validate=${item.validation?.ok ? 'clean' : 'needs_attention'}`,
        `attention=${item.needsAttention ? 'yes' : 'no'}`,
      ].join(' :: ')
    )

    if (shouldScrape && item.lastRunSummary && !item.lastRunSummary.parseError) {
      lines.push(
        `  scrape saved=${item.lastRunSummary.successCount || 0} dupes=${item.lastRunSummary.duplicateCount || 0} errors=${item.lastRunSummary.errorCount || 0}`
      )
    }

    if (item.validation?.summary && !item.validation.summary.parseError) {
      lines.push(
        `  validation bitwiseMissing=${item.validation.summary.bitwiseMissing} bitwiseExtra=${item.validation.summary.bitwiseExtra} visualMissing=${item.validation.summary.visualMissing} visualExtra=${item.validation.summary.visualExtra} videoVisualMissing=${item.validation.summary.videoVisualMissing} videoVisualExtra=${item.validation.summary.videoVisualExtra}`
      )
    }

    if (item.errorArtifactsCleared.length > 0) {
      lines.push(`  cleared error logs=${item.errorArtifactsCleared.length}`)
    }
  }

  if (report.nasSync) {
    lines.push('')
    lines.push(
      `NAS sync attempted=${report.nasSync.attempted.length} ok=${report.nasSync.synced.length} failed=${report.nasSync.failed.length} pending=${report.nasSync.pendingAfterRun.length}`
    )
  }

  fs.writeFileSync(latestTextPath, lines.join('\n'))
  writeErrorsToCheckSummary(report)
}

function calculateTotals(results) {
  return (Array.isArray(results) ? results : []).reduce(
    (totals, item) => {
      totals.modelsProcessed += 1
      if (item.needsAttention) {
        totals.modelsNeedingAttention += 1
      } else {
        totals.modelsClean += 1
      }

      if (shouldScrape) {
        const summary = item?.lastRunSummary
        if (summary && !summary.parseError) {
          totals.filesSaved += Number(summary.successCount || 0)
          totals.sourceItemsHandled += Number(summary.combinedTotal || 0)
          totals.duplicates += Number(summary.duplicateCount || 0)
          totals.runErrors += Number(summary.errorCount || 0)
        }
      }

      return totals
    },
    buildEmptyTotals()
  )
}

function loadState() {
  if (!fs.existsSync(statePath)) {
    return {
      pendingNasSyncModels: [],
      lastSyncedAt: null,
      lastSyncResults: [],
    }
  }

  try {
    const raw = fs.readFileSync(statePath, 'utf8').trim()
    const parsed = raw ? JSON.parse(raw) : {}
    return {
      pendingNasSyncModels: Array.isArray(parsed?.pendingNasSyncModels)
        ? parsed.pendingNasSyncModels
        : [],
      lastSyncedAt: parsed?.lastSyncedAt || null,
      lastSyncResults: Array.isArray(parsed?.lastSyncResults)
        ? parsed.lastSyncResults
        : [],
    }
  } catch {
    return {
      pendingNasSyncModels: [],
      lastSyncedAt: null,
      lastSyncResults: [],
    }
  }
}

function saveState(state) {
  fs.writeFileSync(
    statePath,
    JSON.stringify(
      {
        pendingNasSyncModels: Array.isArray(state?.pendingNasSyncModels)
          ? [...state.pendingNasSyncModels].sort((a, b) => a.localeCompare(b))
          : [],
        lastSyncedAt: state?.lastSyncedAt || null,
        lastSyncResults: Array.isArray(state?.lastSyncResults)
          ? state.lastSyncResults
          : [],
      },
      null,
      2
    )
  )
}

function queuePendingNasSyncModel(state, modelName) {
  if (!modelName) return
  const pending = new Set(
    Array.isArray(state?.pendingNasSyncModels) ? state.pendingNasSyncModels : []
  )
  pending.add(modelName)
  state.pendingNasSyncModels = [...pending]
  saveState(state)
}

async function syncPendingModelsToNas(state) {
  const pendingModels = Array.isArray(state?.pendingNasSyncModels)
    ? [...state.pendingNasSyncModels].sort((a, b) => a.localeCompare(b))
    : []

  if (!pendingModels.length) {
    return {
      attempted: [],
      synced: [],
      failed: [],
      pendingAfterRun: [],
    }
  }

  const synced = []
  const failed = []
  const pendingAfterRun = new Set(pendingModels)

  for (const modelName of pendingModels) {
    const sourcePath = path.join(datasetRoot, modelName)
    const targetPath = path.join(nasDatasetRoot, modelName)

    if (!fs.existsSync(sourcePath)) {
      pendingAfterRun.delete(modelName)
      continue
    }

    try {
      const command = await runRobocopyModelSync(sourcePath, targetPath)
      synced.push({ model: modelName, sourcePath, targetPath, command })
      pendingAfterRun.delete(modelName)
    } catch (err) {
      failed.push({
        model: modelName,
        sourcePath,
        targetPath,
        error: err.message,
      })
    }
  }

  state.pendingNasSyncModels = [...pendingAfterRun]
  state.lastSyncedAt = new Date().toISOString()
  state.lastSyncResults = [...synced, ...failed]
  saveState(state)

  return {
    attempted: pendingModels,
    synced,
    failed,
    pendingAfterRun: state.pendingNasSyncModels,
  }
}

function runRobocopyModelSync(sourcePath, targetPath) {
  return new Promise((resolve, reject) => {
    try {
      ensureDir(targetPath)
    } catch (err) {
      return reject(
        new Error(`Could not prepare NAS target ${targetPath}: ${err.message}`)
      )
    }

    const args = [
      sourcePath,
      targetPath,
      '/E',
      '/XC',
      '/XN',
      '/XO',
      '/R:2',
      '/W:5',
      '/NFL',
      '/NDL',
      '/NJH',
      '/NJS',
      '/NP',
    ]
    const command = ['robocopy', ...args].join(' ')
    const child = spawn('robocopy', args, {
      cwd: rootDir,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    })

    let stdout = ''
    let stderr = ''

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString()
    })
    child.on('error', reject)
    child.on('exit', (code) => {
      if ((code ?? 0) > 3) {
        return reject(
          new Error(
            `robocopy failed (${code}): ${command}\n${stderr || stdout}`.trim()
          )
        )
      }
      resolve(command)
    })
  })
}

function writeErrorsToCheckSummary(report) {
  const items = []

  for (const item of Array.isArray(report?.results) ? report.results : []) {
    if (!item.needsAttention) continue

    const detailParts = []
    if (item.scrape.status === 'failed') {
      detailParts.push(`scrape failed across ${item.scrape.runs || 0} source run(s)`)
    }
    if (item.validation?.summary) {
      const summaryBits = item.validation.summary
      if (
        !item.validation.clean &&
        !summaryBits.parseError &&
        Object.values(summaryBits).some((value) => Number(value || 0) > 0)
      ) {
        detailParts.push(
          `validation missing bitwise=${summaryBits.bitwiseMissing || 0} visual=${summaryBits.visualMissing || 0} video=${summaryBits.videoVisualMissing || 0} extra bitwise=${summaryBits.bitwiseExtra || 0} visual=${summaryBits.visualExtra || 0} video=${summaryBits.videoVisualExtra || 0}`
        )
      }
    }

    if (shouldScrape) {
      const runErrorCount = getRunErrorCount(item.lastRunSummary)
      if (runErrorCount > 0) {
        detailParts.push(`${runErrorCount} scrape run error(s)`)
      }
    }

    if (!item.validation?.clean && item.localRunErrors.length > 0) {
      detailParts.push(
        `${item.localRunErrors.filter((entry) => entry.kind === 'latest_json').length} local error log file(s) still present`
      )
    }

    items.push({
      model: item.model,
      status: item.scrape.status === 'failed' ? 'needs_repair' : 'needs_review',
      count: shouldScrape ? getRunErrorCount(item.lastRunSummary) : undefined,
      details: detailParts.join('; ') || 'Model still needs local review.',
    })
  }

  for (const failed of report?.nasSync?.failed || []) {
    items.push({
      model: failed.model,
      status: 'nas_sync_failed',
      details: failed.error,
    })
  }

  for (const pendingModel of report?.nasSync?.pendingAfterRun || []) {
    if (report?.nasSync?.failed?.some((entry) => entry.model === pendingModel)) {
      continue
    }
    items.push({
      model: pendingModel,
      status: 'nas_sync_pending',
      details: 'Pending retry on next repair run.',
    })
  }

  upsertErrorsSource('repair', {
    title: 'Repair',
    summary:
      items.length > 0
        ? `${items.length} model-level repair follow-up item(s) still need attention.`
        : 'Latest repair run is clean.',
    commandHint: shouldScrape ? 'npm run repair -- --scrape' : 'npm run repair',
    items,
  })
}

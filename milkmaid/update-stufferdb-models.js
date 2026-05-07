const fs = require('fs')
const path = require('path')
const { spawn } = require('child_process')
const minimist = require('minimist')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  string: ['model', 'models', 'start-from', 'registry', 'log-dir'],
  boolean: ['stop-on-error', 'with-repair'],
  default: {
    limit: 0,
    'stop-on-error': false,
    'with-repair': false,
  },
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const rootDir = path.join(__dirname, '..')
const registryPath = path.resolve(
  String(argv.registry || path.join(rootDir, 'model_aliases.json'))
)
const logDir = path.resolve(
  String(argv['log-dir'] || path.join(rootDir, 'tmp', 'update-stufferdb'))
)
const latestReportPath = path.join(logDir, 'update-stufferdb-latest.json')
const latestTextPath = path.join(logDir, 'update-stufferdb-latest.txt')
const limit = Math.max(parseInt(argv.limit, 10) || 0, 0)
const singleModel = argv.model ? String(argv.model).trim() : null
const explicitModels = String(argv.models || '')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean)
const positionalSelector =
  !singleModel && !explicitModels.length && argv._.length
    ? String(argv._[0] || '').trim()
    : null
const startFrom = argv['start-from']
  ? String(argv['start-from']).trim()
  : positionalSelector
const withRepair = Boolean(argv['with-repair'])

main().catch((err) => {
  console.error(`Fatal updater error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  ensureDir(logDir)
  const registry = JSON.parse(fs.readFileSync(registryPath, 'utf8'))
  const queue = buildQueue(registry)
  const selectedQueue = applySelection(queue)
  const report = {
    generatedAt: new Date().toISOString(),
    registryPath,
    totalModelsInRegistry: queue.length,
    selectedModels: selectedQueue.length,
    stopOnError: Boolean(argv['stop-on-error']),
    withRepair,
    totals: {
      filesSaved: 0,
      sourceItemsHandled: 0,
      duplicates: 0,
      errors: 0,
    },
    results: [],
  }

  console.log(
    `Update queue: ${selectedQueue.length} model(s) selected from ${queue.length} with StufferDB sources`
  )

  for (let index = 0; index < selectedQueue.length; index += 1) {
    const item = selectedQueue[index]
    console.log('')
    console.log(
      `[${index + 1}/${selectedQueue.length}] Updating ${item.model} from ${item.sources.length} source(s)`
    )

    const result = await runModelUpdate(item)
    report.results.push(result)
    report.totals = calculateTotals(report.results)
    writeReport(report)

    if (
      argv['stop-on-error'] &&
      (!result.scrape.ok ||
        (withRepair &&
          (!result.hashPrune.ok ||
            !result.hashBackfill.ok ||
            !result.validation.ok)))
    ) {
      console.log('Stopping on first error because --stop-on-error was set.')
      break
    }
  }

  writeReport(report)
  console.log('')
  console.log(`Latest report: ${latestReportPath}`)
}

function printHelp() {
  console.log(`Usage: node milkmaid/update-stufferdb-models.js [options]

Options:
  [start-from]          Optional positional shorthand for --start-from.
  --model <name>         Update one model only.
  --models <a,b,c>       Update a comma-separated set of models.
  --start-from <name>    Start from this canonical model name.
  --limit <n>            Only process the first n selected models.
  --registry <path>      Override model_aliases.json path.
  --log-dir <path>       Override updater report directory.
  --stop-on-error        Stop the batch when one model fails.
  --with-repair          After each scrape, also prune/backfill/validate hashes.
  -h, --help             Show help.

Notes:
  Each model is scraped once per configured StufferDB source URL.
  By default this updater is scrape-only so reruns stay fast.
  Use --with-repair when you explicitly want per-model prune/backfill/validate work.
`)
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function buildQueue(registry) {
  return Object.entries(registry || {})
    .map(([model, entry]) => ({
      model,
      sources: Array.isArray(entry?.sources?.stufferdb)
        ? entry.sources.stufferdb
            .map((source) => String(source?.url || '').trim())
            .filter(Boolean)
        : [],
    }))
    .filter((item) => item.sources.length > 0)
    .sort((left, right) => left.model.localeCompare(right.model))
}

function applySelection(queue) {
  let next = queue

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

async function runModelUpdate(item) {
  const result = {
    model: item.model,
    startedAt: new Date().toISOString(),
    sourceUrls: item.sources,
    scrapeRuns: [],
    scrape: null,
    hashPrune: null,
    hashBackfill: null,
    validation: null,
    finishedAt: null,
    lastRunSummaryPath: null,
    sourceSummaries: [],
  }

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
      {
        cwd: rootDir,
        label: `milkmaid:${item.model}`,
      }
    )

    result.scrapeRuns.push(scrapeRun)

    const runSummary = readLastRunSummary(item.model)
    if (runSummary) {
      result.sourceSummaries.push({
        sourceUrl,
        status: runSummary.status || null,
        saved: Number(runSummary.successCount || 0),
        duplicates: Number(runSummary.duplicateCount || 0),
        errors: Number(runSummary.errorCount || 0),
        finishedAt: runSummary.finishedAt || null,
        categoryRunList: Array.isArray(runSummary.categoryRunList)
          ? runSummary.categoryRunList
          : [],
      })
      result.lastRunSummaryPath = getLastRunSummaryPath(item.model)
    }

    if (!scrapeRun.ok && argv['stop-on-error']) {
      break
    }
  }

  result.scrape = summarizeScrapeRuns(result.scrapeRuns)

  if (!withRepair) {
    result.hashPrune = skippedCommandResult(
      'pruneModelHashes skipped by default; rerun with --with-repair to enable'
    )
    result.hashBackfill = skippedCommandResult(
      'backfillModelHashes skipped by default; rerun with --with-repair to enable'
    )
    result.validation = skippedCommandResult(
      'validateModelHashes skipped by default; rerun with --with-repair to enable'
    )
  } else if (result.scrape.ok) {
    result.hashPrune = await runCommand(
      process.execPath,
      [
        path.join(rootDir, 'scrapyard', 'pruneModelHashes.js'),
        '--model',
        item.model,
      ],
      { cwd: rootDir, label: `prune:${item.model}` }
    )
  } else {
    result.hashPrune = skippedCommandResult(
      'pruneModelHashes skipped because one or more scrapes failed'
    )
  }

  if (!withRepair) {
    result.hashBackfill = skippedCommandResult(
      'backfillModelHashes skipped by default; rerun with --with-repair to enable'
    )
  } else if (result.scrape.ok) {
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
  } else {
    result.hashBackfill = skippedCommandResult(
      'backfillModelHashes skipped because one or more scrapes failed'
    )
  }

  if (withRepair) {
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
  }

  result.finishedAt = new Date().toISOString()
  return result
}

function getLastRunSummaryPath(modelName) {
  return path.join(
    process.env.APPDATA || '',
    '.slopvault',
    'dataset',
    modelName,
    'milkmaid-last-run.json'
  )
}

function readLastRunSummary(modelName) {
  const summaryPath = getLastRunSummaryPath(modelName)
  if (!fs.existsSync(summaryPath)) {
    return null
  }

  try {
    return JSON.parse(fs.readFileSync(summaryPath, 'utf8'))
  } catch (err) {
    return {
      parseError: err.message,
    }
  }
}

function summarizeScrapeRuns(scrapeRuns) {
  const failures = scrapeRuns.filter((run) => !run.ok)
  return {
    ok: failures.length === 0,
    runs: scrapeRuns.length,
    failures: failures.length,
    labels: scrapeRuns.map((run) => run.label),
    commands: scrapeRuns.map((run) => run.command),
  }
}

function skippedCommandResult(command) {
  return {
    ok: true,
    skipped: true,
    code: null,
    command,
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

function runCommand(command, args, { cwd, label, env } = {}) {
  return new Promise((resolve) => {
    const child = spawn(command, args, {
      cwd,
      stdio: 'inherit',
      env: { ...process.env, ...(env || {}) },
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
    `Registry: ${report.registryPath}`,
    `Selected models: ${report.selectedModels}`,
    `Totals: saved=${report.totals.filesSaved} sourceItems=${report.totals.sourceItemsHandled} dupes=${report.totals.duplicates} errors=${report.totals.errors}`,
    '',
  ]

  for (const item of report.results) {
    lines.push(
      [
        item.model,
        `scrape=${item.scrape.ok ? 'ok' : 'fail'}`,
        `sources=${item.scrape.runs || 0}`,
        `prune=${item.hashPrune?.skipped ? 'skipped' : item.hashPrune?.ok ? 'ok' : 'fail'}`,
        `backfill=${item.hashBackfill?.skipped ? 'skipped' : item.hashBackfill?.ok ? 'ok' : 'fail'}`,
        `validate=${
          item.validation?.skipped
            ? 'skipped'
            : item.validation?.ok
              ? 'clean'
              : 'needs_attention'
        }`,
      ].join(' :: ')
    )

    const aggregate = summarizeSourceSummaries(item.sourceSummaries)
    lines.push(
      `  saved=${aggregate.saved} dupes=${aggregate.duplicates} errors=${aggregate.errors}`
    )

    if (item.validation?.summary && !item.validation.summary.parseError) {
      lines.push(
        `  validation bitwiseMissing=${item.validation.summary.bitwiseMissing} visualMissing=${item.validation.summary.visualMissing} videoVisualMissing=${item.validation.summary.videoVisualMissing}`
      )
    }
  }

  fs.writeFileSync(latestTextPath, lines.join('\n'))
}

function summarizeSourceSummaries(sourceSummaries) {
  return sourceSummaries.reduce(
    (totals, summary) => {
      totals.saved += Number(summary.saved || 0)
      totals.duplicates += Number(summary.duplicates || 0)
      totals.errors += Number(summary.errors || 0)
      return totals
    },
    { saved: 0, duplicates: 0, errors: 0 }
  )
}

function calculateTotals(results) {
  return results.reduce(
    (totals, result) => {
      const perSourceTotals = summarizeSourceSummaries(result.sourceSummaries)
      totals.filesSaved += perSourceTotals.saved
      totals.sourceItemsHandled += perSourceTotals.saved + perSourceTotals.duplicates
      totals.duplicates += perSourceTotals.duplicates
      totals.errors += perSourceTotals.errors
      return totals
    },
    {
      filesSaved: 0,
      sourceItemsHandled: 0,
      duplicates: 0,
      errors: 0,
    }
  )
}

const fs = require('fs')
const path = require('path')
const minimist = require('minimist')
const { spawn } = require('child_process')
const { upsertErrorsSource } = require('../scrapyard/errorsToCheck')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  string: ['model', 'models', 'start-from', 'registry', 'log-dir'],
  boolean: ['stop-on-error'],
  default: {
    limit: 0,
    'stop-on-error': false,
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
  String(argv['log-dir'] || path.join(rootDir, 'tmp', 'repair-stufferdb'))
)
const latestReportPath = path.join(logDir, 'repair-stufferdb-latest.json')
const latestTextPath = path.join(logDir, 'repair-stufferdb-latest.txt')
const runStamp = new Date().toISOString().replace(/[:.]/g, '-')
const limit = Math.max(parseInt(argv.limit, 10) || 0, 0)
const singleModel = argv.model ? String(argv.model).trim() : null
const explicitModels = String(argv.models || '')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean)
const startFrom = argv['start-from'] ? String(argv['start-from']).trim() : null

main().catch((err) => {
  console.error(`Fatal repair-runner error: ${err.stack || err.message}`)
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
    totals: {
      filesSaved: 0,
      sourceItemsHandled: 0,
      duplicates: 0,
      errors: 0,
    },
    results: [],
  }

  console.log(
    `Repair queue: ${selectedQueue.length} model(s) selected from ${queue.length} with StufferDB sources`
  )

  for (let index = 0; index < selectedQueue.length; index += 1) {
    const item = selectedQueue[index]
    console.log('')
    console.log(
      `[${index + 1}/${selectedQueue.length}] Repairing ${item.model}`
    )

    const result = await runModelRepair(item)
    report.results.push(result)
    report.totals = calculateTotals(report.results)
    writeReport(report)

    if (
      argv['stop-on-error'] &&
      (!result.scrape.ok ||
        !result.hashPrune.ok ||
        !result.hashBackfill.ok ||
        !result.validation.ok)
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
  console.log(`Usage: node milkmaid/repair-stufferdb-models.js [options]

Options:
  --model <name>         Repair one model only.
  --models <a,b,c>       Repair a comma-separated set of models.
  --start-from <name>    Start from this canonical model name.
  --limit <n>            Only process the first n selected models.
  --registry <path>      Override model_aliases.json path.
  --log-dir <path>       Override repair runner report directory.
  --stop-on-error        Stop the batch when one model fails.
  -h, --help             Show help.
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

async function runModelRepair(item) {
  const startedAt = new Date().toISOString()
  const result = {
    model: item.model,
    startedAt,
    sourceUrls: item.sources,
    scrapeRuns: [],
    scrape: null,
    hashPrune: null,
    hashBackfill: null,
    validation: null,
    finishedAt: null,
    lastRunSummaryPath: null,
    lastRunSummary: null,
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
      { cwd: rootDir, label: `milkmaid:${item.model}` }
    )
    result.scrapeRuns.push(scrapeRun)
    if (!scrapeRun.ok && argv['stop-on-error']) {
      break
    }
  }

  result.scrape = summarizeScrapeRuns(result.scrapeRuns)

  if (result.scrape.ok) {
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
    result.hashPrune = {
      ok: false,
      skipped: true,
      code: null,
      command: 'pruneModelHashes skipped because scrape failed',
    }
  }

  if (result.scrape.ok) {
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
    result.hashBackfill = {
      ok: false,
      skipped: true,
      code: null,
      command: 'backfillModelHashes skipped because scrape failed',
    }
  }

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

  const latestRunSummaryPath = path.join(
    process.env.APPDATA || '',
    '.slopvault',
    'dataset',
    item.model,
    'milkmaid-last-run.json'
  )
  result.lastRunSummaryPath = latestRunSummaryPath
  if (fs.existsSync(latestRunSummaryPath)) {
    try {
      result.lastRunSummary = JSON.parse(
        fs.readFileSync(latestRunSummaryPath, 'utf8')
      )
    } catch (err) {
      result.lastRunSummary = {
        parseError: err.message,
      }
    }
  }

  result.finishedAt = new Date().toISOString()
  return result
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

function runCommand(command, args, { cwd, label }) {
  return new Promise((resolve) => {
    const child = spawn(command, args, {
      cwd,
      stdio: 'inherit',
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
        `prune=${item.hashPrune?.ok ? 'ok' : item.hashPrune?.skipped ? 'skipped' : 'fail'}`,
        `backfill=${item.hashBackfill?.ok ? 'ok' : item.hashBackfill?.skipped ? 'skipped' : 'fail'}`,
        `validate=${item.validation?.ok ? 'clean' : 'needs_attention'}`,
      ].join(' :: ')
    )

    const summary = item.lastRunSummary
    if (summary && !summary.parseError) {
      lines.push(
        `  saved=${summary.successCount || 0} dupes=${summary.duplicateCount || 0} errors=${summary.errorCount || 0}`
      )
    }

    if (item.validation?.summary && !item.validation.summary.parseError) {
      lines.push(
        `  validation bitwiseMissing=${item.validation.summary.bitwiseMissing} visualMissing=${item.validation.summary.visualMissing} videoVisualMissing=${item.validation.summary.videoVisualMissing}`
      )
    }
  }

  fs.writeFileSync(latestTextPath, lines.join('\n'))
  writeErrorsToCheckSummary(report)
}

function calculateTotals(results) {
  return (Array.isArray(results) ? results : []).reduce(
    (totals, item) => {
      const summary = item?.lastRunSummary
      if (!summary || summary.parseError) return totals

      totals.filesSaved += Number(summary.successCount || 0)
      totals.sourceItemsHandled += Number(summary.combinedTotal || 0)
      totals.duplicates += Number(summary.duplicateCount || 0)
      totals.errors += Number(summary.errorCount || 0)
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

function writeErrorsToCheckSummary(report) {
  const items = []

  for (const item of Array.isArray(report?.results) ? report.results : []) {
    const hasScrapeFailure = !item?.scrape?.ok
    const hasValidationFailure = !item?.validation?.ok
    const summary = item?.lastRunSummary
    const errorCount =
      summary && !summary.parseError ? Number(summary.errorCount || 0) : 0

    if (!hasScrapeFailure && !hasValidationFailure && errorCount <= 0) continue

    const detailParts = []
    if (hasScrapeFailure) {
      detailParts.push(`scrape failed across ${item.scrape?.runs || 0} source run(s)`)
    }
    if (errorCount > 0) {
      detailParts.push(`${errorCount} run error(s)`)
    }
    if (hasValidationFailure && item?.validation?.summary) {
      const summaryBits = item.validation.summary
      detailParts.push(
        `validation missing bitwise=${summaryBits.bitwiseMissing || 0} visual=${summaryBits.visualMissing || 0} video=${summaryBits.videoVisualMissing || 0}`
      )
    }

    items.push({
      model: item.model,
      status: hasScrapeFailure ? 'needs_repair' : 'needs_review',
      count: errorCount,
      details: detailParts.join('; '),
    })
  }

  upsertErrorsSource('repair-stufferdb', {
    title: 'Repair StufferDB',
    summary:
      items.length > 0
        ? `${items.length} model(s) still need attention from the latest repair:stufferdb batch.`
        : 'Latest repair:stufferdb batch is clean.',
    commandHint: 'npm run repair:stufferdb',
    items,
  })
}

'use strict'

const path = require('path')
const fs = require('fs')
const minimist = require('minimist')

const {
  loadModelRegistry,
  findCanonicalModelName,
  sanitize,
} = require('./modelRegistry')
const {
  parseSourceUrl,
  getScraperScript,
  describeSource,
} = require('./sourceRouter')

const rootDir = path.join(__dirname, '..')
const registryPath = path.join(rootDir, 'model_aliases.json')

const STRING_OPTIONS = [
  'model',
  'pages',
  'max-posts',
  'max-files',
  'post-concurrency',
  'image-concurrency',
  'video-concurrency',
  'cookie',
  'cookie-file',
  'browser-executable',
  'browser-profile',
  'browser-connect',
  'browser-validate-ms',
  'only-models',
  'models',
  'start-from',
  'limit',
  'delay-ms',
  'source',
  'host-contains',
  'registry',
  'log-dir',
]

const BOOLEAN_OPTIONS = [
  'dry-run',
  'preflight',
  'skip-nas-sync',
  'track-source',
  'keep-history',
  'browser-media',
  'browser-headless',
  'headless',
  'review-errors',
  'no-model-infer',
  'stop-on-error',
  'with-repair',
  'help',
]

function printHelp() {
  console.log(`Usage: npm run scrape -- <source-url> [options]

Runs the unified scraper launcher for one StufferDB, Reddit, Coomer, CoomerFans, or Kemono URL.
With no URL, opens the interactive scrape launcher.

Options:
  --model <canonical>              Force the destination model bucket.
  --skip-nas-sync                  Skip the post-run NAS sync.
  --keep-history                   Preserve prior last-run metadata where supported.
  --review-errors                  Pause Milkmaid for SlopVault review before NAS sync.
  --pages <n|a-b>                  Hoghaul page limit.
  --max-posts <n>                  Hoghaul post limit.
  --max-files <n>                  Hoghaul media limit.
  --post-concurrency <n>           Hoghaul post fetch concurrency.
  --image-concurrency <n>          Hoghaul image/gif concurrency.
  --video-concurrency <n>          Hoghaul video concurrency.
  --dry-run                        Hoghaul dry run.
  --preflight                      Hoghaul API preflight.
  --track-source                   Keep source tracking history where supported.
  --no-browser-media               Disable Hoghaul browser media fallback.
  --cookie <header>                Hoghaul browser cookie header.
  --cookie-file <path>             Hoghaul browser cookie file.
  --browser-profile <path>         Hoghaul browser profile path.
  --browser-connect <url>          Hoghaul browser debug endpoint.
  --browser-validate-ms <ms>       Hoghaul browser validation timeout.
  --help                           Show this help.
`)
}

function parseRunnerArgs(argvInput = process.argv.slice(2)) {
  if (Array.isArray(argvInput)) {
    return minimist(argvInput, {
      string: STRING_OPTIONS,
      boolean: BOOLEAN_OPTIONS,
      alias: {
        m: 'model',
        h: 'help',
      },
    })
  }
  return {
    _: [],
    ...(argvInput || {}),
  }
}

function appendOption(args, flag, value) {
  if (value === undefined || value === null || value === '') return
  args.push(flag, String(value))
}

function appendBoolean(args, flag, value) {
  if (value === true) args.push(flag)
}

function isTruthy(value) {
  return (
    value === true ||
    value === 'true' ||
    value === '1' ||
    value === 1 ||
    value === 'yes'
  )
}

function getOption(argv, name) {
  if (argv?.[name] !== undefined) return argv[name]
  const envName = `npm_config_${String(name).replace(/-/g, '_')}`
  return process.env[envName]
}

function normalizeList(value) {
  if (!value) return null
  return new Set(
    String(value)
      .split(',')
      .map((part) => part.trim())
      .filter(Boolean)
  )
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function appendOptionalBoolean(args, optionName, value) {
  if (value === true) {
    args.push(`--${optionName}`)
  } else if (value === false) {
    args.push(`--no-${optionName}`)
  }
}

function runNodeScript(scriptPath, args, { log = console.log } = {}) {
  const { spawnSync } = require('child_process')
  log('')
  log(`Running: node ${scriptPath} ${args.join(' ')}`.trim())
  log('')
  const result = spawnSync(process.execPath, [scriptPath, ...args], {
    cwd: rootDir,
    stdio: 'inherit',
  })
  return result.status ?? 1
}

function runScriptResult(scriptPath, args, label) {
  const code = runNodeScript(scriptPath, args)
  return {
    ok: code === 0,
    code,
    label,
    command: `node ${scriptPath} ${args.join(' ')}`.trim(),
  }
}

async function runInteractiveLauncher() {
  const { main } = require('./run-scrape-interactive')
  await main()
  return 0
}

async function runInProcessScraper(parsedSource, options) {
  if (parsedSource.scraper === 'milkmaid') {
    const { runMilkmaidScrape } = require('../milkmaid/milkmaid')
    return runMilkmaidScrape(options)
  }

  if (parsedSource.scraper === 'hoghaul') {
    const { runHoghaulScrape } = require('../hoghaul/hoghaul')
    return runHoghaulScrape(options)
  }

  throw new Error(`No in-process scraper is registered for ${parsedSource}`)
}

function inferCanonicalModel(parsedSource, explicitModel) {
  if (explicitModel) return explicitModel
  if (!parsedSource?.rawName) return ''

  const registry = loadModelRegistry(registryPath)
  return findCanonicalModelName(registry, sanitize(parsedSource.rawName)) || ''
}

function appendSharedOptions(args, parsedSource, argv) {
  const modelName = argv['no-model-infer']
    ? ''
    : inferCanonicalModel(parsedSource, argv.model)
  appendOption(args, '--model', modelName)
  appendBoolean(args, '--skip-nas-sync', argv['skip-nas-sync'])
  appendBoolean(args, '--keep-history', argv['keep-history'])
}

function appendHoghaulOptions(args, argv) {
  appendOption(args, '--pages', argv.pages)
  appendOption(args, '--max-posts', argv['max-posts'])
  appendOption(args, '--max-files', argv['max-files'])
  appendOption(args, '--post-concurrency', argv['post-concurrency'])
  appendOption(args, '--image-concurrency', argv['image-concurrency'])
  appendOption(args, '--video-concurrency', argv['video-concurrency'])
  appendOption(args, '--cookie', argv.cookie)
  appendOption(args, '--cookie-file', argv['cookie-file'])
  appendOption(args, '--browser-executable', argv['browser-executable'])
  appendOption(args, '--browser-profile', argv['browser-profile'])
  appendOption(args, '--browser-connect', argv['browser-connect'])
  appendOption(args, '--browser-validate-ms', argv['browser-validate-ms'])
  appendBoolean(args, '--dry-run', argv['dry-run'])
  appendBoolean(args, '--preflight', argv.preflight)
  appendBoolean(args, '--track-source', argv['track-source'])
  appendBoolean(args, '--browser-headless', argv['browser-headless'])
  appendBoolean(args, '--headless', argv.headless)
  appendOptionalBoolean(args, 'browser-media', argv['browser-media'])
}

function appendMilkmaidOptions(args, argv) {
  appendBoolean(args, '--review-errors', argv['review-errors'])
}

function getRunnerModelName(parsedSource, argv) {
  return argv['no-model-infer']
    ? ''
    : inferCanonicalModel(parsedSource, argv.model)
}

function buildScraperArgs(parsedSource, argvInput = {}) {
  const argv = parseRunnerArgs(argvInput)
  const args = [parsedSource.url]
  appendSharedOptions(args, parsedSource, argv)

  if (parsedSource.scraper === 'milkmaid') {
    appendMilkmaidOptions(args, argv)
  } else if (parsedSource.scraper === 'hoghaul') {
    appendHoghaulOptions(args, argv)
  }

  return args
}

function buildScraperOptions(parsedSource, argvInput = {}) {
  const argv = parseRunnerArgs(argvInput)
  const modelName = getRunnerModelName(parsedSource, argv)
  const sharedOptions = {
    inputUrl: parsedSource.url,
    model: modelName,
    modelOverride: modelName,
    skipNasSync: Boolean(argv['skip-nas-sync']),
    keepHistory: Boolean(argv['keep-history']),
  }

  if (parsedSource.scraper === 'milkmaid') {
    return {
      ...sharedOptions,
      reviewErrors: Boolean(argv['review-errors']),
    }
  }

  if (parsedSource.scraper === 'hoghaul') {
    return {
      ...sharedOptions,
      pages: argv.pages,
      maxPosts: argv['max-posts'],
      maxFiles: argv['max-files'],
      postConcurrency: argv['post-concurrency'],
      imageConcurrency: argv['image-concurrency'],
      videoConcurrency: argv['video-concurrency'],
      cookie: argv.cookie,
      cookieFile: argv['cookie-file'],
      browserExecutable: argv['browser-executable'],
      browserProfile: argv['browser-profile'],
      browserConnect: argv['browser-connect'],
      browserValidateMs: argv['browser-validate-ms'],
      dryRun: Boolean(argv['dry-run']),
      preflight: Boolean(argv.preflight),
      trackSource: Boolean(argv['track-source']),
      browserMedia: argv['browser-media'],
      browserHeadless: Boolean(argv['browser-headless'] || argv.headless),
      headless: Boolean(argv.headless),
    }
  }

  return sharedOptions
}

async function runScrape(inputUrl, argvInput = {}, deps = {}) {
  const log = deps.log || console.log
  const error = deps.error || console.error
  const argv = parseRunnerArgs(argvInput)
  const parsedSource = parseSourceUrl(inputUrl)
  if (!parsedSource) {
    error(
      'Could not recognize that URL as StufferDB, Reddit, Coomer, CoomerFans, or Kemono.'
    )
    return 1
  }

  const scriptPath = getScraperScript(parsedSource)
  if (!scriptPath) {
    error(`No scraper is registered for ${describeSource(parsedSource)}.`)
    return 1
  }

  log(`Detected source: ${describeSource(parsedSource)}`)
  if (parsedSource.rawName) {
    log(`Detected name: ${parsedSource.rawName}`)
  }

  const runCommand = deps.runCommand || runNodeScript
  const args = buildScraperArgs(parsedSource, argv)
  if (deps.runCommand) {
    return runCommand(scriptPath, args, { log })
  }
  return runInProcessScraper(
    parsedSource,
    buildScraperOptions(parsedSource, argv)
  )
}

function loadRegistry(registryFile = registryPath) {
  return JSON.parse(fs.readFileSync(registryFile, 'utf8'))
}

function collectSourceTargets(registry, sourceKey, modelFilter, hostContains) {
  const targets = []

  for (const [modelName, entry] of Object.entries(registry || {})) {
    if (modelFilter && !modelFilter.has(modelName)) continue
    const sources = Array.isArray(entry?.sources?.[sourceKey])
      ? entry.sources[sourceKey]
      : []

    for (const source of sources) {
      const url = String(source?.url || '').trim()
      if (!url) continue
      if (hostContains && !url.toLowerCase().includes(hostContains)) continue
      targets.push({
        modelName,
        url,
      })
    }
  }

  return targets
}

function buildSourceBatchOptions(argv) {
  const options = {
    'post-concurrency': getOption(argv, 'post-concurrency') || '8',
    'image-concurrency': getOption(argv, 'image-concurrency') || '6',
    'video-concurrency': getOption(argv, 'video-concurrency') || '6',
  }

  for (const name of ['pages', 'max-posts', 'max-files']) {
    const value = getOption(argv, name)
    if (value !== undefined && value !== null && value !== '') {
      options[name] = value
    }
  }

  const keepHistory = !(
    getOption(argv, 'keep-history') === false ||
    getOption(argv, 'keep-history') === 'false'
  )
  if (keepHistory) options['keep-history'] = true
  if (isTruthy(getOption(argv, 'skip-nas-sync')))
    options['skip-nas-sync'] = true
  if (isTruthy(getOption(argv, 'dry-run'))) options['dry-run'] = true
  return options
}

function printSourceBatchHelp() {
  console.log(`Usage: node hoghaul/run-source-batch.js --source=<coomer|kemono> [options]

Options:
  --source <name>             Registry source key to run (required).
  --only-models <a,b,c>       Limit to canonical model names.
  --host-contains <text>      Optional URL host filter, e.g. coomerfans.com.
  --pages <n|a-b>             Limit pages.
  --max-posts <n>             Limit posts per source.
  --max-files <n>             Limit media files per source.
  --post-concurrency <n>      Post fetch concurrency.
  --image-concurrency <n>     Image/gif concurrency.
  --video-concurrency <n>     Video concurrency.
  --delay-ms <n>              Delay between models.
  --dry-run                   Dry run.
  --skip-nas-sync             Skip NAS sync.
  --keep-history              Preserve last-run logs.
  --help                      Show this help.
`)
}

async function runSourceBatch(sourceKeyOrArgv, argvInput = {}) {
  const sourceKeyProvided = typeof sourceKeyOrArgv === 'string'
  const argv = parseRunnerArgs(sourceKeyProvided ? argvInput : sourceKeyOrArgv)

  if (isTruthy(getOption(argv, 'help'))) {
    printSourceBatchHelp()
    return 0
  }

  const sourceKey = String(
    sourceKeyProvided ? sourceKeyOrArgv : getOption(argv, 'source') || ''
  )
    .trim()
    .toLowerCase()
  if (!sourceKey) {
    printSourceBatchHelp()
    return 1
  }

  const registry = loadRegistry()
  const modelFilter = normalizeList(getOption(argv, 'only-models'))
  const hostContains = String(getOption(argv, 'host-contains') || '')
    .trim()
    .toLowerCase()
  const targets = collectSourceTargets(
    registry,
    sourceKey,
    modelFilter,
    hostContains
  )

  if (targets.length === 0) {
    console.log(`No ${sourceKey}-backed model sources found.`)
    return 0
  }

  console.log(`Found ${targets.length} ${sourceKey}-backed model source(s).`)

  const delayMs = Number.parseInt(getOption(argv, 'delay-ms'), 10) || 0
  const sharedOptions = buildSourceBatchOptions(argv)

  for (let index = 0; index < targets.length; index += 1) {
    const target = targets[index]
    console.log(
      `\n[${index + 1}/${targets.length}] ${target.modelName} -> ${target.url}`
    )

    const status = await runScrape(target.url, {
      ...sharedOptions,
      model: target.modelName,
    })
    if (status !== 0) return status || 1

    if (delayMs > 0 && index < targets.length - 1) {
      await sleep(delayMs)
    }
  }

  return 0
}

function buildStufferQueue(registry) {
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

function resolveStufferPositionalSelector(argv) {
  const positionalArgs = Array.isArray(argv._)
    ? argv._.map((value) => String(value).trim()).filter(Boolean)
    : []
  if (!positionalArgs.length) return null

  if (positionalArgs.length > 1) {
    throw new Error(
      `Unexpected positional arguments: ${positionalArgs.join(', ')}`
    )
  }

  if (argv.model || argv.models || argv['start-from']) {
    throw new Error(
      `Positional selector "${positionalArgs[0]}" cannot be combined with --model, --models, or --start-from`
    )
  }

  return positionalArgs[0]
}

function selectStufferQueue(queue, argv) {
  let next = queue
  const singleModel = getOption(argv, 'model')
    ? String(getOption(argv, 'model')).trim()
    : null
  const explicitModels = String(
    getOption(argv, 'models') || getOption(argv, 'only-models') || ''
  )
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean)
  const positionalSelector = resolveStufferPositionalSelector(argv)
  const startFrom = getOption(argv, 'start-from')
    ? String(getOption(argv, 'start-from')).trim()
    : positionalSelector
  const limit = Math.max(Number.parseInt(getOption(argv, 'limit'), 10) || 0, 0)

  if (singleModel) next = next.filter((item) => item.model === singleModel)

  if (explicitModels.length) {
    const wanted = new Set(explicitModels)
    next = next.filter((item) => wanted.has(item.model))
  }

  if (startFrom) {
    next = next.filter((item) => item.model.localeCompare(startFrom) >= 0)
  }

  if (limit > 0) next = next.slice(0, limit)
  return next
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
  if (!fs.existsSync(summaryPath)) return null

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

async function runStufferModelUpdate(item, context) {
  const { logDir, withRepair, stopOnError } = context
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
    const label = `milkmaid:${item.model}`
    const code = await runScrape(sourceUrl, {
      model: item.model,
      'skip-nas-sync': true,
    })
    const scrapeRun = {
      ok: code === 0,
      code,
      label,
      command: `scrape ${sourceUrl} --model ${item.model} --skip-nas-sync`,
    }
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

    if (!scrapeRun.ok && stopOnError) break
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
    result.hashPrune = runScriptResult(
      path.join('scrapyard', 'pruneModelHashes.js'),
      ['--model', item.model],
      `prune:${item.model}`
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
    result.hashBackfill = runScriptResult(
      path.join('scrapyard', 'backfillModelHashes.js'),
      ['--model', item.model, '--include-video-visuals'],
      `backfill:${item.model}`
    )
  } else {
    result.hashBackfill = skippedCommandResult(
      'backfillModelHashes skipped because one or more scrapes failed'
    )
  }

  if (withRepair) {
    const validationPath = path.join(logDir, `${item.model}.validate.json`)
    result.validation = runScriptResult(
      path.join('scrapyard', 'validateModelHashes.js'),
      ['--model', item.model, '--json-out', validationPath],
      `validate:${item.model}`
    )
    result.validation = hydrateValidationResult(
      result.validation,
      validationPath
    )
  }

  result.finishedAt = new Date().toISOString()
  return result
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

function calculateStufferTotals(results) {
  return results.reduce(
    (totals, result) => {
      const perSourceTotals = summarizeSourceSummaries(result.sourceSummaries)
      totals.filesSaved += perSourceTotals.saved
      totals.sourceItemsHandled +=
        perSourceTotals.saved + perSourceTotals.duplicates
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

function writeStufferReport(report, latestReportPath, latestTextPath) {
  report.totals = calculateStufferTotals(report.results)
  fs.writeFileSync(latestReportPath, JSON.stringify(report, null, 2))

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

function printStufferBatchHelp() {
  console.log(`Usage: node milkmaid/update-stufferdb-models.js [options]

Options:
  [start-from]           Optional positional shorthand for --start-from.
  --model <name>         Update one model only.
  --models <a,b,c>       Update a comma-separated set of models.
  --start-from <name>    Start from this canonical model name.
  --limit <n>            Only process the first n selected models.
  --registry <path>      Override model_aliases.json path.
  --log-dir <path>       Override updater report directory.
  --stop-on-error        Stop the batch when one model fails.
  --with-repair          Also prune/backfill/validate hashes.
  -h, --help             Show help.
`)
}

async function runStufferDbBatch(argvInput = {}) {
  const argv = parseRunnerArgs(argvInput)
  if (isTruthy(getOption(argv, 'help'))) {
    printStufferBatchHelp()
    return 0
  }

  const stufferRegistryPath = path.resolve(
    String(getOption(argv, 'registry') || registryPath)
  )
  const logDir = path.resolve(
    String(
      getOption(argv, 'log-dir') ||
        path.join(rootDir, 'tmp', 'update-stufferdb')
    )
  )
  const latestReportPath = path.join(logDir, 'update-stufferdb-latest.json')
  const latestTextPath = path.join(logDir, 'update-stufferdb-latest.txt')
  fs.mkdirSync(logDir, { recursive: true })

  const registry = loadRegistry(stufferRegistryPath)
  const queue = buildStufferQueue(registry)
  const selectedQueue = selectStufferQueue(queue, argv)
  const withRepair = isTruthy(getOption(argv, 'with-repair'))
  const stopOnError = isTruthy(getOption(argv, 'stop-on-error'))
  const report = {
    generatedAt: new Date().toISOString(),
    registryPath: stufferRegistryPath,
    totalModelsInRegistry: queue.length,
    selectedModels: selectedQueue.length,
    stopOnError,
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

    const result = await runStufferModelUpdate(item, {
      logDir,
      withRepair,
      stopOnError,
    })
    report.results.push(result)
    writeStufferReport(report, latestReportPath, latestTextPath)

    if (
      stopOnError &&
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

  writeStufferReport(report, latestReportPath, latestTextPath)
  console.log('')
  console.log(`Latest report: ${latestReportPath}`)

  const failed = report.results.some(
    (result) =>
      !result.scrape.ok ||
      (withRepair &&
        (!result.hashPrune.ok ||
          !result.hashBackfill.ok ||
          !result.validation.ok))
  )
  return failed ? 1 : 0
}

function printAllSourcesHelp() {
  console.log(`Usage: node scrapyard/run-all-source-updates.js [options]

Runs all StufferDB, Coomer, and Kemono model sources in sequence.

Options:
  --only-models <a,b,c>       Limit to canonical model names.
  --start-from <name>         Start StufferDB from this canonical model name.
  --limit <n>                 Limit StufferDB queue size.
  --pages <n|a-b>             Limit Coomer/Kemono pages.
  --max-posts <n>             Limit Coomer/Kemono posts per source.
  --max-files <n>             Limit Coomer/Kemono media files per source.
  --post-concurrency <n>      Post fetch concurrency.
  --image-concurrency <n>     Image/gif concurrency.
  --video-concurrency <n>     Video concurrency.
  --delay-ms <n>              Delay between Coomer/Kemono models.
  --dry-run                   Dry run Coomer/Kemono.
  --skip-nas-sync             Skip NAS sync.
  --stop-on-error             Stop when StufferDB updater hits a failure.
  --help                      Show this help.
`)
}

async function runAllSourceUpdates(argvInput = {}) {
  const argv = parseRunnerArgs(argvInput)
  if (isTruthy(getOption(argv, 'help'))) {
    printAllSourcesHelp()
    return 0
  }

  console.log('Running StufferDB batch...')
  const stufferStatus = await runStufferDbBatch({
    models: getOption(argv, 'only-models'),
    'start-from': getOption(argv, 'start-from'),
    limit: getOption(argv, 'limit'),
    'stop-on-error': isTruthy(getOption(argv, 'stop-on-error')),
  })
  if (stufferStatus !== 0) return stufferStatus

  const sourceOptions = {
    'only-models': getOption(argv, 'only-models'),
    pages: getOption(argv, 'pages'),
    'max-posts': getOption(argv, 'max-posts'),
    'max-files': getOption(argv, 'max-files'),
    'post-concurrency': getOption(argv, 'post-concurrency'),
    'image-concurrency': getOption(argv, 'image-concurrency'),
    'video-concurrency': getOption(argv, 'video-concurrency'),
    'delay-ms': getOption(argv, 'delay-ms'),
    'dry-run': isTruthy(getOption(argv, 'dry-run')),
    'skip-nas-sync': isTruthy(getOption(argv, 'skip-nas-sync')),
  }

  console.log('\nRunning Coomer batch...')
  const coomerStatus = await runSourceBatch('coomer', sourceOptions)
  if (coomerStatus !== 0) return coomerStatus

  console.log('\nRunning Kemono batch...')
  return runSourceBatch('kemono', sourceOptions)
}

async function runScraperCli(argvInput = process.argv.slice(2), deps = {}) {
  const argv = parseRunnerArgs(argvInput)

  if (argv.help) {
    printHelp()
    return 0
  }

  const inputUrl = argv._[0]
  if (!inputUrl) {
    return runInteractiveLauncher()
  }

  return runScrape(inputUrl, argv, deps)
}

module.exports = {
  rootDir,
  registryPath,
  printHelp,
  parseRunnerArgs,
  appendOption,
  appendBoolean,
  runNodeScript,
  inferCanonicalModel,
  buildScraperArgs,
  buildScraperOptions,
  runScrape,
  runSourceBatch,
  runStufferDbBatch,
  runAllSourceUpdates,
  runScraperCli,
}

if (require.main === module) {
  runScraperCli()
    .then((code) => {
      process.exitCode = code
    })
    .catch((err) => {
      console.error(`Scraper runner failed: ${err.stack || err.message}`)
      process.exitCode = 1
    })
}

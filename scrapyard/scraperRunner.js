'use strict'

const path = require('path')
const fs = require('fs')

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
const {
  getOption,
  isTruthy,
  normalizeHoghaulRunOptions,
  normalizeMilkmaidRunOptions,
  parseRunnerArgs,
} = require('./scraperOptions')
const runLifecycle = require('./runLifecycle')

const rootDir = path.join(__dirname, '..')
const registryPath = path.join(rootDir, 'model_aliases.json')
const ALL_SOURCE_ORDER = ['reddit', 'kemono', 'coomer', 'stufferdb']

function formatDuration(ms) {
  const totalSeconds = Math.max(0, Math.round(Number(ms || 0) / 1000))
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60
  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`
  if (minutes > 0) return `${minutes}m ${seconds}s`
  return `${seconds}s`
}

function formatBytes(bytes) {
  const value = Number(bytes || 0)
  if (!Number.isFinite(value) || value <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let size = value
  let unitIndex = 0
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex += 1
  }
  return `${size.toFixed(unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`
}

function getSummaryCounter(summary, name, fallbackNames = []) {
  const counters = summary?.counters || {}
  for (const key of [name, ...fallbackNames]) {
    const value = counters[key] ?? summary?.[key]
    if (value !== undefined && value !== null && value !== '') {
      return Number(value) || 0
    }
  }
  return 0
}

function formatScrapeSummaryLine(summary) {
  const processed = getSummaryCounter(summary, 'processed')
  const expected = getSummaryCounter(summary, 'expectedMedia')
  const saved = getSummaryCounter(summary, 'saved', ['successCount'])
  const skipped = getSummaryCounter(summary, 'skipped')
  const dupes = getSummaryCounter(summary, 'duplicates', ['duplicateCount'])
  const failed = getSummaryCounter(summary, 'failures', ['errorCount'])
  const savedBytes = Number(summary?.transfer?.savedBytes || 0)
  const pieces = [
    `time ${formatDuration(summary?.durationMs)}`,
    `processed ${processed}/${expected}`,
    `saved ${saved}`,
    `skipped ${skipped}`,
    `dupes ${dupes}`,
    `failed ${failed}`,
    `downloaded ${formatBytes(savedBytes)}`,
  ]
  return `Run stats: ${pieces.join(' | ')}`
}

function printHelp() {
  console.log(`Usage:
  npm run scrape -- <source-url> [options]
  npm run scrape -- scrape <source-url> [options]
  npm run scrape -- update <all|stufferdb|reddit|coomer|coomerfans|kemono> [options]
  npm run scrape -- repair [options]
  npm run scrape -- sync <--push|--pull|--model <name>> [options]

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

Repair options:
  --model <name>                   Repair one model only.
  --models <a,b,c>                 Repair selected models.
  --start-from <name>              Start from this model.
  --limit <n>                      Limit selected models.
  --scrape                         Repair runner also re-scrapes sources.

Sync options:
  --push                           Push local dataset to NAS.
  --pull                           Pull NAS dataset to local.
  --model <name>                   Sync one model to NAS.
  --cleanup-mp4=true               Remove mirrored local MP4s after push.
  --cleanup-gif-mp4=true           Remove GIF-derived MP4s after push.
  --help                           Show this help.
`)
}

function appendOption(args, flag, value) {
  if (value === undefined || value === null || value === '') return
  args.push(flag, String(value))
}

function appendBoolean(args, flag, value) {
  if (value === true) args.push(flag)
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

function runNodeScriptQuiet(scriptPath, args) {
  const { spawnSync } = require('child_process')
  const result = spawnSync(process.execPath, [scriptPath, ...args], {
    cwd: rootDir,
    stdio: 'inherit',
  })
  return result.status ?? 1
}

function runNodeScriptInteractive(
  scriptPath,
  args,
  { log = console.log } = {}
) {
  const { spawn } = require('child_process')
  log('')
  log(`Running: node ${scriptPath} ${args.join(' ')}`.trim())
  log('')

  return new Promise((resolve) => {
    const child = spawn(process.execPath, [scriptPath, ...args], {
      cwd: rootDir,
      stdio: 'inherit',
    })
    let settled = false
    let killTimer = null

    const cleanup = () => {
      process.off('SIGINT', onSigint)
      process.off('SIGTERM', onSigterm)
      if (killTimer) clearTimeout(killTimer)
    }
    const forwardSignal = (signal, fallbackCode) => {
      if (settled) return
      child.kill(signal)
      killTimer = setTimeout(() => {
        if (!settled && child.exitCode === null) child.kill('SIGKILL')
      }, 5000)
      if (killTimer.unref) killTimer.unref()
      process.exitCode = fallbackCode
    }
    const onSigint = () => forwardSignal('SIGINT', 130)
    const onSigterm = () => forwardSignal('SIGTERM', 143)

    process.on('SIGINT', onSigint)
    process.on('SIGTERM', onSigterm)
    child.on('error', (err) => {
      if (settled) return
      settled = true
      cleanup()
      console.error(`Failed to run ${scriptPath}: ${err.message}`)
      resolve(1)
    })
    child.on('exit', (code, signal) => {
      if (settled) return
      settled = true
      cleanup()
      if (signal === 'SIGINT') return resolve(130)
      if (signal === 'SIGTERM') return resolve(143)
      resolve(code ?? 1)
    })
  })
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

function buildRepairArgs(argvInput = {}) {
  const argv = parseRunnerArgs(argvInput)
  const args = []
  for (const name of [
    'model',
    'models',
    'start-from',
    'limit',
    'registry',
    'log-dir',
    'dataset-root',
    'nas-dataset-root',
  ]) {
    appendOption(args, `--${name}`, getOption(argv, name))
  }
  appendBoolean(
    args,
    '--stop-on-error',
    isTruthy(getOption(argv, 'stop-on-error'))
  )
  appendBoolean(args, '--scrape', isTruthy(getOption(argv, 'scrape')))
  appendBoolean(
    args,
    '--skip-nas-sync',
    isTruthy(getOption(argv, 'skip-nas-sync'))
  )
  appendBoolean(args, '--help', isTruthy(getOption(argv, 'help')))
  return args
}

function buildSyncArgs(argvInput = {}) {
  const argv = parseRunnerArgs(argvInput)
  const args = []
  appendBoolean(args, '--push', isTruthy(getOption(argv, 'push')))
  appendBoolean(args, '--pull', isTruthy(getOption(argv, 'pull')))
  appendOption(args, '--model', getOption(argv, 'model'))
  appendOption(args, '--cleanup-mp4', getOption(argv, 'cleanup-mp4'))
  appendOption(args, '--cleanup-gif-mp4', getOption(argv, 'cleanup-gif-mp4'))
  appendBoolean(args, '--help', isTruthy(getOption(argv, 'help')))
  return args
}

function runRepair(argvInput = {}) {
  return runNodeScript(
    path.join('milkmaid', 'repair-stufferdb-models.js'),
    buildRepairArgs(argvInput)
  )
}

function runSync(argvInput = {}) {
  return runNodeScript(
    path.join('scrapyard', 'sync.js'),
    buildSyncArgs(argvInput)
  )
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
  const modelName = isTruthy(getOption(argv, 'no-model-infer'))
    ? ''
    : inferCanonicalModel(parsedSource, getOption(argv, 'model'))
  appendOption(args, '--model', modelName)
  appendBoolean(
    args,
    '--skip-nas-sync',
    isTruthy(getOption(argv, 'skip-nas-sync'))
  )
  appendBoolean(
    args,
    '--keep-history',
    isTruthy(getOption(argv, 'keep-history'))
  )
}

function appendHoghaulOptions(args, argv) {
  appendOption(args, '--pages', getOption(argv, 'pages'))
  appendOption(args, '--max-posts', getOption(argv, 'max-posts'))
  appendOption(args, '--max-files', getOption(argv, 'max-files'))
  appendOption(args, '--post-concurrency', getOption(argv, 'post-concurrency'))
  appendOption(
    args,
    '--image-concurrency',
    getOption(argv, 'image-concurrency')
  )
  appendOption(
    args,
    '--video-concurrency',
    getOption(argv, 'video-concurrency')
  )
  appendOption(args, '--cookie', getOption(argv, 'cookie'))
  appendOption(args, '--cookie-file', getOption(argv, 'cookie-file'))
  appendOption(
    args,
    '--browser-executable',
    getOption(argv, 'browser-executable')
  )
  appendOption(args, '--browser-profile', getOption(argv, 'browser-profile'))
  appendOption(args, '--browser-connect', getOption(argv, 'browser-connect'))
  appendOption(
    args,
    '--browser-validate-ms',
    getOption(argv, 'browser-validate-ms')
  )
  appendBoolean(args, '--dry-run', isTruthy(getOption(argv, 'dry-run')))
  appendBoolean(args, '--preflight', isTruthy(getOption(argv, 'preflight')))
  appendBoolean(
    args,
    '--track-source',
    isTruthy(getOption(argv, 'track-source'))
  )
  appendBoolean(
    args,
    '--browser-headless',
    isTruthy(getOption(argv, 'browser-headless'))
  )
  appendBoolean(args, '--headless', isTruthy(getOption(argv, 'headless')))
  appendOptionalBoolean(args, 'browser-media', getOption(argv, 'browser-media'))
}

function appendMilkmaidOptions(args, argv) {
  appendBoolean(
    args,
    '--review-errors',
    isTruthy(getOption(argv, 'review-errors'))
  )
}

function getRunnerModelName(parsedSource, argv) {
  return isTruthy(getOption(argv, 'no-model-infer'))
    ? ''
    : inferCanonicalModel(parsedSource, getOption(argv, 'model'))
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
    skipNasSync: isTruthy(getOption(argv, 'skip-nas-sync')),
    keepHistory: isTruthy(getOption(argv, 'keep-history')),
  }

  if (parsedSource.scraper === 'milkmaid') {
    return normalizeMilkmaidRunOptions({
      ...argv,
      ...sharedOptions,
      reviewErrors: isTruthy(getOption(argv, 'review-errors')),
    })
  }

  if (parsedSource.scraper === 'hoghaul') {
    return normalizeHoghaulRunOptions({
      ...argv,
      ...sharedOptions,
      pages: getOption(argv, 'pages'),
      maxPosts: getOption(argv, 'max-posts'),
      maxFiles: getOption(argv, 'max-files'),
      postConcurrency: getOption(argv, 'post-concurrency'),
      imageConcurrency: getOption(argv, 'image-concurrency'),
      videoConcurrency: getOption(argv, 'video-concurrency'),
      cookie: getOption(argv, 'cookie'),
      cookieFile: getOption(argv, 'cookie-file'),
      browserExecutable: getOption(argv, 'browser-executable'),
      browserProfile: getOption(argv, 'browser-profile'),
      browserConnect: getOption(argv, 'browser-connect'),
      browserValidateMs: getOption(argv, 'browser-validate-ms'),
      dryRun: isTruthy(getOption(argv, 'dry-run')),
      preflight: isTruthy(getOption(argv, 'preflight')),
      trackSource: isTruthy(getOption(argv, 'track-source')),
      browserMedia: getOption(argv, 'browser-media'),
      browserHeadless: isTruthy(getOption(argv, 'browser-headless')),
      headless: isTruthy(getOption(argv, 'headless')),
    })
  }

  return sharedOptions
}

function getModelRunSummaryPath(modelName, source = 'milkmaid') {
  return path.join(
    process.env.APPDATA || '',
    '.slopvault',
    'dataset',
    modelName,
    `${source}-last-run.json`
  )
}

function readModelRunSummary(modelName, source = 'milkmaid') {
  const summaryPath = getModelRunSummaryPath(modelName, source)
  if (!fs.existsSync(summaryPath)) return null

  try {
    return JSON.parse(fs.readFileSync(summaryPath, 'utf8'))
  } catch (err) {
    return {
      parseError: err.message,
    }
  }
}

function getStatsFromRunSummary(summary) {
  if (!summary || summary.parseError) return null
  return runLifecycle.getRunProgressStats(
    {
      counters: summary.counters || {},
      transfer: summary.transfer || {},
    },
    {
      processed: summary.mediaCount,
      expectedMedia: summary.mediaCount,
      saved: summary.successCount,
      duplicates: summary.duplicateCount,
      failures: summary.errorCount,
      transfer: summary.transfer || {},
    }
  )
}

function printRecoveredRunSummary(summary, { log = console.log } = {}) {
  const stats = getStatsFromRunSummary(summary)
  if (!stats) return

  log('')
  log(
    'Hoghaul ended before a normal run_finished event; recovered latest counters:'
  )
  log(runLifecycle.formatRunSummaryLine(stats))
  if (summary.logPath) log(`Run log: ${summary.logPath}`)
}

async function runHoghaulScript(
  scriptPath,
  args,
  parsedSource,
  argv,
  { log } = {}
) {
  const modelName = getRunnerModelName(parsedSource, argv)
  const code = await runNodeScriptInteractive(scriptPath, args, { log })
  const summary = modelName ? readModelRunSummary(modelName, 'hoghaul') : null
  if (summary?.status === 'running') {
    printRecoveredRunSummary(summary, { log })
    return code === 0 ? 1 : code
  }
  return code
}

function applyScrapePositionalFallback(inputUrl, argvInput = {}) {
  const argv = parseRunnerArgs(argvInput)
  const positionals = Array.isArray(argv._) ? argv._ : []
  if (positionals.length <= 1) return argv

  const extras = positionals.slice(1).filter((value) => {
    const text = String(value || '').trim()
    return text && !/^https?:\/\//i.test(text)
  })
  if (!extras.length) return argv

  const next = {
    ...argv,
    _: [inputUrl],
  }
  const hasMeaningfulValue = (name) => {
    const value = getOption(next, name)
    if (value === undefined || value === null || value === '') return false
    return value !== true && value !== 'true'
  }
  const consume = (name) => {
    if (hasMeaningfulValue(name) || extras.length === 0) return
    next[name] = extras.shift()
  }

  consume('model')
  consume('pages')
  consume('max-posts')
  consume('max-files')
  return next
}

async function runScrape(inputUrl, argvInput = {}, deps = {}) {
  const log = deps.log || console.log
  const error = deps.error || console.error
  const argv = applyScrapePositionalFallback(inputUrl, argvInput)
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
  let status = 0
  if (deps.runCommand) {
    status = await runCommand(scriptPath, args, { log })
  } else if (parsedSource.scraper === 'hoghaul') {
    status = await runHoghaulScript(scriptPath, args, parsedSource, argv, {
      log,
    })
  } else {
    status = await runInProcessScraper(
      parsedSource,
      buildScraperOptions(parsedSource, argv)
    )
  }

  const modelName = getRunnerModelName(parsedSource, argv)
  const summary = modelName
    ? readModelRunSummary(modelName, parsedSource.scraper)
    : null
  if (summary?.durationMs !== undefined) log(formatScrapeSummaryLine(summary))
  return status
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

function getSourceLabel(sourceKey, url) {
  const parsed = parseSourceUrl(url)
  if (!parsed) return sourceKey
  if (parsed.sourceType === 'coomerfans') return 'coomerfans'
  return parsed.sourceType || sourceKey
}

function normalizeRegistrySourceUrls(sourceList) {
  return (Array.isArray(sourceList) ? sourceList : [])
    .map((source) => String(source?.url || '').trim())
    .filter(Boolean)
}

function getOrderedSourceKeys(sources = {}) {
  const sourceKeys = Object.keys(sources || {})
  const known = ALL_SOURCE_ORDER.filter((sourceKey) =>
    sourceKeys.includes(sourceKey)
  )
  const extra = sourceKeys
    .filter((sourceKey) => !ALL_SOURCE_ORDER.includes(sourceKey))
    .sort((left, right) => left.localeCompare(right))
  return [...known, ...extra]
}

function buildAllSourceQueue(registry) {
  return Object.entries(registry || {})
    .map(([model, entry]) => {
      const sources =
        entry?.sources && typeof entry.sources === 'object' ? entry.sources : {}
      const targets = []

      for (const sourceKey of getOrderedSourceKeys(sources)) {
        for (const url of normalizeRegistrySourceUrls(sources[sourceKey])) {
          targets.push({
            sourceKey,
            url,
            label: getSourceLabel(sourceKey, url),
          })
        }
      }

      return {
        model,
        sources: targets,
      }
    })
    .filter((item) => item.sources.length > 0)
    .sort((left, right) => left.model.localeCompare(right.model))
}

function selectAllSourceQueue(queue, argv) {
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
  const startFrom = getOption(argv, 'start-from')
    ? String(getOption(argv, 'start-from')).trim()
    : null
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
  console.log(`Usage: node scrapyard/run-source-batch.js --source=<coomer|kemono> [options]

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
  return getModelRunSummaryPath(modelName, 'milkmaid')
}

function readLastRunSummary(modelName) {
  return readModelRunSummary(modelName, 'milkmaid')
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
  console.log(`Usage: node scrapyard/run-stufferdb-batch.js [options]

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

Runs every selected model source before moving to the next model.
Registered Reddit, Kemono, Coomer/CoomerFans, and StufferDB sources are included.

Options:
  --model <name>              Update one model only.
  --only-models <a,b,c>       Limit to canonical model names.
  --start-from <name>         Start from this canonical model name.
  --limit <n>                 Limit selected model count.
  --pages <n|a-b>             Limit Hoghaul pages.
  --max-posts <n>             Limit Hoghaul posts per source.
  --max-files <n>             Limit Hoghaul media files per source.
  --post-concurrency <n>      Post fetch concurrency.
  --image-concurrency <n>     Image/gif concurrency.
  --video-concurrency <n>     Video concurrency.
  --delay-ms <n>              Delay between source runs.
  --dry-run                   Dry run Hoghaul sources.
  --skip-nas-sync             Skip NAS sync.
  --stop-on-error             Stop when a source run fails.
  --help                      Show this help.
`)
}

function buildAllSourceRunOptions(argv, modelName, parsedSource) {
  const options = {
    model: modelName,
  }

  const keepHistory = !(
    getOption(argv, 'keep-history') === false ||
    getOption(argv, 'keep-history') === 'false'
  )
  if (keepHistory) options['keep-history'] = true
  if (isTruthy(getOption(argv, 'skip-nas-sync'))) {
    options['skip-nas-sync'] = true
  }

  if (parsedSource?.scraper !== 'hoghaul') return options

  return {
    ...options,
    ...buildSourceBatchOptions(argv),
  }
}

function summarizeSourceRunSummary(summary) {
  const stats = getStatsFromRunSummary(summary)
  return {
    status: summary?.status || null,
    saved: Number(stats?.saved || summary?.successCount || 0),
    duplicates: Number(stats?.duplicates || summary?.duplicateCount || 0),
    errors: Number(stats?.failures || summary?.errorCount || 0),
    processed: Number(stats?.processed || summary?.mediaCount || 0),
    expectedMedia: Number(stats?.expectedMedia || summary?.mediaCount || 0),
    savedBytes: Number(stats?.savedBytes || 0),
    finishedAt: summary?.finishedAt || summary?.updatedAt || null,
    logPath: summary?.logPath || null,
  }
}

async function runAllSourceModelUpdate(item, context = {}) {
  const { argv, stopOnError } = context
  const result = {
    model: item.model,
    startedAt: new Date().toISOString(),
    sources: item.sources,
    runs: [],
    finishedAt: null,
  }

  for (let index = 0; index < item.sources.length; index += 1) {
    const source = item.sources[index]
    const parsedSource = parseSourceUrl(source.url)
    const sourceLabel = source.label || source.sourceKey

    console.log('')
    console.log(
      `  [${index + 1}/${item.sources.length}] ${item.model} -> ${sourceLabel}: ${source.url}`
    )

    if (!parsedSource) {
      const run = {
        ok: false,
        code: 1,
        sourceKey: source.sourceKey,
        label: sourceLabel,
        url: source.url,
        error: 'Unrecognized source URL',
      }
      result.runs.push(run)
      if (stopOnError) break
      continue
    }

    const code = await runScrape(
      source.url,
      buildAllSourceRunOptions(argv, item.model, parsedSource)
    )
    const summary = readModelRunSummary(item.model, parsedSource.scraper)
    const run = {
      ok: code === 0,
      code,
      scraper: parsedSource.scraper,
      sourceType: parsedSource.sourceType,
      sourceKey: source.sourceKey,
      label: sourceLabel,
      url: source.url,
      summary: summarizeSourceRunSummary(summary),
    }
    result.runs.push(run)

    if (code !== 0 && stopOnError) break
  }

  result.finishedAt = new Date().toISOString()
  return result
}

function calculateAllSourceTotals(results) {
  return results.reduce(
    (totals, result) => {
      for (const run of result.runs || []) {
        totals.runs += 1
        if (!run.ok) totals.failures += 1
        totals.saved += Number(run.summary?.saved || 0)
        totals.duplicates += Number(run.summary?.duplicates || 0)
        totals.errors += Number(run.summary?.errors || 0)
        totals.processed += Number(run.summary?.processed || 0)
        totals.expectedMedia += Number(run.summary?.expectedMedia || 0)
        totals.savedBytes += Number(run.summary?.savedBytes || 0)
      }
      return totals
    },
    {
      runs: 0,
      failures: 0,
      saved: 0,
      duplicates: 0,
      errors: 0,
      processed: 0,
      expectedMedia: 0,
      savedBytes: 0,
    }
  )
}

function writeAllSourceReport(report, latestReportPath, latestTextPath) {
  report.totals = calculateAllSourceTotals(report.results)
  fs.writeFileSync(latestReportPath, JSON.stringify(report, null, 2))

  const lines = [
    `Generated: ${report.generatedAt}`,
    `Registry: ${report.registryPath}`,
    `Selected models: ${report.selectedModels}`,
    `Totals: models=${report.results.length} runs=${report.totals.runs} saved=${report.totals.saved} dupes=${report.totals.duplicates} errors=${report.totals.errors} failures=${report.totals.failures} bytes=${runLifecycle.formatBytes(report.totals.savedBytes)}`,
    '',
  ]

  for (const item of report.results) {
    const failedRuns = item.runs.filter((run) => !run.ok).length
    lines.push(
      `${item.model} :: sources=${item.runs.length}/${item.sources.length} :: ${failedRuns ? 'fail' : 'ok'}`
    )
    for (const run of item.runs) {
      lines.push(
        `  ${run.label}: saved=${run.summary.saved} dupes=${run.summary.duplicates} errors=${run.summary.errors} status=${run.summary.status || 'unknown'}`
      )
    }
  }

  fs.writeFileSync(latestTextPath, lines.join('\n') + '\n')
}

async function runAllSourceUpdates(argvInput = {}) {
  const argv = parseRunnerArgs(argvInput)
  if (isTruthy(getOption(argv, 'help'))) {
    printAllSourcesHelp()
    return 0
  }

  const allRegistryPath = path.resolve(
    String(getOption(argv, 'registry') || registryPath)
  )
  const logDir = path.resolve(
    String(
      getOption(argv, 'log-dir') ||
        path.join(rootDir, 'tmp', 'update-all-sources')
    )
  )
  const latestReportPath = path.join(logDir, 'update-all-sources-latest.json')
  const latestTextPath = path.join(logDir, 'update-all-sources-latest.txt')
  fs.mkdirSync(logDir, { recursive: true })

  const registry = loadRegistry(allRegistryPath)
  const queue = buildAllSourceQueue(registry)
  const selectedQueue = selectAllSourceQueue(queue, argv)
  const stopOnError = isTruthy(getOption(argv, 'stop-on-error'))
  const delayMs = Number.parseInt(getOption(argv, 'delay-ms'), 10) || 0
  const report = {
    generatedAt: new Date().toISOString(),
    registryPath: allRegistryPath,
    totalModelsInRegistry: queue.length,
    selectedModels: selectedQueue.length,
    stopOnError,
    totals: {},
    results: [],
  }

  console.log(
    `All-source update queue: ${selectedQueue.length} model(s) selected from ${queue.length} with saved sources`
  )

  for (let index = 0; index < selectedQueue.length; index += 1) {
    const item = selectedQueue[index]
    console.log('')
    console.log(
      `[${index + 1}/${selectedQueue.length}] Updating ${item.model} from ${item.sources.length} source(s)`
    )

    const result = await runAllSourceModelUpdate(item, {
      argv,
      stopOnError,
    })
    report.results.push(result)
    writeAllSourceReport(report, latestReportPath, latestTextPath)

    const failed = result.runs.some((run) => !run.ok)
    if (stopOnError && failed) {
      console.log('Stopping on first error because --stop-on-error was set.')
      break
    }

    if (delayMs > 0 && index < selectedQueue.length - 1) {
      await sleep(delayMs)
    }
  }

  writeAllSourceReport(report, latestReportPath, latestTextPath)
  console.log('')
  console.log(`Latest report: ${latestReportPath}`)

  return report.results.some((result) => result.runs.some((run) => !run.ok))
    ? 1
    : 0
}

async function runScraperCli(argvInput = process.argv.slice(2), deps = {}) {
  const rawArgs = Array.isArray(argvInput) ? argvInput : []
  const command = String(rawArgs[0] || '')
    .trim()
    .toLowerCase()

  if (command === 'scrape') {
    const scrapeArgs = rawArgs.slice(1)
    const argv = parseRunnerArgs(scrapeArgs)
    const inputUrl = argv._[0]
    if (!inputUrl) {
      printHelp()
      return 1
    }
    return runScrape(inputUrl, argv, deps)
  }

  if (command === 'repair') return runRepair(rawArgs.slice(1))
  if (command === 'sync') return runSync(rawArgs.slice(1))

  if (command === 'update') {
    const updateArgs = rawArgs.slice(2)
    const target = String(rawArgs[1] || 'all')
      .trim()
      .toLowerCase()
    if (target === 'all') return runAllSourceUpdates(updateArgs)
    if (target === 'stufferdb' || target === 'stuffer') {
      return runStufferDbBatch(updateArgs)
    }
    if (target === 'coomerfans') {
      return runSourceBatch('coomer', {
        ...parseRunnerArgs(updateArgs),
        'host-contains': 'coomerfans.com',
      })
    }
    if (target === 'coomer' || target === 'kemono' || target === 'reddit') {
      return runSourceBatch(target, updateArgs)
    }
    printHelp()
    return 1
  }

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
  buildAllSourceQueue,
  buildScraperArgs,
  buildScraperOptions,
  applyScrapePositionalFallback,
  buildRepairArgs,
  buildSyncArgs,
  runScrape,
  runRepair,
  runSync,
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

'use strict'

const path = require('path')
const { spawnSync } = require('child_process')
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
        model: 'm',
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

function appendOptionalBoolean(args, optionName, value) {
  if (value === true) {
    args.push(`--${optionName}`)
  } else if (value === false) {
    args.push(`--no-${optionName}`)
  }
}

function runNodeScript(scriptPath, args, { log = console.log } = {}) {
  log('')
  log(`Running: node ${scriptPath} ${args.join(' ')}`.trim())
  log('')
  const result = spawnSync(process.execPath, [scriptPath, ...args], {
    cwd: rootDir,
    stdio: 'inherit',
  })
  return result.status ?? 1
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

function runScrape(inputUrl, argvInput = {}, deps = {}) {
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
  return runCommand(scriptPath, buildScraperArgs(parsedSource, argv), { log })
}

function runScraperCli(argvInput = process.argv.slice(2), deps = {}) {
  const argv = parseRunnerArgs(argvInput)

  if (argv.help) {
    printHelp()
    return 0
  }

  const inputUrl = argv._[0]
  if (!inputUrl) {
    return runNodeScript(
      path.join('scrapyard', 'run-scrape-interactive.js'),
      []
    )
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
  runScrape,
  runScraperCli,
}

if (require.main === module) {
  process.exitCode = runScraperCli()
}

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

function printHelp() {
  console.log(`Usage: npm run scrape -- <source-url> [options]

Scrapes one StufferDB, Reddit, Coomer, CoomerFans, or Kemono source URL.
With no URL, opens the interactive scrape launcher.

Options:
  --model <canonical>         Force the destination model bucket.
  --skip-nas-sync            Pass through to the scraper.
  --pages <n|a-b>            Hoghaul page limit.
  --max-posts <n>            Hoghaul post limit.
  --max-files <n>            Hoghaul media limit.
  --post-concurrency <n>     Hoghaul post fetch concurrency.
  --image-concurrency <n>    Hoghaul image/gif concurrency.
  --video-concurrency <n>    Hoghaul video concurrency.
  --help                     Show this help.
`)
}

function appendOption(args, flag, value) {
  if (value === undefined || value === null || value === '') return
  args.push(flag, String(value))
}

function appendBoolean(args, flag, value) {
  if (value) args.push(flag)
}

function runNodeScript(scriptPath, args) {
  console.log('')
  console.log(`Running: node ${scriptPath} ${args.join(' ')}`.trim())
  console.log('')
  const result = spawnSync(process.execPath, [scriptPath, ...args], {
    cwd: rootDir,
    stdio: 'inherit',
  })
  return result.status || 0
}

function inferCanonicalModel(parsedSource, explicitModel) {
  if (explicitModel) return explicitModel
  if (!parsedSource?.rawName) return ''

  const registry = loadModelRegistry(registryPath)
  return findCanonicalModelName(registry, sanitize(parsedSource.rawName)) || ''
}

function buildScraperArgs(parsedSource, argv) {
  const args = [parsedSource.url]
  appendOption(args, '--model', inferCanonicalModel(parsedSource, argv.model))
  appendBoolean(args, '--skip-nas-sync', argv['skip-nas-sync'])

  if (parsedSource.scraper === 'hoghaul') {
    appendOption(args, '--pages', argv.pages)
    appendOption(args, '--max-posts', argv['max-posts'])
    appendOption(args, '--max-files', argv['max-files'])
    appendOption(args, '--post-concurrency', argv['post-concurrency'])
    appendOption(args, '--image-concurrency', argv['image-concurrency'])
    appendOption(args, '--video-concurrency', argv['video-concurrency'])
  }

  return args
}

function main() {
  const argv = minimist(process.argv.slice(2), {
    string: [
      'model',
      'pages',
      'max-posts',
      'max-files',
      'post-concurrency',
      'image-concurrency',
      'video-concurrency',
    ],
    boolean: ['skip-nas-sync', 'help'],
  })

  if (argv.help) {
    printHelp()
    return
  }

  const inputUrl = argv._[0]
  if (!inputUrl) {
    process.exitCode = runNodeScript(
      path.join('scrapyard', 'run-scrape-interactive.js'),
      []
    )
    return
  }

  const parsedSource = parseSourceUrl(inputUrl)
  if (!parsedSource) {
    console.error(
      'Could not recognize that URL as StufferDB, Reddit, Coomer, CoomerFans, or Kemono.'
    )
    process.exitCode = 1
    return
  }

  const scriptPath = getScraperScript(parsedSource)
  if (!scriptPath) {
    console.error(
      `No scraper is registered for ${describeSource(parsedSource)}.`
    )
    process.exitCode = 1
    return
  }

  console.log(`Detected source: ${describeSource(parsedSource)}`)
  if (parsedSource.rawName) {
    console.log(`Detected name: ${parsedSource.rawName}`)
  }

  process.exitCode = runNodeScript(
    scriptPath,
    buildScraperArgs(parsedSource, argv)
  )
}

main()

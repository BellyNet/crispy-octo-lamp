'use strict'

const path = require('path')
const { spawnSync } = require('child_process')
const minimist = require('minimist')

const rootDir = path.join(__dirname, '..')

function getOption(argv, name) {
  const directValue = argv[name]
  if (directValue !== undefined) return directValue
  const envName = `npm_config_${String(name).replace(/-/g, '_')}`
  return process.env[envName]
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

function printHelp() {
  console.log(`Usage: node scrapyard/run-all-source-updates.js [options]

Runs all StufferDB, Coomer, and Kemono model sources in sequence.

Options:
  --only-models <a,b,c>       Limit to canonical model names.
  --start-from <name>         Start from this canonical model name.
  --limit <n>                 Limit StufferDB queue size.
  --pages <n|a-b>             Limit Hoghaul pages.
  --max-posts <n>             Limit Hoghaul posts per source.
  --max-files <n>             Limit Hoghaul media files per source.
  --post-concurrency <n>      Hoghaul post fetch concurrency.
  --image-concurrency <n>     Hoghaul image/gif concurrency.
  --video-concurrency <n>     Hoghaul video concurrency.
  --delay-ms <n>              Delay between Hoghaul models.
  --dry-run                   Pass through to Hoghaul.
  --skip-nas-sync             Pass through to Hoghaul and Milkmaid.
  --stop-on-error             Stop when StufferDB updater hits a failure.
  --help                      Show this help.
`)
}

function runNodeScript(scriptPath, args) {
  const result = spawnSync(process.execPath, [scriptPath, ...args], {
    cwd: rootDir,
    stdio: 'inherit',
  })
  return result.status || 0
}

function appendOption(args, flag, value) {
  if (value === undefined || value === null || value === '') return
  args.push(flag, String(value))
}

function main() {
  const argv = minimist(process.argv.slice(2), {
    string: [
      'only-models',
      'start-from',
      'limit',
      'pages',
      'max-posts',
      'max-files',
      'post-concurrency',
      'image-concurrency',
      'video-concurrency',
      'delay-ms',
    ],
    boolean: ['dry-run', 'skip-nas-sync', 'stop-on-error', 'help'],
  })

  if (isTruthy(getOption(argv, 'help'))) {
    printHelp()
    return
  }

  const onlyModels = getOption(argv, 'only-models')
  const startFrom = getOption(argv, 'start-from')
  const limit = getOption(argv, 'limit')

  console.log('Running StufferDB batch...')
  const stufferArgs = []
  appendOption(stufferArgs, '--models', onlyModels)
  appendOption(stufferArgs, '--start-from', startFrom)
  appendOption(stufferArgs, '--limit', limit)
  if (isTruthy(getOption(argv, 'stop-on-error'))) {
    stufferArgs.push('--stop-on-error')
  }
  const skipNasSync = isTruthy(getOption(argv, 'skip-nas-sync'))
  if (skipNasSync) {
    stufferArgs.push('--skip-nas-sync')
  }
  const stufferStatus = runNodeScript(
    path.join('milkmaid', 'update-stufferdb-models.js'),
    stufferArgs
  )
  if (stufferStatus !== 0) {
    process.exitCode = stufferStatus
    return
  }

  const sharedHoghaulArgs = []
  appendOption(sharedHoghaulArgs, '--only-models', onlyModels)
  appendOption(sharedHoghaulArgs, '--pages', getOption(argv, 'pages'))
  appendOption(sharedHoghaulArgs, '--max-posts', getOption(argv, 'max-posts'))
  appendOption(sharedHoghaulArgs, '--max-files', getOption(argv, 'max-files'))
  appendOption(
    sharedHoghaulArgs,
    '--post-concurrency',
    getOption(argv, 'post-concurrency')
  )
  appendOption(
    sharedHoghaulArgs,
    '--image-concurrency',
    getOption(argv, 'image-concurrency')
  )
  appendOption(
    sharedHoghaulArgs,
    '--video-concurrency',
    getOption(argv, 'video-concurrency')
  )
  appendOption(sharedHoghaulArgs, '--delay-ms', getOption(argv, 'delay-ms'))
  if (isTruthy(getOption(argv, 'dry-run'))) {
    sharedHoghaulArgs.push('--dry-run')
  }
  if (skipNasSync) {
    sharedHoghaulArgs.push('--skip-nas-sync')
  }

  console.log('\nRunning Coomer batch...')
  const coomerStatus = runNodeScript(path.join('hoghaul', 'run-source-batch.js'), [
    '--source',
    'coomer',
    ...sharedHoghaulArgs,
  ])
  if (coomerStatus !== 0) {
    process.exitCode = coomerStatus
    return
  }

  console.log('\nRunning Kemono batch...')
  const kemonoStatus = runNodeScript(path.join('hoghaul', 'run-source-batch.js'), [
    '--source',
    'kemono',
    ...sharedHoghaulArgs,
  ])
  if (kemonoStatus !== 0) {
    process.exitCode = kemonoStatus
  }
}

main()

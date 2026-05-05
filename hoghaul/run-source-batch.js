'use strict'

const fs = require('fs')
const path = require('path')
const { spawnSync } = require('child_process')
const minimist = require('minimist')

const rootDir = path.join(__dirname, '..')
const registryPath = path.join(rootDir, 'model_aliases.json')

function normalizeList(value) {
  if (!value) return null
  return new Set(
    String(value)
      .split(',')
      .map((part) => part.trim())
      .filter(Boolean)
  )
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
  const directValue = argv[name]
  if (directValue !== undefined) return directValue
  const envName = `npm_config_${String(name).replace(/-/g, '_')}`
  return process.env[envName]
}

function loadRegistry() {
  return JSON.parse(fs.readFileSync(registryPath, 'utf8'))
}

function collectTargets(registry, sourceKey, modelFilter, hostContains) {
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

function printHelp() {
  console.log(`Usage: node hoghaul/run-source-batch.js --source=<coomer|kemono> [options]

Options:
  --source <name>             Registry source key to run (required).
  --only-models <a,b,c>       Limit to canonical model names.
  --host-contains <text>      Optional URL host filter, e.g. coomerfans.com.
  --pages <n|a-b>             Limit Hoghaul pages.
  --max-posts <n>             Limit Hoghaul posts per source.
  --max-files <n>             Limit Hoghaul media files per source.
  --post-concurrency <n>      Hoghaul post fetch concurrency.
  --image-concurrency <n>     Hoghaul image/gif concurrency.
  --video-concurrency <n>     Hoghaul video concurrency.
  --delay-ms <n>              Delay between models.
  --dry-run                   Pass through to Hoghaul.
  --skip-nas-sync             Pass through to Hoghaul.
  --keep-history              Pass through to Hoghaul (default: true).
  --help                      Show this help.
`)
}

function main() {
  const argv = minimist(process.argv.slice(2), {
    string: [
      'source',
      'only-models',
      'host-contains',
      'pages',
      'max-posts',
      'max-files',
      'post-concurrency',
      'image-concurrency',
      'video-concurrency',
      'delay-ms',
    ],
    boolean: ['dry-run', 'skip-nas-sync', 'keep-history', 'help'],
    default: {
      'keep-history': true,
      'post-concurrency': '8',
      'image-concurrency': '6',
      'video-concurrency': '6',
      'delay-ms': '0',
    },
  })

  if (isTruthy(getOption(argv, 'help'))) {
    printHelp()
    return
  }

  const sourceKey = String(getOption(argv, 'source') || '')
    .trim()
    .toLowerCase()
  if (!sourceKey) {
    printHelp()
    process.exitCode = 1
    return
  }

  const registry = loadRegistry()
  const modelFilter = normalizeList(getOption(argv, 'only-models'))
  const hostContains = String(getOption(argv, 'host-contains') || '')
    .trim()
    .toLowerCase()
  const targets = collectTargets(registry, sourceKey, modelFilter, hostContains)

  if (targets.length === 0) {
    console.log(`No ${sourceKey}-backed model sources found.`)
    return
  }

  console.log(`Found ${targets.length} ${sourceKey}-backed model source(s).`)

  const delayMs = Number.parseInt(getOption(argv, 'delay-ms'), 10) || 0
  const keepHistory = !(
    getOption(argv, 'keep-history') === false ||
    getOption(argv, 'keep-history') === 'false'
  )

  for (let index = 0; index < targets.length; index += 1) {
    const target = targets[index]
    console.log(
      `\n[${index + 1}/${targets.length}] ${target.modelName} -> ${target.url}`
    )

    const hoghaulArgs = [
      'hoghaul/hoghaul.js',
      target.url,
      '--model',
      target.modelName,
      '--post-concurrency',
      String(getOption(argv, 'post-concurrency')),
      '--image-concurrency',
      String(getOption(argv, 'image-concurrency')),
      '--video-concurrency',
      String(getOption(argv, 'video-concurrency')),
    ]

    if (getOption(argv, 'pages')) {
      hoghaulArgs.push('--pages', String(getOption(argv, 'pages')))
    }
    if (getOption(argv, 'max-posts')) {
      hoghaulArgs.push('--max-posts', String(getOption(argv, 'max-posts')))
    }
    if (getOption(argv, 'max-files')) {
      hoghaulArgs.push('--max-files', String(getOption(argv, 'max-files')))
    }
    if (keepHistory) hoghaulArgs.push('--keep-history')
    if (isTruthy(getOption(argv, 'skip-nas-sync'))) {
      hoghaulArgs.push('--skip-nas-sync')
    }
    if (isTruthy(getOption(argv, 'dry-run'))) {
      hoghaulArgs.push('--dry-run')
    }

    const result = spawnSync(process.execPath, hoghaulArgs, {
      cwd: rootDir,
      stdio: 'inherit',
    })

    if (result.status !== 0) {
      process.exitCode = result.status || 1
      return
    }

    if (delayMs > 0 && index < targets.length - 1) {
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delayMs)
    }
  }
}

main()

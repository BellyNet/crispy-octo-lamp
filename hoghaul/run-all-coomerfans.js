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

function isCoomerFansUrl(value) {
  return /^https?:\/\/(?:www\.)?coomerfans\.com\//i.test(String(value || ''))
}

function loadRegistry() {
  return JSON.parse(fs.readFileSync(registryPath, 'utf8'))
}

function collectTargets(registry, modelFilter) {
  const targets = []

  for (const [modelName, entry] of Object.entries(registry || {})) {
    if (modelFilter && !modelFilter.has(modelName)) continue
    const sources = Array.isArray(entry?.sources?.coomer)
      ? entry.sources.coomer
      : []

    for (const source of sources) {
      const url = String(source?.url || '').trim()
      if (!isCoomerFansUrl(url)) continue
      targets.push({
        modelName,
        url,
      })
    }
  }

  return targets
}

function main() {
  const argv = minimist(process.argv.slice(2), {
    string: [
      'only-models',
      'pages',
      'max-posts',
      'max-files',
      'post-concurrency',
      'image-concurrency',
      'video-concurrency',
      'delay-ms',
    ],
    boolean: ['dry-run', 'skip-nas-sync', 'keep-history'],
    default: {
      'keep-history': true,
      'post-concurrency': '8',
      'image-concurrency': '6',
      'video-concurrency': '6',
      'delay-ms': '0',
    },
  })

  const getOption = (name) => {
    const directValue = argv[name]
    if (directValue !== undefined) return directValue
    const envName = `npm_config_${String(name).replace(/-/g, '_')}`
    return process.env[envName]
  }

  const registry = loadRegistry()
  const modelFilter = normalizeList(getOption('only-models'))
  const targets = collectTargets(registry, modelFilter)

  if (targets.length === 0) {
    console.log('No coomerfans-backed coomer sources found.')
    return
  }

  console.log(`Found ${targets.length} coomerfans-backed model(s).`)

  const delayMs = Number.parseInt(getOption('delay-ms'), 10) || 0

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
      String(getOption('post-concurrency')),
      '--image-concurrency',
      String(getOption('image-concurrency')),
      '--video-concurrency',
      String(getOption('video-concurrency')),
    ]

    if (getOption('pages'))
      hoghaulArgs.push('--pages', String(getOption('pages')))
    if (getOption('max-posts')) {
      hoghaulArgs.push('--max-posts', String(getOption('max-posts')))
    }
    if (getOption('max-files')) {
      hoghaulArgs.push('--max-files', String(getOption('max-files')))
    }
    if (getOption('keep-history')) hoghaulArgs.push('--keep-history')
    if (getOption('skip-nas-sync')) hoghaulArgs.push('--skip-nas-sync')
    if (getOption('dry-run')) hoghaulArgs.push('--dry-run')

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

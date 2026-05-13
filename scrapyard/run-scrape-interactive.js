'use strict'

const path = require('path')
const readline = require('readline')
const { spawnSync } = require('child_process')

const {
  loadModelRegistry,
  findCanonicalModelName,
  sanitize,
} = require('./modelRegistry')
const { parseSourceUrl } = require('./sourceRouter')
const { runScrape } = require('./scraperRunner')

const rootDir = path.join(__dirname, '..')
const registryPath = path.join(rootDir, 'model_aliases.json')

function ask(rl, prompt) {
  return new Promise((resolve) => rl.question(prompt, resolve))
}

function appendOption(args, flag, value) {
  if (!value) return
  args.push(flag, value)
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

async function askBatchOptions(rl, { includeStartFrom, includeHoghaul }) {
  const onlyModels = (
    await ask(rl, 'Canonical model filter (comma-separated, blank for all): ')
  ).trim()
  const startFrom = includeStartFrom
    ? (await ask(rl, 'Start from model (blank for start of queue): ')).trim()
    : ''
  const skipNasSyncAnswer = (
    await ask(rl, 'Skip NAS sync for this run? [y/N]: ')
  )
    .trim()
    .toLowerCase()
  const skipNasSync = skipNasSyncAnswer === 'y' || skipNasSyncAnswer === 'yes'

  const options = {
    onlyModels,
    startFrom,
    skipNasSync,
  }

  if (includeHoghaul) {
    options.pages = (
      await ask(rl, 'Pages limit (blank for all pages, accepts 1 or 1-3): ')
    ).trim()
    options.maxPosts = (
      await ask(rl, 'Max posts per source (blank for all): ')
    ).trim()
    options.maxFiles = (
      await ask(rl, 'Max files per source (blank for all): ')
    ).trim()
    options.postConcurrency = (
      await ask(rl, 'Post concurrency (blank for default): ')
    ).trim()
    options.imageConcurrency = (
      await ask(rl, 'Image concurrency (blank for default): ')
    ).trim()
    options.videoConcurrency = (
      await ask(rl, 'Video concurrency (blank for default): ')
    ).trim()
  }

  return options
}

function buildHoghaulBatchArgs(sourceKey, options) {
  const args = ['--source', sourceKey]
  appendOption(args, '--only-models', options.onlyModels)
  appendOption(args, '--pages', options.pages)
  appendOption(args, '--max-posts', options.maxPosts)
  appendOption(args, '--max-files', options.maxFiles)
  appendOption(args, '--post-concurrency', options.postConcurrency)
  appendOption(args, '--image-concurrency', options.imageConcurrency)
  appendOption(args, '--video-concurrency', options.videoConcurrency)
  if (options.skipNasSync) args.push('--skip-nas-sync')
  return args
}

function buildStufferArgs(options) {
  const args = []
  appendOption(args, '--models', options.onlyModels)
  appendOption(args, '--start-from', options.startFrom)
  if (options.skipNasSync) args.push('--skip-nas-sync')
  return args
}

async function runSingleUrlFlow(rl) {
  const rawUrl = (await ask(rl, 'Paste source URL: ')).trim()
  const parsed = parseSourceUrl(rawUrl)
  if (!parsed) {
    console.log(
      'Could not recognize that URL as StufferDB, Reddit, Coomer, CoomerFans, or Kemono.'
    )
    return
  }

  const registry = loadModelRegistry(registryPath)
  const suggestedModel = parsed.rawName
    ? findCanonicalModelName(registry, sanitize(parsed.rawName))
    : null

  console.log('')
  console.log(`Detected source: ${parsed.sourceType}`)
  console.log(`Scraper: ${parsed.scraper}`)
  if (parsed.rawName) console.log(`Detected name: ${parsed.rawName}`)
  if (suggestedModel)
    console.log(`Suggested canonical model: ${suggestedModel}`)

  const overridePrompt = suggestedModel
    ? `Canonical model override (Enter to use ${suggestedModel}, type another name for a different/existing model, or "-" for scraper auto-detect): `
    : 'Canonical model override (blank for scraper auto-detect): '
  const overrideAnswer = (await ask(rl, overridePrompt)).trim()

  let canonicalOverride = ''
  if (overrideAnswer === '' && suggestedModel) {
    canonicalOverride = suggestedModel
  } else if (overrideAnswer !== '-') {
    canonicalOverride = overrideAnswer
  }

  const runOptions = {}
  if (canonicalOverride) {
    runOptions.model = canonicalOverride
  } else if (overrideAnswer === '-') {
    runOptions['no-model-infer'] = true
  }

  if (parsed.scraper === 'hoghaul') {
    runOptions.pages = (
      await ask(rl, 'Pages limit for this Hoghaul run (blank for all): ')
    ).trim()
    runOptions['video-concurrency'] = (
      await ask(rl, 'Video concurrency (blank for default): ')
    ).trim()
    runOptions['image-concurrency'] = (
      await ask(rl, 'Image concurrency (blank for default): ')
    ).trim()
    runOptions['post-concurrency'] = (
      await ask(rl, 'Post concurrency (blank for default): ')
    ).trim()
  }

  const status = runScrape(parsed.url, runOptions)
  if (status !== 0) {
    console.log(`Scraper exited with status ${status}.`)
  }
}

async function main() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  })

  try {
    while (true) {
      console.log('')
      console.log('Scrape Launcher')
      console.log('1. Update all models (StufferDB + Coomer + Kemono)')
      console.log('2. Update all StufferDB models')
      console.log('3. Update all Coomer models')
      console.log('4. Update all Kemono models')
      console.log('5. Paste one source URL and run it')
      console.log('6. Quit')

      const choice = (await ask(rl, '\nPick an option: ')).trim()

      if (choice === '6' || /^q(?:uit)?$/i.test(choice)) {
        console.log('Done.')
        break
      }

      if (choice === '1') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: true,
          includeHoghaul: true,
        })
        const args = []
        appendOption(args, '--only-models', options.onlyModels)
        appendOption(args, '--start-from', options.startFrom)
        appendOption(args, '--pages', options.pages)
        appendOption(args, '--max-posts', options.maxPosts)
        appendOption(args, '--max-files', options.maxFiles)
        appendOption(args, '--post-concurrency', options.postConcurrency)
        appendOption(args, '--image-concurrency', options.imageConcurrency)
        appendOption(args, '--video-concurrency', options.videoConcurrency)
        if (options.skipNasSync) args.push('--skip-nas-sync')
        runNodeScript(path.join('scrapyard', 'run-all-source-updates.js'), args)
        continue
      }

      if (choice === '2') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: true,
          includeHoghaul: false,
        })
        runNodeScript(
          path.join('milkmaid', 'update-stufferdb-models.js'),
          buildStufferArgs(options)
        )
        continue
      }

      if (choice === '3') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: false,
          includeHoghaul: true,
        })
        runNodeScript(
          path.join('hoghaul', 'run-source-batch.js'),
          buildHoghaulBatchArgs('coomer', options)
        )
        continue
      }

      if (choice === '4') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: false,
          includeHoghaul: true,
        })
        runNodeScript(
          path.join('hoghaul', 'run-source-batch.js'),
          buildHoghaulBatchArgs('kemono', options)
        )
        continue
      }

      if (choice === '5') {
        await runSingleUrlFlow(rl)
        continue
      }

      console.log('Please choose 1-6.')
    }
  } finally {
    rl.close()
  }
}

main().catch((err) => {
  console.error(`Fatal launcher error: ${err.stack || err.message}`)
  process.exitCode = 1
})

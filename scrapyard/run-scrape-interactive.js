'use strict'

const readline = require('readline')

const {
  loadModelRegistry,
  findCanonicalModelName,
  sanitize,
} = require('./modelRegistry')
const { parseSourceUrl } = require('./sourceRouter')
const {
  runAllSourceUpdates,
  runRepair,
  runScrape,
  runSourceBatch,
  runStufferDbBatch,
  runSync,
} = require('./scraperRunner')

const registryPath = require('./scraperRunner').registryPath

function ask(rl, prompt) {
  return new Promise((resolve) => rl.question(prompt, resolve))
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

function toRunnerBatchOptions(options) {
  return {
    'only-models': options.onlyModels,
    models: options.onlyModels,
    'start-from': options.startFrom,
    pages: options.pages,
    'max-posts': options.maxPosts,
    'max-files': options.maxFiles,
    'post-concurrency': options.postConcurrency,
    'image-concurrency': options.imageConcurrency,
    'video-concurrency': options.videoConcurrency,
    'skip-nas-sync': options.skipNasSync,
  }
}

function findModelByAlias(registry, rawModel) {
  const cleaned = sanitize(rawModel)
  if (!cleaned) return null
  const canonical = findCanonicalModelName(registry, cleaned)
  if (canonical) return canonical
  return registry?.[cleaned] ? cleaned : null
}

function getSourceLabel(sourceKey, url) {
  const parsed = parseSourceUrl(url)
  if (!parsed) return sourceKey
  if (parsed.sourceType === 'coomerfans') return 'coomerfans'
  return parsed.sourceType || sourceKey
}

function collectModelSources(entry) {
  const targets = []
  const sources =
    entry?.sources && typeof entry.sources === 'object' ? entry.sources : {}

  for (const [sourceKey, sourceList] of Object.entries(sources)) {
    for (const source of Array.isArray(sourceList) ? sourceList : []) {
      const url = String(source?.url || '').trim()
      if (!url) continue
      targets.push({
        sourceKey,
        url,
        label: getSourceLabel(sourceKey, url),
      })
    }
  }

  return targets
}

function selectModelSources(targets, answer) {
  const cleaned = String(answer || 'all')
    .trim()
    .toLowerCase()
  if (!cleaned || cleaned === 'all') return targets

  const index = Number.parseInt(cleaned, 10)
  if (Number.isFinite(index) && index >= 1 && index <= targets.length) {
    return [targets[index - 1]]
  }

  const filters = new Set(cleaned.split(',').map((part) => part.trim()))
  return targets.filter((target) => filters.has(target.label))
}

async function askHoghaulOptions(rl) {
  return {
    pages: (
      await ask(rl, 'Pages limit for Hoghaul sources (blank for all): ')
    ).trim(),
    'max-posts': (
      await ask(rl, 'Max posts per Hoghaul source (blank for all): ')
    ).trim(),
    'max-files': (
      await ask(rl, 'Max files per Hoghaul source (blank for all): ')
    ).trim(),
    'post-concurrency': (
      await ask(rl, 'Post concurrency (blank for default): ')
    ).trim(),
    'image-concurrency': (
      await ask(rl, 'Image concurrency (blank for default): ')
    ).trim(),
    'video-concurrency': (
      await ask(rl, 'Video concurrency (blank for default): ')
    ).trim(),
  }
}

async function runModelAliasFlow(rl) {
  const rawModel = (await ask(rl, 'Model or alias: ')).trim()
  const registry = loadModelRegistry(registryPath)
  const canonicalModel = findModelByAlias(registry, rawModel)

  if (!canonicalModel) {
    console.log(`No registry entry found for ${rawModel}.`)
    return
  }

  const targets = collectModelSources(registry[canonicalModel])
  if (!targets.length) {
    console.log(`${canonicalModel} has no saved sources in the registry.`)
    return
  }

  console.log('')
  console.log(`Sources for ${canonicalModel}:`)
  targets.forEach((target, index) => {
    console.log(`${index + 1}. ${target.label}: ${target.url}`)
  })

  const sourceAnswer = await ask(
    rl,
    'Source to run: all, stufferdb, reddit, coomer, coomerfans, kemono, or number [all]: '
  )
  const selectedTargets = selectModelSources(targets, sourceAnswer)
  if (!selectedTargets.length) {
    console.log('No sources matched that selection.')
    return
  }

  const dryRunAnswer = (await ask(rl, 'Dry run? [y/N]: ')).trim().toLowerCase()
  const skipNasSyncAnswer = (
    await ask(rl, 'Skip NAS sync for this run? [y/N]: ')
  )
    .trim()
    .toLowerCase()

  const sharedOptions = {
    model: canonicalModel,
    'dry-run': dryRunAnswer === 'y' || dryRunAnswer === 'yes',
    'skip-nas-sync': skipNasSyncAnswer === 'y' || skipNasSyncAnswer === 'yes',
  }
  const includesHoghaul = selectedTargets.some(
    (target) => parseSourceUrl(target.url)?.scraper === 'hoghaul'
  )
  const hoghaulOptions = includesHoghaul ? await askHoghaulOptions(rl) : {}

  for (let index = 0; index < selectedTargets.length; index += 1) {
    const target = selectedTargets[index]
    const parsed = parseSourceUrl(target.url)
    const runOptions = {
      ...sharedOptions,
      ...(parsed?.scraper === 'hoghaul' ? hoghaulOptions : {}),
    }

    console.log('')
    console.log(
      `[${index + 1}/${selectedTargets.length}] ${canonicalModel} -> ${target.label}`
    )
    const status = await runScrape(target.url, runOptions)
    if (status !== 0) {
      console.log(`Scraper exited with status ${status}.`)
      return
    }
  }
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

  const status = await runScrape(parsed.url, runOptions)
  if (status !== 0) {
    console.log(`Scraper exited with status ${status}.`)
  }
}

async function runRepairFlow(rl) {
  const model = (await ask(rl, 'Model to repair (blank for all): ')).trim()
  const models = model
    ? ''
    : (await ask(rl, 'Models filter (comma-separated, blank for all): ')).trim()
  const startFrom = model
    ? ''
    : (await ask(rl, 'Start from model (blank for start of queue): ')).trim()
  const scrapeAnswer = (
    await ask(rl, 'Re-scrape sources during repair? [y/N]: ')
  )
    .trim()
    .toLowerCase()
  const skipNasSyncAnswer = (
    await ask(rl, 'Skip NAS sync after repair? [y/N]: ')
  )
    .trim()
    .toLowerCase()

  await runRepair({
    model,
    models,
    'start-from': startFrom,
    scrape: scrapeAnswer === 'y' || scrapeAnswer === 'yes',
    'skip-nas-sync': skipNasSyncAnswer === 'y' || skipNasSyncAnswer === 'yes',
  })
}

async function runSyncFlow(rl) {
  const mode = (await ask(rl, 'Sync mode: push, pull, or model? [push]: '))
    .trim()
    .toLowerCase()
  const options = {}
  if (!mode || mode === 'push') {
    options.push = true
    const cleanupAnswer = (
      await ask(rl, 'Remove mirrored local MP4s after push? [y/N]: ')
    )
      .trim()
      .toLowerCase()
    if (cleanupAnswer === 'y' || cleanupAnswer === 'yes') {
      options['cleanup-mp4'] = 'true'
    }
  } else if (mode === 'pull') {
    options.pull = true
  } else {
    options.model =
      mode === 'model' ? (await ask(rl, 'Model to sync: ')).trim() : mode
  }

  await runSync(options)
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
      console.log('5. Run a model/alias from registry')
      console.log('6. Paste one source URL and run it')
      console.log('7. Repair models')
      console.log('8. Sync dataset/NAS')
      console.log('9. Quit')

      const choice = (await ask(rl, '\nPick an option: ')).trim()

      if (choice === '9' || /^q(?:uit)?$/i.test(choice)) {
        console.log('Done.')
        break
      }

      if (choice === '1') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: true,
          includeHoghaul: true,
        })
        await runAllSourceUpdates(toRunnerBatchOptions(options))
        continue
      }

      if (choice === '2') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: true,
          includeHoghaul: false,
        })
        await runStufferDbBatch(toRunnerBatchOptions(options))
        continue
      }

      if (choice === '3') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: false,
          includeHoghaul: true,
        })
        await runSourceBatch('coomer', toRunnerBatchOptions(options))
        continue
      }

      if (choice === '4') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: false,
          includeHoghaul: true,
        })
        await runSourceBatch('kemono', toRunnerBatchOptions(options))
        continue
      }

      if (choice === '5') {
        await runModelAliasFlow(rl)
        continue
      }

      if (choice === '6') {
        await runSingleUrlFlow(rl)
        continue
      }

      if (choice === '7') {
        await runRepairFlow(rl)
        continue
      }

      if (choice === '8') {
        await runSyncFlow(rl)
        continue
      }

      console.log('Please choose 1-9.')
    }
  } finally {
    rl.close()
  }
}

module.exports = {
  main,
}

if (require.main === module) {
  main().catch((err) => {
    console.error(`Fatal launcher error: ${err.stack || err.message}`)
    process.exitCode = 1
  })
}

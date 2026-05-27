'use strict'

const readline = require('readline')

const {
  loadModelRegistry,
  findCanonicalModelName,
  resolveAndTrackModel,
  resolveAndTrackSourceModel,
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

function registerParsedSourceForModel(
  parsed,
  canonicalModel,
  registryFile = registryPath
) {
  const rawName = parsed.rawName || canonicalModel
  if (parsed.scraper === 'milkmaid') {
    return resolveAndTrackModel(
      registryFile,
      rawName,
      parsed.sourceType,
      parsed.url,
      canonicalModel,
      { unknownName: sanitize(canonicalModel) || 'unknown_model' }
    )
  }

  return resolveAndTrackSourceModel(
    registryFile,
    rawName,
    {
      site: parsed.site || parsed.sourceType,
      service: parsed.service,
      userId: parsed.userId,
      username: parsed.username,
      inputUrl: parsed.url,
    },
    canonicalModel,
    { unknownName: sanitize(canonicalModel) || 'unknown_model' }
  )
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

  if (includeHoghaul) options.includeSessionHoghaul = true

  return options
}

const HOGHAUL_SESSION_OPTIONS = [
  ['pages', 'Pages limit'],
  ['maxPosts', 'Max posts per source'],
  ['maxFiles', 'Max files per source'],
  ['postConcurrency', 'Post concurrency'],
  ['imageConcurrency', 'Image concurrency'],
  ['videoConcurrency', 'Video concurrency'],
]

function createSessionOptions() {
  return {
    hoghaul: Object.fromEntries(
      HOGHAUL_SESSION_OPTIONS.map(([name]) => [name, ''])
    ),
  }
}

function toHoghaulRunnerOptions(sessionOptions = {}) {
  const hoghaul = sessionOptions.hoghaul || {}
  return {
    pages: hoghaul.pages,
    'max-posts': hoghaul.maxPosts,
    'max-files': hoghaul.maxFiles,
    'post-concurrency': hoghaul.postConcurrency,
    'image-concurrency': hoghaul.imageConcurrency,
    'video-concurrency': hoghaul.videoConcurrency,
  }
}

function pruneBlankOptions(options) {
  return Object.fromEntries(
    Object.entries(options || {}).filter(([, value]) => {
      return value !== undefined && value !== null && value !== ''
    })
  )
}

function formatHoghaulSessionOptions(sessionOptions = {}) {
  const hoghaul = sessionOptions.hoghaul || {}
  const parts = HOGHAUL_SESSION_OPTIONS.map(([name, label]) => {
    const value = String(hoghaul[name] || '').trim()
    return `${label}: ${value || 'default'}`
  })
  return parts.join(' | ')
}

async function updateSessionOptionsFlow(rl, sessionOptions) {
  console.log('')
  console.log('Current Hoghaul session options:')
  console.log(formatHoghaulSessionOptions(sessionOptions))
  console.log('')
  console.log(
    'Leave a field blank to use the default for the rest of this menu session.'
  )

  for (const [name, label] of HOGHAUL_SESSION_OPTIONS) {
    sessionOptions.hoghaul[name] = (
      await ask(rl, `${label} [${sessionOptions.hoghaul[name] || 'default'}]: `)
    ).trim()
  }
}

function toRunnerBatchOptions(options, sessionOptions = {}) {
  const sessionHoghaulOptions = options.includeSessionHoghaul
    ? toHoghaulRunnerOptions(sessionOptions)
    : {}
  return {
    'only-models': options.onlyModels,
    models: options.onlyModels,
    'start-from': options.startFrom,
    ...sessionHoghaulOptions,
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

async function runModelAliasFlow(rl, sessionOptions) {
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
  const hoghaulOptions = pruneBlankOptions(
    toHoghaulRunnerOptions(sessionOptions)
  )

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

async function runSingleUrlFlow(rl, sessionOptions) {
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
    ? `Known model to save/run under (Enter for ${suggestedModel}, another existing model, or "-" for scraper auto-detect): `
    : 'Known model to save/run under (blank for scraper auto-detect): '
  let canonicalOverride = ''
  let shouldSaveSource = false
  let noModelInfer = false

  while (true) {
    const overrideAnswer = (await ask(rl, overridePrompt)).trim()
    if (overrideAnswer === '-') {
      noModelInfer = true
      break
    }

    if (overrideAnswer === '' && suggestedModel) {
      canonicalOverride = suggestedModel
      shouldSaveSource = true
      break
    }

    if (overrideAnswer === '') break

    const existingModel = findCanonicalModelName(
      registry,
      sanitize(overrideAnswer)
    )
    if (existingModel) {
      canonicalOverride = existingModel
      shouldSaveSource = true
      break
    }

    const createAnswer = (
      await ask(
        rl,
        `No existing model found for "${overrideAnswer}". Create it and save this source there? [y/N]: `
      )
    )
      .trim()
      .toLowerCase()
    if (createAnswer === 'y' || createAnswer === 'yes') {
      canonicalOverride = sanitize(overrideAnswer)
      shouldSaveSource = true
      break
    }

    console.log('Choose an existing model, create this one, or enter "-".')
  }

  if (shouldSaveSource && canonicalOverride) {
    const savedModel = registerParsedSourceForModel(parsed, canonicalOverride)
    canonicalOverride = savedModel
    console.log(`Saved source in model_aliases.json under ${savedModel}.`)
  }

  const runOptions = {}
  if (canonicalOverride) {
    runOptions.model = canonicalOverride
  } else if (noModelInfer) {
    runOptions['no-model-infer'] = true
  }

  if (parsed.scraper === 'hoghaul') {
    Object.assign(
      runOptions,
      pruneBlankOptions(toHoghaulRunnerOptions(sessionOptions))
    )
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
  rl.on('SIGINT', () => {
    process.emit('SIGINT')
  })
  const sessionOptions = createSessionOptions()

  try {
    while (true) {
      console.log('')
      console.log('Scrape Launcher')
      console.log(
        '1. Update all models (model-by-model: Reddit + Kemono + Coomer/CoomerFans + StufferDB)'
      )
      console.log('2. Update all StufferDB models')
      console.log('3. Update all Coomer models')
      console.log('4. Update all Kemono models')
      console.log('5. Set session Hoghaul options')
      console.log('6. Run a model/alias from registry')
      console.log('7. Paste one source URL and run it')
      console.log('8. Repair models')
      console.log('9. Sync dataset/NAS')
      console.log('10. Quit')
      console.log(
        `Hoghaul session: ${formatHoghaulSessionOptions(sessionOptions)}`
      )

      const choice = (await ask(rl, '\nPick an option: ')).trim()

      if (choice === '10' || /^q(?:uit)?$/i.test(choice)) {
        console.log('Done.')
        break
      }

      if (choice === '1') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: true,
          includeHoghaul: true,
        })
        await runAllSourceUpdates(toRunnerBatchOptions(options, sessionOptions))
        continue
      }

      if (choice === '2') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: true,
          includeHoghaul: false,
        })
        await runStufferDbBatch(toRunnerBatchOptions(options, sessionOptions))
        continue
      }

      if (choice === '3') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: false,
          includeHoghaul: true,
        })
        await runSourceBatch(
          'coomer',
          toRunnerBatchOptions(options, sessionOptions)
        )
        continue
      }

      if (choice === '4') {
        const options = await askBatchOptions(rl, {
          includeStartFrom: false,
          includeHoghaul: true,
        })
        await runSourceBatch(
          'kemono',
          toRunnerBatchOptions(options, sessionOptions)
        )
        continue
      }

      if (choice === '5') {
        await updateSessionOptionsFlow(rl, sessionOptions)
        continue
      }

      if (choice === '6') {
        await runModelAliasFlow(rl, sessionOptions)
        continue
      }

      if (choice === '7') {
        await runSingleUrlFlow(rl, sessionOptions)
        continue
      }

      if (choice === '8') {
        await runRepairFlow(rl)
        continue
      }

      if (choice === '9') {
        await runSyncFlow(rl)
        continue
      }

      console.log('Please choose 1-10.')
    }
  } finally {
    rl.close()
  }
}

module.exports = {
  main,
  registerParsedSourceForModel,
}

if (require.main === module) {
  main().catch((err) => {
    console.error(`Fatal launcher error: ${err.stack || err.message}`)
    process.exitCode = 1
  })
}

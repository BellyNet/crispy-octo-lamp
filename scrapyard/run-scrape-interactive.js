'use strict'

const fs = require('fs')
const path = require('path')
const readline = require('readline')

const { createDatasetPaths } = require('./datasetPaths')
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
const datasetPaths = createDatasetPaths({
  rootDir: path.join(__dirname, '..'),
  repairCanUseNasMirror: true,
})

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

function parseCommaList(value) {
  return String(value || '')
    .split(',')
    .map((part) => sanitize(part.trim()))
    .filter(Boolean)
}

function walkFiles(rootDir, predicate, results = []) {
  if (!rootDir || !fs.existsSync(rootDir)) return results
  for (const entry of fs.readdirSync(rootDir, { withFileTypes: true })) {
    const fullPath = path.join(rootDir, entry.name)
    if (entry.isDirectory()) {
      walkFiles(fullPath, predicate, results)
    } else if (!predicate || predicate(fullPath)) {
      results.push(fullPath)
    }
  }
  return results
}

function parseLargeVideoBytes(value) {
  if (Number.isFinite(value) && value > 0) return value
  const match = String(value || '').match(/Received\s+(\d+)/i)
  return match ? Number.parseInt(match[1], 10) : null
}

function formatBytes(bytes) {
  const value = Number(bytes) || 0
  if (value <= 0) return 'unknown size'
  if (value < 1024) return `${value} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let size = value / 1024
  let unit = units.shift()
  while (size >= 1024 && units.length) {
    size /= 1024
    unit = units.shift()
  }
  return `${size.toFixed(size >= 10 ? 1 : 2)} ${unit}`
}

function eventLooksLikeLargeVideoFailure(event) {
  if (event?.type === 'skip_oversized_video') return true
  const error = String(event?.error || '')
  return (
    event?.type === 'lazy_video_error' &&
    /length.*out of range|Received\s+\d+/i.test(error)
  )
}

function eventLooksHandledVideo(event) {
  return [
    'saved_lazy_video',
    'duplicate_bitwise',
    'duplicate_visual',
    'skip_seen_media',
    'skip_lazy_existing',
  ].includes(event?.type)
}

function collectOversizedVideoTargets(options = {}) {
  const modelFilter = new Set(parseCommaList(options.models || ''))
  const datasetDir = options.datasetDir || datasetPaths.datasetDir
  const logFiles = walkFiles(
    datasetDir,
    (filePath) =>
      /[\\/]log[\\/]/i.test(filePath) &&
      /hoghaul-run-.*\.jsonl$/i.test(path.basename(filePath))
  )
  const events = []
  const handledVideos = new Set()
  const targets = new Map()

  for (const logPath of logFiles) {
    let runModelName = ''
    let inputUrl = ''
    const lines = fs.readFileSync(logPath, 'utf8').split(/\r?\n/)
    for (const line of lines) {
      if (!line.trim()) continue
      let event
      try {
        event = JSON.parse(line)
      } catch {
        continue
      }
      if (event.type === 'run_started') {
        runModelName = sanitize(event.modelName || '')
        inputUrl = String(event.inputUrl || '').trim()
        continue
      }

      const modelName = sanitize(event.modelName || runModelName)
      const sourceUrl = String(event.inputUrl || inputUrl || '').trim()
      const filename = String(event.filename || '').trim()
      const withContext = {
        ...event,
        modelName,
        inputUrl: sourceUrl,
        filename,
      }
      events.push(withContext)
      if (eventLooksHandledVideo(withContext) && modelName && filename) {
        handledVideos.add(`${modelName}\n${filename}`)
      }
    }
  }

  for (const event of events) {
    if (!eventLooksLikeLargeVideoFailure(event)) continue

    const modelName = sanitize(event.modelName || '')
    const sourceUrl = String(event.inputUrl || '').trim()
    const filename = String(event.filename || '').trim()
    if (filename && handledVideos.has(`${modelName}\n${filename}`)) continue
    if (!modelName || !sourceUrl) continue
    if (modelFilter.size && !modelFilter.has(modelName)) continue

    const key = `${modelName}\n${sourceUrl}`
    const previous = targets.get(key) || {
      modelName,
      url: sourceUrl,
      count: 0,
      largestBytes: 0,
      latestAt: '',
      sampleFiles: [],
    }
    const bytes =
      parseLargeVideoBytes(event.contentLength) ||
      parseLargeVideoBytes(event.downloadedBytes) ||
      parseLargeVideoBytes(event.error) ||
      0
    previous.count += 1
    previous.largestBytes = Math.max(previous.largestBytes, bytes)
    previous.latestAt =
      !previous.latestAt || String(event.at || '') > previous.latestAt
        ? String(event.at || '')
        : previous.latestAt
    if (filename && previous.sampleFiles.length < 3) {
      previous.sampleFiles.push(filename)
    }
    targets.set(key, previous)
  }

  return Array.from(targets.values()).sort((a, b) => {
    const modelCompare = a.modelName.localeCompare(b.modelName)
    if (modelCompare) return modelCompare
    return a.url.localeCompare(b.url)
  })
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

async function runOversizedVideoFlow(rl) {
  const models = (
    await ask(
      rl,
      'Model filter for oversized retries (comma-separated, blank for all): '
    )
  ).trim()
  const skipNasSyncAnswer = (
    await ask(rl, 'Skip NAS sync after each oversized retry? [y/N]: ')
  )
    .trim()
    .toLowerCase()
  const videoConcurrencyAnswer = (
    await ask(rl, 'Video concurrency for oversized retries [1]: ')
  ).trim()

  const targets = collectOversizedVideoTargets({ models })
  if (!targets.length) {
    console.log('No oversized Hoghaul videos found in saved run logs.')
    return
  }

  console.log('')
  console.log(
    `Found ${targets.length} Hoghaul source(s) with oversized videos:`
  )
  targets.forEach((target, index) => {
    const samples = target.sampleFiles.length
      ? ` | samples: ${target.sampleFiles.join(', ')}`
      : ''
    console.log(
      `${index + 1}. ${target.modelName}: ${target.count} oversized | largest ${formatBytes(target.largestBytes)}${samples}`
    )
    console.log(`   ${target.url}`)
  })

  const confirm = (
    await ask(
      rl,
      `Run ${targets.length} source(s) with the oversized guard disabled? [y/N]: `
    )
  )
    .trim()
    .toLowerCase()
  if (confirm !== 'y' && confirm !== 'yes') {
    console.log('Oversized retry cancelled.')
    return
  }

  const sharedOptions = {
    'download-oversized': true,
    'video-concurrency': videoConcurrencyAnswer || '1',
    'skip-nas-sync': skipNasSyncAnswer === 'y' || skipNasSyncAnswer === 'yes',
    'keep-history': true,
  }

  for (let index = 0; index < targets.length; index += 1) {
    const target = targets[index]
    console.log('')
    console.log(
      `[${index + 1}/${targets.length}] Oversized retry: ${target.modelName}`
    )
    console.log(target.url)
    const status = await runScrape(target.url, {
      ...sharedOptions,
      model: target.modelName,
    })
    if (status !== 0) {
      console.log(`Oversized retry stopped after scraper status ${status}.`)
      return
    }
  }
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
      console.log('10. Download oversized Hoghaul videos')
      console.log('11. Quit')
      console.log(
        `Hoghaul session: ${formatHoghaulSessionOptions(sessionOptions)}`
      )

      const choice = (await ask(rl, '\nPick an option: ')).trim()

      if (choice === '11' || /^q(?:uit)?$/i.test(choice)) {
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

      if (choice === '10') {
        await runOversizedVideoFlow(rl)
        continue
      }

      console.log('Please choose 1-11.')
    }
  } finally {
    rl.close()
  }
}

module.exports = {
  collectOversizedVideoTargets,
  main,
  registerParsedSourceForModel,
}

if (require.main === module) {
  main().catch((err) => {
    console.error(`Fatal launcher error: ${err.stack || err.message}`)
    process.exitCode = 1
  })
}

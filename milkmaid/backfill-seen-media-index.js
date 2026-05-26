'use strict'

const fs = require('fs')
const path = require('path')
const os = require('os')
const minimist = require('minimist')
const { isLikelyMediaUrl } = require('../scrapyard/mediaEntries')
const { createDatasetPaths } = require('../scrapyard/datasetPaths')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  string: ['model', 'models', 'dataset-root', 'start-from'],
  boolean: ['dry-run'],
  default: {
    'dry-run': false,
  },
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const datasetRoot = path.resolve(
  String(
    argv['dataset-root'] ||
      path.join(
        process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
        '.slopvault',
        'dataset'
      )
  )
)
const singleModel = argv.model ? String(argv.model).trim() : null
const explicitModels = String(argv.models || '')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean)
const startFrom = argv['start-from'] ? String(argv['start-from']).trim() : null
const dryRun = Boolean(argv['dry-run'])
const datasetPaths = createDatasetPaths({
  datasetDir: datasetRoot,
  rootDir: path.join(__dirname, '..'),
})

const SAVE_EVENT_TYPES = new Set([
  'saved_image',
  'saved_gif',
  'saved_lazy_video',
  'skip_existing_image',
  'skip_existing_gif',
  'skip_existing_video',
  'skip_lazy_existing',
  'skip_seen_media',
])

main().catch((err) => {
  console.error(`Fatal seen-media backfill error: ${err.stack || err.message}`)
  process.exitCode = 1
})

function printHelp() {
  console.log(`Usage: node milkmaid/backfill-seen-media-index.js [options]

Options:
  --model <name>         Backfill one model only.
  --models <a,b,c>       Backfill a comma-separated set of models.
  --start-from <name>    Start from this canonical model name.
  --dataset-root <path>  Override dataset root.
  --dry-run              Report changes without writing index files.
  -h, --help             Show help.

Notes:
  This scans historical Milkmaid and Hoghaul JSONL run logs and rebuilds
  log/milkmaid-seen-media-index.json for each selected model. NAS-backed
  MP4 paths count as active.
`)
}

async function main() {
  const models = selectModels(listDatasetModels())
  const results = []

  console.log(
    `Seen-media backfill: ${models.length} model(s) selected from ${listDatasetModels().length} dataset folders`
  )

  for (let index = 0; index < models.length; index += 1) {
    const modelName = models[index]
    console.log('')
    console.log(`[${index + 1}/${models.length}] Backfilling ${modelName}`)
    const result = backfillModel(modelName)
    results.push(result)
    console.log(
      `  logs=${result.logFiles} pageUrls=${result.pageUrlCount} mediaUrls=${result.mediaUrlCount} records=${result.recordCount}${dryRun ? ' (dry-run)' : ''}`
    )
  }

  const totals = results.reduce(
    (acc, item) => {
      acc.models += 1
      acc.logs += item.logFiles
      acc.records += item.recordCount
      acc.pageUrls += item.pageUrlCount
      acc.mediaUrls += item.mediaUrlCount
      return acc
    },
    { models: 0, logs: 0, records: 0, pageUrls: 0, mediaUrls: 0 }
  )

  console.log('')
  console.log(
    `Totals: models=${totals.models} logs=${totals.logs} records=${totals.records} pageUrls=${totals.pageUrls} mediaUrls=${totals.mediaUrls}`
  )
}

function listDatasetModels() {
  if (!fs.existsSync(datasetRoot)) return []
  return fs
    .readdirSync(datasetRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b))
}

function selectModels(allModels) {
  let next = [...allModels]

  if (singleModel) {
    next = next.filter((modelName) => modelName === singleModel)
  }

  if (explicitModels.length) {
    const wanted = new Set(explicitModels)
    next = next.filter((modelName) => wanted.has(modelName))
  }

  if (startFrom) {
    next = next.filter((modelName) => modelName.localeCompare(startFrom) >= 0)
  }

  return next
}

function backfillModel(modelName) {
  const modelDir = path.join(datasetRoot, modelName)
  const logDir = path.join(modelDir, 'log')
  const indexPath = path.join(logDir, 'milkmaid-seen-media-index.json')
  const index = {
    version: 1,
    updatedAt: new Date().toISOString(),
    mediaPageUrls: {},
    mediaUrls: {},
  }

  if (!fs.existsSync(logDir)) {
    return {
      model: modelName,
      logFiles: 0,
      recordCount: 0,
      pageUrlCount: 0,
      mediaUrlCount: 0,
    }
  }

  const logFiles = fs
    .readdirSync(logDir)
    .filter(
      (name) =>
        (name.startsWith('milkmaid-run-') ||
          name.startsWith('hoghaul-run-')) &&
        name.endsWith('.jsonl') &&
        !name.includes('errors')
    )
    .sort((a, b) => a.localeCompare(b))

  for (const fileName of logFiles) {
    consumeRunLog(path.join(logDir, fileName), modelName, index)
  }

  if (!dryRun) {
    fs.mkdirSync(logDir, { recursive: true })
    fs.writeFileSync(indexPath, JSON.stringify(index, null, 2) + '\n')
  }

  return {
    model: modelName,
    logFiles: logFiles.length,
    recordCount: countUniqueRecords(index),
    pageUrlCount: Object.keys(index.mediaPageUrls).length,
    mediaUrlCount: Object.keys(index.mediaUrls).length,
  }
}

function consumeRunLog(logPath, modelName, index) {
  const raw = fs.readFileSync(logPath, 'utf8')
  const lines = raw.split(/\r?\n/).filter(Boolean)
  const seenByFilename = new Map()
  const seenBySavedPath = new Map()

  for (const line of lines) {
    let event
    try {
      event = JSON.parse(line)
    } catch {
      continue
    }

    if (event.type === 'media_seen') {
      const candidate = {
        mediaPageUrl: normalizeUrl(event.mediaPageUrl),
        mediaUrl: normalizeMediaUrl(event.mediaUrl),
        filename: String(event.filename || '').trim() || null,
        sourceSite: event.sourceSite || null,
        sourceService: event.sourceService || null,
        sourceUserId: event.sourceUserId || null,
        sourceUsername: event.sourceUsername || null,
        sourceSubreddit: event.sourceSubreddit || null,
        postId: event.postId || null,
        uploadedDate: normalizeIsoDate(event.uploadedDate),
      }
      if (candidate.filename) {
        seenByFilename.set(candidate.filename, candidate)
      }
      const savedPathGuess = buildSavedPathGuess(
        modelName,
        event.filename,
        event.extension
      )
      if (savedPathGuess) {
        seenBySavedPath.set(savedPathGuess, candidate)
      }
      continue
    }

    if (!SAVE_EVENT_TYPES.has(event.type)) continue

    const relativePath = normalizeRelativePath(event.savedPath)
    const filename = String(
      event.filename || path.basename(relativePath || '')
    ).trim()
    const candidate =
      (relativePath && seenBySavedPath.get(relativePath)) ||
      (filename && seenByFilename.get(filename)) ||
      null

    if (!relativePath || !candidate) continue
    const absolutePath = path.join(datasetRoot, ...relativePath.split('/'))
    if (!datasetPaths.existsLocallyOrOnNas(absolutePath)) continue

    const payload = {
      relativePath,
      filename: filename || path.basename(relativePath),
      mediaUrl: candidate.mediaUrl || null,
      mediaPageUrl: candidate.mediaPageUrl || null,
      savedAt: event.at || null,
      sourceSite: candidate.sourceSite || null,
      sourceService: candidate.sourceService || null,
      sourceUserId: candidate.sourceUserId || null,
      sourceUsername: candidate.sourceUsername || null,
      sourceSubreddit: candidate.sourceSubreddit || null,
      postId: candidate.postId || null,
      uploadedDate: candidate.uploadedDate || null,
    }

    if (payload.mediaPageUrl) {
      index.mediaPageUrls[payload.mediaPageUrl] = payload
    }
    if (payload.mediaUrl) {
      index.mediaUrls[payload.mediaUrl] = payload
    }
  }
}

function buildSavedPathGuess(modelName, filename, extension) {
  const normalizedFilename = String(filename || '').trim()
  const normalizedExtension = String(extension || '')
    .trim()
    .toLowerCase()

  if (!normalizedFilename) return null
  if (normalizedExtension === '.gif') {
    return `${modelName}/gif/${normalizedFilename}`
  }
  if (['.mp4', '.webm', '.m4v', '.mov'].includes(normalizedExtension)) {
    return `${modelName}/webm/${normalizedFilename}`
  }
  if (['.jpg', '.jpeg', '.png', '.webp'].includes(normalizedExtension)) {
    return `${modelName}/images/${normalizedFilename}`
  }
  return null
}

function normalizeRelativePath(value) {
  return String(value || '')
    .trim()
    .replace(/\\/g, '/')
}

function normalizeUrl(value) {
  return String(value || '')
    .trim()
    .replace(/&slideshow=$/, '')
}

function normalizeMediaUrl(value) {
  const normalized = normalizeUrl(value)
  return isLikelyMediaUrl(normalized) ? normalized : null
}

function normalizeIsoDate(value) {
  if (value instanceof Date && !isNaN(value.getTime()))
    return value.toISOString()
  if (typeof value === 'string' || typeof value === 'number') {
    const parsed = new Date(value)
    if (!isNaN(parsed.getTime())) return parsed.toISOString()
  }
  return null
}

function countUniqueRecords(index) {
  const refs = new Set()
  for (const value of Object.values(index.mediaPageUrls || {})) {
    if (value?.relativePath) refs.add(value.relativePath)
  }
  for (const value of Object.values(index.mediaUrls || {})) {
    if (value?.relativePath) refs.add(value.relativePath)
  }
  return refs.size
}

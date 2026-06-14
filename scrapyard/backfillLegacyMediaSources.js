'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const minimist = require('minimist')

const mediaDates = require('./mediaDates')
const {
  buildRedditSourceMeta,
  buildSeenIndexByRelativePath,
  buildStufferSourceMeta,
  getModelProfileSource,
  hasSourceUrl,
  sourceMetaFromSeenRecord,
} = require('./legacySourceBackfill')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  boolean: ['apply', 'profile-fallback'],
  string: ['dataset-root', 'model', 'models', 'overrides'],
  default: {
    apply: false,
    'profile-fallback': true,
  },
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const rootDir = path.join(__dirname, '..')
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
const registryPath = path.join(rootDir, 'model_aliases.json')
const overridePath = path.resolve(
  String(argv.overrides || path.join(__dirname, 'legacy-source-overrides.json'))
)
const apply = Boolean(argv.apply)
const useProfileFallback = argv['profile-fallback'] !== false

main().catch((err) => {
  console.error(
    `Fatal legacy source backfill error: ${err.stack || err.message}`
  )
  process.exitCode = 1
})

function printHelp() {
  console.log(`Usage: node scrapyard/backfillLegacyMediaSources.js [options]

Options:
  --dataset-root <path>       Dataset root (default: local Slopvault dataset).
  --model <name>              Process one model.
  --models <a,b,c>            Process a comma-separated model list.
  --overrides <path>          Model-level fallback override JSON.
  --no-profile-fallback       Leave records unresolved instead of linking a
                              known creator profile.
  --apply                     Write through recordExistingMetadata.
  --help                      Show this help.

The default mode is a dry-run. Resolution order is exact seen-media index,
deterministic StufferDB URL, Reddit post ID from filename, then a model-level
profile URL from model_aliases.json or the override file.
`)
}

function readJson(filePath, fallback = {}) {
  if (!fs.existsSync(filePath)) return fallback
  return JSON.parse(fs.readFileSync(filePath, 'utf8'))
}

function selectModels() {
  const requested = new Set(
    [
      argv.model,
      ...String(argv.models || '')
        .split(',')
        .map((value) => value.trim()),
    ].filter(Boolean)
  )

  return fs
    .readdirSync(datasetRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith('.'))
    .map((entry) => entry.name)
    .filter((modelName) => requested.size === 0 || requested.has(modelName))
    .sort((a, b) => a.localeCompare(b))
}

function resolveOverride(overrides, modelName) {
  const value = overrides?.[modelName]
  if (!value || typeof value !== 'object') return null
  if (!value.mediaPageUrl && !value.mediaUrl) return null
  return value
}

function resolveSource({
  modelName,
  relativePath,
  seenByPath,
  registry,
  overrides,
}) {
  const seenSource = sourceMetaFromSeenRecord(seenByPath.get(relativePath))
  if (seenSource) return { method: 'seen-index', source: seenSource }

  const filename = path.basename(relativePath)
  const stufferSource = buildStufferSourceMeta(filename)
  if (stufferSource) {
    return { method: 'stuffer-filename', source: stufferSource }
  }

  const redditSource = buildRedditSourceMeta(filename)
  if (redditSource) {
    return { method: 'reddit-filename', source: redditSource }
  }

  if (!useProfileFallback) return null

  const override = resolveOverride(overrides, modelName)
  if (override) return { method: 'model-override', source: override }

  const profileSource = getModelProfileSource(registry[modelName], filename)
  if (profileSource) {
    return { method: 'model-profile', source: profileSource }
  }

  return null
}

async function main() {
  if (!fs.existsSync(datasetRoot)) {
    throw new Error(`Dataset root does not exist: ${datasetRoot}`)
  }

  const registry = readJson(registryPath)
  const overrides = readJson(overridePath)
  const models = selectModels()
  const totals = {
    records: 0,
    alreadyLinked: 0,
    backfilled: 0,
    unresolved: 0,
    methods: {},
  }
  const unresolved = []

  console.log(`Legacy source backfill: ${models.length} model(s)`)
  console.log(`Dataset: ${datasetRoot}`)
  console.log(`Mode: ${apply ? 'apply' : 'dry-run'}`)
  console.log(
    `Profile fallback: ${useProfileFallback ? 'enabled' : 'disabled'}`
  )

  for (let modelIndex = 0; modelIndex < models.length; modelIndex += 1) {
    const modelName = models[modelIndex]
    const modelDir = path.join(datasetRoot, modelName)
    const sidecarPath = path.join(modelDir, mediaDates.SIDECAR_FILENAME)
    if (!fs.existsSync(sidecarPath)) continue

    const sidecar = readJson(sidecarPath)
    const seenIndex = readJson(
      path.join(modelDir, 'log', 'milkmaid-seen-media-index.json')
    )
    const seenByPath = buildSeenIndexByRelativePath(seenIndex, modelName)
    const modelStats = {
      records: 0,
      alreadyLinked: 0,
      backfilled: 0,
      unresolved: 0,
    }

    for (const [relativePath, record] of Object.entries(sidecar)) {
      if (relativePath.startsWith('__')) continue
      totals.records += 1
      modelStats.records += 1

      if (hasSourceUrl(record)) {
        totals.alreadyLinked += 1
        modelStats.alreadyLinked += 1
        continue
      }

      const resolved = resolveSource({
        modelName,
        relativePath,
        seenByPath,
        registry,
        overrides,
      })
      if (!resolved) {
        totals.unresolved += 1
        modelStats.unresolved += 1
        if (unresolved.length < 100) {
          unresolved.push(`${modelName}/${relativePath}`)
        }
        continue
      }

      totals.backfilled += 1
      modelStats.backfilled += 1
      totals.methods[resolved.method] =
        (totals.methods[resolved.method] || 0) + 1

      if (apply) {
        mediaDates.recordExistingMetadata(
          modelDir,
          relativePath,
          record?.uploaded || null,
          resolved.source
        )
      }
    }

    console.log(
      `[${modelIndex + 1}/${models.length}] ${modelName}: ` +
        `linked=${modelStats.alreadyLinked} backfill=${modelStats.backfilled} ` +
        `unresolved=${modelStats.unresolved}`
    )
  }

  if (apply) mediaDates.flushAllSidecars()

  console.log('')
  console.log(
    `Totals: records=${totals.records} linked=${totals.alreadyLinked} ` +
      `backfilled=${totals.backfilled} unresolved=${totals.unresolved}`
  )
  console.log(`Methods: ${JSON.stringify(totals.methods)}`)

  if (unresolved.length) {
    console.log('Unresolved samples:')
    for (const value of unresolved) console.log(`  ${value}`)
  }

  if (!apply) console.log('Dry-run only. Re-run with --apply to write changes.')
  if (apply && totals.unresolved > 0) process.exitCode = 2
}

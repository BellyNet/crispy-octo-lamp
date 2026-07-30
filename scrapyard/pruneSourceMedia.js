'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const minimist = require('minimist')

const {
  loadBitwiseHashCache,
  saveBitwiseHashCache,
  removeBitwiseRefs,
} = require('./bitwiseHasher')
const {
  loadVisualHashCache,
  saveVisualHashCache,
  removeVisualRefs,
} = require('./visualHasher')
const { removeNasMp4Entries, normalizePath } = require('./nasMp4Index')
const { writeRepoJsonFileSync } = require('./repoFileWriter')

const IMAGE_EXTENSIONS = new Set(['.jpg', '.jpeg', '.png', '.webp', '.gif'])
const VIDEO_EXTENSIONS = new Set(['.mp4', '.webm', '.m4v', '.mov'])
const SIDECAR_FILENAME = '.media-dates.json'
const SEEN_INDEX_FILENAME = 'milkmaid-seen-media-index.json'
const REDDIT_STATE_FILENAME = 'reddit-source-state.json'
const SOURCE_FRONTIER_FILENAME = 'source-frontier-state.json'

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  boolean: [
    'help',
    'apply',
    'delete-nas',
    'remove-source-url',
    'prune-reddit-state',
  ],
  string: [
    'model',
    'source-site',
    'source-service',
    'source-user',
    'source-url',
    'cutoff-date',
    'before-date',
    'media-kind',
    'dataset-root',
    'nas-root',
    'model-aliases',
    'report-dir',
  ],
  default: {
    apply: false,
    'delete-nas': false,
    'remove-source-url': false,
    'prune-reddit-state': true,
    'source-site': 'reddit',
    'media-kind': 'all',
  },
})

if (argv.help || !argv.model || !argv['cutoff-date']) {
  printHelp()
  process.exit(argv.help ? 0 : 1)
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
const nasRoot = path.resolve(
  String(argv['nas-root'] || process.env.NAS_DATASET_DIR || 'Z:\\dataset')
)
const modelAliasesPath = path.resolve(
  String(argv['model-aliases'] || path.join(rootDir, 'model_aliases.json'))
)
const reportDir = path.resolve(
  String(argv['report-dir'] || path.join(rootDir, 'tmp', 'prune-source-media'))
)
const modelName = String(argv.model)
const sourceSite = normalizeKey(argv['source-site'])
const sourceService = normalizeKey(argv['source-service'])
const sourceUser = normalizeKey(argv['source-user'])
const sourceUrl = String(argv['source-url'] || '').trim()
const cutoffDate = parseDateArg(argv['cutoff-date'], 'cutoff-date')
const beforeDate = argv['before-date']
  ? parseDateArg(argv['before-date'], 'before-date')
  : null
const mediaKind = String(argv['media-kind'] || 'all').toLowerCase()
const apply = Boolean(argv.apply)
const deleteNas = Boolean(argv['delete-nas'])
const removeSourceUrl = Boolean(argv['remove-source-url'])
const pruneRedditState = Boolean(argv['prune-reddit-state'])

try {
  main()
} catch (err) {
  console.error(`Fatal source-media prune error: ${err.stack || err.message}`)
  process.exitCode = 1
}

function printHelp() {
  console.log(`Usage: node scrapyard/pruneSourceMedia.js --model <name> --cutoff-date <date> [options]

Dry-runs by default. Matches sidecar rows by source metadata and date, then can
delete files, sidecar rows, hash refs, seen-index refs, source state, and a
matching model_aliases.json source URL in --apply mode.

Options:
  --source-site <site>       Source site to match. Default: reddit.
  --source-service <name>    Optional source service, e.g. submitted.
  --source-user <name>       Optional source username/userId to match.
  --source-url <url>         Source URL to remove from model_aliases.json.
  --cutoff-date <date>       Inclusive lower bound, e.g. 2026-06-20.
  --before-date <date>       Optional exclusive upper bound.
  --media-kind <kind>        all, image, or video. Default: all.
  --dataset-root <path>      Override local dataset root.
  --nas-root <path>          Override NAS dataset root. Default: NAS_DATASET_DIR or Z:\\dataset.
  --delete-nas              Also delete matching NAS files in --apply mode.
  --remove-source-url        Remove matching source URL from model_aliases.json in --apply mode.
  --prune-reddit-state       Remove matched Reddit source state in --apply mode. Default: true.
  --model-aliases <path>     Override model_aliases.json path.
  --report-dir <path>        Override report directory.
  --apply                    Delete/update files. Omit for dry-run.
  -h, --help                 Show help.
`)
}

function main() {
  validateMediaKind(mediaKind)
  const modelRoot = path.join(datasetRoot, modelName)
  const sidecarPath = path.join(modelRoot, SIDECAR_FILENAME)
  if (!fs.existsSync(sidecarPath)) {
    throw new Error(`Missing sidecar: ${sidecarPath}`)
  }

  fs.mkdirSync(reportDir, { recursive: true })
  const backupTag = new Date().toISOString().replace(/[:.]/g, '-')
  const backups = []
  const sidecar = readJsonFile(sidecarPath)
  const targets = collectTargets(sidecar, modelRoot)
  const relativeTargets = new Set(
    targets.flatMap((target) => target.refPaths).map(normalizeRefKey)
  )

  loadBitwiseHashCache()
  loadVisualHashCache()
  const bitwiseRefsRemoved = removeBitwiseRefs((ref) =>
    relativeTargets.has(normalizeRefKey(ref))
  )
  const visualRefsRemoved = removeVisualRefs((ref) =>
    relativeTargets.has(normalizeRefKey(ref))
  )

  const seenResult = pruneSeenIndex(relativeTargets, backupTag, backups)
  const redditStateResult = pruneRedditSourceState(backupTag, backups)
  const frontierResult = pruneSourceFrontierState(backupTag, backups)
  const aliasesResult = pruneModelAliases(backupTag, backups)

  let localFilesDeleted = 0
  let nasFilesDeleted = 0
  let sidecarRowsRemoved = 0

  if (apply) {
    backupFile(sidecarPath, backupTag, backups)
    backupFile(
      path.join(datasetRoot, 'bitwiseHashes.v2.json'),
      backupTag,
      backups
    )
    backupFile(
      path.join(datasetRoot, 'visualHashes.v2.json'),
      backupTag,
      backups
    )
    if (targets.some((target) => isVideoPath(target.relativePath))) {
      backupFile(
        path.join(datasetRoot, 'nas-mp4-index.v1.json'),
        backupTag,
        backups
      )
    }

    for (const target of targets) {
      if (
        target.localExists &&
        deleteFileInsideRoot(datasetRoot, target.localPath)
      ) {
        localFilesDeleted += 1
      }
      if (
        deleteNas &&
        target.nasExists &&
        deleteFileInsideRoot(nasRoot, target.nasPath)
      ) {
        nasFilesDeleted += 1
      }
      delete sidecar[target.relativePath]
      sidecarRowsRemoved += 1
    }

    fs.writeFileSync(sidecarPath, JSON.stringify(sidecar), 'utf8')
    saveBitwiseHashCache()
    saveVisualHashCache()
    removeNasMp4Entries(
      targets
        .filter((target) => isVideoPath(target.relativePath))
        .map((target) => target.datasetPath),
      datasetRoot
    )
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply,
    modelName,
    datasetRoot,
    nasRoot,
    deleteNas,
    modelAliasesPath,
    filters: {
      sourceSite,
      sourceService: sourceService || null,
      sourceUser: sourceUser || null,
      sourceUrl: sourceUrl || null,
      cutoffDate: cutoffDate.toISOString(),
      beforeDate: beforeDate ? beforeDate.toISOString() : null,
      mediaKind,
    },
    targetCount: targets.length,
    targetLocalFilesExisting: targets.filter((target) => target.localExists)
      .length,
    targetNasFilesExisting: targets.filter((target) => target.nasExists).length,
    targetBytesLocal: targets.reduce(
      (sum, target) => sum + target.localBytes,
      0
    ),
    targetBytesNas: targets.reduce((sum, target) => sum + target.nasBytes, 0),
    localFilesDeleted,
    nasFilesDeleted,
    sidecarRowsRemoved,
    bitwiseRefsRemoved,
    visualRefsRemoved,
    seen: seenResult,
    redditState: redditStateResult,
    sourceFrontier: frontierResult,
    modelAliases: aliasesResult,
    backups,
    targets,
  }

  const reportPath = path.join(reportDir, 'prune-source-media-latest.json')
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2) + '\n')

  console.log(`Mode: ${apply ? 'apply' : 'dry-run'}`)
  console.log(`Model: ${modelName}`)
  console.log(`Targets: ${targets.length}`)
  console.log(`Local files existing: ${report.targetLocalFilesExisting}`)
  console.log(`NAS files existing: ${report.targetNasFilesExisting}`)
  console.log(
    `Sidecar rows ${apply ? 'removed' : 'targeted'}: ${targets.length}`
  )
  console.log(
    `Bitwise refs ${apply ? 'removed' : 'targeted'}: ${bitwiseRefsRemoved}`
  )
  console.log(
    `Visual refs ${apply ? 'removed' : 'targeted'}: ${visualRefsRemoved}`
  )
  console.log(
    `Seen refs ${apply ? 'removed' : 'targeted'}: ${seenResult.keysRemoved}`
  )
  console.log(
    `Alias entries ${apply ? 'removed' : 'targeted'}: ${aliasesResult.removed}`
  )
  console.log(`Report: ${reportPath}`)
}

function collectTargets(sidecar, modelRoot) {
  const targets = []
  for (const [relativePath, row] of Object.entries(sidecar)) {
    if (relativePath.startsWith('__')) continue
    if (!matchesMediaKind(relativePath, mediaKind)) continue
    if (!matchesSource(row?.source)) continue
    const date = getRowDate(row)
    if (!date || date < cutoffDate) continue
    if (beforeDate && date >= beforeDate) continue

    const localPath = path.join(modelRoot, ...relativePath.split('/'))
    const nasPath = path.join(nasRoot, modelName, ...relativePath.split('/'))
    const localStat = statFile(localPath)
    const nasStat = statFile(nasPath)
    targets.push({
      relativePath,
      datasetPath: normalizePath(`${modelName}/${relativePath}`),
      refPaths: getTargetRefPaths(relativePath),
      date: date.toISOString(),
      localPath,
      localExists: Boolean(localStat),
      localBytes: localStat?.size || 0,
      nasPath,
      nasExists: Boolean(nasStat),
      nasBytes: nasStat?.size || 0,
      source: {
        site: row?.source?.site || null,
        service: row?.source?.service || null,
        userId: row?.source?.userId || null,
        username: row?.source?.username || null,
        subreddit: row?.source?.subreddit || null,
        postId: row?.source?.postId || null,
        mediaPageUrl: row?.source?.mediaPageUrl || null,
        mediaUrl: row?.source?.mediaUrl || null,
      },
    })
  }
  return targets.sort((a, b) => a.date.localeCompare(b.date))
}

function matchesSource(source = {}) {
  if (normalizeKey(source.site) !== sourceSite) return false
  if (sourceService && normalizeKey(source.service) !== sourceService) {
    return false
  }
  if (sourceUser) {
    const users = [source.userId, source.username].map(normalizeKey)
    if (!users.includes(sourceUser)) return false
  }
  return true
}

function pruneSeenIndex(targets, backupTag, backups) {
  const result = {
    path: path.join(datasetRoot, modelName, 'log', SEEN_INDEX_FILENAME),
    recordsRemoved: 0,
    keysRemoved: 0,
  }
  if (!fs.existsSync(result.path) || targets.size === 0) return result
  const parsed = readJsonFile(result.path)
  const removedPaths = new Set()
  for (const bucketName of ['mediaUrls', 'mediaPageUrls']) {
    const bucket = parsed[bucketName]
    if (!bucket || typeof bucket !== 'object') continue
    for (const [key, entry] of Object.entries(bucket)) {
      const relativePath = normalizeRefKey(entry?.relativePath)
      if (!targets.has(relativePath)) continue
      result.keysRemoved += 1
      removedPaths.add(relativePath)
      if (apply) delete bucket[key]
    }
  }
  result.recordsRemoved = removedPaths.size
  if (apply && result.keysRemoved > 0) {
    backupFile(result.path, backupTag, backups)
    fs.writeFileSync(result.path, JSON.stringify(parsed), 'utf8')
  }
  return result
}

function pruneRedditSourceState(backupTag, backups) {
  const result = {
    path: path.join(datasetRoot, modelName, 'log', REDDIT_STATE_FILENAME),
    removed: 0,
    sourceKeys: [],
  }
  if (
    !pruneRedditState ||
    sourceSite !== 'reddit' ||
    !fs.existsSync(result.path)
  ) {
    return result
  }
  const parsed = readJsonFile(result.path)
  for (const [key, source] of Object.entries(parsed.sources || {})) {
    if (!matchesRedditStateSource(source)) continue
    result.removed += 1
    result.sourceKeys.push(key)
    if (apply) delete parsed.sources[key]
  }
  if (apply && result.removed > 0) {
    backupFile(result.path, backupTag, backups)
    parsed.updatedAt = new Date().toISOString()
    fs.writeFileSync(result.path, JSON.stringify(parsed, null, 2) + '\n')
  }
  return result
}

function pruneSourceFrontierState(backupTag, backups) {
  const result = {
    path: path.join(datasetRoot, modelName, 'log', SOURCE_FRONTIER_FILENAME),
    removed: 0,
    sourceKeys: [],
  }
  if (!fs.existsSync(result.path)) return result
  const parsed = readJsonFile(result.path)
  for (const [key, source] of Object.entries(parsed.sources || {})) {
    if (!matchesFrontierSource(source)) continue
    result.removed += 1
    result.sourceKeys.push(key)
    if (apply) delete parsed.sources[key]
  }
  if (apply && result.removed > 0) {
    backupFile(result.path, backupTag, backups)
    parsed.updatedAt = new Date().toISOString()
    fs.writeFileSync(result.path, JSON.stringify(parsed, null, 2) + '\n')
  }
  return result
}

function pruneModelAliases(backupTag, backups) {
  const result = {
    path: modelAliasesPath,
    removed: 0,
    modelKey: null,
  }
  if (!removeSourceUrl || !sourceUrl || !fs.existsSync(modelAliasesPath)) {
    return result
  }
  const registry = readJsonFile(modelAliasesPath)
  const entry = registry[modelName] || registry[normalizeKey(modelName)]
  if (!entry?.sources || !Array.isArray(entry.sources[sourceSite]))
    return result
  result.modelKey =
    Object.keys(registry).find((key) => registry[key] === entry) || modelName
  const before = entry.sources[sourceSite].length
  entry.sources[sourceSite] = entry.sources[sourceSite].filter(
    (source) => normalizeUrl(source?.url) !== normalizeUrl(sourceUrl)
  )
  result.removed = before - entry.sources[sourceSite].length
  if (entry.sources[sourceSite].length === 0 && apply) {
    delete entry.sources[sourceSite]
  }
  if (apply && result.removed > 0) {
    backupFile(modelAliasesPath, backupTag, backups)
    writeRepoJsonFileSync(modelAliasesPath, registry)
  }
  return result
}

function matchesRedditStateSource(source = {}) {
  if (normalizeKey(source.sourceSite) !== 'reddit') return false
  if (sourceService && normalizeKey(source.sourceService) !== sourceService) {
    return false
  }
  if (sourceUser) {
    const users = [source.sourceUserId, source.sourceUsername].map(normalizeKey)
    if (!users.includes(sourceUser)) return false
  }
  if (
    sourceUrl &&
    source.inputUrl &&
    normalizeUrl(source.inputUrl) !== normalizeUrl(sourceUrl)
  ) {
    return false
  }
  return true
}

function matchesFrontierSource(source = {}) {
  if (normalizeKey(source.site) !== sourceSite) return false
  if (sourceService && normalizeKey(source.service) !== sourceService) {
    return false
  }
  if (sourceUser) {
    const users = [source.userId, source.username].map(normalizeKey)
    if (!users.includes(sourceUser)) return false
  }
  return true
}

function getRowDate(row = {}) {
  for (const value of [row.uploaded, row.resolved?.date, row.filename]) {
    const date = parseOptionalDate(value)
    if (date) return date
  }
  return null
}

function matchesMediaKind(relativePath, kind) {
  if (kind === 'all') return true
  const ext = path.extname(relativePath).toLowerCase()
  if (kind === 'image') return IMAGE_EXTENSIONS.has(ext)
  if (kind === 'video') return VIDEO_EXTENSIONS.has(ext)
  return false
}

function isVideoPath(relativePath) {
  return VIDEO_EXTENSIONS.has(path.extname(relativePath).toLowerCase())
}

function validateMediaKind(kind) {
  if (!['all', 'image', 'video'].includes(kind)) {
    throw new Error(`Invalid --media-kind: ${kind}`)
  }
}

function parseDateArg(value, label) {
  const date = parseOptionalDate(value)
  if (!date) throw new Error(`Invalid --${label}: ${value}`)
  return date
}

function parseOptionalDate(value) {
  if (!value) return null
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? null : date
}

function statFile(filePath) {
  try {
    const stat = fs.statSync(filePath)
    return stat.isFile() ? stat : null
  } catch {
    return null
  }
}

function deleteFileInsideRoot(root, filePath) {
  const resolvedRoot = path.resolve(root)
  const resolvedFile = path.resolve(filePath)
  const relative = path.relative(resolvedRoot, resolvedFile)
  if (relative.startsWith('..') || path.isAbsolute(relative)) {
    throw new Error(
      `Refusing to delete outside ${resolvedRoot}: ${resolvedFile}`
    )
  }
  if (!fs.existsSync(resolvedFile)) return false
  fs.unlinkSync(resolvedFile)
  return true
}

function backupFile(filePath, backupTag, backups) {
  if (!fs.existsSync(filePath)) return null
  const backupPath = `${filePath}.bak-${backupTag}`
  fs.copyFileSync(filePath, backupPath)
  backups.push({ source: filePath, backup: backupPath })
  return backupPath
}

function readJsonFile(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, ''))
}

function normalizeKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
}

function normalizeRefKey(value) {
  return normalizePath(value).toLowerCase()
}

function getTargetRefPaths(relativePath) {
  return [
    `${modelName}/${relativePath}`,
    `${normalizeKey(modelName)}/${relativePath}`,
  ].map(normalizePath)
}

function normalizeUrl(value) {
  return String(value || '')
    .trim()
    .replace(/\/+$/, '')
    .toLowerCase()
}

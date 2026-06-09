'use strict'

const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')
const minimist = require('minimist')
const pLimit = require('p-limit')
const { formatBytes } = require('../scrapyard/runLifecycle')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  boolean: ['help', 'no-cache'],
  default: {
    concurrency: 2,
  },
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const localRoot = path.resolve(
  String(
    argv['local-root'] ||
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
const outputPath = path.resolve(
  String(
    argv.output ||
      path.join(process.cwd(), 'tmp', 'exact-media-duplicates-latest.json')
  )
)
const cachePath = path.resolve(
  String(
    argv.cache || path.join(process.cwd(), 'tmp', 'exact-media-hash-cache.json')
  )
)
const concurrency = parsePositiveInteger(argv.concurrency, 'concurrency')
const modelFilter = new Set(
  String(argv.model || '')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean)
)
const mediaExtensions = new Set([
  '.jpg',
  '.jpeg',
  '.png',
  '.webp',
  '.gif',
  '.mp4',
  '.webm',
  '.m4v',
  '.mov',
])
const videoExtensions = new Set(['.mp4', '.webm', '.m4v', '.mov'])

if (require.main === module) {
  main().catch((err) => {
    console.error(`Exact duplicate audit failed: ${err.stack || err.message}`)
    process.exitCode = 1
  })
}

async function main() {
  const startedAt = new Date().toISOString()
  const records = []
  const scanErrors = []

  console.log('EXACT MEDIA DUPLICATE AUDIT')
  console.log(`Local media: ${localRoot}`)
  console.log(`NAS videos: ${nasRoot}`)
  console.log('Phase 1/3: indexing file sizes...')

  collectMediaFiles(localRoot, 'local', false, records, scanErrors)
  collectMediaFiles(nasRoot, 'nas', true, records, scanErrors)

  const sizeGroups = groupBy(records, (record) => String(record.sizeBytes))
  const candidates = []
  for (const group of sizeGroups.values()) {
    if (group.length < 2) continue
    candidates.push(...group)
  }

  console.log(
    `Indexed ${records.length} physical files; ${candidates.length} share a size with another file.`
  )
  console.log('Phase 2/3: hashing size-matched candidates...')

  const hashCache = argv['no-cache'] ? {} : loadCache(cachePath)
  const nextCache = {}
  let cacheHits = 0
  let hashedFiles = 0
  let completed = 0
  let hashedBytes = 0
  let cacheWrites = 0
  const hashErrors = []
  const limit = pLimit(concurrency)

  await Promise.all(
    candidates.map((record) =>
      limit(async () => {
        try {
          const cacheKey = getCacheKey(record)
          const cached = hashCache[cacheKey]
          if (cached?.md5) {
            record.md5 = cached.md5
            cacheHits += 1
          } else {
            record.md5 = await hashFile(record.absolutePath)
            hashedFiles += 1
            hashedBytes += record.sizeBytes
          }
          nextCache[cacheKey] = { md5: record.md5 }
        } catch (err) {
          hashErrors.push({
            path: record.absolutePath,
            error: err.message,
          })
        } finally {
          completed += 1
          if (!argv['no-cache'] && completed % 25 === 0) {
            saveCache(cachePath, nextCache)
            cacheWrites += 1
          }
          if (completed === candidates.length || completed % 25 === 0) {
            process.stdout.write(
              `\rHashed/checked ${completed}/${candidates.length} | read ${formatBytes(
                hashedBytes
              )} | cache ${cacheHits}`
            )
          }
        }
      })
    )
  )
  if (candidates.length > 0) process.stdout.write('\n')

  if (!argv['no-cache']) {
    saveCache(cachePath, nextCache)
    cacheWrites += 1
  }

  console.log('Phase 3/3: building conservative duplicate groups...')
  const report = buildReport({
    startedAt,
    records,
    candidates,
    cacheHits,
    hashedFiles,
    hashedBytes,
    cacheWrites,
    scanErrors,
    hashErrors,
  })

  fs.mkdirSync(path.dirname(outputPath), { recursive: true })
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`)
  printSummary(report)
}

function printHelp() {
  console.log(`Usage: npm run audit:exact-media-dupes -- [options]

Read-only, zero-false-positive duplicate audit. It indexes local images, GIFs,
and videos plus NAS videos, then computes MD5 only for files sharing a size.

Options:
  -m, --model <names>    Comma-separated canonical model filter.
  --local-root <path>    Local dataset root.
  --nas-root <path>      NAS dataset root. Default: NAS_DATASET_DIR or Z:\\dataset.
  --concurrency <n>      Concurrent hash streams. Default: 2.
  --cache <path>         Hash cache path.
  --no-cache             Ignore and do not write the hash cache.
  --output <path>        JSON report path.
  -h, --help             Show help.

The audit never deletes, moves, or edits media files or scraper hash stores.
`)
}

function collectMediaFiles(root, rootType, videosOnly, records, errors) {
  if (!fs.existsSync(root)) {
    errors.push({ path: root, error: 'root_not_found' })
    return
  }

  const stack = [root]
  while (stack.length > 0) {
    const current = stack.pop()
    let entries
    try {
      entries = fs.readdirSync(current, { withFileTypes: true })
    } catch (err) {
      errors.push({ path: current, error: err.message })
      continue
    }

    for (const entry of entries) {
      const absolutePath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        if (rootType === 'local' && entry.name.toLowerCase() === 'log') continue
        stack.push(absolutePath)
        continue
      }
      if (!entry.isFile()) continue

      const extension = path.extname(entry.name).toLowerCase()
      if (!mediaExtensions.has(extension)) continue
      if (videosOnly && !videoExtensions.has(extension)) continue

      const relativePath = normalizePath(path.relative(root, absolutePath))
      const modelName = relativePath.split('/')[0] || 'unknown'
      if (modelFilter.size > 0 && !modelFilter.has(modelName.toLowerCase())) {
        continue
      }

      try {
        const stat = fs.statSync(absolutePath)
        records.push({
          rootType,
          absolutePath,
          relativePath,
          logicalPath: relativePath.toLowerCase(),
          modelName,
          bucket: relativePath.split('/')[1] || null,
          filename: entry.name,
          extension,
          mediaType: getMediaType(extension),
          sizeBytes: stat.size,
          modifiedAt: stat.mtime.toISOString(),
          modifiedMs: stat.mtimeMs,
          md5: null,
        })
      } catch (err) {
        errors.push({ path: absolutePath, error: err.message })
      }
    }
  }
}

function buildReport(details) {
  const hashedRecords = details.candidates.filter((record) => record.md5)
  const logicalGroups = groupBy(hashedRecords, (record) => record.logicalPath)
  const mirrorConflicts = []
  const logicalRecords = []

  for (const physicalRecords of logicalGroups.values()) {
    const hashes = new Set(physicalRecords.map((record) => record.md5))
    if (hashes.size > 1) {
      mirrorConflicts.push({
        relativePath: physicalRecords[0].relativePath,
        copies: physicalRecords.map(toReportRecord),
      })
    }

    for (const hash of hashes) {
      const copies = physicalRecords.filter((record) => record.md5 === hash)
      logicalRecords.push({
        ...copies[0],
        md5: hash,
        locations: copies.map((record) => ({
          rootType: record.rootType,
          absolutePath: record.absolutePath,
        })),
      })
    }
  }

  const hashGroups = groupBy(logicalRecords, (record) => record.md5)
  const duplicateGroups = []
  for (const [md5, group] of hashGroups.entries()) {
    const uniqueLogicalPaths = new Set(
      group.map((record) => record.logicalPath)
    )
    if (uniqueLogicalPaths.size < 2) continue

    const sorted = [...group].sort(compareRecords)
    const byModel = groupBy(sorted, (record) => record.modelName.toLowerCase())
    const sameModelDuplicateSets = [...byModel.values()].filter(
      (modelRecords) => modelRecords.length > 1
    )
    const sameModelRedundantCopies = sameModelDuplicateSets.reduce(
      (sum, modelRecords) => sum + modelRecords.length - 1,
      0
    )
    const sameModelReclaimableBytes = sameModelDuplicateSets.reduce(
      (sum, modelRecords) =>
        sum + (modelRecords.length - 1) * modelRecords[0].sizeBytes,
      0
    )
    const models = [...new Set(sorted.map((record) => record.modelName))]

    duplicateGroups.push({
      md5,
      mediaType: getGroupMediaType(sorted),
      sizeBytes: sorted[0].sizeBytes,
      copies: sorted.length,
      models,
      crossModel: models.length > 1,
      hasSameModelDuplicates: sameModelDuplicateSets.length > 0,
      sameModelRedundantCopies,
      sameModelReclaimableBytes,
      records: sorted.map(toReportRecord),
    })
  }

  duplicateGroups.sort(
    (left, right) =>
      right.sameModelReclaimableBytes - left.sameModelReclaimableBytes ||
      right.sizeBytes - left.sizeBytes ||
      left.records[0].relativePath.localeCompare(right.records[0].relativePath)
  )

  const sameModelGroups = duplicateGroups.filter(
    (group) => group.hasSameModelDuplicates
  )
  const summary = {
    physicalFilesIndexed: details.records.length,
    sizeMatchedPhysicalFiles: details.candidates.length,
    hashCacheHits: details.cacheHits,
    filesHashedThisRun: details.hashedFiles,
    bytesHashedThisRun: details.hashedBytes,
    cacheWrites: details.cacheWrites,
    exactDuplicateGroups: duplicateGroups.length,
    exactDuplicateLogicalFiles: duplicateGroups.reduce(
      (sum, group) => sum + group.copies,
      0
    ),
    sameModelDuplicateGroups: sameModelGroups.length,
    crossModelGroups: duplicateGroups.filter((group) => group.crossModel)
      .length,
    crossModelOnlyGroups: duplicateGroups.filter(
      (group) => group.crossModel && !group.hasSameModelDuplicates
    ).length,
    sameModelRedundantCopies: sameModelGroups.reduce(
      (sum, group) => sum + group.sameModelRedundantCopies,
      0
    ),
    conservativeReclaimableBytes: sameModelGroups.reduce(
      (sum, group) => sum + group.sameModelReclaimableBytes,
      0
    ),
    mirrorConflicts: mirrorConflicts.length,
    scanErrors: details.scanErrors.length,
    hashErrors: details.hashErrors.length,
    byMediaType: summarizeByMediaType(duplicateGroups),
  }

  return {
    generatedAt: new Date().toISOString(),
    startedAt: details.startedAt,
    mode: 'exact_bytes_md5_size_prefilter',
    readOnly: true,
    roots: {
      localRoot,
      nasRoot,
    },
    filters: {
      models: [...modelFilter],
    },
    summary,
    duplicateGroups,
    mirrorConflicts,
    errors: {
      scan: details.scanErrors,
      hash: details.hashErrors,
    },
  }
}

function summarizeByMediaType(groups) {
  const result = {}
  for (const group of groups) {
    const key = group.mediaType
    if (!result[key]) {
      result[key] = {
        groups: 0,
        logicalFiles: 0,
        sameModelGroups: 0,
        redundantCopies: 0,
        reclaimableBytes: 0,
      }
    }
    result[key].groups += 1
    result[key].logicalFiles += group.copies
    if (group.hasSameModelDuplicates) result[key].sameModelGroups += 1
    result[key].redundantCopies += group.sameModelRedundantCopies
    result[key].reclaimableBytes += group.sameModelReclaimableBytes
  }
  return result
}

function printSummary(report) {
  const summary = report.summary
  console.log('')
  console.log(`Physical files indexed: ${summary.physicalFilesIndexed}`)
  console.log(
    `Size-matched candidates: ${summary.sizeMatchedPhysicalFiles} | hashed now ${summary.filesHashedThisRun} (${formatBytes(
      summary.bytesHashedThisRun
    )}) | cache ${summary.hashCacheHits}`
  )
  console.log(
    `Exact duplicate groups: ${summary.exactDuplicateGroups} | same-model ${summary.sameModelDuplicateGroups} | cross-model-only ${summary.crossModelOnlyGroups}`
  )
  console.log(
    `Conservative same-model redundant copies: ${summary.sameModelRedundantCopies} (${formatBytes(
      summary.conservativeReclaimableBytes
    )})`
  )
  for (const [mediaType, stats] of Object.entries(summary.byMediaType)) {
    console.log(
      `  ${mediaType}: ${stats.groups} group(s), ${stats.redundantCopies} same-model redundant (${formatBytes(
        stats.reclaimableBytes
      )})`
    )
  }
  console.log(`Local/NAS mirror conflicts: ${summary.mirrorConflicts}`)
  console.log(`Errors: ${summary.scanErrors} scan, ${summary.hashErrors} hash`)
  console.log(`Report: ${outputPath}`)
}

function getCacheKey(record) {
  return [
    record.rootType,
    record.absolutePath.toLowerCase(),
    record.sizeBytes,
    Math.trunc(record.modifiedMs),
  ].join('|')
}

function loadCache(filePath) {
  if (!fs.existsSync(filePath)) return {}
  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'))
    return parsed?.entries && typeof parsed.entries === 'object'
      ? parsed.entries
      : {}
  } catch {
    return {}
  }
}

function saveCache(filePath, entries) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true })
  const temporaryPath = `${filePath}.${process.pid}.tmp`
  fs.writeFileSync(
    temporaryPath,
    `${JSON.stringify(
      {
        version: 1,
        updatedAt: new Date().toISOString(),
        entries,
      },
      null,
      2
    )}\n`
  )
  fs.renameSync(temporaryPath, filePath)
}

function hashFile(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('md5')
    const stream = fs.createReadStream(filePath)
    stream.on('data', (chunk) => hash.update(chunk))
    stream.on('error', reject)
    stream.on('end', () => resolve(hash.digest('hex')))
  })
}

function groupBy(values, getKey) {
  const groups = new Map()
  for (const value of values) {
    const key = getKey(value)
    if (!groups.has(key)) groups.set(key, [])
    groups.get(key).push(value)
  }
  return groups
}

function getMediaType(extension) {
  if (extension === '.gif') return 'gif'
  if (videoExtensions.has(extension)) return 'video'
  return 'image'
}

function getGroupMediaType(records) {
  const types = [...new Set(records.map((record) => record.mediaType))]
  return types.length === 1 ? types[0] : 'mixed'
}

function toReportRecord(record) {
  return {
    rootType: record.rootType,
    relativePath: record.relativePath,
    logicalPath: record.logicalPath,
    modelName: record.modelName,
    bucket: record.bucket,
    filename: record.filename,
    mediaType: record.mediaType,
    extension: record.extension,
    sizeBytes: record.sizeBytes,
    modifiedAt: record.modifiedAt,
    absolutePath: record.absolutePath,
    locations: record.locations || [
      {
        rootType: record.rootType,
        absolutePath: record.absolutePath,
      },
    ],
  }
}

function compareRecords(left, right) {
  return (
    left.modelName.localeCompare(right.modelName) ||
    left.relativePath.localeCompare(right.relativePath)
  )
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

function parsePositiveInteger(value, label) {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error(`Invalid --${label}: ${value}`)
  }
  return parsed
}

module.exports = {
  buildReport,
  getMediaType,
}

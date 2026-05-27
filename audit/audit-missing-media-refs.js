'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const minimist = require('minimist')

const argv = minimist(process.argv.slice(2), {
  boolean: ['skip-logs'],
  string: ['dataset-root', 'nas-root', 'report-dir', 'sample-size'],
  default: {
    'skip-logs': false,
    'sample-size': '25',
  },
})

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
const slopvaultRoot = path.dirname(datasetRoot)
const nasRoot = path.resolve(
  String(argv['nas-root'] || process.env.NAS_DATASET_DIR || 'Z:\\dataset')
)
const quarantineRoot = path.join(slopvaultRoot, 'quarantine', 'dataset')
const reportDir = path.resolve(
  String(argv['report-dir'] || path.join(process.cwd(), 'reports'))
)
const sampleSize = Math.max(
  1,
  Number.parseInt(String(argv['sample-size']), 10) || 25
)

const mediaExts = new Set([
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
const videoExts = new Set(['.mp4', '.webm', '.m4v', '.mov'])

main()

function main() {
  fs.mkdirSync(reportDir, { recursive: true })

  console.log(`Dataset root: ${datasetRoot}`)
  console.log(`NAS root: ${nasRoot}`)
  console.log(`Quarantine root: ${quarantineRoot}`)
  console.log('')

  console.log('Scanning local dataset...')
  const localFiles = collectMediaFiles(datasetRoot)
  console.log(`  local media files: ${localFiles.size}`)

  console.log('Scanning NAS dataset...')
  const nasFiles = collectMediaFiles(nasRoot)
  console.log(`  NAS media files: ${nasFiles.size}`)

  console.log('Scanning quarantine dataset...')
  const quarantineFiles = collectMediaFiles(quarantineRoot)
  console.log(`  quarantine media files: ${quarantineFiles.size}`)

  const nasIndex = loadNasIndex()
  console.log(`  NAS MP4 index entries: ${nasIndex.size}`)

  const modelReports = new Map()
  const missing = new Map()
  const staleNasIndex = []

  for (const relativePath of nasIndex) {
    if (!nasFiles.has(relativePath)) {
      staleNasIndex.push(relativePath)
      touchMissing(missing, relativePath).sources.add('nas_mp4_index')
    }
  }

  consumeActualFiles(modelReports, localFiles, 'local')
  consumeActualFiles(modelReports, nasFiles, 'nas')
  consumeActualFiles(modelReports, quarantineFiles, 'quarantine')

  consumeHashStore('bitwise', path.join(datasetRoot, 'bitwiseHashes.v2.json'), {
    modelReports,
    missing,
    localFiles,
    nasFiles,
    nasIndex,
    quarantineFiles,
  })
  consumeHashStore('visual', path.join(datasetRoot, 'visualHashes.v2.json'), {
    modelReports,
    missing,
    localFiles,
    nasFiles,
    nasIndex,
    quarantineFiles,
  })
  consumeSeenIndexes({
    modelReports,
    missing,
    localFiles,
    nasFiles,
    nasIndex,
    quarantineFiles,
  })

  if (!argv['skip-logs']) {
    console.log('Scanning run logs for historical media refs...')
    consumeRunLogs({
      modelReports,
      missing,
      localFiles,
      nasFiles,
      nasIndex,
      quarantineFiles,
    })
  }

  const missingRows = [...missing.values()]
    .map(serializeMissingEntry)
    .sort((a, b) => a.relativePath.localeCompare(b.relativePath))
  const modelRows = [...modelReports.values()]
    .map((report) => finalizeModelReport(report, missingRows))
    .sort((a, b) => {
      const diff = b.missingRefs - a.missingRefs
      return diff || a.model.localeCompare(b.model)
    })

  const summary = buildSummary({
    modelRows,
    missingRows,
    localFiles,
    nasFiles,
    quarantineFiles,
    nasIndex,
    staleNasIndex,
  })

  const generatedAt = new Date().toISOString()
  const jsonPath = path.join(reportDir, 'missing-media-audit-latest.json')
  const modelCsvPath = path.join(reportDir, 'missing-media-audit-by-model.csv')
  const missingCsvPath = path.join(
    reportDir,
    'missing-media-audit-missing-refs.csv'
  )

  fs.writeFileSync(
    jsonPath,
    JSON.stringify(
      {
        generatedAt,
        datasetRoot,
        nasRoot,
        quarantineRoot,
        summary,
        models: modelRows,
        missingRefs: missingRows,
      },
      null,
      2
    ) + '\n'
  )
  writeCsv(modelCsvPath, modelRows)
  writeCsv(missingCsvPath, missingRows.map(formatMissingCsvRow))

  console.log('')
  console.log(`Models scanned: ${summary.models}`)
  console.log(`Local media files: ${summary.localFiles}`)
  console.log(`NAS media files: ${summary.nasFiles}`)
  console.log(`Quarantine media files: ${summary.quarantineFiles}`)
  console.log(`Missing refs: ${summary.missingRefs}`)
  console.log(`Missing video refs: ${summary.missingVideoRefs}`)
  console.log(
    `Missing refs current logic may still treat active: ${summary.staleActiveMissingRefs}`
  )
  console.log(`Stale NAS MP4 index entries: ${summary.staleNasIndexEntries}`)
  console.log('')
  console.log(`JSON: ${jsonPath}`)
  console.log(`By model CSV: ${modelCsvPath}`)
  console.log(`Missing refs CSV: ${missingCsvPath}`)
}

function collectMediaFiles(rootPath) {
  const files = new Map()
  if (!fs.existsSync(rootPath)) return files

  const stack = [rootPath]
  while (stack.length) {
    const current = stack.pop()
    let entries = []
    try {
      entries = fs.readdirSync(current, { withFileTypes: true })
    } catch {
      continue
    }

    for (const entry of entries) {
      const absolutePath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        stack.push(absolutePath)
        continue
      }
      if (!entry.isFile()) continue

      const ext = path.extname(entry.name).toLowerCase()
      if (!mediaExts.has(ext)) continue

      const relativePath = normalizePath(path.relative(rootPath, absolutePath))
      files.set(relativePath, {
        relativePath,
        ext,
        model: getModelName(relativePath),
      })
    }
  }
  return files
}

function loadNasIndex() {
  const indexPath = path.join(datasetRoot, 'nas-mp4-index.v1.json')
  if (!fs.existsSync(indexPath)) return new Set()
  try {
    const parsed = JSON.parse(
      fs.readFileSync(indexPath, 'utf8').replace(/^\uFEFF/, '')
    )
    return new Set(
      (Array.isArray(parsed.entries) ? parsed.entries : [])
        .map((entry) => normalizePath(entry))
        .filter(Boolean)
    )
  } catch (err) {
    console.warn(`Could not parse NAS MP4 index: ${err.message}`)
    return new Set()
  }
}

function consumeActualFiles(modelReports, files, location) {
  for (const file of files.values()) {
    const report = ensureModelReport(modelReports, file.model)
    report[`${location}Files`] += 1
    if (videoExts.has(file.ext)) report[`${location}Videos`] += 1
  }
}

function consumeHashStore(kind, storePath, context) {
  console.log(`Scanning ${kind} hash store...`)
  if (!fs.existsSync(storePath)) return
  const parsed = JSON.parse(
    fs.readFileSync(storePath, 'utf8').replace(/^\uFEFF/, '')
  )
  for (const entry of parsed.entries || []) {
    const refs = Array.isArray(entry.refs) ? entry.refs : []
    for (const ref of refs) {
      const relativePath = normalizeRef(ref)
      if (!relativePath || !isMediaPath(relativePath)) continue
      const state = getRefState(relativePath, context)
      const report = ensureModelReport(context.modelReports, state.model)
      report[`${kind}Refs`] += 1
      if (state.isVideo) report[`${kind}VideoRefs`] += 1
      if (!state.existsOnDiskOrNas) {
        report[`${kind}Missing`] += 1
        if (state.isVideo) report[`${kind}MissingVideos`] += 1
        addMissing(context.missing, relativePath, kind, state)
      }
    }
  }
}

function consumeSeenIndexes(context) {
  console.log('Scanning seen-media indexes...')
  for (const model of listModels(datasetRoot)) {
    const indexPath = path.join(
      datasetRoot,
      model,
      'log',
      'milkmaid-seen-media-index.json'
    )
    if (!fs.existsSync(indexPath)) continue

    let parsed = null
    try {
      parsed = JSON.parse(
        fs.readFileSync(indexPath, 'utf8').replace(/^\uFEFF/, '')
      )
    } catch {
      continue
    }

    const refs = new Map()
    for (const bucket of ['mediaUrls', 'mediaPageUrls']) {
      for (const entry of Object.values(parsed[bucket] || {})) {
        const relativePath = normalizeRef(entry?.relativePath)
        if (!relativePath || !isMediaPath(relativePath)) continue
        const existing = refs.get(relativePath) || {
          urls: new Set(),
        }
        for (const value of [
          ...(entry.mediaUrls || []),
          ...(entry.mediaPageUrls || []),
        ]) {
          if (value) existing.urls.add(String(value))
        }
        refs.set(relativePath, existing)
      }
    }

    const report = ensureModelReport(context.modelReports, model)
    for (const [relativePath, entry] of refs.entries()) {
      const state = getRefState(relativePath, context)
      report.seenRefs += 1
      if (state.isVideo) report.seenVideoRefs += 1
      if (!state.existsOnDiskOrNas) {
        report.seenMissing += 1
        if (state.isVideo) report.seenMissingVideos += 1
        const missingEntry = addMissing(
          context.missing,
          relativePath,
          'seen',
          state
        )
        for (const url of entry.urls) addSample(missingEntry.sampleUrls, url)
      }
    }
  }
}

function consumeRunLogs(context) {
  for (const model of listModels(datasetRoot)) {
    const logDir = path.join(datasetRoot, model, 'log')
    if (!fs.existsSync(logDir)) continue
    const logFiles = fs
      .readdirSync(logDir)
      .filter(
        (name) =>
          (name.startsWith('milkmaid-run-') ||
            name.startsWith('hoghaul-run-')) &&
          name.endsWith('.jsonl') &&
          !name.includes('errors')
      )

    const refs = new Map()
    for (const fileName of logFiles) {
      const logPath = path.join(logDir, fileName)
      const raw = fs.readFileSync(logPath, 'utf8')
      for (const line of raw.split(/\r?\n/)) {
        if (!line.trim()) continue
        let event = null
        try {
          event = JSON.parse(line)
        } catch {
          continue
        }

        const relativePath = normalizeRef(event.savedPath || event.relativePath)
        if (!relativePath || !isMediaPath(relativePath)) continue
        const entry = refs.get(relativePath) || {
          eventTypes: new Set(),
          sampleUrls: new Set(),
          lastAt: null,
        }
        if (event.type) entry.eventTypes.add(String(event.type))
        for (const url of [
          event.mediaUrl,
          event.mediaPageUrl,
          event.sourceUrl,
        ]) {
          if (url) addSample(entry.sampleUrls, url)
        }
        entry.lastAt = event.at || entry.lastAt
        refs.set(relativePath, entry)
      }
    }

    const report = ensureModelReport(context.modelReports, model)
    for (const [relativePath, logEntry] of refs.entries()) {
      const state = getRefState(relativePath, context)
      report.logRefs += 1
      if (state.isVideo) report.logVideoRefs += 1
      if (!state.existsOnDiskOrNas) {
        report.logMissing += 1
        if (state.isVideo) report.logMissingVideos += 1
        const missingEntry = addMissing(
          context.missing,
          relativePath,
          'logs',
          state
        )
        for (const type of logEntry.eventTypes)
          missingEntry.eventTypes.add(type)
        for (const url of logEntry.sampleUrls)
          addSample(missingEntry.sampleUrls, url)
        missingEntry.lastLoggedAt = logEntry.lastAt || missingEntry.lastLoggedAt
      }
    }
  }
}

function addMissing(missing, relativePath, source, state) {
  const entry = touchMissing(missing, relativePath)
  entry.sources.add(source)
  entry.model = state.model
  entry.ext = state.ext
  entry.isVideo = state.isVideo
  entry.existsLocal = state.existsLocal
  entry.existsNas = state.existsNas
  entry.inNasIndex = state.inNasIndex
  entry.existsQuarantine = state.existsQuarantine
  entry.currentLogicMayTreatActive = state.currentLogicMayTreatActive
  return entry
}

function touchMissing(missing, relativePath) {
  if (!missing.has(relativePath)) {
    const ext = path.extname(relativePath).toLowerCase()
    missing.set(relativePath, {
      relativePath,
      model: getModelName(relativePath),
      ext,
      isVideo: videoExts.has(ext),
      sources: new Set(),
      eventTypes: new Set(),
      sampleUrls: new Set(),
      existsLocal: false,
      existsNas: false,
      inNasIndex: false,
      existsQuarantine: false,
      currentLogicMayTreatActive: false,
      lastLoggedAt: null,
    })
  }
  return missing.get(relativePath)
}

function getRefState(relativePath, context) {
  const ext = path.extname(relativePath).toLowerCase()
  const isVideo = videoExts.has(ext)
  const existsLocal = context.localFiles.has(relativePath)
  const existsNas = context.nasFiles.has(relativePath)
  const inNasIndex = context.nasIndex.has(relativePath)
  const existsQuarantine = context.quarantineFiles.has(relativePath)
  return {
    relativePath,
    model: getModelName(relativePath),
    ext,
    isVideo,
    existsLocal,
    existsNas,
    inNasIndex,
    existsQuarantine,
    existsOnDiskOrNas: existsLocal || existsNas,
    currentLogicMayTreatActive: existsLocal || (ext === '.mp4' && inNasIndex),
  }
}

function ensureModelReport(modelReports, model) {
  const modelName = model || '(unknown)'
  if (!modelReports.has(modelName)) {
    modelReports.set(modelName, {
      model: modelName,
      localFiles: 0,
      localVideos: 0,
      nasFiles: 0,
      nasVideos: 0,
      quarantineFiles: 0,
      quarantineVideos: 0,
      bitwiseRefs: 0,
      bitwiseVideoRefs: 0,
      bitwiseMissing: 0,
      bitwiseMissingVideos: 0,
      visualRefs: 0,
      visualVideoRefs: 0,
      visualMissing: 0,
      visualMissingVideos: 0,
      seenRefs: 0,
      seenVideoRefs: 0,
      seenMissing: 0,
      seenMissingVideos: 0,
      logRefs: 0,
      logVideoRefs: 0,
      logMissing: 0,
      logMissingVideos: 0,
    })
  }
  return modelReports.get(modelName)
}

function finalizeModelReport(report, missingRows) {
  const modelMissing = missingRows.filter(
    (entry) => entry.model === report.model
  )
  const missingVideoRefs = modelMissing.filter((entry) => entry.isVideo).length
  const staleActiveMissingRefs = modelMissing.filter(
    (entry) => entry.currentLogicMayTreatActive
  ).length
  return {
    ...report,
    missingRefs: modelMissing.length,
    missingVideoRefs,
    staleActiveMissingRefs,
    missingQuarantineRefs: modelMissing.filter(
      (entry) => entry.existsQuarantine
    ).length,
  }
}

function buildSummary({
  modelRows,
  missingRows,
  localFiles,
  nasFiles,
  quarantineFiles,
  nasIndex,
  staleNasIndex,
}) {
  return {
    models: modelRows.length,
    modelsWithMissingRefs: modelRows.filter((row) => row.missingRefs > 0)
      .length,
    modelsWithMissingVideoRefs: modelRows.filter(
      (row) => row.missingVideoRefs > 0
    ).length,
    localFiles: localFiles.size,
    nasFiles: nasFiles.size,
    quarantineFiles: quarantineFiles.size,
    nasIndexEntries: nasIndex.size,
    staleNasIndexEntries: staleNasIndex.length,
    missingRefs: missingRows.length,
    missingVideoRefs: missingRows.filter((entry) => entry.isVideo).length,
    missingQuarantineRefs: missingRows.filter((entry) => entry.existsQuarantine)
      .length,
    staleActiveMissingRefs: missingRows.filter(
      (entry) => entry.currentLogicMayTreatActive
    ).length,
  }
}

function formatMissingCsvRow(entry) {
  return {
    relativePath: entry.relativePath,
    model: entry.model,
    ext: entry.ext,
    isVideo: entry.isVideo,
    sources: entry.sources.join('|'),
    eventTypes: entry.eventTypes.join('|'),
    existsLocal: entry.existsLocal,
    existsNas: entry.existsNas,
    inNasIndex: entry.inNasIndex,
    existsQuarantine: entry.existsQuarantine,
    currentLogicMayTreatActive: entry.currentLogicMayTreatActive,
    lastLoggedAt: entry.lastLoggedAt || '',
    sampleUrls: entry.sampleUrls.join('|'),
  }
}

function serializeMissingEntry(entry) {
  return {
    ...entry,
    sources: [...entry.sources].sort(),
    eventTypes: [...entry.eventTypes].sort(),
    sampleUrls: [...entry.sampleUrls],
  }
}

function writeCsv(filePath, rows) {
  const headers = rows.length ? Object.keys(rows[0]) : []
  const lines = [headers.join(',')]
  for (const row of rows) {
    lines.push(headers.map((header) => csvCell(row[header])).join(','))
  }
  fs.writeFileSync(filePath, lines.join('\n') + '\n')
}

function csvCell(value) {
  if (value instanceof Set) value = [...value].join('|')
  const text = String(value ?? '')
  if (!/[",\n\r]/.test(text)) return text
  return `"${text.replace(/"/g, '""')}"`
}

function listModels(rootPath) {
  if (!fs.existsSync(rootPath)) return []
  return fs
    .readdirSync(rootPath, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b))
}

function normalizeRef(ref) {
  if (typeof ref === 'string') return normalizePath(ref)
  if (ref && typeof ref === 'object')
    return normalizePath(ref.relativePath || '')
  return ''
}

function normalizePath(value) {
  return String(value || '')
    .trim()
    .replace(/\\/g, '/')
}

function isMediaPath(relativePath) {
  return mediaExts.has(path.extname(relativePath).toLowerCase())
}

function getModelName(relativePath) {
  return normalizePath(relativePath).split('/')[0] || '(unknown)'
}

function addSample(values, value) {
  if (!value || values.size >= sampleSize) return
  values.add(String(value))
}

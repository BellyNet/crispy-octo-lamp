'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const minimist = require('minimist')
const { createDatasetPaths } = require('../scrapyard/datasetPaths')
const { formatBytes } = require('../scrapyard/runLifecycle')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  boolean: ['help'],
  default: {
    hours: 24,
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
const modelFilter = String(argv.model || '')
  .split(',')
  .map((value) => value.trim().toLowerCase())
  .filter(Boolean)
const sinceMs = getSinceMs(argv)
const outputPath = path.resolve(
  String(
    argv.output ||
      path.join(process.cwd(), 'tmp', 'scrape-duplicate-audit-latest.json')
  )
)
const datasetPaths = createDatasetPaths({ datasetDir: datasetRoot })

main()

function main() {
  const logPaths = collectRunLogs()
  const report = buildReport(logPaths)

  fs.mkdirSync(path.dirname(outputPath), { recursive: true })
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`)
  printSummary(report)
}

function printHelp() {
  console.log(`Usage: npm run audit:scrape-dupes -- [options]

Read-only audit of duplicate decisions recorded by recent scrape run logs.

Options:
  --hours <n>          Include logs modified in the last n hours. Default: 24.
  --since <date>       Include logs on or after an ISO date/time.
  -m, --model <names>  Comma-separated canonical model filter.
  --dataset-root <p>   Override the local dataset root.
  --output <path>      JSON report path.
  -h, --help           Show help.
`)
}

function getSinceMs(options) {
  if (options.since) {
    const parsed = Date.parse(String(options.since))
    if (!Number.isFinite(parsed)) {
      throw new Error(`Invalid --since value: ${options.since}`)
    }
    return parsed
  }

  const hours = Number(options.hours)
  if (!Number.isFinite(hours) || hours <= 0) {
    throw new Error(`Invalid --hours value: ${options.hours}`)
  }
  return Date.now() - hours * 60 * 60 * 1000
}

function collectRunLogs() {
  if (!fs.existsSync(datasetRoot)) return []
  const paths = []

  for (const modelEntry of fs.readdirSync(datasetRoot, {
    withFileTypes: true,
  })) {
    if (!modelEntry.isDirectory()) continue
    const modelName = modelEntry.name
    if (
      modelFilter.length > 0 &&
      !modelFilter.includes(modelName.toLowerCase())
    ) {
      continue
    }

    const logDir = path.join(datasetRoot, modelName, 'log')
    if (!fs.existsSync(logDir)) continue
    for (const fileName of fs.readdirSync(logDir)) {
      if (!/^(?:hoghaul|milkmaid)-run-.+\.jsonl$/i.test(fileName)) continue
      const logPath = path.join(logDir, fileName)
      if (fs.statSync(logPath).mtimeMs < sinceMs) continue
      paths.push({ modelName, logPath })
    }
  }

  return paths.sort((left, right) => left.logPath.localeCompare(right.logPath))
}

function buildReport(logPaths) {
  const report = {
    generatedAt: new Date().toISOString(),
    filters: {
      since: new Date(sinceMs).toISOString(),
      models: modelFilter,
    },
    summary: emptyCounters(),
    models: {},
    runs: [],
    duplicateDecisions: [],
  }

  for (const item of logPaths) {
    const run = readRun(item)
    if (!run) continue
    report.runs.push(run.summary)
    mergeCounters(report.summary, run.summary)

    if (!report.models[item.modelName]) {
      report.models[item.modelName] = {
        ...emptyCounters(),
        runs: 0,
      }
    }
    report.models[item.modelName].runs += 1
    mergeCounters(report.models[item.modelName], run.summary)
    report.duplicateDecisions.push(...run.decisions)
  }

  report.summary.logFiles = logPaths.length
  report.summary.completedRuns = report.runs.filter(
    (run) => run.finished
  ).length
  report.summary.incompleteRuns =
    report.runs.length - report.summary.completedRuns
  const postDownloadDecisions = report.duplicateDecisions.filter(
    (decision) => decision.postDownload
  )
  report.summary.uniqueDuplicateMediaUrls = new Set(
    postDownloadDecisions.map((decision) => decision.mediaUrl).filter(Boolean)
  ).size
  const postDownloadUrlCounts = new Map()
  for (const decision of postDownloadDecisions) {
    if (!decision.mediaUrl) continue
    postDownloadUrlCounts.set(
      decision.mediaUrl,
      (postDownloadUrlCounts.get(decision.mediaUrl) || 0) + 1
    )
  }
  report.summary.repeatedPostDownloadUrls = [
    ...postDownloadUrlCounts.values(),
  ].filter((count) => count > 1).length
  report.summary.repeatedPostDownloadEvents = [
    ...postDownloadUrlCounts.values(),
  ].reduce((sum, count) => sum + Math.max(count - 1, 0), 0)
  report.summary.fuzzyDistanceHistogram = buildFuzzyDistanceHistogram(
    postDownloadDecisions
  )

  report.models = Object.fromEntries(
    Object.entries(report.models).sort((left, right) => {
      return (
        right[1].postDownloadDuplicates - left[1].postDownloadDuplicates ||
        left[0].localeCompare(right[0])
      )
    })
  )

  return report
}

function emptyCounters() {
  return {
    logFiles: 0,
    completedRuns: 0,
    incompleteRuns: 0,
    saved: 0,
    earlySkips: 0,
    exactBitwise: 0,
    exactVisual: 0,
    fuzzyVisual: 0,
    pendingVisual: 0,
    postDownloadDuplicates: 0,
    duplicateTargetMissing: 0,
    downloadBytes: 0,
    savedBytes: 0,
    measuredDuplicateDownloadBytes: 0,
    estimatedDuplicateDownloadBytes: 0,
    uniqueDuplicateMediaUrls: 0,
  }
}

function readRun({ modelName, logPath }) {
  const events = []
  for (const line of fs.readFileSync(logPath, 'utf8').split(/\r?\n/)) {
    if (!line.trim()) continue
    try {
      events.push(JSON.parse(line))
    } catch {}
  }
  if (events.length === 0) return null

  const finished = [...events]
    .reverse()
    .find((event) => event.type === 'run_finished')
  const started = events.find((event) => event.type === 'run_started')
  const discoveryStarted = events.find(
    (event) => event.type === 'source_discovery_started'
  )
  const counters = finished?.counters || {}
  const transfer = finished?.transfer || {}
  const summary = {
    logPath,
    modelName,
    sourceSite:
      discoveryStarted?.site || started?.sourceSite || started?.site || null,
    inputUrl: started?.inputUrl || finished?.inputUrl || null,
    startedAt: started?.at || null,
    finishedAt: finished?.at || null,
    finished: Boolean(finished),
    saved: Number(counters.saved ?? finished?.successCount ?? 0) || 0,
    earlySkips: 0,
    exactBitwise: 0,
    exactVisual: 0,
    fuzzyVisual: 0,
    pendingVisual: 0,
    postDownloadDuplicates: 0,
    duplicateTargetMissing: 0,
    downloadBytes: Number(transfer.downloadBytes || 0),
    savedBytes: Number(transfer.savedBytes ?? finished?.savedBytes ?? 0),
    measuredDuplicateDownloadBytes: Number(
      transfer.duplicateDownloadBytes || 0
    ),
    estimatedDuplicateDownloadBytes: 0,
  }
  const decisions = []

  for (const event of events) {
    const classification = classifyEvent(event.type)
    if (!classification) continue
    summary[classification.counter] += 1
    if (classification.postDownload) summary.postDownloadDuplicates += 1

    const savedPath = normalizeRelativePath(event.savedPath)
    const targetExists = savedPath ? duplicateTargetExists(savedPath) : null
    if (targetExists === false) summary.duplicateTargetMissing += 1

    decisions.push({
      at: event.at || null,
      modelName,
      sourceSite: event.sourceSite || summary.sourceSite,
      type: event.type,
      classification: classification.label,
      postDownload: classification.postDownload,
      filename: event.filename || null,
      mediaUrl: event.mediaUrl || null,
      mediaPageUrl: event.mediaPageUrl || null,
      postId: event.postId || null,
      savedPath,
      targetExists,
      hash: event.hash || null,
      visualHash: event.visualHash || null,
      matchedVisualHash: event.matchedVisualHash || null,
      visualDistance:
        Number.isFinite(Number(event.distance)) && event.distance !== null
          ? Number(event.distance)
          : event.type === 'duplicate_visual'
            ? 0
            : null,
    })
  }

  if (
    summary.measuredDuplicateDownloadBytes === 0 &&
    summary.postDownloadDuplicates > 0 &&
    summary.downloadBytes >= summary.savedBytes
  ) {
    summary.estimatedDuplicateDownloadBytes =
      summary.downloadBytes - summary.savedBytes
  }

  return { summary, decisions }
}

function buildFuzzyDistanceHistogram(decisions) {
  const histogram = {}
  for (const decision of decisions) {
    if (decision.type !== 'duplicate_visual_fuzzy') continue
    if (!Number.isFinite(decision.visualDistance)) continue
    const key = String(decision.visualDistance)
    histogram[key] = (histogram[key] || 0) + 1
  }
  return Object.fromEntries(
    Object.entries(histogram).sort(
      (left, right) => Number(left[0]) - Number(right[0])
    )
  )
}

function classifyEvent(type) {
  if (
    type === 'skip_seen_media' ||
    type === 'skip_stable_key' ||
    type === 'skip_lazy_existing' ||
    type === 'skip_existing_image' ||
    type === 'skip_existing_gif' ||
    type === 'skip_existing_video'
  ) {
    return { counter: 'earlySkips', label: 'early_skip', postDownload: false }
  }
  if (type === 'duplicate_bitwise') {
    return { counter: 'exactBitwise', label: 'exact_bytes', postDownload: true }
  }
  if (type === 'duplicate_visual') {
    return { counter: 'exactVisual', label: 'exact_visual', postDownload: true }
  }
  if (type === 'duplicate_visual_fuzzy') {
    return { counter: 'fuzzyVisual', label: 'near_visual', postDownload: true }
  }
  if (type === 'duplicate_visual_pending') {
    return {
      counter: 'pendingVisual',
      label: 'near_visual_same_run',
      postDownload: true,
    }
  }
  return null
}

function normalizeRelativePath(value) {
  const normalized = String(value || '')
    .trim()
    .replace(/\\/g, '/')
  return normalized || null
}

function duplicateTargetExists(relativePath) {
  return datasetPaths.existsLocallyOrOnNas(
    datasetPaths.toDatasetAbsolutePath(relativePath)
  )
}

function mergeCounters(target, source) {
  for (const key of Object.keys(emptyCounters())) {
    if (
      key === 'logFiles' ||
      key === 'completedRuns' ||
      key === 'incompleteRuns' ||
      key === 'uniqueDuplicateMediaUrls'
    ) {
      continue
    }
    target[key] = Number(target[key] || 0) + Number(source[key] || 0)
  }
}

function printSummary(report) {
  const summary = report.summary
  console.log('SCRAPE DUPLICATE AUDIT')
  console.log(
    `Runs: ${summary.completedRuns} complete, ${summary.incompleteRuns} incomplete`
  )
  console.log(`Saved: ${summary.saved}`)
  console.log(`Early skips: ${summary.earlySkips}`)
  console.log(
    `Post-download dupes: ${summary.postDownloadDuplicates} ` +
      `(bitwise ${summary.exactBitwise}, visual ${summary.exactVisual}, ` +
      `fuzzy ${summary.fuzzyVisual}, same-run ${summary.pendingVisual})`
  )
  console.log(
    `Repeated post-download URLs: ${summary.repeatedPostDownloadEvents} extra event(s) across ${summary.repeatedPostDownloadUrls} URL(s)`
  )
  if (Object.keys(summary.fuzzyDistanceHistogram).length > 0) {
    console.log(
      `Fuzzy distances: ${Object.entries(summary.fuzzyDistanceHistogram)
        .map(([distance, count]) => `${distance}:${count}`)
        .join(', ')}`
    )
  }
  console.log(
    `Duplicate download bytes: ${formatBytes(
      summary.measuredDuplicateDownloadBytes
    )} measured, ${formatBytes(
      summary.estimatedDuplicateDownloadBytes
    )} estimated from older logs`
  )
  console.log(`Missing duplicate targets: ${summary.duplicateTargetMissing}`)

  const topModels = Object.entries(report.models)
    .filter(([, stats]) => stats.postDownloadDuplicates > 0)
    .slice(0, 10)
  if (topModels.length > 0) {
    console.log('Top post-download duplicate models:')
    for (const [modelName, stats] of topModels) {
      console.log(
        `  ${modelName}: ${stats.postDownloadDuplicates} ` +
          `(bitwise ${stats.exactBitwise}, visual ${stats.exactVisual}, fuzzy ${stats.fuzzyVisual})`
      )
    }
  }
  console.log(`Report: ${outputPath}`)
}

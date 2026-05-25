'use strict'

const fs = require('fs')
const path = require('path')

function defaultRemoveFileIfExists(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return false
  fs.unlinkSync(filePath)
  return true
}

function appendRunEvent(runLog, type, payload = {}) {
  if (!runLog) return
  fs.appendFileSync(
    runLog.logPath,
    JSON.stringify({
      at: new Date().toISOString(),
      type,
      ...payload,
    }) + '\n'
  )
}

function createRunLog({
  source,
  modelName,
  inputUrl,
  folders,
  keepHistory = false,
  counters = {},
  transfer = {},
  removeFileIfExists = defaultRemoveFileIfExists,
}) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-')
  const logPath = path.join(folders.logDir, `${source}-run-${stamp}.jsonl`)
  const summaryPath = path.join(
    folders.logDir,
    `${source}-run-latest-summary.json`
  )
  const modelSummaryPath = path.join(folders.base, `${source}-last-run.json`)
  const runLog = {
    stamp,
    logPath,
    summaryPath,
    modelSummaryPath,
    modelName,
    inputUrl,
    keepHistory: Boolean(keepHistory),
    startedAt: new Date().toISOString(),
    counters: { ...counters },
    transfer: { ...transfer },
    errors: [],
  }

  removeFileIfExists(modelSummaryPath)
  writeRunSnapshot(runLog)

  appendRunEvent(runLog, 'run_started', {
    modelName,
    inputUrl,
    logPath,
  })

  return runLog
}

function recordRunError(runLog, category, details = {}) {
  if (!runLog) return
  runLog.errors.push({
    at: new Date().toISOString(),
    category,
    ...details,
  })
  writeRunSnapshot(runLog)
}

function incrementRunCounter(runLog, name, delta = 1) {
  if (!runLog?.counters || !name) return
  runLog.counters[name] =
    Number(runLog.counters[name] || 0) + (Number(delta) || 0)
}

function setRunCounter(runLog, name, value) {
  if (!runLog?.counters || !name) return
  runLog.counters[name] = Number(value) || 0
  writeRunSnapshot(runLog)
}

function addRunTransfer(runLog, name, bytes) {
  if (!runLog?.transfer || !name) return
  runLog.transfer[name] =
    Number(runLog.transfer[name] || 0) + (Number(bytes) || 0)
}

function setRunTransfer(runLog, name, bytes) {
  if (!runLog?.transfer || !name) return
  runLog.transfer[name] = Number(bytes) || 0
  writeRunSnapshot(runLog)
}

function writeRunSnapshot(runLog, extra = {}) {
  if (!runLog?.modelSummaryPath) return
  const { status = 'running', ...rest } = extra
  const summary = {
    startedAt: runLog.startedAt,
    updatedAt: new Date().toISOString(),
    modelName: runLog.modelName,
    inputUrl: runLog.inputUrl,
    logPath: runLog.logPath,
    counters: runLog.counters,
    transfer: runLog.transfer,
    errors: runLog.errors,
    ...rest,
    status,
  }
  fs.writeFileSync(
    runLog.modelSummaryPath,
    JSON.stringify(summary, null, 2) + '\n'
  )
}

function getRunCounters(runLog) {
  return runLog?.counters ? { ...runLog.counters } : null
}

function formatPercent(numerator, denominator) {
  if (!Number.isFinite(denominator) || denominator <= 0) return '0.0'
  return ((numerator / denominator) * 100).toFixed(1)
}

function getRunProgressStats(runLog, fallback = {}) {
  const counters = getRunCounters(runLog) || {}
  const transfer = runLog?.transfer || fallback.transfer || {}
  const processed = Number(counters.processed ?? fallback.processed ?? 0) || 0
  const expectedMedia =
    Number(counters.expectedMedia ?? fallback.expectedMedia ?? 0) || 0
  const saved = Number(counters.saved ?? fallback.saved ?? 0) || 0
  const skipped = Number(counters.skipped ?? fallback.skipped ?? 0) || 0
  const duplicates =
    Number(counters.duplicates ?? fallback.duplicates ?? 0) || 0
  const failures = Number(counters.failures ?? fallback.failures ?? 0) || 0
  const savedBytes =
    Number(transfer.savedBytes ?? fallback.savedBytes ?? 0) || 0
  const remaining = Math.max(expectedMedia - processed, 0)

  return {
    processed,
    expectedMedia,
    saved,
    skipped,
    duplicates,
    failures,
    savedBytes,
    remaining,
    percent: formatPercent(processed, expectedMedia),
  }
}

function formatBytes(bytes) {
  const value = Number(bytes) || 0
  if (value >= 1024 ** 3) return `${(value / 1024 ** 3).toFixed(2)} GB`
  if (value >= 1024 ** 2) return `${(value / 1024 ** 2).toFixed(2)} MB`
  if (value >= 1024) return `${(value / 1024).toFixed(1)} KB`
  return `${value} B`
}

function formatRunProgressLine(stats, context = '') {
  const suffix = context ? ` :: ${context}` : ''
  return `Progress: ${stats.processed}/${stats.expectedMedia} (${stats.percent}%) | saved ${stats.saved} | skipped ${stats.skipped} | dupes ${stats.duplicates} | failed ${stats.failures} | remaining ${stats.remaining}${suffix}`
}

function formatRunSummaryLine(stats) {
  return `Done: ${stats.processed}/${stats.expectedMedia} processed | downloaded ${stats.saved} (${formatBytes(stats.savedBytes)}) | skipped ${stats.skipped} | dupes ${stats.duplicates} | failed ${stats.failures}`
}

function noteMediaOutcome(runLog, kind) {
  if (!runLog) return
  incrementRunCounter(runLog, 'processed')
  if (kind === 'saved') {
    incrementRunCounter(runLog, 'saved')
  } else if (kind === 'skipped') {
    incrementRunCounter(runLog, 'skipped')
  } else if (kind === 'duplicate') {
    incrementRunCounter(runLog, 'duplicates')
  } else if (kind === 'failed') {
    incrementRunCounter(runLog, 'failures')
  }
  writeRunSnapshot(runLog)
}

function finalizeRunLog(runLog, extra = {}, options = {}) {
  if (!runLog) return null

  const {
    removeFileIfExists = defaultRemoveFileIfExists,
    summaryTrailingNewline = false,
  } = options
  const { status = 'finished', ...rest } = extra
  const finishedAt = new Date().toISOString()
  const durationMs = Math.max(
    new Date(finishedAt).getTime() - new Date(runLog.startedAt).getTime(),
    0
  )
  const summary = {
    startedAt: runLog.startedAt,
    finishedAt,
    durationMs,
    modelName: runLog.modelName,
    inputUrl: runLog.inputUrl,
    logPath: runLog.logPath,
    counters: runLog.counters,
    transfer: runLog.transfer,
    errors: runLog.errors,
    ...rest,
  }

  const summaryPayload = JSON.stringify(summary, null, 2)
  fs.writeFileSync(
    runLog.summaryPath,
    summaryTrailingNewline ? `${summaryPayload}\n` : summaryPayload
  )
  fs.writeFileSync(
    runLog.modelSummaryPath,
    JSON.stringify(
      {
        ...summary,
        status,
      },
      null,
      2
    ) + '\n'
  )

  const shouldKeepHistory = runLog.keepHistory || runLog.errors.length > 0
  if (!shouldKeepHistory) removeFileIfExists(runLog.logPath)
  return null
}

module.exports = {
  addRunTransfer,
  appendRunEvent,
  createRunLog,
  finalizeRunLog,
  formatRunProgressLine,
  formatRunSummaryLine,
  getRunCounters,
  getRunProgressStats,
  incrementRunCounter,
  noteMediaOutcome,
  recordRunError,
  setRunCounter,
  setRunTransfer,
  writeRunSnapshot,
  formatBytes,
}

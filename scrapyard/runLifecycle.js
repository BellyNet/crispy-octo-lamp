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
  fs.writeFileSync(
    modelSummaryPath,
    JSON.stringify(
      {
        startedAt: runLog.startedAt,
        modelName,
        inputUrl,
        status: 'running',
      },
      null,
      2
    ) + '\n'
  )

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
}

function incrementRunCounter(runLog, name, delta = 1) {
  if (!runLog?.counters || !name) return
  runLog.counters[name] =
    Number(runLog.counters[name] || 0) + (Number(delta) || 0)
}

function setRunCounter(runLog, name, value) {
  if (!runLog?.counters || !name) return
  runLog.counters[name] = Number(value) || 0
}

function addRunTransfer(runLog, name, bytes) {
  if (!runLog?.transfer || !name) return
  runLog.transfer[name] =
    Number(runLog.transfer[name] || 0) + (Number(bytes) || 0)
}

function setRunTransfer(runLog, name, bytes) {
  if (!runLog?.transfer || !name) return
  runLog.transfer[name] = Number(bytes) || 0
}

function getRunCounters(runLog) {
  return runLog?.counters ? { ...runLog.counters } : null
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
  getRunCounters,
  incrementRunCounter,
  noteMediaOutcome,
  recordRunError,
  setRunCounter,
  setRunTransfer,
}

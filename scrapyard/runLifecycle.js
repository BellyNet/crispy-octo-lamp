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
  appendRunEvent,
  createRunLog,
  finalizeRunLog,
  recordRunError,
}

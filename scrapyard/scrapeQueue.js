'use strict'

// File-based scrape queue on the NAS share, shared by the NAS dashboard and
// the PC worker. No server or credentials needed: both sides already have
// the share. Layout under config.scrapeQueueDir:
//
//   runs/<runId>.json          a run and its tasks (one task per source URL)
//   runs/<runId>.cancel        cancel request (written by the dashboard)
//   logs/<runId>/<taskId>.log  output of each task
//   workers/<id>.json          worker heartbeats ("is the PC online?")
//   lock.json                  the global scrape lock
//
// Only one scrape runs at a time anywhere, because every scrape rewrites the
// shared dedup hash stores. The lock is created with an exclusive create,
// which is atomic on the NAS's own disk and over SMB. Only the lock holder
// modifies run files (task status), so they never need merging; the
// dashboard only creates runs and drops cancel markers.

const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')

const config = require('./config')

const LOCK_STALE_MS = 3 * 60 * 1000
const WORKER_ONLINE_MS = 90 * 1000
const MAX_TASK_ATTEMPTS = 2
const TERMINAL = new Set(['done', 'failed', 'canceled'])

function queuePaths(queueDir = config.scrapeQueueDir) {
  return {
    queueDir,
    runsDir: path.join(queueDir, 'runs'),
    logsDir: path.join(queueDir, 'logs'),
    workersDir: path.join(queueDir, 'workers'),
    lockPath: path.join(queueDir, 'lock.json'),
  }
}

function ensureQueueDirs(queueDir) {
  const p = queuePaths(queueDir)
  for (const dir of [p.runsDir, p.logsDir, p.workersDir]) {
    fs.mkdirSync(dir, { recursive: true })
  }
  return p
}

function readJson(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'))
  } catch {
    return null
  }
}

// Write-then-rename so readers never see half a file.
function writeJsonAtomic(filePath, data) {
  const tmp = `${filePath}.${process.pid}.${crypto.randomBytes(4).toString('hex')}.tmp`
  fs.writeFileSync(tmp, JSON.stringify(data, null, 1))
  fs.renameSync(tmp, filePath)
}

function newId() {
  const stamp = new Date()
    .toISOString()
    .replace(/[-:]/g, '')
    .replace(/\..*/, '')
  return `${stamp}-${crypto.randomBytes(3).toString('hex')}`
}

// ── Runs ─────────────────────────────────────────────────────────────────────

// tasks: [{ model, url, sourceId, sourceLabel, requiresBrowser }]
function createRun({ label, scope, tasks, createdBy = 'dashboard' }, queueDir) {
  const p = ensureQueueDirs(queueDir)
  const run = {
    id: newId(),
    label,
    scope,
    createdBy,
    createdAt: new Date().toISOString(),
    tasks: tasks.map((task, index) => ({
      id: `t${String(index + 1).padStart(4, '0')}`,
      model: task.model || null,
      url: task.url,
      sourceId: task.sourceId || null,
      sourceLabel: task.sourceLabel || task.sourceId || 'source',
      requiresBrowser: Boolean(task.requiresBrowser),
      status: 'queued',
      attempts: 0,
      worker: null,
      startedAt: null,
      finishedAt: null,
      exitCode: null,
      summary: null,
    })),
  }
  writeJsonAtomic(path.join(p.runsDir, `${run.id}.json`), run)
  return run
}

function readRun(runId, queueDir) {
  const p = queuePaths(queueDir)
  return readJson(path.join(p.runsDir, `${runId}.json`))
}

function saveRun(run, queueDir) {
  const p = queuePaths(queueDir)
  writeJsonAtomic(path.join(p.runsDir, `${run.id}.json`), run)
}

function isCancelRequested(runId, queueDir) {
  return fs.existsSync(
    path.join(queuePaths(queueDir).runsDir, `${runId}.cancel`)
  )
}

function requestCancel(runId, queueDir) {
  const p = ensureQueueDirs(queueDir)
  fs.writeFileSync(
    path.join(p.runsDir, `${runId}.cancel`),
    new Date().toISOString()
  )
}

function listRunIds(queueDir) {
  const p = queuePaths(queueDir)
  try {
    return fs
      .readdirSync(p.runsDir)
      .filter((name) => name.endsWith('.json'))
      .map((name) => name.slice(0, -5))
      .sort()
  } catch {
    return []
  }
}

function summarizeRun(run, { canceled = false } = {}) {
  const counts = { queued: 0, running: 0, done: 0, failed: 0, canceled: 0 }
  const totals = { saved: 0, duplicates: 0, errors: 0, downloadBytes: 0 }
  let waitingForBrowser = 0
  for (const task of run.tasks) {
    counts[task.status] = (counts[task.status] || 0) + 1
    if (task.status === 'queued' && task.requiresBrowser) waitingForBrowser += 1
    totals.saved += task.summary?.saved || 0
    totals.duplicates += task.summary?.duplicates || 0
    totals.errors += task.summary?.errors || 0
    totals.downloadBytes += task.summary?.downloadBytes || 0
  }
  const active = counts.queued + counts.running
  const status = active
    ? counts.running
      ? 'running'
      : canceled
        ? 'canceling'
        : 'queued'
    : canceled
      ? 'canceled'
      : counts.failed
        ? 'finished-with-errors'
        : 'done'
  const times = run.tasks
    .map((task) => task.finishedAt)
    .filter(Boolean)
    .sort()
  return {
    id: run.id,
    label: run.label,
    scope: run.scope,
    createdBy: run.createdBy,
    createdAt: run.createdAt,
    finishedAt: active ? null : times[times.length - 1] || run.createdAt,
    status,
    counts,
    total: run.tasks.length,
    waitingForBrowser,
    totals,
    current: run.tasks.find((task) => task.status === 'running') || null,
  }
}

function listRunSummaries({ limit = 30 } = {}, queueDir) {
  return listRunIds(queueDir)
    .reverse()
    .slice(0, limit)
    .map((id) => {
      const run = readRun(id, queueDir)
      return run
        ? summarizeRun(run, { canceled: isCancelRequested(id, queueDir) })
        : null
    })
    .filter(Boolean)
}

function activeRunExists(queueDir) {
  return listRunIds(queueDir).some((id) => {
    const run = readRun(id, queueDir)
    return run?.tasks.some((task) => !TERMINAL.has(task.status))
  })
}

// ── Workers ──────────────────────────────────────────────────────────────────

function heartbeatWorker(worker, extra = {}, queueDir) {
  const p = ensureQueueDirs(queueDir)
  writeJsonAtomic(path.join(p.workersDir, `${worker.id}.json`), {
    id: worker.id,
    label: worker.label || worker.id,
    host: os.hostname(),
    browser: Boolean(worker.browser),
    lastSeen: new Date().toISOString(),
    ...extra,
  })
}

function listWorkers(queueDir) {
  const p = queuePaths(queueDir)
  let names = []
  try {
    names = fs
      .readdirSync(p.workersDir)
      .filter((name) => name.endsWith('.json'))
  } catch {}
  const now = Date.now()
  return names
    .map((name) => readJson(path.join(p.workersDir, name)))
    .filter(Boolean)
    .map((worker) => ({
      ...worker,
      online: now - Date.parse(worker.lastSeen || 0) < WORKER_ONLINE_MS,
    }))
}

// ── Lock ─────────────────────────────────────────────────────────────────────

function readLock(queueDir) {
  return readJson(queuePaths(queueDir).lockPath)
}

function lockIsStale(lock) {
  return !lock || Date.now() - Date.parse(lock.heartbeatAt || 0) > LOCK_STALE_MS
}

// Returns true when this holder now owns the lock.
function tryAcquireLock(holder, queueDir) {
  const p = ensureQueueDirs(queueDir)
  const body = {
    holder: holder.id,
    host: os.hostname(),
    pid: process.pid,
    acquiredAt: new Date().toISOString(),
    heartbeatAt: new Date().toISOString(),
    runId: null,
    taskId: null,
    note: holder.note || null,
  }
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      fs.writeFileSync(p.lockPath, JSON.stringify(body), { flag: 'wx' })
      return true
    } catch (err) {
      if (err.code !== 'EEXIST') throw err
      const current = readLock(queueDir)
      if (!lockIsStale(current)) return false
      // The holder stopped heartbeating (crash, PC asleep). Move its lock
      // aside, and put it back if another worker already replaced it
      // between our read and the move; then requeue its task and retry.
      const aside = `${p.lockPath}.stale-${crypto.randomBytes(4).toString('hex')}`
      try {
        fs.renameSync(p.lockPath, aside)
      } catch {
        continue
      }
      const moved = readJson(aside)
      if (moved?.acquiredAt !== current?.acquiredAt) {
        try {
          fs.renameSync(aside, p.lockPath)
        } catch {}
        return false
      }
      fs.unlinkSync(aside)
      requeueInterruptedTask(current, queueDir)
    }
  }
  return false
}

function updateLock(holder, fields = {}, queueDir) {
  const p = queuePaths(queueDir)
  const current = readLock(queueDir)
  if (!current || current.holder !== holder.id) return false
  writeJsonAtomic(p.lockPath, {
    ...current,
    ...fields,
    heartbeatAt: new Date().toISOString(),
  })
  return true
}

function releaseLock(holder, queueDir) {
  const p = queuePaths(queueDir)
  const current = readLock(queueDir)
  if (current && current.holder !== holder.id) return
  try {
    fs.unlinkSync(p.lockPath)
  } catch {}
}

// For scrapes started outside the queue (`npm run scrape` on the PC): hold
// the global lock for the whole run so queued tasks wait instead of racing
// on the hash stores. Returns a release function; throws with
// code SCRAPE_LOCK_BUSY when another scrape holds the lock.
function lockBusyError(lock) {
  const where = lock?.runId
    ? ` (run ${lock.runId}, task ${lock.taskId})`
    : lock?.note
      ? ` (${lock.note})`
      : ''
  const err = new Error(
    `Another scrape is running on ${lock?.holder || 'another worker'}${where}. Wait for it to finish, or cancel it from the dashboard's Scrapes page.`
  )
  err.code = 'SCRAPE_LOCK_BUSY'
  return err
}

function holdScrapeLock(note, queueDir) {
  const holder = { id: `cli-${os.hostname()}-${process.pid}`, note }
  if (!tryAcquireLock(holder, queueDir)) throw lockBusyError(readLock(queueDir))
  const timer = setInterval(() => updateLock(holder, {}, queueDir), 20 * 1000)
  timer.unref?.()
  let released = false
  return () => {
    if (released) return
    released = true
    clearInterval(timer)
    releaseLock(holder, queueDir)
  }
}

function requeueInterruptedTask(lock, queueDir) {
  if (!lock?.runId || !lock?.taskId) return
  const run = readRun(lock.runId, queueDir)
  const task = run?.tasks.find((entry) => entry.id === lock.taskId)
  if (!task || task.status !== 'running') return
  if (task.attempts >= MAX_TASK_ATTEMPTS) {
    task.status = 'failed'
    task.finishedAt = new Date().toISOString()
    task.error = `Interrupted (worker ${lock.holder} stopped responding)`
  } else {
    task.status = 'queued'
    task.worker = null
  }
  saveRun(run, queueDir)
  appendTaskLog(
    run.id,
    task.id,
    `\n[queue] ${lock.holder} stopped responding; task ${task.status === 'queued' ? 're-queued' : 'marked failed'}.\n`,
    queueDir
  )
}

// ── Tasks (call only while holding the lock) ─────────────────────────────────

// Marks canceled tasks, then claims the next queued task this worker can run
// (oldest run first, run order within a run). Returns { run, task } or null.
function claimNextTask(worker, queueDir) {
  for (const runId of listRunIds(queueDir)) {
    const run = readRun(runId, queueDir)
    if (!run) continue
    if (isCancelRequested(runId, queueDir)) {
      let changed = false
      for (const task of run.tasks) {
        if (task.status === 'queued') {
          task.status = 'canceled'
          task.finishedAt = new Date().toISOString()
          changed = true
        }
      }
      if (changed) saveRun(run, queueDir)
      continue
    }
    const task = run.tasks.find(
      (entry) =>
        entry.status === 'queued' && (!entry.requiresBrowser || worker.browser)
    )
    if (!task) continue
    task.status = 'running'
    task.worker = worker.id
    task.attempts = (task.attempts || 0) + 1
    task.startedAt = new Date().toISOString()
    saveRun(run, queueDir)
    return { run, task }
  }
  return null
}

function finishTask(runId, taskId, fields, queueDir) {
  const run = readRun(runId, queueDir)
  const task = run?.tasks.find((entry) => entry.id === taskId)
  if (!task) return
  Object.assign(task, fields, { finishedAt: new Date().toISOString() })
  saveRun(run, queueDir)
}

// ── Logs ─────────────────────────────────────────────────────────────────────

function taskLogPath(runId, taskId, queueDir) {
  return path.join(queuePaths(queueDir).logsDir, runId, `${taskId}.log`)
}

function appendTaskLog(runId, taskId, text, queueDir) {
  const logPath = taskLogPath(runId, taskId, queueDir)
  fs.mkdirSync(path.dirname(logPath), { recursive: true })
  fs.appendFileSync(logPath, text)
}

// Last `bytes` of a task's log, starting at a line boundary.
function readTaskLogTail(runId, taskId, bytes = 64 * 1024, queueDir) {
  const logPath = taskLogPath(runId, taskId, queueDir)
  let fd
  try {
    fd = fs.openSync(logPath, 'r')
    const { size } = fs.fstatSync(fd)
    const start = Math.max(0, size - bytes)
    const buffer = Buffer.alloc(size - start)
    fs.readSync(fd, buffer, 0, buffer.length, start)
    let text = buffer.toString('utf8')
    if (start > 0) text = text.slice(text.indexOf('\n') + 1)
    return { text, size, truncated: start > 0 }
  } catch {
    return { text: '', size: 0, truncated: false }
  } finally {
    if (fd !== undefined) fs.closeSync(fd)
  }
}

module.exports = {
  LOCK_STALE_MS,
  WORKER_ONLINE_MS,
  queuePaths,
  createRun,
  readRun,
  saveRun,
  summarizeRun,
  listRunIds,
  listRunSummaries,
  activeRunExists,
  isCancelRequested,
  requestCancel,
  heartbeatWorker,
  listWorkers,
  readLock,
  lockIsStale,
  tryAcquireLock,
  updateLock,
  releaseLock,
  holdScrapeLock,
  lockBusyError,
  claimNextTask,
  finishTask,
  taskLogPath,
  appendTaskLog,
  readTaskLogTail,
}

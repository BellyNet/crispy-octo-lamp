'use strict'

// How a worker talks to the scrape queue.
//
// local:  direct file access. Only for processes on the NAS itself (the
//         dashboard's NAS worker, scrapes run in the NAS container), where
//         the queue folder is local disk.
// remote: HTTP to the dashboard's worker API. Used by the PC. Over SMB the
//         PC's file cache can serve stale copies of files the NAS changed
//         for minutes, so the PC never reads or writes queue files itself;
//         it only reads the worker token from the share once.
//
// Both expose the same async methods: heartbeat, claim, progress, finish,
// holdCliLock.

const crypto = require('crypto')
const fs = require('fs')
const path = require('path')

const config = require('./config')
const queue = require('./scrapeQueue')

const TOKEN_FILE = 'worker-token'

function taskOwnedBy(run, taskId, workerId) {
  const task = run?.tasks.find((entry) => entry.id === taskId)
  return Boolean(task && task.status === 'running' && task.worker === workerId)
}

function createLocalBackend(queueDir = config.scrapeQueueDir) {
  return {
    kind: 'local',
    async heartbeat(worker, extra = {}) {
      queue.heartbeatWorker(worker, extra, queueDir)
    },
    // Takes the global lock and claims the next task this worker can run.
    // Returns { run: { id, label }, task } or null (lock released).
    async claim(worker) {
      const holder = { id: worker.id }
      if (!queue.tryAcquireLock(holder, queueDir)) return null
      let claimed = null
      try {
        claimed = queue.claimNextTask(worker, queueDir)
      } finally {
        if (!claimed) queue.releaseLock(holder, queueDir)
      }
      if (!claimed) return null
      queue.updateLock(
        holder,
        { runId: claimed.run.id, taskId: claimed.task.id },
        queueDir
      )
      return {
        run: { id: claimed.run.id, label: claimed.run.label },
        task: claimed.task,
      }
    },
    // Appends output, keeps the lock alive. Returns { cancel } — true when
    // the run was canceled or the task is no longer this worker's (e.g. it
    // was re-queued while the PC slept).
    async progress(worker, runId, taskId, text = '') {
      if (text) queue.appendTaskLog(runId, taskId, text, queueDir)
      queue.updateLock({ id: worker.id }, { runId, taskId }, queueDir)
      const owned = taskOwnedBy(
        queue.readRun(runId, queueDir),
        taskId,
        worker.id
      )
      return { cancel: !owned || queue.isCancelRequested(runId, queueDir) }
    },
    // Records the result (only if the task is still this worker's) and
    // releases the lock.
    async finish(worker, runId, taskId, fields) {
      if (taskOwnedBy(queue.readRun(runId, queueDir), taskId, worker.id)) {
        queue.finishTask(runId, taskId, fields, queueDir)
      }
      queue.releaseLock({ id: worker.id }, queueDir)
    },
    // For CLI scrapes. Returns an async release function; throws with code
    // SCRAPE_LOCK_BUSY when busy.
    async holdCliLock(note) {
      const release = queue.holdScrapeLock(note, queueDir)
      return async () => release()
    },
  }
}

function readWorkerToken(queueDir = config.scrapeQueueDir) {
  return fs.readFileSync(path.join(queueDir, TOKEN_FILE), 'utf8').trim()
}

// Dashboard side: create the token on first start (readable only by the
// share user and group).
function ensureWorkerToken(queueDir = config.scrapeQueueDir) {
  const tokenPath = path.join(queueDir, TOKEN_FILE)
  try {
    return readWorkerToken(queueDir)
  } catch {}
  fs.mkdirSync(queueDir, { recursive: true })
  const token = crypto.randomBytes(32).toString('hex')
  fs.writeFileSync(tokenPath, `${token}\n`, { mode: 0o660 })
  return token
}

function createRemoteBackend({
  baseUrl = config.dashboardUrl,
  queueDir = config.scrapeQueueDir,
  timeoutMs = 20000,
} = {}) {
  let token = null
  async function post(endpoint, body) {
    if (!token) token = readWorkerToken(queueDir)
    const res = await fetch(`${baseUrl}/api/worker/${endpoint}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Worker-Token': token,
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs),
    })
    if (res.status === 401) token = null // re-read the token next time
    const data = await res.json().catch(() => ({}))
    if (!res.ok) {
      const err = new Error(data.error || `HTTP ${res.status} from ${endpoint}`)
      err.code = data.code
      throw err
    }
    return data
  }
  return {
    kind: 'remote',
    async heartbeat(worker, extra = {}) {
      await post('heartbeat', { worker, extra })
    },
    async claim(worker) {
      const data = await post('claim', { worker })
      return data.task ? { run: data.run, task: data.task } : null
    },
    async progress(worker, runId, taskId, text = '') {
      return post('progress', { worker, runId, taskId, text })
    },
    async finish(worker, runId, taskId, fields) {
      await post('finish', { worker, runId, taskId, fields })
    },
    async holdCliLock(note) {
      const holder = `cli-${require('os').hostname()}-${process.pid}`
      await post('lock', { action: 'acquire', holder, note })
      const timer = setInterval(() => {
        post('lock', { action: 'heartbeat', holder }).catch(() => {})
      }, 20 * 1000)
      timer.unref?.()
      let released = false
      return async () => {
        if (released) return
        released = true
        clearInterval(timer)
        await post('lock', { action: 'release', holder }).catch(() => {})
      }
    },
  }
}

// The backend for this process: local on the NAS (the image sets
// SCRAPE_QUEUE_MODE=local), remote everywhere else.
function defaultBackend() {
  return process.env.SCRAPE_QUEUE_MODE === 'local'
    ? createLocalBackend()
    : createRemoteBackend()
}

module.exports = {
  createLocalBackend,
  createRemoteBackend,
  defaultBackend,
  ensureWorkerToken,
  readWorkerToken,
}

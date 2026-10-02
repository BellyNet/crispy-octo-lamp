'use strict'

// Scrape worker: takes tasks from the shared queue (scrapeQueue.js) and runs
// them one at a time, streaming output into the task log the dashboard shows.
//
// The NAS dashboard starts one inside its own process (no browser, so it only
// takes Pawchive/OnlyHaven/Coomer tasks). The PC runs one in the background
// with a browser, so it takes the browser-only tasks:
//
//   node scrapyard/scrapeWorker.js --id=pc       (add --no-browser to skip browser tasks)

const fs = require('fs')
const os = require('os')
const path = require('path')
const { spawn, spawnSync } = require('child_process')

const config = require('./config')
const queue = require('./scrapeQueue')

// SCRAPE_TASK_SCRIPT lets tests substitute a fake task.
const TASK_SCRIPT =
  process.env.SCRAPE_TASK_SCRIPT || path.join(__dirname, 'runQueuedTask.js')
const LOCK_HEARTBEAT_MS = 20 * 1000
const CANCEL_CHECK_MS = 5 * 1000
const LOG_FLUSH_MS = 1000
const MAX_LOG_BYTES = 20 * 1024 * 1024

function stripAnsi(text) {
  return String(text).replace(/\x1B\[[0-?]*[ -/]*[@-~]/g, '')
}

function killTree(child) {
  if (!child?.pid) return
  if (process.platform === 'win32') {
    spawnSync('taskkill', ['/PID', String(child.pid), '/T', '/F'], {
      stdio: 'ignore',
    })
    return
  }
  try {
    process.kill(-child.pid, 'SIGTERM')
  } catch {
    try {
      child.kill('SIGTERM')
    } catch {}
  }
}

// options:
//   id, label, browser       identity and whether it can run browser tasks
//   queueDir                 defaults to config.scrapeQueueDir
//   pollMs                   how often to look for work
//   cooldownMs               pause after a task so the other worker gets a turn
//   childEnv                 extra environment for task processes
//   prepareTask()            called before each task (e.g. refresh registry copy)
//   log                      logger for the worker itself
function startWorker(options) {
  const worker = {
    id: options.id,
    label: options.label || options.id,
    browser: Boolean(options.browser),
  }
  const queueDir = options.queueDir || config.scrapeQueueDir
  const pollMs = options.pollMs || 6000
  const cooldownMs = options.cooldownMs || 0
  const log = options.log || console
  let stopped = false
  let busy = false
  let current = null
  let lastFinishedAt = 0
  let timer = null

  function heartbeat() {
    try {
      queue.heartbeatWorker(
        worker,
        {
          current: current
            ? {
                runId: current.run.id,
                taskId: current.task.id,
                model: current.task.model,
                sourceLabel: current.task.sourceLabel,
                startedAt: current.task.startedAt,
              }
            : null,
        },
        queueDir
      )
    } catch (err) {
      log.warn?.(`[worker ${worker.id}] heartbeat failed: ${err.message}`)
    }
  }

  async function runTask({ run, task }) {
    const holder = { id: worker.id }
    queue.updateLock(holder, { runId: run.id, taskId: task.id }, queueDir)
    const resultPath = path.join(
      os.tmpdir(),
      `scrape-task-${run.id}-${task.id}-${process.pid}.json`
    )
    let pending = ''
    let logBytes = 0
    let logCapped = false
    const flush = () => {
      if (!pending) return
      const text = pending
      pending = ''
      if (logCapped) return
      logBytes += Buffer.byteLength(text)
      if (logBytes > MAX_LOG_BYTES) {
        logCapped = true
        queue.appendTaskLog(
          run.id,
          task.id,
          '\n[queue] Log size limit reached; further output is not recorded.\n',
          queueDir
        )
        return
      }
      try {
        queue.appendTaskLog(run.id, task.id, text, queueDir)
      } catch (err) {
        log.warn?.(`[worker ${worker.id}] log write failed: ${err.message}`)
      }
    }
    const write = (text) => {
      pending += stripAnsi(text).replace(/\r(?!\n)/g, '\n')
    }

    write(
      `[queue] ${worker.label} started ${task.model} | ${task.sourceLabel} | ${task.url}` +
        `${task.attempts > 1 ? ` (attempt ${task.attempts})` : ''}\n`
    )
    flush()

    let canceled = false
    let child
    try {
      await options.prepareTask?.(task)
      child = spawn(
        process.execPath,
        [
          TASK_SCRIPT,
          `--model=${task.model}`,
          `--url=${task.url}`,
          `--result=${resultPath}`,
        ],
        {
          cwd: config.rootDir,
          env: {
            ...process.env,
            ...(options.childEnv || {}),
            MILKMAID_PROGRESS_MODE: 'plain',
            SCRAPE_LOCK_HELD: '1',
          },
          stdio: ['ignore', 'pipe', 'pipe'],
          windowsHide: true,
          detached: process.platform !== 'win32',
        }
      )
    } catch (err) {
      write(`[queue] Could not start the task: ${err.message}\n`)
      flush()
      queue.finishTask(
        run.id,
        task.id,
        { status: 'failed', exitCode: null, error: err.message },
        queueDir
      )
      return
    }

    child.stdout.on('data', write)
    child.stderr.on('data', write)
    const flushTimer = setInterval(flush, LOG_FLUSH_MS)
    const lockTimer = setInterval(() => {
      queue.updateLock(holder, { runId: run.id, taskId: task.id }, queueDir)
      heartbeat()
    }, LOCK_HEARTBEAT_MS)
    const cancelTimer = setInterval(() => {
      if (!canceled && queue.isCancelRequested(run.id, queueDir)) {
        canceled = true
        write('\n[queue] Cancel requested; stopping this task.\n')
        killTree(child)
      }
    }, CANCEL_CHECK_MS)

    const exitCode = await new Promise((resolve) => {
      child.on('error', (err) => {
        write(`[queue] Task process error: ${err.message}\n`)
        resolve(1)
      })
      child.on('exit', (code, signal) => resolve(code ?? (signal ? 130 : 1)))
    })
    clearInterval(flushTimer)
    clearInterval(lockTimer)
    clearInterval(cancelTimer)

    let result = null
    try {
      result = JSON.parse(fs.readFileSync(resultPath, 'utf8'))
      fs.unlinkSync(resultPath)
    } catch {}
    const source = result?.runs?.[0]
    const status = canceled ? 'canceled' : exitCode === 0 ? 'done' : 'failed'
    write(`\n[queue] ${worker.label} finished: ${status} (exit ${exitCode})\n`)
    flush()
    queue.finishTask(
      run.id,
      task.id,
      {
        status,
        exitCode,
        summary: source?.summary || null,
        error: source?.error || null,
      },
      queueDir
    )
  }

  async function tick() {
    if (stopped || busy) return
    heartbeat()
    if (cooldownMs && Date.now() - lastFinishedAt < cooldownMs) return
    const holder = { id: worker.id }
    let acquired = false
    try {
      acquired = queue.tryAcquireLock(holder, queueDir)
    } catch (err) {
      log.warn?.(`[worker ${worker.id}] queue unavailable: ${err.message}`)
      return
    }
    if (!acquired) return
    busy = true
    try {
      const claimed = queue.claimNextTask(worker, queueDir)
      if (!claimed) return
      current = claimed
      heartbeat()
      await runTask(claimed)
      lastFinishedAt = Date.now()
    } catch (err) {
      log.error?.(
        `[worker ${worker.id}] task error: ${err.stack || err.message}`
      )
    } finally {
      current = null
      queue.releaseLock(holder, queueDir)
      busy = false
      heartbeat()
    }
  }

  function schedule() {
    if (stopped) return
    timer = setTimeout(async () => {
      await tick()
      schedule()
    }, pollMs)
  }

  heartbeat()
  schedule()
  return {
    worker,
    isBusy: () => busy,
    current: () => current,
    stop() {
      stopped = true
      clearTimeout(timer)
    },
  }
}

// ── PC worker CLI ────────────────────────────────────────────────────────────
if (require.main === module) {
  const minimist = require('minimist')
  const argv = minimist(process.argv.slice(2), {
    string: ['id', 'label'],
    boolean: ['browser'],
    default: { browser: true },
  })
  const id = argv.id || 'pc'
  const logPath = path.join(config.slopvaultRoot, 'scrape-worker.log')
  const logLine = (level, message) => {
    const line = `${new Date().toISOString()} ${level} ${message}\n`
    try {
      fs.mkdirSync(path.dirname(logPath), { recursive: true })
      fs.appendFileSync(logPath, line)
    } catch {}
    if (process.stdout.isTTY) process.stdout.write(line)
  }
  logLine('info', `worker ${id} starting; queue ${config.scrapeQueueDir}`)
  startWorker({
    id,
    label: argv.label || `${os.hostname()} (PC)`,
    browser: argv.browser,
    pollMs: 6000,
    log: {
      warn: (message) => logLine('warn', message),
      error: (message) => logLine('error', message),
    },
  })
}

module.exports = { startWorker }

'use strict'

// Scrape worker: takes tasks from the scrape queue and runs them one at a
// time, streaming output into the task log the dashboard shows.
//
// The NAS dashboard starts one inside its own process (local backend, no
// browser, so it only takes Pawchive/OnlyHaven/Coomer tasks). The PC runs
// one in the background (remote backend, over the dashboard's worker API)
// with a browser, so it takes StufferDB, Tumblr and Reddit tasks:
//
//   node scrapyard/scrapeWorker.js --id=pc     (--no-browser to skip those)

const fs = require('fs')
const os = require('os')
const path = require('path')
const { spawn, spawnSync } = require('child_process')

const config = require('./config')
const { defaultBackend } = require('./scrapeBackends')

// SCRAPE_TASK_SCRIPT lets tests substitute a fake task.
const TASK_SCRIPT =
  process.env.SCRAPE_TASK_SCRIPT || path.join(__dirname, 'runQueuedTask.js')
// How often output is sent, which is also the lock heartbeat and the cancel
// check while a task runs.
const PROGRESS_MS = 2000
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
//   id, label, browser   identity and whether it can run browser tasks
//   backend              scrapeBackends local/remote (default: by environment)
//   pollMs               how often to look for work when idle
//   cooldownMs           pause after a task so the other worker gets a turn
//   childEnv             extra environment for task processes
//   prepareTask(task)    called before each task (e.g. refresh registry copy)
//   log                  { warn, error } for the worker itself
function startWorker(options) {
  const worker = {
    id: options.id,
    label: options.label || options.id,
    browser: Boolean(options.browser),
  }
  const backend = options.backend || defaultBackend()
  const pollMs = options.pollMs || 6000
  const cooldownMs = options.cooldownMs || 0
  const log = options.log || console
  let stopped = false
  let busy = false
  let current = null
  let lastFinishedAt = 0
  let timer = null
  let lastError = null

  function currentInfo() {
    return current
      ? {
          runId: current.run.id,
          taskId: current.task.id,
          model: current.task.model,
          sourceLabel: current.task.sourceLabel,
          startedAt: current.task.startedAt,
        }
      : null
  }

  async function heartbeat() {
    try {
      await backend.heartbeat(worker, { current: currentInfo() })
      lastError = null
    } catch (err) {
      // Log once per distinct failure (NAS offline, dashboard restarting).
      if (err.message !== lastError) {
        lastError = err.message
        log.warn?.(`[worker ${worker.id}] queue unavailable: ${err.message}`)
      }
      throw err
    }
  }

  async function runTask({ run, task }) {
    let pending = ''
    let logBytes = 0
    let logCapped = false
    let canceled = false
    let child = null
    const write = (text) => {
      pending += stripAnsi(text).replace(/\r(?!\n)/g, '\n')
    }
    // Sends buffered output; doubles as lock heartbeat and cancel check.
    // Chained so updates go out one at a time, in order.
    let progressChain = Promise.resolve()
    const sendProgress = () => {
      progressChain = progressChain.then(doSendProgress)
      return progressChain
    }
    const doSendProgress = async () => {
      let text = pending
      pending = ''
      if (logCapped) text = ''
      logBytes += Buffer.byteLength(text)
      if (!logCapped && logBytes > MAX_LOG_BYTES) {
        logCapped = true
        text +=
          '\n[queue] Log size limit reached; further output is not recorded.\n'
      }
      try {
        const { cancel } = await backend.progress(worker, run.id, task.id, text)
        if (cancel && !canceled && child) {
          canceled = true
          write(
            '\n[queue] Cancel requested (or the task was reassigned); stopping it.\n'
          )
          killTree(child)
        }
      } catch (err) {
        pending = text + pending // keep it for the next attempt
        log.warn?.(
          `[worker ${worker.id}] progress update failed: ${err.message}`
        )
      }
    }

    const resultPath = path.join(
      os.tmpdir(),
      `scrape-task-${run.id}-${task.id}-${process.pid}.json`
    )
    write(
      `[queue] ${worker.label} started ${task.model} | ${task.sourceLabel} | ${task.url}` +
        `${task.attempts > 1 ? ` (attempt ${task.attempts})` : ''}\n`
    )
    await sendProgress()

    let exitCode
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
      child.stdout.on('data', write)
      child.stderr.on('data', write)
      const progressTimer = setInterval(sendProgress, PROGRESS_MS)
      exitCode = await new Promise((resolve) => {
        child.on('error', (err) => {
          write(`[queue] Task process error: ${err.message}\n`)
          resolve(1)
        })
        child.on('exit', (code, signal) => resolve(code ?? (signal ? 130 : 1)))
      })
      clearInterval(progressTimer)
    } catch (err) {
      write(`[queue] Could not start the task: ${err.message}\n`)
      exitCode = null
    }

    let result = null
    try {
      result = JSON.parse(fs.readFileSync(resultPath, 'utf8'))
      fs.unlinkSync(resultPath)
    } catch {}
    const source = result?.runs?.[0]
    const status = canceled ? 'canceled' : exitCode === 0 ? 'done' : 'failed'
    write(`\n[queue] ${worker.label} finished: ${status} (exit ${exitCode})\n`)
    await sendProgress()
    await backend.finish(worker, run.id, task.id, {
      status,
      exitCode,
      summary: source?.summary || null,
      error: source?.error || null,
    })
  }

  async function tick() {
    if (stopped || busy) return
    try {
      await heartbeat()
    } catch {
      return
    }
    if (cooldownMs && Date.now() - lastFinishedAt < cooldownMs) return
    busy = true
    try {
      const claimed = await backend.claim(worker)
      if (!claimed) return
      current = claimed
      await heartbeat().catch(() => {})
      await runTask(claimed)
      lastFinishedAt = Date.now()
    } catch (err) {
      log.error?.(
        `[worker ${worker.id}] task error: ${err.stack || err.message}`
      )
    } finally {
      current = null
      busy = false
      heartbeat().catch(() => {})
    }
  }

  function schedule() {
    if (stopped) return
    timer = setTimeout(async () => {
      await tick()
      schedule()
    }, pollMs)
  }

  heartbeat().catch(() => {})
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
  logLine(
    'info',
    `worker ${id} starting; dashboard ${config.dashboardUrl}, token from ${config.scrapeQueueDir}`
  )
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

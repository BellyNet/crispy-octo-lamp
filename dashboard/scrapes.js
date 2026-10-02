'use strict'

// Scrapes page support for the dashboard: API routes over the shared scrape
// queue (scrapyard/scrapeQueue.js), the NAS worker, and the optional nightly
// schedule. The PC worker (scrapyard/scrapeWorker.js) shares the same queue
// through the NAS share.

const fs = require('fs')
const path = require('path')

const config = require('../scrapyard/config')
const queue = require('../scrapyard/scrapeQueue')
const { planScrapeRun } = require('../scrapyard/scrapePlans')
const { loadModelRegistry } = require('../scrapyard/modelRegistry')

const RUN_ID = /^[0-9TZ]+-[0-9a-f]{6}$/
const TASK_ID = /^t\d{4}$/
const SCHEDULE_CHECK_MS = 60 * 1000

function settingsPath() {
  return path.join(config.scrapeQueueDir, 'settings.json')
}

function readSettings() {
  try {
    return JSON.parse(fs.readFileSync(settingsPath(), 'utf8'))
  } catch {
    return { nightly: { enabled: false, utcHour: 6 } }
  }
}

function writeSettings(settings) {
  fs.mkdirSync(config.scrapeQueueDir, { recursive: true })
  fs.writeFileSync(settingsPath(), JSON.stringify(settings, null, 1))
}

function createRunFromRequest({ scope, model, url, createdBy }, registryPath) {
  const plan = planScrapeRun({ scope, model, url }, { registryPath })
  return queue.createRun({ ...plan, scope, createdBy })
}

function mountScrapeRoutes(app, { registryPath, pageDir }) {
  app.get('/scrapes', (_req, res) =>
    res.sendFile('scrapes.html', { root: pageDir })
  )

  app.get('/api/scrapes', (_req, res) => {
    try {
      res.setHeader('Cache-Control', 'no-cache')
      res.json({
        workers: queue.listWorkers(),
        lock: queue.readLock(),
        runs: queue.listRunSummaries({ limit: 25 }),
        settings: readSettings(),
      })
    } catch (err) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/scrapes/models', (_req, res) => {
    try {
      const registry = loadModelRegistry(registryPath)
      res.json(
        Object.entries(registry)
          .map(([name, entry]) => ({
            name,
            sources: Object.values(entry?.sources || {}).reduce(
              (sum, list) => sum + (Array.isArray(list) ? list.length : 0),
              0
            ),
          }))
          .filter((model) => model.sources > 0)
          .sort((a, b) => a.name.localeCompare(b.name))
      )
    } catch (err) {
      res.status(500).json({ error: err.message })
    }
  })

  app.get('/api/scrapes/:runId', (req, res) => {
    if (!RUN_ID.test(req.params.runId)) return res.status(400).end()
    const run = queue.readRun(req.params.runId)
    if (!run) return res.status(404).json({ error: 'Run not found' })
    res.setHeader('Cache-Control', 'no-cache')
    res.json({
      run,
      summary: queue.summarizeRun(run, {
        canceled: queue.isCancelRequested(run.id),
      }),
    })
  })

  app.get('/api/scrapes/:runId/tasks/:taskId/log', (req, res) => {
    const { runId, taskId } = req.params
    if (!RUN_ID.test(runId) || !TASK_ID.test(taskId)) {
      return res.status(400).end()
    }
    const bytes = Math.min(
      Math.max(Number.parseInt(req.query.bytes, 10) || 32768, 1024),
      512 * 1024
    )
    res.setHeader('Cache-Control', 'no-cache')
    res.json(queue.readTaskLogTail(runId, taskId, bytes))
  })

  app.post('/api/scrapes', (req, res) => {
    const scope = req.body?.scope
    if (!['all', 'model', 'url'].includes(scope)) {
      return res.status(400).json({ error: 'scope must be all, model or url' })
    }
    try {
      if (
        scope === 'all' &&
        queue
          .listRunSummaries({ limit: 50 })
          .some(
            (run) =>
              run.scope === 'all' && ['queued', 'running'].includes(run.status)
          )
      ) {
        return res
          .status(409)
          .json({ error: 'An all-sources run is already in progress.' })
      }
      const run = createRunFromRequest(
        {
          scope,
          model: req.body?.model,
          url: req.body?.url,
          createdBy: 'dashboard',
        },
        registryPath
      )
      res.json({ ok: true, run: queue.summarizeRun(run) })
    } catch (err) {
      res.status(400).json({ error: err.message })
    }
  })

  app.post('/api/scrapes/:runId/cancel', (req, res) => {
    if (!RUN_ID.test(req.params.runId)) return res.status(400).end()
    if (!queue.readRun(req.params.runId)) {
      return res.status(404).json({ error: 'Run not found' })
    }
    queue.requestCancel(req.params.runId)
    res.json({ ok: true })
  })

  app.post('/api/scrapes-settings', (req, res) => {
    const utcHour = Number(req.body?.utcHour)
    if (!Number.isInteger(utcHour) || utcHour < 0 || utcHour > 23) {
      return res.status(400).json({ error: 'utcHour must be 0-23' })
    }
    const settings = readSettings()
    settings.nightly = {
      ...settings.nightly,
      enabled: Boolean(req.body?.enabled),
      utcHour,
    }
    writeSettings(settings)
    res.json({ ok: true, settings })
  })
}

// Worker API for the PC worker and PC CLI scrapes (scrapyard/scrapeBackends
// remote backend). Mounted before the dashboard's cookie login: callers
// authenticate with the token kept in the queue folder on the NAS share,
// so anyone who can reach the share can run a worker, and nothing else.
function mountWorkerApi(app, { express, registryPath }) {
  const crypto = require('crypto')
  const {
    createLocalBackend,
    ensureWorkerToken,
  } = require('../scrapyard/scrapeBackends')
  const token = Buffer.from(ensureWorkerToken())
  const local = createLocalBackend()
  const WORKER_ID = /^[\w.-]{1,64}$/

  const requireWorkerToken = (req, res, next) => {
    const given = Buffer.from(String(req.get('X-Worker-Token') || ''))
    if (
      given.length !== token.length ||
      !crypto.timingSafeEqual(given, token)
    ) {
      return res.status(401).json({ error: 'Bad worker token' })
    }
    next()
  }
  const worker = (body) => {
    const id = String(body?.worker?.id || '')
    if (!WORKER_ID.test(id)) throw new Error('Bad worker id')
    return {
      id,
      label: String(body.worker.label || id).slice(0, 100),
      browser: Boolean(body.worker.browser),
    }
  }
  const ids = (body) => {
    const runId = String(body?.runId || '')
    const taskId = String(body?.taskId || '')
    if (!RUN_ID.test(runId) || !TASK_ID.test(taskId)) {
      throw new Error('Bad run or task id')
    }
    return { runId, taskId }
  }
  const handle = (fn) => async (req, res) => {
    try {
      res.json((await fn(req.body)) || { ok: true })
    } catch (err) {
      res
        .status(err.code === 'SCRAPE_LOCK_BUSY' ? 409 : 400)
        .json({ error: err.message, code: err.code })
    }
  }

  const router = express.Router()
  router.use(express.json({ limit: '4mb' }), requireWorkerToken)
  router.post(
    '/heartbeat',
    handle(async (body) => {
      await local.heartbeat(worker(body), body?.extra || {})
    })
  )
  router.post(
    '/claim',
    handle(async (body) => {
      const claimed = await local.claim(worker(body))
      return claimed || { task: null }
    })
  )
  router.post(
    '/progress',
    handle(async (body) => {
      const { runId, taskId } = ids(body)
      return local.progress(
        worker(body),
        runId,
        taskId,
        String(body.text || '')
      )
    })
  )
  router.post(
    '/finish',
    handle(async (body) => {
      const { runId, taskId } = ids(body)
      const fields = body?.fields || {}
      await local.finish(worker(body), runId, taskId, {
        status: ['done', 'failed', 'canceled'].includes(fields.status)
          ? fields.status
          : 'failed',
        exitCode: fields.exitCode ?? null,
        summary: fields.summary || null,
        error: fields.error || null,
      })
    })
  )
  // The NAS registry for the PC's registry sync (scrapyard/registrySync.js).
  router.post(
    '/registry/pull',
    handle(async () => ({ registry: loadModelRegistry(registryPath) }))
  )
  router.post(
    '/registry/apply',
    handle(async (body) => {
      const ops = Array.isArray(body?.ops) ? body.ops : []
      const { applyOpsToRegistry } = require('../scrapyard/registryStore')
      return { registry: await applyOpsToRegistry(ops, registryPath) }
    })
  )
  // Lock for scrapes started by hand on the PC (npm run scrape ...).
  router.post(
    '/lock',
    handle(async (body) => {
      const holder = { id: String(body?.holder || ''), note: body?.note }
      if (!/^cli-[\w.-]{1,80}$/.test(holder.id)) throw new Error('Bad holder')
      if (body.action === 'acquire') {
        if (!queue.tryAcquireLock(holder))
          throw queue.lockBusyError(queue.readLock())
      } else if (body.action === 'heartbeat') {
        queue.updateLock(holder)
      } else if (body.action === 'release') {
        queue.releaseLock(holder)
      } else {
        throw new Error('Bad action')
      }
    })
  )
  app.use('/api/worker', router)
}

// Queues an all-sources run once a day at the configured hour (UTC, so it
// doesn't depend on the container's timezone), unless one is already active.
function startNightlySchedule({ registryPath, log = console }) {
  const check = () => {
    try {
      const settings = readSettings()
      const nightly = settings.nightly || {}
      const now = new Date()
      const today = now.toISOString().slice(0, 10)
      if (!nightly.enabled || now.getUTCHours() !== nightly.utcHour) return
      if (nightly.lastQueuedDate === today) return
      if (queue.activeRunExists()) return
      createRunFromRequest(
        { scope: 'all', createdBy: 'nightly schedule' },
        registryPath
      )
      settings.nightly = { ...nightly, lastQueuedDate: today }
      writeSettings(settings)
      log.log('  Scrapes:   nightly all-sources run queued')
    } catch (err) {
      log.warn('  Scrapes:   nightly schedule check failed:', err.message)
    }
  }
  const timer = setInterval(check, SCHEDULE_CHECK_MS)
  timer.unref?.()
  check()
}

// The NAS worker: runs the tasks that don't need a browser, against the
// NAS registry directly.
function startNasWorker({ log = console }) {
  const { startWorker } = require('../scrapyard/scrapeWorker')
  const { createLocalBackend } = require('../scrapyard/scrapeBackends')
  return startWorker({
    backend: createLocalBackend(),
    id: 'nas',
    label: 'NAS',
    browser: false,
    pollMs: 5000,
    cooldownMs: 8000,
    log: {
      warn: (message) => log.warn(`  Scrapes:   ${message}`),
      error: (message) => log.warn(`  Scrapes:   ${message}`),
    },
  })
}

module.exports = {
  mountScrapeRoutes,
  mountWorkerApi,
  startNightlySchedule,
  startNasWorker,
}

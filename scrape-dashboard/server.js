'use strict'

const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const { spawn, spawnSync } = require('child_process')
const express = require('express')

const {
  sanitize,
  loadModelRegistry,
  findCanonicalModelName,
  findCanonicalModelNameBySource,
} = require('../scrapyard/modelRegistry')
const { parseSourceUrl } = require('../scrapyard/sourceRouter')
const {
  readFreshModelRunSummary,
  summarizeSourceRunSummary,
} = require('../scrapyard/scraperRunner')
const {
  registerParsedSourceForModel,
} = require('../scrapyard/run-scrape-interactive')
const {
  PLATFORMS,
  probeUsername,
} = require('../hoghaul/backfill-sources-interactive')

const rootDir = path.join(__dirname, '..')
const registryPath = path.join(rootDir, 'model_aliases.json')
const runScrapeScript = path.join(rootDir, 'scrapyard', 'run-scrape.js')
const app = express()

const PORT = Number.parseInt(process.env.SCRAPE_DASHBOARD_PORT, 10) || 3430
const PASSWORD =
  process.env.SCRAPE_DASHBOARD_PASSWORD || process.env.DASHBOARD_PASSWORD || ''
const AUTH_COOKIE = 'scrape_dashboard_auth'
const AUTH_TOKEN = PASSWORD
  ? crypto.createHash('sha256').update(PASSWORD).digest('hex')
  : ''

const APPDATA =
  process.env.APPDATA ||
  path.join(process.env.HOME || process.env.USERPROFILE, 'AppData', 'Roaming')
const datasetDir =
  process.env.DATASET_DIR || path.join(APPDATA, '.slopvault', 'dataset')

const SOURCE_KEYS = ['reddit', 'kemono', 'coomer', 'stufferdb']
const JOB_LOG_LIMIT = 2500
const jobs = new Map()
const queue = []
let activeJob = null
let nextJobId = 1

function parseCookies(req) {
  return Object.fromEntries(
    String(req.headers.cookie || '')
      .split(';')
      .map((part) => part.trim().split('='))
      .filter(([name, value]) => name && value)
  )
}

function requireAuth(req, res, next) {
  if (!PASSWORD) return next()
  if (parseCookies(req)[AUTH_COOKIE] === AUTH_TOKEN) return next()
  if (req.path === '/login.html' || req.path === '/auth') return next()
  if (req.path.startsWith('/api/')) {
    return res.status(401).json({ error: 'Authentication required' })
  }
  res.redirect('/login.html')
}

function normalizeUsernameSearchInput(value) {
  return sanitize(
    String(value || '')
      .trim()
      .replace(/^@+/, '')
      .replace(/^u\//i, '')
      .replace(/^user\//i, '')
  )
}

function getPlatformLabel(platform) {
  if (platform === 'kemono') return 'Pawchive'
  if (platform === 'coomer') return 'CoomerFans'
  if (platform === 'reddit') return 'Reddit'
  if (platform === 'stufferdb') return 'StufferDB'
  return platform || 'Unknown'
}

function getStufferDbSearchUrl(username) {
  const query = `site:stufferdb.com ${username}`
  return `https://duckduckgo.com/?q=${encodeURIComponent(query)}`
}

function sourceListFor(entry, sourceKey) {
  return Array.isArray(entry?.sources?.[sourceKey])
    ? entry.sources[sourceKey].filter((source) => source?.url)
    : []
}

function getModels() {
  const registry = loadModelRegistry(registryPath)
  return Object.entries(registry)
    .map(([name, entry]) => {
      const modelDir = path.join(datasetDir, name)
      const sources = Object.fromEntries(
        SOURCE_KEYS.map((key) => [key, sourceListFor(entry, key)])
      )
      return {
        name,
        aliases: Array.isArray(entry?.aliases) ? entry.aliases : [],
        sources,
        sourceCount: Object.values(sources).reduce(
          (count, list) => count + list.length,
          0
        ),
        hasLocalMedia: fs.existsSync(modelDir),
      }
    })
    .sort((left, right) =>
      left.name.localeCompare(right.name, undefined, { sensitivity: 'base' })
    )
}

function getKnownModel(value) {
  const registry = loadModelRegistry(registryPath)
  const requested = sanitize(value)
  return findCanonicalModelName(registry, requested) || requested
}

function findSourceOwner(parsed) {
  if (!parsed) return null
  const registry = loadModelRegistry(registryPath)
  return findCanonicalModelNameBySource(registry, {
    site: parsed.site || parsed.sourceType,
    service: parsed.service,
    userId: parsed.userId,
    username: parsed.username,
    inputUrl: parsed.inputUrl || parsed.url,
    url: parsed.url,
  })
}

function toCandidate(hit, overrides = {}) {
  const parsed = parseSourceUrl(hit.url)
  const platform = hit.platform || parsed?.sourceType || 'unknown'
  return {
    id: `${platform}:${hit.url}`,
    type: 'source',
    platform,
    label: getPlatformLabel(platform),
    service: hit.service || parsed?.service || null,
    userId: hit.id || hit.userId || parsed?.userId || null,
    username: hit.username || hit.name || parsed?.username || null,
    name: hit.name || hit.username || parsed?.rawName || null,
    url: parsed?.url || hit.url,
    parseable: Boolean(parsed),
    existingModel: parsed ? findSourceOwner(parsed) : null,
    verified: hit.verified !== false,
    ...overrides,
  }
}

async function searchSourceCandidates(rawQuery) {
  const query = String(rawQuery || '').trim()
  if (!query) return []

  const parsed = parseSourceUrl(query)
  if (parsed) {
    return [
      toCandidate(
        {
          platform: parsed.sourceType,
          service: parsed.service,
          userId: parsed.userId,
          username: parsed.username || parsed.rawName,
          url: parsed.url,
          name: parsed.rawName || parsed.username,
        },
        { verified: true, source: 'direct-url' }
      ),
    ]
  }

  const username = normalizeUsernameSearchInput(query)
  if (!username) return []

  const candidates = [
    toCandidate(
      {
        platform: 'reddit',
        service: 'submitted',
        username,
        url: PLATFORMS.reddit.userUrl(username),
        name: username,
      },
      { verified: false, source: 'username' }
    ),
  ]

  for (const platform of ['coomer', 'kemono']) {
    const hits = await probeUsername(platform, username)
    for (const hit of hits) {
      if (!candidates.some((candidate) => candidate.url === hit.url)) {
        candidates.push(toCandidate(hit, { source: 'probe' }))
      }
    }
  }

  candidates.push({
    id: `stufferdb-search:${username}`,
    type: 'manual-search',
    platform: 'stufferdb',
    label: getPlatformLabel('stufferdb'),
    service: null,
    userId: null,
    username,
    name: username,
    url: getStufferDbSearchUrl(username),
    parseable: false,
    existingModel: null,
    verified: false,
    source: 'manual-search',
  })

  return candidates
}

function appendOption(args, flag, value) {
  if (value === undefined || value === null || value === '') return
  args.push(flag, String(value))
}

function appendBoolean(args, flag, value) {
  if (value) args.push(flag)
}

function appendScrapeOptions(args, options = {}) {
  appendBoolean(args, '--skip-nas-sync', options.skipNasSync !== false)
  appendBoolean(args, '--dry-run', Boolean(options.dryRun))
  appendBoolean(args, '--keep-history', Boolean(options.keepHistory))
  appendBoolean(args, '--stop-on-error', Boolean(options.stopOnError))
  appendBoolean(
    args,
    '--full-source-refresh',
    Boolean(options.fullSourceRefresh)
  )
  appendBoolean(args, '--browser-visible', Boolean(options.browserVisible))
  appendBoolean(
    args,
    '--download-oversized',
    Boolean(options.downloadOversized)
  )
  appendOption(args, '--pages', options.pages)
  appendOption(args, '--max-posts', options.maxPosts)
  appendOption(args, '--max-files', options.maxFiles)
  appendOption(args, '--post-concurrency', options.postConcurrency)
  appendOption(args, '--image-concurrency', options.imageConcurrency)
  appendOption(args, '--video-concurrency', options.videoConcurrency)
  appendOption(
    args,
    '--reddit-fallback-delay-ms',
    options.redditFallbackDelayMs
  )
  appendOption(
    args,
    '--source-incremental-overlap-pages',
    options.sourceIncrementalOverlapPages
  )
}

function buildScrapeArgs(sourceUrl, modelName, options = {}) {
  const args = [runScrapeScript, sourceUrl]
  appendOption(args, '--model', modelName)
  appendScrapeOptions(args, options)
  appendBoolean(
    args,
    '--reddit-browser-media',
    Boolean(options.redditBrowserMedia)
  )
  return args
}

function buildAllScrapeArgs(options = {}) {
  const args = [runScrapeScript, 'update', 'all']
  appendScrapeOptions(args, options)
  return args
}

function stripAnsi(value) {
  return String(value || '').replace(/\x1B\[[0-?]*[ -/]*[@-~]/g, '')
}

function appendJobLog(job, text, stream = 'stdout') {
  const clean = stripAnsi(text)
  if (!clean) return
  const chunks = clean.split(/\r?\n/)
  for (const chunk of chunks) {
    if (!chunk) continue
    job.log.push({
      at: new Date().toISOString(),
      stream,
      text: chunk,
    })
  }
  if (job.log.length > JOB_LOG_LIMIT) {
    job.log.splice(0, job.log.length - JOB_LOG_LIMIT)
  }
}

function publicJob(job) {
  return {
    id: job.id,
    mode: job.mode,
    status: job.status,
    model: job.model,
    sources: job.sources,
    options: job.options,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    activeSourceIndex: job.activeSourceIndex,
    exitCode: job.exitCode,
    error: job.error,
    runs: job.runs,
    log: job.log,
  }
}

function killProcessTree(pid) {
  if (!pid) return
  if (process.platform === 'win32') {
    spawnSync('taskkill', ['/PID', String(pid), '/T', '/F'], {
      stdio: 'ignore',
    })
    return
  }
  try {
    process.kill(-pid, 'SIGTERM')
  } catch {
    try {
      process.kill(pid, 'SIGTERM')
    } catch {}
  }
}

function runChildForSource(job, sourceUrl, index) {
  return new Promise((resolve) => {
    const parsed = parseSourceUrl(sourceUrl)
    const startedAtMs = Date.now()
    const args = buildScrapeArgs(sourceUrl, job.model, job.options)
    appendJobLog(
      job,
      `[${index + 1}/${job.sources.length}] ${job.model} -> ${getPlatformLabel(
        parsed?.sourceType
      )}: ${sourceUrl}`
    )
    appendJobLog(
      job,
      `node ${args.map((arg) => JSON.stringify(arg)).join(' ')}`
    )

    const child = spawn(process.execPath, args, {
      cwd: rootDir,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    })
    job.child = child
    child.stdout.on('data', (chunk) => appendJobLog(job, chunk, 'stdout'))
    child.stderr.on('data', (chunk) => appendJobLog(job, chunk, 'stderr'))
    child.on('error', (err) => {
      appendJobLog(job, `Failed to start scraper: ${err.message}`, 'stderr')
      resolve({ code: 1, parsed, startedAtMs, error: err.message })
    })
    child.on('exit', (code, signal) => {
      resolve({
        code: code ?? (signal ? 130 : 1),
        signal,
        parsed,
        startedAtMs,
      })
    })
  })
}

function runChildForAllSources(job) {
  return new Promise((resolve) => {
    const args = buildAllScrapeArgs(job.options)
    appendJobLog(job, 'Running all registered source updates')
    appendJobLog(
      job,
      `node ${args.map((arg) => JSON.stringify(arg)).join(' ')}`
    )

    const child = spawn(process.execPath, args, {
      cwd: rootDir,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    })
    job.child = child
    child.stdout.on('data', (chunk) => appendJobLog(job, chunk, 'stdout'))
    child.stderr.on('data', (chunk) => appendJobLog(job, chunk, 'stderr'))
    child.on('error', (err) => {
      appendJobLog(
        job,
        `Failed to start all-source scrape: ${err.message}`,
        'stderr'
      )
      resolve({ code: 1, error: err.message })
    })
    child.on('exit', (code, signal) => {
      resolve({ code: code ?? (signal ? 130 : 1), signal })
    })
  })
}

async function runJob(job) {
  activeJob = job
  job.status = 'running'
  job.startedAt = new Date().toISOString()
  job.activeSourceIndex = 0
  appendJobLog(
    job,
    `Started ${job.mode === 'all' ? 'all-source' : 'scrape'} job for ${job.model}`
  )

  try {
    if (job.mode === 'all') {
      const result = await runChildForAllSources(job)
      job.child = null
      job.runs.push({
        ok: result.code === 0,
        code: result.code,
        signal: result.signal || null,
        scraper: 'all',
        sourceType: 'all',
        url: 'all registered sources',
        summary: null,
      })
      if (result.code !== 0) {
        appendJobLog(
          job,
          `All-source scrape exited with status ${result.code}`,
          'stderr'
        )
      }
    } else {
      for (let index = 0; index < job.sources.length; index += 1) {
        if (job.status === 'canceling') break
        job.activeSourceIndex = index
        const sourceUrl = job.sources[index]
        const result = await runChildForSource(job, sourceUrl, index)
        job.child = null
        const summary = result.parsed
          ? readFreshModelRunSummary(job.model, result.parsed.scraper, {
              inputUrl: result.parsed.inputUrl || result.parsed.url,
              startedAfterMs: result.startedAtMs,
            })
          : null
        const run = {
          ok: result.code === 0,
          code: result.code,
          signal: result.signal || null,
          scraper: result.parsed?.scraper || null,
          sourceType: result.parsed?.sourceType || null,
          url: sourceUrl,
          summary: summarizeSourceRunSummary(summary, {
            ok: result.code === 0,
            sourceType: result.parsed?.sourceType || null,
          }),
        }
        job.runs.push(run)
        if (result.code !== 0) {
          appendJobLog(
            job,
            `Source exited with status ${result.code}`,
            'stderr'
          )
          if (job.options.stopOnError) break
        }
      }
    }

    const failed = job.runs.some((run) => !run.ok)
    job.status =
      job.status === 'canceling' ? 'canceled' : failed ? 'failed' : 'completed'
    job.exitCode = failed ? 1 : 0
  } catch (err) {
    job.status = 'failed'
    job.exitCode = 1
    job.error = err.stack || err.message
    appendJobLog(job, job.error, 'stderr')
  } finally {
    job.finishedAt = new Date().toISOString()
    appendJobLog(job, `Job ${job.status}`)
    activeJob = null
    runNextJob()
  }
}

function runNextJob() {
  if (activeJob || !queue.length) return
  const job = queue.shift()
  runJob(job)
}

app.use(express.urlencoded({ extended: false }))
app.use(express.json({ limit: '128kb' }))

app.post('/auth', (req, res) => {
  if (!PASSWORD || req.body.password === PASSWORD) {
    res.setHeader(
      'Set-Cookie',
      `${AUTH_COOKIE}=${AUTH_TOKEN}; Path=/; HttpOnly; SameSite=Strict; Max-Age=31536000`
    )
    return res.redirect('/')
  }
  res.redirect('/login.html?error=1')
})

app.use(requireAuth)
app.use(express.static(__dirname))

app.get('/', (_req, res) => {
  res.sendFile('index.html', { root: __dirname })
})

app.get('/api/models', (_req, res) => {
  res.json({
    registryPath,
    datasetDir,
    models: getModels(),
  })
})

app.get('/api/source-search', async (req, res) => {
  try {
    const candidates = await searchSourceCandidates(req.query.q)
    res.json({ candidates })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

app.post('/api/models/:model/sources', (req, res) => {
  const model = getKnownModel(req.params.model || req.body.model)
  const parsed = parseSourceUrl(req.body.url)
  if (!model) return res.status(400).json({ error: 'model is required' })
  if (!parsed) return res.status(400).json({ error: 'Unsupported source URL' })

  try {
    const savedModel = registerParsedSourceForModel(parsed, model)
    res.json({
      ok: true,
      model: savedModel,
      source: {
        platform: parsed.sourceType,
        label: getPlatformLabel(parsed.sourceType),
        url: parsed.url,
      },
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

app.get('/api/jobs', (_req, res) => {
  res.json({
    activeJobId: activeJob?.id || null,
    queuedJobIds: queue.map((job) => job.id),
    jobs: Array.from(jobs.values())
      .sort((left, right) => right.id - left.id)
      .slice(0, 30)
      .map(publicJob),
  })
})

app.post('/api/jobs', (req, res) => {
  const mode = req.body.mode === 'all' ? 'all' : 'sources'
  const model = mode === 'all' ? 'ALL SOURCES' : getKnownModel(req.body.model)
  const sources = Array.isArray(req.body.sources)
    ? req.body.sources.map((url) => String(url || '').trim()).filter(Boolean)
    : []
  if (mode !== 'all' && !model) {
    return res.status(400).json({ error: 'model is required' })
  }
  if (mode !== 'all' && !sources.length) {
    return res.status(400).json({ error: 'at least one source is required' })
  }
  for (const source of mode === 'all' ? [] : sources) {
    if (!parseSourceUrl(source)) {
      return res
        .status(400)
        .json({ error: `Unsupported source URL: ${source}` })
    }
  }

  const job = {
    id: nextJobId++,
    mode,
    status: 'queued',
    model,
    sources: mode === 'all' ? [] : sources,
    options: {
      ...(req.body.options || {}),
      skipNasSync: req.body.options?.skipNasSync !== false,
    },
    createdAt: new Date().toISOString(),
    startedAt: null,
    finishedAt: null,
    activeSourceIndex: null,
    exitCode: null,
    error: null,
    runs: [],
    log: [],
    child: null,
  }
  jobs.set(job.id, job)
  queue.push(job)
  runNextJob()
  res.json({ ok: true, job: publicJob(job) })
})

app.get('/api/jobs/:id', (req, res) => {
  const job = jobs.get(Number(req.params.id))
  if (!job) return res.status(404).json({ error: 'job not found' })
  res.json({ job: publicJob(job) })
})

app.post('/api/jobs/:id/cancel', (req, res) => {
  const job = jobs.get(Number(req.params.id))
  if (!job) return res.status(404).json({ error: 'job not found' })
  if (job.status === 'queued') {
    const index = queue.indexOf(job)
    if (index >= 0) queue.splice(index, 1)
    job.status = 'canceled'
    job.finishedAt = new Date().toISOString()
    appendJobLog(job, 'Canceled before start')
    return res.json({ ok: true, job: publicJob(job) })
  }
  if (job.status === 'running' && job.child) {
    job.status = 'canceling'
    appendJobLog(job, 'Cancel requested', 'stderr')
    killProcessTree(job.child.pid)
  }
  res.json({ ok: true, job: publicJob(job) })
})

app.listen(PORT, () => {
  console.log(`Scrape Dashboard: http://localhost:${PORT}`)
  console.log(`Registry: ${registryPath}`)
  console.log(`Dataset: ${datasetDir}`)
})

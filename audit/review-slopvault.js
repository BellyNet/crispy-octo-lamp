const fs = require('fs')
const path = require('path')
const http = require('http')
const crypto = require('crypto')
const { URL } = require('url')
const { spawn } = require('child_process')
const minimist = require('minimist')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
  },
  boolean: [
    'help',
    'hash',
    'hash-findings',
    'archive',
    'fresh',
    'no-open',
    'skip-audit',
    'include-incomplete',
    'reuse-manifest',
  ],
  default: {
    port: 4777,
    hash: false,
    'hash-findings': false,
    archive: false,
    fresh: false,
    'no-open': false,
    'skip-audit': false,
    'include-incomplete': true,
    'reuse-manifest': false,
  },
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const auditDir = __dirname
const rootDir = path.join(auditDir, '..')
const logDir = path.resolve(
  String(argv['log-dir'] || path.join(auditDir, 'logs'))
)
const manifestDir = path.resolve(
  String(argv['output-dir'] || path.join(auditDir, 'manifests'))
)
const dashboardDir = path.resolve(
  String(argv['dashboard-dir'] || path.join(auditDir, 'dashboard'))
)
const dashboardPath = path.join(dashboardDir, 'slopvault-dashboard.html')
const port = Number.parseInt(argv.port, 10) || 4777
const reviewToken = crypto.randomBytes(16).toString('hex')
const slopvaultRoot = path.resolve(
  String(
    argv['slopvault-root'] ||
      path.join(
        process.env.APPDATA || path.join(process.env.HOME, 'AppData', 'Roaming'),
        '.slopvault'
      )
  )
)
const permanentSkipFile = path.join(slopvaultRoot, 'milkmaid-permanent-skips.json')

main().catch((err) => {
  console.error(`Fatal review error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  ensureDir(logDir)
  ensureDir(manifestDir)
  ensureDir(dashboardDir)

  const latestAuditLog = findLatestAuditLog(logDir)
  const shouldRunAudit = argv.fresh || (!argv['skip-audit'] && !latestAuditLog)

  if (shouldRunAudit) {
    await runNodeScript('audit-slopvault.js', buildAuditArgs(false))
  } else if (latestAuditLog) {
    console.log(`Reusing audit log: ${latestAuditLog}`)
  } else if (argv['skip-audit']) {
    throw new Error(
      'No latest audit log exists yet. Run review without --skip-audit or use --fresh.'
    )
  }

  const shouldReuseManifest = Boolean(argv['reuse-manifest'])
  const latestManifestPath = path.join(manifestDir, 'slopvault-manifest-latest.json')
  const latestDashboardPath = path.join(dashboardDir, 'slopvault-dashboard.html')

  if (
    shouldReuseManifest &&
    fs.existsSync(latestManifestPath) &&
    fs.existsSync(latestDashboardPath)
  ) {
    console.log(`Reusing manifest: ${latestManifestPath}`)
    console.log(`Reusing dashboard: ${latestDashboardPath}`)
  } else {
    await regenerateManifest()
  }
  await startServer()
}

function printHelp() {
  console.log(`Usage: node audit/review-slopvault.js [options]

Options:
  --port <n>          Local dashboard port. Default: 4777.
  --hash              Compute md5 hashes while generating the manifest.
  --hash-findings     Hash every flagged audit finding before review.
  --archive           Also keep timestamped audit logs.
  --fresh             Force a new audit scan before opening the dashboard.
  --skip-audit        Reuse the latest audit log instead of running a new scan.
  --reuse-manifest    Reuse the latest manifest/dashboard when they already exist.
  --include-incomplete=false
                      Skip repo incomplete files during audit.
  --no-open           Do not open the dashboard automatically.
  -h, --help          Show help.

Notes:
  The review app runs a localhost-only server so the dashboard can apply
  quarantine decisions without a separate terminal command.
  By default it reuses audit-slopvault-latest.json when available.
`)
}

function buildAuditArgs(apply, decisionsPath = null) {
  const args = []
  if (apply) args.push('--apply')
  if (decisionsPath) args.push('--decisions', decisionsPath)
  if (argv['hash-findings']) args.push('--hash-findings')
  if (argv.archive) args.push('--archive')

  for (const name of [
    'slopvault-root',
    'dataset-root',
    'incomplete-root',
    'quarantine-root',
    'log-dir',
    'min-video-bytes',
    'min-image-bytes',
    'min-gif-bytes',
    'tail-seconds',
    'tail-frames',
  ]) {
    if (argv[name] !== undefined) args.push(`--${name}`, String(argv[name]))
  }

  if (argv['include-incomplete'] === false)
    args.push('--include-incomplete=false')
  return args
}

function buildManifestArgs() {
  const args = []
  if (argv.hash) args.push('--hash')

  const latestAuditLog = findLatestAuditLog(logDir)
  if (latestAuditLog) args.push('--audit-log', latestAuditLog)

  for (const name of [
    'slopvault-root',
    'dataset-root',
    'quarantine-root',
    'output-dir',
    'dashboard-dir',
  ]) {
    if (argv[name] !== undefined) args.push(`--${name}`, String(argv[name]))
  }

  return args
}

function runNodeScript(scriptName, args) {
  return new Promise((resolve, reject) => {
    console.log('')
    console.log(`> node audit/${scriptName} ${args.join(' ')}`)

    const child = spawn(
      process.execPath,
      [path.join(auditDir, scriptName), ...args],
      {
        cwd: rootDir,
        stdio: 'inherit',
      }
    )

    child.on('error', reject)
    child.on('exit', (code) => {
      if (code === 0) return resolve()
      reject(new Error(`${scriptName} exited with code ${code}`))
    })
  })
}

async function regenerateManifest() {
  await runNodeScript('manifest-slopvault.js', buildManifestArgs())
}

function startServer() {
  return new Promise((resolve, reject) => {
    const server = http.createServer(async (req, res) => {
      try {
        const url = new URL(
          req.url,
          `http://${req.headers.host || '127.0.0.1'}`
        )

        if (req.method === 'GET' && url.pathname === '/') {
          return sendFile(res, dashboardPath, 'text/html; charset=utf-8')
        }

        if (req.method === 'GET' && url.pathname === '/media') {
          if (!isAuthorized(req, url)) return sendUnauthorized(res)
          return sendMedia(req, res, url.searchParams.get('path'))
        }

        if (req.method === 'GET' && url.pathname === '/api/status') {
          if (!isAuthorized(req, url)) return sendUnauthorized(res)
          return sendJson(res, {
            ok: true,
            dashboardPath,
            latestAuditLog: findLatestAuditLog(logDir),
          })
        }

        if (req.method === 'POST' && url.pathname === '/api/apply') {
          if (!isAuthorized(req, url)) return sendUnauthorized(res)
          return await applyDecisions(req, res)
        }

        sendJson(res, { ok: false, error: 'Not found' }, 404)
      } catch (err) {
        sendJson(res, { ok: false, error: err.message }, 500)
      }
    })

    server.on('error', reject)
    server.listen(port, '127.0.0.1', () => {
      const url = `http://127.0.0.1:${port}/?token=${reviewToken}`
      console.log('')
      console.log(`Slopvault review dashboard: ${url}`)
      console.log('Keep this process running while you review and apply.')
      if (!argv['no-open']) openBrowser(url)
      resolve(server)
    })
  })
}

function isAuthorized(req, url) {
  return (
    req.headers['x-slopvault-token'] === reviewToken ||
    url.searchParams.get('token') === reviewToken
  )
}

function sendUnauthorized(res) {
  return sendJson(res, { ok: false, error: 'Unauthorized review token.' }, 401)
}

async function applyDecisions(req, res) {
  const payload = await readJsonBody(req)
  const decisions = Array.isArray(payload?.decisions) ? payload.decisions : []

  if (!decisions.length) {
    return sendJson(
      res,
      {
        ok: false,
        error: 'No decisions were provided.',
      },
      400
    )
  }

  const decisionsPath = path.join(
    manifestDir,
    'slopvault-dashboard-decisions-latest.json'
  )

  fs.writeFileSync(
    decisionsPath,
    JSON.stringify(
      {
        ...payload,
        receivedAt: new Date().toISOString(),
        decisions,
      },
      null,
      2
    )
  )

  const permanentSkipsAdded = applyPermanentSkipDecisions(decisions)

  await runNodeScript('audit-slopvault.js', buildAuditArgs(true, decisionsPath))
  await regenerateManifest()

  sendJson(res, {
    ok: true,
    decisionsPath,
    permanentSkipsAdded,
    dashboardPath,
    latestAuditLog: findLatestAuditLog(logDir),
  })
}

function applyPermanentSkipDecisions(decisions) {
  const entries = loadPermanentSkipEntries()
  let added = 0

  for (const decision of decisions) {
    if (decision?.action !== 'permanent-skip') continue

    const entry = {
      relativePath: normalizeRelativePath(decision.relativePath),
      sourceUrl: normalizeSkipUrl(decision.mediaUrl),
      mediaPageUrl: normalizeSkipUrl(decision.mediaPageUrl),
      filename: String(decision.filename || '').trim(),
      reason: String(decision.error || decision.reason || 'review_permanent_skip'),
      note: 'Marked permanent-skip from Slopvault review dashboard.',
      addedAt: new Date().toISOString(),
    }

    if (
      entries.some(
        (existing) =>
          (entry.relativePath && existing.relativePath === entry.relativePath) ||
          (entry.sourceUrl && normalizeSkipUrl(existing.sourceUrl) === entry.sourceUrl) ||
          (entry.mediaPageUrl &&
            normalizeSkipUrl(existing.mediaPageUrl) === entry.mediaPageUrl)
      )
    ) {
      continue
    }

    entries.push(entry)
    added += 1
  }

  if (added > 0) {
    fs.writeFileSync(
      permanentSkipFile,
      JSON.stringify({ version: 1, entries }, null, 2)
    )
  }

  return added
}

function loadPermanentSkipEntries() {
  if (!fs.existsSync(permanentSkipFile)) return []
  try {
    const parsed = JSON.parse(fs.readFileSync(permanentSkipFile, 'utf8'))
    return Array.isArray(parsed?.entries) ? parsed.entries : []
  } catch {
    return []
  }
}

function normalizeSkipUrl(url) {
  return String(url || '').trim().replace(/&acs=[^&]+/gi, '')
}

function normalizeRelativePath(value) {
  return String(value || '').trim().replace(/\\/g, '/')
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = ''
    req.setEncoding('utf8')
    req.on('data', (chunk) => {
      body += chunk
      if (body.length > 10 * 1024 * 1024) {
        reject(new Error('Request body is too large.'))
        req.destroy()
      }
    })
    req.on('end', () => {
      try {
        resolve(JSON.parse(body || '{}'))
      } catch (err) {
        reject(new Error(`Invalid JSON body: ${err.message}`))
      }
    })
    req.on('error', reject)
  })
}

function sendFile(res, filePath, contentType) {
  if (!fs.existsSync(filePath)) {
    return sendJson(res, { ok: false, error: `Missing file: ${filePath}` }, 404)
  }

  res.writeHead(200, {
    'content-type': contentType,
    'cache-control': 'no-store',
  })
  fs.createReadStream(filePath).pipe(res)
}

function sendMedia(req, res, filePath) {
  if (!filePath) {
    return sendJson(res, { ok: false, error: 'Missing media path.' }, 400)
  }

  const resolved = path.resolve(filePath)
  if (!fs.existsSync(resolved)) {
    return sendJson(
      res,
      { ok: false, error: `Missing media: ${resolved}` },
      404
    )
  }

  const stat = fs.statSync(resolved)
  const range = req.headers.range
  const contentType = getMediaContentType(resolved)

  if (range) {
    const match = /^bytes=(\d*)-(\d*)$/.exec(range)
    if (!match) {
      res.writeHead(416, { 'content-range': `bytes */${stat.size}` })
      return res.end()
    }

    const start = match[1] ? Number.parseInt(match[1], 10) : 0
    const end = match[2] ? Number.parseInt(match[2], 10) : stat.size - 1

    if (start >= stat.size || end >= stat.size || start > end) {
      res.writeHead(416, { 'content-range': `bytes */${stat.size}` })
      return res.end()
    }

    res.writeHead(206, {
      'content-type': contentType,
      'content-length': end - start + 1,
      'content-range': `bytes ${start}-${end}/${stat.size}`,
      'accept-ranges': 'bytes',
      'cache-control': 'no-store',
    })
    return fs.createReadStream(resolved, { start, end }).pipe(res)
  }

  res.writeHead(200, {
    'content-type': contentType,
    'content-length': stat.size,
    'accept-ranges': 'bytes',
    'cache-control': 'no-store',
  })
  fs.createReadStream(resolved).pipe(res)
}

function sendJson(res, payload, status = 200) {
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  })
  res.end(JSON.stringify(payload, null, 2))
}

function openBrowser(url) {
  const command =
    process.platform === 'win32'
      ? 'cmd'
      : process.platform === 'darwin'
        ? 'open'
        : 'xdg-open'
  const args = process.platform === 'win32' ? ['/c', 'start', '', url] : [url]

  const child = spawn(command, args, {
    detached: true,
    stdio: 'ignore',
  })
  child.unref()
}

function getMediaContentType(filePath) {
  switch (path.extname(filePath).toLowerCase()) {
    case '.gif':
      return 'image/gif'
    case '.jpg':
    case '.jpeg':
      return 'image/jpeg'
    case '.png':
      return 'image/png'
    case '.webp':
      return 'image/webp'
    case '.webm':
      return 'video/webm'
    case '.mp4':
    case '.m4v':
      return 'video/mp4'
    case '.mov':
      return 'video/quicktime'
    default:
      return 'application/octet-stream'
  }
}

function findLatestAuditLog(dirPath) {
  if (!fs.existsSync(dirPath)) return null

  const latestPath = path.join(dirPath, 'audit-slopvault-latest.json')
  if (fs.existsSync(latestPath)) return latestPath

  return (
    fs
      .readdirSync(dirPath)
      .filter((name) => /^audit-slopvault-.*\.json$/.test(name))
      .map((name) => path.join(dirPath, name))
      .map((filePath) => ({
        filePath,
        mtime: fs.statSync(filePath).mtimeMs,
      }))
      .sort((a, b) => b.mtime - a.mtime)[0]?.filePath || null
  )
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

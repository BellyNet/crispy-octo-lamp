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
  boolean: ['help', 'hash', 'no-open', 'reuse-dashboard'],
  default: {
    port: 4778,
    hash: true,
    'no-open': false,
    'reuse-dashboard': false,
  },
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const auditDir = __dirname
const rootDir = path.join(auditDir, '..')
const manifestDir = path.resolve(
  String(argv['output-dir'] || path.join(auditDir, 'manifests'))
)
const dashboardDir = path.resolve(
  String(argv['dashboard-dir'] || path.join(auditDir, 'dashboard'))
)
const sourceManifestPath = path.join(manifestDir, 'slopvault-manifest-latest.json')
const duplicateManifestPath = path.join(
  manifestDir,
  'slopvault-duplicate-manifest-latest.json'
)
const duplicateDashboardPath = path.join(
  dashboardDir,
  'slopvault-duplicates-dashboard.html'
)
const duplicateDecisionsPath = path.join(
  manifestDir,
  'slopvault-duplicate-dashboard-decisions-latest.json'
)
const port = Number.parseInt(argv.port, 10) || 4778
const reviewToken = crypto.randomBytes(16).toString('hex')

main().catch((err) => {
  console.error(`Fatal duplicate review error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  ensureDir(manifestDir)
  ensureDir(dashboardDir)

  await ensureHashedSourceManifest()
  await ensureDuplicateDashboard()
  await startServer()
}

function printHelp() {
  console.log(`Usage: node audit/review-slopvault-duplicates.js [options]

Options:
  --port <n>          Local duplicate-review port. Default: 4778.
  --hash              Rebuild the main slopvault manifest with hashes when needed.
  --reuse-dashboard   Reuse the latest duplicate manifest/dashboard when they exist.
  --no-open           Do not open the browser automatically.
  -h, --help          Show help.
`)
}

async function ensureHashedSourceManifest() {
  if (fs.existsSync(sourceManifestPath)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(sourceManifestPath, 'utf8'))
      if (parsed?.hashAlgorithm === 'md5') {
        console.log(`Reusing hashed source manifest: ${sourceManifestPath}`)
        return
      }
    } catch {}
  }

  if (!argv.hash) {
    throw new Error(
      `Missing hashed source manifest at ${sourceManifestPath}. Re-run with --hash or build it first.`
    )
  }

  await runNodeScript('manifest-slopvault.js', ['--hash'])
}

async function ensureDuplicateDashboard() {
  if (
    argv['reuse-dashboard'] &&
    fs.existsSync(duplicateManifestPath) &&
    fs.existsSync(duplicateDashboardPath)
  ) {
    console.log(`Reusing duplicate manifest: ${duplicateManifestPath}`)
    console.log(`Reusing duplicate dashboard: ${duplicateDashboardPath}`)
    return
  }

  await runNodeScript('manifest-slopvault-duplicates.js', [])
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
          return sendFile(res, duplicateDashboardPath, 'text/html; charset=utf-8')
        }

        if (req.method === 'GET' && url.pathname === '/media') {
          if (!isAuthorized(req, url)) return sendUnauthorized(res)
          return sendMedia(req, res, url.searchParams.get('path'))
        }

        if (req.method === 'GET' && url.pathname === '/api/status') {
          if (!isAuthorized(req, url)) return sendUnauthorized(res)
          return sendJson(res, {
            ok: true,
            duplicateManifestPath,
            duplicateDashboardPath,
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
      console.log(`Slopvault duplicate dashboard: ${url}`)
      console.log('Keep this process running while you review duplicates.')
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
  const groups = Array.isArray(payload?.groups) ? payload.groups : []

  if (!decisions.length) {
    return sendJson(res, { ok: false, error: 'No duplicate decisions were provided.' }, 400)
  }

  fs.writeFileSync(
    duplicateDecisionsPath,
    JSON.stringify(
      {
        ...payload,
        receivedAt: new Date().toISOString(),
        groups,
        decisions,
      },
      null,
      2
    )
  )

  await runNodeScript('audit-slopvault.js', ['--apply', '--decisions', duplicateDecisionsPath])
  await runNodeScript('manifest-slopvault.js', ['--hash'])
  await runNodeScript('manifest-slopvault-duplicates.js', [])

  sendJson(res, {
    ok: true,
    decisionsPath: duplicateDecisionsPath,
    duplicateManifestPath,
    duplicateDashboardPath,
  })
}

function runNodeScript(scriptName, args) {
  return new Promise((resolve, reject) => {
    console.log(`> node audit/${scriptName} ${args.join(' ')}`.trim())

    const child = spawn(process.execPath, [path.join(auditDir, scriptName), ...args], {
      cwd: rootDir,
      stdio: 'inherit',
    })

    child.on('error', reject)
    child.on('exit', (code) => {
      if (code === 0) return resolve()
      reject(new Error(`${scriptName} exited with code ${code}`))
    })
  })
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
    return sendJson(res, { ok: false, error: `Missing media: ${resolved}` }, 404)
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

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

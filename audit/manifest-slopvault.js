const fs = require('fs')
const path = require('path')
const os = require('os')
const crypto = require('crypto')
const minimist = require('minimist')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
  },
  boolean: ['help', 'hash'],
  default: {
    hash: false,
  },
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const slopvaultRoot = path.resolve(
  String(
    argv['slopvault-root'] ||
      path.join(
        process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
        '.slopvault'
      )
  )
)
const datasetRoot = path.resolve(
  String(argv['dataset-root'] || path.join(slopvaultRoot, 'dataset'))
)
const quarantineRoot = path.resolve(
  String(argv['quarantine-root'] || path.join(slopvaultRoot, 'quarantine'))
)
const outputDir = path.resolve(
  String(argv['output-dir'] || path.join(__dirname, 'manifests'))
)
const dashboardDir = path.resolve(
  String(argv['dashboard-dir'] || path.join(__dirname, 'dashboard'))
)
const auditLogPath = argv['audit-log']
  ? path.resolve(String(argv['audit-log']))
  : findLatestAuditLog(path.join(__dirname, 'logs'))
const shouldHash = Boolean(argv.hash)
const runStamp = new Date().toISOString().replace(/[:.]/g, '-')

const mediaExtensions = new Set([
  '.jpg',
  '.jpeg',
  '.png',
  '.webp',
  '.gif',
  '.mp4',
  '.webm',
  '.m4v',
  '.mov',
])

main().catch((err) => {
  console.error(`Fatal manifest error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  ensureDir(outputDir)
  ensureDir(dashboardDir)

  const records = []
  const roots = buildScanRoots()
  const auditLog = loadAuditLog(auditLogPath)

  for (const scanRoot of roots) {
    if (!fs.existsSync(scanRoot.root)) continue

    console.log(`Scanning ${scanRoot.label}: ${scanRoot.root}`)
    for (const filePath of collectMediaFiles(scanRoot.root)) {
      records.push(await buildRecord(filePath, scanRoot))
    }
  }

  const manifest = buildManifest(records, auditLog)
  const manifestPath = path.join(
    outputDir,
    `slopvault-manifest-${runStamp}.json`
  )
  const latestManifestPath = path.join(
    outputDir,
    'slopvault-manifest-latest.json'
  )
  const dashboardPath = path.join(
    dashboardDir,
    `slopvault-dashboard-${runStamp}.html`
  )
  const latestDashboardPath = path.join(
    dashboardDir,
    'slopvault-dashboard.html'
  )

  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2))
  fs.writeFileSync(latestManifestPath, JSON.stringify(manifest, null, 2))

  const html = renderDashboard(manifest)
  fs.writeFileSync(dashboardPath, html)
  fs.writeFileSync(latestDashboardPath, html)

  console.log('')
  console.log(`Records: ${manifest.summary.totalFiles}`)
  console.log(`Dataset files: ${manifest.summary.datasetFiles}`)
  console.log(`Quarantine files: ${manifest.summary.quarantineFiles}`)
  console.log(`Audit findings: ${manifest.summary.auditFindings}`)
  console.log(
    `Audit quarantine candidates: ${manifest.summary.auditQuarantineEligible}`
  )
  console.log(`Audit log: ${manifest.audit.logPath || 'none found'}`)
  console.log(`Manifest: ${manifestPath}`)
  console.log(`Dashboard: ${latestDashboardPath}`)
}

function printHelp() {
  console.log(`Usage: node audit/manifest-slopvault.js [options]

Options:
  --hash                     Compute md5 hashes for exact duplicate detection.
  --audit-log <path>         Use a specific audit JSON log. Defaults to latest.
  --slopvault-root <path>    Override Slopvault root.
  --dataset-root <path>      Override dataset root.
  --quarantine-root <path>   Override quarantine root.
  --output-dir <path>        Override manifest output directory.
  --dashboard-dir <path>     Override dashboard output directory.
  -h, --help                 Show help.

Notes:
  The dashboard is a self-contained local HTML file.
  The dashboard records review decisions only; the audit script applies them.
  Run: npm run audit:slopvault -- --apply --decisions <exported-json>
`)
}

function buildScanRoots() {
  return [
    {
      label: 'dataset',
      root: datasetRoot,
      pathPrefix: 'dataset',
    },
    {
      label: 'quarantine_dataset',
      root: path.join(quarantineRoot, 'dataset'),
      pathPrefix: 'dataset',
      quarantined: true,
    },
    {
      label: 'quarantine_incomplete',
      root: path.join(quarantineRoot, 'incomplete'),
      pathPrefix: 'incomplete',
      quarantined: true,
    },
  ]
}

function collectMediaFiles(root) {
  const files = []
  const stack = [root]

  while (stack.length) {
    const current = stack.pop()
    let entries = []

    try {
      entries = fs.readdirSync(current, { withFileTypes: true })
    } catch (err) {
      console.warn(`Could not read ${current}: ${err.message}`)
      continue
    }

    for (const entry of entries) {
      const fullPath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        stack.push(fullPath)
        continue
      }

      if (!entry.isFile()) continue

      if (mediaExtensions.has(path.extname(entry.name).toLowerCase())) {
        files.push(fullPath)
      }
    }
  }

  return files.sort((a, b) => a.localeCompare(b))
}

async function buildRecord(filePath, scanRoot) {
  const stat = fs.statSync(filePath)
  const relativeToRoot = normalizePath(path.relative(scanRoot.root, filePath))
  const datasetRelativePath = normalizePath(
    path.join(scanRoot.pathPrefix, relativeToRoot)
  )
  const parts = relativeToRoot.split('/')
  const ext = path.extname(filePath).toLowerCase()
  const mediaType = getMediaType(ext)

  return {
    id: crypto
      .createHash('sha1')
      .update(`${scanRoot.label}:${filePath}`)
      .digest('hex'),
    root: scanRoot.label,
    quarantined: Boolean(scanRoot.quarantined),
    model: scanRoot.pathPrefix === 'dataset' ? parts[0] || null : null,
    bucket:
      scanRoot.pathPrefix === 'dataset' ? parts[1] || null : parts[0] || null,
    filename: path.basename(filePath),
    extension: ext,
    mediaType,
    absolutePath: filePath,
    fileUri: pathToFileUri(filePath),
    relativePath: relativeToRoot,
    datasetRelativePath,
    sizeBytes: stat.size,
    modifiedAt: stat.mtime.toISOString(),
    md5: shouldHash ? await hashFile(filePath) : null,
  }
}

function buildManifest(records, auditLog) {
  const duplicates = buildDuplicateGroups(records)
  const byMediaType = countBy(records, (record) => record.mediaType)
  const byRoot = countBy(records, (record) => record.root)
  const auditFindings = buildAuditFindings(auditLog, records)

  return {
    generatedAt: new Date().toISOString(),
    hashAlgorithm: shouldHash ? 'md5' : null,
    roots: {
      slopvaultRoot,
      datasetRoot,
      quarantineRoot,
    },
    summary: {
      totalFiles: records.length,
      datasetFiles: records.filter((record) => record.root === 'dataset')
        .length,
      quarantineFiles: records.filter((record) => record.quarantined).length,
      auditFindings: auditFindings.length,
      auditQuarantineEligible: auditFindings.filter(
        (finding) => finding.quarantineEligible
      ).length,
      byMediaType,
      byRoot,
    },
    audit: {
      logPath: auditLog?.logPath || null,
      generatedAt: auditLog?.finishedAt || auditLog?.startedAt || null,
      summary: auditLog ? omitFindings(auditLog) : null,
      findings: auditFindings,
    },
    duplicates,
    records,
  }
}

function buildAuditFindings(auditLog, records) {
  if (!auditLog?.findings?.length) return []

  const recordsByPath = new Map(
    records.map((record) => [
      normalizeAbsolutePath(record.absolutePath),
      record,
    ])
  )

  return auditLog.findings.map((finding) => {
    const sourcePath = finding.sourcePath || ''
    const matchedRecord = recordsByPath.get(normalizeAbsolutePath(sourcePath))
    const id =
      finding.id || createFindingId(finding.sourceType, finding.relativePath)
    const quarantineEligible = Boolean(finding.quarantineEligible)

    return {
      ...finding,
      id,
      sourcePath,
      sourceFileUri: sourcePath ? pathToFileUri(sourcePath) : null,
      quarantinePath: finding.quarantinePath || null,
      quarantineFileUri: finding.quarantinePath
        ? pathToFileUri(finding.quarantinePath)
        : null,
      relativePath: normalizePath(finding.relativePath || ''),
      reasons: Array.isArray(finding.reasons) ? finding.reasons : [],
      quarantineEligible,
      defaultAction: quarantineEligible ? 'quarantine' : 'keep',
      matchedRecordId: matchedRecord?.id || null,
      model: matchedRecord?.model || inferModelFromFinding(finding),
      bucket: matchedRecord?.bucket || inferBucketFromFinding(finding),
    }
  })
}

function omitFindings(auditLog) {
  const { findings, ...summary } = auditLog
  return summary
}

function inferModelFromFinding(finding) {
  if (finding.sourceType !== 'dataset') return null
  return normalizePath(finding.relativePath || '').split('/')[0] || null
}

function inferBucketFromFinding(finding) {
  return normalizePath(finding.relativePath || '').split('/')[1] || null
}

function buildDuplicateGroups(records) {
  const keyToRecords = new Map()

  for (const record of records) {
    const key = record.md5
      ? `md5:${record.md5}`
      : `size-name:${record.sizeBytes}:${record.filename.toLowerCase()}`

    if (!keyToRecords.has(key)) keyToRecords.set(key, [])
    keyToRecords.get(key).push(record)
  }

  return Array.from(keyToRecords.entries())
    .filter(([, group]) => group.length > 1)
    .map(([key, group]) => ({
      key,
      count: group.length,
      totalBytes: group.reduce((sum, record) => sum + record.sizeBytes, 0),
      records: group.map((record) => record.id),
    }))
    .sort((a, b) => b.totalBytes - a.totalBytes)
}

function countBy(items, getKey) {
  const counts = {}
  for (const item of items) {
    const key = getKey(item) || 'unknown'
    counts[key] = (counts[key] || 0) + 1
  }
  return counts
}

function getMediaType(ext) {
  if (['.mp4', '.webm', '.m4v', '.mov'].includes(ext)) return 'video'
  if (ext === '.gif') return 'gif'
  return 'image'
}

function findLatestAuditLog(logDir) {
  if (!fs.existsSync(logDir)) return null

  const latestPath = path.join(logDir, 'audit-slopvault-latest.json')
  if (fs.existsSync(latestPath)) return latestPath

  return (
    fs
      .readdirSync(logDir)
      .filter((name) => /^audit-slopvault-.*\.json$/.test(name))
      .map((name) => path.join(logDir, name))
      .map((filePath) => ({
        filePath,
        mtime: fs.statSync(filePath).mtimeMs,
      }))
      .sort((a, b) => b.mtime - a.mtime)[0]?.filePath || null
  )
}

function loadAuditLog(filePath) {
  if (!filePath) return null
  if (!fs.existsSync(filePath)) {
    throw new Error(`Audit log not found: ${filePath}`)
  }

  const raw = fs.readFileSync(filePath, 'utf-8').replace(/^\uFEFF/, '')
  const parsed = JSON.parse(raw)
  parsed.logPath = filePath
  parsed.findings = Array.isArray(parsed.findings) ? parsed.findings : []
  return parsed
}

function createFindingId(sourceType, relativePath) {
  return `${sourceType || 'unknown'}:${normalizePath(relativePath || '')}`
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

function normalizeAbsolutePath(value) {
  return path.resolve(String(value || '')).toLowerCase()
}

function pathToFileUri(filePath) {
  const resolved = path.resolve(filePath).replace(/\\/g, '/')
  return `file:///${encodeURI(resolved)}`
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function hashFile(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('md5')
    const stream = fs.createReadStream(filePath)
    stream.on('error', reject)
    stream.on('data', (chunk) => hash.update(chunk))
    stream.on('end', () => resolve(hash.digest('hex')))
  })
}

function renderDashboard(manifest) {
  const data = JSON.stringify(manifest).replace(/</g, '\\u003c')

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Slopvault Audit Review</title>
  <style>
    :root {
      --ink: #e8f0ff;
      --muted: #8ea0bd;
      --paper: #07111f;
      --card: rgba(12, 24, 42, .78);
      --card-solid: #0e1b2d;
      --line: rgba(140, 167, 210, .18);
      --accent: #35d0ff;
      --accent-2: #79f2c9;
      --good: #45d483;
      --warn: #ffbf4d;
      --bad: #ff5b6e;
      --shadow: rgba(0, 0, 0, .28);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Aptos", "Helvetica Neue", Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 16% -10%, rgba(53, 208, 255, .28), transparent 26rem),
        radial-gradient(circle at 84% 12%, rgba(121, 242, 201, .16), transparent 24rem),
        linear-gradient(135deg, #050a13, #0b1525 48%, #101827);
      min-height: 100vh;
    }
    header {
      padding: 34px 40px 22px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(7, 17, 31, .92), rgba(7, 17, 31, .55));
      backdrop-filter: blur(16px);
    }
    h1 {
      margin: 0;
      font-size: clamp(34px, 5vw, 68px);
      letter-spacing: -0.05em;
      line-height: .92;
      font-weight: 760;
    }
    .subhead { color: var(--muted); margin-top: 12px; max-width: 980px; }
    main { padding: 24px 40px 48px; }
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 14px;
      margin-bottom: 20px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 14px 40px var(--shadow);
      backdrop-filter: blur(18px);
    }
    .card strong { display: block; font-size: 28px; }
    .card span { color: var(--muted); font-size: 13px; text-transform: uppercase; letter-spacing: .08em; }
    .notice {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 20px;
      margin-bottom: 18px;
      padding: 16px 18px;
      box-shadow: 0 14px 40px var(--shadow);
    }
    .notice code {
      background: rgba(53, 208, 255, .12);
      border-radius: 8px;
      padding: 2px 6px;
      color: var(--accent);
    }
    .controls {
      display: grid;
      grid-template-columns: minmax(220px, 1.1fr) 160px 170px 190px 170px;
      gap: 10px;
      margin-bottom: 18px;
    }
    input, select, button {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: var(--card);
      color: var(--ink);
      padding: 11px 14px;
      font: inherit;
    }
    input::placeholder { color: #7990b4; }
    select {
      appearance: none;
      padding-right: 44px;
      line-height: 1.25;
      background-image:
        linear-gradient(45deg, transparent 50%, #9fc3ff 50%),
        linear-gradient(135deg, #9fc3ff 50%, transparent 50%);
      background-position:
        calc(100% - 24px) calc(50% - 3px),
        calc(100% - 18px) calc(50% - 3px);
      background-size: 6px 6px, 6px 6px;
      background-repeat: no-repeat;
    }
    button { cursor: pointer; }
    button.primary {
      background: linear-gradient(135deg, var(--accent), var(--accent-2));
      color: #04101d;
      border-color: var(--accent);
      font-weight: 700;
    }
    button.danger {
      background: rgba(255, 91, 110, .16);
      color: #ffd9de;
      border-color: var(--bad);
    }
    button.good {
      background: rgba(69, 212, 131, .16);
      color: #d8ffe8;
      border-color: var(--good);
    }
    button.ghost {
      background: rgba(159, 195, 255, .08);
    }
    button:disabled,
    select:disabled {
      opacity: .55;
      cursor: not-allowed;
    }
    .toolbar {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      margin: 18px 0;
    }
    .toolbar .spacer { flex: 1 1 auto; }
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1.3fr) minmax(360px, .7fr);
      gap: 18px;
      align-items: start;
    }
    .panel {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 22px;
      overflow: hidden;
      box-shadow: 0 18px 50px var(--shadow);
    }
    .table-wrap { max-height: 72vh; overflow: auto; }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th {
      position: sticky;
      top: 0;
      background: var(--card-solid);
      z-index: 1;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .06em;
    }
    tr { cursor: pointer; }
    tr:hover, tr.selected { background: rgba(53, 208, 255, .08); }
    .path { overflow-wrap: anywhere; }
    .path code {
      font-size: 12px;
      color: #bcd2f7;
      background: rgba(159, 195, 255, .08);
      padding: 2px 6px;
      border-radius: 8px;
    }
    .badge {
      display: inline-block;
      border-radius: 999px;
      padding: 3px 8px;
      background: rgba(53, 208, 255, .13);
      color: #c8f4ff;
      font-size: 12px;
      margin: 0 4px 4px 0;
      white-space: nowrap;
    }
    .badge.ok { background: rgba(69, 212, 131, .16); color: #bafbd5; }
    .badge.warn { background: rgba(255, 191, 77, .16); color: #ffe0a3; }
    .badge.bad { background: rgba(255, 91, 110, .18); color: #ffd0d7; }
    .decision-cell { min-width: 150px; }
    .decision-status { margin-top: 6px; }
    .row-actions {
      display: flex;
      gap: 8px;
      align-items: center;
      margin-bottom: 6px;
    }
    .row-icon {
      width: 34px;
      height: 34px;
      padding: 0;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 10px;
    }
    .row-icon svg {
      width: 16px;
      height: 16px;
      fill: currentColor;
      pointer-events: none;
    }
    .row-icon.active {
      box-shadow: inset 0 0 0 1px currentColor;
    }
    .actions { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
    .row-check {
      width: 16px;
      height: 16px;
      accent-color: var(--accent);
      cursor: pointer;
    }
    .viewer { padding: 18px; position: sticky; top: 16px; }
    .viewer h2 { margin: 0 0 8px; font-size: 22px; }
    .viewer p { color: var(--muted); overflow-wrap: anywhere; }
    .preview {
      width: 100%;
      min-height: 260px;
      display: grid;
      place-items: center;
      background: #020812;
      border-radius: 18px;
      overflow: hidden;
      margin-top: 14px;
      color: #fff8ef;
      text-align: center;
      padding: 12px;
    }
    .preview img, .preview video {
      width: 100%;
      max-height: 520px;
      object-fit: contain;
      background: #020812;
    }
    .meta-list {
      display: grid;
      gap: 8px;
      margin-top: 12px;
      font-size: 13px;
    }
    .meta-list div { overflow-wrap: anywhere; }
    .meta-link {
      color: var(--accent);
      text-decoration: none;
      font-weight: 600;
    }
    .meta-link:hover { text-decoration: underline; }
    .api-state {
      color: var(--muted);
      font-size: 13px;
    }
    .helper {
      color: var(--muted);
      font-size: 12px;
    }
    @media (max-width: 1200px) {
      main, header { padding-left: 18px; padding-right: 18px; }
      .controls, .layout { grid-template-columns: 1fr; }
      .viewer { position: static; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Slopvault Audit Review</h1>
    <div class="subhead">
      Generated at ${escapeHtml(manifest.generatedAt)} from audit log
      ${manifest.audit.logPath ? `<code>${escapeHtml(manifest.audit.logPath)}</code>` : '<code>none found</code>'}.
      Default decisions are prefilled from the suggested action so you can review fast, make overrides only where needed, then apply the current decision set in one shot.
    </div>
  </header>
  <main>
    <section class="cards">
      <div class="card"><strong id="auditFindings"></strong><span>Audit Findings</span></div>
      <div class="card"><strong id="quarantineCandidates"></strong><span>Suggested Quarantine</span></div>
      <div class="card"><strong id="overrideCount"></strong><span>Overrides</span></div>
      <div class="card"><strong id="visibleCount"></strong><span>Visible</span></div>
    </section>

    <section class="notice">
      Review starts with the suggested action already selected for every finding. Change only the rows you disagree with, use the bulk tools for visible or checked rows, then click <strong>Apply Quarantine</strong> to accept the current decision set.
    </section>

    <section class="controls">
      <input id="search" placeholder="Search model, filename, reason, path...">
      <select id="typeFilter">
        <option value="">All media</option>
        <option value="image">Images</option>
        <option value="gif">GIFs</option>
        <option value="video">Videos</option>
      </select>
      <select id="eligibilityFilter">
        <option value="">All findings</option>
        <option value="true">Suggested quarantine</option>
        <option value="false">Suggested keep</option>
      </select>
      <select id="decisionFilter">
        <option value="">All current decisions</option>
        <option value="quarantine">Current: quarantine</option>
        <option value="keep">Current: keep</option>
        <option value="overridden">Overrides only</option>
      </select>
      <select id="reasonFilter">
        <option value="">All reasons</option>
      </select>
    </section>

    <section class="toolbar">
      <button class="danger" id="applyDecisions" disabled>Apply Quarantine</button>
      <button class="ghost" id="exportDecisions">Export Decisions JSON</button>
      <button class="ghost" id="useSuggestedVisible">Use Suggested Visible</button>
      <button class="ghost" id="selectVisible">Select Visible</button>
      <button class="ghost" id="clearSelected">Clear Selected</button>
      <button class="danger" id="selectedQuarantine">Selected Quarantine</button>
      <button class="good" id="selectedKeep">Selected Keep</button>
      <span class="spacer"></span>
      <span id="saveState"></span>
      <span class="api-state" id="apiState"></span>
    </section>

    <section class="layout">
      <div class="panel table-wrap">
        <table>
          <thead>
            <tr>
              <th><input class="row-check" id="toggleVisible" type="checkbox" aria-label="Select visible rows"></th>
              <th>Decision</th>
              <th>Type</th>
              <th>Model</th>
              <th>Reasons</th>
              <th>Path</th>
              <th>Size</th>
            </tr>
          </thead>
          <tbody id="rows"></tbody>
        </table>
      </div>
      <aside class="panel viewer">
        <h2 id="viewerTitle">Select a finding</h2>
        <p id="viewerMeta">Use the main table decisions for fast review, or the viewer buttons if you want to inspect one item at a time.</p>
        <div class="preview" id="preview">No finding selected</div>
        <div class="actions">
          <button class="danger" id="viewerQuarantine">Quarantine</button>
          <button class="good" id="viewerKeep">Keep</button>
          <button class="ghost" id="viewerSuggested">Suggested</button>
        </div>
        <div class="helper">Changing a decision here advances to the next visible finding.</div>
        <div class="meta-list" id="details"></div>
      </aside>
    </section>
  </main>

  <script>
    const manifest = ${data};
    const findings = manifest.audit.findings || [];
    const byId = new Map(findings.map(finding => [finding.id, finding]));
    const storageKey = 'slopvault-audit-decisions:' + (manifest.audit.logPath || manifest.generatedAt);
    const selectionKey = storageKey + ':selection';
    const reviewToken = new URLSearchParams(window.location.search).get('token') || '';
    const decisions = loadDecisions();
    const selectedRows = loadSelection();
    let selectedId = findings[0]?.id || null;
    let visibleFindings = [];

    const els = {
      auditFindings: document.querySelector('#auditFindings'),
      quarantineCandidates: document.querySelector('#quarantineCandidates'),
      overrideCount: document.querySelector('#overrideCount'),
      visibleCount: document.querySelector('#visibleCount'),
      rows: document.querySelector('#rows'),
      search: document.querySelector('#search'),
      typeFilter: document.querySelector('#typeFilter'),
      eligibilityFilter: document.querySelector('#eligibilityFilter'),
      decisionFilter: document.querySelector('#decisionFilter'),
      reasonFilter: document.querySelector('#reasonFilter'),
      toggleVisible: document.querySelector('#toggleVisible'),
      exportDecisions: document.querySelector('#exportDecisions'),
      applyDecisions: document.querySelector('#applyDecisions'),
      useSuggestedVisible: document.querySelector('#useSuggestedVisible'),
      selectVisible: document.querySelector('#selectVisible'),
      clearSelected: document.querySelector('#clearSelected'),
      selectedQuarantine: document.querySelector('#selectedQuarantine'),
      selectedKeep: document.querySelector('#selectedKeep'),
      saveState: document.querySelector('#saveState'),
      apiState: document.querySelector('#apiState'),
      viewerTitle: document.querySelector('#viewerTitle'),
      viewerMeta: document.querySelector('#viewerMeta'),
      preview: document.querySelector('#preview'),
      viewerQuarantine: document.querySelector('#viewerQuarantine'),
      viewerKeep: document.querySelector('#viewerKeep'),
      viewerSuggested: document.querySelector('#viewerSuggested'),
      details: document.querySelector('#details'),
    };

    hydrateReasonFilter();
    detectReviewServer();
    render();

    for (const input of [els.search, els.typeFilter, els.eligibilityFilter, els.decisionFilter, els.reasonFilter]) {
      input.addEventListener('input', render);
    }

    els.exportDecisions.addEventListener('click', exportDecisions);
    els.applyDecisions.addEventListener('click', applyDecisions);
    els.useSuggestedVisible.addEventListener('click', () => setVisibleToSuggested());
    els.selectVisible.addEventListener('click', () => setSelectionForVisible(true));
    els.clearSelected.addEventListener('click', () => clearSelectedRows());
    els.selectedQuarantine.addEventListener('click', () => setSelectedRowsDecision('quarantine'));
    els.selectedKeep.addEventListener('click', () => setSelectedRowsDecision('keep'));
    els.toggleVisible.addEventListener('change', () => setSelectionForVisible(els.toggleVisible.checked));
    els.viewerQuarantine.addEventListener('click', () => setDecision(selectedId, 'quarantine', true));
    els.viewerKeep.addEventListener('click', () => setDecision(selectedId, 'keep', true));
    els.viewerSuggested.addEventListener('click', () => resetDecision(selectedId, true));

    function hydrateReasonFilter() {
      const reasons = [...new Set(findings.flatMap(finding => finding.reasons || []))].sort();
      for (const reason of reasons) {
        const option = document.createElement('option');
        option.value = reason;
        option.textContent = reason;
        els.reasonFilter.appendChild(option);
      }
    }

    function loadDecisions() {
      try {
        const parsed = JSON.parse(localStorage.getItem(storageKey) || '{}');
        return parsed && typeof parsed === 'object' ? parsed : {};
      } catch {
        return {};
      }
    }

    function loadSelection() {
      try {
        const parsed = JSON.parse(localStorage.getItem(selectionKey) || '[]');
        return new Set(Array.isArray(parsed) ? parsed : []);
      } catch {
        return new Set();
      }
    }

    function saveUiState() {
      localStorage.setItem(storageKey, JSON.stringify(decisions));
      localStorage.setItem(selectionKey, JSON.stringify(Array.from(selectedRows)));
      els.saveState.textContent = 'Saved locally';
      window.setTimeout(() => {
        els.saveState.textContent = '';
      }, 1500);
    }

    function getDecision(finding) {
      return decisions[finding.id] || finding.defaultAction || 'keep';
    }

    function hasOverride(finding) {
      return Object.prototype.hasOwnProperty.call(decisions, finding.id);
    }

    function resetDecision(id, advance = false) {
      if (!id) return;
      delete decisions[id];
      saveUiState();
      if (advance) selectNextVisible(id);
      render();
    }

    function setDecision(id, action, advance = false) {
      if (!id) return;
      const finding = findings.find(item => item.id === id);
      if (!finding) return;

      if (!action || action === finding.defaultAction) {
        delete decisions[id];
      } else {
        decisions[id] = action;
      }

      saveUiState();
      if (advance) selectNextVisible(id);
      render();
    }

    function selectNextVisible(currentId) {
      const index = visibleFindings.findIndex(item => item.id === currentId);
      if (index >= 0 && index < visibleFindings.length - 1) {
        selectedId = visibleFindings[index + 1].id;
      }
    }

    function setVisibleToSuggested() {
      for (const finding of visibleFindings) {
        delete decisions[finding.id];
      }
      saveUiState();
      render();
    }

    function setSelectionForVisible(isSelected) {
      for (const finding of visibleFindings) {
        if (isSelected) {
          selectedRows.add(finding.id);
        } else {
          selectedRows.delete(finding.id);
        }
      }
      saveUiState();
      render();
    }

    function clearSelectedRows() {
      selectedRows.clear();
      saveUiState();
      render();
    }

    function setSelectedRowsDecision(action) {
      for (const id of selectedRows) {
        const finding = byId.get(id);
        if (!finding) continue;
        if (!action || action === finding.defaultAction) {
          delete decisions[id];
        } else {
          decisions[id] = action;
        }
      }
      saveUiState();
      render();
    }

    function filteredFindings() {
      const q = els.search.value.trim().toLowerCase();
      const type = els.typeFilter.value;
      const eligible = els.eligibilityFilter.value;
      const decision = els.decisionFilter.value;
      const reason = els.reasonFilter.value;

      return findings.filter(finding => {
        if (type && finding.mediaType !== type) return false;
        if (eligible && String(finding.quarantineEligible) !== eligible) return false;
        if (decision === 'overridden' && !hasOverride(finding)) return false;
        if (decision && decision !== 'overridden' && getDecision(finding) !== decision) return false;
        if (reason && !(finding.reasons || []).includes(reason)) return false;
        if (!q) return true;
        return [
          finding.model,
          finding.bucket,
          finding.relativePath,
          finding.sourcePath,
          finding.quarantinePath,
          ...(finding.reasons || []),
        ]
          .filter(Boolean)
          .some(value => String(value).toLowerCase().includes(q));
      });
    }

    function render() {
      visibleFindings = filteredFindings();
      if (!visibleFindings.some(finding => finding.id === selectedId)) {
        selectedId = visibleFindings[0]?.id || null;
      }

      els.auditFindings.textContent = findings.length.toLocaleString();
      els.quarantineCandidates.textContent = findings.filter(finding => finding.quarantineEligible).length.toLocaleString();
      els.overrideCount.textContent = Object.keys(decisions).length.toLocaleString();
      els.visibleCount.textContent = visibleFindings.length.toLocaleString();
      els.toggleVisible.checked =
        visibleFindings.length > 0 &&
        visibleFindings.every(finding => selectedRows.has(finding.id));

      renderRows();
      showFinding(findings.find(finding => finding.id === selectedId));
    }

    function renderRows() {
      const page = visibleFindings.slice(0, 1000);
      els.rows.innerHTML = page.map(finding => {
        const decision = getDecision(finding);
        const decisionClass = decision === 'quarantine' ? 'bad' : 'ok';
        const selected = finding.id === selectedId ? ' class="selected"' : '';
        const checked = selectedRows.has(finding.id) ? ' checked' : '';
        const marker = hasOverride(finding)
          ? '<span class="badge">override</span>'
          : '<span class="badge ok">suggested</span>';
        const trashClass = decision === 'quarantine' ? 'danger active' : 'ghost';
        const keepClass = decision === 'keep' ? 'good active' : 'ghost';
        return \`
          <tr data-id="\${escapeAttr(finding.id)}"\${selected}>
            <td><input class="row-check" data-check-id="\${escapeAttr(finding.id)}" type="checkbox"\${checked} aria-label="Select row"></td>
            <td class="decision-cell">
              <div class="row-actions">
                <button class="row-icon \${trashClass}" data-action-id="\${escapeAttr(finding.id)}" data-action-value="quarantine" title="Quarantine" aria-label="Quarantine">
                  <svg viewBox="0 0 24 24" aria-hidden="true">
                    <path d="M9 3h6l1 2h4v2H4V5h4l1-2zm1 6h2v8h-2V9zm4 0h2v8h-2V9zM7 9h2v8H7V9zm-1 12h12l1-13H5l1 13z"/>
                  </svg>
                </button>
                <button class="row-icon \${keepClass}" data-action-id="\${escapeAttr(finding.id)}" data-action-value="keep" title="Keep" aria-label="Keep">
                  <svg viewBox="0 0 24 24" aria-hidden="true">
                    <path d="M5 4h11l3 3v13H5V4zm2 2v12h10V8.5L15.5 6H15v4H9V6H7zm4 0v2h2V6h-2zm-2 8h6v3H9v-3z"/>
                  </svg>
                </button>
              </div>
              <div class="decision-status"><span class="badge \${decisionClass}">\${decision}</span>\${marker}</div>
            </td>
            <td>\${finding.mediaType || ''}</td>
            <td>\${finding.model || ''}</td>
            <td>\${(finding.reasons || []).map(reason => '<span class="badge warn">' + escapeHtml(reason) + '</span>').join('')}</td>
            <td class="path">\${escapeHtml(finding.relativePath || '')}</td>
            <td>\${formatBytes(finding.sizeBytes || 0)}</td>
          </tr>
        \`;
      }).join('');

      for (const row of els.rows.querySelectorAll('tr')) {
        row.addEventListener('click', () => {
          selectedId = row.dataset.id;
          render();
        });
      }

      for (const checkbox of els.rows.querySelectorAll('[data-check-id]')) {
        checkbox.addEventListener('click', (event) => event.stopPropagation());
        checkbox.addEventListener('change', (event) => {
          const id = event.target.dataset.checkId;
          if (event.target.checked) {
            selectedRows.add(id);
          } else {
            selectedRows.delete(id);
          }
          saveUiState();
          render();
        });
      }

      for (const button of els.rows.querySelectorAll('[data-action-id]')) {
        button.addEventListener('click', (event) => {
          event.stopPropagation();
          setDecision(
            event.currentTarget.dataset.actionId,
            event.currentTarget.dataset.actionValue,
            true
          );
        });
      }
    }

    function showFinding(finding) {
      if (!finding) {
        els.viewerTitle.textContent = 'No findings';
        els.viewerMeta.textContent = 'No audit findings match the current filters.';
        els.preview.textContent = 'No finding selected';
        els.details.innerHTML = '';
        return;
      }

      const decision = getDecision(finding);
      els.viewerTitle.textContent = finding.relativePath || finding.sourcePath || finding.id;
      els.viewerMeta.innerHTML = \`
        <span class="badge \${finding.quarantineEligible ? 'bad' : 'warn'}">\${finding.quarantineEligible ? 'quarantine candidate' : 'report-only'}</span>
        <span class="badge \${decision === 'quarantine' ? 'bad' : 'ok'}">current: \${decision}</span>
        <span class="badge">suggested: \${finding.defaultAction || 'keep'}</span>
        \${hasOverride(finding) ? '<span class="badge">override saved</span>' : ''}
      \`;

      if (finding.mediaType === 'video') {
        els.preview.innerHTML = \`<video src="\${getMediaUrl(finding)}" controls preload="metadata"></video>\`;
      } else if (finding.mediaType === 'image' || finding.mediaType === 'gif') {
        els.preview.innerHTML = \`<img src="\${getMediaUrl(finding)}" alt="">\`;
      } else {
        els.preview.textContent = 'No preview available';
      }

      els.details.innerHTML = \`
        <div><strong>Reasons:</strong> \${(finding.reasons || []).map(escapeHtml).join(', ')}</div>
        <div><strong>Size:</strong> \${formatBytes(finding.sizeBytes || 0)}</div>
        <div><strong>Hash:</strong> \${escapeHtml(finding.contentHash?.value || 'not recorded')}</div>
        <div><strong>Source:</strong> <a class="meta-link" href="\${getMediaUrl(finding)}" target="_blank" rel="noreferrer">Source</a></div>
        <div><strong>Source Path:</strong> <code>\${escapeHtml(finding.sourcePath || '')}</code></div>
        <div><strong>Quarantine target:</strong> <code>\${escapeHtml(finding.quarantinePath || '')}</code></div>
        <div><strong>ID:</strong> \${escapeHtml(finding.id)}</div>
      \`;
    }

    function exportDecisions() {
      const exported = buildDecisionExport();
      const blob = new Blob([JSON.stringify(exported, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = 'slopvault-audit-decisions-' + new Date().toISOString().replace(/[:.]/g, '-') + '.json';
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }

    function getMediaUrl(finding) {
      if (
        (window.location.protocol === 'http:' || window.location.protocol === 'https:') &&
        finding.sourcePath
      ) {
        const tokenParam = reviewToken ? '&token=' + encodeURIComponent(reviewToken) : '';
        return '/media?path=' + encodeURIComponent(finding.sourcePath) + tokenParam;
      }

      return finding.sourceFileUri || '';
    }

    function buildDecisionExport() {
      return {
        generatedAt: new Date().toISOString(),
        auditLogPath: manifest.audit.logPath,
        manifestGeneratedAt: manifest.generatedAt,
        decisions: findings.map((finding) => {
          return {
            id: finding.id,
            action: getDecision(finding),
            sourcePath: finding.sourcePath || null,
            relativePath: finding.relativePath || null,
            reasons: finding.reasons || [],
            quarantineEligible: Boolean(finding.quarantineEligible),
            contentHash: finding.contentHash || null,
          };
        }),
      };
    }

    async function detectReviewServer() {
      if (window.location.protocol !== 'http:' && window.location.protocol !== 'https:') {
        els.applyDecisions.disabled = true;
        els.apiState.textContent = 'Apply requires npm run review:slopvault';
        return;
      }

      try {
        const response = await fetch('/api/status', {
          cache: 'no-store',
          headers: { 'x-slopvault-token': reviewToken },
        });
        if (!response.ok) throw new Error('status unavailable');
        els.applyDecisions.disabled = false;
        els.apiState.textContent = 'Local review server connected';
      } catch {
        els.applyDecisions.disabled = true;
        els.apiState.textContent = 'Apply server unavailable';
      }
    }

    async function applyDecisions() {
      const exported = buildDecisionExport();
      const quarantineCount = exported.decisions.filter(item => item.action === 'quarantine').length;

      if (!quarantineCount) {
        els.apiState.textContent = 'No quarantine decisions to apply';
        return;
      }

      if (!confirm('Move ' + quarantineCount + ' reviewed file(s) to quarantine now?')) return;

      els.applyDecisions.disabled = true;
      els.apiState.textContent = 'Applying quarantine... keep this tab open';

      try {
        const response = await fetch('/api/apply', {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-slopvault-token': reviewToken,
          },
          body: JSON.stringify(exported),
        });
        const result = await response.json();
        if (!response.ok || !result.ok) {
          throw new Error(result.error || 'Apply failed');
        }

        els.apiState.textContent = 'Applied. Reloading dashboard...';
        window.setTimeout(() => window.location.reload(), 800);
      } catch (err) {
        els.applyDecisions.disabled = false;
        els.apiState.textContent = 'Apply failed: ' + err.message;
      }
    }

    function formatBytes(bytes) {
      const units = ['B', 'KB', 'MB', 'GB'];
      let value = bytes;
      let unit = 0;
      while (value >= 1024 && unit < units.length - 1) {
        value /= 1024;
        unit++;
      }
      return value.toFixed(unit ? 1 : 0) + ' ' + units[unit];
    }

    function escapeHtml(value) {
      return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
    }

    function escapeAttr(value) {
      return escapeHtml(value).replace(/'/g, '&#39;');
    }
  </script>
</body>
</html>`
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

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
  String(
    argv['quarantine-root'] || path.join(__dirname, 'quarantine', 'slopvault')
  )
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
    .notice code { background: rgba(53, 208, 255, .12); border-radius: 8px; padding: 2px 6px; color: var(--accent); }
    .controls {
      display: grid;
      grid-template-columns: minmax(220px, 1fr) 140px 170px 160px 150px;
      gap: 10px;
      margin-bottom: 18px;
    }
    input, select, button {
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--card);
      color: var(--ink);
      padding: 11px 14px;
      font: inherit;
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
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(360px, .75fr);
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
    .table-wrap { max-height: 68vh; overflow: auto; }
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
    .actions { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
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
    .footer-tools {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      margin: 18px 0;
    }
    @media (max-width: 1100px) {
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
      Duplicate analysis is still in the manifest, but this page is intentionally focused on corrupt/quarantine candidates.
    </div>
  </header>
  <main>
    <section class="cards">
      <div class="card"><strong id="auditFindings"></strong><span>Audit Findings</span></div>
      <div class="card"><strong id="quarantineCandidates"></strong><span>Quarantine Candidates</span></div>
      <div class="card"><strong id="reviewedCount"></strong><span>Reviewed</span></div>
      <div class="card"><strong id="visibleCount"></strong><span>Visible</span></div>
    </section>

    <section class="notice">
      This dashboard is the dry-run review step. Choose <strong>Quarantine</strong> or <strong>Keep</strong>, export the decisions JSON, then apply it with:
      <code>npm run audit:slopvault -- --apply --decisions &lt;exported-json&gt;</code>.
      The page itself does not move or delete files.
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
        <option value="true">Quarantine candidates</option>
        <option value="false">Report-only</option>
      </select>
      <select id="decisionFilter">
        <option value="">All decisions</option>
        <option value="quarantine">Quarantine</option>
        <option value="keep">Keep</option>
        <option value="unreviewed">Unreviewed</option>
      </select>
      <select id="reasonFilter">
        <option value="">All reasons</option>
      </select>
    </section>

    <section class="footer-tools">
      <button class="primary" id="exportDecisions">Export Decisions JSON</button>
      <button id="markVisibleQuarantine">Mark Visible Quarantine</button>
      <button id="markVisibleKeep">Mark Visible Keep</button>
      <button id="clearVisible">Clear Visible</button>
      <span id="saveState"></span>
    </section>

    <section class="layout">
      <div class="panel table-wrap">
        <table>
          <thead>
            <tr>
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
        <p id="viewerMeta">Pick a row to preview local media and review why it was flagged.</p>
        <div class="preview" id="preview">No finding selected</div>
        <div class="actions">
          <button class="danger" id="viewerQuarantine">Quarantine</button>
          <button class="good" id="viewerKeep">Keep</button>
          <button id="viewerReset">Reset</button>
        </div>
        <div class="meta-list" id="details"></div>
      </aside>
    </section>
  </main>

  <script>
    const manifest = ${data};
    const findings = manifest.audit.findings || [];
    const storageKey = 'slopvault-audit-decisions:' + (manifest.audit.logPath || manifest.generatedAt);
    const decisions = loadDecisions();
    let selectedId = findings[0]?.id || null;
    let visibleFindings = [];

    const els = {
      auditFindings: document.querySelector('#auditFindings'),
      quarantineCandidates: document.querySelector('#quarantineCandidates'),
      reviewedCount: document.querySelector('#reviewedCount'),
      visibleCount: document.querySelector('#visibleCount'),
      rows: document.querySelector('#rows'),
      search: document.querySelector('#search'),
      typeFilter: document.querySelector('#typeFilter'),
      eligibilityFilter: document.querySelector('#eligibilityFilter'),
      decisionFilter: document.querySelector('#decisionFilter'),
      reasonFilter: document.querySelector('#reasonFilter'),
      exportDecisions: document.querySelector('#exportDecisions'),
      markVisibleQuarantine: document.querySelector('#markVisibleQuarantine'),
      markVisibleKeep: document.querySelector('#markVisibleKeep'),
      clearVisible: document.querySelector('#clearVisible'),
      saveState: document.querySelector('#saveState'),
      viewerTitle: document.querySelector('#viewerTitle'),
      viewerMeta: document.querySelector('#viewerMeta'),
      preview: document.querySelector('#preview'),
      viewerQuarantine: document.querySelector('#viewerQuarantine'),
      viewerKeep: document.querySelector('#viewerKeep'),
      viewerReset: document.querySelector('#viewerReset'),
      details: document.querySelector('#details'),
    };

    hydrateReasonFilter();
    render();

    for (const input of [els.search, els.typeFilter, els.eligibilityFilter, els.decisionFilter, els.reasonFilter]) {
      input.addEventListener('input', render);
    }

    els.exportDecisions.addEventListener('click', exportDecisions);
    els.markVisibleQuarantine.addEventListener('click', () => setVisibleDecisions('quarantine'));
    els.markVisibleKeep.addEventListener('click', () => setVisibleDecisions('keep'));
    els.clearVisible.addEventListener('click', () => clearVisibleDecisions());
    els.viewerQuarantine.addEventListener('click', () => setDecision(selectedId, 'quarantine'));
    els.viewerKeep.addEventListener('click', () => setDecision(selectedId, 'keep'));
    els.viewerReset.addEventListener('click', () => setDecision(selectedId, null));

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

    function saveDecisions() {
      localStorage.setItem(storageKey, JSON.stringify(decisions));
      els.saveState.textContent = 'Saved locally';
      window.setTimeout(() => {
        els.saveState.textContent = '';
      }, 1500);
    }

    function getDecision(finding) {
      return decisions[finding.id] || 'unreviewed';
    }

    function setDecision(id, action) {
      if (!id) return;
      const finding = findings.find(item => item.id === id);
      if (!finding) return;

      if (action) {
        decisions[id] = action;
      } else {
        delete decisions[id];
      }

      saveDecisions();
      render();
    }

    function setVisibleDecisions(action) {
      for (const finding of visibleFindings) {
        decisions[finding.id] = action;
      }
      saveDecisions();
      render();
    }

    function clearVisibleDecisions() {
      for (const finding of visibleFindings) {
        delete decisions[finding.id];
      }
      saveDecisions();
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
        if (decision && getDecision(finding) !== decision) return false;
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
      els.reviewedCount.textContent = Object.keys(decisions).length.toLocaleString();
      els.visibleCount.textContent = visibleFindings.length.toLocaleString();

      renderRows();
      showFinding(findings.find(finding => finding.id === selectedId));
    }

    function renderRows() {
      const page = visibleFindings.slice(0, 1000);
      els.rows.innerHTML = page.map(finding => {
        const decision = getDecision(finding);
        const decisionClass = decision === 'quarantine' ? 'bad' : decision === 'keep' ? 'ok' : 'warn';
        const selected = finding.id === selectedId ? ' class="selected"' : '';
        return \`
          <tr data-id="\${escapeAttr(finding.id)}"\${selected}>
            <td><span class="badge \${decisionClass}">\${decision}</span></td>
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
        <span class="badge \${decision === 'quarantine' ? 'bad' : decision === 'keep' ? 'ok' : 'warn'}">decision: \${decision}</span>
        <span class="badge">suggested: \${finding.defaultAction || 'keep'}</span>
      \`;

      if (finding.mediaType === 'video') {
        els.preview.innerHTML = \`<video src="\${finding.sourceFileUri || ''}" controls preload="metadata"></video>\`;
      } else if (finding.mediaType === 'image' || finding.mediaType === 'gif') {
        els.preview.innerHTML = \`<img src="\${finding.sourceFileUri || ''}" alt="">\`;
      } else {
        els.preview.textContent = 'No preview available';
      }

      els.details.innerHTML = \`
        <div><strong>Reasons:</strong> \${(finding.reasons || []).map(escapeHtml).join(', ')}</div>
        <div><strong>Size:</strong> \${formatBytes(finding.sizeBytes || 0)}</div>
        <div><strong>Source:</strong> <a href="\${finding.sourceFileUri || '#'}">\${escapeHtml(finding.sourcePath || '')}</a></div>
        <div><strong>Quarantine target:</strong> \${escapeHtml(finding.quarantinePath || '')}</div>
        <div><strong>ID:</strong> \${escapeHtml(finding.id)}</div>
      \`;
    }

    function exportDecisions() {
      const exported = {
        generatedAt: new Date().toISOString(),
        auditLogPath: manifest.audit.logPath,
        manifestGeneratedAt: manifest.generatedAt,
        decisions: Object.entries(decisions).map(([id, action]) => {
          const finding = findings.find(item => item.id === id);
          return {
            id,
            action,
            sourcePath: finding?.sourcePath || null,
            relativePath: finding?.relativePath || null,
            reasons: finding?.reasons || [],
            quarantineEligible: Boolean(finding?.quarantineEligible),
          };
        }),
      };
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

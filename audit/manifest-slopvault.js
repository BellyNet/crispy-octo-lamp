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

const rootDir = path.join(__dirname, '..')
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

  for (const scanRoot of roots) {
    if (!fs.existsSync(scanRoot.root)) continue

    console.log(`Scanning ${scanRoot.label}: ${scanRoot.root}`)
    for (const filePath of collectMediaFiles(scanRoot.root)) {
      records.push(await buildRecord(filePath, scanRoot))
    }
  }

  const manifest = buildManifest(records)
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
  console.log(`Potential duplicate groups: ${manifest.duplicates.length}`)
  console.log(`Manifest: ${manifestPath}`)
  console.log(`Dashboard: ${latestDashboardPath}`)
}

function printHelp() {
  console.log(`Usage: node audit/manifest-slopvault.js [options]

Options:
  --hash                     Compute md5 hashes for exact duplicate detection.
  --slopvault-root <path>    Override Slopvault root.
  --dataset-root <path>      Override dataset root.
  --quarantine-root <path>   Override quarantine root.
  --output-dir <path>        Override manifest output directory.
  --dashboard-dir <path>     Override dashboard output directory.
  -h, --help                 Show help.

Notes:
  The dashboard is a self-contained local HTML file.
  Hashing is optional because reading every byte of the dataset is slower.
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

function buildManifest(records) {
  const duplicates = buildDuplicateGroups(records)
  const byMediaType = countBy(records, (record) => record.mediaType)
  const byRoot = countBy(records, (record) => record.root)

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
      byMediaType,
      byRoot,
    },
    duplicates,
    records,
  }
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

function normalizePath(value) {
  return value.split(path.sep).join('/')
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
  <title>Slopvault Dashboard</title>
  <style>
    :root {
      --ink: #201a17;
      --muted: #766b64;
      --paper: #fff8ef;
      --card: #fffdf8;
      --line: #eadccd;
      --accent: #b44b2a;
      --accent-soft: #f5d0bf;
      --good: #2e6f4e;
      --warn: #996a11;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 20% 0%, rgba(180,75,42,.18), transparent 28rem),
        linear-gradient(135deg, #fff8ef, #f5eadc 55%, #efe0cf);
    }
    header {
      padding: 34px 40px 22px;
      border-bottom: 1px solid var(--line);
    }
    h1 {
      margin: 0;
      font-size: clamp(34px, 5vw, 68px);
      letter-spacing: -0.06em;
      line-height: .9;
    }
    .subhead { color: var(--muted); margin-top: 12px; }
    main { padding: 24px 40px 48px; }
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 14px;
      margin-bottom: 20px;
    }
    .card {
      background: rgba(255,253,248,.82);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 14px 40px rgba(91,55,31,.08);
    }
    .card strong { display: block; font-size: 28px; }
    .card span { color: var(--muted); font-size: 13px; text-transform: uppercase; letter-spacing: .08em; }
    .controls {
      display: grid;
      grid-template-columns: minmax(220px, 1fr) 150px 150px 150px;
      gap: 10px;
      margin-bottom: 18px;
    }
    input, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--card);
      color: var(--ink);
      padding: 11px 14px;
      font: inherit;
    }
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1.3fr) minmax(340px, .7fr);
      gap: 18px;
      align-items: start;
    }
    .panel {
      background: rgba(255,253,248,.9);
      border: 1px solid var(--line);
      border-radius: 22px;
      overflow: hidden;
      box-shadow: 0 18px 50px rgba(91,55,31,.1);
    }
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
      background: #fff3e3;
      z-index: 1;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .06em;
    }
    tr { cursor: pointer; }
    tr:hover { background: #fff3e3; }
    .badge {
      display: inline-block;
      border-radius: 999px;
      padding: 3px 8px;
      background: var(--accent-soft);
      color: #612511;
      font-size: 12px;
    }
    .badge.ok { background: #d7eadc; color: var(--good); }
    .badge.warn { background: #f4e0ad; color: var(--warn); }
    .viewer { padding: 18px; position: sticky; top: 16px; }
    .viewer h2 { margin: 0 0 8px; font-size: 22px; }
    .viewer p { color: var(--muted); overflow-wrap: anywhere; }
    .preview {
      width: 100%;
      min-height: 260px;
      display: grid;
      place-items: center;
      background: #1d1714;
      border-radius: 18px;
      overflow: hidden;
      margin-top: 14px;
    }
    .preview img, .preview video {
      width: 100%;
      max-height: 520px;
      object-fit: contain;
      background: #1d1714;
    }
    .dupes {
      margin-top: 18px;
      padding: 18px;
    }
    .dupe-list {
      display: grid;
      gap: 8px;
      max-height: 320px;
      overflow: auto;
    }
    .dupe-item {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 10px;
      background: #fff8ef;
      font-size: 13px;
    }
    @media (max-width: 980px) {
      main, header { padding-left: 18px; padding-right: 18px; }
      .controls, .layout { grid-template-columns: 1fr; }
      .viewer { position: static; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Slopvault Dashboard</h1>
    <div class="subhead">Generated at ${escapeHtml(manifest.generatedAt)}. ${manifest.hashAlgorithm ? `Exact hashes: ${manifest.hashAlgorithm}` : 'Hashing skipped; duplicate groups are size/name hints.'}</div>
  </header>
  <main>
    <section class="cards">
      <div class="card"><strong id="totalFiles"></strong><span>Total Files</span></div>
      <div class="card"><strong id="datasetFiles"></strong><span>Dataset</span></div>
      <div class="card"><strong id="quarantineFiles"></strong><span>Quarantine</span></div>
      <div class="card"><strong id="duplicateGroups"></strong><span>Duplicate Groups</span></div>
    </section>

    <section class="controls">
      <input id="search" placeholder="Search model, filename, path...">
      <select id="rootFilter">
        <option value="">All roots</option>
        <option value="dataset">Dataset</option>
        <option value="quarantine_dataset">Quarantine dataset</option>
        <option value="quarantine_incomplete">Quarantine incomplete</option>
      </select>
      <select id="typeFilter">
        <option value="">All media</option>
        <option value="image">Images</option>
        <option value="gif">GIFs</option>
        <option value="video">Videos</option>
      </select>
      <select id="quarantineFilter">
        <option value="">All states</option>
        <option value="true">Quarantined</option>
        <option value="false">Not quarantined</option>
      </select>
    </section>

    <section class="layout">
      <div class="panel">
        <table>
          <thead>
            <tr>
              <th>State</th>
              <th>Type</th>
              <th>Model</th>
              <th>Path</th>
              <th>Size</th>
            </tr>
          </thead>
          <tbody id="rows"></tbody>
        </table>
      </div>
      <aside class="panel viewer">
        <h2 id="viewerTitle">Select a file</h2>
        <p id="viewerMeta">Pick a row to preview local media and inspect its path.</p>
        <div class="preview" id="preview">No file selected</div>
      </aside>
    </section>

    <section class="panel dupes">
      <h2>Duplicate Candidates</h2>
      <p>Groups are exact md5 matches when generated with <code>--hash</code>; otherwise they are size + filename hints.</p>
      <div class="dupe-list" id="dupes"></div>
    </section>
  </main>

  <script>
    const manifest = ${data};
    const records = manifest.records;
    const byId = new Map(records.map(record => [record.id, record]));

    const els = {
      totalFiles: document.querySelector('#totalFiles'),
      datasetFiles: document.querySelector('#datasetFiles'),
      quarantineFiles: document.querySelector('#quarantineFiles'),
      duplicateGroups: document.querySelector('#duplicateGroups'),
      rows: document.querySelector('#rows'),
      search: document.querySelector('#search'),
      rootFilter: document.querySelector('#rootFilter'),
      typeFilter: document.querySelector('#typeFilter'),
      quarantineFilter: document.querySelector('#quarantineFilter'),
      viewerTitle: document.querySelector('#viewerTitle'),
      viewerMeta: document.querySelector('#viewerMeta'),
      preview: document.querySelector('#preview'),
      dupes: document.querySelector('#dupes'),
    };

    els.totalFiles.textContent = manifest.summary.totalFiles.toLocaleString();
    els.datasetFiles.textContent = manifest.summary.datasetFiles.toLocaleString();
    els.quarantineFiles.textContent = manifest.summary.quarantineFiles.toLocaleString();
    els.duplicateGroups.textContent = manifest.duplicates.length.toLocaleString();

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

    function filteredRecords() {
      const q = els.search.value.trim().toLowerCase();
      const root = els.rootFilter.value;
      const type = els.typeFilter.value;
      const quarantine = els.quarantineFilter.value;
      return records.filter(record => {
        if (root && record.root !== root) return false;
        if (type && record.mediaType !== type) return false;
        if (quarantine && String(record.quarantined) !== quarantine) return false;
        if (!q) return true;
        return [record.model, record.filename, record.datasetRelativePath, record.absolutePath]
          .filter(Boolean)
          .some(value => String(value).toLowerCase().includes(q));
      });
    }

    function renderRows() {
      const page = filteredRecords().slice(0, 700);
      els.rows.innerHTML = page.map(record => \`
        <tr data-id="\${record.id}">
          <td><span class="badge \${record.quarantined ? 'warn' : 'ok'}">\${record.quarantined ? 'quarantine' : 'dataset'}</span></td>
          <td>\${record.mediaType}</td>
          <td>\${record.model || ''}</td>
          <td>\${record.datasetRelativePath}</td>
          <td>\${formatBytes(record.sizeBytes)}</td>
        </tr>
      \`).join('');

      for (const row of els.rows.querySelectorAll('tr')) {
        row.addEventListener('click', () => showRecord(byId.get(row.dataset.id)));
      }
    }

    function showRecord(record) {
      els.viewerTitle.textContent = record.filename;
      els.viewerMeta.innerHTML = \`
        <strong>\${record.datasetRelativePath}</strong><br>
        \${formatBytes(record.sizeBytes)} · \${record.mediaType} · \${record.root}<br>
        \${record.md5 ? 'md5: ' + record.md5 + '<br>' : ''}
        <a href="\${record.fileUri}">\${record.absolutePath}</a>
      \`;

      if (record.mediaType === 'video') {
        els.preview.innerHTML = \`<video src="\${record.fileUri}" controls preload="metadata"></video>\`;
      } else {
        els.preview.innerHTML = \`<img src="\${record.fileUri}" alt="">\`;
      }
    }

    function renderDupes() {
      els.dupes.innerHTML = manifest.duplicates.slice(0, 100).map(group => {
        const paths = group.records
          .map(id => byId.get(id))
          .filter(Boolean)
          .map(record => \`<div>\${record.datasetRelativePath} <span class="badge">\${formatBytes(record.sizeBytes)}</span></div>\`)
          .join('');
        return \`<div class="dupe-item"><strong>\${group.count} files · \${formatBytes(group.totalBytes)}</strong>\${paths}</div>\`;
      }).join('') || '<p>No duplicate candidates.</p>';
    }

    for (const input of [els.search, els.rootFilter, els.typeFilter, els.quarantineFilter]) {
      input.addEventListener('input', renderRows);
    }

    renderRows();
    renderDupes();
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

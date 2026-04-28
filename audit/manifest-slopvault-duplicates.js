const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const minimist = require('minimist')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
  },
  boolean: ['help'],
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const auditDir = __dirname
const outputDir = path.resolve(
  String(argv['output-dir'] || path.join(auditDir, 'manifests'))
)
const dashboardDir = path.resolve(
  String(argv['dashboard-dir'] || path.join(auditDir, 'dashboard'))
)
const sourceManifestPath = path.resolve(
  String(
    argv.manifest || path.join(outputDir, 'slopvault-manifest-latest.json')
  )
)
const decisionsPath = path.resolve(
  String(
    argv.decisions ||
      path.join(
        outputDir,
        'slopvault-duplicate-dashboard-decisions-latest.json'
      )
  )
)
const runStamp = new Date().toISOString().replace(/[:.]/g, '-')

Promise.resolve()
  .then(main)
  .catch((err) => {
    console.error(`Fatal duplicate manifest error: ${err.stack || err.message}`)
    process.exitCode = 1
  })

function main() {
  ensureDir(outputDir)
  ensureDir(dashboardDir)

  if (!fs.existsSync(sourceManifestPath)) {
    throw new Error(`Missing source manifest: ${sourceManifestPath}`)
  }

  const sourceManifest = JSON.parse(
    fs.readFileSync(sourceManifestPath, 'utf8').replace(/^\uFEFF/, '')
  )
  if (sourceManifest.hashAlgorithm !== 'md5') {
    throw new Error(
      `Source manifest must be hash-backed (md5). Rebuild ${sourceManifestPath} with --hash first.`
    )
  }

  const persistedSelections = loadPersistedSelections(decisionsPath)
  const manifest = buildDuplicateManifest(sourceManifest, persistedSelections)

  const manifestPath = path.join(
    outputDir,
    `slopvault-duplicate-manifest-${runStamp}.json`
  )
  const latestManifestPath = path.join(
    outputDir,
    'slopvault-duplicate-manifest-latest.json'
  )
  const dashboardPath = path.join(
    dashboardDir,
    `slopvault-duplicates-dashboard-${runStamp}.html`
  )
  const latestDashboardPath = path.join(
    dashboardDir,
    'slopvault-duplicates-dashboard.html'
  )

  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2))
  fs.writeFileSync(latestManifestPath, JSON.stringify(manifest, null, 2))
  fs.writeFileSync(dashboardPath, renderDashboard(manifest))
  fs.writeFileSync(latestDashboardPath, renderDashboard(manifest))

  console.log(`Source manifest: ${sourceManifestPath}`)
  console.log(`Duplicate groups: ${manifest.summary.totalGroups}`)
  console.log(`Cross-model groups: ${manifest.summary.crossModelGroups}`)
  console.log(`Duplicate files in review: ${manifest.summary.totalActiveFiles}`)
  console.log(`Suggested quarantines: ${manifest.summary.suggestedQuarantines}`)
  console.log(`Manifest: ${latestManifestPath}`)
  console.log(`Dashboard: ${latestDashboardPath}`)
}

function printHelp() {
  console.log(`Usage: node audit/manifest-slopvault-duplicates.js [options]

Options:
  --manifest <path>       Source slopvault manifest JSON. Must be built with --hash.
  --decisions <path>      Existing duplicate dashboard decisions JSON.
  --output-dir <path>     Output directory for duplicate manifest JSON.
  --dashboard-dir <path>  Output directory for duplicate dashboard HTML.
  -h, --help              Show help.
`)
}

function loadPersistedSelections(filePath) {
  if (!fs.existsSync(filePath)) return new Map()

  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'))
    const groups = Array.isArray(parsed?.groups) ? parsed.groups : []
    const byGroupId = new Map()

    for (const group of groups) {
      if (!group?.groupId) continue

      const keptRecordIds = Array.isArray(group.keptRecordIds)
        ? group.keptRecordIds.map((value) => String(value))
        : group?.preferredRecordId
          ? [String(group.preferredRecordId)]
          : []

      if (!keptRecordIds.length) continue
      byGroupId.set(String(group.groupId), keptRecordIds)
    }

    return byGroupId
  } catch (err) {
    console.warn(
      `Could not load duplicate decisions ${filePath}: ${err.message}`
    )
    return new Map()
  }
}

function buildDuplicateManifest(sourceManifest, persistedSelections) {
  const recordsById = new Map(
    (Array.isArray(sourceManifest.records) ? sourceManifest.records : []).map(
      (record) => [record.id, record]
    )
  )
  const groups = []

  for (const duplicateGroup of Array.isArray(sourceManifest.duplicates)
    ? sourceManifest.duplicates
    : []) {
    if (!String(duplicateGroup?.key || '').startsWith('md5:')) continue

    const activeRecords = (
      Array.isArray(duplicateGroup.records) ? duplicateGroup.records : []
    )
      .map((recordId) => recordsById.get(recordId))
      .filter(Boolean)
      .filter((record) => record.root === 'dataset' && !record.quarantined)
      .map((record) => toDuplicateRecord(record, sourceManifest.roots))

    if (activeRecords.length < 2) continue

    const sortedRecords = [...activeRecords].sort(compareDuplicateRecords)
    const groupId = createGroupId(duplicateGroup.key)
    const suggestedKeep = chooseSuggestedKeep(sortedRecords)
    const persistedKeptIds = Array.isArray(persistedSelections.get(groupId))
      ? persistedSelections.get(groupId)
      : []
    const currentKeptIds = sortedRecords
      .filter((record) => persistedKeptIds.includes(record.id))
      .map((record) => record.id)
    const effectiveKeptIds = currentKeptIds.length
      ? currentKeptIds
      : [suggestedKeep.id]
    const models = [
      ...new Set(sortedRecords.map((record) => record.model).filter(Boolean)),
    ]

    groups.push({
      id: groupId,
      contentHash: {
        algorithm: 'md5',
        value: duplicateGroup.key.replace(/^md5:/, ''),
      },
      mediaType: sortedRecords[0]?.mediaType || 'image',
      totalCopies: sortedRecords.length,
      crossModel: models.length > 1,
      models,
      suggestedKeepId: suggestedKeep.id,
      suggestedKeepPath: suggestedKeep.relativePath,
      currentKeptIds: effectiveKeptIds,
      currentKeptPaths: sortedRecords
        .filter((record) => effectiveKeptIds.includes(record.id))
        .map((record) => record.relativePath),
      records: sortedRecords,
    })
  }

  groups.sort((left, right) => {
    if (left.crossModel !== right.crossModel) {
      return left.crossModel ? -1 : 1
    }
    if (left.totalCopies !== right.totalCopies) {
      return right.totalCopies - left.totalCopies
    }
    return left.suggestedKeepPath.localeCompare(right.suggestedKeepPath)
  })

  return {
    generatedAt: new Date().toISOString(),
    sourceManifestPath,
    roots: sourceManifest.roots || {},
    summary: {
      totalGroups: groups.length,
      crossModelGroups: groups.filter((group) => group.crossModel).length,
      totalActiveFiles: groups.reduce(
        (sum, group) => sum + group.records.length,
        0
      ),
      suggestedQuarantines: groups.reduce(
        (sum, group) => sum + Math.max(0, group.records.length - 1),
        0
      ),
    },
    groups,
  }
}

function toDuplicateRecord(record, roots) {
  return {
    id: record.id,
    model: record.model || null,
    bucket: record.bucket || null,
    filename: record.filename || null,
    mediaType: record.mediaType || null,
    relativePath: normalizePath(record.relativePath || ''),
    datasetRelativePath: normalizePath(record.datasetRelativePath || ''),
    sourcePath: record.absolutePath || null,
    sourceFileUri: record.fileUri || null,
    quarantinePath: buildQuarantinePath(record.relativePath, roots),
    sizeBytes: record.sizeBytes || 0,
    modifiedAt: record.modifiedAt || null,
    occurrenceAt: extractOccurrenceAt(record),
  }
}

function buildQuarantinePath(relativePath, roots) {
  const normalized = normalizePath(relativePath)
  const quarantineRoot = roots?.quarantineRoot
  if (!normalized || !quarantineRoot) return null

  return path.join(quarantineRoot, 'dataset', ...normalized.split('/'))
}

function extractOccurrenceAt(record) {
  const match = /^(\d{14})-/.exec(String(record.filename || ''))
  if (!match) return null

  const value = match[1]
  const iso =
    `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}` +
    `T${value.slice(8, 10)}:${value.slice(10, 12)}:${value.slice(12, 14)}Z`
  const ms = Date.parse(iso)
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null
}

function chooseSuggestedKeep(records) {
  return [...records].sort(compareDuplicateRecords)[0]
}

function compareDuplicateRecords(left, right) {
  const leftOccurrence = left.occurrenceAt
    ? Date.parse(left.occurrenceAt)
    : Infinity
  const rightOccurrence = right.occurrenceAt
    ? Date.parse(right.occurrenceAt)
    : Infinity

  if (leftOccurrence !== rightOccurrence) {
    return leftOccurrence - rightOccurrence
  }

  const leftModified = left.modifiedAt ? Date.parse(left.modifiedAt) : Infinity
  const rightModified = right.modifiedAt
    ? Date.parse(right.modifiedAt)
    : Infinity
  if (leftModified !== rightModified) {
    return leftModified - rightModified
  }

  return left.relativePath.localeCompare(right.relativePath)
}

function createGroupId(value) {
  return crypto
    .createHash('sha1')
    .update(String(value || ''))
    .digest('hex')
}

function normalizePath(value) {
  return String(value || '')
    .trim()
    .replace(/\\/g, '/')
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function renderDashboard(manifest) {
  const manifestJson = JSON.stringify(manifest)

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Slopvault Duplicate Review</title>
  <style>
    :root {
      --bg: #f4efe7;
      --panel: #fffaf1;
      --line: #d8c9b0;
      --ink: #1f1b16;
      --muted: #6b6254;
      --accent: #0f766e;
      --accent-soft: #d4f0ed;
      --warn: #b45309;
      --warn-soft: #fde7c2;
      --keep: #166534;
      --keep-soft: #dcfce7;
    }

    * { box-sizing: border-box; }
    html, body {
      height: 100%;
      overflow: hidden;
    }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15,118,110,0.12), transparent 28%),
        linear-gradient(180deg, #f7f0e5 0%, #f0e6d6 100%);
    }
    .app {
      display: grid;
      grid-template-columns: 320px 1fr;
      height: 100vh;
    }
    .sidebar {
      border-right: 1px solid var(--line);
      background: rgba(255,250,241,0.92);
      padding: 18px;
      overflow: auto;
      min-height: 0;
    }
    .main {
      padding: 20px;
      overflow: hidden;
      min-height: 0;
      display: grid;
      grid-template-rows: auto 1fr;
    }
    #viewer {
      min-height: 0;
      overflow: hidden;
      display: grid;
      grid-template-rows: auto 1fr;
      gap: 10px;
    }
    h1, h2, h3, h4, p { margin: 0; }
    .summary, .group-row, .media-card, .copy-card {
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 16px;
    }
    .summary {
      padding: 14px;
      margin-bottom: 16px;
    }
    .summary p + p { margin-top: 6px; color: var(--muted); }
    .toolbar {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 16px;
    }
    button {
      border: 1px solid var(--line);
      background: white;
      color: var(--ink);
      border-radius: 999px;
      padding: 10px 14px;
      font: inherit;
      cursor: pointer;
    }
    button.primary {
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }
    button.keep {
      background: var(--keep-soft);
      border-color: #86efac;
      color: var(--keep);
    }
    button:disabled { opacity: 0.55; cursor: default; }
    .group-list {
      display: grid;
      gap: 10px;
    }
    .group-row {
      padding: 12px;
      cursor: pointer;
    }
    .group-row.active {
      border-color: var(--accent);
      box-shadow: 0 0 0 2px rgba(15,118,110,0.15);
    }
    .badge {
      display: inline-block;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 12px;
      line-height: 1.4;
      background: var(--warn-soft);
      color: var(--warn);
      margin-right: 6px;
    }
    .badge.keep {
      background: var(--keep-soft);
      color: var(--keep);
    }
    .compare-shell {
      position: relative;
      z-index: 1;
      background: linear-gradient(180deg, rgba(240,230,214,0.98) 0%, rgba(240,230,214,0.98) 100%);
      padding-bottom: 4px;
    }
    .compare-layout {
      display: grid;
      grid-template-columns: minmax(260px, 0.9fr) minmax(420px, 1.1fr);
      gap: 16px;
      align-items: start;
      margin-bottom: 10px;
    }
    .media-card {
      padding: 14px;
    }
    .media-frame {
      background: #efe5d5;
      border-radius: 12px;
      overflow: hidden;
      min-height: 160px;
      display: flex;
      align-items: center;
      justify-content: center;
      margin-top: 8px;
    }
    .media-frame img, .media-frame video {
      width: 100%;
      max-height: 28vh;
      object-fit: contain;
      display: block;
      background: #e8dcc8;
    }
    .meta {
      margin-top: 10px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
      word-break: break-word;
    }
    .copies {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
    }
    .copies-section {
      min-height: 0;
      overflow: auto;
      padding-right: 6px;
      padding-bottom: 12px;
    }
    .compare-options {
      display: grid;
      gap: 8px;
      max-height: 34vh;
      overflow: auto;
      padding-right: 4px;
    }
    .compare-option {
      border: 1px solid var(--line);
      background: rgba(255,250,241,0.96);
      border-radius: 12px;
      padding: 10px 12px;
      min-width: 0;
    }
    .compare-option.active {
      border-color: var(--accent);
      box-shadow: 0 0 0 2px rgba(15,118,110,0.15);
    }
    .compare-option.keep {
      border-color: #86efac;
    }
    .compare-option .meta {
      margin-top: 4px;
      font-size: 12px;
      line-height: 1.35;
    }
    .choice-row {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: flex-start;
    }
    .choice-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
      min-width: 168px;
    }
    .copy-card {
      padding: 12px;
    }
    .copy-card.selected {
      border-color: var(--accent);
      box-shadow: 0 0 0 2px rgba(15,118,110,0.15);
    }
    .copy-card.keep {
      border-color: #86efac;
      box-shadow: 0 0 0 2px rgba(34,197,94,0.14);
    }
    .copy-actions {
      display: flex;
      gap: 8px;
      margin-top: 10px;
      flex-wrap: wrap;
    }
    .empty {
      padding: 28px;
      text-align: center;
      color: var(--muted);
    }
    @media (max-width: 1000px) {
      html, body { overflow: auto; }
      .app { grid-template-columns: 1fr; height: auto; min-height: 100vh; }
      .sidebar { border-right: 0; border-bottom: 1px solid var(--line); }
      .main { overflow: visible; display: block; }
      #viewer { overflow: visible; display: block; }
      .copies-section { overflow: visible; padding-right: 0; }
      .compare-layout { grid-template-columns: 1fr; }
      .compare-options { max-height: none; }
    }
  </style>
</head>
<body>
  <div class="app">
    <aside class="sidebar">
      <div class="summary">
        <h1>Duplicate Review</h1>
        <p>${manifest.summary.totalGroups} exact duplicate groups</p>
        <p>${manifest.summary.crossModelGroups} cross-model groups, ${manifest.summary.suggestedQuarantines} suggested quarantines</p>
      </div>
      <div class="toolbar">
        <label><input id="crossModelOnly" type="checkbox"> Cross-model only</label>
      </div>
      <div id="groupList" class="group-list"></div>
    </aside>
    <main class="main">
      <div class="toolbar">
        <button id="prevGroup">Previous</button>
        <button id="nextGroup">Next</button>
        <button id="applyDecisions" class="primary">Apply Duplicate Decisions</button>
        <span id="statusText"></span>
      </div>
      <div id="viewer"></div>
    </main>
  </div>
  <script>
    const manifest = ${manifestJson};
    const reviewToken = new URLSearchParams(window.location.search).get('token') || '';
    const els = {
      groupList: document.getElementById('groupList'),
      viewer: document.getElementById('viewer'),
      crossModelOnly: document.getElementById('crossModelOnly'),
      prevGroup: document.getElementById('prevGroup'),
      nextGroup: document.getElementById('nextGroup'),
      applyDecisions: document.getElementById('applyDecisions'),
      statusText: document.getElementById('statusText'),
    };

    const keptByGroup = new Map(
      manifest.groups.map(group => [group.id, new Set(group.currentKeptIds || [])])
    );
    const selectedCompareByGroup = new Map();
    let visibleGroups = [];
    let selectedGroupId = manifest.groups[0] ? manifest.groups[0].id : null;

    for (const group of manifest.groups) {
      const compare = group.records.find(record => !isKept(group, record.id));
      if (compare) selectedCompareByGroup.set(group.id, compare.id);
    }

    els.crossModelOnly.addEventListener('change', render);
    els.prevGroup.addEventListener('click', () => moveGroup(-1));
    els.nextGroup.addEventListener('click', () => moveGroup(1));
    els.applyDecisions.addEventListener('click', applyDecisions);

    render();
    detectReviewServer();

    function render() {
      visibleGroups = manifest.groups.filter(group => !els.crossModelOnly.checked || group.crossModel);
      if (!visibleGroups.length) {
        selectedGroupId = null;
      } else if (!visibleGroups.some(group => group.id === selectedGroupId)) {
        selectedGroupId = visibleGroups[0].id;
      }

      renderGroupList();
      renderViewer();
    }

    function renderGroupList() {
      if (!visibleGroups.length) {
        els.groupList.innerHTML = '<div class="empty">No duplicate groups match the current filter.</div>';
        return;
      }

      els.groupList.innerHTML = visibleGroups.map(group => {
        const suggestedKeep = getSuggestedKeepRecord(group);
        const keptCount = getKeptRecords(group).length;
        const cross = group.crossModel ? '<span class="badge">cross-model</span>' : '';
        const keepBadge = '<span class="badge keep">kept: ' + escapeHtml(String(keptCount)) + '</span>';
        const active = group.id === selectedGroupId ? ' active' : '';

        return '<div class="group-row' + active + '" data-group-id="' + escapeAttr(group.id) + '">' +
          '<div>' + cross + keepBadge + '</div>' +
          '<div><strong>' + escapeHtml(suggestedKeep.relativePath) + '</strong></div>' +
          '<div class="meta">' + escapeHtml(String(group.totalCopies)) + ' active copies across ' + escapeHtml(String(group.models.length)) + ' models</div>' +
        '</div>';
      }).join('');

      for (const row of els.groupList.querySelectorAll('.group-row')) {
        row.addEventListener('click', () => {
          selectedGroupId = row.dataset.groupId;
          render();
        });
      }
    }

    function renderViewer() {
      const group = visibleGroups.find(item => item.id === selectedGroupId);
      if (!group) {
        els.viewer.innerHTML = '<div class="empty">Choose a duplicate group to compare.</div>';
        return;
      }

      const suggestedKeep = getSuggestedKeepRecord(group);
      const compare = getCompareRecord(group);
      const keptRecords = getKeptRecords(group);

      els.viewer.innerHTML =
        '<div class="compare-shell">' +
          '<div class="summary">' +
            '<p><strong>Rule of thumb:</strong> keep the earliest appearance and quarantine the rest, but you can keep multiple copies when each model should retain its own file.</p>' +
            '<p>Suggested keep: <code>' + escapeHtml(suggestedKeep.relativePath) + '</code></p>' +
            '<p>Currently keeping ' + escapeHtml(String(keptRecords.length)) + ' of ' + escapeHtml(String(group.records.length)) + ' active copies.</p>' +
          '</div>' +
          '<div class="compare-meta">' +
            '<div><strong>Suggested model:</strong> ' + escapeHtml(suggestedKeep.model || 'unknown') + '</div>' +
            '<div><strong>Comparing to:</strong> ' + escapeHtml(compare ? (compare.model || 'unknown') : 'none') + '</div>' +
            '<div><strong>Total copies:</strong> ' + escapeHtml(String(group.records.length)) + '</div>' +
          '</div>' +
          '<div class="compare-grid">' +
            renderMediaCard(group, suggestedKeep, 'Suggested keep', group.suggestedKeepId === suggestedKeep.id) +
            renderMediaCard(group, compare, 'Compare candidate', false) +
          '</div>' +
          '<div><strong>Compare against any copy in this group:</strong></div>' +
          '<div class="compare-options">' +
            group.records.map(record => renderCompareOption(group, record, compare)).join('') +
          '</div>' +
        '</div>' +
        '<div class="copies-section">' +
          '<h3 style="margin: 0 0 12px;">All active copies in this group</h3>' +
          '<div class="copies">' +
            group.records.map(record => renderCopyCard(group, record, suggestedKeep, compare)).join('') +
          '</div>' +
        '</div>';

      for (const button of els.viewer.querySelectorAll('[data-toggle-keep-record-id]')) {
        button.addEventListener('click', () => {
          toggleKeep(group, button.dataset.toggleKeepRecordId);
          render();
        });
      }

      for (const button of els.viewer.querySelectorAll('[data-compare-record-id]')) {
        button.addEventListener('click', () => {
          selectedCompareByGroup.set(group.id, button.dataset.compareRecordId);
          render();
        });
      }
    }

    function renderMediaCard(group, record, title, isKeep) {
      if (!record) {
        return '<section class="media-card"><h2>' + escapeHtml(title) + '</h2><div class="empty">No second copy left in this group.</div></section>';
      }

      const kept = isKept(group, record.id);
      const label = kept ? 'kept' : 'quarantine';
      const action = kept ? 'Uncheck keep' : 'Keep this copy';

      return '<section class="media-card">' +
        '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;">' +
          '<div>' +
            '<h2>' + escapeHtml(title) + '</h2>' +
            '<div>' + (kept ? '<span class="badge keep">kept</span>' : '<span class="badge">quarantine</span>') + (isKeep ? '<span class="badge keep">suggested</span>' : '') + '</div>' +
          '</div>' +
          '<button class="' + (kept ? 'keep' : '') + '" data-toggle-keep-record-id="' + escapeAttr(record.id) + '">' + action + '</button>' +
        '</div>' +
        '<div class="media-frame">' + renderPreview(record) + '</div>' +
        '<div class="meta">' +
          '<div><strong>Model:</strong> ' + escapeHtml(record.model || 'unknown') + '</div>' +
          '<div><strong>Bucket:</strong> ' + escapeHtml(record.bucket || 'unknown') + '</div>' +
          '<div><strong>Path:</strong> <code>' + escapeHtml(record.relativePath) + '</code></div>' +
          '<div><strong>Filename:</strong> ' + escapeHtml(record.filename || 'unknown') + '</div>' +
          '<div><strong>Seen at:</strong> ' + escapeHtml(record.occurrenceAt || record.modifiedAt || 'unknown') + '</div>' +
          '<div><strong>Size:</strong> ' + escapeHtml(formatBytes(record.sizeBytes || 0)) + '</div>' +
          '<div><strong>Decision:</strong> ' + escapeHtml(label) + '</div>' +
        '</div>' +
      '</section>';
    }

    function renderCompareOption(group, record, compare) {
      const active = compare && compare.id === record.id ? ' active' : '';
      const kept = isKept(group, record.id) ? ' keep' : '';
      return '<article class="compare-option' + active + kept + '">' +
        '<div><strong>' + escapeHtml(record.model || 'unknown') + '</strong></div>' +
        '<div class="meta"><code>' + escapeHtml(record.relativePath) + '</code></div>' +
        '<div class="meta">Seen at ' + escapeHtml(record.occurrenceAt || record.modifiedAt || 'unknown') + '</div>' +
        '<div class="copy-actions">' +
          '<button data-compare-record-id="' + escapeAttr(record.id) + '">Preview this copy</button>' +
          '<button class="' + (isKept(group, record.id) ? 'keep' : '') + '" data-toggle-keep-record-id="' + escapeAttr(record.id) + '">' + (isKept(group, record.id) ? 'Uncheck keep' : 'Keep this copy') + '</button>' +
        '</div>' +
      '</article>';
    }

    function renderCopyCard(group, record, keep, compare) {
      const selected = compare && compare.id === record.id ? ' selected' : '';
      const keepClass = isKept(group, record.id) ? ' keep' : '';
      const kept = isKept(group, record.id);
      return '<article class="copy-card' + selected + keepClass + '">' +
        '<div><strong>' + escapeHtml(record.model || 'unknown') + '</strong></div>' +
        '<div class="meta"><code>' + escapeHtml(record.relativePath) + '</code></div>' +
        '<div class="meta">Seen at ' + escapeHtml(record.occurrenceAt || record.modifiedAt || 'unknown') + '</div>' +
        '<div class="copy-actions">' +
          '<button class="' + (kept ? 'keep' : '') + '" data-toggle-keep-record-id="' + escapeAttr(record.id) + '">' + (kept ? 'Uncheck keep' : 'Keep this copy') + '</button>' +
          (compare && compare.id !== record.id
            ? '<button data-compare-record-id="' + escapeAttr(record.id) + '">Compare</button>'
            : '') +
        '</div>' +
      '</article>';
    }

    function getSuggestedKeepRecord(group) {
      return group.records.find(record => record.id === group.suggestedKeepId) || group.records[0];
    }

    function getKeptIds(group) {
      return keptByGroup.get(group.id) || new Set([group.suggestedKeepId]);
    }

    function getKeptRecords(group) {
      const keptIds = getKeptIds(group);
      return group.records.filter(record => keptIds.has(record.id));
    }

    function isKept(group, recordId) {
      return getKeptIds(group).has(recordId);
    }

    function toggleKeep(group, recordId) {
      const next = new Set(getKeptIds(group));
      if (next.has(recordId)) {
        if (next.size === 1) return;
        next.delete(recordId);
      } else {
        next.add(recordId);
      }
      keptByGroup.set(group.id, next);

      const currentCompare = selectedCompareByGroup.get(group.id);
      if (currentCompare && !group.records.some(record => record.id === currentCompare)) {
        selectedCompareByGroup.delete(group.id);
      }
    }

    function getCompareRecord(group) {
      const selectedId = selectedCompareByGroup.get(group.id);
      const selected = group.records.find(record => record.id === selectedId);
      return selected || group.records.find(record => record.id !== group.suggestedKeepId) || group.records[0] || null;
    }

    function moveGroup(direction) {
      if (!visibleGroups.length) return;
      const index = visibleGroups.findIndex(group => group.id === selectedGroupId);
      const nextIndex = index < 0 ? 0 : Math.max(0, Math.min(visibleGroups.length - 1, index + direction));
      selectedGroupId = visibleGroups[nextIndex].id;
      render();
    }

    function renderPreview(record) {
      const mediaUrl = getMediaUrl(record);
      if (!mediaUrl) return 'No preview available';
      if (record.mediaType === 'video') {
        return '<video src="' + mediaUrl + '" controls preload="metadata"></video>';
      }
      return '<img src="' + mediaUrl + '" alt="">';
    }

    function getMediaUrl(record) {
      if (!record || !record.sourcePath) return '';
      const tokenParam = reviewToken ? '&token=' + encodeURIComponent(reviewToken) : '';
      if (window.location.protocol === 'http:' || window.location.protocol === 'https:') {
        return '/media?path=' + encodeURIComponent(record.sourcePath) + tokenParam;
      }
      return record.sourceFileUri || '';
    }

    function buildDecisionExport() {
      const groups = manifest.groups.map(group => ({
        groupId: group.id,
        keptRecordIds: Array.from(getKeptIds(group)),
      }));

      const decisions = manifest.groups.flatMap(group => {
        const keptIds = getKeptIds(group);
        return group.records.map(record => ({
          id: 'exact_duplicate:' + record.relativePath,
          action: keptIds.has(record.id) ? 'keep' : 'quarantine',
          reviewType: 'exact_duplicate',
          sourceType: 'dataset',
          mediaType: record.mediaType,
          filename: record.filename,
          sourcePath: record.sourcePath,
          relativePath: record.relativePath,
          quarantinePath: record.quarantinePath,
          reasons: group.crossModel ? ['exact_duplicate', 'cross_model_duplicate'] : ['exact_duplicate'],
          quarantineEligible: true,
          contentHash: group.contentHash,
        }));
      });

      return {
        generatedAt: new Date().toISOString(),
        sourceManifestPath: manifest.sourceManifestPath,
        groups,
        decisions,
      };
    }

    async function detectReviewServer() {
      if (window.location.protocol !== 'http:' && window.location.protocol !== 'https:') {
        els.applyDecisions.disabled = true;
        els.statusText.textContent = 'Apply requires the local duplicate review server';
        return;
      }

      try {
        const response = await fetch('/api/status', {
          cache: 'no-store',
          headers: { 'x-slopvault-token': reviewToken },
        });
        if (!response.ok) throw new Error('status unavailable');
        els.statusText.textContent = 'Local duplicate review server connected';
      } catch {
        els.applyDecisions.disabled = true;
        els.statusText.textContent = 'Apply server unavailable';
      }
    }

    async function applyDecisions() {
      const exported = buildDecisionExport();
      const quarantineCount = exported.decisions.filter(item => item.action === 'quarantine').length;
      const keepCount = exported.decisions.filter(item => item.action === 'keep').length;

      if (!exported.decisions.length) {
        els.statusText.textContent = 'No duplicate decisions to apply';
        return;
      }

      if (!confirm(
        'Apply duplicate decisions now?\\n\\nKeep: ' + keepCount + '\\nQuarantine: ' + quarantineCount
      )) return;

      els.applyDecisions.disabled = true;
      els.statusText.textContent = 'Applying duplicate decisions... keep this tab open';

      try {
        const response = await fetch('/api/apply', {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-slopvault-token': reviewToken,
          },
          body: JSON.stringify(exported),
        });
        const payload = await response.json();
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || 'Apply failed');
        }
        els.statusText.textContent = 'Duplicate decisions applied. Refresh the page for updated groups.';
      } catch (err) {
        els.statusText.textContent = err.message;
      } finally {
        els.applyDecisions.disabled = false;
      }
    }

    function formatBytes(value) {
      if (!Number.isFinite(value) || value <= 0) return '0 B';
      const units = ['B', 'KB', 'MB', 'GB'];
      let size = value;
      let unitIndex = 0;
      while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex += 1;
      }
      return size.toFixed(size >= 100 || unitIndex === 0 ? 0 : 1) + ' ' + units[unitIndex];
    }

    function escapeHtml(value) {
      return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function escapeAttr(value) {
      return escapeHtml(value);
    }
  </script>
</body>
</html>`
}

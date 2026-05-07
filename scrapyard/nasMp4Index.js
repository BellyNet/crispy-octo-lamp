const fs = require('fs')
const path = require('path')
const os = require('os')

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

function getDefaultDatasetRoot() {
  return path.join(
    process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
    '.slopvault',
    'dataset'
  )
}

function getNasMp4IndexPath(datasetRoot = getDefaultDatasetRoot()) {
  return path.join(path.resolve(datasetRoot), 'nas-mp4-index.v1.json')
}

let cache = {
  datasetRoot: null,
  indexPath: null,
  values: new Set(),
}

function loadNasMp4Index(datasetRoot = getDefaultDatasetRoot(), options = {}) {
  const resolvedRoot = path.resolve(datasetRoot)
  const indexPath = getNasMp4IndexPath(resolvedRoot)
  if (
    !options.forceReload &&
    cache.datasetRoot === resolvedRoot &&
    cache.indexPath === indexPath
  ) {
    return cache.values
  }

  let values = new Set()
  if (fs.existsSync(indexPath)) {
    try {
      const raw = fs.readFileSync(indexPath, 'utf8').replace(/^\uFEFF/, '')
      const parsed = raw.trim() ? JSON.parse(raw) : {}
      const entries = Array.isArray(parsed?.entries) ? parsed.entries : []
      values = new Set(entries.map((entry) => normalizePath(entry)).filter(Boolean))
    } catch (err) {
      console.warn(`Failed to load NAS MP4 index: ${err.message}`)
    }
  }

  cache = {
    datasetRoot: resolvedRoot,
    indexPath,
    values,
  }
  return cache.values
}

function saveNasMp4Index(entries, datasetRoot = getDefaultDatasetRoot()) {
  const resolvedRoot = path.resolve(datasetRoot)
  const indexPath = getNasMp4IndexPath(resolvedRoot)
  const values = entries instanceof Set ? entries : new Set(entries || [])
  const payload = {
    version: 1,
    updatedAt: new Date().toISOString(),
    entryCount: values.size,
    entries: [...values].map((entry) => normalizePath(entry)).filter(Boolean).sort((a, b) =>
      a.localeCompare(b)
    ),
  }

  fs.mkdirSync(path.dirname(indexPath), { recursive: true })
  fs.writeFileSync(indexPath, JSON.stringify(payload))
  cache = {
    datasetRoot: resolvedRoot,
    indexPath,
    values: new Set(payload.entries),
  }
  return indexPath
}

function hasNasMp4RelativePath(relativePath, datasetRoot = getDefaultDatasetRoot()) {
  return loadNasMp4Index(datasetRoot).has(normalizePath(relativePath))
}

function setNasMp4Entries(entries, datasetRoot = getDefaultDatasetRoot()) {
  return saveNasMp4Index(new Set((entries || []).map((entry) => normalizePath(entry))), datasetRoot)
}

function mergeNasMp4Entries(entries, datasetRoot = getDefaultDatasetRoot()) {
  const values = new Set(loadNasMp4Index(datasetRoot))
  for (const entry of entries || []) {
    const normalized = normalizePath(entry)
    if (normalized) values.add(normalized)
  }
  return saveNasMp4Index(values, datasetRoot)
}

function removeNasMp4Entries(entries, datasetRoot = getDefaultDatasetRoot()) {
  const values = new Set(loadNasMp4Index(datasetRoot))
  for (const entry of entries || []) {
    values.delete(normalizePath(entry))
  }
  return saveNasMp4Index(values, datasetRoot)
}

function collectMp4RelativePaths(rootPath, datasetRoot = getDefaultDatasetRoot()) {
  const resolvedRoot = path.resolve(rootPath)
  const resolvedDatasetRoot = path.resolve(datasetRoot)
  const results = []
  walkForMp4(resolvedRoot, resolvedDatasetRoot, results)
  return results
}

function syncNasMp4IndexToMirror(mirrorRoot, datasetRoot = getDefaultDatasetRoot()) {
  const resolvedMirrorRoot = path.resolve(String(mirrorRoot || ''))
  const indexPath = getNasMp4IndexPath(datasetRoot)
  const destinationPath = path.join(resolvedMirrorRoot, path.basename(indexPath))
  fs.mkdirSync(path.dirname(destinationPath), { recursive: true })
  fs.copyFileSync(indexPath, destinationPath)
  return destinationPath
}

function walkForMp4(currentPath, datasetRoot, results) {
  if (!fs.existsSync(currentPath)) return

  const entries = fs.readdirSync(currentPath, { withFileTypes: true })
  for (const entry of entries) {
    const absolutePath = path.join(currentPath, entry.name)
    if (entry.isDirectory()) {
      walkForMp4(absolutePath, datasetRoot, results)
      continue
    }

    if (!entry.isFile() || !entry.name.toLowerCase().endsWith('.mp4')) continue
    results.push(normalizePath(path.relative(datasetRoot, absolutePath)))
  }
}

module.exports = {
  normalizePath,
  getDefaultDatasetRoot,
  getNasMp4IndexPath,
  loadNasMp4Index,
  saveNasMp4Index,
  hasNasMp4RelativePath,
  setNasMp4Entries,
  mergeNasMp4Entries,
  removeNasMp4Entries,
  collectMp4RelativePaths,
  syncNasMp4IndexToMirror,
}

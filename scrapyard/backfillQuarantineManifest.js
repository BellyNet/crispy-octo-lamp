const fs = require('fs')
const path = require('path')
const os = require('os')
const crypto = require('crypto')
const {
  loadBitwiseHashCache,
  getBitwiseHashRecord,
} = require('./bitwiseHasher')
const {
  loadVisualHashCache,
  getVisualHashFromBuffer,
  getVisualHashRecord,
} = require('./visualHasher')

const slopvaultRoot = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  '.slopvault'
)
const datasetRoot = path.join(slopvaultRoot, 'dataset')
const quarantineRoot = path.join(slopvaultRoot, 'quarantine')
const manifestPath = path.join(quarantineRoot, 'quarantine-manifest.json')
const auditLogPath = path.join(__dirname, '..', 'audit', 'logs', 'audit-slopvault-latest.json')
const mediaExts = new Set([
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
  console.error(`Fatal quarantine manifest backfill error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  loadBitwiseHashCache()
  loadVisualHashCache()

  const manifest = loadManifest()
  const auditFindings = loadLatestAuditFindings()
  const lookupByQuarantinePath = new Map(
    auditFindings
      .filter((finding) => finding?.quarantinePath)
      .map((finding) => [finding.quarantinePath, finding])
  )

  const files = [
    ...collectFiles(path.join(quarantineRoot, 'dataset')),
    ...collectFiles(path.join(quarantineRoot, 'incomplete')),
  ]

  let created = 0
  let updated = 0

  for (const filePath of files) {
    const ext = path.extname(filePath).toLowerCase()
    if (!mediaExts.has(ext)) continue

    const isDataset = filePath.startsWith(path.join(quarantineRoot, 'dataset'))
    const relativePath = normalizePath(
      path.relative(
        isDataset ? path.join(quarantineRoot, 'dataset') : path.join(quarantineRoot, 'incomplete'),
        filePath
      )
    )
    const sourceType = isDataset ? 'dataset' : 'incomplete'
    const stat = fs.statSync(filePath)
    const auditFinding = lookupByQuarantinePath.get(filePath)
    const contentHash = await hashFile(filePath)
    const hashLinkage = await buildHashLinkage(filePath, sourceType, relativePath, contentHash)
    const entry = {
      id: `${sourceType}:${relativePath}`,
      sourceType,
      mediaType: getMediaType(ext),
      model: relativePath.split('/')[0] || null,
      relativePath,
      sourcePathAtAudit:
        sourceType === 'dataset'
          ? path.join(datasetRoot, relativePath.replace(/\//g, path.sep))
          : path.join(__dirname, '..', 'incomplete', relativePath.replace(/\//g, path.sep)),
      quarantinePath: filePath,
      reasons:
        Array.isArray(auditFinding?.reasons) && auditFinding.reasons.length
          ? auditFinding.reasons
          : ['backfilled_existing_quarantine'],
      sizeBytes: stat.size,
      modifiedAt: stat.mtime.toISOString(),
      contentHash: {
        algorithm: 'md5',
        value: contentHash,
      },
      hashLinkage,
      audit: {
        runId: auditFinding?.runId || 'backfill-quarantine-manifest',
        mode: auditFinding ? 'apply' : 'backfill',
        movedAt: auditFinding?.finishedAt || new Date().toISOString(),
        decisionBacked: false,
      },
      state: buildCurrentState(sourceType, relativePath, filePath),
    }

    const index = manifest.items.findIndex(
      (item) => item.id === entry.id || item.quarantinePath === entry.quarantinePath
    )

    if (index >= 0) {
      manifest.items[index] = {
        ...manifest.items[index],
        ...entry,
      }
      updated += 1
    } else {
      manifest.items.push(entry)
      created += 1
    }
  }

  manifest.updatedAt = new Date().toISOString()
  fs.mkdirSync(path.dirname(manifestPath), { recursive: true })
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2))

  console.log(`Manifest: ${manifestPath}`)
  console.log(`Scanned quarantine files: ${files.length}`)
  console.log(`Created entries: ${created}`)
  console.log(`Updated entries: ${updated}`)
  console.log(`Total manifest items: ${manifest.items.length}`)
}

function loadManifest() {
  if (!fs.existsSync(manifestPath)) {
    return { version: 1, updatedAt: null, items: [] }
  }

  const raw = fs.readFileSync(manifestPath, 'utf8').trim()
  const parsed = raw ? JSON.parse(raw) : {}
  return {
    version: parsed?.version || 1,
    updatedAt: parsed?.updatedAt || null,
    items: Array.isArray(parsed?.items) ? parsed.items : [],
  }
}

function loadLatestAuditFindings() {
  if (!fs.existsSync(auditLogPath)) return []
  try {
    const parsed = JSON.parse(fs.readFileSync(auditLogPath, 'utf8'))
    return Array.isArray(parsed?.findings)
      ? parsed.findings.map((finding) => ({
          ...finding,
          runId: parsed?.runId || null,
          finishedAt: parsed?.finishedAt || null,
        }))
      : []
  } catch (err) {
    return []
  }
}

function collectFiles(root) {
  if (!fs.existsSync(root)) return []
  const files = []
  const stack = [root]

  while (stack.length) {
    const current = stack.pop()
    const entries = fs.readdirSync(current, { withFileTypes: true })
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name)
      if (entry.isDirectory()) stack.push(fullPath)
      else if (entry.isFile()) files.push(fullPath)
    }
  }

  return files.sort((a, b) => a.localeCompare(b))
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

async function hashFile(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('md5')
    const stream = fs.createReadStream(filePath)
    stream.on('error', reject)
    stream.on('data', (chunk) => hash.update(chunk))
    stream.on('end', () => resolve(hash.digest('hex')))
  })
}

function getRecordRefs(record) {
  return Array.isArray(record?.refs)
    ? record.refs.map((ref) => normalizePath(ref)).filter(Boolean)
    : []
}

function summarizeRecordRefs(refs) {
  const normalizedRefs = [...new Set(refs.map((ref) => normalizePath(ref)).filter(Boolean))]
  let activeCount = 0
  let quarantineCount = 0
  let missingCount = 0

  for (const ref of normalizedRefs) {
    const activePath = path.join(datasetRoot, ref.replace(/\//g, path.sep))
    const quarantinePath = path.join(quarantineRoot, 'dataset', ref.replace(/\//g, path.sep))
    if (fs.existsSync(activePath)) activeCount += 1
    else if (fs.existsSync(quarantinePath)) quarantineCount += 1
    else missingCount += 1
  }

  return {
    refCount: normalizedRefs.length,
    activeCount,
    quarantineCount,
    missingCount,
    refs: normalizedRefs,
  }
}

async function buildHashLinkage(filePath, sourceType, relativePath, contentHash) {
  const linkage = { bitwise: null, visual: null }
  const bitwiseRecord = getBitwiseHashRecord(contentHash)
  linkage.bitwise = {
    hash: contentHash,
    ...(bitwiseRecord
      ? summarizeRecordRefs(getRecordRefs(bitwiseRecord))
      : { refCount: 0, activeCount: 0, quarantineCount: 0, missingCount: 0, refs: [] }),
  }

  const ext = path.extname(filePath).toLowerCase()
  if (sourceType === 'dataset' && ['.jpg', '.jpeg', '.png', '.webp', '.gif'].includes(ext)) {
    try {
      const buffer = fs.readFileSync(filePath)
      const visualHash = await getVisualHashFromBuffer(buffer)
      if (visualHash) {
        const visualRecord = getVisualHashRecord(visualHash)
        linkage.visual = {
          hash: visualHash,
          ...(visualRecord
            ? summarizeRecordRefs(getRecordRefs(visualRecord))
            : { refCount: 0, activeCount: 0, quarantineCount: 0, missingCount: 0, refs: [] }),
        }
      }
    } catch (err) {
      linkage.visual = {
        hash: null,
        error: err.message,
        refCount: 0,
        activeCount: 0,
        quarantineCount: 0,
        missingCount: 0,
        refs: [],
      }
    }
  }

  return linkage
}

function buildCurrentState(sourceType, relativePath, quarantinePath) {
  const activeDatasetPath =
    sourceType === 'dataset'
      ? path.join(datasetRoot, relativePath.replace(/\//g, path.sep))
      : null
  const activeDatasetExists = activeDatasetPath
    ? fs.existsSync(activeDatasetPath)
    : false
  const quarantineExists = fs.existsSync(quarantinePath)

  let repairState = 'quarantined'
  if (activeDatasetExists && !quarantineExists) repairState = 'repaired'
  else if (activeDatasetExists && quarantineExists) {
    repairState = 'replacement_present_pending_review'
  } else if (!activeDatasetExists && !quarantineExists) {
    repairState = 'missing_both'
  }

  return {
    activeDatasetExists,
    activeDatasetPath,
    quarantineExists,
    repairState,
  }
}

function getMediaType(ext) {
  if (['.mp4', '.webm', '.m4v', '.mov'].includes(ext)) return 'video'
  if (ext === '.gif') return 'gif'
  return 'image'
}

const fs = require('fs')
const path = require('path')
const os = require('os')
const crypto = require('crypto')
const { execFile } = require('child_process')
const minimist = require('minimist')
const sharp = require('sharp')
const {
  loadBitwiseHashCache,
  getBitwiseHashRecord,
} = require('../scrapyard/bitwiseHasher')
const {
  loadVisualHashCache,
  getVisualHashFromBuffer,
  getVisualHashRecord,
} = require('../scrapyard/visualHasher')

const argv = minimist(process.argv.slice(2), {
  alias: {
    d: 'dry-run',
    h: 'help',
  },
  boolean: [
    'dry-run',
    'apply',
    'help',
    'include-incomplete',
    'hash-findings',
    'archive',
  ],
  default: {
    'dry-run': false,
    apply: false,
    'include-incomplete': true,
    'min-video-bytes': 64 * 1024,
    'min-image-bytes': 8 * 1024,
    'min-gif-bytes': 8 * 1024,
    'tail-seconds': 5,
    'tail-frames': 5,
    'hash-findings': false,
    archive: false,
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
const incompleteRoot = path.resolve(
  String(argv['incomplete-root'] || path.join(rootDir, 'incomplete'))
)
const quarantineBase = path.resolve(
  String(argv['quarantine-root'] || path.join(slopvaultRoot, 'quarantine'))
)
const quarantineManifestPath = path.join(
  quarantineBase,
  'quarantine-manifest.json'
)
const decisionsPath = argv.decisions
  ? path.resolve(String(argv.decisions))
  : null
const logDir = path.resolve(
  String(argv['log-dir'] || path.join(__dirname, 'logs'))
)

const minVideoBytes = normalizePositiveInteger(
  argv['min-video-bytes'],
  64 * 1024
)
const minImageBytes = normalizePositiveInteger(
  argv['min-image-bytes'],
  8 * 1024
)
const minGifBytes = normalizePositiveInteger(argv['min-gif-bytes'], 8 * 1024)
const tailSeconds = normalizePositiveInteger(argv['tail-seconds'], 5)
const tailFrames = normalizePositiveInteger(argv['tail-frames'], 5)
const includeIncomplete = Boolean(argv['include-incomplete'])
const hashFindings = Boolean(argv['hash-findings'])
const archiveLogs = Boolean(argv.archive)
const dryRun = argv.apply ? false : true
const decisions = decisionsPath ? loadDecisions(decisionsPath) : null
const startedAt = new Date()
const runStamp = formatTimestamp(startedAt)
const logBase = `audit-slopvault-${runStamp}`

const videoExtensions = new Set(['.mp4', '.webm', '.m4v', '.mov'])
const imageExtensions = new Set(['.jpg', '.jpeg', '.png', '.webp'])
const gifExtensions = new Set(['.gif'])
const allExtensions = new Set([
  ...videoExtensions,
  ...imageExtensions,
  ...gifExtensions,
])
const placeholderBasenames = new Set([
  'ajax_loader',
  'ajax-loader',
  'loader',
  'spinner',
  'loading',
])

const summary = {
  startedAt: startedAt.toISOString(),
  mode: dryRun ? 'dry-run' : 'apply',
  slopvaultRoot,
  datasetRoot,
  incompleteRoot,
  quarantineBase,
  scannedFiles: 0,
  flaggedFiles: 0,
  movedFiles: 0,
  quarantineEligibleFiles: 0,
  manifestEntriesTracked: 0,
  rootsScanned: [],
  flaggedByReason: {},
  flaggedByType: {},
  errors: [],
  quarantineManifestPath,
}
let quarantineManifest = null

main().catch((err) => {
  console.error(`Fatal audit error: ${err.message}`)
  process.exitCode = 1
})

async function main() {
  ensureDir(logDir)
  ensureDir(quarantineBase)
  loadBitwiseHashCache()
  loadVisualHashCache()
  quarantineManifest = loadQuarantineManifest()

  console.log(
    `Starting Slopvault media audit in ${dryRun ? 'dry-run' : 'apply'} mode`
  )
  console.log(`Slopvault root: ${slopvaultRoot}`)
  console.log(`Dataset root: ${datasetRoot}`)
  console.log(`Incomplete root: ${incompleteRoot}`)
  console.log(`Quarantine base: ${quarantineBase}`)
  if (decisionsPath) console.log(`Review decisions: ${decisionsPath}`)
  console.log(
    `Minimum sizes: video=${minVideoBytes} image=${minImageBytes} gif=${minGifBytes}`
  )
  console.log(
    `Video tail decode check: last ${tailSeconds}s / ${tailFrames} frames`
  )

  const auditTargets = []

  if (fs.existsSync(datasetRoot)) {
    auditTargets.push({
      label: 'dataset',
      root: datasetRoot,
      quarantineRoot: path.join(quarantineBase, 'dataset'),
    })
  } else {
    summary.errors.push(`Dataset root not found: ${datasetRoot}`)
  }

  if (includeIncomplete) {
    if (fs.existsSync(incompleteRoot)) {
      auditTargets.push({
        label: 'incomplete',
        root: incompleteRoot,
        quarantineRoot: path.join(quarantineBase, 'incomplete'),
      })
    } else {
      summary.errors.push(`Incomplete root not found: ${incompleteRoot}`)
    }
  }

  if (!auditTargets.length) {
    console.log('No audit roots found. Nothing to do.')
    await writeLogs([])
    return
  }

  const findings = []

  for (const target of auditTargets) {
    summary.rootsScanned.push(target.root)
    const files = collectMediaFiles(target.root)

    console.log(`Scanning ${files.length} media files under ${target.label}`)

    for (const filePath of files) {
      summary.scannedFiles += 1
      const finding = await inspectMediaFile(filePath, target)
      if (!finding) continue

      findings.push(finding)
      summary.flaggedFiles += 1
      summary.flaggedByType[finding.mediaType] =
        (summary.flaggedByType[finding.mediaType] || 0) + 1

      for (const reason of finding.reasons) {
        summary.flaggedByReason[reason] =
          (summary.flaggedByReason[reason] || 0) + 1
      }

      const shouldMove = !dryRun && shouldQuarantineFinding(finding)

      if (finding.quarantineEligible) {
        summary.quarantineEligibleFiles += 1
      }

      console.log(
        `[FLAG] ${finding.relativePath} :: ${finding.reasons.join(', ')}${
          finding.quarantineEligible ? ' :: quarantine' : ' :: report-only'
        }`
      )

      if (hashFindings || shouldMove) {
        await attachContentHash(finding)
        await attachHashLinkage(finding)
      }

      if (shouldMove) {
        await quarantineFinding(finding)
        summary.movedFiles += 1
      }
    }
  }

  for (const finding of collectDecisionBackedFindings()) {
    findings.push(finding)
    summary.flaggedFiles += 1
    summary.flaggedByType[finding.mediaType] =
      (summary.flaggedByType[finding.mediaType] || 0) + 1

    for (const reason of finding.reasons) {
      summary.flaggedByReason[reason] =
        (summary.flaggedByReason[reason] || 0) + 1
    }

    const shouldMove = !dryRun && shouldQuarantineFinding(finding)

    if (finding.quarantineEligible) {
      summary.quarantineEligibleFiles += 1
    }

    console.log(
      `[FLAG] ${finding.relativePath} :: ${finding.reasons.join(', ')}${
        finding.quarantineEligible ? ' :: quarantine' : ' :: report-only'
      }`
    )

    if (hashFindings || shouldMove) {
      if (!finding.contentHash?.value) {
        await attachContentHash(finding)
      }
      await attachHashLinkage(finding)
    }

    if (shouldMove) {
      await quarantineFinding(finding)
      summary.movedFiles += 1
    }
  }

  await writeLogs(findings)

  console.log('')
  console.log(`Scanned: ${summary.scannedFiles}`)
  console.log(`Flagged: ${summary.flaggedFiles}`)
  console.log(`Quarantine eligible: ${summary.quarantineEligibleFiles}`)
  console.log(`Moved: ${summary.movedFiles}`)
  console.log(`Quarantine manifest: ${quarantineManifestPath}`)
  console.log(`Log file: ${path.join(logDir, 'audit-slopvault-latest.json')}`)
}

function printHelp() {
  console.log(`Usage: node audit/audit-slopvault.js [options]

Options:
  -d, --dry-run                 Dry-run mode. Accepted for convenience.
  --apply                       Move only quarantine-eligible files.
  --slopvault-root <path>       Override Slopvault root.
  --dataset-root <path>         Override dataset root.
  --incomplete-root <path>      Override incomplete root.
  --quarantine-root <path>      Override quarantine base.
  --decisions <path>            Only apply dashboard-approved decisions.
  --log-dir <path>              Override audit log directory.
  --min-video-bytes <n>         Report videos smaller than this size.
  --min-image-bytes <n>         Report images smaller than this size.
  --min-gif-bytes <n>           Report GIFs smaller than this size.
  --tail-seconds <n>            Seek this far from end for video decode check.
  --tail-frames <n>             Decode this many tail frames for video check.
  --hash-findings               Hash every flagged finding during this run.
  --archive                     Also write timestamped audit log copies.
  --include-incomplete          Include repo incomplete files. Default: true.
  -h, --help                    Show help.

Notes:
  The script defaults to dry-run unless --apply is provided.
  Apply mode quarantines high-confidence problem files by default.
  With --decisions, apply mode quarantines dashboard-approved files.
  Quarantine paths mirror the Slopvault dataset/incomplete layout.
`)
}

function normalizePositiveInteger(value, fallback) {
  const normalized = parseInt(value, 10)
  return Number.isFinite(normalized) && normalized > 0 ? normalized : fallback
}

function formatTimestamp(date) {
  return date.toISOString().replace(/[:.]/g, '-')
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function loadQuarantineManifest() {
  if (!fs.existsSync(quarantineManifestPath)) {
    return {
      version: 1,
      updatedAt: null,
      items: [],
    }
  }

  try {
    const raw = fs.readFileSync(quarantineManifestPath, 'utf-8').replace(/^\uFEFF/, '')
    const parsed = raw.trim() ? JSON.parse(raw) : {}
    return {
      version: parsed?.version || 1,
      updatedAt: parsed?.updatedAt || null,
      items: Array.isArray(parsed?.items) ? parsed.items : [],
    }
  } catch (err) {
    summary.errors.push(
      `Could not load quarantine manifest ${quarantineManifestPath}: ${err.message}`
    )
    return {
      version: 1,
      updatedAt: null,
      items: [],
    }
  }
}

function saveQuarantineManifest() {
  if (!quarantineManifest) return

  refreshQuarantineManifestState()
  quarantineManifest.updatedAt = new Date().toISOString()
  summary.manifestEntriesTracked = quarantineManifest.items.length

  fs.writeFileSync(
    quarantineManifestPath,
    JSON.stringify(quarantineManifest, null, 2)
  )
}

function refreshQuarantineManifestState() {
  if (!quarantineManifest?.items) return

  for (const item of quarantineManifest.items) {
    item.state = buildCurrentManifestState(item)
  }
}

function buildCurrentManifestState(item) {
  const activeDatasetPath =
    item.sourceType === 'dataset' && item.relativePath
      ? path.join(datasetRoot, item.relativePath)
      : null
  const quarantineExists = item.quarantinePath
    ? fs.existsSync(item.quarantinePath)
    : false
  const activeDatasetExists = activeDatasetPath
    ? fs.existsSync(activeDatasetPath)
    : false

  let repairState = 'quarantined'
  if (activeDatasetExists && !quarantineExists) {
    repairState = 'repaired'
  } else if (activeDatasetExists && quarantineExists) {
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

function upsertQuarantineManifestEntry(finding) {
  if (!quarantineManifest) return

  const nextEntry = {
    id: finding.id,
    sourceType: finding.sourceType,
    mediaType: finding.mediaType,
    model: getFindingModelName(finding),
    relativePath: normalizePath(finding.relativePath),
    sourcePathAtAudit: finding.sourcePath,
    quarantinePath: finding.quarantinePath,
    reasons: [...finding.reasons],
    sizeBytes: finding.sizeBytes,
    modifiedAt: finding.modifiedAt,
    contentHash: finding.contentHash || null,
    hashLinkage: finding.hashLinkage || null,
    audit: {
      runId: logBase,
      mode: summary.mode,
      movedAt: new Date().toISOString(),
      decisionBacked: Boolean(decisions),
    },
    state: buildCurrentManifestState({
      sourceType: finding.sourceType,
      relativePath: normalizePath(finding.relativePath),
      quarantinePath: finding.quarantinePath,
    }),
  }

  const existingIndex = quarantineManifest.items.findIndex(
    (item) =>
      item.id === nextEntry.id ||
      (item.quarantinePath && item.quarantinePath === nextEntry.quarantinePath)
  )

  if (existingIndex >= 0) {
    quarantineManifest.items[existingIndex] = {
      ...quarantineManifest.items[existingIndex],
      ...nextEntry,
    }
  } else {
    quarantineManifest.items.push(nextEntry)
  }
}

function getFindingModelName(finding) {
  const normalizedRelativePath = normalizePath(finding.relativePath)
  const parts = normalizedRelativePath.split('/').filter(Boolean)
  if (!parts.length) return null
  return parts[0]
}

function getHashRecordRefs(record) {
  return Array.isArray(record?.refs)
    ? record.refs
        .map((ref) =>
          typeof ref === 'string' ? normalizePath(ref) : normalizePath(ref?.relativePath)
        )
        .filter(Boolean)
    : []
}

function summarizeRecordRefs(refs) {
  const normalizedRefs = [...new Set(refs.map((ref) => normalizePath(ref)).filter(Boolean))]
  let activeCount = 0
  let quarantineCount = 0
  let missingCount = 0

  for (const ref of normalizedRefs) {
    const activePath = path.join(datasetRoot, ref.replace(/\//g, path.sep))
    const quarantinePath = path.join(
      quarantineBase,
      'dataset',
      ref.replace(/\//g, path.sep)
    )

    if (fs.existsSync(activePath)) {
      activeCount += 1
    } else if (fs.existsSync(quarantinePath)) {
      quarantineCount += 1
    } else {
      missingCount += 1
    }
  }

  return {
    refCount: normalizedRefs.length,
    activeCount,
    quarantineCount,
    missingCount,
    refs: normalizedRefs,
  }
}

async function attachHashLinkage(finding) {
  const linkage = {
    bitwise: null,
    visual: null,
  }

  if (finding.contentHash?.value) {
    const bitwiseRecord = getBitwiseHashRecord(finding.contentHash.value)
    if (bitwiseRecord) {
      linkage.bitwise = {
        hash: finding.contentHash.value,
        ...summarizeRecordRefs(getHashRecordRefs(bitwiseRecord)),
      }
    } else {
      linkage.bitwise = {
        hash: finding.contentHash.value,
        refCount: 0,
        activeCount: 0,
        quarantineCount: 0,
        missingCount: 0,
        refs: [],
      }
    }
  }

  if (finding.sourceType === 'dataset' && ['image', 'gif'].includes(finding.mediaType)) {
    try {
      const buffer = fs.readFileSync(finding.sourcePath)
      const visualHash = await getVisualHashFromBuffer(buffer)
      if (visualHash) {
        const visualRecord = getVisualHashRecord(visualHash)
        linkage.visual = {
          hash: visualHash,
          ...(visualRecord
            ? summarizeRecordRefs(getHashRecordRefs(visualRecord))
            : {
                refCount: 0,
                activeCount: 0,
                quarantineCount: 0,
                missingCount: 0,
                refs: [],
              }),
        }
      }
    } catch (err) {
      summary.errors.push(
        `Failed to build visual hash linkage for ${finding.sourcePath}: ${err.message}`
      )
    }
  }

  finding.hashLinkage = linkage
}

function loadDecisions(filePath) {
  try {
    const raw = fs.readFileSync(filePath, 'utf-8').replace(/^\uFEFF/, '')
    const parsed = JSON.parse(raw)
    const items = Array.isArray(parsed?.decisions) ? parsed.decisions : []
    const byId = new Map()

    for (const item of items) {
      if (!item?.id || !item?.action) continue
      byId.set(item.id, item.action)
    }

    return { byId, items }
  } catch (err) {
    throw new Error(`Could not load decisions file ${filePath}: ${err.message}`)
  }
}

function shouldQuarantineFinding(finding) {
  if (!decisions) return finding.quarantineEligible
  return decisions.byId.get(finding.id) === 'quarantine'
}

function collectDecisionBackedFindings() {
  if (!decisions?.items?.length) return []

  return decisions.items
    .filter((item) => item?.action === 'quarantine')
    .filter((item) => item?.reviewType === 'exact_duplicate')
    .map((item) => {
      const sourcePath = path.resolve(String(item.sourcePath || ''))
      const quarantinePath = path.resolve(String(item.quarantinePath || ''))
      const relativePath = normalizePath(item.relativePath || '')

      if (!sourcePath || !quarantinePath || !relativePath) return null
      if (!fs.existsSync(sourcePath)) return null
      if (sourcePath.toLowerCase() === quarantinePath.toLowerCase()) return null

      const stat = fs.statSync(sourcePath)

      return {
        id: String(item.id),
        sourcePath,
        sourceType: String(item.sourceType || 'dataset'),
        mediaType: String(item.mediaType || inferMediaTypeFromFilePath(sourcePath)),
        relativePath,
        quarantinePath,
        sizeBytes: stat.size,
        modifiedAt: stat.mtime.toISOString(),
        reasons:
          Array.isArray(item.reasons) && item.reasons.length
            ? item.reasons
            : ['exact_duplicate'],
        quarantineEligible: true,
        contentHash: item.contentHash || null,
      }
    })
    .filter(Boolean)
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
      summary.errors.push(`Failed to read directory ${current}: ${err.message}`)
      continue
    }

    for (const entry of entries) {
      const fullPath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        stack.push(fullPath)
        continue
      }

      if (!entry.isFile()) continue

      const ext = path.extname(entry.name).toLowerCase()
      if (allExtensions.has(ext)) {
        files.push(fullPath)
      }
    }
  }

  return files.sort((a, b) => a.localeCompare(b))
}

async function inspectMediaFile(filePath, target) {
  const ext = path.extname(filePath).toLowerCase()

  if (videoExtensions.has(ext)) {
    return inspectVideoFile(filePath, target)
  }

  if (gifExtensions.has(ext)) {
    return inspectImageLikeFile(filePath, target, 'gif')
  }

  if (imageExtensions.has(ext)) {
    return inspectImageLikeFile(filePath, target, 'image')
  }

  return null
}

async function inspectVideoFile(filePath, target) {
  const relativePath = path.relative(target.root, filePath)
  const stat = fs.statSync(filePath)
  const reasons = []
  const probe = await probeVideo(filePath)
  const tailDecode =
    !probe.error && probe.hasVideoStream
      ? await probeVideoTailDecode(filePath)
      : { error: null }
  const basename = getNormalizedBasename(filePath)

  if (placeholderBasenames.has(basename)) {
    reasons.push('known_placeholder_name')
  }

  if (stat.size < minVideoBytes) {
    reasons.push(`small_video_size<${minVideoBytes}`)
  }

  if (probe.error) {
    reasons.push('ffprobe_error')
  }

  if (!probe.hasVideoStream) {
    reasons.push('missing_video_stream')
  }

  if (
    !probe.error &&
    probe.hasVideoStream &&
    (!Number.isFinite(probe.durationSeconds) || probe.durationSeconds <= 0)
  ) {
    reasons.push('invalid_duration_metadata')
  }

  if (tailDecode.error) {
    reasons.push('tail_decode_error')
  }

  if (!reasons.length) return null

  const quarantineEligible =
    reasons.includes('known_placeholder_name') ||
    reasons.includes('ffprobe_error') ||
    reasons.includes('missing_video_stream') ||
    reasons.includes('tail_decode_error')

  return buildFinding({
    filePath,
    target,
    mediaType: 'video',
    stat,
    reasons,
    quarantineEligible,
    extra: {
      durationSeconds: Number.isFinite(probe.durationSeconds)
        ? probe.durationSeconds
        : null,
      hasVideoStream: probe.hasVideoStream,
      ffprobeError: probe.error || null,
      tailDecodeError: tailDecode.error || null,
    },
  })
}

async function inspectImageLikeFile(filePath, target, mediaType) {
  const relativePath = path.relative(target.root, filePath)
  const stat = fs.statSync(filePath)
  const reasons = []
  const basename = getNormalizedBasename(filePath)
  const metadata = await probeImage(filePath)
  const minBytes = mediaType === 'gif' ? minGifBytes : minImageBytes

  if (placeholderBasenames.has(basename)) {
    reasons.push('known_placeholder_name')
  }

  if (stat.size < minBytes) {
    reasons.push(`small_${mediaType}_size<${minBytes}`)
  }

  if (metadata.error) {
    reasons.push('image_parse_error')
  } else {
    if (!metadata.width || !metadata.height) {
      reasons.push('missing_dimensions')
    }

    if (
      metadata.width &&
      metadata.height &&
      metadata.width <= 32 &&
      metadata.height <= 32
    ) {
      reasons.push('tiny_dimensions')
    }
  }

  if (!reasons.length) return null

  const quarantineEligible =
    reasons.includes('known_placeholder_name') ||
    reasons.includes('image_parse_error') ||
    reasons.includes('missing_dimensions')

  return buildFinding({
    filePath,
    target,
    mediaType,
    stat,
    reasons,
    quarantineEligible,
    extra: {
      width: metadata.width || null,
      height: metadata.height || null,
      pages: metadata.pages || null,
      imageError: metadata.error || null,
    },
  })
}

function buildFinding({
  filePath,
  target,
  mediaType,
  stat,
  reasons,
  quarantineEligible,
  extra,
}) {
  const relativePath = path.relative(target.root, filePath)

  return {
    id: createFindingId(target.label, relativePath),
    sourcePath: filePath,
    sourceType: target.label,
    mediaType,
    relativePath,
    quarantinePath: path.join(target.quarantineRoot, relativePath),
    sizeBytes: stat.size,
    modifiedAt: stat.mtime.toISOString(),
    reasons,
    quarantineEligible,
    ...extra,
  }
}

async function attachContentHash(finding) {
  try {
    finding.contentHash = {
      algorithm: 'md5',
      value: await hashFile(finding.sourcePath),
    }
  } catch (err) {
    finding.contentHash = {
      algorithm: 'md5',
      value: null,
      error: err.message,
    }
    summary.errors.push(`Failed to hash ${finding.sourcePath}: ${err.message}`)
  }
}

function inferMediaTypeFromFilePath(filePath) {
  const ext = path.extname(filePath).toLowerCase()
  if (videoExtensions.has(ext)) return 'video'
  if (gifExtensions.has(ext)) return 'gif'
  return 'image'
}

function createFindingId(sourceType, relativePath) {
  return `${sourceType}:${normalizePath(relativePath)}`
}

function normalizePath(value) {
  return String(value || '')
    .split(path.sep)
    .join('/')
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

function getNormalizedBasename(filePath) {
  return path.basename(filePath, path.extname(filePath)).toLowerCase()
}

function probeVideo(filePath) {
  return new Promise((resolve) => {
    execFile(
      'ffprobe',
      [
        '-v',
        'error',
        '-print_format',
        'json',
        '-show_format',
        '-show_streams',
        filePath,
      ],
      (err, stdout) => {
        if (err) {
          return resolve({
            durationSeconds: null,
            hasVideoStream: false,
            error: err.message,
          })
        }

        try {
          const parsed = JSON.parse(stdout || '{}')
          const streams = Array.isArray(parsed.streams) ? parsed.streams : []
          const formatDuration = parseFloat(parsed.format?.duration)
          const videoStream = streams.find(
            (stream) => stream.codec_type === 'video'
          )
          const streamDuration = parseFloat(videoStream?.duration)
          const durationSeconds = Number.isFinite(streamDuration)
            ? streamDuration
            : Number.isFinite(formatDuration)
              ? formatDuration
              : null

          resolve({
            durationSeconds,
            hasVideoStream: Boolean(videoStream),
            error: null,
          })
        } catch (parseErr) {
          resolve({
            durationSeconds: null,
            hasVideoStream: false,
            error: `Failed to parse ffprobe output: ${parseErr.message}`,
          })
        }
      }
    )
  })
}

function probeVideoTailDecode(filePath) {
  return new Promise((resolve) => {
    execFile(
      'ffmpeg',
      [
        '-v',
        'error',
        '-xerror',
        '-sseof',
        `-${tailSeconds}`,
        '-i',
        filePath,
        '-map',
        '0:v:0',
        '-frames:v',
        String(tailFrames),
        '-f',
        'null',
        '-',
      ],
      { timeout: 60_000 },
      (err, stdout, stderr) => {
        if (err) {
          return resolve({
            error: (stderr || stdout || err.message || '').trim(),
          })
        }

        resolve({ error: null })
      }
    )
  })
}

async function probeImage(filePath) {
  try {
    const metadata = await sharp(filePath, { animated: true }).metadata()
    return {
      width: metadata.width || null,
      height: metadata.height || null,
      pages: metadata.pages || null,
      error: null,
    }
  } catch (err) {
    return {
      width: null,
      height: null,
      pages: null,
      error: err.message,
    }
  }
}

async function quarantineFinding(finding) {
  ensureDir(path.dirname(finding.quarantinePath))

  try {
    fs.renameSync(finding.sourcePath, finding.quarantinePath)
  } catch (err) {
    if (err.code !== 'EXDEV') {
      throw err
    }

    fs.copyFileSync(finding.sourcePath, finding.quarantinePath)
    fs.unlinkSync(finding.sourcePath)
  }

  upsertQuarantineManifestEntry(finding)
}

async function writeLogs(findings) {
  const summaryPath = path.join(logDir, 'audit-slopvault-latest.json')
  const reportPath = path.join(logDir, 'audit-slopvault-latest.txt')

  saveQuarantineManifest()

  const payload = {
    ...summary,
    runId: logBase,
    finishedAt: new Date().toISOString(),
    findings,
  }

  fs.writeFileSync(summaryPath, JSON.stringify(payload, null, 2))

  const lines = [
    `Audit run: ${logBase}`,
    `Mode: ${summary.mode}`,
    `Scanned files: ${summary.scannedFiles}`,
    `Flagged files: ${summary.flaggedFiles}`,
    `Quarantine eligible: ${summary.quarantineEligibleFiles}`,
    `Moved files: ${summary.movedFiles}`,
    `Tracked quarantine entries: ${summary.manifestEntriesTracked}`,
    `Quarantine manifest: ${quarantineManifestPath}`,
    '',
  ]

  for (const finding of findings) {
    lines.push(
      `${finding.mediaType} | ${finding.relativePath} | ${finding.reasons.join(', ')} | ${finding.quarantineEligible ? 'quarantine' : 'report-only'} | ${finding.sizeBytes} bytes`
    )
  }

  if (summary.errors.length) {
    lines.push('')
    lines.push('Errors:')
    for (const error of summary.errors) {
      lines.push(error)
    }
  }

  fs.writeFileSync(reportPath, `${lines.join('\n')}\n`)

  if (archiveLogs) {
    fs.writeFileSync(
      path.join(logDir, `${logBase}.json`),
      JSON.stringify(payload, null, 2)
    )
    fs.writeFileSync(
      path.join(logDir, `${logBase}.txt`),
      `${lines.join('\n')}\n`
    )
  }
}

const fs = require('fs')
const path = require('path')
const os = require('os')
const { execFile } = require('child_process')
const minimist = require('minimist')
const sharp = require('sharp')

const argv = minimist(process.argv.slice(2), {
  alias: {
    d: 'dry-run',
    h: 'help',
  },
  boolean: ['dry-run', 'apply', 'help', 'include-incomplete'],
  default: {
    'dry-run': false,
    apply: false,
    'include-incomplete': true,
    'min-video-bytes': 64 * 1024,
    'min-image-bytes': 8 * 1024,
    'min-gif-bytes': 8 * 1024,
    'tail-seconds': 5,
    'tail-frames': 5,
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
  String(
    argv['quarantine-root'] || path.join(__dirname, 'quarantine', 'slopvault')
  )
)
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
const dryRun = argv.apply ? false : true
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
  rootsScanned: [],
  flaggedByReason: {},
  flaggedByType: {},
  errors: [],
}

main().catch((err) => {
  console.error(`Fatal audit error: ${err.message}`)
  process.exitCode = 1
})

async function main() {
  ensureDir(logDir)

  console.log(
    `Starting Slopvault media audit in ${dryRun ? 'dry-run' : 'apply'} mode`
  )
  console.log(`Slopvault root: ${slopvaultRoot}`)
  console.log(`Dataset root: ${datasetRoot}`)
  console.log(`Incomplete root: ${incompleteRoot}`)
  console.log(`Quarantine base: ${quarantineBase}`)
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

      if (finding.quarantineEligible) {
        summary.quarantineEligibleFiles += 1
      }

      console.log(
        `[FLAG] ${finding.relativePath} :: ${finding.reasons.join(', ')}${
          finding.quarantineEligible ? ' :: quarantine' : ' :: report-only'
        }`
      )

      if (!dryRun && finding.quarantineEligible) {
        await quarantineFinding(finding)
        summary.movedFiles += 1
      }
    }
  }

  await writeLogs(findings)

  console.log('')
  console.log(`Scanned: ${summary.scannedFiles}`)
  console.log(`Flagged: ${summary.flaggedFiles}`)
  console.log(`Quarantine eligible: ${summary.quarantineEligibleFiles}`)
  console.log(`Moved: ${summary.movedFiles}`)
  console.log(`Log file: ${path.join(logDir, `${logBase}.json`)}`)
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
  --log-dir <path>              Override audit log directory.
  --min-video-bytes <n>         Report videos smaller than this size.
  --min-image-bytes <n>         Report images smaller than this size.
  --min-gif-bytes <n>           Report GIFs smaller than this size.
  --tail-seconds <n>            Seek this far from end for video decode check.
  --tail-frames <n>             Decode this many tail frames for video check.
  --include-incomplete          Include repo incomplete files. Default: true.
  -h, --help                    Show help.

Notes:
  The script defaults to dry-run unless --apply is provided.
  Apply mode only quarantines high-confidence problem files.
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
}

async function writeLogs(findings) {
  const summaryPath = path.join(logDir, `${logBase}.json`)
  const reportPath = path.join(logDir, `${logBase}.txt`)

  fs.writeFileSync(
    summaryPath,
    JSON.stringify(
      {
        ...summary,
        finishedAt: new Date().toISOString(),
        findings,
      },
      null,
      2
    )
  )

  const lines = [
    `Audit run: ${logBase}`,
    `Mode: ${summary.mode}`,
    `Scanned files: ${summary.scannedFiles}`,
    `Flagged files: ${summary.flaggedFiles}`,
    `Quarantine eligible: ${summary.quarantineEligibleFiles}`,
    `Moved files: ${summary.movedFiles}`,
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
}

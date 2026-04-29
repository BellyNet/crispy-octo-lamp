const fs = require('fs')
const path = require('path')
const os = require('os')
const { execFile } = require('child_process')
const minimist = require('minimist')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    i: 'input',
  },
  string: ['input', 'output-root'],
  default: {
    'probe-window': 2,
    'min-trim-gap': 2,
    'max-iterations': 18,
    'duration-tolerance': 0.5,
  },
})

if (argv.help || !argv.input) {
  printHelp()
  process.exit(argv.help ? 0 : 1)
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
const outputRoot = path.resolve(
  String(
    argv['output-root'] || path.join(slopvaultRoot, 'quarantine', 'salvaged')
  )
)
const inputPath = path.resolve(String(argv.input))
const probeWindowSeconds = normalizePositiveNumber(argv['probe-window'], 2)
const minTrimGapSeconds = normalizePositiveNumber(argv['min-trim-gap'], 2)
const maxIterations = Math.max(parseInt(argv['max-iterations'], 10) || 18, 4)
const durationTolerance = normalizePositiveNumber(
  argv['duration-tolerance'],
  0.5
)

main().catch((err) => {
  console.error(`Fatal salvage error: ${err.stack || err.message}`)
  process.exitCode = 1
})

function logStatus(message) {
  process.stderr.write(`${message}\n`)
}

async function main() {
  if (!fs.existsSync(inputPath)) {
    throw new Error(`Input video not found: ${inputPath}`)
  }

  logStatus(`Inspecting ${inputPath}`)
  const probe = await probeVideo(inputPath)
  if (probe.error) {
    throw new Error(`ffprobe failed: ${probe.error}`)
  }
  if (!Number.isFinite(probe.durationSeconds) || probe.durationSeconds <= 0) {
    throw new Error(`Video duration is invalid for ${inputPath}`)
  }
  logStatus(`Probed duration ${probe.durationSeconds.toFixed(3)}s`)

  logStatus('Checking tail decode...')
  const tailReport = await canDecodeUntil(inputPath, probe.durationSeconds)
  if (tailReport.ok) {
    logStatus('Tail already decodes cleanly')
    console.log(
      'Video tail already decodes cleanly. No salvage output created.'
    )
    console.log(JSON.stringify(buildSummary(probe, null, null, true), null, 2))
    return
  }

  logStatus('Searching for last good cut point...')
  const salvageDuration = await findLastGoodEndTime(
    inputPath,
    probe.durationSeconds
  )
  const trimGap = probe.durationSeconds - salvageDuration
  logStatus(
    `Found cut at ${salvageDuration.toFixed(3)}s, trimming ${trimGap.toFixed(3)}s`
  )

  if (trimGap < minTrimGapSeconds) {
    logStatus('Trim gap too small, skipping output')
    console.log(
      `Only ${trimGap.toFixed(
        3
      )}s would be trimmed, which is below --min-trim-gap. Skipping salvage.`
    )
    console.log(
      JSON.stringify(buildSummary(probe, salvageDuration, null, false), null, 2)
    )
    return
  }

  const outputPath = buildOutputPath(inputPath)
  ensureDir(path.dirname(outputPath))

  logStatus(`Writing salvaged output to ${outputPath}`)
  await salvageVideo(inputPath, outputPath, salvageDuration)

  logStatus('Verifying salvaged output...')
  const outputProbe = await probeVideo(outputPath)
  const outputTail = await canDecodeUntil(
    outputPath,
    outputProbe.durationSeconds || salvageDuration
  )

  const summary = buildSummary(
    probe,
    salvageDuration,
    {
      outputPath,
      durationSeconds: outputProbe.durationSeconds || null,
      tailDecodeOk: outputTail.ok,
      ffprobeError: outputProbe.error || null,
      sizeBytes: fs.existsSync(outputPath)
        ? fs.statSync(outputPath).size
        : null,
    },
    false
  )

  const reportPath = `${outputPath}.json`
  fs.writeFileSync(reportPath, JSON.stringify(summary, null, 2))
  logStatus(
    outputTail.ok
      ? 'Salvaged output tail verified cleanly'
      : 'Salvaged output still has tail decode issues'
  )

  console.log(JSON.stringify(summary, null, 2))
}

function printHelp() {
  console.log(`Usage: node audit/salvage-tail-videos.js --input <video> [options]

Options:
  -i, --input <path>           Broken video to salvage.
  --slopvault-root <path>      Override Slopvault root.
  --output-root <path>         Override salvage root.
  --probe-window <seconds>     Probe window size near candidate cut point.
  --min-trim-gap <seconds>     Minimum trimmed tail before output is written.
  --max-iterations <n>         Binary-search iterations.
  --duration-tolerance <sec>   Search stop tolerance.
  -h, --help                   Show help.

Notes:
  Outputs are written under %APPDATA%\\.slopvault\\quarantine\\salvaged by default.
  The original broken file is left untouched.
`)
}

function normalizePositiveNumber(value, fallback) {
  const parsed = Number.parseFloat(value)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

function buildOutputPath(filePath) {
  const normalized = normalizePath(filePath)
  const datasetMarker = '/.slopvault/'
  const markerIndex = normalized.toLowerCase().indexOf(datasetMarker)
  let relative = path.basename(filePath)

  if (markerIndex >= 0) {
    relative = normalized.slice(markerIndex + datasetMarker.length)
  }

  const parsed = path.parse(relative)
  return path.join(outputRoot, parsed.dir, `${parsed.name}.salvaged.mp4`)
}

function buildSummary(inputProbe, salvageDuration, output, alreadyHealthy) {
  return {
    generatedAt: new Date().toISOString(),
    inputPath,
    outputRoot,
    probeWindowSeconds,
    input: {
      durationSeconds: inputProbe.durationSeconds,
      width: inputProbe.width,
      height: inputProbe.height,
      codec: inputProbe.codec,
      streamCount: inputProbe.streamCount,
      sizeBytes: fs.statSync(inputPath).size,
    },
    alreadyHealthy,
    salvageDurationSeconds: Number.isFinite(salvageDuration)
      ? salvageDuration
      : null,
    trimmedSeconds:
      Number.isFinite(salvageDuration) &&
      Number.isFinite(inputProbe.durationSeconds)
        ? Math.max(inputProbe.durationSeconds - salvageDuration, 0)
        : null,
    output,
  }
}

function probeVideo(filePath) {
  return new Promise((resolve) => {
    execFile(
      'ffprobe',
      [
        '-v',
        'error',
        '-show_entries',
        'format=duration:stream=index,codec_type,codec_name,width,height',
        '-of',
        'json',
        filePath,
      ],
      { timeout: 60_000 },
      (err, stdout, stderr) => {
        if (err) {
          return resolve({
            error: (stderr || stdout || err.message || '').trim(),
            durationSeconds: null,
            streamCount: 0,
            width: null,
            height: null,
            codec: null,
          })
        }

        try {
          const parsed = JSON.parse(stdout || '{}')
          const streams = Array.isArray(parsed.streams) ? parsed.streams : []
          const videoStream =
            streams.find((stream) => stream.codec_type === 'video') || null

          resolve({
            error: null,
            durationSeconds: Number.parseFloat(parsed?.format?.duration || ''),
            streamCount: streams.length,
            width: videoStream?.width || null,
            height: videoStream?.height || null,
            codec: videoStream?.codec_name || null,
          })
        } catch (parseErr) {
          resolve({
            error: `Failed to parse ffprobe output: ${parseErr.message}`,
            durationSeconds: null,
            streamCount: 0,
            width: null,
            height: null,
            codec: null,
          })
        }
      }
    )
  })
}

function canDecodeUntil(filePath, endTimeSeconds) {
  const safeEnd = Math.max(Number(endTimeSeconds) || 0, 0)
  const start = Math.max(safeEnd - probeWindowSeconds, 0)
  const window = Math.max(safeEnd - start, 0.25)

  return new Promise((resolve) => {
    execFile(
      'ffmpeg',
      [
        '-v',
        'error',
        '-xerror',
        '-i',
        filePath,
        '-ss',
        start.toFixed(3),
        '-map',
        '0:v:0',
        '-t',
        window.toFixed(3),
        '-f',
        'null',
        '-',
      ],
      { timeout: 60_000 },
      (err, stdout, stderr) => {
        if (err) {
          return resolve({
            ok: false,
            error: (stderr || stdout || err.message || '').trim(),
            start,
            end: safeEnd,
          })
        }

        resolve({
          ok: true,
          error: null,
          start,
          end: safeEnd,
        })
      }
    )
  })
}

async function findLastGoodEndTime(filePath, durationSeconds) {
  let low = 0
  let high = durationSeconds
  let best = 0

  for (let i = 0; i < maxIterations; i++) {
    const mid = (low + high) / 2
    const report = await canDecodeUntil(filePath, mid)

    if (report.ok) {
      best = mid
      low = mid
    } else {
      high = mid
    }

    if (Math.abs(high - low) <= durationTolerance) break
  }

  return Math.max(best - 0.25, 0)
}

function salvageVideo(inputFilePath, outputFilePath, salvageDuration) {
  return new Promise((resolve, reject) => {
    execFile(
      'ffmpeg',
      [
        '-y',
        '-v',
        'error',
        '-i',
        inputFilePath,
        '-t',
        salvageDuration.toFixed(3),
        '-map',
        '0:v:0',
        '-map',
        '0:a?',
        '-c:v',
        'libx264',
        '-preset',
        'veryfast',
        '-crf',
        '18',
        '-c:a',
        'aac',
        '-b:a',
        '160k',
        '-movflags',
        '+faststart',
        outputFilePath,
      ],
      { timeout: 0, maxBuffer: 1024 * 1024 * 8 },
      (err, stdout, stderr) => {
        if (err) {
          return reject(
            new Error((stderr || stdout || err.message || '').trim())
          )
        }
        resolve()
      }
    )
  })
}

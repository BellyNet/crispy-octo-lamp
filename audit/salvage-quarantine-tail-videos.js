const fs = require('fs')
const path = require('path')
const os = require('os')
const { spawn } = require('child_process')
const minimist = require('minimist')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
  },
  string: ['model', 'quarantine-manifest', 'output-dir'],
  default: {
    limit: 0,
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
const quarantineManifestPath = path.resolve(
  String(
    argv['quarantine-manifest'] ||
      path.join(slopvaultRoot, 'quarantine', 'quarantine-manifest.json')
  )
)
const outputDir = path.resolve(
  String(argv['output-dir'] || path.join(__dirname, 'logs'))
)
const limit = Math.max(parseInt(argv.limit, 10) || 0, 0)
const modelFilter = String(argv.model || '')
  .trim()
  .toLowerCase()
const runStamp = new Date().toISOString().replace(/[:.]/g, '-')
const latestJsonPath = path.join(outputDir, 'salvage-tail-videos-latest.json')
const latestTxtPath = path.join(outputDir, 'salvage-tail-videos-latest.txt')

main().catch((err) => {
  console.error(`Fatal salvage batch error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  ensureDir(outputDir)

  const manifest = JSON.parse(fs.readFileSync(quarantineManifestPath, 'utf8'))
  const items = Array.isArray(manifest?.items) ? manifest.items : []
  const targets = items
    .filter((item) => Array.isArray(item.reasons))
    .filter((item) => item.reasons.includes('tail_decode_error'))
    .filter((item) => item?.state?.quarantineExists)
    .filter((item) => item.quarantinePath && fs.existsSync(item.quarantinePath))
    .filter((item) =>
      modelFilter
        ? String(item.model || inferModel(item.relativePath))
            .toLowerCase()
            .includes(modelFilter)
        : true
    )
    .sort((a, b) =>
      String(a.relativePath || '').localeCompare(String(b.relativePath || ''))
    )

  const selectedTargets = limit > 0 ? targets.slice(0, limit) : targets
  const results = []

  console.log(
    `Batch-salvaging ${selectedTargets.length} quarantined tail-decode videos`
  )
  if (modelFilter) console.log(`Model filter: ${modelFilter}`)
  if (limit > 0) console.log(`Limit: ${limit}`)

  for (let index = 0; index < selectedTargets.length; index += 1) {
    const target = selectedTargets[index]
    const label = `[${index + 1}/${selectedTargets.length}] ${target.relativePath}`
    console.log(label)
    const result = await runSalvage(target)
    results.push(result)
    console.log(
      `  -> ${result.status}${
        Number.isFinite(result.trimmedSeconds)
          ? `, trimmed ${result.trimmedSeconds.toFixed(3)}s`
          : ''
      }`
    )
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    slopvaultRoot,
    quarantineManifestPath,
    modelFilter: modelFilter || null,
    limit: limit || null,
    scannedTargets: selectedTargets.length,
    counts: {
      salvaged: results.filter((item) => item.status === 'salvaged').length,
      skippedHealthy: results.filter(
        (item) => item.status === 'already_healthy'
      ).length,
      skippedShortTrim: results.filter(
        (item) => item.status === 'trim_gap_too_small'
      ).length,
      failed: results.filter((item) => item.status === 'failed').length,
    },
    results,
  }

  fs.writeFileSync(latestJsonPath, JSON.stringify(summary, null, 2))
  fs.writeFileSync(latestTxtPath, renderTextSummary(summary))

  const archivedJsonPath = path.join(
    outputDir,
    `salvage-tail-videos-${runStamp}.json`
  )
  const archivedTxtPath = path.join(
    outputDir,
    `salvage-tail-videos-${runStamp}.txt`
  )
  fs.writeFileSync(archivedJsonPath, JSON.stringify(summary, null, 2))
  fs.writeFileSync(archivedTxtPath, renderTextSummary(summary))

  console.log('')
  console.log(`Latest JSON: ${latestJsonPath}`)
  console.log(`Latest TXT: ${latestTxtPath}`)
  console.log(
    `Counts: salvaged=${summary.counts.salvaged}, healthy=${summary.counts.skippedHealthy}, short-trim=${summary.counts.skippedShortTrim}, failed=${summary.counts.failed}`
  )
}

function printHelp() {
  console.log(`Usage: node audit/salvage-quarantine-tail-videos.js [options]

Options:
  --model <name>                Only process one model or matching models.
  --limit <n>                   Only process the first n matching files.
  --slopvault-root <path>       Override Slopvault root.
  --quarantine-manifest <path>  Override quarantine manifest path.
  --output-dir <path>           Override report directory.
  -h, --help                    Show help.
`)
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
}

function inferModel(relativePath) {
  return (
    String(relativePath || '')
      .replace(/\\/g, '/')
      .split('/')[0] || null
  )
}

function runSalvage(target) {
  return new Promise((resolve) => {
    const child = spawn(
      process.execPath,
      [
        path.join(__dirname, 'salvage-tail-videos.js'),
        '--input',
        String(target.quarantinePath),
      ],
      {
        cwd: path.join(__dirname, '..'),
        stdio: ['ignore', 'pipe', 'pipe'],
      }
    )

    let stdout = ''
    let stderr = ''
    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString()
    })
    child.on('error', (err) => {
      resolve({
        model: target.model || inferModel(target.relativePath),
        relativePath: target.relativePath,
        quarantinePath: target.quarantinePath,
        status: 'failed',
        error: err.message,
      })
    })
    child.on('exit', (code) => {
      try {
        const parsed = JSON.parse(stdout || '{}')
        const status = parsed.alreadyHealthy
          ? 'already_healthy'
          : parsed.output
            ? 'salvaged'
            : 'trim_gap_too_small'

        resolve({
          model: target.model || inferModel(target.relativePath),
          relativePath: target.relativePath,
          quarantinePath: target.quarantinePath,
          status,
          inputDurationSeconds: parsed?.input?.durationSeconds ?? null,
          salvageDurationSeconds: parsed?.salvageDurationSeconds ?? null,
          trimmedSeconds: parsed?.trimmedSeconds ?? null,
          outputPath: parsed?.output?.outputPath || null,
          outputDurationSeconds: parsed?.output?.durationSeconds || null,
          outputTailDecodeOk: parsed?.output?.tailDecodeOk || false,
          outputSizeBytes: parsed?.output?.sizeBytes || null,
          stderr: stderr.trim() || null,
          exitCode: code,
        })
      } catch (err) {
        resolve({
          model: target.model || inferModel(target.relativePath),
          relativePath: target.relativePath,
          quarantinePath: target.quarantinePath,
          status: 'failed',
          error: err.message,
          stdout: stdout.trim() || null,
          stderr: stderr.trim() || null,
          exitCode: code,
        })
      }
    })
  })
}

function renderTextSummary(summary) {
  const lines = [
    `Generated: ${summary.generatedAt}`,
    `Manifest: ${summary.quarantineManifestPath}`,
    `Model filter: ${summary.modelFilter || 'all'}`,
    `Limit: ${summary.limit || 'none'}`,
    `Scanned: ${summary.scannedTargets}`,
    `Salvaged: ${summary.counts.salvaged}`,
    `Already healthy: ${summary.counts.skippedHealthy}`,
    `Trim gap too small: ${summary.counts.skippedShortTrim}`,
    `Failed: ${summary.counts.failed}`,
    '',
  ]

  for (const item of summary.results) {
    lines.push(
      [
        item.status.toUpperCase(),
        item.relativePath,
        item.trimmedSeconds != null
          ? `trimmed=${Number(item.trimmedSeconds).toFixed(3)}s`
          : null,
        item.outputPath ? `output=${item.outputPath}` : null,
        item.error ? `error=${item.error}` : null,
      ]
        .filter(Boolean)
        .join(' :: ')
    )
  }

  return lines.join('\n')
}

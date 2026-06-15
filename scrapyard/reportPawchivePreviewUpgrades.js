'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const minimist = require('minimist')

function collectPawchivePreviewUpgrades(options = {}) {
  const datasetDir =
    options.datasetDir ||
    path.join(
      process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
      '.slopvault',
      'dataset'
    )
  const modelFilter = new Set(
    String(options.models || '')
      .split(',')
      .map((value) => value.trim())
      .filter(Boolean)
  )
  const items = []

  if (!fs.existsSync(datasetDir)) return items

  for (const modelName of fs.readdirSync(datasetDir)) {
    if (modelFilter.size > 0 && !modelFilter.has(modelName)) continue
    const sidecarPath = path.join(datasetDir, modelName, '.media-dates.json')
    if (!fs.existsSync(sidecarPath)) continue

    let sidecar
    try {
      sidecar = JSON.parse(fs.readFileSync(sidecarPath, 'utf8'))
    } catch {
      continue
    }

    for (const [relativePath, record] of Object.entries(sidecar)) {
      if (relativePath.startsWith('__')) continue
      const source = record?.source
      if (source?.mediaQuality !== 'pawchive_preview') continue

      items.push({
        model: modelName,
        relativePath: `${modelName}/${relativePath}`.replace(/\\/g, '/'),
        status: source.fullResolutionStatus || 'pending',
        needsFullResolution: source.needsFullResolution === true,
        mediaPageUrl: source.mediaPageUrl || null,
        previewUrl: source.mediaUrl || null,
        fullResolutionUrl: source.fullResolutionUrl || null,
        resolvedPath: source.fullResolutionResolvedPath || null,
      })
    }
  }

  return items.sort(
    (left, right) =>
      left.model.localeCompare(right.model) ||
      left.relativePath.localeCompare(right.relativePath)
  )
}

function main() {
  const argv = minimist(process.argv.slice(2))
  const datasetDir = argv['dataset-dir']
    ? path.resolve(String(argv['dataset-dir']))
    : undefined
  const outputPath = path.resolve(
    String(
      argv.output ||
        path.join(
          __dirname,
          '..',
          'tmp',
          'pawchive-preview-upgrades-latest.json'
        )
    )
  )
  const items = collectPawchivePreviewUpgrades({
    datasetDir,
    models: argv.models || argv.model,
  })
  const pending = items.filter((item) => item.needsFullResolution)
  const resolved = items.filter((item) => !item.needsFullResolution)
  const report = {
    generatedAt: new Date().toISOString(),
    summary: {
      total: items.length,
      pending: pending.length,
      resolved: resolved.length,
    },
    items,
  }

  fs.mkdirSync(path.dirname(outputPath), { recursive: true })
  fs.writeFileSync(outputPath, JSON.stringify(report, null, 2) + '\n')
  console.log(
    `Pawchive preview upgrades: ${pending.length} pending, ${resolved.length} resolved`
  )
  console.log(`Report: ${outputPath}`)
}

if (require.main === module) main()

module.exports = {
  collectPawchivePreviewUpgrades,
}

const fs = require('fs')
const path = require('path')
const os = require('os')
const minimist = require('minimist')
const {
  loadBitwiseHashCache,
  getBitwiseHashRecord,
} = require('./bitwiseHasher')

const argv = minimist(process.argv.slice(2), {
  alias: {
    h: 'help',
    m: 'model',
  },
  boolean: ['help'],
})

if (argv.help) {
  printHelp()
  process.exit(0)
}

const slopvaultRoot = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  '.slopvault'
)
const datasetRoot = path.join(slopvaultRoot, 'dataset')
const quarantineRoot = path.join(slopvaultRoot, 'quarantine')
const manifestPath = path.join(quarantineRoot, 'quarantine-manifest.json')
const targetModel = argv.model ? String(argv.model) : null

main().catch((err) => {
  console.error(
    `Fatal quarantine validation error: ${err.stack || err.message}`
  )
  process.exitCode = 1
})

function printHelp() {
  console.log(`Usage: node scrapyard/validateQuarantineState.js [options]

Options:
  --model <name>   Limit report to one model.
  -h, --help       Show help.
`)
}

async function main() {
  if (!fs.existsSync(manifestPath)) {
    throw new Error(`Quarantine manifest not found: ${manifestPath}`)
  }

  loadBitwiseHashCache()

  const parsed = JSON.parse(fs.readFileSync(manifestPath, 'utf8'))
  const items = Array.isArray(parsed?.items) ? parsed.items : []
  const filtered = targetModel
    ? items.filter((item) => item?.model === targetModel)
    : items

  const summary = {
    total: filtered.length,
    quarantined: 0,
    repaired: 0,
    replacementPresentPendingReview: 0,
    missingBoth: 0,
    unknown: 0,
    staleBitwiseRefs: 0,
  }

  for (const item of filtered) {
    const state = getCurrentState(item)
    if (state.repairState === 'quarantined') summary.quarantined += 1
    else if (state.repairState === 'repaired') summary.repaired += 1
    else if (state.repairState === 'replacement_present_pending_review') {
      summary.replacementPresentPendingReview += 1
    } else if (state.repairState === 'missing_both') summary.missingBoth += 1
    else summary.unknown += 1

    if (item?.contentHash?.value) {
      const record = getBitwiseHashRecord(item.contentHash.value)
      const refs = Array.isArray(record?.refs) ? record.refs : []
      const hasActiveRef = refs.some((ref) =>
        fs.existsSync(
          path.join(datasetRoot, String(ref).replace(/\//g, path.sep))
        )
      )

      if (!hasActiveRef && state.repairState === 'repaired') {
        summary.staleBitwiseRefs += 1
      }
    }
  }

  console.log(`Manifest: ${manifestPath}`)
  if (targetModel) console.log(`Model: ${targetModel}`)
  console.log(`Total tracked entries: ${summary.total}`)
  console.log(`Still quarantined: ${summary.quarantined}`)
  console.log(`Repaired: ${summary.repaired}`)
  console.log(
    `Replacement present, quarantine still exists: ${summary.replacementPresentPendingReview}`
  )
  console.log(`Missing both dataset and quarantine: ${summary.missingBoth}`)
  console.log(`Stale repaired bitwise refs: ${summary.staleBitwiseRefs}`)
}

function getCurrentState(item) {
  const activeDatasetPath =
    item?.sourceType === 'dataset' && item?.relativePath
      ? path.join(
          datasetRoot,
          String(item.relativePath).replace(/\//g, path.sep)
        )
      : null
  const activeDatasetExists = activeDatasetPath
    ? fs.existsSync(activeDatasetPath)
    : false
  const quarantineExists = item?.quarantinePath
    ? fs.existsSync(item.quarantinePath)
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
    quarantineExists,
    repairState,
  }
}

const fs = require('fs')
const path = require('path')
const minimist = require('minimist')

const {
  getDefaultDatasetRoot,
  getNasMp4IndexPath,
  setNasMp4Entries,
  mergeNasMp4Entries,
  collectMp4RelativePaths,
} = require('./nasMp4Index')

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

const datasetRoot = path.resolve(
  String(argv['dataset-root'] || getDefaultDatasetRoot())
)
const nasRoot = argv['nas-root'] ? path.resolve(String(argv['nas-root'])) : null
const mergeLocalRoot = argv['merge-local-root']
  ? path.resolve(String(argv['merge-local-root']))
  : null

main()

function main() {
  if (!nasRoot && !mergeLocalRoot) {
    console.error('Specify either --nas-root or --merge-local-root.')
    process.exit(1)
  }

  let mode = ''
  let entries = []
  if (nasRoot) {
    mode = 'rebuild_from_nas'
    entries = collectMp4RelativePaths(nasRoot, nasRoot)
    setNasMp4Entries(entries, datasetRoot)
  } else {
    mode = 'merge_local_root'
    entries = collectMp4RelativePaths(mergeLocalRoot, datasetRoot)
    mergeNasMp4Entries(entries, datasetRoot)
  }

  const indexPath = getNasMp4IndexPath(datasetRoot)
  const stat = fs.statSync(indexPath)
  console.log(`Mode: ${mode}`)
  console.log(`Dataset root: ${datasetRoot}`)
  if (nasRoot) console.log(`NAS root: ${nasRoot}`)
  if (mergeLocalRoot) console.log(`Merged local root: ${mergeLocalRoot}`)
  console.log(`Video entries processed: ${entries.length}`)
  console.log(`Index path: ${indexPath}`)
  console.log(`Index size bytes: ${stat.size}`)
}

function printHelp() {
  console.log(`Usage: node scrapyard/rebuildNasMp4Index.js [options]

Options:
  --dataset-root <path>     Override local dataset root.
  --nas-root <path>         Rebuild the full index by scanning this NAS dataset root.
  --merge-local-root <path> Merge local video paths into the existing NAS index.
  -h, --help                Show help.

Notes:
  The NAS media index lives at the dataset root as nas-mp4-index.v1.json
  for compatibility and covers mp4/m4v/mov/webm paths.
`)
}

const path = require('path')
const { exec, execFile } = require('child_process')
const minimist = require('minimist')
const {
  collectMp4RelativePaths,
  mergeNasMp4Entries,
  syncNasMp4IndexToMirror,
} = require('./nasMp4Index')

require('dotenv').config({ path: path.join(__dirname, '..', '.env') })

const argv = minimist(process.argv.slice(2), {
  string: ['model', 'cleanup-mp4', 'cleanup-gif-mp4'],
  boolean: ['push', 'pull', 'help'],
})

function getOption(name) {
  if (argv[name] !== undefined) return argv[name]
  return process.env[`npm_config_${String(name).replace(/-/g, '_')}`]
}

function isTruthy(value) {
  return (
    value === true ||
    value === 'true' ||
    value === '1' ||
    value === 1 ||
    value === 'yes'
  )
}

function printHelp() {
  console.log(`Usage:
  npm run scrape -- sync --push
  npm run scrape -- sync --pull
  npm run scrape -- sync --model <name>

Options:
  --cleanup-mp4=true       Remove local MP4s that are verified on NAS after push.
  --cleanup-gif-mp4=true   Remove GIF-derived local MP4s after push.
`)
}

if (argv.help) {
  printHelp()
  process.exit(0)
}

const baseLocal = process.env.LOCAL_DATASET_DIR
  ? path.resolve(process.env.LOCAL_DATASET_DIR)
  : path.join(process.env.APPDATA, '.slopvault', 'dataset')
const baseNAS = process.env.NAS_DATASET_DIR
  ? path.resolve(process.env.NAS_DATASET_DIR)
  : 'Z:\\dataset'

const isPush = isTruthy(getOption('push'))
const isPull = isTruthy(getOption('pull'))
const modelName = getOption('model')
const cleanupGifDerivedMp4s =
  isTruthy(getOption('cleanup-gif-mp4')) ||
  isTruthy(process.env.npm_config_cleanup_gif_mp4)
const cleanupMirroredMp4s =
  isTruthy(getOption('cleanup-mp4')) ||
  isTruthy(process.env.npm_config_cleanup_mp4)

let cmd = ''

if (isPush) {
  cmd = `robocopy "${baseLocal}" "${baseNAS}" /E /XC /XN /XO`
} else if (isPull) {
  cmd = `robocopy "${baseNAS}" "${baseLocal}" /MIR`
} else if (modelName) {
  cmd = `robocopy "${baseLocal}\\${modelName}" "${baseNAS}\\${modelName}" /E /XC /XN /XO`
} else {
  console.error(
    'Missing flag.\nUsage:\n  npm run scrape -- sync --push\n  npm run scrape -- sync --pull\n  npm run scrape -- sync --model=<name>\nOptional:\n  npm run scrape -- sync --push --cleanup-mp4=true\n  npm run scrape -- sync --push --cleanup-gif-mp4=true'
  )
  process.exit(1)
}

console.log(`Running: ${cmd}`)
exec(`powershell -Command "${cmd}"`, (err, stdout, stderr) => {
  console.log(stdout)
  if (err && err.code > 3) {
    console.error('Sync failed:', stderr || err.message)
    process.exit(1)
  }

  const mergeRoot = modelName ? path.join(baseLocal, modelName) : baseLocal
  mergeNasMp4Entries(collectMp4RelativePaths(mergeRoot, baseLocal), baseLocal)
  syncNasMp4IndexToMirror(baseNAS, baseLocal)

  if (!isPush || (!cleanupGifDerivedMp4s && !cleanupMirroredMp4s)) {
    console.log('Sync complete!')
    return
  }

  const cleanupScript = cleanupMirroredMp4s
    ? 'removeMirroredMp4s.js'
    : 'removeGifDerivedMp4s.js'
  const cleanupLabel = cleanupMirroredMp4s
    ? 'verified mirrored MP4 cleanup'
    : 'verified GIF-derived MP4 cleanup'
  const cleanupArgs = [
    path.join(__dirname, cleanupScript),
    '--apply',
    '--dataset-root',
    baseLocal,
    '--mirror-root',
    baseNAS,
  ]

  console.log(`Running ${cleanupLabel} after sync...`)
  execFile(
    process.execPath,
    cleanupArgs,
    (cleanupErr, cleanupStdout, cleanupStderr) => {
      if (cleanupStdout) {
        console.log(cleanupStdout)
      }
      if (cleanupErr) {
        console.error(
          'Post-sync cleanup failed:',
          cleanupStderr || cleanupErr.message
        )
        process.exit(1)
      }

      if (cleanupStderr) {
        console.error(cleanupStderr)
      }
      console.log('Sync complete!')
    }
  )
})

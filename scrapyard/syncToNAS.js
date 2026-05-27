const path = require('path')
const { exec } = require('child_process')
const fs = require('fs')

require('dotenv').config({ path: path.join(__dirname, '..', '.env') }) // ← LOAD .env

const model = process.argv[2]
const allowMirrorDelete =
  process.argv.includes('--mirror-delete') ||
  process.argv.includes('--allow-delete')
if (!model) {
  console.error('❌ You must specify a model name: node syncToNas.js <model>')
  process.exit(1)
}

const localBase = process.env.LOCAL_DATASET_DIR
const nasBase = process.env.NAS_DATASET_DIR

if (!localBase || !nasBase) {
  console.error('❌ LOCAL_DATASET_DIR or NAS_DATASET_DIR is missing from .env')
  process.exit(1)
}

const localPath = path.join(localBase, model)
const nasPath = path.join(nasBase, model)

if (!fs.existsSync(localPath)) {
  console.error(`❌ Local model folder not found: ${localPath}`)
  process.exit(1)
}

console.log(`📤 Syncing ${model} from local → NAS...`)
try {
  const robocopyMode = allowMirrorDelete ? '/MIR' : '/E /XC /XN /XO'
  const robocopyCmd = `robocopy "${localPath}" "${nasPath}" ${robocopyMode} /NFL /NDL /NJH /NJS /NP /R:2 /W:5`

  if (allowMirrorDelete) {
    console.warn(
      'Mirror-delete mode enabled. NAS files missing locally can be deleted.'
    )
  }

  exec(robocopyCmd, (err, stdout, stderr) => {
    const exitCode = err?.code ?? 0
    if (exitCode > 3) {
      console.error(`❌ Sync failed:`, stderr || stdout)
    } else {
      console.log(`✅ Sync completed with exit code ${exitCode}`)
    }
  })
} catch (err) {
  console.error('❌ Sync failed:', err.message)
}

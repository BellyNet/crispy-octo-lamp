const fs = require('fs')
const path = require('path')
const { execSync } = require('child_process')
require('dotenv').config({ path: path.join(__dirname, '..', '.env') })

const vaultDrive = 'Z:'
const vaultPath = path.join(vaultDrive, 'dataset')
const fallbackPath = path.join(
  process.env.APPDATA || path.join(process.env.HOME, 'AppData', 'Roaming'),
  '.slopvault_pending',
  'dataset'
)

const user = process.env.VAULT69_USER
const pass = process.env.VAULT69_PASS
const uncPath = process.env.VAULT69_PATH

const localDataset = path.join(
  process.env.APPDATA || path.join(process.env.HOME, 'AppData', 'Roaming'),
  '.slopvault',
  'dataset'
)

function isZMappedToCorrectShare() {
  try {
    const result = execSync('net use').toString().toLowerCase()
    const normalized = uncPath.toLowerCase().replace(/\\/g, '/')
    return result.includes('z:') && result.includes(normalized)
  } catch {
    return false
  }
}

function mountVault69() {
  try {
    execSync(
      `net use ${vaultDrive} ${uncPath} /user:${user} ${pass} /persistent:yes`,
      {
        stdio: 'ignore',
      }
    )
    return true
  } catch {
    return false
  }
}

function syncToNAS() {
  let destination = vaultPath

  if (!isZMappedToCorrectShare()) {
    console.warn(`⚠️ Z: not mapped to ${uncPath}. Attempting mount...`)
    if (!mountVault69()) {
      console.warn(`❌ Mount failed. Using fallback path: ${fallbackPath}`)
      destination = fallbackPath
    }
  }

  if (!fs.existsSync(destination)) {
    fs.mkdirSync(destination, { recursive: true })
    console.log(`📁 Created destination: ${destination}`)
  }

  const cmd = `robocopy "${localDataset}" "${destination}" /MIR /NFL /NDL /NJH /NJS /NP /R:2 /W:5`

  console.log(`📦 Syncing slopvault dataset → ${destination}`)
  try {
    execSync(cmd, { stdio: 'inherit' })
    console.log(`✅ Sync complete.`)
  } catch (err) {
    console.error(`❌ Sync failed:`, err.message)
  }
}

module.exports = { syncToNAS }

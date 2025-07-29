const fs = require('fs')
const path = require('path')

const datasetDir = path.join(
  process.env.APPDATA || path.join(process.env.HOME, 'AppData', 'Roaming'),
  '.slopvault',
  'dataset'
)

const filesToClear = ['visualHashes.json', 'bitwiseHashes.json']

// ✅ Clear hash cache files
for (const file of filesToClear) {
  const fullPath = path.join(datasetDir, file)
  if (fs.existsSync(fullPath)) {
    fs.writeFileSync(fullPath, '[]', 'utf-8')
    console.log(`✅ Cleared ${file}`)
  } else {
    console.log(`ℹ️ No ${file} found.`)
  }
}

// ✅ Clear incomplete tasks (supports subfolders)
const cleanupDirs = [
  path.join(__dirname, '..', 'milkmaid', 'incomplete'),
  path.join(__dirname, '..', 'hoghaul', 'incomplete'),
]

for (const dir of cleanupDirs) {
  if (fs.existsSync(dir)) {
    fs.readdirSync(dir).forEach((item) => {
      const fullPath = path.join(dir, item)
      const stat = fs.lstatSync(fullPath)

      if (stat.isFile()) {
        fs.unlinkSync(fullPath)
        console.log(`🗑️ Deleted file: ${fullPath}`)
      } else if (stat.isDirectory()) {
        fs.rmSync(fullPath, { recursive: true, force: true })
        console.log(`🗑️ Deleted folder: ${fullPath}`)
      }
    })
  }
}

console.log(
  '🎉 All dupe caches and incomplete tasks wiped. Next scrape = fresh start!'
)

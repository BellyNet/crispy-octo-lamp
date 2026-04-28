const fs = require('fs')
const path = require('path')
const os = require('os')
const readline = require('readline')

const slopvaultRoot = path.join(
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
  '.slopvault'
)
const datasetRoot = path.join(slopvaultRoot, 'dataset')
const bitwiseV2Path = path.join(datasetRoot, 'bitwiseHashes.v2.json')
const visualV2Path = path.join(datasetRoot, 'visualHashes.v2.json')

main().catch((err) => {
  console.error(`Fatal remap error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  const sourceModel = await askModelName('Source model to fix', 'unknown_cow')
  const targetModel = await askModelName('Correct target model')

  if (!sourceModel || !targetModel) {
    throw new Error('Both source and target models are required.')
  }

  if (sourceModel === targetModel) {
    throw new Error('Source and target models cannot be the same.')
  }

  const sourceRoot = path.join(datasetRoot, sourceModel)
  const targetRoot = path.join(datasetRoot, targetModel)

  if (!fs.existsSync(sourceRoot)) {
    throw new Error(`Source model folder not found: ${sourceRoot}`)
  }

  const files = collectFiles(sourceRoot)
  const mediaFiles = files.filter((filePath) => isMediaFile(filePath))
  const logFiles = files.filter((filePath) => !isMediaFile(filePath))

  if (!mediaFiles.length) {
    throw new Error(`No media files found under ${sourceRoot}`)
  }

  console.log(`\nSource: ${sourceModel}`)
  console.log(`Target: ${targetModel}`)
  console.log(`Media files to move: ${mediaFiles.length}`)
  console.log(`Other files to move: ${logFiles.length}`)

  const confirm = await askYesNo(
    'Proceed with remap, hash ref rewrite, and source cleanup?',
    true
  )
  if (!confirm) {
    console.log('Cancelled.')
    return
  }

  fs.mkdirSync(targetRoot, { recursive: true })

  const movedPaths = []
  for (const filePath of files) {
    const relativePath = path.relative(sourceRoot, filePath)
    const destinationPath = path.join(targetRoot, relativePath)
    fs.mkdirSync(path.dirname(destinationPath), { recursive: true })

    if (fs.existsSync(destinationPath)) {
      throw new Error(`Refusing to overwrite existing file: ${destinationPath}`)
    }

    fs.renameSync(filePath, destinationPath)
    movedPaths.push({
      from: normalizePath(path.relative(datasetRoot, filePath)),
      to: normalizePath(path.relative(datasetRoot, destinationPath)),
    })
  }

  cleanupEmptyParentDirs(sourceRoot, datasetRoot)

  const bitwiseStats = rewriteHashRefs(bitwiseV2Path, movedPaths)
  const visualStats = rewriteHashRefs(visualV2Path, movedPaths)

  console.log(
    `\nMoved ${movedPaths.length} files from ${sourceModel} to ${targetModel}.`
  )
  console.log(
    `Bitwise hash refs updated: ${bitwiseStats.updatedRefs} across ${bitwiseStats.updatedEntries} entries.`
  )
  console.log(
    `Visual hash refs updated: ${visualStats.updatedRefs} across ${visualStats.updatedEntries} entries.`
  )

  const sourceStillExists = fs.existsSync(sourceRoot)
  console.log(
    sourceStillExists
      ? `Source folder still exists: ${sourceRoot}`
      : `Source folder removed: ${sourceRoot}`
  )
  console.log(`Target folder ready: ${targetRoot}`)
  console.log(
    '\nNext step: rerun Milkmaid and confirm the correct model when prompted.'
  )
}

function normalizePath(value) {
  return String(value || '').replace(/\\/g, '/')
}

function isMediaFile(filePath) {
  const ext = path.extname(filePath).toLowerCase()
  return [
    '.jpg',
    '.jpeg',
    '.png',
    '.webp',
    '.gif',
    '.mp4',
    '.webm',
    '.m4v',
    '.mov',
  ].includes(ext)
}

function collectFiles(root) {
  const files = []
  const stack = [root]

  while (stack.length) {
    const current = stack.pop()
    const entries = fs.readdirSync(current, { withFileTypes: true })

    for (const entry of entries) {
      const fullPath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        stack.push(fullPath)
      } else if (entry.isFile()) {
        files.push(fullPath)
      }
    }
  }

  return files.sort((a, b) => a.localeCompare(b))
}

function rewriteHashRefs(storePath, movedPaths) {
  if (!fs.existsSync(storePath)) {
    return { updatedRefs: 0, updatedEntries: 0 }
  }

  const raw = fs.readFileSync(storePath, 'utf8').trim()
  const parsed = raw ? JSON.parse(raw) : { entries: [] }
  const entries = Array.isArray(parsed?.entries) ? parsed.entries : []
  const refMap = new Map(movedPaths.map((item) => [item.from, item.to]))

  let updatedRefs = 0
  let updatedEntries = 0

  for (const entry of entries) {
    if (!Array.isArray(entry?.refs)) continue

    let entryTouched = false
    entry.refs = entry.refs.map((ref) => {
      const normalizedRef = normalizePath(ref)
      const nextRef = refMap.get(normalizedRef)
      if (nextRef && nextRef !== normalizedRef) {
        updatedRefs += 1
        entryTouched = true
        return nextRef
      }
      return normalizedRef
    })

    entry.refs = [...new Set(entry.refs)].sort((a, b) => a.localeCompare(b))

    if (entryTouched) {
      updatedEntries += 1
    }
  }

  fs.writeFileSync(storePath, JSON.stringify(parsed))
  return { updatedRefs, updatedEntries }
}

function cleanupEmptyParentDirs(startPath, stopPath) {
  let current = path.resolve(startPath)
  const resolvedStop = path.resolve(stopPath)

  while (
    current &&
    current.startsWith(resolvedStop) &&
    current !== resolvedStop
  ) {
    if (!fs.existsSync(current)) {
      current = path.dirname(current)
      continue
    }

    if (fs.readdirSync(current).length > 0) break
    fs.rmdirSync(current)
    current = path.dirname(current)
  }
}

function askQuestion(prompt) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    })

    rl.question(`${prompt}: `, (answer) => {
      rl.close()
      resolve(answer.trim())
    })
  })
}

async function askModelName(prompt, defaultValue = '') {
  const label = defaultValue ? `${prompt} [${defaultValue}]` : prompt
  const answer = await askQuestion(label)
  return sanitizeModelName(answer || defaultValue)
}

async function askYesNo(prompt, defaultValue = true) {
  const suffix = defaultValue ? '[Y/n]' : '[y/N]'
  const answer = (await askQuestion(`${prompt} ${suffix}`)).toLowerCase()

  if (!answer) return defaultValue
  if (['y', 'yes'].includes(answer)) return true
  if (['n', 'no'].includes(answer)) return false
  return defaultValue
}

function sanitizeModelName(value) {
  return String(value || '')
    .replace(/[^a-z0-9_\-]/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase()
}

const fs = require('fs')
const path = require('path')
const { spawnSync } = require('child_process')
require('dotenv').config({ path: path.join(__dirname, '..', '.env'), quiet: true })

const rootDir = path.join(__dirname, '..')
const MODEL_ALIASES_FILENAME = 'model_aliases.json'
const NAS_MODEL_ALIASES_FILENAME = 'model-aliases.json'
const DEFAULT_NAS_DATASET_DIR = 'Z:\\dataset'
const PRETTIER_PARSERS = new Map([
  ['.cjs', 'babel'],
  ['.css', 'css'],
  ['.html', 'html'],
  ['.js', 'babel'],
  ['.json', 'json'],
  ['.jsonc', 'json'],
  ['.md', 'markdown'],
  ['.mjs', 'babel'],
  ['.ts', 'typescript'],
  ['.yaml', 'yaml'],
  ['.yml', 'yaml'],
])

function shouldFormat(filePath, formatWithPrettier) {
  if (!formatWithPrettier) return false
  return PRETTIER_PARSERS.has(path.extname(filePath).toLowerCase())
}

function formatRepoFile(filePath) {
  const resolvedPath = path.resolve(filePath)
  const relativePath = path.relative(rootDir, resolvedPath)

  if (relativePath.startsWith('..') || path.isAbsolute(relativePath)) {
    return
  }

  const parser = PRETTIER_PARSERS.get(path.extname(resolvedPath).toLowerCase())
  const prettierScript = `
const fs = require('fs')
const prettier = require('prettier')

;(async () => {
  const filePath = ${JSON.stringify(relativePath)}
  const parser = ${JSON.stringify(parser || null)}
  const source = fs.readFileSync(filePath, 'utf8')
  const config = (await prettier.resolveConfig(filePath)) || {}
  const formatted = await prettier.format(source, {
    ...config,
    filepath: filePath,
    ...(parser ? { parser } : {}),
  })

  if (formatted !== source) {
    fs.writeFileSync(filePath, formatted, 'utf8')
  }
})().catch((err) => {
  console.error(err.stack || err.message)
  process.exit(1)
})
`.trim()

  const result = spawnSync(
    process.execPath,
    ['-e', prettierScript],
    {
      cwd: rootDir,
      encoding: 'utf8',
    }
  )

  if (result.status !== 0) {
    const stderr = (result.stderr || '').trim()
    const stdout = (result.stdout || '').trim()
    throw new Error(
      `Prettier failed for ${resolvedPath}: ${stderr || stdout || 'unknown error'}`
    )
  }
}

function isRootModelAliasesFile(filePath) {
  const resolvedPath = path.resolve(filePath)
  return (
    path.basename(resolvedPath).toLowerCase() === MODEL_ALIASES_FILENAME &&
    path.dirname(resolvedPath) === rootDir
  )
}

function getNasModelAliasesPath() {
  const nasDatasetRoot = path.resolve(
    String(process.env.NAS_DATASET_DIR || DEFAULT_NAS_DATASET_DIR)
  )
  return path.join(path.dirname(nasDatasetRoot), NAS_MODEL_ALIASES_FILENAME)
}

function syncModelAliasesToNas(filePath) {
  if (!isRootModelAliasesFile(filePath)) return

  const resolvedPath = path.resolve(filePath)
  const nasModelAliasesPath = getNasModelAliasesPath()
  fs.mkdirSync(path.dirname(nasModelAliasesPath), { recursive: true })
  fs.copyFileSync(resolvedPath, nasModelAliasesPath)
}

function writeRepoFileSync(filePath, contents, options = {}) {
  const resolvedPath = path.resolve(filePath)
  const encoding = options.encoding || 'utf8'
  const formatWithPrettier = options.formatWithPrettier !== false

  fs.writeFileSync(resolvedPath, contents, encoding)

  if (shouldFormat(resolvedPath, formatWithPrettier)) {
    formatRepoFile(resolvedPath)
  }

  syncModelAliasesToNas(resolvedPath)
}

function writeRepoJsonFileSync(filePath, value, options = {}) {
  const spacing = options.spacing ?? 2
  const appendNewline = options.appendNewline !== false
  const payload =
    JSON.stringify(value, null, spacing) + (appendNewline ? '\n' : '')

  writeRepoFileSync(filePath, payload, options)
}

module.exports = {
  formatRepoFile,
  writeRepoFileSync,
  writeRepoJsonFileSync,
}

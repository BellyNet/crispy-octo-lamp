'use strict'

const fs = require('fs')
const path = require('path')

const repoRoot = path.join(__dirname, '..')
const registryPath = path.join(repoRoot, 'model_aliases.json')
const permanentSkipPath = path.join(
  __dirname,
  'source-backfill-permanent-skips.json'
)
const csvPath = path.join(repoRoot, 'source-backfill-manual-review.csv')
const markdownPath = path.join(repoRoot, 'source-backfill-manual-review.md')

const platforms = [
  { key: 'coomer', label: 'Coomer' },
  { key: 'kemono', label: 'Pawchive' },
  { key: 'reddit', label: 'Reddit' },
  { key: 'stufferdb', label: 'StufferDB' },
]

function sanitize(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
}

function readJson(filePath, fallback) {
  if (!fs.existsSync(filePath)) return fallback
  return JSON.parse(fs.readFileSync(filePath, 'utf8'))
}

function hasSource(entry, platform) {
  const sources = entry?.sources?.[platform]
  return Array.isArray(sources) && sources.length > 0
}

function isPermanentlySkipped(skipState, model, platform) {
  return Boolean(skipState?.skips?.[sanitize(model)]?.[platform])
}

function getSearchUrl(platform, model) {
  const query = encodeURIComponent(model)
  if (platform === 'coomer') {
    return `https://coomerfans.com/?q=${query}`
  }
  if (platform === 'kemono') {
    return `https://pawchive.st/artists?q=${query}`
  }
  if (platform === 'reddit') {
    return `https://www.reddit.com/search/?q=${query}&type=users`
  }
  return `https://www.google.com/search?q=${encodeURIComponent(
    `site:stufferdb.com/index?/category ${model}`
  )}`
}

function csvCell(value) {
  const text = String(value ?? '')
  return `"${text.replace(/"/g, '""')}"`
}

function markdownText(value) {
  return String(value ?? '')
    .replace(/\|/g, '\\|')
    .replace(/\r?\n/g, ' ')
}

function buildRows(registry, skipState) {
  return Object.entries(registry)
    .map(([model, entry]) => {
      const missing = platforms.filter(
        ({ key }) =>
          !hasSource(entry, key) && !isPermanentlySkipped(skipState, model, key)
      )
      if (missing.length === 0) return null
      return {
        model,
        aliases: Array.isArray(entry?.aliases) ? entry.aliases : [],
        missing,
      }
    })
    .filter(Boolean)
    .sort((left, right) => left.model.localeCompare(right.model))
}

function writeCsv(rows) {
  const headers = [
    'model',
    'aliases',
    'missing_sources',
    'coomer_search',
    'coomer_match',
    'pawchive_search',
    'pawchive_match',
    'reddit_search',
    'reddit_match',
    'stufferdb_search',
    'stufferdb_match',
    'notes',
  ]
  const lines = [headers.map(csvCell).join(',')]

  for (const row of rows) {
    const missingKeys = new Set(row.missing.map(({ key }) => key))
    const values = [
      row.model,
      row.aliases.join('; '),
      row.missing.map(({ label }) => label).join('; '),
      missingKeys.has('coomer') ? getSearchUrl('coomer', row.model) : '',
      '',
      missingKeys.has('kemono') ? getSearchUrl('kemono', row.model) : '',
      '',
      missingKeys.has('reddit') ? getSearchUrl('reddit', row.model) : '',
      '',
      missingKeys.has('stufferdb') ? getSearchUrl('stufferdb', row.model) : '',
      '',
      '',
    ]
    lines.push(values.map(csvCell).join(','))
  }

  fs.writeFileSync(csvPath, `${lines.join('\n')}\n`)
}

function writeMarkdown(rows) {
  const lines = [
    '# Manual Source Backfill Review',
    '',
    `Generated: ${new Date().toISOString()}`,
    '',
    'Paste any confirmed profile/category URL into the Matches / notes column, then give this file back to Codex to update `model_aliases.json`.',
    '',
    '| Model | Aliases | Missing sources | Search links | Matches / notes |',
    '| --- | --- | --- | --- | --- |',
  ]

  for (const row of rows) {
    const searchLinks = row.missing
      .map(
        ({ key, label }) =>
          `[${label}](${getSearchUrl(key, row.model).replace(/\)/g, '%29')})`
      )
      .join(' / ')
    lines.push(
      `| ${markdownText(row.model)} | ${markdownText(
        row.aliases.join(', ')
      )} | ${row.missing.map(({ label }) => label).join(', ')} | ${searchLinks} |  |`
    )
  }

  fs.writeFileSync(markdownPath, `${lines.join('\n')}\n`)
}

function run() {
  const registry = readJson(registryPath, {})
  const skipState = readJson(permanentSkipPath, { skips: {} })
  const rows = buildRows(registry, skipState)
  writeCsv(rows)
  writeMarkdown(rows)

  const counts = Object.fromEntries(
    platforms.map(({ key, label }) => [
      label,
      rows.filter((row) => row.missing.some((source) => source.key === key))
        .length,
    ])
  )
  console.log(`Wrote ${rows.length} models needing manual source review:`)
  console.log(`  ${csvPath}`)
  console.log(`  ${markdownPath}`)
  console.log(
    `Missing sources: ${Object.entries(counts)
      .map(([label, count]) => `${label} ${count}`)
      .join(', ')}`
  )
}

run()

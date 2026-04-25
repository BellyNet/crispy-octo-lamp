const fs = require('fs')
const path = require('path')

const rootDir = path.join(__dirname, '..')
const reportDir = path.join(rootDir, 'tmp', 'repair-stufferdb')
const inputPath = path.join(reportDir, 'repair-stufferdb-latest.json')
const latestJsonPath = path.join(reportDir, 'repair-failure-summary-latest.json')
const latestMdPath = path.join(reportDir, 'repair-failure-summary-latest.md')

main()

function main() {
  if (!fs.existsSync(inputPath)) {
    throw new Error(`Missing repair report: ${inputPath}`)
  }

  const repairReport = JSON.parse(fs.readFileSync(inputPath, 'utf8'))
  const rows = Array.isArray(repairReport.results) ? repairReport.results : []

  const summary = {
    generatedAt: new Date().toISOString(),
    inputPath,
    totals: {
      models: rows.length,
      scrapeFailures: 0,
      runErrors: 0,
      mediaErrors: 0,
      lazyVideoErrors: 0,
      topLevelRunErrors: 0,
    },
    scrapeFailures: [],
    byErrorMessage: {},
    byCategory: {},
    byModel: {},
    failures: [],
  }

  for (const row of rows) {
    const errors = Array.isArray(row?.lastRunSummary?.errors)
      ? row.lastRunSummary.errors
      : []
    const scrapeFailed = !row?.scrape?.ok

    if (scrapeFailed) {
      summary.totals.scrapeFailures += 1
      summary.scrapeFailures.push({
        model: row.model,
        code: row.scrape?.code ?? null,
        sourceUrl: row.sourceUrls?.[0] || null,
        status: row.lastRunSummary?.status || null,
        errorCount: row.lastRunSummary?.errorCount || 0,
      })
    }

    if (!errors.length) continue
    summary.byModel[row.model] = errors.length

    for (const error of errors) {
      summary.totals.runErrors += 1

      const category = String(error.category || 'unknown')
      summary.byCategory[category] = (summary.byCategory[category] || 0) + 1
      if (category === 'media_error') summary.totals.mediaErrors += 1
      if (category === 'lazy_video_error') summary.totals.lazyVideoErrors += 1
      if (category === 'run_error') summary.totals.topLevelRunErrors += 1

      const message = String(error.error || 'unknown')
      summary.byErrorMessage[message] =
        (summary.byErrorMessage[message] || 0) + 1

      summary.failures.push({
        model: row.model,
        category,
        error: message,
        at: error.at || null,
        sourceCategoryUrl: row.sourceUrls?.[0] || null,
        inputUrl: row.lastRunSummary?.inputUrl || null,
        mediaPageUrl: error.mediaPageUrl || null,
        mediaUrl: error.mediaUrl || null,
        filename: error.filename || null,
        savedPath: error.savedPath || null,
        sourceCommand: row.scrape?.command || null,
      })
    }
  }

  summary.scrapeFailures.sort((a, b) => a.model.localeCompare(b.model))
  summary.failures.sort((a, b) => {
    if (a.model !== b.model) return a.model.localeCompare(b.model)
    return String(a.at || '').localeCompare(String(b.at || ''))
  })

  fs.writeFileSync(latestJsonPath, JSON.stringify(summary, null, 2))
  fs.writeFileSync(latestMdPath, renderMarkdown(summary))

  console.log(`Wrote JSON: ${latestJsonPath}`)
  console.log(`Wrote Markdown: ${latestMdPath}`)
  console.log(
    `Scrape failures: ${summary.totals.scrapeFailures}; total run errors: ${summary.totals.runErrors}`
  )
}

function renderMarkdown(summary) {
  const lines = [
    '# Repair Failure Summary',
    '',
    `Generated: ${summary.generatedAt}`,
    `Source report: ${inputPath}`,
    '',
    '## Totals',
    '',
    `- Models processed: ${summary.totals.models}`,
    `- Scrape failures: ${summary.totals.scrapeFailures}`,
    `- Total run errors: ${summary.totals.runErrors}`,
    `- Media errors: ${summary.totals.mediaErrors}`,
    `- Lazy video errors: ${summary.totals.lazyVideoErrors}`,
    `- Top-level run errors: ${summary.totals.topLevelRunErrors}`,
    '',
    '## Scrape Failures',
    '',
  ]

  if (!summary.scrapeFailures.length) {
    lines.push('None.', '')
  } else {
    for (const item of summary.scrapeFailures) {
      lines.push(
        `- \`${item.model}\` :: code=\`${item.code}\` :: status=\`${item.status || 'n/a'}\` :: source=${toMdLink(
          'category',
          item.sourceUrl
        )}`
      )
    }
    lines.push('')
  }

  lines.push('## Common Error Messages', '')
  for (const [message, count] of Object.entries(summary.byErrorMessage).sort(
    (left, right) => right[1] - left[1]
  )) {
    lines.push(`- \`${message}\` :: ${count}`)
  }
  lines.push('', '## Failures', '')

  for (const failure of summary.failures) {
    const parts = [
      `- \`${failure.model}\``,
      `\`${failure.category}\``,
      `\`${failure.error}\``,
    ]
    if (failure.filename) parts.push(`file=\`${failure.filename}\``)
    if (failure.savedPath) parts.push(`saved=\`${failure.savedPath}\``)
    if (failure.mediaPageUrl) parts.push(`page=${toMdLink('gallery', failure.mediaPageUrl)}`)
    if (failure.mediaUrl) parts.push(`media=${toMdLink('source', failure.mediaUrl)}`)
    else if (failure.inputUrl) parts.push(`category=${toMdLink('category', failure.inputUrl)}`)
    lines.push(parts.join(' :: '))
  }

  lines.push('')
  return lines.join('\n')
}

function toMdLink(label, url) {
  return url ? `[${label}](${url})` : 'n/a'
}

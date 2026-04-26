const fs = require('fs')
const path = require('path')

const rootDir = path.join(__dirname, '..')
const reportDir = path.join(rootDir, 'tmp', 'repair-stufferdb')
const inputPath = path.join(reportDir, 'repair-stufferdb-latest.json')
const latestJsonPath = path.join(reportDir, 'repair-failure-summary-latest.json')
const latestMdPath = path.join(reportDir, 'repair-failure-summary-latest.md')
const BUCKET_LABELS = {
  page_timeout_concurrency: 'page concurrency / timeout issues',
  bad_source_configuration: 'bad source configuration',
  process_crash_case: 'process crash cases',
  upstream_bad_media: 'upstream-bad media',
}

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
    bucketCounts: {},
    bucketModels: {},
    buckets: [],
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

      const failure = {
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
      }

      const bucket = classifyFailure(failure)
      summary.bucketCounts[bucket.key] = (summary.bucketCounts[bucket.key] || 0) + 1
      if (!summary.bucketModels[bucket.key]) {
        summary.bucketModels[bucket.key] = new Set()
      }
      summary.bucketModels[bucket.key].add(row.model)

      summary.failures.push({
        ...failure,
        bucket: bucket.key,
        bucketLabel: bucket.label,
        bucketReason: bucket.reason,
      })
    }
  }

  summary.scrapeFailures.sort((a, b) => a.model.localeCompare(b.model))
  summary.buckets = Object.entries(summary.bucketCounts)
    .map(([key, count]) => ({
      key,
      label: BUCKET_LABELS[key] || key,
      count,
      models: Array.from(summary.bucketModels[key] || []).sort(),
    }))
    .sort((left, right) => right.count - left.count)
  summary.bucketModels = Object.fromEntries(
    Object.entries(summary.bucketModels).map(([key, models]) => [
      key,
      Array.from(models).sort(),
    ])
  )
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
    '## Bucketed Failures',
    '',
  ]

  if (!summary.buckets.length) {
    lines.push('None.', '')
  } else {
    for (const bucket of summary.buckets) {
      lines.push(
        `- \`${bucket.label}\` :: ${bucket.count} :: models=\`${bucket.models.join(', ') || 'n/a'}\``
      )
    }
    lines.push('')
  }

  lines.push(
    '## Scrape Failures',
    '',
  )

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
      `bucket=\`${failure.bucketLabel}\``,
      `\`${failure.category}\``,
      `\`${failure.error}\``,
    ]
    if (failure.bucketReason) parts.push(`reason=\`${failure.bucketReason}\``)
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

function classifyFailure(failure) {
  const category = String(failure.category || 'unknown')
  const error = String(failure.error || 'unknown').toLowerCase()
  const inputUrl = String(failure.inputUrl || '')
  const mediaUrl = String(failure.mediaUrl || '')

  if (error.includes('navigation timeout')) {
    return bucket(
      'page_timeout_concurrency',
      'navigation timeout while opening category or picture pages'
    )
  }

  if (category === 'gif_conversion_error') {
    return bucket(
      'process_crash_case',
      'local conversion code failed during post-download processing'
    )
  }

  if (category === 'run_error') {
    return bucket(
      'process_crash_case',
      'top-level run failed outside a single media item'
    )
  }

  if (
    inputUrl.includes('stufferai.com/index?/category/') ||
    mediaUrl.includes('cdn.stufferdb.com/index?/category/')
  ) {
    return bucket(
      'bad_source_configuration',
      'source/category mapping looks misconfigured or inconsistent'
    )
  }

  if (
    category === 'lazy_video_error' ||
    error.includes('tail_decode_error') ||
    error.includes('ffprobe_failed') ||
    error.includes('invalid_duration') ||
    error.includes('http 404')
  ) {
    return bucket(
      'upstream_bad_media',
      'downloaded media is missing, truncated, or structurally invalid'
    )
  }

  return bucket(
    'process_crash_case',
    'unclassified internal failure that needs code-side diagnosis'
  )
}

function bucket(key, reason) {
  return {
    key,
    label: BUCKET_LABELS[key] || key,
    reason,
  }
}

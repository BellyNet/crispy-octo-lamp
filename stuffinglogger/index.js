const chalk = require('chalk').default
const stripAnsi = require('strip-ansi').default
const readline = require('readline')
const ansiEscapes = require('ansi-escapes')

const plainProgressMode =
  process.env.MILKMAID_PROGRESS_MODE === 'plain' ||
  process.env.MILKMAID_PLAIN_PROGRESS === '1'

const scrapeFillChars = ['=']
const lazyFillChars = ['#']
const gifFillChars = ['+']

let persistentFillChar = pickFillChar(scrapeFillChars)

let pinnedTopLineText = ''
let pinnedBottomLineText = ''
let reservedProgressRows = false
const PINNED_ROW_COUNT = 2
let lastPlainScrapeBucket = null
let lastPlainLazyBucket = null

const scrapeLines = [
  'Scanning source pages.',
  'Checking new media candidates.',
  'Working through the category list.',
]

const gifLines = [
  'Processing GIF media.',
  'Checking animated images.',
  'Finishing GIF work.',
]

const lazyLines = [
  'Downloading queued videos.',
  'Streaming video bytes.',
  'Working through lazy downloads.',
]

const finishers = [
  'Scrape pass complete.',
  'Finished processing this run.',
  'Run complete.',
]

const milestoneLines = {
  25: ['Quarter complete.'],
  50: ['Halfway through.'],
  75: ['Three quarters complete.'],
  100: ['Complete.'],
}

const statusHeaders = [
  'status: scanning',
  'status: processing',
  'status: downloading',
]

function hasPinnedTerminalSupport() {
  return Boolean(
    !plainProgressMode &&
      process.stdout.isTTY &&
      Number.isFinite(process.stdout.rows) &&
      process.stdout.rows > PINNED_ROW_COUNT
  )
}

function ensurePinnedRows() {
  if (reservedProgressRows) return
  if (!hasPinnedTerminalSupport()) return
  process.stdout.write('\n'.repeat(PINNED_ROW_COUNT))
  reservedProgressRows = true
}

function redrawPinnedLines() {
  if (!reservedProgressRows || !hasPinnedTerminalSupport()) return

  process.stdout.write(
    ansiEscapes.cursorTo(0, process.stdout.rows - PINNED_ROW_COUNT)
  )
  readline.clearLine(process.stdout, 0)
  process.stdout.write(pinnedTopLineText)

  process.stdout.write(ansiEscapes.cursorTo(0, process.stdout.rows - 1))
  readline.clearLine(process.stdout, 0)
  process.stdout.write(pinnedBottomLineText)
}

function setPinnedLines(topText = '', bottomText = '') {
  pinnedTopLineText = topText || ''
  pinnedBottomLineText = bottomText || ''

  if (!hasPinnedTerminalSupport()) {
    if (pinnedTopLineText) process.stdout.write(`${pinnedTopLineText}\n`)
    if (pinnedBottomLineText) process.stdout.write(`${pinnedBottomLineText}\n`)
    return
  }

  ensurePinnedRows()
  redrawPinnedLines()
}

function logScrollingMessage(message = '') {
  if (!hasPinnedTerminalSupport()) {
    process.stdout.write(`${message}\n`)
    return
  }

  ensurePinnedRows()
  const maxMessageWidth = Math.max((process.stdout.columns || 80) - 1, 20)
  const displayMessage = truncateDisplayText(message, maxMessageWidth)

  process.stdout.write(
    ansiEscapes.cursorTo(0, process.stdout.rows - PINNED_ROW_COUNT)
  )
  process.stdout.write(ansiEscapes.eraseDown)
  process.stdout.write(`${displayMessage}\n`)
  redrawPinnedLines()
}

function pickFillChar(pool) {
  return pool[Math.floor(Math.random() * pool.length)]
}

function pickLine(pool, index = null) {
  if (!Array.isArray(pool) || pool.length === 0) return ''
  if (typeof index === 'number') return pool[index % pool.length]
  return pool[Math.floor(Math.random() * pool.length)]
}

function resetProgressBar(fillChar = null, phase = 'scrape') {
  const pool =
    phase === 'lazy'
      ? lazyFillChars
      : phase === 'gif'
        ? gifFillChars
        : scrapeFillChars

  persistentFillChar = fillChar || pickFillChar(pool)
  if (phase === 'lazy') {
    lastPlainLazyBucket = null
  } else {
    lastPlainScrapeBucket = null
  }
}

function getStatusHeader(index = null) {
  return pickLine(statusHeaders, index)
}

function getScrapeLine(index = null) {
  return pickLine(scrapeLines, index)
}

function getMilestoneLine(percent) {
  const lines = milestoneLines[percent]
  if (!lines?.length) return ''
  return pickLine(lines)
}

function getMilestoneBucket(current, total) {
  const safeTotal = Math.max(total || 1, 1)
  const ratio = current / safeTotal

  if (ratio >= 1) return 100
  if (ratio >= 0.75) return 75
  if (ratio >= 0.5) return 50
  if (ratio >= 0.25) return 25
  return null
}

function getMidPhrase(current, total) {
  const safeTotal = Math.max(total || 1, 1)
  const percent = (current / safeTotal) * 100

  if (percent > 90) return 'wrapping up...'
  if (percent > 80) return 'almost there...'
  if (percent > 70) return 'moving steadily...'
  if (percent > 60) return 'good progress...'
  if (percent > 50) return 'halfway through...'
  if (percent > 40) return 'working through the queue...'
  if (percent > 30) return 'building momentum...'
  if (percent > 20) return 'warming up...'
  if (percent > 10) return 'getting started...'
  return 'starting scan...'
}

function getDisplayWidth(text) {
  return stripAnsi(String(text || '')).length
}

function truncateDisplayText(text, maxWidth) {
  const source = String(text || '')
  if (!Number.isFinite(maxWidth) || maxWidth <= 0) return ''
  if (getDisplayWidth(source) <= maxWidth) return source

  const ellipsis = '...'
  const targetWidth = Math.max(maxWidth - ellipsis.length, 0)
  return `${source.slice(0, targetWidth)}${ellipsis}`
}

function getProgressRatio(current, total) {
  const safeCurrent = Math.max(Number(current) || 0, 0)
  const safeTotal = Math.max(Number(total) || 0, 1)

  if (safeCurrent <= safeTotal) {
    return {
      ratio: safeCurrent / safeTotal,
      overflow: false,
    }
  }

  const headroomTotal = safeCurrent + Math.max(3, Math.ceil(safeCurrent * 0.03))
  return {
    ratio: safeCurrent / headroomTotal,
    overflow: true,
  }
}

function buildProgressBar(leftText, current, total, fillChar = persistentFillChar) {
  const terminalWidth = process.stdout.columns || 80
  const visibleLeft = getDisplayWidth(leftText)
  const innerWidth = Math.max(terminalWidth - visibleLeft - 5, 10)

  const { ratio } = getProgressRatio(current, total)
  const boundedRatio = Math.max(0, Math.min(1, ratio || 0))
  const filledWidth = Math.floor(innerWidth * boundedRatio)
  const emptyCount = Math.max(innerWidth - filledWidth, 0)
  const safeFillChar = String(fillChar || '=').slice(0, 1)

  return `[${safeFillChar.repeat(filledWidth)}${'-'.repeat(emptyCount)}]`
}

function drawPinnedLines(topText, bottomText = '') {
  setPinnedLines(topText, bottomText)
}

function formatBytes(bytes) {
  const safeBytes = Number(bytes) || 0
  if (safeBytes <= 0) return '0 B'

  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let value = safeBytes
  let unitIndex = 0

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex += 1
  }

  const decimals = unitIndex === 0 ? 0 : value >= 100 ? 0 : value >= 10 ? 1 : 2
  return `${value.toFixed(decimals)} ${units[unitIndex]}`
}

function formatSpeed(bytesPerSecond) {
  const safeSpeed = Number(bytesPerSecond) || 0
  if (safeSpeed <= 0) return null
  return `${formatBytes(safeSpeed)}/s`
}

function formatEta(seconds) {
  const safeSeconds = Number(seconds)
  if (!Number.isFinite(safeSeconds) || safeSeconds < 0) return null

  const rounded = Math.max(Math.round(safeSeconds), 0)
  const hours = Math.floor(rounded / 3600)
  const minutes = Math.floor((rounded % 3600) / 60)
  const secs = rounded % 60

  if (hours > 0) return `${hours}h ${minutes}m`
  if (minutes > 0) return `${minutes}m ${secs}s`
  return `${secs}s`
}

function truncateLabel(text, maxLength = 42) {
  const normalized = String(text || '').trim()
  if (!normalized) return null
  if (normalized.length <= maxLength) return normalized
  return `...${normalized.slice(-(maxLength - 3))}`
}

function logProgress(current, total, options = {}) {
  const safeTotal = Math.max(total || 1, 1)
  const percent = (Math.max(Number(current) || 0, 0) / safeTotal) * 100
  const plainBucket = Math.min(100, Math.floor(percent / 10) * 10)
  const bottomText =
    typeof options.bottomText === 'string' ? options.bottomText.trim() : ''

  if (plainProgressMode) {
    if (plainBucket === lastPlainScrapeBucket && current < safeTotal) {
      return
    }

    lastPlainScrapeBucket = plainBucket
    const line = `[scrape] ${current}/${safeTotal} (${percent.toFixed(1)}%) ${getMidPhrase(current, safeTotal)}`
    process.stdout.write(
      `${bottomText ? `${line} | ${stripAnsi(bottomText)}` : line}\n`
    )
    return
  }

  const { overflow } = getProgressRatio(current, safeTotal)
  const progressStats = chalk.black.bgCyan(
    ` ${current}/${safeTotal}${overflow ? '+' : ''} `
  )
  const phaseText = chalk.whiteBright('SCRAPE')
  const phrase = chalk.gray(getMidPhrase(current, safeTotal))
  const leftText = `${progressStats} ${phaseText} ${phrase} `
  const bar = buildProgressBar(leftText, current, safeTotal, persistentFillChar)
  drawPinnedLines(`${leftText}${chalk.cyan(bar)}`, chalk.gray(bottomText))
}

function logLazyProgress(percent, downloadedBytes, totalBytes = 0, options = {}) {
  const safePercent = Math.max(0, Math.min(100, Number(percent) || 0))
  const plainBucket = Math.min(100, Math.floor(safePercent / 10) * 10)

  if (plainProgressMode) {
    if (plainBucket === lastPlainLazyBucket && safePercent < 100) {
      return
    }

    lastPlainLazyBucket = plainBucket
    const details = [
      `${formatBytes(downloadedBytes)} / ${totalBytes > 0 ? formatBytes(totalBytes) : '?'}`,
    ]

    const speedText = formatSpeed(options.speedBytesPerSecond)
    if (speedText) details.push(speedText)

    if (Number.isFinite(options.activeCount) && options.activeCount > 0) {
      details.push(`${options.activeCount} active`)
    }

    const currentLabel = truncateLabel(options.currentLabel)
    if (currentLabel) details.push(currentLabel)

    process.stdout.write(
      `[lazy] ${safePercent.toFixed(1)}% (${details.join(' | ')})\n`
    )
    return
  }

  const leftText = `${chalk.black.bgMagenta(` ${safePercent.toFixed(1)}% `)} ${chalk.whiteBright('LAZY')} ${chalk.gray('downloading video bytes...')} `
  const bar = buildProgressBar(leftText, safePercent, 100, persistentFillChar)
  const details = [
    `${formatBytes(downloadedBytes)} / ${totalBytes > 0 ? formatBytes(totalBytes) : '?'}`,
  ]

  const speedText = formatSpeed(options.speedBytesPerSecond)
  if (speedText) details.push(speedText)

  const etaText = totalBytes > 0 ? formatEta(options.etaSeconds) : null
  if (etaText) details.push(`ETA ${etaText}`)

  if (
    Number.isFinite(options.completedCount) &&
    Number.isFinite(options.totalCount) &&
    options.totalCount > 0
  ) {
    details.push(`${options.completedCount}/${options.totalCount} done`)
  }

  if (Number.isFinite(options.activeCount) && options.activeCount > 0) {
    details.push(`${options.activeCount} active`)
  }

  const currentLabel = truncateLabel(options.currentLabel)
  if (currentLabel) details.push(currentLabel)

  drawPinnedLines(
    `${leftText}${chalk.magenta(bar)}`,
    chalk.gray(`(${details.join(' | ')})`)
  )
}

function logGifConversion(index) {
  return pickLine(gifLines, index)
}

function logLazyDownload(index) {
  return pickLine(lazyLines, index)
}

function getCompletionLine() {
  return pickLine(finishers)
}

module.exports = {
  logProgress,
  logLazyProgress,
  resetProgressBar,
  logGifConversion,
  logLazyDownload,
  getCompletionLine,
  getScrapeLine,
  getStatusHeader,
  getMilestoneLine,
  getMilestoneBucket,
  logScrollingMessage,
}

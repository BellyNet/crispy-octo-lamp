const chalk = require('chalk').default
const stripAnsi = require('strip-ansi').default
const readline = require('readline')
const ansiEscapes = require('ansi-escapes')

const scrapeEmojis = ['🐷', '🐽', '🍰', '🍕', '🧁', '🐄']
const lazyEmojis = ['🥛', '🧈', '🍮', '🫃', '🍓', '💞']
const gifEmojis = ['🧈', '🍑', '🍮', '🫃', '🐄', '🐷']
const FOOTER_ROWS = 2

let persistentEmoji = pickEmoji(scrapeEmojis)
let pinnedFooterLines = Array(FOOTER_ROWS).fill('')
let reservedFooterRows = false

function hasPinnedTerminalSupport() {
  return Boolean(
    process.stdout.isTTY &&
      Number.isFinite(process.stdout.rows) &&
      process.stdout.rows > FOOTER_ROWS &&
      Number.isFinite(process.stdout.columns) &&
      process.stdout.columns > 20
  )
}

function getTerminalWidth() {
  return process.stdout.columns || 80
}

function getFooterRowOffset(index) {
  return process.stdout.rows - FOOTER_ROWS + index
}

function fitPlainText(text, maxWidth = getTerminalWidth()) {
  const normalized = String(text || '')
    .replace(/\s+/g, ' ')
    .trim()
  if (!normalized) return ''
  if (normalized.length <= maxWidth) return normalized
  if (maxWidth <= 3) return normalized.slice(0, maxWidth)
  return `${normalized.slice(0, maxWidth - 3)}...`
}

function fitRenderedText(text, maxWidth = getTerminalWidth()) {
  const visibleLength = stripAnsi(String(text || '')).length
  if (visibleLength <= maxWidth) return text
  return fitPlainText(stripAnsi(String(text || '')), maxWidth)
}

function ensurePinnedRows() {
  if (reservedFooterRows || !hasPinnedTerminalSupport()) return
  process.stdout.write('\n'.repeat(FOOTER_ROWS))
  reservedFooterRows = true
}

function clearPinnedFooter() {
  if (!reservedFooterRows || !hasPinnedTerminalSupport()) return

  for (let index = 0; index < FOOTER_ROWS; index++) {
    process.stdout.write(ansiEscapes.cursorTo(0, getFooterRowOffset(index)))
    readline.clearLine(process.stdout, 0)
  }
}

function redrawPinnedFooter() {
  if (!reservedFooterRows || !hasPinnedTerminalSupport()) return

  clearPinnedFooter()

  for (let index = 0; index < FOOTER_ROWS; index++) {
    process.stdout.write(ansiEscapes.cursorTo(0, getFooterRowOffset(index)))
    process.stdout.write(fitRenderedText(pinnedFooterLines[index] || ''))
  }
}

function setPinnedFooter(lines) {
  const nextLines = Array.from({ length: FOOTER_ROWS }, (_, index) =>
    fitRenderedText(lines[index] || '')
  )
  pinnedFooterLines = nextLines

  if (!hasPinnedTerminalSupport()) {
    for (const line of nextLines) {
      if (line) process.stdout.write(`${stripAnsi(line)}\n`)
    }
    return
  }

  ensurePinnedRows()
  redrawPinnedFooter()
}

function logScrollingMessage(message = '') {
  const normalized = fitPlainText(message)

  if (!hasPinnedTerminalSupport()) {
    process.stdout.write(`${normalized}\n`)
    return
  }

  ensurePinnedRows()
  clearPinnedFooter()
  process.stdout.write(ansiEscapes.cursorTo(0, process.stdout.rows - 1))
  readline.clearLine(process.stdout, 0)
  process.stdout.write(`${normalized}\n`)
  redrawPinnedFooter()
}

const scrapeLines = [
  '🐄 Soft little cow is nosing around for another treat before she’s even done swallowing the last one.',
  '🐷 Hungry piggy is sniffing out something warm and filling for that empty little belly.',
  '🍰 She’s got that needy look again—like a bratty cow who knows she’s about to overdo it.',
  '🐽 Little piggy is already wandering back toward the trough without meaning to.',
  '🧁 She swore she was done snacking, but that soft belly says otherwise.',
  '🥛 Sweet cow girl is looking for something rich enough to make her wobble.',
  '🍓 Piggy’s poking around for a mouthful that turns into five.',
  '🐄 She’s not even acting hungry anymore—just drawn toward another bite like a silly little heifer.',
  '🍞 Full cheeks, empty hands, and still somehow looking for more.',
  '🐷 Little piggy is roaming around like she forgot how full she got last time.',
  '🍮 Soft brat wants “just a nibble,” which of course means stuffing herself silly.',
  '🐽 She’s got that helpless snacky look, like once she starts she won’t stop.',
  '🐄 Pretty little cow is drifting back to the feed again, all soft and absentminded about it.',
  '🍰 She keeps telling herself one more treat won’t hurt, and she never means just one.',
  '🥐 Piggy’s got that look in her eyes like she’s about to fill up way too fast again.',
]

const gifLines = [
  '🐷 Her belly’s sloshing and those thick thighs won’t stop brushing together.',
  '🐄 Soft little cow is wobbling all over, full belly leading the way.',
  '🧈 All that plush softness just jiggling with every tiny movement.',
  '🍮 She moves like pudding now—slow, soft, and completely overfilled.',
  '🥛 That round belly keeps wobbling like it’s still settling.',
  '🍰 Thick thighs swaying, soft tummy bouncing, little piggy looking properly stuffed.',
  '🐽 Every step makes her belly and thighs slosh together in the sweetest way.',
  '🫃 She’s so full her whole body has that heavy, wobbling softness to it.',
  '🐄 Cow belly swaying low and slow, like she got fed a little too well.',
  '🧁 Big soft piggy is jiggling top to bottom without even trying.',
  '🍓 Belly bouncing, thighs kissing, cheeks warm—she’s a wobbling little mess.',
  '🥞 She’s all plush tummy, heavy thighs, and slow overstuffed wiggles.',
  '🍦 Stuffed too full to move gracefully, so now everything just sloshes.',
  '🐷 Her belly keeps bouncing like it’s proud of what she did.',
  '💞 Soft all over, full all over, wobbling like a darling little dairy cow.',
]

const lazyLines = [
  '🐷 Piggy’s taking slow little bites now, but she still can’t seem to stop.',
  '🐄 Sweet cow girl is being fed nice and easy, just enough to keep that belly rounding out.',
  '🍰 She’s already full, but she keeps opening up for one more bite.',
  '🥛 Slow stuffing for a soft little thing who always says she can’t eat another bite—right before she does.',
  '🐽 She’s chewing so slowly now, like a stuffed piggy who got in too deep but doesn’t want it to end.',
  '🫃 Little piggy is full to the brim and still making room, somehow.',
  '🍮 She keeps taking bites like she’s embarrassed by it, but not enough to stop.',
  '🐄 Pretty little cow is still taking her feed, warm and docile and way too full already.',
  '🧁 One more sweet little bite for the overstuffed brat.',
  '🍞 She’s being filled up so gently, and that soft belly just keeps rising.',
  '🐷 Slow bites, full cheeks, thick thighs spread a little wider every minute.',
  '🥐 She looks like she wants to stop, but her mouth keeps opening anyway.',
  '🍓 Stuffed piggy is eating with that dazed little look she gets when she’s gone past full.',
  '🐄 Cow girl is taking it nice and slow, heavy belly settling in her lap.',
  '💞 Soft, sleepy, overfed little thing still nibbling like she was made for this.',
]

const finishers = [
  '🐄 Soft little cow is so full she can barely waddle, and she’s blushing because she loved every second of it.',
  '🐷 Piggy’s belly is heavy, her thighs are rubbing, and she looks way too pleased for someone this overstuffed.',
  '🍰 She got so full she can hardly move now—just waddling and blushing and secretly wishing for dessert.',
  '🫃 That belly is round and swaying, and she’s got that shy little look like she knows she overdid it again.',
  '🐽 Stuffed silly, waddling slow, cheeks warm, and still not really ready to be done.',
  '🥛 She looks embarrassed by how full she got, but her happy little face gives her away.',
  '🐄 Pretty cow girl got fed until she was slow and heavy and absolutely glowing with it.',
  '🍮 Belly packed full, steps clumsy, and somehow she’d still make room for something sweet.',
  '🐷 Piggy waddled herself into that soft overfull haze and stayed there on purpose.',
  '🧁 She’s so stuffed her belly sways when she walks, and she still looks like she wants another treat.',
  '🍓 Overfed, flushed, and just a little ashamed—but not ashamed enough to regret a single bite.',
  '🐄 Sweet heifer got herself impossibly full and now she’s standing there all dazed and lovely about it.',
  '🥞 She ate until her little waddle came out, then acted shy like she didn’t adore getting this stuffed.',
  '🍦 Too full to move properly, too happy to care, and still thinking about one last little bite.',
  '💞 Soft piggy got carried away again—now she’s all round belly, wobbly steps, and bashful satisfaction.',
]

const milestoneLines = {
  25: [
    '🐷 Piggy’s just getting started.',
    '🍰 First few bites down, and she’s already looking softer.',
    '🐄 Little cow is warming up nicely now.',
  ],
  50: [
    '🫃 Half full and getting that sweet heavy-bellied look.',
    '🥛 Belly rounding out now. She’s in trouble.',
    '🐽 Piggy’s at that point where she knows she should slow down, but won’t.',
  ],
  75: [
    '🐄 She’s getting waddly now, poor soft thing.',
    '🍮 Stuffed enough to blush, still taking more.',
    '🐷 Belly full, cheeks warm, and somehow she keeps going.',
  ],
  100: [
    '💞 Properly stuffed and very quietly thrilled about it.',
    '🍓 Full to the brim and still wishing there was room for dessert.',
    '🐄 Waddling, blushing, and entirely too pleased with herself.',
  ],
}

const statusHeaders = [
  '🐷 piggy mode: hungry',
  '🐄 cow mode: overfed',
  '🍰 stuffing session: active',
  '🧈 softness levels: rising',
  '🍮 belly status: rounding out',
  '💞 bratty feeder mode: engaged',
]

function pickEmoji(pool) {
  return pool[Math.floor(Math.random() * pool.length)]
}

function pickLine(pool, index = null) {
  if (!Array.isArray(pool) || pool.length === 0) return ''
  if (typeof index === 'number') return pool[index % pool.length]
  return pool[Math.floor(Math.random() * pool.length)]
}

function resetProgressBar(emoji = null, phase = 'scrape') {
  const pool =
    phase === 'lazy' ? lazyEmojis : phase === 'gif' ? gifEmojis : scrapeEmojis

  persistentEmoji = emoji || pickEmoji(pool)
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

  if (percent > 90) return 'waddling and very full...'
  if (percent > 80) return 'blushing through it...'
  if (percent > 70) return 'way too stuffed to stop...'
  if (percent > 60) return 'soft belly getting heavy...'
  if (percent > 50) return 'rounding out nicely...'
  if (percent > 40) return 'getting fuller by the bite...'
  if (percent > 30) return 'snacking like a little piggy...'
  if (percent > 20) return 'warming up that belly...'
  if (percent > 10) return 'just getting started...'
  return 'hungry and nosing around...'
}

function buildEmojiBar(leftText, ratio, emoji = persistentEmoji) {
  const terminalWidth = getTerminalWidth()
  const visibleLeft = stripAnsi(leftText).length
  const innerWidth = Math.max(terminalWidth - visibleLeft - 3, 10)
  const boundedRatio = Math.max(0, Math.min(1, ratio || 0))
  const filledWidth = Math.floor(innerWidth * boundedRatio)
  const emojiWidth = 2
  const emojiCount = Math.floor(filledWidth / emojiWidth)
  const emptyCount = Math.max(innerWidth - emojiCount * emojiWidth, 0)

  return `[${emoji.repeat(emojiCount)}${'—'.repeat(emptyCount)}]`
}

function formatBytes(bytes) {
  const safeBytes = Number(bytes) || 0
  if (safeBytes <= 0) return '0 B'

  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let value = safeBytes
  let unitIndex = 0

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex++
  }

  const decimals = unitIndex === 0 ? 0 : value >= 100 ? 0 : value >= 10 ? 1 : 2
  return `${value.toFixed(decimals)} ${units[unitIndex]}`
}

function formatSpeed(bytesPerSecond) {
  const safeSpeed = Number(bytesPerSecond) || 0
  if (safeSpeed <= 0) return null
  return `${formatBytes(safeSpeed)}/s`
}

function formatDuration(durationMs) {
  const safeMs = Number(durationMs) || 0
  const roundedSeconds = Math.max(Math.round(safeMs / 1000), 0)
  const hours = Math.floor(roundedSeconds / 3600)
  const minutes = Math.floor((roundedSeconds % 3600) / 60)
  const seconds = roundedSeconds % 60

  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`
  if (minutes > 0) return `${minutes}m ${seconds}s`
  return `${seconds}s`
}

function buildDetailLine(parts) {
  return fitPlainText(parts.filter(Boolean).join(' | '))
}

function logProgress(current, total, options = {}) {
  const safeTotal = Math.max(total || 1, 1)
  const ratio = current / safeTotal
  const progressStats = chalk.cyan(`${current}/${safeTotal}`)
  const pig = chalk.magentaBright('🐷')
  const phrase = chalk.gray(getMidPhrase(current, safeTotal))
  const leftText = `${progressStats} ${pig} ${phrase} `
  const bar = buildEmojiBar(leftText, ratio)
  const detailLine = buildDetailLine([
    options.totalTransferredBytes > 0
      ? `Run ${formatBytes(options.totalTransferredBytes)} downloaded`
      : null,
    Number.isFinite(options.savedCount) ? `${options.savedCount} saved` : null,
    Number.isFinite(options.duplicateCount)
      ? `${options.duplicateCount} dupes`
      : null,
    Number.isFinite(options.errorCount) ? `${options.errorCount} errors` : null,
  ])

  setPinnedFooter([`${leftText}${bar}`, chalk.gray(detailLine)])
}

function logLazyProgress(
  percent,
  downloadedBytes,
  totalBytes = 0,
  options = {}
) {
  const safePercent = Math.max(0, Math.min(100, Number(percent) || 0))
  const ratio = safePercent / 100
  const leftText = `${chalk.cyan(`${safePercent.toFixed(1)}%`)} ${chalk.magentaBright('🐷')} ${chalk.gray('slow stuffing...')} `
  const bar = buildEmojiBar(leftText, ratio)
  const detailLine = buildDetailLine([
    options.totalTransferredBytes > 0
      ? `Run ${formatBytes(options.totalTransferredBytes)}`
      : null,
    `Lazy ${formatBytes(downloadedBytes)} / ${totalBytes > 0 ? formatBytes(totalBytes) : '?'}`,
    options.averageSpeedBytesPerSecond
      ? `Avg ${formatSpeed(options.averageSpeedBytesPerSecond)}`
      : null,
    totalBytes > 0 && Number.isFinite(options.etaSeconds)
      ? `ETA ${formatDuration(options.etaSeconds * 1000)}`
      : null,
    Number.isFinite(options.completedCount) &&
    Number.isFinite(options.totalCount)
      ? `${options.completedCount}/${options.totalCount} done`
      : null,
    Number.isFinite(options.activeCount) && options.activeCount > 0
      ? `${options.activeCount} active`
      : null,
  ])

  setPinnedFooter([`${leftText}${bar}`, chalk.gray(detailLine)])
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
  clearPinnedFooter,
  formatBytes,
  formatDuration,
  getCompletionLine,
  getMilestoneBucket,
  getMilestoneLine,
  getScrapeLine,
  getStatusHeader,
  logGifConversion,
  logLazyDownload,
  logLazyProgress,
  logProgress,
  logScrollingMessage,
  resetProgressBar,
}

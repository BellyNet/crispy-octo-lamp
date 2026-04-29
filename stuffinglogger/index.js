const chalk = require('chalk').default
const stripAnsi = require('strip-ansi').default
const readline = require('readline')
const ansiEscapes = require('ansi-escapes')

const scrapeEmojis = ['🐷', '🐽', '🍰', '🍕', '🧁', '🐄']
const lazyEmojis = ['🥛', '🧈', '🍮', '🫃', '🍓', '💞']
const gifEmojis = ['🧈', '🍑', '🍮', '🫃', '🐄', '🐷']

let persistentEmoji = pickEmoji(scrapeEmojis)

let pinnedTopLineText = ''
let pinnedBottomLineText = ''
let reservedProgressRows = false
const PINNED_ROW_COUNT = 2

function hasPinnedTerminalSupport() {
  return Boolean(
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

  process.stdout.write(ansiEscapes.cursorTo(0, process.stdout.rows - PINNED_ROW_COUNT))
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

  // Print the message above the reserved bottom rows, then redraw the pinned lines.
  process.stdout.write(ansiEscapes.cursorTo(0, process.stdout.rows - PINNED_ROW_COUNT))
  process.stdout.write(ansiEscapes.eraseDown)
  process.stdout.write(`${message}\n`)
  redrawPinnedLines()
}

const scrapeLines = [
  '🐄 Soft little cow is nosing around for another treat before she is even done swallowing the last one.',
  '🐷 Hungry piggy is sniffing out something warm and filling for that empty little belly.',
  '🍰 She has got that needy look again, like a bratty cow who knows she is about to overdo it.',
  '🐽 Little piggy is already wandering back toward the trough without meaning to.',
  '🧁 She swore she was done snacking, but that soft belly says otherwise.',
  '🥛 Sweet cow girl is looking for something rich enough to make her wobble.',
  '🍓 Piggy is poking around for a mouthful that turns into five.',
  '🐄 She is not even acting hungry anymore, just drawn toward another bite like a silly little heifer.',
  '🍞 Full cheeks, empty hands, and still somehow looking for more.',
  '🐷 Little piggy is roaming around like she forgot how full she got last time.',
  '🍮 Soft brat wants just a nibble, which of course means stuffing herself silly.',
  '🐽 She has got that helpless snacky look, like once she starts she will not stop.',
  '🐄 Pretty little cow is drifting back to the feed again, all soft and absentminded about it.',
  '🍰 She keeps telling herself one more treat will not hurt, and she never means just one.',
  '🥐 Piggy has got that look in her eyes like she is about to fill up way too fast again.',
]

const gifLines = [
  '🐷 Her belly is sloshing and those thick thighs will not stop brushing together.',
  '🐄 Soft little cow is wobbling all over, full belly leading the way.',
  '🧈 All that plush softness just jiggling with every tiny movement.',
  '🍮 She moves like pudding now, slow, soft, and completely overfilled.',
  '🥛 That round belly keeps wobbling like it is still settling.',
  '🍰 Thick thighs swaying, soft tummy bouncing, little piggy looking properly stuffed.',
  '🐽 Every step makes her belly and thighs slosh together in the sweetest way.',
  '🫃 She is so full her whole body has that heavy, wobbling softness to it.',
  '🐄 Cow belly swaying low and slow, like she got fed a little too well.',
  '🧁 Big soft piggy is jiggling top to bottom without even trying.',
  '🍓 Belly bouncing, thighs kissing, cheeks warm, she is a wobbling little mess.',
  '🥖 She is all plush tummy, heavy thighs, and slow overstuffed wiggles.',
  '🍦 Stuffed too full to move gracefully, so now everything just sloshes.',
  '🐷 Her belly keeps bouncing like it is proud of what she did.',
  '💞 Soft all over, full all over, wobbling like a darling little dairy cow.',
]

const lazyLines = [
  '🐷 Piggy is taking slow little bites now, but she still cannot seem to stop.',
  '🐄 Sweet cow girl is being fed nice and easy, just enough to keep that belly rounding out.',
  '🍰 She is already full, but she keeps opening up for one more bite.',
  '🥛 Slow stuffing for a soft little thing who always says she cannot eat another bite, right before she does.',
  '🐽 She is chewing so slowly now, like a stuffed piggy who got in too deep but does not want it to end.',
  '🫃 Little piggy is full to the brim and still making room, somehow.',
  '🍮 She keeps taking bites like she is embarrassed by it, but not enough to stop.',
  '🐄 Pretty little cow is still taking her feed, warm and docile and way too full already.',
  '🧁 One more sweet little bite for the overstuffed brat.',
  '🍞 She is being filled up so gently, and that soft belly just keeps rising.',
  '🐷 Slow bites, full cheeks, thick thighs spread a little wider every minute.',
  '🥐 She looks like she wants to stop, but her mouth keeps opening anyway.',
  '🍓 Stuffed piggy is eating with that dazed little look she gets when she has gone past full.',
  '🐄 Cow girl is taking it nice and slow, heavy belly settling in her lap.',
  '💞 Soft, sleepy, overfed little thing still nibbling like she was made for this.',
]

const finishers = [
  '🐄 Soft little cow is so full she can barely waddle, and she is blushing because she loved every second of it.',
  '🐷 Piggy belly is heavy, her thighs are rubbing, and she looks way too pleased for someone this overstuffed.',
  '🍰 She got so full she can hardly move now, just waddling and blushing and secretly wishing for dessert.',
  '🫃 That belly is round and swaying, and she has got that shy little look like she knows she overdid it again.',
  '🐽 Stuffed silly, waddling slow, cheeks warm, and still not really ready to be done.',
  '🥛 She looks embarrassed by how full she got, but her happy little face gives her away.',
  '🐄 Pretty cow girl got fed until she was slow and heavy and absolutely glowing with it.',
  '🍮 Belly packed full, steps clumsy, and somehow she would still make room for something sweet.',
  '🐷 Piggy waddled herself into that soft overfull haze and stayed there on purpose.',
  '🧁 She is so stuffed her belly sways when she walks, and she still looks like she wants another treat.',
  '🍓 Overfed, flushed, and just a little ashamed, but not ashamed enough to regret a single bite.',
  '🐄 Sweet heifer got herself impossibly full and now she is standing there all dazed and lovely about it.',
  '🥖 She ate until her little waddle came out, then acted shy like she did not adore getting this stuffed.',
  '🍦 Too full to move properly, too happy to care, and still thinking about one last little bite.',
  '💞 Soft piggy got carried away again, now she is all round belly, wobbly steps, and bashful satisfaction.',
]

const milestoneLines = {
  25: [
    '🐷 Piggy is just getting started.',
    '🍰 First few bites down, and she is already looking softer.',
    '🐄 Little cow is warming up nicely now.',
  ],
  50: [
    '🫃 Half full and getting that sweet heavy-bellied look.',
    '🥛 Belly rounding out now. She is in trouble.',
    '🐽 Piggy is at that point where she knows she should slow down, but will not.',
  ],
  75: [
    '🐄 She is getting waddly now, poor soft thing.',
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

function getDisplayWidth(text) {
  const visible = stripAnsi(String(text || ''))
  let width = 0

  for (const char of Array.from(visible)) {
    const codePoint = char.codePointAt(0) || 0

    if (
      codePoint === 0x200d ||
      (codePoint >= 0xfe00 && codePoint <= 0xfe0f)
    ) {
      continue
    }

    width += codePoint > 0xffff ? 2 : 1
  }

  return width
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

function buildEmojiBar(leftText, current, total, emoji = persistentEmoji) {
  const terminalWidth = process.stdout.columns || 80
  const visibleLeft = getDisplayWidth(leftText)
  const innerWidth = Math.max(terminalWidth - visibleLeft - 5, 10)

  const { ratio } = getProgressRatio(current, total)
  const boundedRatio = Math.max(0, Math.min(1, ratio || 0))
  const filledWidth = Math.floor(innerWidth * boundedRatio)

  const emojiWidth = 2
  const emojiCount = Math.floor(filledWidth / emojiWidth)
  const emptyCount = Math.max(innerWidth - emojiCount * emojiWidth, 0)

  return `[${emoji.repeat(emojiCount)}${'—'.repeat(emptyCount)}]`
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

function logProgress(current, total) {
  const safeTotal = Math.max(total || 1, 1)
  const { overflow } = getProgressRatio(current, safeTotal)
  const progressStats = chalk.cyan(
    `${current}/${safeTotal}${overflow ? '+' : ''}`
  )
  const pig = chalk.magentaBright('🐷')
  const phrase = chalk.gray(getMidPhrase(current, safeTotal))
  const leftText = `${progressStats} ${pig} ${phrase} `
  const bar = buildEmojiBar(leftText, current, safeTotal)
  const bottomText = chalk.gray(
    'File save / skip / fail updates should scroll above these pinned stats.'
  )

  drawPinnedLines(`${leftText}${bar}`, bottomText)
}

function logLazyProgress(percent, downloadedBytes, totalBytes = 0, options = {}) {
  const safePercent = Math.max(0, Math.min(100, Number(percent) || 0))
  const lazyProgressValue = safePercent
  const leftText = `${chalk.cyan(`${safePercent.toFixed(1)}%`)} ${chalk.magentaBright('🐷')} ${chalk.gray('slow stuffing...')} `
  const bar = buildEmojiBar(leftText, lazyProgressValue, 100)
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
    `${leftText}${bar}`,
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

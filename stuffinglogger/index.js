const readline = require('readline')

const foodEmojis = ['🍩', '🍰', '🍕', '🍔', '🍝', '🥛', '🧈']
const phrases = [
  'just getting started...',
  'filling out nicely...',
  "ohh she's getting heavier...",
  'stuffed and still going...',
  'jiggling with every step...',
  'leaking out the edges...',
  'oozing bits at 75%...',
  'engorged and ravenous...',
  'too full to move...',
  '💥 BURSTING 💥',
]

function getMidPhrase(current, total) {
  const percent = (current / total) * 100
  if (percent > 90) return phrases[9]
  if (percent > 80) return phrases[8]
  if (percent > 70) return phrases[7]
  if (percent > 60) return phrases[6]
  if (percent > 50) return phrases[5]
  if (percent > 40) return phrases[4]
  if (percent > 30) return phrases[3]
  if (percent > 20) return phrases[2]
  if (percent > 10) return phrases[1]
  return phrases[0]
}

function logProgress(current, total) {
  const barLength = 10
  const filledLength = Math.floor((current / total) * barLength)
  const bar = foodEmojis.slice(0, filledLength).join('').padEnd(barLength, '—')
  const phrase = getMidPhrase(current, total)

  const statusLine = `🐷 [${bar}] ${current}/${total} – ${phrase}`

  readline.cursorTo(process.stdout, 0)
  readline.clearLine(process.stdout, 0)
  process.stdout.write(statusLine.padEnd(process.stdout.columns || 80))
}

const gifLines = [
  '🍑 Jiggle queued. It’s gonna wiggle.',
  '🧈 Too thicc to stay still—converting wobble to watchable.',
  '🐽 Turning shake into smut.',
  '💃 Boobs bouncing, thighs clapping—MP4 incoming.',
  '🥵 Quivering fat captured. Format upgraded.',
  '🫠 Jellied and jiggled—she’s smoothing out.',
  '🧃 That belly ripple? Now in HD.',
  '🎞️ Frame by frame, she’s shaking it all out.',
  '📹 Vibrating violently… one file at a time.',
]

function logGifConversion(index) {
  return gifLines[index % gifLines.length]
}

const lazyLines = [
  '🍝 Video queued—she’s slurping it down slow.',
  '📼 Full meal media—served lazy.',
  '🫃 Big bites take time… she’s chewing through video.',
  '🐷 Savoring every second—downloading that fat.',
  '🐄 File fattening in progress.',
  '🧸 Bulky content incoming. She’ll be stuffed soon.',
  '🎬 Chunky motion—feeding in progress.',
  '🥛 Creamy video. Down the hatch.',
  '🍲 A full course of curves—still being swallowed.',
]

function logLazyDownload(index) {
  return lazyLines[index % lazyLines.length]
}

const finishers = [
  '🍰 Her belly’s packed tight. No room left, not even for crumbs.',
  '🍩 Stuffed beyond recognition. That’s a full-grown media sow.',
  '🥛 Creamed, dumped, and distended—this one’s done.',
  '🧃 Every file fed. She’s groaning and gurgling.',
  '🍝 That’s a wrap—she swallowed the whole damn archive.',
  '🧈 Not a single bit left behind. Just rolls, grease, and glory.',
  '🥵 Bursting at the seams. There’s no going back now.',
  '💾 Fully uploaded. Fully engorged. She’s a digital dump truck.',
  '🍽️ Glutted, gulped, and gut-heavy. That’s how we end things here.',
]

function getCompletionLine() {
  return finishers[Math.floor(Math.random() * finishers.length)]
}

module.exports = {
  logProgress,
  logGifConversion,
  logLazyDownload,
  getCompletionLine,
}

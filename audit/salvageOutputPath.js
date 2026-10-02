'use strict'

const path = require('path')
const config = require('../scrapyard/config')

// Where the salvaged copy of a video goes: <outputRoot>/<where it came
// from>/<name>.salvaged.mp4, e.g. salvaged/quarantine/dataset/<model>/webm/…
// for a quarantined file. Paths recorded before the move to the NAS (under
// %APPDATA%\.slopvault) map to the same layout.
function salvageOutputPath(outputRoot, filePath) {
  let relative = path.basename(filePath)
  for (const [root, label] of [
    [config.quarantineDir, 'quarantine'],
    [config.datasetDir, 'dataset'],
  ]) {
    const inside = path.relative(root, filePath)
    if (inside && !inside.startsWith('..') && !path.isAbsolute(inside)) {
      relative = path.join(label, inside)
      break
    }
  }
  const normalized = String(filePath || '').replace(/\\/g, '/')
  const marker = normalized.toLowerCase().indexOf('/.slopvault/')
  if (relative === path.basename(filePath) && marker >= 0) {
    relative = normalized.slice(marker + '/.slopvault/'.length)
  }
  const parsed = path.parse(relative)
  return path.join(outputRoot, parsed.dir, `${parsed.name}.salvaged.mp4`)
}

module.exports = { salvageOutputPath }

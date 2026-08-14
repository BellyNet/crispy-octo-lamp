'use strict'

const readline = require('readline')

function createStatusLineLogger(baseLogger = console) {
  let active = false
  let lastStatusText = null
  const output = process.stdout

  function writeLine(text) {
    const nextText = String(text || '')
    if (nextText === lastStatusText) return
    lastStatusText = nextText

    if (!output?.isTTY) {
      baseLogger.log?.(nextText)
      return
    }

    readline.clearLine(output, 0)
    readline.cursorTo(output, 0)
    output.write(nextText)
    active = true
  }

  function finish(text = '') {
    const nextText = String(text || '')
    if (!active) {
      if (nextText && nextText !== lastStatusText) baseLogger.log?.(nextText)
      lastStatusText = null
      return
    }

    if (nextText) writeLine(nextText)
    output.write('\n')
    active = false
    lastStatusText = null
  }

  function log(...args) {
    if (active && output?.isTTY) {
      output.write('\n')
      active = false
    }
    lastStatusText = null
    baseLogger.log?.(...args)
  }

  function warn(...args) {
    log(...args)
  }

  return {
    ...baseLogger,
    log,
    warn,
    status: writeLine,
    statusDone: finish,
  }
}

module.exports = {
  createStatusLineLogger,
}

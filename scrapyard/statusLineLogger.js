'use strict'

const readline = require('readline')

function createStatusLineLogger(baseLogger = console) {
  let active = false
  const output = process.stdout

  function writeLine(text) {
    if (!output?.isTTY) {
      baseLogger.log?.(text)
      return
    }

    readline.clearLine(output, 0)
    readline.cursorTo(output, 0)
    output.write(String(text || ''))
    active = true
  }

  function finish(text = '') {
    if (!active) {
      if (text) baseLogger.log?.(text)
      return
    }

    if (text) writeLine(text)
    output.write('\n')
    active = false
  }

  function log(...args) {
    if (active && output?.isTTY) {
      output.write('\n')
      active = false
    }
    baseLogger.log?.(...args)
  }

  return {
    ...baseLogger,
    log,
    status: writeLine,
    statusDone: finish,
  }
}

module.exports = {
  createStatusLineLogger,
}

'use strict'

const { runSourceBatch, withCliScrapeLock } = require('./scraperRunner')

withCliScrapeLock('source batch', () => runSourceBatch(process.argv.slice(2)))
  .then((code) => {
    process.exitCode = code
  })
  .catch((err) => {
    console.error(`Source batch failed: ${err.stack || err.message}`)
    process.exitCode = 1
  })

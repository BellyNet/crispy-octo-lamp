'use strict'

const { runStufferDbBatch } = require('../scrapyard/scraperRunner')

runStufferDbBatch(process.argv.slice(2))
  .then((code) => {
    process.exitCode = code
  })
  .catch((err) => {
    console.error(`StufferDB update failed: ${err.stack || err.message}`)
    process.exitCode = 1
  })

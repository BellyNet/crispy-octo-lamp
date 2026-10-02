'use strict'

const { runStufferDbBatch, withCliScrapeLock } = require('./scraperRunner')

withCliScrapeLock('npm run update:stufferdb', () =>
  runStufferDbBatch(process.argv.slice(2))
)
  .then((code) => {
    process.exitCode = code
  })
  .catch((err) => {
    console.error(`StufferDB update failed: ${err.stack || err.message}`)
    process.exitCode = 1
  })

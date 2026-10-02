'use strict'

const { runAllSourceUpdates, withCliScrapeLock } = require('./scraperRunner')

withCliScrapeLock('npm run update:all-models', () =>
  runAllSourceUpdates(process.argv.slice(2))
)
  .then((code) => {
    process.exitCode = code
  })
  .catch((err) => {
    console.error(`All-source update failed: ${err.stack || err.message}`)
    process.exitCode = 1
  })

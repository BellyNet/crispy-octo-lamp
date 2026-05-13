'use strict'

const { runAllSourceUpdates } = require('./scraperRunner')

runAllSourceUpdates(process.argv.slice(2))
  .then((code) => {
    process.exitCode = code
  })
  .catch((err) => {
    console.error(`All-source update failed: ${err.stack || err.message}`)
    process.exitCode = 1
  })

'use strict'

const {
  parseRunnerArgs,
  runSourceBatch,
} = require('../scrapyard/scraperRunner')

runSourceBatch('coomer', {
  ...parseRunnerArgs(process.argv.slice(2)),
  'host-contains': 'coomerfans.com',
})
  .then((code) => {
    process.exitCode = code
  })
  .catch((err) => {
    console.error(`CoomerFans batch failed: ${err.stack || err.message}`)
    process.exitCode = 1
  })

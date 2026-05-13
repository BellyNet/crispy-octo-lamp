'use strict'

const { runScraperCli } = require('./scraperRunner')

runScraperCli()
  .then((code) => {
    process.exitCode = code
  })
  .catch((err) => {
    console.error(`Scraper runner failed: ${err.stack || err.message}`)
    process.exitCode = 1
  })

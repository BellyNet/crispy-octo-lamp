'use strict'

// Runs one queued scrape task (one source of one model) the same way the
// all-source update does, and writes the result for the worker to record.
//
//   node scrapyard/runQueuedTask.js --model=<name> --url=<source url> --result=<file>

const fs = require('fs')
const minimist = require('minimist')

const { parseRunnerArgs, runAllSourceModelUpdate } = require('./scraperRunner')
const { parseSourceUrl, findSourceForParsed } = require('./sourceRouter')

async function main() {
  const argv = minimist(process.argv.slice(2), {
    string: ['model', 'url', 'result'],
  })
  if (!argv.model || !argv.url) {
    console.error(
      'Usage: runQueuedTask.js --model=<name> --url=<url> [--result=<file>]'
    )
    return 2
  }

  const parsed = parseSourceUrl(argv.url)
  const definition = findSourceForParsed(parsed)
  const result = await runAllSourceModelUpdate(
    {
      model: argv.model,
      sources: [
        {
          sourceKey: definition?.registryKey || parsed?.sourceType || 'unknown',
          url: argv.url,
          label: definition?.runLabel || parsed?.sourceType || 'source',
        },
      ],
    },
    // Same defaults as the nightly all-source update.
    { argv: parseRunnerArgs(['--auto-inactivate-never-saved-reddit']) }
  )

  if (argv.result) fs.writeFileSync(argv.result, JSON.stringify(result))
  const run = result.runs[0]
  return run?.ok ? 0 : run?.code || 1
}

main()
  .then((code) => {
    process.exitCode = code
  })
  .catch((err) => {
    console.error(`Queued task failed: ${err.stack || err.message}`)
    process.exitCode = 1
  })

'use strict'

const assert = require('assert')

const {
  buildRepairArgs,
  buildScraperOptions,
  buildSyncArgs,
  runScrape,
  runScraperCli,
} = require('./scraperRunner')
const { parseSourceUrl } = require('./sourceRouter')

async function withConsoleSilenced(callback) {
  const originalLog = console.log
  try {
    console.log = () => {}
    return await callback()
  } finally {
    console.log = originalLog
  }
}

async function assertRouted(url, expected) {
  const parsed = parseSourceUrl(url)
  assert(parsed, `Expected ${url} to parse`)
  for (const [key, value] of Object.entries(expected)) {
    assert.strictEqual(parsed[key], value, `${url} ${key}`)
  }

  let command = null
  const status = await runScrape(
    url,
    { model: 'test_model', 'skip-nas-sync': true },
    {
      log: () => {},
      error: (message) => {
        throw new Error(message)
      },
      runCommand: (scriptPath, args) => {
        command = { scriptPath, args }
        return 0
      },
    }
  )
  assert.strictEqual(status, 0)
  assert(command, `Expected ${url} to build a command`)
  assert(command.args.includes('--model'), `${url} missing --model`)
  assert(command.args.includes('test_model'), `${url} missing model value`)
  return parsed
}

async function main() {
  const reddit = await assertRouted(
    'https://www.reddit.com/user/abigailgray256/submitted/',
    {
      scraper: 'hoghaul',
      sourceType: 'reddit',
      rawName: 'abigailgray256',
    }
  )
  await assertRouted('https://coomerfans.com/u/onlyfans/123/name_here', {
    scraper: 'hoghaul',
    sourceType: 'coomerfans',
    rawName: 'name_here',
  })
  await assertRouted('https://coomerfans.com/?q=name_here', {
    scraper: 'hoghaul',
    sourceType: 'coomerfans',
    rawName: 'name_here',
  })
  await assertRouted('https://coomer.su/onlyfans/user/name_here', {
    scraper: 'hoghaul',
    sourceType: 'coomer',
    rawName: 'name_here',
  })
  await assertRouted('https://kemono.su/patreon/user/12345', {
    scraper: 'hoghaul',
    sourceType: 'kemono',
    rawName: '12345',
  })
  await assertRouted('https://stufferdb.com/index?/category/2333', {
    scraper: 'milkmaid',
    sourceType: 'stufferdb',
    rawName: null,
  })

  const redditOptions = buildScraperOptions(reddit, {
    model: 'abigailgray256',
    'dry-run': true,
    'skip-nas-sync': true,
    'browser-media': false,
    pages: '1',
    'max-posts': '2',
  })
  assert.strictEqual(redditOptions.model, 'abigailgray256')
  assert.strictEqual(redditOptions.dryRun, true)
  assert.strictEqual(redditOptions.skipNasSync, true)
  assert.strictEqual(redditOptions.useBrowserMedia, false)
  assert.strictEqual(redditOptions.pages, '1')
  assert.strictEqual(redditOptions.maxPosts, '2')

  assert.deepStrictEqual(
    buildRepairArgs({
      model: 'abc',
      scrape: true,
      'skip-nas-sync': true,
    }),
    ['--model', 'abc', '--scrape', '--skip-nas-sync']
  )
  assert.deepStrictEqual(buildSyncArgs({ push: true, 'cleanup-mp4': 'true' }), [
    '--push',
    '--cleanup-mp4',
    'true',
  ])

  const badUpdateStatus = await withConsoleSilenced(() =>
    runScraperCli(['update', 'bogus'], {
      log: () => {},
      error: () => {},
    })
  )
  assert.strictEqual(badUpdateStatus, 1)

  console.log('Unified scraper smoke passed.')
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err.stack || err.message)
    process.exitCode = 1
  })
}

module.exports = {
  main,
}

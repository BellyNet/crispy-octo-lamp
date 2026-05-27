'use strict'

const assert = require('assert')
const fs = require('fs')
const os = require('os')
const path = require('path')

const {
  applyScrapePositionalFallback,
  buildAllSourceQueue,
  buildRepairArgs,
  buildScraperOptions,
  buildSyncArgs,
  runScrape,
  runScraperCli,
} = require('./scraperRunner')
const { parseSourceUrl } = require('./sourceRouter')
const {
  getMediaEntrySeenDetails,
  getMediaEntryUrls,
  isLikelyMediaUrl,
  normalizeMediaEntry,
} = require('./mediaEntries')
const {
  getStufferDbFallbackUrls,
  normalizeStufferDbCategoryUrl,
  normalizeStufferDbPictureUrl,
} = require('./sourceAdapters/stufferdb')
const { fetchCoomerFansPosts } = require('./sourceAdapters/coomerFans')
const { fetchCoomerKemonoPosts } = require('./sourceAdapters/coomerKemono')
const { registerParsedSourceForModel } = require('./run-scrape-interactive')

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
  const stufferAi = await assertRouted(
    'https://stufferai.com/picture?/659098/category/8586',
    {
      scraper: 'milkmaid',
      sourceType: 'stufferdb',
      rawName: null,
    }
  )
  assert.strictEqual(
    stufferAi.url,
    'https://stufferdb.com/picture?/659098/category/8586'
  )
  assert.deepStrictEqual(
    getStufferDbFallbackUrls('https://stufferai.com/index?/category/8586'),
    [
      'https://stufferdb.com/index?/category/8586',
      'https://stufferai.com/index?/category/8586',
    ]
  )
  assert.strictEqual(
    normalizeStufferDbPictureUrl(
      'https://stufferai.com/index?/picture?/659098/category/8586&amp;slideshow='
    ),
    'https://stufferdb.com/picture?/659098/category/8586'
  )
  assert.strictEqual(
    normalizeStufferDbCategoryUrl(
      'https://stufferai.com/index?/category/8586&acs=123'
    ),
    'https://stufferdb.com/index?/category/8586'
  )

  const stufferEntry = normalizeMediaEntry({
    filename: '20260517215133-b0e8b25b-la.jpg',
    mediaUrl:
      'https://cdn.stufferdb.com/_data/i/upload/2026/05/17/20260517215133-b0e8b25b-la.jpg',
    mediaUrls: [
      'https://cdn.stufferdb.com/_data/i/upload/2026/05/17/20260517215133-b0e8b25b-la.jpg',
    ],
    sourceUrls: ['https://stufferdb.com/index?/category/8586'],
    mediaPageUrl: 'https://stufferdb.com/picture?/659098/category/8586',
  })
  assert.strictEqual(
    isLikelyMediaUrl('https://stufferdb.com/index?/category/8586'),
    false
  )
  assert.deepStrictEqual(getMediaEntryUrls(stufferEntry), [
    'https://cdn.stufferdb.com/_data/i/upload/2026/05/17/20260517215133-b0e8b25b-la.jpg',
  ])
  assert.deepStrictEqual(getMediaEntrySeenDetails(stufferEntry).mediaUrls, [
    'https://cdn.stufferdb.com/_data/i/upload/2026/05/17/20260517215133-b0e8b25b-la.jpg',
  ])

  const allSourceQueue = buildAllSourceQueue({
    beta_model: {
      sources: {
        stufferdb: [{ url: 'https://stufferdb.com/index?/category/2' }],
      },
    },
    alpha_model: {
      sources: {
        coomer: [{ url: 'https://coomerfans.com/u/onlyfans/123/alpha_model' }],
        reddit: [{ url: 'https://www.reddit.com/user/alpha_model/submitted/' }],
        kemono: [{ url: 'https://kemono.su/patreon/user/456' }],
        stufferdb: [{ url: 'https://stufferdb.com/index?/category/1' }],
      },
    },
  })
  assert.deepStrictEqual(
    allSourceQueue.map((item) => item.model),
    ['alpha_model', 'beta_model']
  )
  assert.deepStrictEqual(
    allSourceQueue[0].sources.map((source) => source.label),
    ['reddit', 'kemono', 'coomerfans', 'stufferdb']
  )

  const tempRegistryDir = fs.mkdtempSync(
    path.join(os.tmpdir(), 'scrape-registry-')
  )
  const tempRegistryPath = path.join(tempRegistryDir, 'model_aliases.json')
  fs.writeFileSync(
    tempRegistryPath,
    JSON.stringify({ known_model: { aliases: ['known_model'], sources: {} } }),
    'utf8'
  )
  registerParsedSourceForModel(
    parseSourceUrl('https://coomerfans.com/u/onlyfans/123/name_here'),
    'known_model',
    tempRegistryPath
  )
  registerParsedSourceForModel(
    parseSourceUrl('https://stufferai.com/index?/category/8586'),
    'known_model',
    tempRegistryPath
  )
  const tempRegistry = JSON.parse(fs.readFileSync(tempRegistryPath, 'utf8'))
  assert.strictEqual(
    tempRegistry.known_model.sources.coomer[0].url,
    'https://coomerfans.com/u/onlyfans/123/name_here'
  )
  assert.strictEqual(
    tempRegistry.known_model.sources.stufferdb[0].url,
    'https://stufferdb.com/index?/category/8586'
  )

  const coomerFansStatuses = []
  await fetchCoomerFansPosts(
    {
      origin: 'https://coomerfans.com',
      site: 'coomerfans',
      service: 'onlyfans',
      userId: '123',
      rawName: 'name_here',
    },
    {},
    {
      fetchHtml: async (url) => {
        if (url.includes('/p/1/123/onlyfans')) {
          return {
            html: 'Added 2026-05-01 00:00:00 +0000 UTC https://img1.coomerfans.com/storage/a/b/one.mp4',
          }
        }
        if (url.includes('/p/2/123/onlyfans')) {
          return {
            html: 'Added 2026-05-02 00:00:00 +0000 UTC https://img1.coomerfans.com/storage/a/b/two.jpg',
          }
        }
        if (url.includes('page=2')) return { html: '' }
        return {
          html: '<a href="/p/1/123/onlyfans">one</a><a href="/p/2/123/onlyfans">two</a>',
        }
      },
      logger: {
        status: (line) => coomerFansStatuses.push(line),
        statusDone: (line) => coomerFansStatuses.push(line),
      },
    }
  )
  assert(
    coomerFansStatuses.some((line) =>
      String(line).includes(
        'Fetching coomerfans pages: 1 page(s), 2 post(s), 2 media'
      )
    )
  )

  const coomerStatuses = []
  await fetchCoomerKemonoPosts(
    {
      origin: 'https://coomer.su',
      site: 'coomer',
      service: 'onlyfans',
      userId: '123',
    },
    {},
    {
      fetchJson: async (url) => ({
        data: url.includes('o=0') ? [{ id: '1', file: null }] : [],
      }),
      logger: {
        status: (line) => coomerStatuses.push(line),
        statusDone: (line) => coomerStatuses.push(line),
      },
      pageSize: 1,
    }
  )
  assert(
    coomerStatuses.some((line) =>
      String(line).includes('Fetching coomer pages: 1 page(s), 1 post(s)')
    )
  )

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

  const fallbackArgs = applyScrapePositionalFallback(reddit.url, {
    _: [reddit.url, 'abigailgray256', '1', '5'],
    model: 'true',
    pages: 'true',
    'max-posts': 'true',
    'dry-run': true,
    'skip-nas-sync': true,
  })
  assert.strictEqual(fallbackArgs.model, 'abigailgray256')
  assert.strictEqual(fallbackArgs.pages, '1')
  assert.strictEqual(fallbackArgs['max-posts'], '5')

  let fallbackCommand = null
  const fallbackStatus = await runScrape(reddit.url, fallbackArgs, {
    log: () => {},
    error: (message) => {
      throw new Error(message)
    },
    runCommand: (scriptPath, args) => {
      fallbackCommand = { scriptPath, args }
      return 0
    },
  })
  assert.strictEqual(fallbackStatus, 0)
  assert.deepStrictEqual(fallbackCommand.args, [
    reddit.url,
    '--model',
    'abigailgray256',
    '--skip-nas-sync',
    '--pages',
    '1',
    '--max-posts',
    '5',
    '--dry-run',
  ])

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

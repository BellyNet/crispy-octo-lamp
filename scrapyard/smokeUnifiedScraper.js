'use strict'

const assert = require('assert')
const fs = require('fs')
const os = require('os')
const path = require('path')

const { collectOversizedVideoTargets } = require('./run-scrape-interactive')
const {
  applyScrapePositionalFallback,
  buildAllSourceQueue,
  buildAllSourceRunOptions,
  buildRepairArgs,
  buildScraperOptions,
  buildSyncArgs,
  getTemporarilyDisabledSourceReason,
  isSuccessfulRunStatus,
  runScrape,
  runAllSourceModelUpdate,
  runScraperCli,
} = require('./scraperRunner')
const { parseSourceUrl } = require('./sourceRouter')
const {
  backfillSeenSourcePostsFromRunEvents,
  createBoundaryPageFilter,
  getSourceCheckpoint,
  loadConfirmedSourceFrontier,
  recordCompletedSourcePosts,
  recordSeenSourcePosts,
  recordSourceCheckpoint,
} = require('./sourceFrontier')
const {
  createIncrementalSourceState,
  getRedditSourceStatePath,
} = require('./redditSourceState')
const {
  getMediaEntrySeenDetails,
  getMediaEntrySourceDetails,
  getMediaEntryUrls,
  isLikelyMediaUrl,
  normalizeMediaEntry,
} = require('./mediaEntries')
const mediaDates = require('./mediaDates')
const { createMediaSeenIndex } = require('./mediaSeenIndex')
const { evictVerifiedLocalMp4s, syncModelMetadataToNas } = require('./nasSync')
const {
  buildStufferDbMediaEntry,
  getStufferDbFallbackUrls,
  getStufferDbRedditSubredditFromMediaUrl,
  normalizeStufferDbCategoryUrl,
  normalizeStufferDbPictureUrl,
  withStufferDbNewestFirst,
} = require('./sourceAdapters/stufferdb')
const {
  fetchCoomerFansPosts,
  parseCoomerFansCaption,
} = require('./sourceAdapters/coomerFans')
const {
  fetchCoomerKemonoPosts,
  getMediaEntriesFromPost,
} = require('./sourceAdapters/coomerKemono')
const {
  extractTitleFromOldRedditPostHtml,
  fetchRedditPosts,
  getRedditPostTitle,
} = require('./sourceAdapters/reddit')
const {
  buildRedditSourceMeta,
  buildSeenIndexByRelativePath,
  buildStufferSourceMeta,
  getModelProfileSource,
  sourceMetaFromSeenRecord,
} = require('./legacySourceBackfill')
const { registerParsedSourceForModel } = require('./run-scrape-interactive')
const {
  getActiveStufferDbCheckpoint,
  getPermanentLazyVideoFailure,
} = require('../milkmaid/milkmaid')
const runLifecycle = require('./runLifecycle')
const {
  collectPawchivePreviewUpgrades,
} = require('./reportPawchivePreviewUpgrades')
const {
  normalizeSeenUrl: normalizeHoghaulSeenUrl,
} = require('../hoghaul/hoghaul')

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
  assert.deepStrictEqual(
    buildStufferSourceMeta('20230329200009-5564aa10-la.jpg'),
    {
      site: 'stufferdb',
      originalName: '20230329200009-5564aa10-la.jpg',
      mediaUrl:
        'https://cdn.stufferdb.com/_data/i/upload/2023/03/29/20230329200009-5564aa10-la.jpg',
    }
  )
  assert.deepStrictEqual(
    buildStufferSourceMeta('20211214175048-d185061d.mp4'),
    {
      site: 'stufferdb',
      originalName: '20211214175048-d185061d.mp4',
      mediaUrl:
        'https://stufferai.com/upload/2021/12/14/20211214175048-d185061d.mp4',
    }
  )
  assert.deepStrictEqual(
    getActiveStufferDbCheckpoint(
      {
        id: '660723',
        mediaPageUrl:
          'https://stufferdb.com/index?/picture?/660723/category/16772&acs=1',
      },
      false
    ),
    {
      id: '660723',
      mediaPageUrl: 'https://stufferdb.com/picture?/660723/category/16772',
    }
  )
  assert.strictEqual(
    getActiveStufferDbCheckpoint(
      {
        id: '660723',
        mediaPageUrl: 'https://stufferdb.com/picture?/660723/category/16772',
      },
      true
    ),
    null
  )
  assert.deepStrictEqual(
    buildRedditSourceMeta('StuffersNSFW_abigailgray256_1spqp2x_1-la.png'),
    {
      site: 'reddit',
      postId: '1spqp2x',
      mediaPageUrl: 'https://www.reddit.com/comments/1spqp2x/',
    }
  )
  assert.deepStrictEqual(buildRedditSourceMeta('bbwgonewild_1tmaqav.mp4'), {
    site: 'reddit',
    postId: '1tmaqav',
    mediaPageUrl: 'https://www.reddit.com/comments/1tmaqav/',
  })
  assert.strictEqual(
    getStufferDbRedditSubredditFromMediaUrl(
      'https://cdn.stufferdb.com/_data/i/galleries/Reddit/BBW_sfw/sample.jpg'
    ),
    'BBW_sfw'
  )
  assert.strictEqual(
    getStufferDbRedditSubredditFromMediaUrl(
      'https://cdn.stufferdb.com/_data/i/upload/2024/10/29/sample.jpg'
    ),
    null
  )
  assert.strictEqual(
    buildStufferDbMediaEntry(
      {
        url: 'https://stufferdb.com/index?/category/16772',
        modelName: 'sample_model',
      },
      'https://stufferdb.com/picture?/660723/category/16772',
      {
        mediaUrl:
          'https://cdn.stufferdb.com/_data/i/galleries/Reddit/BBW_sfw/sample.jpg',
        filename: 'sample.jpg',
      }
    ).sourceSubreddit,
    'bbw_sfw'
  )
  const legacySeenByPath = buildSeenIndexByRelativePath(
    {
      mediaPageUrls: {
        page: {
          relativePath: 'sample_model/images/a.jpg',
          mediaPageUrl: 'https://www.reddit.com/comments/abc123/',
          sourceSite: 'reddit',
          postId: 'abc123',
          title: 'Recovered title',
        },
      },
      mediaUrls: {
        media: {
          relativePath: 'sample_model/images/a.jpg',
          mediaUrl: 'https://i.redd.it/a.jpg',
        },
      },
    },
    'sample_model'
  )
  assert.deepStrictEqual(
    sourceMetaFromSeenRecord(legacySeenByPath.get('images/a.jpg')),
    {
      site: 'reddit',
      service: null,
      userId: null,
      username: null,
      subreddit: null,
      postId: 'abc123',
      title: 'Recovered title',
      text: null,
      originalName: null,
      mediaPageUrl: 'https://www.reddit.com/comments/abc123/',
      mediaUrl: 'https://i.redd.it/a.jpg',
    }
  )
  assert.deepStrictEqual(
    getModelProfileSource(
      {
        sources: {
          coomer: [
            {
              url: 'https://coomerfans.com/u/onlyfans/123/sample_model',
              service: 'onlyfans',
              userId: '123',
              discoveredAs: 'sample_model',
            },
          ],
        },
      },
      '2892ac-01976408-260d-7799-a25f-f3ad52122664.jpg'
    ),
    {
      site: 'coomerfans',
      service: 'onlyfans',
      userId: '123',
      username: 'sample_model',
      mediaPageUrl: 'https://coomerfans.com/u/onlyfans/123/sample_model',
    }
  )

  assert.strictEqual(
    getRedditPostTitle({
      title: '<b>Tom &amp;amp; Jerry</b>\u0007',
    }),
    'Tom & Jerry'
  )
  assert.strictEqual(
    getRedditPostTitle({
      permalink: '/r/test/comments/abc123/a%20title_%26_more/',
    }),
    'a title & more'
  )
  assert.strictEqual(
    extractTitleFromOldRedditPostHtml('<title>Welcome to Reddit</title>'),
    null
  )

  assert.deepStrictEqual(
    getMediaEntrySourceDetails({
      sourceSite: 'reddit',
      postId: 'abc123',
      title: 'Saved post title',
      originalName: 'original-file',
      mediaQuality: null,
      needsFullResolution: null,
      fullResolutionStatus: null,
      fullResolutionUrl: null,
      fullResolutionResolvedPath: null,
    }),
    {
      sourceSite: 'reddit',
      sourceService: null,
      sourceUserId: null,
      sourceUsername: null,
      sourceSubreddit: null,
      postId: 'abc123',
      title: 'Saved post title',
      text: null,
      originalName: 'original-file',
    }
  )

  const metadataSyncRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), 'metadata-sync-smoke-')
  )
  const metadataLocalRoot = path.join(metadataSyncRoot, 'local')
  const metadataNasRoot = path.join(metadataSyncRoot, 'nas')
  const metadataModelDir = path.join(metadataLocalRoot, 'sample_model')
  fs.mkdirSync(metadataModelDir, { recursive: true })
  fs.writeFileSync(
    path.join(metadataModelDir, '.media-dates.json'),
    JSON.stringify({
      __version: 4,
      'images/a.jpg': { source: { title: 'Shortened caption...' } },
      'images/b.jpg': { source: { title: null } },
    })
  )
  assert.strictEqual(
    mediaDates.recordExistingMetadata(
      metadataModelDir,
      'images/a.jpg',
      '2026-05-01T00:00:00.000Z',
      {
        sourceSite: 'coomerfans',
        title: 'Full caption and details',
        text: 'Full body text for dashboard',
        mediaUrl: 'https://img1.coomerfans.com/storage/a/b/one.jpg',
        mediaQuality: 'pawchive_preview',
        needsFullResolution: false,
        fullResolutionStatus: 'resolved_existing_visual_match',
        fullResolutionResolvedPath: 'sample_model/images/full.jpg',
      },
      {
        comments: [
          {
            author: 'Pawchive Patron',
            posted: '2026-05-01T01:00:00.000Z',
            text: 'Complete archived comment',
          },
        ],
        commentCount: 1,
      }
    ),
    true
  )
  assert.strictEqual(
    mediaDates.recordExistingMetadata(
      metadataModelDir,
      'images/b.jpg',
      '2026-05-02T00:00:00.000Z',
      {
        sourceSite: 'reddit',
        mediaPageUrl:
          'https://www.reddit.com/r/Feedism/comments/abc123/full_reddit_title_here/',
        mediaUrl: 'https://i.redd.it/full-title.jpg',
      }
    ),
    true
  )
  mediaDates.flushAllSidecars()
  assert.deepStrictEqual(
    syncModelMetadataToNas({
      modelName: 'sample_model',
      datasetDir: metadataLocalRoot,
      nasDatasetDir: metadataNasRoot,
    }),
    { copied: 1, skipped: 0 }
  )
  const syncedRecord = JSON.parse(
    fs.readFileSync(
      path.join(metadataNasRoot, 'sample_model', '.media-dates.json'),
      'utf8'
    )
  )['images/a.jpg']
  const syncedMetadata = syncedRecord.source
  const syncedRedditMetadata = JSON.parse(
    fs.readFileSync(
      path.join(metadataNasRoot, 'sample_model', '.media-dates.json'),
      'utf8'
    )
  )['images/b.jpg'].source
  assert.strictEqual(syncedMetadata.title, 'Full caption and details')
  assert.strictEqual(syncedMetadata.text, 'Full body text for dashboard')
  assert.strictEqual(syncedRedditMetadata.title, 'full reddit title here')
  assert.strictEqual(syncedRedditMetadata.text, 'full reddit title here')
  assert.strictEqual(syncedMetadata.needsFullResolution, false)
  assert.strictEqual(
    syncedMetadata.fullResolutionStatus,
    'resolved_existing_visual_match'
  )
  assert.strictEqual(
    syncedMetadata.fullResolutionResolvedPath,
    'sample_model/images/full.jpg'
  )
  assert.deepStrictEqual(syncedRecord.comments, [
    {
      author: 'Pawchive Patron',
      posted: '2026-05-01T01:00:00.000Z',
      text: 'Complete archived comment',
    },
  ])
  assert.strictEqual(syncedRecord.commentCount, 1)
  assert.deepStrictEqual(
    collectPawchivePreviewUpgrades({ datasetDir: metadataLocalRoot }),
    [
      {
        model: 'sample_model',
        relativePath: 'sample_model/images/a.jpg',
        status: 'resolved_existing_visual_match',
        needsFullResolution: false,
        mediaPageUrl: null,
        previewUrl: 'https://img1.coomerfans.com/storage/a/b/one.jpg',
        fullResolutionUrl: null,
        resolvedPath: 'sample_model/images/full.jpg',
      },
    ]
  )

  const mp4CleanupRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), 'nas-mp4-cleanup-smoke-')
  )
  const mp4LocalRoot = path.join(mp4CleanupRoot, 'local')
  const mp4NasRoot = path.join(mp4CleanupRoot, 'nas')
  const mp4ModelName = 'sample_model'
  const mp4LocalVideoDir = path.join(mp4LocalRoot, mp4ModelName, 'webm')
  const mp4NasVideoDir = path.join(mp4NasRoot, mp4ModelName, 'webm')
  fs.mkdirSync(mp4LocalVideoDir, { recursive: true })
  fs.mkdirSync(mp4NasVideoDir, { recursive: true })
  fs.writeFileSync(path.join(mp4LocalVideoDir, 'verified.mp4'), 'verified')
  fs.writeFileSync(path.join(mp4NasVideoDir, 'verified.mp4'), 'verified')
  fs.writeFileSync(path.join(mp4LocalVideoDir, 'tolerated.mp4'), 'local')
  fs.writeFileSync(
    path.join(mp4NasVideoDir, 'tolerated.mp4'),
    'local plus a few bytes'
  )
  fs.writeFileSync(path.join(mp4LocalVideoDir, 'mismatch.mp4'), 'local')
  fs.writeFileSync(
    path.join(mp4NasVideoDir, 'mismatch.mp4'),
    'different'.repeat(16)
  )
  fs.writeFileSync(path.join(mp4LocalVideoDir, 'missing.mp4'), 'missing')
  fs.writeFileSync(path.join(mp4LocalVideoDir, 'verified.webm'), 'webm')
  fs.writeFileSync(path.join(mp4NasVideoDir, 'verified.webm'), 'webm')
  const mp4Cleanup = evictVerifiedLocalMp4s({
    modelName: mp4ModelName,
    datasetDir: mp4LocalRoot,
    nasDatasetDir: mp4NasRoot,
  })
  assert.strictEqual(mp4Cleanup.scannedFiles, 5)
  assert.strictEqual(mp4Cleanup.verifiedFiles, 3)
  assert.strictEqual(mp4Cleanup.deletedFiles, 2)
  assert.strictEqual(mp4Cleanup.missingOnNas, 1)
  assert.strictEqual(mp4Cleanup.sizeMismatches, 1)
  assert.strictEqual(
    fs.existsSync(path.join(mp4LocalVideoDir, 'verified.mp4')),
    false
  )
  assert.strictEqual(
    fs.existsSync(path.join(mp4LocalVideoDir, 'tolerated.mp4')),
    false
  )
  assert.strictEqual(
    fs.existsSync(path.join(mp4LocalVideoDir, 'mismatch.mp4')),
    true
  )
  assert.strictEqual(
    fs.existsSync(path.join(mp4LocalVideoDir, 'missing.mp4')),
    true
  )
  assert.strictEqual(
    fs.existsSync(path.join(mp4LocalVideoDir, 'verified.webm')),
    true
  )
  const nasMp4Index = JSON.parse(
    fs.readFileSync(path.join(mp4LocalRoot, 'nas-mp4-index.v1.json'), 'utf8')
  )
  assert.deepStrictEqual(nasMp4Index.entries, [
    'sample_model/webm/tolerated.mp4',
    'sample_model/webm/verified.mp4',
    'sample_model/webm/verified.webm',
  ])
  assert.strictEqual(
    fs.existsSync(path.join(mp4NasRoot, 'nas-mp4-index.v1.json')),
    true
  )
  fs.rmSync(mp4CleanupRoot, { recursive: true, force: true })

  const metadataLogDir = path.join(metadataModelDir, 'log')
  fs.mkdirSync(metadataLogDir, { recursive: true })
  const seenIndex = createMediaSeenIndex({
    datasetDir: metadataLocalRoot,
    existsLocallyOrOnNas: () => true,
  })
  const seenDetails = {
    relativePath: 'sample_model/images/a.jpg',
    mediaUrl: 'https://img1.coomerfans.com/storage/a/b/one.jpg',
    sourceSite: 'coomerfans',
    title: 'Shortened caption...',
  }
  seenIndex.recordSuccessfulSeenMedia(metadataLogDir, seenDetails)
  seenIndex.recordSuccessfulSeenMedia(metadataLogDir, {
    ...seenDetails,
    title: 'Full caption and details',
  })
  assert.strictEqual(
    seenIndex.loadMediaSeenIndex(metadataLogDir).mediaUrls[seenDetails.mediaUrl]
      .title,
    'Full caption and details'
  )
  const sourceSeenIndex = createMediaSeenIndex({
    datasetDir: metadataLocalRoot,
    existsLocallyOrOnNas: () => true,
    normalizeUrl: normalizeHoghaulSeenUrl,
  })
  const pawchiveSeenDetails = {
    relativePath: 'sample_model/images/pawchive.jpg',
    mediaUrl:
      'https://img.pawchive.st/data/a/b/one.jpg?f=one.jpg&token=discard',
    mediaPageUrl: 'https://pawchive.st/patreon/user/24586027/post/full-test',
    sourceSite: 'kemono',
    sourceService: 'patreon',
    sourceUserId: '24586027',
  }
  sourceSeenIndex.recordSuccessfulSeenMedia(metadataLogDir, pawchiveSeenDetails)
  const pawchiveSeenMatch = sourceSeenIndex.getSuccessfulSeenMediaMatch(
    metadataLogDir,
    null,
    'https://img.pawchive.st/data/a/b/one.jpg?different=1'
  )
  assert.strictEqual(
    pawchiveSeenMatch?.relativePath,
    pawchiveSeenDetails.relativePath
  )
  const kemonoSeenIndex = createMediaSeenIndex({
    datasetDir: metadataLocalRoot,
    existsLocallyOrOnNas: () => true,
    normalizeUrl: normalizeHoghaulSeenUrl,
  })
  const kemonoSeenDetails = {
    relativePath: 'sample_model/images/kemono.jpg',
    mediaUrl: 'https://kemono.su/data/a/b/two.jpg?f=two.jpg&token=old',
    sourceSite: 'kemono',
  }
  kemonoSeenIndex.recordSuccessfulSeenMedia(metadataLogDir, kemonoSeenDetails)
  const kemonoSeenMatch = kemonoSeenIndex.getSuccessfulSeenMediaMatch(
    metadataLogDir,
    null,
    'https://kemono.cr/data/a/b/two.jpg?f=two.jpg&token=new'
  )
  assert.strictEqual(
    kemonoSeenMatch?.relativePath,
    kemonoSeenDetails.relativePath
  )
  fs.rmSync(metadataSyncRoot, { recursive: true, force: true })

  assert.deepStrictEqual(getPermanentLazyVideoFailure('HTTP 404'), {
    reason: 'upstream_media_missing',
    note: 'Upstream media returned HTTP 404; skipping future reruns unless manually cleared.',
    preservePartial: false,
  })
  assert.deepStrictEqual(getPermanentLazyVideoFailure('HTTP 410'), {
    reason: 'upstream_media_missing',
    note: 'Upstream media returned HTTP 410; skipping future reruns unless manually cleared.',
    preservePartial: false,
  })
  assert.strictEqual(getPermanentLazyVideoFailure('HTTP 429'), null)
  assert.strictEqual(
    getPermanentLazyVideoFailure('No lazy download progress for 90000ms'),
    null
  )

  const reddit = await assertRouted(
    'https://www.reddit.com/user/abigailgray256/submitted/',
    {
      scraper: 'hoghaul',
      sourceType: 'reddit',
      rawName: 'abigailgray256',
    }
  )
  assert.strictEqual(isSuccessfulRunStatus('no_new_posts'), true)
  assert.strictEqual(isSuccessfulRunStatus('failed'), false)
  const redditBatchOptions = buildAllSourceRunOptions({}, 'test_model', reddit)
  assert.strictEqual(redditBatchOptions['skip-nas-sync'], true)
  let batchSourceRuns = 0
  let batchSyncRuns = 0
  const batchResult = await withConsoleSilenced(() =>
    runAllSourceModelUpdate(
      {
        model: 'test_model',
        sources: [
          {
            sourceKey: 'reddit',
            label: 'reddit:first',
            url: 'https://www.reddit.com/user/first/submitted/',
          },
          {
            sourceKey: 'stufferdb',
            label: 'stufferdb:second',
            url: 'https://stufferdb.com/category/second',
          },
        ],
      },
      {
        argv: {},
        stopOnError: false,
        runSource: async (_url, options) => {
          batchSourceRuns += 1
          assert.strictEqual(options['skip-nas-sync'], true)
          return 0
        },
        syncModel: async ({ modelName }) => {
          batchSyncRuns += 1
          assert.strictEqual(modelName, 'test_model')
          return {
            ok: true,
            code: 1,
            cleanup: {
              deletedFiles: 2,
              deletedBytes: 2048,
            },
          }
        },
        datasetPaths: {
          datasetDir: 'local-dataset',
          nasDatasetDir: 'nas-dataset',
        },
      }
    )
  )
  assert.strictEqual(batchSourceRuns, 2)
  assert.strictEqual(batchSyncRuns, 1)
  assert.strictEqual(batchResult.nasSync.ok, true)
  assert.strictEqual(batchResult.nasSync.cleanup.deletedFiles, 2)
  const failedBatchSyncResult = await withConsoleSilenced(() =>
    runAllSourceModelUpdate(
      {
        model: 'test_model',
        sources: [
          {
            sourceKey: 'reddit',
            label: 'reddit:first',
            url: 'https://www.reddit.com/user/first/submitted/',
          },
        ],
      },
      {
        argv: {},
        stopOnError: false,
        runSource: async () => 0,
        syncModel: async () => {
          throw new Error('NAS unavailable')
        },
        error: () => {},
        datasetPaths: {
          datasetDir: 'local-dataset',
          nasDatasetDir: 'nas-dataset',
        },
      }
    )
  )
  assert.strictEqual(failedBatchSyncResult.nasSync.ok, false)
  assert.strictEqual(failedBatchSyncResult.nasSync.error, 'NAS unavailable')
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
  const pawchive = await assertRouted('https://kemono.su/patreon/user/12345', {
    scraper: 'hoghaul',
    sourceType: 'kemono',
    rawName: '12345',
    origin: 'https://pawchive.st',
    url: 'https://pawchive.st/patreon/user/12345',
  })
  assert.strictEqual(getTemporarilyDisabledSourceReason(pawchive), null)
  await assertRouted('https://pawchive.st/patreon/user/24586027', {
    scraper: 'hoghaul',
    sourceType: 'kemono',
    rawName: '24586027',
    origin: 'https://pawchive.st',
    url: 'https://pawchive.st/patreon/user/24586027',
  })
  const stufferdb = await assertRouted(
    'https://stufferdb.com/index?/category/2333',
    {
      scraper: 'milkmaid',
      sourceType: 'stufferdb',
      rawName: null,
    }
  )
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
        kemono: [{ url: 'https://pawchive.st/patreon/user/456' }],
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
    ['reddit', 'pawchive', 'coomerfans', 'stufferdb']
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
  const coomerFansPosts = await fetchCoomerFansPosts(
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
            html: [
              '<meta property="og:title" content="name_here / Shortened caption..." />',
              '<meta property="og:description" content="name_here - Full fallback caption" />',
              '<div class="post-wrap">',
              '<h1>Shortened caption...</h1>',
              '<span class="post-date">Added 2026-05-01 00:00:00 +0000 UTC</span>',
              '<p>Full caption &amp; details</p>',
              '<div class="post-body">https://img1.coomerfans.com/storage/a/b/one.mp4</div>',
              '</div>',
            ].join(''),
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
  assert.strictEqual(
    coomerFansPosts[0].mediaEntries[0].title,
    'Full caption & details'
  )
  assert.strictEqual(
    coomerFansPosts[0].mediaEntries[0].text,
    'Full caption & details'
  )
  assert.strictEqual(
    parseCoomerFansCaption(
      [
        '<meta property="og:title" content="abigailgray256 / A little teaser from a set I never released! Tip $5 to see t.." />',
        '<div class="post-wrap">',
        '<h1>A little teaser from a set I never released! Tip $5 to see t..</h1>',
        '<p>A little teaser from a set I never released! Tip $5 to see the rest in your inbox! 😜</p>',
        '<div class="post-body"><img src="https://img1.coomerfans.com/storage/a/b/one.jpg"></div>',
        '</div>',
      ].join(''),
      'abigailgray256'
    ),
    'A little teaser from a set I never released! Tip $5 to see the rest in your inbox! 😜'
  )

  const coomerMediaEntries = getMediaEntriesFromPost(
    {
      origin: 'https://coomer.su',
      site: 'coomer',
      service: 'onlyfans',
      userId: '123',
    },
    {
      id: 'caption-test',
      title: 'Short title',
      content: '<p>Full caption &amp; details</p>',
      file: { path: '/a/b/one.jpg' },
    }
  )
  assert.strictEqual(
    coomerMediaEntries[0].title,
    'Short title - Full caption & details'
  )
  assert.strictEqual(coomerMediaEntries[0].text, 'Full caption & details')
  const pawchivePreviewEntries = getMediaEntriesFromPost(
    {
      origin: 'https://pawchive.st',
      site: 'kemono',
      service: 'patreon',
      userId: '24586027',
    },
    {
      id: 'preview-test',
      title: 'Pawchive preview',
      file: { path: '/a/b/one.jpg' },
      has_full: false,
    }
  )
  assert.strictEqual(
    pawchivePreviewEntries[0].mediaUrl,
    'https://img.pawchive.st/thumbnail/data/a/b/one.jpg'
  )
  assert.strictEqual(
    pawchivePreviewEntries[0].filename,
    'one.pawchive-preview.jpg'
  )
  assert.strictEqual(pawchivePreviewEntries[0].mediaQuality, 'pawchive_preview')
  assert.strictEqual(pawchivePreviewEntries[0].needsFullResolution, true)
  assert.strictEqual(pawchivePreviewEntries[0].fullResolutionStatus, 'pending')
  assert.strictEqual(
    pawchivePreviewEntries[0].fullResolutionUrl,
    'https://img.pawchive.st/data/a/b/one.jpg'
  )
  const pawchiveMediaEntries = getMediaEntriesFromPost(
    {
      origin: 'https://pawchive.st',
      site: 'kemono',
      service: 'patreon',
      userId: '24586027',
    },
    {
      id: 'full-test',
      title: 'Pawchive full asset',
      file: { path: '/a/b/one.jpg' },
      has_full: true,
    }
  )
  assert.strictEqual(
    pawchiveMediaEntries[0].mediaUrl,
    'https://img.pawchive.st/data/a/b/one.jpg'
  )
  assert.strictEqual(
    pawchiveMediaEntries[0].mediaPageUrl,
    'https://pawchive.st/patreon/user/24586027/post/full-test'
  )
  assert(
    pawchiveMediaEntries[0].mediaPageUrls.includes(
      'https://kemono.cr/patreon/user/24586027/post/full-test'
    ),
    'expected Pawchive entries to include legacy Kemono page aliases'
  )
  assert(
    pawchiveMediaEntries[0].mediaUrls.includes(
      'https://kemono.cr/data/a/b/one.jpg'
    ),
    'expected Pawchive entries to include legacy Kemono media aliases'
  )
  assert.strictEqual(pawchiveMediaEntries[0].filename, 'one.jpg')
  assert.strictEqual(pawchiveMediaEntries[0].mediaQuality, 'full')
  assert.strictEqual(pawchiveMediaEntries[0].needsFullResolution, false)

  const pawchiveLinkedEntries = getMediaEntriesFromPost(
    {
      origin: 'https://pawchive.st',
      site: 'kemono',
      service: 'patreon',
      userId: '24586027',
    },
    {
      id: '152002320',
      title: 'What Working Out Looks Like Now',
      content:
        '<p>Full post caption &amp; details</p><p><a href="https://www.dropbox.com/scl/fi/link/JigglyWorkOut.mov?rlkey=key&amp;dl=0">Download</a></p>',
      embed: {
        url: 'https://www.dropbox.com/scl/fi/link/JigglyWorkOut.mov?rlkey=key&dl=0',
        subject: 'JigglyWorkOut.mov',
      },
      published: '2026-03-02T01:16:52',
      pageMeta: {
        comments: [
          {
            author: 'Patron',
            posted: '2026-03-02T02:00:00',
            text: 'Full comment text',
          },
        ],
        commentCount: 1,
      },
    }
  )
  assert.strictEqual(pawchiveLinkedEntries.length, 1)
  assert.strictEqual(
    pawchiveLinkedEntries[0].filename,
    '152002320-JigglyWorkOut.mov'
  )
  assert.strictEqual(
    pawchiveLinkedEntries[0].mediaUrl,
    'https://dl.dropboxusercontent.com/scl/fi/link/JigglyWorkOut.mov?rlkey=key&dl=1'
  )
  assert.deepStrictEqual(pawchiveLinkedEntries[0].mediaUrls, [
    'https://dl.dropboxusercontent.com/scl/fi/link/JigglyWorkOut.mov?rlkey=key&dl=1',
    'https://www.dropbox.com/scl/fi/link/JigglyWorkOut.mov?rlkey=key&dl=0',
  ])
  assert.strictEqual(
    pawchiveLinkedEntries[0].title,
    'What Working Out Looks Like Now - Full post caption & details'
  )
  assert.strictEqual(
    pawchiveLinkedEntries[0].text,
    'Full post caption & details'
  )
  assert.strictEqual(pawchiveLinkedEntries[0].mediaQuality, 'external_full')
  assert.deepStrictEqual(pawchiveLinkedEntries[0].pageMeta.comments[0], {
    author: 'Patron',
    posted: '2026-03-02T02:00:00',
    text: 'Full comment text',
  })

  const pawchiveMetadataStatuses = []
  const enrichedPawchivePosts = await fetchCoomerKemonoPosts(
    {
      origin: 'https://pawchive.st',
      site: 'kemono',
      service: 'patreon',
      userId: '24586027',
    },
    { maxPosts: 1, postConcurrency: 2 },
    {
      fetchJson: async (url) => {
        if (url.endsWith('/comments')) {
          return {
            data: [
              {
                commenter_name: 'Pawchive Patron',
                published: '2026-03-02T02:00:00',
                content: '<p>Complete comment &amp; reaction</p>',
              },
            ],
          }
        }
        return {
          data: [
            {
              id: '152002320',
              title: 'Full Pawchive title',
              content: '<p>Full Pawchive caption</p>',
              detail_fetched: true,
              file: null,
            },
          ],
        }
      },
      logger: {
        status: (line) => pawchiveMetadataStatuses.push(line),
        statusDone: (line) => pawchiveMetadataStatuses.push(line),
        log: (line) => pawchiveMetadataStatuses.push(line),
      },
      pageSize: 50,
    }
  )
  assert.strictEqual(enrichedPawchivePosts[0].mediaEntries.length, 0)
  assert.strictEqual(
    enrichedPawchivePosts[0].title,
    'Full Pawchive title - Full Pawchive caption'
  )
  assert.strictEqual(enrichedPawchivePosts[0].text, 'Full Pawchive caption')
  assert.deepStrictEqual(enrichedPawchivePosts[0].pageMeta, {
    comments: [
      {
        author: 'Pawchive Patron',
        posted: '2026-03-02T02:00:00',
        text: 'Complete comment & reaction',
      },
    ],
    commentCount: 1,
  })
  assert(
    pawchiveMetadataStatuses.some((line) =>
      String(line).includes('Fetching kemono metadata: 1/1 post(s)')
    )
  )

  const coomerStatuses = []
  const coomerPosts = await fetchCoomerKemonoPosts(
    {
      origin: 'https://coomer.su',
      site: 'coomer',
      service: 'onlyfans',
      userId: '123',
    },
    {},
    {
      fetchJson: async (url) => {
        if (url.includes('/post/1/comments')) return { data: [] }
        if (url.includes('/post/1')) {
          return {
            data: {
              id: '1',
              title: 'Full detail title',
              content: '<p>Full detail body</p>',
              file: { path: '/a/b/full.jpg' },
            },
          }
        }
        return {
          data: url.includes('o=0')
            ? [{ id: '1', title: 'Listing title', file: null }]
            : [],
        }
      },
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
  assert.strictEqual(
    coomerPosts[0].title,
    'Full detail title - Full detail body'
  )
  assert.strictEqual(coomerPosts[0].text, 'Full detail body')
  assert.strictEqual(
    coomerPosts[0].mediaEntries[0].title,
    'Full detail title - Full detail body'
  )
  assert.strictEqual(coomerPosts[0].mediaEntries[0].text, 'Full detail body')

  const redditDiscoveryEvents = []
  const redditListingPages = []
  let redditFetchCount = 0
  const emptyRedditPosts = await fetchRedditPosts(
    {
      origin: 'https://www.reddit.com',
      site: 'reddit',
      service: 'submitted',
      userId: 'empty_user',
      username: 'empty_user',
    },
    {},
    {
      fetchHtml: async (url) => {
        redditFetchCount += 1
        return {
          html: url.includes('.rss') ? '<feed></feed>' : '<html></html>',
          byteLength: 13,
          statusCode: 200,
          url,
        }
      },
      redgifsClient: {
        parseRedgifsId: () => null,
      },
      redditHtmlDelayMs: 0,
      redditHtmlMaxRetries: 0,
      appendRunEvent: (type, payload) =>
        redditDiscoveryEvents.push({ type, ...payload }),
      onListingPage: (details) => redditListingPages.push(details),
      logger: { log: () => {}, warn: () => {} },
    }
  )
  assert.deepStrictEqual(emptyRedditPosts, [])
  assert.strictEqual(redditFetchCount, 2)
  assert.deepStrictEqual(
    redditListingPages.map((page) => page.mode),
    ['old_html', 'rss']
  )
  assert(
    redditDiscoveryEvents.some(
      (event) =>
        event.type === 'reddit_discovery_fallback' &&
        event.reason === 'empty_listing'
    )
  )
  assert(
    redditDiscoveryEvents.some(
      (event) =>
        event.type === 'reddit_html_throttle_configured' &&
        event.delayMs === 0 &&
        event.scope === 'listing_and_gallery'
    )
  )

  const redditGalleryEvents = []
  const redditGalleryProgress = []
  const redditGalleryStatuses = []
  const redditGalleryPosts = await fetchRedditPosts(
    {
      origin: 'https://www.reddit.com',
      site: 'reddit',
      service: 'submitted',
      userId: 'gallery_user',
      username: 'gallery_user',
    },
    { endPage: 0 },
    {
      fetchHtml: async (url) => {
        if (/\/comments\/gallery1/i.test(url)) {
          return {
            html: '<img src="https://i.redd.it/gallery-image.jpg">',
            byteLength: 50,
            statusCode: 200,
            url,
          }
        }
        return {
          html: [
            '<div class="thing" data-fullname="t3_gallery1"',
            ' data-permalink="/r/test/comments/gallery1/title/"',
            ' data-url="https://www.reddit.com/gallery/gallery1"',
            ' data-is-gallery="true" data-timestamp="1710000000000"',
            ' data-subreddit="test"></div>',
          ].join(''),
          byteLength: 250,
          statusCode: 200,
          url,
        }
      },
      redgifsClient: {
        parseRedgifsId: () => null,
      },
      redditHtmlDelayMs: 0,
      redditHtmlMaxRetries: 0,
      appendRunEvent: (type, payload) =>
        redditGalleryEvents.push({ type, ...payload }),
      onDiscoveryProgress: (details) => redditGalleryProgress.push(details),
      logger: {
        log: () => {},
        warn: () => {},
        status: (line) => redditGalleryStatuses.push(line),
      },
    }
  )
  assert.strictEqual(redditGalleryPosts.length, 1)
  assert.strictEqual(redditGalleryPosts[0].title, 'title')
  assert.strictEqual(redditGalleryPosts[0].text, 'title')
  assert.strictEqual(redditGalleryPosts[0].mediaEntries.length, 1)
  assert.strictEqual(redditGalleryPosts[0].mediaEntries[0].text, 'title')
  assert(
    redditGalleryEvents.some(
      (event) =>
        event.type === 'reddit_gallery_hydration_started' &&
        event.galleryCount === 1
    )
  )
  assert(
    redditGalleryEvents.some(
      (event) =>
        event.type === 'reddit_html_request_started' &&
        event.requestKind === 'gallery/post'
    )
  )
  assert(
    redditGalleryEvents.some(
      (event) =>
        event.type === 'reddit_gallery_hydration_post_finished' &&
        event.mediaUrlCount === 1
    )
  )
  assert.strictEqual(redditGalleryProgress.at(-1).current, 1)
  assert(
    redditGalleryStatuses.some((line) =>
      String(line).includes('Resolving Reddit gallery 1/1')
    )
  )

  const redditTitleHydrationEvents = []
  const redditHydratedTitlePosts = await fetchRedditPosts(
    {
      origin: 'https://www.reddit.com',
      site: 'reddit',
      service: 'submitted',
      userId: 'missing_title_user',
      username: 'missing_title_user',
    },
    { endPage: 0 },
    {
      fetchHtml: async (url) => {
        if (/\/comments\/missing_title\//i.test(url)) {
          return {
            html: '<a class="title may-blank">Recovered Reddit title</a>',
            byteLength: 55,
            statusCode: 200,
            url,
          }
        }
        return {
          html: [
            '<div class="thing" data-fullname="t3_missing_title"',
            ' data-permalink="/r/test/comments/missing_title/_/"',
            ' data-url="https://i.redd.it/missing-title.jpg"',
            ' data-timestamp="1710000000000"',
            ' data-subreddit="test"></div>',
          ].join(''),
          byteLength: 230,
          statusCode: 200,
          url,
        }
      },
      redgifsClient: {
        parseRedgifsId: () => null,
      },
      redditHtmlDelayMs: 0,
      redditHtmlMaxRetries: 0,
      appendRunEvent: (type, payload) =>
        redditTitleHydrationEvents.push({ type, ...payload }),
      logger: { log: () => {}, warn: () => {}, status: () => {} },
    }
  )
  assert.strictEqual(redditHydratedTitlePosts.length, 1)
  assert.strictEqual(
    redditHydratedTitlePosts[0].title,
    'Recovered Reddit title'
  )
  assert.strictEqual(redditHydratedTitlePosts[0].text, 'Recovered Reddit title')
  assert.strictEqual(
    redditHydratedTitlePosts[0].mediaEntries[0].title,
    'Recovered Reddit title'
  )
  assert(
    redditTitleHydrationEvents.some(
      (event) =>
        event.type === 'reddit_title_hydration_finished' && event.foundTitle
    )
  )

  const redditPermalinkTitleHydrationEvents = []
  const redditPermalinkTitlePosts = await fetchRedditPosts(
    {
      origin: 'https://www.reddit.com',
      site: 'reddit',
      service: 'submitted',
      userId: 'permalink_title_user',
      username: 'permalink_title_user',
    },
    { endPage: 0 },
    {
      fetchHtml: async (url) => {
        if (/\/comments\/slug_title\//i.test(url)) {
          return {
            html: [
              '<div class="thing" data-fullname="t3_slug_title"',
              ' data-title="This is the full Reddit post title, not just the URL slug"></div>',
            ].join(''),
            byteLength: 80,
            statusCode: 200,
            url,
          }
        }
        return {
          html: [
            '<div class="thing" data-fullname="t3_slug_title"',
            ' data-permalink="/r/test/comments/slug_title/this_is_the_url_slug/"',
            ' data-url="https://i.redd.it/slug-title.jpg"',
            ' data-timestamp="1710000000000"',
            ' data-subreddit="test"></div>',
          ].join(''),
          byteLength: 230,
          statusCode: 200,
          url,
        }
      },
      redgifsClient: {
        parseRedgifsId: () => null,
      },
      redditHtmlDelayMs: 0,
      redditHtmlMaxRetries: 0,
      appendRunEvent: (type, payload) =>
        redditPermalinkTitleHydrationEvents.push({ type, ...payload }),
      logger: { log: () => {}, warn: () => {}, status: () => {} },
    }
  )
  assert.strictEqual(redditPermalinkTitlePosts.length, 1)
  assert.strictEqual(
    redditPermalinkTitlePosts[0].title,
    'This is the full Reddit post title, not just the URL slug'
  )
  assert.strictEqual(
    redditPermalinkTitlePosts[0].mediaEntries[0].title,
    'This is the full Reddit post title, not just the URL slug'
  )
  assert(
    redditPermalinkTitleHydrationEvents.some(
      (event) =>
        event.type === 'reddit_title_hydration_finished' && event.foundTitle
    )
  )

  const redditAccessEvents = []
  let redditAccessFetchCount = 0
  await fetchRedditPosts(
    {
      origin: 'https://www.reddit.com',
      site: 'reddit',
      service: 'submitted',
      userId: 'blocked_user',
      username: 'blocked_user',
    },
    {},
    {
      fetchHtml: async (url) => {
        redditAccessFetchCount += 1
        if (!url.includes('.rss')) throw new Error('HTTP 403: blocked')
        return {
          html: '<feed></feed>',
          byteLength: 13,
          statusCode: 200,
          url,
        }
      },
      redgifsClient: {},
      redditHtmlDelayMs: 0,
      redditHtmlMaxRetries: 0,
      appendRunEvent: (type, payload) =>
        redditAccessEvents.push({ type, ...payload }),
      logger: { log: () => {}, warn: () => {} },
    }
  )
  assert.strictEqual(redditAccessFetchCount, 2)
  assert(
    redditAccessEvents.some(
      (event) =>
        event.type === 'reddit_discovery_fallback' &&
        event.reason === 'access_denied'
    )
  )

  const lifecycleRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), 'run-lifecycle-smoke-')
  )
  const lifecycleFolders = {
    base: path.join(lifecycleRoot, 'sample_model'),
    logDir: path.join(lifecycleRoot, 'sample_model', 'log'),
  }
  fs.mkdirSync(lifecycleFolders.logDir, { recursive: true })
  const lifecycleLog = runLifecycle.createRunLog({
    source: 'hoghaul',
    modelName: 'sample_model',
    inputUrl: 'https://www.reddit.com/user/sample_model/submitted/',
    folders: lifecycleFolders,
    keepHistory: true,
  })
  runLifecycle.finalizeRunLog(lifecycleLog, {
    status: 'no_new_posts',
    reason: 'noNewPosts',
  })
  const lifecycleSummary = JSON.parse(
    fs.readFileSync(lifecycleLog.summaryPath, 'utf8')
  )
  assert.strictEqual(lifecycleSummary.status, 'no_new_posts')

  const redditOptions = buildScraperOptions(reddit, {
    model: 'abigailgray256',
    'dry-run': true,
    'skip-nas-sync': true,
    'browser-media': false,
    'download-oversized': true,
    'full-source-refresh': true,
    'source-incremental-overlap-pages': '2',
    pages: '1',
    'max-posts': '2',
  })
  assert.strictEqual(redditOptions.model, 'abigailgray256')
  assert.strictEqual(redditOptions.dryRun, true)
  assert.strictEqual(redditOptions.skipNasSync, true)
  assert.strictEqual(redditOptions.useBrowserMedia, false)
  assert.strictEqual(redditOptions.downloadOversized, true)
  assert.strictEqual(redditOptions.fullSourceRefresh, true)
  assert.strictEqual(redditOptions.sourceIncrementalOverlapPages, '2')
  assert.strictEqual(redditOptions.pages, '1')
  assert.strictEqual(redditOptions.maxPosts, '2')

  const stufferOptions = buildScraperOptions(stufferdb, {
    model: 'sample_model',
    'full-source-refresh': true,
    'source-incremental-overlap-pages': '3',
  })
  assert.strictEqual(stufferOptions.fullSourceRefresh, true)
  assert.strictEqual(stufferOptions.sourceIncrementalOverlapPages, '3')
  assert.strictEqual(
    withStufferDbNewestFirst(
      'https://stufferdb.com/index?/category/2333&image_order=2'
    ),
    'https://stufferdb.com/index?/category/2333&image_order=5'
  )
  assert.strictEqual(
    withStufferDbNewestFirst(
      'https://stufferdb.com/index?/category/2333/start-150&image_order=5'
    ),
    'https://stufferdb.com/index?/category/2333/start-150'
  )

  const fallbackArgs = applyScrapePositionalFallback(reddit.url, {
    _: [reddit.url, 'abigailgray256', '1', '5'],
    model: 'true',
    pages: 'true',
    'max-posts': 'true',
    'dry-run': true,
    'skip-nas-sync': true,
    'download-oversized': true,
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
    '--download-oversized',
  ])

  const tempDataset = fs.mkdtempSync(
    path.join(os.tmpdir(), 'hoghaul-oversized-smoke-')
  )
  const tempLogDir = path.join(tempDataset, 'sample_model', 'log')
  fs.mkdirSync(tempLogDir, { recursive: true })
  fs.writeFileSync(
    path.join(tempLogDir, 'hoghaul-run-2026-01-01T00-00-00-000Z.jsonl'),
    [
      JSON.stringify({
        at: '2026-01-01T00:00:00.000Z',
        type: 'run_started',
        modelName: 'sample_model',
        inputUrl: 'https://coomerfans.com/u/onlyfans/123/sample_model',
      }),
      JSON.stringify({
        at: '2026-01-01T00:01:00.000Z',
        type: 'skip_oversized_video',
        modelName: 'sample_model',
        filename: 'huge.mp4',
        contentLength: 2317869251,
      }),
    ].join('\n') + '\n'
  )
  const oversizedTargets = collectOversizedVideoTargets({
    datasetDir: tempDataset,
    models: 'sample_model',
  })
  assert.strictEqual(oversizedTargets.length, 1)
  assert.strictEqual(oversizedTargets[0].modelName, 'sample_model')
  assert.strictEqual(oversizedTargets[0].count, 1)
  assert.strictEqual(oversizedTargets[0].largestBytes, 2317869251)

  const frontierDataset = fs.mkdtempSync(
    path.join(os.tmpdir(), 'source-frontier-smoke-')
  )
  const frontierModelDir = path.join(frontierDataset, 'sample_model')
  const frontierLogDir = path.join(frontierModelDir, 'log')
  const frontierImagePath = path.join(frontierModelDir, 'images', 'known.jpg')
  fs.mkdirSync(path.dirname(frontierImagePath), { recursive: true })
  fs.mkdirSync(frontierLogDir, { recursive: true })
  fs.writeFileSync(frontierImagePath, 'known')
  fs.writeFileSync(
    path.join(frontierLogDir, 'milkmaid-seen-media-index.json'),
    JSON.stringify({
      mediaUrls: {},
      mediaPageUrls: {
        'https://coomerfans.com/p/123/456/onlyfans': {
          relativePath: 'sample_model/images/known.jpg',
          sourceSite: 'coomerfans',
          sourceService: 'onlyfans',
          sourceUserId: '456',
          sourceUsername: 'sample_model',
          postId: '123',
        },
      },
    })
  )
  const frontierSource = {
    site: 'coomerfans',
    service: 'onlyfans',
    userId: '456',
    rawName: 'sample_model',
  }
  recordCompletedSourcePosts(frontierLogDir, frontierSource, ['123'])
  const frontier = loadConfirmedSourceFrontier(frontierLogDir, frontierSource, {
    datasetPaths: {
      toDatasetAbsolutePath: (relativePath) =>
        path.join(frontierDataset, relativePath),
      existsLocallyOrOnNas: fs.existsSync,
    },
  })
  assert.strictEqual(frontier.knownPostCount, 1)
  const pageFilter = createBoundaryPageFilter(frontier, { overlapPages: 1 })
  assert.deepStrictEqual(pageFilter.filterPage([{ id: '123' }]).items, [])
  assert.strictEqual(
    pageFilter.filterPage([{ id: 'older' }]).stopAfterPage,
    true
  )

  const pawchiveSeenSource = {
    origin: 'https://pawchive.st',
    site: 'kemono',
    service: 'patreon',
    userId: '11678891',
    rawName: '11678891',
  }
  recordSeenSourcePosts(frontierLogDir, pawchiveSeenSource, ['26720458'])
  const pawchiveSeenFrontier = loadConfirmedSourceFrontier(
    frontierLogDir,
    pawchiveSeenSource,
    {
      datasetPaths: {
        toDatasetAbsolutePath: (relativePath) =>
          path.join(frontierDataset, relativePath),
        existsLocallyOrOnNas: fs.existsSync,
      },
    }
  )
  assert.strictEqual(pawchiveSeenFrontier.knownPostCount, 1)
  assert.strictEqual(pawchiveSeenFrontier.skippablePostCount, 1)
  assert.deepStrictEqual(
    createBoundaryPageFilter(pawchiveSeenFrontier).filterPage([
      { id: '26720458' },
    ]).items,
    []
  )

  const pawchiveBackfillLogDir = path.join(frontierModelDir, 'pawchive-log')
  fs.mkdirSync(pawchiveBackfillLogDir, { recursive: true })
  fs.writeFileSync(
    path.join(pawchiveBackfillLogDir, 'hoghaul-run-2026-07-03.jsonl'),
    `${JSON.stringify({
      type: 'media_seen',
      sourceSite: 'kemono',
      sourceService: 'patreon',
      sourceUserId: '11678891',
      postId: '23020731',
      mediaPageUrl: 'https://kemono.cr/patreon/user/11678891/post/23020731',
    })}\n${JSON.stringify({
      type: 'dom_media_extracted',
      mediaPageUrl: 'https://pawchive.st/patreon/user/11678891/post/22976025',
    })}\n`
  )
  const pawchiveBackfill = backfillSeenSourcePostsFromRunEvents(
    pawchiveBackfillLogDir,
    pawchiveSeenSource
  )
  assert.strictEqual(pawchiveBackfill.addedPostCount, 2)
  const pawchiveBackfillFrontier = loadConfirmedSourceFrontier(
    pawchiveBackfillLogDir,
    pawchiveSeenSource
  )
  assert.strictEqual(pawchiveBackfillFrontier.skippablePostCount, 2)

  const redditStateDataset = fs.mkdtempSync(
    path.join(os.tmpdir(), 'reddit-state-smoke-')
  )
  const redditStateModelDir = path.join(redditStateDataset, 'sample_model')
  const redditStateLogDir = path.join(redditStateModelDir, 'log')
  fs.mkdirSync(path.join(redditStateModelDir, 'images'), { recursive: true })
  fs.mkdirSync(redditStateLogDir, { recursive: true })
  fs.writeFileSync(path.join(redditStateModelDir, 'images', 'old.jpg'), 'old')
  fs.writeFileSync(path.join(redditStateModelDir, 'images', 'new.jpg'), 'new')
  fs.writeFileSync(
    getRedditSourceStatePath(redditStateLogDir),
    JSON.stringify(
      {
        version: 1,
        updatedAt: '2026-06-01T00:00:00.000Z',
        sources: {
          'submitted/sample_model': {
            sourceSite: 'reddit',
            sourceService: 'submitted',
            sourceUserId: 'sample_model',
            sourceUsername: 'sample_model',
            sourceKey: 'submitted/sample_model',
            latestPostId: 'oldpost',
            latestCreatedUtc: 1769904000,
            posts: {
              oldpost: {
                postId: 'oldpost',
                createdUtc: 1769904000,
                relativePaths: ['sample_model/images/old.jpg'],
              },
            },
          },
        },
      },
      null,
      2
    )
  )
  fs.writeFileSync(
    path.join(redditStateLogDir, 'milkmaid-seen-media-index.json'),
    JSON.stringify({
      mediaUrls: {},
      mediaPageUrls: {
        'https://old.reddit.com/r/test/comments/newpost/new_title/': {
          relativePath: 'sample_model/images/new.jpg',
          sourceSite: 'reddit',
          sourceService: 'submitted',
          sourceUserId: 'sample_model',
          sourceUsername: 'sample_model',
          postId: 'newpost',
          uploadedDate: '2026-06-20T00:00:00.000Z',
          mediaPageUrl:
            'https://old.reddit.com/r/test/comments/newpost/new_title/',
          mediaUrl: 'https://i.redd.it/new.jpg',
        },
      },
    })
  )
  const redditStateContext = createIncrementalSourceState(
    redditStateLogDir,
    {
      site: 'reddit',
      service: 'submitted',
      userId: 'sample_model',
      username: 'sample_model',
    },
    {
      datasetPaths: {
        toDatasetAbsolutePath: (relativePath) =>
          path.join(redditStateDataset, relativePath),
        existsLocallyOrOnNas: fs.existsSync,
      },
    }
  )
  assert.strictEqual(
    redditStateContext.incrementalState.latestPostId,
    'newpost'
  )
  assert(
    redditStateContext.incrementalState.knownPostIds.has('newpost'),
    'expected Reddit incremental state to refresh from seen-media index'
  )

  const checkpointSource = {
    site: 'stufferdb',
    service: 'category',
    userId: '2333',
    rawName: 'sample_model',
  }
  recordSourceCheckpoint(frontierLogDir, checkpointSource, {
    id: '659098',
    mediaPageUrl: 'https://stufferdb.com/picture?/659098/category/2333',
  })
  recordCompletedSourcePosts(frontierLogDir, checkpointSource, ['659098'])
  assert.strictEqual(
    getSourceCheckpoint(frontierLogDir, checkpointSource).id,
    '659098'
  )

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

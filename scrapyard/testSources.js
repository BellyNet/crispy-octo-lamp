'use strict'

// Fixture test for scrapyard/sources.js: every source routes to the right
// definition, and preflight/fetchPosts hand each adapter the same arguments
// hoghaul's per-site branches used to.
const assert = require('assert')
const { parseSourceUrl, findSourceForParsed } = require('./sourceRouter')
const {
  SOURCES,
  REGISTRY_KEY_RUN_ORDER,
  findSourceForSite,
  getRegistryKeyForSite,
  describeSourceLink,
  listSourcesForClient,
} = require('./sources')
const { PAWCHIVE_ORIGIN } = require('./pawchive')

const SAMPLE_URLS = {
  stufferdb: 'https://stufferdb.com/index?/category/1234',
  coomerfans: 'https://coomerfans.com/u/onlyfans/333819/somebody',
  onlyhaven: 'https://cum.st/creators/onlyfans/12345',
  coomer: 'https://coomer.st/onlyfans/user/someone',
  kemono: 'https://pawchive.pw/patreon/user/123',
  reddit: 'https://www.reddit.com/user/someone/submitted/',
  tumblr: 'https://someblog.tumblr.com/',
}

// Every definition is reachable and has a sample here.
assert.deepStrictEqual(
  Object.keys(SAMPLE_URLS).sort(),
  SOURCES.map((source) => source.id).sort()
)
for (const source of SOURCES) {
  for (const field of ['id', 'label', 'registryKey', 'runLabel', 'letter']) {
    assert.ok(source[field], `${source.id} is missing ${field}`)
  }
  if (source.engine === 'hoghaul') {
    assert.strictEqual(typeof source.preflight, 'function', source.id)
    assert.strictEqual(typeof source.fetchPosts, 'function', source.id)
  }
}

for (const [id, url] of Object.entries(SAMPLE_URLS)) {
  const parsed = parseSourceUrl(url)
  assert.ok(parsed, `${url} should parse`)
  assert.strictEqual(findSourceForParsed(parsed).id, id, url)
}

assert.deepStrictEqual(REGISTRY_KEY_RUN_ORDER, [
  'reddit',
  'kemono',
  'coomer',
  'stufferdb',
  'tumblr',
])
assert.strictEqual(getRegistryKeyForSite('coomerfans'), 'coomer')
assert.strictEqual(getRegistryKeyForSite('kemono'), 'kemono')
assert.strictEqual(getRegistryKeyForSite(undefined), undefined)
assert.strictEqual(getRegistryKeyForSite('newsite'), 'newsite')
assert.strictEqual(
  findSourceForSite('coomerfans', ['https://cum.st/x']).id,
  'onlyhaven'
)
assert.strictEqual(
  findSourceForSite('coomerfans', ['https://coomerfans.com/x']).id,
  'coomerfans'
)
const clientList = listSourcesForClient()
assert.ok(clientList.sources.every((source) => source.id && source.label))
assert.deepStrictEqual(
  clientList.registryKeys.map(({ key, letter }) => `${key}:${letter}`),
  ['reddit:R', 'kemono:K', 'coomer:OF', 'stufferdb:S', 'tumblr:T']
)
assert.deepStrictEqual(
  [
    'https://www.reddit.com/user/abc/submitted/',
    'https://pawchive.pw/patreon/user/1',
    'https://cum.st/creators/onlyfans/12',
    'https://bubsxl.tumblr.com/',
    'https://example.com/x',
  ].map((url) => describeSourceLink(url).label),
  [
    'Reddit · u/abc',
    'Pawchive · Patreon',
    'OnlyHaven · OnlyFans',
    'Tumblr · bubsxl',
    'example.com',
  ]
)

// Stub every adapter function and record what each definition passes.
const calls = []
function stub(modulePath, names) {
  const mod = require(modulePath)
  for (const name of names) {
    mod[name] = (...args) => {
      calls.push({ name, args })
      return name
    }
  }
}
stub('./sourceAdapters/reddit', [
  'preflightRedditSource',
  'fetchRedditPosts',
  'fetchKnownRedditGalleryPosts',
])
stub('./sourceAdapters/coomerFans', [
  'preflightCoomerFansSource',
  'fetchCoomerFansPosts',
])
stub('./sourceAdapters/tumblr', ['preflightTumblrSource', 'fetchTumblrPosts'])
stub('./sourceAdapters/coomerKemono', [
  'preflightCoomerKemonoSource',
  'fetchCoomerKemonoPosts',
  'getMediaEntriesFromPost',
])

const ctx = {
  fetchHtml: 'ctx.fetchHtml',
  fetchJson: 'ctx.fetchJson',
  appendRunEvent: 'ctx.appendRunEvent',
  logger: 'ctx.logger',
  normalizeUrl: 'ctx.normalizeUrl',
  redgifsClient: 'ctx.redgifsClient',
}
const deps = {
  fullSourceRefresh: 'deps.fullSourceRefresh',
  sourceFrontier: 'deps.sourceFrontier',
  sourceIncrementalOverlapPages: 'deps.overlap',
}
const options = { postConcurrency: 9 }

function lastCall() {
  return calls[calls.length - 1]
}
function definitionFor(id) {
  return SOURCES.find((source) => source.id === id)
}
function sourceFor(id) {
  return parseSourceUrl(SAMPLE_URLS[id])
}

// Reddit
definitionFor('reddit').preflight(sourceFor('reddit'), 0, ctx)
assert.deepStrictEqual(lastCall().args[1], {
  fetchHtml: ctx.fetchHtml,
  fetchJson: ctx.fetchJson,
  pageSize: 100,
})
const redditDeps = {
  ...deps,
  fetchPostHtml: 'deps.fetchPostHtml',
  fetchPostText: 'deps.fetchPostText',
  fallbackDelayMs: 5,
  redditFullRefresh: true,
  redditIncrementalOverlapPosts: 3,
  redditSourceState: 'deps.redditSourceState',
  galleryCache: 'deps.galleryCache',
  onGalleryHydrated: 'deps.onGalleryHydrated',
  onDiscoveryProgress: 'deps.onDiscoveryProgress',
  onListingPage: 'deps.onListingPage',
}
const expectedRedditDeps = {
  fetchHtml: ctx.fetchHtml,
  fetchJson: ctx.fetchJson,
  fetchPostHtml: 'deps.fetchPostHtml',
  fetchPostText: 'deps.fetchPostText',
  fallbackDelayMs: 5,
  redditFullRefresh: true,
  redditIncrementalOverlapPosts: 3,
  redditSourceState: 'deps.redditSourceState',
  galleryCache: 'deps.galleryCache',
  onGalleryHydrated: 'deps.onGalleryHydrated',
  onDiscoveryProgress: 'deps.onDiscoveryProgress',
  onListingPage: 'deps.onListingPage',
  appendRunEvent: ctx.appendRunEvent,
  logger: ctx.logger,
  normalizeUrl: ctx.normalizeUrl,
  pageSize: 100,
  redgifsClient: ctx.redgifsClient,
  suppressIncrementalLog: true,
}
definitionFor('reddit').fetchPosts(
  sourceFor('reddit'),
  options,
  redditDeps,
  ctx
)
assert.strictEqual(lastCall().name, 'fetchRedditPosts')
assert.deepStrictEqual(lastCall().args[2], expectedRedditDeps)
definitionFor('reddit').fetchPosts(
  sourceFor('reddit'),
  options,
  { ...redditDeps, knownGalleryPosts: ['p1'] },
  ctx
)
assert.strictEqual(lastCall().name, 'fetchKnownRedditGalleryPosts')
assert.deepStrictEqual(lastCall().args[1], ['p1'])
assert.deepStrictEqual(lastCall().args[3], expectedRedditDeps)

// OnlyHaven / CoomerFans
for (const id of ['onlyhaven', 'coomerfans']) {
  definitionFor(id).preflight(sourceFor(id), 2, ctx)
  assert.strictEqual(lastCall().args[1], 2)
  assert.deepStrictEqual(lastCall().args[2], {
    fetchHtml: ctx.fetchHtml,
    fetchJson: ctx.fetchJson,
    logger: console,
  })
  definitionFor(id).fetchPosts(sourceFor(id), options, deps, ctx)
  assert.deepStrictEqual(lastCall().args[2], {
    fetchHtml: ctx.fetchHtml,
    fetchJson: ctx.fetchJson,
    fullSourceRefresh: deps.fullSourceRefresh,
    logger: ctx.logger,
    sourceFrontier: deps.sourceFrontier,
    sourceIncrementalOverlapPages: deps.sourceIncrementalOverlapPages,
  })
  definitionFor(id).fetchPosts(
    sourceFor(id),
    options,
    { ...deps, fetchHtml: 'deps.fetchHtml' },
    ctx
  )
  assert.strictEqual(lastCall().args[2].fetchHtml, 'deps.fetchHtml')
  assert.deepStrictEqual(definitionFor(id).defaults, {
    imageConcurrency: 3,
    postConcurrency: 8,
  })
}

// Tumblr
definitionFor('tumblr').preflight(sourceFor('tumblr'), 1, ctx)
assert.deepStrictEqual(lastCall().args[2], {
  fetchJson: ctx.fetchJson,
  pageSize: 50,
})
definitionFor('tumblr').fetchPosts(sourceFor('tumblr'), options, deps, ctx)
assert.deepStrictEqual(lastCall().args[2], {
  fetchJson: ctx.fetchJson,
  fullSourceRefresh: deps.fullSourceRefresh,
  logger: ctx.logger,
  sourceFrontier: deps.sourceFrontier,
  sourceIncrementalOverlapPages: deps.sourceIncrementalOverlapPages,
  pageSize: 50,
})

// Coomer and Pawchive: Pawchive pins post concurrency to 1.
for (const [id, postConcurrency] of [
  ['coomer', 9],
  ['kemono', 1],
]) {
  definitionFor(id).preflight(sourceFor(id), 3, ctx)
  assert.deepStrictEqual(lastCall().args[2], {
    fetchJson: ctx.fetchJson,
    pageSize: 50,
  })
  definitionFor(id).fetchPosts(sourceFor(id), options, deps, ctx)
  assert.deepStrictEqual(lastCall().args[2], {
    fetchJson: ctx.fetchJson,
    fullSourceRefresh: deps.fullSourceRefresh,
    logger: ctx.logger,
    normalizeUrl: ctx.normalizeUrl,
    pageSize: 50,
    postConcurrency,
    sourceFrontier: deps.sourceFrontier,
    sourceIncrementalOverlapPages: deps.sourceIncrementalOverlapPages,
  })
  definitionFor(id).mediaEntriesFromPost(sourceFor(id), { id: 'p' }, ctx)
  assert.strictEqual(lastCall().name, 'getMediaEntriesFromPost')
  assert.deepStrictEqual(lastCall().args[2], { normalizeUrl: ctx.normalizeUrl })
}
assert.strictEqual(sourceFor('kemono').origin, PAWCHIVE_ORIGIN)
assert.deepStrictEqual(definitionFor('kemono').defaults, { postConcurrency: 4 })
assert.strictEqual(definitionFor('coomer').defaults, undefined)

console.log('Source definitions fixture passed.')

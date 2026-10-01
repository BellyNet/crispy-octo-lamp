'use strict'

// The one list of scrape sources. Routing, the registry key a source is saved
// under, run order, labels, per-source defaults, browser policy and the
// adapter calls all come from here, so adding a source means writing its
// adapter (scrapyard/sourceAdapters/) and adding one entry below.
//
// Each entry:
//   id            stable identifier, also used for dashboard badges/filters
//   label         display name
//   site          the `site` value scrapers record in sidecars and frontier
//                 state. Persisted on disk, so never rename it; OnlyHaven and
//                 legacy CoomerFans share 'coomerfans' for that reason.
//   registryKey   model_aliases.json `sources.<key>` list this source lives in
//   runLabel      short name used in runner logs ("-- SOURCE 1/3: x -> label")
//   letter        short badge for the dashboard's per-registry-key source
//                 dots (sources sharing a registry key share a letter)
//   engine        'hoghaul' (unified post scraper) or 'milkmaid' (StufferDB)
//   matchesHost   (hostname) => boolean, checked in list order
//   parseUrl      (URL, hostname) => source fields; throws a descriptive
//                 Error when the URL is on this host but not a creator page
//   searchUrl     optional (name) => URL for a manual creator search
//   defaults      optional per-source concurrency defaults
//   useBrowserMedia  optional (source, requested, runOptions, env) => boolean
//   mediaEntriesFromPost  optional (source, post, ctx) => entries, for
//                 adapters whose posts don't carry `mediaEntries` already
//   preflight     (source, page, ctx) => report        [hoghaul engine]
//   fetchPosts    (source, options, deps, ctx) => posts [hoghaul engine]
//
// `ctx` is the scraper's shared toolkit (fetchHtml, fetchJson, logger,
// normalizeUrl, appendRunEvent, redgifsClient); `deps` carries per-run state.
// Adapters are required lazily so the dashboard can read labels without
// loading scraper code.

const {
  PAWCHIVE_ORIGIN,
  getPawchiveUserUrl,
  isPawchiveHost,
  isPawchiveOrKemonoHost,
} = require('./pawchive')

const TUMBLR_RESERVED_SUBDOMAINS = new Set([
  'www',
  'api',
  'assets',
  'embed',
  'engine',
  't',
  'render',
])

// modelRegistry requires this module, so pull sanitize in lazily.
function sanitize(value) {
  return require('./modelRegistry').sanitize(value)
}

function isTumblrHost(host) {
  return host === 'tumblr.com' || host.endsWith('.tumblr.com')
}

function isOnlyHavenHost(host) {
  return host === 'cum.st' || host.endsWith('.cum.st')
}

function isPawchiveOrigin(url) {
  try {
    return isPawchiveHost(new URL(url).hostname.toLowerCase())
  } catch {
    return false
  }
}

function pathParts(parsed) {
  return parsed.pathname.split('/').filter(Boolean)
}

// Coomer-style creator URLs: /<service>/user/<id>
function parseCreatorPath(parsed, site) {
  const parts = pathParts(parsed)
  const userIndex = parts.indexOf('user')
  const service = parts[0]
  const userId = userIndex >= 0 ? parts[userIndex + 1] : null

  if (!service || !userId) {
    throw new Error(
      'Expected a creator URL like /onlyfans/user/name or /patreon/user/id'
    )
  }

  return {
    inputUrl:
      site === 'kemono'
        ? getPawchiveUserUrl(service, userId)
        : parsed.toString(),
    origin: site === 'kemono' ? PAWCHIVE_ORIGIN : parsed.origin,
    site,
    service,
    userId,
    rawName: sanitize(userId),
  }
}

function coomerKemonoAdapter() {
  return require('./sourceAdapters/coomerKemono')
}

function coomerFansAdapter() {
  return require('./sourceAdapters/coomerFans')
}

const COOMER_KEMONO_PAGE_SIZE = 50

// Shared by Coomer and Pawchive, which use the same API shape.
const coomerKemonoRuntime = {
  mediaEntriesFromPost(source, post, ctx) {
    return coomerKemonoAdapter().getMediaEntriesFromPost(source, post, {
      normalizeUrl: ctx.normalizeUrl,
    })
  },
  preflight(source, page, ctx) {
    return coomerKemonoAdapter().preflightCoomerKemonoSource(source, page, {
      fetchJson: ctx.fetchJson,
      pageSize: COOMER_KEMONO_PAGE_SIZE,
    })
  },
  fetchPosts(source, options, deps, ctx) {
    return coomerKemonoAdapter().fetchCoomerKemonoPosts(source, options, {
      fetchJson: ctx.fetchJson,
      fullSourceRefresh: deps.fullSourceRefresh,
      logger: ctx.logger,
      normalizeUrl: ctx.normalizeUrl,
      pageSize: COOMER_KEMONO_PAGE_SIZE,
      postConcurrency: isPawchiveOrigin(source.origin)
        ? 1
        : options.postConcurrency,
      sourceFrontier: deps.sourceFrontier,
      sourceIncrementalOverlapPages: deps.sourceIncrementalOverlapPages,
    })
  },
}

// Shared by OnlyHaven and legacy CoomerFans (one adapter, site 'coomerfans').
const coomerFansRuntime = {
  defaults: { imageConcurrency: 3, postConcurrency: 8 },
  useBrowserMedia(source, requested) {
    return !coomerFansAdapter().isOnlyHavenSource(source) && Boolean(requested)
  },
  preflight(source, page, ctx) {
    return coomerFansAdapter().preflightCoomerFansSource(source, page, {
      fetchHtml: ctx.fetchHtml,
      fetchJson: ctx.fetchJson,
      logger: console,
    })
  },
  fetchPosts(source, options, deps, ctx) {
    return coomerFansAdapter().fetchCoomerFansPosts(source, options, {
      fetchHtml: deps.fetchHtml || ctx.fetchHtml,
      fetchJson: ctx.fetchJson,
      fullSourceRefresh: deps.fullSourceRefresh,
      logger: ctx.logger,
      sourceFrontier: deps.sourceFrontier,
      sourceIncrementalOverlapPages: deps.sourceIncrementalOverlapPages,
    })
  },
}

const REDDIT_PAGE_SIZE = 100
const TUMBLR_PAGE_SIZE = 50

const SOURCES = [
  {
    id: 'stufferdb',
    label: 'StufferDB',
    registryKey: 'stufferdb',
    runLabel: 'stufferdb',
    letter: 'S',
    engine: 'milkmaid',
    runOrder: 4,
    matchesHost: (host) =>
      host.includes('stufferdb') || host.includes('stufferai'),
    // StufferDB results carry no `site`; frontier keys and run summaries for
    // it were always written that way.
    parseUrl(parsed) {
      parsed.hostname = 'stufferdb.com'
      const normalized = parsed.toString()
      if (!/\/(?:category|picture)(?:[/?#]|$)/i.test(normalized)) return null
      return { sourceType: 'stufferdb', url: normalized, rawName: null }
    },
    searchUrl: (name) =>
      `https://stufferdb.com/search.php?q=${encodeURIComponent(name)}`,
  },
  {
    id: 'coomerfans',
    label: 'CoomerFans',
    site: 'coomerfans',
    registryKey: 'coomer',
    runLabel: 'coomerfans',
    letter: 'OF',
    engine: 'hoghaul',
    runOrder: 3,
    matchesHost: (host) => host.includes('coomerfans'),
    parseUrl(parsed) {
      const parts = pathParts(parsed)
      if (parts[0] === 'u' && parts[1] && parts[2] && parts[3]) {
        return {
          inputUrl: parsed.toString(),
          origin: parsed.origin,
          site: 'coomerfans',
          service: parts[1],
          userId: parts[2],
          rawName: sanitize(parts[3]),
        }
      }

      const queryName = parsed.searchParams.get('q')
      if (queryName) {
        return {
          inputUrl: parsed.toString(),
          origin: parsed.origin,
          site: 'coomerfans',
          service: 'onlyfans',
          userId: null,
          rawName: sanitize(queryName),
        }
      }

      throw new Error(
        'Expected a CoomerFans URL like /u/onlyfans/id/name or /?q=name'
      )
    },
    searchUrl: (name) =>
      `https://coomerfans.com/?q=${encodeURIComponent(name)}`,
    ...coomerFansRuntime,
  },
  {
    id: 'onlyhaven',
    label: 'OnlyHaven',
    site: 'coomerfans',
    registryKey: 'coomer',
    runLabel: 'coomerfans',
    letter: 'OF',
    engine: 'hoghaul',
    runOrder: 3,
    matchesHost: isOnlyHavenHost,
    parseUrl(parsed) {
      const parts = pathParts(parsed)
      if (parts[0] === 'creators' && parts[1] && parts[2]) {
        return {
          inputUrl: `${parsed.origin}/creators/${parts[1]}/${parts[2]}`,
          origin: parsed.origin,
          site: 'coomerfans',
          service: parts[1],
          userId: parts[2],
          rawName: sanitize(parts[2]),
        }
      }

      const queryName =
        parsed.searchParams.get('q') || parsed.searchParams.get('search')
      if (queryName) {
        return {
          inputUrl: parsed.toString(),
          origin: parsed.origin,
          site: 'coomerfans',
          service: 'onlyfans',
          userId: null,
          rawName: sanitize(queryName),
        }
      }

      throw new Error(
        'Expected an OnlyHaven URL like /creators/onlyfans/id or /creators?q=name'
      )
    },
    searchUrl: (name) =>
      `https://cum.st/creators?cq=${encodeURIComponent(name)}`,
    ...coomerFansRuntime,
  },
  {
    id: 'coomer',
    label: 'Coomer',
    site: 'coomer',
    registryKey: 'coomer',
    runLabel: 'coomer',
    letter: 'OF',
    engine: 'hoghaul',
    runOrder: 3,
    matchesHost: (host) => host.includes('coomer'),
    parseUrl: (parsed) => parseCreatorPath(parsed, 'coomer'),
    ...coomerKemonoRuntime,
  },
  {
    id: 'kemono',
    label: 'Pawchive',
    site: 'kemono',
    registryKey: 'kemono',
    runLabel: 'pawchive',
    letter: 'K',
    engine: 'hoghaul',
    runOrder: 2,
    matchesHost: isPawchiveOrKemonoHost,
    parseUrl: (parsed) => parseCreatorPath(parsed, 'kemono'),
    searchUrl: (name) =>
      `${PAWCHIVE_ORIGIN}/artists?q=${encodeURIComponent(name)}`,
    defaults: { postConcurrency: 4 },
    // Pawchive serves media directly; never needs the browser downloader.
    useBrowserMedia: () => false,
    ...coomerKemonoRuntime,
  },
  {
    id: 'reddit',
    label: 'Reddit',
    site: 'reddit',
    registryKey: 'reddit',
    runLabel: 'reddit',
    letter: 'R',
    engine: 'hoghaul',
    runOrder: 1,
    matchesHost: (host) =>
      host === 'reddit.com' || host.endsWith('.reddit.com'),
    parseUrl(parsed) {
      const parts = pathParts(parsed)
      const userIndex = parts.findIndex((part) =>
        /^(?:user|u)$/i.test(String(part || ''))
      )
      const username = userIndex >= 0 ? parts[userIndex + 1] : null
      if (username) {
        const cleanUsername = username.replace(/^u_/, '')
        return {
          inputUrl: `https://www.reddit.com/user/${cleanUsername}/submitted/`,
          origin: 'https://www.reddit.com',
          site: 'reddit',
          service: 'submitted',
          userId: cleanUsername,
          username: cleanUsername,
          rawName: sanitize(cleanUsername),
        }
      }

      throw new Error(
        'Expected a Reddit user URL like /user/name/submitted or /user/name'
      )
    },
    searchUrl: (name) =>
      `https://www.reddit.com/search/?q=${encodeURIComponent(name)}&type=users`,
    // Reddit media downloads only go through the browser when asked for
    // explicitly; plain requests are the paced default.
    useBrowserMedia(source, requested, runOptions = {}, env = process.env) {
      if (!runOptions.redditBrowserMedia && !env.HOGHAUL_REDDIT_BROWSER_MEDIA) {
        return false
      }
      return Boolean(requested)
    },
    preflight(source, page, ctx) {
      return require('./sourceAdapters/reddit').preflightRedditSource(source, {
        fetchHtml: ctx.fetchHtml,
        fetchJson: ctx.fetchJson,
        pageSize: REDDIT_PAGE_SIZE,
      })
    },
    fetchPosts(source, options, deps, ctx) {
      const adapter = require('./sourceAdapters/reddit')
      const redditDeps = {
        fetchHtml: ctx.fetchHtml,
        fetchJson: ctx.fetchJson,
        fetchPostHtml: deps.fetchPostHtml,
        fetchPostText: deps.fetchPostText,
        fallbackDelayMs: deps.fallbackDelayMs,
        redditFullRefresh: deps.redditFullRefresh,
        redditIncrementalOverlapPosts: deps.redditIncrementalOverlapPosts,
        redditSourceState: deps.redditSourceState,
        galleryCache: deps.galleryCache,
        onGalleryHydrated: deps.onGalleryHydrated,
        onDiscoveryProgress: deps.onDiscoveryProgress,
        onListingPage: deps.onListingPage,
        appendRunEvent: ctx.appendRunEvent,
        logger: ctx.logger,
        normalizeUrl: ctx.normalizeUrl,
        pageSize: REDDIT_PAGE_SIZE,
        redgifsClient: ctx.redgifsClient,
        suppressIncrementalLog: true,
      }
      return Array.isArray(deps.knownGalleryPosts)
        ? adapter.fetchKnownRedditGalleryPosts(
            source,
            deps.knownGalleryPosts,
            options,
            redditDeps
          )
        : adapter.fetchRedditPosts(source, options, redditDeps)
    },
  },
  {
    id: 'tumblr',
    label: 'Tumblr',
    site: 'tumblr',
    registryKey: 'tumblr',
    runLabel: 'tumblr',
    letter: 'T',
    engine: 'hoghaul',
    runOrder: 5,
    matchesHost: isTumblrHost,
    parseUrl(parsed, host) {
      const parts = pathParts(parsed)
      const subdomain = host.replace(/\.tumblr\.com$/i, '')
      let blogName = null
      if (host !== 'tumblr.com' && !TUMBLR_RESERVED_SUBDOMAINS.has(subdomain)) {
        blogName = subdomain
      } else if (parts[0] === 'blog' && parts[1] === 'view' && parts[2]) {
        blogName = parts[2]
      } else if (parts[0] && parts[0] !== 'blog') {
        blogName = parts[0]
      }

      if (!blogName) {
        throw new Error(
          'Expected a Tumblr blog URL like https://<blog>.tumblr.com or https://www.tumblr.com/<blog>'
        )
      }

      const cleanBlogName = blogName.toLowerCase()
      return {
        inputUrl: `https://${cleanBlogName}.tumblr.com/`,
        origin: `https://${cleanBlogName}.tumblr.com`,
        site: 'tumblr',
        service: 'blog',
        userId: cleanBlogName,
        username: cleanBlogName,
        rawName: sanitize(cleanBlogName),
      }
    },
    searchUrl: (name) =>
      `https://www.tumblr.com/search/${encodeURIComponent(name)}`,
    useBrowserMedia: () => false,
    preflight(source, page, ctx) {
      return require('./sourceAdapters/tumblr').preflightTumblrSource(
        source,
        page,
        { fetchJson: ctx.fetchJson, pageSize: TUMBLR_PAGE_SIZE }
      )
    },
    fetchPosts(source, options, deps, ctx) {
      return require('./sourceAdapters/tumblr').fetchTumblrPosts(
        source,
        options,
        {
          fetchJson: ctx.fetchJson,
          fullSourceRefresh: deps.fullSourceRefresh,
          logger: ctx.logger,
          sourceFrontier: deps.sourceFrontier,
          sourceIncrementalOverlapPages: deps.sourceIncrementalOverlapPages,
          pageSize: TUMBLR_PAGE_SIZE,
        }
      )
    },
  },
]

function normalizeSourceUrlInput(inputUrl) {
  const raw = String(inputUrl || '').trim()
  const markdownMatch = raw.match(/^\[[^\]]+\]\((https?:\/\/[^)]+)\)$/i)
  if (markdownMatch) return markdownMatch[1].trim()
  const angleMatch = raw.match(/^<\s*(https?:\/\/[^>]+)\s*>$/i)
  if (angleMatch) return angleMatch[1].trim()
  return raw
}

function hostOf(url) {
  try {
    return new URL(normalizeSourceUrlInput(url)).hostname.toLowerCase()
  } catch {
    return ''
  }
}

function findSourceByHost(host) {
  return SOURCES.find((source) => source.matchesHost(host)) || null
}

function findSourceForUrl(url) {
  const host = hostOf(url)
  return host ? findSourceByHost(host) : null
}

function getSourceById(id) {
  return SOURCES.find((source) => source.id === id) || null
}

// The definition for a parsed source (or a sidecar's { site, url }). Sites
// shared by several sources are told apart by URL host.
function findSourceForSite(site, urls = []) {
  const candidates = SOURCES.filter((source) => source.site === site)
  if (candidates.length <= 1) return candidates[0] || getSourceById(site)
  for (const url of urls) {
    const host = hostOf(url)
    const match = host && candidates.find((source) => source.matchesHost(host))
    if (match) return match
  }
  return candidates[0]
}

function findSourceForParsed(parsedSource) {
  if (!parsedSource) return null
  if (parsedSource.sourceType === 'stufferdb') return getSourceById('stufferdb')
  return findSourceForSite(parsedSource.site || parsedSource.sourceType, [
    parsedSource.origin,
    parsedSource.inputUrl,
    parsedSource.url,
  ])
}

function getRegistryKeyForSite(site) {
  if (!site) return site
  const source = SOURCES.find((entry) => entry.site === site)
  return source ? source.registryKey : site
}

// Registry keys in the order an all-source update visits them.
const REGISTRY_KEY_RUN_ORDER = [
  ...new Set(
    [...SOURCES]
      .sort((left, right) => left.runOrder - right.runOrder)
      .map((source) => source.registryKey)
  ),
]

// Display names for the creator platforms in Coomer/Pawchive/OnlyHaven URLs.
const SERVICE_LABELS = {
  onlyfans: 'OnlyFans',
  fansly: 'Fansly',
  patreon: 'Patreon',
  fanbox: 'Fanbox',
  candfans: 'C&F',
  subscribestar: 'SubStar',
  gumroad: 'Gumroad',
  afdian: 'Afdian',
  boosty: 'Boosty',
  discord: 'Discord',
  fantia: 'Fantia',
  dlsite: 'DLsite',
}

// { url, label, sourceId } for a registry source URL, e.g. "Reddit · u/name"
// or "Pawchive · Patreon". Unknown hosts fall back to the hostname.
function describeSourceLink(url) {
  const { parseSourceUrl } = require('./sourceRouter')
  const parsed = parseSourceUrl(url)
  const source = parsed ? findSourceForParsed(parsed) : findSourceForUrl(url)
  if (!source) return { url, label: hostOf(url) || url, sourceId: null }

  let detail = ''
  if (source.id === 'reddit' && parsed?.username) {
    detail = `u/${parsed.username}`
  } else if (source.id === 'tumblr' && parsed?.username) {
    detail = parsed.username
  } else if (parsed?.service && source.registryKey !== 'reddit') {
    detail = SERVICE_LABELS[parsed.service] || parsed.service
  }
  return {
    url,
    label: detail ? `${source.label} · ${detail}` : source.label,
    sourceId: source.id,
  }
}

// What the dashboards need to render badges, filters and source dots.
function listSourcesForClient() {
  return {
    sources: SOURCES.map((source) => ({
      id: source.id,
      label: source.label,
      site: source.site || source.id,
      registryKey: source.registryKey,
    })),
    registryKeys: REGISTRY_KEY_RUN_ORDER.map((key) => {
      const members = SOURCES.filter((source) => source.registryKey === key)
      return {
        key,
        letter: members[0].letter,
        label: members.map((source) => source.label).join(' / '),
      }
    }),
  }
}

module.exports = {
  SOURCES,
  REGISTRY_KEY_RUN_ORDER,
  normalizeSourceUrlInput,
  findSourceByHost,
  findSourceForUrl,
  findSourceForSite,
  findSourceForParsed,
  getSourceById,
  getRegistryKeyForSite,
  describeSourceLink,
  listSourcesForClient,
}

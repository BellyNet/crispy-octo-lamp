'use strict'

// Sources page support: look up a model's sources, add/remove/turn sources
// on and off, create models, and search the sites for a creator. All edits
// go to the NAS registry through scrapyard/registryStore.js; the PC picks
// them up on its next registry sync.

const queue = require('../scrapyard/scrapeQueue')
const {
  loadModelRegistry,
  sanitize,
  findCanonicalModelName,
  ensureModelEntryShape,
  upsertStufferdbSource,
  upsertRedditSource,
  upsertSourceInfo,
} = require('../scrapyard/modelRegistry')
const { updateRegistry } = require('../scrapyard/registryStore')
const {
  parseSourceUrl,
  findSourceForParsed,
} = require('../scrapyard/sourceRouter')
const { describeSourceLink } = require('../scrapyard/sources')
const { searchSources, findSourceOwner } = require('../scrapyard/sourceSearch')
const { planScrapeRun } = require('../scrapyard/scrapePlans')

const LISTS = ['sources', 'inactiveSources']

function describeEntry(name, entry) {
  const rows = (list) =>
    Object.entries(entry?.[list] || {}).flatMap(([key, sources]) =>
      (Array.isArray(sources) ? sources : []).map((source) => {
        const parsed = parseSourceUrl(source.url)
        const definition = parsed ? findSourceForParsed(parsed) : null
        return {
          key,
          url: source.url,
          label: describeSourceLink(source.url).label,
          sourceId: definition?.id || key,
          requiresBrowser: Boolean(definition?.requiresBrowser),
          lastCheckedAt: source.lastCheckedAt || null,
          inactiveAt: source.inactiveAt || null,
          inactiveReason: source.inactiveReason || null,
        }
      })
    )
  return {
    name,
    aliases: entry?.aliases || [],
    sources: rows('sources'),
    inactive: rows('inactiveSources'),
  }
}

// Adds a parsed source URL to a model (the same registry shape the scrapers
// write). Returns the canonical model name.
function addSourceToRegistry(registry, parsed, model, { createModel }) {
  const cleaned = sanitize(model)
  if (!cleaned) throw new Error('Choose a model.')
  const canonical =
    findCanonicalModelName(registry, cleaned) || (createModel ? cleaned : null)
  if (!canonical) {
    throw new Error(
      `There is no model called "${model}". Tick "new model" to create it.`
    )
  }
  const owner = findSourceOwner(registry, parsed)
  if (owner && owner !== canonical) {
    throw new Error(`That source already belongs to ${owner}.`)
  }

  registry[canonical] = ensureModelEntryShape(registry[canonical], canonical)
  const entry = registry[canonical]
  const numericName = /^\d+$/.test(String(parsed.rawName || ''))
  const rawName = sanitize(
    parsed.username || (numericName ? '' : parsed.rawName) || canonical
  )
  if (rawName && !entry.aliases.some((alias) => sanitize(alias) === rawName)) {
    entry.aliases.push(rawName)
  }

  if (parsed.sourceType === 'stufferdb') {
    upsertStufferdbSource(entry, parsed.url, rawName)
  } else if (parsed.sourceType === 'reddit') {
    upsertRedditSource(entry, parsed.url, rawName)
  } else {
    upsertSourceInfo(
      entry,
      {
        site: parsed.site || parsed.sourceType,
        service: parsed.service,
        userId: parsed.userId,
        username: parsed.username || null,
        inputUrl: parsed.url,
      },
      rawName
    )
  }

  // Re-adding a turned-off source turns it back on.
  for (const [key, list] of Object.entries(entry.inactiveSources || {})) {
    entry.inactiveSources[key] = list.filter(
      (source) => source.url !== parsed.url
    )
  }
  return canonical
}

function moveSource(registry, { model, key, url }, from, to, patch) {
  const entry = registry[model]
  const list = entry?.[from]?.[key]
  const index = Array.isArray(list)
    ? list.findIndex((source) => source.url === url)
    : -1
  if (index < 0) throw new Error('Source not found.')
  const [source] = list.splice(index, 1)
  if (to) {
    entry[to] ||= {}
    // Some older entries list a URL in both places; keep one copy.
    entry[to][key] = (entry[to][key] || []).filter((other) => other.url !== url)
    entry[to][key].push(patch(source))
  }
}

function mountSourceRoutes(app, { registryPath, pageDir }) {
  const respond = (fn) => async (req, res) => {
    try {
      res.setHeader('Cache-Control', 'no-cache')
      res.json(await fn(req))
    } catch (err) {
      res.status(400).json({ error: err.message })
    }
  }
  const target = (body) => {
    const model = sanitize(body?.model)
    const key = String(body?.key || '')
    const url = String(body?.url || '')
    if (!model || !key || !url)
      throw new Error('model, key and url are required')
    return { model, key, url }
  }

  app.get('/sources', (_req, res) =>
    res.sendFile('sources.html', { root: pageDir })
  )

  app.get(
    '/api/sources/models',
    respond(() =>
      Object.entries(loadModelRegistry(registryPath))
        .map(([name, entry]) => ({
          name,
          aliases: entry?.aliases || [],
          sources: Object.values(entry?.sources || {}).reduce(
            (sum, list) => sum + (Array.isArray(list) ? list.length : 0),
            0
          ),
        }))
        .sort((a, b) => a.name.localeCompare(b.name))
    )
  )

  app.get(
    '/api/sources/model/:name',
    respond((req) => {
      const registry = loadModelRegistry(registryPath)
      const name = findCanonicalModelName(registry, sanitize(req.params.name))
      if (!name) throw new Error(`No model called ${req.params.name}.`)
      return describeEntry(name, registry[name])
    })
  )

  app.get(
    '/api/sources/search',
    respond((req) => searchSources(req.query.q, { registryPath }))
  )

  app.post(
    '/api/sources/add',
    respond(async (req) => {
      const parsed = parseSourceUrl(req.body?.url)
      if (!parsed) throw new Error('That URL is not a supported source.')
      const model = await updateRegistry(
        (registry) =>
          addSourceToRegistry(registry, parsed, req.body?.model, {
            createModel: Boolean(req.body?.createModel),
          }),
        registryPath
      )
      let run = null
      if (req.body?.scrapeNow) {
        const plan = planScrapeRun(
          { scope: 'url', model, url: parsed.url },
          { registryPath }
        )
        run = queue.summarizeRun(
          queue.createRun({ ...plan, scope: 'url', createdBy: 'sources page' })
        )
      }
      return {
        ok: true,
        model,
        run,
        entry: describeEntry(model, loadModelRegistry(registryPath)[model]),
      }
    })
  )

  for (const [route, from, to, patch] of [
    [
      'deactivate',
      'sources',
      'inactiveSources',
      (source) => ({
        ...source,
        inactiveAt: new Date().toISOString(),
        inactiveReason: 'turned off in dashboard',
      }),
    ],
    [
      'reactivate',
      'inactiveSources',
      'sources',
      ({ inactiveAt, inactiveReason, ...source }) => source,
    ],
    ['remove', null, null, null],
  ]) {
    app.post(
      `/api/sources/${route}`,
      respond(async (req) => {
        const change = target(req.body)
        await updateRegistry((registry) => {
          if (route === 'remove') {
            const list = LISTS.find((name) =>
              (registry[change.model]?.[name]?.[change.key] || []).some(
                (source) => source.url === change.url
              )
            )
            if (!list) throw new Error('Source not found.')
            moveSource(registry, change, list, null)
          } else {
            moveSource(registry, change, from, to, patch)
          }
        }, registryPath)
        return {
          ok: true,
          entry: describeEntry(
            change.model,
            loadModelRegistry(registryPath)[change.model]
          ),
        }
      })
    )
  }
}

module.exports = { mountSourceRoutes, addSourceToRegistry }

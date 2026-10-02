'use strict'

// Turns "scrape everything", "scrape this model" or "scrape this URL" into
// queue tasks (one per source URL), using the same source selection as the
// all-source update.

const config = require('./config')
const { loadModelRegistry, sanitize } = require('./modelRegistry')
const { buildAllSourceQueue, inferCanonicalModel } = require('./scraperRunner')
const { parseSourceUrl, findSourceForParsed } = require('./sourceRouter')

function taskForUrl(model, url) {
  const parsed = parseSourceUrl(url)
  if (!parsed) return null
  const source = findSourceForParsed(parsed)
  return {
    model,
    url: parsed.url,
    sourceId: source?.id || parsed.sourceType,
    sourceLabel: source?.label || parsed.sourceType,
    requiresBrowser: Boolean(source?.requiresBrowser),
  }
}

// scope: 'all' | 'model' | 'url'
function planScrapeRun(
  { scope, model, url },
  { registryPath = config.registryPath } = {}
) {
  if (scope === 'url') {
    const parsed = parseSourceUrl(url)
    if (!parsed) throw new Error('That URL is not a supported source.')
    const resolvedModel =
      sanitize(model) ||
      inferCanonicalModel(parsed, '') ||
      sanitize(parsed.rawName)
    if (!resolvedModel) {
      throw new Error('Choose a model for this URL (it has no creator name).')
    }
    return {
      label: `${resolvedModel}: ${url}`,
      tasks: [taskForUrl(resolvedModel, url)],
    }
  }

  const registry = loadModelRegistry(registryPath)
  let queue = buildAllSourceQueue(registry, {
    skipCompletedLegacyCoomerFans: true,
  })
  if (scope === 'model') {
    const wanted = sanitize(model)
    queue = queue.filter((item) => item.model === wanted)
    if (!queue.length) throw new Error(`No active sources for ${model}.`)
  }
  const tasks = queue.flatMap((item) =>
    item.sources
      .map((source) => taskForUrl(item.model, source.url))
      .filter(Boolean)
  )
  if (!tasks.length) throw new Error('Nothing to scrape.')
  return {
    label:
      scope === 'model' ? `${queue[0].model} (all sources)` : 'All sources',
    tasks,
  }
}

module.exports = { planScrapeRun }

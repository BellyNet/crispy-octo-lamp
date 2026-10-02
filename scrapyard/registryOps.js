'use strict'

// Changes to model_aliases.json as small operations, so edits made in two
// places (the dashboard on the NAS, a scrape on the PC) can be merged
// instead of one copy overwriting the other.
//
// diffRegistries(before, after) -> ops
// applyRegistryOps(registry, ops) -> registry (mutated and returned)
//
// Ops (sources are matched by URL within a model/list/registry key):
//   { op: 'createModel', model, entry }
//   { op: 'deleteModel', model }
//   { op: 'addAlias' | 'removeAlias', model, alias }
//   { op: 'upsertSource', model, list, key, source }
//   { op: 'removeSource', model, list, key, url }
//   { op: 'setSourceReview', model, value }
// where list is 'sources' or 'inactiveSources'.

const LISTS = ['sources', 'inactiveSources']

function sourceUrl(source) {
  return String(source?.url || '').trim()
}

function sameJson(left, right) {
  return JSON.stringify(left ?? null) === JSON.stringify(right ?? null)
}

function byUrl(list) {
  const map = new Map()
  for (const source of Array.isArray(list) ? list : []) {
    const url = sourceUrl(source)
    if (url) map.set(url, source)
  }
  return map
}

function diffModel(model, before, after, ops) {
  const beforeAliases = new Set(before?.aliases || [])
  const afterAliases = new Set(after?.aliases || [])
  for (const alias of afterAliases) {
    if (!beforeAliases.has(alias)) ops.push({ op: 'addAlias', model, alias })
  }
  for (const alias of beforeAliases) {
    if (!afterAliases.has(alias)) ops.push({ op: 'removeAlias', model, alias })
  }

  for (const list of LISTS) {
    const beforeLists = before?.[list] || {}
    const afterLists = after?.[list] || {}
    const keys = new Set([
      ...Object.keys(beforeLists),
      ...Object.keys(afterLists),
    ])
    for (const key of keys) {
      const beforeSources = byUrl(beforeLists[key])
      const afterSources = byUrl(afterLists[key])
      for (const [url, source] of afterSources) {
        if (!sameJson(beforeSources.get(url), source)) {
          ops.push({ op: 'upsertSource', model, list, key, source })
        }
      }
      for (const url of beforeSources.keys()) {
        if (!afterSources.has(url)) {
          ops.push({ op: 'removeSource', model, list, key, url })
        }
      }
    }
  }

  if (!sameJson(before?.sourceReview || {}, after?.sourceReview || {})) {
    ops.push({ op: 'setSourceReview', model, value: after?.sourceReview || {} })
  }
}

function diffRegistries(before = {}, after = {}) {
  const ops = []
  for (const [model, entry] of Object.entries(after || {})) {
    if (!before || !(model in before)) {
      ops.push({ op: 'createModel', model, entry })
    } else {
      diffModel(model, before[model], entry, ops)
    }
  }
  for (const model of Object.keys(before || {})) {
    if (!after || !(model in after)) ops.push({ op: 'deleteModel', model })
  }
  return ops
}

function ensureEntry(registry, model) {
  if (!registry[model] || typeof registry[model] !== 'object') {
    registry[model] = { aliases: [model], sources: {} }
  }
  const entry = registry[model]
  if (!Array.isArray(entry.aliases)) entry.aliases = []
  for (const list of LISTS) {
    if (!entry[list] || typeof entry[list] !== 'object') entry[list] = {}
  }
  return entry
}

function applyRegistryOps(registry, ops = []) {
  for (const change of ops) {
    const { op, model } = change || {}
    if (!model || typeof model !== 'string') continue
    if (op === 'createModel') {
      if (!registry[model]) {
        registry[model] = change.entry || { aliases: [model], sources: {} }
      } else {
        // Created on both sides: merge the new entry in as upserts.
        applyRegistryOps(
          registry,
          diffRegistries({ [model]: {} }, { [model]: change.entry })
        )
      }
    } else if (op === 'deleteModel') {
      delete registry[model]
    } else if (op === 'addAlias') {
      const entry = ensureEntry(registry, model)
      if (!entry.aliases.includes(change.alias))
        entry.aliases.push(change.alias)
    } else if (op === 'removeAlias') {
      if (!registry[model]) continue
      const entry = ensureEntry(registry, model)
      entry.aliases = entry.aliases.filter((alias) => alias !== change.alias)
    } else if (op === 'upsertSource') {
      if (!LISTS.includes(change.list) || !change.key) continue
      const entry = ensureEntry(registry, model)
      const list = (entry[change.list][change.key] ||= [])
      const url = sourceUrl(change.source)
      const index = list.findIndex((source) => sourceUrl(source) === url)
      if (index >= 0) list[index] = change.source
      else list.push(change.source)
    } else if (op === 'removeSource') {
      if (!registry[model] || !LISTS.includes(change.list)) continue
      const entry = ensureEntry(registry, model)
      const list = entry[change.list][change.key]
      if (!Array.isArray(list)) continue
      // An emptied list stays as [], like the scrapers leave it.
      entry[change.list][change.key] = list.filter(
        (source) => sourceUrl(source) !== change.url
      )
    } else if (op === 'setSourceReview') {
      if (!registry[model]) continue
      registry[model].sourceReview = change.value || {}
    }
  }
  return registry
}

module.exports = { diffRegistries, applyRegistryOps }

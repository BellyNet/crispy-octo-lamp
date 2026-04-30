'use strict'

const fs = require('fs')
const path = require('path')
const { writeRepoJsonFileSync } = require('./repoFileWriter')

function sortStringValues(values) {
  return Array.from(new Set((values || []).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b)
  )
}

function sortStufferSources(sources) {
  return [...(Array.isArray(sources) ? sources : [])].sort((a, b) => {
    const left =
      String(a?.discoveredAs || '') ||
      String(a?.categoryId || '') ||
      String(a?.url || '')
    const right =
      String(b?.discoveredAs || '') ||
      String(b?.categoryId || '') ||
      String(b?.url || '')
    return left.localeCompare(right)
  })
}

function sortPlatformSources(sources) {
  if (!sources || typeof sources !== 'object') return {}

  const sorted = {}
  for (const [platform, entries] of Object.entries(sources)) {
    if (platform === 'stufferdb') {
      sorted[platform] = sortStufferSources(entries)
    } else {
      // coomer, kemono, etc. — preserve as-is (arrays of { url } objects)
      sorted[platform] = Array.isArray(entries) ? entries : []
    }
  }
  return sorted
}

function sortModelRegistry(registry) {
  return Object.fromEntries(
    Object.entries(registry || {})
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([canonicalName, entry]) => [
        canonicalName,
        {
          aliases: sortStringValues(entry?.aliases),
          sources: sortPlatformSources(entry?.sources),
        },
      ])
  )
}

function main() {
  const registryPath = path.join(__dirname, '..', 'model_aliases.json')
  const registry = JSON.parse(fs.readFileSync(registryPath, 'utf8'))
  const sortedRegistry = sortModelRegistry(registry)
  writeRepoJsonFileSync(registryPath, sortedRegistry)
  console.log(`Sorted model registry: ${registryPath}`)
}

main()

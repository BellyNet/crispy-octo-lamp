const fs = require('fs')
const path = require('path')

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

function sortModelRegistry(registry) {
  return Object.fromEntries(
    Object.entries(registry || {})
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([canonicalName, entry]) => [
        canonicalName,
        {
          aliases: sortStringValues(entry?.aliases),
          sources: {
            stufferdb: sortStufferSources(entry?.sources?.stufferdb),
          },
        },
      ])
  )
}

function main() {
  const registryPath = path.join(__dirname, '..', 'model_aliases.json')
  const registry = JSON.parse(fs.readFileSync(registryPath, 'utf8'))
  const sortedRegistry = sortModelRegistry(registry)
  fs.writeFileSync(registryPath, JSON.stringify(sortedRegistry, null, 2) + '\n')
  console.log(`Sorted model registry: ${registryPath}`)
}

main()

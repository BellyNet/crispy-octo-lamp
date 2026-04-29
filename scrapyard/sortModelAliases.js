'use strict'

const fs = require('fs')
const path = require('path')
const { execSync } = require('child_process')

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

  // Write with standard JSON indent first, then let Prettier normalise it
  fs.writeFileSync(registryPath, JSON.stringify(sortedRegistry, null, 2) + '\n')

  try {
    execSync(`npx prettier --write "${registryPath}"`, {
      cwd: path.join(__dirname, '..'),
      stdio: 'pipe',
    })
    console.log(`Sorted and formatted model registry: ${registryPath}`)
  } catch {
    // Prettier not available — JSON.stringify output is still valid
    console.log(`Sorted model registry: ${registryPath} (Prettier unavailable)`)
  }
}

main()

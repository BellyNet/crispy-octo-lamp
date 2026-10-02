'use strict'

// The model registry on the NAS is the one everything else syncs with. All
// changes made by the dashboard process (Sources page edits, changes the PC
// sends after a scrape) go through updateRegistry, which reads the current
// file, applies the change and saves, one change at a time.

const config = require('./config')
const { loadModelRegistry, saveModelRegistry } = require('./modelRegistry')
const { applyRegistryOps } = require('./registryOps')

let chain = Promise.resolve()

function updateRegistry(mutate, registryPath = config.registryPath) {
  const run = chain.then(() => {
    const registry = loadModelRegistry(registryPath)
    const result = mutate(registry)
    saveModelRegistry(registryPath, registry)
    return result
  })
  chain = run.catch(() => {})
  return run
}

function applyOpsToRegistry(ops, registryPath = config.registryPath) {
  return updateRegistry((registry) => {
    applyRegistryOps(registry, ops)
  }, registryPath).then(() => loadModelRegistry(registryPath))
}

module.exports = { updateRegistry, applyOpsToRegistry }

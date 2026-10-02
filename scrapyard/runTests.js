'use strict'

// `npm test`: runs every fixture suite with the dataset, NAS and state roots
// pointed at throwaway temp folders. The real defaults are the live NAS
// dataset, so no test may ever be able to resolve to them.
const fs = require('fs')
const os = require('os')
const path = require('path')
const { spawnSync } = require('child_process')

const SUITES = [
  'scrapyard/smokeUnifiedScraper.js',
  'dashboard/testExactDuplicateNightly.js',
  'audit/testChosenCrossModelExactCleanup.js',
  'scrapyard/testHammingIndex.js',
  'scrapyard/testSources.js',
  'scrapyard/testDatasetLocation.js',
  'scrapyard/testScrapeQueue.js',
]

const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), 'lora-tests-'))
const env = {
  ...process.env,
  DATASET_DIR: path.join(sandbox, 'dataset'),
  NAS_DATASET_DIR: path.join(sandbox, 'nas-dataset'),
  NAS_DASHBOARD_CACHE_DIR: path.join(sandbox, 'dashboard-cache'),
  SLOPVAULT_ROOT: path.join(sandbox, 'slopvault'),
  MODEL_REGISTRY_PATH: path.join(sandbox, 'model_aliases.json'),
  SCRAPE_QUEUE_DIR: path.join(sandbox, 'scrape-queue'),
  SCRAPE_QUEUE_MODE: 'local',
}
for (const dir of ['dataset', 'nas-dataset', 'dashboard-cache', 'slopvault']) {
  fs.mkdirSync(path.join(sandbox, dir), { recursive: true })
}
fs.copyFileSync(
  path.join(__dirname, '..', 'model_aliases.json'),
  env.MODEL_REGISTRY_PATH
)

let failed = 0
for (const suite of SUITES) {
  const result = spawnSync(process.execPath, [suite], {
    cwd: path.join(__dirname, '..'),
    env,
    stdio: 'inherit',
  })
  if (result.status !== 0) {
    failed += 1
    console.error(`FAILED: ${suite} (exit ${result.status})`)
  }
}
fs.rmSync(sandbox, { recursive: true, force: true })
process.exitCode = failed ? 1 : 0

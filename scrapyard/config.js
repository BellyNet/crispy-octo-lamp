'use strict'

// Where everything lives. Scrapers, repair tools and the scrape dashboard
// resolve their paths from here.
//
// Overrides, from the environment or the repo's .env:
//   NAS_DATASET_DIR          the dataset, on the NAS (default Z:\dataset)
//   DATASET_DIR              point one run at a different dataset folder
//   NAS_DASHBOARD_CACHE_DIR  dashboard cache share (default: next to the NAS
//                            dataset, Z:\dashboard-cache)
//   SLOPVAULT_ROOT           local state root: quarantine, browser profiles,
//                            OAuth tokens (default %APPDATA%\.slopvault)
//   MODEL_REGISTRY_PATH      model_aliases.json
//
// With the dataset on the NAS, scrapyard/datasetLocation.js makes every
// local-to-NAS sync and "evict the local copy" step a no-op.

const os = require('os')
const path = require('path')

const rootDir = path.join(__dirname, '..')

try {
  require('dotenv').config({ path: path.join(rootDir, '.env'), quiet: true })
} catch {}

const appDataDir =
  process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming')
const slopvaultRoot = path.resolve(
  process.env.SLOPVAULT_ROOT || path.join(appDataDir, '.slopvault')
)
const nasDatasetDir = path.resolve(process.env.NAS_DATASET_DIR || 'Z:\\dataset')
// The dataset lives on the NAS; scrapers write straight to it. (The old
// local copy under %APPDATA%\.slopvault\dataset is no longer used, and
// LOCAL_DATASET_DIR is deliberately ignored so a leftover .env entry can't
// send scrapes back to it.) DATASET_DIR still points a run elsewhere.
const datasetDir = path.resolve(process.env.DATASET_DIR || nasDatasetDir)
const nasDashboardCacheDir = path.resolve(
  process.env.NAS_DASHBOARD_CACHE_DIR ||
    path.join(path.dirname(nasDatasetDir), 'dashboard-cache')
)
const quarantineDir = path.join(slopvaultRoot, 'quarantine')

module.exports = {
  rootDir,
  appDataDir,
  slopvaultRoot,
  datasetDir,
  nasDatasetDir,
  nasDashboardCacheDir,
  quarantineDir,
  quarantineDatasetDir: path.join(quarantineDir, 'dataset'),
  quarantineManifestPath: path.join(quarantineDir, 'quarantine-manifest.json'),
  registryPath: path.resolve(
    process.env.MODEL_REGISTRY_PATH ||
      process.env.HOGHAUL_REGISTRY_PATH ||
      path.join(rootDir, 'model_aliases.json')
  ),
  tmpDir: path.join(rootDir, 'tmp'),
}

'use strict'

// Where everything lives. Scrapers, repair tools and the scrape dashboard
// resolve their paths from here, so moving the dataset (e.g. onto the NAS)
// is a config change rather than an edit to every script.
//
// Overrides, from the environment or the repo's .env:
//   DATASET_DIR / LOCAL_DATASET_DIR  dataset the scrapers read and write
//   NAS_DATASET_DIR                  NAS dataset mirror (default Z:\dataset)
//   NAS_DASHBOARD_CACHE_DIR          dashboard cache share (default: next to
//                                    the NAS dataset, Z:\dashboard-cache)
//   SLOPVAULT_ROOT                   state root: quarantine, hash stores,
//                                    browser profiles, OAuth tokens
//   MODEL_REGISTRY_PATH              model_aliases.json

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
const datasetDir = path.resolve(
  process.env.DATASET_DIR ||
    process.env.LOCAL_DATASET_DIR ||
    path.join(slopvaultRoot, 'dataset')
)
const nasDatasetDir = path.resolve(process.env.NAS_DATASET_DIR || 'Z:\\dataset')
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

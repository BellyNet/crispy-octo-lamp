'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')

const { hasNasMp4RelativePath } = require('./nasMp4Index')

function createDatasetPaths(options = {}) {
  const rootDir = options.rootDir || path.join(__dirname, '..')
  const slopvaultRoot =
    options.slopvaultRoot ||
    path.join(
      process.env.APPDATA || path.join(os.homedir(), 'AppData', 'Roaming'),
      '.slopvault'
    )
  const datasetDir = options.datasetDir || path.join(slopvaultRoot, 'dataset')
  const quarantineDatasetDir =
    options.quarantineDatasetDir ||
    path.join(slopvaultRoot, 'quarantine', 'dataset')
  const nasDatasetDir = path.resolve(
    String(
      options.nasDatasetDir || process.env.NAS_DATASET_DIR || 'Z:\\dataset'
    )
  )
  const repairCanUseNasMirror = Boolean(options.repairCanUseNasMirror)

  function getIncompleteDirs(modelName) {
    const base = path.join(rootDir, 'incomplete', modelName)
    const gifs = path.join(base, 'gifs')
    const videos = path.join(base, 'videos')

    fs.mkdirSync(gifs, { recursive: true })
    fs.mkdirSync(videos, { recursive: true })

    return { base, gifs, videos }
  }

  function createModelFolders(modelName) {
    const base = path.join(datasetDir, modelName)
    const images = path.join(base, 'images')
    const logDir = path.join(base, 'log')
    const incomplete = getIncompleteDirs(modelName)

    fs.mkdirSync(images, { recursive: true })
    fs.mkdirSync(logDir, { recursive: true })

    return {
      base,
      images,
      logDir,
      incompleteGifDir: incomplete.gifs,
      incompleteVideoDir: incomplete.videos,
      createGifFolder: () => {
        const gifPath = path.join(base, 'gif')
        fs.mkdirSync(gifPath, { recursive: true })
        return gifPath
      },
      createWebmFolder: () => {
        const webmPath = path.join(base, 'webm')
        fs.mkdirSync(webmPath, { recursive: true })
        return webmPath
      },
    }
  }

  function getDatasetRelativePath(filePath) {
    return path.relative(datasetDir, filePath).replace(/\\/g, '/')
  }

  function getQuarantineMirrorPath(filePath) {
    return path.join(
      quarantineDatasetDir,
      getDatasetRelativePath(filePath).replace(/\//g, path.sep)
    )
  }

  function getNasMirrorPath(filePath) {
    return path.join(
      nasDatasetDir,
      getDatasetRelativePath(filePath).replace(/\//g, path.sep)
    )
  }

  function isQuarantinedPath(filePath) {
    return fs.existsSync(getQuarantineMirrorPath(filePath))
  }

  function existsAtExactPath(filePath) {
    return fs.existsSync(filePath)
  }

  function existsForRepair(filePath) {
    if (existsAtExactPath(filePath)) return !isQuarantinedPath(filePath)
    return (
      repairCanUseNasMirror && existsAtExactPath(getNasMirrorPath(filePath))
    )
  }

  function existsLocallyOrOnNas(filePath) {
    if (existsAtExactPath(filePath)) return true
    if (path.extname(String(filePath || '')).toLowerCase() !== '.mp4') {
      return false
    }
    return hasNasMp4RelativePath(getDatasetRelativePath(filePath), datasetDir)
  }

  function existsLocallyOnNasOrInQuarantine(filePath) {
    return existsLocallyOrOnNas(filePath) || isQuarantinedPath(filePath)
  }

  function toDatasetAbsolutePath(relativePath) {
    return path.join(
      datasetDir,
      String(relativePath || '').replace(/\//g, path.sep)
    )
  }

  return {
    rootDir,
    slopvaultRoot,
    datasetDir,
    quarantineDatasetDir,
    nasDatasetDir,
    getIncompleteDirs,
    createModelFolders,
    getDatasetRelativePath,
    getQuarantineMirrorPath,
    getNasMirrorPath,
    isQuarantinedPath,
    existsAtExactPath,
    existsForRepair,
    existsLocallyOrOnNas,
    existsLocallyOnNasOrInQuarantine,
    toDatasetAbsolutePath,
  }
}

module.exports = {
  createDatasetPaths,
}

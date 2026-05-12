'use strict'

const path = require('path')

function createDuplicateChecker(options = {}) {
  const {
    datasetDir,
    existsLocallyOrOnNas,
    getBitwiseHashRecord,
    isBitwiseDupe,
    getVisualHashRecord,
    isVisualDupe,
    getVisualHashEntries,
    getVisualHashDistance,
  } = options

  if (!datasetDir) {
    throw new Error('createDuplicateChecker requires datasetDir')
  }
  if (typeof existsLocallyOrOnNas !== 'function') {
    throw new Error('createDuplicateChecker requires existsLocallyOrOnNas')
  }

  const pendingVisualClaims = new Map()

  function getRecordRefs(record) {
    if (!Array.isArray(record?.refs)) return []
    return record.refs
      .map((ref) => {
        if (typeof ref === 'string') return ref.replace(/\\/g, '/')
        if (ref && typeof ref === 'object' && ref.relativePath) {
          return String(ref.relativePath).replace(/\\/g, '/')
        }
        return ''
      })
      .filter(Boolean)
  }

  function getActiveRecordRefs(record) {
    return getRecordRefs(record).filter((relativePath) =>
      existsLocallyOrOnNas(
        path.join(datasetDir, relativePath.replace(/\//g, path.sep))
      )
    )
  }

  function requireBitwiseHelpers(name) {
    if (
      typeof getBitwiseHashRecord !== 'function' ||
      typeof isBitwiseDupe !== 'function'
    ) {
      throw new Error(
        `${name} requires getBitwiseHashRecord and isBitwiseDupe to be provided`
      )
    }
  }

  function requireVisualHelpers(name) {
    if (
      typeof getVisualHashRecord !== 'function' ||
      typeof isVisualDupe !== 'function'
    ) {
      throw new Error(
        `${name} requires getVisualHashRecord and isVisualDupe to be provided`
      )
    }
  }

  function requireFuzzyVisualHelpers(name) {
    if (
      typeof getVisualHashEntries !== 'function' ||
      typeof getVisualHashDistance !== 'function'
    ) {
      throw new Error(
        `${name} requires getVisualHashEntries and getVisualHashDistance to be provided`
      )
    }
  }

  function getBitwiseDuplicationRecord(hash) {
    requireBitwiseHelpers('getBitwiseDuplicationRecord')
    const record = getBitwiseHashRecord(hash)
    const activeRefs = getActiveRecordRefs(record)
    return {
      record,
      activeRefs,
      isDuplicate: activeRefs.length > 0 && isBitwiseDupe(hash),
    }
  }

  function getVisualDuplicationRecord(visualHash) {
    requireVisualHelpers('getVisualDuplicationRecord')
    const record = getVisualHashRecord(visualHash)
    const activeRefs = getActiveRecordRefs(record)
    return {
      record,
      activeRefs,
      isDuplicate: activeRefs.length > 0 && isVisualDupe(visualHash),
    }
  }

  function isSameModelRef(modelName, relativePath) {
    return String(relativePath || '').startsWith(`${modelName}/`)
  }

  function getFuzzyVisualDuplicationRecord(modelName, visualHash, maxDistance) {
    if (!visualHash || !Number.isFinite(maxDistance) || maxDistance < 0) {
      return null
    }
    requireFuzzyVisualHelpers('getFuzzyVisualDuplicationRecord')

    let bestMatch = null
    for (const entry of getVisualHashEntries()) {
      const candidateHash = String(entry?.hash || '')
      const distance = getVisualHashDistance(visualHash, candidateHash)
      if (distance === null || distance > maxDistance) continue

      const activeRefs = getActiveRecordRefs(entry).filter((relativePath) =>
        isSameModelRef(modelName, relativePath)
      )
      if (activeRefs.length === 0) continue

      if (
        !bestMatch ||
        distance < bestMatch.distance ||
        (distance === bestMatch.distance &&
          candidateHash.localeCompare(bestMatch.matchedHash) < 0)
      ) {
        bestMatch = {
          record: entry,
          activeRefs,
          distance,
          matchedHash: candidateHash,
          isDuplicate: true,
        }
      }
    }

    return bestMatch
  }

  function getPendingImageVisualDuplicate(modelName, visualHash, maxDistance) {
    if (!visualHash || !Number.isFinite(maxDistance) || maxDistance < 0) {
      return null
    }
    if (typeof getVisualHashDistance !== 'function') {
      throw new Error(
        'getPendingImageVisualDuplicate requires getVisualHashDistance to be provided'
      )
    }

    let bestMatch = null
    for (const claim of pendingVisualClaims.values()) {
      if (!claim || claim.modelName !== modelName) continue
      const distance = getVisualHashDistance(visualHash, claim.visualHash)
      if (distance === null || distance > maxDistance) continue
      if (
        !bestMatch ||
        distance < bestMatch.distance ||
        (distance === bestMatch.distance &&
          claim.relativePath.localeCompare(bestMatch.activeRefs[0]) < 0)
      ) {
        bestMatch = {
          activeRefs: [claim.relativePath],
          distance,
          matchedHash: claim.visualHash,
          isDuplicate: true,
        }
      }
    }

    return bestMatch
  }

  function reservePendingImageVisualClaim(modelName, relativePath, visualHash) {
    const claimKey = `${modelName}:${relativePath}`
    pendingVisualClaims.set(claimKey, {
      modelName,
      relativePath,
      visualHash,
    })
    return claimKey
  }

  function releasePendingImageVisualClaim(claimKey) {
    if (!claimKey) return
    pendingVisualClaims.delete(claimKey)
  }

  return {
    getRecordRefs,
    getActiveRecordRefs,
    getBitwiseDuplicationRecord,
    getVisualDuplicationRecord,
    getFuzzyVisualDuplicationRecord,
    getPendingImageVisualDuplicate,
    reservePendingImageVisualClaim,
    releasePendingImageVisualClaim,
  }
}

module.exports = {
  createDuplicateChecker,
}

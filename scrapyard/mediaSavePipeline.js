'use strict'

const fs = require('fs')
const { createHash } = require('crypto')
const { getMediaEntryPageUrls, getMediaEntryUrls } = require('./mediaEntries')

function createMediaSavePipeline(options = {}) {
  const {
    mediaSaver,
    appendRunEvent,
    recordSuccessfulSeenMedia,
    getSuccessfulSeenMediaMatch,
    existsLocallyOrOnNas,
    knownFilenames,
    isQuarantinedPath = () => false,
    onDuplicate = () => {},
    onSaved = () => {},
    onQueued = () => {},
    onOutcome = () => {},
  } = options

  if (!mediaSaver) {
    throw new Error('createMediaSavePipeline requires mediaSaver')
  }
  if (typeof appendRunEvent !== 'function') {
    throw new Error('createMediaSavePipeline requires appendRunEvent')
  }
  if (typeof recordSuccessfulSeenMedia !== 'function') {
    throw new Error(
      'createMediaSavePipeline requires recordSuccessfulSeenMedia'
    )
  }
  if (typeof getSuccessfulSeenMediaMatch !== 'function') {
    throw new Error(
      'createMediaSavePipeline requires getSuccessfulSeenMediaMatch'
    )
  }
  if (typeof existsLocallyOrOnNas !== 'function') {
    throw new Error('createMediaSavePipeline requires existsLocallyOrOnNas')
  }

  function getDestination({ modelName, folders, entry, kind }) {
    return mediaSaver.getDestination({
      modelName,
      folders,
      filename: entry.filename,
      kind: kind || entry.kind,
    })
  }

  function recordOutcome(kind, label, details = {}) {
    onOutcome({ kind, label, ...details })
  }

  function recordDuplicate({
    modelName,
    folders,
    entry,
    destination,
    reason,
    extra = {},
    savedPath = null,
    recordSeen = false,
  }) {
    const duplicatePath =
      savedPath || destination?.relativePath || extra.savedPath || null
    onDuplicate({ modelName, folders, entry, destination, reason, extra })
    appendRunEvent(
      reason,
      mediaSaver.buildDuplicateEvent({
        entry,
        savedPath: duplicatePath,
        extra: {
          modelName,
          ...extra,
        },
      })
    )

    if (recordSeen && folders?.logDir && duplicatePath) {
      recordSuccessfulSeenMedia(
        folders.logDir,
        mediaSaver.buildSeenRecord(entry, {
          relativePath: duplicatePath,
          savedPath: duplicatePath,
          filename: entry.filename,
        })
      )
    }

    recordOutcome(
      mediaSaver.getOutcomeKindForReason(reason),
      `${reason}: ${entry.filename}`,
      { modelName, entry, destination, reason }
    )
  }

  function recordMediaSeen({ modelName, entry, destination }) {
    appendRunEvent(
      'media_seen',
      mediaSaver.buildMediaSeenEvent({ modelName, entry, destination })
    )
  }

  function getSeenMediaMatch(folders, entry) {
    return getSuccessfulSeenMediaMatch(
      folders.logDir,
      getMediaEntryPageUrls(entry),
      getMediaEntryUrls(entry)
    )
  }

  function recordSaved({
    modelName,
    folders,
    entry,
    destination,
    sizeBytes,
    hash = null,
    visualHash = null,
    kind = entry.kind,
    extra = {},
  }) {
    const stats = mediaSaver.buildSavedStats({ sizeBytes, kind })
    onSaved({ modelName, folders, entry, destination, stats, hash, visualHash })
    if (knownFilenames?.add) knownFilenames.add(entry.filename)

    recordSuccessfulSeenMedia(
      folders.logDir,
      mediaSaver.buildSeenRecord(entry, destination)
    )
    appendRunEvent(
      destination.savedEventType,
      mediaSaver.buildSavedEvent({
        modelName,
        entry,
        destination,
        hash,
        visualHash,
        extra,
      })
    )
    recordOutcome('saved', `${destination.savedOutcome}: ${entry.filename}`, {
      modelName,
      entry,
      destination,
    })
    return stats
  }

  function queueVideo({
    modelName,
    folders,
    entry,
    destination,
    queue = null,
    queueItem = {},
  }) {
    if (queue !== null && !Array.isArray(queue)) {
      throw new Error('queueVideo queue must be an array when provided')
    }
    if (queue) {
      queue.push({
        url: entry.mediaUrl,
        path: destination.finalPath,
        tmpPath: destination.tmpPath,
        filename: entry.filename,
        uploadedDate: entry.uploadedDate,
        mediaPageUrl: entry.mediaPageUrl,
        pageMeta: entry.pageMeta,
        ...queueItem,
      })
    }
    onQueued({ modelName, folders, entry, destination })
    appendRunEvent(
      'queued_lazy_video',
      mediaSaver.buildQueuedEvent({ modelName, entry, destination })
    )
  }

  function isKnownOrExisting(destination, entry) {
    return (
      Boolean(knownFilenames?.has?.(entry.filename)) ||
      existsLocallyOrOnNas(destination.finalPath)
    )
  }

  function getExistingExtra(destination) {
    return {
      quarantinedMirrorExists: isQuarantinedPath(destination.finalPath),
    }
  }

  async function saveImageLikeMedia({
    modelName,
    folders,
    entry,
    destination,
    kind,
    downloadBuffer,
    getBitwiseDuplicationRecord,
    getVisualHashFromBuffer = null,
    getVisualDuplicationRecord = null,
    getFuzzyVisualDuplicationRecord = null,
    getPendingImageVisualDuplicate = null,
    reservePendingImageVisualClaim = null,
    releasePendingImageVisualClaim = null,
    addBitwiseHash,
    addVisualHash = null,
    saveBitwiseHashCache = null,
    saveVisualHashCache = null,
    shouldAddBitwiseHash = () => true,
    checkExistingBeforeDownload = true,
    duplicateRecordSeen = false,
    visualChecks = kind === 'image',
    fuzzyVisualDistance = null,
    pendingVisualDistance = null,
    addVisualHashBeforeSave = false,
    saveVisualHashCacheOnSave = false,
    pageMeta = entry.pageMeta,
  }) {
    if (typeof downloadBuffer !== 'function') {
      throw new Error('saveImageLikeMedia requires downloadBuffer')
    }
    if (typeof getBitwiseDuplicationRecord !== 'function') {
      throw new Error('saveImageLikeMedia requires getBitwiseDuplicationRecord')
    }
    if (typeof addBitwiseHash !== 'function') {
      throw new Error('saveImageLikeMedia requires addBitwiseHash')
    }

    const { bucket, finalPath, relativePath } = destination
    if (checkExistingBeforeDownload && isKnownOrExisting(destination, entry)) {
      const reason = `skip_existing_${kind}`
      recordDuplicate({
        modelName,
        folders,
        entry,
        destination,
        reason,
        extra: getExistingExtra(destination),
        savedPath: relativePath,
        recordSeen: duplicateRecordSeen,
      })
      return { status: 'duplicate', reason, destination }
    }

    const buffer = await downloadBuffer(entry.mediaUrl, entry)
    const hash = createHash('md5').update(buffer).digest('hex')
    const bitwiseMatch = getBitwiseDuplicationRecord(hash)
    if (bitwiseMatch.isDuplicate) {
      const reason = 'duplicate_bitwise'
      recordDuplicate({
        modelName,
        folders,
        entry,
        destination,
        reason,
        extra: {
          hash,
          activeRefs: bitwiseMatch.activeRefs.slice(0, 5),
        },
        savedPath: duplicateRecordSeen ? bitwiseMatch.activeRefs[0] : null,
        recordSeen: duplicateRecordSeen,
      })
      return { status: 'duplicate', reason, hash, match: bitwiseMatch }
    }

    let visualHash = null
    let visualClaimKey = null
    if (visualChecks && typeof getVisualHashFromBuffer === 'function') {
      visualHash = await getVisualHashFromBuffer(buffer)
      const visualMatch =
        visualHash && typeof getVisualDuplicationRecord === 'function'
          ? getVisualDuplicationRecord(visualHash)
          : null
      if (visualMatch?.isDuplicate) {
        const reason = 'duplicate_visual'
        recordDuplicate({
          modelName,
          folders,
          entry,
          destination,
          reason,
          extra: {
            visualHash,
            activeRefs: visualMatch.activeRefs.slice(0, 5),
          },
          savedPath: duplicateRecordSeen ? visualMatch.activeRefs[0] : null,
          recordSeen: duplicateRecordSeen,
        })
        return { status: 'duplicate', reason, visualHash, match: visualMatch }
      }

      const fuzzyMatch =
        visualHash &&
        Number.isFinite(fuzzyVisualDistance) &&
        typeof getFuzzyVisualDuplicationRecord === 'function'
          ? getFuzzyVisualDuplicationRecord(
              modelName,
              visualHash,
              fuzzyVisualDistance
            )
          : null
      if (fuzzyMatch?.isDuplicate) {
        const reason = 'duplicate_visual_fuzzy'
        recordDuplicate({
          modelName,
          folders,
          entry,
          destination,
          reason,
          extra: {
            visualHash,
            matchedVisualHash: fuzzyMatch.matchedHash,
            distance: fuzzyMatch.distance,
          },
          savedPath: duplicateRecordSeen ? fuzzyMatch.activeRefs[0] : null,
          recordSeen: duplicateRecordSeen,
        })
        return { status: 'duplicate', reason, visualHash, match: fuzzyMatch }
      }

      const pendingMatch =
        visualHash &&
        Number.isFinite(pendingVisualDistance) &&
        typeof getPendingImageVisualDuplicate === 'function'
          ? getPendingImageVisualDuplicate(
              modelName,
              visualHash,
              pendingVisualDistance
            )
          : null
      if (pendingMatch?.isDuplicate) {
        const reason = 'duplicate_visual_pending'
        recordDuplicate({
          modelName,
          folders,
          entry,
          destination,
          reason,
          extra: {
            visualHash,
            matchedVisualHash: pendingMatch.matchedHash,
            distance: pendingMatch.distance,
          },
          savedPath: duplicateRecordSeen ? pendingMatch.activeRefs[0] : null,
          recordSeen: duplicateRecordSeen,
        })
        return { status: 'duplicate', reason, visualHash, match: pendingMatch }
      }

      if (
        visualHash &&
        addVisualHashBeforeSave &&
        typeof addVisualHash === 'function'
      ) {
        addVisualHash(visualHash)
      }

      if (visualHash && typeof reservePendingImageVisualClaim === 'function') {
        visualClaimKey = reservePendingImageVisualClaim(
          modelName,
          relativePath,
          visualHash
        )
      }
    }

    if (!checkExistingBeforeDownload && isKnownOrExisting(destination, entry)) {
      const reason = `skip_existing_${kind}`
      recordDuplicate({
        modelName,
        folders,
        entry,
        destination,
        reason,
        extra: getExistingExtra(destination),
        savedPath: relativePath,
        recordSeen: duplicateRecordSeen,
      })
      return { status: 'duplicate', reason, destination }
    }

    try {
      fs.writeFileSync(finalPath, buffer)
      const { metadata, recordedDate, fileDate } =
        await mediaSaver.finalizeImage({
          modelName,
          bucket,
          filename: entry.filename,
          buffer,
          absolutePath: finalPath,
          mediaType: kind === 'gif' ? 'gif' : 'image',
          uploadedDate: entry.uploadedDate,
          pageMeta,
          entry,
        })

      if (shouldAddBitwiseHash({ hash, metadata, entry, destination })) {
        addBitwiseHash(hash, metadata)
        saveBitwiseHashCache?.()
      }
      if (visualHash && typeof addVisualHash === 'function') {
        addVisualHash(visualHash, metadata)
      }
      if (visualHash && saveVisualHashCacheOnSave) {
        saveVisualHashCache?.()
      }

      const stats = recordSaved({
        modelName,
        folders,
        entry,
        destination,
        sizeBytes: buffer.length,
        hash,
        visualHash,
        kind,
      })
      return {
        status: 'saved',
        destination,
        hash,
        visualHash,
        metadata,
        recordedDate,
        fileDate,
        stats,
      }
    } finally {
      releasePendingImageVisualClaim?.(visualClaimKey)
    }
  }

  return {
    getDestination,
    getExistingExtra,
    getSeenMediaMatch,
    isKnownOrExisting,
    queueVideo,
    recordDuplicate,
    recordMediaSeen,
    recordOutcome,
    recordSaved,
    saveImageLikeMedia,
  }
}

module.exports = {
  createMediaSavePipeline,
}

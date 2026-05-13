'use strict'

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
  }
}

module.exports = {
  createMediaSavePipeline,
}

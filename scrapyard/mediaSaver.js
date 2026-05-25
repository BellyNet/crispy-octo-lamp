'use strict'

const path = require('path')
const fs = require('fs')
const mediaFileRecords = require('./mediaFileRecords')

function createMediaSaver({
  datasetDir,
  source,
  mediaDates,
  fileRecords = mediaFileRecords,
  getExtraMetadata = () => ({}),
  getEventMetadata = getExtraMetadata,
  getSeenDetails = getDefaultSeenDetails,
}) {
  if (!datasetDir) throw new Error('datasetDir is required')
  if (!source) throw new Error('source is required')
  if (!mediaDates) throw new Error('mediaDates is required')

  function getModelDir(modelName) {
    return path.join(datasetDir, modelName)
  }

  function getDatasetRelativePath(filePath) {
    return path.relative(datasetDir, filePath).replace(/\\/g, '/')
  }

  function getKindInfo(kind) {
    if (kind === 'gif') {
      return {
        bucket: 'gif',
        mediaType: 'gif',
        savedEventType: 'saved_gif',
        savedOutcome: 'saved_gif',
      }
    }
    if (kind === 'video') {
      return {
        bucket: 'webm',
        mediaType: 'video',
        savedEventType: 'saved_lazy_video',
        savedOutcome: 'saved_video',
      }
    }
    return {
      bucket: 'images',
      mediaType: 'image',
      savedEventType: 'saved_image',
      savedOutcome: 'saved_image',
    }
  }

  function getBucketDir({ modelName, folders, bucket, create = true }) {
    if (bucket === 'images' && folders?.images) return folders.images
    if (bucket === 'gif' && create && folders?.createGifFolder) {
      return folders.createGifFolder()
    }
    if (bucket === 'webm' && create && folders?.createWebmFolder) {
      return folders.createWebmFolder()
    }

    return path.join(folders?.base || getModelDir(modelName), bucket)
  }

  function getDestination({
    modelName,
    folders,
    filename,
    kind,
    create = true,
  }) {
    const info = getKindInfo(kind)
    const finalDir = getBucketDir({
      modelName,
      folders,
      bucket: info.bucket,
      create,
    })
    if (create) fs.mkdirSync(finalDir, { recursive: true })
    const finalPath = path.join(finalDir, filename)
    const relativePath = getDatasetRelativePath(finalPath)
    const tmpPath =
      kind === 'video' && folders?.incompleteVideoDir
        ? path.join(folders.incompleteVideoDir, filename)
        : kind === 'gif' && folders?.incompleteGifDir
          ? path.join(folders.incompleteGifDir, filename)
          : null

    return {
      ...info,
      filename,
      finalDir,
      finalPath,
      relativePath,
      tmpPath,
      extension: path.extname(filename).toLowerCase(),
    }
  }

  async function recordImageDates({
    modelName,
    bucket,
    filename,
    buffer,
    uploadedDate,
    pageMeta,
  }) {
    return mediaDates.recordImageDates(
      getModelDir(modelName),
      bucket,
      filename,
      buffer,
      uploadedDate,
      pageMeta
    )
  }

  async function recordVideoDates({
    modelName,
    bucket = 'webm',
    filename,
    filePath,
    uploadedDate,
    pageMeta,
  }) {
    return mediaDates.recordVideoDates(
      getModelDir(modelName),
      bucket,
      filename,
      filePath,
      uploadedDate,
      pageMeta
    )
  }

  function applyRecordedTimestamp(filePath, recordedDate, fallbackDate) {
    return fileRecords.applyFileTimestamp(
      filePath,
      fileRecords.parseResolvedDate(recordedDate?.date) || fallbackDate
    )
  }

  function buildHashMetadata({
    modelName,
    absolutePath,
    mediaType,
    sizeBytes,
    modifiedAt,
    entry = {},
    extra = {},
  }) {
    return fileRecords.buildHashMetadata({
      datasetDir,
      source,
      modelName,
      absolutePath,
      mediaType,
      sizeBytes,
      modifiedAt,
      extra: {
        ...getExtraMetadata(entry),
        ...extra,
      },
    })
  }

  function buildSeenRecord(entry, destination, extra = {}) {
    return {
      relativePath: destination.relativePath,
      filename: destination.filename || entry?.filename || null,
      ...getSeenDetails(entry),
      ...extra,
    }
  }

  function buildMediaSeenEvent({ modelName, entry, destination }) {
    return {
      modelName,
      ...getSeenDetails(entry),
      ...getEventMetadata(entry),
      filename: destination.filename,
      extension: destination.extension,
      bucket: destination.bucket,
      candidateRelativePath: destination.relativePath,
    }
  }

  function buildDuplicateEvent({ entry, savedPath, extra = {} }) {
    return {
      filename: entry.filename,
      ...getSeenDetails(entry),
      ...getEventMetadata(entry),
      savedPath,
      ...extra,
    }
  }

  function getOutcomeKindForReason(reason) {
    return String(reason || '').startsWith('skip_') ? 'skipped' : 'duplicate'
  }

  function buildSavedEvent({
    modelName,
    entry,
    destination,
    hash = null,
    visualHash = null,
    extra = {},
  }) {
    return {
      modelName,
      filename: destination.filename,
      savedPath: destination.relativePath,
      hash,
      visualHash,
      ...getEventMetadata(entry),
      ...extra,
    }
  }

  function buildQueuedEvent({ modelName, entry, destination, extra = {} }) {
    return {
      modelName,
      filename: destination.filename,
      ...getSeenDetails(entry),
      ...getEventMetadata(entry),
      savedPath: destination.relativePath,
      ...extra,
    }
  }

  function buildErrorEvent({
    modelName,
    entry,
    destination,
    error,
    extra = {},
  }) {
    return {
      modelName,
      filename: destination?.filename || entry?.filename || null,
      ...getSeenDetails(entry),
      ...getEventMetadata(entry),
      savedPath: destination?.relativePath || null,
      error: error instanceof Error ? error.message : String(error || ''),
      ...extra,
    }
  }

  function buildSavedStats({ sizeBytes, kind }) {
    const bytes = Number.isFinite(sizeBytes) && sizeBytes > 0 ? sizeBytes : 0
    return {
      savedBytes: bytes,
      lazyTransferredBytes: kind === 'video' ? bytes : 0,
    }
  }

  async function finalizeImage({
    modelName,
    bucket,
    filename,
    buffer,
    absolutePath,
    mediaType,
    uploadedDate,
    pageMeta,
    entry,
    extra,
  }) {
    const recordedDate = await recordImageDates({
      modelName,
      bucket,
      filename,
      buffer,
      uploadedDate,
      pageMeta,
    })
    const fileDate = applyRecordedTimestamp(
      absolutePath,
      recordedDate,
      uploadedDate
    )
    const metadata = buildHashMetadata({
      modelName,
      absolutePath,
      mediaType,
      sizeBytes: buffer.length,
      modifiedAt: fileDate,
      entry,
      extra,
    })

    return { recordedDate, fileDate, metadata }
  }

  async function finalizeVideo({
    modelName,
    bucket = 'webm',
    filename,
    filePath,
    mediaType = 'video',
    sizeBytes,
    uploadedDate,
    pageMeta,
    entry,
    extra,
  }) {
    const recordedDate = await recordVideoDates({
      modelName,
      bucket,
      filename,
      filePath,
      uploadedDate,
      pageMeta,
    })
    const fileDate = applyRecordedTimestamp(
      filePath,
      recordedDate,
      uploadedDate
    )
    const metadata = buildHashMetadata({
      modelName,
      absolutePath: filePath,
      mediaType,
      sizeBytes,
      modifiedAt: fileDate,
      entry,
      extra,
    })

    return { recordedDate, fileDate, metadata }
  }

  return {
    recordImageDates,
    recordVideoDates,
    applyRecordedTimestamp,
    buildHashMetadata,
    getDatasetRelativePath,
    getKindInfo,
    getDestination,
    buildSeenRecord,
    buildMediaSeenEvent,
    buildDuplicateEvent,
    getOutcomeKindForReason,
    buildSavedEvent,
    buildQueuedEvent,
    buildErrorEvent,
    buildSavedStats,
    finalizeImage,
    finalizeVideo,
  }
}

function getDefaultSeenDetails(entry = {}) {
  const mediaUrls = Array.isArray(entry.mediaUrls)
    ? entry.mediaUrls
    : [entry.mediaUrl].filter(Boolean)
  const mediaPageUrls = Array.isArray(entry.mediaPageUrls)
    ? entry.mediaPageUrls
    : [entry.mediaPageUrl].filter(Boolean)

  return {
    mediaUrl: entry.mediaUrl || mediaUrls[0] || null,
    mediaPageUrl: entry.mediaPageUrl || mediaPageUrls[0] || null,
    mediaUrls,
    mediaPageUrls,
  }
}

module.exports = {
  createMediaSaver,
  getDefaultSeenDetails,
}

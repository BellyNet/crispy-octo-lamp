'use strict'

const express = require('express')
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const sharp = require('sharp')

const app = express()
const PORT = Number.parseInt(process.env.ORIENTATION_REVIEW_PORT || '4780', 10)

const APPDATA =
  process.env.APPDATA ||
  path.join(process.env.HOME || process.env.USERPROFILE, 'AppData', 'Roaming')
const slopvaultRoot = path.join(APPDATA, '.slopvault')
const datasetDir = process.env.DATASET_DIR || path.join(slopvaultRoot, 'dataset')
const statePath = path.join(
  __dirname,
  '..',
  'audit',
  'manifests',
  'slopvault-orientation-review-state.json'
)
const appHtmlPath = path.join(__dirname, 'orientation-review.html')
const reviewToken = crypto.randomBytes(16).toString('hex')

const IMAGE_EXTENSIONS = new Set(['.jpg', '.jpeg', '.png', '.webp'])
const recordCache = new Map()

main().catch((err) => {
  console.error(`Fatal orientation review error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  fs.mkdirSync(path.dirname(statePath), { recursive: true })
  app.use(express.json({ limit: '2mb' }))

  app.get('/', (_req, res) => {
    res.type('html').send(fs.readFileSync(appHtmlPath, 'utf8'))
  })

  app.get('/media', authorize, (req, res) => {
    sendMedia(res, req.query.path)
  })

  app.get('/api/state', authorize, async (req, res) => {
    const requestedModel = String(req.query.model || '')
    const mode = normalizeMode(req.query.mode)
    const targetRecordId = String(req.query.recordId || '')
    const payload = await buildStatePayload({
      requestedModel,
      mode,
      targetRecordId,
    })
    res.json({ ok: true, ...payload })
  })

  app.post('/api/models/:model/records/:recordId/advance', authorize, async (req, res) => {
    const model = String(req.params.model || '')
    const recordId = String(req.params.recordId || '')
    const mode = normalizeMode(req.body?.mode)
    const payload = await saveProgress({
      model,
      recordId,
      mode,
      advance: true,
    })
    res.json({ ok: true, ...payload })
  })

  app.post('/api/models/:model/records/:recordId/rotate', authorize, async (req, res) => {
    const model = String(req.params.model || '')
    const recordId = String(req.params.recordId || '')
    const direction = req.body?.direction === 'ccw' ? 'ccw' : 'cw'
    const mode = normalizeMode(req.body?.mode)
    const advance = Boolean(req.body?.advance)

    const records = await loadModelRecords(model)
    const record = records.find((item) => item.id === recordId)
    if (!record) {
      return res.status(404).json({ ok: false, error: `Unknown record: ${recordId}` })
    }

    await rotateImage(record.filePath, direction)
    recordCache.delete(model)

    const payload = await saveProgress({
      model,
      recordId,
      mode,
      advance,
      rotation: direction,
    })
    res.json({ ok: true, ...payload })
  })

  app.listen(PORT, '127.0.0.1', () => {
    const url = `http://127.0.0.1:${PORT}/?token=${reviewToken}`
    console.log(`Orientation review app: ${url}`)
    console.log('Keep this process running while you review image rotations.')
  })
}

function authorize(req, res, next) {
  const token = req.headers['x-slopvault-token'] || req.query.token
  if (token && token !== reviewToken) {
    return res.status(401).json({ ok: false, error: 'Unauthorized review token.' })
  }
  next()
}

function normalizeMode(value) {
  void value
  return 'all'
}

function loadState() {
  if (!fs.existsSync(statePath)) {
    return { version: 1, updatedAt: null, models: {} }
  }

  try {
    const parsed = JSON.parse(fs.readFileSync(statePath, 'utf8'))
    return {
      version: 1,
      updatedAt: parsed.updatedAt || null,
      models: parsed.models && typeof parsed.models === 'object' ? parsed.models : {},
    }
  } catch {
    return { version: 1, updatedAt: null, models: {} }
  }
}

function saveState(state) {
  state.updatedAt = new Date().toISOString()
  fs.writeFileSync(statePath, JSON.stringify(state, null, 2))
}

function getModelState(state, model) {
  if (!state.models[model]) {
    state.models[model] = {
      reviewedIds: [],
      currentRecordIdByMode: {},
      rotations: [],
    }
  }
  return state.models[model]
}

async function buildStatePayload({ requestedModel, mode, targetRecordId }) {
  const models = await listModels()
  const state = loadState()

  const activeModel =
    requestedModel && models.includes(requestedModel)
      ? requestedModel
      : models[0] || null

  const modelSummaries = []
  for (const model of models) {
    const records = await loadModelRecords(model)
    const modelState = getModelState(state, model)
    modelSummaries.push(summarizeModel(model, records, modelState))
  }

  if (!activeModel) {
    return {
      models: modelSummaries,
      currentModel: null,
      mode,
      summary: { total: 0, reviewed: 0, percent: 100 },
      currentRecord: null,
      hasPrevious: false,
      hasNext: false,
    }
  }

  const records = await loadModelRecords(activeModel)
  const modelState = getModelState(state, activeModel)
  const visibleRecords = filterRecords(records, mode)
  const reviewSet = new Set(modelState.reviewedIds || [])
  const currentRecordId =
    targetRecordId && visibleRecords.some((record) => record.id === targetRecordId)
      ? targetRecordId
      : resolveCurrentRecordId(visibleRecords, modelState, mode, reviewSet)

  const currentIndex = visibleRecords.findIndex((record) => record.id === currentRecordId)
  const currentRecord =
    currentIndex >= 0
      ? decorateRecord(
          visibleRecords[currentIndex],
          currentIndex,
          visibleRecords.length,
          reviewSet.has(visibleRecords[currentIndex].id)
        )
      : null

  return {
    models: modelSummaries,
    currentModel: activeModel,
    mode,
    summary: {
      total: visibleRecords.length,
      reviewed: visibleRecords.filter((record) => reviewSet.has(record.id)).length,
      percent: visibleRecords.length
        ? Math.round(
            (visibleRecords.filter((record) => reviewSet.has(record.id)).length /
              visibleRecords.length) *
              1000
          ) / 10
        : 100,
    },
    currentRecord,
    hasPrevious: currentIndex > 0,
    hasNext: currentIndex >= 0 && currentIndex < visibleRecords.length - 1,
    previousRecordId: currentIndex > 0 ? visibleRecords[currentIndex - 1].id : null,
    nextRecordId:
      currentIndex >= 0 && currentIndex < visibleRecords.length - 1
        ? visibleRecords[currentIndex + 1].id
        : null,
  }
}

function summarizeModel(model, records, modelState) {
  const reviewSet = new Set(modelState.reviewedIds || [])
  const suspectRecords = filterRecords(records, 'suspects')

  return {
    model,
    counts: {
      all: records.length,
      suspects: suspectRecords.length,
      reviewedAll: records.filter((record) => reviewSet.has(record.id)).length,
      reviewedSuspects: suspectRecords.filter((record) => reviewSet.has(record.id)).length,
    },
  }
}

function filterRecords(records, mode) {
  if (mode === 'all') return records
  return records.filter((record) => record.suspect)
}

function resolveCurrentRecordId(records, modelState, mode, reviewSet) {
  const savedRecordId = modelState.currentRecordIdByMode?.[mode]
  if (savedRecordId && records.some((record) => record.id === savedRecordId)) {
    return savedRecordId
  }

  const nextUnreviewed = records.find((record) => !reviewSet.has(record.id))
  return nextUnreviewed?.id || null
}

function decorateRecord(record, index, total, reviewed) {
  return {
    ...record,
    index,
    total,
    reviewed,
  }
}

async function saveProgress({ model, recordId, mode, advance, rotation }) {
  const state = loadState()
  const modelState = getModelState(state, model)
  const records = await loadModelRecords(model)
  const visibleRecords = filterRecords(records, mode)
  const currentIndex = visibleRecords.findIndex((record) => record.id === recordId)

  if (currentIndex < 0) {
    throw new Error(`Unknown record for ${model}: ${recordId}`)
  }

  const reviewSet = new Set(modelState.reviewedIds || [])
  reviewSet.add(recordId)
  modelState.reviewedIds = Array.from(reviewSet)

  if (rotation) {
    modelState.rotations = Array.isArray(modelState.rotations) ? modelState.rotations : []
    modelState.rotations.push({
      recordId,
      direction: rotation,
      at: new Date().toISOString(),
    })
    modelState.rotations = modelState.rotations.slice(-5000)
  }

  const nextRecord = advance
    ? visibleRecords.slice(currentIndex + 1).find((record) => !reviewSet.has(record.id)) ||
      visibleRecords[currentIndex + 1] ||
      null
    : visibleRecords[currentIndex]

  modelState.currentRecordIdByMode = {
    ...(modelState.currentRecordIdByMode || {}),
    [mode]: nextRecord?.id || null,
  }

  saveState(state)
  return buildStatePayload({
    requestedModel: model,
    mode,
    targetRecordId: nextRecord?.id || null,
  })
}

async function listModels() {
  const entries = await fs.promises.readdir(datasetDir, { withFileTypes: true })
  return entries
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith('.'))
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }))
}

async function loadModelRecords(model) {
  if (recordCache.has(model)) {
    return recordCache.get(model)
  }

  const imagesDir = safeSubPath(datasetDir, model, 'images')
  if (!imagesDir) {
    recordCache.set(model, [])
    return []
  }

  let filenames = []
  try {
    filenames = await fs.promises.readdir(imagesDir)
  } catch {
    recordCache.set(model, [])
    return []
  }

  const records = []
  for (const filename of filenames.sort((a, b) => a.localeCompare(b))) {
    const ext = path.extname(filename).toLowerCase()
    if (!IMAGE_EXTENSIONS.has(ext)) continue

    const filePath = safeSubPath(imagesDir, filename)
    if (!filePath) continue

    try {
      const [stat, metadata] = await Promise.all([
        fs.promises.stat(filePath),
        sharp(filePath).metadata(),
      ])

      const width = Number(metadata.width || 0)
      const height = Number(metadata.height || 0)
      const suspect = width > 0 && height > 0 && width > height * 1.08

      records.push({
        id: `images/${filename}`,
        filename,
        filePath,
        relativePath: `${model}/images/${filename}`.replace(/\\/g, '/'),
        mediaUrl: `/media?path=${encodeURIComponent(filePath)}`,
        width,
        height,
        suspect,
        sizeBytes: stat.size,
        modifiedAt: stat.mtime.toISOString(),
      })
    } catch {}
  }

  recordCache.set(model, records)
  return records
}

async function rotateImage(filePath, direction) {
  const stat = await fs.promises.stat(filePath)
  const angle = direction === 'ccw' ? -90 : 90
  const tempPath = `${filePath}.rotate-tmp`

  const buffer = await sharp(filePath)
    .rotate(angle)
    .withMetadata({ orientation: 1 })
    .toBuffer()

  await fs.promises.writeFile(tempPath, buffer)
  await fs.promises.rename(tempPath, filePath)
  await fs.promises.utimes(filePath, stat.atime, stat.mtime)
}

function safeSubPath(base, ...parts) {
  const resolved = path.resolve(path.join(base, ...parts))
  const baseResolved = path.resolve(base)
  if (resolved !== baseResolved && !resolved.startsWith(baseResolved + path.sep)) return null
  return resolved
}

function sendMedia(res, filePath) {
  const resolved = path.resolve(String(filePath || ''))
  if (!resolved || !fs.existsSync(resolved)) {
    return res.status(404).json({ ok: false, error: `Missing media: ${resolved}` })
  }

  const stat = fs.statSync(resolved)
  const ext = path.extname(resolved).toLowerCase()
  const contentType =
    ext === '.jpg' || ext === '.jpeg'
      ? 'image/jpeg'
      : ext === '.png'
        ? 'image/png'
        : ext === '.webp'
          ? 'image/webp'
          : 'application/octet-stream'

  res.writeHead(200, {
    'content-type': contentType,
    'content-length': stat.size,
    'cache-control': 'no-store',
  })
  fs.createReadStream(resolved).pipe(res)
}

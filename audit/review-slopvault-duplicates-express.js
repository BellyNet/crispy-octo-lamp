'use strict'

const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const { spawn } = require('child_process')
const express = require('express')

const app = express()
const PORT = Number.parseInt(process.env.DUPLICATE_REVIEW_PORT || '4779', 10)

const auditDir = __dirname
const rootDir = path.join(auditDir, '..')
const manifestDir = path.join(auditDir, 'manifests')
const dashboardDir = path.join(auditDir, 'dashboard')
const sourceManifestPath = path.join(manifestDir, 'slopvault-manifest-latest.json')
const duplicateManifestPath = path.join(
  manifestDir,
  'slopvault-duplicate-manifest-latest.json'
)
const decisionsPath = path.join(
  manifestDir,
  'slopvault-duplicate-dashboard-decisions-latest.json'
)
const appHtmlPath = path.join(auditDir, 'duplicate-review-app.html')
const reviewToken = crypto.randomBytes(16).toString('hex')

main().catch((err) => {
  console.error(`Fatal duplicate express review error: ${err.stack || err.message}`)
  process.exitCode = 1
})

async function main() {
  await ensureManifest()

  app.use(express.json({ limit: '10mb' }))

  app.get('/', (_req, res) => {
    res.type('html').send(fs.readFileSync(appHtmlPath, 'utf8'))
  })

  app.get('/media', authorize, (req, res) => {
    sendMedia(res, req.query.path)
  })

  app.get('/api/state', authorize, (_req, res) => {
    const payload = buildStatePayload(String(_req.query.groupId || ''))
    res.json({ ok: true, ...payload })
  })

  app.post('/api/groups/:groupId', authorize, (req, res) => {
    const groupId = String(req.params.groupId || '')
    const keptRecordIds = Array.isArray(req.body?.keptRecordIds)
      ? req.body.keptRecordIds.map((value) => String(value))
      : []
    const focusedRecordId = req.body?.focusedRecordId
      ? String(req.body.focusedRecordId)
      : null
    const advance = Boolean(req.body?.advance)

    const manifest = loadDuplicateManifest()
    const group = manifest.groups.find((item) => item.id === groupId)
    if (!group) {
      return res.status(404).json({ ok: false, error: `Unknown group: ${groupId}` })
    }

    const validKeepIds = group.records
      .filter((record) => keptRecordIds.includes(record.id))
      .map((record) => record.id)
    const nextKeepIds = validKeepIds.length
      ? validKeepIds
      : [group.suggestedKeepId]

    const store = loadDecisionStore()
    upsertDecisionGroup(store, {
      groupId,
      keptRecordIds: nextKeepIds,
      focusedRecordId: focusedRecordId && group.records.some((record) => record.id === focusedRecordId)
        ? focusedRecordId
        : nextKeepIds[0],
      reviewedAt: new Date().toISOString(),
    })
    saveDecisionStore(store)

    const payload = buildStatePayload(
      advance ? nextGroupId(manifest, groupId) : groupId
    )
    res.json({ ok: true, ...payload })
  })

  app.post('/api/apply', authorize, async (_req, res) => {
    await ensureManifest()
    const manifest = loadDuplicateManifest()
    const store = loadDecisionStore()
    const decisions = []

    for (const group of manifest.groups) {
      const saved = store.groups.find((item) => item.groupId === group.id)
      const keptIds = Array.isArray(saved?.keptRecordIds) && saved.keptRecordIds.length
        ? saved.keptRecordIds
        : [group.suggestedKeepId]

      for (const record of group.records) {
        decisions.push({
          id: `exact_duplicate:${record.relativePath}`,
          action: keptIds.includes(record.id) ? 'keep' : 'quarantine',
          reviewType: 'exact_duplicate',
          sourceType: 'dataset',
          mediaType: record.mediaType,
          filename: record.filename,
          sourcePath: record.sourcePath,
          relativePath: record.relativePath,
          quarantinePath: record.quarantinePath,
          reasons: group.crossModel
            ? ['exact_duplicate', 'cross_model_duplicate']
            : ['exact_duplicate'],
          quarantineEligible: true,
          contentHash: group.contentHash,
        })
      }
    }

    fs.writeFileSync(
      decisionsPath,
      JSON.stringify(
        {
          version: 2,
          updatedAt: new Date().toISOString(),
          groups: store.groups,
          decisions,
        },
        null,
        2
      )
    )

    await runNodeScript('audit-slopvault.js', ['--apply', '--decisions', decisionsPath])
    await runNodeScript('manifest-slopvault.js', ['--hash'])
    await runNodeScript('manifest-slopvault-duplicates.js', [])

    res.json({ ok: true })
  })

  app.listen(PORT, '127.0.0.1', () => {
    const url = `http://127.0.0.1:${PORT}/?token=${reviewToken}`
    console.log(`Duplicate review app: ${url}`)
    console.log('Keep this process running while you review duplicates.')
  })
}

async function ensureManifest() {
  if (!fs.existsSync(sourceManifestPath)) {
    await runNodeScript('manifest-slopvault.js', ['--hash'])
  } else {
    const source = JSON.parse(fs.readFileSync(sourceManifestPath, 'utf8'))
    if (source.hashAlgorithm !== 'md5') {
      await runNodeScript('manifest-slopvault.js', ['--hash'])
    }
  }

  if (!fs.existsSync(duplicateManifestPath)) {
    await runNodeScript('manifest-slopvault-duplicates.js', [])
  }
}

function authorize(req, res, next) {
  const token = req.headers['x-slopvault-token'] || req.query.token
  if (token && token !== reviewToken) {
    return res.status(401).json({ ok: false, error: 'Unauthorized review token.' })
  }
  next()
}

function loadDuplicateManifest() {
  return JSON.parse(fs.readFileSync(duplicateManifestPath, 'utf8'))
}

function loadDecisionStore() {
  if (!fs.existsSync(decisionsPath)) {
    return { version: 2, updatedAt: null, groups: [] }
  }

  try {
    const parsed = JSON.parse(fs.readFileSync(decisionsPath, 'utf8'))
    return {
      version: 2,
      updatedAt: parsed.updatedAt || null,
      groups: Array.isArray(parsed.groups) ? parsed.groups : [],
    }
  } catch {
    return { version: 2, updatedAt: null, groups: [] }
  }
}

function saveDecisionStore(store) {
  store.updatedAt = new Date().toISOString()
  fs.writeFileSync(decisionsPath, JSON.stringify(store, null, 2))
}

function upsertDecisionGroup(store, nextGroup) {
  const index = store.groups.findIndex((group) => group.groupId === nextGroup.groupId)
  if (index >= 0) {
    store.groups[index] = { ...store.groups[index], ...nextGroup }
  } else {
    store.groups.push(nextGroup)
  }
}

function buildStatePayload(targetGroupId) {
  const manifest = loadDuplicateManifest()
  const reviewGroups = manifest.groups.filter((group) => group.crossModel)
  const store = loadDecisionStore()
  const order = reviewGroups.map((group) => group.id)
  const reviewedSet = new Set(
    store.groups
      .filter((group) => Array.isArray(group.keptRecordIds) && group.keptRecordIds.length)
      .filter((group) => order.includes(group.groupId))
      .map((group) => group.groupId)
  )

  const defaultGroupId =
    reviewedSet.size >= order.length
      ? null
      : targetGroupId && order.includes(targetGroupId)
        ? targetGroupId
        : order.find((groupId) => !reviewedSet.has(groupId)) || null

  const currentIndex = defaultGroupId ? order.indexOf(defaultGroupId) : -1
  const currentGroup = defaultGroupId
    ? materializeGroup(
        reviewGroups.find((group) => group.id === defaultGroupId),
        store.groups.find((group) => group.groupId === defaultGroupId)
      )
    : null

  return {
    summary: {
      totalGroups: order.length,
      reviewedGroups: reviewedSet.size,
      percentReviewed: order.length
        ? Math.round((reviewedSet.size / order.length) * 1000) / 10
        : 100,
    },
    groupOrder: order,
    currentIndex,
    hasPrevious: currentIndex > 0,
    hasNext: currentIndex >= 0 && currentIndex < order.length - 1,
    currentGroup,
  }
}

function materializeGroup(group, saved) {
  if (!group) return null

  const keptRecordIds = Array.isArray(saved?.keptRecordIds) && saved.keptRecordIds.length
    ? saved.keptRecordIds.filter((recordId) =>
        group.records.some((record) => record.id === recordId)
      )
    : group.currentKeptIds

  const focusedRecordId =
    saved?.focusedRecordId &&
    group.records.some((record) => record.id === saved.focusedRecordId)
      ? saved.focusedRecordId
      : keptRecordIds[0] || group.suggestedKeepId

  return {
    ...group,
    keptRecordIds,
    focusedRecordId,
    reviewedAt: saved?.reviewedAt || null,
  }
}

function nextGroupId(manifest, currentGroupId) {
  const order = manifest.groups.map((group) => group.id)
  const index = order.indexOf(currentGroupId)
  if (index < 0) return currentGroupId
  return order[index + 1] || currentGroupId
}

function sendMedia(res, filePath) {
  const resolved = path.resolve(String(filePath || ''))
  if (!resolved || !fs.existsSync(resolved)) {
    return res.status(404).json({ ok: false, error: `Missing media: ${resolved}` })
  }

  const stat = fs.statSync(resolved)
  const ext = path.extname(resolved).toLowerCase()
  const contentType =
    ext === '.gif' ? 'image/gif' :
    ext === '.jpg' || ext === '.jpeg' ? 'image/jpeg' :
    ext === '.png' ? 'image/png' :
    ext === '.webp' ? 'image/webp' :
    ext === '.webm' ? 'video/webm' :
    ext === '.mp4' || ext === '.m4v' ? 'video/mp4' :
    ext === '.mov' ? 'video/quicktime' :
    'application/octet-stream'

  res.writeHead(200, {
    'content-type': contentType,
    'content-length': stat.size,
    'cache-control': 'no-store',
    'accept-ranges': 'bytes',
  })
  fs.createReadStream(resolved).pipe(res)
}

function runNodeScript(scriptName, args) {
  return new Promise((resolve, reject) => {
    console.log(`> node audit/${scriptName} ${args.join(' ')}`.trim())

    const child = spawn(
      process.execPath,
      [path.join(auditDir, scriptName), ...args],
      { cwd: rootDir, stdio: 'inherit' }
    )

    child.on('error', reject)
    child.on('exit', (code) => {
      if (code === 0) return resolve()
      reject(new Error(`${scriptName} exited with code ${code}`))
    })
  })
}

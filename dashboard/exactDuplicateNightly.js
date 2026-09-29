'use strict'

const fs = require('fs')
const path = require('path')
const { spawn } = require('child_process')

function runNode(script, args, logPath) {
  return new Promise((resolve, reject) => {
    const log = fs.createWriteStream(logPath, { flags: 'a' })
    const child = spawn(process.execPath, [script, ...args], {
      cwd: path.join(__dirname, '..'),
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    child.stdout.pipe(log, { end: false })
    child.stderr.pipe(log, { end: false })
    child.on('error', reject)
    child.on('close', (code) => {
      log.end()
      if (code === 0) resolve()
      else reject(new Error(`${path.basename(script)} exited ${code}; see ${logPath}`))
    })
  })
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'))
}

function migrateDecisions(previous, review) {
  const groups = {}
  for (const group of review.crossModelGroups) {
    const choice = previous?.groups?.[group.id]
    if (choice && group.models.includes(choice.keepModel)) groups[group.id] = choice
  }
  return { version: 2, auditedAt: review.auditedAt, groups }
}

async function refreshExactDuplicateReview({ datasetDir, thumbDir }) {
  const auditScript = path.join(__dirname, '..', 'audit', 'audit-exact-media-duplicates.js')
  const exportScript = path.join(__dirname, '..', 'audit', 'export-exact-media-review.js')
  const auditPath = path.join(thumbDir, 'exact-media-audit-latest.json')
  const reviewPath = path.join(thumbDir, 'exact-media-review-latest.json')
  const decisionsPath = path.join(thumbDir, 'exact-duplicate-decisions.json')
  const cachePath = path.join(thumbDir, 'exact-media-hash-cache.json')
  const logPath = path.join(thumbDir, 'exact-media-nightly.log')
  const auditWorking = `${auditPath}.${process.pid}.working`
  const reviewWorking = `${reviewPath}.${process.pid}.working`
  fs.writeFileSync(logPath, `Exact duplicate audit started ${new Date().toISOString()}\n`)
  try {
    await runNode(auditScript, [
      '--nas-only', '--nas-all-media', '--concurrency=1',
      `--nas-root=${datasetDir}`, `--cache=${cachePath}`, `--output=${auditWorking}`,
    ], logPath)
    const audit = readJson(auditWorking)
    if (audit.summary.scanErrors || audit.summary.hashErrors || audit.summary.mirrorConflicts) {
      throw new Error(`Exact duplicate audit had ${audit.summary.scanErrors} scan, ${audit.summary.hashErrors} hash, and ${audit.summary.mirrorConflicts} mirror errors`)
    }
    await runNode(exportScript, [auditWorking, reviewWorking], logPath)
    const review = readJson(reviewWorking)
    let previous = null
    try { previous = readJson(decisionsPath) } catch {}
    const decisions = migrateDecisions(previous, review)
    const decisionsWorking = `${decisionsPath}.${process.pid}.working`
    fs.writeFileSync(decisionsWorking, JSON.stringify(decisions, null, 2) + '\n')
    // A backup preserves the prior review and choices if either publish fails.
    const backupDir = path.join(thumbDir, 'exact-media-review-backup')
    fs.mkdirSync(backupDir, { recursive: true })
    for (const filePath of [reviewPath, decisionsPath]) {
      if (fs.existsSync(filePath)) fs.copyFileSync(filePath, path.join(backupDir, path.basename(filePath)))
    }
    try {
      fs.renameSync(auditWorking, auditPath)
      fs.renameSync(reviewWorking, reviewPath)
      fs.renameSync(decisionsWorking, decisionsPath)
    } catch (error) {
      for (const filePath of [reviewPath, decisionsPath]) {
        const backup = path.join(backupDir, path.basename(filePath))
        if (fs.existsSync(backup)) fs.copyFileSync(backup, filePath)
        else if (fs.existsSync(filePath)) fs.unlinkSync(filePath)
      }
      throw error
    }
    return {
      auditedAt: review.auditedAt,
      exactGroups: audit.summary.exactDuplicateGroups,
      sameModelGroups: review.sameModelGroups.length,
      crossModelGroups: review.crossModelGroups.length,
      preservedChoices: Object.keys(decisions.groups).length,
      logPath,
    }
  } finally {
    for (const filePath of [auditWorking, reviewWorking, `${decisionsPath}.${process.pid}.working`]) {
      try { fs.unlinkSync(filePath) } catch {}
    }
  }
}

module.exports = { refreshExactDuplicateReview, migrateDecisions }

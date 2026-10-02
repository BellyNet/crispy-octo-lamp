'use strict'

// Fixture test for the scrape queue: two workers drain a run through a fake
// task script, like production: the NAS worker uses the queue folder
// directly, the PC worker goes through the dashboard's worker API over HTTP.
// Checks routing, that tasks never overlap, logs/results, failures, cancel,
// stale-lock recovery and the CLI lock (local and over HTTP).
const assert = require('assert')
const fs = require('fs')
const os = require('os')
const path = require('path')

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'scrape-queue-test-'))
const queueDir = path.join(root, 'queue')
const timeline = path.join(root, 'timeline.log')
const fakeTask = path.join(root, 'fake-task.js')

// Fake scraper: options ride on the URL (?ms=…&fail=1&saved=…).
fs.writeFileSync(
  fakeTask,
  `
const fs = require('fs')
const arg = (name) => (process.argv.find((a) => a.startsWith('--' + name + '=')) || '').split('=').slice(1).join('=')
const url = new URL(arg('url'))
const ms = Number(url.searchParams.get('ms') || 150)
fs.appendFileSync(${JSON.stringify(timeline)}, 'start ' + Date.now() + ' ' + url.pathname + '\\n')
console.log('fake scraping ' + url.pathname)
setTimeout(() => {
  console.log('\\u001b[32mdone\\u001b[0m with ' + url.pathname)
  fs.appendFileSync(${JSON.stringify(timeline)}, 'end ' + Date.now() + ' ' + url.pathname + '\\n')
  const fail = url.searchParams.get('fail') === '1'
  fs.writeFileSync(arg('result'), JSON.stringify({ runs: [{ ok: !fail, code: fail ? 1 : 0, summary: { saved: Number(url.searchParams.get('saved') || 0) } }] }))
  process.exit(fail ? 1 : 0)
}, ms)
`
)
process.env.SCRAPE_TASK_SCRIPT = fakeTask
process.env.SCRAPE_QUEUE_DIR = queueDir

const express = require('express')
const queue = require('./scrapeQueue')
const { startWorker } = require('./scrapeWorker')
const { createLocalBackend, createRemoteBackend } = require('./scrapeBackends')
const { mountWorkerApi } = require('../dashboard/scrapes')

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
async function waitFor(check, label, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (check()) return
    await sleep(100)
  }
  throw new Error(`Timed out waiting for ${label}`)
}
function task(name, requiresBrowser, query = '') {
  return {
    model: name,
    url: `https://example.test/${name}?${query}`,
    sourceLabel: requiresBrowser ? 'Browser' : 'Plain',
    requiresBrowser,
  }
}
const quietLog = { warn() {}, error() {} }

;(async () => {
  // The dashboard's worker API on a random local port.
  const app = express()
  mountWorkerApi(app, { express })
  const server = await new Promise((resolve) => {
    const s = app.listen(0, '127.0.0.1', () => resolve(s))
  })
  const baseUrl = `http://127.0.0.1:${server.address().port}`
  const remote = createRemoteBackend({ baseUrl, queueDir })

  const nas = startWorker({
    id: 'nas',
    browser: false,
    backend: createLocalBackend(queueDir),
    pollMs: 80,
    cooldownMs: 120,
    log: quietLog,
  })
  const pc = startWorker({
    id: 'pc',
    browser: true,
    backend: remote,
    pollMs: 80,
    log: quietLog,
  })

  // 1. Routing, no overlap, results and failures.
  const run = queue.createRun(
    {
      label: 'test',
      scope: 'all',
      tasks: [
        task('a', false, 'saved=2'),
        task('b', true, 'saved=5'),
        task('c', false, 'fail=1'),
        task('d', true, 'saved=1'),
        task('e', false, 'saved=3'),
      ],
    },
    queueDir
  )
  await waitFor(() => {
    const summary = queue.summarizeRun(queue.readRun(run.id, queueDir))
    return summary.counts.queued + summary.counts.running === 0
  }, 'first run to finish')

  const finished = queue.readRun(run.id, queueDir)
  const byModel = Object.fromEntries(finished.tasks.map((t) => [t.model, t]))
  for (const name of ['a', 'c', 'e'])
    assert.strictEqual(
      byModel[name].worker,
      'nas',
      `${name} should run on the NAS`
    )
  for (const name of ['b', 'd'])
    assert.strictEqual(
      byModel[name].worker,
      'pc',
      `${name} should run on the PC`
    )
  assert.strictEqual(byModel.c.status, 'failed')
  assert.strictEqual(byModel.a.status, 'done')
  assert.strictEqual(byModel.b.summary.saved, 5)
  const summary = queue.summarizeRun(finished)
  assert.strictEqual(summary.status, 'finished-with-errors')
  assert.strictEqual(summary.totals.saved, 11)

  const log = queue.readTaskLogTail(run.id, byModel.b.id, 65536, queueDir).text
  assert.match(log, /\[queue\] pc started b/)
  assert.match(log, /fake scraping \/b/)
  assert.match(log, /done with \/b/)
  assert.doesNotMatch(log, /\u001b\[/, 'ANSI codes are stripped')

  // Never two tasks at once.
  const events = fs
    .readFileSync(timeline, 'utf8')
    .trim()
    .split('\n')
    .map((line) => line.split(' '))
  let running = 0
  for (const [kind] of events) {
    running += kind === 'start' ? 1 : -1
    assert.ok(running <= 1, 'two tasks overlapped')
  }

  // 2. Cancel stops the running task and skips the rest.
  const slow = queue.createRun(
    {
      label: 'slow',
      scope: 'model',
      tasks: [
        task('s1', false, 'ms=20000'),
        task('s2', false),
        task('s3', true),
      ],
    },
    queueDir
  )
  await waitFor(
    () => queue.readRun(slow.id, queueDir).tasks[0].status === 'running',
    'slow task to start'
  )
  queue.requestCancel(slow.id, queueDir)
  await waitFor(
    () => {
      const s = queue.summarizeRun(queue.readRun(slow.id, queueDir), {
        canceled: true,
      })
      return s.status === 'canceled'
    },
    'cancel to complete',
    20000
  )
  const slowTasks = queue.readRun(slow.id, queueDir).tasks
  assert.deepStrictEqual(
    slowTasks.map((t) => t.status),
    ['canceled', 'canceled', 'canceled']
  )
  assert.match(
    queue.readTaskLogTail(slow.id, slowTasks[0].id, 65536, queueDir).text,
    /Cancel requested/
  )

  nas.stop()
  pc.stop()
  await waitFor(() => !nas.isBusy() && !pc.isBusy(), 'workers to go idle')
  await sleep(300)

  // 3. A crashed holder's lock goes stale: the task is re-queued and the
  // lock can be taken over.
  const orphan = queue.createRun(
    { label: 'orphan', scope: 'url', tasks: [task('o', false)] },
    queueDir
  )
  const orphanRun = queue.readRun(orphan.id, queueDir)
  orphanRun.tasks[0].status = 'running'
  orphanRun.tasks[0].attempts = 1
  queue.saveRun(orphanRun, queueDir)
  const old = new Date(Date.now() - queue.LOCK_STALE_MS - 1000).toISOString()
  fs.writeFileSync(
    queue.queuePaths(queueDir).lockPath,
    JSON.stringify({
      holder: 'ghost',
      acquiredAt: old,
      heartbeatAt: old,
      runId: orphan.id,
      taskId: orphanRun.tasks[0].id,
    })
  )
  assert.strictEqual(queue.tryAcquireLock({ id: 'rescuer' }, queueDir), true)
  assert.strictEqual(
    queue.readRun(orphan.id, queueDir).tasks[0].status,
    'queued'
  )

  // 4. The CLI lock refuses while someone holds the lock, works once free.
  assert.throws(
    () => queue.holdScrapeLock('test cli', queueDir),
    (err) => err.code === 'SCRAPE_LOCK_BUSY'
  )
  queue.releaseLock({ id: 'rescuer' }, queueDir)
  const release = queue.holdScrapeLock('test cli', queueDir)
  assert.ok(queue.readLock(queueDir).holder.startsWith('cli-'))
  release()
  assert.strictEqual(queue.readLock(queueDir), null)

  // 5. The same over HTTP (manual scrapes on the PC), and a bad token is
  // refused.
  const releaseRemote = await remote.holdCliLock('pc cli')
  assert.ok(queue.readLock(queueDir).holder.startsWith('cli-'))
  await assert.rejects(
    () => remote.holdCliLock('second cli'),
    (err) => err.code === 'SCRAPE_LOCK_BUSY'
  )
  await releaseRemote()
  assert.strictEqual(queue.readLock(queueDir), null)
  const res = await fetch(`${baseUrl}/api/worker/claim`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Worker-Token': 'nope' },
    body: JSON.stringify({ worker: { id: 'x' } }),
  })
  assert.strictEqual(res.status, 401)

  server.close()

  fs.rmSync(root, { recursive: true, force: true })
  console.log('Scrape queue fixture passed.')
  process.exit(0)
})().catch((err) => {
  console.error(err)
  process.exit(1)
})

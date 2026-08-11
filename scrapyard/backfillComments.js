#!/usr/bin/env node
'use strict'

/**
 * backfillComments.js — refetch comments for sidecar entries that were
 * written before the scraper started persisting them.
 *
 * Walks every dataset/<user>/.media-dates.json, finds entries that have a
 * `source.mediaPageUrl` but an empty `comments` array, and refetches just
 * the comments (never re-downloads media). Writes the sidecar back with
 * atomic rename so a Ctrl-C mid-run leaves the file valid.
 *
 * Coverage (as of writing):
 *   - kemono / pawchive    -> JSON API (implemented)
 *   - coomerfans / coomer  -> no supported comments endpoint (titles only)
 *   - stufferdb            -> needs Playwright (run under main scraper)
 *   - reddit               -> needs Pushshift / old-Reddit HTML (TODO)
 *
 * Usage:
 *   node scrapyard/backfillComments.js                     # dry-run summary
 *   node scrapyard/backfillComments.js --apply             # actually fetch + write
 *   node scrapyard/backfillComments.js --user ramenslurper # scope to one model
 *   node scrapyard/backfillComments.js --limit 200 --apply # first 200 entries
 *   node scrapyard/backfillComments.js --site kemono --apply
 */

const fs = require('fs')
const path = require('path')
const { createHttpClient } = require('./httpClient.js')

// ─── ARGS ─────────────────────────────────────────────────────────────────────
const args = process.argv.slice(2)
const flag = (name) => args.includes(`--${name}`)
const opt = (name, fallback = null) => {
  const i = args.indexOf(`--${name}`)
  if (i === -1 || i === args.length - 1) return fallback
  return args[i + 1]
}
const APPLY = flag('apply')
const LIMIT = parseInt(opt('limit', '0'), 10) || Infinity
const SCOPE_USER = opt('user', null)
const SCOPE_SITE = opt('site', null)
const CONCURRENCY = parseInt(opt('concurrency', '3'), 10) || 3
const DATASET_DIR =
  process.env.DATASET_DIR ||
  (process.platform === 'win32'
    ? 'F:/Vault69/dataset' // local dev fallback
    : '/share/Vault69/dataset')

// ─── COMMENT NORMALIZATION ────────────────────────────────────────────────────
// Kept in sync with normalizePawchiveComments in sourceAdapters/coomerKemono.js
// — same shape, same content-stripping rules. Copied here so this script is a
// single-file backfill and doesn't drag the whole scraper module tree in.
function cleanText(v) {
  if (typeof v !== 'string') return null
  const t = v
    .replace(/<[^>]+>/g, '') // strip HTML tags — pawchive returns <p>…</p>
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
  return t || null
}

function normalizeApiComments(list) {
  if (!Array.isArray(list)) return []
  return list
    .map((c) => ({
      author: cleanText(c?.commenter_name),
      posted: typeof c?.published === 'string' ? c.published.trim() : null,
      text: cleanText(c?.content),
    }))
    .filter((c) => c.text)
}

// ─── ADAPTERS ────────────────────────────────────────────────────────────────
// Each adapter reads a sidecar entry and returns the URL to fetch comments
// from, or null if the entry can't be handled (missing fields, unsupported
// site). Comments API pattern: getPostApiUrl(...)/comments.
function coomerKemonoCommentsUrl(entry) {
  const s = entry?.source
  if (!s) return null
  if (s.site !== 'kemono') return null
  if (!s.service || !s.userId || !s.postId) return null
  let origin
  try {
    origin = new URL(s.mediaPageUrl).origin
  } catch {
    origin = 'https://kemono.cr'
  }
  return `${origin}/api/v1/${s.service}/user/${encodeURIComponent(
    s.userId
  )}/post/${encodeURIComponent(s.postId)}/comments`
}

const HANDLERS = {
  coomerfans: {
    name: 'coomerfans',
    url: () => null,
    todo: 'no supported comments endpoint; use repair:coomerfans-titles for titles',
  },
  coomer: {
    name: 'coomer',
    url: () => null,
    todo: 'no supported comments endpoint',
  },
  kemono: { name: 'kemono', url: coomerKemonoCommentsUrl },
  stufferdb: {
    name: 'stufferdb',
    url: () => null,
    todo: 'needs Playwright — run under the main scraper',
  },
  reddit: {
    name: 'reddit',
    url: () => null,
    todo: 'needs Pushshift/old.reddit.com scrape',
  },
}

// ─── WORK LIST BUILD ─────────────────────────────────────────────────────────
function readSidecar(userDir) {
  const p = path.join(userDir, '.media-dates.json')
  try {
    return { path: p, data: JSON.parse(fs.readFileSync(p, 'utf8')) }
  } catch {
    return null
  }
}

function needsBackfill(entry) {
  const c = entry?.comments
  return Array.isArray(c) ? c.length === 0 : true
}

function collectTargets() {
  let users
  try {
    users = fs.readdirSync(DATASET_DIR, { withFileTypes: true })
  } catch (err) {
    console.error(`Cannot read dataset dir ${DATASET_DIR}: ${err.message}`)
    process.exit(1)
  }
  const perSite = {}
  const targets = [] // { userDir, sidecarPath, sidecarData, key, entry, site, url }
  for (const d of users) {
    if (!d.isDirectory() || d.name.startsWith('.')) continue
    if (SCOPE_USER && d.name !== SCOPE_USER) continue
    const userDir = path.join(DATASET_DIR, d.name)
    const sc = readSidecar(userDir)
    if (!sc) continue
    for (const [key, entry] of Object.entries(sc.data)) {
      if (key === '__version') continue
      const site = entry?.source?.site
      if (!site) continue
      perSite[site] = perSite[site] || { total: 0, missing: 0 }
      perSite[site].total++
      if (!needsBackfill(entry)) continue
      perSite[site].missing++
      if (SCOPE_SITE && site !== SCOPE_SITE) continue
      const handler = HANDLERS[site]
      if (!handler || handler.todo) continue
      const url = handler.url(entry)
      if (!url) continue
      targets.push({
        userDir,
        sidecarPath: sc.path,
        sidecarData: sc.data,
        key,
        entry,
        site,
        url,
      })
      if (targets.length >= LIMIT) break
    }
    if (targets.length >= LIMIT) break
  }
  return { perSite, targets }
}

// ─── WRITE SIDECAR ATOMICALLY ────────────────────────────────────────────────
// Group updates by sidecarPath so we do one write per user, not per entry.
function writeSidecarAtomic(sidecarPath, data) {
  const tmp = sidecarPath + '.tmp-backfill'
  fs.writeFileSync(tmp, JSON.stringify(data, null, 0))
  fs.renameSync(tmp, sidecarPath)
}

// ─── MAIN ────────────────────────────────────────────────────────────────────
async function main() {
  console.log('backfillComments — walking', DATASET_DIR)
  console.log('mode:', APPLY ? 'APPLY (writing sidecars)' : 'dry-run')
  if (SCOPE_USER) console.log('user filter:', SCOPE_USER)
  if (SCOPE_SITE) console.log('site filter:', SCOPE_SITE)
  if (LIMIT !== Infinity) console.log('limit:', LIMIT)

  const { perSite, targets } = collectTargets()
  console.log('\nPer-site totals across sidecars:')
  const totalRow = { total: 0, missing: 0 }
  for (const site of Object.keys(perSite).sort()) {
    const s = perSite[site]
    totalRow.total += s.total
    totalRow.missing += s.missing
    const handler = HANDLERS[site]
    const status = !handler
      ? 'no handler'
      : handler.todo
        ? `TODO: ${handler.todo}`
        : 'backfillable'
    console.log(
      `  ${site.padEnd(12)} total=${String(s.total).padStart(6)}  missing=${String(s.missing).padStart(6)}  [${status}]`
    )
  }
  console.log(
    `  ${'-'.repeat(12)} total=${String(totalRow.total).padStart(6)}  missing=${String(totalRow.missing).padStart(6)}`
  )

  console.log(`\nActionable this run: ${targets.length} entries`)
  if (!APPLY) {
    console.log('\n(dry-run — nothing written. Re-run with --apply to fetch.)')
    return
  }
  if (!targets.length) return

  const http = createHttpClient({
    timeoutMs: 20000,
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
  })
  let done = 0
  let fetched = 0
  let empty = 0
  let failed = 0
  const dirty = new Map() // sidecarPath → data

  // Group by host so we can rate-limit per host if needed. For now use a
  // simple global concurrency window.
  const queue = targets.slice()
  const worker = async () => {
    while (queue.length) {
      const t = queue.shift()
      if (!t) return
      try {
        const res = await http.fetchJson(t.url, {
          headers: {},
        })
        const comments = normalizeApiComments(res?.data?.data ?? res?.data)
        const commentCount = Array.isArray(res?.data?.data)
          ? res.data.data.length
          : Array.isArray(res?.data)
            ? res.data.length
            : comments.length
        // Update the entry in place. The sidecarData object is shared
        // across all targets for this user, so writing once per sidecar
        // at the end captures every mutation.
        t.entry.comments = comments
        t.entry.commentCount = commentCount
        dirty.set(t.sidecarPath, t.sidecarData)
        if (comments.length === 0) empty++
        else fetched++
      } catch (err) {
        failed++
        if (failed <= 5) {
          console.warn(
            `  fail ${t.site} ${t.entry?.source?.postId}: ${err.message}`
          )
        }
      }
      done++
      if (done % 100 === 0 || done === targets.length) {
        process.stdout.write(
          `\r  ${done}/${targets.length} · with=${fetched} empty=${empty} fail=${failed}   `
        )
      }
    }
  }
  await Promise.all(
    Array.from({ length: CONCURRENCY }, () => worker())
  )
  console.log()

  console.log(`\nWriting ${dirty.size} updated sidecar(s)…`)
  for (const [p, data] of dirty) {
    try {
      writeSidecarAtomic(p, data)
    } catch (err) {
      console.warn(`  write fail ${p}: ${err.message}`)
    }
  }

  console.log(
    `\nDone. fetched=${fetched} empty=${empty} fail=${failed} sidecars_written=${dirty.size}`
  )
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})

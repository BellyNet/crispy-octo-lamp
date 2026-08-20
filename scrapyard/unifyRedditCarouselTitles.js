#!/usr/bin/env node
'use strict'

/**
 * unifyRedditCarouselTitles.js — every file belonging to the same
 * reddit post should carry the same source.title, so a carousel is
 * visually recognizable in the dashboard grid.
 *
 * Across old scrape runs the same post picked up different titles:
 *   - real API title:      "[OCW] Sexy or gross?"
 *   - permalink fallback:  "ocw sexy or gross"   (from the URL slug)
 * This walks each dataset/<user>/.media-dates.json, groups reddit
 * entries by postId, picks the best title, and rewrites siblings so
 * they all match.
 *
 * "Best" = a title that is NOT the permalink slug of any sibling's
 * mediaPageUrl. Ties broken by length (real API titles have
 * capitalization + punctuation, so they win over the lowercase slug).
 *
 * Usage:
 *   node scrapyard/unifyRedditCarouselTitles.js               # dry-run
 *   node scrapyard/unifyRedditCarouselTitles.js --apply       # write
 *   node scrapyard/unifyRedditCarouselTitles.js --user lanabells221 --apply
 */

const fs = require('fs')
const path = require('path')

const { getRedditTitleFromPermalink } = require('./mediaDates.js')

const args = process.argv.slice(2)
const APPLY = args.includes('--apply')
const userIdx = args.indexOf('--user')
const SCOPE_USER = userIdx !== -1 ? args[userIdx + 1] : null
const DATASET_DIR =
  process.env.DATASET_DIR ||
  path.join(
    process.env.APPDATA ||
      path.join(process.env.HOME || process.env.USERPROFILE, 'AppData/Roaming'),
    '.slopvault',
    'dataset'
  )

function loadSidecar(userDir) {
  const p = path.join(userDir, '.media-dates.json')
  try {
    return { path: p, data: JSON.parse(fs.readFileSync(p, 'utf8')) }
  } catch {
    return null
  }
}

function writeAtomic(p, data) {
  const tmp = p + '.tmp-unify-titles'
  fs.writeFileSync(tmp, JSON.stringify(data))
  fs.renameSync(tmp, p)
}

// Group reddit entries by postId. Returns Map<postId, [{key, entry}]>.
function groupRedditByPost(data) {
  const groups = new Map()
  for (const [key, entry] of Object.entries(data)) {
    if (key === '__version') continue
    const src = entry?.source
    if (!src || src.site !== 'reddit') continue
    const postId = src.postId
    if (!postId) continue
    let list = groups.get(postId)
    if (!list) {
      list = []
      groups.set(postId, list)
    }
    list.push({ key, entry })
  }
  return groups
}

// Pick the best title from a group. Rules:
//   1. Skip empty titles.
//   2. Deprioritize any title equal (case-insensitive, trimmed) to the
//      permalink-slug derived from ANY sibling's mediaPageUrl — those
//      are the fallbacks we want to replace.
//   3. Among survivors, longest wins. Ties: first-seen.
//   4. If everything is a fallback, keep the longest anyway (better
//      than blanking a title).
function pickBestTitle(group) {
  const slugSet = new Set()
  for (const { entry } of group) {
    const url = entry?.source?.mediaPageUrl
    const slug = getRedditTitleFromPermalink(url)
    if (slug) slugSet.add(slug.trim().toLowerCase())
  }
  const scored = group
    .map(({ entry }) => {
      const t = entry?.source?.title
      if (typeof t !== 'string' || !t.trim()) return null
      const trimmed = t.trim()
      const isFallback = slugSet.has(trimmed.toLowerCase())
      return { title: trimmed, isFallback, length: trimmed.length }
    })
    .filter(Boolean)
  if (!scored.length) return null
  const real = scored.filter((s) => !s.isFallback)
  const pool = real.length ? real : scored
  pool.sort((a, b) => b.length - a.length)
  return pool[0].title
}

function processUser(userDir) {
  const sc = loadSidecar(userDir)
  if (!sc) return null
  const groups = groupRedditByPost(sc.data)
  const changed = [] // { postId, files, oldTitles, newTitle }
  for (const [postId, group] of groups) {
    if (group.length < 2) continue
    const best = pickBestTitle(group)
    if (!best) continue
    const distinct = new Set(
      group.map(({ entry }) => (entry.source.title || '').trim())
    )
    if (distinct.size <= 1 && distinct.has(best)) continue
    changed.push({
      postId,
      files: group.length,
      oldTitles: [...distinct],
      newTitle: best,
    })
    for (const { entry } of group) {
      entry.source.title = best
    }
  }
  if (changed.length && APPLY) writeAtomic(sc.path, sc.data)
  return { changed, sidecarPath: sc.path }
}

function main() {
  console.log('unifyRedditCarouselTitles — walking', DATASET_DIR)
  console.log('mode:', APPLY ? 'APPLY' : 'dry-run')
  if (SCOPE_USER) console.log('user:', SCOPE_USER)
  const users = fs
    .readdirSync(DATASET_DIR, { withFileTypes: true })
    .filter((d) => d.isDirectory() && !d.name.startsWith('.'))
    .filter((d) => !SCOPE_USER || d.name === SCOPE_USER)
  let touchedUsers = 0
  let totalGroups = 0
  let totalFiles = 0
  for (const d of users) {
    const r = processUser(path.join(DATASET_DIR, d.name))
    if (!r || !r.changed.length) continue
    touchedUsers++
    totalGroups += r.changed.length
    for (const c of r.changed) totalFiles += c.files
    console.log(
      `\n  ${d.name}  (${r.changed.length} carousel${r.changed.length === 1 ? '' : 's'})`
    )
    for (const c of r.changed.slice(0, 5)) {
      console.log(
        `    ${c.postId}  ×${c.files}  → "${c.newTitle}"  (was: ${c.oldTitles.map((t) => JSON.stringify(t)).join(', ')})`
      )
    }
    if (r.changed.length > 5) console.log(`    …+${r.changed.length - 5} more`)
  }
  console.log(
    `\n${touchedUsers} user(s), ${totalGroups} carousel(s), ${totalFiles} file(s) ${APPLY ? 'updated' : 'would update'}`
  )
  if (!APPLY) console.log('(dry-run — re-run with --apply to write)')
}

main()

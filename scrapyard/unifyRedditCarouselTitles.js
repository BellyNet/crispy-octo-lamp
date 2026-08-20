#!/usr/bin/env node
'use strict'

/**
 * unifyRedditCarouselTitles.js — every file belonging to the same
 * reddit post should carry the same source.title, so a carousel is
 * visually recognizable in the dashboard grid.
 *
 * Two grouping passes:
 *
 * 1. Direct reddit siblings — entries with source.site === 'reddit'
 *    and a shared postId. Older scrape runs stored the permalink-slug
 *    fallback ("ocw sexy or gross") while later runs got the real API
 *    title ("[OCW] Sexy or gross?"); this pass picks the best and
 *    rewrites the group so every file agrees.
 *
 * 2. Cross-site siblings — filenames often embed a reddit post ID
 *    even when the source ended up as stufferdb / coomerfans (the same
 *    carousel got re-hosted). Any entry whose filename contains a
 *    known-reddit post ID as an underscore-delimited token gets pulled
 *    into that carousel. "Known" = a post ID we saw in a reddit URL in
 *    the same sidecar, so we don't false-match on random substrings.
 *
 * "Best" title = one that is NOT the permalink slug of any sibling's
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

// Pull the reddit post ID out of a mediaPageUrl. Returns null if the
// URL isn't a reddit /comments/<id>/ URL.
function redditPostIdFromUrl(url) {
  if (typeof url !== 'string') return null
  const m = url.match(
    /https?:\/\/(?:[a-z0-9-]+\.)?reddit\.com\/(?:r\/[^/]+\/)?comments\/([a-z0-9]+)/i
  )
  return m ? m[1].toLowerCase() : null
}

// Given a filename basename and a set of known reddit post IDs, return
// the ID that appears as an underscore-delimited token, or null. We
// require the token to be surrounded by _ . - or basename boundaries,
// so a random hex hash containing the substring doesn't match.
function knownRedditIdInFilename(filename, knownIds) {
  if (typeof filename !== 'string' || !knownIds.size) return null
  const lower = filename.toLowerCase()
  const tokens = lower.split(/[_.\-/]/)
  for (const t of tokens) {
    if (knownIds.has(t)) return t
  }
  return null
}

// Reddit-shaped ID: 5-8 base36 chars starting with a digit and
// containing at least one letter. Filters both pure-numeric stufferdb
// picture IDs (671508) and short username fragments.
const REDDIT_ID_SHAPE = /^\d[a-z0-9]{4,7}$/

function looksLikeRedditId(token) {
  return REDDIT_ID_SHAPE.test(token) && /[a-z]/.test(token)
}

// Group every entry by a shared reddit post ID:
//   - reddit-sourced entries → id from source.postId or the URL
//   - non-reddit entries     → id embedded in the filename, but only
//                              if that id is "known": either a reddit
//                              URL/postId elsewhere in the sidecar
//                              references it, OR the reddit-shaped
//                              token appears in ≥2 filenames (siblings
//                              of a re-hosted carousel with no reddit
//                              entry in the sidecar).
// Returns Map<postId, [{key, entry}]>.
function groupByRedditCarousel(data) {
  const knownIds = new Set()
  for (const [key, entry] of Object.entries(data)) {
    if (key === '__version') continue
    const src = entry?.source
    if (!src) continue
    if (src.site === 'reddit' && typeof src.postId === 'string') {
      knownIds.add(src.postId.toLowerCase())
    }
    const urlId = redditPostIdFromUrl(src.mediaPageUrl)
    if (urlId) knownIds.add(urlId)
  }
  // Second signal: reddit-shaped tokens repeated across ≥2 filenames.
  const tokenCounts = new Map()
  for (const key of Object.keys(data)) {
    if (key === '__version') continue
    const filename = key.slice(key.lastIndexOf('/') + 1).toLowerCase()
    const seen = new Set()
    for (const t of filename.split(/[_.\-/]/)) {
      if (!looksLikeRedditId(t) || seen.has(t)) continue
      seen.add(t)
      tokenCounts.set(t, (tokenCounts.get(t) || 0) + 1)
    }
  }
  for (const [t, n] of tokenCounts) {
    if (n >= 2) knownIds.add(t)
  }

  const groups = new Map()
  const push = (id, key, entry) => {
    let list = groups.get(id)
    if (!list) {
      list = []
      groups.set(id, list)
    }
    list.push({ key, entry })
  }

  for (const [key, entry] of Object.entries(data)) {
    if (key === '__version') continue
    const src = entry?.source
    if (!src) continue
    if (src.site === 'reddit') {
      const id =
        (typeof src.postId === 'string' && src.postId.toLowerCase()) ||
        redditPostIdFromUrl(src.mediaPageUrl)
      if (id) push(id, key, entry)
      continue
    }
    // Non-reddit entry: match on a reddit ID embedded in the filename.
    // key is "<folder>/<filename>"; strip the folder before scanning.
    const filename = key.slice(key.lastIndexOf('/') + 1)
    const id = knownRedditIdInFilename(filename, knownIds)
    if (id) push(id, key, entry)
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
//   5. Refuse to merge if the "real" titles disagree with each other
//      (more than one distinct non-fallback title). That's a signal
//      the group is a false-positive — e.g. a token-repeat heuristic
//      accidentally grouped an entire coomerfans user's video library
//      because every filename starts with the same user-ID prefix.
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
  if (real.length) {
    const distinctReal = new Set(real.map((r) => r.title))
    if (distinctReal.size > 1) return null
  }
  const pool = real.length ? real : scored
  pool.sort((a, b) => b.length - a.length)
  return pool[0].title
}

// When a carousel contains at least one bona-fide reddit entry, treat
// reddit as the canonical post source and rewrite the non-reddit
// siblings' identity fields to point at it. Non-reddit entries kept
// their original download URL (source.mediaUrl) — that's still where
// the bytes actually came from — but the dashboard's site badge and
// "Source" link work off site/mediaPageUrl and are what the user
// wants driven by reddit when reddit is one of the hosts.
//
// Returns the fields to copy, or null if no reddit anchor exists in
// the group (e.g. the group was matched purely by a repeated filename
// token with no reddit sibling present).
function findRedditAnchor(group) {
  for (const { entry } of group) {
    const s = entry?.source
    if (!s) continue
    if (s.site !== 'reddit') continue
    if (typeof s.mediaPageUrl !== 'string') continue
    if (!/reddit\.com/.test(s.mediaPageUrl)) continue
    return {
      site: 'reddit',
      postId: typeof s.postId === 'string' ? s.postId : null,
      subreddit: typeof s.subreddit === 'string' ? s.subreddit : null,
      mediaPageUrl: s.mediaPageUrl,
    }
  }
  return null
}

function processUser(userDir) {
  const sc = loadSidecar(userDir)
  if (!sc) return null
  const groups = groupByRedditCarousel(sc.data)
  const titleChanges = [] // { postId, files, sites, oldTitles, newTitle }
  const promoted = [] // { postId, files, fromSites, toUrl }
  for (const [postId, group] of groups) {
    if (group.length < 2) continue

    // Pass 1: title unification.
    const best = pickBestTitle(group)
    let titlePassRan = false
    if (best) {
      const distinct = new Set(
        group.map(({ entry }) => (entry.source.title || '').trim())
      )
      if (!(distinct.size <= 1 && distinct.has(best))) {
        const sites = [
          ...new Set(group.map(({ entry }) => entry.source.site || '?')),
        ]
        titleChanges.push({
          postId,
          files: group.length,
          sites,
          oldTitles: [...distinct],
          newTitle: best,
        })
        for (const { entry } of group) entry.source.title = best
        titlePassRan = true
      }
    }

    // Pass 2: reddit source promotion. Only when the title pass agreed
    // this group is a real carousel (best !== null) — that already
    // enforces the "one distinct real title" guardrail, so the group
    // isn't a token-collision false positive.
    if (!best) continue
    const anchor = findRedditAnchor(group)
    if (!anchor) continue
    const targets = group.filter(({ entry }) => entry.source.site !== 'reddit')
    if (!targets.length) continue
    const fromSites = [
      ...new Set(targets.map(({ entry }) => entry.source.site || '?')),
    ]
    promoted.push({
      postId,
      files: targets.length,
      fromSites,
      toUrl: anchor.mediaPageUrl,
    })
    for (const { entry } of targets) {
      entry.source.site = anchor.site
      if (anchor.postId) entry.source.postId = anchor.postId
      if (anchor.subreddit) entry.source.subreddit = anchor.subreddit
      entry.source.mediaPageUrl = anchor.mediaPageUrl
    }
    void titlePassRan // silence lint if unused
  }
  if ((titleChanges.length || promoted.length) && APPLY) {
    writeAtomic(sc.path, sc.data)
  }
  return { titleChanges, promoted, sidecarPath: sc.path }
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
  let totalTitleGroups = 0
  let totalTitleFiles = 0
  let totalPromoteGroups = 0
  let totalPromoteFiles = 0
  for (const d of users) {
    const r = processUser(path.join(DATASET_DIR, d.name))
    if (!r || (!r.titleChanges.length && !r.promoted.length)) continue
    touchedUsers++
    totalTitleGroups += r.titleChanges.length
    for (const c of r.titleChanges) totalTitleFiles += c.files
    totalPromoteGroups += r.promoted.length
    for (const p of r.promoted) totalPromoteFiles += p.files
    const parts = []
    if (r.titleChanges.length)
      parts.push(`${r.titleChanges.length} title carousel(s)`)
    if (r.promoted.length)
      parts.push(`${r.promoted.length} reddit-promoted group(s)`)
    console.log(`\n  ${d.name}  (${parts.join(', ')})`)
    for (const c of r.titleChanges.slice(0, 3)) {
      const sites = c.sites.length > 1 ? ` [${c.sites.join('+')}]` : ''
      console.log(
        `    title  ${c.postId}  ×${c.files}${sites}  → "${c.newTitle}"`
      )
    }
    if (r.titleChanges.length > 3)
      console.log(`    …+${r.titleChanges.length - 3} more title changes`)
    for (const p of r.promoted.slice(0, 3)) {
      console.log(
        `    promote ${p.postId}  ×${p.files} [${p.fromSites.join('+')}→reddit]  ${p.toUrl}`
      )
    }
    if (r.promoted.length > 3)
      console.log(`    …+${r.promoted.length - 3} more promotions`)
  }
  const verb = APPLY ? 'updated' : 'would update'
  console.log(
    `\n${touchedUsers} user(s):` +
      `\n  titles:    ${totalTitleGroups} carousel(s), ${totalTitleFiles} file(s) ${verb}` +
      `\n  promoted:  ${totalPromoteGroups} group(s), ${totalPromoteFiles} file(s) ${verb}`
  )
  if (!APPLY) console.log('(dry-run — re-run with --apply to write)')
}

main()

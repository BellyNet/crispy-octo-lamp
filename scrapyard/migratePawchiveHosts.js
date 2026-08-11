#!/usr/bin/env node
'use strict'

/**
 * migratePawchiveHosts.js — rewrite legacy pawchive.st URLs in
 * every dataset/<user>/.media-dates.json to the current pawchive.pw
 * host. The scraper long since migrated (see scrapyard/pawchive.js);
 * this walks the historical sidecar entries so the dashboard's
 * "Source" links and any downstream URL consumers stop pointing at
 * a dead host.
 *
 * Also rewrites the old img.pawchive.st preview host to img.pawchive.pw
 * for symmetry.
 *
 * Usage:
 *   node scrapyard/migratePawchiveHosts.js               # dry-run
 *   node scrapyard/migratePawchiveHosts.js --apply       # actually write
 *   node scrapyard/migratePawchiveHosts.js --user candii_kayn --apply
 */

const fs = require('fs')
const path = require('path')

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

// Order matters — img.pawchive.st must match before pawchive.st.
const REPLACEMENTS = [
  [/https?:\/\/img\.pawchive\.st/gi, 'https://img.pawchive.pw'],
  [/https?:\/\/pawchive\.st/gi, 'https://pawchive.pw'],
]

function migrate(str) {
  let out = str
  for (const [re, to] of REPLACEMENTS) out = out.replace(re, to)
  return out
}

function processUser(userDir) {
  const p = path.join(userDir, '.media-dates.json')
  let raw
  try {
    raw = fs.readFileSync(p, 'utf8')
  } catch {
    return null
  }
  const before = (raw.match(/pawchive\.st/gi) || []).length
  if (!before) return { path: p, before: 0, after: 0, changed: false }
  const migrated = migrate(raw)
  const after = (migrated.match(/pawchive\.st/gi) || []).length
  if (APPLY && migrated !== raw) {
    const tmp = p + '.tmp-migrate'
    fs.writeFileSync(tmp, migrated)
    fs.renameSync(tmp, p)
  }
  return { path: p, before, after, changed: migrated !== raw }
}

function main() {
  console.log('migratePawchiveHosts — walking', DATASET_DIR)
  console.log('mode:', APPLY ? 'APPLY' : 'dry-run')
  if (SCOPE_USER) console.log('user:', SCOPE_USER)
  const users = fs
    .readdirSync(DATASET_DIR, { withFileTypes: true })
    .filter((d) => d.isDirectory() && !d.name.startsWith('.'))
    .filter((d) => !SCOPE_USER || d.name === SCOPE_USER)
  let touched = 0
  let totalBefore = 0
  for (const d of users) {
    const r = processUser(path.join(DATASET_DIR, d.name))
    if (!r || r.before === 0) continue
    touched++
    totalBefore += r.before
    console.log(
      `  ${d.name.padEnd(28)}  pawchive.st: ${r.before} → ${r.after}${
        r.changed ? (APPLY ? ' [written]' : ' [would write]') : ''
      }`
    )
  }
  console.log(
    `\n${touched} sidecar(s) with stale hosts, ${totalBefore} refs total`
  )
  if (!APPLY) console.log('(dry-run — re-run with --apply to write)')
}

main()

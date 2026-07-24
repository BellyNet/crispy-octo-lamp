# Codex handoff — Reddit title backfill + decode-failing videos

**Handed off:** 2026-07-10
**Owner (scraper):** codex
**Owner (dashboard):** claude
**Status:** open

Two threads in this handoff. Both are scraper-side and should be
picked up as their own work items — dashboard changes for each have
already shipped and the client can pick up the fixed data as soon as
the scraper repairs it.

---

## Thread 1 — Reddit post titles are silently truncated

### What the user sees

Post titles in the lightbox info panel end in `..` (two dots) and cut
off around 93 characters. Most obvious on `cakedupkayyla` — every
long-form title is chopped at word boundaries near the end. Example
straight from the sidecar:

```
"cakedupkayyla / POV: you're pinned underneath your growing giantess while sh.."
"cakedupkayyla / burpin belly & bikini fun 🫧"                       (fine)
"cakedupkayyla / left dinner feeling so overstuffed & bloated, my belly could.."
"cakedupkayyla / Huffle𝘚𝘵𝘶𝘧𝘧𝘦𝘥 ✨🐷\n \nCLIP CONTAINS: Role Play, Stuffing, FatCh.."  (116 chars, truncated)
```

Not a CSS clamp — the raw `source.title` field in
`dataset/<user>/.media-dates.json` already carries the truncated
string. Real Reddit titles can be up to ~300 chars and the JSON API /
old-reddit HTML both return them in full.

### Root cause

`scrapyard/sourceAdapters/reddit.js::extractTitleFromOldRedditPostHtml`
falls through four extraction strategies in order:

1. `data-title` attribute on the post `<div>` — full title ✅
2. `<a class="title">…</a>` link text — full title ✅
3. `<meta property="og:title" content="…">` — **truncated to ~93 chars with `..`** ❌
4. `<title>` tag — variant, usually full

Existing sidecar rows were populated when either strategy 3 was
reached first (og:title is prominent in Reddit's HTML) or when the
post was fetched via a Reddit surface that only exposed og:title.
The truncation was persisted to disk and never re-checked because
the hydration path bails early on any non-empty title.

### What already shipped (dashboard side)

Commit `3e8fbe4` — [reddit.js](scrapyard/sourceAdapters/reddit.js):

- New helper `looksLikeTruncatedRedditTitle(title)` that flags titles
  ≥60 chars ending in `..` or `…`. Cheap heuristic — false positives
  just re-fetch the same title from source, no harm.
- `hydrateMissingRedditTitle()` extended to also refetch when the
  existing title matches that heuristic (was: only when missing).
- Exports `hydrateMissingRedditTitle`, `looksLikeTruncatedRedditTitle`,
  `fetchOldRedditHtml`, `extractTitleFromOldRedditPostHtml`,
  `getOldRedditOrigin` so a backfill script can use them without
  dragging the whole scraper module tree in.

Future scrapes that touch a truncated post will fix it automatically.
Existing sidecars **won't self-heal** — nothing triggers hydration
unless the post is re-scraped.

### What's needed from codex

1. **Standalone backfill script** — `scrapyard/backfillTruncatedTitles.js`, mirroring the shape of `scrapyard/backfillComments.js`:
   - Walk `dataset/<user>/.media-dates.json`.
   - For each entry with `source.site === 'reddit'` where
     `looksLikeTruncatedRedditTitle(source.title)` is true, refetch
     via `hydrateMissingRedditTitle` (or the same
     `fetchOldRedditHtml` + `extractTitleFromOldRedditPostHtml` path
     with cookies).
   - Atomic-rename write so a Ctrl-C mid-run leaves the sidecar valid.
   - `--dry-run` (default) prints a summary, `--apply` writes.
   - `--user cakedupkayyla` scopes to one model.
   - `--concurrency N` for parallel HTTP.

2. **Auth**: Use the same `cookies.txt` (Netscape format) the scraper
   uses — `.gitignore`d already; picked up via `httpClient.js`. Some
   NSFW subreddits 403 without auth. Reuse whatever token/cookie
   plumbing `fetchOldRedditHtml` needs.

3. **Rate limit**: 1–2 req/s per hostname to stay under Reddit's
   anti-scrape limits. Reuse `deps.rateLimiter` if there is one.

4. **Run against affected models**: `cakedupkayyla`, and any model
   the eye-check flags with a similar rate of `..` endings.
   Rough SQL-ish estimate: any user whose sidecar has more than 20%
   of `source.title` fields ending in `..`.

### Optional / stretch

- Add a scan-time metric that counts truncated titles per model and
  surfaces it in `/api/scan-status` or the maintenance summary, so
  future truncations get caught before piling up.
- Detect `og:title` truncation at the *scrape* time (not just at
  hydrate) — if strategy 3 hits and returns something ending in
  `..`, keep trying strategies 4 and beyond before persisting.

### Update 2026-07-24 — slug-fallback path also needs hydration

The `..` heuristic misses a second class of truncation: posts where
the Reddit API returned an **empty** `post.title` and the scraper
fell back to `getTitleFromPermalink(post.permalink)`. The permalink
slug is capped by Reddit at ~50 chars, and it never ends in `..` or
`…`, so `looksLikeTruncatedRedditTitle` returns `false` and
`hydrateMissingRedditTitle` bails.

**Concrete example** — `diablapr`, post `1qzagn4`
(`r/chubby`), stored title: `"the best thing about fat girls is
we jiggle when"` (48 chars). The permalink is
`.../the_best_thing_about_fat_girls_is_we_jiggle_when/` — the slug
is exactly the stored title. Real post title on Reddit is longer;
API-side it was empty at scrape time so we cached the slug.

**Root cause**
`hydrateMissingRedditTitle`
([scrapyard/sourceAdapters/reddit.js:753-760](scrapyard/sourceAdapters/reddit.js#L753-L760))
runs its guard against `getRedditPostTitle(post)`, which already
merges the slug fallback in, so it can't tell "real API title" from
"slug-substitute". Any slug-derived title that also happens to be
under 60 chars silently blocks re-fetch forever.

**Recommended fix** — check the raw `post.title` (before the
slug fallback), not the merged current title:

```js
async function hydrateMissingRedditTitle(source, post, deps = {}) {
  const apiTitle = cleanRedditText(post.title)
  // Hydrate when the API didn't give us a title (slug fallback is
  // ~50-char capped, indistinguishable from a truncated real title),
  // OR when the current title matches the `..` truncation pattern.
  const merged = getRedditPostTitle(post)
  if (apiTitle && !looksLikeTruncatedRedditTitle(merged)) {
    return post
  }
  // …rest unchanged
}
```

Two-line diff, no false positives (empty API title is a reliable
signal), and re-fetch already goes through the `data-title` /
`<a class="title">` extraction path which returns the full title.

**Backfill** — extend the `scrapyard/backfillTruncatedTitles.js`
detection rule (from Thread 1's ask) to also flag sidecars where
`source.title` **equals the URL slug of `source.mediaPageUrl`**
(with `_` → ` `). That's the definitive slug-fallback signature and
catches every affected historical row without false positives from
posts that happen to be short.

**Dashboard side (already shipped)** — the lightbox headline links
to the source post and shows the stored title fully (no CSS clamp
in the info panel). Cards clamp long titles with ellipsis; the full
stored value shows on hover / when the info panel opens. Once codex
re-hydrates, cards + info panel pick up the fuller titles on the
next scan without further UI work.

---

## Thread 2 — decode-failing videos (incomplete downloads)

### What the user sees

A subset of `.mp4` / `.webm` / `.m4v` files fail to play in the
lightbox — the video element fires `error` and the fallback drops in
the source URL for `.mp4`s (works) or shows nothing for `.webm`s (iOS
Safari can't decode). Symptom is consistent with tail-decode failure:
container header is fine, but the tail is missing or corrupted, most
likely because the download was interrupted and the file was
persisted anyway.

### What already shipped

- The dashboard has an error-boundary on `<video>` that falls through
  to `<img>` for gifs and to `item.url` for videos. It doesn't flag
  or record the failure — the user only sees an unplayable clip.
- No server-side detection of broken files exists yet.

### What was scanned

An `ffprobe -v error` + `ffmpeg -sseof -3 -f null -` two-pass scan
was run against **20,035** video files
(mp4: 14,935 / webm: 30 / m4v: 5,070) at `/share/Vault69/dataset/`.

**Header pass** — flags files where ffprobe can't parse the
container (missing moov, wrong format, zero bytes).
**Tail pass** — flags files where the last 3 seconds fail to decode
(the "incomplete download" signature).

The lists live on the NAS at:

- `/tmp/videoscan-failures.txt` — header-pass failures (each entry
  is a `|FAIL|<path>` marker line followed by ffprobe stderr).
- `/tmp/videoscan-deep-failures.txt` — tail-decode failures (same
  format).
- Combined + deduplicated summary at the bottom of this file (see
  **Appendix A**) once the scan completes.

If you don't have SSH access to the NAS, ask for a copy of the two
`.txt` files to be dropped into `handoffs/data/` (not committed —
they can be 10-100 KB each depending on failure count).

### What's needed from codex

1. **Reproduce**: pull the two failure lists off the NAS and confirm
   the flagged files fail on your side too. A file that decodes fine
   in `ffplay` but errors in `<video>` is a browser/codec issue, not
   a decode issue — those don't belong on the delete list.

2. **Root cause per source**: the scraper adapter that produced each
   failing file is derivable from the sidecar's `source.site`. Some
   likely culprits:
   - `coomerfans`/`kemono` — long CDN downloads over an unstable
     connection; check for `stream.pipe(fs.createWriteStream)` paths
     without `finished` verification.
   - `reddit` — v.redd.it HLS/DASH assembly can leave truncated MP4s
     if a chunk 404s mid-download.
   - `stufferdb` — Playwright download hooks that don't wait for the
     download-complete signal.

3. **Fix the scraper end**: whichever adapter is landing partial
   files needs `Content-Length` verification, retry on truncation, or
   an atomic rename (`.tmp` → final) that only happens after the
   stream fully drains. Reference the recent `.mp4.tmp` /
   `.tmp.mp4` pattern in `dashboard/server.js::generateMobileVariant`
   for the atomicity pattern.

4. **Repair the corrupt inventory**: for each existing broken file,
   pick one:
   - **Re-download** if the source URL is still live. The sidecar's
     `source.mediaUrl` / `source.mediaPageUrl` is the entry point.
   - **Delete** if the source is gone. Use the dashboard's existing
     flag-for-deletion API (`POST /api/users/:user/flag`,
     `folder`/`filename`, `flagged: true`). That marks it for the
     next `runMediaMaintenance` sweep.

5. **Optional**: add a `--verify` mode to the scraper that runs the
   same tail-decode ffmpeg check on every downloaded file before
   accepting it. Cheap insurance against the same class of bug.

### Optional / stretch

- Emit a per-model failure count on the dashboard (small chip on the
  home tile) so bad batches surface quickly.
- Add the tail-decode check to the nightly maintenance pass so newly-
  broken files are flagged automatically over time.

---

## Appendix A — video failure lists

*Populated after the running scan completes. Paths are relative to
`/share/Vault69/dataset/`. If a section is empty, no files of that
class were found.*

### A.1 Header-pass failures (bad container / missing moov)

Confirmed by `ffprobe -v error` on 2026-07-10. All four are `.mp4`
files in `webm/` subdirs of Reddit-sourced datasets. ffprobe error
in parens:

```
chloe_curvingchloe_bigcuties_/webm/20210531062559-d5dd9e45.mp4    (moov atom not found)
chloe_curvingchloe_bigcuties_/webm/20210929051836-0205138b.mp4    (Invalid NAL unit size + moov not found)
chloe_curvingchloe_bigcuties_/webm/20211029203302-6375f777.mp4    (moov atom not found)
mary_boberry/webm/20240627201727-21d58928.mp4                     (moov atom not found)
```

"moov atom not found" is the textbook signature of a truncated
`.mp4` — the download stopped before the moov (metadata) index was
written to the tail. Playback is impossible without re-fetching.

### A.2 Tail-decode failures (incomplete download signature)

Full deep-pass on all 20,035 videos completed 2026-07-10 15:52 ET.
Command: `ffmpeg -v error -nostdin -sseof -3 -i FILE -f null -`
with benign non-monotonic-dts warnings filtered out. Failures = the
last 3 seconds fail to decode.

**40 files**, all in `webm/` subdirs:

```
acdc34434_laura_fatty/webm/20211130190134-b8fb1165.mp4
alissbonyt/webm/20210725113622-e3ed8dde.mp4
alissbonyt/webm/20230613001321-a4f0d4c4.mp4
alissbonyt/webm/20240717090533-39388514.mp4
alissbonyt/webm/20240717090929-4d40b18e.mp4
anybodylost6726/webm/2892ac-01960b49-1458-7a29-bdde-3ee1d600996c.mp4
azismiss/webm/20211112124847-da7e64cb.mp4
azismiss/webm/20211112124923-1a7754e3.mp4
azismiss/webm/20211112125015-1cbce647.mp4
azismiss/webm/20211112125211-fe895104.mp4
azismiss/webm/20211112125259-c9be00cc.mp4
bbw_breanna/webm/20201124105853-1e5ac909.mp4
beccabae/webm/20190114162527-f0dcd698.mp4
bella_abbondanza/webm/20210309181745-2b212428.mp4
bella_abbondanza/webm/20220612224814-0a58b229.mp4
bella_abbondanza/webm/20220612225606-91e96a34.mp4
bodylovebritt/webm/20211012172051-077cca0e.mp4
bodylovebritt/webm/20221211204030-d54dc28b.mp4
candii_kayn/webm/20231206222336-33eba64f.mp4
candii_kayn/webm/20240222214631-e304da1a.mp4
candii_kayn/webm/20240222215924-efb5e708.mp4
candii_kayn/webm/20251014200040-afd24d97.mp4
candii_kayn/webm/20251022214808-e7d844a4.mp4
candii_kayn/webm/20251226194125-39b622ae.mp4
candii_kayn/webm/20260401165520-c7c398ac.mp4
chloe_curvingchloe_bigcuties_/webm/20210531062559-d5dd9e45.mp4
chloe_curvingchloe_bigcuties_/webm/20210929051836-0205138b.mp4
chloe_curvingchloe_bigcuties_/webm/20211029203302-6375f777.mp4
lilmamakay/webm/20221114113245-aaf826ed.mp4
margot_bbw/.webm-backup/20210522205239-f6e3fe5e.webm       ← .webm-backup, not shown by dashboard
margot_bbw/webm/20210504163612-4ab07935.mp4
margot_bbw/webm/20210522205239-f6e3fe5e.webm
mary_boberry/webm/20240627201727-21d58928.mp4
muffinmaid/webm/20240711191651-7c7f330e.mp4
prettyplsgemini/webm/2892ac-01970e25-3706-7e0c-8cf1-e18dc49e7359.mp4
prettyplsgemini/webm/2892ac-019713b8-b1d8-7c06-8a0d-117eb40aefac.mp4
prettyplsgemini/webm/2892ac-019714d3-c4f9-7940-8dc9-2e6e8d865e21.mp4
pumpkincakezz/webm/20221107143937-e76d0c8d.mp4
sabrina_ssbbw/webm/20170823023109-52c5caa9.mp4
skylar_bc/webm/20251223092220-c4934bf3.mp4
```

Header-pass strict subset (all four): `chloe_curvingchloe_bigcuties_`
(3 files) + `mary_boberry` (1). Those are the "moov atom not found"
files — completely unplayable. The other 36 have parseable headers
but fail to decode near the end — the "download started, connection
dropped mid-stream, no verification before persisting" signature.

One extra: `margot_bbw/.webm-backup/…webm` shows a `.webm-backup`
folder — that's the pre-transcode archive the dashboard doesn't
list (it only shows `webm/`, `gif/`, `images/`). Whatever's making
these backups picked up a corrupt file too; worth checking the
same file's twin at `margot_bbw/webm/…webm` (also on this list).

### A.3 By-user summary

```
candii_kayn                       7 files
azismiss                          5 files
alissbonyt                        4 files
chloe_curvingchloe_bigcuties_     3 files
prettyplsgemini                   3 files
margot_bbw                        3 files   (1 in .webm-backup)
bella_abbondanza                  3 files
bodylovebritt                     2 files
skylar_bc                         1 file
sabrina_ssbbw                     1 file
pumpkincakezz                     1 file
muffinmaid                        1 file
mary_boberry                      1 file
lilmamakay                        1 file
beccabae                          1 file
bbw_breanna                       1 file
anybodylost6726                   1 file
acdc34434_laura_fatty             1 file
──────────────────────────────
total decode-fail                40 files
```

candii_kayn especially concentrated (7 in one model out of ~40
total failures), and their filenames span 2023-2026 — suggests
this model gets scraped repeatedly and the same adapter path
lands truncated files often. Prime candidate for reproducing the
scraper bug in Thread 2.

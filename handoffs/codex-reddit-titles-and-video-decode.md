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

Full deep-pass on all 20,035 videos running as of 10:25 ET. Uses
`ffmpeg -v error -nostdin -sseof -3 -i FILE -f null -`, filtering
out benign non-monotonic-dts warnings. Sample of 200 files hit 1
additional failure — rough extrapolation to ~100 across the full
inventory. Definitive list will be on the NAS at
`/tmp/videoscan-deep-failures.txt` when the pass finishes
(estimated ~40 min from launch, so ~11:05 ET).

The four header-pass files above are a subset of these — they all
fail the deep pass too.

### A.3 By-user summary

Partial (from header pass only):

```
chloe_curvingchloe_bigcuties_    3 files
mary_boberry                     1 file
────────────────────────────
total header-fail                4 files
```

Full by-user summary from the deep pass will be appended below once
the scan completes.

# Handoff: deploy dashboard fixes + run Reddit/GIF repairs against the NAS

**For:** a Claude agent running locally on the Windows machine (repo at
`F:\Dev\LoRA-Training`, NAS share mapped as `Z:\` = `/share/Vault69`).
**Why local:** the work was done in a cloud session that can't reach the NAS
(`192.168.50.13`) or run the PowerShell deploy. Everything below needs LAN
access to the NAS.

> **Update:** after deploying, steps 2 and 3 can instead be run from the
> dashboard's **Admin → Repairs** panel (Preview / Apply buttons, live log),
> which runs the same scripts inside the NAS container. The commands below
> are the manual fallback.

## Context

Branch `claude/confident-davinci-arlq2d` has fixes that are pushed but
**not yet merged to `main`**:

- Reddit captions showing "Reddit - The heart of the internet" (Reddit's
  own login/block page title was saved as the post title).
- Reddit captions cut off at ~50 chars (title rebuilt from the permalink
  slug when the real title wasn't fetched).
- Reddit "Posted" dates showing the download day (mp4 `creation_time` from
  local muxing beat the real post date; RSS used `<updated>`).
- Mobile model-page header squashed; Reddit source links never shown.
- GIFs that play a few frames then stop: likely truncated downloads.
  Downloads now fail on short responses; `audit:gifs` finds and re-fetches
  the existing truncated files.

The dashboard hides junk titles and fixes dates on its own after deploy.
The **scripts below** are needed to (a) fetch full Reddit titles and real
post dates into the sidecars, and (b) find and replace truncated GIFs.

## Steps

### 1. Merge and deploy

```powershell
cd F:\Dev\LoRA-Training
git fetch origin
git checkout main
git pull origin main
git merge --ff-only origin/claude/confident-davinci-arlq2d   # should fast-forward
git push origin main
npm ci
.\deploy-dashboard.ps1
```

If the fast-forward fails, `main` moved: merge normally and resolve, don't
force-push. After deploy, the first container start rescans every model
once (response-cache version bump), about as long as a nightly scan.

### 2. Reddit titles + post dates (dry run first)

The scraper must **not** be running at the same time: both write
`.media-dates.json`.

```powershell
node scrapyard/repairRedditMetadataTitles.js --dataset=Z:\dataset --fetch-missing
```

Check the summary: "Resolved N Reddit post(s) via api/info", "Would
correct N posted date(s)", and sample titles. Sample titles should be full
post titles, never "Reddit - …". If `api/info` batches report HTTP 403/429,
the script falls back to slower per-post HTML; add `--delay-ms=2000` if it
gets rate-limited. Scope to one model first with `--model=bellasky_`.

Then apply:

```powershell
node scrapyard/repairRedditMetadataTitles.js --dataset=Z:\dataset --fetch-missing --apply
```

The dashboard notices the sidecar change on its next fingerprint tick and
rescans. No restart needed.

### 3. GIF audit + repair

Report only:

```powershell
node audit/audit-gifs.js --dataset=Z:\dataset --model=bellasky_
```

Classifications: `truncated download` (the "cut off" symptom), `single
frame`, `not a gif (actually …)`. Then run on the whole dataset (drop
`--model`) to see how widespread truncation is, and by which site.

Re-download truncated files (dry run, then apply). `THUMB_DIR` **must**
point at the dashboard cache, or the old cut-off thumb and mobile MP4 keep
being served:

```powershell
node audit/audit-gifs.js --dataset=Z:\dataset --thumb-dir=Z:\dashboard-cache --model=bellasky_ --redownload
node audit/audit-gifs.js --dataset=Z:\dataset --thumb-dir=Z:\dashboard-cache --model=bellasky_ --redownload --apply
```

A file is only replaced if the fresh copy is a complete GIF **and** larger
than the existing one. Rows saying `skipped: fresh copy is also truncated`
mean the source itself is broken or gone; list those for the user rather
than deleting anything.

### 4. Verify on the dashboard

On a phone, open `bellasky_`:
- Captions are full Reddit titles; none say "Reddit - The heart of the internet".
- The lightbox "Posted" date matches the date on the Reddit post (open the link).
- The header shows name + stats on one row and controls on a second; a
  "Reddit u/…" link appears if the registry has a Reddit source for her
  (the repo copy of `model_aliases.json` only lists StufferDB; check `Z:\model_aliases.json`).
- Previously cut-off GIFs play through in the lightbox (SD and HD).

## If the GIF audit finds no truncated files

Then truncation isn't the cause. Ask the user whether "cut off" means the
animation stops early or the picture is cropped. Grid cards crop to a
square (`object-fit: cover`) by design; the lightbox shows the full frame.
Report what the audit found before changing anything.

## Don't

- Don't run the repair scripts while a scrape is running.
- Don't delete source media. The GIF script only replaces with a verified
  better copy.
- Don't raise `MOBILE_ENCODE_CONCURRENCY` above 6 (see `docker-compose.yml`).

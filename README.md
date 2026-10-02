# LoRA-Training Runbook

This repo is the local control center for collecting, repairing, reviewing, hashing, and syncing the Slopvault dataset.

## Paths

All paths come from [scrapyard/config.js](scrapyard/config.js); override them in the environment or `.env`.

- Local dataset root: `%APPDATA%\.slopvault\dataset` (`DATASET_DIR` / `LOCAL_DATASET_DIR`)
- Local quarantine root: `%APPDATA%\.slopvault\quarantine` (under `SLOPVAULT_ROOT`)
- Default NAS dataset root: `Z:\dataset` (`NAS_DATASET_DIR`)
- Model registry: [model_aliases.json](model_aliases.json) (`MODEL_REGISTRY_PATH`)

## Running scrapes from the dashboard

Open the dashboard's **Scrapes** page (the download icon in the header) to start a run (all sources, one model, or one URL), watch its live output, see run history, and turn on the nightly all-sources run.

- Runs are queued on the NAS (`Z:\slopvault-state\scrapes`). One source runs at a time across both machines, because every scrape updates the shared dedup records.
- The NAS runs Pawchive, OnlyHaven and Coomer sources itself.
- StufferDB, Tumblr and Reddit need a browser, so the PC runs them. They wait in the queue until the PC worker is online.
- Manual `npm run scrape ...` runs take the same lock, so they never collide with queued ones.

PC worker (once): `.\install-scrape-worker.ps1` registers a logon task that runs it in the background with no window (`-Uninstall` removes it). Its log is `%APPDATA%\.slopvault\scrape-worker.log`.

## Deploying the NAS dashboard

```powershell
.\deploy-dashboard.ps1            # deploy the commit checked out here
.\deploy-dashboard.ps1 -Rollback  # switch back to the previous deploy
```

The script packages the current commit (`git archive`, so uncommitted changes are not included), builds it on the NAS, keeps the previous deploy at `/share/Vault69/slopvault-dashboard.prev`, and restarts the container. The container runs as the share user (uid 1000), and each deploy hands any root-owned files in the dataset and dashboard cache back to that user, so the PC can always read and modify them over SMB.

The login password lives in `/share/Vault69/slopvault-dashboard/.env` as `DASHBOARD_PASSWORD`; the script asks for it the first time. One-time setup for passwordless SSH: `.\setup-deploy-ssh.ps1`.

## Adding a scrape source

Every source is one entry in [scrapyard/sources.js](scrapyard/sources.js). The router, the all-source runner, the registry, the scraper and both dashboards read from that list.

1. Write the adapter in `scrapyard/sourceAdapters/<name>.js`. It needs a `preflight` (fetch one page, report post count) and a `fetchPosts` that returns posts with `mediaEntries`. `tumblr.js` is the smallest example.
2. Add an entry to `SOURCES` in `scrapyard/sources.js`: `id`, `label`, `site` (written into sidecars, so never rename it later), `registryKey`, `runLabel`, `letter`, `engine: 'hoghaul'`, `runOrder`, `matchesHost`, `parseUrl`, and `preflight` / `fetchPosts` wrappers that pass the adapter what it needs from `ctx` (shared helpers like `fetchJson`) and `deps` (per-run state). Optional: `defaults`, `useBrowserMedia`, `searchUrl`, `mediaEntriesFromPost`.
3. Add a sample URL for it to `SAMPLE_URLS` in `scrapyard/testSources.js` and run `npm test`.
4. Try it: `npm run scrape -- "<url>" --preflight --skip-nas-sync`.

## Common Workflows

### 1. Scrape a new StufferDB model

```powershell
npm run scrape -- "https://stufferdb.com/index?/category/1234"
npm run scrape -- "https://stufferdb.com/index?/category/1234" --media-concurrency=10 --video-concurrency=6 --page-concurrency=5
```

Notes:
- `milkmaid` scrapes a StufferDB category and child categories.
- If the detected page alias is wrong, it now asks for:
  - the page alias as shown on the site
  - the canonical model bucket that alias belongs under
- To force a StufferDB scrape into a specific existing model bucket, use:

```powershell
npm run scrape -- "https://stufferdb.com/index?/category/22889" --model=heyyadriana
```

  That keeps the detected page alias for registry tracking, but saves the scrape into the `heyyadriana` dataset bucket.
- Concurrency can be tuned with `--media-concurrency=`, `--video-concurrency=`, and `--page-concurrency=`.
- `milkmaid` writes into the local Slopvault dataset first.

### 2. Scrape Coomer / Pawchive / mixed-source models

Single model, direct URL:

```powershell
npm run scrape -- "https://coomerfans.com/u/onlyfans/333819/cakedupkayyla" --model=cakedupkayyla
```

Single model, rerun through the registry batch path:

```powershell
npm run hoghaul:all-coomer -- --only-models=cakedupkayyla
npm run hoghaul:all-pawchive -- --only-models=candii_kayn
```

All models:

```powershell
npm run update:all-models
```

Source-specific batches:

```powershell
npm run update:stufferdb
npm run hoghaul:all-coomer
npm run hoghaul:all-pawchive
```

Interactive launcher:

```powershell
npm run scrape:interactive
```

Notes:
- CoomerFans URLs still live under `sources.coomer` in `model_aliases.json`.
- Pawchive prefers full-resolution media for posts marked `has_full: true`. Otherwise it downloads Pawchive's preview derivative, which is usually 800px on the long edge and comparable to the current dataset's median resolution.
- Pawchive previews are marked as needing a full-resolution upgrade. Run `npm run report:pawchive-previews` to list them and `npm run upgrade:pawchive-previews` to fully rescan Pawchive for newly available originals.
- An exact visual match to a materially larger existing file marks a Pawchive preview as resolved. A later full-resolution asset is still downloaded when its only visual match is a stored preview.
- Pawchive scrapes also download Dropbox shares and direct linked image/video files found in post content or embeds. Full post titles/captions and archived comments are stored in the media sidecar metadata.
- Hoghaul repeat scrapes reuse seen-media, existing files, and hash checks, so reruns should skip already handled media quickly.
- Useful Hoghaul tuning flags:

```powershell
npm run hoghaul:all-coomer -- --only-models=bbw_bonnie --video-concurrency=6 --image-concurrency=6 --post-concurrency=8
```

### 3. Force rerun a model

Use these when you want to revisit a model even if it already exists in the dataset.

Force rerun a StufferDB URL into a specific canonical model:

```powershell
npm run scrape -- "https://stufferdb.com/index?/category/22889" --model=heyyadriana
```

Rerun one StufferDB model from registry sources:

```powershell
npm run update:stufferdb -- --models=heyyadriana
```

Rerun one Coomer model from registry sources:

```powershell
npm run hoghaul:all-coomer -- --only-models=heyyadriana
```

Rerun one Pawchive model from registry sources:

```powershell
npm run hoghaul:all-pawchive -- --only-models=candii_kayn
```

Local repair-only revisit without a fresh scrape:

```powershell
npm run repair -- --model=heyyadriana
```

Notes:
- `--model=<name>` on `milkmaid` forces the canonical dataset bucket.
- `--only-models=` on Hoghaul batch runs narrows the batch to one or more canonical model names.
- `update:stufferdb -- --models=...` reruns registry-backed StufferDB models without needing to paste the source URL again.

### 4. Repair local model folders

```powershell
npm run repair
```

Use this when you want to check the local dataset model-by-model without doing a fresh scrape update first.

What it does:
- walks local model folders under `%APPDATA%\.slopvault\dataset`
- runs prune/backfill/validate for each selected model
- clears resolved `milkmaid-run-errors-*` artifacts when a model is now clean
- writes a top-level `%APPDATA%\.slopvault\errors-to-check-latest.md`

Useful variants:

```powershell
npm run repair -- --model=tianastummy
npm run repair -- --models=tianastummy,udderly_adorable
npm run repair -- --only-errors
npm run repair -- --scrape-only
npm run repair -- --start-from=laurenlushh
npm run repair -- --skip-nas-sync
```

### 5. Repair and scrape a StufferDB batch

```powershell
npm run repair:stufferdb -- --model=tianastummy
```

Use this when you want the repair pass to also rerun `milkmaid` from StufferDB sources before local prune/backfill/validate.

If you only want to revisit models still listed in `%APPDATA%\.slopvault\errors-to-check-latest.md`, use:

```powershell
npm run repair -- --only-errors
```

If you only want to refresh StufferDB pages and populate seen-media cache without
running prune/backfill/validate, use:

```powershell
npm run repair -- --scrape-only
```

To rebuild seen-media cache from existing historical milkmaid logs first, use:

```powershell
npm run backfill:seen-media
```

### 6. Run session repair for quarantined tail-decode videos

```powershell
npm run repair:tail-decode
```

What it does:
- salvages quarantined tail-decode videos
- promotes successful salvages back into the dataset
- runs prune/backfill/validate for affected models
- writes reports under `tmp/session-repair`

Useful variants:

```powershell
npm run repair:tail-decode -- --dry-run
npm run repair:tail-decode -- --model=udderly_adorable
npm run repair:tail-decode -- --all
npm run repair:tail-decode -- --limit=20
```

### 7. Review repair failures

```powershell
npm run report:repair-failures
```

Outputs:
- `tmp/repair-stufferdb/repair-failure-summary-latest.json`
- `tmp/repair-stufferdb/repair-failure-summary-latest.md`

### 8. Open the main model viewer

```powershell
npm run dashboard
```

Use this to browse a model’s media and metadata.

### 9. Review duplicate files

```powershell
npm run review:slopvault-duplicates-express
```

Use this for cross-model exact duplicate review.

Related commands:

```powershell
npm run manifest:slopvault-duplicates
npm run review:slopvault-duplicates
```

### 10. Review image orientation manually

```powershell
npm run review:orientation
```

Features:
- one model at a time
- all still images in filename order
- `R` rotate right and save
- `L` rotate left and save
- `Space` or `Right Arrow` accept and move next
- `Left Arrow` previous

### 11. Audit the Slopvault dataset

```powershell
npm run audit:slopvault
npm run manifest:slopvault
npm run review:slopvault
```

Use this for:
- quarantine review
- run-error review
- duplicate and audit findings

### 12. Rebuild or repair model hash data

```powershell
npm run prune:model-hashes -- --model=model_name
npm run backfill:model-hashes -- --model=model_name --include-video-visuals
npm run validate:model-hashes -- --model=model_name
```

Use these when a model’s hash stores drift from the actual files on disk.

### 13. Backfill support data

```powershell
npm run backfill:sources
npm run backfill:exif
npm run backfill:quarantine-manifest
```

Use these for:
- filling missing StufferDB source links
- `backfill:sources-interactive` auto-matches every model first and reports progress per model. Clean no-match results are remembered in an ignored runtime state file and skipped until aliases or missing sources change; use `--retry-auto` to force retries.
- extracting EXIF/uploaded dates into sidecars
- normalizing the quarantine manifest

## Where the dataset lives

The dataset lives only on the NAS (`Z:\dataset`). Scrapes on the PC write straight to it, and there is no local copy to sync. Sync and "evict the local copy" steps switch themselves off when the dataset and the NAS are the same folder (`scrapyard/datasetLocation.js`).

The old local copy under `%APPDATA%\.slopvault\dataset` is no longer used. `npm run evict:nas-media` (dry run) and `npm run evict:nas-media:apply` remove its files, but only those confirmed on the NAS at the same size.

## Script Reference

### Scraping

- `npm run scrape -- "<url>"`
  - scrape any supported source URL (StufferDB, Reddit, CoomerFans/OnlyHaven, Pawchive, Tumblr); the source is detected from the URL
- `npm run hoghaul:all-coomerfans`
  - batch scrape all `coomerfans.com` URLs stored under `sources.coomer`
- `npm run hoghaul:all-coomer`
  - batch scrape all `sources.coomer` entries
- `npm run hoghaul:all-pawchive`
  - batch scrape all Pawchive URLs stored under `sources.kemono`
- `npm run update:stufferdb`
  - refresh/update StufferDB models from the registry
- `npm run update:all-models`
  - run all configured StufferDB, Coomer, and Pawchive updates
- `npm run scrape:interactive`
  - interactive launcher for all-model, per-source, or pasted-URL scrapes
- `npm run repair`
  - local dataset repair pass across model folders, with prune/backfill/validate
- `npm run repair:stufferdb`
  - same repair pass, but with StufferDB scraping enabled first

### Repair and audit

- `npm run repair:tail-decode`
  - session repair for quarantined tail-decode videos
- `npm run report:repair-failures`
  - bucket and summarize repair failures
- `npm run audit:slopvault`
  - audit dataset/quarantine state
- `npm run manifest:slopvault`
  - rebuild the Slopvault manifest
- `npm run review:slopvault`
  - open the main Slopvault review dashboard

### Duplicate and orientation review

- `npm run manifest:slopvault-duplicates`
  - rebuild exact-duplicate manifest
- `npm run review:slopvault-duplicates`
  - serve the original duplicate dashboard
- `npm run review:slopvault-duplicates-express`
  - serve the newer guided duplicate review app
- `npm run review:orientation`
  - serve the manual rotation/orientation review app

### Hash and registry maintenance

- `npm run sort:model-aliases`
  - sort and normalize the model registry file
- `npm run purge-hashes`
  - clear model hash caches
- `npm run prune:model-hashes`
  - remove stale hash entries
- `npm run remap:model`
  - remap model dataset files between buckets
- `npm run backfill:model-hashes`
  - rebuild missing hash data
- `npm run validate:model-hashes`
  - compare dataset files to stored hash manifests
- `npm run validate:quarantine`
  - validate quarantine state against manifest

### Other support tools

- `npm run backfill:sources`
  - backfill missing StufferDB source/category metadata
- `npm run match:errored-video-visuals`
  - try to reconcile video visual hash problems
- `npm run backfill:quarantine-manifest`
  - normalize quarantine manifest metadata
- `npm run dashboard`
  - open the local model viewer

## Good Defaults

If you are unsure what to run:

1. Start scrapes from the dashboard's Scrapes page (or `npm run scrape -- "<url>"` on the PC).
2. Review anything suspicious:
   - `npm run review:slopvault`
   - `npm run review:slopvault-duplicates-express`
   - `npm run review:orientation`

## Known Gotcha

`model_aliases.json` often looks noisy on Windows because:
- the repo formatter wants `LF`
- Git is usually configured with `core.autocrlf=true`
- the scrapers also re-sort and update alias/source timestamps at runtime

So a dirty `model_aliases.json` does not always mean a meaningful manual edit.

# LoRA-Training Runbook

This repo is the local control center for collecting, repairing, reviewing, hashing, and syncing the Slopvault dataset.

## Paths

- Local dataset root: `%APPDATA%\.slopvault\dataset`
- Local quarantine root: `%APPDATA%\.slopvault\quarantine`
- Default NAS dataset root: `Z:\dataset`
- Model registry: [model_aliases.json](/C:/Users/jagsr/.codex/worktrees/5ed8/LoRA-Training/model_aliases.json)

## Common Workflows

### 1. Scrape a new StufferDB model

```powershell
npm run milkmaid -- "https://stufferdb.com/index?/category/1234"
npm run milkmaid -- "https://stufferdb.com/index?/category/1234" --media-concurrency=10 --video-concurrency=6 --page-concurrency=5
```

Notes:
- `milkmaid` scrapes a StufferDB category and child categories.
- If the detected page alias is wrong, it now asks for:
  - the page alias as shown on the site
  - the canonical model bucket that alias belongs under
- To force a StufferDB scrape into a specific existing model bucket, use:

```powershell
npm run milkmaid -- "https://stufferdb.com/index?/category/22889" --model=heyyadriana
```

  That keeps the detected page alias for registry tracking, but saves the scrape into the `heyyadriana` dataset bucket.
- Concurrency can be tuned with `--media-concurrency=`, `--video-concurrency=`, and `--page-concurrency=`.
- `milkmaid` writes into the local Slopvault dataset first.

### 2. Scrape Coomer / Kemono / mixed-source models

Single model, direct URL:

```powershell
npm run hoghaul -- "https://coomerfans.com/u/onlyfans/333819/cakedupkayyla" --model=cakedupkayyla
```

Single model, rerun through the registry batch path:

```powershell
npm run hoghaul:all-coomer -- --only-models=cakedupkayyla
npm run hoghaul:all-kemono -- --only-models=satanpanties666
```

All models:

```powershell
npm run update:all-models
```

Source-specific batches:

```powershell
npm run update:stufferdb
npm run hoghaul:all-coomer
npm run hoghaul:all-kemono
```

Interactive launcher:

```powershell
npm run scrape:interactive
```

Notes:
- CoomerFans URLs still live under `sources.coomer` in `model_aliases.json`.
- Hoghaul repeat scrapes reuse seen-media, existing files, and hash checks, so reruns should skip already handled media quickly.
- Useful Hoghaul tuning flags:

```powershell
npm run hoghaul:all-coomer -- --only-models=bbw_bonnie --video-concurrency=6 --image-concurrency=6 --post-concurrency=8
```

### 3. Force rerun a model

Use these when you want to revisit a model even if it already exists in the dataset.

Force rerun a StufferDB URL into a specific canonical model:

```powershell
npm run milkmaid -- "https://stufferdb.com/index?/category/22889" --model=heyyadriana
```

Rerun one StufferDB model from registry sources:

```powershell
npm run update:stufferdb -- --models=heyyadriana
```

Rerun one Coomer model from registry sources:

```powershell
npm run hoghaul:all-coomer -- --only-models=heyyadriana
```

Rerun one Kemono model from registry sources:

```powershell
npm run hoghaul:all-kemono -- --only-models=heyyadriana
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
- syncs touched models to the NAS
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
- syncs affected models to the NAS
- writes reports under `tmp/session-repair`

Important behavior:
- `npm run repair:tail-decode` keeps a persistent pending NAS sync queue in `tmp/session-repair/session-repair-state.json`
- if a repair run fails after touching models but before sync finishes, the next repair run will retry NAS sync for those pending models

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
- extracting EXIF/uploaded dates into sidecars
- normalizing the quarantine manifest

## NAS Sync

There are two ways to sync.

### Automatic

`npm run repair` now attempts to sync all touched local models to the NAS after maintenance. It also clears resolved model error logs so the next run only surfaces active local issues.

### Manual push local -> NAS

Push the full local dataset:

```powershell
.\update-nas.ps1
```

Push a single model:

```powershell
npm run sync --model=model_name
```

Notes:
- this uses `robocopy`
- it copies local dataset changes to the NAS model folder

### Manual pull NAS -> local

```powershell
.\update-local.ps1
```

This pulls the full NAS dataset down to local.

### Compare local vs NAS without copying

```powershell
.\compare-nas.ps1
```

This writes a dry-run diff log to [slopvault-diff.txt](/C:/Users/jagsr/.codex/worktrees/5ed8/LoRA-Training/slopvault-diff.txt).

## Script Reference

### Scraping

- `npm run milkmaid`
  - scrape a StufferDB gallery/category into the local dataset
- `npm run hoghaul`
  - scrape Coomer-backed sources
- `npm run hoghaul:all-coomerfans`
  - batch scrape all `coomerfans.com` URLs stored under `sources.coomer`
- `npm run hoghaul:all-coomer`
  - batch scrape all `sources.coomer` entries
- `npm run hoghaul:all-kemono`
  - batch scrape all `sources.kemono` entries
- `npm run update:stufferdb`
  - refresh/update StufferDB models from the registry
- `npm run update:all-models`
  - run all configured StufferDB, Coomer, and Kemono updates
- `npm run scrape:interactive`
  - interactive launcher for all-model, per-source, or pasted-URL scrapes
- `npm run repair`
  - local dataset repair pass across model folders, with prune/backfill/validate and NAS sync
- `npm run repair:stufferdb`
  - same repair pass, but with StufferDB scraping enabled first

### Repair and audit

- `npm run repair:tail-decode`
  - session repair for quarantined tail-decode videos plus NAS sync
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

- `npm run sync`
  - model-level NAS sync helper
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

1. Pull from NAS if you think local is behind:
   - `.\update-local.ps1`
2. Run the scrape or repair you need:
   - `npm run milkmaid -- "<url>"`
   - `npm run hoghaul -- "<url>" --model=model_name`
   - or `npm run repair`
3. Review anything suspicious:
   - `npm run review:slopvault`
   - `npm run review:slopvault-duplicates-express`
   - `npm run review:orientation`
4. Push back to NAS:
   - `.\update-nas.ps1`

## Known Gotcha

`model_aliases.json` often looks noisy on Windows because:
- the repo formatter wants `LF`
- Git is usually configured with `core.autocrlf=true`
- the scrapers also re-sort and update alias/source timestamps at runtime

So a dirty `model_aliases.json` does not always mean a meaningful manual edit.

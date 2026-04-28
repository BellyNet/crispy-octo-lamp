# Worktree Handoff

## Summary

This repo has been pushed a long way toward a trustworthy StufferDB repair pipeline.

The big shift was moving from "rerun and hope" to:

- quarantine suspicious media first
- rerun models in a repair-aware way
- validate media before promoting it back into the dataset
- keep structured hash coverage so we can map files back to models and paths
- generate reports for the remaining scraper/source failures

We are currently back on `main`.

## Important Files

### Scrapers
- [F:\Dev\LoRA-Training\milkmaid\milkmaid.js](F:/Dev/LoRA-Training/milkmaid/milkmaid.js) — StufferDB scraper
- [F:\Dev\LoRA-Training\hoghaul\hoghaul.js](F:/Dev/LoRA-Training/hoghaul/hoghaul.js) — Coomer.st scraper

### Model Registry (single source of truth)
- [F:\Dev\LoRA-Training\model_aliases.json](F:/Dev/LoRA-Training/model_aliases.json) — unified registry: all models, aliases, and per-platform sources
- [F:\Dev\LoRA-Training\scrapyard\modelRegistry.js](F:/Dev/LoRA-Training/scrapyard/modelRegistry.js) — shared registry module used by all scrapers

  Registry format:
  ```json
  {
    "canonical_name": {
      "aliases": ["alias1", "alias2"],
      "sources": {
        "stufferdb": [{ "url", "categoryId", "discoveredAs", "lastCheckedAt" }],
        "coomer":    [{ "url", "service", "discoveredAs", "lastCheckedAt" }]
      }
    }
  }
  ```
  All scrapers call `resolveAndTrackModel(registryPath, rawName, platform, sourceUrl)` — never write the registry directly.

### Coomer backfill tools
- [F:\Dev\LoRA-Training\hoghaul\backfill-coomer-sources.js](F:/Dev/LoRA-Training/hoghaul/backfill-coomer-sources.js) — auto-backfill: checks every alias against Coomer API, writes hits
- [F:\Dev\LoRA-Training\hoghaul\backfill-coomer-sources-interactive.js](F:/Dev/LoRA-Training/hoghaul/backfill-coomer-sources-interactive.js) — interactive: for the 52 models not auto-matched; try username variants, paste URLs

### Milkmaid repair / reporting
- [F:\Dev\LoRA-Training\milkmaid\repair-stufferdb-models.js](F:/Dev/LoRA-Training/milkmaid/repair-stufferdb-models.js)
- [F:\Dev\LoRA-Training\milkmaid\report-repair-failures.js](F:/Dev/LoRA-Training/milkmaid/report-repair-failures.js)

### Audit
- [F:\Dev\LoRA-Training\audit\audit-slopvault.js](F:/Dev/LoRA-Training/audit/audit-slopvault.js)
- [F:\Dev\LoRA-Training\audit\manifest-slopvault.js](F:/Dev/LoRA-Training/audit/manifest-slopvault.js)
- [F:\Dev\LoRA-Training\audit\review-slopvault.js](F:/Dev/LoRA-Training/audit/review-slopvault.js)
- [F:\Dev\LoRA-Training\audit\salvage-tail-videos.js](F:/Dev/LoRA-Training/audit/salvage-tail-videos.js)
- [F:\Dev\LoRA-Training\audit\salvage-quarantine-tail-videos.js](F:/Dev/LoRA-Training/audit/salvage-quarantine-tail-videos.js)

### Hashing / scrapyard
- [F:\Dev\LoRA-Training\scrapyard\hashStore.js](F:/Dev/LoRA-Training/scrapyard/hashStore.js)
- [F:\Dev\LoRA-Training\scrapyard\bitwiseHasher.js](F:/Dev/LoRA-Training/scrapyard/bitwiseHasher.js)
- [F:\Dev\LoRA-Training\scrapyard\visualHasher.js](F:/Dev/LoRA-Training/scrapyard/visualHasher.js)
- [F:\Dev\LoRA-Training\scrapyard\backfillModelHashes.js](F:/Dev/LoRA-Training/scrapyard/backfillModelHashes.js)
- [F:\Dev\LoRA-Training\scrapyard\pruneModelHashes.js](F:/Dev/LoRA-Training/scrapyard/pruneModelHashes.js)
- [F:\Dev\LoRA-Training\scrapyard\validateModelHashes.js](F:/Dev/LoRA-Training/scrapyard/validateModelHashes.js)
- [F:\Dev\LoRA-Training\scrapyard\backfillQuarantineManifest.js](F:/Dev/LoRA-Training/scrapyard/backfillQuarantineManifest.js)
- [F:\Dev\LoRA-Training\scrapyard\validateQuarantineState.js](F:/Dev/LoRA-Training/scrapyard/validateQuarantineState.js)
- [F:\Dev\LoRA-Training\scrapyard\remapModelData.js](F:/Dev/LoRA-Training/scrapyard/remapModelData.js)
- [F:\Dev\LoRA-Training\scrapyard\sortModelAliases.js](F:/Dev/LoRA-Training/scrapyard/sortModelAliases.js)

## Dataset State

Live dataset state is not stored in git. It lives under Slopvault:

- dataset root:
  [C:\Users\jagsr\AppData\Roaming\.slopvault\dataset](C:/Users/jagsr/AppData/Roaming/.slopvault/dataset)
- quarantine root:
  [C:\Users\jagsr\AppData\Roaming\.slopvault\quarantine](C:/Users/jagsr/AppData/Roaming/.slopvault/quarantine)

Important runtime files:

- legacy flat hashes:
  - `bitwiseHashes.json`
  - `visualHashes.json`
- active structured hashes:
  - `bitwiseHashes.v2.json`
  - `visualHashes.v2.json`
- permanent skip list:
  [C:\Users\jagsr\AppData\Roaming\.slopvault\milkmaid-permanent-skips.json](C:/Users/jagsr/AppData/Roaming/.slopvault/milkmaid-permanent-skips.json)
- quarantine manifest:
  [C:\Users\jagsr\AppData\Roaming\.slopvault\quarantine\quarantine-manifest.json](C:/Users/jagsr/AppData/Roaming/.slopvault/quarantine/quarantine-manifest.json)

## What Has Been Done

### Unified model registry + Coomer integration (latest session)

`model_aliases.json` is now the **single source of truth** for all model names
and sources across every scraper. Both milkmaid (StufferDB) and hoghaul (Coomer)
write into it through the shared `scrapyard/modelRegistry.js` module.

**`scrapyard/modelRegistry.js`** (new shared module)
- `sanitize`, `loadModelRegistry`, `saveModelRegistry`, `sortModelRegistry`
- `findCanonicalModelName`, `ensureModelEntryShape` — preserves all existing platform sources
- `upsertStufferdbSource`, `upsertCoomerSource`, `upsertGenericSource`
- `resolveAndTrackModel(registryPath, rawName, platform, sourceUrl)` — unified entry point; any future platform is one argument away

**`milkmaid/milkmaid.js`** — refactored to import from `modelRegistry.js`
- Removed ~160 lines of inline registry logic
- Call site updated: `resolveAndTrackModel(aliasMapPath, rawName, 'stufferdb', inputUrl)`

**`hoghaul/backfill-coomer-sources.js`** (new)
- Iterates all 78 registry models, tries every alias × every Coomer service
- Uses `/api/v1/{service}/user/{username}/profile` endpoint (`Accept: text/css` required)
- Writes hits to `sources.coomer` via `resolveAndTrackModel`
- Supports `--dry-run`, `--force`, `--delay=ms`
- Result: **26 of 78 models** matched and written to registry; 0 errors

**`hoghaul/backfill-coomer-sources-interactive.js`** (new)
- For the 52 models the auto-backfill missed
- Auto-probes all aliases first, then drops into a readline loop
- Type a username → probed against all 8 services → open hits in Yandex for y/s
- Paste a full `coomer.st` URL → validated via API → confirm to save
- `s` = skip model, `q` = quit; saves immediately on each accept

**EXIF removal** (completed earlier in session)
- `milkmaid/backfill-exif-dates.js` — rewritten: all `exifr` calls removed, now uses `media-dates.js` sidecars only
- `dashboard/server.js` — live EXIF fallback removed; only video date extraction remains
- `package.json` — `exifr` dependency removed; script renamed `backfill:dates`

**Coomer API notes for future agents**
- Endpoint: `GET https://coomer.st/api/v1/{service}/user/{username}/profile`
- Required header: `Accept: text/css` (without it you get HTTP 403)
- Services: `onlyfans fansly patreon candfans subscribestar gumroad afdian boosty`
- Stored URL format (browseable, not API): `https://coomer.st/{service}/user/{username}`
- Search endpoint (`/api/v1/creators?q=...`) exists but ignores the `q` param — direct profile lookup is the only reliable method

### Milkmaid repair-awareness

Milkmaid is now repair-aware:

- quarantined files are treated as missing
- stale dataset copies are cleared before retry
- lazy videos write to `incomplete/<model>/videos` first
- only validated media is promoted into dataset
- validation checks include:
  - `ffprobe`
  - duration sanity
  - tail decode
- known bad upstream files can be permanently skipped
- model runs write per-model logs and latest summaries

### Hashing work

Structured v2 hash stores were added and kept outside git in Slopvault.

Key decisions:

- old flat hash files were preserved
- new structured data lives in separate `v2` files
- refs were trimmed down to essential relative paths
- the files were minified to reduce size

Coverage status after the full repair pass:

- bitwise mismatches were effectively eliminated
- visual mismatches were reduced to a tiny outlier set
- remaining issues are mostly scraper/source problems, not hash integrity problems

### Audit / quarantine work

We built a quarantine-first audit flow:

- audit suspicious files first
- review in dashboard before moving anything
- quarantine decisions apply through the dashboard
- old reviewed quarantine findings no longer keep resurfacing
- quarantine manifest now tracks moved files and repair linkage

### Repair runner

The repair runner now:

1. reruns model from `model_aliases.json`
2. prunes stale hash refs
3. backfills current hashes
4. validates the model

Main file:

- [F:\Dev\LoRA-Training\milkmaid\repair-stufferdb-models.js](F:/Dev/LoRA-Training/milkmaid/repair-stufferdb-models.js)

### Failure reporting

A dedicated repair failure report now exists:

- [F:\Dev\LoRA-Training\tmp\repair-stufferdb\repair-failure-summary-latest.md](F:/Dev/LoRA-Training/tmp/repair-stufferdb/repair-failure-summary-latest.md)
- [F:\Dev\LoRA-Training\tmp\repair-stufferdb\repair-failure-summary-latest.json](F:/Dev/LoRA-Training/tmp/repair-stufferdb/repair-failure-summary-latest.json)

It includes:

- top-level failed models
- aggregated error categories
- per-file failures
- clickable gallery/source links

### Key debugging result

One important root cause was confirmed:

- many `media_error` failures were not selector bugs
- the same picture pages load fine one-by-one
- they fail when Milkmaid opens too many in parallel

We changed Milkmaid to:

- lower picture-page concurrency
- retry timed-out picture pages
- add lazy download timeouts so bad video streams cannot hang the whole run forever

This was validated on `kitty_piggy`.

## Latest Known Results

After the full `repair:stufferdb` batch:

- `74` models selected
- most models became hash-consistent
- total file writes in the last big repair run were about `2723`
- major remaining work is scraper/source cleanup rather than hash cleanup

Common failure themes:

- picture page navigation timeouts
- bad upstream videos with `tail_decode_error`
- a small number of process/config failures on specific models

## Known Problem Models

These were the models still needing focused diagnosis after the full repair run:

- `alissbonyt`
- `darya_smirnova`
- `kellijellibelli`
- `laurenlushh`
- `mary_boberry`
- `tianastummy`
- `udderly_adorable`

`kitty_piggy` was a good proof case:

- it originally hung and threw huge timeout counts
- after the concurrency + lazy-timeout patch, it finished cleanly
- the remaining failures there are now true source-side media failures, not whole-run hangs

## Useful Commands

Auto-backfill Coomer sources for all registry models:

```powershell
npm run backfill:coomer
# or with options:
node hoghaul/backfill-coomer-sources.js --dry-run --delay=500
node hoghaul/backfill-coomer-sources.js --force
```

Interactive Coomer backfill (for models the auto-pass missed):

```powershell
npm run backfill:coomer-interactive
```

Full repair batch:

```powershell
npm run repair:stufferdb
```

One model:

```powershell
node milkmaid\repair-stufferdb-models.js --model kitty_piggy
```

Selected models:

```powershell
node milkmaid\repair-stufferdb-models.js --models alissbonyt,darya_smirnova,kellijellibelli,laurenlushh,mary_boberry,tianastummy,udderly_adorable
```

Generate failure report:

```powershell
npm run report:repair-failures
```

Open review dashboard:

```powershell
npm run review:slopvault
```

Salvage one broken tail video:

```powershell
npm run salvage:tail-video -- "C:\full\path\to\broken.mp4"
```

Batch salvage quarantined tail videos:

```powershell
npm run salvage:tail-batch
```

## Recommended Next Steps

1. Rerun the remaining failed models with the current timeout/concurrency fixes.

2. Separate the remaining failures into buckets:
   - page concurrency / timeout issues
   - bad source configuration
   - process crash cases
   - upstream-bad media

3. Improve run-error review:
   - primary link should be the StufferDB picture page
   - raw `stufferai` URL should be secondary
   - actions should be `keep` or `permanent-skip`

4. Add automatic totals to the repair runner summary every time:
   - files saved
   - source items handled
   - dupes
   - errors

5. After scraper cleanup is stable, move on to duplicate/curation work:
   - duplicate dashboard
   - better video duplicate scoring
   - quality ranking
   - keep/delete review
   - orientation/cleanup checks

6. Run `npm run backfill:coomer-interactive` to manually resolve the 52 models
   still missing a Coomer source — try spelling variations and paste confirmed URLs.

## Local Notes

There are unrelated untracked local files in the worktree that were intentionally left alone:

- [F:\Dev\LoRA-Training\.claude](F:/Dev/LoRA-Training/.claude)
- [F:\Dev\LoRA-Training\future-ideas.md](F:/Dev/LoRA-Training/future-ideas.md)

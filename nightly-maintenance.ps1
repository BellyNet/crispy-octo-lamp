# nightly-maintenance.ps1
# Run the heavy iOS-compatibility passes that used to live in the
# per-model scrape sync. They're idempotent and skip any work that's
# already done, so re-running nightly is safe and cheap on a quiet day.
#
# Order matters:
#   1) WebM -> MP4 (H.264/AAC). iPhone Safari can't decode VP9/Opus at all.
#      Originals move to dataset/<user>/.webm-backup/.
#   2) Faststart remux. Moves the moov atom to the front of every
#      .mp4/.m4v that wasn't already faststart-optimised. Pure remux --
#      no re-encode, no quality loss.
#
# Default target is the local dataset (LOCAL_DATASET_DIR from .env). Pass
# `-Target NAS` to run against Z:\dataset instead, e.g. when only the NAS
# copy of a model needs touching up.
#
# Schedule via Task Scheduler:
#   Action  -> powershell.exe -NoProfile -File F:\Dev\LoRA-Training\nightly-maintenance.ps1
#   Trigger -> daily at 03:30 (before the dashboard's 04:00 nightly scan)

param(
  [ValidateSet('Local', 'NAS')]
  [string]$Target = 'Local'
)

$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

if ($Target -eq 'NAS') {
  $env:DATASET_DIR = 'Z:\dataset'
} else {
  Remove-Item Env:\DATASET_DIR -ErrorAction SilentlyContinue
}

Write-Host ''
Write-Host "[nightly] target = $Target" -ForegroundColor Cyan
Write-Host ''

$start = Get-Date

Write-Host '[1/2] webm -> mp4 transcode' -ForegroundColor Cyan
& node "$ScriptDir\scrapyard\transcodeWebm.js"
if ($LASTEXITCODE -ne 0) {
  Write-Host "webm transcode exited $LASTEXITCODE -- continuing" -ForegroundColor Yellow
}

Write-Host ''
Write-Host '[2/2] faststart remux' -ForegroundColor Cyan
& node "$ScriptDir\scrapyard\faststartMp4.js"
if ($LASTEXITCODE -ne 0) {
  Write-Host "faststart pass exited $LASTEXITCODE -- continuing" -ForegroundColor Yellow
}

$elapsed = (Get-Date) - $start
Write-Host ''
Write-Host ("[nightly] done in {0:N0}m {1:N0}s" -f $elapsed.TotalMinutes, $elapsed.Seconds) -ForegroundColor Green
Write-Host ''

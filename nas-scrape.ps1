# nas-scrape.ps1
# Runs one scrape on the NAS, inside the dashboard's image, against the NAS
# dataset. Your PC only starts it and shows the output.
#
#   .\nas-scrape.ps1 "https://pawchive.pw/patreon/user/123"
#   .\nas-scrape.ps1 "https://cum.st/creators/onlyfans/12345" --max-posts=20
#
# Takes the same options as `npm run scrape`. Only sources that don't need a
# browser work on the NAS (Pawchive, OnlyHaven, Coomer); StufferDB, Tumblr
# and Reddit need Chrome and refuse to start here, so run those on the PC.
#
# Until the NAS owns the model registry (scheduling work), each run uses a
# fresh working copy of Z:\model_aliases.json and discards its registry
# updates (mostly "last checked" times), so it can't overwrite the PC's copy.

param(
  [Parameter(Mandatory = $true, Position = 0)]
  [string]$Url,
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$ScrapeArgs = @()
)

$ErrorActionPreference = 'Stop'

$NasHost    = if ($env:NAS_HOST) { $env:NAS_HOST } else { '192.168.50.13' }
$NasUser    = if ($env:NAS_USER) { $env:NAS_USER } else { 'tjegan' }
$RemotePath = if ($env:NAS_PATH) { $env:NAS_PATH } else { '/share/Vault69/slopvault-dashboard' }
$StatePath  = '/share/Vault69/slopvault-state'
$Project    = 'slopvault-dashboard'
$Target     = "$NasUser@$NasHost"
$SshOptions = @('-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', '-o', 'StrictHostKeyChecking=accept-new')

# A PC scrape writes the same dedup hash stores; two writers lose updates.
$pcScrapers = Get-CimInstance Win32_Process -Filter "name = 'node.exe'" |
  Where-Object { $_.CommandLine -match 'run-scrape|run-source-batch|run-all-source-updates|run-stufferdb-batch|hoghaul\.js|milkmaid\.js|run-session-repair' }
if ($pcScrapers) {
  Write-Host 'A scrape is running on this PC; wait for it to finish first:' -ForegroundColor Red
  $pcScrapers | ForEach-Object { Write-Host "  PID $($_.ProcessId): $($_.CommandLine)" }
  exit 2
}

# Single-quote each argument for sh.
function ConvertTo-ShellArg([string]$Value) {
  return "'" + ($Value -replace "'", "'\''") + "'"
}
$quotedArgs = (@($Url) + $ScrapeArgs | ForEach-Object { ConvertTo-ShellArg $_ }) -join ' '

$script = @"
set -e
DOCKER="/share/CACHEDEV1_DATA/.qpkg/container-station/bin/docker"
export DOCKER_CLI_PLUGIN_EXTRA_DIRS="/share/CACHEDEV1_DATA/.qpkg/container-station/usr/local/lib/docker/cli-plugins"
export DOCKER_CONFIG="/tmp/docker-config-`$(id -un)"
mkdir -p "`$DOCKER_CONFIG" '$StatePath/tmp' '$StatePath/incomplete'

if [ -n "`$(`$DOCKER ps -q --filter name=slopvault-scrape)" ]; then
  echo "A NAS scrape is already running:"
  `$DOCKER ps --filter name=slopvault-scrape --format '  {{.Names}} (up {{.RunningFor}})'
  exit 4
fi

cp /share/Vault69/model_aliases.json '$StatePath/model_aliases.json'
cd '$RemotePath'
echo "[nas] Scraping on the NAS (deployed commit `$(cat DEPLOYED_COMMIT 2>/dev/null || echo unknown))..."
`$DOCKER compose -p $Project run --rm -T --no-deps \
  --name "slopvault-scrape-`$(date +%Y%m%d-%H%M%S)" \
  -e MODEL_REGISTRY_PATH=/data/state/model_aliases.json \
  -e SKIP_REGISTRY_PUSH=1 \
  -e MILKMAID_PROGRESS_MODE=plain \
  dashboard sh -c 'umask 0002 && exec node scrapyard/run-scrape.js "`$@"' nas-scrape $quotedArgs </dev/null
"@

$bytes = [Text.Encoding]::UTF8.GetBytes(($script -replace "`r", ''))
$encoded = [Convert]::ToBase64String($bytes)
$remote = "f=`$(mktemp) && printf %s $encoded | base64 -d > `$f && sh `$f </dev/null; rc=`$?; rm -f `$f; exit `$rc"

$saved = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
& ssh @SshOptions $Target $remote | Out-Host
$code = $LASTEXITCODE
$ErrorActionPreference = $saved

if ($code -eq 0) {
  Write-Host 'NAS scrape finished.' -ForegroundColor Green
} else {
  Write-Host "NAS scrape exited with code $code." -ForegroundColor Red
}
exit $code

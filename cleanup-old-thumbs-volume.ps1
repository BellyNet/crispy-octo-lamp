# cleanup-old-thumbs-volume.ps1
# Run this ONLY after migrate-thumbs-to-nas.ps1 has succeeded and you have
# verified the dashboard is healthy against the NAS bind mount. Removes the
# now-orphaned Docker named volume `dashboard-thumbs` and any dangling
# volumes left over from earlier deploys.
#
# Auth: SSH key first, plink.exe + .deploy-secrets.local fallback.

$ErrorActionPreference = 'Stop'

$NasHost     = if ($env:NAS_HOST) { $env:NAS_HOST } else { '192.168.50.13' }
$NasUser     = if ($env:NAS_USER) { $env:NAS_USER } else { 'tjegan' }
$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$SecretsFile = Join-Path $ScriptDir '.deploy-secrets.local'

$remoteCmd = @'
set -e

DOCKER="/share/CACHEDEV1_DATA/.qpkg/container-station/bin/docker"
COMPOSE_PLUGIN_DIR="/share/CACHEDEV1_DATA/.qpkg/container-station/usr/local/lib/docker/cli-plugins"

export DOCKER_CONFIG="/tmp/docker-config-tjegan"
mkdir -p "$DOCKER_CONFIG"
export DOCKER_CLI_PLUGIN_EXTRA_DIRS="$COMPOSE_PLUGIN_DIR"

# Sanity: refuse to prune if the current dashboard container is somehow
# still using it (would only happen if the redeploy step of the migration
# script was skipped).
IN_USE_BY=$($DOCKER ps -a --filter volume=dashboard-thumbs --format '{{.Names}}' 2>/dev/null || true)
if [ -n "$IN_USE_BY" ]; then
  echo "[remote] Refusing to remove: dashboard-thumbs still attached to: $IN_USE_BY"
  echo "[remote] Run migrate-thumbs-to-nas.ps1 first so the container mounts the bind path."
  exit 1
fi

if $DOCKER volume inspect dashboard-thumbs > /dev/null 2>&1; then
  BEFORE=$($DOCKER volume inspect dashboard-thumbs --format '{{.Mountpoint}}' 2>/dev/null || echo unknown)
  echo "[remote] Removing dashboard-thumbs (mountpoint was: $BEFORE)"
  $DOCKER volume rm dashboard-thumbs
  echo "[remote] Removed."
else
  echo "[remote] dashboard-thumbs not present — nothing to do."
fi

echo "[remote] Remaining volumes:"
$DOCKER volume ls
'@

Write-Host ''
Write-Host "Connecting to $NasUser@$NasHost..." -ForegroundColor Cyan

$sshArgs = @(
  '-o', 'BatchMode=yes',
  '-o', 'ConnectTimeout=5',
  '-o', 'StrictHostKeyChecking=accept-new',
  "$NasUser@$NasHost",
  $remoteCmd
)

$savedPref = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
& ssh @sshArgs
$sshExitCode = $LASTEXITCODE
$ErrorActionPreference = $savedPref

if ($sshExitCode -eq 0) {
  Write-Host ''
  Write-Host 'Cleanup complete.' -ForegroundColor Green
  exit 0
}
elseif ($sshExitCode -eq 255) {
  Write-Host 'SSH key auth failed. Trying password fallback...' -ForegroundColor Yellow

  if (-not (Test-Path $SecretsFile)) {
    Write-Host "No SSH key and no password file at $SecretsFile" -ForegroundColor Red
    exit 1
  }
  if (-not (Get-Command plink.exe -ErrorAction SilentlyContinue)) {
    Write-Host 'plink.exe not on PATH. Install PuTTY, or run setup-deploy-ssh.ps1.' -ForegroundColor Red
    exit 1
  }

  $password = (Get-Content -Raw $SecretsFile).Trim()

  $savedPref = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  & plink.exe -batch -pw $password "$NasUser@$NasHost" $remoteCmd
  $plinkExit = $LASTEXITCODE
  $ErrorActionPreference = $savedPref

  if ($plinkExit -ne 0) {
    Write-Host "plink exit $plinkExit" -ForegroundColor Red
    exit $plinkExit
  }

  Write-Host ''
  Write-Host 'Cleanup complete.' -ForegroundColor Green
  exit 0
}
else {
  Write-Host "Remote command failed with exit $sshExitCode." -ForegroundColor Red
  exit $sshExitCode
}

# migrate-thumbs-to-nas.ps1
# One-shot: switch dashboard's derived-asset storage from the Docker named
# volume `dashboard-thumbs` to a NAS bind mount at /share/Vault69/dashboard-cache.
#
# Sequence:
#   1) Mirror repo to the NAS (picks up the new docker-compose.yml)
#   2) SSH: stop container, copy volume contents into the bind-mount path,
#      then rebuild + recreate the container against the new mount
#   3) Print status
#
# After you've verified the dashboard is healthy, run
# .\cleanup-old-thumbs-volume.ps1 to drop the now-orphaned Docker volume.
#
# Idempotent: the migration copy only runs when the destination folder is
# empty AND the old volume still exists, so re-running this after cleanup
# is a no-op on the migration side (it still redeploys).
#
# Auth: SSH key first, plink.exe + .deploy-secrets.local fallback (same as
# deploy-dashboard.ps1).

$ErrorActionPreference = 'Stop'

$NasHost     = if ($env:NAS_HOST) { $env:NAS_HOST } else { '192.168.50.13' }
$NasUser     = if ($env:NAS_USER) { $env:NAS_USER } else { 'tjegan' }
$RemotePath  = if ($env:NAS_PATH) { $env:NAS_PATH } else { '/share/Vault69/slopvault-dashboard' }
$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$SecretsFile = Join-Path $ScriptDir '.deploy-secrets.local'

Write-Host ''
Write-Host '[1/3] Mirroring repo to NAS share...' -ForegroundColor Cyan

& "$ScriptDir\update-repo.ps1"

if ($LASTEXITCODE -gt 7) {
  Write-Host "robocopy returned exit $LASTEXITCODE, treated as failure" -ForegroundColor Red
  exit 1
}

$remoteCmdTemplate = @'
set -e
cd '{0}'

DOCKER="/share/CACHEDEV1_DATA/.qpkg/container-station/bin/docker"
COMPOSE_PLUGIN_DIR="/share/CACHEDEV1_DATA/.qpkg/container-station/usr/local/lib/docker/cli-plugins"
NAS_CACHE_DIR="/share/Vault69/dashboard-cache"

export DOCKER_CONFIG="/tmp/docker-config-tjegan"
mkdir -p "$DOCKER_CONFIG"
export DOCKER_CLI_PLUGIN_EXTRA_DIRS="$COMPOSE_PLUGIN_DIR"

echo "[remote] Docker:"
$DOCKER --version

# Make sure the bind-mount target exists (safe if already there)
mkdir -p "$NAS_CACHE_DIR"

echo "[remote] Checking migration prerequisites..."
HAS_OLD_VOLUME=no
if $DOCKER volume inspect dashboard-thumbs > /dev/null 2>&1; then
  HAS_OLD_VOLUME=yes
fi

DEST_EMPTY=no
if [ -z "$(ls -A "$NAS_CACHE_DIR" 2>/dev/null)" ]; then
  DEST_EMPTY=yes
fi

echo "[remote]   old volume present: $HAS_OLD_VOLUME"
echo "[remote]   destination empty:  $DEST_EMPTY"

if [ "$HAS_OLD_VOLUME" = "yes" ] && [ "$DEST_EMPTY" = "yes" ]; then
  echo "[remote] Stopping dashboard so nothing writes during the copy..."
  $DOCKER compose stop dashboard || true

  echo "[remote] Copying dashboard-thumbs volume -> $NAS_CACHE_DIR ..."
  # -a preserves timestamps so the existsSync short-circuits in the app
  # skip regeneration for every file that was already cached.
  $DOCKER run --rm \
    -v dashboard-thumbs:/from \
    -v "$NAS_CACHE_DIR":/to \
    alpine sh -c 'cp -a /from/. /to/ && echo "[remote] copied size: $(du -sh /to | cut -f1)"'
elif [ "$DEST_EMPTY" = "no" ]; then
  echo "[remote] Destination not empty — skipping migration copy (already done)."
else
  echo "[remote] Old volume not present — nothing to migrate."
fi

echo "[remote] Rebuilding and restarting with new mount..."
# --force-recreate so the container definitely picks up the new volume
# mount even when the image layers all hit cache.
$DOCKER compose up -d --build --force-recreate

echo "[remote] Containers:"
$DOCKER compose ps

echo "[remote] Recent logs:"
$DOCKER compose logs --tail=30 dashboard
'@

$remoteCmd = $remoteCmdTemplate -f $RemotePath

Write-Host ''
Write-Host "[2/3] Connecting to $NasUser@$NasHost..." -ForegroundColor Cyan

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
  # success
}
elseif ($sshExitCode -eq 255) {
  Write-Host ''
  Write-Host 'SSH key auth failed. Trying password fallback...' -ForegroundColor Yellow

  if (-not (Test-Path $SecretsFile)) {
    Write-Host ''
    Write-Host 'No SSH key and no password file. Options:' -ForegroundColor Red
    Write-Host '  1) Run .\setup-deploy-ssh.ps1 to enroll an SSH key, recommended' -ForegroundColor Yellow
    Write-Host "  2) Create $SecretsFile with one line: your NAS password" -ForegroundColor Yellow
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
}
else {
  Write-Host ''
  Write-Host "Remote command failed with exit $sshExitCode." -ForegroundColor Red
  exit $sshExitCode
}

Write-Host ''
Write-Host '[3/3] Migration + redeploy complete.' -ForegroundColor Green
Write-Host "Dashboard should be live at http://${NasHost}:3420" -ForegroundColor Green
Write-Host ''
Write-Host 'Next steps:' -ForegroundColor Cyan
Write-Host '  1) Open the dashboard on desktop AND iPhone — verify grid thumbs load'
Write-Host '     and lightbox playback works for a gif and a video.'
Write-Host '  2) When you are confident everything is good, run:'
Write-Host '       .\cleanup-old-thumbs-volume.ps1' -ForegroundColor Yellow
Write-Host ''

# deploy-dashboard.ps1
# 1) Mirror repo to the NAS share via update-repo.ps1
# 2) SSH into the NAS and rebuild the Docker container
# 3) Print container status and recent logs
#
# Auth: tries SSH key first. Falls back to plink.exe + password
# from .deploy-secrets.local if key auth fails.
#
# Override config via env vars: NAS_HOST, NAS_USER, NAS_PATH

$ErrorActionPreference = 'Stop'

$NasHost     = if ($env:NAS_HOST) { $env:NAS_HOST } else { '192.168.50.13' }
$NasUser     = if ($env:NAS_USER) { $env:NAS_USER } else { 'tjegan' }
$RemotePath  = if ($env:NAS_PATH) { $env:NAS_PATH } else { '/share/Vault69/slopvault-dashboard' }
$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$SecretsFile = Join-Path $ScriptDir '.deploy-secrets.local'

Write-Host ''
Write-Host '[1/3] Mirroring repo to NAS share...' -ForegroundColor Cyan

& "$ScriptDir\update-repo.ps1"

# robocopy success codes are 0-7; 8+ are errors
if ($LASTEXITCODE -gt 7) {
  Write-Host "robocopy returned exit $LASTEXITCODE, treated as failure" -ForegroundColor Red
  exit 1
}

# Build the remote command.
# Single-quoted here-string: PowerShell does not expand variables inside.
# Remote path gets inserted with -f.
$remoteCmdTemplate = @'
set -e
cd '{0}'

DOCKER="/share/CACHEDEV1_DATA/.qpkg/container-station/bin/docker"
COMPOSE_PLUGIN_DIR="/share/CACHEDEV1_DATA/.qpkg/container-station/usr/local/lib/docker/cli-plugins"

# Avoid QNAP Docker config permission weirdness
export DOCKER_CONFIG="/tmp/docker-config-tjegan"
mkdir -p "$DOCKER_CONFIG"

# Tell Docker where QNAP hid the compose plugin
export DOCKER_CLI_PLUGIN_EXTRA_DIRS="$COMPOSE_PLUGIN_DIR"

echo "[remote] Docker:"
$DOCKER --version

echo "[remote] Compose:"
$DOCKER compose version

echo "[remote] Rebuilding..."
$DOCKER compose up -d --build

echo "[remote] Containers:"
$DOCKER compose ps

echo "[remote] Recent logs:"
$DOCKER compose logs --tail=20
'@

$remoteCmd = $remoteCmdTemplate -f $RemotePath

Write-Host ''
Write-Host "[2/3] Connecting to $NasUser@$NasHost..." -ForegroundColor Cyan

# Try SSH key auth first.
# BatchMode=yes makes ssh fail fast instead of prompting.
$sshArgs = @(
  '-o', 'BatchMode=yes',
  '-o', 'ConnectTimeout=5',
  '-o', 'StrictHostKeyChecking=accept-new',
  "$NasUser@$NasHost",
  $remoteCmd
)

# Run ssh with ErrorActionPreference relaxed. Under 'Stop', every line
# ssh writes to stderr (host-key warnings, Docker BuildKit chatter, etc.)
# is wrapped as an ErrorRecord and terminates the script — sending us
# into the password-fallback path and exiting before `docker compose up
# -d` actually recreates the container. We use $LASTEXITCODE to
# distinguish the cases ssh itself reports:
#   0   → remote command succeeded
#   255 → ssh couldn't connect or authenticate (try the plink fallback)
#   *   → remote command ran but exited non-zero (propagate that exit)
#
# Inlined (no helper function) because a PowerShell function captures
# ssh's stdout into its return value, which would clobber $sshExitCode
# with every docker-build line ssh streams through stdout.
$savedPref = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
& ssh @sshArgs
$sshExitCode = $LASTEXITCODE
$ErrorActionPreference = $savedPref

if ($sshExitCode -eq 0) {
  # success — fall through to [3/3]
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
  Write-Host "Remote deploy command failed with exit $sshExitCode." -ForegroundColor Red
  Write-Host 'SSH connected fine — something failed on the NAS side. Check the output above.' -ForegroundColor Yellow
  exit $sshExitCode
}

Write-Host ''
Write-Host '[3/3] Deploy complete.' -ForegroundColor Green
Write-Host "Dashboard should be live again at http://${NasHost}:3420" -ForegroundColor Green
Write-Host ''
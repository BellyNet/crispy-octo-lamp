# deploy-dashboard.ps1
# Deploys the commit checked out in THIS folder to the NAS and rebuilds the
# dashboard container. Uncommitted changes are not deployed.
#
#   .\deploy-dashboard.ps1            deploy HEAD
#   .\deploy-dashboard.ps1 -Rollback  switch back to the previous deploy
#
# What a deploy does:
#   1. `git archive HEAD` -> copy to the NAS -> unpack into <path>.next,
#      carrying over the NAS's .env (DASHBOARD_PASSWORD lives there).
#   2. Build the image from <path>.next. A failed build leaves the running
#      dashboard untouched.
#   3. Keep the current folder as <path>.prev (for -Rollback) and swap in the
#      new one.
#   4. Stop the dashboard, hand any root-owned files in the dataset and
#      dashboard cache to the share user (1000:100), and start the new
#      container, which runs as that user.
#
# Auth: SSH key (run .\setup-deploy-ssh.ps1 once). Override the target with
# NAS_HOST, NAS_USER, NAS_PATH.

param(
  [switch]$Rollback
)

$ErrorActionPreference = 'Stop'

$NasHost    = if ($env:NAS_HOST) { $env:NAS_HOST } else { '192.168.50.13' }
$NasUser    = if ($env:NAS_USER) { $env:NAS_USER } else { 'tjegan' }
$RemotePath = if ($env:NAS_PATH) { $env:NAS_PATH } else { '/share/Vault69/slopvault-dashboard' }
$Project    = 'slopvault-dashboard'
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$Target     = "$NasUser@$NasHost"
$SshOptions = @('-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', '-o', 'StrictHostKeyChecking=accept-new')

# QNAP Container Station keeps docker and the compose plugin off PATH.
$DockerPreamble = @'
DOCKER="/share/CACHEDEV1_DATA/.qpkg/container-station/bin/docker"
export DOCKER_CLI_PLUGIN_EXTRA_DIRS="/share/CACHEDEV1_DATA/.qpkg/container-station/usr/local/lib/docker/cli-plugins"
export DOCKER_CONFIG="/tmp/docker-config-$(id -un)"
mkdir -p "$DOCKER_CONFIG"
'@

# Runs a shell script on the NAS, streaming its output. Returns the exit code.
# The script travels base64-encoded: Windows PowerShell 5.1 mangles double
# quotes in native-command arguments, and piping via stdin adds CRLFs.
# ssh's stderr (BuildKit progress, warnings) must not trip $ErrorActionPreference.
function Invoke-Remote([string]$Script) {
  $bytes = [Text.Encoding]::UTF8.GetBytes(($Script -replace "`r", ''))
  $encoded = [Convert]::ToBase64String($bytes)
  $saved = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  & ssh @SshOptions $Target "printf %s $encoded | base64 -d | sh" | Out-Host
  $code = $LASTEXITCODE
  $ErrorActionPreference = $saved
  return $code
}

function Fail([string]$Message, [int]$Code = 1) {
  Write-Host $Message -ForegroundColor Red
  exit $Code
}

$check = Invoke-Remote 'true'
if ($check -eq 255) {
  Fail "Can't reach $Target with an SSH key. Run .\setup-deploy-ssh.ps1 first."
}

if ($Rollback) {
  Write-Host "Rolling back $RemotePath to the previous deploy..." -ForegroundColor Cyan
  $script = @"
set -e
$DockerPreamble
DEST='$RemotePath'; PREV="`$DEST.prev"
[ -d "`$PREV" ] || { echo "No previous deploy at `$PREV"; exit 3; }
cd "`$DEST" && `$DOCKER compose -p $Project stop || true
cd /
mv "`$DEST" "`$DEST.rollback"
mv "`$PREV" "`$DEST"
mv "`$DEST.rollback" "`$PREV"
cd "`$DEST"
echo "[remote] Now running: `$(cat DEPLOYED_COMMIT 2>/dev/null || echo 'pre-git-archive deploy')"
`$DOCKER compose -p $Project up -d --build --force-recreate
`$DOCKER compose -p $Project ps
"@
  $code = Invoke-Remote $script
  if ($code -ne 0) { Fail "Rollback failed (exit $code)." $code }
  Write-Host 'Rollback complete.' -ForegroundColor Green
  exit 0
}

# ── 1. Package the commit ────────────────────────────────────────────────────
$sha = (& git -C $ScriptDir rev-parse --short HEAD).Trim()
$branch = (& git -C $ScriptDir rev-parse --abbrev-ref HEAD).Trim()
if (& git -C $ScriptDir status --porcelain) {
  Write-Host 'Note: uncommitted changes are NOT included in this deploy.' -ForegroundColor Yellow
}
Write-Host ''
Write-Host "[1/4] Packaging $branch @ $sha..." -ForegroundColor Cyan
$tarName = "slopvault-dashboard-$sha.tar"
$remoteTar = "$RemotePath.upload-$sha.tar"
$tarPath = Join-Path $env:TEMP $tarName
& git -C $ScriptDir archive --format=tar -o $tarPath HEAD
if ($LASTEXITCODE -ne 0) { Fail 'git archive failed.' }

$saved = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
& scp @SshOptions -q $tarPath "${Target}:$remoteTar"
$scpCode = $LASTEXITCODE
$ErrorActionPreference = $saved
Remove-Item $tarPath -ErrorAction SilentlyContinue
if ($scpCode -ne 0) { Fail "Copying the package to the NAS failed (exit $scpCode)." }

# ── 2. Make sure the NAS .env has a dashboard password ───────────────────────
$hasPassword = Invoke-Remote "grep -qs '^DASHBOARD_PASSWORD=' '$RemotePath/.env'"
if ($hasPassword -ne 0) {
  Write-Host ''
  Write-Host "No DASHBOARD_PASSWORD in $RemotePath/.env yet." -ForegroundColor Yellow
  $secure = Read-Host 'Choose the dashboard login password' -AsSecureString
  $plain = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
  if (-not $plain -or $plain -match "['`r`n]") {
    Fail 'Password must be non-empty and must not contain quotes or line breaks.'
  }
  # Sent base64-encoded so no character needs shell escaping; single-quoted
  # in .env so docker compose doesn't interpolate it.
  $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($plain))
  $plain = $null
  $code = Invoke-Remote @"
mkdir -p '$RemotePath'
printf "\nDASHBOARD_PASSWORD='%s'\n" "`$(printf %s '$b64' | base64 -d)" >> '$RemotePath/.env'
"@
  if ($code -ne 0) { Fail "Couldn't write the password to the NAS .env." }
}

# ── 3 + 4. Build, swap, fix ownership, start ─────────────────────────────────
Write-Host ''
Write-Host "[2/4] Building on $Target..." -ForegroundColor Cyan
$remoteScript = @"
set -e
$DockerPreamble
SHA='$sha'; DEST='$RemotePath'; NEXT="`$DEST.next"; PREV="`$DEST.prev"; TAR='$remoteTar'

rm -rf "`$NEXT"
mkdir -p "`$NEXT"
tar -xf "`$TAR" -C "`$NEXT"
rm -f "`$TAR"
[ -f "`$DEST/.env" ] && cp -p "`$DEST/.env" "`$NEXT/.env"
echo "`$SHA" > "`$NEXT/DEPLOYED_COMMIT"

cd "`$NEXT"
`$DOCKER compose -p $Project build

echo "[remote] [3/4] Swapping in the new deploy (previous kept at `$PREV)..."
if [ -d "`$DEST" ]; then
  cd "`$DEST"
  `$DOCKER compose -p $Project stop || true
  cd /
  rm -rf "`$PREV"
  mv "`$DEST" "`$PREV"
fi
mv "`$NEXT" "`$DEST"
cd "`$DEST"

echo "[remote] Handing root-owned files to the share user (1000:100)..."
`$DOCKER compose -p $Project run --rm --no-deps --user 0:0 --entrypoint sh dashboard -c '
  chown -R 1000:100 /data/thumbs &&
  chmod -R u+rwX,g+rwX,o+rX /data/thumbs &&
  find /data/dataset -xdev -user 0 -exec chown 1000:100 {} + &&
  echo "[remote] ownership ok"'

echo "[remote] [4/4] Starting the dashboard..."
`$DOCKER compose -p $Project up -d --force-recreate
`$DOCKER compose -p $Project ps
sleep 3
`$DOCKER compose -p $Project logs --tail=20
"@

$code = Invoke-Remote $remoteScript
if ($code -ne 0) {
  Fail "Deploy failed on the NAS (exit $code). If the swap already happened, .\deploy-dashboard.ps1 -Rollback restores the previous deploy." $code
}

Write-Host ''
Write-Host "Deployed $branch @ $sha." -ForegroundColor Green
Write-Host "Dashboard: http://${NasHost}:3420" -ForegroundColor Green
Write-Host ''

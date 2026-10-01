# setup-deploy-ssh.ps1
# One-time setup for passwordless SSH to the NAS.

$ErrorActionPreference = 'Stop'

$NasHost = if ($env:NAS_HOST) { $env:NAS_HOST } else { '192.168.50.13' }
$NasUser = if ($env:NAS_USER) { $env:NAS_USER } else { 'tjegan' }
$KeyPath = Join-Path $env:USERPROFILE '.ssh\id_ed25519'
$PubPath = "$KeyPath.pub"

Write-Host ''
Write-Host '--- SSH key setup for NAS deploys -------------------------------' -ForegroundColor Cyan
Write-Host "Target: $NasUser@$NasHost"
Write-Host "Key:    $KeyPath"
Write-Host ''

# 1. Generate key if it does not already exist
if (-not (Test-Path $KeyPath)) {
  Write-Host '[1/3] Generating new ed25519 key with no passphrase...' -ForegroundColor Cyan

  $sshDir = Split-Path -Parent $KeyPath
  if (-not (Test-Path $sshDir)) {
    New-Item -ItemType Directory -Force -Path $sshDir | Out-Null
  }

  & ssh-keygen -t ed25519 -N '' -f $KeyPath -C "lora-training-deploy"

  if ($LASTEXITCODE -ne 0) {
    Write-Host 'ssh-keygen failed' -ForegroundColor Red
    exit 1
  }
} else {
  Write-Host "[1/3] Reusing existing key at $KeyPath" -ForegroundColor Cyan
}

if (-not (Test-Path $PubPath)) {
  Write-Host "Public key file missing: $PubPath" -ForegroundColor Red
  exit 1
}

$pubKey = (Get-Content -Raw $PubPath).Trim()

Write-Host ''
Write-Host '[2/3] Copying public key to NAS authorized_keys...' -ForegroundColor Cyan
Write-Host 'You will be prompted for your NAS password once.' -ForegroundColor Gray
Write-Host ''

# Escape single quotes just in case.
$pubKeyEscaped = $pubKey.Replace("'", "'\''")

$remoteSetup = @"
set -e
mkdir -p ~/.ssh
chmod 700 ~/.ssh
touch ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
if ! grep -qF '$pubKeyEscaped' ~/.ssh/authorized_keys; then
  echo '$pubKeyEscaped' >> ~/.ssh/authorized_keys
  echo 'Key added.'
else
  echo 'Key already present.'
fi
"@

& ssh -o StrictHostKeyChecking=accept-new "$NasUser@$NasHost" $remoteSetup

if ($LASTEXITCODE -ne 0) {
  Write-Host ''
  Write-Host 'Copy failed. Common causes:' -ForegroundColor Red
  Write-Host '  - Wrong NAS password' -ForegroundColor Yellow
  Write-Host '  - SSH is disabled on the NAS' -ForegroundColor Yellow
  Write-Host '  - Public key auth is disabled in SSH settings' -ForegroundColor Yellow
  Write-Host '  - Home directory or ~/.ssh permissions are weird on the NAS' -ForegroundColor Yellow
  exit 1
}

# 3. Verify key-only login works
Write-Host ''
Write-Host '[3/3] Verifying passwordless login...' -ForegroundColor Cyan

& ssh `
  -o BatchMode=yes `
  -o ConnectTimeout=5 `
  -o StrictHostKeyChecking=accept-new `
  "$NasUser@$NasHost" `
  'echo OK from $(hostname)'

if ($LASTEXITCODE -eq 0) {
  Write-Host ''
  Write-Host 'All set. You can now run .\deploy-dashboard.ps1 without a password.' -ForegroundColor Green
  Write-Host 'If you have .deploy-secrets.local left over from before, delete it.' -ForegroundColor Gray
  Write-Host ''
} else {
  Write-Host ''
  Write-Host 'Key auth still not working.' -ForegroundColor Red
  Write-Host 'Try manually:' -ForegroundColor Yellow
  Write-Host "ssh -i `"$KeyPath`" $NasUser@$NasHost"
  exit 1
}
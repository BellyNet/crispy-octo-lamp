$local = "$env:APPDATA\.slopvault\dataset"
$nas   = "Z:\dataset"
$log   = Join-Path $PSScriptRoot "slopvault-push.log"

robocopy $local $nas /E /XJ /TEE /LOG:$log

# Keep the NAS dashboard's model registry in sync. docker-compose bind-mounts
# /share/Vault69/model_aliases.json (= Z:\model_aliases.json) into the container,
# so updating that file is enough — the dashboard rereads it on the next
# /api/users tick. Without this copy it drifts behind the local file and new
# model sources stop appearing as badges in the UI.
$registry = Join-Path $PSScriptRoot "model_aliases.json"
if ((Test-Path $registry) -and (Test-Path "Z:\")) {
  Copy-Item -Path $registry -Destination "Z:\model_aliases.json" -Force
}
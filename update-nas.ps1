$local = "$env:APPDATA\.slopvault\dataset"
$nas   = "Z:\dataset"
$log   = Join-Path $PSScriptRoot "slopvault-push.log"

robocopy $local $nas /E /XJ /TEE /LOG:$log
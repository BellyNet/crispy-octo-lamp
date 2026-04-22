$local = "$env:APPDATA\.slopvault\dataset"
$nas   = "Z:\dataset"
$log   = Join-Path $PSScriptRoot "slopvault-pull.log"

robocopy $nas $local /E /XJ /TEE /LOG:$log
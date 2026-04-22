$local = "$env:APPDATA\.slopvault\dataset"
$nas   = "Z:\dataset"
$log   = Join-Path $PSScriptRoot "slopvault-diff.txt"

robocopy $local $nas /E /L /FP /TS /TEE /LOG:$log /NJH /NJS /NDL
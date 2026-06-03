$local = "F:\Dev\LoRA-Training"
$nas   = "Z:\slopvault-dashboard"

robocopy $local $nas /E /XO /XJ /XD node_modules .git tmp incomplete /XF .env /TEE

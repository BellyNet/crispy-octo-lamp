# install-scrape-worker.ps1
# Runs the PC scrape worker in the background whenever you're logged in, with
# no window. It picks up the queued scrapes that need a browser (StufferDB,
# Tumblr, Reddit); watch them on the dashboard's Scrapes page.
#
#   .\install-scrape-worker.ps1             install (or update) and start it
#   .\install-scrape-worker.ps1 -Uninstall  stop it and remove the task
#
# Run it from the checkout the worker should use (normally F:\Dev\LoRA-Training).
# The worker's own log: %APPDATA%\.slopvault\scrape-worker.log

param(
  [switch]$Uninstall
)

$ErrorActionPreference = 'Stop'
$TaskName = 'LoRA Scrape Worker'
$Repo = $PSScriptRoot
$Launcher = Join-Path $Repo 'scripts\scrape-worker.vbs'

function Stop-Worker {
  Get-CimInstance Win32_Process -Filter "name = 'node.exe'" |
    Where-Object { $_.CommandLine -match 'scrapeWorker\.js' } |
    ForEach-Object {
      Write-Host "Stopping worker (PID $($_.ProcessId))"
      Stop-Process -Id $_.ProcessId -Force
    }
}

if ($Uninstall) {
  if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed the '$TaskName' task."
  }
  Stop-Worker
  exit 0
}

if (-not (Test-Path $Launcher)) {
  Write-Host "Missing $Launcher; run this from the repo checkout." -ForegroundColor Red
  exit 1
}

# Restart a crashed worker after a minute, indefinitely; never time out;
# ignore a second start while one is running.
$action = New-ScheduledTaskAction -Execute 'wscript.exe' -Argument "`"$Launcher`"" -WorkingDirectory $Repo
$trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -StartWhenAvailable `
  -MultipleInstances IgnoreNew `
  -ExecutionTimeLimit ([TimeSpan]::Zero) `
  -RestartCount 999 `
  -RestartInterval (New-TimeSpan -Minutes 1)

Stop-Worker
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
  -Description 'Runs queued LoRA-Training scrapes that need a browser. See the dashboard Scrapes page.' `
  -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName

Write-Host "Installed and started '$TaskName' (from $Repo)." -ForegroundColor Green
Write-Host 'It starts at logon with no window. Check it on the dashboard Scrapes page.'

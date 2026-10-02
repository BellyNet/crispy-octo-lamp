' Starts the PC scrape worker with no console window. Run at logon by the
' "LoRA Scrape Worker" task (install-scrape-worker.ps1). Exits with the
' worker's exit code so the task can restart it if it crashes.
Set fso = CreateObject("Scripting.FileSystemObject")
Set shell = CreateObject("WScript.Shell")
repo = fso.GetParentFolderName(fso.GetParentFolderName(WScript.ScriptFullName))
shell.CurrentDirectory = repo

node = shell.ExpandEnvironmentStrings("%ProgramFiles%") & "\nodejs\node.exe"
If Not fso.FileExists(node) Then node = "node"

WScript.Quit shell.Run("""" & node & """ scrapyard\scrapeWorker.js --id=pc", 0, True)

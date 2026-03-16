$ErrorActionPreference = "Stop"
$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$script = Join-Path $PSScriptRoot "scripts\run_keyword_monitor.py"
& $python $script @args

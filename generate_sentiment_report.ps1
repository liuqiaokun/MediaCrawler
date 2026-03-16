$ErrorActionPreference = "Stop"
$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$script = Join-Path $PSScriptRoot "scripts\generate_sentiment_report.py"
& $python $script @args

$ErrorActionPreference = "Stop"
$env:PROJECT_BASE_DIR = (Get-Location).Path
$env:DATA_DIR = Join-Path (Get-Location).Path "data"

python -m ingestion.download_sources
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
python -m ingestion.generate_booking_events
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
python -m ingestion.validate_files
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Local source data is ready under ./data"

param(
    [string]$SeasonLabels = $env:WORKER_DATASET_SEASON_LABELS,
    [string]$BaseUrl = "https://www.covers.com/sport/basketball/nba/teams",
    [string]$TeamCodes = $env:WORKER_DATASET_TEAM_CODES,
    [string]$RequestedBy = "phase-5-initial-production-dataset-load",
    [string]$RunLabel = "initial-production-dataset-load",
    [switch]$BrowserFallback,
    [switch]$StopOnError,
    [switch]$SkipPayloadPersistence
)

$ErrorActionPreference = "Stop"

$backendPath = Join-Path $PSScriptRoot "..\backend"
$resolvedBackendPath = Resolve-Path $backendPath
$backendSrcPath = Join-Path $resolvedBackendPath "src"
$originalPythonPath = $env:PYTHONPATH

Push-Location $resolvedBackendPath
try {
    if ([string]::IsNullOrWhiteSpace($originalPythonPath)) {
        $env:PYTHONPATH = $backendSrcPath
    }
    else {
        $env:PYTHONPATH = "$backendSrcPath$([IO.Path]::PathSeparator)$originalPythonPath"
    }

    $arguments = @(
        "-m",
        "bookmaker_detector_api.cli.initial_production_dataset_load",
        "--base-url",
        $BaseUrl,
        "--requested-by",
        $RequestedBy,
        "--run-label",
        $RunLabel
    )

    if (-not [string]::IsNullOrWhiteSpace($TeamCodes)) {
        $arguments += @("--team-codes", $TeamCodes)
    }

    if (-not [string]::IsNullOrWhiteSpace($SeasonLabels)) {
        $arguments += @("--season-labels", $SeasonLabels)
    }

    if ($StopOnError) {
        $arguments += "--stop-on-error"
    }

    if ($BrowserFallback) {
        $arguments += "--browser-fallback"
    }

    if ($SkipPayloadPersistence) {
        $arguments += "--skip-payload-persistence"
    }

    python @arguments
}
finally {
    $env:PYTHONPATH = $originalPythonPath
    Pop-Location
}

param(
    [string]$SourceUrlTemplate = $env:WORKER_DATASET_SOURCE_URL_TEMPLATE,
    [string]$TeamCodes = $env:WORKER_DATASET_TEAM_CODES,
    [string]$SeasonLabels = $env:WORKER_DATASET_SEASON_LABELS,
    [string]$RequestedBy = "phase-5-initial-production-dataset-load",
    [string]$RunLabel = "initial-production-dataset-load",
    [switch]$StopOnError,
    [switch]$SkipPayloadPersistence
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($SourceUrlTemplate)) {
    throw "SourceUrlTemplate is required. Set -SourceUrlTemplate or WORKER_DATASET_SOURCE_URL_TEMPLATE."
}

$backendPath = Join-Path $PSScriptRoot "..\backend"
$resolvedBackendPath = Resolve-Path $backendPath

Push-Location $resolvedBackendPath
try {
    $arguments = @(
        "-m",
        "bookmaker_detector_api.cli.initial_production_dataset_load",
        "--source-url-template",
        $SourceUrlTemplate,
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

    if ($SkipPayloadPersistence) {
        $arguments += "--skip-payload-persistence"
    }

    python @arguments
}
finally {
    Pop-Location
}

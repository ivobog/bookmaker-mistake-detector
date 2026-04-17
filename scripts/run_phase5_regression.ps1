param(
  [switch]$SkipBackend,
  [switch]$SkipFrontend
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$script:steps = @()

function Invoke-Step {
  param(
    [string]$Name,
    [scriptblock]$Action
  )

  Write-Host ""
  Write-Host "==> $Name" -ForegroundColor Cyan
  & $Action
  if ($LASTEXITCODE -ne 0) {
    throw "Step failed: $Name (exit code $LASTEXITCODE)"
  }
  $script:steps += $Name
}

Push-Location $repoRoot
try {
  if (-not $SkipBackend) {
    Invoke-Step "Backend Ruff" {
      python -m ruff check backend\src backend\tests
    }

    Invoke-Step "Backend Pytest" {
      python -m pytest backend\tests
    }

    Invoke-Step "Python Compile Check" {
      python -m compileall backend\src worker\src
    }
  }

  if (-not $SkipFrontend) {
    Push-Location (Join-Path $repoRoot "frontend")
    try {
      Invoke-Step "Frontend Typecheck" {
        npm.cmd run typecheck
      }

      Invoke-Step "Frontend Lint" {
        npm.cmd run lint
      }

      Invoke-Step "Frontend Build" {
        npm.cmd run build
      }
    }
    finally {
      Pop-Location
    }
  }

  Write-Host ""
  Write-Host "Phase 5 regression pass completed successfully." -ForegroundColor Green
  Write-Host "Completed steps:" -ForegroundColor Green
  foreach ($step in $script:steps) {
    Write-Host " - $step"
  }
}
finally {
  Pop-Location
}

<#
.SYNOPSIS
Refresh dashboard-ready admin climate bundle metrics for one state.

.DESCRIPTION
Computes only the NASA NEX climate metrics required by active thematic and
sector-wise dashboard bundles, then rebuilds legacy masters, thematic composite
masters, sector-wise proposal bundle masters, processed_optimised artifacts, and
strict parity reports.

Riverine Flood is excluded because it is sourced from the JRC flood-depth
workflow, not the NEX climate compute pipeline.

.EXAMPLE
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 -State Telangana -Level all

.EXAMPLE
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 -State Telangana -Level block -PlanOnly

.EXAMPLE
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 -State Telangana -Level all -LogRoot D:/projects/irt_data/processed_optimised/logs/dashboard_climate_refresh
#>

[CmdletBinding()]
param(
    [string]$State = "Telangana",

    [ValidateSet("district", "block", "all")]
    [string[]]$Level = @("all"),

    [int]$Workers = 36,

    [string]$Python = "",

    [ValidateSet("default", "preserve", "delete_after_ensemble")]
    [string]$YearlyCleanupPolicy = "preserve",

    [string]$ReportRoot = "",

    [string]$LogRoot = "",

    [switch]$PlanOnly,

    [switch]$SkipCompute,

    [switch]$SkipMaster,

    [switch]$SkipBundles,

    [switch]$SkipOptimized,

    [switch]$SkipAudit,

    [switch]$IncludeGeometry,

    [switch]$IncludeContext
)

$ErrorActionPreference = "Stop"

function Resolve-PythonCommand {
    param([string]$RequestedPython)

    function Test-PythonHasGeoPandas {
        param([string]$Candidate)

        if (-not $Candidate -or -not $Candidate.Trim()) {
            return $false
        }

        try {
            $null = & $Candidate -c "import geopandas" 2>$null
            return $LASTEXITCODE -eq 0
        } catch {
            return $false
        }
    }

    if ($RequestedPython -and $RequestedPython.Trim()) {
        return $RequestedPython
    }

    $candidates = New-Object System.Collections.Generic.List[string]

    if ($env:CONDA_PREFIX) {
        $condaPython = Join-Path $env:CONDA_PREFIX "python.exe"
        if (Test-Path $condaPython) {
            $candidates.Add($condaPython)
        }

        $posixCondaPython = Join-Path $env:CONDA_PREFIX "bin/python"
        if (Test-Path $posixCondaPython) {
            $candidates.Add($posixCondaPython)
        }
    }

    $knownWindowsIrtPython = Join-Path $env:USERPROFILE "AppData\Local\miniconda3\envs\irt\python.exe"
    if (Test-Path $knownWindowsIrtPython) {
        $candidates.Add($knownWindowsIrtPython)
    }

    $candidates.Add("python")

    foreach ($candidate in $candidates) {
        if (Test-PythonHasGeoPandas -Candidate $candidate) {
            return $candidate
        }
    }

    return $candidates[0]
}

$Python = Resolve-PythonCommand -RequestedPython $Python

function Format-CommandForDisplay {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Executable,

        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $parts = @($Executable) + $Arguments
    return ($parts | ForEach-Object {
        $part = [string]$_
        if ($part -match '[\s"]') {
            return '"' + ($part -replace '"', '\"') + '"'
        }
        return $part
    }) -join ' '
}

function Invoke-NativeChecked {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label,

        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    Write-Host ""
    Write-Host "==> $Label"
    $commandLine = Format-CommandForDisplay -Executable $Python -Arguments $Arguments
    Write-Host $commandLine

    if ($PlanOnly) {
        return
    }

    $script:StepIndex += 1
    $logToken = Get-SafePathToken -Value $Label
    $logPath = Join-Path $script:RunLogRoot ("{0:00}_{1}.log" -f $script:StepIndex, $logToken)
    Write-Host "log: $logPath"

    @(
        "timestamp_start=$((Get-Date).ToString('o'))"
        "label=$Label"
        "command=$commandLine"
        ""
    ) | Set-Content -Encoding UTF8 -Path $logPath

    & $Python @Arguments 2>&1 | Tee-Object -FilePath $logPath -Append
    $exitCode = $LASTEXITCODE

    @(
        ""
        "timestamp_end=$((Get-Date).ToString('o'))"
        "exit_code=$exitCode"
    ) | Add-Content -Encoding UTF8 -Path $logPath

    if ($exitCode -ne 0) {
        throw "$Label failed with exit code $exitCode. See log: $logPath"
    }
}

function Get-SafePathToken {
    param([Parameter(Mandatory = $true)][string]$Value)
    return ($Value -replace '[^A-Za-z0-9_.-]', '_')
}

$scopeCode = @'
import json
from india_resilience_tool.config.paths import get_paths_config
from tools.pipeline import compute_indices_multiprocess as CMP
from india_resilience_tool.config.dashboard_bundles import THEMATIC_DASHBOARD_BUNDLES, SECTOR_WISE_DASHBOARD_BUNDLES
from india_resilience_tool.config.bundle_weights import get_bundle_weights
from india_resilience_tool.config.metrics_registry import DOMAINS
from india_resilience_tool.config.proposal_bundles import get_proposal_bundle_source_metric_slugs

compute_slugs = {m["slug"] for m in CMP.METRICS}

thematic_composites = [
    spec.composite_slug
    for spec in THEMATIC_DASHBOARD_BUNDLES
    if spec.canonical_bundle != "Riverine Flood"
]

sector_composites = [spec.composite_slug for spec in SECTOR_WISE_DASHBOARD_BUNDLES]

wanted = set()
scored_by_bundle = {}
for spec in THEMATIC_DASHBOARD_BUNDLES:
    if spec.canonical_bundle == "Riverine Flood":
        continue
    entries = list(get_bundle_weights(spec.canonical_bundle))
    scored_by_bundle[spec.canonical_bundle] = {entry.metric_slug for entry in entries}
    for entry in entries:
        wanted.add(entry.metric_slug)

for spec in SECTOR_WISE_DASHBOARD_BUNDLES:
    for slug in get_proposal_bundle_source_metric_slugs(spec.composite_slug):
        wanted.add(slug)

# CHG-0012: include Heat Stress diagnostic slugs that appear under the domain
# but are not scored in composite_heat_stress (WBGT/SWBGT). They are now
# grid-first and must be refreshed alongside scored Heat Stress inputs.
heat_stress_domain = set(DOMAINS.get("Heat Stress", []))
heat_stress_scored = scored_by_bundle.get("Heat Stress", set())
diagnostic_slugs = sorted(
    (heat_stress_domain - heat_stress_scored - {"composite_heat_stress"}) & compute_slugs
)
wanted.update(diagnostic_slugs)

cfg = get_paths_config()
print(json.dumps({
    "source_metrics": sorted(wanted & compute_slugs),
    "diagnostic_slugs": diagnostic_slugs,
    "thematic_composites": thematic_composites,
    "sector_composites": sector_composites,
    "optimized_output_root": str(cfg.optimized_output_root),
}))
'@

$scopeJson = $scopeCode | & $Python -
if ($LASTEXITCODE -ne 0) {
    throw "Failed to resolve dashboard climate bundle scope."
}

$scope = $scopeJson | ConvertFrom-Json
$sourceMetrics = @($scope.source_metrics)
$diagnosticSlugs = @($scope.diagnostic_slugs)
$thematicComposites = @($scope.thematic_composites)
$sectorComposites = @($scope.sector_composites)
$optimizedMetrics = @($sourceMetrics + $thematicComposites + $sectorComposites | Sort-Object -Unique)

if ($sourceMetrics.Count -eq 0) {
    throw "No thematic/sector-wise source metrics were selected."
}

if (-not $ReportRoot) {
    $ReportRoot = [string]$scope.optimized_output_root
}

if (-not $LogRoot) {
    $LogRoot = Join-Path $ReportRoot "logs/dashboard_climate_refresh"
}

if ($Level -contains "all") {
    $levelsToRun = @("district", "block")
} else {
    $levelsToRun = @($Level | Sort-Object -Unique)
}

$stateToken = Get-SafePathToken -Value $State
$runToken = Get-Date -Format "yyyyMMdd_HHmmss"
$script:RunLogRoot = Join-Path (Join-Path $LogRoot $stateToken) $runToken
$script:StepIndex = 0

if (-not $PlanOnly) {
    New-Item -ItemType Directory -Path $script:RunLogRoot -Force | Out-Null
}

Write-Host "Dashboard climate bundle refresh"
Write-Host "  state: $State"
Write-Host "  levels: $($levelsToRun -join ', ')"
Write-Host "  python: $Python"
Write-Host "  source_metrics: $($sourceMetrics.Count)"
Write-Host "  diagnostic_slugs: $($diagnosticSlugs.Count)"
Write-Host "  thematic_composites: $($thematicComposites.Count)"
Write-Host "  sector_composites: $($sectorComposites.Count)"
Write-Host "  optimized_metric_roots: $($optimizedMetrics.Count)"
Write-Host "  report_root: $ReportRoot"
Write-Host "  log_root: $script:RunLogRoot"
Write-Host "  plan_only: $PlanOnly"

foreach ($levelName in $levelsToRun) {
    Write-Host ""
    Write-Host "##############################################################################"
    Write-Host "LEVEL: $($levelName.ToUpperInvariant())"
    Write-Host "##############################################################################"

    if (-not $SkipCompute) {
        $computeArgs = @(
            "-m", "tools.pipeline.compute_indices_multiprocess",
            "--state", $State,
            "--level", $levelName,
            "--overwrite",
            "--yearly-cleanup-policy", $YearlyCleanupPolicy,
            "--metrics"
        ) + $sourceMetrics
        Invoke-NativeChecked -Label "Compute source climate metrics ($levelName)" -Arguments $computeArgs
    }

    if (-not $SkipMaster) {
        $masterArgs = @(
            "-m", "tools.pipeline.build_master_metrics",
            "--state", $State,
            "--level", $levelName,
            "--workers", [string]$Workers,
            "--metrics"
        ) + $sourceMetrics
        Invoke-NativeChecked -Label "Build source metric masters ($levelName)" -Arguments $masterArgs
    }

    if (-not $SkipBundles) {
        $thematicArgs = @()
        foreach ($slug in $thematicComposites) {
            $thematicArgs += @("--metric", [string]$slug)
        }
        $compositeArgs = @(
            "-m", "tools.pipeline.build_composite_metrics",
            "--state", $State,
            "--level", $levelName,
            "--overwrite"
        ) + $thematicArgs
        Invoke-NativeChecked -Label "Build thematic composite masters ($levelName)" -Arguments $compositeArgs

        $sectorArgs = @()
        foreach ($slug in $sectorComposites) {
            $sectorArgs += @("--bundle", [string]$slug)
        }
        $proposalArgs = @(
            "-m", "tools.pipeline.build_proposal_bundles",
            "--state", $State,
            "--level", $levelName,
            "--overwrite"
        ) + $sectorArgs
        Invoke-NativeChecked -Label "Build sector-wise proposal bundle masters ($levelName)" -Arguments $proposalArgs
    }

    $optimizedArgs = @()
    foreach ($slug in $optimizedMetrics) {
        $optimizedArgs += @("--metric", [string]$slug)
    }

    $reportPath = Join-Path $ReportRoot "parity_report_${stateToken}_${levelName}_dashboard_climate.json"

    if (-not $SkipOptimized) {
        $buildArgs = @(
            "-m", "tools.optimized.build_processed_optimised",
            "--state", $State,
            "--level", $levelName
        ) + $optimizedArgs + @(
            "--overwrite",
            "--prune-scope",
            "--report-path", $reportPath
        )

        if (-not $IncludeGeometry) {
            $buildArgs += "--skip-geometry"
        }
        if (-not $IncludeContext) {
            $buildArgs += "--skip-context"
        }

        Invoke-NativeChecked -Label "Build processed_optimised dashboard climate artifacts ($levelName)" -Arguments $buildArgs
    }

    if (-not $SkipAudit) {
        $auditArgs = @(
            "-m", "tools.optimized.audit_processed_optimised_parity",
            "--state", $State,
            "--level", $levelName
        ) + $optimizedArgs + @(
            "--strict",
            "--report-path", $reportPath
        )

        if ($levelName -eq "block") {
            $auditArgs += "--require-block-yearly-models"
        }

        Invoke-NativeChecked -Label "Audit processed_optimised dashboard climate artifacts ($levelName)" -Arguments $auditArgs
    }
}

Write-Host ""
Write-Host "Dashboard climate source metrics selected: $($sourceMetrics.Count)"
Write-Host "Optimized dashboard metrics/composites selected: $($optimizedMetrics.Count)"
Write-Host "Refresh complete."

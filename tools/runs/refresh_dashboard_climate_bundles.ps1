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

    [string[]]$Bundle = @(),

    [switch]$Overwrite,

    [string[]]$OverwriteMetrics = @(),

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

# CHG-C6: -Workers is opt-in. Detect whether the user supplied it explicitly so we
# only forward --workers to compute/master when asked; otherwise each downstream CLI
# picks its own machine-aware default (compute defaults to 75% of cores).
$workersSupplied = $PSBoundParameters.ContainsKey('Workers')
if ($workersSupplied -and $Workers -lt 1) {
    throw "-Workers must be >= 1 when supplied (got $Workers)."
}

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
import os
import sys
from india_resilience_tool.config.paths import get_paths_config
from tools.pipeline import compute_indices_multiprocess as CMP
from india_resilience_tool.config.dashboard_bundles import (
    DASHBOARD_BUNDLES,
    THEMATIC_DASHBOARD_BUNDLES,
    SECTOR_WISE_DASHBOARD_BUNDLES,
)
from india_resilience_tool.config.bundle_weights import get_bundle_weights
from india_resilience_tool.config.metrics_registry import DOMAINS
from india_resilience_tool.config.proposal_bundles import get_proposal_bundle_source_metric_slugs


def _norm(value):
    return " ".join(str(value or "").strip().lower().split())


compute_slugs = {m["slug"] for m in CMP.METRICS}

# -Bundle scoping: PowerShell serializes the requested canonical bundle names as
# JSON into IRT_SELECTED_BUNDLES_JSON (robust against spaces / & / | in names).
selected_thematic = list(THEMATIC_DASHBOARD_BUNDLES)
selected_sector = list(SECTOR_WISE_DASHBOARD_BUNDLES)

raw = os.environ.get("IRT_SELECTED_BUNDLES_JSON", "").strip()
if raw:
    try:
        requested = json.loads(raw)
    except Exception as exc:  # noqa: BLE001
        print(f"Invalid IRT_SELECTED_BUNDLES_JSON: {exc}", file=sys.stderr)
        sys.exit(2)
    if isinstance(requested, str):
        requested = [requested]
    requested = [r for r in (requested or []) if str(r).strip()]
    if requested:
        canon_by_norm = {_norm(s.canonical_bundle): s.canonical_bundle for s in DASHBOARD_BUNDLES}
        resolved = []
        unknown = []
        for r in requested:
            key = _norm(r)
            if key in canon_by_norm:
                resolved.append(canon_by_norm[key])
            else:
                unknown.append(str(r))
        if unknown:
            valid = ", ".join(s.canonical_bundle for s in DASHBOARD_BUNDLES)
            print(
                f"Unknown -Bundle value(s): {', '.join(unknown)}. "
                f"Valid canonical bundles: {valid}",
                file=sys.stderr,
            )
            sys.exit(2)
        resolved_set = set(resolved)
        if "Riverine Flood" in resolved_set:
            print(
                "Riverine Flood is dashboard-visible but out of scope for this NEX "
                "climate refresh; use the JRC flood-depth workflow instead.",
                file=sys.stderr,
            )
            sys.exit(2)
        selected_thematic = [s for s in THEMATIC_DASHBOARD_BUNDLES if s.canonical_bundle in resolved_set]
        selected_sector = [s for s in SECTOR_WISE_DASHBOARD_BUNDLES if s.canonical_bundle in resolved_set]

thematic_composites = [
    spec.composite_slug
    for spec in selected_thematic
    if spec.canonical_bundle != "Riverine Flood"
]

sector_composites = [spec.composite_slug for spec in selected_sector]

wanted = set()
scored_by_bundle = {}
for spec in selected_thematic:
    if spec.canonical_bundle == "Riverine Flood":
        continue
    entries = list(get_bundle_weights(spec.canonical_bundle))
    scored_by_bundle[spec.canonical_bundle] = {entry.metric_slug for entry in entries}
    for entry in entries:
        wanted.add(entry.metric_slug)

for spec in selected_sector:
    for slug in get_proposal_bundle_source_metric_slugs(spec.composite_slug):
        wanted.add(slug)

# CHG-0012: include Heat Stress diagnostic slugs that appear under the domain
# but are not scored in composite_heat_stress (WBGT/SWBGT). They are grid-first
# and must be refreshed alongside scored Heat Stress inputs -- but only when
# Heat Stress is actually in scope.
diagnostic_slugs = []
if any(spec.canonical_bundle == "Heat Stress" for spec in selected_thematic):
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

# CHG-bundle: set IRT_SELECTED_BUNDLES_JSON only around the resolver invocation,
# then restore the prior value so it cannot leak into later Python calls.
$prevSelectedBundles = $env:IRT_SELECTED_BUNDLES_JSON
try {
    if ($Bundle.Count -gt 0) {
        $env:IRT_SELECTED_BUNDLES_JSON = ($Bundle | ConvertTo-Json -Compress)
    } else {
        Remove-Item Env:\IRT_SELECTED_BUNDLES_JSON -ErrorAction SilentlyContinue
    }
    $scopeJson = $scopeCode | & $Python -
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to resolve dashboard climate bundle scope (see message above)."
    }
} finally {
    if ($null -eq $prevSelectedBundles) {
        Remove-Item Env:\IRT_SELECTED_BUNDLES_JSON -ErrorAction SilentlyContinue
    } else {
        $env:IRT_SELECTED_BUNDLES_JSON = $prevSelectedBundles
    }
}

# CHG-C5: embedded, Streamlit-free master freshness probe.
#
# Intent matches india_resilience_tool/app/master_freshness.py (decide whether a
# master must be rebuilt), but uses the compute *completion markers* under
# processed/<slug>/.markers rather than scanning every *_periods.csv. Markers are
# written when a compute task/ensemble completes (so they are newer than the master
# whenever that slug was recomputed) and are the same signal --skip-existing trusts;
# scanning them is ~100x cheaper than walking the per-unit periods tree. A master is
# stale when it is missing, or when the newest in-scope marker is newer than it.
$freshnessCode = @'
import json
import os
import sys
from pathlib import Path
from india_resilience_tool.config.paths import get_paths_config

state = os.environ.get("IRT_FRESHNESS_STATE", "")
level = os.environ.get("IRT_FRESHNESS_LEVEL", "district")
raw = os.environ.get("IRT_FRESHNESS_SLUGS_JSON", "[]")
try:
    slugs = json.loads(raw)
except Exception:  # noqa: BLE001
    slugs = []
if isinstance(slugs, str):
    slugs = [slugs]
slugs = [str(s).strip() for s in (slugs or []) if str(s).strip()]

master_filename = "master_metrics_by_block.csv" if level == "block" else "master_metrics_by_district.csv"
scope_token = "scope=" + state
# Periods for a given admin level live under a level-specific subdir.
periods_dir = "blocks" if level == "block" else "districts"
base = Path(get_paths_config().base_output_root)

# When set, also compare against raw *_periods.csv mtimes (slower, but catches source
# changes that did not write a fresh compute marker -- e.g. -SkipCompute runs or manual
# periods repair). Always used as a fallback when a slug has no in-scope markers.
periods_fallback = str(os.environ.get("IRT_FRESHNESS_PERIODS_FALLBACK", "")).strip() in {"1", "true", "yes"}


def _newest_marker_mtime(slug):
    """Newest mtime among compute/ensemble markers for this slug at (state, level)."""
    markers_root = base / slug / ".markers"
    newest = 0.0
    candidates = []
    # Per-(model, scenario) compute task markers: compute/<level>/scope=<state>/...
    compute_scope = markers_root / "compute" / level / scope_token
    if compute_scope.exists():
        candidates.extend(compute_scope.rglob("*.json"))
    # Filter-aware ensemble markers: ensembles/<level>/scope=<state>/filters=*.json
    ensemble_scope = markers_root / "ensembles" / level / scope_token
    if ensemble_scope.exists():
        candidates.extend(ensemble_scope.rglob("*.json"))
    # Legacy ensemble marker file: ensembles/<level>/scope=<state>.json
    legacy_ensemble = markers_root / "ensembles" / level / (scope_token + ".json")
    if legacy_ensemble.exists():
        candidates.append(legacy_ensemble)
    for f in candidates:
        try:
            newest = max(newest, f.stat().st_mtime)
        except Exception:  # noqa: BLE001
            pass
    return newest


def _has_periods_newer_than(slug, threshold):
    """Return True as soon as any in-level *_periods.csv is newer than threshold."""
    periods_root = base / slug / state / periods_dir
    if not periods_root.exists():
        return False
    for f in periods_root.rglob("*_periods.csv"):
        try:
            if f.stat().st_mtime > threshold:
                return True
        except Exception:  # noqa: BLE001
            pass
    return False


stale = []
for slug in slugs:
    master_path = base / slug / state / master_filename
    if not master_path.exists():
        stale.append(slug)
        continue
    try:
        master_mtime = master_path.stat().st_mtime
    except Exception:  # noqa: BLE001
        stale.append(slug)
        continue
    threshold = master_mtime + 1.0
    newest_marker = _newest_marker_mtime(slug)
    if newest_marker > 0.0 and newest_marker > threshold:
        stale.append(slug)
        continue
    # Periods fallback: catch source changes with no fresh marker. Always applied when
    # the slug has no in-scope markers; also applied for every slug when the caller
    # requests it (e.g. -SkipCompute, where markers may not reflect the on-disk source).
    if (periods_fallback or newest_marker == 0.0) and _has_periods_newer_than(slug, threshold):
        stale.append(slug)

print(json.dumps({"stale": sorted(stale)}))
'@

function Get-StaleMasterSlugs {
    param(
        [Parameter(Mandatory = $true)][string]$StateName,
        [Parameter(Mandatory = $true)][string]$LevelName,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$Slugs,
        [switch]$PeriodsFallback
    )

    if ($Slugs.Count -eq 0) {
        return @()
    }

    $prevState = $env:IRT_FRESHNESS_STATE
    $prevLevel = $env:IRT_FRESHNESS_LEVEL
    $prevSlugs = $env:IRT_FRESHNESS_SLUGS_JSON
    $prevFallback = $env:IRT_FRESHNESS_PERIODS_FALLBACK
    try {
        $env:IRT_FRESHNESS_STATE = $StateName
        $env:IRT_FRESHNESS_LEVEL = $LevelName
        $env:IRT_FRESHNESS_SLUGS_JSON = ($Slugs | ConvertTo-Json -Compress)
        $env:IRT_FRESHNESS_PERIODS_FALLBACK = if ($PeriodsFallback) { "1" } else { "0" }
        $out = $freshnessCode | & $Python -
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to evaluate master freshness for $LevelName."
        }
        $parsed = $out | ConvertFrom-Json
        return @($parsed.stale)
    } finally {
        if ($null -eq $prevState) { Remove-Item Env:\IRT_FRESHNESS_STATE -ErrorAction SilentlyContinue } else { $env:IRT_FRESHNESS_STATE = $prevState }
        if ($null -eq $prevLevel) { Remove-Item Env:\IRT_FRESHNESS_LEVEL -ErrorAction SilentlyContinue } else { $env:IRT_FRESHNESS_LEVEL = $prevLevel }
        if ($null -eq $prevSlugs) { Remove-Item Env:\IRT_FRESHNESS_SLUGS_JSON -ErrorAction SilentlyContinue } else { $env:IRT_FRESHNESS_SLUGS_JSON = $prevSlugs }
        if ($null -eq $prevFallback) { Remove-Item Env:\IRT_FRESHNESS_PERIODS_FALLBACK -ErrorAction SilentlyContinue } else { $env:IRT_FRESHNESS_PERIODS_FALLBACK = $prevFallback }
    }
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

# CHG-C3: hybrid recompute validation. -Overwrite (everything in scope) and
# -OverwriteMetrics (a scoped subset) are mutually exclusive, and every
# -OverwriteMetrics slug must be in the resolved in-scope source-metric set.
if ($Overwrite -and $OverwriteMetrics.Count -gt 0) {
    throw "Specify either -Overwrite or -OverwriteMetrics, not both."
}
if ($OverwriteMetrics.Count -gt 0) {
    $invalidOverwrite = @($OverwriteMetrics | Where-Object { $sourceMetrics -notcontains $_ })
    if ($invalidOverwrite.Count -gt 0) {
        throw ("-OverwriteMetrics contains slug(s) not in scope: " +
            ($invalidOverwrite -join ', ') +
            ". In-scope source metrics: " + ($sourceMetrics -join ', '))
    }
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

# CHG-C10: keep full-scope parity reports at their established name, but give
# -Bundle runs a distinct, scope-tagged report so a bundle refresh cannot silently
# overwrite the full dashboard parity report (or a different bundle subset's).
if ($Bundle.Count -gt 0) {
    $bundleToken = Get-SafePathToken -Value (($Bundle | Sort-Object -Unique) -join "+")
    $reportScopeSuffix = "_bundle_${bundleToken}"
} else {
    $reportScopeSuffix = ""
}

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
        $computeBase = @(
            "-m", "tools.pipeline.compute_indices_multiprocess",
            "--state", $State,
            "--level", $levelName,
            "--yearly-cleanup-policy", $YearlyCleanupPolicy
        )
        if ($workersSupplied) {
            $computeBase += @("--workers", [string]$Workers)
        }

        if ($Overwrite) {
            # Force a full recompute of the in-scope set: --overwrite deletes first,
            # then --skip-existing no-ops on whatever remains.
            $computeArgs = $computeBase + @("--skip-existing", "--overwrite", "--metrics") + $sourceMetrics
            Invoke-NativeChecked -Label "Compute source climate metrics (overwrite) ($levelName)" -Arguments $computeArgs
        }
        elseif ($OverwriteMetrics.Count -gt 0) {
            # Pass 1: forced recompute of the requested subset only.
            $forceArgs = $computeBase + @("--overwrite", "--metrics") + $OverwriteMetrics
            Invoke-NativeChecked -Label "Compute forced-overwrite subset ($levelName)" -Arguments $forceArgs
            # Pass 2: skip-existing fills in the rest of the in-scope set.
            $skipArgs = $computeBase + @("--skip-existing", "--metrics") + $sourceMetrics
            Invoke-NativeChecked -Label "Compute remaining source metrics (skip-existing) ($levelName)" -Arguments $skipArgs
        }
        else {
            # Default: incremental compute via completion markers.
            $computeArgs = $computeBase + @("--skip-existing", "--metrics") + $sourceMetrics
            Invoke-NativeChecked -Label "Compute source climate metrics (skip-existing) ($levelName)" -Arguments $computeArgs
        }
    }

    if (-not $SkipMaster) {
        # CHG-C5: build_master_metrics --skip-existing short-circuits on whole-master
        # existence, not per-slug freshness, so a recomputed metric can leave a stale
        # master. Split the in-scope set into: force-rebuild (stale OR overwrite-forced)
        # and fresh (cheap --skip-existing no-op).
        # -SkipCompute means the on-disk source may have changed without a fresh marker
        # this run, so verify master freshness against raw *_periods.csv too.
        $staleSlugs = @(Get-StaleMasterSlugs -StateName $State -LevelName $levelName -Slugs $sourceMetrics -PeriodsFallback:$SkipCompute)

        $forcedSlugs = @()
        if ($Overwrite) {
            $forcedSlugs = @($sourceMetrics)
        }
        elseif ($OverwriteMetrics.Count -gt 0) {
            $forcedSlugs = @($OverwriteMetrics)
        }

        $rebuildSet = @(@($staleSlugs + $forcedSlugs) | Sort-Object -Unique | Where-Object { $sourceMetrics -contains $_ })
        $freshSet = @($sourceMetrics | Where-Object { $rebuildSet -notcontains $_ })

        Write-Host "  master rebuild (stale/forced): $($rebuildSet.Count); fresh skip-existing: $($freshSet.Count)"

        $masterBase = @(
            "-m", "tools.pipeline.build_master_metrics",
            "--state", $State,
            "--level", $levelName
        )
        if ($workersSupplied) {
            $masterBase += @("--workers", [string]$Workers)
        }

        if ($rebuildSet.Count -gt 0) {
            $rebuildArgs = $masterBase + @("--metrics") + $rebuildSet
            Invoke-NativeChecked -Label "Rebuild stale/forced source metric masters ($levelName)" -Arguments $rebuildArgs
        }
        if ($freshSet.Count -gt 0) {
            $freshArgs = $masterBase + @("--skip-existing", "--metrics") + $freshSet
            Invoke-NativeChecked -Label "Skip-existing source metric masters ($levelName)" -Arguments $freshArgs
        }
    }

    if (-not $SkipBundles) {
        # CHG-C8: an empty --metric / --bundle filter means "build everything" to the
        # underlying builders, which would silently undo -Bundle scoping. Only invoke
        # each builder when its family actually has in-scope composites.
        if ($thematicComposites.Count -gt 0) {
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
        }
        else {
            Write-Host ""
            Write-Host "==> No thematic bundles in scope; skipping thematic composite build ($levelName)"
        }

        if ($sectorComposites.Count -gt 0) {
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
        else {
            Write-Host ""
            Write-Host "==> No sector bundles in scope; skipping sector proposal build ($levelName)"
        }
    }

    $optimizedArgs = @()
    foreach ($slug in $optimizedMetrics) {
        $optimizedArgs += @("--metric", [string]$slug)
    }

    $reportPath = Join-Path $ReportRoot "parity_report_${stateToken}_${levelName}_dashboard_climate${reportScopeSuffix}.json"

    if (-not $SkipOptimized) {
        $buildArgs = @(
            "-m", "tools.optimized.build_processed_optimised",
            "--state", $State,
            "--level", $levelName
        ) + $optimizedArgs + @(
            "--overwrite",
            "--prune-scope",
            "--skip-audit",
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

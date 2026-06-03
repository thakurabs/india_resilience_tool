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

.PARAMETER FailOnComputeError
By default, per-task compute failures (e.g. metrics whose input years are
truncated/missing) are surfaced as warnings plus a *_compute_failures.json sidecar
and do not block publishing. Pass -FailOnComputeError to escalate any bundle that
had compute-task failures to a bundle failure (taint + skip publish + non-zero
exit), for strict/CI use. Ensemble failures always hard-fail regardless.

.EXAMPLE
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 -State Telangana -Level all

.EXAMPLE
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 -State Telangana -Level district -Bundle "Drought Risk" -FailOnComputeError

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

    [switch]$IncludeContext,

    # CHG-0046: per-bundle fail isolation publishes nothing by default when any
    # selected bundle fails (never produce a mixed-generation runtime). Opt in to
    # publish the succeeded subset and emit a *_partial_run.json manifest.
    [switch]$AllowPartialPublish,

    # CHG-0057: per-task compute failures (e.g. truncated/missing input years) are
    # surfaced as warnings + a *_compute_failures.json sidecar by default and do NOT
    # block publish. Opt in to escalate any bundle with compute failures to a bundle
    # failure (taint + skip publish + non-zero exit) for strict/CI use.
    [switch]$FailOnComputeError
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

    # CHG-0057: reset before any early return so a later (Plan/native) call can never
    # inherit a stale path; reassigned only once this call's $logPath is created.
    $script:LastStepLogPath = $null

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
    $script:LastStepLogPath = $logPath
    Write-Host "log: $logPath"

    @(
        "timestamp_start=$((Get-Date).ToString('o'))"
        "label=$Label"
        "command=$commandLine"
        ""
    ) | Set-Content -Encoding UTF8 -Path $logPath

    # Some pipeline entrypoints intentionally emit bootstrap/status text on stderr.
    # Treat the native process exit code as the failure signal here so informative
    # stderr lines do not become terminating PowerShell errors under Stop mode.
    $prevErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $Python @Arguments 2>&1 | Tee-Object -FilePath $logPath -Append
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $prevErrorActionPreference
    }

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

# CHG-0057: parse the per-task compute-failure count from a compute step log.
# The compute CLI emits exactly one stable summary line per single-level run
# (compute_indices_multiprocess.py:7257): "Computation: <s>s, Success: <n>, Failed: <n>".
# This anchors on that line and sums across matches; it does NOT collide with the
# progress logger ("... failed=<n> ...") or the ensemble hard-fail summary.
function Get-ComputeFailureCount {
    param([string]$LogPath)
    if ([string]::IsNullOrWhiteSpace($LogPath) -or -not (Test-Path -LiteralPath $LogPath)) {
        return 0
    }
    $total = 0
    foreach ($m in (Select-String -LiteralPath $LogPath -Pattern 'Computation:.*?,\s*Failed:\s*(\d+)' -AllMatches)) {
        foreach ($match in $m.Matches) {
            $total += [int]$match.Groups[1].Value
        }
    }
    return $total
}

# CHG-0057: wrap Invoke-NativeChecked for compute calls so every compute step's log and
# parsed failure count are recorded, even when the call throws (ensemble hard-fail). The
# record is appended in the finally so the original failure still propagates afterward.
function Invoke-ComputeChecked {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][ref]$Steps
    )
    try {
        Invoke-NativeChecked -Label $Label -Arguments $Arguments
    }
    finally {
        if (-not $PlanOnly) {
            $log = $script:LastStepLogPath
            $Steps.Value += [pscustomobject]@{
                label  = $Label
                log    = $log
                failed = (Get-ComputeFailureCount -LogPath $log)
            }
        }
    }
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

# CHG-0044 (C2): emit a per-bundle breakdown in addition to the flat union keys.
# Each bundle entry carries its own compute/master/composite scope so the runner can
# loop bundle-by-bundle with fail isolation, then run a single union optimized+audit
# pass over the bundles that succeeded. Bundles are emitted in DASHBOARD_BUNDLES
# (catalog) order so the report scope token is deterministic. Heat Stress diagnostic
# slugs (WBGT/SWBGT) are folded into the Heat Stress bundle's source_metrics so they
# are computed/mastered with that bundle, never globally.
sel_thematic_ids = {id(s) for s in selected_thematic}
sel_sector_ids = {id(s) for s in selected_sector}
bundles = []
for spec in DASHBOARD_BUNDLES:
    if spec.canonical_bundle == "Riverine Flood":
        continue
    if id(spec) in sel_thematic_ids:
        diags = list(diagnostic_slugs) if spec.canonical_bundle == "Heat Stress" else []
        srcs = sorted(
            (scored_by_bundle.get(spec.canonical_bundle, set()) | set(diags)) & compute_slugs
        )
        bundles.append({
            "canonical": spec.canonical_bundle,
            "family": "thematic",
            "composite_slug": spec.composite_slug,
            "source_metrics": srcs,
            "diagnostic_slugs": diags,
        })
    elif id(spec) in sel_sector_ids:
        srcs = sorted(
            set(get_proposal_bundle_source_metric_slugs(spec.composite_slug)) & compute_slugs
        )
        bundles.append({
            "canonical": spec.canonical_bundle,
            "family": "sector",
            "composite_slug": spec.composite_slug,
            "source_metrics": srcs,
            "diagnostic_slugs": [],
        })

cfg = get_paths_config()
print(json.dumps({
    "source_metrics": sorted(wanted & compute_slugs),
    "diagnostic_slugs": diagnostic_slugs,
    "thematic_composites": thematic_composites,
    "sector_composites": sector_composites,
    # CHG-0052: emit the per-bundle breakdown as a dedicated top-level JSON-array
    # *string* (not a nested array-of-objects). PowerShell re-parses this string with
    # ConvertFrom-Json, which is robust to the Windows PowerShell single-element-array
    # collapse that turned a nested `bundles` property into $null for one-bundle runs.
    "bundles_json": json.dumps(bundles),
    "base_output_root": str(cfg.base_output_root),
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
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from india_resilience_tool.config.paths import get_paths_config

# CHG-0048 (W2): optional per-slug timing for the freshness probe. stdout MUST stay
# JSON-only (Get-StaleMasterSlugs parses it with ConvertFrom-Json), so timings are
# attached as a "timings" field inside the JSON payload -- never printed as free text.
timing_enabled = str(os.environ.get("IRT_FRESHNESS_TIMING", "")).strip() in {"1", "true", "yes"}

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


# CHG-0055 (W2): the periods fallback is the freshness probe's hot spot. When nothing
# is newer than the master it must visit every *_periods.csv in the slug's tree (no
# early exit), and at block level those trees are huge -- a single slug measured at
# ~22 min, the full 54-slug block probe at ~69 min. Two fixes, both behaviour-preserving
# (the set of slugs reported stale is identical to the old rglob walk):
#   1. Split each slug's periods tree into independent walk units (one per immediate
#      subdirectory, recursive, plus one shallow unit for files directly under the root)
#      and process all units across a bounded thread pool. This load-balances one giant
#      tree across workers instead of letting the heaviest slug floor the wall time.
#   2. Walk with os.scandir + name.endswith (no per-entry Path/fnmatch alloc) and
#      early-exit the moment any newer file is found for a slug (stop_event), matching
#      the original "return True on first newer" semantics. Symlinked dirs are not
#      followed (periods trees contain none; avoids walk cycles).
def _periods_walk_units(slug):
    """Walk units covering ALL *_periods.csv under the slug's periods root.

    Returns a list of (kind, path): ("tree", subdir) walked recursively for each
    immediate subdirectory, plus ("shallow", root) for files directly under the root.
    Together these cover exactly the files pathlib rglob("*_periods.csv") would yield.
    """
    periods_root = base / slug / state / periods_dir
    if not periods_root.exists():
        return []
    units = []
    try:
        with os.scandir(periods_root) as it:
            for e in it:
                try:
                    if e.is_dir(follow_symlinks=False):
                        units.append(("tree", e.path))
                except OSError:
                    continue
    except OSError:
        return [("tree", str(periods_root))]
    units.append(("shallow", str(periods_root)))
    return units


def _unit_has_newer(kind, path, threshold, stop_event):
    """True if any *_periods.csv in this unit is newer than threshold (early-exit)."""
    if kind == "shallow":
        try:
            with os.scandir(path) as it:
                for e in it:
                    if stop_event.is_set():
                        return False
                    try:
                        if e.name.endswith("_periods.csv") and e.is_file(follow_symlinks=False):
                            if e.stat(follow_symlinks=False).st_mtime > threshold:
                                return True
                    except OSError:
                        continue
        except OSError:
            return False
        return False
    stack = [path]
    while stack:
        if stop_event.is_set():
            return False
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for e in it:
                    if stop_event.is_set():
                        return False
                    try:
                        if e.is_dir(follow_symlinks=False):
                            stack.append(e.path)
                        elif e.name.endswith("_periods.csv"):
                            if e.stat(follow_symlinks=False).st_mtime > threshold:
                                return True
                    except OSError:
                        continue
        except OSError:
            continue
    return False


# Phase 1 (cheap, serial): master existence + markers decide which slugs still need the
# periods fallback walk. This mirrors the original per-slug logic exactly.
stale = []
need_periods = []  # (slug, threshold)
phase1_t0 = time.perf_counter() if timing_enabled else 0.0
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
    if periods_fallback or newest_marker == 0.0:
        need_periods.append((slug, threshold))
phase1_seconds = (time.perf_counter() - phase1_t0) if timing_enabled else 0.0

# Phase 2 (parallel): walk the periods trees across a bounded thread pool. I/O-bound
# stat/scandir release the GIL, so threads are effective. Per-slug stop events give the
# same first-newer early-exit the serial walk had.
workers = 0
try:
    workers = int(str(os.environ.get("IRT_FRESHNESS_WORKERS", "")).strip() or "0")
except ValueError:
    workers = 0
if workers <= 0:
    workers = min(32, (os.cpu_count() or 8) * 2)

per_slug_seconds = {} if timing_enabled else None
periods_wall = 0.0
if need_periods:
    stop_events = {slug: threading.Event() for slug, _ in need_periods}
    found = set()
    found_lock = threading.Lock()
    time_lock = threading.Lock()
    work_units = []
    for slug, threshold in need_periods:
        for kind, path in _periods_walk_units(slug):
            work_units.append((slug, kind, path, threshold))

    def _run(unit):
        slug, kind, path, threshold = unit
        t = time.perf_counter() if timing_enabled else 0.0
        try:
            hit = _unit_has_newer(kind, path, threshold, stop_events[slug])
        finally:
            if timing_enabled:
                dt = time.perf_counter() - t
                with time_lock:
                    per_slug_seconds[slug] = per_slug_seconds.get(slug, 0.0) + dt
        if hit:
            stop_events[slug].set()
            with found_lock:
                found.add(slug)

    wall_t0 = time.perf_counter()
    if work_units:
        with ThreadPoolExecutor(max_workers=min(workers, len(work_units))) as ex:
            list(ex.map(_run, work_units))
    periods_wall = time.perf_counter() - wall_t0
    stale.extend(found)

payload = {"stale": sorted(set(stale))}
if timing_enabled:
    per_slug = per_slug_seconds or {}
    payload["timings"] = {k: round(v, 6) for k, v in sorted(per_slug.items(), key=lambda kv: kv[1], reverse=True)}
    payload["timing_meta"] = {
        "periods_wall_seconds": round(periods_wall, 4),
        "periods_work_seconds": round(sum(per_slug.values()), 4),
        "phase1_seconds": round(phase1_seconds, 4),
        "workers": workers,
        "slugs_walked": len(need_periods),
    }
print(json.dumps(payload))
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
        # CHG-0048 (W2): surface per-slug probe timings when IRT_FRESHNESS_TIMING is set
        # (the probe attaches them inside the JSON payload, slowest-first). This is the
        # measurement hook for deciding whether the periods-scan fallback needs optimizing.
        if ($null -ne $parsed.timings) {
            $total = 0.0
            $rows = @($parsed.timings.PSObject.Properties)
            Write-Host ("  [freshness timing] {0}: {1} slug(s), periods_fallback={2}" -f $LevelName, $Slugs.Count, [bool]$PeriodsFallback)
            foreach ($p in $rows) {
                $secs = [double]$p.Value
                $total += $secs
                Write-Host ("    {0,10:N4}s  {1}" -f $secs, $p.Name)
            }
            Write-Host ("    {0,10:N4}s  per-slug work (sum across {1} walked slugs)" -f $total, $rows.Count)
            if ($null -ne $parsed.timing_meta) {
                $m = $parsed.timing_meta
                Write-Host ("    {0,10:N4}s  PERIODS WALL (parallel, {1} workers)" -f [double]$m.periods_wall_seconds, [int]$m.workers)
                Write-Host ("    {0,10:N4}s  phase1 (markers/master)" -f [double]$m.phase1_seconds)
            }
        }
        return @($parsed.stale)
    } finally {
        if ($null -eq $prevState) { Remove-Item Env:\IRT_FRESHNESS_STATE -ErrorAction SilentlyContinue } else { $env:IRT_FRESHNESS_STATE = $prevState }
        if ($null -eq $prevLevel) { Remove-Item Env:\IRT_FRESHNESS_LEVEL -ErrorAction SilentlyContinue } else { $env:IRT_FRESHNESS_LEVEL = $prevLevel }
        if ($null -eq $prevSlugs) { Remove-Item Env:\IRT_FRESHNESS_SLUGS_JSON -ErrorAction SilentlyContinue } else { $env:IRT_FRESHNESS_SLUGS_JSON = $prevSlugs }
        if ($null -eq $prevFallback) { Remove-Item Env:\IRT_FRESHNESS_PERIODS_FALLBACK -ErrorAction SilentlyContinue } else { $env:IRT_FRESHNESS_PERIODS_FALLBACK = $prevFallback }
    }
}

# CHG-0044 (C2): deterministic, compact report scope token built from composite slugs
# in catalog order plus a short stable hash of that ordered set. Used for any subset or
# partial-success run so it can never silently overwrite a clean full-scope report.
function Get-ScopeToken {
    param([Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$CompositeSlugs)
    $ordered = @($CompositeSlugs)
    if ($ordered.Count -eq 0) { return "none" }
    $joined = ($ordered -join ",")
    $sha = [System.Security.Cryptography.SHA1]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($joined)
        $hash = -join ($sha.ComputeHash($bytes) | ForEach-Object { $_.ToString("x2") })
    } finally {
        $sha.Dispose()
    }
    $hash8 = $hash.Substring(0, 8)
    $short = ($ordered | ForEach-Object { ($_ -replace '^composite_', '') }) -join "+"
    $safe = Get-SafePathToken -Value $short
    if ($safe.Length -gt 48) { $safe = $safe.Substring(0, 48) }
    return "${safe}_${hash8}"
}

# CHG-0045 (W1.f): on a per-bundle failure, master writes are non-atomic
# (master_builder.py:1159-1164) and Phase-1 freshness treats a master newer than its
# markers as fresh. Delete the master CSV/parquet + the four state_*_<level>.csv summary
# siblings for the tainted slugs so a half-written master can never be reused -- the
# probe then reports the slug stale and a later bundle force-rebuilds it.
function Remove-TaintedMasterOutputs {
    param(
        [Parameter(Mandatory = $true)][string]$BaseOutputRoot,
        [Parameter(Mandatory = $true)][string]$StateName,
        [Parameter(Mandatory = $true)][string]$LevelName,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$Slugs
    )
    $masterCsv = if ($LevelName -eq "block") { "master_metrics_by_block.csv" } else { "master_metrics_by_district.csv" }
    $masterParquet = $masterCsv -replace '\.csv$', '.parquet'
    foreach ($slug in $Slugs) {
        $stateDir = Join-Path (Join-Path $BaseOutputRoot $slug) $StateName
        $targets = @(
            (Join-Path $stateDir $masterCsv),
            (Join-Path $stateDir $masterParquet),
            (Join-Path $stateDir "state_model_averages_${LevelName}.csv"),
            (Join-Path $stateDir "state_ensemble_stats_${LevelName}.csv"),
            (Join-Path $stateDir "state_yearly_model_averages_${LevelName}.csv"),
            (Join-Path $stateDir "state_yearly_ensemble_stats_${LevelName}.csv")
        )
        foreach ($t in $targets) {
            if (Test-Path -LiteralPath $t) {
                if ($PlanOnly) {
                    Write-Host "    [plan] would delete tainted master output: $t"
                } else {
                    Remove-Item -LiteralPath $t -Force -ErrorAction SilentlyContinue
                    Write-Host "    deleted tainted master output: $t"
                }
            }
        }
    }
}

$scope = $scopeJson | ConvertFrom-Json
$sourceMetrics = @($scope.source_metrics)
$diagnosticSlugs = @($scope.diagnostic_slugs)
$thematicComposites = @($scope.thematic_composites)
$sectorComposites = @($scope.sector_composites)
# CHG-0052: re-parse the dedicated bundles JSON-array string and normalize to a clean
# array of bundle objects. Re-parsing a top-level array string (rather than reading a
# nested `bundles` property) avoids the Windows PowerShell single-element-array collapse
# that previously surfaced one-bundle runs as a single $null iteration. Validate each
# entry's shape so a malformed resolver payload fails loudly instead of as a null-index.
$bundles = @()
foreach ($b in @([string]$scope.bundles_json | ConvertFrom-Json)) {
    if ($null -eq $b) { continue }
    # A missing 'canonical' deserializes to $null -> [string]$null = '' -> -not '' = $true.
    # (Avoid $b.PSObject.Properties['name'] here: that string indexer returns $null in
    # Windows PowerShell even when the property exists, which mis-fired this guard.)
    if (-not [string]$b.canonical) {
        throw "Resolver emitted a malformed bundle entry (missing 'canonical'): $($b | ConvertTo-Json -Compress -Depth 5)"
    }
    $bundles += $b
}
if ($bundles.Count -eq 0) {
    throw "Resolver returned no usable bundles for the selected scope."
}
$baseOutputRoot = [string]$scope.base_output_root
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

# CHG-0044 (C2): report naming is now decided per level, after the per-bundle loop, from
# the set of bundles that actually published (see Get-ScopeToken). Full-scope all-success
# runs keep the established name; any subset/partial run gets a tokenized name.

# Per-level outcomes for the end-of-run summary + non-zero exit on any bundle failure.
$runOutcomes = @()
$anyFailure = $false

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
Write-Host "  bundles: $($bundles.Count)"
Write-Host "  allow_partial_publish: $AllowPartialPublish"
Write-Host "  fail_on_compute_error: $FailOnComputeError"
Write-Host "  report_root: $ReportRoot"
Write-Host "  log_root: $script:RunLogRoot"
Write-Host "  plan_only: $PlanOnly"
if ($PlanOnly) {
    Write-Host "  [plan] compute-failure surfacing active (warn + *_compute_failures.json sidecar); fail_on_compute_error=$FailOnComputeError"
}

foreach ($levelName in $levelsToRun) {
    Write-Host ""
    Write-Host "##############################################################################"
    Write-Host "LEVEL: $($levelName.ToUpperInvariant())"
    Write-Host "##############################################################################"

    # CHG-0047 (W1.h): build the per-level slug-state cache ONCE. Each bundle subsets it
    # instead of re-invoking the freshness probe. Keys: slug -> { Stale, Forced, Tainted,
    # Done }. Done is set on bundle success so shared slugs are computed/mastered exactly
    # once; Tainted is set on bundle failure (CHG-0045) to force a later rebuild.
    $forcedAll = @()
    if ($Overwrite) {
        $forcedAll = @($sourceMetrics)
    }
    elseif ($OverwriteMetrics.Count -gt 0) {
        $forcedAll = @($OverwriteMetrics)
    }

    # CHG-C5: -SkipCompute means the on-disk source may have changed without a fresh
    # marker this run, so verify master freshness against raw *_periods.csv too.
    $staleSlugs = @()
    if (-not $SkipMaster) {
        $staleSlugs = @(Get-StaleMasterSlugs -StateName $State -LevelName $levelName -Slugs $sourceMetrics -PeriodsFallback:$SkipCompute)
    }

    $slugState = @{}
    foreach ($slug in $sourceMetrics) {
        $slugState[$slug] = [pscustomobject]@{
            Stale   = ($staleSlugs -contains $slug)
            Forced  = ($forcedAll -contains $slug)
            Tainted = $false
            Done    = $false
        }
    }
    Write-Host "  slug-state: stale=$($staleSlugs.Count) forced=$($forcedAll.Count) of $($sourceMetrics.Count) in-scope source metrics"

    $succeeded = @()   # bundle objects that completed compute->master->composite
    $failed = @()      # [pscustomobject]@{ bundle; error }
    # CHG-0057: per-level compute-failure records, appended before any -FailOnComputeError
    # throw so they survive the per-bundle catch and feed the summary + sidecar.
    $computeWarnings = @()  # [pscustomobject]@{ bundle; failed_total; steps }

    # CHG-0044 (C2): iterate bundle-by-bundle with fail isolation. The hybrid recompute /
    # freshness / worker logic is reused from Phase 1, just parameterized by the current
    # bundle's metric list (subset of $slugState) instead of the global union.
    # NOTE: the loop variable must NOT be $bundle -- that collides case-insensitively with
    # the [string[]]$Bundle parameter, whose type constraint would coerce each PSCustomObject
    # to a string array (blanking .canonical/.source_metrics). Use $bundleSpec.
    foreach ($bundleSpec in $bundles) {
        $bname = [string]$bundleSpec.canonical
        $bsources = @($bundleSpec.source_metrics)
        Write-Host ""
        Write-Host "------------------------------------------------------------------------------"
        Write-Host "BUNDLE: $bname [$($bundleSpec.family)] -> $($bundleSpec.composite_slug) ($levelName)"
        Write-Host "------------------------------------------------------------------------------"

        # CHG-0057: per-compute-step records for this bundle ({ label; log; failed }).
        $computeSteps = @()

        try {
            if (-not $SkipCompute) {
                $toCompute = @($bsources | Where-Object { -not $slugState[$_].Done })
                if ($toCompute.Count -gt 0) {
                    $computeBase = @(
                        "-m", "tools.pipeline.compute_indices_multiprocess",
                        "--state", $State,
                        "--level", $levelName,
                        "--yearly-cleanup-policy", $YearlyCleanupPolicy
                    )
                    if ($workersSupplied) {
                        $computeBase += @("--workers", [string]$Workers)
                    }

                    $forcedHere = @($toCompute | Where-Object { $slugState[$_].Forced })
                    if ($forcedHere.Count -gt 0 -and $Overwrite) {
                        # Force a full recompute of this bundle's set: --overwrite deletes
                        # first, then --skip-existing no-ops on whatever remains.
                        $computeArgs = $computeBase + @("--skip-existing", "--overwrite", "--metrics") + $toCompute
                        Invoke-ComputeChecked -Label "Compute [$bname] (overwrite) ($levelName)" -Arguments $computeArgs -Steps ([ref]$computeSteps)
                    }
                    elseif ($forcedHere.Count -gt 0) {
                        # Pass 1: forced recompute of the requested subset within this bundle.
                        $forceArgs = $computeBase + @("--overwrite", "--metrics") + $forcedHere
                        Invoke-ComputeChecked -Label "Compute [$bname] forced-overwrite subset ($levelName)" -Arguments $forceArgs -Steps ([ref]$computeSteps)
                        # Pass 2: skip-existing fills in the rest of this bundle's set.
                        $skipArgs = $computeBase + @("--skip-existing", "--metrics") + $toCompute
                        Invoke-ComputeChecked -Label "Compute [$bname] remaining (skip-existing) ($levelName)" -Arguments $skipArgs -Steps ([ref]$computeSteps)
                    }
                    else {
                        # Default: incremental compute via completion markers.
                        $computeArgs = $computeBase + @("--skip-existing", "--metrics") + $toCompute
                        Invoke-ComputeChecked -Label "Compute [$bname] (skip-existing) ($levelName)" -Arguments $computeArgs -Steps ([ref]$computeSteps)
                    }
                }
            }

            # CHG-0057: surface per-task compute failures (the compute CLI exits 0 on task
            # failures; only ensemble failures hard-fail). Default: warn + record, keep
            # publishing. -FailOnComputeError: escalate to a bundle failure via the catch.
            $bundleComputeFailed = [int](($computeSteps | Measure-Object -Property failed -Sum).Sum)
            if ($bundleComputeFailed -gt 0) {
                Write-Host "  WARNING: Compute [$bname] ($levelName) reported $bundleComputeFailed failed task(s); see step logs."
                $computeWarnings += [pscustomobject]@{
                    bundle       = $bname
                    failed_total = $bundleComputeFailed
                    steps        = @($computeSteps)
                }
                if ($FailOnComputeError) {
                    throw "Compute failures ($bundleComputeFailed) in bundle '$bname'; -FailOnComputeError is set"
                }
            }

            if (-not $SkipMaster) {
                # CHG-C5: build_master_metrics --skip-existing short-circuits on whole-master
                # existence, not per-slug freshness, so a recomputed metric can leave a stale
                # master. Rebuild stale|forced|tainted slugs; skip-existing the rest. Slugs an
                # earlier bundle already built (Done) are skipped entirely.
                $pending = @($bsources | Where-Object { -not $slugState[$_].Done })
                $rebuildSet = @($pending | Where-Object { $slugState[$_].Stale -or $slugState[$_].Forced -or $slugState[$_].Tainted })
                $freshSet = @($pending | Where-Object { $rebuildSet -notcontains $_ })

                Write-Host "  [$bname] master rebuild (stale/forced/tainted): $($rebuildSet.Count); fresh skip-existing: $($freshSet.Count)"

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
                    Invoke-NativeChecked -Label "Rebuild masters [$bname] ($levelName)" -Arguments $rebuildArgs
                }
                if ($freshSet.Count -gt 0) {
                    $freshArgs = $masterBase + @("--skip-existing", "--metrics") + $freshSet
                    Invoke-NativeChecked -Label "Skip-existing masters [$bname] ($levelName)" -Arguments $freshArgs
                }
            }

            if (-not $SkipBundles) {
                if ($bundleSpec.family -eq "thematic") {
                    $compositeArgs = @(
                        "-m", "tools.pipeline.build_composite_metrics",
                        "--state", $State,
                        "--level", $levelName,
                        "--overwrite",
                        "--metric", [string]$bundleSpec.composite_slug
                    )
                    Invoke-NativeChecked -Label "Build thematic composite [$bname] ($levelName)" -Arguments $compositeArgs
                }
                else {
                    $proposalArgs = @(
                        "-m", "tools.pipeline.build_proposal_bundles",
                        "--state", $State,
                        "--level", $levelName,
                        "--overwrite",
                        "--bundle", [string]$bundleSpec.composite_slug
                    )
                    Invoke-NativeChecked -Label "Build sector proposal [$bname] ($levelName)" -Arguments $proposalArgs
                }
            }

            # Mark this bundle's slugs done so shared slugs are not reprocessed by a later
            # bundle (single-pass shared work via the per-level cache). CHG-0053: also
            # CLEAR taint here -- if an earlier failed bundle tainted a shared slug and this
            # bundle has now successfully recomputed/remastered it, the slug is valid again
            # and must be eligible for the (partial) publish set, not permanently excluded.
            foreach ($slug in $bsources) {
                $slugState[$slug].Done = $true
                $slugState[$slug].Tainted = $false
            }
            $succeeded += $bundleSpec
        }
        catch {
            $msg = "$($_.Exception.Message)"
            Write-Host ""
            Write-Host "==> BUNDLE FAILED: $bname ($levelName) -- $msg"
            # CHG-0045 (W1.f): taint this bundle's not-yet-done slugs and delete any
            # half-written master outputs so a later bundle force-rebuilds them rather than
            # reusing a partial master that the freshness probe would treat as fresh.
            $taint = @($bsources | Where-Object { -not $slugState[$_].Done })
            foreach ($slug in $taint) {
                $slugState[$slug].Tainted = $true
                $slugState[$slug].Stale = $true
            }
            if ($taint.Count -gt 0) {
                Write-Host "  tainting $($taint.Count) slug(s); cleaning half-written master outputs"
                Remove-TaintedMasterOutputs -BaseOutputRoot $baseOutputRoot -StateName $State -LevelName $levelName -Slugs $taint
            }
            $failed += [pscustomobject]@{ bundle = $bname; error = $msg }
            $anyFailure = $true
        }
    }

    # CHG-0046 (W1.g): single union optimized+audit pass over the bundles that succeeded.
    # Default policy never produces a mixed-generation runtime: if any bundle failed and
    # -AllowPartialPublish was not supplied, skip optimized+audit entirely for this level.
    $publishedComposites = @($succeeded | ForEach-Object { [string]$_.composite_slug })
    $publishedSources = @()
    foreach ($b in $succeeded) { $publishedSources += @($b.source_metrics) }
    $publishedSources = @($publishedSources | Sort-Object -Unique | Where-Object { -not $slugState[$_].Tainted })
    $publishSet = @($publishedSources + $publishedComposites | Sort-Object -Unique)

    $levelHadFailure = ($failed.Count -gt 0)
    $publish = $true
    $publishReason = ""
    if ($levelHadFailure -and -not $AllowPartialPublish) {
        $publish = $false
        $publishReason = "optimized+audit skipped (bundle failure; -AllowPartialPublish not set); runtime left at last-good state"
    }
    elseif ($succeeded.Count -eq 0) {
        $publish = $false
        $publishReason = "optimized+audit skipped (no bundle succeeded)"
    }
    elseif ($publishSet.Count -eq 0) {
        $publish = $false
        $publishReason = "optimized+audit skipped (empty publish set)"
    }

    # CHG-0044 (W1.d): full-scope all-success keeps the established report name; any subset
    # or partial run gets a deterministic tokenized name so it cannot overwrite a clean one.
    if ($Bundle.Count -eq 0 -and -not $levelHadFailure) {
        $scopeToken = ""
        $reportPath = Join-Path $ReportRoot "parity_report_${stateToken}_${levelName}_dashboard_climate.json"
    }
    else {
        $scopeToken = Get-ScopeToken -CompositeSlugs $publishedComposites
        $reportPath = Join-Path $ReportRoot "parity_report_${stateToken}_${levelName}_dashboard_climate_scope-${scopeToken}.json"
    }

    if (-not $publish) {
        Write-Host ""
        Write-Host "==> $publishReason ($levelName)"
    }
    else {
        $optimizedArgs = @()
        foreach ($slug in $publishSet) {
            $optimizedArgs += @("--metric", [string]$slug)
        }

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

        # CHG-0046: when publishing a partial subset, write a manifest beside the report so
        # downstream ops can detect the mixed/partial state.
        if ($levelHadFailure -and $AllowPartialPublish) {
            $taintedSlugs = @($slugState.Keys | Where-Object { $slugState[$_].Tainted } | Sort-Object)
            $manifestPath = Join-Path $ReportRoot "parity_report_${stateToken}_${levelName}_dashboard_climate_scope-${scopeToken}_partial_run.json"
            $manifest = [ordered]@{
                state             = $State
                level             = $levelName
                partial           = $true
                succeeded_bundles = @($succeeded | ForEach-Object { [string]$_.canonical })
                failed_bundles    = @($failed)
                tainted_slugs     = $taintedSlugs
                published_metrics = $publishSet
                report_path       = $reportPath
                timestamp         = (Get-Date).ToString('o')
            }
            if ($PlanOnly) {
                Write-Host "    [plan] would write partial-run manifest: $manifestPath"
            }
            else {
                $manifest | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 -Path $manifestPath
                Write-Host "    wrote partial-run manifest: $manifestPath"
            }
        }
    }

    # CHG-0057: write the compute-failure sidecar OUTSIDE the publish/non-publish branch
    # (the path is derived from the finalized $reportPath, so it matches the full / scoped
    # / partial report name) so it is emitted on both the warn-and-publish path and the
    # -FailOnComputeError path where publishing is skipped.
    if ($computeWarnings.Count -gt 0) {
        $computeFailPath = $reportPath -replace '\.json$', '_compute_failures.json'
        $computeFailReport = [ordered]@{
            state                  = $State
            level                  = $levelName
            fail_on_compute_error  = [bool]$FailOnComputeError
            report_path            = $reportPath
            timestamp              = (Get-Date).ToString('o')
            bundles                = @($computeWarnings)
        }
        if ($PlanOnly) {
            Write-Host "    [plan] would write compute-failure sidecar: $computeFailPath"
        }
        else {
            $computeFailReport | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 -Path $computeFailPath
            Write-Host "    wrote compute-failure sidecar: $computeFailPath"
        }
    }

    $runOutcomes += [pscustomobject]@{
        level            = $levelName
        succeeded        = @($succeeded | ForEach-Object { [string]$_.canonical })
        failed           = @($failed)
        published        = $publish
        reason           = $publishReason
        compute_warnings = @($computeWarnings)
    }
}

Write-Host ""
Write-Host "=============================================================================="
Write-Host "RUN SUMMARY"
Write-Host "=============================================================================="
foreach ($o in $runOutcomes) {
    Write-Host ""
    Write-Host "LEVEL $($o.level): $($o.succeeded.Count) bundle(s) ok, $($o.failed.Count) failed; optimized/audit published: $($o.published)"
    if (-not $o.published -and $o.reason) {
        Write-Host "  $($o.reason)"
    }
    foreach ($f in $o.failed) {
        $firstLine = (($f.error -split "`n")[0]).Trim()
        Write-Host "  FAILED: $($f.bundle) -- $firstLine"
    }
    # CHG-0057: surface per-task compute failures (warn-path; these do not set exit 1).
    if ($o.compute_warnings.Count -gt 0) {
        Write-Host "  COMPUTE WARNINGS:"
        foreach ($w in $o.compute_warnings) {
            Write-Host "    $($w.bundle): $($w.failed_total) failed task(s)"
        }
    }
}

Write-Host ""
Write-Host "Dashboard climate source metrics selected: $($sourceMetrics.Count)"
Write-Host "Optimized dashboard metrics/composites selected: $($optimizedMetrics.Count)"

if ($anyFailure) {
    Write-Host "Refresh completed WITH FAILURES (one or more bundles failed)."
    exit 1
}

Write-Host "Refresh complete."

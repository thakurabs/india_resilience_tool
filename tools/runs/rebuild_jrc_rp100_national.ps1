<#
.SYNOPSIS
Rebuild Riverine Flood artifacts nationally against the strict JRC RP-100 source.

.DESCRIPTION
Loops tools/runs/refresh_dashboard_riverine_flood_bundle.ps1 over every state
that is not yet built against the strict v2.1.2 source manifest, in place.

State selection is derived, not hardcoded: a state is considered already-strict
when its per-state QA run_summary.csv reports a strict_rp100 metric_kind AND a
non-empty source_manifest. Pass -States to override the derived list, or
-IncludeStrict to force a rebuild of every state.

CAVEAT: detection reads the JRC master run_summary only. It cannot see whether a
state's downstream composite / processed_optimised artifacts completed. A state
interrupted after its masters were written but before its optimized build
finished will look "strict" and be skipped. Re-run such states explicitly via
-States.

Publication is IN PLACE with -Overwrite and has no rollback. Stop the Streamlit
app before running: it serves from the files this rebuilds, and its caches do
not detect swapped files.

.EXAMPLE
powershell -ExecutionPolicy Bypass -File tools/runs/rebuild_jrc_rp100_national.ps1 -PlanOnly

.EXAMPLE
powershell -ExecutionPolicy Bypass -File tools/runs/rebuild_jrc_rp100_national.ps1 -States "Karnataka","Kerala"
#>

[CmdletBinding()]
param(
    [string]$Repo = "D:\projects\india_resilience_tool",

    [string]$DataDir = "D:\projects\irt_data",

    [string]$SourceManifest = "D:/projects/irt_data/jrc_raw_new/source_manifest.json",

    [string[]]$States = @(),

    [switch]$IncludeStrict,

    [string]$LogPath = "",

    [switch]$PlanOnly
)

$ErrorActionPreference = "Stop"

function Test-StateIsStrict {
    param([string]$State)

    $summary = Join-Path $DataDir "jrc_flood_depth\$State\qa\run_summary.csv"
    if (-not (Test-Path $summary)) {
        return $false
    }

    try {
        $rows = Import-Csv -Path $summary
    } catch {
        return $false
    }
    if (-not $rows) {
        return $false
    }

    $row = @($rows)[0]
    $kind = [string]$row.metric_kind
    $manifest = [string]$row.source_manifest

    if (-not $kind -or $kind -notmatch "strict_rp100") {
        return $false
    }
    if (-not $manifest -or $manifest -eq "None" -or -not $manifest.Trim()) {
        return $false
    }
    return $true
}

$indexRoot = Join-Path $DataDir "processed\jrc_flood_depth_index_rp100"
if (-not (Test-Path $indexRoot)) {
    throw "Metric root not found: $indexRoot"
}

$allStates = Get-ChildItem -Path $indexRoot -Directory | Select-Object -ExpandProperty Name | Sort-Object

if ($States -and $States.Count -gt 0) {
    $unknown = $States | Where-Object { $allStates -notcontains $_ }
    if ($unknown) {
        throw ("Unknown state(s): " + ($unknown -join ", "))
    }
    $targets = $States
} elseif ($IncludeStrict) {
    $targets = $allStates
} else {
    $targets = @($allStates | Where-Object { -not (Test-StateIsStrict -State $_) })
}

if (-not $targets -or $targets.Count -eq 0) {
    Write-Host "Nothing to rebuild: every state is already built against the strict manifest."
    return
}

if (-not $LogPath -or -not $LogPath.Trim()) {
    $stamp = (Get-Date -Format "yyyyMMdd_HHmmss")
    $LogPath = Join-Path $Repo "scratch\results\jrc_national_rebuild_$stamp.csv"
}
$logDir = Split-Path -Parent $LogPath
if ($logDir -and -not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Force $logDir | Out-Null
}

$refresh = Join-Path $Repo "tools\runs\refresh_dashboard_riverine_flood_bundle.ps1"
if (-not (Test-Path $refresh)) {
    throw "Refresh script not found: $refresh"
}
if (-not (Test-Path $SourceManifest.Replace("/", "\"))) {
    throw "Source manifest not found: $SourceManifest"
}

Set-Location $Repo

$results = New-Object System.Collections.Generic.List[psobject]
$index = 0
$failed = 0

Write-Host "NATIONAL JRC RP-100 REBUILD"
Write-Host ("states: {0} of {1}" -f $targets.Count, $allStates.Count)
Write-Host "source_manifest: $SourceManifest"
Write-Host "log: $LogPath"
if ($PlanOnly) { Write-Host "mode: plan-only" }
Write-Host ""

foreach ($state in $targets) {
    $index++
    $started = Get-Date
    Write-Host ("[{0}/{1}] {2} ..." -f $index, $targets.Count, $state)

    $refreshArgs = @(
        "-ExecutionPolicy", "Bypass",
        "-File", $refresh,
        "-State", $state,
        "-SourceManifest", $SourceManifest,
        "-Overwrite"
    )
    if ($PlanOnly) { $refreshArgs += "-PlanOnly" }

    $status = "ok"
    $detail = ""
    try {
        & powershell.exe @refreshArgs
        if ($LASTEXITCODE -ne 0) {
            $status = "failed"
            $detail = "exit code $LASTEXITCODE"
            $failed++
        }
    } catch {
        $status = "failed"
        $detail = $_.Exception.Message
        $failed++
    }

    $elapsed = [math]::Round(((Get-Date) - $started).TotalSeconds, 1)
    Write-Host ("    -> {0} ({1}s)" -f $status, $elapsed)

    $results.Add([pscustomobject]@{
        index       = $index
        state       = $state
        status      = $status
        elapsed_sec = $elapsed
        started_utc = $started.ToUniversalTime().ToString("s")
        detail      = $detail
    })

    $results | Export-Csv -NoTypeInformation -Path $LogPath
}

Write-Host ""
Write-Host ("NATIONAL REBUILD COMPLETE: {0} ok, {1} failed" -f ($targets.Count - $failed), $failed)
Write-Host "log: $LogPath"

if ($failed -gt 0) {
    Write-Host ""
    Write-Host "FAILED STATES:"
    $results | Where-Object { $_.status -eq "failed" } | ForEach-Object {
        Write-Host ("  {0}: {1}" -f $_.state, $_.detail)
    }
    exit 1
}

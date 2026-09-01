<#
.SYNOPSIS
Refresh dashboard-ready Riverine Flood artifacts for one state.

.DESCRIPTION
Builds the selected state's JRC flood-depth admin masters, then rebuilds the
district/block Riverine Flood composite masters, then refreshes the scoped
processed_optimised artifacts and parity audit for the Riverine Flood bundle.

This script is the ready-made operator workflow for any state whose JRC rasters
and canonical admin boundaries are available.

.EXAMPLE
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_riverine_flood_bundle.ps1 -State Maharashtra

.EXAMPLE
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_riverine_flood_bundle.ps1 -State Telangana -SourceManifest D:/projects/irt_data/jrc_raw_new/source_manifest.json -PlanOnly
#>

[CmdletBinding()]
param(
    [string]$State = "Telangana",

    [string]$SourceManifest = "D:/projects/irt_data/jrc_raw_new/source_manifest.json",

    [Alias("SourceDir")]
    [string]$JrcDir = "",

    [switch]$Rp100Only,

    [ValidateSet("m", "cm", "mm")]
    [string]$AssumeUnits = "m",

    [string]$QaDir = "",

    [string]$OverlayDir = "",

    [string]$DistrictsPath = "",

    [string]$BlocksPath = "",

    [string]$Python = "",

    [switch]$Overwrite,

    [switch]$PlanOnly,

    [switch]$IncludeSharedAdmin
)

$ErrorActionPreference = "Stop"
$DefaultSourceManifest = "D:/projects/irt_data/jrc_raw_new/source_manifest.json"

$RiverineMetrics = @(
    "composite_flood_jrc_depth",
    "jrc_flood_depth_index_rp100",
    "jrc_flood_extent_rp100",
    "jrc_flood_depth_rp100"
)

function Resolve-PythonCommand {
    param([string]$RequestedPython)

    function Test-PythonCandidate {
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
        if (Test-PythonCandidate -Candidate $candidate) {
            return $candidate
        }
    }

    return $candidates[0]
}

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
    Write-Host (Format-CommandForDisplay -Executable $Python -Arguments $Arguments)

    if ($PlanOnly) {
        return
    }

    & $Python @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code $LASTEXITCODE."
    }
}

$Python = Resolve-PythonCommand -RequestedPython $Python
$sourceManifestWasExplicit = $PSBoundParameters.ContainsKey("SourceManifest")
$jrcDirWasExplicit = $PSBoundParameters.ContainsKey("JrcDir")
$useStrictManifest = -not $jrcDirWasExplicit

if ($jrcDirWasExplicit -and $sourceManifestWasExplicit) {
    throw "Pass either -SourceManifest or -JrcDir, not both."
}
if ($jrcDirWasExplicit -and $Rp100Only) {
    throw "-Rp100Only is only valid with -SourceManifest strict mode, not legacy -JrcDir."
}
if ($useStrictManifest -and (-not $SourceManifest -or -not $SourceManifest.Trim())) {
    $SourceManifest = $DefaultSourceManifest
}

$jrcArgs = @(
    "-m", "tools.runs.prepare_dashboard",
    "jrc-flood-depth",
    "--state", $State
)
if ($useStrictManifest) {
    $jrcArgs += @("--source-manifest", $SourceManifest, "--rp100-only")
} else {
    $jrcArgs += @("--source-dir", $JrcDir, "--assume-units", $AssumeUnits)
}
if ($QaDir) {
    $jrcArgs += @("--qa-dir", $QaDir)
}
if ($OverlayDir) {
    $jrcArgs += @("--overlay-dir", $OverlayDir)
}
if ($DistrictsPath) {
    $jrcArgs += @("--districts-path", $DistrictsPath)
}
if ($BlocksPath) {
    $jrcArgs += @("--blocks-path", $BlocksPath)
}
if ($Overwrite) {
    $jrcArgs += "--overwrite"
}
$compositeArgs = @(
    "-m", "tools.pipeline.build_composite_metrics",
    "--metric", "composite_flood_jrc_depth",
    "--state", $State,
    "--level", "district",
    "--level", "block"
)
if ($Overwrite) {
    $compositeArgs += "--overwrite"
}

$optimizedArgs = @(
    "-m", "tools.optimized.build_processed_optimised",
    "--state", $State,
    "--level", "district",
    "--level", "block",
    "--skip-audit"
)
foreach ($metric in $RiverineMetrics) {
    $optimizedArgs += @("--metric", $metric)
}
if ($IncludeSharedAdmin) {
    $optimizedArgs += "--include-shared-admin-artifacts"
}

$auditArgs = @(
    "-m", "tools.optimized.audit_processed_optimised_parity",
    "--state", $State,
    "--level", "district",
    "--level", "block"
)
foreach ($metric in $RiverineMetrics) {
    $auditArgs += @("--metric", $metric)
}
if ($IncludeSharedAdmin) {
    $auditArgs += "--include-shared-admin-artifacts"
}

Write-Host "RIVERINE FLOOD REFRESH"
Write-Host "state: $State"
if ($useStrictManifest) {
    Write-Host "source_manifest: $SourceManifest"
    Write-Host "rp100_only: True"
} else {
    Write-Host "jrc_dir: $JrcDir"
    Write-Host "assume_units: $AssumeUnits"
}
Write-Host "levels: district, block"
Write-Host ("metrics: " + ($RiverineMetrics -join ", "))
if ($PlanOnly) {
    Write-Host "mode: plan-only"
}

Invoke-NativeChecked -Label "Prepare JRC flood-depth bundle" -Arguments $jrcArgs
Invoke-NativeChecked -Label "Build Riverine Flood composite masters" -Arguments $compositeArgs
Invoke-NativeChecked -Label "Build processed_optimised Riverine Flood bundle" -Arguments $optimizedArgs
Invoke-NativeChecked -Label "Audit processed_optimised Riverine Flood parity" -Arguments $auditArgs

Write-Host ""
Write-Host "RIVERINE FLOOD REFRESH COMPLETE"

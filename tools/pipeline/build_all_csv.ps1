# ==== EDIT THESE ====
$Root    = "D:\projects\irt"                    # adjust to your base folder
$Vars    = @("tas_gt32")                        # add variables if needed
$States  = @("Telangana")                       # add states if needed
$Metric  = "days_gt_32C"
$Geojson = Join-Path $Root "data\districts_4326.geojson"
# ====================

foreach ($Var in $Vars) {
  $OutRoot = Join-Path $Root ("processed\" + $Var)
  foreach ($State in $States) {
    $StateDir = Join-Path $OutRoot $State
    New-Item -ItemType Directory -Force -Path $StateDir | Out-Null
    $OutCsv = Join-Path $StateDir "master_metrics_by_district.csv"

    Write-Host "Building: VAR=$Var STATE=$State"
    python build_master_metrics.py `
      -r $OutRoot `
      -s $State `
      -m $Metric `
      -g $Geojson `
      -o $OutCsv

    Write-Host "Done -> $OutCsv`n"
  }
}

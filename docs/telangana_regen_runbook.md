# Telangana Regeneration Runbook (new LGD boundaries)

**Purpose:** regenerate the dashboard data for **Telangana** against the new
single-source admin boundaries (`blocks_4326.geojson`, `districts_4326.geojson`,
`states_4326.geojson`, built from `LGD_Blocks`). Order is **national context
first, then Telangana climate, then Telangana riverine flood**, with a full
force-recompute (`--overwrite`).

This runbook is written to be followed **literally, top to bottom**. Run each
command, wait for it to finish, confirm it succeeded (exit code `0` / prints its
"wrote …" / "complete" line), then move to the next. **If any command errors or
exits non-zero, STOP and report which command failed and the last ~30 lines of
output. Do not improvise or skip ahead.**

---

## 0. Environment

- OS: Windows. Shell: PowerShell.
- Work from the repo root: `D:\projects\india_resilience_tool`
- Activate the conda env first:

```powershell
conda activate irt
cd D:\projects\india_resilience_tool
```

- `IRT_DATA_DIR` resolves to `D:\projects\irt_data` (the data root). All commands
  below assume that default.
- In every command, `python` means the `irt` env's Python (active after
  `conda activate irt`).

---

## 0b. Environment for agentic / WSL execution (READ IF NOT IN NATIVE POWERSHELL)

This runbook was written for a native Windows **PowerShell** operator. If you are an
automation agent running in **WSL bash**, apply these universal transforms to
**every** command below. (Native PowerShell operators can skip this section and use
the commands as written.)

1. **Python interpreter — there is no `conda activate` in WSL bash.** Invoke the
   `irt` env Python by absolute path, and always pass the `-X utf8` flag (see #2):
   `/mnt/c/Users/22015611/AppData/Local/miniconda3/envs/irt/python.exe`
   So `python -m tools.foo ...` becomes
   `/mnt/c/Users/22015611/AppData/Local/miniconda3/envs/irt/python.exe -X utf8 -m tools.foo ...`

2. **UTF-8 console is required (`-X utf8`).** Several scripts `print()` Unicode
   (e.g. `↔`) that the Windows cp1252 console cannot encode, raising
   `UnicodeEncodeError: 'charmap' codec can't encode character`. Pass the
   **`-X utf8`** interpreter flag on every direct Python call — it is process-scoped
   with no data/logic impact. Bash-side `PYTHONIOENCODING=`/`PYTHONUTF8=` exports do
   **not** propagate into Windows `.exe` processes, so do not rely on them; use
   `-X utf8`.

3. **Stage 2 PowerShell script is the exception.** `-X utf8` cannot reach the
   `.ps1`'s child Python processes. Set the encoding inside PowerShell instead, and
   run it in the **background** (it is long):
   ```
   powershell.exe -ExecutionPolicy Bypass -Command "$env:PYTHONUTF8=1; & 'tools/runs/refresh_dashboard_climate_bundles.ps1' -State Telangana -Level all -Overwrite"
   ```

4. **Data paths.** `IRT_DATA_DIR` = `D:\projects\irt_data` (Windows) =
   `/mnt/d/projects/irt_data` (WSL). Commands that take `--data-dir` keep the Windows
   form `D:\projects\irt_data`; filesystem inspection from bash uses
   `/mnt/d/projects/irt_data`.

5. **Long stages / timeouts.** Stage 2 exceeds a 10-minute command timeout — run it
   in the background and poll. Do not let a timeout trigger a retry.

---

## 🚫 HARD RULES — never run these (they destroy the new boundaries)

The new boundary files are the **source of truth** and must never be regenerated
by this runbook. The following commands rebuild `blocks_4326.geojson` from the
**old** `Block_GH_WUP` source and will silently corrupt the dashboard. **Do NOT
run any of them:**

1. `python -m tools.geodata.build_blocks_geojson …`  ← rebuilds blocks from legacy source
2. `python -m tools.geodata.build_admin_boundaries_from_lgd --overwrite`  ← boundaries already built
3. `python -m tools.runs.prepare_dashboard population-exposure --overwrite`  ← its first step rebuilds blocks
4. `…\refresh_dashboard_riverine_flood_bundle.ps1 … -Overwrite`  ← passes `--overwrite` to jrc-flood-depth, which rebuilds blocks
5. `python -m tools.runs.prepare_dashboard jrc-flood-depth … --overwrite`  ← same as #4

Stage 3 below deliberately uses the **direct JRC masters builder** instead of #4/#5
precisely to avoid this. The exposure layers in Stage 1A use **direct sub-steps**
instead of #3.

**Tripwire:** before you start (Stage 0) you will record the size of
`blocks_4326.geojson`. After **every** stage, re-check it is unchanged. If it
changes, one of the hard-rule commands was run by mistake — STOP immediately.

---

## Stage 0 — Preflight (read-only, ~1 min)

Confirm the new boundaries are in place and record the tripwire value.

```powershell
python -c "import geopandas as gpd; b=gpd.read_file(r'D:\projects\irt_data\blocks_4326.geojson'); d=gpd.read_file(r'D:\projects\irt_data\districts_4326.geojson'); s=gpd.read_file(r'D:\projects\irt_data\states_4326.geojson'); print('blocks',len(b),'districts',len(d),'states',len(s)); print('sample states', sorted(s['state_name'].unique())[:3]); print('telangana districts', (d['state_name']=='Telangana').sum())"
```

**Expected:** `blocks 7134 districts 783 states 36`, state names in Title-Case
(e.g. `Andaman & Nicobar Islands`), `telangana districts 33`. If the counts or
casing differ, STOP — the boundaries are not the new ones.

Record the tripwire (note the printed `Length`):

```powershell
(Get-Item D:\projects\irt_data\blocks_4326.geojson).Length
```

Write that number down. Re-run this line after each stage; it must stay identical.

---

## Stage 1A — National exposure layers (pan-India)

These have **no `--state`** flag — they rebuild for all of India and feed the
**Exposure Snapshot** card. Run in order.

```powershell
python -m tools.geodata.build_population_admin_masters --overwrite
python -m tools.geodata.build_rural_facilities_admin_masters --overwrite
python -m tools.geodata.build_built_up_area_admin_masters --overwrite
python -m tools.geodata.build_lulc_admin_masters --overwrite
```

Rebuild the optimized runtime bundle for all exposure metrics (this also
regenerates the shared simplified **geometry** and **context** from the new
boundaries — important):

```powershell
python -m tools.optimized.build_processed_optimised --metric population_total --metric population_density --metric rural_facilities_total_count --metric rural_facilities_agro_count --metric rural_facilities_education_count --metric rural_facilities_health_count --metric rural_facilities_service_count --metric rural_facilities_total_count_per_100k --metric rural_facilities_agro_count_per_100k --metric rural_facilities_education_count_per_100k --metric rural_facilities_health_count_per_100k --metric rural_facilities_service_count_per_100k --metric built_up_area_km2 --metric built_up_area_share_pct --metric lulc_agri_area_km2 --metric lulc_agri_share_pct --overwrite --skip-audit
```

Build the exposure summary parquet (must run **after** the optimized build so it
is not clobbered):

```powershell
python -m tools.pipeline.build_admin_exposure_summary --data-dir D:\projects\irt_data
```

> **Note (CHG-0079, applied 2026-06-10):** pre-LGD orphaned state-master directories
> under the exposure slugs (old spellings of renamed states, e.g.
> `DADRA & NAGAR HAVELI & DAMAN & DIU`, `CHHATISGARH`, `Lakshadweep-UT`,
> `UTTARPRADESH`) were quarantined to `processed/_stale_prelgd_bak/` so this
> builder's duplicate-`admin_key` guard passes. If it ever fails again with
> `ValueError: Duplicate admin_key per level`, the cause is the same — a
> `--overwrite` rebuild left old-named state dirs alongside the new canonical ones.
> Fix: quarantine any state dir under the affected slug whose master files were
> **not** written by the current run (compare file mtimes; check both the district
> and block masters), then re-run this step.

Audit the exposure metrics:

```powershell
python -m tools.optimized.audit_processed_optimised_parity --metric population_total --metric population_density --metric rural_facilities_total_count --metric rural_facilities_agro_count --metric rural_facilities_education_count --metric rural_facilities_health_count --metric rural_facilities_service_count --metric rural_facilities_total_count_per_100k --metric rural_facilities_agro_count_per_100k --metric rural_facilities_education_count_per_100k --metric rural_facilities_health_count_per_100k --metric rural_facilities_service_count_per_100k --metric built_up_area_km2 --metric built_up_area_share_pct --metric lulc_agri_area_km2 --metric lulc_agri_share_pct
```

**Done when:** all commands exit 0 and the audit reports no parity gaps.
Re-check the Stage 0 tripwire (`blocks_4326.geojson` size unchanged).

---

## Stage 1B — National hydro context + groundwater (pan-India)

Feeds the **Hydrological Context** card and the Groundwater layer. Rebuild the
four district/block ↔ basin/sub-basin crosswalks against the new boundaries:

```powershell
python -m tools.geodata.build_district_subbasin_crosswalk --overwrite
python -m tools.geodata.build_block_subbasin_crosswalk --overwrite
python -m tools.geodata.build_district_basin_crosswalk --overwrite
python -m tools.geodata.build_block_basin_crosswalk --overwrite
```

Rebuild groundwater district masters:

```powershell
python -m tools.geodata.build_groundwater_district_masters --overwrite
```

> **Caveat (rollout, not Telangana):** groundwater state-master dirs are named from
> the builder's `canonical_state` mapping, which still uses pre-LGD spellings for a
> few renamed states (`chhatisgarh`, `jammu and kashmir`, `andaman and nicobar
> islands`, `lakshadweep ut`). The `--overwrite` rebuild overwrites these in place
> (no new orphan, so this stage does not crash), and **Telangana is unaffected**
> (`telangana` aliases identically). But the gw layer for those states may not join
> cleanly to the renamed boundaries — flag for the pan-India rollout. The hydro
> summary itself is safe: it reads `processed_optimised/context/*_basin.parquet`
> crosswalk parquets, not per-state gw dirs.

Rebuild the optimized bundle for groundwater (this regenerates the
`context/*_basin.parquet` / `context/*_subbasin.parquet` files from the fresh
crosswalks, which the hydro summary reads next):

```powershell
python -m tools.optimized.build_processed_optimised --metric gw_stage_extraction_pct --metric gw_future_availability_ham --metric gw_extractable_resource_ham --metric gw_total_extraction_ham --overwrite --skip-audit
```

Build the hydro summary parquet (must run **after** the optimized build):

```powershell
python -m tools.pipeline.build_admin_hydro_summary --data-dir D:\projects\irt_data
```

Audit groundwater:

```powershell
python -m tools.optimized.audit_processed_optimised_parity --metric gw_stage_extraction_pct --metric gw_future_availability_ham --metric gw_extractable_resource_ham --metric gw_total_extraction_ham
```

**Done when:** all commands exit 0 and the audit reports no parity gaps.
Re-check the Stage 0 tripwire.

---

## Stage 2 — Telangana climate bundles (per-state, LONG)

This is the heavy compute (NASA NEX climate metrics → masters → composites →
sector proposals → optimized → strict audit), for **district and block**. It is
safe for the boundaries (it never rebuilds blocks-geojson and skips
geometry/context by default). `-Overwrite` forces a full recompute against the
new boundaries.

```powershell
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 -State Telangana -Level all -Overwrite
```

**Expect this to run for a long time** (compute over many models/scenarios for 33
districts and 588 blocks). Watch the tail of the output.

**Done when:** the script prints `Refresh complete.` and exits 0. If it instead
prints `Refresh completed WITH FAILURES`, STOP and report the `RUN SUMMARY`
section (which bundle(s) failed).

Re-check the Stage 0 tripwire.

---

## Stage 3 — Telangana riverine flood (per-state)

**Do NOT use** `refresh_dashboard_riverine_flood_bundle.ps1 -Overwrite` (hard rule
#4). Run these four direct steps instead. Step 1 calls the JRC masters builder
**directly**, which only *reads* the block boundaries and never rebuilds them.

```powershell
python -m tools.geodata.build_jrc_flood_depth_admin_masters --state Telangana --source-dir "D:\projects\irt_data\Floodlayers_JRC-20260413T042625Z-3-001\Floodlayers_JRC" --assume-units m --overwrite
```

```powershell
python -m tools.pipeline.build_composite_metrics --metric composite_flood_jrc_depth --state Telangana --level district --level block --overwrite
```

```powershell
python -m tools.optimized.build_processed_optimised --state Telangana --level district --level block --metric composite_flood_jrc_depth --metric jrc_flood_depth_index_rp100 --metric jrc_flood_extent_rp100 --metric jrc_flood_depth_rp100 --skip-audit
```

```powershell
python -m tools.optimized.audit_processed_optimised_parity --state Telangana --level district --level block --metric composite_flood_jrc_depth --metric jrc_flood_depth_index_rp100 --metric jrc_flood_extent_rp100 --metric jrc_flood_depth_rp100
```

**Done when:** all four exit 0 and the audit reports no parity gaps.
Re-check the Stage 0 tripwire.

---

## Stage 4 — Final verification

1. **Tripwire (last time):** `(Get-Item D:\projects\irt_data\blocks_4326.geojson).Length`
   must equal the Stage 0 value. If not, the boundaries were corrupted — report it.

2. **Context parquets refreshed:** confirm both are newer than the start of this run:

```powershell
Get-Item D:\projects\irt_data\processed_optimised\context\admin_exposure_summary.parquet | Select-Object Name,LastWriteTime
Get-Item D:\projects\irt_data\processed_optimised\context\admin_hydro_summary.parquet | Select-Object Name,LastWriteTime
```

3. **Dashboard smoke test (manual):**

```powershell
streamlit run main.py
```

In the app, set state = **Telangana**, admin mode, and confirm:
   - choropleths render with the new district/block shapes (no missing/blank units),
   - the **Exposure Snapshot** card shows population / rural facilities / built-up /
     LULC for a selected Telangana district **and** block,
   - the **Hydrological Context** card shows basin / sub-basin overlap,
   - the **Groundwater** and **Riverine Flood** layers render,
   - the climate bundles (Heat Risk, Drought Risk, etc.) render for Telangana.

If all of the above hold, the Telangana regeneration is complete.

---

## If something fails

- Report: the exact command, its exit code, and the last ~30 lines of output.
- Do **not** attempt to "fix" by running any command from the HARD RULES list.
- Per-state stages (2 and 3) can be re-run for Telangana safely. The national
  stages (1A, 1B) can also be re-run; they overwrite their own outputs.
- The earlier Aqueduct removal moved its data to `*.bak-<timestamp>` folders under
  `D:\projects\irt_data`; ignore those — they are not part of this run.

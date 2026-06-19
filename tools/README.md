# Tools — India Resilience Tool (IRT)

This folder contains **ops / diagnostic / data-prep** scripts that are considered part of IRT functionality,
but are intentionally kept out of the runtime package.

Run these from the **repo root** so imports like `paths.py` resolve correctly.

## Canonical runner

The recommended operator entrypoint is:

```bash
python -m tools.runs.prepare_dashboard --help
```

Examples:

```bash
python -m tools.runs.prepare_dashboard climate-hazards
```

```bash
python -m tools.runs.prepare_dashboard climate-hazards --level hydro
```

```bash
python -m tools.runs.prepare_dashboard climate-hazards --metrics tas_annual_mean
```

```bash
python -m tools.runs.prepare_dashboard climate-hazards --level hydro --plan-only
```

```bash
python -m tools.runs.prepare_dashboard climate-hazards --level hydro --audit-only
```

```bash
python -m tools.runs.prepare_dashboard aqueduct
```

```bash
python -m tools.runs.prepare_dashboard jrc-flood-depth --state Maharashtra --source-dir /path/to/Floodlayers_JRC --assume-units m --plan-only
```

Targeted Riverine Flood refresh for one state, including district + block composite publish:

```powershell
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_riverine_flood_bundle.ps1 -State Maharashtra -JrcDir D:/projects/irt_data/Floodlayers_JRC
```

> **`jrc-flood-depth` blocks-geojson behavior (CHG-0065).** The JRC builder only
> *reads* the canonical blocks GeoJSON, so the plan schedules the `blocks-geojson`
> step only when that file is **missing** or when `--overwrite` is passed:
> - **No `--overwrite`, canonical blocks present (normal case):** `blocks-geojson`
>   is skipped — the plan is just builder → optimized → audit, and runs cleanly.
> - **`--overwrite`:** `blocks-geojson` runs and **rebuilds the pipeline-wide
>   canonical blocks GeoJSON** (`blocks_4326.geojson`, all 36 states) + its QA CSVs
>   as a side-effect. The rebuild is deterministic from the source shapefile, so
>   content is normally unchanged, but it is a broader blast radius than a JRC run
>   needs — prefer the no-`--overwrite` form unless you specifically intend to
>   regenerate canonical boundaries.
> - **Fresh machine, canonical blocks absent:** `blocks-geojson` runs first
>   (required) regardless of `--overwrite`.

```bash
python -m tools.runs.prepare_dashboard dashboard-package --plan-only
```

Targeted dashboard climate refresh for active thematic + sector-wise admin bundles:

```powershell
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 -State Telangana -Level all
```

Each executed stage writes an auditable stdout/stderr log under
`processed_optimised/logs/dashboard_climate_refresh/<state>/<timestamp>/` by
default; pass `-LogRoot <path>` to store logs elsewhere.

Scoping and recompute control:
- `-Bundle "<canonical name>" [-Bundle "<another>"]` limits the whole run
  (compute → masters → composites/proposals → optimized → audit) to the source
  metrics and composites of the named dashboard bundle(s) from
  `india_resilience_tool/config/dashboard_bundles.py`. Names are
  case/whitespace-insensitive; unknown names fail fast and list the valid set.
  `Riverine Flood` is rejected (use the JRC flood-depth workflow instead).
- Compute is incremental by default: it passes `--skip-existing`, so an unchanged
  re-run recomputes nothing. Use `-Overwrite` to force a full recompute of the
  in-scope metrics, or `-OverwriteMetrics slug1 slug2` to force just a subset
  (the two are mutually exclusive; every slug must be in scope).
- Masters are rebuilt freshness-aware: the runner probes each in-scope metric's
  compute completion markers and only rebuilds masters that are missing, stale, or
  force-overwritten; fresh masters are skipped (`--skip-existing`). If a metric has
  no markers, or when `-SkipCompute` is used (the on-disk source may have changed
  without a fresh marker), it additionally checks raw `*_periods.csv` mtimes.
- Bundle-scoped runs write a distinct, scope-tagged parity report
  (`..._dashboard_climate_bundle_<names>.json`) so they cannot overwrite the
  full-scope report; full-scope runs keep the `..._dashboard_climate.json` name.
- `-Workers` is opt-in. When omitted, compute and master builders pick their own
  machine-aware defaults; pass `-Workers N` (N ≥ 1) to override.
- The single `processed_optimised` build runs with `--skip-audit`; parity is
  verified once by the dedicated strict audit stage (no longer audited twice).

By default the runner is non-destructive and dashboard-oriented:
- climate runs default to `--level all`
- climate runs resolve live metrics per requested level
- admin climate runs now build persisted district/block composite masters for the 6 thematic dashboard bundles after climate master generation and before optimized refresh
- JRC `jrc-flood-depth --overwrite` refreshes the JRC masters/QA and updates optimized outputs in place without wiping unrelated bundle contents — **but** see the `--overwrite` caveat below: it also forces a rebuild of the pipeline-wide canonical blocks GeoJSON
- the reused `jrc_flood_depth_index_rp100` slug now represents the RP-100 Flood Severity Index derived from RP-100 depth plus RP-100 extent, so operators must rebuild JRC outputs after pulling that methodology change
- climate compute uses validated completion markers and `--skip-existing` by default unless `--overwrite` is supplied
- climate `--overwrite` now clears the selected compute marker/output slice before rebuilding, including stale hydro alias trees for the selected scope
- climate and bundle runs refresh `processed_optimised`
- the optimized parity audit runs automatically
- climate runs return non-zero when the requested readiness state is still incomplete for stages that actually ran; intentionally skipped stages remain informational in post-run readiness
- use `--overwrite` only when you want to force a rebuild

For the full command catalog, see [`../docs/command_catalog.md`](../docs/command_catalog.md).

## Pipeline

| Script | Purpose | Run |
|---|---|---|
| `tools/pipeline/compute_indices_multiprocess.py` | Build processed climate index artifacts for admin and hydro levels, with validated completion markers, source-inventory prewarm, optional `--skip-existing`, targeted `--overwrite` cleanup, and an immediate bootstrap banner before the heavy runtime imports | `python -m tools.pipeline.compute_indices_multiprocess --help` |
| `tools/pipeline/compute_indices.py` | Build processed index artifacts (single-process; debug) | `python -m tools.pipeline.compute_indices --help` |
| `tools/pipeline/build_spatial_weights.py` | Build private Heat Risk v2 sparse area-overlap caches under `IRT_DATA_DIR/processed/_internal/spatial_weights/` from a boundary layer and climate grid sample; resolves defaults from the effective `--data-dir`, skips valid caches, and requires `--overwrite` to replace stale ones | `python -m tools.pipeline.build_spatial_weights --help` |
| `tools/pipeline/build_master_metrics.py` | Build admin and hydro master CSVs plus summary sidecars; hydro levels auto-use `processed/{metric}/hydro/` | `python -m tools.pipeline.build_master_metrics --help` |
| `tools/pipeline/build_composite_metrics.py` | Build persisted district/block composite masters for the 6 thematic dashboard bundles under `processed/<composite_slug>/<state>/master_metrics_by_{district,block}.{csv,parquet}` | `python -m tools.pipeline.build_composite_metrics --help` |
| `tools/pipeline/build_proposal_bundles.py` | Build persisted district/block proposal climate-risk bundle masters plus the `r95p_interannual_variability` helper masters under `processed/<slug>/<state>/master_metrics_by_{district,block}.{csv,parquet}`; the dashboard surfaces these as grouped `Sector-wise - ...` bundles, including district and block views for `Life & Livelihood Loss Risk` when its persisted block proposal bundle master is present | `python -m tools.pipeline.build_proposal_bundles --help` |
| `tools/pipeline/build_glance_view_model.py` | Build persisted Glance view-model Parquet artifacts for landing runtime under `processed_optimised/context/glance/v1/{composite_slug}/{scenario}/{period}/`; normal operators get this through `tools.optimized.build_processed_optimised` | `python -m tools.pipeline.build_glance_view_model --help` |
| `tools/pipeline/build_all_csv.ps1` | Windows helper to run common builds | `powershell -File tools/pipeline/build_all_csv.ps1` |
| `tools/runs/refresh_dashboard_climate_bundles.ps1` | Windows/PowerShell operator script that computes only active thematic and sector-wise dashboard climate source metrics for district and/or block, then rebuilds masters, composites, proposal bundles, `processed_optimised`, and strict parity reports. Supports `-Bundle` scoping to named dashboard bundles, incremental `--skip-existing` compute by default with opt-in `-Overwrite`/`-OverwriteMetrics`, freshness-aware (marker-driven) master rebuilds, opt-in `-Workers`, and a single (non-duplicated) strict parity audit. **Per-bundle execution (C2):** the compute→master→composite stages run bundle-by-bundle with fail isolation (one bundle failing does not abort the rest), then a single union optimized+audit pass runs over the bundles that succeeded; shared source metrics are computed/mastered exactly once via a per-level slug-state cache. On a bundle failure the bundle's not-yet-built source slugs are *tainted* and their half-written master outputs deleted so a later bundle force-rebuilds them. **Partial-publish policy:** if any bundle fails, optimized+audit is **skipped by default** (the runtime keeps its last-good state) and the script exits non-zero; pass `-AllowPartialPublish` to publish the succeeded subset and emit a `*_partial_run.json` manifest. Reports keep their established name for full-scope all-success runs and use a deterministic tokenized name (`*_scope-<token>.json`) for any subset/partial run. **Compute-failure surfacing (CHG-0059):** the compute CLI exits 0 on per-task failures (only ensemble failures hard-fail), so the runner parses each compute step's `Computation: …Failed: N` summary line and surfaces per-task failures as a WARNING, a `COMPUTE WARNINGS` block in the run summary, and a report-derived `*_compute_failures.json` sidecar (written even when publish is skipped) — without blocking publish by default. Pass `-FailOnComputeError` to escalate any bundle with compute failures to a bundle failure (taint + skip publish + non-zero exit). **Freshness probe (W2):** the master-freshness `*_periods.csv` fallback (used under `-SkipCompute` / no-marker slugs) walks the periods trees with `os.scandir` + per-slug early-exit across a bounded thread pool — set `IRT_FRESHNESS_WORKERS` to override the worker count (default `min(32, cpu*2)`), and `IRT_FRESHNESS_TIMING=1` to print per-slug work time plus the parallel wall time. The set of slugs reported stale is identical to the prior serial walk | `powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_climate_bundles.ps1 -State Telangana -Level all` / `... -Bundle "Heat Stress" -PlanOnly` |
| `tools/runs/refresh_dashboard_riverine_flood_bundle.ps1` | Windows/PowerShell operator script that refreshes the dashboard-ready Riverine Flood bundle for one state end to end: state-scoped JRC flood-depth masters via `prepare_dashboard jrc-flood-depth`, district + block `composite_flood_jrc_depth` masters, state-scoped `processed_optimised` artifacts for `composite_flood_jrc_depth`, `jrc_flood_depth_index_rp100`, `jrc_flood_extent_rp100`, and `jrc_flood_depth_rp100`, and a scoped strict parity audit. Supports `-PlanOnly`, optional builder-path overrides (`-QaDir`, `-OverlayDir`, `-DistrictsPath`, `-BlocksPath`), and `-IncludeSharedAdmin` when the operator intentionally wants shared admin artifacts refreshed alongside the state-scoped run | `powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_riverine_flood_bundle.ps1 -State Maharashtra -JrcDir D:/projects/irt_data/Floodlayers_JRC` / `... -PlanOnly` |

## Diagnostics

| Script | Purpose | Run |
|---|---|---|
| `tools/diagnostics/heat_stress_gridfirst_parity.py` | Non-destructive comparison of legacy polygon-mean-first vs Heat Stress v2 grid-first CSV extracts, reporting per-metric deltas, rank shifts, and top movers | `python -m tools.diagnostics.heat_stress_gridfirst_parity --help` |
| `tools/diagnostics/audit_thematic_bundle_completeness.py` | Non-destructive audit of the 6 thematic dashboard bundles against `docs/bundle_calculation_audit.md`, checking processed component masters, persisted composite masters, and scenario/period pair parity for the selected states/levels | `python -m tools.diagnostics.audit_thematic_bundle_completeness --help` |
| `tools/diagnostics/spi_diagnostic.py` | Sanity checks for SPI outputs (distribution/mean/std) | `python -m tools.diagnostics.spi_diagnostic --help` |
| `tools/diagnostics/profile_drought_fullpass.py` | Read-only full-pass drought profiler (CHG-0111 gate): runs all 7 gridfirst drought slugs for one (model, scenario), reporting the cube-rebuild redundancy factor, the within-drought split (cube load+resample vs SPI gamma-fit vs grid aggregation), and projected dedup ceilings for a monthly-cube cache (CHG-0108) and a per-scale SPI-grid cache (CHG-0109). Loads NetCDFs only; writes nothing. | `python -m tools.diagnostics.profile_drought_fullpass --help` |
| `tools/diagnostics/audit_compute_consumption.py` | Read-only registry introspection (Front 0): for every computed `PIPELINE_SLUGS` slug, reports which scored bundles consume it (thematic `LANDING_BUNDLE_WEIGHTS` + sectoral `PROPOSAL_BUNDLES`) and which active dashboard domains (`DOMAINS`/`PILLAR_DOMAINS`) list it. Classifies each as `scored` / `browsable_only` (served individually, no composite) / `orphan` (no bundle, no domain) to flag compute we can drop before optimizing. No data/IO. | `python -m tools.diagnostics.audit_compute_consumption --help` |
| `tools/diagnostics/debug_build_master.py` | Debug helper for master build issues | `python -m tools.diagnostics.debug_build_master --help` |
| `tools/diagnostics/verify_states_geojson.py` | Verify `states_4326.geojson` is consistent with `districts_4326.geojson` | `python -m tools.diagnostics.verify_states_geojson` |
| `tools/diagnostics/verify_districts_blocks_geojson.py` | Sanity + parity checks for `districts_4326.geojson` and `blocks_4326.geojson` | `python -m tools.diagnostics.verify_districts_blocks_geojson districts` / `... blocks --sample 50` |
| `tools/diagnostics/verify_admin_join_consistency.py` | Cross-level join consistency for all three boundary layers: (1) naming — block→district→state mapping + `state_lgd_code`↔`state_name` agreement; (2) geometry — per-unit IoU/residual of children-dissolve vs parent polygon (EPSG:6933). Optional `--figures-dir` renders example district/state nesting maps, an area-parity scatter, and an IoU-band chart. Reads `{states,districts,blocks}_4326.geojson` from `--geojson-dir` (default: IRT data dir); only writes figures. Exit 1 if any unit's IoU < `--min-iou` (default 0.999). | `python -m tools.diagnostics.verify_admin_join_consistency --help` |
| `tools/diagnostics/roster_audit.py` | Canonical-roster audit + boundary-migration housekeeping (CHG-0089), keyed off the published per-state geometry shard. **Audit** (default, read-only): stale published masters, raw orphan dirs across BOTH the periods and `ensembles/` source subtrees, and a **completeness gate** (`canonical ⊆ published` over master **and** yearly_ensemble keys; exit 1 if any keeper is short). **`--quarantine-processed`**: move old-named raw dirs (periods + ensembles) → `processed/_stale_prelgd_bak/`, with a district file-level new-name interlock (hard-stop if a renamed unit lacks new-named periods/ensembles). **`--prune-optimised`**: move deferred-stale published masters → out-of-bundle `_stale_optimised_prelgd_bak/`, with a keeper-component guard. Apply modes default to `--dry-run` and write a JSON move-manifest. `--state`/`--level {district,block,all}`/`--keepers`. | `python -m tools.diagnostics.roster_audit --help` |
| `tools/diagnostics/migrate_trailing_dot_dirs.py` | Migrate processed dirs/files whose name component ends in a Windows-illegal trailing dot/space (e.g. block `Parali_V_.` -> `Parali_V_`), which Win32 cannot address (`WinError 3` aborts the optimized build; single-level globs silently skip the unit). Renames the offending directory **and** the descendant files that carry the old token as a stem prefix (`Parali_V_._periods.csv` -> `Parali_V__periods.csv`). **Dry-run by default** (needs `--apply`); idempotent; **collision-guarded** (refuses to merge when the sanitized target already exists, exits non-zero). On native Windows uses the `\\?\` extended-length prefix to address the dotted source; simplest to run from the WSL view where the dotted name is already addressable. `--root`/`--state`/`--verbose`. Pairs with the `safe_fs_component` hardening in `india_resilience_tool/utils/naming.py`. | `python -m tools.diagnostics.migrate_trailing_dot_dirs --help` |
| `tools/diagnostics/profile_prepare_dashboard.py` | End-to-end per-stage wall-clock timer for a real `prepare_dashboard` run (CHG-0097). Monkeypatches the orchestrator's single `execute_plan` chokepoint, then calls `prepare_dashboard.main` **unchanged** so it inherits the real arg parsing/plan building/readiness gating. Times each `PlannedCommand` and classifies it into canonical stages (`01_load+compute`, `02_exposure_context`, `03_admin_aggregation`, `04_bundle_assembly`, `05_optimized_publish`, …); reports **sum-of-stages**, **true wall**, and **unattributed** overhead (readiness scans + interpreter startup). Wrapper flags `--profile-json`/`--profile-csv`; all other args forward to `prepare_dashboard` (a leading `--` separator is stripped before forwarding). **NOT read-only — drives the real pipeline and writes processed outputs**; for a true cold-path number pass `--overwrite` against a disposable `IRT_DATA_DIR` copy, or use `--plan-only`/`--dry-run` (write-free, no timing). | `python -m tools.diagnostics.profile_prepare_dashboard climate-hazards --level district --models CanESM5 --scenarios historical --plan-only` |

## Geo / data acquisition / prep

| Script | Purpose | Run |
|---|---|---|
| `tools/geodata/convert_blocks_shp_to_geojson.py` | Convert block boundaries shapefile → GeoJSON | `python -m tools.geodata.convert_blocks_shp_to_geojson --help` |
| `tools/geodata/inspect_block_shapefile.py` | Inspect boundary shapefile/GeoJSON structure | `python -m tools.geodata.inspect_block_shapefile --help` |
| `tools/geodata/build_admin_boundaries_from_lgd.py` | **Single source of truth** for the admin hierarchy: derive `blocks_4326.geojson`, `districts_4326.geojson`, and `states_4326.geojson` from one bharatlas `LGD_Blocks` shapefile so all three nest exactly by construction (districts = dissolve of blocks by name; states = dissolve of districts). Canonical Title-Case state names; ADM3-loader-identical district/block label repair. Backs up existing outputs to `.bak-<timestamp>` on `--overwrite` | `python -m tools.geodata.build_admin_boundaries_from_lgd --help` |
| `tools/geodata/build_blocks_geojson.py` | _(Superseded by `build_admin_boundaries_from_lgd.py`)_ Rebuild only `blocks_4326.geojson` from the legacy `Block_GH_WUP` source block shapefile with label QA | `python -m tools.geodata.build_blocks_geojson --help` |
| `tools/geodata/build_adm1_geojson.py` | Build the compact optimized ADM1 state-polygons artifact for fast dashboard boot | `python -m tools.geodata.build_adm1_geojson --help` |
| `tools/geodata/build_states_geojson.py` | Build full-fidelity `states_4326.geojson` by dissolving `districts_4326.geojson` (unsimplified, shareable companion to district boundaries; not used at runtime) | `python -m tools.geodata.build_states_geojson --help` |
| `tools/geodata/build_district_subbasin_crosswalk.py` | Build the canonical district ↔ sub-basin crosswalk CSV from district and sub-basin GeoJSONs | `python -m tools.geodata.build_district_subbasin_crosswalk --help` |
| `tools/geodata/build_block_subbasin_crosswalk.py` | Build the canonical block ↔ sub-basin crosswalk CSV from block and sub-basin GeoJSONs | `python -m tools.geodata.build_block_subbasin_crosswalk --help` |
| `tools/geodata/build_district_basin_crosswalk.py` | Build the canonical district ↔ basin crosswalk CSV from district and basin GeoJSONs | `python -m tools.geodata.build_district_basin_crosswalk --help` |
| `tools/geodata/build_block_basin_crosswalk.py` | Build the canonical block ↔ basin crosswalk CSV from block and basin GeoJSONs | `python -m tools.geodata.build_block_basin_crosswalk --help` |
| `tools/geodata/prepare_aqueduct_baseline.py` | Build a clean Aqueduct baseline GeoJSON by joining baseline CSV attributes onto `future_annual` HydroBASINS geometry keyed by `pfaf_id` | `python -m tools.geodata.prepare_aqueduct_baseline --help` |
| `tools/geodata/build_aqueduct_admin_crosswalk.py` | Build direct Aqueduct HydroSHEDS Level 6 ↔ district overlap CSVs for admin-boundary transfer | `python -m tools.geodata.build_aqueduct_admin_crosswalk --help` |
| `tools/geodata/build_aqueduct_block_crosswalk.py` | Build direct Aqueduct HydroSHEDS Level 6 ↔ block overlap CSVs for admin-boundary transfer | `python -m tools.geodata.build_aqueduct_block_crosswalk --help` |
| `tools/geodata/build_aqueduct_admin_masters.py` | Build district and block master CSVs for the onboarded Aqueduct metrics under `processed/{metric_slug}/{state}/master_metrics_by_{district,block}.csv` | `python -m tools.geodata.build_aqueduct_admin_masters --help` |
| `tools/geodata/build_aqueduct_hydro_crosswalk.py` | Build Aqueduct HydroSHEDS Level 6 ↔ SOI basin/sub-basin overlap CSVs for area-weighted transfer | `python -m tools.geodata.build_aqueduct_hydro_crosswalk --help` |
| `tools/geodata/build_aqueduct_hydro_masters.py` | Build SOI basin/sub-basin master CSVs for the onboarded Aqueduct hydro metrics under `processed/{metric_slug}/hydro/` | `python -m tools.geodata.build_aqueduct_hydro_masters --help` |
| `tools/geodata/build_population_admin_masters.py` | Build district and block population exposure masters (`population_total`, `population_density`) and the display-only population overlay PNG/metadata from the 2025 raster | `python -m tools.geodata.build_population_admin_masters --help` |
| `tools/geodata/build_lulc_admin_masters.py` | Build district and block agricultural LULC exposure masters (`lulc_agri_area_km2`, `lulc_agri_share_pct`) and the display-only binary agricultural LULC overlay PNG/metadata from `LULC_2_Agri.tif` | `python -m tools.geodata.build_lulc_admin_masters --help` |
| `tools/geodata/build_groundwater_district_masters.py` | Build district groundwater assessment masters from the 2024-2025 GEC workbook with district-alias QA outputs | `python -m tools.geodata.build_groundwater_district_masters --help` |
| `tools/geodata/build_jrc_flood_depth_admin_masters.py` | Build per-state (`--state`, default Telangana) district/block JRC flood-depth masters using block flooded-cell `p95` and district flooded-area weighting, plus the derived RP100 flood-index, flood-extent masters, RP-100 display overlay artifacts (pan-India, shared), and state-scoped QA CSVs | `python -m tools.geodata.build_jrc_flood_depth_admin_masters --help` |
| `tools/optimized/build_processed_optimised.py` | Build the compact `processed_optimised` runtime bundle from the legacy `processed/` tree plus canonical geometry/context files, including persisted Glance view models, exact pre-scan task counting, deterministic parallel yearly processing, level filtering, nested terminal progress bars, and a post-build parity audit. **Yearly-loader backend:** the parallel yearly-model/ensemble reads run on a `ProcessPoolExecutor` by default; set `IRT_YEARLY_EXECUTOR=thread` (opt-in) to use a thread pool with per-worker adaptive chunking — safe because these workers read CSVs only (no geospatial/pyproj calls) — which removes Windows spawn overhead and lets small single-state jobs fan out. A single-chunk job always runs serially in-process (no pool spawn) regardless of backend. Output is byte-identical across backends. | `python -m tools.optimized.build_processed_optimised --help` |
| `tools/optimized/audit_processed_optimised_parity.py` | Audit the optimized runtime bundle against the dashboard-visible legacy processed contract, with optional level filtering, and emit `parity_report.json` | `python -m tools.optimized.audit_processed_optimised_parity --help` |
| `tools/geodata/validate_aqueduct_workflow.py` | Validate the Aqueduct cleanup, crosswalk, coverage, sensitivity, and master-value workflow and write per-metric validation bundles under `IRT_DATA_DIR/aqueduct/validation/{metric_slug}/` | `python -m tools.geodata.validate_aqueduct_workflow --help` |
| `tools/geodata/clean_river_network.py` | Clean the Survey of India river shapefile into canonical river artifacts (`river_network.parquet`, display GeoJSON, QA CSV) | `python -m tools.geodata.clean_river_network --help` |
| `tools/geodata/build_river_basin_reconciliation.py` | Build the canonical hydro-basin ↔ river-basin reconciliation CSV used by hydro river overlays | `python -m tools.geodata.build_river_basin_reconciliation --help` |
| `tools/geodata/build_river_subbasin_diagnostics.py` | Build the hydro sub-basin vs river-name diagnostics CSV used by hydro sub-basin overlays | `python -m tools.geodata.build_river_subbasin_diagnostics --help` |
| `tools/geodata/build_river_topology.py` | Build topology-ready river reaches, nodes, adjacency, and QA artifacts from the canonical river parquet | `python -m tools.geodata.build_river_topology --help` |
| `tools/subbasin_shp_explore.py` | Inspect, optionally repair, and export canonical basin/sub-basin GeoJSONs from `waterbasin_goi.shp` | `python -m tools.subbasin_shp_explore --help` |
| `tools/data_acquisition/download_era5_daily_stats_structured.py` | Download/structure ERA5 daily stats | `python -m tools.data_acquisition.download_era5_daily_stats_structured --help` |
| `tools/data_acquisition/nex_india_subset_download_s3_v1.py` | Download NEX India subset from S3 (serial; retained as a fallback) | `python -m tools.data_acquisition.nex_india_subset_download_s3_v1 --help` |
| `tools/data_acquisition/nex_india_subset_download_s3_v2.py` | Parallel pan-India NEX-GDDP-CMIP6 downloader: scope-cached S3 listing, ThreadPoolExecutor, atomic writes, classified retries, `--verify` quarantine, year × experiment intersection. Outputs to `${out_dir}/${member_dir}/${exp}/${var}/${model}/${year}.nc` (default `member_dir=r1i1p1f1_panIndia`). | `python -m tools.data_acquisition.nex_india_subset_download_s3_v2 --help` |
| `tools/data_prep/prepare_reanalysis_for_pipeline.py` | Prepare ERA5/IMD inputs for pipeline | `python -m tools.data_prep.prepare_reanalysis_for_pipeline --help` |
| `tools/data_prep/organize_era5_legacy_nc_files.py` | Reorganize legacy ERA5 NetCDF layout | `python -m tools.data_prep.organize_era5_legacy_nc_files --help` |
| `tools/data_prep/derive_hurs_from_era5_tas_tdps.py` | Derive humidity inputs from ERA5 fields | `python -m tools.data_prep.derive_hurs_from_era5_tas_tdps --help` |

`tools/data_acquisition/nex_india_subset_download_s3_v2.py` notes:
- Output layout: `${out_dir}/${member_dir}/${experiment}/${variable}/${model}/${year}.nc` (default `member_dir=r1i1p1f1_panIndia`).
- Default `--workers 8`; default `--open-mode download-first` (safer; `direct` additionally requires `s3fs` + `fsspec`).
- `--skip-existing` (default) skips non-empty files. `--verify` opens existing files; corrupt ones are moved to `*.bad` unless `--delete-bad-existing` is set.
- `--years 1990-2010,2050` is intersected with each experiment's policy range (`historical` 1951–2014; `ssp*` 2015–2100).
- Exit codes: `0` = clean; `1` = at least one task failed (or a scope had duplicate-year keys, e.g. `gn`/`gr1` mixed); `2` = no failures but a corrupt local file was quarantined with no S3 key to replace it.
- Atomic writes via unique `.tmp` + `os.replace`; safe on POSIX and NTFS. Temp source NetCDFs are cleaned on every path (success, failure, partial).

PowerShell + conda example (Windows operator):
```powershell
conda activate irt
$env:IRT_DATA_DIR = "D:\projects\irt_data_pan_india"
python -m tools.data_acquisition.nex_india_subset_download_s3_v2 `
    --out-dir $env:IRT_DATA_DIR `
    --workers 8
```

WSL/bash example:
```bash
IRT_DATA_DIR=/mnt/d/projects/irt_data_pan_india \
  python -m tools.data_acquisition.nex_india_subset_download_s3_v2 --workers 8
```

Scale advisory: pan-India × 5 variables × 3 experiments × all available CMIP6 models × full policy year range is a multi-hour, large-disk run even after bbox subsetting. Probe scale first with a small dry-run:
```bash
python -m tools.data_acquisition.nex_india_subset_download_s3_v2 \
    --variables pr --models GFDL-ESM4 --years 2000-2001 --dry-run
```
Windows tip: if HDF5 writes get flaky under parallelism, fall back to `--workers 2`.

**Important — output is not yet consumed by the compute pipeline.** Outputs land under `${out_dir}/r1i1p1f1_panIndia/`. The compute pipeline (`tools/pipeline/compute_indices_multiprocess.py` etc., resolved via `india_resilience_tool/config/paths.py`) currently reads `${out_dir}/r1i1p1f1/`. Until a separate staging or pipeline-config change lands, `_v2` downloads do not feed the compute pipeline. `_v1.py` and `download_pan_india_raw.sh` are unchanged and remain in service for the existing serial workflow.

`tools/subbasin_shp_explore.py` notes:
- source: `waterbasin_goi.shp`
- optional repair: `--repair-invalid`
- canonical outputs: `basins.geojson` and `subbasins.geojson`

`tools/geodata/prepare_aqueduct_baseline.py` notes:
- geometry source: Aqueduct GDB `future_annual` layer
- attribute source: `Aqueduct40_baseline_annual_*.csv`
- default scope: India-only (`gid_0 == IND`, excludes `pfaf_id = -9999`)
- canonical outputs:
  - `IRT_DATA_DIR/aqueduct/baseline_clean_india.geojson`
  - `IRT_DATA_DIR/aqueduct/baseline_clean_india_qa.csv`
  - `IRT_DATA_DIR/aqueduct/future_annual_india.geojson`
- baseline geometry is intentionally not used; the tool aggregates segmented baseline rows to one row per `pfaf_id`
  and also emits the India-only `future_annual` subset with the source future attributes preserved

`tools/geodata/build_aqueduct_hydro_crosswalk.py` notes:
- inputs:
  - `IRT_DATA_DIR/aqueduct/baseline_clean_india.geojson`
  - `IRT_DATA_DIR/basins.geojson`
  - `IRT_DATA_DIR/subbasins.geojson`
- analysis CRS: `EPSG:6933`
- outputs:
  - `IRT_DATA_DIR/aqueduct/aqueduct_basin_crosswalk.csv`
  - `IRT_DATA_DIR/aqueduct/aqueduct_subbasin_crosswalk.csv`

`tools/geodata/build_aqueduct_admin_crosswalk.py` notes:
- inputs:
  - `IRT_DATA_DIR/aqueduct/baseline_clean_india.geojson`
  - `IRT_DATA_DIR/districts_4326.geojson`
- analysis CRS: `EPSG:6933`
- output:
  - `IRT_DATA_DIR/aqueduct/aqueduct_district_crosswalk.csv`

`tools/geodata/build_aqueduct_block_crosswalk.py` notes:
- inputs:
  - `IRT_DATA_DIR/aqueduct/baseline_clean_india.geojson`
  - `IRT_DATA_DIR/blocks_4326.geojson`
- analysis CRS: `EPSG:6933`
- output:
  - `IRT_DATA_DIR/aqueduct/aqueduct_block_crosswalk.csv`

`tools/geodata/build_aqueduct_hydro_masters.py` notes:
- inputs:
  - `IRT_DATA_DIR/aqueduct/baseline_clean_india.geojson`
  - `IRT_DATA_DIR/aqueduct/future_annual_india.geojson`
  - Aqueduct hydro crosswalk CSVs
- outputs:
  - `IRT_DATA_DIR/processed/aq_water_stress/hydro/master_metrics_by_basin.csv`
  - `IRT_DATA_DIR/processed/aq_water_stress/hydro/master_metrics_by_sub_basin.csv`
  - `IRT_DATA_DIR/processed/aq_interannual_variability/hydro/master_metrics_by_basin.csv`
  - `IRT_DATA_DIR/processed/aq_interannual_variability/hydro/master_metrics_by_sub_basin.csv`
  - `IRT_DATA_DIR/processed/aq_seasonal_variability/hydro/master_metrics_by_basin.csv`
  - `IRT_DATA_DIR/processed/aq_seasonal_variability/hydro/master_metrics_by_sub_basin.csv`
  - `IRT_DATA_DIR/processed/aq_water_depletion/hydro/master_metrics_by_basin.csv`
  - `IRT_DATA_DIR/processed/aq_water_depletion/hydro/master_metrics_by_sub_basin.csv`
  - QA CSVs under `IRT_DATA_DIR/aqueduct/`
- current onboarded metric mappings:
  - water stress: baseline `bws_raw`, future `*_ws_x_r`
  - interannual variability: baseline `iav_raw`, future `*_iv_x_r`
  - seasonal variability: baseline `sev_raw`, future `*_sv_x_r`
  - water depletion: baseline `bwd_raw`, future `*_wd_x_r`
- if `--metric-slug` is omitted or set to `all`, the tool builds all onboarded Aqueduct hydro metrics

`tools/geodata/build_aqueduct_admin_masters.py` notes:
- inputs:
  - `IRT_DATA_DIR/aqueduct/baseline_clean_india.geojson`
  - `IRT_DATA_DIR/aqueduct/future_annual_india.geojson`
  - `IRT_DATA_DIR/aqueduct/aqueduct_district_crosswalk.csv`
  - `IRT_DATA_DIR/aqueduct/aqueduct_block_crosswalk.csv`
- outputs:
  - `IRT_DATA_DIR/processed/aq_water_stress/{state}/master_metrics_by_district.csv`
  - `IRT_DATA_DIR/processed/aq_water_stress/{state}/master_metrics_by_block.csv`
  - `IRT_DATA_DIR/processed/aq_interannual_variability/{state}/master_metrics_by_district.csv`
  - `IRT_DATA_DIR/processed/aq_interannual_variability/{state}/master_metrics_by_block.csv`
  - `IRT_DATA_DIR/processed/aq_seasonal_variability/{state}/master_metrics_by_district.csv`
  - `IRT_DATA_DIR/processed/aq_seasonal_variability/{state}/master_metrics_by_block.csv`
  - `IRT_DATA_DIR/processed/aq_water_depletion/{state}/master_metrics_by_district.csv`
  - `IRT_DATA_DIR/processed/aq_water_depletion/{state}/master_metrics_by_block.csv`
  - district and block QA CSVs under `IRT_DATA_DIR/aqueduct/`
- if `--metric-slug` is omitted or set to `all`, the tool builds all onboarded Aqueduct admin metrics

`tools/geodata/build_population_admin_masters.py` notes:
- source raster:
  - `IRT_DATA_DIR/population-*/population/ind_pop_2025_CN_1km_R2025A_UA_v1.tif`
- canonical boundary inputs:
  - `IRT_DATA_DIR/districts_4326.geojson`
  - `IRT_DATA_DIR/blocks_4326.geojson`
- outputs:
  - `IRT_DATA_DIR/processed/population_total/{state}/master_metrics_by_district.csv`
  - `IRT_DATA_DIR/processed/population_total/{state}/master_metrics_by_block.csv`
  - `IRT_DATA_DIR/processed/population_density/{state}/master_metrics_by_district.csv`
  - `IRT_DATA_DIR/processed/population_density/{state}/master_metrics_by_block.csv`
  - `IRT_DATA_DIR/population/overlay/population_exposure_2025_overlay.png`
  - `IRT_DATA_DIR/population/overlay/population_exposure_2025_overlay_meta.json`
- QA CSVs under `IRT_DATA_DIR/population/`
- uses raster cell-center inclusion (`all_touched=False`) and canonical polygon area in `EPSG:6933`
- the overlay is display-only binned people-per-source-cell context; dashboard runtime and optimized bundles read the exported PNG/metadata, never the raw TIFF

`tools/geodata/build_rural_facilities_admin_masters.py` notes:
- source shapefiles:
  - `Agroinfrastructure.shp`
  - `Educationinfrastructure.shp`
  - `Healthinfrastructure.shp`
  - `Serviceinfrastructure.shp`
- outputs:
  - `IRT_DATA_DIR/processed/rural_facilities_*/*/master_metrics_by_district.{csv,parquet}`
  - `IRT_DATA_DIR/processed/rural_facilities_*/*/master_metrics_by_block.{csv,parquet}`
  - `IRT_DATA_DIR/rural_facilities/overlay/rural_facilities_density_<category>_overlay.png`
  - `IRT_DATA_DIR/rural_facilities/overlay/rural_facilities_density_<category>_overlay_meta.json`
- QA files are written under `IRT_DATA_DIR/rural_facilities/`
- counts use deterministic point coverage into canonical blocks, with unmatched/ambiguous/invalid coordinates reported in QA

`tools/geodata/build_built_up_area_admin_masters.py` notes:
- source raster:
  - `IRT_DATA_DIR/built_up_area/Cleaned_India_Built_Surface_WGS84.tif`
  - a timestamped Drive download path may be supplied with `--raster`; move/rename the stable operational copy to the canonical path above for repeatable runs
- canonical boundary inputs:
  - `IRT_DATA_DIR/districts_4326.geojson`
  - `IRT_DATA_DIR/blocks_4326.geojson` (optional; missing blocks warn and district outputs still build)
- outputs:
  - `IRT_DATA_DIR/processed/built_up_area_km2/{state}/master_metrics_by_district.{csv,parquet}`
  - `IRT_DATA_DIR/processed/built_up_area_km2/{state}/master_metrics_by_block.{csv,parquet}`
  - `IRT_DATA_DIR/processed/built_up_area_share_pct/{state}/master_metrics_by_district.{csv,parquet}`
  - `IRT_DATA_DIR/processed/built_up_area_share_pct/{state}/master_metrics_by_block.{csv,parquet}`
  - `IRT_DATA_DIR/built_up_area/overlay/built_up_area_current_overlay.png`
  - `IRT_DATA_DIR/built_up_area/overlay/built_up_area_current_overlay_meta.json`
- QA files are written under `IRT_DATA_DIR/built_up_area/`
- source values are `m2/source cell`; `0` is valid no built-up and `65535` is invalid/background
- tabulation reprojects vectors to the raster CRS and uses `all_touched=False`; area-share denominators use polygon area in `EPSG:6933`
- useful commands:
  - `python -m tools.geodata.build_built_up_area_admin_masters --help`
  - `python -m tools.runs.prepare_dashboard built-up-area --built-up-raster "<path>" --plan-only`

`tools/geodata/build_lulc_admin_masters.py` notes:
- source raster:
  - `IRT_DATA_DIR/lulc/LULC_2_Agri.tif`
  - alternate source paths may be supplied with `--raster`; keep the canonical copy above for repeatable runs
- canonical boundary inputs:
  - `IRT_DATA_DIR/districts_4326.geojson`
  - `IRT_DATA_DIR/blocks_4326.geojson` (optional; missing blocks warn and district outputs still build)
- outputs:
  - `IRT_DATA_DIR/processed/lulc_agri_area_km2/{state}/master_metrics_by_district.{csv,parquet}`
  - `IRT_DATA_DIR/processed/lulc_agri_area_km2/{state}/master_metrics_by_block.{csv,parquet}`
  - `IRT_DATA_DIR/processed/lulc_agri_share_pct/{state}/master_metrics_by_district.{csv,parquet}`
  - `IRT_DATA_DIR/processed/lulc_agri_share_pct/{state}/master_metrics_by_block.{csv,parquet}`
  - `IRT_DATA_DIR/lulc/overlay/lulc_agri_current_overlay.png`
  - `IRT_DATA_DIR/lulc/overlay/lulc_agri_current_overlay_meta.json`
- QA files are written under `IRT_DATA_DIR/lulc/`
- source values are binary: `1` is agricultural LULC; `0` is nodata/background; unexpected values fail unless `--allow-unexpected-values` is supplied
- tabulation reads the raster through a nearest-neighbor `EPSG:6933` WarpedVRT and uses `all_touched=False`; area-share denominators use polygon area in `EPSG:6933`
- guardrails fail national totals outside `1,200,000-2,300,000 km2` unless `--allow-total-outlier` is supplied and district/block shares above `100.01%` unless `--allow-share-outlier` is supplied
- useful commands:
  - `python -m tools.geodata.build_lulc_admin_masters --help`
  - `python -m tools.runs.prepare_dashboard lulc --lulc-raster "<path>" --plan-only`

`tools/geodata/build_groundwater_district_masters.py` notes:
- source workbook:
  - `IRT_DATA_DIR/CentralReport1773820094787.xlsx`
- canonical boundary input:
  - `IRT_DATA_DIR/districts_4326.geojson`
- outputs:
  - `IRT_DATA_DIR/processed/gw_stage_extraction_pct/{state}/master_metrics_by_district.csv`
  - `IRT_DATA_DIR/processed/gw_future_availability_ham/{state}/master_metrics_by_district.csv`
  - `IRT_DATA_DIR/processed/gw_extractable_resource_ham/{state}/master_metrics_by_district.csv`
  - `IRT_DATA_DIR/processed/gw_total_extraction_ham/{state}/master_metrics_by_district.csv`
  - QA CSVs under `IRT_DATA_DIR/groundwater/`
- the tool refuses to write masters if any source districts remain unmatched after alias resolution

`tools/optimized/build_processed_optimised.py` notes:
- reads from:
  - `IRT_DATA_DIR/processed/`
  - current canonical root-level geometry and context artifacts under `IRT_DATA_DIR/`
- writes to:
  - `IRT_DATA_DIR/processed_optimised/`
  - `IRT_DATA_DIR/processed_optimised/parity_report.json` for unscoped runs, or an explicit `--report-path` for scoped runs
- retained runtime contract:
  - Parquet-only masters
  - yearly ensemble facts
  - yearly per-model facts
  - optional level-filtered rebuilds via `--level`
  - simplified display GeoJSON with persisted `area_m2`
  - compact selector indexes:
    - `context/admin_block_index.parquet`
    - `context/hydro_subbasin_index.parquet`
- terminal UX:
  - exact pre-scan task counting before execution
  - `--overwrite` rewrites only the selected optimized targets in place
  - `--overwrite --prune-scope` deletes only the exact selected output files before rewriting
  - `--full-rebuild` is the explicit destructive whole-bundle reset
  - `--dry-run` prints the resolved write/delete plan without mutating the bundle
  - yearly-model and yearly-ensemble stages use deterministic process-parallel execution by default at roughly `80%` of logical CPUs
  - `--workers <N>` overrides the default worker count
  - `--workers 1` forces serial execution
  - nested `tqdm` progress bars during execution
  - `--no-progress` disables the bars
  - `--state <name>` scopes admin district/block work to resolved legacy state roots while preserving their discovered names in output paths
  - state-scoped runs leave shared-global admin artifacts, `bundle_manifest.json`, and the global `parity_report.json` untouched by default
  - `--include-shared-admin-artifacts` opt-in rebuilds shared-global admin artifacts during a scoped run
  - state-scoped parity reports are written only when `--report-path` is supplied
- parity:
  - yearly ensemble facts are migrated directly from legacy ensemble CSVs
  - hydro yearly ensemble facts fall back to legacy hydro per-model yearly CSVs when the legacy hydro `ensembles/` tree is missing or empty
  - yearly model facts are migrated from legacy per-model CSVs where the UI exposes model-member overlays
  - a post-build audit reports any remaining missing optimized artifacts required by dashboard-visible flows
  - `tools.optimized.audit_processed_optimised_parity.py` accepts matching `--level` filters
- runtime preference:
  - the dashboard prefers optimized geometry shards and selector indexes when present, falling back to canonical geometry only when an optimized artifact is missing
- dropped runtime fields:
  - `std`
  - `p05`
  - `p95`
  - `n_models`
  - `values_per_model`

`tools/optimized/audit_processed_optimised_parity.py` notes:
- compares `processed_optimised/` against the dashboard-visible legacy `processed/` contract
- validates expected optimized masters, yearly facts, geometry, context, and manifest outputs
- accepts repeatable `--state` for admin-scoped audits
- leaves the global `parity_report.json` untouched on scoped runs unless `--report-path` is supplied
- exits non-zero when parity gaps remain

`tools/geodata/build_admin_boundaries_from_lgd.py` notes:
- source shapefile (resolved from the first existing of):
  - `IRT_DATA_DIR/LGD_Blocks/LGD_Blocks.shp`
  - `IRT_DATA_DIR/_tmp_lgd_blocks/LGD_Blocks.shp`
  - `IRT_DATA_DIR/LGD_Blocks.shp`
  - override with `--source`
- canonical outputs (all three derived from the same atomic block layer so they nest exactly):
  - `IRT_DATA_DIR/blocks_4326.geojson` — atomic blocks (one row per block)
  - `IRT_DATA_DIR/districts_4326.geojson` — dissolve of blocks by `(state_name, district_name)`
  - `IRT_DATA_DIR/states_4326.geojson` — dissolve of districts by `state_name`
- design contract:
  - district identity keyed on **name**, not `dist_lgd` (preserves 2023 splits that still share a parent LGD code; the modal LGD code is kept only as a reference attribute)
  - state names canonicalized to Title-Case via an exhaustive map; an unmapped source state is a **hard error**
  - district/block labels run through the same `repair_adm3_identity_columns` the ADM3 loader applies at runtime, so the district file and block-derived district references match exactly at load time
  - redundant trailing `" District"` suffix stripped (e.g. `Lakshadweep District` → `Lakshadweep`)
  - fails the build on the same suspicious admin-label characters the block loader rejects
- safety: refuses to clobber without `--overwrite`; on overwrite, backs up each existing output to `<file>.bak-<timestamp>` unless `--no-backup`
- `--dry-run` prints the full per-state district/block roster + hierarchy QA (and `--qa-out` writes the per-state table to CSV) without writing any GeoJSON
- current roster: 7,134 blocks · 783 districts · 36 states/UTs (Arunachal Pradesh fully present)

`tools/geodata/build_blocks_geojson.py` notes:
- **superseded** by `build_admin_boundaries_from_lgd.py`, which now produces `blocks_4326.geojson` (along with the matching districts/states) from the bharatlas `LGD_Blocks` source; this legacy builder rebuilds only the block layer from the older `Block_GH_WUP` shapefile and is retained for reference
- source shapefile:
  - `IRT_DATA_DIR/Block_GH_WUP_POP R2025A _GHS_WUP/Block_GH_WUP_POP R2025A _GHS_WUP.shp`
- canonical output:
  - `IRT_DATA_DIR/blocks_4326.geojson`
- QA outputs:
  - `IRT_DATA_DIR/block_boundary_repair_summary.csv`
  - `IRT_DATA_DIR/block_boundary_label_anomalies.csv`
- refuses to publish a canonical block GeoJSON if suspicious admin-label corruption remains after canonicalization

`tools/geodata/validate_aqueduct_workflow.py` notes:
- writes per-metric validation bundles under:
  - `IRT_DATA_DIR/aqueduct/validation/aq_water_stress/`
  - `IRT_DATA_DIR/aqueduct/validation/aq_interannual_variability/`
  - `IRT_DATA_DIR/aqueduct/validation/aq_seasonal_variability/`
  - `IRT_DATA_DIR/aqueduct/validation/aq_water_depletion/`
- each bundle now includes district, block, basin, and sub-basin validation outputs
- if `--metric-slug` is omitted or set to `all`, the validator runs for all onboarded Aqueduct admin and hydro metrics

Aqueduct methodology note:
- see [`docs/aqueduct_onboarding_methodology.md`](../docs/aqueduct_onboarding_methodology.md) for the end-to-end explanation of baseline cleanup, district/block transfer, crosswalk construction, and SOI hydro transfer.
- see [`docs/aqueduct_field_contract.md`](../docs/aqueduct_field_contract.md) for the current Aqueduct source-field mappings used by the onboarded Aqueduct district, block, and hydro metrics.

`tools/geodata/clean_river_network.py` notes:
- source: `river_network_goi.shp`
- canonical output: `IRT_DATA_DIR/river_network.parquet`
- derived outputs:
  - `IRT_DATA_DIR/river_network_display.geojson`
  - `IRT_DATA_DIR/river_network_qa.csv`
- preserves raw source fields and adds canonical cleaned columns plus QA flags
- first tranche only: cleaning + QA, no topology/routing inference

`tools/geodata/build_river_basin_reconciliation.py` notes:
- inputs:
  - `IRT_DATA_DIR/basins.geojson`
  - `IRT_DATA_DIR/river_network_display.geojson`
- canonical output:
  - `IRT_DATA_DIR/river_basin_name_reconciliation.csv`
- emits one row per hydro basin with:
  - `matched`
  - `review_required`
  - `no_source_rivers`
- hydro river overlays consume this CSV at runtime

`tools/geodata/build_river_subbasin_diagnostics.py` notes:
- inputs:
  - `IRT_DATA_DIR/subbasins.geojson`
  - `IRT_DATA_DIR/river_network_display.geojson`
- output:
  - `IRT_DATA_DIR/river_subbasin_diagnostics.csv`

`tools/geodata/build_river_topology.py` notes:
- input:
  - `IRT_DATA_DIR/river_network.parquet`
- hydro context inputs:
  - `IRT_DATA_DIR/basins.geojson`
  - `IRT_DATA_DIR/subbasins.geojson`
- outputs:
  - `IRT_DATA_DIR/river_reaches.parquet`
  - `IRT_DATA_DIR/river_nodes.parquet`
  - `IRT_DATA_DIR/river_adjacency.parquet`
  - `IRT_DATA_DIR/river_topology_qa.csv`
  - `IRT_DATA_DIR/river_missing_assignments.csv`
  - `IRT_DATA_DIR/river_missing_assignments.geojson`

## Legacy / one-offs

| Script | Purpose | Run |
|---|---|---|
| `tools/legacy/DONOTUSE_ArtparkGenerateReport.py` | Historical one-off report script (kept for reproducibility) | `python tools/legacy/DONOTUSE_ArtparkGenerateReport.py` |

## Telangana Block Yearly Model Recovery

Use explicit preserve cleanup when rebuilding block climate metrics that must feed dashboard model-member traces.

python -m tools.pipeline.compute_indices_multiprocess --state Telangana --level block --overwrite --yearly-cleanup-policy preserve --metrics tas_annual_mean
python -m tools.optimized.build_processed_optimised --state Telangana --level block --overwrite --prune-scope --skip-geometry --skip-context --metric tas_annual_mean

Generate repeated metric flags from the optimized yearly inventory:
python -m tools.diagnostics.list_optimized_yearly_metrics --state Telangana --level block --format args

Run the strict state-scoped parity audit after rebuilding:
python -m tools.optimized.audit_processed_optimised_parity --state Telangana --level block --require-block-yearly-models --strict --report-path D:/projects/irt_data/processed_optimised/parity_report_telangana_block_yearly_models.json

Notes:
- compute marker schema version is 5 and ensemble marker schema version is 4
- default cleanup deletes block yearly CSVs after ensembles but preserves district, basin, and sub-basin yearly CSVs
- preserve keeps block per-model yearly CSVs; budget disk before full-state runs

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
python -m tools.runs.prepare_dashboard aqueduct
```

```bash
python -m tools.runs.prepare_dashboard dashboard-package --plan-only
```

By default the runner is non-destructive and dashboard-oriented:
- climate runs default to `--level all`
- climate and bundle runs refresh `processed_optimised`
- the optimized parity audit runs automatically
- use `--overwrite` only when you want to force a rebuild

For the full command catalog, see [`../docs/command_catalog.md`](../docs/command_catalog.md).

## Pipeline

| Script | Purpose | Run |
|---|---|---|
| `tools/pipeline/compute_indices_multiprocess.py` | Build processed index artifacts (multi-process; district + block) | `python -m tools.pipeline.compute_indices_multiprocess --help` |
| `tools/pipeline/compute_indices.py` | Build processed index artifacts (single-process; debug) | `python -m tools.pipeline.compute_indices --help` |
| `tools/pipeline/build_master_metrics.py` | Build admin and hydro master CSVs plus summary sidecars; hydro levels auto-use `processed/{metric}/hydro/` | `python -m tools.pipeline.build_master_metrics --help` |
| `tools/pipeline/build_all_csv.ps1` | Windows helper to run common builds | `powershell -File tools/pipeline/build_all_csv.ps1` |

## Diagnostics

| Script | Purpose | Run |
|---|---|---|
| `tools/diagnostics/spi_diagnostic.py` | Sanity checks for SPI outputs (distribution/mean/std) | `python -m tools.diagnostics.spi_diagnostic --help` |
| `tools/diagnostics/debug_build_master.py` | Debug helper for master build issues | `python -m tools.diagnostics.debug_build_master --help` |

## Geo / data acquisition / prep

| Script | Purpose | Run |
|---|---|---|
| `tools/geodata/convert_blocks_shp_to_geojson.py` | Convert block boundaries shapefile → GeoJSON | `python -m tools.geodata.convert_blocks_shp_to_geojson --help` |
| `tools/geodata/inspect_block_shapefile.py` | Inspect boundary shapefile/GeoJSON structure | `python -m tools.geodata.inspect_block_shapefile --help` |
| `tools/geodata/build_blocks_geojson.py` | Rebuild the canonical `blocks_4326.geojson` from the source block shapefile with label QA | `python -m tools.geodata.build_blocks_geojson --help` |
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
| `tools/geodata/build_population_admin_masters.py` | Build district and block population exposure masters (`population_total`, `population_density`) from the 2025 raster | `python -m tools.geodata.build_population_admin_masters --help` |
| `tools/geodata/build_groundwater_district_masters.py` | Build district groundwater assessment masters from the 2024-2025 GEC workbook with district-alias QA outputs | `python -m tools.geodata.build_groundwater_district_masters --help` |
| `tools/optimized/build_processed_optimised.py` | Build the compact `processed_optimised` runtime bundle from the legacy `processed/` tree plus canonical geometry/context files, with exact pre-scan task counting, yearly parity migration, nested terminal progress bars, and a post-build parity audit | `python -m tools.optimized.build_processed_optimised --help` |
| `tools/optimized/audit_processed_optimised_parity.py` | Audit the optimized runtime bundle against the dashboard-visible legacy processed contract and emit `parity_report.json` | `python -m tools.optimized.audit_processed_optimised_parity --help` |
| `tools/geodata/validate_aqueduct_workflow.py` | Validate the Aqueduct cleanup, crosswalk, coverage, sensitivity, and master-value workflow and write per-metric validation bundles under `IRT_DATA_DIR/aqueduct/validation/{metric_slug}/` | `python -m tools.geodata.validate_aqueduct_workflow --help` |
| `tools/geodata/clean_river_network.py` | Clean the Survey of India river shapefile into canonical river artifacts (`river_network.parquet`, display GeoJSON, QA CSV) | `python -m tools.geodata.clean_river_network --help` |
| `tools/geodata/build_river_basin_reconciliation.py` | Build the canonical hydro-basin ↔ river-basin reconciliation CSV used by hydro river overlays | `python -m tools.geodata.build_river_basin_reconciliation --help` |
| `tools/geodata/build_river_subbasin_diagnostics.py` | Build the hydro sub-basin vs river-name diagnostics CSV used by hydro sub-basin overlays | `python -m tools.geodata.build_river_subbasin_diagnostics --help` |
| `tools/geodata/build_river_topology.py` | Build topology-ready river reaches, nodes, adjacency, and QA artifacts from the canonical river parquet | `python -m tools.geodata.build_river_topology --help` |
| `tools/subbasin_shp_explore.py` | Inspect, optionally repair, and export canonical basin/sub-basin GeoJSONs from `waterbasin_goi.shp` | `python -m tools.subbasin_shp_explore --help` |
| `tools/data_acquisition/download_era5_daily_stats_structured.py` | Download/structure ERA5 daily stats | `python -m tools.data_acquisition.download_era5_daily_stats_structured --help` |
| `tools/data_acquisition/nex_india_subset_download_s3_v1.py` | Download NEX India subset from S3 | `python -m tools.data_acquisition.nex_india_subset_download_s3_v1 --help` |
| `tools/data_prep/prepare_reanalysis_for_pipeline.py` | Prepare ERA5/IMD inputs for pipeline | `python -m tools.data_prep.prepare_reanalysis_for_pipeline --help` |
| `tools/data_prep/organize_era5_legacy_nc_files.py` | Reorganize legacy ERA5 NetCDF layout | `python -m tools.data_prep.organize_era5_legacy_nc_files --help` |
| `tools/data_prep/derive_hurs_from_era5_tas_tdps.py` | Derive humidity inputs from ERA5 fields | `python -m tools.data_prep.derive_hurs_from_era5_tas_tdps --help` |

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
- QA CSVs under `IRT_DATA_DIR/population/`
- uses raster cell-center inclusion (`all_touched=False`) and canonical polygon area in `EPSG:6933`

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
  - `IRT_DATA_DIR/processed_optimised/parity_report.json`
- retained runtime contract:
  - Parquet-only masters
  - yearly ensemble facts
  - yearly per-model facts
  - simplified display GeoJSON with persisted `area_m2`
  - compact selector indexes:
    - `context/admin_block_index.parquet`
    - `context/hydro_subbasin_index.parquet`
- terminal UX:
  - exact pre-scan task counting before execution
  - nested `tqdm` progress bars during execution
  - `--no-progress` disables the bars
- parity:
  - yearly ensemble facts are migrated directly from legacy ensemble CSVs
  - hydro yearly ensemble facts fall back to legacy hydro per-model yearly CSVs when the legacy hydro `ensembles/` tree is missing or empty
  - yearly model facts are migrated from legacy per-model CSVs where the UI exposes model-member overlays
  - a post-build audit reports any remaining missing optimized artifacts required by dashboard-visible flows
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
- exits non-zero when parity gaps remain

`tools/geodata/build_blocks_geojson.py` notes:
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

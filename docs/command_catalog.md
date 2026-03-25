# Command Catalog

This file is the single source of truth for common operational commands in IRT.

The recommended path is the new runner:

```bash
python -m tools.runs.prepare_dashboard --help
```

Run it from the repo root with the correct Conda environment active.

## Canonical runner

### List available bundles and steps

```bash
python -m tools.runs.prepare_dashboard list
```

### Preview an Aqueduct run without executing anything

```bash
python -m tools.runs.prepare_dashboard aqueduct --dry-run --overwrite
```

### Run the full Aqueduct workflow

```bash
python -m tools.runs.prepare_dashboard aqueduct --overwrite
```

Default bundle contents:
- canonical block-boundary refresh
- district crosswalk
- block crosswalk
- hydro crosswalk
- admin masters
- hydro masters
- Aqueduct validation

Optional raw baseline prep:

```bash
python -m tools.runs.prepare_dashboard aqueduct --prepare-baseline --source-gdb /path/to/Aq40_Y2023D07M05.gdb --baseline-csv /path/to/Aqueduct40_baseline_annual_y2023m07d05.csv --overwrite
```

### Run climate hazard processing

```bash
python -m tools.runs.prepare_dashboard climate-hazards --level all --state Telangana --overwrite
```

Notes:
- `--level all` expands to `district`, `block`, `basin`, and `sub_basin`
- if no `--state` is passed for admin levels, the runner defaults to `Telangana`
- the runner orchestrates:
  - `tools.pipeline.compute_indices_multiprocess`
  - `tools.pipeline.build_master_metrics`

### Build population exposure masters

```bash
python -m tools.runs.prepare_dashboard population-exposure --overwrite
```

Optional raster override:

```bash
python -m tools.runs.prepare_dashboard population-exposure --population-raster /path/to/ind_pop_2025_CN_1km_R2025A_UA_v1.tif --overwrite
```

The runner refreshes the canonical block boundaries first:

```bash
python -m tools.runs.prepare_dashboard blocks-geojson --overwrite
```

### Build groundwater district masters

```bash
python -m tools.runs.prepare_dashboard groundwater --overwrite
```

Optional source and alias overrides:

```bash
python -m tools.runs.prepare_dashboard groundwater --groundwater-workbook /path/to/CentralReport1773820094787.xlsx --groundwater-alias-csv /path/to/groundwater_district_aliases.csv --overwrite
```

This bundle writes district-only masters for:
- `gw_stage_extraction_pct`
- `gw_future_availability_ham`
- `gw_extractable_resource_ham`
- `gw_total_extraction_ham`

### Prepare the dashboard package end to end

```bash
python -m tools.runs.prepare_dashboard dashboard-package --level all --state Telangana --overwrite
```

This bundle now includes:
- canonical block-boundary refresh
- climate hazard compute + master builds
- Aqueduct prep + validation
- population exposure master builds
- groundwater district master builds

Optional validation tests at the end:

```bash
python -m tools.runs.prepare_dashboard dashboard-package --level all --state Telangana --overwrite --include-pytest
```

### Run validation only

```bash
python -m tools.runs.prepare_dashboard validate --overwrite --include-pytest
```

### Run a single step

Examples:

```bash
python -m tools.runs.prepare_dashboard aqueduct-block-crosswalk --overwrite
```

```bash
python -m tools.runs.prepare_dashboard climate-compute --level hydro --metrics tas_annual_mean prcptot
```

## Underlying commands

Use these when you need direct control or debugging.

### Dashboard

```bash
streamlit run main.py
```

```bash
streamlit run india_resilience_tool/app/main.py
```

### Climate hazards

```bash
python -m tools.pipeline.compute_indices_multiprocess --help
python -m tools.pipeline.build_master_metrics --help
```

Examples:

```bash
python -m tools.pipeline.compute_indices_multiprocess --level district --state Telangana --metrics tas_annual_mean
```

```bash
python -m tools.pipeline.build_master_metrics --level district --state Telangana --metrics tas_annual_mean
```

### Aqueduct

```bash
python -m tools.geodata.prepare_aqueduct_baseline --help
python -m tools.geodata.build_aqueduct_admin_crosswalk --help
python -m tools.geodata.build_aqueduct_block_crosswalk --help
python -m tools.geodata.build_aqueduct_hydro_crosswalk --help
python -m tools.geodata.build_aqueduct_admin_masters --help
python -m tools.geodata.build_aqueduct_hydro_masters --help
python -m tools.geodata.validate_aqueduct_workflow --help
```

### Population exposure

```bash
python -m tools.geodata.build_blocks_geojson --help
python -m tools.geodata.build_population_admin_masters --help
```

### Groundwater

```bash
python -m tools.geodata.build_groundwater_district_masters --help
```

### Optimized runtime bundle

```bash
python -m tools.optimized.build_processed_optimised --help
```

Build the compact dashboard-serving bundle from the legacy `processed/` tree:

```bash
python -m tools.optimized.build_processed_optimised --overwrite
```

Resume after a late failure without deleting the partial bundle first:

```bash
python -m tools.optimized.build_processed_optimised
```

Disable nested `tqdm` progress bars:

```bash
python -m tools.optimized.build_processed_optimised --overwrite --no-progress
```

Audit optimized-bundle parity against the dashboard-visible legacy `processed/` contract:

```bash
python -m tools.optimized.audit_processed_optimised_parity
```

### Tests

```bash
python -m pytest -q
```

Targeted validation set used by the runner:

```bash
python -m pytest -q tests/test_build_blocks_geojson.py tests/test_prepare_aqueduct_baseline.py tests/test_aqueduct_admin_transfer.py tests/test_aqueduct_hydro_transfer.py tests/test_groundwater_district_masters.py tests/test_population_admin_masters.py tests/test_validate_aqueduct_workflow.py tests/test_metrics_registry.py tests/test_config.py tests/test_available_states.py tests/test_crosswalk_generator.py
```

## Expected outputs

### Climate hazards
- processed climate artifacts under `IRT_DATA_DIR/processed/{metric}/...`
- master CSVs from `tools.pipeline.build_master_metrics`

### Aqueduct
- crosswalks under `IRT_DATA_DIR/aqueduct/`
- district/block masters under `IRT_DATA_DIR/processed/{metric_slug}/{state}/`
- hydro masters under `IRT_DATA_DIR/processed/{metric_slug}/hydro/`
- validation bundles under `IRT_DATA_DIR/aqueduct/validation/{metric_slug}/`

### Population exposure
- canonical block GeoJSON under `IRT_DATA_DIR/blocks_4326.geojson`
- block-boundary QA outputs under `IRT_DATA_DIR/block_boundary_*.csv`
- district/block masters under `IRT_DATA_DIR/processed/population_{total,density}/{state}/`
- QA bundles under `IRT_DATA_DIR/population/`

### Groundwater
- district masters under `IRT_DATA_DIR/processed/gw_*/{state}/`
- alias/crosswalk QA outputs under `IRT_DATA_DIR/groundwater/`

### Optimized runtime bundle
- compact runtime bundle under `IRT_DATA_DIR/processed_optimised/`
- Parquet masters under `IRT_DATA_DIR/processed_optimised/metrics/<slug>/masters/...`
- yearly ensemble Parquet facts under `IRT_DATA_DIR/processed_optimised/metrics/<slug>/yearly_ensemble/...`
- yearly model Parquet facts under `IRT_DATA_DIR/processed_optimised/metrics/<slug>/yearly_models/...`
- simplified runtime geometry under `IRT_DATA_DIR/processed_optimised/geometry/...`
  - optimized geometry shards persist `area_m2` for summary weighting reuse
- compact selector indexes under `IRT_DATA_DIR/processed_optimised/context/`
  - `admin_block_index.parquet`
  - `hydro_subbasin_index.parquet`
- context artifacts, `bundle_manifest.json`, and `parity_report.json` under `IRT_DATA_DIR/processed_optimised/`

## Notes

- The runner assumes the environment is already activated; it does not manage Conda itself.
- `--dry-run` is the safest way to inspect what will execute.
- `--overwrite` is passed through only to commands that support it.

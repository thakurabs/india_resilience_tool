# India Resilience Tool (IRT) - Codebase Manifest

## Overview

IRT is a Streamlit-based climate-risk and resilience dashboard organized around two spatial families:

- **Admin**: district, block
- **Hydro**: basin, sub-basin

The current working tree supports:
- map, rankings, and details flows for district, block, basin, and sub-basin
- admin portfolio workflows for district and block
- hydro boundary loading and hydro processed-output discovery
- actionable polygon crosswalk context, navigation, and related-unit highlighting across district/block and basin/sub-basin views
- optional hydro-only river overlay in basin/sub-basin maps

The crosswalk layer is currently **read-optimized and explanatory**. It is not yet a full weighted transfer engine across spatial families.

**Author:** Abu Bakar Siddiqui Thakur  
**Email:** absthakur@resilience.org.in  
**Primary stack:** Python 3.10+, Streamlit, Pandas, GeoPandas, Folium, Matplotlib, Plotly, Xarray

## Quick reference

### Main entry points

| Command | Purpose |
|---------|---------|
| `streamlit run main.py` | Launch dashboard from root entrypoint |
| `streamlit run india_resilience_tool/app/main.py` | Launch dashboard from package entrypoint |
| `python -m tools.pipeline.build_master_metrics` | Rebuild master CSVs |
| `python -m tools.pipeline.compute_indices_multiprocess --help` | Show compute-pipeline options |
| `python -m tools.pipeline.compute_indices_multiprocess --level district --metrics <slug>` | Build district outputs |
| `python -m tools.pipeline.compute_indices_multiprocess --level block --metrics <slug>` | Build block outputs |
| `python -m tools.pipeline.compute_indices_multiprocess --level basin --metrics <slug>` | Build basin outputs |
| `python -m tools.pipeline.compute_indices_multiprocess --level sub_basin --metrics <slug>` | Build sub-basin outputs |
| `python -m tools.subbasin_shp_explore --help` | Inspect/repair/export hydro boundaries |
| `python -m tools.geodata.build_district_subbasin_crosswalk --overwrite` | Build district ↔ sub-basin crosswalk CSV |
| `python -m tools.geodata.build_block_subbasin_crosswalk --overwrite` | Build block ↔ sub-basin crosswalk CSV |
| `python -m tools.geodata.build_district_basin_crosswalk --overwrite` | Build district ↔ basin crosswalk CSV |
| `python -m tools.geodata.build_block_basin_crosswalk --overwrite` | Build block ↔ basin crosswalk CSV |
| `python -m tools.geodata.clean_river_network --src <path> --overwrite` | Clean Survey of India river network into canonical river artifacts |
| `python -m tools.geodata.build_river_basin_reconciliation --overwrite` | Build hydro-basin ↔ river-basin reconciliation CSV |
| `python -m tools.geodata.build_river_subbasin_diagnostics --overwrite` | Build hydro sub-basin vs river-name diagnostics CSV |
| `python -m tools.geodata.build_river_topology --overwrite` | Build topology-ready river reaches, nodes, adjacency, and QA artifacts |
| `python -m pytest -q` | Run tests |

### Key environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `IRT_PILOT_STATE` | `Telangana` | Default admin state in the UI |
| `IRT_DATA_DIR` | resolved in `paths.py` | Base directory for boundaries, crosswalks, and processed outputs |
| `IRT_PROCESSED_ROOT` | `IRT_DATA_DIR/processed/{metric}` | Optional processed-root override |
| `IRT_DEBUG` | `0` | Enable debug/perf output |

## Top-level repo map

### Root files

| Path | Purpose |
|------|---------|
| `AGENTS.md` | Repo-wide agent instructions and workflow guardrails |
| `README.md` | Human-facing setup and usage guide |
| `MANIFEST.md` | AI/engineer-facing repo map and contracts |
| `main.py` | Root Streamlit entrypoint |
| `paths.py` | Canonical path and data-contract configuration |
| `environment.yml` | Canonical Conda environment |
| `environment.freeze.yml` | Reference environment snapshot |
| `requirements.txt` | Pointer/reference requirements file |
| `requirements.freeze.txt` | Freeze/export reference |
| `LICENSE` | License text |
| `resilience_actions_logo_transparent.png` | Branding asset used in the sidebar |

### Primary directories

| Path | Purpose |
|------|---------|
| `india_resilience_tool/` | Main application package |
| `tools/` | Operational, data-prep, pipeline, diagnostics, and geodata utilities |
| `tests/` | Main pytest suite |
| `docs/` | Handoffs, smoke tests, and repo/process notes |
| `notebooks/` | Exploratory notebooks and notebook-specific instructions |

Notes:
- `__pycache__/` directories are intentionally omitted below.
- Local logs, zips, and untracked working files are not treated as canonical repo modules.

## Package inventory

### `india_resilience_tool/analysis/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `map_enrichment.py` | Streamlit-free map enrichment helpers: baseline/delta, ranking, tooltip prep |
| `metrics.py` | Risk-class and percentile/ranking helpers |
| `portfolio.py` | Portfolio comparison logic and portfolio-level data prep |
| `timeseries.py` | Yearly series loading for admin and hydro flows |

### `india_resilience_tool/app/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `adm2_cache.py` | Streamlit-cached ADM2 loading and FeatureCollection helpers |
| `case_study_runtime.py` | Runtime helpers for district-focused case-study export |
| `color_range_controls.py` | Robust color-range default calculation for maps |
| `crosswalk_runtime.py` | App-layer crosswalk navigation and overlay-state helpers |
| `details_runtime.py` | Right-panel orchestration and data prep for details views |
| `geo_cache.py` | Streamlit-cached admin and hydro geometry loading/builders |
| `geography.py` | Filesystem-backed admin geography discovery helpers |
| `geography_controls.py` | Sidebar geography + analysis-focus controls for admin and hydro |
| `help_text.py` | Tooltip/help-text helpers for ribbon widgets |
| `left_panel_runtime.py` | Left-panel orchestration for map vs rankings |
| `main.py` | Package Streamlit entrypoint |
| `map_layer_runtime.py` | Streamlit-free Folium layer construction using cached FeatureCollections |
| `map_pipeline.py` | Merge -> enrich -> colors -> map/rankings pipeline |
| `master_cache.py` | Streamlit session-state cache for master CSV + schema loading |
| `master_freshness.py` | Master CSV freshness/rebuild gating helpers |
| `perf.py` | Lightweight timing/performance instrumentation |
| `point_selection_ui.py` | Coordinate input, preview, and saved-point support |
| `portfolio_multistate.py` | Multi-state portfolio helper functions |
| `portfolio_state_runtime.py` | Session-state wrappers around portfolio operations |
| `portfolio_ui.py` | Portfolio right-panel UI and comparison workflows |
| `ribbon.py` | Metric selection ribbon, master loading, and hydro-master readiness checks |
| `runtime.py` | Canonical app orchestrator (`run_app`) |
| `sidebar.py` | Family/level/view selector widgets and jump-once helpers |
| `sidebar_branding.py` | Sidebar logo/branding render block |
| `state.py` | Session-state defaults, level constants, and level-aware helpers |

#### `india_resilience_tool/app/views/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `details_panel.py` | Render the single-unit details panel and crosswalk context/actions |
| `map_view.py` | Render Folium map and extract click payloads |
| `rankings_view.py` | Rankings table rendering and portfolio add flows |
| `state_summary_view.py` | State summary view for admin-focused overview flows |

### `india_resilience_tool/compute/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `master_builder.py` | Build master CSVs, including hydro master enrichment |
| `spi_adapter.py` | SPI adapter around `climate-indices` |

#### `india_resilience_tool/compute/tests/`

| File | Purpose |
|------|---------|
| `test_spi_adapter.py` | SPI adapter tests |

### `india_resilience_tool/config/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `constants.py` | UI, styling, scenario, and geometry-render constants |
| `metrics_registry.py` | Canonical metric and bundle registry |
| `paths.py` | Library-side path config mirroring root `paths.py` |
| `variables.py` | Dashboard-facing variable registry derived from metrics registry |

### `india_resilience_tool/data/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `adm2_loader.py` | District boundary loading, normalization, and FeatureCollection builders |
| `adm3_loader.py` | Block boundary loading and normalization |
| `crosswalks.py` | Polygon crosswalk validation and context builders for district/block ↔ basin/sub-basin |
| `discovery.py` | Processed-artifact discovery helpers for yearly files and outputs |
| `hydro_loader.py` | Basin/sub-basin loading, validation, keys, and render simplification |
| `river_loader.py` | Cleaned river-display loading, validation, reconciliation, diagnostics, and hydro filtering helpers |
| `river_topology.py` | Streamlit-free river reach validation and hydro-side river summary builders |
| `master_columns.py` | Streamlit-free master column resolution helpers |
| `master_loader.py` | Robust master CSV loading, normalization, and schema parsing |
| `merge.py` | Boundary ↔ master merge helpers for district, block, basin, and sub-basin |
| `spatial_match.py` | Click/selection matching helpers for admin and hydro flows |

### `india_resilience_tool/utils/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `naming.py` | Name normalization, aliasing, and join-key helpers |
| `processed_io.py` | Lightweight Parquet/CSV I/O helpers for processed outputs |

### `india_resilience_tool/viz/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `charts.py` | Chart and figure generation for details and portfolio flows |
| `colors.py` | Color scales, legends, and map-color helpers |
| `exports.py` | PDF/ZIP export helpers |
| `folium_featurecollection.py` | Streamlit-free FeatureCollection patching/filtering helpers |
| `formatting.py` | Numeric/text formatting helpers |
| `style.py` | Shared plotting/style helpers |
| `tables.py` | Rankings and comparison table formatting/builders |

## Tools inventory

### `tools/`

| File | Purpose |
|------|---------|
| `AGENTS.md` | Tooling-specific agent instructions |
| `README.md` | Tooling overview and command reference |
| `__init__.py` | Package marker |
| `subbasin_shp_explore.py` | Inspect, repair, and export canonical hydro boundaries |

### `tools/data_acquisition/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `download_era5_daily_stats_structured.py` | Download structured ERA5 daily stats from CDS |
| `nex_india_subset_download_s3_v1.py` | Download NEX India subsets from S3 |

### `tools/data_prep/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `derive_hurs_from_era5_tas_tdps.py` | Derive relative humidity from ERA5 tas + dew point |
| `organize_era5_legacy_nc_files.py` | Reorganize legacy ERA5 NetCDF files |
| `prepare_reanalysis_for_pipeline.py` | Prepare ERA5/IMD reanalysis data for compute pipeline ingestion |

### `tools/diagnostics/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `debug_build_master.py` | Diagnose master-building issues |
| `spi_diagnostic.py` | SPI output sanity checks and diagnostics |

### `tools/geodata/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `build_district_subbasin_crosswalk.py` | Shared polygon crosswalk builders plus the district ↔ sub-basin CLI |
| `build_block_subbasin_crosswalk.py` | Build canonical block ↔ sub-basin crosswalk CSV |
| `build_district_basin_crosswalk.py` | Build canonical district ↔ basin crosswalk CSV |
| `build_block_basin_crosswalk.py` | Build canonical block ↔ basin crosswalk CSV |
| `clean_river_network.py` | Clean Survey of India river shapefile into canonical GeoParquet + display GeoJSON + QA CSV |
| `build_river_basin_reconciliation.py` | Build the canonical hydro-basin ↔ river-basin reconciliation CSV for river overlays |
| `build_river_subbasin_diagnostics.py` | Build hydro sub-basin vs river-name diagnostics CSV |
| `build_river_topology.py` | Build topology-ready river reaches, nodes, adjacency, and QA artifacts |
| `convert_blocks_shp_to_geojson.py` | Convert block shapefile to standardized GeoJSON |
| `inspect_block_shapefile.py` | Inspect and optionally convert block shapefiles |

### `tools/legacy/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `DONOTUSE_ArtparkGenerateReport.py` | Legacy script kept for reference only |

### `tools/pipeline/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `build_all_csv.ps1` | PowerShell helper for CSV build workflows |
| `build_master_metrics.py` | CLI wrapper around `compute.master_builder` |
| `compute_indices.py` | Older single-process compute pipeline (district/block oriented) |
| `compute_indices_multiprocess.py` | Main multi-process compute pipeline for admin and hydro |

## Test inventory

### Test entrypoint

```bash
python -m pytest -q
```

### Test modules under `tests/`

#### App/UI/runtime
- `test_app_adm2_cache.py`
- `test_app_dashboard_entry.py`
- `test_app_details_panel.py`
- `test_app_map_view_extract.py`
- `test_app_orchestrator_entry.py`
- `test_app_perf.py`
- `test_app_point_selection_ui.py`
- `test_app_portfolio_ui.py`
- `test_app_rankings_view.py`
- `test_app_sidebar_import.py`
- `test_app_state.py`
- `test_app_state_summary_view.py`
- `test_legend_html.py`
- `test_main_app_import.py`
- `test_map_view_layout.py`
- `test_root_main_entrypoint.py`
- `test_scenario_ui_labels.py`
- `test_state_defaults.py`

#### Data, paths, merge, contracts
- `test_available_states.py`
- `test_config.py`
- `test_clean_river_network.py`
- `test_crosswalk_context.py`
- `test_crosswalk_generator.py`
- `test_crosswalk_runtime.py`
- `test_hydro_contracts.py`
- `test_import_boundaries.py`
- `test_imports_smoke.py`
- `test_master_loader.py`
- `test_merge.py`
- `test_metrics_registry.py`
- `test_naming.py`
- `test_paths_resolution.py`
- `test_processed_io_parquet_filters.py`
- `test_prune_legacy_csv.py`
- `test_river_loader.py`
- `test_river_overlay_contract.py`
- `test_river_reconciliation.py`
- `test_river_topology.py`
- `test_timeseries.py`
- `test_timeseries_models.py`

#### Analysis, enrichment, portfolio
- `test_analysis_metrics.py`
- `test_map_enrichment.py`
- `test_portfolio.py`
- `test_portfolio_grouping_helpers.py`
- `test_portfolio_tier1_guards.py`
- `test_portfolio_tier2_manage_helpers.py`
- `test_portfolio_tier3_multistate.py`
- `test_state_profile_trend_band_fallback.py`

#### Compute and legacy parity
- `test_build_master_state_summaries.py`
- `test_compute_indices_synthetic.py`
- `test_compute_indices_synthetic_comprehensive.py`
- `test_legacy_dashboard_map_portfolio_wiring.py`
- `test_legacy_dashboard_portfolio_panel_call.py`
- `test_legacy_dashboard_state_profile_files.py`

#### Visualization
- `test_viz_charts.py`
- `test_viz_colors.py`
- `test_viz_exports.py`
- `test_viz_scenario_yaxis_scaling.py`
- `test_viz_tables.py`
- `test_viz_trend_spaghetti.py`

#### Repo/process guards
- `test_no_emojis.py`

## Docs and supporting repo files

### `docs/`

| File | Purpose |
|------|---------|
| `HANDOFF.md` | Persistent handoff ledger |
| `dead_code_candidate_report.md` | Dead-code analysis notes |
| `functionality_contract.md` | Product/functionality contract notes |
| `manual_smoke_test.md` | Manual smoke-test checklist |
| `module_responsibility_map.md` | Historical module responsibility notes |
| `pytest_baseline_failures.md` | Known/recorded pytest baseline failures |
| `refactor_acceptance.md` | Refactor acceptance criteria/history |

### Other notable root assets

| File | Purpose |
|------|---------|
| `irt_agents_bundle.zip` | Agent-bundle artifact kept at repo root |
| `irt_agents_data_catalog_patch.zip` | Patch/archive artifact |
| `irt_data_catalog_patch.zip` | Patch/archive artifact |
| `spi3_err.log` / `spi3_out.log` / `spi3_tel.log` | Local diagnostic logs |

## Data contracts

### Boundary inputs expected under `IRT_DATA_DIR`

| Artifact | Purpose |
|----------|---------|
| `districts_4326.geojson` | ADM2 district boundaries |
| `blocks_4326.geojson` | ADM3 block boundaries |
| `basins.geojson` | Canonical basin boundaries |
| `subbasins.geojson` | Canonical sub-basin boundaries |
| `district_subbasin_crosswalk.csv` | District ↔ sub-basin overlap registry |
| `block_subbasin_crosswalk.csv` | Block ↔ sub-basin overlap registry |
| `district_basin_crosswalk.csv` | District ↔ basin overlap registry |
| `block_basin_crosswalk.csv` | Block ↔ basin overlap registry |
| `river_network.parquet` | Canonical cleaned river-network line artifact |
| `river_network_display.geojson` | Simplified river-network display artifact |
| `river_network_qa.csv` | Row-level QA flags for the cleaned river network |
| `river_basin_name_reconciliation.csv` | Hydro-basin ↔ river-basin reconciliation registry used by hydro river overlays |
| `river_subbasin_diagnostics.csv` | Hydro sub-basin vs river-name diagnostics registry for sub-basin overlays |
| `river_reaches.parquet` | Topology-ready river reach artifact |
| `river_nodes.parquet` | Topology-ready river node artifact |
| `river_adjacency.parquet` | Reach-to-reach adjacency artifact |
| `river_topology_qa.csv` | QA rows for topology-ready reach artifacts |
| `river_missing_assignments.csv` | Focused diagnostics for reaches still missing basin/sub-basin assignment |
| `river_missing_assignments.geojson` | Visual-debug layer for unresolved river reach assignments |

### Canonical identifier expectations

| Level | Required identifiers |
|------|-----------------------|
| District | `state_name`, `district_name` |
| Block | `state_name`, `district_name`, `block_name` |
| Basin | `basin_id`, `basin_name` |
| Sub-basin | `basin_id`, `basin_name`, `subbasin_id`, `subbasin_code`, `subbasin_name` |

### Processed output layout

#### Admin

```text
processed/{metric_slug}/{state}/
├── master_metrics_by_district.csv
├── master_metrics_by_block.csv
├── state_model_averages_district.csv
├── state_ensemble_stats_district.csv
├── state_yearly_model_averages_district.csv
├── state_yearly_ensemble_stats_district.csv
├── state_model_averages_block.csv
├── state_ensemble_stats_block.csv
├── state_yearly_model_averages_block.csv
├── state_yearly_ensemble_stats_block.csv
├── districts/
└── blocks/
```

Identifier columns:
- district master: `state`, `district`
- block master: `state`, `district`, `block`

#### Hydro

```text
processed/{metric_slug}/hydro/
├── master_metrics_by_basin.csv
├── master_metrics_by_sub_basin.csv
├── basins/
│   ├── {basin}/{model}/{scenario}/{basin}_yearly.csv
│   └── ensembles/{basin}/{scenario}/{basin}_yearly_ensemble.csv
└── sub_basins/
    ├── {basin}/{sub_basin}/{model}/{scenario}/{sub_basin}_yearly.csv
    └── ensembles/{basin}/{sub_basin}/{scenario}/{sub_basin}_yearly_ensemble.csv
```

Identifier columns:
- basin master: `basin_id`, `basin_name`
- sub-basin master: `basin_id`, `basin_name`, `subbasin_id`, `subbasin_code`, `subbasin_name`

### Crosswalk artifact

Current canonical crosswalks:
- `district_subbasin_crosswalk.csv`
- `block_subbasin_crosswalk.csv`
- `district_basin_crosswalk.csv`
- `block_basin_crosswalk.csv`

Required columns:
- `district_name`
- `state_name`
- `subbasin_id`
- `subbasin_name`
- `basin_id`
- `basin_name`
- `intersection_area_km2`
- `district_area_fraction_in_subbasin`
- `subbasin_area_fraction_in_district`

Current behavior:
- district and block details -> basin + sub-basin context
- basin and sub-basin details -> administrative context
- hydro admin-context defaults to districts, with blocks as an optional drill-down
- related-unit map overlays
- admin -> hydro jump
- hydro -> admin jump

Not yet supported:
- weighted transfer across spatial families
- river-network crosswalk/topology layer

### River-network artifact

Current canonical river-network cleaning outputs:
- `river_network.parquet`
- `river_network_display.geojson`
- `river_network_qa.csv`
- `river_basin_name_reconciliation.csv`

Current behavior:
- offline cleaning + QA only
- preserves raw Survey of India fields and adds canonical cleaned columns
- hydro-only display overlay available via `river_network_display.geojson`
- basin-level overlay matching is driven by `river_basin_name_reconciliation.csv`
- sub-basin overlay diagnostics are supported via `river_subbasin_diagnostics.csv`
- topology-ready reach/node/adjacency artifacts are supported offline
- no upstream/downstream routing UI, river crosswalks, or river-based metric computation yet

## Current status vs deferred work

### Implemented now
- Admin family: district and block
- Hydro family: basin and sub-basin
- Hydro compute outputs and hydro master contracts
- Hydro map/rankings/details flows
- Polygon crosswalk context and actionability for district/block ↔ basin/sub-basin
- Hydro-only river display overlay for basin/sub-basin maps

### Deferred
- Weighted admin ↔ hydro translation engine
- Hydro portfolio workflows
- River-network/reach translation layer

Long-lived deferred work and shelved initiatives are tracked in `docs/BACKLOG.md`.

## Contact

For questions about the codebase:
- **Author:** Abu Bakar Siddiqui Thakur
- **Email:** absthakur@resilience.org.in

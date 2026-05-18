# India Resilience Tool (IRT) - Codebase Manifest

## Overview

IRT is a Streamlit-based climate-risk and resilience dashboard organized around two spatial families:

- **Admin**: district, block
- **Hydro**: basin, sub-basin

The current working tree supports:
- a default climate-hazard landing / discovery surface that opens on an India state-level bundle map and drills down India -> state -> district before handing off to the detailed workflow
- a grouped dashboard bundle scope covering exact selector labels like `Thematic - Heat Risk` and `Sector-wise - Health Risk`
- thematic bundles for `Heat Risk`, `Heat Stress`, `Drought Risk`, `Flood & Extreme Rainfall Risk`, `Cold Risk`, and `Agriculture & Growing Conditions`
- sector-wise bundles for `Agricultural Risk`, `Health Risk`, `Industrial Risk`, `Investment / Financial Risk`, `Infrastructure Risk`, `Asset Risk (Thermal Power Plants)`, `Asset Risk (Hydropower Plants)`, and `Life & Livelihood Loss Risk`
- `Life & Livelihood Loss Risk` is available at district and block level when the persisted block proposal bundle master has been built
- declarative landing bundle weights in `india_resilience_tool/config/bundle_weights.py`, now used for all visible Glance bundles
- persisted visible-Glance composite metrics declared in `india_resilience_tool/config/composite_metrics.py` and optimized Glance view-model artifacts built offline from admin master files
- unified Glance `drivers.parquet` artifacts carrying state, district, and optional block-scoped metric/rule drivers for block drawer drill-downs
- Glance Rankings answer/export helpers that emit copyable prose, visible-row CSVs, and Excel answer packs from the same filtered ranking frame shown in the UI
- explicit state-click handling on the India overview map and validated district-click handling within state focus
- type-to-filter geography suggestions in the landing top bar that mirror the map drill-down flow
- a top-right deep-dive `Back to Glance` action that returns to landing mode using a reverse handoff, with Glance -> Deep Dive now opening the matching persisted composite metric
- map, rankings, and details flows for district, block, basin, and sub-basin
- drill-down-only nationwide behavior for the finest-grain views:
  - `Admin -> Block` requires a selected state
  - `Hydro -> Sub-basin` requires a selected basin
- portfolio workflows for district, block, basin, and sub-basin
- assessment-pillar and domain-based metric navigation, separating climate hazards from bio-physical hazards
- static exposure-layer support for admin district/block views
- static groundwater snapshot support for admin district views
- hydro boundary loading and hydro processed-output discovery
- Aqueduct direct district/block masters plus SOI hydro masters for water stress, interannual variability, seasonal variability, and water depletion
- population exposure masters for total population and population density on district/block units
- `population_exposure_2025_raster` display-only overlay support across admin and hydro map levels, backed by exported PNG/metadata artifacts rather than the raw TIFF
- rural facilities exposure masters for total/category counts and per-100k people rates on district/block units
- `rural_facilities_density` display-only overlay support across admin and hydro map levels, backed by exported category PNG/metadata artifacts
- groundwater district masters for extraction stage, future availability, extractable resource, and total extraction
- actionable polygon crosswalk context, navigation, and related-unit highlighting across district/block and basin/sub-basin views
- shared reference overlay framework for the river network (hydro basin/sub-basin views, plus admin district/block views sliced by selected district when the river artifact carries a `district_names_clean` column) and the admin RP-100 flood-depth raster

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
| `python -m tools.runs.prepare_dashboard --help` | Show the canonical dashboard-ready prep command for climate, persisted visible-Glance composites, Aqueduct, population, groundwater, Telangana JRC flood depth, validation, and full package workflows, including level-aware climate readiness, optimized refresh, and final readiness verification |
| `python -m tools.pipeline.build_composite_metrics --help` | Build persisted district/block composite masters for the 6 thematic dashboard bundles under the legacy `processed/` metric layout |
| `python -m tools.pipeline.build_proposal_bundles --help` | Build persisted admin district/block proposal climate-risk bundle masters under `processed/<proposal_composite_slug>/<state>/` and the helper `r95p_interannual_variability` masters |
| `python -m tools.pipeline.build_glance_view_model --help` | Build persisted optimized Glance view-model artifacts under `processed_optimised/context/glance/v1/{composite_slug}/{scenario}/{period}/`; normal dashboard prep gets these through `build_processed_optimised` |
| `python -m tools.optimized.build_processed_optimised --help` | Build the compact `processed_optimised` runtime bundle from the legacy `processed/` tree, with scoped `--overwrite`, optional `--prune-scope`, destructive `--full-rebuild`, `--dry-run`, exact pre-scan task counting, hydro yearly fallback-from-models, optional `--level` filtering, `--workers` overrides, and nested terminal progress bars |
| `python -m tools.optimized.audit_processed_optimised_parity --help` | Audit `processed_optimised` against the dashboard-visible legacy processed contract, with optional `--level` filtering, and write `parity_report.json` |
| `python -m tools.pipeline.build_master_metrics` | Rebuild admin and hydro master CSVs; hydro levels auto-resolve `processed/{metric}/hydro/` |
| `python -m tools.pipeline.compute_indices_multiprocess --help` | Show compute-pipeline options |
| `python -m tools.pipeline.compute_indices_multiprocess --level district --metrics <slug>` | Build district outputs |
| `python -m tools.pipeline.compute_indices_multiprocess --level block --metrics <slug>` | Build block outputs |
| `python -m tools.pipeline.compute_indices_multiprocess --level basin --metrics <slug>` | Build basin outputs |
| `python -m tools.pipeline.compute_indices_multiprocess --level sub_basin --metrics <slug>` | Build sub-basin outputs |
| `python -m tools.pipeline.build_spatial_weights --help` | Build private Heat Risk v2 grid-first spatial-weight caches under `processed/_internal/spatial_weights/`; annual grid-first metric fields are persisted by compute under `processed/_internal/heat_risk/grid_metrics/` |
| `python -m tools.pipeline.compute_indices_multiprocess --level district --metrics spi3_max_spell_lt_minus1` | Build a Drought Risk v2 grid-first duration metric; private annual/period NetCDF caches are persisted under `processed/_internal/drought_risk/grid_metrics/` |
| `python -m tools.subbasin_shp_explore --help` | Inspect/repair/export hydro boundaries |
| `python -m tools.geodata.build_district_subbasin_crosswalk --overwrite` | Build district ↔ sub-basin crosswalk CSV |
| `python -m tools.geodata.build_block_subbasin_crosswalk --overwrite` | Build block ↔ sub-basin crosswalk CSV |
| `python -m tools.geodata.build_district_basin_crosswalk --overwrite` | Build district ↔ basin crosswalk CSV |
| `python -m tools.geodata.build_block_basin_crosswalk --overwrite` | Build block ↔ basin crosswalk CSV |
| `python -m tools.geodata.build_blocks_geojson --overwrite` | Rebuild the canonical block GeoJSON and block-label QA outputs |
| `python -m tools.geodata.build_adm1_geojson --overwrite` | Build the compact optimized ADM1 state-polygons artifact for fast dashboard boot |
| `python -m tools.geodata.prepare_aqueduct_baseline --help` | Build the canonical clean Aqueduct baseline artifact and India-only future geometry subset from future geometry + baseline CSV |
| `python -m tools.geodata.build_aqueduct_admin_crosswalk --help` | Build Aqueduct HydroSHEDS ↔ district overlap CSVs |
| `python -m tools.geodata.build_aqueduct_block_crosswalk --help` | Build Aqueduct HydroSHEDS ↔ block overlap CSVs |
| `python -m tools.geodata.build_aqueduct_admin_masters --help` | Build Aqueduct district/block master CSVs on canonical admin units |
| `python -m tools.geodata.build_aqueduct_hydro_crosswalk --help` | Build Aqueduct HydroSHEDS ↔ SOI basin/sub-basin overlap CSVs |
| `python -m tools.geodata.build_aqueduct_hydro_masters --help` | Build Aqueduct hydro master CSVs on SOI basin/sub-basin units |
| `python -m tools.geodata.build_population_admin_masters --help` | Build district/block population exposure master CSVs plus the display-only population exposure PNG/metadata overlay from the 2025 raster |
| `python -m tools.geodata.build_rural_facilities_admin_masters --help` | Build district/block rural facilities exposure master CSVs plus display-only density PNG/metadata overlays for total/agro/education/health/service categories |
| `python -m tools.geodata.build_built_up_area_admin_masters --help` | Build district/block built-up area exposure master CSVs plus the display-only built-up area PNG/metadata overlay from the cleaned built-surface raster |
| `python -m tools.geodata.build_lulc_admin_masters --help` | Build district/block agricultural LULC exposure master CSVs plus the display-only binary agricultural LULC PNG/metadata overlay |
| `python -m tools.geodata.build_groundwater_district_masters --help` | Build district groundwater assessment master CSVs from the 2024-2025 GEC workbook |
| `python -m tools.geodata.clean_river_network --src <path> --overwrite` | Clean Survey of India river network into canonical river artifacts |
| `python -m tools.geodata.build_river_basin_reconciliation --overwrite` | Build hydro-basin ↔ river-basin reconciliation CSV |
| `python -m tools.geodata.build_river_subbasin_diagnostics --overwrite` | Build hydro sub-basin vs river-name diagnostics CSV |
| `python -m tools.geodata.build_river_topology --overwrite` | Build topology-ready river reaches, nodes, adjacency, and QA artifacts |
| `python -m tools.pipeline.enrich_river_network_districts [--dry-run]` | Spatial-join cleaned river display with districts and rewrite `river_network_display.geojson` in place with a `district_names_clean` column (drives admin-view river overlay; backs up original to `.bak` on first run) |
| `python -m pytest -q` | Run tests |

### Key environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `IRT_PILOT_STATE` | `Telangana` | Default admin state in the UI |
| `IRT_DATA_DIR` | resolved in `paths.py` | Base directory for boundaries, crosswalks, and processed outputs |
| `IRT_PROCESSED_ROOT` | `IRT_DATA_DIR/processed/{metric}` | Optional processed-root override |
| `IRT_PROCESSED_OPTIMISED_ROOT` | `IRT_DATA_DIR/processed_optimised` | Optional optimized runtime-bundle override |
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

Reference overlay contracts:
- `rp100_flood_depth_raster`: display-only RP-100 flood-depth raster overlay backed by `jrc_flood_depth/overlay/rp100_depth_overlay.png` and metadata, with optimized copies under `processed_optimised/context/`.
- The RP-100 overlay render layer may carry `OverlayRenderLayer.legend_html`, derived from static `RP100_FLOOD_DEPTH_BINS`, so the active reference overlay can share the existing map legend column.
- `build_rp100_flood_depth_legend_html()` builds the compact RP-100 legend from those bins; its swatches mirror the exporter `RP100_OVERLAY_COLORS` ramp: transparent `<=0`, then `#d6f0ff`, `#9dd9ff`, `#5bb7f0`, `#2f7fc1`, `#1d4f91`, and `#0f2f5f`.
- `population_exposure_2025_raster`: display-only India-wide population exposure overlay backed by `population/overlay/population_exposure_2025_overlay.png` and `population/overlay/population_exposure_2025_overlay_meta.json`, with optimized copies under `processed_optimised/context/population/overlay/`.
- Population overlay display semantics are binned people per source cell from `ind_pop_2025_CN_1km_R2025A_UA_v1.tif`; the canonical ramp is transparent for `<=0`, then `#fff7bc`, `#fee391`, `#fec44f`, `#fe9929`, `#ec7014`, `#cc4c02`, `#993404`, `#7f1d1d`, and `#4c0519` for values above 10000.
- `built_up_area_current_raster`: display-only India-wide built-up area overlay backed by `built_up_area/overlay/built_up_area_current_overlay.png` and `built_up_area/overlay/built_up_area_current_overlay_meta.json`, with optimized copies under `processed_optimised/context/built_up_area/overlay/`.
- Built-up source contract: operators should place `Cleaned_India_Built_Surface_WGS84.tif` at `IRT_DATA_DIR/built_up_area/`; timestamped download paths may be passed to the builder with `--raster` but are not hard-coded defaults.
- Built-up metric masters are `processed/built_up_area_km2/{state}/master_metrics_by_{district,block}.{csv,parquet}` and `processed/built_up_area_share_pct/{state}/master_metrics_by_{district,block}.{csv,parquet}` with columns `built_up_area_km2__snapshot__Current__mean` and `built_up_area_share_pct__snapshot__Current__mean`.
- Built-up raster values are interpreted as `m2/source cell`; `0` is valid no built-up and `65535` is invalid/background. The builder scans the raster before writes and fails national totals outside `43,000-220,000 km2` unless `--allow-total-outlier` is supplied.
- Built-up tabulation happens in the source raster CRS by reprojecting admin vectors to the raster CRS; the raster is not reprojected for metrics. Edge cells use `all_touched=False`.
- Built-up area share uses full polygon area in `EPSG:6933`: `built_up_m2 / polygon_area_m2 * 100`. QA also records raster-supported area denominators, but those do not replace the canonical geometry-based share.
- Built-up v1 is excluded from composites, proposal bundles, and bundle weights.
- Built-up overlay display semantics are binned `m2/source cell` in an EPSG:3857 PNG; bins are transparent `0`, then `0-100`, `100-500`, `500-1000`, `1000-2500`, `2500-5000`, and `>5000 m2/cell` with colors `#edf8fb`, `#b2e2e2`, `#66c2a4`, `#2ca25f`, `#006d2c`, and `#00441b`.
- `lulc_agri_current_raster`: display-only India-wide agricultural LULC overlay backed by `lulc/overlay/lulc_agri_current_overlay.png` and `lulc/overlay/lulc_agri_current_overlay_meta.json`, with optimized copies under `processed_optimised/context/lulc/overlay/`.
- LULC agriculture source contract: operators should place `LULC_2_Agri.tif` at `IRT_DATA_DIR/lulc/`; alternate paths may be passed to the builder with `--raster` / `--lulc-raster`.
- LULC agriculture metric masters are `processed/lulc_agri_area_km2/{state}/master_metrics_by_{district,block}.{csv,parquet}` and `processed/lulc_agri_share_pct/{state}/master_metrics_by_{district,block}.{csv,parquet}` with columns `lulc_agri_area_km2__snapshot__Current__mean` and `lulc_agri_share_pct__snapshot__Current__mean`.
- LULC raster values are interpreted as binary classes: only `1` is agricultural LULC; `0` is nodata/background and is never treated as an explicit non-agriculture class. The builder scans the raster before writes and rejects values outside `{0, 1}` unless `--allow-unexpected-values` is supplied.
- LULC tabulation reads the source through a nearest-neighbor `EPSG:6933` equal-area `WarpedVRT`; area is `agri_cell_count * abs(transform.a * transform.e)`. Edge cells use `all_touched=False`.
- LULC agriculture share uses full polygon area in `EPSG:6933`: `agri_area_m2 / polygon_area_m2 * 100`. QA records raster extent support area, support coverage, low-coverage flags, and `share_out_of_range`.
- LULC guardrails fail national agriculture totals outside `1,200,000-2,300,000 km2` unless `--allow-total-outlier` is supplied, and fail district/block shares above `100.01%` unless `--allow-share-outlier` is supplied.
- LULC v1 is excluded from composites, proposal bundles, and bundle weights.
- LULC overlay display semantics are binary class colors in an EPSG:3857 PNG: transparent for `0`, `#2ca25f` for `1`.
- `rural_facilities_density`: display-only category-selectable rural facilities density overlay backed by `rural_facilities/overlay/rural_facilities_density_<category>_overlay.png` and metadata, with optimized copies under `processed_optimised/context/rural_facilities/overlay/`. Artifacts are written per real category (`agro`, `education`, `health`, `service`); the `total` UI selection is virtual and is rendered at runtime by stacking the four per-category layers.
- Each real-category PNG uses a distinct single-hue ramp (agro=greens, education=blues, health=reds, service=oranges) so the four layers stay distinguishable when stacked under `total`.
- Rural facilities overlay display semantics are facilities per 1,000 km2 on an EPSG:6933 10 km grid reprojected to EPSG:3857 for Leaflet.
- The Folium map registers per-category panes (`irt-rural-facilities-density-{agro,education,health,service}`); the active overlay layer carries a `legend_html` colorbar that is stacked into the map legend column.
- Dashboard runtime reads exported overlay artifacts only; it does not read source TIFFs.

Aqueduct methodology note:
- `docs/aqueduct_onboarding_methodology.md` is the canonical narrative for Aqueduct cleanup, HydroSHEDS `pfaf_id` normalization, direct `pfaf_id -> district/block` transfer, and HydroSHEDS → SOI hydro transfer.
- that same doc now includes a short reader guide for interpreting the Aqueduct validation bundles under `IRT_DATA_DIR/aqueduct/validation/{metric_slug}/`
- `docs/aqueduct_field_contract.md` records the currently used Aqueduct source-field mappings and interpretation notes for the onboarded Aqueduct district, block, and hydro metrics.
- `docs/command_catalog.md` is the canonical operator-facing command catalog for dashboard prep, Aqueduct, climate hazards, population exposure, groundwater, and validation workflows.

## Package inventory

### `india_resilience_tool/analysis/`

| File | Purpose |
|------|---------|
| `bundle_scores.py` | Streamlit-free landing bundle-score normalization, aggregation, and driver helpers |
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
| `dashboard_bundle_runtime.py` | Runtime helpers for dashboard bundle visibility and composite-source lookup |
| `details_runtime.py` | Right-panel orchestration and data prep for details views |
| `geo_cache.py` | Streamlit-cached admin and hydro geometry loading/builders |
| `geography.py` | Filesystem-backed admin geography discovery helpers |
| `geography_controls.py` | Sidebar geography + analysis-focus controls for admin and hydro |
| `glance_exports.py` | Streamlit-free Glance Rankings answer, CSV, and Excel answer-pack helpers |
| `help_text.py` | Tooltip/help-text helpers for ribbon widgets |
| `landing_runtime.py` | Climate-hazard landing/discovery orchestrator that loads persisted optimized Glance view models only, plus state transitions and Deep Dive handoff |
| `left_panel_runtime.py` | Left-panel orchestration for map vs rankings |
| `main.py` | Package Streamlit entrypoint |
| `map_layer_runtime.py` | Streamlit-free Folium layer construction using cached FeatureCollections |
| `map_pipeline.py` | Merge -> enrich -> colors -> map/rankings pipeline, including fine-grain drill-down guards and rankings-only fast paths |
| `master_cache.py` | Streamlit session-state cache for master CSV + schema loading |
| `master_freshness.py` | Master CSV freshness/rebuild gating helpers |
| `overlays.py` | Shared reference-overlay framework: session keys, visibility/availability, RP-100 artifact validation/discovery, and river/flood render-layer contracts |
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
| `map_view.py` | Render Folium map and extract level-aware click payloads, including landing state clicks |
| `hydro_summary_view.py` | Hydro basin summary panel for basin-wide sub-basin selections |
| `rankings_view.py` | Rankings table rendering and portfolio add flows |
| `state_summary_view.py` | State summary view for admin-focused overview flows |

### `india_resilience_tool/compute/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `composite_metrics.py` | Streamlit-free builders for persisted district/block composite Glance metric masters |
| `glance_view_model.py` | Streamlit-free builder for persisted optimized Glance district/state scores, drivers, attributes, and distributions |
| `proposal_bundles.py` | Streamlit-free builders for persisted proposal climate-risk bundle masters plus the `r95p_interannual_variability` helper masters |
| `master_builder.py` | Build master CSVs, including hydro master enrichment and Parquet companions for runtime serving |
| `spi_adapter.py` | SPI adapter around `climate-indices` |
| `gridfirst_spatial.py` | Shared grid-first spatial overlap and NetCDF/sidecar cache helpers used by Heat Risk v2 and Drought Risk v2 |
| `drought_risk_gridfirst.py` | Drought Risk v2 grid-cell SPI, annual count/spell metrics, period rollups, NaN-aware polygon aggregation, and private cache helpers |

#### `india_resilience_tool/compute/tests/`

| File | Purpose |
|------|---------|
| `test_spi_adapter.py` | SPI adapter tests |

### `india_resilience_tool/config/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `bundle_weights.py` | Declarative landing bundle weights used for all visible Glance bundle score aggregations |
| `composite_metrics.py` | Declarative visible-Glance bundle -> persisted composite metric mapping and helpers |
| `dashboard_bundles.py` | Declarative dashboard bundle catalog: ordering, grouped selector labels, canonical bundle names, and composite-slug mapping |
| `proposal_bundles.py` | Declarative proposal climate-risk bundle specs, exact rule order, and validation helpers for the offline proposal-bundle builder |
| `constants.py` | UI, styling, scenario, and geometry-render constants |
| `metrics_registry.py` | Canonical metric, pillar, and domain registry |
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
| `master_loader.py` | Robust master-table loading, normalization, schema parsing, and Parquet-first runtime preference |
| `optimized_bundle.py` | Path helpers and compact-contract helpers for the `processed_optimised` runtime bundle, including optimized geometry, context, and Glance view-model paths |
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
| `nex_india_subset_download_s3_v1.py` | Download NEX India subsets from S3 (serial; retained as fallback) |
| `nex_india_subset_download_s3_v2.py` | Parallel pan-India NEX-GDDP-CMIP6 downloader. Four-layer design (CLI/config → scope-cached S3 manifest discovery → local skip/verify/quarantine policy → bounded ThreadPoolExecutor with atomic writes and classified retries). Writes to `${out_dir}/${member_dir}/...` with `member_dir` defaulting to `r1i1p1f1_panIndia` — distinct from the compute-pipeline-consumed `r1i1p1f1/` root. `_v1.py` and `download_pan_india_raw.sh` are unchanged; rewiring downstream pipeline tools to consume the pan-India root is a separate future change. |

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
| `build_blocks_geojson.py` | Rebuild the canonical `blocks_4326.geojson` with canonical block identity columns and label QA |
| `build_adm1_geojson.py` | Build `processed_optimised/geometry/admin/adm1.geojson` from canonical district boundaries for ADM1-first dashboard boot |
| `build_block_subbasin_crosswalk.py` | Build canonical block ↔ sub-basin crosswalk CSV |
| `build_district_basin_crosswalk.py` | Build canonical district ↔ basin crosswalk CSV |
| `build_block_basin_crosswalk.py` | Build canonical block ↔ basin crosswalk CSV |
| `prepare_aqueduct_baseline.py` | Build a clean Aqueduct baseline GeoJSON, QA CSV, and India-only `future_annual` GeoJSON with source future attributes preserved |
| `build_aqueduct_admin_crosswalk.py` | Build Aqueduct HydroSHEDS Level 6 ↔ district overlap CSVs in `EPSG:6933` |
| `build_aqueduct_block_crosswalk.py` | Build Aqueduct HydroSHEDS Level 6 ↔ block overlap CSVs in `EPSG:6933` |
| `build_aqueduct_admin_masters.py` | Build `processed/{aqueduct_metric_slug}/{state}/master_metrics_by_{district,block}.{csv,parquet}` from direct Aqueduct admin overlaps |
| `build_aqueduct_hydro_crosswalk.py` | Build Aqueduct HydroSHEDS Level 6 ↔ SOI basin/sub-basin overlap CSVs in `EPSG:6933` |
| `build_aqueduct_hydro_masters.py` | Build `processed/{aqueduct_metric_slug}/hydro/` master `{csv,parquet}` files from Aqueduct overlaps for the onboarded hydro metrics |
| `build_population_admin_masters.py` | Build district/block population total and density master `{csv,parquet}` files from the 2025 population raster plus `population/overlay/population_exposure_2025_overlay.{png,json}` |
| `build_groundwater_district_masters.py` | Build district groundwater assessment master `{csv,parquet}` files from the 2024-2025 GEC workbook plus a canonical district alias QA package |
| `build_jrc_flood_depth_admin_masters.py` | Build Telangana district/block JRC flood-depth master `{csv,parquet}` files using block flooded-cell `p95` and district flooded-area weighting, plus the derived RP100 Flood Severity Index, RP100 flood-extent masters, RP-100 display overlay PNG/metadata, provenance-aware run summary rows, and stable QA CSVs |
| `validate_aqueduct_workflow.py` | Validate Aqueduct cleanup plus direct district/block and SOI hydro transfer outputs for the onboarded Aqueduct metrics |
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
| `build_composite_metrics.py` | CLI wrapper that writes persisted district/block composite masters for the thematic dashboard bundles |
| `build_proposal_bundles.py` | CLI wrapper that writes persisted district/block proposal climate-risk bundle masters and the `r95p_interannual_variability` helper masters |
| `build_master_metrics.py` | CLI wrapper around `compute.master_builder` |
| `compute_indices.py` | Older single-process compute pipeline (district/block oriented) |
| `compute_indices_multiprocess.py` | Main multi-process compute pipeline for admin and hydro |

### `tools/runs/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `prepare_dashboard.py` | Canonical operator entrypoint that orchestrates bundle prep, optimized runtime refresh, and final readiness verification for climate, Aqueduct, population exposure, groundwater, Telangana JRC flood depth, validation, and dashboard-package workflows |

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
- `test_app_geo_cache.py`
- `test_app_geography_controls.py`
- `test_app_landing_runtime.py`
- `test_app_left_panel_runtime.py`
- `test_app_map_layer_runtime.py`
- `test_app_map_pipeline.py`
- `test_app_map_view_extract.py`
- `test_app_orchestrator_entry.py`
- `test_app_perf.py`
- `test_app_point_selection_ui.py`
- `test_app_portfolio_ui.py`
- `test_app_rankings_view.py`
- `test_app_ribbon.py`
- `test_app_runtime_view.py`
- `test_app_sidebar_import.py`
- `test_app_state.py`
- `test_app_state_summary_view.py`
- `test_hydro_summary_view.py`
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
- `test_aqueduct_admin_transfer.py`
- `test_aqueduct_hydro_transfer.py`
- `test_build_blocks_geojson.py`
- `test_crosswalk_context.py`
- `test_crosswalk_generator.py`
- `test_crosswalk_runtime.py`
- `test_groundwater_district_masters.py`
- `test_hydro_contracts.py`
- `test_import_boundaries.py`
- `test_imports_smoke.py`
- `test_jrc_flood_depth_admin_masters.py`
- `test_master_loader.py`
- `test_merge.py`
- `test_metrics_registry.py`
- `test_naming.py`
- `test_optimized_bundle.py`
- `test_paths_resolution.py`
- `test_population_admin_masters.py`
- `test_prepare_aqueduct_baseline.py`
- `test_prepare_dashboard_runner.py`
- `test_processed_io_parquet_filters.py`
- `test_prune_legacy_csv.py`
- `test_river_loader.py`
- `test_river_overlay_contract.py`
- `test_river_reconciliation.py`
- `test_river_topology.py`
- `test_timeseries.py`
- `test_timeseries_models.py`
- `test_timeseries_optimized.py`
- `test_validate_aqueduct_workflow.py`

#### Config and bundles
- `test_bundle_scores.py`
- `test_bundle_weights.py`
- `test_composite_metrics.py`
- `test_dashboard_bundles.py`
- `test_proposal_bundle_builder.py`
- `test_proposal_bundle_config.py`

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
- `test_compute_indices_admin_ensembles.py`
- `test_compute_indices_cold_risk_metrics.py`
- `test_compute_indices_heat_stress_metrics.py`
- `test_compute_indices_marker_validation.py`
- `test_compute_indices_proposal_metrics.py`
- `test_compute_indices_synthetic.py`
- `test_compute_indices_synthetic_comprehensive.py`
- `test_hydro_compute_pipeline.py`
- `test_legacy_dashboard_map_portfolio_wiring.py`
- `test_legacy_dashboard_portfolio_panel_call.py`
- `test_legacy_dashboard_state_profile_files.py`

#### Visualization
- `test_viz_charts.py`
- `test_viz_colors.py`
- `test_viz_exports.py`
- `test_viz_folium_featurecollection.py`
- `test_viz_formatting.py`
- `test_viz_scenario_yaxis_scaling.py`
- `test_viz_tables.py`
- `test_viz_trend_spaghetti.py`

#### Repo/process guards
- `test_no_emojis.py`

## Docs and supporting repo files

### `docs/`

| File | Purpose |
|------|---------|
| `BACKLOG.md` | Long-lived deferred work and shelved initiatives |
| `HANDOFF.md` | Persistent handoff ledger |
| `aqueduct_field_contract.md` | Canonical Aqueduct source-field mappings for onboarded district, block, and hydro metrics |
| `aqueduct_onboarding_methodology.md` | Narrative for Aqueduct cleanup, pfaf_id normalization, direct admin transfer, and HydroSHEDS → SOI hydro transfer |
| `bundle_task_master.md` | Task tracking for bundle-weight methodology migration |
| `climate_risk_indicator_inventory.md` | Working inventory for aligning dashboard taxonomy with proposed climate risk indicators |
| `command_catalog.md` | Canonical operator-facing command catalog for dashboard prep workflows |
| `dead_code_candidate_report.md` | Dead-code analysis notes |
| `functionality_contract.md` | Product/functionality contract notes |
| `manual_smoke_test.md` | Manual smoke-test checklist |
| `module_responsibility_map.md` | Historical module responsibility notes |
| `proposal_bundle_methodology.md` | Methodology narrative for sector climate hazard pressure proposal bundles |
| `pytest_baseline_failures.md` | Known/recorded pytest baseline failures |
| `refactor_acceptance.md` | Refactor acceptance criteria/history |

#### `docs/benchmarks/`

| File | Purpose |
|------|---------|
| `bechmark_iith_groundwater_dashboard.md` | Benchmark review comparing IITH groundwater dashboard against IRT |

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
| `jrc_flood_depth/overlay/rp100_depth_overlay.png` | Canonical RP-100 flood-depth display overlay exported from `RP100_depth.tif` |
| `jrc_flood_depth/overlay/rp100_depth_overlay_meta.json` | RP-100 overlay metadata with Web Mercator image CRS, EPSG:4326 bounds, source maximum, and fixed display scale |

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

### Reference overlays

River and RP-100 flood-depth references share `india_resilience_tool/app/overlays.py`.
The RP-100 overlay is display-only: the builder exports `rp100_depth_overlay.png`
and `rp100_depth_overlay_meta.json` from `RP100_depth.tif`; dashboard runtime reads
only those artifacts. The PNG is exported in EPSG:3857 for Leaflet ImageOverlay
alignment, while metadata stores EPSG:4326 bounds for placement. The display
scale is fixed at `0.0-10.0 m`; `depth <= 0.0` is transparent,
`0.0 < depth <= 0.5` is `#d6f0ff`, `0.5 < depth <= 1.0` is
`#9dd9ff`, `1.0 < depth <= 2.0` is `#5bb7f0`, `2.0 < depth <= 4.0` is
`#2f7fc1`, `4.0 < depth <= 7.0` is `#1d4f91`, and `depth > 7.0` is `#0f2f5f`.

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
### `tools/optimized/`

| File | Purpose |
|------|---------|
| `__init__.py` | Package marker |
| `build_processed_optimised.py` | Build the minimized `processed_optimised` runtime bundle from legacy processed outputs plus current canonical geometry/context artifacts, including admin/hydro yearly parity outputs, hydro yearly fallback-from-models, selector-index artifacts, persisted geometry `area_m2`, optional level filtering, and a post-build parity audit |
| `audit_processed_optimised_parity.py` | Audit the optimized runtime bundle against the legacy processed contract, with optional level filtering, and emit a parity report |

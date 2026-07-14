# India Resilience Tool (IRT)

The India Resilience Tool is a Streamlit dashboard for exploring climate-risk metrics across admin geographies:

- **Admin**: district and block

IRT combines processed climate-model outputs, boundary layers, rankings, trends, and details views into a single exploration workflow. The current dashboard supports admin map/rankings/details flows, while retaining hydrology context and related basin/sub-basin highlighting for district/block units.

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-1.51.0-red.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## Current capabilities

### Core dashboard
- Default landing discovery surface:
  - includes a top-level `Read the Docs` view that renders the Technical Guidance Note inside the dashboard without triggering dashboard compute
  - launches into an India state-level climate-hazard screening map
  - defaults to the `Heat Risk` bundle under `SSP5-8.5`, `2040-2060`
  - surfaces one grouped bundle list in Glance and Deep Dive with exact selector labels such as `Thematic - Heat Risk` and `Sector-wise - Health Risk`
  - thematic bundles remain available for `Heat Risk`, `Heat Stress`, `Drought Risk`, `Flood & Extreme Rainfall Risk`, and `Cold Risk`
  - sector-wise bundles are available for `Agricultural Risk`, `Health Risk`, `Industrial Risk`, `Investment / Financial Risk`, `Infrastructure Risk`, `Asset Risk (Thermal Power Plants)`, `Asset Risk (Hydropower Plants)`, and `Life & Livelihood Loss Risk`
  - `Agricultural Risk` is the survivor agriculture bundle; the former `Agriculture & Growing Conditions` thematic bundle is retired and treated as a legacy alias to `Agricultural Risk`
  - `Life & Livelihood Loss Risk` is available at district and block level when the persisted block proposal bundle master has been built
  - each visible Glance bundle now reads a persisted optimized Glance view model from disk; the dashboard no longer computes scores, ranks, bands, drivers, attributes, or distributions at runtime
  - `Deep Dive` from Glance opens the matching persisted composite metric such as `Composite Heat Stress`
  - supports India -> state -> district drill-down before entering Deep Dive
  - block drill-down drawers use block-scoped persisted driver rows when available, with a clearly labeled parent-district fallback for older Glance artifacts or bundles without block driver inputs
  - Glance Rankings include an Answer & Export section that generates copyable prose, downloads the currently visible ranking rows as CSV, and downloads an Excel answer pack with ranking, drivers, metadata, and method notes
  - uses explicit state clicks at India overview and district clicks within the selected state
  - top-bar geography search provides type-to-filter state and district suggestions
  - Deep Dive preserves current bundle, scenario-period, and geography and opens the existing detailed workflow
- Level selector: `District` / `Block`
- Ribbon-driven metric selection:
  - assessment pillar
  - domain
  - metric
  - scenario
  - period
  - statistic
  - map mode
- Top-level taxonomy:
  - `Climate Hazards` for climate-model-derived heat, cold, rainfall, flood, drought, and variability layers
  - `Bio-physical Hazards` for externally sourced physical hazard layers such as Aqueduct and groundwater assessment
  - `Exposure` for static exposure layers such as population
- Groundwater status onboarding:
  - district-only metrics from the 2024-2025 GEC workbook
  - Stage of Ground Water Extraction
  - Net Annual Ground Water Availability for Future Use
  - Annual Extractable Ground Water Resource
  - Ground Water Extraction for All Uses
  - fixed snapshot semantics: `snapshot`, `2024-2025`
- Population exposure onboarding:
  - Total Population on district and block units
  - Population Density on district and block units
  - fixed snapshot semantics: `snapshot`, `2025`
  - optional `Reference overlays` sidebar section across admin and hydro views can display a binned `Population exposure (2025)` raster overlay
  - the dashboard runtime reads only exported population overlay PNG/metadata artifacts, not the raw population TIFF
- Rural facilities exposure onboarding:
  - Total, agro, education, health, and service rural facility counts on district and block units
  - matching per-100k people rates using the 2025 population denominator masters
  - fixed snapshot semantics: `snapshot`, `2019-2021`
  - optional `Reference overlays` sidebar section across admin and hydro views can display category-selectable rural facilities density artifacts
- Built-up area exposure onboarding:
  - Built-up Area and Built-up Area Share on district and block units
  - fixed snapshot semantics: `snapshot`, `Current`
  - source raster contract: `IRT_DATA_DIR/built_up_area/Cleaned_India_Built_Surface_WGS84.tif`
  - optional `Reference overlays` sidebar section across admin and hydro views can display the display-only built-up area raster overlay
  - the dashboard runtime reads only exported built-up overlay PNG/metadata artifacts, not the raw TIFF
- Agricultural LULC exposure onboarding:
  - Agricultural LULC Area and Agricultural LULC Share on district and block units
  - fixed snapshot semantics: `snapshot`, `Current`
  - source raster contract: `IRT_DATA_DIR/lulc/LULC_2_Agri.tif`
  - optional `Reference overlays` sidebar section across admin and hydro views can display the display-only agricultural LULC raster overlay
  - the dashboard runtime reads only exported LULC overlay PNG/metadata artifacts, not the raw TIFF
- JRC flood-depth onboarding:
  - state-scoped district and block metrics under `Bio-physical Hazards -> Flood Inundation Depth (JRC)` for any state whose JRC masters have been built and published
  - RP-100 can be rebuilt from a strict `source_manifest.json` that points to aligned 3-arc-second `RP100_depth.vrt` and `RP100_tile_coverage.vrt` sources; strict mode keeps missing source coverage as no-data while treating covered `-9999`/zero/non-positive depth as dry support
  - derived `Flood Severity Index (RP-100)` persisted from RP-100 depth plus RP-100 extent using a fixed severity matrix
  - derived `RP-100 Flood Extent` persisted from the RP-100 depth layer as the share of total polygon area covered by positive depth
  - `RP-10 Flood Depth`, `RP-50 Flood Depth`, `RP-100 Flood Depth`, `RP-500 Flood Depth`
  - fixed snapshot semantics: `snapshot`, `Current`
  - block depth values use flooded-cell `p95` depth; district depth values use flooded-area-weighted means of child block flooded-cell `p95` depths
  - extent is stored as a `0-1` fraction, displayed as a percent, and uses total-polygon-area semantics at both block and district levels
  - optional `Reference overlays` sidebar section in admin district/block views can display the RP-100 flood-depth raster as a display-only overlay
  - the dashboard runtime reads only exported overlay artifacts, not the raw `RP100_depth.tif`
- Water-risk Aqueduct onboarding:
  - Aqueduct water stress on district and block units
  - Aqueduct interannual variability on district and block units
  - Aqueduct seasonal variability on district and block units
  - Aqueduct water depletion on district and block units
  - native Aqueduct scenarios: `historical`, `bau`, `opt`, `pes`
  - the offline Aqueduct SOI basin/sub-basin masters are no longer surfaced in the dashboard (see "Hydro context support")
- Map view and rankings table for district and block levels
- Fine-grain performance guards:
  - `Admin -> Block` requires a selected state before rendering map or rankings
  - nationwide overview remains available at `District`
- Right-side details panel with:
  - risk or metric summary
  - trend over time (when yearly source files exist)
  - scenario comparison (when the metric supports it)
  - case-study export for admin single-unit flows

### Portfolio support
- Implemented for **district** and **block**

### Hydro context support

The navigable Hydro spatial family (basin/sub-basin map, rankings, details, and portfolio views) has been removed; the dashboard is now **admin-only** (district and block). Hydrology is retained purely as *context* for admin district/block units — a Hydrological Context card, a dominant-basin outline overlay, an optional river overlay, and polygon crosswalk context. The offline basin/sub-basin climate compute pipeline and its `master_metrics_by_basin.csv` / `master_metrics_by_sub_basin.csv` hydro masters have likewise been retired.

- Canonical hydro boundaries (drive the admin basin-outline overlay and crosswalk context):
  - `basins.geojson`
  - `subbasins.geojson`
- Optional river overlay:
  - uses `river_network_display.geojson`
  - available in admin district/block views when a district is selected and the river artifact carries district attribution
  - exposed through the shared `Reference overlays` sidebar section and off by default
- Offline river topology artifacts:
  - `river_reaches.parquet`
  - `river_nodes.parquet`
  - `river_adjacency.parquet`
  - `river_topology_qa.csv`
  - `river_missing_assignments.csv`
  - `river_missing_assignments.geojson`
  - hydro details can use `river_reaches.parquet` for a compact river summary when present

### Crosswalk support
- Canonical crosswalk artifacts:
  - `district_subbasin_crosswalk.csv`
  - `block_subbasin_crosswalk.csv`
  - `district_basin_crosswalk.csv`
  - `block_basin_crosswalk.csv`
- Current dashboard use:
  - district and block details show **Basin context** and **Hydrology context**
  - related-unit highlight overlay on the map
  - hydro admin-context drill-down defaults to districts, with blocks available as an optional drill-down

### Explicitly not implemented yet
- Weighted admin ↔ hydro metric transfer
- River-network crosswalks or topology-aware routing

Long-lived deferred work and shelved follow-ups are tracked in `docs/BACKLOG.md`.

## Quick start

### Prerequisites
- Python 3.10+
- Conda
- Boundary files and processed climate outputs in `IRT_DATA_DIR`

### Installation

```bash
git clone https://github.com/thakurabs/india_resilience_tool.git
cd india_resilience_tool
conda env create -f environment.yml
conda activate irt
```

`pip` / `venv` installs are not supported for this repo; the geospatial stack is expected to come from `conda-forge`.
Keep Arrow/Parquet packages (`pyarrow`, `pyarrow-core`, `libarrow*`) installed by conda, not pip. Mixing pip `pyarrow` with conda GDAL/Arrow DLLs can break Streamlit components and Parquet reads on Windows.

Recommended post-install smoke check:

```bash
python -c "from pyproj import CRS; print(CRS('EPSG:4326'))"
python -c "import pyarrow as pa; import altair, jsonschema; print('pyarrow', pa.__version__, 'altair', altair.__version__)"
python -c "from climate_indices import compute, indices; print('climate-indices OK')"
```

### Run the dashboard

```bash
streamlit run main.py
```

Alternative entrypoint:

```bash
streamlit run india_resilience_tool/app/main.py
```

Open: `http://localhost:8501`

Proposal bundle builder:

```bash
python -m tools.pipeline.build_proposal_bundles --help
```

This offline builder computes the proposal climate-risk bundles for admin `district` and `block` units only. It writes persisted proposal bundle masters under `IRT_DATA_DIR/processed/<composite_slug>/<state>/`.
The dashboard surfaces those sector-wise proposal composites through grouped labels like `Sector-wise - Health Risk`, including district and block views for `Life & Livelihood Loss Risk` when its persisted block proposal bundle master is present.
All 8 sector-wise proposal bundles use lens-decomposed rules with explicit weights and a 0.70 `available_rule_weight_fraction` gate. Infrastructure Risk follows `docs/lens_scoring_methodology.md` §9 with explicit rainfall/heat rule weights across `rx1day_ge_200`, `rx5day_ge_400`, and `txx_ge_45`, while keeping the `rx5day_ge_400` slug pending a later data-contract rename. Agricultural Risk follows §12, Investment / Financial Risk follows §8 with five blended rules and an impact-free R99p regime signal, and Life & Livelihood Loss Risk follows §13 with HIGH-confidence Rx1day, MEDIUM-confidence CDD and WSDI (IMD / ICAR / mortality-literature anchored), and the reused Industrial §7.2 Rx5day band — `cdd_ge_40` and `wsdi_ge_5` slugs retained pending a later data-contract rename. Asset Risk (Thermal Power Plants) follows §10 (CHG-0057/0058) with `cdd_ge_30` (band 30-90 days), `txx_ge_45` (band 40-45 degC), and `spi3_low_flow_proxy_norm` as an absolute+change low-flow proxy with no impact band; public slugs are retained pending the separate data-contract rename track. Asset Risk (Hydropower Plants) follows §11 (CHG-0036/0059) with `rx5day_ge_500` (band 250-500 mm/5 days) and `cdd_ge_60` (band 30-90 days) plus the helper-derived `r95p_interannual_variability_norm` regime rule (absolute+change, no impact band); the `r95p_interannual_variability` helper now emits canonical `r95p_interannual_variability__historical__1990-2010__mean` values, with legacy source-master epochs tolerated only as fallback, so its change lens is operational, and its code slugs plus the grid-first / CV-vs-sigma helper provenance caveat remain deferred under CHG-0024.

Heat Risk v2 spatial-weight cache builder:

```bash
python -m tools.pipeline.build_spatial_weights --help
```

This optional offline builder writes private grid-first area-overlap caches under `IRT_DATA_DIR/processed/_internal/spatial_weights/`. The climate compute pipeline can build missing caches on demand; dashboard runtime does not import `exactextract`. The builder now resolves default sample NetCDF and boundary inputs from the effective `--data-dir`, skips valid existing caches, and requires `--overwrite` before replacing a stale cache.

Heat Risk v2 compute also persists private annual per-cell metric fields under `IRT_DATA_DIR/processed/_internal/heat_risk/grid_metrics/<metric>/<model>/<scenario>/<year>.nc` with JSON sidecars. District/block use grid-first cellwise compute for `tas_annual_mean`, `tasmax_summer_mean`, `tas_summer_mean`, `txge30_hot_days`, and `tasmin_tropical_nights_gt25` in addition to the earlier percentile/heatwave slugs. These files are inspection and cache artifacts only; public dashboard CSV paths, slugs, units, and columns remain unchanged.

Heat Stress v2 keeps the `composite_heat_stress` slug but scores only eight retained metrics. Six Heat Stress-only metrics (`twb_annual_mean`, `twb_summer_mean`, `twb_annual_max`, `twb_days_ge_28`, `twb_days_ge_30`, and `tasmin_tropical_nights_gt28`) are computed grid-first for admin district/block outputs and cached under `IRT_DATA_DIR/processed/_internal/heat_stress/grid_metrics/<metric>/<model>/<grid_id>/<scenario>/<year>.nc`. TN90p and WSDI continue to reuse the Heat Risk v2 grid-first percentile path. WBD, WBGT, and simplified WBGT metrics remain registered legacy diagnostics but are no longer Heat Stress domain or scoring members.

Drought Risk v2 uses `climate-indices==2.2.0` SPI on monthly precipitation totals computed from daily `pr` grids, then derives grid-first admin district/block metrics before polygon aggregation. The active Drought bundle still uses the six event-count and max-spell metrics; `spi3_count_months_lt_minus1` is now also grid-first for admin district/block levels and uses explicit `period_mean` / coverage floors in the registry. Private Drought caches live under `IRT_DATA_DIR/processed/_internal/drought_risk/`; the public processed CSV and optimized bundle contracts remain unchanged.

Extreme Rainfall | Flash Flood Risk v2 computes the active six-metric admin bundle on climate grid cells first for district/block outputs, then applies area-weighted polygon aggregation with retained-cell coverage guards. The proposal-only `r99p_extreme_wet_precip` slug now reuses the same admin grid-first percentile infrastructure with a locked `1990-2010` baseline, `linear` quantile method, and strict `>` exceedance. Private annual grids and percentile-threshold caches live under `IRT_DATA_DIR/processed/_internal/extreme_rainfall/`; public slugs, CSV paths, metric columns, composite weights, and optimized artifacts remain unchanged.

CHG-0038 does not change `jrc_flood_depth_index_rp100` or `r95p_interannual_variability`; both remain out of scope and on their existing behavior.

Launch behavior:
- the app now opens into a climate-hazard discovery landing surface by default
- the landing search bar filters state and district suggestions as you type
- the Deep Dive screen includes a top-right `Back to Glance` action
- `Back to Glance` restores the current climate/admin district context when compatible, and otherwise reopens the last stored glance context
- use `Deep Dive` from the landing page to enter the existing detailed ribbon/sidebar workflow on the persisted composite metric for the active Glance bundle

Performance note:
- the dashboard now reads the compact `processed_optimised/` runtime bundle by default
- runtime tables in that bundle are Parquet-only
- the legacy `processed/` tree is kept as a migration/source workspace and is not modified by the optimized-bundle build

Optimized runtime bundle:

```bash
python -m tools.optimized.build_processed_optimised
```

This builds `IRT_DATA_DIR/processed_optimised/` from the existing legacy `IRT_DATA_DIR/processed/` tree and current canonical geometry/context files. The optimized bundle retains:
- yearly time-series
- per-model yearly overlays
- case-study export inputs
- compact ADM1 state polygons at `processed_optimised/geometry/admin/adm1.geojson` for ADM1-first dashboard boot
- simplified runtime geometry shards plus a compact selector index for block dropdowns
- reference overlay context files, including river display artifacts and exposure/hazard overlay PNG/metadata when present
- Glance view-model artifacts under `processed_optimised/context/glance/v1/{composite_slug}/{scenario}/{period}/`

Glance artifacts can also be built directly for debugging:

```bash
python -m tools.pipeline.build_glance_view_model --help
```

The normal operator sequence is: build climate/proposal/exposure/JRC masters, run `python -m tools.optimized.build_processed_optimised`, rebuild lightweight context summaries when exposure inputs changed, then deploy the dashboard. Streamlit reads the persisted Glance view-model Parquet files only; formatting or scoring changes require rebuilding those artifacts.

JRC RP-100 reference overlay artifacts:
- the existing JRC build command also exports `IRT_DATA_DIR/jrc_flood_depth/overlay/rp100_depth_overlay.png`
- metadata is written to `IRT_DATA_DIR/jrc_flood_depth/overlay/rp100_depth_overlay_meta.json`
- the overlay is a visualization reference only; metrics, rankings, legends, and details continue to use the persisted master tables
- dashboard runtime never reads `RP100_depth.tif`

Population exposure reference overlay artifacts:
- the population build command also exports `IRT_DATA_DIR/population/overlay/population_exposure_2025_overlay.png`
- metadata is written to `IRT_DATA_DIR/population/overlay/population_exposure_2025_overlay_meta.json`
- optimized runtime copies live under `IRT_DATA_DIR/processed_optimised/context/population/overlay/`
- the overlay is a display-only binned people-per-source-cell reference; population metrics, rankings, legends, and details continue to use the persisted master tables
- dashboard runtime never reads `ind_pop_2025_CN_1km_R2025A_UA_v1.tif`

Rural facilities density reference overlay artifacts:
- the rural facilities build command exports five category PNG/metadata pairs under `IRT_DATA_DIR/rural_facilities/overlay/`
- optimized runtime copies live under `IRT_DATA_DIR/processed_optimised/context/rural_facilities/overlay/`
- the overlay is a display-only density reference in facilities per 1,000 km2; rural facilities metrics, rankings, legends, and details continue to use persisted master tables

Built-up area reference overlay artifacts:
- place the canonical TIFF at `IRT_DATA_DIR/built_up_area/Cleaned_India_Built_Surface_WGS84.tif`, or pass a timestamped Drive download path with `--raster` / `--built-up-raster`
- the built-up build command exports `IRT_DATA_DIR/built_up_area/overlay/built_up_area_current_overlay.png`
- metadata is written to `IRT_DATA_DIR/built_up_area/overlay/built_up_area_current_overlay_meta.json`
- optimized runtime copies live under `IRT_DATA_DIR/processed_optimised/context/built_up_area/overlay/`
- the overlay is a display-only binned `m2/source cell` reference; built-up metrics, rankings, legends, and details continue to use persisted master tables
- dashboard runtime never reads `Cleaned_India_Built_Surface_WGS84.tif`

Built-up area operator sequence:

```bash
python -m tools.geodata.build_built_up_area_admin_masters --help
python -m tools.runs.prepare_dashboard built-up-area --built-up-raster "<path-to-Cleaned_India_Built_Surface_WGS84.tif>" --plan-only
```

Agricultural LULC reference overlay artifacts:
- place the canonical TIFF at `IRT_DATA_DIR/lulc/LULC_2_Agri.tif`, or pass a source path with `--raster` / `--lulc-raster`
- the LULC build command exports `IRT_DATA_DIR/lulc/overlay/lulc_agri_current_overlay.png`
- metadata is written to `IRT_DATA_DIR/lulc/overlay/lulc_agri_current_overlay_meta.json`
- optimized runtime copies live under `IRT_DATA_DIR/processed_optimised/context/lulc/overlay/`
- the overlay is a display-only binary-class reference; LULC metrics, rankings, legends, and details continue to use persisted master tables
- dashboard runtime never reads `LULC_2_Agri.tif`

Agricultural LULC operator sequence:

```bash
python -m tools.geodata.build_lulc_admin_masters --help
python -m tools.runs.prepare_dashboard lulc --lulc-raster "<path-to-LULC_2_Agri.tif>" --plan-only
```

By default, the builder now parallelizes yearly-model and yearly-ensemble stages using roughly `80%` of available logical CPUs. Use `--workers 1` to force serial execution, or pass an explicit worker count when you want tighter control.

Block yearly model-member recovery:
- climate compute markers use schema version 5; ensemble markers use schema version 4
- yearly-cleanup-policy default keeps legacy behavior: block per-model yearly CSVs are deleted after ensemble generation, while district yearly CSVs are preserved
- yearly-cleanup-policy preserve keeps block per-model yearly CSVs so optimized yearly_models/admin/block can be rebuilt for model-member trend traces
- yearly-cleanup-policy delete_after_ensemble is accepted only with level block
- preserving block yearly CSVs can require substantial disk space; run a one-metric pilot before a full state recovery

Telangana block recovery pilot:
python -m tools.pipeline.compute_indices_multiprocess --state Telangana --level block --overwrite --yearly-cleanup-policy preserve --metrics tas_annual_mean
python -m tools.optimized.build_processed_optimised --state Telangana --level block --overwrite --prune-scope --skip-geometry --skip-context --metric tas_annual_mean

For a full Telangana block recovery, list optimized yearly metrics and pass the emitted flags to compute and optimized rebuild commands:
python -m tools.diagnostics.list_optimized_yearly_metrics --state Telangana --level block --format args
python -m tools.optimized.audit_processed_optimised_parity --state Telangana --level block --require-block-yearly-models --strict --report-path D:/projects/irt_data/processed_optimised/parity_report_telangana_block_yearly_models.json

The optimized builder also supports level-filtered refreshes and state-scoped admin refreshes:

```bash
python -m tools.optimized.build_processed_optimised --overwrite --prune-scope --level block --metric tas_annual_mean
python -m tools.optimized.build_processed_optimised --level district --metric tas_annual_mean
python -m tools.optimized.build_processed_optimised --metric tas_annual_mean --workers 4 --skip-geometry --skip-context --skip-audit
python -m tools.optimized.build_processed_optimised --metric txx_annual_max --level district --state Telangana --overwrite --prune-scope
```

Flag semantics:
- default runs rewrite planned outputs in place and preserve unrelated bundle contents
- `--overwrite` forces rewrite of the selected targets only
- `--overwrite --prune-scope` deletes only the exact selected output files before rewriting; unrelated files in the same directories are preserved
- `--full-rebuild` is the destructive whole-bundle reset
- `--dry-run` prints the resolved write/delete plan without mutating `processed_optimised/`
- `--state <name>` scopes admin district/block work to the resolved legacy state roots while preserving the discovered root names in output paths
- state-scoped runs leave shared-global admin artifacts such as `adm1.geojson`, `admin_block_index.parquet`, Glance outputs, `bundle_manifest.json`, and the global `parity_report.json` untouched by default
- `--include-shared-admin-artifacts` opt-in rebuilds shared-global admin artifacts during a scoped run
- state-scoped audit output is written only when `--report-path <path>` is supplied

while dropping duplicate runtime fields such as:
- `std`
- `p05`
- `p95`
- `n_models`
- `values_per_model`

When run in an interactive terminal, the builder now performs an exact pre-scan and shows nested `tqdm` progress bars for:
- total tasks across the full run
- the current active stage

Use `--no-progress` to suppress the progress bars.

The dashboard prefers optimized runtime assets when they are present:
- Parquet masters and yearly facts from `processed_optimised/metrics/...`
- simplified state-scoped geometry from `processed_optimised/geometry/...`
- compact selector metadata from `processed_optimised/context/admin_block_index.parquet`

Optimized geometry outputs also persist `area_m2`, which the summary views reuse instead of recomputing geodesic area weights on every render.

Persisted visible-Glance composite metrics:

```bash
python -m tools.pipeline.build_composite_metrics --help
```

This writes admin-only district and block composite masters for the 6 visible Glance bundles under:
- `IRT_DATA_DIR/processed/<composite_slug>/<state>/master_metrics_by_district.csv`
- `IRT_DATA_DIR/processed/<composite_slug>/<state>/master_metrics_by_block.csv`

The canonical prep runner now schedules this composite step automatically after climate master builds and before optimized runtime refresh for admin climate runs.

Parity audit:

```bash
python -m tools.optimized.audit_processed_optimised_parity
```

This validates that every dashboard-visible optimized artifact expected from the legacy `processed/` tree is present under `processed_optimised/` and writes `parity_report.json` into the optimized bundle root for unscoped runs.

The parity audit also supports level-filtered checks:

```bash
python -m tools.optimized.audit_processed_optimised_parity --level block --metric tas_annual_mean
python -m tools.optimized.audit_processed_optimised_parity --level district --state Telangana --report-path IRT_DATA_DIR/processed_optimised/parity_report__admin__Telangana.json
```

## Data setup

IRT reads from `DATA_DIR` in `paths.py`, or from `IRT_DATA_DIR` if the environment variable is set.

### Boundary and crosswalk inputs

Place these in `IRT_DATA_DIR`:

- `districts_4326.geojson`
- `blocks_4326.geojson`
- `basins.geojson`
- `subbasins.geojson`
- `district_subbasin_crosswalk.csv` (optional but required for district/sub-basin context/actions)
- `block_subbasin_crosswalk.csv` (optional but required for block/sub-basin context/actions)
- `district_basin_crosswalk.csv` (optional but required for district/basin context/actions)
- `block_basin_crosswalk.csv` (optional but required for block/basin context/actions)
- `river_network.parquet` (optional canonical cleaned river artifact; not yet used by the dashboard runtime)
- `river_network_display.geojson` (optional derived display artifact for inspection)
- `river_network_qa.csv` (optional QA artifact from river cleaning)
- `river_reaches.parquet` (optional topology-ready reach artifact)
- `river_nodes.parquet` (optional topology-ready node artifact)
- `river_adjacency.parquet` (optional topology-ready reach adjacency artifact)
- `river_topology_qa.csv` (optional topology QA artifact)
- `river_missing_assignments.csv` (optional focused diagnostics for unresolved river hydro assignments)
- `river_missing_assignments.geojson` (optional visual-debug layer for unresolved river hydro assignments)
- `aqueduct/baseline_clean_india.geojson` (optional canonical Aqueduct baseline artifact for onboarding, derived from clean `future_annual` geometry + aggregated baseline CSV metrics)
- `aqueduct/baseline_clean_india_qa.csv` (optional QA diagnostics for the clean Aqueduct baseline artifact)
- `aqueduct/future_annual_india.geojson` (optional India-only Aqueduct `future_annual` geometry subset keyed by `pfaf_id`)
- `aqueduct/aqueduct_basin_crosswalk.csv` (optional Aqueduct HydroSHEDS ↔ SOI basin overlap table)
- `aqueduct/aqueduct_subbasin_crosswalk.csv` (optional Aqueduct HydroSHEDS ↔ SOI sub-basin overlap table)
- `aqueduct/aqueduct_district_crosswalk.csv` (optional Aqueduct HydroSHEDS ↔ district overlap table for direct admin transfer)
- `aqueduct/aqueduct_block_crosswalk.csv` (optional Aqueduct HydroSHEDS ↔ block overlap table for direct admin transfer)
- `aqueduct/aq_water_stress_basin_master_qa.csv` (optional QA for the Aqueduct basin master build)
- `aqueduct/aq_water_stress_subbasin_master_qa.csv` (optional QA for the Aqueduct sub-basin master build)
- `aqueduct/aq_water_stress_district_master_qa.csv` (optional QA for the Aqueduct district master build)
- `aqueduct/aq_water_stress_block_master_qa.csv` (optional QA for the Aqueduct block master build)
- `aqueduct/aq_interannual_variability_basin_master_qa.csv` (optional QA for Aqueduct interannual-variability basin masters)
- `aqueduct/aq_interannual_variability_subbasin_master_qa.csv` (optional QA for Aqueduct interannual-variability sub-basin masters)
- `aqueduct/aq_interannual_variability_district_master_qa.csv` (optional QA for Aqueduct interannual-variability district masters)
- `aqueduct/aq_interannual_variability_block_master_qa.csv` (optional QA for Aqueduct interannual-variability block masters)
- `aqueduct/aq_seasonal_variability_basin_master_qa.csv` (optional QA for Aqueduct seasonal-variability basin masters)
- `aqueduct/aq_seasonal_variability_subbasin_master_qa.csv` (optional QA for Aqueduct seasonal-variability sub-basin masters)
- `aqueduct/aq_seasonal_variability_district_master_qa.csv` (optional QA for Aqueduct seasonal-variability district masters)
- `aqueduct/aq_seasonal_variability_block_master_qa.csv` (optional QA for Aqueduct seasonal-variability block masters)
- `aqueduct/aq_water_depletion_basin_master_qa.csv` (optional QA for Aqueduct water-depletion basin masters)
- `aqueduct/aq_water_depletion_subbasin_master_qa.csv` (optional QA for Aqueduct water-depletion sub-basin masters)
- `aqueduct/aq_water_depletion_district_master_qa.csv` (optional QA for Aqueduct water-depletion district masters)
- `aqueduct/aq_water_depletion_block_master_qa.csv` (optional QA for Aqueduct water-depletion block masters)
- `population-*/population/ind_pop_2025_CN_1km_R2025A_UA_v1.tif` (optional source raster for population exposure onboarding)
- `population/overlay/population_exposure_2025_overlay.png` (optional display-only population exposure overlay)
- `population/overlay/population_exposure_2025_overlay_meta.json` (optional population overlay metadata)
- `population/population_district_master_qa.csv` (optional QA for district population masters)
- `population/population_block_master_qa.csv` (optional QA for block population masters)
- `population/population_district_vs_blocks_qa.csv` (optional district vs sum(blocks) consistency QA)
- `population/population_national_summary.csv` (optional national raster-vs-admin population summary)
- `CentralReport1773820094787.xlsx` (optional source workbook for groundwater district onboarding)
- `groundwater/groundwater_summary.csv` (optional QA summary for groundwater onboarding)
- `groundwater/groundwater_source_extract.csv` (optional normalized groundwater source extract)
- `groundwater/groundwater_district_crosswalk.csv` (optional groundwater source-to-canonical district mapping)
- `groundwater/groundwater_unmatched_districts.csv` (optional QA for unmatched groundwater districts)
- `groundwater/groundwater_district_alias_template.csv` (optional template for manual groundwater district alias curation)

All boundary GeoJSONs are expected in `EPSG:4326`.

Aqueduct methodology note:

- See [`docs/aqueduct_onboarding_methodology.md`](docs/aqueduct_onboarding_methodology.md) for the full post-processing workflow, including `pfaf_id`-based baseline cleanup and HydroSHEDS → SOI hydro transfer.
- That methodology doc also includes a short "How to read the validation package" section for interpreting the generated Aqueduct validation outputs.
- See [`docs/aqueduct_field_contract.md`](docs/aqueduct_field_contract.md) for the current Aqueduct source-field mappings used by the onboarded Aqueduct district, block, and hydro metrics.

### Processed outputs layout

Legacy processed outputs live under:

```text
IRT_DATA_DIR/
└── processed/
    └── {metric_slug}/
```

#### Legacy admin layout

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

#### Legacy hydro layout (retired)

The offline basin/sub-basin **climate** compute pipeline has been retired, so `processed/{metric_slug}/hydro/` is no longer produced or consumed for climate metrics. Any pre-existing `processed/{metric_slug}/hydro/` trees from earlier runs are inert and are not read by the dashboard. The historical layout was:

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

The retained offline Aqueduct hydro builder still writes SOI basin/sub-basin masters under the same `processed/{metric_slug}/hydro/` convention, but these are not surfaced in the admin-only dashboard. Its currently supported slugs are:

```text
processed/aq_water_stress/hydro/
├── master_metrics_by_basin.csv
└── master_metrics_by_sub_basin.csv

processed/aq_interannual_variability/hydro/
├── master_metrics_by_basin.csv
└── master_metrics_by_sub_basin.csv

processed/aq_seasonal_variability/hydro/
├── master_metrics_by_basin.csv
└── master_metrics_by_sub_basin.csv

processed/aq_water_depletion/hydro/
├── master_metrics_by_basin.csv
└── master_metrics_by_sub_basin.csv
```

The same onboarded Aqueduct slugs also support direct district and block masters under:

```text
processed/aq_water_stress/{state}/master_metrics_by_district.csv
processed/aq_water_stress/{state}/master_metrics_by_block.csv
processed/aq_interannual_variability/{state}/master_metrics_by_district.csv
processed/aq_interannual_variability/{state}/master_metrics_by_block.csv
processed/aq_seasonal_variability/{state}/master_metrics_by_district.csv
processed/aq_seasonal_variability/{state}/master_metrics_by_block.csv
processed/aq_water_depletion/{state}/master_metrics_by_district.csv
processed/aq_water_depletion/{state}/master_metrics_by_block.csv
```

Population exposure metrics currently support direct admin masters under:

```text
processed/population_total/{state}/master_metrics_by_district.csv
processed/population_total/{state}/master_metrics_by_block.csv
processed/population_density/{state}/master_metrics_by_district.csv
processed/population_density/{state}/master_metrics_by_block.csv
```

Groundwater status metrics currently support direct district masters under:

```text
processed/gw_stage_extraction_pct/{state}/master_metrics_by_district.csv
processed/gw_future_availability_ham/{state}/master_metrics_by_district.csv
processed/gw_extractable_resource_ham/{state}/master_metrics_by_district.csv
processed/gw_total_extraction_ham/{state}/master_metrics_by_district.csv
```

## Common commands

The canonical operational runner is now:

```bash
python -m tools.runs.prepare_dashboard --help
```

For a single command reference, see [`docs/command_catalog.md`](docs/command_catalog.md).

Regenerate the committed in-dashboard Technical Guidance Note asset after editing
`docs/technical_guidance_note.md` or its approved figures:

```bash
python -m tools.docs.build_technical_note_html
```

### Prepare climate hazards for the dashboard

```bash
python -m tools.runs.prepare_dashboard climate-hazards
```

Default behavior:
- `--level all` is implied
- resolves the live climate metric set per requested level
- computes only missing runnable climate outputs by default using validated completion markers
- builds only missing admin (district/block) masters
- refreshes only the requested `processed_optimised` levels and metrics
- reruns readiness verification after execution and returns non-zero if the requested bundle is still incomplete
- preserves current outputs unless `--overwrite` is supplied

The supported `--level` values are `all` (implied), `admin`, `district`, and `block`.

Block-only:

```bash
python -m tools.runs.prepare_dashboard climate-hazards --level block
```

One metric:

```bash
python -m tools.runs.prepare_dashboard climate-hazards --metrics tas_annual_mean
```

One metric, one model, one scenario:

```bash
python -m tools.runs.prepare_dashboard climate-hazards --metrics r95ptot_contribution_pct --models CanESM5 --scenarios historical
```

Plan-only:

```bash
python -m tools.runs.prepare_dashboard climate-hazards --level district --plan-only
```

Audit-only:

```bash
python -m tools.runs.prepare_dashboard climate-hazards --level district --audit-only
```

### Prepare the dashboard package with the canonical runner

```bash
python -m tools.runs.prepare_dashboard dashboard-package
```

This bundle includes climate hazards, Aqueduct, population exposure, groundwater prep, and optional JRC flood-depth prep,
and now refreshes `processed_optimised` plus the final audit as part of the same run.
When block-level products are part of the run, the runner now refreshes the canonical
`IRT_DATA_DIR/blocks_4326.geojson` first.

Preview first:

```bash
python -m tools.runs.prepare_dashboard dashboard-package --plan-only
```

Optional JRC flood-depth prep:

```bash
python -m tools.runs.prepare_dashboard jrc-flood-depth --source-dir /path/to/Floodlayers_JRC --assume-units m --overwrite
python -m tools.runs.prepare_dashboard dashboard-package --include-jrc-flood-depth --jrc-source-dir /path/to/Floodlayers_JRC --jrc-assume-units m --overwrite
```

For JRC flood-depth prep, runner `--overwrite` refreshes the selected state's JRC masters and QA outputs but does not wipe unrelated
`processed_optimised` metric artifacts.

For the full dashboard-ready Riverine Flood bundle for one state, including district and block composite publish:

```powershell
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_riverine_flood_bundle.ps1 -State Maharashtra -JrcDir D:/projects/irt_data/Floodlayers_JRC
```

### Build population exposure masters

```bash
python -m tools.runs.prepare_dashboard population-exposure --overwrite
python -m tools.geodata.build_population_admin_masters --overwrite
```

This aggregates the 2025 1 km population raster onto canonical district and block polygons and writes:
- `processed/population_total/{state}/master_metrics_by_district.csv`
- `processed/population_total/{state}/master_metrics_by_block.csv`
- `processed/population_density/{state}/master_metrics_by_district.csv`
- `processed/population_density/{state}/master_metrics_by_block.csv`
- `population/overlay/population_exposure_2025_overlay.png`
- `population/overlay/population_exposure_2025_overlay_meta.json`

The overlay is a display-only India raster reference using binned people-per-source-cell colors. Runtime maps use the exported PNG/metadata pair, including the optimized copies under `processed_optimised/context/population/overlay/`, and never read the raw TIFF.

### Build rural facilities exposure masters

```bash
python -m tools.runs.prepare_dashboard rural-facilities --overwrite
python -m tools.runs.prepare_dashboard dashboard-package --include-rural-facilities --overwrite
```

Optional inputs:
- `--rural-facilities-source-dir /path/to/Ruralfacilties_4files`
- `--rural-facilities-qa-dir /path/to/qa`
- `--rural-facilities-overlay-dir /path/to/overlay`

Outputs include `processed/rural_facilities_*/*/master_metrics_by_{district,block}.{csv,parquet}` plus `rural_facilities/overlay/rural_facilities_density_<category>_overlay.{png,json}` for `total`, `agro`, `education`, `health`, and `service`.

### Build groundwater district masters

```bash
python -m tools.runs.prepare_dashboard groundwater --overwrite
python -m tools.geodata.build_groundwater_district_masters --overwrite
```

This parses the 2024-2025 GEC workbook, resolves source districts onto the canonical
district GeoJSON through an explicit alias workflow, and writes:
- `processed/gw_stage_extraction_pct/{state}/master_metrics_by_district.csv`
- `processed/gw_future_availability_ham/{state}/master_metrics_by_district.csv`
- `processed/gw_extractable_resource_ham/{state}/master_metrics_by_district.csv`
- `processed/gw_total_extraction_ham/{state}/master_metrics_by_district.csv`

### Build JRC flood-depth masters for one state

Prepare a planned strict RP-100 source manifest scaffold without downloading rasters:

```bash
python -m tools.data_acquisition.prepare_jrc_rp100_source --boundary-path /path/to/districts_4326.geojson --tile-extents-path /path/to/tile_extents.geojson --output-dir /path/to/jrc_rp100_v2_1_2 --dry-run
```

```bash
python -m tools.runs.prepare_dashboard jrc-flood-depth --state Telangana --source-dir /path/to/Floodlayers_JRC --assume-units m --overwrite
python -m tools.geodata.build_jrc_flood_depth_admin_masters --state Telangana --source-dir /path/to/Floodlayers_JRC --assume-units m --overwrite
```

This builds district and block snapshot masters for the selected state. For example, `--state Telangana` writes:
- `processed/jrc_flood_depth_index_rp100/Telangana/master_metrics_by_district.csv`
- `processed/jrc_flood_depth_index_rp100/Telangana/master_metrics_by_block.csv`
- `processed/jrc_flood_extent_rp100/Telangana/master_metrics_by_district.csv`
- `processed/jrc_flood_extent_rp100/Telangana/master_metrics_by_block.csv`
- `processed/jrc_flood_depth_rp10/Telangana/master_metrics_by_district.csv`
- `processed/jrc_flood_depth_rp10/Telangana/master_metrics_by_block.csv`
- `processed/jrc_flood_depth_rp50/Telangana/master_metrics_by_district.csv`
- `processed/jrc_flood_depth_rp50/Telangana/master_metrics_by_block.csv`
- `processed/jrc_flood_depth_rp100/Telangana/master_metrics_by_district.csv`
- `processed/jrc_flood_depth_rp100/Telangana/master_metrics_by_block.csv`
- `processed/jrc_flood_depth_rp500/Telangana/master_metrics_by_district.csv`
- `processed/jrc_flood_depth_rp500/Telangana/master_metrics_by_block.csv`

JRC coverage is interpreted from raster extent overlap for this dataset family: positive values contribute flood depth,
and zero values inside raster extent are treated as dry cells.
The same workflow also writes the derived `jrc_flood_depth_index_rp100` severity-class masters and
`jrc_flood_extent_rp100` extent masters. Flood extent is stored as a fraction, displayed as a percent,
and uses total-polygon-area semantics, with raster-supported area retained in QA outputs. The RP-100 severity
index now combines RP-100 depth and RP-100 extent through the fixed 5x5 severity matrix, and the `run_summary.csv`
records that provenance with a `derived_severity_matrix` row. Rebuild the JRC masters and optimized outputs after
pulling this change because older persisted `jrc_flood_depth_index_rp100` outputs are methodologically incompatible.

### Refresh the full Riverine Flood bundle for one state

```powershell
powershell -ExecutionPolicy Bypass -File tools/runs/refresh_dashboard_riverine_flood_bundle.ps1 -State Maharashtra -JrcDir D:/projects/irt_data/Floodlayers_JRC
```

This script runs the full admin Riverine Flood workflow for the selected state:
1. `prepare_dashboard jrc-flood-depth` to build state-scoped JRC masters
2. `build_composite_metrics` for `composite_flood_jrc_depth` at `district` and `block`
3. `build_processed_optimised` scoped to the selected state and Riverine Flood metrics
4. `audit_processed_optimised_parity` scoped to the selected state and Riverine Flood metrics

Use `-PlanOnly` to print the exact commands without running them.

### Rebuild the canonical block boundaries

```bash
python -m tools.geodata.build_blocks_geojson --overwrite
```

This refreshes `IRT_DATA_DIR/blocks_4326.geojson` from the source block shapefile,
repairs canonical block identity columns, and emits QA CSVs for suspicious labels.

### Hydro boundary preparation

```bash
python -m tools.subbasin_shp_explore --help
```

This utility inspects the canonical `waterbasin_goi.shp`, can repair invalid hydro geometries, and exports:
- `basins.geojson`
- `subbasins.geojson`

### Build the district ↔ sub-basin crosswalk

```bash
python -m tools.geodata.build_district_subbasin_crosswalk --overwrite
python -m tools.geodata.build_aqueduct_hydro_crosswalk --overwrite
python -m tools.geodata.build_aqueduct_hydro_masters --overwrite
```

### Build the remaining polygon crosswalks

```bash
python -m tools.geodata.build_block_subbasin_crosswalk --overwrite
python -m tools.geodata.build_district_basin_crosswalk --overwrite
python -m tools.geodata.build_block_basin_crosswalk --overwrite
```

### Build the clean Aqueduct baseline artifact

```bash
python -m tools.geodata.prepare_aqueduct_baseline --help
python -m tools.geodata.prepare_aqueduct_baseline --source-gdb /path/to/Aq40_Y2023D07M05.gdb --baseline-csv /path/to/Aqueduct40_baseline_annual_y2023m07d05.csv --overwrite
```

This tool uses the Aqueduct `future_annual` geometry as the canonical HydroBASINS Level 6 base, aggregates segmented `baseline_annual` CSV rows to one record per `pfaf_id`, and also writes an India-only `future_annual` GeoJSON with the source future attributes preserved.

### Build the Aqueduct district crosswalk

```bash
python -m tools.geodata.build_aqueduct_admin_crosswalk --overwrite
```

This builds the direct Aqueduct `pfaf_id` ↔ district overlap table used for admin-boundary Aqueduct transfer in `EPSG:6933`.

### Build the Aqueduct block crosswalk

```bash
python -m tools.geodata.build_aqueduct_block_crosswalk --overwrite
```

This builds the direct Aqueduct `pfaf_id` ↔ block overlap table used for admin-boundary Aqueduct transfer in `EPSG:6933`.

### Build the Aqueduct admin masters

```bash
python -m tools.geodata.build_aqueduct_admin_masters --overwrite
```

This writes state-sliced district and block master CSVs for all onboarded Aqueduct metrics under `processed/{metric_slug}/{state}/master_metrics_by_district.csv` and `processed/{metric_slug}/{state}/master_metrics_by_block.csv`.

### Build or refresh Aqueduct hydro masters

```bash
python -m tools.geodata.build_aqueduct_hydro_masters --overwrite
```

### Validate the Aqueduct workflow

```bash
python -m tools.geodata.validate_aqueduct_workflow --overwrite
```

The validator now emits per-metric bundles covering district, block, basin, and sub-basin transfer outputs under `IRT_DATA_DIR/aqueduct/validation/{metric_slug}/`.

### Clean the Survey of India river network

```bash
python -m tools.geodata.clean_river_network --src /path/to/river_network_goi.shp --overwrite
```

This creates the first canonical river artifacts under `IRT_DATA_DIR`:
- `river_network.parquet`
- `river_network_display.geojson`
- `river_network_qa.csv`

The former `build_river_basin_reconciliation` and `build_river_subbasin_diagnostics` builders drove the retired navigable hydro (basin/sub-basin) river overlay and have been removed along with it; their `river_basin_name_reconciliation.csv` / `river_subbasin_diagnostics.csv` outputs are no longer built or consumed.

### Build river topology and missing-assignment diagnostics

```bash
python -m tools.geodata.build_river_topology --overwrite
```

This writes:
- `river_reaches.parquet`
- `river_nodes.parquet`
- `river_adjacency.parquet`
- `river_topology_qa.csv`
- `river_missing_assignments.csv`
- `river_missing_assignments.geojson`

The admin district/block river overlay is driven directly by the district-sliced river display artifact. Topology-ready river artifacts are supported offline, but upstream/downstream routing and river-based metric computation are still deferred.

## Usage notes

### Admin-only spatial family
- The dashboard operates on governance/action units only: **district** and **block**
- Watershed/process units (basin, sub-basin) are no longer navigable; hydrology is surfaced only as context on admin units

### Current crosswalk behavior
When the polygon crosswalk CSVs are present:
- district and block details expose related basins and sub-basins as **context**
- you can highlight related basin/sub-basin units on the admin map as a reference overlay

### Current limitations
- Crosswalks are **read-optimized and explanatory**, not analytical transfer engines
- Basin/sub-basin metrics are no longer computed or served by the tool

## Development

### Tests

```bash
python -m pytest -q
```

### Formatting and checks

```bash
black india_resilience_tool/
ruff check india_resilience_tool/
mypy india_resilience_tool/
```

### Adding a metric
1. Add the metric to `india_resilience_tool/config/metrics_registry.py`
2. Place the slug in the appropriate domain(s) and pillar
3. Ensure processed artifacts exist for the metric
4. Rebuild or refresh masters as needed

For a detailed repo map and module responsibilities, see [MANIFEST.md](MANIFEST.md).

## License

MIT License — see [LICENSE](LICENSE) for details.

## Contact

**Author:** Abu Bakar Siddiqui Thakur  
**Email:** absthakur@resilience.org.in

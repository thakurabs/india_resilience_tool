# India Resilience Tool (IRT)

The India Resilience Tool is a Streamlit dashboard for exploring climate-risk metrics across two spatial families:

- **Admin**: district and block
- **Hydro**: basin and sub-basin

IRT combines processed climate-model outputs, boundary layers, rankings, trends, and details views into a single exploration workflow. The current codebase supports admin and hydro map/rankings/details flows, hydro-specific boundaries and processed outputs, and an actionable polygon crosswalk bridge across **district/block** and **basin/sub-basin** views.

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Streamlit](https://img.shields.io/badge/streamlit-1.51.0-red.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## Current capabilities

### Core dashboard
- Spatial-family selector: `Admin` or `Hydro`
- Level selector:
  - Admin: `District` / `Block`
  - Hydro: `Basin` / `Sub-basin`
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
  - `Bio-physical Hazards` for externally sourced physical hazard layers such as Aqueduct
  - `Exposure` for static exposure layers such as population
- Population exposure onboarding:
  - Total Population on district and block units
  - Population Density on district and block units
  - fixed snapshot semantics: `snapshot`, `2025`
- Water-risk Aqueduct onboarding:
  - Aqueduct water stress on SOI basin, SOI sub-basin, district, and block units
  - Aqueduct interannual variability on SOI basin, SOI sub-basin, district, and block units
  - Aqueduct seasonal variability on SOI basin, SOI sub-basin, district, and block units
  - Aqueduct water depletion on SOI basin, SOI sub-basin, district, and block units
  - native Aqueduct scenarios: `historical`, `bau`, `opt`, `pes`
- Map view and rankings table for all four levels
- Right-side details panel with:
  - risk or metric summary
  - trend over time (when yearly source files exist)
  - scenario comparison (when the metric supports it)
  - case-study export for admin single-unit flows

### Portfolio support
- Implemented for **district**, **block**, **basin**, and **sub-basin**

### Hydro support
- Canonical hydro boundaries:
  - `basins.geojson`
  - `subbasins.geojson`
- Hydro processed outputs under `processed/{metric}/hydro/`
- Hydro master CSVs:
  - `master_metrics_by_basin.csv`
  - `master_metrics_by_sub_basin.csv`
- Optional hydro river overlay:
  - uses `river_network_display.geojson`
  - basin matching is mediated by `river_basin_name_reconciliation.csv`
  - available only in hydro basin/sub-basin views
  - toggle is off by default
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
  - basin and sub-basin details show **Administrative context**
  - related-unit highlight overlay on the map
  - admin -> hydro navigation
  - hydro -> admin navigation
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

### Run the dashboard

```bash
streamlit run main.py
```

Alternative entrypoint:

```bash
streamlit run india_resilience_tool/app/main.py
```

Open: `http://localhost:8501`

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
- `river_basin_name_reconciliation.csv` (optional but required for reliable hydro basin river overlays)
- `river_subbasin_diagnostics.csv` (optional diagnostics artifact for hydro sub-basin river overlays)
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
- `population/population_district_master_qa.csv` (optional QA for district population masters)
- `population/population_block_master_qa.csv` (optional QA for block population masters)
- `population/population_district_vs_blocks_qa.csv` (optional district vs sum(blocks) consistency QA)
- `population/population_national_summary.csv` (optional national raster-vs-admin population summary)

All boundary GeoJSONs are expected in `EPSG:4326`.

Aqueduct methodology note:

- See [`docs/aqueduct_onboarding_methodology.md`](docs/aqueduct_onboarding_methodology.md) for the full post-processing workflow, including `pfaf_id`-based baseline cleanup and HydroSHEDS → SOI hydro transfer.
- That methodology doc also includes a short "How to read the validation package" section for interpreting the generated Aqueduct validation outputs.
- See [`docs/aqueduct_field_contract.md`](docs/aqueduct_field_contract.md) for the current Aqueduct source-field mappings used by the onboarded Aqueduct district, block, and hydro metrics.

### Processed outputs layout

Processed outputs live under:

```text
IRT_DATA_DIR/
└── processed/
    └── {metric_slug}/
```

#### Admin layout

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

#### Hydro layout

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

For Aqueduct hydro onboarding, the currently supported slugs are:

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

## Common commands

The canonical operational runner is now:

```bash
python -m tools.runs.prepare_dashboard --help
```

For a single command reference, see [`docs/command_catalog.md`](docs/command_catalog.md).

### Build or refresh processed outputs

```bash
python -m tools.pipeline.compute_indices_multiprocess --help
python -m tools.pipeline.compute_indices_multiprocess --level district --metrics tas_annual_mean
python -m tools.pipeline.compute_indices_multiprocess --level block --metrics tas_annual_mean
python -m tools.pipeline.compute_indices_multiprocess --level basin --metrics tas_annual_mean
python -m tools.pipeline.compute_indices_multiprocess --level sub_basin --metrics tas_annual_mean
```

### Rebuild master CSVs

```bash
python -m tools.pipeline.build_master_metrics
```

### Prepare the dashboard package with the canonical runner

```bash
python -m tools.runs.prepare_dashboard dashboard-package --level all --state Telangana --overwrite
```

This bundle now includes climate hazards, Aqueduct, and population exposure prep.

Preview first:

```bash
python -m tools.runs.prepare_dashboard dashboard-package --level all --state Telangana --overwrite --dry-run
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

Then build the hydro-basin reconciliation table used by the hydro river overlay:

```bash
python -m tools.geodata.build_river_basin_reconciliation --overwrite
```

This writes:
- `river_basin_name_reconciliation.csv`

### Build river topology and missing-assignment diagnostics

```bash
python -m tools.geodata.build_river_subbasin_diagnostics --overwrite
python -m tools.geodata.build_river_topology --overwrite
```

This writes:
- `river_reaches.parquet`
- `river_nodes.parquet`
- `river_adjacency.parquet`
- `river_subbasin_diagnostics.csv`
- `river_topology_qa.csv`
- `river_missing_assignments.csv`
- `river_missing_assignments.geojson`

The optional hydro-only river overlay in basin/sub-basin views depends on the reconciliation and diagnostics files. Topology-ready river artifacts are supported offline, but upstream/downstream routing, admin-side river overlays, and river-based metric computation are still deferred.

## Usage notes

### Admin vs Hydro
- Use **Admin** when you want governance/action units: district or block
- Use **Hydro** when you want watershed/process units: basin or sub-basin

### Current crosswalk behavior
When the polygon crosswalk CSVs are present:
- district and block details expose related basins and sub-basins
- basin and sub-basin details expose related districts, with blocks as an optional drill-down
- you can highlight related units on the map
- you can jump across the admin/hydro bridge for the current polygon pair

### Current limitations
- Crosswalks are currently **read-optimized and explanatory**, not analytical transfer engines
- Basin metrics and sub-basin metrics should be computed directly on their own polygons
- Hydro UI is single-unit oriented today; portfolio/cross-family aggregation is future work

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

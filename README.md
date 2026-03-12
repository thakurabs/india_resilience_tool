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
  - risk domain
  - metric
  - scenario
  - period
  - statistic
  - map mode
- Map view and rankings table for all four levels
- Right-side details panel with:
  - risk summary
  - trend over time
  - scenario comparison
  - case-study export for admin single-unit flows

### Portfolio support
- Implemented for **district** and **block**
- Not implemented yet for **basin** and **sub-basin**

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
- Hydro portfolio workflows

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

All boundary GeoJSONs are expected in `EPSG:4326`.

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

## Common commands

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
```

### Build the remaining polygon crosswalks

```bash
python -m tools.geodata.build_block_subbasin_crosswalk --overwrite
python -m tools.geodata.build_district_basin_crosswalk --overwrite
python -m tools.geodata.build_block_basin_crosswalk --overwrite
```

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
2. Place the slug in the appropriate bundle(s)
3. Ensure processed artifacts exist for the metric
4. Rebuild or refresh masters as needed

For a detailed repo map and module responsibilities, see [MANIFEST.md](MANIFEST.md).

## License

MIT License — see [LICENSE](LICENSE) for details.

## Contact

**Author:** Abu Bakar Siddiqui Thakur  
**Email:** absthakur@resilience.org.in

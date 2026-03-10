# India Resilience Tool (IRT) - Codebase Manifest

## Overview

IRT is a Streamlit-based climate-risk dashboard organized around two spatial families:

- **Admin**: district, block
- **Hydro**: basin, sub-basin

The current codebase supports:
- map, rankings, and details flows for all four levels
- admin portfolio workflows for district and block
- hydro-specific boundary loading and processed-output discovery
- a first actionable crosswalk layer for **district ↔ sub-basin**

The crosswalk layer is currently **relationship- and explanation-oriented**. It does not yet perform weighted analytical transfer of metrics between admin and hydro units.

**Author:** Abu Bakar Siddiqui Thakur  
**Email:** absthakur@resilience.org.in  
**Tech Stack:** Python 3.10+, Streamlit, Pandas, GeoPandas, Folium, Matplotlib, Plotly

## Quick reference

### Entry points

| Command | Purpose |
|---------|---------|
| `streamlit run main.py` | Launch dashboard |
| `streamlit run india_resilience_tool/app/main.py` | Launch dashboard (alternative) |
| `python -m tools.pipeline.build_master_metrics` | Rebuild master CSVs |
| `python -m tools.pipeline.compute_indices_multiprocess --help` | Show compute pipeline options |
| `python -m tools.pipeline.compute_indices_multiprocess --level district --metrics <slug>` | Build district outputs |
| `python -m tools.pipeline.compute_indices_multiprocess --level block --metrics <slug>` | Build block outputs |
| `python -m tools.pipeline.compute_indices_multiprocess --level basin --metrics <slug>` | Build basin outputs |
| `python -m tools.pipeline.compute_indices_multiprocess --level sub_basin --metrics <slug>` | Build sub-basin outputs |
| `python -m tools.subbasin_shp_explore --help` | Inspect/repair/export canonical hydro boundaries |
| `python -m tools.geodata.build_district_subbasin_crosswalk --overwrite` | Build district ↔ sub-basin crosswalk CSV |

### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `IRT_PILOT_STATE` | `Telangana` | Default admin state in the app |
| `IRT_DATA_DIR` | from `paths.py` | Base directory for boundaries, crosswalks, and processed outputs |
| `IRT_PROCESSED_ROOT` | `IRT_DATA_DIR/processed/{metric}` | Override processed root |
| `IRT_DEBUG` | `0` | Enable debug/perf output |

## Current project structure

```text
india_resilience_tool/
├── analysis/
│   ├── map_enrichment.py
│   ├── metrics.py
│   ├── portfolio.py
│   └── timeseries.py
├── app/
│   ├── color_range_controls.py
│   ├── crosswalk_runtime.py
│   ├── details_runtime.py
│   ├── geo_cache.py
│   ├── geography.py
│   ├── geography_controls.py
│   ├── left_panel_runtime.py
│   ├── map_layer_runtime.py
│   ├── map_pipeline.py
│   ├── master_freshness.py
│   ├── perf.py
│   ├── point_selection_ui.py
│   ├── portfolio_multistate.py
│   ├── portfolio_state_runtime.py
│   ├── portfolio_ui.py
│   ├── ribbon.py
│   ├── runtime.py
│   ├── sidebar.py
│   ├── sidebar_branding.py
│   ├── state.py
│   └── views/
│       ├── details_panel.py
│       ├── map_view.py
│       ├── rankings_view.py
│       └── state_summary_view.py
├── compute/
│   ├── master_builder.py
│   └── spi_adapter.py
├── config/
│   ├── constants.py
│   ├── metrics_registry.py
│   ├── paths.py
│   └── variables.py
├── data/
│   ├── adm2_loader.py
│   ├── adm3_loader.py
│   ├── crosswalks.py
│   ├── discovery.py
│   ├── hydro_loader.py
│   ├── master_columns.py
│   ├── master_loader.py
│   ├── merge.py
│   └── spatial_match.py
├── utils/
│   ├── naming.py
│   └── processed_io.py
└── viz/
    ├── charts.py
    ├── colors.py
    ├── exports.py
    ├── folium_featurecollection.py
    ├── formatting.py
    ├── style.py
    └── tables.py

Root:
├── paths.py
├── README.md
├── MANIFEST.md
├── environment.yml
├── tools/
└── tests/
```

## Data contracts

### Boundary inputs

Expected under `IRT_DATA_DIR`:

| Artifact | Purpose |
|----------|---------|
| `districts_4326.geojson` | ADM2 district boundaries |
| `blocks_4326.geojson` | ADM3 block boundaries |
| `basins.geojson` | Canonical basin boundaries |
| `subbasins.geojson` | Canonical sub-basin boundaries |
| `district_subbasin_crosswalk.csv` | District ↔ sub-basin overlap registry |

### Boundary identifier expectations

| Level | Key columns |
|------|-------------|
| District | `state_name`, `district_name`, `geometry` |
| Block | `state_name`, `district_name`, `block_name`, `geometry` |
| Basin | `basin_id`, `basin_name`, `hydro_level`, `geometry` |
| Sub-basin | `basin_id`, `basin_name`, `subbasin_id`, `subbasin_code`, `subbasin_name`, `hydro_level`, `geometry` |

### Processed outputs

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

Identifier columns:
- district master: `state`, `district`
- block master: `state`, `district`, `block`

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

Identifier columns:
- basin master: `basin_id`, `basin_name`
- sub-basin master: `basin_id`, `basin_name`, `subbasin_id`, `subbasin_code`, `subbasin_name`

### Crosswalk artifact

Current canonical crosswalk:
- `district_subbasin_crosswalk.csv`

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

Current use:
- district details -> hydrology context
- sub-basin details -> administrative context
- related-unit overlay on the map
- district -> sub-basin jump
- sub-basin -> district jump

Not yet supported:
- block crosswalk artifacts
- basin crosswalk artifacts
- weighted translation across spatial families

## Module responsibilities

### App layer

#### `app/runtime.py`
Canonical Streamlit orchestrator. It wires:
- sidebar selectors
- geography controls
- metric ribbon
- map/rankings pipeline
- right-side details panel

#### `app/sidebar.py`
Family-aware sidebar routing:
- `Admin` vs `Hydro`
- `District` / `Block` / `Basin` / `Sub-basin`
- view switching and jump-once flags

#### `app/geography_controls.py`
Selection controls for the active level:
- admin selectors for state/district/block
- hydro selectors for basin/sub-basin
- analysis-focus gating

#### `app/map_pipeline.py`
Shared map/rankings build path:
- merge boundaries with the current master table
- compute baseline/delta/rank/risk/tooltip fields
- filter visible units for the active selection
- hand off to the Folium layer builder

#### `app/map_layer_runtime.py`
Streamlit-free Folium map builder:
- uses cached geometry FeatureCollections
- patches runtime properties into features
- supports optional related-unit overlay for crosswalk actions

#### `app/details_runtime.py`
Builds the right-side panel context:
- climate summary
- trend/scenario data
- current crosswalk context for district and sub-basin

#### `app/crosswalk_runtime.py`
App-layer helper for actionable crosswalk behavior:
- set/clear related-unit overlays
- queue district -> sub-basin navigation
- queue sub-basin -> district navigation

#### `app/views/details_panel.py`
Renders the details UI and current crosswalk actions:
- show context summary
- highlight related units
- open a related unit across admin/hydro families

#### `app/views/map_view.py`
Renders the Folium map and extracts click payloads. The map key is selection-aware so crosswalk overlays and hydro filters rerender correctly.

### Data layer

#### `data/hydro_loader.py`
Canonical hydro boundary loading and normalization:
- validates required hydro columns
- ensures stable hydro keys
- provides render-only simplification helpers

#### `data/crosswalks.py`
Read-optimized district ↔ sub-basin crosswalk service:
- validates the CSV contract
- builds district hydrology context
- builds sub-basin administrative context
- returns deterministic summary objects for the UI

#### `data/merge.py`
Boundary ↔ master merge helpers for:
- district
- block
- basin
- sub-basin

#### `data/discovery.py`
Processed-artifact discovery for yearly files and supporting file-system lookups across admin and hydro layouts.

#### `data/spatial_match.py`
Click and explicit-selection matching helpers used to resolve details-panel rows for admin and hydro selections.

### Compute layer

#### `compute/master_builder.py`
Builds master CSVs and enriches hydro masters with canonical hydro identifiers from the basin/sub-basin GeoJSONs.

#### `tools/pipeline/compute_indices_multiprocess.py`
Primary compute pipeline for district, block, basin, and sub-basin outputs. Current hydro support includes:
- direct basin computation
- direct sub-basin computation
- hydro output layout under `processed/{metric}/hydro/`
- shared spatial coverage QC policy

### Tools

#### `tools/subbasin_shp_explore.py`
Hydro boundary prep utility for the canonical `waterbasin_goi.shp` source:
- inspect schema and hierarchy
- report invalid geometries
- optionally repair invalid features
- export `basins.geojson` and `subbasins.geojson`

#### `tools/geodata/build_district_subbasin_crosswalk.py`
Offline generator for the canonical district ↔ sub-basin crosswalk CSV.

## Session state and UI contracts

Important current state keys include:
- `spatial_family`
- `admin_level`
- `analysis_mode`
- `selected_state`
- `selected_district`
- `selected_block`
- `selected_basin`
- `selected_subbasin`
- `crosswalk_overlay`
- `_pending_crosswalk_navigation`

Current UI contract:
- family first, then level
- hydro uses `Single basin focus` / `Single sub-basin focus`
- admin portfolios exist only for district and block

## Current status vs deferred work

### Implemented now
- Admin + hydro level support in the app
- Hydro boundary loading and hydro compute outputs
- Basin/sub-basin map, rankings, and details flows
- District ↔ sub-basin crosswalk context and actionability

### Deferred
- Block ↔ sub-basin crosswalks
- Basin ↔ admin crosswalk registry
- Weighted admin ↔ hydro metric transfer
- Hydro portfolio workflows
- River-network/reach translation layer

## Tests and validation

Primary test entrypoint:

```bash
python -m pytest -q
```

Current crosswalk/hydro-specific tests include:
- `tests/test_hydro_contracts.py`
- `tests/test_crosswalk_context.py`
- `tests/test_crosswalk_generator.py`
- `tests/test_crosswalk_runtime.py`

## Contact

For questions about this codebase:
- **Author:** Abu Bakar Siddiqui Thakur
- **Email:** absthakur@resilience.org.in

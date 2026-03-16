# Tools — India Resilience Tool (IRT)

This folder contains **ops / diagnostic / data-prep** scripts that are considered part of IRT functionality,
but are intentionally kept out of the runtime package.

Run these from the **repo root** so imports like `paths.py` resolve correctly.

## Pipeline

| Script | Purpose | Run |
|---|---|---|
| `tools/pipeline/compute_indices_multiprocess.py` | Build Parquet-only processed index artifacts under `processed_parquet/<metric>/build` (multi-process; district + block) | `python -m tools.pipeline.compute_indices_multiprocess --help` |
| `tools/pipeline/compute_indices.py` | Build processed index artifacts (single-process; debug) | `python -m tools.pipeline.compute_indices --help` |
| `tools/pipeline/build_master_metrics.py` | Build Parquet master tables (district + block) + state summary files from `processed_parquet/.../build` | `python -m tools.pipeline.build_master_metrics --help` |
| `tools/pipeline/publish_processed_outputs.py` | Promote Parquet build artifacts from `processed_parquet/.../build` into `processed_parquet/.../published` with archive-on-replace | `python -m tools.pipeline.publish_processed_outputs --help` |
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
| `tools/geodata/build_district_subbasin_crosswalk.py` | Build the canonical district ↔ sub-basin crosswalk CSV from district and sub-basin GeoJSONs | `python -m tools.geodata.build_district_subbasin_crosswalk --help` |
| `tools/geodata/build_block_subbasin_crosswalk.py` | Build the canonical block ↔ sub-basin crosswalk CSV from block and sub-basin GeoJSONs | `python -m tools.geodata.build_block_subbasin_crosswalk --help` |
| `tools/geodata/build_district_basin_crosswalk.py` | Build the canonical district ↔ basin crosswalk CSV from district and basin GeoJSONs | `python -m tools.geodata.build_district_basin_crosswalk --help` |
| `tools/geodata/build_block_basin_crosswalk.py` | Build the canonical block ↔ basin crosswalk CSV from block and basin GeoJSONs | `python -m tools.geodata.build_block_basin_crosswalk --help` |
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

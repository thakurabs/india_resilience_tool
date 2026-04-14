#!/usr/bin/env python3
"""
Uniform, future-proof index pipeline for the India Resilience Tool.

Features:
- Multiprocessing support for parallel computation
- Configurable number of workers  
- Progress tracking
- Discovers models across ALL climate variables
- Support for both district (ADM2) and block (ADM3) level aggregation
- Clean folder structure: districts/ and blocks/ subfolders
- SPI/SPEI computation via climate-indices package (NOAA-validated) or legacy scipy

Output Structure:
    processed/{metric}/{state}/
    ├── master_metrics_by_district.csv
    ├── master_metrics_by_block.csv
    ├── districts/                      # District-level data
    │   ├── {district}/
    │   │   └── {model}/{scenario}/
    │   │       ├── {district}_yearly.csv
    │   │       └── {district}_periods.csv
    │   └── ensembles/
    │       └── {district}/{scenario}/
    │           └── {district}_yearly_ensemble.csv
    └── blocks/                         # Block-level data
        ├── {district}/
        │   └── {block}/
        │       └── {model}/{scenario}/
        │           ├── {block}_yearly.csv
        │           └── {block}_periods.csv
        └── ensembles/
            └── {district}/{block}/{scenario}/
                └── {block}_yearly_ensemble.csv

Usage:
  python compute_indices_multiprocess.py                        # Default (district + block, 75% CPUs)
  python compute_indices_multiprocess.py --level district       # District only
  python compute_indices_multiprocess.py --level block          # Block only
  python compute_indices_multiprocess.py -w 8                   # Use 8 workers
  python compute_indices_multiprocess.py -w 1 -v                # Sequential + verbose
  python compute_indices_multiprocess.py --list-metrics         # List available metrics
  python compute_indices_multiprocess.py --list-models          # List discovered models
  python compute_indices_multiprocess.py --state Telangana      # Specific state
  python compute_indices_multiprocess.py --spi-legacy           # Force legacy SPI (scipy-based)
  python compute_indices_multiprocess.py --spi-distribution pearson  # Use Pearson Type III for SPI

SPI/SPEI Implementation:
  By default, this pipeline uses the climate-indices package (https://github.com/monocongo/climate_indices)
  for SPI/SPEI computation when available. This provides:
  - Peer-reviewed, NOAA-developed algorithms
  - Support for both Gamma and Pearson Type III distributions
  - Proper zero-inflation handling
  - Numba-accelerated performance
  
  If climate-indices is not installed, or --spi-legacy is specified, the pipeline
  falls back to the legacy scipy-based implementation.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

import os, glob, sys, time, argparse, logging, json, traceback, hashlib, shutil
from collections import defaultdict
from pathlib import Path
from typing import Any, Literal, Optional, Sequence
from dataclasses import dataclass
from functools import partial
from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from rasterio import features
from affine import Affine
from shapely.geometry import box

from paths import (
    BASE_OUTPUT_ROOT,
    BASINS_PATH,
    BLOCKS_PATH,
    DATA_ROOT,
    DISTRICTS_PATH,
    SUBBASINS_PATH,
)
from india_resilience_tool.data.hydro_loader import ensure_hydro_columns
from india_resilience_tool.config.metrics_registry import PIPELINE_METRICS_RAW
from india_resilience_tool.utils.naming import hydro_fs_token
from india_resilience_tool.utils.processed_io import (
    ensure_directory,
    glob_paths,
    path_exists,
    read_csv,
    remove_tree,
    unlink_file,
    write_csv,
)

# -----------------------------------------------------------------------------
# CLIMATE-INDICES PACKAGE INTEGRATION (SPI/SPEI)
# -----------------------------------------------------------------------------
# Try to import the climate-indices adapter for scientifically-validated SPI
try:
    from india_resilience_tool.compute.spi_adapter import (
        compute_spi_rows_climate_indices,
        Distribution as SPIDistribution,
        CLIMATE_INDICES_AVAILABLE,
    )
except ImportError:
    CLIMATE_INDICES_AVAILABLE = False
    compute_spi_rows_climate_indices = None
    SPIDistribution = None

# Configuration flag: Set to True to use climate-indices package for SPI/SPEI
# When False, falls back to the legacy scipy-based implementation
USE_CLIMATE_INDICES_PACKAGE = CLIMATE_INDICES_AVAILABLE

# SPI distribution selection for climate-indices (set from CLI via --spi-distribution)
SPI_DISTRIBUTION: str = "gamma"

# Type alias for administrative level
AdminLevel = Literal["district", "block", "basin", "sub_basin"]

# Folder names for clean separation
DISTRICT_FOLDER = "districts"
BLOCK_FOLDER = "blocks"
BASIN_FOLDER = "basins"
SUB_BASIN_FOLDER = "sub_basins"
HYDRO_ROOT_NAME = "hydro"
COVERAGE_THRESHOLD = 0.80
NULL_LIKE_STRINGS = {"", "nan", "<na>", "none", "nat"}

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
SCENARIOS = {
    "historical": {"subdir": "historical/tas", "periods": {"1990-2010": (1990, 2010)}},
    "ssp245": {"subdir": "ssp245/tas", "periods": {"2020-2040": (2020, 2040), "2040-2060": (2040, 2060), "2060-2080": (2060, 2080)}},
    "ssp585": {"subdir": "ssp585/tas", "periods": {"2020-2040": (2020, 2040), "2040-2060": (2040, 2060), "2060-2080": (2060, 2080)}},
}
MIN_YEARS_REQUIRED_FRACTION = 0.6
MIN_YEARS_ABSOLUTE = 5
METRICS = PIPELINE_METRICS_RAW
DEFAULT_WORKERS = max(1, int(cpu_count() * 0.75))

def setup_logging(verbose: bool = False):
    import sys
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,   # critical: overwrites any pre-existing handlers
    )

# -----------------------------------------------------------------------------
# BASIC HELPERS
# -----------------------------------------------------------------------------
def _is_blank_like(value: object) -> bool:
    """Return True when a scalar identifier-like value should be treated as missing."""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    return str(value).strip().lower() in NULL_LIKE_STRINGS


def _safe_component(name: object) -> str:
    """Build a deterministic hydro-safe folder/file component."""
    return hydro_fs_token(str(name).strip())


def metric_root(slug: str) -> Path:
    root = BASE_OUTPUT_ROOT / slug
    root.mkdir(parents=True, exist_ok=True)
    return root

def get_level_folder(level: AdminLevel) -> str:
    """Get the subfolder name for a given level."""
    if level == "sub_basin":
        return SUB_BASIN_FOLDER
    if level == "basin":
        return BASIN_FOLDER
    return BLOCK_FOLDER if level == "block" else DISTRICT_FOLDER

def normalize_lat_lon(ds: xr.Dataset) -> xr.Dataset:
    ren = {}
    for c in ["latitude", "y"]:
        if c in ds.dims: ren[c] = "lat"
    for c in ["longitude", "x"]:
        if c in ds.dims: ren[c] = "lon"
    return ds.rename(ren) if ren else ds

def pr_to_mm_per_day(da: xr.DataArray) -> xr.DataArray:
    units = (getattr(da, "attrs", {}).get("units", "") or "").strip().lower()
    return da * 86400.0 if units in {"kg m-2 s-1", "kg m-2 s^-1", "kg/m^2/s"} else da

# -----------------------------------------------------------------------------
# BOUNDARY LOADING (Generalized for district/block)
# -----------------------------------------------------------------------------
def get_boundary_path(level: AdminLevel) -> Path:
    """Get the boundary file path based on level."""
    if level == "sub_basin":
        return SUBBASINS_PATH
    if level == "basin":
        return BASINS_PATH
    return BLOCKS_PATH if level == "block" else DISTRICTS_PATH


def _validate_hydro_boundary_identity(
    gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
) -> None:
    """Ensure hydro boundary inputs contain non-empty identifiers before mask building."""
    if level == "sub_basin":
        required = ["basin_id", "basin_name", "subbasin_id", "subbasin_name"]
    elif level == "basin":
        required = ["basin_id", "basin_name"]
    else:
        return

    invalid_mask = pd.Series(False, index=gdf.index, dtype=bool)
    for col in required:
        invalid_mask |= gdf[col].map(_is_blank_like)

    if invalid_mask.any():
        sample = gdf.loc[invalid_mask, required].head(5).to_dict("records")
        raise ValueError(
            f"Hydro boundary inputs contain blank identity values for level={level}. "
            f"Required={required}. Sample rows={sample}"
        )

def load_boundaries(
    path: Path,
    state_filter: Optional[str] = None,
    level: AdminLevel = "district",
) -> gpd.GeoDataFrame:
    """
    Load boundary file and optionally filter to a specific state.
    """
    gdf = gpd.read_file(path)
    
    if level in {"basin", "sub_basin"}:
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        gdf = ensure_hydro_columns(gdf, level="sub_basin" if level == "sub_basin" else "basin")
        _validate_hydro_boundary_identity(gdf, level=level)
        return gdf

    # Find state column
    state_cols = ["STATE_UT", "state_ut", "STATE", "STATE_LGD", "ST_NM", "state_name"]
    state_col = next((c for c in state_cols if c in gdf.columns), None)
    if not state_col:
        raise ValueError(f"No state column in {path}")
    
    # Normalize state names for filtering
    s = gdf[state_col].astype(str).str.normalize("NFKC").str.strip().str.lower().str.replace(r"\s+", " ", regex=True)
    gdf["_state_norm"] = s
    
    # Filter to state if specified
    if state_filter:
        filter_keys = {state_filter.lower().strip()}
        if "telangana" in filter_keys:
            filter_keys.update({"telengana", "telangana state"})
        gdf = gdf[gdf["_state_norm"].isin(filter_keys)]
        if gdf.empty:
            raise ValueError(f"No rows found for state: {state_filter}")
    
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    
    gdf = gdf.drop(columns=["_state_norm"])
    
    # Standardize column names based on level
    if level == "block":
        gdf = _standardize_block_columns(gdf)
    else:
        gdf = _standardize_district_columns(gdf)
    
    return gdf

def _standardize_district_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ensure district GeoDataFrame has standard columns."""
    out = gdf.copy()
    if "DISTRICT" in out.columns and "district_name" not in out.columns:
        out["district_name"] = out["DISTRICT"].astype(str).str.strip()
    elif "district_name" not in out.columns:
        for cand in ["District", "DIST_NAME", "district"]:
            if cand in out.columns:
                out["district_name"] = out[cand].astype(str).str.strip()
                break
    if "STATE_UT" in out.columns and "state_name" not in out.columns:
        out["state_name"] = out["STATE_UT"].astype(str).str.strip()
    return out

def _standardize_block_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ensure block GeoDataFrame has standard columns."""
    out = gdf.copy()
    
    # Block/subdistrict name
    if "Sub_dist" in out.columns and "block_name" not in out.columns:
        out["block_name"] = out["Sub_dist"].astype(str).str.strip()
    elif "block_name" not in out.columns:
        for cand in ["SUBDISTRICT", "BLOCK", "TEHSIL", "MANDAL", "TALUK"]:
            if cand in out.columns:
                out["block_name"] = out[cand].astype(str).str.strip()
                break
    
    # District name (parent)
    if "District" in out.columns and "district_name" not in out.columns:
        out["district_name"] = out["District"].astype(str).str.strip()
    elif "DISTRICT" in out.columns and "district_name" not in out.columns:
        out["district_name"] = out["DISTRICT"].astype(str).str.strip()
    
    # State name
    if "STATE_UT" in out.columns and "state_name" not in out.columns:
        out["state_name"] = out["STATE_UT"].astype(str).str.strip()
    
    return out

def get_unit_name_column(level: AdminLevel) -> str:
    """Get the column name for the spatial unit based on level."""
    if level == "sub_basin":
        return "subbasin_name"
    if level == "basin":
        return "basin_name"
    return "block_name" if level == "block" else "district_name"

# Legacy compatibility
def load_telangana_districts(path: Path) -> gpd.GeoDataFrame:
    """Legacy function - use load_boundaries() instead."""
    return load_boundaries(path, state_filter="Telangana", level="district")

# -----------------------------------------------------------------------------
# MASK BUILDING (Generalized)
# -----------------------------------------------------------------------------
def _unit_key_from_row(row: pd.Series, level: AdminLevel) -> str:
    """Return the level-specific unit key used for mask/output grouping."""
    unit_col = get_unit_name_column(level)
    unit_name = str(row.get(unit_col, "")).strip()

    if level == "block":
        district = str(row.get("district_name", "")).strip()
        return f"{district}||{unit_name}" if district else unit_name
    if level == "sub_basin":
        basin_name = str(row.get("basin_name", "")).strip()
        return f"{basin_name}||{unit_name}" if basin_name else unit_name
    return unit_name


def _sample_extent_polygon(sample_ds: xr.Dataset):
    """Build a polygon covering the full sample climate grid extent."""
    lats = np.asarray(sample_ds["lat"].values, dtype=float)
    lons = np.asarray(sample_ds["lon"].values, dtype=float)
    if lats.size == 0 or lons.size == 0:
        raise ValueError("Sample dataset has empty lat/lon coordinates")

    yres = abs(float(lats[1] - lats[0])) if lats.size > 1 else 0.0
    xres = abs(float(lons[1] - lons[0])) if lons.size > 1 else 0.0
    min_lat = float(np.min(lats)) - (yres / 2.0)
    max_lat = float(np.max(lats)) + (yres / 2.0)
    min_lon = float(np.min(lons)) - (xres / 2.0)
    max_lon = float(np.max(lons)) + (xres / 2.0)
    return box(min_lon, min_lat, max_lon, max_lat)


def _outside_extent_coverage_rows(
    gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
) -> pd.DataFrame:
    """Return coverage-QC rows for hydro units outside the climate-data extent."""
    rows: list[dict[str, object]] = []
    unit_col = get_unit_name_column(level)
    for _, row in gdf.iterrows():
        unit_name = str(row.get(unit_col, "")).strip()
        qc_row: dict[str, object] = {
            "unit_key": _unit_key_from_row(row, level),
            "coverage_fraction": 0.0,
            "coverage_ok": False,
            "coverage_threshold": COVERAGE_THRESHOLD,
            "covered_cells": 0,
            "total_cells": 0,
            "eligible_for_processing": False,
            "extent_intersects": False,
            "reason": "outside_climate_extent",
        }
        if level == "basin":
            qc_row["basin_id"] = str(row.get("basin_id", "")).strip()
            qc_row["basin_name"] = unit_name
        elif level == "sub_basin":
            qc_row["basin_id"] = str(row.get("basin_id", "")).strip()
            qc_row["basin_name"] = str(row.get("basin_name", "")).strip()
            qc_row["subbasin_id"] = str(row.get("subbasin_id", "")).strip()
            qc_row["subbasin_name"] = unit_name
        rows.append(qc_row)
    return pd.DataFrame(rows)


def _filter_hydro_units_to_climate_extent(
    gdf: gpd.GeoDataFrame,
    sample_ds: xr.Dataset,
    *,
    level: AdminLevel,
    slug: str,
    model: str,
    scenario: str,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """Restrict hydro units to those intersecting the available climate-data extent."""
    if level not in {"basin", "sub_basin"}:
        return gdf.copy(), pd.DataFrame()

    climate_extent = _sample_extent_polygon(sample_ds)
    intersects_mask = gdf.geometry.intersects(climate_extent).fillna(False)
    eligible_gdf = gdf.loc[intersects_mask].copy()
    excluded_gdf = gdf.loc[~intersects_mask].copy()

    if not excluded_gdf.empty:
        unit_col = get_unit_name_column(level)
        sample_units = excluded_gdf[unit_col].astype(str).head(5).tolist()
        logging.info(
            "[%s] Hydro extent filter for %s/%s at level=%s kept %d of %d units; excluded %d outside climate extent. Sample excluded units: %s",
            slug,
            model,
            scenario,
            level,
            len(eligible_gdf),
            len(gdf),
            len(excluded_gdf),
            sample_units,
        )

    return eligible_gdf, _outside_extent_coverage_rows(excluded_gdf, level=level)


def _prune_excluded_hydro_outputs(
    metric_root_path: Path,
    *,
    state_name: str,
    level: AdminLevel,
    model: str,
    scenario: str,
    excluded_coverage_df: pd.DataFrame,
    slug: str,
) -> None:
    """Delete stale hydro outputs for units excluded by the current climate extent."""
    if excluded_coverage_df is None or excluded_coverage_df.empty:
        return

    level_folder = get_level_folder(level)
    removed_files = 0
    for _, row in excluded_coverage_df.iterrows():
        if level == "basin":
            basin_safe = _safe_component(row.get("basin_name", ""))
            scenario_dir = metric_root_path / state_name / level_folder / basin_safe / model / scenario
            basename = basin_safe
        elif level == "sub_basin":
            basin_safe = _safe_component(row.get("basin_name", ""))
            sub_basin_safe = _safe_component(row.get("subbasin_name", ""))
            scenario_dir = metric_root_path / state_name / level_folder / basin_safe / sub_basin_safe / model / scenario
            basename = sub_basin_safe
        else:
            continue

        for suffix in ("_yearly.csv", "_periods.csv"):
            path = scenario_dir / f"{basename}{suffix}"
            if path.exists():
                path.unlink()
                removed_files += 1

        current = scenario_dir
        while current != metric_root_path and current.exists():
            try:
                current.rmdir()
            except OSError:
                break
            current = current.parent

    if removed_files:
        logging.info(
            "[%s] Removed %d stale hydro output files outside the current climate extent for %s/%s level=%s",
            slug,
            removed_files,
            model,
            scenario,
            level,
        )


def _prune_excluded_hydro_ensemble_outputs(
    metric_root_path: Path,
    *,
    state_name: str,
    level: AdminLevel,
    scenario: str,
    excluded_coverage_df: pd.DataFrame,
    slug: str,
) -> None:
    """Delete stale hydro ensemble outputs for units excluded by the current climate extent."""
    if excluded_coverage_df is None or excluded_coverage_df.empty:
        return

    ensembles_root = metric_root_path / state_name / get_level_folder(level) / "ensembles"
    if not ensembles_root.exists():
        return

    removed_files = 0
    for _, row in excluded_coverage_df.iterrows():
        if level == "basin":
            basin_safe = _safe_component(row.get("basin_name", ""))
            scenario_dir = ensembles_root / basin_safe / scenario
        elif level == "sub_basin":
            basin_safe = _safe_component(row.get("basin_name", ""))
            sub_basin_safe = _safe_component(row.get("subbasin_name", ""))
            scenario_dir = ensembles_root / basin_safe / sub_basin_safe / scenario
        else:
            continue

        for path in scenario_dir.glob("*_yearly_ensemble.csv"):
            path.unlink()
            removed_files += 1

        current = scenario_dir
        while current != ensembles_root and current.exists():
            try:
                current.rmdir()
            except OSError:
                break
            current = current.parent

    if removed_files:
        logging.info(
            "[%s] Removed %d stale hydro ensemble files outside the current climate extent for scenario=%s level=%s",
            slug,
            removed_files,
            scenario,
            level,
        )


def build_unit_masks(
    gdf: gpd.GeoDataFrame,
    sample_ds: xr.Dataset,
    level: AdminLevel = "district",
) -> tuple[dict[str, xr.DataArray], pd.DataFrame]:
    """
    Build raster masks for each spatial unit (district or block).
    
    Returns:
    - dict of unit_key -> raster mask
    - DataFrame with coverage QC per unit
    """
    unit_col = get_unit_name_column(level)
    
    if unit_col not in gdf.columns:
        # Fallback for legacy district data
        if level == "district" and "DISTRICT" in gdf.columns:
            unit_col = "DISTRICT"
        else:
            raise ValueError(f"'{unit_col}' not found in GDF. Available: {list(gdf.columns)}")
    
    lats, lons = sample_ds["lat"].values, sample_ds["lon"].values
    height, width = lats.size, lons.size
    
    if not np.all(np.diff(lons) > 0):
        raise ValueError("Longitude not strictly increasing")
    
    xres, yres = lons[1] - lons[0], lats[1] - lats[0]
    transform = Affine.translation(lons[0] - xres/2, lats[0] - yres/2) * Affine.scale(xres, yres)
    
    try:
        sample_var = next(iter(sample_ds.data_vars))
        valid_grid = sample_ds[sample_var]
        if "time" in valid_grid.dims:
            valid_grid = valid_grid.isel(time=0)
        valid_footprint = valid_grid.notnull()
    except Exception:
        valid_footprint = xr.DataArray(
            np.ones((height, width), dtype=bool),
            coords={"lat": sample_ds["lat"], "lon": sample_ds["lon"]},
            dims=("lat", "lon"),
        )

    masks: dict[str, xr.DataArray] = {}
    coverage_rows: list[dict[str, object]] = []
    for _, row in gdf.iterrows():
        if row.geometry is None:
            continue

        unit_name = str(row[unit_col]).strip()
        key = _unit_key_from_row(row, level)
        
        mask = features.rasterize(
            [(row.geometry, 1)],
            out_shape=(height, width),
            transform=transform,
            fill=0,
            all_touched=True,
            dtype="uint8"
        )
        mask_da = xr.DataArray(
            mask.astype(bool),
            coords={"lat": sample_ds["lat"], "lon": sample_ds["lon"]},
            dims=("lat", "lon")
        )
        masks[key] = mask_da

        total_cells = int(mask_da.sum().item())
        covered_cells = int((mask_da & valid_footprint).sum().item()) if total_cells > 0 else 0
        coverage_fraction = (
            float(covered_cells / total_cells)
            if total_cells > 0
            else 0.0
        )

        qc_row = {
            "unit_key": key,
            "coverage_fraction": coverage_fraction,
            "coverage_ok": coverage_fraction >= COVERAGE_THRESHOLD,
            "coverage_threshold": COVERAGE_THRESHOLD,
            "covered_cells": covered_cells,
            "total_cells": total_cells,
            "eligible_for_processing": True,
            "extent_intersects": True,
            "reason": (
                "ok"
                if coverage_fraction >= COVERAGE_THRESHOLD
                else "below_spatial_coverage_threshold"
            ),
        }
        if level == "block":
            qc_row["district"] = str(row.get("district_name", "")).strip()
            qc_row["block"] = unit_name
        elif level == "district":
            qc_row["district"] = unit_name
        elif level == "basin":
            qc_row["basin_id"] = str(row.get("basin_id", "")).strip()
            qc_row["basin_name"] = unit_name
        elif level == "sub_basin":
            qc_row["basin_id"] = str(row.get("basin_id", "")).strip()
            qc_row["basin_name"] = str(row.get("basin_name", "")).strip()
            qc_row["subbasin_id"] = str(row.get("subbasin_id", "")).strip()
            qc_row["subbasin_name"] = unit_name
        coverage_rows.append(qc_row)

    return masks, pd.DataFrame(coverage_rows)

# Legacy compatibility
def build_district_masks(gdf: gpd.GeoDataFrame, sample_ds: xr.Dataset, district_name_col: str = "DISTRICT") -> dict:
    """Legacy function - use build_unit_masks() instead."""
    if district_name_col not in gdf.columns and "district_name" in gdf.columns:
        gdf = gdf.copy()
        gdf[district_name_col] = gdf["district_name"]
    masks, _coverage_df = build_unit_masks(gdf, sample_ds, level="district")
    return masks


def _add_unit_fields_from_key(row: dict, unit_key: str, level: AdminLevel) -> None:
    """Populate row identifier columns from a level-specific unit key."""
    if level == "block":
        if "||" in unit_key:
            district, block = unit_key.split("||", 1)
            row["district"] = district
            row["block"] = block
        else:
            row["district"] = "Unknown"
            row["block"] = unit_key
        return

    if level == "sub_basin":
        if "||" in unit_key:
            basin, sub_basin = unit_key.split("||", 1)
            row["basin"] = basin
            row["sub_basin"] = sub_basin
        else:
            row["basin"] = "Unknown"
            row["sub_basin"] = unit_key
        return

    if level == "basin":
        row["basin"] = unit_key
        return

    row["district"] = unit_key


def _append_coverage_failure_rows(
    rows: list[dict],
    coverage_df: pd.DataFrame,
    years: list[int],
    *,
    level: AdminLevel,
    value_col: str,
) -> None:
    """Append NaN rows for units below the spatial coverage threshold."""
    if coverage_df is None or coverage_df.empty:
        return

    eligibility_mask = pd.Series(True, index=coverage_df.index, dtype=bool)
    if "eligible_for_processing" in coverage_df.columns:
        eligibility_mask = coverage_df["eligible_for_processing"].fillna(True).astype(bool)

    failing = coverage_df[eligibility_mask & ~coverage_df["coverage_ok"].astype(bool)].copy()
    if failing.empty:
        return

    for _, qc_row in failing.iterrows():
        unit_key = str(qc_row.get("unit_key", "")).strip()
        for year in years:
            row = {
                "year": int(year),
                "value": np.nan,
                value_col: np.nan,
                "source_file": "",
            }
            _add_unit_fields_from_key(row, unit_key, level)
            rows.append(row)


def _validate_output_unit_fields(
    df: pd.DataFrame,
    *,
    level: AdminLevel,
    slug: str,
    model: str,
    scenario: str,
    stage_label: str,
) -> None:
    """Validate hydro identity columns before grouping or writing outputs."""
    if level == "sub_basin":
        required = ["basin", "sub_basin"]
    elif level == "basin":
        required = ["basin"]
    else:
        return

    missing_cols = [col for col in required if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"[{slug}] Missing required hydro identity columns before writing {stage_label} "
            f"for level={level}, model={model}, scenario={scenario}: {missing_cols}"
        )

    invalid_mask = pd.Series(False, index=df.index, dtype=bool)
    for col in required:
        invalid_mask |= df[col].map(_is_blank_like)

    if invalid_mask.any():
        sample_cols = [col for col in required if col in df.columns]
        for optional_col in ["year", "period", "value"]:
            if optional_col in df.columns and optional_col not in sample_cols:
                sample_cols.append(optional_col)
        sample = df.loc[invalid_mask, sample_cols].head(5).to_dict("records")
        raise ValueError(
            f"[{slug}] Invalid hydro identity values before writing {stage_label} "
            f"for level={level}, model={model}, scenario={scenario}. "
            f"Required={required}. Sample rows={sample}"
        )


def _write_coverage_qc(
    metric_root_path: Path,
    *,
    state_name: str,
    level: AdminLevel,
    model: str,
    scenario: str,
    coverage_df: pd.DataFrame,
) -> None:
    """Write spatial coverage QC for a metric/model/scenario/level combination."""
    if coverage_df is None or coverage_df.empty:
        return

    qc_root = metric_root_path / state_name / get_level_folder(level)
    qc_root.mkdir(parents=True, exist_ok=True)
    out_path = qc_root / f"coverage_qc_{model}_{scenario}.csv"
    coverage_df.to_csv(out_path, index=False)

# -----------------------------------------------------------------------------
# RUN-LENGTH HELPERS
# -----------------------------------------------------------------------------
def _run_length_stats(mask: np.ndarray, min_len: int) -> tuple[int, int]:
    max_run, total_days, current = 0, 0, 0
    for v in mask:
        if v: current += 1
        else:
            if current >= min_len: total_days += current; max_run = max(max_run, current)
            current = 0
    if current >= min_len: total_days += current; max_run = max(max_run, current)
    return max_run, total_days

def _count_events(mask: np.ndarray, min_len: int) -> int:
    arr = np.asarray(mask, dtype=bool)
    if arr.size == 0: return 0
    events, run_len = 0, 0
    for v in arr:
        if v: run_len += 1
        else:
            if run_len >= min_len: events += 1
            run_len = 0
    if run_len >= min_len: events += 1
    return events

def _get_district_daily_mean(da: xr.DataArray, mask: xr.DataArray) -> xr.DataArray:
    if "time" not in da.dims:
        return da

    spatial_dims = tuple(dim for dim in ("lat", "lon") if dim in da.dims)
    if any(int(da.sizes.get(dim, 0)) == 0 for dim in spatial_dims):
        return da.isel(time=slice(0, 0))

    try:
        masked = da.where(mask)
        daily_mean = masked.mean(dim=spatial_dims, skipna=True) if spatial_dims else masked
    except Exception:
        return da.isel(time=slice(0, 0))

    return daily_mean.dropna(dim="time", how="all") if "time" in daily_mean.dims else daily_mean


def _filter_to_baseline(
    da: xr.DataArray,
    baseline_years: tuple[int, int],
    strict: bool = False,
) -> xr.DataArray:
    """
    Filter a time-indexed DataArray to only include data within baseline years.
    
    Args:
        da: DataArray with 'time' dimension
        baseline_years: Tuple of (start_year, end_year) inclusive
        
    Returns:
        DataArray filtered to baseline period, or original if filtering fails
        (unless strict=True, in which case an empty baseline is returned).
    """
    if da.size == 0 or "time" not in da.dims:
        if strict and "time" in da.dims:
            return da.isel(time=slice(0, 0))
        return da
    
    start_year, end_year = baseline_years
    try:
        years = da["time"].dt.year
        mask = (years >= start_year) & (years <= end_year)
        filtered = da.where(mask, drop=True)
        # If baseline period has no data, fall back only if strict=False
        if filtered.size == 0:
            if strict:
                logging.warning(
                    "Baseline years %s-%s missing from series; returning empty baseline.",
                    start_year,
                    end_year,
                )
                return filtered
            return da
        return filtered
    except Exception:
        # If filtering fails (e.g., no time coordinate), return original unless strict
        if strict and "time" in da.dims:
            logging.warning("Baseline filtering failed; returning empty baseline.")
            return da.isel(time=slice(0, 0))
        return da


# -----------------------------------------------------------------------------
# WET-BULB TEMPERATURE (Stull 2011 approximation)
# -----------------------------------------------------------------------------
def _wet_bulb_stull_c(t_c: xr.DataArray, rh_pct: xr.DataArray) -> xr.DataArray:
    """
    Approximate wet-bulb temperature (°C) from air temperature (°C) and RH (%).

    Uses the Stull (2011) approximation, valid for typical near-surface conditions.

    Args:
        t_c: Air temperature in °C (time series).
        rh_pct: Relative humidity in % (0-100) (time series).

    Returns:
        Wet-bulb temperature in °C (time series).
    """
    rh = rh_pct.clip(min=0.0, max=100.0)
    # Stull (2011) approximation
    return (
        t_c * np.arctan(0.151977 * np.sqrt(rh + 8.313659))
        + np.arctan(t_c + rh)
        - np.arctan(rh - 1.676331)
        + 0.00391838 * (rh ** 1.5) * np.arctan(0.023101 * rh)
        - 4.686035
    )


def _wet_bulb_daily_mean_c(
    tas_da: xr.DataArray,
    hurs_da: xr.DataArray,
    mask: xr.DataArray,
) -> xr.DataArray | None:
    """Return a district-mean daily wet-bulb series in °C using the Stull approximation."""
    tas_k = _get_district_daily_mean(tas_da, mask)
    rh = _get_district_daily_mean(hurs_da, mask)
    if tas_k.sizes.get("time", 0) == 0:
        return None

    tas_k = _drop_feb29_time(tas_k)
    rh = _drop_feb29_time(rh)

    try:
        rh_max = float(rh.max(dim="time", skipna=True).item())
        if rh_max <= 1.5:
            rh = rh * 100.0
    except Exception:
        pass

    return _wet_bulb_stull_c(tas_k - 273.15, rh)


def _wet_bulb_depression_daily_mean_c(
    tas_da: xr.DataArray,
    hurs_da: xr.DataArray,
    mask: xr.DataArray,
) -> xr.DataArray | None:
    """Return a district-mean daily wet-bulb depression series in °C."""
    twb_c = _wet_bulb_daily_mean_c(tas_da, hurs_da, mask)
    if twb_c is None:
        return None

    tas_k = _get_district_daily_mean(tas_da, mask)
    if tas_k.sizes.get("time", 0) == 0:
        return None
    tas_k = _drop_feb29_time(tas_k)
    tas_c = tas_k - 273.15
    return tas_c - twb_c


def wet_bulb_annual_mean_stull(tas_da: xr.DataArray, hurs_da: xr.DataArray, mask: xr.DataArray) -> float:
    """Annual mean wet-bulb temperature (°C) using Stull approximation."""
    twb = _wet_bulb_daily_mean_c(tas_da, hurs_da, mask)
    if twb is None:
        return np.nan
    return float(twb.mean(dim="time", skipna=True).item())


def wet_bulb_annual_max_stull(tas_da: xr.DataArray, hurs_da: xr.DataArray, mask: xr.DataArray) -> float:
    """Annual maximum wet-bulb temperature (°C) using Stull approximation."""
    twb = _wet_bulb_daily_mean_c(tas_da, hurs_da, mask)
    if twb is None:
        return np.nan
    return float(twb.max(dim="time", skipna=True).item())


def wet_bulb_seasonal_mean_stull(
    tas_da: xr.DataArray,
    hurs_da: xr.DataArray,
    mask: xr.DataArray,
    months: Sequence[int],
    **kwargs: Any,
) -> float:
    """Seasonal mean wet-bulb temperature (°C) using the Stull approximation."""
    _ = kwargs
    twb = _wet_bulb_daily_mean_c(tas_da, hurs_da, mask)
    if twb is None or "time" not in twb.dims:
        return np.nan
    ds = twb.sel(time=twb["time"].dt.month.isin(months))
    if ds.sizes.get("time", 0) == 0:
        return np.nan
    return float(ds.mean(dim="time", skipna=True).item())

def wet_bulb_days_ge_threshold_stull(
    tas_da: xr.DataArray,
    hurs_da: xr.DataArray,
    mask: xr.DataArray,
    thresh_c: float = 30.0,
    **kwargs: Any,
) -> int:
    """Count of days per year where wet-bulb temperature (°C) is >= `thresh_c` (Stull)."""
    _ = kwargs
    twb = _wet_bulb_daily_mean_c(tas_da, hurs_da, mask)
    if twb is None:
        return 0
    flags = (twb >= float(thresh_c)).fillna(False)
    return int(flags.sum(dim="time").item())


def wet_bulb_depression_days_le_threshold_stull(
    tas_da: xr.DataArray,
    hurs_da: xr.DataArray,
    mask: xr.DataArray,
    thresh_c: float = 3.0,
    **kwargs: Any,
) -> int:
    """Count of days per year where wet-bulb depression (tas - Twb) is <= `thresh_c` (Stull)."""
    _ = kwargs
    wbd = _wet_bulb_depression_daily_mean_c(tas_da, hurs_da, mask)
    if wbd is None:
        return 0
    flags = (wbd <= float(thresh_c)).fillna(False)
    return int(flags.sum(dim="time").item())


def wet_bulb_depression_days_range_stull(
    tas_da: xr.DataArray,
    hurs_da: xr.DataArray,
    mask: xr.DataArray,
    lower_c: float,
    upper_c: float,
    lower_inclusive: bool = False,
    upper_inclusive: bool = True,
    **kwargs: Any,
) -> int:
    """Count of days where wet-bulb depression falls within a configured °C range."""
    _ = kwargs
    wbd = _wet_bulb_depression_daily_mean_c(tas_da, hurs_da, mask)
    if wbd is None:
        return 0

    lower = wbd >= float(lower_c) if lower_inclusive else wbd > float(lower_c)
    upper = wbd <= float(upper_c) if upper_inclusive else wbd < float(upper_c)
    flags = (lower & upper).fillna(False)
    return int(flags.sum(dim="time").item())


def wet_bulb_depression_longest_run_le_threshold_stull(
    tas_da: xr.DataArray,
    hurs_da: xr.DataArray,
    mask: xr.DataArray,
    thresh_c: float = 3.0,
    min_spell_days: int = 3,
    **kwargs: Any,
) -> int:
    """Return the longest humid-heat run with wet-bulb depression at or below the threshold."""
    _ = kwargs
    wbd = _wet_bulb_depression_daily_mean_c(tas_da, hurs_da, mask)
    if wbd is None:
        return 0
    arr = np.asarray((wbd <= float(thresh_c)).fillna(False).values, dtype=bool)
    max_run, _ = _run_length_stats(arr, int(min_spell_days))
    return int(max_run)

# -----------------------------------------------------------------------------
# TEMPERATURE COMPUTE FUNCTIONS
# -----------------------------------------------------------------------------

def count_days_above_threshold(da, mask, thresh_k):
    """
    Count days where the (area-mean) daily series is strictly above a threshold.

    Notes:
      - Intended for temperature thresholds provided in Kelvin (e.g., 303.15 for 30°C).
      - Treats missing values as non-events to avoid NaN-propagation.
    """
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0:
        return 0
    flags = (dm > float(thresh_k)).fillna(False)
    return int(flags.sum(dim="time").item())

def count_days_ge_threshold(da, mask, thresh_k):
    """
    Count days where the (area-mean) daily series is >= a threshold.

    Notes:
      - Intended for temperature thresholds provided in Kelvin.
      - Treats missing values as non-events.
    """
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0:
        return 0
    flags = (dm >= float(thresh_k)).fillna(False)
    return int(flags.sum(dim="time").item())

def count_days_below_threshold(da, mask, thresh_k):
    """
    Count days where the (area-mean) daily series is below a threshold.

    Notes:
      - Assumes temperature inputs are in Kelvin when threshold is provided as Kelvin.
      - Explicitly treats missing values as non-events to avoid NaN-propagation.
    """
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0:
        return 0
    flags = (dm < float(thresh_k)).fillna(False)
    return int(flags.sum(dim="time").item())

def count_days_le_threshold(da, mask, thresh_k):
    """
    Count days where the (area-mean) daily series is at or below a threshold.

    Notes:
      - Intended for threshold definitions that are explicitly inclusive in the methodology.
      - Treats missing values as non-events.
    """
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0:
        return 0
    flags = (dm <= float(thresh_k)).fillna(False)
    return int(flags.sum(dim="time").item())

def annual_mean(da, mask):
    dm = _get_district_daily_mean(da, mask)
    return float(dm.mean(dim="time").item()) - 273.15 if dm.size > 0 else np.nan

def seasonal_mean(da, mask, months):
    dm = da.where(mask)
    if "time" not in dm.dims: raise ValueError("Expected 'time' dimension")
    ds = dm.sel(time=dm["time"].dt.month.isin(months))
    if ds.sizes.get("time", 0) == 0: return np.nan
    daily = ds.mean(dim=("lat", "lon"), skipna=True).dropna(dim="time", how="all")
    return float(daily.mean(dim="time").item()) - 273.15 if daily.size > 0 else np.nan

def seasonal_min(da, mask, months):
    dm = da.where(mask)
    if "time" not in dm.dims: raise ValueError("Expected 'time' dimension")
    ds = dm.sel(time=dm["time"].dt.month.isin(months))
    if ds.sizes.get("time", 0) == 0: return np.nan
    daily = ds.mean(dim=("lat", "lon"), skipna=True).dropna(dim="time", how="all")
    return float(daily.min(dim="time").item()) - 273.15 if daily.size > 0 else np.nan

def annual_max_temperature(da, mask):
    dm = _get_district_daily_mean(da, mask)
    return float(dm.max(dim="time").item()) - 273.15 if dm.size > 0 else np.nan

def annual_min_temperature(da, mask):
    dm = _get_district_daily_mean(da, mask)
    return float(dm.min(dim="time").item()) - 273.15 if dm.size > 0 else np.nan

def longest_consecutive_run_above_threshold(da, mask, thresh_k, min_len=1):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return 0
    arr = np.asarray((dm > float(thresh_k)).fillna(False).values, dtype=bool)
    max_run, _ = _run_length_stats(arr, int(min_len))
    return int(max_run)

def longest_consecutive_run_le_threshold(da, mask, thresh_k, min_len=1):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return 0
    arr = np.asarray((dm <= float(thresh_k)).fillna(False).values, dtype=bool)
    max_run, _ = _run_length_stats(arr, int(min_len))
    return int(max_run)

def consecutive_run_events_above_threshold(da, mask, thresh_k, min_event_days=6):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return 0
    return _count_events(np.asarray((dm > thresh_k).fillna(False).values, dtype=bool), min_event_days)

def percentile_days_above(da, mask, percentile=90, baseline_years=(1985, 2014)):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years, strict=False)
    thresh = float(baseline_dm.quantile(percentile / 100.0).item())
    return 100.0 * (dm > thresh).sum().item() / dm.size

def tx90p_etccdi(
    da,
    mask,
    percentile=90,
    baseline_years=(1961, 1990),
    window_days=5,
    direction="above",
    exceed_ge=True,
    quantile_method="nearest",
    smooth=None,
):
    """
    Placeholder compute_fn for tx90p in ETCCDI style.

    IMPORTANT:
    The *correct* ETCCDI tx90p needs baseline thresholds computed across many years
    (and a +/-window around day-of-year). Because this pipeline processes one-year
    NetCDFs, the real implementation is handled in the TX90P special-case workflow
    (like SPI) inside process_metric_for_model_scenario().

    This function exists so `compute_fn` resolution succeeds if metrics_registry.py
    uses compute="tx90p_etccdi". It will be bypassed by the special-case path.
    """
    # Fallback (not ETCCDI-correct in yearly-file mode):
    dir_norm = str(direction or "").strip().lower()
    if dir_norm in {"below", "lt", "<", "lower"}:
        return percentile_days_below(da, mask, percentile=percentile, baseline_years=baseline_years)
    return percentile_days_above(da, mask, percentile=percentile, baseline_years=baseline_years)

def percentile_days_below(da, mask, percentile=10, baseline_years=(1985, 2014)):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years, strict=False)
    thresh = float(baseline_dm.quantile(percentile / 100.0).item())
    return 100.0 * (dm < thresh).sum().item() / dm.size

def warm_spell_duration_index(
    da,
    mask,
    percentile=90,
    min_spell_days=6,
    baseline_years=(1985, 2014),
    window_days=None,
    quantile_method="nearest",
    exceed_ge=True,
    smooth=None,
    direction="above",
):
    """Legacy per-year WSDI. Prefer the multi-year baseline workflow."""
    if window_days is not None:
        try:
            min_spell_days = int(window_days)
        except Exception:
            pass
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years, strict=True)
    if baseline_dm.size == 0:
        return np.nan
    thresh = float(baseline_dm.quantile(percentile / 100.0).item())
    _, total = _run_length_stats(np.asarray((dm > thresh).fillna(False).values, dtype=bool), min_spell_days)
    return int(total)

def cold_spell_duration_index(
    da,
    mask,
    percentile=10,
    min_spell_days=6,
    baseline_years=(1985, 2014),
    window_days=None,
    quantile_method="nearest",
    exceed_ge=True,
    smooth=None,
    direction="below",
):
    """Legacy per-year CSDI. Prefer the multi-year baseline workflow."""
    if window_days is not None:
        try:
            min_spell_days = int(window_days)
        except Exception:
            pass
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years, strict=True)
    if baseline_dm.size == 0:
        return np.nan
    thresh = float(baseline_dm.quantile(percentile / 100.0).item())
    _, total = _run_length_stats(np.asarray((dm < thresh).fillna(False).values, dtype=bool), min_spell_days)
    return int(total)

def heatwave_duration_index(da, mask, baseline_years=(1985, 2014), delta_c=5.0, abs_thresh_k=313.15, min_spell_days=5):
    """Legacy per-year HWDI. Prefer the baseline-aware multi-year workflow."""
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    baseline_dm = _filter_to_baseline(dm, baseline_years, strict=True)
    if baseline_dm.size == 0:
        return np.nan
    baseline_dm = _drop_feb29_time(baseline_dm)
    base_doy = _dayofyear_noleap(baseline_dm).rename("doy")
    baseline_mean = baseline_dm.groupby(base_doy).mean(dim="time", skipna=True)
    baseline_mean = baseline_mean.reindex(doy=np.arange(1, 366))
    delta_k = float(delta_c)
    thresh = xr.where(baseline_mean + delta_k >= abs_thresh_k, baseline_mean + delta_k, abs_thresh_k)
    eva = _drop_feb29_time(dm)
    eva_doy = _dayofyear_noleap(eva)
    thr_for_days = thresh.sel(doy=eva_doy)
    flags = np.asarray((eva >= thr_for_days).fillna(False).values, dtype=bool)
    _, total_days = _run_length_stats(flags, min_spell_days)
    return float(total_days)

def heatwave_frequency_percentile(
    da,
    mask,
    baseline_years=(1985, 2014),
    pct=90,
    min_spell_days=5,
    window_days=None,
    quantile_method="nearest",
    exceed_ge=True,
    smooth=None,
):
    if window_days is not None:
        try:
            min_spell_days = int(window_days)
        except Exception:
            pass
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years, strict=True)
    if baseline_dm.size == 0:
        return np.nan
    thresh = float(baseline_dm.quantile(pct / 100.0).item())
    _, total = _run_length_stats(np.asarray((dm > thresh).fillna(False).values, dtype=bool), min_spell_days)
    return int(total)

def heatwave_event_count(da, mask, baseline_years=(1985, 2014), delta_c=5.0, abs_thresh_k=313.15, min_spell_days=5):
    """Legacy per-year HWDI event count. Prefer baseline-aware multi-year workflow."""
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    baseline_dm = _filter_to_baseline(dm, baseline_years, strict=True)
    if baseline_dm.size == 0:
        return np.nan
    baseline_dm = _drop_feb29_time(baseline_dm)
    base_doy = _dayofyear_noleap(baseline_dm).rename("doy")
    baseline_mean = baseline_dm.groupby(base_doy).mean(dim="time", skipna=True)
    baseline_mean = baseline_mean.reindex(doy=np.arange(1, 366))
    delta_k = float(delta_c)
    thresh = xr.where(baseline_mean + delta_k >= abs_thresh_k, baseline_mean + delta_k, abs_thresh_k)
    eva = _drop_feb29_time(dm)
    eva_doy = _dayofyear_noleap(eva)
    thr_for_days = thresh.sel(doy=eva_doy)
    flags = np.asarray((eva >= thr_for_days).fillna(False).values, dtype=bool)
    return float(_count_events(flags, min_spell_days))

def heatwave_event_count_percentile(
    da,
    mask,
    baseline_years=(1985, 2014),
    pct=90,
    min_spell_days=5,
    window_days=None,
    quantile_method="nearest",
    exceed_ge=True,
    smooth=None,
):
    if window_days is not None:
        try:
            min_spell_days = int(window_days)
        except Exception:
            pass
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years, strict=True)
    if baseline_dm.size == 0:
        return np.nan
    thresh = float(baseline_dm.quantile(pct / 100.0).item())
    return _count_events(np.asarray((dm > thresh).fillna(False).values, dtype=bool), min_spell_days)

def heatwave_magnitude(da, mask, baseline_years=(1985, 2014), min_spell_days=3):
    """Legacy per-year HWM. Prefer the baseline-aware multi-year workflow."""
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate 90th percentile threshold from baseline period
    baseline_dm = _filter_to_baseline(dm, baseline_years, strict=True)
    if baseline_dm.size == 0:
        return np.nan
    thresh = float(baseline_dm.quantile(0.9).item())
    hw_mask = (dm > thresh).values
    hw_days, spell = [], []
    for i, v in enumerate(hw_mask):
        if v: spell.append(i)
        else:
            if len(spell) >= min_spell_days: hw_days.extend(spell)
            spell = []
    if len(spell) >= min_spell_days: hw_days.extend(spell)
    if not hw_days: return np.nan
    return float(dm.isel(time=hw_days).mean().item()) - 273.15

def heatwave_amplitude(
    da,
    mask,
    baseline_years=(1985, 2014),
    min_spell_days=3,
    pct=90,
    window_days=None,
    quantile_method="nearest",
    exceed_ge=True,
    smooth=None,
):
    """Legacy per-year HWA. Prefer the baseline-aware multi-year workflow."""
    if window_days is not None:
        try:
            min_spell_days = int(window_days)
        except Exception:
            pass
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate 90th percentile threshold from baseline period
    baseline_dm = _filter_to_baseline(dm, baseline_years, strict=True)
    if baseline_dm.size == 0:
        return np.nan
    try:
        thresh = float(baseline_dm.quantile(float(pct) / 100.0).item())
    except Exception:
        thresh = float(baseline_dm.quantile(0.9).item())
    hw_mask = (dm > thresh).values
    spells, spell = [], []
    for i, v in enumerate(hw_mask):
        if v: spell.append(i)
        else:
            if len(spell) >= min_spell_days:
                st = dm.isel(time=spell)
                spells.append((float(st.mean().item()), float(st.max().item())))
            spell = []
    if len(spell) >= min_spell_days:
        st = dm.isel(time=spell)
        spells.append((float(st.mean().item()), float(st.max().item())))
    if not spells: return np.nan
    return max(spells, key=lambda x: x[0])[1] - 273.15

def daily_temperature_range(
    da_tasmax: xr.DataArray,
    da_tasmin: xr.DataArray,
    mask: xr.DataArray,
) -> float:
    """DTR: mean of (tasmax - tasmin) over the year.

    Notes:
        Temperatures are expected in Kelvin (CMIP-style), but the *difference*
        is identical in K and °C.
    """
    tx = _get_district_daily_mean(da_tasmax, mask)
    tn = _get_district_daily_mean(da_tasmin, mask)
    if tx.size == 0 or tn.size == 0:
        return np.nan

    tx, tn = xr.align(tx, tn, join="inner")
    if tx.size == 0:
        return np.nan

    dtr = tx - tn
    return float(dtr.mean(dim="time", skipna=True).item())


def extreme_temperature_range(
    da_tasmax: xr.DataArray,
    da_tasmin: xr.DataArray,
    mask: xr.DataArray,
) -> float:
    """ETR: max(tasmax) - min(tasmin) within the year.

    Notes:
        Temperatures are expected in Kelvin (CMIP-style), but the *difference*
        is identical in K and °C.
    """
    tx = _get_district_daily_mean(da_tasmax, mask)
    tn = _get_district_daily_mean(da_tasmin, mask)
    if tx.size == 0 or tn.size == 0:
        return np.nan

    tx, tn = xr.align(tx, tn, join="inner")
    if tx.size == 0:
        return np.nan

    txx = float(tx.max(dim="time", skipna=True).item())
    tnn = float(tn.min(dim="time", skipna=True).item())
    return txx - tnn

def growing_season_length(da, mask, thresh_k=278.15, min_spell_days=6):
    """
    ETCCDI-style Growing Season Length (GSL).

    Definition (standard):
      - Start: first occurrence of at least `min_spell_days` consecutive days with TG > 5°C.
      - End: first occurrence (after July 1) of at least `min_spell_days` consecutive days with TG < 5°C.
      - GSL = (end_start_index - start_index) if end exists, else (n_days - start_index).

    Notes:
      - Uses a calendar-aware July 1 cutoff instead of `n_days//2`.
      - Avoids the common off-by-one artifact that can yield constant 364.
    """
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0:
        return 0

    # Keep consistent with other day-of-year style workflows
    dm = _drop_feb29_time(dm)

    above = np.asarray((dm > float(thresh_k)).fillna(False).values, dtype=bool)
    below = np.asarray((dm < float(thresh_k)).fillna(False).values, dtype=bool)
    n_days = int(above.size)

    # Find season start: first run of `min_spell_days` above threshold
    start_idx = None
    run = 0
    for i, v in enumerate(above):
        if v:
            run += 1
            if run >= int(min_spell_days) and start_idx is None:
                start_idx = int(i) - int(min_spell_days) + 1
        else:
            run = 0

    if start_idx is None:
        return 0

    # Find the index corresponding to July 1 (calendar-aware). Fallback to mid-year if needed.
    search_start = n_days // 2
    try:
        months = dm["time"].dt.month.values
        days = dm["time"].dt.day.values
        idxs = np.where((months > 7) | ((months == 7) & (days >= 1)))[0]
        if idxs.size > 0:
            search_start = int(idxs[0])
    except Exception:
        pass

    # Find season end: first run of `min_spell_days` below threshold starting after July 1
    end_start = None
    run = 0
    for i in range(int(search_start), n_days):
        if below[i]:
            run += 1
            if run >= int(min_spell_days):
                end_start = int(i) - int(min_spell_days) + 1
                break
        else:
            run = 0

    gsl = (end_start if end_start is not None else n_days) - int(start_idx)
    return int(max(0, gsl))

# -----------------------------------------------------------------------------
# PRECIPITATION COMPUTE FUNCTIONS
# -----------------------------------------------------------------------------
def count_rainy_days(da, mask, thresh_mm=2.5):
    return int((_get_district_daily_mean(pr_to_mm_per_day(da), mask) > thresh_mm).sum().item())

def rx1day(da, mask):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    return float(dm.max().item()) if dm.size > 0 else np.nan

def rx5day(da, mask, window_days=5):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return np.nan
    return float(dm.rolling(time=window_days, min_periods=window_days).sum().max().item())

def rx5day_events_over_threshold(da, mask, event_thresh_mm=50.0, window_days=5):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return 0
    rolling = dm.rolling(time=window_days, min_periods=window_days).sum()
    return _count_events((rolling >= event_thresh_mm).fillna(False).values, 1)

def simple_daily_intensity_index(da, mask, wet_day_thresh_mm=1.0):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return np.nan
    total, wet_days = float(dm.sum().item()), int((dm >= wet_day_thresh_mm).sum().item())
    return total / wet_days if wet_days > 0 else np.nan

def total_wet_day_precipitation(da, mask, wet_thresh_mm=1.0):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return np.nan
    return float(dm.where(dm >= wet_thresh_mm, drop=True).sum().item())

def consecutive_wet_days(da, mask, wet_thresh_mm=1.0):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return 0
    max_run, _ = _run_length_stats((dm >= wet_thresh_mm).values, 1)
    return int(max_run)

def consecutive_dry_days(da, mask, dry_thresh_mm=1.0):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return 0
    max_run, _ = _run_length_stats((dm < dry_thresh_mm).values, 1)
    return int(max_run)

def consecutive_dry_day_events(da, mask, dry_thresh_mm=1.0, min_event_days=6):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return 0
    return _count_events((dm < dry_thresh_mm).values, min_event_days)

def percentile_precipitation_total(
    da,
    mask,
    percentile=95,
    baseline_years=(1985, 2014),
    quantile_method="nearest",
    exceed_ge=True,
    wet_day_mm: float = 1.0,
):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return np.nan
    wet = dm.where(dm >= float(wet_day_mm), drop=True)
    if wet.size == 0: return 0.0
    # Calculate threshold from baseline period only (wet days in baseline)
    baseline_wet = _filter_to_baseline(wet, baseline_years)
    if baseline_wet.size == 0:
        baseline_wet = wet  # Fall back to full series if no baseline data
    thresh = float(baseline_wet.quantile(percentile / 100.0).item())
    return float(dm.where(dm > thresh, 0).sum().item())

def percentile_precipitation_contribution(
    da,
    mask,
    percentile=95,
    baseline_years=(1985, 2014),
    quantile_method="nearest",
    exceed_ge=True,
    wet_day_mm: float = 1.0,
):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return np.nan
    wet = dm.where(dm >= float(wet_day_mm), drop=True)
    if wet.size == 0: return 0.0
    prcptot = float(wet.sum().item())
    if prcptot <= 0: return 0.0
    # Calculate threshold from baseline period only (wet days in baseline)
    baseline_wet = _filter_to_baseline(wet, baseline_years)
    if baseline_wet.size == 0:
        baseline_wet = wet  # Fall back to full series if no baseline data
    thresh = float(baseline_wet.quantile(percentile / 100.0).item())
    return 100.0 * float(dm.where(dm > thresh, 0).sum().item()) / prcptot

def standardised_precipitation_index(
    da,
    mask,
    scale_months=3,
    baseline_years=(1985, 2014),
    annual_aggregation=None,
    threshold=None,
    quantile_method="nearest",
    exceed_ge=True,
):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return np.nan
    total = float(dm.sum().item())
    # Calculate mean and std from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years)
    mean_p, std_p = float(baseline_dm.mean().item()) * 365, float(baseline_dm.std().item()) * np.sqrt(365)
    return (total - mean_p) / std_p if std_p > 0 else 0.0

def standardised_precipitation_evapotranspiration_index(da, mask, scale_months=3, baseline_years=(1985, 2014), annual_aggregation=None):
    return standardised_precipitation_index(da, mask, scale_months, baseline_years, annual_aggregation=annual_aggregation)


# --------------------------------------------------------------------------
# SPI / SPEI (Option A): scientifically standard SPI using monthly accumulations
# --------------------------------------------------------------------------
# NOTE:
# - We keep the legacy per-year compute functions above for unit tests / backwards-compat,
#   but the *pipeline* routes SPI/SPEI slugs through the multi-year workflow below so
#   percentiles/distribution fitting are done correctly on a baseline period.
#
# SPI algorithm (common practice):
# 1) Aggregate daily precipitation to monthly totals.
# 2) Compute k-month rolling totals (SPI-k).
# 3) Fit a Gamma distribution *per calendar month* on the baseline period (with zero handling).
# 4) Convert Gamma CDF to a standard normal deviate (SPI).
#
# For future scenarios: fit parameters from the model's HISTORICAL baseline period
# (same model, baseline_years) and apply to the scenario series.

SPI_COMPUTE_NAMES = {
    "standardised_precipitation_index",
    "standardised_precipitation_evapotranspiration_index",  # currently treated as SPI (see SPEI note below)
}

TX90P_COMPUTE_NAMES = {
    "tx90p_etccdi",
}

SPELL_COMPUTE_NAMES = {
    "warm_spell_duration_index",
    "cold_spell_duration_index",
}

HEATWAVE_PERCENTILE_COMPUTE_NAMES = {
    "heatwave_frequency_percentile",
    "heatwave_event_count_percentile",
}

HEATWAVE_BASELINE_COMPUTE_NAMES = {
    "heatwave_amplitude",
    "heatwave_magnitude",
}

HEATWAVE_DELTA_COMPUTE_NAMES = {
    "heatwave_duration_index",
    "heatwave_event_count",
}

PRECIP_PERCENTILE_COMPUTE_NAMES = {
    "percentile_precipitation_total",
    "percentile_precipitation_contribution",
}

def _require_scipy_stats():
    """Import scipy.stats only when SPI/SPEI is requested (keeps base pipeline lighter)."""
    try:
        from scipy.stats import gamma as _gamma_dist  # type: ignore
        from scipy.stats import norm as _norm_dist    # type: ignore
        return _gamma_dist, _norm_dist
    except Exception as e:
        raise ImportError(
            "SPI/SPEI computation requires SciPy (scipy.stats). "
            "Install (recommended): conda install -c conda-forge scipy "
            "or: pip install scipy"
        ) from e


def _collect_monthly_totals_by_unit(
    year_to_paths: dict[int, dict[str, Path]],
    varname: str,
    masks: dict[str, xr.DataArray],
) -> dict[str, xr.DataArray]:
    """
    For each unit mask, build a continuous monthly total series across all available years.
    Returns: {unit_key: DataArray(time=monthly)}
    """
    out: dict[str, list[xr.DataArray]] = {k: [] for k in masks.keys()}
    if not year_to_paths:
        return {k: xr.DataArray([], dims=("time",)) for k in masks.keys()}

    for year in sorted(year_to_paths.keys()):
        p = year_to_paths[year].get(varname)
        if p is None:
            continue
        ds = None
        try:
            ds = normalize_lat_lon(xr.open_dataset(p))
            if varname not in ds:
                continue
            da = ds[varname]
            # SPI expects precipitation totals; convert units if needed.
            if varname == "pr":
                da = pr_to_mm_per_day(da)

            # compute monthly totals per unit
            for unit_key, mask in masks.items():
                dm = _get_district_daily_mean(da, mask)
                if dm.sizes.get("time", 0) == 0:
                    continue
                # monthly totals (month-start index)
                mon = dm.resample(time="MS").sum(dim="time", skipna=True)
                if mon.sizes.get("time", 0) == 0:
                    continue
                out[unit_key].append(mon)
        except Exception:
            # keep going; caller will see gaps as missing months/years
            continue
        finally:
            try:
                if ds is not None:
                    ds.close()
            except Exception:
                pass

    stitched: dict[str, xr.DataArray] = {}
    for unit_key, parts in out.items():
        if not parts:
            stitched[unit_key] = xr.DataArray([], dims=("time",))
            continue
        s = xr.concat(parts, dim="time")
        # ensure time sorted + unique
        s = s.sortby("time")
        # drop duplicates if any
        _, idx = np.unique(s["time"].values, return_index=True)
        stitched[unit_key] = s.isel(time=np.sort(idx))
    return stitched


def _fit_spi_gamma_params_by_month(
    monthly_accum: xr.DataArray,
    baseline_years: tuple[int, int],
    min_samples_per_month: int = 20,
) -> dict[int, dict]:
    """
    Fit Gamma params for each calendar month on baseline years, with zero handling.
    Returns dict: month -> {"method": "gamma", "shape":..., "scale":..., "q":...}
    Fallback method per month: {"method": "normal", "mean":..., "std":...}
    """
    gamma_dist, _ = _require_scipy_stats()

    if monthly_accum.sizes.get("time", 0) == 0 or "time" not in monthly_accum.dims:
        return {}

    y0, y1 = baseline_years
    try:
        years = monthly_accum["time"].dt.year
        base = monthly_accum.where((years >= y0) & (years <= y1), drop=True)
    except Exception:
        base = monthly_accum

    # If baseline slice is empty, fall back to full series
    if base.sizes.get("time", 0) == 0:
        base = monthly_accum

    params_by_month: dict[int, dict] = {}
    for m in range(1, 13):
        try:
            samp = base.where(base["time"].dt.month == m, drop=True).values
        except Exception:
            continue
        samp = np.asarray(samp, dtype=float)
        samp = samp[np.isfinite(samp)]
        if samp.size < max(5, min_samples_per_month // 4):
            continue

        # zero handling (including exact 0)
        zeros = np.sum(samp <= 0.0)
        q = float(zeros) / float(samp.size) if samp.size else 0.0
        nz = samp[samp > 0.0]

        if nz.size >= min_samples_per_month:
            try:
                # standard SPI: Gamma with loc=0 on positive values
                shape, loc, scale = gamma_dist.fit(nz, floc=0.0)
                if not (np.isfinite(shape) and np.isfinite(scale) and scale > 0 and shape > 0):
                    raise ValueError("non-finite gamma params")
                params_by_month[m] = {"method": "gamma", "shape": float(shape), "scale": float(scale), "q": float(q)}
                continue
            except Exception:
                pass  # fall back to normal below

        # fallback: z-score on baseline for that month
        mu = float(np.mean(samp)) if samp.size else 0.0
        sd = float(np.std(samp, ddof=0)) if samp.size else 0.0
        params_by_month[m] = {"method": "normal", "mean": mu, "std": sd, "q": float(q)}

    return params_by_month


def _spi_from_monthly_accum(
    monthly_accum: xr.DataArray,
    params_by_month: dict[int, dict],
    clip_prob: float = 1e-6,
) -> xr.DataArray:
    """Convert monthly accumulated precipitation to SPI values using fitted parameters."""
    gamma_dist, norm_dist = _require_scipy_stats()

    if monthly_accum.sizes.get("time", 0) == 0 or not params_by_month:
        return xr.full_like(monthly_accum, np.nan)

    months = monthly_accum["time"].dt.month.values
    vals = monthly_accum.values.astype(float)

    out = np.full(vals.shape, np.nan, dtype=float)
    for i, (x, m) in enumerate(zip(vals, months)):
        if not np.isfinite(x):
            continue
        p = params_by_month.get(int(m))
        if not p:
            continue

        q = float(p.get("q", 0.0))
        if p["method"] == "gamma":
            shape = float(p["shape"])
            scale = float(p["scale"])
            # Gamma CDF on x (note: if x<=0, cdf=0)
            g = float(gamma_dist.cdf(max(0.0, x), a=shape, loc=0.0, scale=scale))
            H = q + (1.0 - q) * g
        else:
            mu = float(p.get("mean", 0.0))
            sd = float(p.get("std", 0.0))
            if sd <= 0:
                # degenerate distribution; treat as "no anomaly"
                H = 0.5
            else:
                # convert z-score to probability
                z = (x - mu) / sd
                H = float(norm_dist.cdf(z))

        # Avoid infs from ppf(0/1)
        H = min(1.0 - clip_prob, max(clip_prob, H))
        out[i] = float(norm_dist.ppf(H))

    return xr.DataArray(out, coords=monthly_accum.coords, dims=monthly_accum.dims, name="spi")


def _annualize_spi(
    spi_monthly: xr.DataArray,
    min_months_per_year: int = 9,
    annual_aggregation: str = "mean",
    threshold: float | None = None,
) -> xr.DataArray:
    """
    Aggregate monthly SPI to annual values with a minimum months threshold.

    Supported annual_aggregation:
      - "mean": annual mean SPI over available months
      - "count_months_lt": count months with SPI < threshold
      - "count_months_gt": count months with SPI > threshold
      - "count_events_lt": count contiguous SPI < threshold monthly runs
    """
    if spi_monthly.sizes.get("time", 0) == 0:
        return xr.DataArray([], dims=("year",))

    grp = spi_monthly.groupby("time.year")
    n_valid = grp.count(dim="time")

    annual_aggregation = str(annual_aggregation or "mean").strip().lower()

    if annual_aggregation == "mean":
        out = grp.mean(dim="time", skipna=True)
        out = out.where(n_valid >= int(min_months_per_year), drop=True)
        out.name = "spi_yearly"
        return out

    if annual_aggregation in {"count_months_lt", "count_months_gt"}:
        if threshold is None:
            raise ValueError(f"annual_aggregation='{annual_aggregation}' requires a numeric threshold")

        if annual_aggregation == "count_months_lt":
            flags = (spi_monthly < float(threshold)).fillna(False)
        else:
            flags = (spi_monthly > float(threshold)).fillna(False)

        out = flags.groupby("time.year").sum(dim="time")
        out = out.where(n_valid >= int(min_months_per_year), drop=True)
        out.name = "spi_yearly"
        return out

    if annual_aggregation == "count_events_lt":
        if threshold is None:
            raise ValueError(f"annual_aggregation='{annual_aggregation}' requires a numeric threshold")

        flags = (spi_monthly < float(threshold)).fillna(False)
        years = flags.groupby("time.year")
        n_valid = grp.count(dim="time")

        event_rows: list[tuple[int, int]] = []
        for year, year_flags in years:
            valid_months = int(n_valid.sel(year=year).item())
            if valid_months < int(min_months_per_year):
                continue
            event_rows.append(
                (
                    int(year),
                    int(_count_events(np.asarray(year_flags.values, dtype=bool), min_len=1)),
                )
            )

        if not event_rows:
            return xr.DataArray([], dims=("year",))

        out = xr.DataArray(
            [count for _, count in event_rows],
            coords={"year": [year for year, _ in event_rows]},
            dims=("year",),
            name="spi_yearly",
        )
        return out

    raise ValueError(f"Unsupported annual_aggregation='{annual_aggregation}'")


def _compute_spi_spei_rows(
    metric: dict,
    model: str,
    scenario: str,
    scenario_conf: dict,
    year_to_paths: dict[int, dict[str, Path]],
    masks: dict[str, xr.DataArray],
    level: AdminLevel,
    baseline_years: tuple[int, int],
    scale_months: int,
) -> list[dict]:
    """
    Multi-year SPI/SPEI path.
    - For historical: fit on (baseline_years) within historical and compute SPI for all available years.
    - For SSPs: fit on the model's historical baseline and apply to scenario monthly accumulations.
    """
    slug = metric["slug"]
    value_col = metric["value_col"]
    varname = (metric.get("var") or "pr").strip()

    if varname != "pr":
        logging.warning(f"[{slug}] SPI path expects var 'pr' (got '{varname}'); proceeding anyway.")
    scale_months = int(scale_months)

    # Collect monthly totals for the target scenario series
    scen_monthly_by_unit = _collect_monthly_totals_by_unit(year_to_paths, varname, masks)

    # Build calibration (baseline) year_to_paths (historical of same model), unless scenario itself is historical.
    calib_year_to_paths = year_to_paths
    if scenario != "historical":
        hist_conf = SCENARIOS.get("historical")
        if hist_conf is None:
            logging.warning(f"[{slug}] No 'historical' scenario configured; using scenario series for calibration.")
        else:
            hist_dir = var_data_dir(DATA_ROOT, hist_conf["subdir"], varname, model)
            if not hist_dir.exists():
                logging.warning(f"[{slug}] Historical dir missing for calibration: {hist_dir}. Using scenario series.")
            else:
                valid_year_files, _ = validated_year_files_for_var(hist_dir, varname)
                if not valid_year_files:
                    logging.warning(f"[{slug}] No valid historical files for calibration in {hist_dir}. Using scenario series.")
                else:
                    calib_year_to_paths = {y: {varname: p} for y, p in valid_year_files.items()}

    calib_monthly_by_unit = (
        scen_monthly_by_unit if calib_year_to_paths is year_to_paths
        else _collect_monthly_totals_by_unit(calib_year_to_paths, varname, masks)
    )

    # Compute SPI per unit
    rows: list[dict] = []
    min_months_per_year = int(metric.get("params", {}).get("min_months_per_year", 9))

    for unit_key in masks.keys():
        scen_mon = scen_monthly_by_unit.get(unit_key)
        calib_mon = calib_monthly_by_unit.get(unit_key)

        if scen_mon is None or scen_mon.sizes.get("time", 0) == 0:
            continue
        if calib_mon is None or calib_mon.sizes.get("time", 0) == 0:
            continue

        # k-month rolling totals
        scen_acc = scen_mon.rolling(time=scale_months, min_periods=scale_months).sum()
        calib_acc = calib_mon.rolling(time=scale_months, min_periods=scale_months).sum()

        # Fit distribution on baseline and transform scenario
        params_by_month = _fit_spi_gamma_params_by_month(calib_acc, baseline_years=baseline_years)
        if not params_by_month:
            logging.warning(f"[{slug}] Could not fit SPI params for unit={unit_key} (baseline={baseline_years}); skipping.")
            continue

        spi_mon = _spi_from_monthly_accum(scen_acc, params_by_month)
        annual_aggregation = (metric.get("params", {}) or {}).get("annual_aggregation", "mean")
        threshold = (metric.get("params", {}) or {}).get("threshold", None)

        spi_yearly = _annualize_spi(
            spi_mon,
            min_months_per_year=min_months_per_year,
            annual_aggregation=annual_aggregation,
            threshold=threshold,
        )

        if spi_yearly.sizes.get("year", 0) == 0:
            continue

        # Emit rows in the same schema as other metrics
        for y in spi_yearly["year"].values:
            y_int = int(y)
            v = float(spi_yearly.sel(year=y).item())
            source_path = ""
            try:
                source_path = str(year_to_paths.get(y_int, {}).get(varname, ""))
            except Exception:
                source_path = ""
            row = {
                "year": y_int,
                "value": v,
                value_col: v,
                "source_file": source_path,
            }

            _add_unit_fields_from_key(row, unit_key, level)

            rows.append(row)

    # SPEI note: true SPEI requires a PET formulation and water-balance distribution (commonly log-logistic).
    # In the current registry, SPEI metrics are wired to 'pr' only, so they are treated as SPI here as well.
    if metric.get("compute") == "standardised_precipitation_evapotranspiration_index":
        logging.info(f"[{slug}] NOTE: SPEI is currently computed as SPI (registry uses only 'pr').")

    return rows

# ----------------------------
# TX90P (ETCCDI-style) WORKFLOW (multi-year baseline)
# ----------------------------
def _drop_feb29_time(da: xr.DataArray) -> xr.DataArray:
    """Drop Feb 29 (works for datetime64 + cftime)."""
    month = da["time"].dt.month
    day = da["time"].dt.day
    keep = ~((month == 2) & (day == 29))
    return da.sel(time=da["time"][keep])

def _dayofyear_noleap(da: xr.DataArray) -> xr.DataArray:
    """
    Day-of-year on a 365-day basis:
    for leap years, subtract 1 from days after Feb 28 so Mar 1 aligns.
    """
    t = da["time"]
    doy = t.dt.dayofyear
    # xarray supports .dt.is_leap_year for both datetime64 and cftime
    is_leap = t.dt.is_leap_year
    after_feb = t.dt.month > 2
    adj = xr.where(is_leap & after_feb, 1, 0)
    return doy - adj

def _quantile_compat(da: xr.DataArray, q: float, dim: str, method: str = "nearest") -> xr.DataArray:
    """
    Quantile with compatibility across xarray versions:
    newer: method=...
    older: interpolation=...
    """
    try:
        return da.quantile(q, dim=dim, skipna=True, method=method)
    except TypeError:
        return da.quantile(q, dim=dim, skipna=True, interpolation=method)

def _smooth_doy_wrap(thresh: xr.DataArray, smooth: int) -> xr.DataArray:
    """
    Optional smoothing across doy with wrap-around.
    smooth must be odd (e.g., 5, 11). Applies rolling mean.
    """
    if smooth is None:
        return thresh
    if smooth < 3 or smooth % 2 != 1:
        raise ValueError("smooth must be an odd int >= 3, or None.")
    half = smooth // 2
    left = thresh.isel(doy=slice(-half, None))
    right = thresh.isel(doy=slice(0, half))
    padded = xr.concat([left, thresh, right], dim="doy_pad")
    sm = padded.rolling(doy_pad=smooth, center=True).mean()
    sm = sm.isel(doy_pad=slice(half, half + thresh.sizes["doy"]))
    sm = sm.rename({"doy_pad": "doy"})
    sm = sm.assign_coords(doy=thresh["doy"].values)
    return sm

def _collect_daily_mean_by_unit(
    year_to_paths: dict[int, dict[str, Path]],
    varname: str,
    masks: dict[str, xr.DataArray],
    years_subset: list[int] | None = None,
) -> dict[str, xr.DataArray]:
    """
    Collect daily area-mean series for each unit across many years by concatenating time.
    """
    years = sorted(years_subset) if years_subset is not None else sorted(year_to_paths.keys())
    series_parts: dict[str, list[xr.DataArray]] = {u: [] for u in masks.keys()}

    for year in years:
        p = year_to_paths.get(year, {}).get(varname)
        if p is None or (not p.exists()):
            continue

        time_coder = xr.coders.CFDatetimeCoder(use_cftime=True)
        ds = xr.open_dataset(p, decode_times=time_coder)
        ds = normalize_lat_lon(ds)
        if varname not in ds:
            continue

        da = ds[varname]

        # IMPORTANT: Convert precipitation from kg m-2 s-1 to mm/day BEFORE any spatial averaging.
        # Many xarray reductions (e.g., mean) drop attrs like "units", which would break pr_to_mm_per_day()
        # if conversion is attempted later.
        if varname == "pr":
            da = pr_to_mm_per_day(da)

        da = _drop_feb29_time(da)

        for unit, mask in masks.items():
            dm = _get_district_daily_mean(da, mask)  # -> time series
            if dm.size > 0:
                series_parts[unit].append(dm)

    out: dict[str, xr.DataArray] = {}
    for unit, parts in series_parts.items():
        if not parts:
            out[unit] = xr.DataArray(np.array([]), dims=("time",), coords={"time": []})
        else:
            out[unit] = xr.concat(parts, dim="time")

    return out

def _resolve_baseline_year_to_paths(
    metric: dict,
    primary_var: str,
    model: str,
    scenario: str,
    scenario_conf: dict,
    year_to_paths: dict[int, dict[str, Path]],
) -> tuple[dict[int, dict[str, Path]], bool]:
    """
    Resolve baseline-year files for a metric, enforcing historical-only baselines
    for non-historical scenarios. Returns (baseline_year_to_paths, missing_baseline).
    """
    if scenario == "historical":
        return year_to_paths, False

    hist_conf = scenario_conf.get("historical")
    if not hist_conf:
        logging.warning(f"[{metric['slug']}] Missing historical scenario config for baseline.")
        return {}, True

    hist_dir = var_data_dir(DATA_ROOT, hist_conf["subdir"], primary_var, model)
    if not hist_dir.exists():
        logging.warning(f"[{metric['slug']}] Missing historical dir for baseline: {hist_dir}.")
        return {}, True

    valid_year_files, _ = validated_year_files_for_var(hist_dir, primary_var)
    if not valid_year_files:
        logging.warning(f"[{metric['slug']}] No valid historical yearly files for baseline in: {hist_dir}.")
        return {}, True

    baseline_year_to_paths = {y: {primary_var: p} for y, p in valid_year_files.items()}
    return baseline_year_to_paths, False

def _compute_doy_percentile_thresholds(
    base: xr.DataArray,
    pct: int,
    window_days: int,
    quantile_method: str,
    smooth: int | None,
) -> xr.DataArray | None:
    """
    Build day-of-year percentile thresholds (365-day) from a baseline series.
    """
    base = _drop_feb29_time(base)
    if base.size == 0:
        return None
    base_doy = _dayofyear_noleap(base)
    doys = np.arange(1, 366)

    if window_days % 2 != 1:
        raise ValueError("window_days must be odd (e.g., 5, 7, 11).")
    half = window_days // 2
    q = pct / 100.0

    thr_list = []
    for d in doys:
        win = np.arange(d - half, d + half + 1)
        win = np.where(win < 1, win + 365, win)
        win = np.where(win > 365, win - 365, win)
        mask = base_doy.isin(win)
        base_win = base.where(mask, drop=True)
        if base_win.size == 0:
            return None
        thr = _quantile_compat(base_win, q=q, dim="time", method=quantile_method)
        if getattr(thr, "size", 0) == 0:
            return None
        thr_list.append(thr)

    if not thr_list:
        return None
    thresh = xr.concat(thr_list, dim="doy").assign_coords(doy=doys)
    return _smooth_doy_wrap(thresh, smooth=smooth)

def _spell_indices(flags: np.ndarray, min_spell_days: int) -> list[list[int]]:
    """
    Return index lists for spells of at least min_spell_days from a boolean mask.
    """
    spells: list[list[int]] = []
    run: list[int] = []
    for i, v in enumerate(flags):
        if v:
            run.append(i)
        else:
            if len(run) >= min_spell_days:
                spells.append(run)
            run = []
    if len(run) >= min_spell_days:
        spells.append(run)
    return spells

def _compute_tx90p_etccdi_yearly(
    series: xr.DataArray,
    baseline_years: tuple[int, int],
    eval_years: list[int],
    percentile: int = 90,
    window_days: int = 5,
    exceed_ge: bool = True,
    quantile_method: str = "nearest",
    smooth: int | None = None,
    direction: str = "above",
) -> dict[int, float]:
    """
    Compute ETCCDI-style percentile-day index (yearly % of days meeting a threshold)
    from a daily series.

    This generalizes TX90p/TN90p (direction="above") and TX10p/TN10p (direction="below"):

      - Baseline thresholds vary by day-of-year (365-day basis)
      - Thresholds use +/- window around day-of-year
      - Feb 29 is dropped and doy is computed on a no-leap basis

    Args:
        series: Daily series (area-mean) with time coordinate.
        baseline_years: (start_year, end_year) inclusive.
        eval_years: Years to compute yearly percentages for.
        percentile: Percentile (e.g., 90 for warm extremes, 10 for cool extremes).
        window_days: Odd integer window length for day-of-year percentile thresholds.
        exceed_ge:
            For direction="above":
              - True  -> count days where value >= threshold
              - False -> count days where value >  threshold
            For direction="below":
              - True  -> count days where value <= threshold
              - False -> count days where value <  threshold
        quantile_method: Quantile method passed to _quantile_compat.
        smooth: Optional smoothing window (odd integer) applied to doy thresholds.
        direction: "above" or "below"
    """
    if series.size == 0:
        return {y: np.nan for y in eval_years}

    direction = str(direction).strip().lower()
    if direction not in {"above", "below"}:
        raise ValueError(f"Invalid direction='{direction}'. Expected 'above' or 'below'.")

    years = series["time"].dt.year
    bs, be = baseline_years

    base = series.sel(time=series["time"][(years >= bs) & (years <= be)])
    if base.size == 0:
        return {y: np.nan for y in eval_years}

    thresh = _compute_doy_percentile_thresholds(
        base=base,
        pct=percentile,
        window_days=window_days,
        quantile_method=quantile_method,
        smooth=smooth,
    )
    if thresh is None:
        return {y: np.nan for y in eval_years}

    out: dict[int, float] = {}
    for y in eval_years:
        eva = series.sel(time=series["time"][years == y])
        if eva.size == 0:
            out[y] = np.nan
            continue

        eva = _drop_feb29_time(eva)
        eva_doy = _dayofyear_noleap(eva)

        thr_for_days = thresh.sel(doy=eva_doy)

        if direction == "above":
            if exceed_ge:
                sel = eva >= thr_for_days
            else:
                sel = eva > thr_for_days
        else:
            if exceed_ge:
                sel = eva <= thr_for_days
            else:
                sel = eva < thr_for_days

        out[y] = float(sel.mean(skipna=True).values * 100.0)

    return out

def _compute_tx90p_rows_for_metric(
    metric: dict,
    model: str,
    scenario: str,
    scenario_conf: dict,
    year_to_paths: dict[int, dict[str, Path]],
    masks: dict[str, xr.DataArray],
    level: AdminLevel = "district",
) -> list[dict]:
    """
    SPI-like special-case: compute tx90p using multi-year baseline thresholds.
    """
    params = metric.get("params", {}) or {}

    baseline_years = tuple(params.get("baseline_years", (1961, 1990)))
    percentile = int(params.get("percentile", 90))
    window_days = int(params.get("window_days", 5))
    exceed_ge = bool(params.get("exceed_ge", True))
    quantile_method = str(params.get("quantile_method", "nearest"))
    direction = str(params.get("direction", "above"))
    smooth = params.get("smooth", None)
    if smooth is not None:
        smooth = int(smooth)

    # Which variable are we using?
    # Prefer tasmax if present in the yearly files; otherwise use metric["var"].
    primary_var = metric["var"]

    # Eval years = years we are actually producing outputs for in this scenario
    eval_years = sorted(year_to_paths.keys())

    # Baseline source:
    # - historical: baseline is from historical itself
    # - non-historical: baseline must come from historical data directory
    #   (same pattern you used for SPI)
    if scenario == "historical":
        baseline_year_to_paths = year_to_paths
        baseline_var = primary_var
    else:
        # For SSP scenarios: thresholds must be calibrated from the model's HISTORICAL baseline years
        hist_conf = scenario_conf.get("historical")
        if not hist_conf:
            # Fallback (shouldn't happen if SCENARIOS has 'historical')
            baseline_year_to_paths = year_to_paths
            baseline_var = primary_var
        else:
            hist_dir = var_data_dir(DATA_ROOT, hist_conf["subdir"], primary_var, model)
            if not hist_dir.exists():
                logging.warning(f"[{metric['slug']}] Missing historical dir for baseline: {hist_dir}. Using scenario series.")
                baseline_year_to_paths = year_to_paths
            else:
                valid_year_files, _ = validated_year_files_for_var(hist_dir, primary_var)
                if not valid_year_files:
                    logging.warning(f"[{metric['slug']}] No valid historical yearly files for baseline in: {hist_dir}. Using scenario series.")
                    baseline_year_to_paths = year_to_paths
                else:
                    baseline_year_to_paths = {y: {primary_var: p} for y, p in valid_year_files.items()}
            baseline_var = primary_var

    # Collect baseline + eval daily series per unit
    # (we collect from *both* sources if scenario != historical)
    # For historical scenario, these dicts point to same.
    baseline_series_by_unit = _collect_daily_mean_by_unit(
        baseline_year_to_paths, baseline_var, masks
    )

    if scenario == "historical":
        full_series_by_unit = baseline_series_by_unit
    else:
        eval_series_by_unit = _collect_daily_mean_by_unit(
            year_to_paths, primary_var, masks
        )
        full_series_by_unit = {}
        for unit in masks.keys():
            b = baseline_series_by_unit.get(unit)
            e = eval_series_by_unit.get(unit)
            if (b is None) or (b.size == 0):
                full_series_by_unit[unit] = e
            elif (e is None) or (e.size == 0):
                full_series_by_unit[unit] = b
            else:
                full_series_by_unit[unit] = xr.concat([b, e], dim="time")

    rows: list[dict] = []
    for unit, series in full_series_by_unit.items():
        year_vals = _compute_tx90p_etccdi_yearly(
            series=series,
            baseline_years=baseline_years,
            eval_years=eval_years,
            percentile=percentile,
            window_days=window_days,
            exceed_ge=exceed_ge,
            quantile_method=quantile_method,
            smooth=smooth,
            direction=direction,
        )
        value_col = metric["value_col"]

        for year in eval_years:
            v = float(year_vals.get(year, np.nan))

            row = {
                "year": int(year),
                "value": v,
                value_col: v,
                "source_file": "",  # optional: you can fill with a representative file if you want
            }

            _add_unit_fields_from_key(row, unit, level)
            rows.append(row)

    return rows

def _compute_spell_duration_rows_for_metric(
    metric: dict,
    model: str,
    scenario: str,
    scenario_conf: dict,
    year_to_paths: dict[int, dict[str, Path]],
    masks: dict[str, xr.DataArray],
    level: AdminLevel = "district",
) -> list[dict]:
    """
    Special-case: warm/cold spell duration indices with ETCCDI-style
    day-of-year percentile thresholds (multi-year baseline).
    """
    params = metric.get("params", {}) or {}

    compute_name = str(metric.get("compute", "") or "")
    baseline_years = tuple(params.get("baseline_years", (1981, 2010)))
    percentile_default = 90 if compute_name == "warm_spell_duration_index" else 10
    percentile = int(params.get("percentile", percentile_default))
    window_days = int(params.get("window_days", 5))
    quantile_method = str(params.get("quantile_method", "nearest"))
    exceed_ge = bool(params.get("exceed_ge", True))
    smooth = params.get("smooth", None)
    if smooth is not None:
        smooth = int(smooth)
    min_spell_days = int(params.get("min_spell_days", 6))
    direction = str(params.get("direction") or ("above" if compute_name == "warm_spell_duration_index" else "below"))
    if direction not in {"above", "below"}:
        raise ValueError(f"Invalid direction='{direction}'. Expected 'above' or 'below'.")

    primary_var = metric["var"]
    eval_years = sorted(year_to_paths.keys())

    baseline_year_to_paths, _ = _resolve_baseline_year_to_paths(
        metric=metric,
        primary_var=primary_var,
        model=model,
        scenario=scenario,
        scenario_conf=scenario_conf,
        year_to_paths=year_to_paths,
    )
    baseline_var = primary_var

    baseline_series_by_unit = _collect_daily_mean_by_unit(
        baseline_year_to_paths, baseline_var, masks
    )

    if scenario == "historical":
        eval_series_by_unit = baseline_series_by_unit
    else:
        eval_series_by_unit = _collect_daily_mean_by_unit(
            year_to_paths, primary_var, masks
        )

    rows: list[dict] = []
    value_col = metric["value_col"]

    for unit in masks.keys():
        base_series = baseline_series_by_unit.get(unit)
        eval_series = eval_series_by_unit.get(unit)

        if base_series is None or base_series.size == 0:
            year_vals = {y: np.nan for y in eval_years}
        else:
            years = base_series["time"].dt.year
            bs, be = baseline_years
            base = base_series.sel(time=base_series["time"][(years >= bs) & (years <= be)])
            if base.size == 0:
                year_vals = {y: np.nan for y in eval_years}
            else:
                thresh = _compute_doy_percentile_thresholds(
                    base=base,
                    pct=percentile,
                    window_days=window_days,
                    quantile_method=quantile_method,
                    smooth=smooth,
                )
                if thresh is None:
                    year_vals = {y: np.nan for y in eval_years}
                elif eval_series is None or eval_series.size == 0:
                    year_vals = {y: np.nan for y in eval_years}
                else:
                    year_vals = {}
                    eval_years_series = eval_series["time"].dt.year
                    for y in eval_years:
                        eva = eval_series.sel(time=eval_series["time"][eval_years_series == y])
                        if eva.size == 0:
                            year_vals[y] = np.nan
                            continue

                        eva = _drop_feb29_time(eva)
                        eva_doy = _dayofyear_noleap(eva)
                        thr_for_days = thresh.sel(doy=eva_doy)

                        if direction == "above":
                            if exceed_ge:
                                flags = np.asarray((eva >= thr_for_days).fillna(False).values, dtype=bool)
                            else:
                                flags = np.asarray((eva > thr_for_days).fillna(False).values, dtype=bool)
                        else:
                            if exceed_ge:
                                flags = np.asarray((eva <= thr_for_days).fillna(False).values, dtype=bool)
                            else:
                                flags = np.asarray((eva < thr_for_days).fillna(False).values, dtype=bool)

                        _, total_days = _run_length_stats(flags, min_spell_days)
                        year_vals[y] = float(total_days)

        for year in eval_years:
            v = float(year_vals.get(year, np.nan))
            row = {
                "year": int(year),
                "value": v,
                value_col: v,
                "source_file": "",
            }
            _add_unit_fields_from_key(row, unit, level)
            rows.append(row)

    return rows

def _compute_heatwave_percentile_rows_for_metric(
    metric: dict,
    model: str,
    scenario: str,
    scenario_conf: dict,
    year_to_paths: dict[int, dict[str, Path]],
    masks: dict[str, xr.DataArray],
    level: AdminLevel = "district",
) -> list[dict]:
    """
    Special-case: heatwave metrics based on a percentile threshold must calibrate
    thresholds from historical baseline years (multi-year), not per-year.

    Uses ETCCDI-style day-of-year thresholds (windowed percentiles) consistent with TX90p/TN90p.
    """
    params = metric.get("params", {}) or {}

    baseline_years = tuple(params.get("baseline_years", (1981, 2010)))
    pct = int(params.get("pct", 90))
    window_days = int(params.get("window_days", 5))
    exceed_ge = bool(params.get("exceed_ge", True))
    quantile_method = str(params.get("quantile_method", "nearest"))
    min_spell_days = int(params.get("min_spell_days", 5))
    smooth = params.get("smooth", None)
    if smooth is not None:
        smooth = int(smooth)

    primary_var = metric["var"]
    eval_years = sorted(year_to_paths.keys())

    baseline_year_to_paths, _ = _resolve_baseline_year_to_paths(
        metric=metric,
        primary_var=primary_var,
        model=model,
        scenario=scenario,
        scenario_conf=scenario_conf,
        year_to_paths=year_to_paths,
    )
    baseline_var = primary_var

    baseline_series_by_unit = _collect_daily_mean_by_unit(
        baseline_year_to_paths, baseline_var, masks
    )

    if scenario == "historical":
        eval_series_by_unit = baseline_series_by_unit
    else:
        eval_series_by_unit = _collect_daily_mean_by_unit(
            year_to_paths, primary_var, masks
        )

    rows: list[dict] = []
    value_col = metric["value_col"]
    compute_name = metric.get("compute")

    for unit in masks.keys():
        base_series = baseline_series_by_unit.get(unit)
        eval_series = eval_series_by_unit.get(unit)
        # Compute day-of-year thresholds from baseline, then evaluate boolean exceedance per year
        if base_series is None or base_series.size == 0:
            year_vals = {y: np.nan for y in eval_years}
        else:
            years = base_series["time"].dt.year
            bs, be = baseline_years
            base = base_series.sel(time=base_series["time"][(years >= bs) & (years <= be)])
            if base.size == 0:
                year_vals = {y: np.nan for y in eval_years}
            else:
                thresh = _compute_doy_percentile_thresholds(
                    base=base,
                    pct=pct,
                    window_days=window_days,
                    quantile_method=quantile_method,
                    smooth=smooth,
                )
                if thresh is None:
                    year_vals = {y: np.nan for y in eval_years}
                else:
                    year_vals = {}
                    for y in eval_years:
                        if eval_series is None or eval_series.size == 0:
                            year_vals[y] = np.nan
                            continue
                        eval_years_series = eval_series["time"].dt.year
                        eva = eval_series.sel(time=eval_series["time"][eval_years_series == y])
                        if eva.size == 0:
                            year_vals[y] = np.nan
                            continue

                        eva = _drop_feb29_time(eva)
                        eva_doy = _dayofyear_noleap(eva)
                        thr_for_days = thresh.sel(doy=eva_doy)

                        if exceed_ge:
                            flags = np.asarray((eva >= thr_for_days).fillna(False).values, dtype=bool)
                        else:
                            flags = np.asarray((eva > thr_for_days).fillna(False).values, dtype=bool)

                        if compute_name == "heatwave_frequency_percentile":
                            _, total_days = _run_length_stats(flags, min_spell_days)
                            year_vals[y] = float(total_days)
                        else:
                            year_vals[y] = float(_count_events(flags, min_spell_days))

        for year in eval_years:
            v = float(year_vals.get(year, np.nan))
            src_map = baseline_year_to_paths if scenario == "historical" else year_to_paths
            src_path = src_map.get(int(year), {}).get(primary_var)
            row = {
                "year": int(year),
                "value": v,
                value_col: v,
                "source_file": str(src_path) if src_path is not None else "",
            }
            _add_unit_fields_from_key(row, unit, level)
            rows.append(row)

    return rows

def _compute_heatwave_baseline_rows_for_metric(
    metric: dict,
    model: str,
    scenario: str,
    scenario_conf: dict,
    year_to_paths: dict[int, dict[str, Path]],
    masks: dict[str, xr.DataArray],
    level: AdminLevel = "district",
) -> list[dict]:
    """
    Special-case: heatwave amplitude/magnitude metrics based on percentile thresholds
    calibrated from historical baseline years (multi-year), applied to evaluation years.

    Definitions:
      - HWM: maximum mean exceedance above the percentile threshold across all spells.
      - HWA: peak daily temperature (°C) within the hottest spell (highest mean exceedance).
    """
    params = metric.get("params", {}) or {}

    baseline_years = tuple(params.get("baseline_years", (1981, 2010)))
    pct = int(params.get("pct", params.get("percentile", 90)))
    window_days = int(params.get("window_days", 5))
    exceed_ge = bool(params.get("exceed_ge", True))
    quantile_method = str(params.get("quantile_method", "nearest"))
    min_spell_days = int(params.get("min_spell_days", 3))
    smooth = params.get("smooth", None)
    if smooth is not None:
        smooth = int(smooth)

    primary_var = metric["var"]
    eval_years = sorted(year_to_paths.keys())
    compute_name = metric.get("compute")

    baseline_year_to_paths, _ = _resolve_baseline_year_to_paths(
        metric=metric,
        primary_var=primary_var,
        model=model,
        scenario=scenario,
        scenario_conf=scenario_conf,
        year_to_paths=year_to_paths,
    )

    baseline_series_by_unit = _collect_daily_mean_by_unit(
        baseline_year_to_paths, primary_var, masks
    )
    if scenario == "historical":
        eval_series_by_unit = baseline_series_by_unit
    else:
        eval_series_by_unit = _collect_daily_mean_by_unit(
            year_to_paths, primary_var, masks
        )

    rows: list[dict] = []
    value_col = metric["value_col"]

    for unit in masks.keys():
        base_series = baseline_series_by_unit.get(unit)
        eval_series = eval_series_by_unit.get(unit)

        if base_series is None or base_series.size == 0:
            year_vals = {y: np.nan for y in eval_years}
        else:
            years = base_series["time"].dt.year
            bs, be = baseline_years
            base = base_series.sel(time=base_series["time"][(years >= bs) & (years <= be)])
            if base.size == 0:
                year_vals = {y: np.nan for y in eval_years}
            else:
                thresh = _compute_doy_percentile_thresholds(
                    base=base,
                    pct=pct,
                    window_days=window_days,
                    quantile_method=quantile_method,
                    smooth=smooth,
                )
                if thresh is None:
                    year_vals = {y: np.nan for y in eval_years}
                else:
                    year_vals = {}
                    for y in eval_years:
                        if eval_series is None or eval_series.size == 0:
                            year_vals[y] = np.nan
                            continue
                        eval_years_series = eval_series["time"].dt.year
                        eva = eval_series.sel(time=eval_series["time"][eval_years_series == y])
                        if eva.size == 0:
                            year_vals[y] = np.nan
                            continue

                        eva = _drop_feb29_time(eva)
                        eva_doy = _dayofyear_noleap(eva)
                        thr_for_days = thresh.sel(doy=eva_doy)

                        if exceed_ge:
                            flags = np.asarray((eva >= thr_for_days).fillna(False).values, dtype=bool)
                        else:
                            flags = np.asarray((eva > thr_for_days).fillna(False).values, dtype=bool)

                        spells = _spell_indices(flags, min_spell_days)
                        if not spells:
                            year_vals[y] = np.nan
                            continue

                        # Track per-spell stats:
                        #  - mean_exceed_k: mean exceedance above threshold (K; same numeric as °C differences)
                        #  - mean_temp_c: mean temperature during the spell (°C)
                        #  - max_temp_c:  peak temperature during the spell (°C)
                        event_stats: list[tuple[float, float, float]] = []
                        for spell in spells:
                            event_t = eva.isel(time=spell)
                            event_thr = thr_for_days.isel(time=spell)

                            mean_exceed_k = float((event_t - event_thr).mean(skipna=True).item())
                            mean_temp_c = float(event_t.mean(skipna=True).item()) - 273.15
                            max_temp_c = float(event_t.max(skipna=True).item()) - 273.15

                            event_stats.append((mean_exceed_k, mean_temp_c, max_temp_c))

                        # Choose the "hottest" spell by mean exceedance above the threshold
                        hottest = max(event_stats, key=lambda x: x[0])

                        # Output:
                        #  - heatwave_magnitude: mean temperature (°C) during the hottest spell
                        #  - heatwave_amplitude: peak temperature (°C) during the hottest spell
                        if compute_name == "heatwave_magnitude":
                            year_vals[y] = float(hottest[0])
                        else:
                            year_vals[y] = float(hottest[2])

        # Emit rows for all evaluation years for this unit
        for year in eval_years:
            v = float(year_vals.get(year, np.nan))

            src_map = baseline_year_to_paths if scenario == "historical" else year_to_paths
            src_path = src_map.get(int(year), {}).get(primary_var)

            row = {
                "year": int(year),
                "value": v,
                value_col: v,
                "source_file": str(src_path) if src_path is not None else "",
            }

            _add_unit_fields_from_key(row, unit, level)

            rows.append(row)

    return rows

def _compute_heatwave_delta_rows_for_metric(
    metric: dict,
    model: str,
    scenario: str,
    scenario_conf: dict,
    year_to_paths: dict[int, dict[str, Path]],
    masks: dict[str, xr.DataArray],
    level: AdminLevel = "district",
) -> list[dict]:
    """
    Special-case: heatwave duration/event metrics using baseline mean + delta thresholds.

    Threshold per day-of-year = max(abs_thresh_k, baseline_mean + delta_c).
    """
    params = metric.get("params", {}) or {}

    baseline_years = tuple(params.get("baseline_years", (1981, 2010)))
    delta_c = float(params.get("delta_c", 5.0))
    abs_thresh_k = float(params.get("abs_thresh_k", 313.15))
    min_spell_days = int(params.get("min_spell_days", 5))

    primary_var = metric["var"]
    eval_years = sorted(year_to_paths.keys())
    compute_name = metric.get("compute")

    baseline_year_to_paths, _ = _resolve_baseline_year_to_paths(
        metric=metric,
        primary_var=primary_var,
        model=model,
        scenario=scenario,
        scenario_conf=scenario_conf,
        year_to_paths=year_to_paths,
    )

    baseline_series_by_unit = _collect_daily_mean_by_unit(
        baseline_year_to_paths, primary_var, masks
    )
    if scenario == "historical":
        eval_series_by_unit = baseline_series_by_unit
        eval_source_year_to_paths = baseline_year_to_paths
    else:
        eval_series_by_unit = _collect_daily_mean_by_unit(
            year_to_paths, primary_var, masks
        )
        eval_source_year_to_paths = year_to_paths

    rows: list[dict] = []
    value_col = metric["value_col"]

    for unit in masks.keys():
        base_series = baseline_series_by_unit.get(unit)
        eval_series = eval_series_by_unit.get(unit)

        if base_series is None or base_series.size == 0:
            year_vals = {y: np.nan for y in eval_years}
        else:
            years = base_series["time"].dt.year
            bs, be = baseline_years
            base = base_series.sel(time=base_series["time"][(years >= bs) & (years <= be)])
            if base.size == 0:
                year_vals = {y: np.nan for y in eval_years}
            else:
                base = _drop_feb29_time(base)
                base_doy = _dayofyear_noleap(base).rename("doy")
                baseline_mean = base.groupby(base_doy).mean(dim="time", skipna=True)
                baseline_mean = baseline_mean.reindex(doy=np.arange(1, 366))
                delta_k = float(delta_c)
                thresh = xr.where(baseline_mean + delta_k >= abs_thresh_k, baseline_mean + delta_k, abs_thresh_k)

                year_vals = {}
                for y in eval_years:
                    if eval_series is None or eval_series.size == 0:
                        year_vals[y] = np.nan
                        continue
                    eval_years_series = eval_series["time"].dt.year
                    eva = eval_series.sel(time=eval_series["time"][eval_years_series == y])
                    if eva.size == 0:
                        year_vals[y] = np.nan
                        continue

                    eva = _drop_feb29_time(eva)
                    eva_doy = _dayofyear_noleap(eva)
                    thr_for_days = thresh.sel(doy=eva_doy)
                    flags = np.asarray((eva >= thr_for_days).fillna(False).values, dtype=bool)

                    if compute_name == "heatwave_duration_index":
                        _, total_days = _run_length_stats(flags, min_spell_days)
                        year_vals[y] = float(total_days)
                    else:
                        year_vals[y] = float(_count_events(flags, min_spell_days))

        for year in eval_years:
            v = float(year_vals.get(year, np.nan))
            row = {
                "year": int(year),
                "value": v,
                value_col: v,
                "source_file": "",
            }
            _add_unit_fields_from_key(row, unit, level)
            rows.append(row)

    return rows


def _compute_precip_percentile_rows_for_metric(
    metric: dict,
    model: str,
    scenario: str,
    scenario_conf: dict,
    year_to_paths: dict[int, dict[str, Path]],
    masks: dict[str, xr.DataArray],
    level: AdminLevel = "district",
) -> list[dict]:
    """
    Special-case: precipitation percentile metrics (R95p, R95pTOT) must calibrate
    wet-day thresholds from historical baseline years (multi-year), not per-year.

    Threshold is computed from baseline wet days (>= wet_day_mm) over baseline years,
    then applied to evaluation years.
    """
    params = metric.get("params", {}) or {}

    baseline_years = tuple(params.get("baseline_years", (1981, 2010)))
    percentile = int(params.get("percentile", 95))
    quantile_method = str(params.get("quantile_method", "nearest"))
    exceed_ge = bool(params.get("exceed_ge", True))
    wet_day_mm = float(params.get("wet_day_mm", 1.0))

    primary_var = metric["var"]
    eval_years = sorted(year_to_paths.keys())

    # Baseline source: historical for SSP scenarios
    if scenario == "historical":
        baseline_year_to_paths = year_to_paths
        baseline_var = primary_var
    else:
        hist_conf = scenario_conf.get("historical")
        if not hist_conf:
            baseline_year_to_paths = year_to_paths
            baseline_var = primary_var
        else:
            hist_dir = var_data_dir(DATA_ROOT, hist_conf["subdir"], primary_var, model)
            if not hist_dir.exists():
                logging.warning(f"[{metric['slug']}] Missing historical dir for baseline: {hist_dir}. Using scenario series.")
                baseline_year_to_paths = year_to_paths
            else:
                valid_year_files, _ = validated_year_files_for_var(hist_dir, primary_var)
                baseline_year_to_paths = {y: {primary_var: p} for y, p in valid_year_files.items()} if valid_year_files else year_to_paths
            baseline_var = primary_var

    baseline_series_by_unit = _collect_daily_mean_by_unit(
        baseline_year_to_paths, baseline_var, masks
    )

    if scenario == "historical":
        full_series_by_unit = baseline_series_by_unit
    else:
        eval_series_by_unit = _collect_daily_mean_by_unit(
            year_to_paths, primary_var, masks
        )
        full_series_by_unit = {}
        for unit in masks.keys():
            b = baseline_series_by_unit.get(unit)
            e = eval_series_by_unit.get(unit)
            if (b is None) or (b.size == 0):
                full_series_by_unit[unit] = e
            elif (e is None) or (e.size == 0):
                full_series_by_unit[unit] = b
            else:
                full_series_by_unit[unit] = xr.concat([b, e], dim="time")

    rows: list[dict] = []
    value_col = metric["value_col"]
    compute_name = metric.get("compute")

    for unit, series_raw in full_series_by_unit.items():
        if series_raw is None or series_raw.size == 0:
            year_vals = {y: np.nan for y in eval_years}
        else:
            # Convert precipitation to mm/day (your helper expects DataArray with units compatible)
            series = pr_to_mm_per_day(series_raw)

            years = series["time"].dt.year
            bs, be = baseline_years

            base = series.sel(time=series["time"][(years >= bs) & (years <= be)])
            wet_base = base.where(base >= wet_day_mm, drop=True)

            if wet_base.size == 0:
                year_vals = {y: np.nan for y in eval_years}
            else:
                q = percentile / 100.0
                thresh = float(_quantile_compat(wet_base, q=q, dim="time", method=quantile_method).item())

                year_vals = {}
                for y in eval_years:
                    eva = series.sel(time=series["time"][years == y])
                    if eva.size == 0:
                        year_vals[y] = np.nan
                        continue

                    wet_eva = eva.where(eva >= wet_day_mm, drop=True)
                    prcptot = float(wet_eva.sum().item()) if wet_eva.size > 0 else 0.0

                    if exceed_ge:
                        exceed = eva >= thresh
                    else:
                        exceed = eva > thresh

                    rpx = float(eva.where(exceed, 0.0).sum().item())

                    if compute_name == "percentile_precipitation_total":
                        year_vals[y] = float(rpx)
                    else:
                        year_vals[y] = float(100.0 * rpx / prcptot) if prcptot > 0 else 0.0

        for year in eval_years:
            v = float(year_vals.get(year, np.nan))
            row = {
                "year": int(year),
                "value": v,
                value_col: v,
                "source_file": "",
            }
            _add_unit_fields_from_key(row, unit, level)
            rows.append(row)

    return rows


def _compute_seasonal_mean_djf_cross_year_rows_for_metric(
    metric: dict,
    model: str,
    scenario: str,
    year_to_paths: dict[int, dict[str, Path]],
    masks: dict[str, xr.DataArray],
    level: AdminLevel,
) -> list[dict]:
    """
    Seasonal mean for DJF computed as:
      Dec (year-1) + Jan/Feb (year)

    This avoids the common per-year-file pitfall where "DJF of year Y" is mistakenly
    computed as Dec(Y) + Jan(Y) + Feb(Y).
    """
    slug = metric["slug"]
    value_col = metric["value_col"]
    varname = metric.get("var")
    months = list((metric.get("params", {}) or {}).get("months", []))

    # Guard: this helper is only for DJF
    if set(months) != {12, 1, 2}:
        raise ValueError(f"[{slug}] DJF cross-year helper called for non-DJF months={months}")

    rows: list[dict] = []
    years = sorted(year_to_paths.keys())

    for unit, mask in masks.items():
        for year in years:
            cur_path = year_to_paths.get(year, {}).get(varname)
            prev_path = year_to_paths.get(year - 1, {}).get(varname)

            # If we don't have the previous year, we can't form DJF correctly.
            if cur_path is None or prev_path is None:
                v = np.nan
            else:
                ds_prev = normalize_lat_lon(xr.open_dataset(prev_path))
                ds_cur = normalize_lat_lon(xr.open_dataset(cur_path))
                try:
                    da_prev = ds_prev[varname]
                    da_cur = ds_cur[varname]

                    # Drop Feb 29 for consistency
                    da_prev = _drop_feb29_time(da_prev)
                    da_cur = _drop_feb29_time(da_cur)

                    # Select Dec(prev) and Jan-Feb(cur)
                    dec_prev = da_prev.sel(time=da_prev["time"].dt.month == 12)
                    jf_cur = da_cur.sel(time=da_cur["time"].dt.month.isin([1, 2]))

                    if dec_prev.sizes.get("time", 0) == 0 or jf_cur.sizes.get("time", 0) == 0:
                        v = np.nan
                    else:
                        da = xr.concat([dec_prev, jf_cur], dim="time")
                        v = float(seasonal_mean(da, mask, months=[12, 1, 2]))
                finally:
                    ds_prev.close()
                    ds_cur.close()

            row = {"year": int(year), "value": float(v), value_col: float(v), "source_file": ""}
            _add_unit_fields_from_key(row, unit, level)
            rows.append(row)

    return rows


# -----------------------------------------------------------------------------
# FILE I/O HELPERS
# -----------------------------------------------------------------------------
def yearly_files_for_dir(dirpath: Path) -> dict:
    out = {}
    for f in glob.glob(str(dirpath / "*.nc")):
        y = os.path.splitext(os.path.basename(f))[0]
        if y.isdigit(): out[int(y)] = Path(f)
    return dict(sorted(out.items()))

def var_data_dir(data_root: Path, scenario_subdir: str, varname: str, model: str) -> Path:
    parts = list(Path(scenario_subdir).parts)
    if not parts: raise ValueError(f"Invalid scenario_subdir: {scenario_subdir}")
    parts[-1] = varname
    return data_root / Path(*parts) / model

def try_open_nc(path: Path, try_engines=("netcdf4", "h5netcdf", "scipy")) -> bool:
    for eng in try_engines:
        try: xr.open_dataset(path, engine=eng).close(); return True
        except: continue
    return False

def validated_year_files(data_dir: Path) -> tuple[dict, dict]:
    year_files = yearly_files_for_dir(data_dir)
    valid, bad = {}, {}
    for year, p in year_files.items():
        try: sz = p.stat().st_size
        except Exception as e: bad[year] = {"path": p, "reason": f"stat_failed: {e}"}; continue
        if sz == 0: bad[year] = {"path": p, "reason": "zero_size"}; continue
        if try_open_nc(p): valid[year] = p
        else: bad[year] = {"path": p, "reason": "open_failed"}
    return dict(sorted(valid.items())), bad


def validated_year_files_for_var(data_dir: Path, varname: str) -> tuple[dict, dict]:
    """Return yearly files that open successfully and contain the requested variable."""
    valid, bad = validated_year_files(data_dir)
    if not valid:
        return valid, bad

    var_valid: dict[int, Path] = {}
    var_bad: dict[int, dict[str, Any]] = dict(bad)
    for year, path in valid.items():
        try:
            ds = normalize_lat_lon(xr.open_dataset(path))
            try:
                if varname in ds and getattr(ds[varname], "size", 0) > 0:
                    var_valid[year] = path
                else:
                    var_bad[year] = {"path": path, "reason": f"missing_variable:{varname}"}
            finally:
                ds.close()
        except Exception as exc:
            var_bad[year] = {"path": path, "reason": f"variable_check_failed:{exc}"}
    return dict(sorted(var_valid.items())), var_bad

def discover_models(data_root: Path, scenarios: dict, variables: list = None) -> list:
    if variables is None: variables = ["tas", "tasmax", "tasmin", "pr"]
    models = set()
    for _, scen_conf in scenarios.items():
        base_parts = Path(scen_conf["subdir"]).parts
        for var in variables:
            model_base = data_root / base_parts[0] / var
            if not model_base.exists(): continue
            for entry in model_base.iterdir():
                if entry.is_dir(): models.add(entry.name)
    return sorted(models)

MODELS = discover_models(DATA_ROOT, SCENARIOS)

def required_vars_for_metric(metric: dict) -> list[str]:
    """Return required CMIP variables for a metric dict (supports multi-var metrics)."""
    vars_field = metric.get("vars")
    if isinstance(vars_field, (list, tuple)) and vars_field:
        return [str(v) for v in vars_field]
    v = metric.get("var")
    return [str(v)] if v else []


COMPUTE_MARKER_SCHEMA_VERSION = 3
ENSEMBLE_MARKER_SCHEMA_VERSION = 3
SKIP_REASON_MISSING_REQUIRED_VARS = "missing_required_vars"
SKIP_REASON_NO_AVAILABLE_YEARS = "no_available_years"
SKIP_REASON_NO_COMMON_YEARS = "no_common_years"
SKIP_REASON_INVALID_SOURCE_FILES = "invalid_source_files"
SKIP_REASON_NO_TASKS_AFTER_FILTERS = "no_tasks_after_filters"


def _scope_name_for_level(level: AdminLevel, state: str) -> str:
    """Return the directory scope name used by compute outputs for one level."""
    return HYDRO_ROOT_NAME if level in {"basin", "sub_basin"} else str(state).strip()


def _hash_common_years(years: Sequence[int]) -> str:
    """Return a stable digest for one task's overlapping yearly files."""
    payload = ",".join(str(int(year)) for year in sorted({int(year) for year in years}))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _boundary_signature(level: AdminLevel, state: str) -> tuple[str, int]:
    """Return the boundary path and mtime used to validate compute markers."""
    _ = state
    boundary_path = get_boundary_path(level).resolve()
    try:
        mtime_ns = boundary_path.stat().st_mtime_ns
    except OSError:
        mtime_ns = 0
    return str(boundary_path), int(mtime_ns)


def _markers_root_for_slug(slug: str) -> Path:
    return metric_root(slug) / ".markers"


def _task_marker_path(*, slug: str, level: AdminLevel, scope_name: str, model: str, scenario: str) -> Path:
    return (
        _markers_root_for_slug(slug)
        / "compute"
        / str(level)
        / f"scope={scope_name}"
        / f"model={model}"
        / f"scenario={scenario}.json"
    )


def _ensemble_marker_path(*, slug: str, level: AdminLevel, scope_name: str) -> Path:
    return (
        _markers_root_for_slug(slug)
        / "ensembles"
        / str(level)
        / f"scope={scope_name}.json"
    )


def _normalize_filter_values(values: Sequence[str] | None) -> tuple[str, ...] | None:
    """Return a stable normalized tuple for marker filter scope comparisons."""
    if values is None:
        return None
    normalized = sorted({str(value).strip() for value in values if str(value).strip()})
    return tuple(normalized)


def _ensemble_filter_scope_payload(
    *,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Return the normalized filter scope stored in ensemble markers."""
    models = _normalize_filter_values(allowed_models)
    scenarios = _normalize_filter_values(allowed_scenarios)
    return {
        "models": list(models) if models is not None else None,
        "scenarios": list(scenarios) if scenarios is not None else None,
    }


def _ensemble_filter_scope_signature(
    *,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> str:
    """Return a stable digest for one ensemble marker filter scope."""
    payload = _ensemble_filter_scope_payload(
        allowed_models=allowed_models,
        allowed_scenarios=allowed_scenarios,
    )
    return hashlib.sha1(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _filter_aware_ensemble_marker_path(
    *,
    slug: str,
    level: AdminLevel,
    scope_name: str,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> Path:
    """Return the marker path for one ensemble build scope."""
    scope_sig = _ensemble_filter_scope_signature(
        allowed_models=allowed_models,
        allowed_scenarios=allowed_scenarios,
    )
    return (
        _markers_root_for_slug(slug)
        / "ensembles"
        / str(level)
        / f"scope={scope_name}"
        / f"filters={scope_sig}.json"
    )


def _canonical_hydro_unit_key(path: Path, *, level: AdminLevel, ensemble: bool) -> str | tuple[str, str] | None:
    """Return the canonical hydro unit key encoded by one output path."""
    try:
        if level == "basin":
            basin_dir = path.parents[1].name if ensemble else path.parents[2].name
            return hydro_fs_token(basin_dir)
        basin_dir = path.parents[2].name if ensemble else path.parents[3].name
        sub_basin_dir = path.parents[1].name if ensemble else path.parents[2].name
        return (hydro_fs_token(basin_dir), hydro_fs_token(sub_basin_dir))
    except IndexError:
        return None


def _hydro_unit_key_from_output_contents(
    path: Path,
    *,
    level: AdminLevel,
) -> str | tuple[str, str] | None:
    """Return the canonical hydro unit key encoded by one output CSV's identity columns."""
    identity_columns = {"basin", "sub_basin", "basin_name", "subbasin_name"}
    try:
        df = read_csv(path, usecols=lambda column: str(column) in identity_columns, nrows=1)
    except Exception:
        return None

    if df.empty:
        return None

    row = df.iloc[0]
    basin_value = next(
        (
            str(row.get(column)).strip()
            for column in ("basin", "basin_name")
            if column in df.columns and not _is_blank_like(row.get(column))
        ),
        "",
    )
    if not basin_value:
        return None

    if level == "basin":
        return hydro_fs_token(basin_value)

    sub_basin_value = next(
        (
            str(row.get(column)).strip()
            for column in ("sub_basin", "subbasin_name")
            if column in df.columns and not _is_blank_like(row.get(column))
        ),
        "",
    )
    if not sub_basin_value:
        return None
    return (hydro_fs_token(basin_value), hydro_fs_token(sub_basin_value))


def _hydro_output_unit_key(
    path: Path,
    *,
    level: AdminLevel,
    ensemble: bool,
) -> str | tuple[str, str] | None:
    """Return the canonical hydro unit key for one output, preferring file contents over directory names."""
    return _hydro_unit_key_from_output_contents(path, level=level) or _canonical_hydro_unit_key(
        path,
        level=level,
        ensemble=ensemble,
    )


def _selected_values_or_all(values: Sequence[str] | None, fallback: Sequence[str]) -> tuple[str, ...]:
    selected = _normalize_filter_values(values)
    return selected if selected is not None else tuple(str(value).strip() for value in fallback)


def _remove_tree_if_exists(path: Path) -> None:
    """Delete one directory tree if it exists."""
    remove_tree(path)


def _unlink_if_exists(path: Path) -> None:
    """Delete one marker file if it exists."""
    unlink_file(path)


def _cleanup_compute_outputs_for_overwrite(
    *,
    slug: str,
    level: AdminLevel,
    scope_name: str,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> None:
    """Remove selected compute outputs, coverage QC files, and markers before overwrite."""
    models = set(_selected_values_or_all(allowed_models, MODELS))
    scenarios = set(_selected_values_or_all(allowed_scenarios, tuple(SCENARIOS.keys())))

    level_root = metric_root(slug) / scope_name / get_level_folder(level)
    if path_exists(level_root):
        candidate_dirs: list[Path] = []
        for path in level_root.rglob("*"):
            if not path.is_dir():
                continue
            if "ensembles" in path.parts:
                continue
            if path.name not in scenarios:
                continue
            if path.parent.name not in models:
                continue
            candidate_dirs.append(path)

        for path in sorted(candidate_dirs, key=lambda item: len(item.parts), reverse=True):
            _remove_tree_if_exists(path)

        for model in models:
            for scenario in scenarios:
                _unlink_if_exists(level_root / f"coverage_qc_{model}_{scenario}.csv")

    ensembles_root = level_root / "ensembles"
    if path_exists(ensembles_root):
        scenario_dirs = [
            path
            for path in ensembles_root.rglob("*")
            if path.is_dir() and path.name in scenarios
        ]
        for path in sorted(scenario_dirs, key=lambda item: len(item.parts), reverse=True):
            _remove_tree_if_exists(path)

    marker_root = _markers_root_for_slug(slug)
    if path_exists(marker_root):
        for model in models:
            for scenario in scenarios:
                _unlink_if_exists(
                    _task_marker_path(
                        slug=slug,
                        level=level,
                        scope_name=scope_name,
                        model=model,
                        scenario=scenario,
                    )
                )
        _unlink_if_exists(
            _filter_aware_ensemble_marker_path(
                slug=slug,
                level=level,
                scope_name=scope_name,
                allowed_models=tuple(sorted(models)),
                allowed_scenarios=tuple(sorted(scenarios)),
            )
        )


def _task_output_file_counts(
    *,
    slug: str,
    level: AdminLevel,
    scope_name: str,
    model: str,
    scenario: str,
) -> tuple[int, int]:
    """Count yearly and period outputs for one compute task."""
    level_root = metric_root(slug) / scope_name / get_level_folder(level)
    if not level_root.exists():
        return 0, 0

    if scope_name == HYDRO_ROOT_NAME and level in {"basin", "sub_basin"}:
        yearly_units: set[str] | set[tuple[str, str]] = set()
        period_units: set[str] | set[tuple[str, str]] = set()

        if level == "basin":
            yearly_pattern = f"*/{model}/{scenario}/*_yearly.csv"
            periods_pattern = f"*/{model}/{scenario}/*_periods.csv"
        else:
            yearly_pattern = f"*/*/{model}/{scenario}/*_yearly.csv"
            periods_pattern = f"*/*/{model}/{scenario}/*_periods.csv"

        for path in glob_paths(level_root, yearly_pattern):
            unit_key = _hydro_output_unit_key(path, level=level, ensemble=False)
            if unit_key is not None:
                yearly_units.add(unit_key)

        for path in glob_paths(level_root, periods_pattern):
            unit_key = _hydro_output_unit_key(path, level=level, ensemble=False)
            if unit_key is not None:
                period_units.add(unit_key)

        return len(yearly_units), len(period_units)

    if level == "district":
        yearly_pattern = f"*/{model}/{scenario}/*_yearly.csv"
        periods_pattern = f"*/{model}/{scenario}/*_periods.csv"
    elif level == "block":
        yearly_pattern = f"*/*/{model}/{scenario}/*_yearly.csv"
        periods_pattern = f"*/*/{model}/{scenario}/*_periods.csv"
    elif level == "basin":
        yearly_pattern = f"*/{model}/{scenario}/*_yearly.csv"
        periods_pattern = f"*/{model}/{scenario}/*_periods.csv"
    else:
        yearly_pattern = f"*/*/{model}/{scenario}/*_yearly.csv"
        periods_pattern = f"*/*/{model}/{scenario}/*_periods.csv"

    yearly_count = sum(1 for _ in level_root.glob(yearly_pattern))
    periods_count = sum(1 for _ in level_root.glob(periods_pattern))
    return yearly_count, periods_count


def _compute_marker_yearly_cleanup_policy(level: AdminLevel) -> str:
    """Return the yearly-output retention policy encoded in compute markers."""
    return "delete_after_ensemble" if level == "block" else "preserve"


def _ensemble_output_count(
    *,
    slug: str,
    level: AdminLevel,
    scope_name: str,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> int:
    """Count ensemble yearly CSVs for one metric/level scope and filter slice."""
    ensembles_root = metric_root(slug) / scope_name / get_level_folder(level) / "ensembles"
    if not ensembles_root.exists():
        return 0
    yearly_pattern = "**/*_yearly_ensemble.csv"

    allowed_scenarios_norm = _normalize_filter_values(allowed_scenarios)
    eligible_units: set[str] | set[tuple[str, str]] | None = None
    if scope_name == HYDRO_ROOT_NAME and level in {"basin", "sub_basin"}:
        eligible_units = _hydro_ensemble_scope_from_coverage_qc(
            metric_root(slug) / scope_name / get_level_folder(level),
            level=level,
            allowed_models=allowed_models,
            allowed_scenarios=allowed_scenarios,
        )

    if scope_name == HYDRO_ROOT_NAME and level in {"basin", "sub_basin"}:
        output_units: set[tuple[str, str]] | set[tuple[tuple[str, str], str]] = set()
        for path in glob_paths(ensembles_root, yearly_pattern):
            scenario_name = path.parent.name
            if allowed_scenarios_norm is not None and scenario_name not in allowed_scenarios_norm:
                continue

            unit_key = _hydro_output_unit_key(path, level=level, ensemble=True)
            if unit_key is None:
                continue

            if eligible_units is not None and unit_key not in eligible_units:
                continue

            output_units.add((unit_key, scenario_name))
        return len(output_units)

    count = 0
    for path in glob_paths(ensembles_root, yearly_pattern):
        scenario_name = path.parent.name
        if allowed_scenarios_norm is not None and scenario_name not in allowed_scenarios_norm:
            continue

        if eligible_units is not None:
            if level == "basin":
                basin_key = path.parents[1].name
                if basin_key not in eligible_units:
                    continue
            else:
                unit_key = (path.parents[2].name, path.parents[1].name)
                if unit_key not in eligible_units:
                    continue

        count += 1
    return count


# -----------------------------------------------------------------------------
# CORE PROCESSING FUNCTION (Generalized for district/block)
# -----------------------------------------------------------------------------
def process_metric_for_model_scenario(
    metric: dict,
    model: str,
    scenario: str,
    scenario_conf: dict,
    gdf: gpd.GeoDataFrame,
    level: AdminLevel = "district",
    state_name: str = "Telangana",
):
    """Process ONE metric for ONE (model, scenario) at specified level."""
    slug = metric["slug"]
    value_col = metric["value_col"]
    req_vars = required_vars_for_metric(metric)
    primary_var = metric.get("var") or (req_vars[0] if req_vars else None)
    if not primary_var:
        logging.error(f"[{slug}] Metric has no var/vars defined")
        return

    compute_fn = globals().get(metric.get("compute"))
    if compute_fn is None:
        logging.error(f"[{slug}] Unknown compute '{metric.get('compute')}'")
        return

    params = metric.get("params", {})
    metric_root_path = metric_root(slug)

    # Resolve year files (supports multi-var metrics like wet-bulb temperature)
    year_to_paths: dict[int, dict[str, Path]] = {}

    if len(req_vars) <= 1:
        data_dir = var_data_dir(DATA_ROOT, scenario_conf["subdir"], primary_var, model)
        if not data_dir.exists():
            return
        valid_year_files, _bad_year_files = validated_year_files_for_var(data_dir, primary_var)
        if not valid_year_files:
            return
        year_to_paths = {y: {primary_var: p} for y, p in valid_year_files.items()}
    else:
        valid_by_var: dict[str, dict[int, Path]] = {}
        for v in req_vars:
            vdir = var_data_dir(DATA_ROOT, scenario_conf["subdir"], v, model)
            if not vdir.exists():
                logging.info(f"[{slug}] Skipping {model}/{scenario}: missing variable directory '{v}'")
                return
            valid_year_files, _bad_year_files = validated_year_files_for_var(vdir, v)
            if not valid_year_files:
                logging.info(f"[{slug}] Skipping {model}/{scenario}: no valid yearly files for '{v}'")
                return
            valid_by_var[v] = valid_year_files

        common_years = set.intersection(*(set(d.keys()) for d in valid_by_var.values()))
        if not common_years:
            logging.info(f"[{slug}] Skipping {model}/{scenario}: no overlapping years across {req_vars}")
            return

        for y in sorted(common_years):
            year_to_paths[y] = {v: valid_by_var[v][y] for v in req_vars}

    if not year_to_paths:
        return

    # Build masks using a sample file from the primary variable
    sample_year = next(iter(year_to_paths.keys()))
    sample_path = year_to_paths[sample_year].get(primary_var)
    if sample_path is None:
        sample_path = next(iter(year_to_paths[sample_year].values()))

    ds_sample = normalize_lat_lon(xr.open_dataset(sample_path))
    if primary_var not in ds_sample:
        ds_sample.close()
        return

    extent_excluded_df = pd.DataFrame()
    boundary_gdf = gdf
    if level in {"basin", "sub_basin"}:
        boundary_gdf, extent_excluded_df = _filter_hydro_units_to_climate_extent(
            gdf,
            ds_sample,
            level=level,
            slug=slug,
            model=model,
            scenario=scenario,
        )
        _prune_excluded_hydro_outputs(
            metric_root_path,
            state_name=state_name,
            level=level,
            model=model,
            scenario=scenario,
            excluded_coverage_df=extent_excluded_df,
            slug=slug,
        )
        _prune_excluded_hydro_ensemble_outputs(
            metric_root_path,
            state_name=state_name,
            level=level,
            scenario=scenario,
            excluded_coverage_df=extent_excluded_df,
            slug=slug,
        )
        if boundary_gdf.empty:
            logging.warning(
                f"[{slug}] No hydro units intersect the climate-data extent for {model}/{scenario} at level={level}"
            )
            ds_sample.close()
            if not extent_excluded_df.empty:
                _write_coverage_qc(
                    metric_root_path,
                    state_name=state_name,
                    level=level,
                    model=model,
                    scenario=scenario,
                    coverage_df=extent_excluded_df,
                )
            return

    masks, coverage_df = build_unit_masks(boundary_gdf, ds_sample, level=level)
    ds_sample.close()

    if not extent_excluded_df.empty:
        coverage_df = pd.concat([coverage_df, extent_excluded_df], ignore_index=True, sort=False)

    if not masks:
        logging.warning(f"[{slug}] No valid masks built for {level} level")
        _write_coverage_qc(
            metric_root_path,
            state_name=state_name,
            level=level,
            model=model,
            scenario=scenario,
            coverage_df=coverage_df,
        )
        return

    eligible_mask = pd.Series(True, index=coverage_df.index, dtype=bool)
    if "eligible_for_processing" in coverage_df.columns:
        eligible_mask = coverage_df["eligible_for_processing"].fillna(True).astype(bool)
    valid_units = set(
        coverage_df.loc[eligible_mask & coverage_df["coverage_ok"].astype(bool), "unit_key"]
        .astype(str)
        .tolist()
    )
    masks = {k: v for k, v in masks.items() if k in valid_units}
    if not masks:
        logging.warning(
            f"[{slug}] No units met the spatial coverage threshold for level={level}"
        )
        _write_coverage_qc(
            metric_root_path,
            state_name=state_name,
            level=level,
            model=model,
            scenario=scenario,
            coverage_df=coverage_df,
        )
        return

    # Get the level subfolder
    level_folder = get_level_folder(level)

    rows = []

    # ------------------------------------------------------------------
    # Special-case SPI/SPEI: multi-year baseline-calibrated SPI workflow
    # ------------------------------------------------------------------
    if metric.get("compute") in SPI_COMPUTE_NAMES:
        baseline_years = tuple(params.get("baseline_years", (1981, 2010)))
        scale_months = int(params.get("scale_months", 3))
        
        # Determine which implementation to use
        use_climate_indices = (
            USE_CLIMATE_INDICES_PACKAGE 
            and CLIMATE_INDICES_AVAILABLE 
            and compute_spi_rows_climate_indices is not None
        )
        
        try:
            if use_climate_indices:
                # Use the climate-indices package (scientifically validated)
                logging.debug(f"[{slug}] Using climate-indices package for SPI computation")

                # Make a safe copy so we can inject runtime SPI params (distribution + monthly CSV settings)
                metric_for_spi = dict(metric)
                metric_for_spi["params"] = dict(metric.get("params") or {})
                metric_for_spi["params"].setdefault("distribution", SPI_DISTRIBUTION)
                metric_for_spi["params"].setdefault("write_monthly_csv", True)
                metric_for_spi["params"].setdefault("use_monthly_cache", True)

                varname = (metric_for_spi.get("var") or "pr").strip()

                # Collect monthly totals for scenario
                scen_monthly_by_unit = _collect_monthly_totals_by_unit(year_to_paths, varname, masks)

                # Build calibration data (historical for SSP scenarios)
                calib_year_to_paths = year_to_paths
                if scenario != "historical":
                    hist_conf = SCENARIOS.get("historical")
                    if hist_conf:
                        hist_dir = var_data_dir(DATA_ROOT, hist_conf["subdir"], varname, model)
                        if hist_dir.exists():
                            valid_year_files, _ = validated_year_files_for_var(hist_dir, varname)
                            if valid_year_files:
                                calib_year_to_paths = {y: {varname: p} for y, p in valid_year_files.items()}

                calib_monthly_by_unit = (
                    scen_monthly_by_unit if calib_year_to_paths is year_to_paths
                    else _collect_monthly_totals_by_unit(calib_year_to_paths, varname, masks)
                )

                # Call the climate-indices adapter (also writes *_monthly.csv per unit)
                rows = compute_spi_rows_climate_indices(
                    metric=metric_for_spi,
                    model=model,
                    scenario=scenario,
                    scenario_conf=scenario_conf,
                    scen_monthly_by_unit=scen_monthly_by_unit,
                    calib_monthly_by_unit=calib_monthly_by_unit,
                    masks=masks,
                    level=level,
                    baseline_years=baseline_years,
                    scale_months=scale_months,
                    year_to_paths=year_to_paths,
                    metric_root_path=metric_root_path,
                    state_name=state_name,
                    level_folder=level_folder,
                )
            else:
                # Fallback to legacy scipy-based implementation
                logging.debug(f"[{slug}] Using legacy SPI implementation (climate-indices not available)")
                rows = _compute_spi_spei_rows(
                    metric=metric,
                    model=model,
                    scenario=scenario,
                    scenario_conf=scenario_conf,
                    year_to_paths=year_to_paths,
                    masks=masks,
                    level=level,
                    baseline_years=baseline_years,
                    scale_months=scale_months,
                )
        except Exception as e:
            logging.error(f"[{slug}] SPI/SPEI computation failed for {model}/{scenario}: {e}")
            logging.debug(traceback.format_exc())
            raise

    elif metric.get("compute") in TX90P_COMPUTE_NAMES:
        try:
            rows = _compute_tx90p_rows_for_metric(
                metric=metric,
                model=model,
                scenario=scenario,
                scenario_conf=SCENARIOS,   # IMPORTANT: pass the global SCENARIOS map (see note below)
                year_to_paths=year_to_paths,
                masks=masks,
                level=level,
            )
        except Exception as e:
            logging.error(f"[{slug}] TX90P computation failed for {model}/{scenario}: {e}")
            logging.debug(traceback.format_exc())
            raise

    # ------------------------------------------------------------------
    # Special-case warm/cold spell duration indices: multi-year baseline thresholds
    # ------------------------------------------------------------------
    elif metric.get("compute") in SPELL_COMPUTE_NAMES:
        try:
            rows = _compute_spell_duration_rows_for_metric(
                metric=metric,
                model=model,
                scenario=scenario,
                scenario_conf=SCENARIOS,
                year_to_paths=year_to_paths,
                masks=masks,
                level=level,
            )
        except Exception as e:
            logging.error(f"[{slug}] Spell duration computation failed for {model}/{scenario}: {e}")
            logging.debug(traceback.format_exc())
            raise

    # ------------------------------------------------------------------
    # Special-case heatwave percentile metrics: multi-year baseline thresholds
    # ------------------------------------------------------------------
    elif metric.get("compute") in HEATWAVE_PERCENTILE_COMPUTE_NAMES:
        try:
            rows = _compute_heatwave_percentile_rows_for_metric(
                metric=metric,
                model=model,
                scenario=scenario,
                scenario_conf=SCENARIOS,
                year_to_paths=year_to_paths,
                masks=masks,
                level=level,
            )
        except Exception as e:
            logging.error(f"[{slug}] Heatwave percentile computation failed for {model}/{scenario}: {e}")
            logging.debug(traceback.format_exc())
            raise

    # ------------------------------------------------------------------
    # Special-case heatwave amplitude/magnitude: historical baseline thresholds
    # ------------------------------------------------------------------
    elif metric.get("compute") in HEATWAVE_BASELINE_COMPUTE_NAMES:
        try:
            rows = _compute_heatwave_baseline_rows_for_metric(
                metric=metric,
                model=model,
                scenario=scenario,
                scenario_conf=SCENARIOS,
                year_to_paths=year_to_paths,
                masks=masks,
                level=level,
            )
        except Exception as e:
            logging.error(f"[{slug}] Heatwave amplitude/magnitude computation failed for {model}/{scenario}: {e}")
            logging.debug(traceback.format_exc())
            raise

    # ------------------------------------------------------------------
    # Special-case heatwave duration/event indices: baseline mean + delta
    # ------------------------------------------------------------------
    elif metric.get("compute") in HEATWAVE_DELTA_COMPUTE_NAMES:
        try:
            rows = _compute_heatwave_delta_rows_for_metric(
                metric=metric,
                model=model,
                scenario=scenario,
                scenario_conf=SCENARIOS,
                year_to_paths=year_to_paths,
                masks=masks,
                level=level,
            )
        except Exception as e:
            logging.error(f"[{slug}] Heatwave duration/event computation failed for {model}/{scenario}: {e}")
            logging.debug(traceback.format_exc())
            raise

    # ------------------------------------------------------------------
    # Special-case precipitation percentile metrics: multi-year baseline thresholds
    # ------------------------------------------------------------------
    elif metric.get("compute") in PRECIP_PERCENTILE_COMPUTE_NAMES:
        try:
            rows = _compute_precip_percentile_rows_for_metric(
                metric=metric,
                model=model,
                scenario=scenario,
                scenario_conf=SCENARIOS,
                year_to_paths=year_to_paths,
                masks=masks,
                level=level,
            )
        except Exception as e:
            logging.error(f"[{slug}] Precip percentile computation failed for {model}/{scenario}: {e}")
            logging.debug(traceback.format_exc())
            raise

    # ------------------------------------------------------------------
    # Special-case DJF seasonal mean (Dec from previous year + Jan/Feb current year)
    # ------------------------------------------------------------------
    elif metric.get("compute") == "seasonal_mean" and set((params or {}).get("months", [])) == {12, 1, 2}:
        try:
            rows = _compute_seasonal_mean_djf_cross_year_rows_for_metric(
                metric=metric,
                model=model,
                scenario=scenario,
                year_to_paths=year_to_paths,
                masks=masks,
                level=level,
            )
        except Exception as e:
            logging.error(f"[{slug}] DJF seasonal mean computation failed for {model}/{scenario}: {e}")
            logging.debug(traceback.format_exc())
            raise

    # ------------------------------------------------------------------
    # Default per-year metric computation
    # --------------------------------------------------
    else:
        for year, paths_by_var in year_to_paths.items():
            ds_by_var: dict[str, xr.Dataset] = {}
            da_by_var: dict[str, xr.DataArray] = {}

            try:
                # Open each required variable once for this year.
                for v, nc_path in paths_by_var.items():
                    ds = normalize_lat_lon(xr.open_dataset(nc_path))
                    if v not in ds:
                        raise KeyError(f"Variable '{v}' not found in {nc_path}")
                    ds_by_var[v] = ds

                    da = ds[v]
                    if v == "pr":
                        da = pr_to_mm_per_day(da)

                    da_by_var[v] = da

                for unit_key, mask in masks.items():
                    if len(req_vars) <= 1:
                        v = compute_fn(da_by_var[primary_var], mask, **params)
                    else:
                        # Multi-var metrics (currently assumes two vars)
                        v = compute_fn(da_by_var[req_vars[0]], da_by_var[req_vars[1]], mask, **params)

                    row = {
                        "year": year,
                        "value": v,
                        value_col: v,
                        "source_file": str(paths_by_var.get(primary_var) or next(iter(paths_by_var.values()))),
                    }

                    _add_unit_fields_from_key(row, unit_key, level)

                    rows.append(row)

            except Exception as e:
                logging.debug(f"[{slug}] Failed {model}/{scenario}/{year}: {e}")
            finally:
                for ds in ds_by_var.values():
                    try:
                        ds.close()
                    except Exception:
                        pass
    if not rows:
        return

    _append_coverage_failure_rows(
        rows,
        coverage_df,
        sorted(year_to_paths.keys()),
        level=level,
        value_col=value_col,
    )

    df_yearly = pd.DataFrame(rows)
    _validate_output_unit_fields(
        df_yearly,
        level=level,
        slug=slug,
        model=model,
        scenario=scenario,
        stage_label="yearly outputs",
    )

    # Period aggregation (mean over years used, consistent with other day-count metrics)
    # Note: For SPI, years may come from climate-indices output which may differ from year_to_paths
    available_years = set(df_yearly["year"].unique())
    if level == "sub_basin":
        group_cols = ["basin", "sub_basin"]
    elif level == "basin":
        group_cols = ["basin"]
    elif level == "block":
        group_cols = ["district", "block"]
    else:
        group_cols = ["district"]
    period_frames = []

    for period_name, (y0, y1) in scenario_conf["periods"].items():
        # Use years actually present in the data, not year_to_paths
        avail = [y for y in available_years if y0 <= y <= y1]
        n_req, n_avail = y1 - y0 + 1, len(avail)
        if n_avail >= MIN_YEARS_ABSOLUTE and n_avail / n_req >= MIN_YEARS_REQUIRED_FRACTION:
            try:
                grp = df_yearly[df_yearly["year"].isin(avail)].groupby(
                    [c for c in group_cols if c in df_yearly.columns]
                ).agg({"value": "mean"}).reset_index()
                grp["period"] = period_name
                grp["years_used_count"] = n_avail
                grp["years_requested"] = n_req
                grp[value_col] = grp["value"]
                period_frames.append(grp)
            except Exception as e:
                logging.warning(f"[{slug}] Period aggregation failed for {period_name}: {e}")

    df_periods = pd.concat(period_frames, ignore_index=True) if period_frames else pd.DataFrame()
    if not df_periods.empty:
        _validate_output_unit_fields(
            df_periods,
            level=level,
            slug=slug,
            model=model,
            scenario=scenario,
            stage_label="period outputs",
        )

    # Write outputs with clean folder structure
    try:
        yearly_file_count = 0
        period_file_count = 0
        if level == "sub_basin":
            for (basin, sub_basin), grp_df in df_yearly.groupby(["basin", "sub_basin"]):
                basin_safe = _safe_component(basin)
                sub_basin_safe = _safe_component(sub_basin)

                out_dir = metric_root_path / state_name / level_folder / basin_safe / sub_basin_safe / model / scenario
                ensure_directory(out_dir)
                grp_df = grp_df.copy()
                grp_df["model"] = model
                grp_df["scenario"] = scenario
                write_csv(grp_df, out_dir / f"{sub_basin_safe}_yearly.csv", index=False)
                yearly_file_count += 1

                if not df_periods.empty:
                    period_mask = (
                        (df_periods["basin"] == basin) &
                        (df_periods["sub_basin"] == sub_basin)
                    )
                    period_grp = df_periods.loc[period_mask].copy()
                    if not period_grp.empty:
                        period_grp["model"] = model
                        period_grp["scenario"] = scenario
                        write_csv(period_grp, out_dir / f"{sub_basin_safe}_periods.csv", index=False)
                        period_file_count += 1
        elif level == "basin":
            for basin_name in df_yearly["basin"].unique():
                basin_safe = _safe_component(basin_name)

                out_dir = metric_root_path / state_name / level_folder / basin_safe / model / scenario
                ensure_directory(out_dir)

                basin_df = df_yearly[df_yearly["basin"] == basin_name].copy()
                basin_df["model"] = model
                basin_df["scenario"] = scenario
                write_csv(basin_df, out_dir / f"{basin_safe}_yearly.csv", index=False)
                yearly_file_count += 1

                if not df_periods.empty:
                    period_df = df_periods[df_periods["basin"] == basin_name].copy()
                    if not period_df.empty:
                        period_df["model"] = model
                        period_df["scenario"] = scenario
                        write_csv(period_df, out_dir / f"{basin_safe}_periods.csv", index=False)
                        period_file_count += 1
        elif level == "block":
            # Structure: {metric}/{state}/blocks/{district}/{block}/{model}/{scenario}/
            for (district, block), grp_df in df_yearly.groupby(["district", "block"]):
                district_safe = _safe_component(district)
                block_safe = _safe_component(block)

                out_dir = metric_root_path / state_name / level_folder / district_safe / block_safe / model / scenario
                ensure_directory(out_dir)
                grp_df = grp_df.copy()  # Avoid SettingWithCopyWarning
                grp_df["model"] = model
                grp_df["scenario"] = scenario

                write_csv(grp_df, out_dir / f"{block_safe}_yearly.csv", index=False)
                yearly_file_count += 1

                if not df_periods.empty:
                    period_mask = (
                        (df_periods["district"] == district) &
                        (df_periods["block"] == block)
                    )
                    period_grp = df_periods.loc[period_mask].copy()

                    if not period_grp.empty:
                        period_grp["model"] = model
                        period_grp["scenario"] = scenario
                        write_csv(period_grp, out_dir / f"{block_safe}_periods.csv", index=False)
                        period_file_count += 1
        else:
            # Structure: {metric}/{state}/districts/{district}/{model}/{scenario}/
            for dist_name in df_yearly["district"].unique():
                dist_safe = _safe_component(dist_name)

                out_dir = metric_root_path / state_name / level_folder / dist_safe / model / scenario
                ensure_directory(out_dir)

                dist_df = df_yearly[df_yearly["district"] == dist_name].copy()
                dist_df["model"] = model
                dist_df["scenario"] = scenario
                write_csv(dist_df, out_dir / f"{dist_safe}_yearly.csv", index=False)
                yearly_file_count += 1

                if not df_periods.empty:
                    period_df = df_periods[df_periods["district"] == dist_name].copy()
                    if not period_df.empty:
                        period_df["model"] = model
                        period_df["scenario"] = scenario
                        write_csv(period_df, out_dir / f"{dist_safe}_periods.csv", index=False)
                        period_file_count += 1
        
        logging.debug(f"[{slug}] Wrote {len(df_yearly)} yearly rows, {len(df_periods)} period rows for {model}/{scenario}")
        _write_coverage_qc(
            metric_root_path,
            state_name=state_name,
            level=level,
            model=model,
            scenario=scenario,
            coverage_df=coverage_df,
        )
        return {
            "yearly_file_count": yearly_file_count,
            "period_file_count": period_file_count,
        }
    except Exception as e:
        logging.error(f"[{slug}] Failed to write output files for {model}/{scenario}: {e}")
        logging.debug(traceback.format_exc())
        raise


# -----------------------------------------------------------------------------
# ENSEMBLE COMPUTATION (Generalized)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class EnsembleBuildStats:
    """Aggregate write/failure counts for one ensemble build pass."""

    written_count: int = 0
    expected_output_count: int = 0
    missing_expected_output_count: int = 0
    skipped_input_count: int = 0
    failure_count: int = 0
    errors: tuple[str, ...] = ()
    skipped_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class EnsembleJobResult:
    """Structured result for one metric-level ensemble job."""

    slug: str
    level: AdminLevel
    scope_name: str
    status: str
    written_count: int
    expected_output_count: int
    missing_expected_output_count: int
    skipped_input_count: int
    failure_count: int
    summary: str
    errors: tuple[str, ...] = ()
    skipped_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class PipelineRunResult:
    """Structured result for one pipeline level run."""

    level: AdminLevel
    scope_name: str
    compute_failed_count: int
    ensemble_results: tuple[EnsembleJobResult, ...]


def _merge_ensemble_stats(*parts: EnsembleBuildStats) -> EnsembleBuildStats:
    """Merge one or more ensemble stat payloads."""
    return EnsembleBuildStats(
        written_count=sum(part.written_count for part in parts),
        expected_output_count=sum(part.expected_output_count for part in parts),
        missing_expected_output_count=sum(part.missing_expected_output_count for part in parts),
        skipped_input_count=sum(part.skipped_input_count for part in parts),
        failure_count=sum(part.failure_count for part in parts),
        errors=tuple(error for part in parts for error in part.errors),
        skipped_reasons=tuple(reason for part in parts for reason in part.skipped_reasons),
    )


def _candidate_value_columns(df: pd.DataFrame, metadata_columns: set[str]) -> list[str]:
    """Return non-metadata value candidates from one yearly CSV."""
    return [column for column in df.columns if column not in metadata_columns]


def _clean_ensemble_yearly_frame(
    df: pd.DataFrame,
    *,
    metadata_columns: set[str],
    model_name: str,
) -> tuple[pd.DataFrame | None, str | None]:
    """Return a cleaned yearly frame or a skip reason for ensemble stats."""
    if "year" not in df.columns:
        return None, "missing_year"

    if "value" in df.columns:
        value_column = "value"
    else:
        candidates = _candidate_value_columns(df, metadata_columns)
        if not candidates:
            return None, "missing_value_column"
        if len(candidates) > 1:
            return None, "ambiguous_value_column"
        value_column = candidates[0]

    cleaned = pd.DataFrame(
        {
            "year": pd.to_numeric(df["year"], errors="coerce"),
            "value": pd.to_numeric(df[value_column], errors="coerce"),
        }
    ).dropna(subset=["year", "value"])
    if cleaned.empty:
        return None, "no_numeric_rows"

    cleaned["year"] = cleaned["year"].astype(int)
    cleaned["model"] = model_name
    return cleaned[["year", "value", "model"]], None


def _sorted_child_dirs(root: Path, *, allowed_names: Sequence[str] | None = None) -> list[Path]:
    """Return child directories optionally filtered by an allowed-name scope."""
    allowed = set(_normalize_filter_values(allowed_names) or ())
    return sorted(
        [
            entry
            for entry in root.iterdir()
            if entry.is_dir() and (not allowed or entry.name in allowed)
        ],
        key=lambda path: path.name,
    )


def compute_ensembles_generic(
    output_root: Path,
    state: str = "Telangana",
    level: AdminLevel = "district",
    slug: str | None = None,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> EnsembleBuildStats:
    """Compute ensemble statistics across models."""
    root = Path(output_root)
    level_folder = get_level_folder(level)
    
    # Data lives in: {state}/{level_folder}/...
    # Ensembles go to: {state}/{level_folder}/ensembles/...
    level_root = root / state / level_folder
    
    if not level_root.exists():
        message = f"Level root does not exist: {level_root}"
        logging.warning(message)
        return EnsembleBuildStats(failure_count=1, errors=(message,))
    
    ensembles_root = level_root / "ensembles"
    ensembles_root.mkdir(parents=True, exist_ok=True)
    
    if level == "sub_basin":
        return _compute_sub_basin_ensembles(
            level_root,
            ensembles_root,
            slug=slug,
            allowed_models=allowed_models,
            allowed_scenarios=allowed_scenarios,
        )
    elif level == "basin":
        return _compute_basin_ensembles(
            level_root,
            ensembles_root,
            slug=slug,
            allowed_models=allowed_models,
            allowed_scenarios=allowed_scenarios,
        )
    elif level == "block":
        return _compute_block_ensembles(
            level_root,
            ensembles_root,
            slug=slug,
            allowed_models=allowed_models,
            allowed_scenarios=allowed_scenarios,
        )
    else:
        return _compute_district_ensembles(
            level_root,
            ensembles_root,
            slug=slug,
            allowed_models=allowed_models,
            allowed_scenarios=allowed_scenarios,
        )

def _compute_district_ensembles(
    level_root: Path,
    ensembles_root: Path,
    *,
    slug: str | None = None,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> EnsembleBuildStats:
    """Compute ensembles for district-level data."""
    stats = EnsembleBuildStats()
    skip_dirs = {"ensembles"}
    district_dirs = [
        p for p in level_root.iterdir()
        if p.is_dir() and p.name not in skip_dirs
    ]
    
    for ddir in district_dirs:
        district = ddir.name
        model_dirs = _sorted_child_dirs(ddir, allowed_names=allowed_models)
        if not model_dirs:
            continue
        allowed_scenarios_norm = _normalize_filter_values(allowed_scenarios)
        if allowed_scenarios_norm is None:
            scenarios = sorted({s.name for m in model_dirs for s in m.iterdir() if s.is_dir()})
        else:
            scenarios = list(allowed_scenarios_norm)
        metadata_columns = {"district", "model", "scenario", "year", "source_file"}
        
        for scenario in scenarios:
            model_yearly = []
            expected_output = False
            for m in model_dirs:
                ycsv = m / scenario / f"{district}_yearly.csv"
                if not path_exists(ycsv):
                    continue
                expected_output = True
                try:
                    dfy = read_csv(ycsv)
                    cleaned, skip_reason = _clean_ensemble_yearly_frame(
                        dfy,
                        metadata_columns=metadata_columns,
                        model_name=m.name,
                    )
                    if cleaned is None:
                        message = f"district={district} model={m.name} scenario={scenario}: {skip_reason}"
                        stats = _merge_ensemble_stats(
                            stats,
                            EnsembleBuildStats(
                                skipped_input_count=1,
                                skipped_reasons=(message,),
                            ),
                        )
                        continue
                    model_yearly.append(cleaned)
                except Exception as e:
                    message = f"district={district} model={m.name} scenario={scenario}: {e}"
                    stats = _merge_ensemble_stats(
                        stats,
                        EnsembleBuildStats(
                            failure_count=1,
                            errors=(message,),
                        ),
                    )

            if not expected_output:
                continue
            stats = _merge_ensemble_stats(
                stats,
                EnsembleBuildStats(expected_output_count=1),
            )

            if not model_yearly:
                message = f"district={district} scenario={scenario}: no valid filtered yearly inputs"
                stats = _merge_ensemble_stats(
                    stats,
                    EnsembleBuildStats(
                        missing_expected_output_count=1,
                        errors=(message,),
                    ),
                )
                continue

            try:
                written = _write_ensemble_stats(
                    model_yearly,
                    ensembles_root / district / scenario,
                    district,
                )
                if written == 0:
                    message = f"district={district} scenario={scenario}: no ensemble outputs produced"
                    stats = _merge_ensemble_stats(
                        stats,
                        EnsembleBuildStats(
                            missing_expected_output_count=1,
                            errors=(message,),
                        ),
                    )
                else:
                    stats = _merge_ensemble_stats(
                        stats,
                        EnsembleBuildStats(written_count=written),
                    )
            except Exception as e:
                message = f"district={district} scenario={scenario}: {e}"
                stats = _merge_ensemble_stats(
                    stats,
                    EnsembleBuildStats(
                        failure_count=1,
                        missing_expected_output_count=1,
                        errors=(message,),
                    ),
                )
    return stats

def _compute_block_ensembles(
    level_root: Path,
    ensembles_root: Path,
    *,
    slug: str | None = None,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> EnsembleBuildStats:
    """Compute ensembles for block-level data."""
    stats = EnsembleBuildStats()
    skip_dirs = {"ensembles"}
    district_dirs = [
        p for p in level_root.iterdir()
        if p.is_dir() and p.name not in skip_dirs
    ]
    
    for ddir in district_dirs:
        district = ddir.name
        block_dirs = [p for p in ddir.iterdir() if p.is_dir()]
        
        for bdir in block_dirs:
            block = bdir.name
            model_dirs = _sorted_child_dirs(bdir, allowed_names=allowed_models)
            if not model_dirs:
                continue
            allowed_scenarios_norm = _normalize_filter_values(allowed_scenarios)
            if allowed_scenarios_norm is None:
                scenarios = sorted({s.name for m in model_dirs for s in m.iterdir() if s.is_dir()})
            else:
                scenarios = list(allowed_scenarios_norm)
            metadata_columns = {"district", "block", "model", "scenario", "year", "source_file"}
            
            for scenario in scenarios:
                model_yearly = []
                expected_output = False
                for m in model_dirs:
                    ycsv = m / scenario / f"{block}_yearly.csv"
                    if not path_exists(ycsv):
                        continue
                    expected_output = True
                    try:
                        dfy = read_csv(ycsv)
                        cleaned, skip_reason = _clean_ensemble_yearly_frame(
                            dfy,
                            metadata_columns=metadata_columns,
                            model_name=m.name,
                        )
                        if cleaned is None:
                            message = (
                                f"district={district} block={block} model={m.name} "
                                f"scenario={scenario}: {skip_reason}"
                            )
                            stats = _merge_ensemble_stats(
                                stats,
                                EnsembleBuildStats(
                                    skipped_input_count=1,
                                    skipped_reasons=(message,),
                                ),
                            )
                            continue
                        model_yearly.append(cleaned)
                    except Exception as e:
                        message = (
                            f"district={district} block={block} model={m.name} "
                            f"scenario={scenario}: {e}"
                        )
                        stats = _merge_ensemble_stats(
                            stats,
                            EnsembleBuildStats(
                                failure_count=1,
                                errors=(message,),
                            ),
                        )

                if not expected_output:
                    continue
                stats = _merge_ensemble_stats(
                    stats,
                    EnsembleBuildStats(expected_output_count=1),
                )

                if not model_yearly:
                    message = (
                        f"district={district} block={block} scenario={scenario}: "
                        "no valid filtered yearly inputs"
                    )
                    stats = _merge_ensemble_stats(
                        stats,
                        EnsembleBuildStats(
                            missing_expected_output_count=1,
                            errors=(message,),
                        ),
                    )
                    continue

                out_dir = ensembles_root / district / block / scenario
                try:
                    written = _write_ensemble_stats(model_yearly, out_dir, block)
                    if written == 0:
                        message = (
                            f"district={district} block={block} scenario={scenario}: "
                            "no ensemble outputs produced"
                        )
                        stats = _merge_ensemble_stats(
                            stats,
                            EnsembleBuildStats(
                                missing_expected_output_count=1,
                                errors=(message,),
                            ),
                        )
                        continue
                    stats = _merge_ensemble_stats(
                        stats,
                        EnsembleBuildStats(written_count=written),
                    )
                except Exception as e:
                    logging.warning(
                        "Failed to write ensemble yearly for block=%s district=%s scenario=%s: %s",
                        block,
                        district,
                        scenario,
                        e,
                    )
                    message = f"district={district} block={block} scenario={scenario}: {e}"
                    stats = _merge_ensemble_stats(
                        stats,
                        EnsembleBuildStats(
                            failure_count=1,
                            missing_expected_output_count=1,
                            errors=(message,),
                        ),
                    )
                    continue

                # After successfully writing ensemble outputs, delete per-model yearly CSVs
                out_csv = out_dir / f"{block}_yearly_ensemble.csv"
                if path_exists(out_csv):
                    for m in model_dirs:
                        ycsv = m / scenario / f"{block}_yearly.csv"
                        if path_exists(ycsv):
                            try:
                                unlink_file(ycsv)
                            except Exception as e:
                                logging.debug(
                                    "Could not delete per-model block yearly CSV: %s (%s)",
                                    ycsv,
                                    e,
                                )
    return stats


def _hydro_ensemble_scope_from_coverage_qc(
    level_root: Path,
    *,
    level: AdminLevel,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> set[str] | set[tuple[str, str]] | None:
    """Return eligible hydro output directories inferred from coverage-QC files."""
    models = tuple(_normalize_filter_values(allowed_models) or ())
    scenarios = tuple(_normalize_filter_values(allowed_scenarios) or ())

    candidate_paths: list[Path] = []
    if models and scenarios:
        for model in models:
            for scenario in scenarios:
                path = level_root / f"coverage_qc_{model}_{scenario}.csv"
                if path.exists():
                    candidate_paths.append(path)
    else:
        candidate_paths = sorted(level_root.glob("coverage_qc_*.csv"))

    if not candidate_paths:
        return None

    if level == "basin":
        eligible_units: set[str] = set()
    else:
        eligible_units = set()

    for path in candidate_paths:
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            logging.debug("Failed to read hydro coverage QC %s: %s", path, exc)
            continue
        if df.empty:
            continue

        if "eligible_for_processing" in df.columns:
            eligible_mask = df["eligible_for_processing"].fillna(False).astype(bool)
        elif "coverage_ok" in df.columns:
            eligible_mask = df["coverage_ok"].fillna(False).astype(bool)
        else:
            continue

        eligible_df = df.loc[eligible_mask].copy()
        if eligible_df.empty:
            continue

        if level == "basin":
            if "basin_name" not in eligible_df.columns:
                continue
            eligible_units.update(
                _safe_component(name)
                for name in eligible_df["basin_name"].astype(str).tolist()
            )
        else:
            required = {"basin_name", "subbasin_name"}
            if not required.issubset(eligible_df.columns):
                continue
            eligible_units.update(
                (
                    _safe_component(basin_name),
                    _safe_component(sub_basin_name),
                )
                for basin_name, sub_basin_name in zip(
                    eligible_df["basin_name"].astype(str),
                    eligible_df["subbasin_name"].astype(str),
                )
            )

    return eligible_units


def _compute_basin_ensembles(
    level_root: Path,
    ensembles_root: Path,
    *,
    slug: str | None = None,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> EnsembleBuildStats:
    """Compute ensembles for basin-level data."""
    stats = EnsembleBuildStats()
    skip_dirs = {"ensembles"}
    basin_dirs = [
        p for p in level_root.iterdir()
        if p.is_dir() and p.name not in skip_dirs
    ]
    eligible_basin_dirs = _hydro_ensemble_scope_from_coverage_qc(
        level_root,
        level="basin",
        allowed_models=allowed_models,
        allowed_scenarios=allowed_scenarios,
    )
    if eligible_basin_dirs is not None:
        total_basin_dirs = len(basin_dirs)
        basin_dirs = [path for path in basin_dirs if path.name in eligible_basin_dirs]
        logging.info(
            "[%s] Hydro basin ensembles restricted by coverage QC: eligible=%d excluded=%d",
            slug or "ensemble",
            len(basin_dirs),
            total_basin_dirs - len(basin_dirs),
        )
    metadata_columns = {"basin", "model", "scenario", "year", "source_file"}

    for bdir in basin_dirs:
        basin = bdir.name
        model_dirs = _sorted_child_dirs(bdir, allowed_names=allowed_models)
        if not model_dirs:
            continue
        allowed_scenarios_norm = _normalize_filter_values(allowed_scenarios)
        if allowed_scenarios_norm is None:
            scenarios = sorted({s.name for m in model_dirs for s in m.iterdir() if s.is_dir()})
        else:
            scenarios = list(allowed_scenarios_norm)

        for scenario in scenarios:
            model_yearly = []
            expected_output = False
            for m in model_dirs:
                ycsv = m / scenario / f"{basin}_yearly.csv"
                if not path_exists(ycsv):
                    continue
                expected_output = True
                try:
                    dfy = read_csv(ycsv)
                    cleaned, skip_reason = _clean_ensemble_yearly_frame(
                        dfy,
                        metadata_columns=metadata_columns,
                        model_name=m.name,
                    )
                    if cleaned is None:
                        message = f"basin={basin} model={m.name} scenario={scenario}: {skip_reason}"
                        stats = _merge_ensemble_stats(
                            stats,
                            EnsembleBuildStats(
                                skipped_input_count=1,
                                skipped_reasons=(message,),
                            ),
                        )
                        continue
                    model_yearly.append(cleaned)
                except Exception as e:
                    message = f"basin={basin} model={m.name} scenario={scenario}: {e}"
                    logging.warning(
                        "[%s] Failed to read hydro basin yearly for basin=%s model=%s scenario=%s: %s",
                        slug or "ensemble",
                        basin,
                        m.name,
                        scenario,
                        e,
                    )
                    stats = _merge_ensemble_stats(
                        stats,
                        EnsembleBuildStats(failure_count=1, errors=(message,)),
                    )

            if not expected_output:
                continue
            stats = _merge_ensemble_stats(
                stats,
                EnsembleBuildStats(expected_output_count=1),
            )

            if not model_yearly:
                message = f"basin={basin} scenario={scenario}: no valid filtered yearly inputs"
                stats = _merge_ensemble_stats(
                    stats,
                    EnsembleBuildStats(
                        missing_expected_output_count=1,
                        errors=(message,),
                    ),
                )
                continue

            try:
                written = _write_ensemble_stats(
                    model_yearly,
                    ensembles_root / basin / scenario,
                    basin,
                    file_stem=hydro_fs_token(basin),
                )
                if written == 0:
                    message = f"basin={basin} scenario={scenario}: no ensemble outputs produced"
                    stats = _merge_ensemble_stats(
                        stats,
                        EnsembleBuildStats(
                            missing_expected_output_count=1,
                            errors=(message,),
                        ),
                    )
                else:
                    stats = _merge_ensemble_stats(
                        stats,
                        EnsembleBuildStats(written_count=written),
                    )
            except Exception as e:
                message = f"basin={basin} scenario={scenario}: {e}"
                logging.warning(
                    "[%s] Failed to write hydro basin ensemble for basin=%s scenario=%s: %s",
                    slug or "ensemble",
                    basin,
                    scenario,
                    e,
                )
                stats = _merge_ensemble_stats(
                    stats,
                    EnsembleBuildStats(
                        failure_count=1,
                        missing_expected_output_count=1,
                        errors=(message,),
                    ),
                )
    return stats


def _compute_sub_basin_ensembles(
    level_root: Path,
    ensembles_root: Path,
    *,
    slug: str | None = None,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> EnsembleBuildStats:
    """Compute ensembles for sub-basin-level data."""
    stats = EnsembleBuildStats()
    skip_dirs = {"ensembles"}
    basin_dirs = [
        p for p in level_root.iterdir()
        if p.is_dir() and p.name not in skip_dirs
    ]
    eligible_sub_basin_dirs = _hydro_ensemble_scope_from_coverage_qc(
        level_root,
        level="sub_basin",
        allowed_models=allowed_models,
        allowed_scenarios=allowed_scenarios,
    )
    if eligible_sub_basin_dirs is not None:
        eligible_basin_names = {basin_name for basin_name, _sub_basin_name in eligible_sub_basin_dirs}
        basin_dirs = [path for path in basin_dirs if path.name in eligible_basin_names]
    metadata_columns = {"basin", "sub_basin", "model", "scenario", "year", "source_file"}

    for basin_dir in basin_dirs:
        basin = basin_dir.name
        sub_basin_dirs = [p for p in basin_dir.iterdir() if p.is_dir()]
        if eligible_sub_basin_dirs is not None:
            sub_basin_dirs = [
                path for path in sub_basin_dirs if (basin, path.name) in eligible_sub_basin_dirs
            ]
        for sbdir in sub_basin_dirs:
            sub_basin = sbdir.name
            model_dirs = _sorted_child_dirs(sbdir, allowed_names=allowed_models)
            if not model_dirs:
                continue
            allowed_scenarios_norm = _normalize_filter_values(allowed_scenarios)
            if allowed_scenarios_norm is None:
                scenarios = sorted({s.name for m in model_dirs for s in m.iterdir() if s.is_dir()})
            else:
                scenarios = list(allowed_scenarios_norm)

            for scenario in scenarios:
                model_yearly = []
                expected_output = False
                for m in model_dirs:
                    ycsv = m / scenario / f"{sub_basin}_yearly.csv"
                    if not path_exists(ycsv):
                        continue
                    expected_output = True
                    try:
                        dfy = read_csv(ycsv)
                        cleaned, skip_reason = _clean_ensemble_yearly_frame(
                            dfy,
                            metadata_columns=metadata_columns,
                            model_name=m.name,
                        )
                        if cleaned is None:
                            message = (
                                f"basin={basin} sub_basin={sub_basin} "
                                f"model={m.name} scenario={scenario}: {skip_reason}"
                            )
                            stats = _merge_ensemble_stats(
                                stats,
                                EnsembleBuildStats(
                                    skipped_input_count=1,
                                    skipped_reasons=(message,),
                                ),
                            )
                            continue
                        model_yearly.append(cleaned)
                    except Exception as e:
                        message = f"basin={basin} sub_basin={sub_basin} model={m.name} scenario={scenario}: {e}"
                        logging.warning(
                            "[%s] Failed to read hydro sub-basin yearly for basin=%s sub_basin=%s model=%s scenario=%s: %s",
                            slug or "ensemble",
                            basin,
                            sub_basin,
                            m.name,
                            scenario,
                            e,
                        )
                        stats = _merge_ensemble_stats(
                            stats,
                            EnsembleBuildStats(failure_count=1, errors=(message,)),
                        )

                if not expected_output:
                    continue
                stats = _merge_ensemble_stats(
                    stats,
                    EnsembleBuildStats(expected_output_count=1),
                )

                if not model_yearly:
                    message = (
                        f"basin={basin} sub_basin={sub_basin} scenario={scenario}: "
                        "no valid filtered yearly inputs"
                    )
                    stats = _merge_ensemble_stats(
                        stats,
                        EnsembleBuildStats(
                            missing_expected_output_count=1,
                            errors=(message,),
                        ),
                    )
                    continue

                try:
                    written = _write_ensemble_stats(
                        model_yearly,
                        ensembles_root / basin / sub_basin / scenario,
                        sub_basin,
                        file_stem=hydro_fs_token(sub_basin),
                    )
                    if written == 0:
                        message = (
                            f"basin={basin} sub_basin={sub_basin} scenario={scenario}: "
                            "no ensemble outputs produced"
                        )
                        stats = _merge_ensemble_stats(
                            stats,
                            EnsembleBuildStats(
                                missing_expected_output_count=1,
                                errors=(message,),
                            ),
                        )
                    else:
                        stats = _merge_ensemble_stats(
                            stats,
                            EnsembleBuildStats(written_count=written),
                        )
                except Exception as e:
                    message = f"basin={basin} sub_basin={sub_basin} scenario={scenario}: {e}"
                    logging.warning(
                        "[%s] Failed to write hydro sub-basin ensemble for basin=%s sub_basin=%s scenario=%s: %s",
                        slug or "ensemble",
                        basin,
                        sub_basin,
                        scenario,
                        e,
                    )
                    stats = _merge_ensemble_stats(
                        stats,
                        EnsembleBuildStats(
                            failure_count=1,
                            missing_expected_output_count=1,
                            errors=(message,),
                        ),
                    )
    return stats


def _write_ensemble_stats(
    model_yearly: list,
    out_dir: Path,
    unit_name: str,
    *,
    file_stem: str | None = None,
) -> int:
    """Write ensemble statistics CSV."""
    if not model_yearly:
        return 0
    df_yc = pd.concat(model_yearly, ignore_index=True)
    if "year" not in df_yc.columns or "value" not in df_yc.columns or "model" not in df_yc.columns:
        return 0
    df_yc["year"] = df_yc["year"].astype(int)
    pivot = df_yc.pivot_table(index="year", columns="model", values="value", aggfunc="first")
    if pivot.empty:
        return 0
    summary = pd.DataFrame({
        "year": pivot.index,
        "n_models": pivot.count(axis=1),
        "ensemble_mean": pivot.mean(axis=1),
        "ensemble_std": pivot.std(axis=1, ddof=0),
        "ensemble_median": pivot.median(axis=1),
        "ensemble_p05": pivot.quantile(0.05, axis=1),
        "ensemble_p95": pivot.quantile(0.95, axis=1),
    }).reset_index(drop=True)

    write_csv(summary, out_dir / f"{file_stem or unit_name}_yearly_ensemble.csv", index=False)
    return 1

# -----------------------------------------------------------------------------
# MULTIPROCESSING (Updated for level support)
# -----------------------------------------------------------------------------
@dataclass
class ProcessingTask:
    metric_idx: int
    slug: str
    model: str
    scenario: str
    scenario_conf: dict
    task_id: int
    total_tasks: int
    level: str = "district"
    state_name: str = "Telangana"
    required_vars: tuple[str, ...] = ()
    common_years_hash: str = ""
    scope_name: str = "Telangana"


@dataclass(frozen=True)
class ProcessingTaskPlan:
    """Runnable compute task plan plus per-metric skip reasons."""

    level: str
    scope_name: str
    selected_metrics: tuple[str, ...]
    tasks: tuple[ProcessingTask, ...]
    skipped_counts_by_reason: dict[str, int]
    skipped_reasons_by_metric: dict[str, tuple[str, ...]]


def build_processing_task_plan(
    *,
    metrics_filter: Sequence[str] | None = None,
    models_filter: Sequence[str] | None = None,
    scenarios_filter: Sequence[str] | None = None,
    level: AdminLevel = "district",
    state: str = "Telangana",
) -> ProcessingTaskPlan:
    """Return the exact runnable compute task universe for one level/scope."""
    metrics_to_process = [
        (i, m)
        for i, m in enumerate(METRICS)
        if not metrics_filter or m["slug"] in metrics_filter
    ]
    models_to_process = [m for m in MODELS if not models_filter or m in models_filter]
    scenarios_to_process = {
        k: v for k, v in SCENARIOS.items() if not scenarios_filter or k in scenarios_filter
    }
    selected_metrics = tuple(m["slug"] for _, m in metrics_to_process)
    scope_name = _scope_name_for_level(level, state)

    for _, metric in metrics_to_process:
        metric_root(metric["slug"])

    years_cache: dict[tuple[str, str, str], set[int]] = {}
    tasks: list[ProcessingTask] = []
    skipped_counts: dict[str, int] = defaultdict(int)
    skipped_reasons_by_metric: dict[str, set[str]] = defaultdict(set)

    def _years_for(model_name: str, scenario_name: str, sconf: dict, varname: str) -> set[int]:
        key = (model_name, scenario_name, varname)
        if key in years_cache:
            return years_cache[key]
        d = var_data_dir(DATA_ROOT, sconf["subdir"], varname, model_name)
        valid_year_files, _bad_year_files = validated_year_files_for_var(d, varname) if d.exists() else ({}, {})
        yrs = set(valid_year_files.keys())
        years_cache[key] = yrs
        return yrs

    for model in models_to_process:
        for scenario, sconf in scenarios_to_process.items():
            for midx, metric in metrics_to_process:
                slug = str(metric["slug"]).strip()
                req_vars = tuple(required_vars_for_metric(metric))
                if not req_vars:
                    skipped_counts[SKIP_REASON_MISSING_REQUIRED_VARS] += 1
                    skipped_reasons_by_metric[slug].add(SKIP_REASON_MISSING_REQUIRED_VARS)
                    continue

                year_sets = []
                source_invalid = False
                for v in req_vars:
                    d = var_data_dir(DATA_ROOT, sconf["subdir"], v, model)
                    if not d.exists():
                        year_sets.append(set())
                        continue
                    raw_years = yearly_files_for_dir(d)
                    valid_year_files, _bad_year_files = validated_year_files_for_var(d, v)
                    if raw_years and not valid_year_files:
                        source_invalid = True
                    year_sets.append(set(valid_year_files.keys()))
                if any(len(years) == 0 for years in year_sets):
                    reason = SKIP_REASON_INVALID_SOURCE_FILES if source_invalid else SKIP_REASON_NO_AVAILABLE_YEARS
                    skipped_counts[reason] += 1
                    skipped_reasons_by_metric[slug].add(reason)
                    continue

                common_years = set.intersection(*year_sets) if year_sets else set()
                if not common_years:
                    skipped_counts[SKIP_REASON_NO_COMMON_YEARS] += 1
                    skipped_reasons_by_metric[slug].add(SKIP_REASON_NO_COMMON_YEARS)
                    continue

                tasks.append(
                    ProcessingTask(
                        metric_idx=midx,
                        slug=slug,
                        model=model,
                        scenario=scenario,
                        scenario_conf=sconf,
                        task_id=len(tasks),
                        total_tasks=0,
                        level=level,
                        state_name=state,
                        required_vars=req_vars,
                        common_years_hash=_hash_common_years(sorted(common_years)),
                        scope_name=scope_name,
                    )
                )

    runnable_metrics = {task.slug for task in tasks}
    for slug in selected_metrics:
        if slug not in runnable_metrics:
            if not skipped_reasons_by_metric[slug]:
                skipped_reasons_by_metric[slug].add(SKIP_REASON_NO_TASKS_AFTER_FILTERS)

    for task in tasks:
        task.total_tasks = len(tasks)

    return ProcessingTaskPlan(
        level=level,
        scope_name=scope_name,
        selected_metrics=selected_metrics,
        tasks=tuple(tasks),
        skipped_counts_by_reason=dict(sorted(skipped_counts.items())),
        skipped_reasons_by_metric={
            slug: tuple(sorted(reasons))
            for slug, reasons in sorted(skipped_reasons_by_metric.items())
        },
    )


def _load_marker_json(path: Path) -> Optional[dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_marker_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


@dataclass(frozen=True)
class MarkerValidationStatus:
    """Explain whether one compute or ensemble marker is currently reusable."""

    valid: bool
    reason: str
    detail: str = ""


def task_completion_marker_status(task: ProcessingTask) -> MarkerValidationStatus:
    """Return validator status for one compute task marker."""
    marker_path = _task_marker_path(
        slug=task.slug,
        level=task.level,
        scope_name=task.scope_name,
        model=task.model,
        scenario=task.scenario,
    )
    payload = _load_marker_json(marker_path)
    if not payload:
        return MarkerValidationStatus(valid=False, reason="missing_compute_marker")

    boundary_path, boundary_mtime_ns = _boundary_signature(task.level, task.state_name)
    if int(payload.get("schema_version", -1)) != COMPUTE_MARKER_SCHEMA_VERSION:
        return MarkerValidationStatus(valid=False, reason="compute_marker_schema_mismatch")
    if str(payload.get("slug", "")).strip() != task.slug:
        return MarkerValidationStatus(valid=False, reason="compute_marker_slug_mismatch")
    if str(payload.get("level", "")).strip() != task.level:
        return MarkerValidationStatus(valid=False, reason="compute_marker_level_mismatch")
    if str(payload.get("scope", "")).strip() != task.scope_name:
        return MarkerValidationStatus(valid=False, reason="compute_marker_scope_mismatch")
    if str(payload.get("model", "")).strip() != task.model:
        return MarkerValidationStatus(valid=False, reason="compute_marker_model_mismatch")
    if str(payload.get("scenario", "")).strip() != task.scenario:
        return MarkerValidationStatus(valid=False, reason="compute_marker_scenario_mismatch")
    if tuple(payload.get("required_vars", [])) != tuple(task.required_vars):
        return MarkerValidationStatus(valid=False, reason="compute_marker_required_vars_mismatch")
    if str(payload.get("common_years_hash", "")).strip() != task.common_years_hash:
        return MarkerValidationStatus(valid=False, reason="compute_marker_common_years_mismatch")
    if str(payload.get("boundary_path", "")).strip() != boundary_path:
        return MarkerValidationStatus(valid=False, reason="compute_marker_boundary_path_mismatch")
    if int(payload.get("boundary_mtime_ns", -1)) != int(boundary_mtime_ns):
        return MarkerValidationStatus(valid=False, reason="compute_marker_boundary_mtime_mismatch")

    yearly_expected = int(payload.get("yearly_file_count", -1))
    periods_expected = int(payload.get("period_file_count", -1))
    yearly_cleanup_policy = str(
        payload.get("yearly_cleanup_policy", _compute_marker_yearly_cleanup_policy(task.level))
    ).strip()
    yearly_actual, periods_actual = _task_output_file_counts(
        slug=task.slug,
        level=task.level,
        scope_name=task.scope_name,
        model=task.model,
        scenario=task.scenario,
    )
    yearly_counts_valid = yearly_actual == yearly_expected
    if (
        task.level == "block"
        and yearly_cleanup_policy == "delete_after_ensemble"
        and yearly_actual == 0
        and yearly_expected >= 0
    ):
        yearly_counts_valid = True
    if not yearly_counts_valid or periods_actual != periods_expected:
        return MarkerValidationStatus(
            valid=False,
            reason="compute_marker_output_count_mismatch",
            detail=(
                f"expected(yearly={yearly_expected}, periods={periods_expected}) "
                f"actual(yearly={yearly_actual}, periods={periods_actual})"
            ),
        )

    return MarkerValidationStatus(valid=True, reason="ok")


def task_completion_marker_valid(task: ProcessingTask) -> bool:
    """Return True when one compute task marker can safely skip rerun."""
    return task_completion_marker_status(task).valid


def ensemble_completion_marker_status(
    *,
    slug: str,
    level: AdminLevel,
    scope_name: str,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> MarkerValidationStatus:
    """Return validator status for one ensemble marker."""
    marker_path = _filter_aware_ensemble_marker_path(
        slug=slug,
        level=level,
        scope_name=scope_name,
        allowed_models=allowed_models,
        allowed_scenarios=allowed_scenarios,
    )
    payload = _load_marker_json(marker_path)
    if not payload:
        return MarkerValidationStatus(valid=False, reason="missing_ensemble_marker")

    boundary_path, boundary_mtime_ns = _boundary_signature(level, scope_name)
    if int(payload.get("schema_version", -1)) != ENSEMBLE_MARKER_SCHEMA_VERSION:
        return MarkerValidationStatus(valid=False, reason="ensemble_marker_schema_mismatch")
    if str(payload.get("slug", "")).strip() != str(slug).strip():
        return MarkerValidationStatus(valid=False, reason="ensemble_marker_slug_mismatch")
    if str(payload.get("level", "")).strip() != str(level).strip():
        return MarkerValidationStatus(valid=False, reason="ensemble_marker_level_mismatch")
    if str(payload.get("scope", "")).strip() != str(scope_name).strip():
        return MarkerValidationStatus(valid=False, reason="ensemble_marker_scope_mismatch")
    if str(payload.get("boundary_path", "")).strip() != boundary_path:
        return MarkerValidationStatus(valid=False, reason="ensemble_marker_boundary_path_mismatch")
    if int(payload.get("boundary_mtime_ns", -1)) != int(boundary_mtime_ns):
        return MarkerValidationStatus(valid=False, reason="ensemble_marker_boundary_mtime_mismatch")
    if payload.get("filter_scope") != _ensemble_filter_scope_payload(
        allowed_models=allowed_models,
        allowed_scenarios=allowed_scenarios,
    ):
        return MarkerValidationStatus(valid=False, reason="ensemble_marker_filter_scope_mismatch")

    expected = int(payload.get("expected_output_count", -1))
    actual = _ensemble_output_count(
        slug=slug,
        level=level,
        scope_name=scope_name,
        allowed_models=allowed_models,
        allowed_scenarios=allowed_scenarios,
    )
    if actual != expected:
        return MarkerValidationStatus(
            valid=False,
            reason="ensemble_marker_output_count_mismatch",
            detail=f"expected={expected} actual={actual}",
        )
    return MarkerValidationStatus(valid=True, reason="ok")


def ensemble_completion_marker_valid(
    *,
    slug: str,
    level: AdminLevel,
    scope_name: str,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
) -> bool:
    """Return True when one ensemble marker can safely skip rebuild."""
    return ensemble_completion_marker_status(
        slug=slug,
        level=level,
        scope_name=scope_name,
        allowed_models=allowed_models,
        allowed_scenarios=allowed_scenarios,
    ).valid


def _write_task_completion_marker(task: ProcessingTask, *, output_meta: Optional[dict[str, int]] = None) -> None:
    output_meta = output_meta or {}
    boundary_path, boundary_mtime_ns = _boundary_signature(task.level, task.state_name)
    payload = {
        "schema_version": COMPUTE_MARKER_SCHEMA_VERSION,
        "slug": task.slug,
        "level": task.level,
        "scope": task.scope_name,
        "model": task.model,
        "scenario": task.scenario,
        "required_vars": list(task.required_vars),
        "common_years_hash": task.common_years_hash,
        "boundary_path": boundary_path,
        "boundary_mtime_ns": int(boundary_mtime_ns),
        "yearly_file_count": int(output_meta.get("yearly_file_count", 0)),
        "period_file_count": int(output_meta.get("period_file_count", 0)),
        "yearly_cleanup_policy": _compute_marker_yearly_cleanup_policy(task.level),
        "completed_at": time.time(),
    }
    _write_marker_json(
        _task_marker_path(
            slug=task.slug,
            level=task.level,
            scope_name=task.scope_name,
            model=task.model,
            scenario=task.scenario,
        ),
        payload,
    )


def _write_ensemble_completion_marker(
    *,
    slug: str,
    level: AdminLevel,
    scope_name: str,
    allowed_models: Sequence[str] | None = None,
    allowed_scenarios: Sequence[str] | None = None,
    expected_output_count: int,
) -> None:
    boundary_path, boundary_mtime_ns = _boundary_signature(level, scope_name)
    payload = {
        "schema_version": ENSEMBLE_MARKER_SCHEMA_VERSION,
        "slug": slug,
        "level": level,
        "scope": scope_name,
        "boundary_path": boundary_path,
        "boundary_mtime_ns": int(boundary_mtime_ns),
        "filter_scope": _ensemble_filter_scope_payload(
            allowed_models=allowed_models,
            allowed_scenarios=allowed_scenarios,
        ),
        "expected_output_count": int(expected_output_count),
        "completed_at": time.time(),
    }
    _write_marker_json(
        _filter_aware_ensemble_marker_path(
            slug=slug,
            level=level,
            scope_name=scope_name,
            allowed_models=allowed_models,
            allowed_scenarios=allowed_scenarios,
        ),
        payload,
    )

# Global worker state
_worker_gdf = None
_worker_level = "district"
_worker_state = "Telangana"

def _worker_init(level: str = "district", state: str = "Telangana"):
    global _worker_gdf, _worker_level, _worker_state
    _worker_level = level
    _worker_state = HYDRO_ROOT_NAME if level in {"basin", "sub_basin"} else state
    boundary_path = get_boundary_path(level)
    state_filter = None if level in {"basin", "sub_basin"} else state
    _worker_gdf = load_boundaries(boundary_path, state_filter=state_filter, level=level)

def _execute_processing_task(task: ProcessingTask, gdf: gpd.GeoDataFrame) -> dict[str, Any]:
    """Run one compute task and persist a completion marker on success."""
    metric = METRICS[task.metric_idx]
    output_meta = process_metric_for_model_scenario(
        metric,
        task.model,
        task.scenario,
        task.scenario_conf,
        gdf,
        level=task.level,
        state_name=task.scope_name,
    )
    _write_task_completion_marker(task, output_meta=output_meta if isinstance(output_meta, dict) else None)
    return {
        "yearly_file_count": int((output_meta or {}).get("yearly_file_count", 0)) if isinstance(output_meta, dict) else 0,
        "period_file_count": int((output_meta or {}).get("period_file_count", 0)) if isinstance(output_meta, dict) else 0,
    }


def _worker_process_task(task: ProcessingTask) -> dict:
    global _worker_gdf, _worker_level, _worker_state
    start = time.time()
    result = {
        "task_id": task.task_id,
        "slug": task.slug,
        "model": task.model,
        "scenario": task.scenario,
        "status": "success",
        "error": None,
    }
    try:
        result.update(_execute_processing_task(task, _worker_gdf))
    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
    result["duration"] = time.time() - start
    return result

def _compute_ensembles_for_metric(
    args: tuple[str, AdminLevel, str, tuple[str, ...] | None, tuple[str, ...] | None]
) -> EnsembleJobResult:
    slug, level, state, allowed_models, allowed_scenarios = args
    try:
        stats = compute_ensembles_generic(
            metric_root(slug),
            state=state,
            level=level,
            slug=slug,
            allowed_models=allowed_models,
            allowed_scenarios=allowed_scenarios,
        )
    except Exception as e:
        summary = f"{state}/{level}/{slug}: expected=0, wrote=0, failures=1, first_error={e}"
        return EnsembleJobResult(
            slug=slug,
            level=level,
            scope_name=state,
            status="failed",
            written_count=0,
            expected_output_count=0,
            missing_expected_output_count=0,
            skipped_input_count=0,
            failure_count=1,
            summary=summary,
            errors=(str(e),),
        )

    zero_output_is_failure = state == HYDRO_ROOT_NAME and level in {"basin", "sub_basin"}
    hard_failure = (
        stats.failure_count > 0
        or stats.missing_expected_output_count > 0
        or stats.written_count != stats.expected_output_count
        or (zero_output_is_failure and stats.expected_output_count == 0)
    )
    if hard_failure:
        first_error = stats.errors[0] if stats.errors else "unknown ensemble failure"
        summary = (
            f"{state}/{level}/{slug}: expected={stats.expected_output_count}, "
            f"wrote={stats.written_count}, missing={stats.missing_expected_output_count}, "
            f"skipped_inputs={stats.skipped_input_count}, failures={stats.failure_count}, "
            f"first_error={first_error}"
        )
        return EnsembleJobResult(
            slug=slug,
            level=level,
            scope_name=state,
            status="failed",
            written_count=stats.written_count,
            expected_output_count=stats.expected_output_count,
            missing_expected_output_count=stats.missing_expected_output_count,
            skipped_input_count=stats.skipped_input_count,
            failure_count=stats.failure_count,
            summary=summary,
            errors=stats.errors,
            skipped_reasons=stats.skipped_reasons,
        )
    try:
        _write_ensemble_completion_marker(
            slug=slug,
            level=level,
            scope_name=state,
            allowed_models=allowed_models,
            allowed_scenarios=allowed_scenarios,
            expected_output_count=stats.expected_output_count,
        )
    except Exception as e:
        summary = (
            f"{state}/{level}/{slug}: expected={stats.expected_output_count}, wrote={stats.written_count}, "
            f"failures=1, first_error=marker_write_failed: {e}"
        )
        return EnsembleJobResult(
            slug=slug,
            level=level,
            scope_name=state,
            status="failed",
            written_count=stats.written_count,
            expected_output_count=stats.expected_output_count,
            missing_expected_output_count=stats.missing_expected_output_count,
            skipped_input_count=stats.skipped_input_count,
            failure_count=1,
            summary=summary,
            errors=(f"marker_write_failed: {e}",),
            skipped_reasons=stats.skipped_reasons,
        )
    return EnsembleJobResult(
        slug=slug,
        level=level,
        scope_name=state,
        status="success",
        written_count=stats.written_count,
        expected_output_count=stats.expected_output_count,
        missing_expected_output_count=stats.missing_expected_output_count,
        skipped_input_count=stats.skipped_input_count,
        failure_count=stats.failure_count,
        summary=(
            f"{state}/{level}/{slug}: expected={stats.expected_output_count}, "
            f"wrote={stats.written_count}, skipped_inputs={stats.skipped_input_count}, "
            f"failures={stats.failure_count}"
        ),
        errors=stats.errors,
        skipped_reasons=stats.skipped_reasons,
    )


def run_pipeline_parallel(
    num_workers: int = DEFAULT_WORKERS,
    verbose: bool = False,
    metrics_filter: list = None,
    models_filter: list = None,
    scenarios_filter: list = None,
    level: AdminLevel = "district",
    state: str = "Telangana",
    skip_existing: bool = False,
    overwrite: bool = False,
) -> PipelineRunResult:
    """Run the pipeline with parallel processing."""
    setup_logging(verbose)

    task_plan = build_processing_task_plan(
        metrics_filter=metrics_filter,
        models_filter=models_filter,
        scenarios_filter=scenarios_filter,
        level=level,
        state=state,
    )
    metrics_to_process = [(i, m) for i, m in enumerate(METRICS) if m["slug"] in set(task_plan.selected_metrics)]
    models_to_process = [m for m in MODELS if not models_filter or m in models_filter]
    scenarios_to_process = {k: v for k, v in SCENARIOS.items() if not scenarios_filter or k in scenarios_filter}
    tasks = list(task_plan.tasks)
    effective_state = task_plan.scope_name
    ensemble_models = _normalize_filter_values(models_filter)
    ensemble_scenarios = _normalize_filter_values(scenarios_filter)

    if overwrite:
        for slug in sorted({task.slug for task in tasks}):
            _cleanup_compute_outputs_for_overwrite(
                slug=slug,
                level=level,
                scope_name=effective_state,
                allowed_models=models_filter,
                allowed_scenarios=scenarios_filter,
            )

    if task_plan.skipped_counts_by_reason:
        joined = ", ".join(f"{reason}={count}" for reason, count in sorted(task_plan.skipped_counts_by_reason.items()))
        logging.info(f"Task builder skipped combinations ({joined})")

    ensemble_needed_slugs: set[str] = set(task.slug for task in tasks) if not skip_existing else set()
    if skip_existing:
        runnable_tasks = list(tasks)
        tasks = []
        skipped_existing = 0
        for task in runnable_tasks:
            if task_completion_marker_valid(task):
                skipped_existing += 1
            else:
                tasks.append(task)
                ensemble_needed_slugs.add(task.slug)
        for slug in {task.slug for task in runnable_tasks}:
            if slug not in {task.slug for task in tasks} and not ensemble_completion_marker_valid(
                slug=slug,
                level=level,
                scope_name=effective_state,
                allowed_models=ensemble_models,
                allowed_scenarios=ensemble_scenarios,
            ):
                ensemble_needed_slugs.add(slug)
        if skipped_existing:
            logging.info(
                "Skipping %s runnable compute tasks because validated completion markers already exist",
                skipped_existing,
            )

    if level == "sub_basin":
        level_display = "Sub-basin"
    elif level == "basin":
        level_display = "Basin"
    else:
        level_display = "Block" if level == "block" else "District"
    level_folder = get_level_folder(level)
    
    # Determine SPI implementation info
    spi_impl = "climate-indices package" if USE_CLIMATE_INDICES_PACKAGE and CLIMATE_INDICES_AVAILABLE else "legacy (scipy)"
    
    logging.info("=" * 60)
    logging.info("India Resilience Tool - Climate Index Pipeline")
    logging.info(f"Level: {level_display} (folder: {level_folder}/)")
    logging.info(f"Output scope: {effective_state}")
    logging.info(f"Metrics: {len(metrics_to_process)}, Models: {len(models_to_process)}, Scenarios: {len(scenarios_to_process)}")
    logging.info(f"Runnable tasks: {len(task_plan.tasks)}, Tasks to execute: {len(tasks)}, Workers: {num_workers}")
    logging.info(f"SPI/SPEI implementation: {spi_impl}")
    logging.info("=" * 60)

    if not tasks and not ensemble_needed_slugs:
        logging.info("No compute or ensemble work is pending.")
        return PipelineRunResult(
            level=level,
            scope_name=effective_state,
            compute_failed_count=0,
            ensemble_results=(),
        )

    start = time.time()
    results = []
    completed = 0
    failed = 0
    
    if num_workers == 1:
        # Sequential mode
        boundary_path = get_boundary_path(level)
        gdf = load_boundaries(
            boundary_path,
            state_filter=None if level in {"basin", "sub_basin"} else state,
            level=level,
        )
        logging.info(f"Loaded {len(gdf)} {level} boundaries for {effective_state}")

        for task in tasks:
            try:
                result = _execute_processing_task(task, gdf)
                result["status"] = "success"
                results.append(result)
            except Exception as e:
                logging.error(f"Task failed for {task.slug}/{task.model}/{task.scenario}: {e}")
                logging.debug(traceback.format_exc())
                results.append({"status": "failed", "error": str(e)})
                failed += 1
            completed += 1
            if completed % 10 == 0:
                logging.info(f"Progress: {completed}/{len(tasks)} ({failed} failed)")
    else:
        # Parallel mode
        init_fn = partial(_worker_init, level, state)
        with Pool(num_workers, initializer=init_fn) as pool:
            for r in pool.imap_unordered(_worker_process_task, tasks):
                results.append(r)
                completed += 1
                if r["status"] == "failed":
                    failed += 1
                if completed % 10 == 0:
                    logging.info(f"Progress: {completed}/{len(tasks)} ({failed} failed)")

    logging.info(f"Computation: {time.time() - start:.1f}s, Success: {completed - failed}, Failed: {failed}")
    logging.info("Building ensembles...")

    ensemble_args = [
        (slug, level, effective_state, ensemble_models, ensemble_scenarios)
        for slug in sorted(ensemble_needed_slugs)
    ]
    ensemble_results: list[EnsembleJobResult] = []
    if ensemble_args:
        if num_workers == 1:
            for args in ensemble_args:
                ensemble_results.append(_compute_ensembles_for_metric(args))
        else:
            with Pool(num_workers) as pool:
                ensemble_results = list(pool.imap_unordered(_compute_ensembles_for_metric, ensemble_args))
    else:
        logging.info("No ensemble work is pending.")

    logging.info(f"TOTAL: {time.time() - start:.1f}s")
    return PipelineRunResult(
        level=level,
        scope_name=effective_state,
        compute_failed_count=failed,
        ensemble_results=tuple(ensemble_results),
    )

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="IRT Climate Index Pipeline (Multiprocess)")
    parser.add_argument("-w", "--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Number of worker processes (default: {DEFAULT_WORKERS})")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable verbose/debug logging")
    parser.add_argument(
        "-l",
        "--level",
        choices=["district", "block", "basin", "sub_basin", "both"],
        default="both",
        help="Spatial level for aggregation (default: both = district + block)",
    )
    parser.add_argument("-s", "--state", default="Telangana",
                        help="State to process (default: Telangana)")
    parser.add_argument("--metrics", nargs="+",
                        help="Filter to specific metric slugs")
    parser.add_argument("--models", nargs="+",
                        help="Filter to specific models")
    parser.add_argument("--scenarios", nargs="+",
                        help="Filter to specific scenarios")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip compute tasks with validated completion markers and intact outputs.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete the selected compute outputs and markers before rebuilding.",
    )
    parser.add_argument("--list-metrics", action="store_true",
                        help="List available metrics and exit")
    parser.add_argument("--list-models", action="store_true",
                        help="List discovered models and exit")
    parser.add_argument("--spi-legacy", action="store_true",
                        help="Force use of legacy SPI implementation (scipy-based) instead of climate-indices package")
    parser.add_argument("--spi-distribution", choices=["gamma", "pearson"], default="gamma",
                        help="Distribution for SPI fitting when using climate-indices package (default: gamma)")
    args = parser.parse_args(argv)
    
    # Handle SPI implementation + distribution flags
    global USE_CLIMATE_INDICES_PACKAGE
    global SPI_DISTRIBUTION
    SPI_DISTRIBUTION = args.spi_distribution

    if args.spi_legacy:
        USE_CLIMATE_INDICES_PACKAGE = False
        logging.info("SPI: Using legacy scipy-based implementation (--spi-legacy flag)")
    elif CLIMATE_INDICES_AVAILABLE:
        logging.info("SPI: Using climate-indices package")
    else:
        logging.info("SPI: Using legacy scipy-based implementation (climate-indices not installed)")
    
    if args.list_metrics:
        print("Available metrics:")
        for m in METRICS:
            print(f"  {m['slug']}: {m['name']}")
        print(f"Total: {len(METRICS)}")
        return 0
    
    if args.list_models:
        print("Discovered models:")
        for m in MODELS:
            print(f"  {m}")
        print(f"Total: {len(MODELS)}")
        return 0
    
    # Ensure our banners use the same log format as the pipeline itself
    setup_logging(args.verbose)

    levels_to_run = ["district", "block"] if args.level == "both" else [args.level]
    total_runs = len(levels_to_run)
    run_results: list[PipelineRunResult] = []

    for run_idx, lvl in enumerate(levels_to_run, start=1):
        logging.info("#" * 78)
        logging.info(f"RUN {run_idx}/{total_runs}: {lvl.upper()} LEVEL")
        logging.info("#" * 78)

        run_results.append(
            run_pipeline_parallel(
                num_workers=args.workers,
                verbose=args.verbose,
                metrics_filter=args.metrics,
                models_filter=args.models,
                scenarios_filter=args.scenarios,
                level=lvl,
                state=args.state,
                skip_existing=bool(args.skip_existing),
                overwrite=bool(args.overwrite),
            )
        )

    failed_ensemble_results = [
        result
        for run_result in run_results
        for result in run_result.ensemble_results
        if result.status != "success"
    ]
    if failed_ensemble_results:
        logging.error("ENSEMBLE FAILURE SUMMARY (%s jobs)", len(failed_ensemble_results))
        for result in failed_ensemble_results:
            logging.error("  %s", result.summary)
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

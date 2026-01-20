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

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

import os, glob, sys, time, argparse, logging, json, traceback
from pathlib import Path
from typing import Literal, Optional
from dataclasses import dataclass
from functools import partial
from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from rasterio import features
from affine import Affine

from paths import DATA_ROOT, DISTRICTS_PATH, BLOCKS_PATH, BASE_OUTPUT_ROOT
from india_resilience_tool.config.metrics_registry import PIPELINE_METRICS_RAW

# Type alias for administrative level
AdminLevel = Literal["district", "block"]

# Folder names for clean separation
DISTRICT_FOLDER = "districts"
BLOCK_FOLDER = "blocks"

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------
SCENARIOS = {
    "historical": {"subdir": "historical/tas", "periods": {"1990-2010": (1990, 2010)}},
    "ssp245": {"subdir": "ssp245/tas", "periods": {"2020-2040": (2020, 2040), "2040-2060": (2040, 2060)}},
    "ssp585": {"subdir": "ssp585/tas", "periods": {"2020-2040": (2020, 2040), "2040-2060": (2040, 2060)}},
}
MIN_YEARS_REQUIRED_FRACTION = 0.6
MIN_YEARS_ABSOLUTE = 5
METRICS = PIPELINE_METRICS_RAW
DEFAULT_WORKERS = max(1, int(cpu_count() * 0.75))

def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

# -----------------------------------------------------------------------------
# BASIC HELPERS
# -----------------------------------------------------------------------------
def metric_root(slug: str) -> Path:
    root = BASE_OUTPUT_ROOT / slug
    root.mkdir(parents=True, exist_ok=True)
    return root

def get_level_folder(level: AdminLevel) -> str:
    """Get the subfolder name for a given level."""
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
    return BLOCKS_PATH if level == "block" else DISTRICTS_PATH

def load_boundaries(
    path: Path,
    state_filter: Optional[str] = None,
    level: AdminLevel = "district",
) -> gpd.GeoDataFrame:
    """
    Load boundary file and optionally filter to a specific state.
    """
    gdf = gpd.read_file(path)
    
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
    return "block_name" if level == "block" else "district_name"

# Legacy compatibility
def load_telangana_districts(path: Path) -> gpd.GeoDataFrame:
    """Legacy function - use load_boundaries() instead."""
    return load_boundaries(path, state_filter="Telangana", level="district")

# -----------------------------------------------------------------------------
# MASK BUILDING (Generalized)
# -----------------------------------------------------------------------------
def build_unit_masks(
    gdf: gpd.GeoDataFrame,
    sample_ds: xr.Dataset,
    level: AdminLevel = "district",
) -> dict:
    """
    Build raster masks for each spatial unit (district or block).
    
    Returns dict with keys:
    - For districts: "district_name"
    - For blocks: "district_name||block_name" (double pipe separator)
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
    
    masks = {}
    for _, row in gdf.iterrows():
        if row.geometry is None:
            continue
        
        unit_name = str(row[unit_col]).strip()
        
        # For blocks, include district name to ensure uniqueness
        # Use || as separator (safe for filenames when we split later)
        if level == "block" and "district_name" in gdf.columns:
            district = str(row["district_name"]).strip()
            key = f"{district}||{unit_name}"
        else:
            key = unit_name
        
        mask = features.rasterize(
            [(row.geometry, 1)],
            out_shape=(height, width),
            transform=transform,
            fill=0,
            all_touched=True,
            dtype="uint8"
        )
        masks[key] = xr.DataArray(
            mask.astype(bool),
            coords={"lat": sample_ds["lat"], "lon": sample_ds["lon"]},
            dims=("lat", "lon")
        )
    
    return masks

# Legacy compatibility
def build_district_masks(gdf: gpd.GeoDataFrame, sample_ds: xr.Dataset, district_name_col: str = "DISTRICT") -> dict:
    """Legacy function - use build_unit_masks() instead."""
    if district_name_col not in gdf.columns and "district_name" in gdf.columns:
        gdf = gdf.copy()
        gdf[district_name_col] = gdf["district_name"]
    return build_unit_masks(gdf, sample_ds, level="district")

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
    daily_mean = da.where(mask).mean(dim=("lat", "lon"), skipna=True)
    return daily_mean.dropna(dim="time", how="all") if "time" in daily_mean.dims else daily_mean


def _filter_to_baseline(da: xr.DataArray, baseline_years: tuple[int, int]) -> xr.DataArray:
    """
    Filter a time-indexed DataArray to only include data within baseline years.
    
    Args:
        da: DataArray with 'time' dimension
        baseline_years: Tuple of (start_year, end_year) inclusive
        
    Returns:
        DataArray filtered to baseline period, or original if filtering fails
    """
    if da.size == 0 or "time" not in da.dims:
        return da
    
    start_year, end_year = baseline_years
    try:
        years = da["time"].dt.year
        mask = (years >= start_year) & (years <= end_year)
        filtered = da.where(mask, drop=True)
        # If baseline period has no data, fall back to full series
        if filtered.size == 0:
            return da
        return filtered
    except Exception:
        # If filtering fails (e.g., no time coordinate), return original
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


def wet_bulb_annual_mean_stull(tas_da: xr.DataArray, hurs_da: xr.DataArray, mask: xr.DataArray) -> float:
    """Annual mean wet-bulb temperature (°C) using Stull approximation."""
    tas_k = _get_district_daily_mean(tas_da, mask)
    rh = _get_district_daily_mean(hurs_da, mask)
    if tas_k.sizes.get("time", 0) == 0:
        return np.nan
    twb = _wet_bulb_stull_c(tas_k - 273.15, rh)
    return float(twb.mean(dim="time", skipna=True).item())


def wet_bulb_annual_max_stull(tas_da: xr.DataArray, hurs_da: xr.DataArray, mask: xr.DataArray) -> float:
    """Annual maximum wet-bulb temperature (°C) using Stull approximation."""
    tas_k = _get_district_daily_mean(tas_da, mask)
    rh = _get_district_daily_mean(hurs_da, mask)
    if tas_k.sizes.get("time", 0) == 0:
        return np.nan
    twb = _wet_bulb_stull_c(tas_k - 273.15, rh)
    return float(twb.max(dim="time", skipna=True).item())


def wet_bulb_days_ge_threshold_stull(
    tas_da: xr.DataArray,
    hurs_da: xr.DataArray,
    mask: xr.DataArray,
    thresh_c: float = 30.0,
) -> int:
    """Count of days per year where wet-bulb temperature (°C) is >= `thresh_c` (Stull)."""
    tas_k = _get_district_daily_mean(tas_da, mask)
    rh = _get_district_daily_mean(hurs_da, mask)
    if tas_k.sizes.get("time", 0) == 0:
        return 0
    twb = _wet_bulb_stull_c(tas_k - 273.15, rh)
    return int((twb >= float(thresh_c)).sum(dim="time", skipna=True).item())

# -----------------------------------------------------------------------------
# TEMPERATURE COMPUTE FUNCTIONS
# -----------------------------------------------------------------------------
def count_days_above_threshold(da, mask, thresh_k): return int((_get_district_daily_mean(da, mask) > thresh_k).sum().item())
def count_days_ge_threshold(da, mask, thresh_k): return int((_get_district_daily_mean(da, mask) >= thresh_k).sum().item())
def count_days_below_threshold(da, mask, thresh_k): return int((_get_district_daily_mean(da, mask) < thresh_k).sum().item())

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

def consecutive_run_events_above_threshold(da, mask, thresh_k, min_event_days=6):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return 0
    return _count_events(np.asarray((dm > thresh_k).fillna(False).values, dtype=bool), min_event_days)

def percentile_days_above(da, mask, percentile=90, baseline_years=(1985, 2014)):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years)
    thresh = float(baseline_dm.quantile(percentile / 100.0).item())
    return 100.0 * (dm > thresh).sum().item() / dm.size

def percentile_days_below(da, mask, percentile=10, baseline_years=(1985, 2014)):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years)
    thresh = float(baseline_dm.quantile(percentile / 100.0).item())
    return 100.0 * (dm < thresh).sum().item() / dm.size

def warm_spell_duration_index(da, mask, percentile=90, min_spell_days=6, baseline_years=(1985, 2014)):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return 0
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years)
    thresh = float(baseline_dm.quantile(percentile / 100.0).item())
    _, total = _run_length_stats(np.asarray((dm > thresh).fillna(False).values, dtype=bool), min_spell_days)
    return int(total)

def cold_spell_duration_index(da, mask, percentile=10, min_spell_days=6, baseline_years=(1985, 2014)):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return 0
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years)
    thresh = float(baseline_dm.quantile(percentile / 100.0).item())
    _, total = _run_length_stats(np.asarray((dm < thresh).fillna(False).values, dtype=bool), min_spell_days)
    return int(total)

def heatwave_duration_index(da, mask, baseline_years=(1985, 2014), delta_c=5.0, abs_thresh_k=313.15, min_spell_days=5):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return 0
    # Calculate 90th percentile threshold from baseline period
    baseline_dm = _filter_to_baseline(dm, baseline_years)
    thresh = max(abs_thresh_k, float(baseline_dm.quantile(0.9).item()))
    max_run, _ = _run_length_stats(np.asarray((dm >= thresh).fillna(False).values, dtype=bool), min_spell_days)
    return int(max_run)

def heatwave_frequency_percentile(da, mask, baseline_years=(1985, 2014), pct=90, min_spell_days=5):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return 0
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years)
    thresh = float(baseline_dm.quantile(pct / 100.0).item())
    _, total = _run_length_stats(np.asarray((dm > thresh).fillna(False).values, dtype=bool), min_spell_days)
    return int(total)

def heatwave_event_count(da, mask, baseline_years=(1985, 2014), delta_c=5.0, abs_thresh_k=313.15, min_spell_days=5):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return 0
    # Calculate 90th percentile threshold from baseline period
    baseline_dm = _filter_to_baseline(dm, baseline_years)
    thresh = max(abs_thresh_k, float(baseline_dm.quantile(0.9).item()))
    return _count_events(np.asarray((dm >= thresh).fillna(False).values, dtype=bool), min_spell_days)

def heatwave_event_count_percentile(da, mask, baseline_years=(1985, 2014), pct=90, min_spell_days=5):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return 0
    # Calculate threshold from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years)
    thresh = float(baseline_dm.quantile(pct / 100.0).item())
    return _count_events(np.asarray((dm > thresh).fillna(False).values, dtype=bool), min_spell_days)

def heatwave_magnitude(da, mask, baseline_years=(1985, 2014), min_spell_days=3):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate 90th percentile threshold from baseline period
    baseline_dm = _filter_to_baseline(dm, baseline_years)
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

def heatwave_amplitude(da, mask, baseline_years=(1985, 2014), min_spell_days=3):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return np.nan
    # Calculate 90th percentile threshold from baseline period
    baseline_dm = _filter_to_baseline(dm, baseline_years)
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
    return float((tx - tn).mean().item())


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
    return float(tx.max().item()) - float(tn.min().item())

def growing_season_length(da, mask, thresh_k=278.15, min_spell_days=6):
    dm = _get_district_daily_mean(da, mask)
    if dm.size == 0: return 0
    above, below = (dm > thresh_k).values, (dm < thresh_k).values
    n_days = len(above)
    start_idx, run = None, 0
    for i, v in enumerate(above):
        if v:
            run += 1
            if run >= min_spell_days and start_idx is None: start_idx = i - min_spell_days + 1
        else: run = 0
    if start_idx is None: return 0
    mid_year, end_idx, run = n_days // 2, None, 0
    for i in range(mid_year, n_days):
        if below[i]:
            run += 1
            if run >= min_spell_days: end_idx = i - min_spell_days + 1; break
        else: run = 0
    return max(0, (end_idx or n_days - 1) - start_idx)

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

def percentile_precipitation_total(da, mask, percentile=95, baseline_years=(1985, 2014)):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return np.nan
    wet = dm.where(dm >= 1.0, drop=True)
    if wet.size == 0: return 0.0
    # Calculate threshold from baseline period only (wet days in baseline)
    baseline_wet = _filter_to_baseline(wet, baseline_years)
    if baseline_wet.size == 0:
        baseline_wet = wet  # Fall back to full series if no baseline data
    thresh = float(baseline_wet.quantile(percentile / 100.0).item())
    return float(dm.where(dm > thresh, 0).sum().item())

def percentile_precipitation_contribution(da, mask, percentile=95, baseline_years=(1985, 2014)):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return np.nan
    wet = dm.where(dm >= 1.0, drop=True)
    if wet.size == 0: return 0.0
    prcptot = float(wet.sum().item())
    if prcptot <= 0: return 0.0
    # Calculate threshold from baseline period only (wet days in baseline)
    baseline_wet = _filter_to_baseline(wet, baseline_years)
    if baseline_wet.size == 0:
        baseline_wet = wet  # Fall back to full series if no baseline data
    thresh = float(baseline_wet.quantile(percentile / 100.0).item())
    return 100.0 * float(dm.where(dm > thresh, 0).sum().item()) / prcptot

def standardised_precipitation_index(da, mask, scale_months=3, baseline_years=(1985, 2014)):
    dm = _get_district_daily_mean(pr_to_mm_per_day(da), mask)
    if dm.size == 0: return np.nan
    total = float(dm.sum().item())
    # Calculate mean and std from baseline period only
    baseline_dm = _filter_to_baseline(dm, baseline_years)
    mean_p, std_p = float(baseline_dm.mean().item()) * 365, float(baseline_dm.std().item()) * np.sqrt(365)
    return (total - mean_p) / std_p if std_p > 0 else 0.0

def standardised_precipitation_evapotranspiration_index(da, mask, scale_months=3, baseline_years=(1985, 2014)):
    return standardised_precipitation_index(da, mask, scale_months, baseline_years)

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
        valid_year_files, _bad_year_files = validated_year_files(data_dir)
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
            valid_year_files, _bad_year_files = validated_year_files(vdir)
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

    masks = build_unit_masks(gdf, ds_sample, level=level)
    ds_sample.close()

    if not masks:
        logging.warning(f"[{slug}] No valid masks built for {level} level")
        return

    # Get the level subfolder
    level_folder = BLOCK_FOLDER if level == "block" else DISTRICT_FOLDER

    rows = []
    for year, paths_by_var in year_to_paths.items():
        ds_by_var: dict[str, xr.Dataset] = {}
        da_by_var: dict[str, xr.DataArray] = {}

        try:
            for v, nc_path in paths_by_var.items():
                ds = normalize_lat_lon(xr.open_dataset(nc_path))
                if v not in ds:
                    raise KeyError(f"Variable '{v}' not found in {nc_path}")
                ds_by_var[v] = ds
                da_by_var[v] = ds[v]
            for v, nc_path in paths_by_var.items():
                ds = normalize_lat_lon(xr.open_dataset(nc_path))
                if v not in ds:
                    raise KeyError(f"Variable '{v}' not found in {nc_path}")
                ds_by_var[v] = ds
                da_by_var[v] = ds[v]
                ## Following lines for debugging
                # if v == "tasmin" and year == 2000:
                #     da_by_var[v] = xr.full_like(da_by_var[v], 300.0)  # 300 K constant

            for unit_key, mask in masks.items():
                if len(req_vars) <= 1:
                    v = compute_fn(da_by_var[primary_var], mask, **params)
                else:
                    # Wet-bulb and other multi-var metrics (currently assumes two vars)
                    v = compute_fn(da_by_var[req_vars[0]], da_by_var[req_vars[1]], mask, **params)

                row = {
                    "year": year,
                    "value": v,
                    value_col: v,
                    "source_file": str(paths_by_var.get(primary_var) or next(iter(paths_by_var.values()))),
                }

                # Parse unit_key based on level
                if level == "block":
                    # Key format: "district||block"
                    if "||" in unit_key:
                        district, block = unit_key.split("||", 1)
                        row["district"] = district
                        row["block"] = block
                    else:
                        row["district"] = "Unknown"
                        row["block"] = unit_key
                else:
                    row["district"] = unit_key

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

    df_yearly = pd.DataFrame(rows)

    # Period aggregation (mean over years used, consistent with other day-count metrics)
    group_cols = ["district", "block", "model", "scenario"] if level == "block" else ["district", "model", "scenario"]
    period_frames = []

    for period_name, (y0, y1) in scenario_conf["periods"].items():
        avail = [y for y in year_to_paths.keys() if y0 <= y <= y1]
        n_req, n_avail = y1 - y0 + 1, len(avail)
        if n_avail >= MIN_YEARS_ABSOLUTE and n_avail / n_req >= MIN_YEARS_REQUIRED_FRACTION:
            grp = df_yearly[df_yearly["year"].isin(avail)].groupby(
                [c for c in group_cols if c in df_yearly.columns]
            ).agg({"value": "mean"}).reset_index()
            grp["period"] = period_name
            grp["years_used_count"] = n_avail
            grp["years_requested"] = n_req
            grp[value_col] = grp["value"]
            period_frames.append(grp)

    df_periods = pd.concat(period_frames, ignore_index=True) if period_frames else pd.DataFrame()

    # Write outputs with clean folder structure
    if level == "block":
        # Structure: {metric}/{state}/blocks/{district}/{block}/{model}/{scenario}/
        for (district, block), grp_df in df_yearly.groupby(["district", "block"]):
            district_safe = district.replace(" ", "_").replace("/", "_")
            block_safe = block.replace(" ", "_").replace("/", "_")

            out_dir = metric_root_path / state_name / level_folder / district_safe / block_safe / model / scenario
            out_dir.mkdir(parents=True, exist_ok=True)
            grp_df["model"] = model
            grp_df["scenario"] = scenario

            grp_df.to_csv(out_dir / f"{block_safe}_yearly.csv", index=False)

            if not df_periods.empty:
                period_mask = (
                    (df_periods["district"] == district) &
                    (df_periods["block"] == block)
                )
                period_grp = df_periods.loc[period_mask].copy()

                if not period_grp.empty:
                    # Avoid pandas SettingWithCopyWarning by working on an explicit copy.
                    period_grp["model"] = model
                    period_grp["scenario"] = scenario
                    period_grp.to_csv(out_dir / f"{block_safe}_periods.csv", index=False)
    else:
        # Structure: {metric}/{state}/districts/{district}/{model}/{scenario}/
        for dist_name in df_yearly["district"].unique():
            dist_safe = dist_name.replace(" ", "_").replace("/", "_")

            out_dir = metric_root_path / state_name / level_folder / dist_safe / model / scenario
            out_dir.mkdir(parents=True, exist_ok=True)

            dist_df = df_yearly[df_yearly["district"] == dist_name].copy()
            dist_df["model"] = model
            dist_df["scenario"] = scenario
            dist_df.to_csv(out_dir / f"{dist_safe}_yearly.csv", index=False)

            if not df_periods.empty:
                period_df = df_periods[df_periods["district"] == dist_name].copy()
                if not period_df.empty:
                    period_df["model"] = model
                    period_df["scenario"] = scenario
                    period_df.to_csv(out_dir / f"{dist_safe}_periods.csv", index=False)


# -----------------------------------------------------------------------------
# ENSEMBLE COMPUTATION (Generalized)
# -----------------------------------------------------------------------------
def compute_ensembles_generic(
    output_root: Path,
    state: str = "Telangana",
    level: AdminLevel = "district",
):
    """Compute ensemble statistics across models."""
    root = Path(output_root)
    level_folder = get_level_folder(level)
    
    # Data lives in: {state}/{level_folder}/...
    # Ensembles go to: {state}/{level_folder}/ensembles/...
    level_root = root / state / level_folder
    
    if not level_root.exists():
        logging.warning(f"Level root does not exist: {level_root}")
        return
    
    ensembles_root = level_root / "ensembles"
    ensembles_root.mkdir(parents=True, exist_ok=True)
    
    if level == "block":
        _compute_block_ensembles(level_root, ensembles_root)
    else:
        _compute_district_ensembles(level_root, ensembles_root)

def _compute_district_ensembles(level_root: Path, ensembles_root: Path):
    """Compute ensembles for district-level data."""
    skip_dirs = {"ensembles"}
    district_dirs = [
        p for p in level_root.iterdir()
        if p.is_dir() and p.name not in skip_dirs
    ]
    
    for ddir in district_dirs:
        district = ddir.name
        model_dirs = [p for p in ddir.iterdir() if p.is_dir()]
        scenarios = sorted({s.name for m in model_dirs for s in m.iterdir() if s.is_dir()})
        
        for scenario in scenarios:
            model_yearly = []
            for m in model_dirs:
                ycsv = m / scenario / f"{district}_yearly.csv"
                if ycsv.exists():
                    try:
                        dfy = pd.read_csv(ycsv)
                        if "value" not in dfy.columns:
                            cols = [c for c in dfy.columns if c not in {"district", "model", "scenario", "year", "source_file"}]
                            if cols: dfy["value"] = dfy[cols[0]]
                        dfy["model"] = m.name
                        model_yearly.append(dfy)
                    except:
                        pass
            
            if model_yearly:
                _write_ensemble_stats(model_yearly, ensembles_root / district / scenario, district)

def _compute_block_ensembles(level_root: Path, ensembles_root: Path):
    """Compute ensembles for block-level data."""
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
            model_dirs = [p for p in bdir.iterdir() if p.is_dir()]
            scenarios = sorted({s.name for m in model_dirs for s in m.iterdir() if s.is_dir()})
            
            for scenario in scenarios:
                model_yearly = []
                for m in model_dirs:
                    ycsv = m / scenario / f"{block}_yearly.csv"
                    if ycsv.exists():
                        try:
                            dfy = pd.read_csv(ycsv)
                            if "value" not in dfy.columns:
                                cols = [c for c in dfy.columns if c not in {"district", "block", "model", "scenario", "year", "source_file"}]
                                if cols: dfy["value"] = dfy[cols[0]]
                            dfy["model"] = m.name
                            model_yearly.append(dfy)
                        except:
                            pass
                
                if model_yearly:
                    out_dir = ensembles_root / district / block / scenario
                    _write_ensemble_stats(model_yearly, out_dir, block)

def _write_ensemble_stats(model_yearly: list, out_dir: Path, unit_name: str):
    """Write ensemble statistics CSV."""
    df_yc = pd.concat(model_yearly, ignore_index=True)
    if "year" in df_yc.columns:
        df_yc["year"] = df_yc["year"].astype(int)
        pivot = df_yc.pivot_table(index="year", columns="model", values="value", aggfunc="first")
        summary = pd.DataFrame({
            "year": pivot.index,
            "n_models": pivot.count(axis=1),
            "ensemble_mean": pivot.mean(axis=1),
            "ensemble_std": pivot.std(axis=1, ddof=0),
            "ensemble_median": pivot.median(axis=1),
            "ensemble_p05": pivot.quantile(0.05, axis=1),
            "ensemble_p95": pivot.quantile(0.95, axis=1),
        }).reset_index(drop=True)
        
        out_dir.mkdir(parents=True, exist_ok=True)
        summary.to_csv(out_dir / f"{unit_name}_yearly_ensemble.csv", index=False)

# -----------------------------------------------------------------------------
# MULTIPROCESSING (Updated for level support)
# -----------------------------------------------------------------------------
@dataclass
class ProcessingTask:
    metric_idx: int
    model: str
    scenario: str
    scenario_conf: dict
    task_id: int
    total_tasks: int
    level: str = "district"
    state_name: str = "Telangana"

# Global worker state
_worker_gdf = None
_worker_level = "district"
_worker_state = "Telangana"

def _worker_init(level: str = "district", state: str = "Telangana"):
    global _worker_gdf, _worker_level, _worker_state
    _worker_level = level
    _worker_state = state
    boundary_path = get_boundary_path(level)
    _worker_gdf = load_boundaries(boundary_path, state_filter=state, level=level)

def _worker_process_task(task: ProcessingTask) -> dict:
    global _worker_gdf, _worker_level, _worker_state
    start = time.time()
    metric = METRICS[task.metric_idx]
    result = {
        "task_id": task.task_id,
        "slug": metric["slug"],
        "model": task.model,
        "scenario": task.scenario,
        "status": "success",
        "error": None
    }
    try:
        process_metric_for_model_scenario(
            metric, task.model, task.scenario, task.scenario_conf, _worker_gdf,
            level=_worker_level, state_name=_worker_state
        )
    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
    result["duration"] = time.time() - start
    return result

def _compute_ensembles_for_metric(args: tuple) -> dict:
    slug, level, state = args
    result = {"slug": slug, "status": "success", "error": None}
    try:
        compute_ensembles_generic(metric_root(slug), state=state, level=level)
    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
    return result


def run_pipeline_parallel(
    num_workers: int = DEFAULT_WORKERS,
    verbose: bool = False,
    metrics_filter: list = None,
    models_filter: list = None,
    scenarios_filter: list = None,
    level: AdminLevel = "district",
    state: str = "Telangana",
):
    """Run the pipeline with parallel processing."""
    setup_logging(verbose)
    
    metrics_to_process = [(i, m) for i, m in enumerate(METRICS) if not metrics_filter or m["slug"] in metrics_filter]
    models_to_process = [m for m in MODELS if not models_filter or m in models_filter]
    scenarios_to_process = {k: v for k, v in SCENARIOS.items() if not scenarios_filter or k in scenarios_filter}
    
    for _, m in metrics_to_process:
        metric_root(m["slug"])
    tasks = []

    # Cache available years per (model, scenario, var) to avoid repeated disk scans
    years_cache: dict[tuple[str, str, str], set[int]] = {}
    skipped = 0

    def _years_for(model_name: str, scenario_name: str, sconf: dict, varname: str) -> set[int]:
        key = (model_name, scenario_name, varname)
        if key in years_cache:
            return years_cache[key]
        d = var_data_dir(DATA_ROOT, sconf["subdir"], varname, model_name)
        yrs = set(yearly_files_for_dir(d).keys()) if d.exists() else set()
        years_cache[key] = yrs
        return yrs

    for model in models_to_process:
        for scenario, sconf in scenarios_to_process.items():
            for midx, metric in metrics_to_process:
                req_vars = required_vars_for_metric(metric)
                if not req_vars:
                    skipped += 1
                    continue

                year_sets = [_years_for(model, scenario, sconf, v) for v in req_vars]
                if any(len(s) == 0 for s in year_sets):
                    skipped += 1
                    continue

                common_years = set.intersection(*year_sets) if year_sets else set()
                if not common_years:
                    skipped += 1
                    continue

                tasks.append(
                    ProcessingTask(
                        midx,
                        model,
                        scenario,
                        sconf,
                        len(tasks),
                        0,
                        level=level,
                        state_name=state,
                    )
                )

    for t in tasks:
        t.total_tasks = len(tasks)

    if skipped:
        logging.info(
            f"Task builder skipped {skipped} (metric, model, scenario) combinations due to missing required variables/years"
        )

    level_display = "Block" if level == "block" else "District"
    level_folder = get_level_folder(level)
    
    logging.info("=" * 60)
    logging.info("India Resilience Tool - Climate Index Pipeline")
    logging.info(f"Level: {level_display} (folder: {level_folder}/)")
    logging.info(f"State: {state}")
    logging.info(f"Metrics: {len(metrics_to_process)}, Models: {len(models_to_process)}, Scenarios: {len(scenarios_to_process)}")
    logging.info(f"Total tasks: {len(tasks)}, Workers: {num_workers}")
    logging.info("=" * 60)
    
    if not tasks:
        logging.warning("No tasks!")
        return
    
    start = time.time()
    results = []
    completed = 0
    failed = 0
    
    if num_workers == 1:
        # Sequential mode
        boundary_path = get_boundary_path(level)
        gdf = load_boundaries(boundary_path, state_filter=state, level=level)
        logging.info(f"Loaded {len(gdf)} {level} boundaries for {state}")
        
        for task in tasks:
            metric = METRICS[task.metric_idx]
            try:
                process_metric_for_model_scenario(
                    metric, task.model, task.scenario, task.scenario_conf, gdf,
                    level=level, state_name=state
                )
                results.append({"status": "success"})
            except Exception as e:
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
    
    # Build ensembles
    slugs = [m["slug"] for _, m in metrics_to_process]
    ensemble_args = [(s, level, state) for s in slugs]
    
    if num_workers == 1:
        for args in ensemble_args:
            _compute_ensembles_for_metric(args)
    else:
        with Pool(num_workers) as pool:
            list(pool.imap_unordered(_compute_ensembles_for_metric, ensemble_args))
    
    logging.info(f"TOTAL: {time.time() - start:.1f}s")

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="IRT Climate Index Pipeline (Multiprocess)")
    parser.add_argument("-w", "--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Number of worker processes (default: {DEFAULT_WORKERS})")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable verbose/debug logging")
    parser.add_argument(
        "-l",
        "--level",
        choices=["district", "block", "both"],
        default="both",
        help="Administrative level for spatial aggregation (default: both)",
    )
    parser.add_argument("-s", "--state", default="Telangana",
                        help="State to process (default: Telangana)")
    parser.add_argument("--metrics", nargs="+",
                        help="Filter to specific metric slugs")
    parser.add_argument("--models", nargs="+",
                        help="Filter to specific models")
    parser.add_argument("--scenarios", nargs="+",
                        help="Filter to specific scenarios")
    parser.add_argument("--list-metrics", action="store_true",
                        help="List available metrics and exit")
    parser.add_argument("--list-models", action="store_true",
                        help="List discovered models and exit")
    args = parser.parse_args()
    
    if args.list_metrics:
        print("Available metrics:")
        for m in METRICS:
            print(f"  {m['slug']}: {m['name']}")
        print(f"Total: {len(METRICS)}")
        return
    
    if args.list_models:
        print("Discovered models:")
        for m in MODELS:
            print(f"  {m}")
        print(f"Total: {len(MODELS)}")
        return
    
    # Ensure our banners use the same log format as the pipeline itself
    setup_logging(args.verbose)

    levels_to_run = ["district", "block"] if args.level == "both" else [args.level]
    total_runs = len(levels_to_run)

    for run_idx, lvl in enumerate(levels_to_run, start=1):
        logging.info("#" * 78)
        logging.info(f"RUN {run_idx}/{total_runs}: {lvl.upper()} LEVEL")
        logging.info("#" * 78)

        run_pipeline_parallel(
            num_workers=args.workers,
            verbose=args.verbose,
            metrics_filter=args.metrics,
            models_filter=args.models,
            scenarios_filter=args.scenarios,
            level=lvl,
            state=args.state,
        )

if __name__ == "__main__":
    main()
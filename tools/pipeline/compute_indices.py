#!/usr/bin/env python3
"""
Uniform, future-proof index pipeline for the India Resilience Tool.

Supports both district-level (ADM2) and block-level (ADM3) spatial aggregation.

Includes all standard Climdex indices plus custom IRT indices:
- Heat risk / thermal stress indices
- Cold risk indices  
- Precipitation / flood-related indices
- Drought / dryness indices

Usage:
    # District-level (default, backward compatible)
    python compute_indices.py
    python compute_indices.py --level district
    
    # Block-level
    python compute_indices.py --level block
    
    # Specific state only
    python compute_indices.py --level block --state Telangana

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

import os
import glob
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from rasterio import features
from affine import Affine
import logging
import traceback
import json
from typing import Literal, Optional, Dict, Any

from paths import DATA_ROOT, DISTRICTS_PATH, BLOCKS_PATH, BASE_OUTPUT_ROOT
from india_resilience_tool.config.metrics_registry import PIPELINE_METRICS_RAW

# Type alias for administrative level
AdminLevel = Literal["district", "block"]

# Scenarios
SCENARIOS = {
    "historical": {
        "subdir": "historical/tas",
        "periods": {"1990-2010": (1990, 2010)},
    },
    "ssp245": {
        "subdir": "ssp245/tas",
        "periods": {"2020-2040": (2020, 2040), "2040-2060": (2040, 2060)},
    },
    "ssp585": {
        "subdir": "ssp585/tas",
        "periods": {"2020-2040": (2020, 2040), "2040-2060": (2040, 2060)},
    },
}

MIN_YEARS_REQUIRED_FRACTION = 0.6
MIN_YEARS_ABSOLUTE = 5
METRICS = PIPELINE_METRICS_RAW

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# -----------------------------------------------------------------------------
# BASIC HELPERS
# -----------------------------------------------------------------------------
def metric_root(slug: str, level: AdminLevel = "district") -> Path:
    """
    Get the output root for a metric, optionally with level suffix.
    
    For backward compatibility:
    - level="district" -> BASE_OUTPUT_ROOT / slug (no suffix)
    - level="block" -> BASE_OUTPUT_ROOT / slug (same path, but outputs go to block subfolders)
    """
    root = BASE_OUTPUT_ROOT / slug
    root.mkdir(parents=True, exist_ok=True)
    return root


def normalize_lat_lon(ds: xr.Dataset) -> xr.Dataset:
    ren = {}
    for cand in ["latitude", "y"]:
        if cand in ds.dims:
            ren[cand] = "lat"
    for cand in ["longitude", "x"]:
        if cand in ds.dims:
            ren[cand] = "lon"
    if ren:
        ds = ds.rename(ren)
    return ds


def pr_to_mm_per_day(da: xr.DataArray) -> xr.DataArray:
    """Convert precipitation from kg m-2 s-1 to mm/day if needed."""
    units = (getattr(da, "attrs", {}).get("units", "") or "").strip().lower()
    if units in {"kg m-2 s-1", "kg m-2 s^-1", "kg/m^2/s"}:
        return da * 86400.0
    return da


# -----------------------------------------------------------------------------
# BOUNDARY LOADING (Generalized for district/block)
# -----------------------------------------------------------------------------
def load_boundaries(
    path: Path,
    state_filter: Optional[str] = None,
    level: AdminLevel = "district",
) -> gpd.GeoDataFrame:
    """
    Load boundary file and optionally filter to a specific state.
    
    Args:
        path: Path to GeoJSON/Shapefile
        state_filter: Optional state name to filter to
        level: "district" or "block" - determines column mapping
        
    Returns:
        GeoDataFrame with standardized columns
    """
    gdf = gpd.read_file(path)
    
    # Determine state column
    candidate_state_cols = ["STATE_UT", "state_ut", "STATE", "STATE_LGD", "ST_NM", "state_name"]
    state_col = next((c for c in candidate_state_cols if c in gdf.columns), None)
    if not state_col:
        raise ValueError(f"Could not find a state column in {path}.")

    # Normalize state names for filtering
    s = gdf[state_col].astype(str)
    s = s.str.normalize("NFKC").str.strip().str.lower().str.replace(r"\s+", " ", regex=True)
    gdf["_state_norm"] = s

    # Filter to state if specified
    if state_filter:
        filter_keys = {state_filter.lower().strip()}
        # Add common variations
        if "telangana" in filter_keys:
            filter_keys.add("telengana")
            filter_keys.add("telangana state")
        
        gdf = gdf[gdf["_state_norm"].isin(filter_keys)]
        if gdf.empty:
            raise ValueError(f"No rows found for state: {state_filter}")

    # Set CRS if missing
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
    
    # District name
    if "DISTRICT" in out.columns and "district_name" not in out.columns:
        out["district_name"] = out["DISTRICT"].astype(str).str.strip()
    elif "district_name" not in out.columns:
        for cand in ["District", "DIST_NAME", "district"]:
            if cand in out.columns:
                out["district_name"] = out[cand].astype(str).str.strip()
                break
    
    # State name
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


def get_boundary_path(level: AdminLevel) -> Path:
    """Get the boundary file path based on level."""
    return BLOCKS_PATH if level == "block" else DISTRICTS_PATH


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
    
    Args:
        gdf: GeoDataFrame with boundaries
        sample_ds: Sample xarray Dataset to get grid dimensions
        level: "district" or "block"
        
    Returns:
        Dict mapping unit names to xarray DataArray masks
    """
    unit_col = get_unit_name_column(level)
    
    if unit_col not in gdf.columns:
        raise ValueError(f"'{unit_col}' not found in GDF. Available: {list(gdf.columns)}")
    
    lats = sample_ds["lat"].values
    lons = sample_ds["lon"].values
    height, width = lats.size, lons.size

    if np.all(np.diff(lons) > 0):
        xres = lons[1] - lons[0]
        xoff = lons[0] - xres / 2
    else:
        raise ValueError("Longitude not strictly increasing.")
    
    yres = lats[1] - lats[0]
    yoff = lats[0] - yres / 2
    transform = Affine.translation(xoff, yoff) * Affine.scale(xres, yres)

    masks = {}
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None:
            continue
        
        name = str(row[unit_col]).strip()
        
        # For blocks, include district name to ensure uniqueness
        if level == "block" and "district_name" in gdf.columns:
            district = str(row["district_name"]).strip()
            # Use a separator that's safe for filenames
            name = f"{district}__{name}"
        
        mask = features.rasterize(
            [(geom, 1)],
            out_shape=(height, width),
            transform=transform,
            fill=0,
            all_touched=True,
            dtype="uint8"
        )
        mask_da = xr.DataArray(
            mask.astype(bool),
            coords={"lat": sample_ds["lat"], "lon": sample_ds["lon"]},
            dims=("lat", "lon"),
            name="mask"
        )
        masks[name] = mask_da
    
    return masks


# Legacy function for backward compatibility
def load_telangana_districts(path: Path) -> gpd.GeoDataFrame:
    """Legacy function - use load_boundaries() instead."""
    return load_boundaries(path, state_filter="Telangana", level="district")


def build_district_masks(
    telangana_gdf: gpd.GeoDataFrame,
    sample_ds: xr.Dataset,
    district_name_col: str = "DISTRICT"
) -> dict:
    """Legacy function - use build_unit_masks() instead."""
    # Ensure the expected column exists
    if district_name_col not in telangana_gdf.columns and "district_name" in telangana_gdf.columns:
        telangana_gdf = telangana_gdf.copy()
        telangana_gdf[district_name_col] = telangana_gdf["district_name"]
    
    return build_unit_masks(telangana_gdf, sample_ds, level="district")


# -----------------------------------------------------------------------------
# RUN-LENGTH HELPERS
# -----------------------------------------------------------------------------
def _run_length_stats(mask: np.ndarray, min_len: int) -> tuple[int, int]:
    """Return (max_run_length, total_days_in_qualifying_runs)."""
    max_run = 0
    total_days = 0
    current = 0
    for v in mask:
        if v:
            current += 1
        else:
            if current >= min_len:
                total_days += current
                if current > max_run:
                    max_run = current
            current = 0
    if current >= min_len:
        total_days += current
        if current > max_run:
            max_run = current
    return max_run, total_days


def _count_events(mask: np.ndarray, min_len: int) -> int:
    """Count contiguous True-runs of length >= min_len."""
    arr = np.asarray(mask, dtype=bool)
    if arr.size == 0:
        return 0
    events = 0
    run_len = 0
    for v in arr:
        if v:
            run_len += 1
        else:
            if run_len >= min_len:
                events += 1
            run_len = 0
    if run_len >= min_len:
        events += 1
    return int(events)


def _get_district_daily_mean(da: xr.DataArray, mask: xr.DataArray) -> xr.DataArray:
    """Get district-mean daily time series."""
    da_masked = da.where(mask)
    daily_mean = da_masked.mean(dim=("lat", "lon"), skipna=True)
    if "time" in daily_mean.dims:
        daily_mean = daily_mean.dropna(dim="time", how="all")
    return daily_mean


# -----------------------------------------------------------------------------
# TEMPERATURE COMPUTE FUNCTIONS
# -----------------------------------------------------------------------------

def count_days_above_threshold(da: xr.DataArray, mask: xr.DataArray, thresh_k: float) -> int:
    """Count days where district-mean temperature exceeds threshold (Kelvin)."""
    daily_mean = _get_district_daily_mean(da, mask)
    return int((daily_mean > thresh_k).sum().item())


def count_days_ge_threshold(da: xr.DataArray, mask: xr.DataArray, thresh_k: float) -> int:
    """Count days where district-mean temperature >= threshold (Kelvin)."""
    daily_mean = _get_district_daily_mean(da, mask)
    return int((daily_mean >= thresh_k).sum().item())


def count_days_below_threshold(da: xr.DataArray, mask: xr.DataArray, thresh_k: float) -> int:
    """Count days where district-mean temperature is below threshold (Kelvin)."""
    daily_mean = _get_district_daily_mean(da, mask)
    return int((daily_mean < thresh_k).sum().item())


def annual_mean(da: xr.DataArray, mask: xr.DataArray) -> float:
    """Annual mean temperature in °C."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return np.nan
    annual_mean_k = float(daily_mean.mean(dim="time").item())
    return annual_mean_k - 273.15


def seasonal_mean(da: xr.DataArray, mask: xr.DataArray, months: list[int]) -> float:
    """Seasonal mean temperature in °C for specified months."""
    da_masked = da.where(mask)
    if "time" not in da_masked.dims:
        raise ValueError("Expected 'time' dimension.")
    month_idx = da_masked["time"].dt.month
    da_season = da_masked.sel(time=month_idx.isin(months))
    if da_season.sizes.get("time", 0) == 0:
        return np.nan
    daily_mean = da_season.mean(dim=("lat", "lon"), skipna=True)
    daily_mean = daily_mean.dropna(dim="time", how="all")
    if daily_mean.size == 0:
        return np.nan
    return float(daily_mean.mean(dim="time").item()) - 273.15


def annual_max_temperature(da: xr.DataArray, mask: xr.DataArray) -> float:
    """Annual maximum temperature (TXx or TNx) in °C."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return np.nan
    return float(daily_mean.max(dim="time").item()) - 273.15


def annual_min_temperature(da: xr.DataArray, mask: xr.DataArray) -> float:
    """Annual minimum temperature (TXn or TNn) in °C."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return np.nan
    return float(daily_mean.min(dim="time").item()) - 273.15


def longest_consecutive_run_above_threshold(da: xr.DataArray, mask: xr.DataArray, 
                                            thresh_k: float, min_len: int = 1) -> int:
    """Longest consecutive run where daily mean exceeds threshold."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return 0
    cond = daily_mean > float(thresh_k)
    arr = np.asarray(cond.fillna(False).values, dtype=bool)
    max_run, _ = _run_length_stats(arr, min_len=int(min_len))
    return int(max_run)


def consecutive_run_events_above_threshold(da: xr.DataArray, mask: xr.DataArray,
                                           thresh_k: float, min_event_days: int = 6) -> int:
    """Count events (spells >= min_event_days) where daily mean exceeds threshold."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return 0
    cond = daily_mean > thresh_k
    arr = np.asarray(cond.fillna(False).values, dtype=bool)
    return _count_events(arr, min_len=min_event_days)


def percentile_days_above(da: xr.DataArray, mask: xr.DataArray, 
                          percentile: int = 90, baseline_years: tuple = (1985, 2014)) -> float:
    """Percentage of days above the Nth percentile (TX90p, TN90p)."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return np.nan
    thresh = float(daily_mean.quantile(percentile / 100.0).item())
    above = (daily_mean > thresh).sum().item()
    total = daily_mean.size
    return 100.0 * above / total if total > 0 else np.nan


def percentile_days_below(da: xr.DataArray, mask: xr.DataArray,
                          percentile: int = 10, baseline_years: tuple = (1985, 2014)) -> float:
    """Percentage of days below the Nth percentile (TX10p, TN10p)."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return np.nan
    thresh = float(daily_mean.quantile(percentile / 100.0).item())
    below = (daily_mean < thresh).sum().item()
    total = daily_mean.size
    return 100.0 * below / total if total > 0 else np.nan


def warm_spell_duration_index(da: xr.DataArray, mask: xr.DataArray,
                              percentile: int = 90, min_spell_days: int = 6,
                              baseline_years: tuple = (1985, 2014)) -> int:
    """WSDI: days in warm spells (>=6 consecutive days with TX > 90th pctl)."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return 0
    thresh = float(daily_mean.quantile(percentile / 100.0).item())
    hot = daily_mean > thresh
    arr = np.asarray(hot.fillna(False).values, dtype=bool)
    _, total_days = _run_length_stats(arr, min_len=min_spell_days)
    return int(total_days)


def cold_spell_duration_index(da: xr.DataArray, mask: xr.DataArray,
                              percentile: int = 10, min_spell_days: int = 6,
                              baseline_years: tuple = (1985, 2014)) -> int:
    """CSDI: days in cold spells (>=6 consecutive days with TN < 10th pctl)."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return 0
    thresh = float(daily_mean.quantile(percentile / 100.0).item())
    cold = daily_mean < thresh
    arr = np.asarray(cold.fillna(False).values, dtype=bool)
    _, total_days = _run_length_stats(arr, min_len=min_spell_days)
    return int(total_days)


def heatwave_duration_index(da: xr.DataArray, mask: xr.DataArray,
                            baseline_years: tuple = (1985, 2014), delta_c: float = 5.0,
                            abs_thresh_k: float = 40.0 + 273.15, min_spell_days: int = 5) -> int:
    """HWDI: length of longest heatwave spell."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return 0
    year_p90 = float(daily_mean.quantile(0.9).item())
    thresh = max(abs_thresh_k, year_p90)
    hw = daily_mean >= thresh
    arr = np.asarray(hw.fillna(False).values, dtype=bool)
    max_run, _ = _run_length_stats(arr, min_len=min_spell_days)
    return int(max_run)


def heatwave_frequency_percentile(da: xr.DataArray, mask: xr.DataArray,
                                  baseline_years: tuple = (1985, 2014), pct: int = 90,
                                  min_spell_days: int = 5) -> int:
    """HWFI: total days in heatwave spells."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return 0
    thresh = float(daily_mean.quantile(pct / 100.0).item())
    hot = daily_mean > thresh
    arr = np.asarray(hot.fillna(False).values, dtype=bool)
    _, total_days = _run_length_stats(arr, min_len=min_spell_days)
    return int(total_days)


def heatwave_event_count(da: xr.DataArray, mask: xr.DataArray,
                         baseline_years: tuple = (1985, 2014), delta_c: float = 5.0,
                         abs_thresh_k: float = 40.0 + 273.15, min_spell_days: int = 5) -> int:
    """HWDI events: number of heatwave spells."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return 0
    year_p90 = float(daily_mean.quantile(0.9).item())
    thresh = max(abs_thresh_k, year_p90)
    hw = daily_mean >= thresh
    arr = np.asarray(hw.fillna(False).values, dtype=bool)
    return _count_events(arr, min_len=min_spell_days)


def heatwave_event_count_percentile(da: xr.DataArray, mask: xr.DataArray,
                                    baseline_years: tuple = (1985, 2014), pct: int = 90,
                                    min_spell_days: int = 5) -> int:
    """HWFI events: number of hot-spell events."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return 0
    thresh = float(daily_mean.quantile(pct / 100.0).item())
    hot = daily_mean > thresh
    arr = np.asarray(hot.fillna(False).values, dtype=bool)
    return _count_events(arr, min_len=min_spell_days)


def heatwave_magnitude(da: xr.DataArray, mask: xr.DataArray,
                       baseline_years: tuple = (1985, 2014), min_spell_days: int = 3) -> float:
    """HWM: mean temperature across all heatwave days."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return np.nan
    thresh = float(daily_mean.quantile(0.9).item())
    hw_mask = (daily_mean > thresh).values
    hw_days = []
    current_spell = []
    for i, v in enumerate(hw_mask):
        if v:
            current_spell.append(i)
        else:
            if len(current_spell) >= min_spell_days:
                hw_days.extend(current_spell)
            current_spell = []
    if len(current_spell) >= min_spell_days:
        hw_days.extend(current_spell)
    
    if not hw_days:
        return np.nan
    hw_temps = daily_mean.isel(time=hw_days)
    return float(hw_temps.mean().item()) - 273.15


def heatwave_amplitude(da: xr.DataArray, mask: xr.DataArray,
                       baseline_years: tuple = (1985, 2014), min_spell_days: int = 3) -> float:
    """HWA: peak temperature in the hottest heatwave."""
    daily_mean = _get_district_daily_mean(da, mask)
    if daily_mean.size == 0:
        return np.nan
    thresh = float(daily_mean.quantile(0.9).item())
    hw_mask = (daily_mean > thresh).values
    
    spells = []
    current_spell = []
    for i, v in enumerate(hw_mask):
        if v:
            current_spell.append(i)
        else:
            if len(current_spell) >= min_spell_days:
                spell_temps = daily_mean.isel(time=current_spell)
                spells.append((float(spell_temps.mean().item()), float(spell_temps.max().item())))
            current_spell = []
    if len(current_spell) >= min_spell_days:
        spell_temps = daily_mean.isel(time=current_spell)
        spells.append((float(spell_temps.mean().item()), float(spell_temps.max().item())))
    
    if not spells:
        return np.nan
    hottest = max(spells, key=lambda x: x[0])
    return hottest[1] - 273.15


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


# -----------------------------------------------------------------------------
# PRECIPITATION COMPUTE FUNCTIONS (placeholder signatures)
# -----------------------------------------------------------------------------
# Add your precipitation functions here following the same pattern


# -----------------------------------------------------------------------------
# FILE DISCOVERY
# -----------------------------------------------------------------------------
def var_data_dir(data_root: Path, subdir: str, var: str, model: str) -> Path:
    """Construct path to variable data directory."""
    base_parts = Path(subdir).parts
    var_subdir = Path(base_parts[0]) / var
    return data_root / var_subdir / model


def try_open_nc(path: Path) -> bool:
    """Test if a NetCDF file can be opened."""
    try:
        with xr.open_dataset(path) as ds:
            pass
        return True
    except Exception:
        return False


def validated_year_files(data_dir: Path) -> tuple[dict, dict]:
    """Find and validate yearly NetCDF files."""
    pattern = str(data_dir / "*.nc")
    files = glob.glob(pattern)
    
    valid = {}
    bad = {}
    
    for f in files:
        p = Path(f)
        try:
            year = int(p.stem.split("_")[-1])
        except (ValueError, IndexError):
            continue
        
        try:
            sz = p.stat().st_size
        except Exception as e:
            bad[year] = {"path": p, "reason": f"stat_failed: {e}", "magic": None}
            continue
        if sz == 0:
            bad[year] = {"path": p, "reason": "zero_size", "magic": None}
            continue
        if try_open_nc(p):
            valid[year] = p
        else:
            bad[year] = {"path": p, "reason": "open_failed", "magic": None}
    
    return dict(sorted(valid.items())), bad


def discover_models(data_root: Path, scenarios: dict, variables: list = None) -> list:
    """Discover all models across all variables."""
    if variables is None:
        variables = ["tas", "tasmax", "tasmin", "pr"]
    
    models = set()
    for _, scen_conf in scenarios.items():
        base_subdir = scen_conf["subdir"]
        base_parts = Path(base_subdir).parts
        
        for var in variables:
            var_subdir = Path(base_parts[0]) / var
            model_base = data_root / var_subdir
            
            if not model_base.exists():
                continue
            for entry in model_base.iterdir():
                if entry.is_dir():
                    models.add(entry.name)
    
    return sorted(models)


MODELS = discover_models(DATA_ROOT, SCENARIOS)


# -----------------------------------------------------------------------------
# MAIN PROCESSING (Generalized for district/block)
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
    """
    Process ONE metric for ONE (model, scenario).

    Supports both single-var metrics (metric["var"]) and multi-var metrics
    (metric["vars"]) such as DTR/ETR.
    """
    slug = metric["slug"]
    value_col = metric["value_col"]

    req_vars = metric.get("vars") or ([metric.get("var")] if metric.get("var") else [])
    req_vars = [v for v in req_vars if v]
    primary_var = metric.get("var") or (req_vars[0] if req_vars else None)
    if not primary_var:
        logging.error(f"[{slug}] Metric has no var/vars defined")
        return

    compute_name = metric.get("compute")
    compute_fn = globals().get(compute_name)
    if compute_fn is None:
        logging.error(f"[{slug}] Unknown compute function '{compute_name}'.")
        return

    params = metric.get("params", {})
    metric_root_path = metric_root(slug, level)

    # Resolve year files (supports multi-var metrics)
    year_to_paths: dict[int, dict[str, Path]] = {}

    if len(req_vars) <= 1:
        data_dir = var_data_dir(DATA_ROOT, scenario_conf["subdir"], primary_var, model)
        if not data_dir.exists():
            logging.warning(f"[{slug}] Missing data dir: {data_dir}")
            return
        valid_year_files, _bad_files = validated_year_files(data_dir)
        if not valid_year_files:
            logging.warning(f"[{slug}] No valid files in {data_dir}")
            return
        year_to_paths = {y: {primary_var: p} for y, p in valid_year_files.items()}
    else:
        valid_by_var: dict[str, dict[int, Path]] = {}
        for v in req_vars:
            vdir = var_data_dir(DATA_ROOT, scenario_conf["subdir"], v, model)
            if not vdir.exists():
                logging.warning(f"[{slug}] Missing data dir for var '{v}': {vdir}")
                return
            valid_year_files, _bad_files = validated_year_files(vdir)
            if not valid_year_files:
                logging.warning(f"[{slug}] No valid files in {vdir}")
                return
            valid_by_var[v] = valid_year_files

        common_years = set.intersection(*(set(d.keys()) for d in valid_by_var.values()))
        if not common_years:
            logging.warning(f"[{slug}] No overlapping years across vars: {req_vars}")
            return

        for y in sorted(common_years):
            year_to_paths[y] = {v: valid_by_var[v][y] for v in req_vars}

    if not year_to_paths:
        return

    # Build masks using a sample file from the primary variable
    sample_year = next(iter(year_to_paths.keys()))
    sample_path = year_to_paths[sample_year].get(primary_var) or next(iter(year_to_paths[sample_year].values()))

    ds_sample = xr.open_dataset(sample_path)
    ds_sample = normalize_lat_lon(ds_sample)
    if primary_var not in ds_sample:
        ds_sample.close()
        logging.warning(f"[{slug}] '{primary_var}' not found in {sample_path}")
        return

    masks = build_unit_masks(gdf, ds_sample, level=level)
    ds_sample.close()

    if not masks:
        logging.warning(f"[{slug}] No valid masks built for {level} level")
        return

    # Yearly computation
    rows: list[dict] = []
    for year, paths_by_var in year_to_paths.items():
        logging.info(f"[{slug}] Processing {year} ({level} level)")
        ds_by_var: dict[str, xr.Dataset] = {}
        da_by_var: dict[str, xr.DataArray] = {}

        try:
            for v, nc_path in paths_by_var.items():
                ds = xr.open_dataset(nc_path)
                ds = normalize_lat_lon(ds)
                if v not in ds:
                    raise KeyError(f"Variable '{v}' not found in {nc_path}")
                ds_by_var[v] = ds
                da_by_var[v] = ds[v]

            for unit_name, mask_da in masks.items():
                try:
                    if len(req_vars) <= 1:
                        v_out = compute_fn(da_by_var[primary_var], mask_da, **params)
                    else:
                        # Currently assumes two input variables (e.g., DTR/ETR)
                        v_out = compute_fn(da_by_var[req_vars[0]], da_by_var[req_vars[1]], mask_da, **params)
                except Exception as e:
                    logging.error(f"[{slug}] Error for {unit_name}, {year}: {e}")
                    v_out = None

                row = {
                    "model": model,
                    "scenario": scenario,
                    "year": year,
                    "value": v_out,
                    value_col: v_out,
                    "source_file": str(paths_by_var.get(primary_var) or next(iter(paths_by_var.values()))),
                }

                # Add appropriate ID columns based on level
                if level == "block":
                    # unit_name format: "district__block"
                    if "__" in unit_name:
                        district, block = unit_name.split("__", 1)
                        row["district"] = district
                        row["block"] = block
                    else:
                        row["district"] = "Unknown"
                        row["block"] = unit_name
                else:
                    row["district"] = unit_name

                rows.append(row)

        except Exception as e:
            logging.error(f"[{slug}] Failed to process {model}/{scenario}/{year}: {e}")
        finally:
            for ds in ds_by_var.values():
                try:
                    ds.close()
                except Exception:
                    pass

    if not rows:
        return

    df_yearly = pd.DataFrame(rows)

    # Period aggregation
    period_frames = []
    group_cols = ["district", "block", "model", "scenario"] if level == "block" else ["district", "model", "scenario"]

    for period_name, (y0, y1) in scenario_conf["periods"].items():
        available_years = [y for y in year_to_paths.keys() if y0 <= y <= y1]
        n_req = y1 - y0 + 1
        n_avail = len(available_years)
        frac = n_avail / n_req if n_req > 0 else 0.0

        if n_avail >= MIN_YEARS_ABSOLUTE and frac >= MIN_YEARS_REQUIRED_FRACTION:
            df_sub = df_yearly[df_yearly["year"].isin(available_years)]
            grp = df_sub.groupby([c for c in group_cols if c in df_sub.columns]).agg({"value": "mean"}).reset_index()
            grp["period"] = period_name
            grp["years_used_count"] = n_avail
            grp["years_requested"] = n_req
            grp[value_col] = grp["value"]
            period_frames.append(grp)

    df_periods = pd.concat(period_frames, ignore_index=True) if period_frames else pd.DataFrame()

    # Write outputs
    if level == "block":
        for (district, block), grp_df in df_yearly.groupby(["district", "block"]):
            district_safe = district.replace(" ", "_").replace("/", "_")
            block_safe = block.replace(" ", "_").replace("/", "_")

            out_dir = metric_root_path / state_name / district_safe / block_safe / model / scenario
            out_dir.mkdir(parents=True, exist_ok=True)

            grp_df.to_csv(out_dir / f"{block_safe}_yearly.csv", index=False)

            if not df_periods.empty:
                period_grp = df_periods[
                    (df_periods["district"] == district) &
                    (df_periods["block"] == block)
                ]
                if not period_grp.empty:
                    period_grp.to_csv(out_dir / f"{block_safe}_periods.csv", index=False)
    else:
        for dist_name in df_yearly["district"].unique():
            out_dir = metric_root_path / state_name / dist_name.replace(" ", "_") / model / scenario
            out_dir.mkdir(parents=True, exist_ok=True)

            df_yearly[df_yearly["district"] == dist_name].to_csv(
                out_dir / f"{dist_name.replace(' ', '_')}_yearly.csv", index=False
            )

            if not df_periods.empty:
                df_periods[df_periods["district"] == dist_name].to_csv(
                    out_dir / f"{dist_name.replace(' ', '_')}_periods.csv", index=False
                )


def compute_ensembles_generic(
    output_root: Path,
    state: str = "Telangana",
    level: AdminLevel = "district",
):
    """
    Compute ensemble statistics across models.
    
    Args:
        output_root: Root path for metric outputs
        state: State name
        level: "district" or "block"
    """
    root = Path(output_root)
    state_root = root / state
    ensembles_root = state_root / "ensembles"
    ensembles_root.mkdir(parents=True, exist_ok=True)

    if level == "block":
        # For blocks: state_root / district / block / model / scenario
        _compute_block_ensembles(state_root, ensembles_root)
    else:
        # Original district logic
        _compute_district_ensembles(state_root, ensembles_root)


def _compute_district_ensembles(state_root: Path, ensembles_root: Path):
    """Compute ensembles for district-level data."""
    district_dirs = [
        p for p in state_root.iterdir() 
        if p.is_dir() and p.name not in {"validation_reports", "ensembles"}
    ]
    
    for ddir in district_dirs:
        district = ddir.name
        model_dirs = [p for p in ddir.iterdir() if p.is_dir()]
        scenarios = sorted({s.name for m in model_dirs for s in m.iterdir() if s.is_dir()})
        
        for scenario in scenarios:
            model_yearly = []
            for m in model_dirs:
                yearly_csv = m / scenario / f"{district}_yearly.csv"
                if yearly_csv.exists():
                    try:
                        dfy = pd.read_csv(yearly_csv)
                        if "value" not in dfy.columns:
                            cols = [c for c in dfy.columns if c not in 
                                   {"district", "model", "scenario", "year", "source_file"}]
                            if cols:
                                dfy["value"] = dfy[cols[0]]
                        dfy["model"] = m.name
                        model_yearly.append(dfy)
                    except Exception:
                        pass

            if model_yearly:
                _write_ensemble_stats(model_yearly, ensembles_root / district / scenario, district)


def _compute_block_ensembles(state_root: Path, ensembles_root: Path):
    """Compute ensembles for block-level data."""
    # Structure: state_root / district / block / model / scenario
    district_dirs = [
        p for p in state_root.iterdir() 
        if p.is_dir() and p.name not in {"validation_reports", "ensembles"}
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
                    yearly_csv = m / scenario / f"{block}_yearly.csv"
                    if yearly_csv.exists():
                        try:
                            dfy = pd.read_csv(yearly_csv)
                            if "value" not in dfy.columns:
                                cols = [c for c in dfy.columns if c not in 
                                       {"district", "block", "model", "scenario", "year", "source_file"}]
                                if cols:
                                    dfy["value"] = dfy[cols[0]]
                            dfy["model"] = m.name
                            model_yearly.append(dfy)
                        except Exception:
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
        yearly_summary = pd.DataFrame({
            "year": pivot.index,
            "n_models": pivot.count(axis=1),
            "ensemble_mean": pivot.mean(axis=1),
            "ensemble_std": pivot.std(axis=1, ddof=0),
            "ensemble_median": pivot.median(axis=1),
            "ensemble_p05": pivot.quantile(0.05, axis=1),
            "ensemble_p95": pivot.quantile(0.95, axis=1),
        }).reset_index(drop=True)
        
        out_dir.mkdir(parents=True, exist_ok=True)
        yearly_summary.to_csv(out_dir / f"{unit_name}_yearly_ensemble.csv", index=False)


# -----------------------------------------------------------------------------
# CLI & MAIN
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute climate indices at district or block level."
    )
    parser.add_argument(
        "--level", "-l",
        choices=["district", "block"],
        default="district",
        help="Administrative level for spatial aggregation (default: district)"
    )
    parser.add_argument(
        "--state", "-s",
        default="Telangana",
        help="State to process (default: Telangana)"
    )
    parser.add_argument(
        "--metric", "-m",
        default=None,
        help="Process only this metric slug (default: all metrics)"
    )
    return parser.parse_args()


def main():
    """Main pipeline driver."""
    args = parse_args()
    level: AdminLevel = args.level
    state_name = args.state
    
    logging.info(f"Starting pipeline: level={level}, state={state_name}")
    
    # Initialize metric directories
    for m in METRICS:
        metric_root(m["slug"], level)
    
    # Load boundaries
    boundary_path = get_boundary_path(level)
    logging.info(f"Loading boundaries from: {boundary_path}")
    
    try:
        gdf = load_boundaries(boundary_path, state_filter=state_name, level=level)
        logging.info(f"Loaded {len(gdf)} {level} boundaries for {state_name}")
    except Exception as e:
        logging.error(f"Failed to load boundaries: {e}")
        return
    
    # Filter metrics if specified
    metrics_to_process = METRICS
    if args.metric:
        metrics_to_process = [m for m in METRICS if m["slug"] == args.metric]
        if not metrics_to_process:
            logging.error(f"Metric '{args.metric}' not found in registry")
            return
    
    # Process each model/scenario/metric
    for model in MODELS:
        for scenario, sconf in SCENARIOS.items():
            for metric in metrics_to_process:
                try:
                    process_metric_for_model_scenario(
                        metric, model, scenario, sconf, gdf,
                        level=level, state_name=state_name
                    )
                except Exception as e:
                    logging.error(f"Failed processing {metric['slug']}: {e}")
                    traceback.print_exc()

    # Build ensembles
    for metric in metrics_to_process:
        try:
            compute_ensembles_generic(
                metric_root(metric["slug"], level),
                state=state_name,
                level=level
            )
        except Exception as e:
            logging.error(f"Ensembles failed for {metric['slug']}: {e}")


if __name__ == "__main__":
    main()

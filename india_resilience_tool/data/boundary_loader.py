"""
Unified boundary loading for IRT dashboard.

This module provides a single interface to load either district (ADM2) or 
block (ADM3) boundaries, abstracting the level-specific details.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Literal, Optional, Tuple, Union

import geopandas as gpd
import pandas as pd

# Import level-specific loaders
from india_resilience_tool.data.adm2_loader import (
    load_local_adm2,
    ensure_key_column as ensure_key_column_adm2,
    featurecollections_by_state as featurecollections_by_state_adm2,
    build_adm1_from_adm2,
)
from india_resilience_tool.data.adm3_loader import (
    load_local_adm3,
    ensure_key_column as ensure_key_column_adm3,
    featurecollections_by_state as featurecollections_by_state_adm3,
    build_adm1_from_adm3,
    get_blocks_for_district,
    get_districts_with_blocks,
)

PathLike = Union[str, Path]
BBox = Tuple[float, float, float, float]
AdminLevel = Literal["district", "block"]


def load_boundaries(
    path: PathLike,
    *,
    level: AdminLevel = "district",
    tolerance: float = 0.001,
    bbox: Optional[BBox] = None,
    min_area: float = 1e-6,
) -> gpd.GeoDataFrame:
    """
    Load boundary file for specified administrative level.
    
    Args:
        path: Path to boundary file (GeoJSON/Shapefile)
        level: "district" for ADM2, "block" for ADM3
        tolerance: Simplification tolerance in degrees
        bbox: Cropping bbox (min_lon, min_lat, max_lon, max_lat)
        min_area: Minimum polygon area threshold
        
    Returns:
        Prepared GeoDataFrame with standardized columns
    """
    if level == "block":
        return load_local_adm3(path, tolerance=tolerance, bbox=bbox, min_area=min_area)
    return load_local_adm2(path, tolerance=tolerance, bbox=bbox, min_area=min_area)


def get_unit_name_column(level: AdminLevel) -> str:
    """Get the primary unit name column for a level."""
    return "block_name" if level == "block" else "district_name"


def get_unit_display_name(level: AdminLevel) -> str:
    """Get the display name for units at this level."""
    return "Block" if level == "block" else "District"


def get_unit_display_name_plural(level: AdminLevel) -> str:
    """Get the plural display name for units at this level."""
    return "Blocks" if level == "block" else "Districts"


def ensure_key_column(
    gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
    alias_fn: Callable[[str], str],
    key_col: str = "__key",
) -> gpd.GeoDataFrame:
    """
    Ensure a deterministic join key column exists.
    
    For districts: key = normalized(district_name)
    For blocks: key = normalized(district_name)|normalized(block_name)
    """
    if level == "block":
        return ensure_key_column_adm3(
            gdf,
            block_col="block_name",
            district_col="district_name",
            alias_fn=alias_fn,
            key_col=key_col,
        )
    return ensure_key_column_adm2(
        gdf,
        district_col="district_name",
        alias_fn=alias_fn,
        key_col=key_col,
    )


def featurecollections_by_state(
    gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
    normalize_state_fn: Callable[[str], str],
    keep_cols: list[str],
) -> dict[str, dict]:
    """
    Build a FeatureCollection per normalized state.
    
    Delegates to level-specific implementation.
    """
    state_col = "state_name"
    
    if level == "block":
        return featurecollections_by_state_adm3(
            gdf,
            state_col=state_col,
            normalize_state_fn=normalize_state_fn,
            keep_cols=keep_cols,
        )
    return featurecollections_by_state_adm2(
        gdf,
        state_col=state_col,
        normalize_state_fn=normalize_state_fn,
        keep_cols=keep_cols,
    )


def build_adm1_from_boundaries(
    gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
) -> gpd.GeoDataFrame:
    """
    Derive ADM1 (state) boundaries by dissolving.
    """
    if level == "block":
        return build_adm1_from_adm3(gdf, state_col="state_name")
    return build_adm1_from_adm2(gdf, state_col="state_name")


def get_states(gdf: gpd.GeoDataFrame) -> list[str]:
    """Get sorted list of unique states in the GeoDataFrame."""
    if "state_name" not in gdf.columns:
        return []
    return sorted(gdf["state_name"].dropna().unique().tolist())


def get_districts(
    gdf: gpd.GeoDataFrame,
    state: Optional[str] = None,
) -> list[str]:
    """
    Get sorted list of districts.
    
    Args:
        gdf: GeoDataFrame with district_name column
        state: Optional state filter
        
    Returns:
        Sorted list of district names
    """
    if "district_name" not in gdf.columns:
        return []
    
    filtered = gdf
    if state and "state_name" in gdf.columns:
        filtered = gdf[gdf["state_name"] == state]
    
    return sorted(filtered["district_name"].dropna().unique().tolist())


def get_units(
    gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
    state: Optional[str] = None,
    district: Optional[str] = None,
) -> list[str]:
    """
    Get sorted list of units (districts or blocks) with optional filters.
    
    Args:
        gdf: GeoDataFrame with appropriate columns
        level: "district" or "block"
        state: Optional state filter
        district: Optional district filter (only for blocks)
        
    Returns:
        Sorted list of unit names
    """
    unit_col = get_unit_name_column(level)
    
    if unit_col not in gdf.columns:
        return []
    
    filtered = gdf
    
    if state and "state_name" in filtered.columns:
        filtered = filtered[filtered["state_name"] == state]
    
    if level == "block" and district and "district_name" in filtered.columns:
        filtered = filtered[filtered["district_name"] == district]
    
    return sorted(filtered[unit_col].dropna().unique().tolist())


def filter_by_state(
    gdf: gpd.GeoDataFrame,
    state: str,
) -> gpd.GeoDataFrame:
    """Filter GeoDataFrame to a specific state."""
    if "state_name" not in gdf.columns:
        return gdf
    return gdf[gdf["state_name"] == state].copy()


def filter_by_district(
    gdf: gpd.GeoDataFrame,
    district: str,
) -> gpd.GeoDataFrame:
    """Filter GeoDataFrame to a specific district."""
    if "district_name" not in gdf.columns:
        return gdf
    return gdf[gdf["district_name"] == district].copy()


def get_unit_count(
    gdf: gpd.GeoDataFrame,
    level: AdminLevel,
    state: Optional[str] = None,
) -> int:
    """Get count of units at this level, optionally filtered by state."""
    unit_col = get_unit_name_column(level)
    
    if unit_col not in gdf.columns:
        return 0
    
    filtered = gdf
    if state and "state_name" in filtered.columns:
        filtered = filtered[filtered["state_name"] == state]
    
    return filtered[unit_col].nunique()


def get_hierarchy_info(
    gdf: gpd.GeoDataFrame,
    level: AdminLevel,
) -> dict:
    """
    Get hierarchy information for the loaded boundaries.
    
    Returns:
        Dict with counts and structure info
    """
    info = {
        "level": level,
        "total_units": get_unit_count(gdf, level),
        "states": get_states(gdf),
        "n_states": len(get_states(gdf)),
    }
    
    if level == "block":
        info["n_districts"] = gdf["district_name"].nunique() if "district_name" in gdf.columns else 0
        
        # Blocks per district stats
        if "district_name" in gdf.columns and "block_name" in gdf.columns:
            blocks_per_dist = gdf.groupby("district_name")["block_name"].nunique()
            info["blocks_per_district"] = {
                "min": int(blocks_per_dist.min()),
                "max": int(blocks_per_dist.max()),
                "mean": float(blocks_per_dist.mean()),
            }
    
    return info

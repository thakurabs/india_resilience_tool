"""
ADM3 (Block/Subdistrict) loading and preparation utilities for IRT.

This module mirrors adm2_loader.py but handles block-level boundaries.
It is intentionally Streamlit-free (no st.cache_data here).
Caching should be applied in the Streamlit layer.

Column mapping from source shapefile:
    - Sub_dist   -> block_name (subdistrict/block name)
    - District   -> district_name (parent district)
    - STATE_UT   -> state_name (parent state)
    - Subdis_Typ -> block_type (Sub_Division, Tehsil, Mandal, etc.)
    - Subdis_LGD -> block_lgd_code (LGD code)

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Tuple, Union

import geopandas as gpd
import pandas as pd
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform


PathLike = Union[str, Path]
BBox = Tuple[float, float, float, float]  # (min_lon, min_lat, max_lon, max_lat)


def drop_z(geom: BaseGeometry) -> BaseGeometry:
    """
    Drop Z dimension from geometries if present.
    """
    try:
        return transform(lambda x, y, z=None: (x, y), geom)
    except Exception:
        return geom


def ensure_epsg4326(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Ensure GeoDataFrame is in EPSG:4326.
    """
    if gdf.crs is None:
        return gdf.set_crs("EPSG:4326")
    return gdf.to_crs("EPSG:4326")


def ensure_adm3_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Ensure canonical columns for block-level data:
      - block_name (from Sub_dist)
      - district_name (from District)
      - state_name (from STATE_UT)
      - block_type (from Subdis_Typ, optional)
      - block_lgd_code (from Subdis_LGD, optional)
    """
    out = gdf.copy()

    # Block name: Sub_dist -> block_name
    if "Sub_dist" in out.columns and "block_name" not in out.columns:
        out["block_name"] = out["Sub_dist"].astype(str).str.strip()
    elif "block_name" not in out.columns:
        # Fallback: try other common names
        for cand in ["SUBDISTRICT", "BLOCK", "TEHSIL", "MANDAL", "TALUK"]:
            if cand in out.columns:
                out["block_name"] = out[cand].astype(str).str.strip()
                break
        if "block_name" not in out.columns:
            txt_cols = [c for c in out.columns if out[c].dtype == object and c != "geometry"]
            out["block_name"] = out[txt_cols[0]].astype(str).str.strip() if txt_cols else out.index.astype(str)

    # District name: District -> district_name
    if "District" in out.columns and "district_name" not in out.columns:
        out["district_name"] = out["District"].astype(str).str.strip()
    elif "DISTRICT" in out.columns and "district_name" not in out.columns:
        out["district_name"] = out["DISTRICT"].astype(str).str.strip()
    if "district_name" not in out.columns:
        out["district_name"] = "Unknown"

    # State name: STATE_UT -> state_name
    if "STATE_UT" in out.columns and "state_name" not in out.columns:
        out["state_name"] = out["STATE_UT"].astype(str).str.strip()
    elif "STATE_LGD" in out.columns and "state_name" not in out.columns:
        out["state_name"] = out["STATE_LGD"].astype(str)
    if "state_name" not in out.columns:
        out["state_name"] = "Unknown"

    # Block type (optional): Subdis_Typ -> block_type
    if "Subdis_Typ" in out.columns and "block_type" not in out.columns:
        out["block_type"] = out["Subdis_Typ"].astype(str).str.strip()

    # LGD code (optional): Subdis_LGD -> block_lgd_code
    if "Subdis_LGD" in out.columns and "block_lgd_code" not in out.columns:
        out["block_lgd_code"] = out["Subdis_LGD"].astype(str).str.strip()

    return out


def crop_to_bbox(gdf: gpd.GeoDataFrame, bbox: Optional[BBox]) -> gpd.GeoDataFrame:
    """
    Crop to bbox in EPSG:4326. Falls back to centroid crop if .cx fails.
    """
    if bbox is None:
        return gdf

    min_lon, min_lat, max_lon, max_lat = bbox
    try:
        return gdf.cx[min_lon:max_lon, min_lat:max_lat]
    except Exception:
        pts = gdf.geometry.centroid
        mask = pts.x.between(min_lon, max_lon) & pts.y.between(min_lat, max_lat)
        return gdf[mask]


def simplify_and_filter(
    gdf: gpd.GeoDataFrame,
    *,
    tolerance: float,
    min_area: float,
) -> gpd.GeoDataFrame:
    """
    Simplify geometries and drop tiny polygons to speed up map rendering.
    """
    out = gdf.copy()
    out["geometry"] = out["geometry"].apply(lambda geom: drop_z(geom))
    out = ensure_epsg4326(out)
    out = ensure_adm3_columns(out)

    out["geometry"] = out["geometry"].simplify(tolerance, preserve_topology=True)
    out = out[out.geometry.area > float(min_area)].reset_index(drop=True)
    return out


def load_local_adm3(
    path: PathLike,
    *,
    tolerance: float,
    bbox: Optional[BBox],
    min_area: float,
) -> gpd.GeoDataFrame:
    """
    Load ADM3 (block/subdistrict) from file and apply standard preparation steps.

    Args:
        path: Path to ADM3 file (GeoJSON/Shapefile/etc.)
        tolerance: Simplification tolerance in degrees
        bbox: Cropping bbox (min_lon, min_lat, max_lon, max_lat)
        min_area: Minimum polygon area threshold (degrees^2)

    Returns:
        Prepared ADM3 GeoDataFrame in EPSG:4326 with block_name/district_name/state_name.
    """
    gdf = gpd.read_file(str(path))
    gdf = simplify_and_filter(gdf, tolerance=tolerance, min_area=min_area)
    gdf = crop_to_bbox(gdf, bbox)
    return gdf.reset_index(drop=True)


def ensure_key_column(
    gdf: gpd.GeoDataFrame,
    *,
    block_col: str = "block_name",
    district_col: str = "district_name",
    alias_fn: Callable[[str], str],
    key_col: str = "__key",
) -> gpd.GeoDataFrame:
    """
    Ensure a deterministic join key column exists for blocks.
    
    Key format: "{normalized_district}|{normalized_block}"
    This ensures uniqueness since block names may repeat across districts.
    """
    out = gdf.copy()
    if key_col not in out.columns:
        dist_norm = out[district_col].astype(str).map(alias_fn)
        block_norm = out[block_col].astype(str).map(alias_fn)
        out[key_col] = dist_norm + "|" + block_norm
    return out


def filter_by_state(
    gdf: gpd.GeoDataFrame,
    state: str,
    *,
    state_col: str = "state_name",
    normalize_fn: Optional[Callable[[str], str]] = None,
) -> gpd.GeoDataFrame:
    """
    Filter GeoDataFrame to a specific state.
    
    Args:
        gdf: Input GeoDataFrame with state_col
        state: State name to filter to
        state_col: Column containing state names
        normalize_fn: Optional normalization function for comparison
        
    Returns:
        Filtered GeoDataFrame
    """
    if state_col not in gdf.columns:
        return gdf
    
    if normalize_fn is None:
        normalize_fn = lambda x: str(x).strip().upper()
    
    state_norm = normalize_fn(state)
    mask = gdf[state_col].astype(str).map(normalize_fn) == state_norm
    return gdf[mask].copy()


def filter_by_district(
    gdf: gpd.GeoDataFrame,
    district: str,
    *,
    district_col: str = "district_name",
    normalize_fn: Optional[Callable[[str], str]] = None,
) -> gpd.GeoDataFrame:
    """
    Filter GeoDataFrame to a specific district.
    
    Args:
        gdf: Input GeoDataFrame with district_col
        district: District name to filter to
        district_col: Column containing district names
        normalize_fn: Optional normalization function for comparison
        
    Returns:
        Filtered GeoDataFrame
    """
    if district_col not in gdf.columns:
        return gdf
    
    if normalize_fn is None:
        normalize_fn = lambda x: str(x).strip().upper()
    
    district_norm = normalize_fn(district)
    mask = gdf[district_col].astype(str).map(normalize_fn) == district_norm
    return gdf[mask].copy()


def get_blocks_for_district(
    gdf: gpd.GeoDataFrame,
    state: str,
    district: str,
    *,
    state_col: str = "state_name",
    district_col: str = "district_name",
    block_col: str = "block_name",
    normalize_fn: Optional[Callable[[str], str]] = None,
) -> list[str]:
    """
    Get list of block names for a specific district.
    
    Returns:
        Sorted list of block names
    """
    filtered = filter_by_state(gdf, state, state_col=state_col, normalize_fn=normalize_fn)
    filtered = filter_by_district(filtered, district, district_col=district_col, normalize_fn=normalize_fn)
    
    if filtered.empty or block_col not in filtered.columns:
        return []
    
    return sorted(filtered[block_col].dropna().unique().tolist())


def featurecollections_by_state(
    gdf,
    *,
    state_col: str = "state_name",
    normalize_state_fn: Callable[[str], str],
    keep_cols: list[str],
) -> dict[str, dict]:
    """
    Build a FeatureCollection per normalized state.

    IMPORTANT: include non-geometry keep_cols as feature.properties.
    Folium GeoJsonTooltip asserts if requested tooltip fields are missing from properties.

    Args:
        gdf: GeoDataFrame with at least state_col, geometry, and keep_cols.
        state_col: column containing state names.
        normalize_state_fn: function to normalize state names for dictionary keys.
        keep_cols: columns to preserve; geometry will become feature.geometry,
                   all other keep_cols become feature.properties.

    Returns:
        Dict: normalized_state -> GeoJSON FeatureCollection dict
    """
    # Defensive: ensure we always have geometry
    if "geometry" not in keep_cols:
        keep_cols = [*keep_cols, "geometry"]

    props_cols = [c for c in keep_cols if c != "geometry" and c in gdf.columns]

    by_state: dict[str, dict] = {}

    # Group by raw state names (as present in data)
    for raw_state, g in gdf.groupby(state_col, dropna=False):
        norm_state = normalize_state_fn(str(raw_state)) if raw_state is not None else "unknown"

        features: list[dict] = []
        for _, row in g.iterrows():
            geom = row["geometry"]
            if geom is None:
                continue

            # Build properties dict from requested cols (excluding geometry)
            props: dict = {}
            for c in props_cols:
                v = row.get(c)
                # make JSON-serializable / stable
                if pd.isna(v):
                    v = None
                elif hasattr(v, "item"):  # numpy scalar
                    v = v.item()
                props[c] = v

            features.append(
                {
                    "type": "Feature",
                    "properties": props,
                    "geometry": geom.__geo_interface__,
                }
            )

        by_state[norm_state] = {
            "type": "FeatureCollection",
            "features": features,
        }

    return by_state

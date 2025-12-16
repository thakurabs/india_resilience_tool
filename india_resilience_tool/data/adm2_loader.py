"""
ADM2 loading and preparation utilities for IRT.

This module is intentionally Streamlit-free (no st.cache_data here).
Caching should be applied in the Streamlit layer.

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


def ensure_adm2_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Ensure canonical columns:
      - district_name
      - state_name
    """
    out = gdf.copy()

    if "DISTRICT" in out.columns and "district_name" not in out.columns:
        out["district_name"] = out["DISTRICT"].astype(str).str.strip()
    if "district_name" not in out.columns:
        txt_cols = [c for c in out.columns if out[c].dtype == object and c != "geometry"]
        out["district_name"] = out[txt_cols[0]].astype(str).str.strip() if txt_cols else out.index.astype(str)

    if "STATE_UT" in out.columns and "state_name" not in out.columns:
        out["state_name"] = out["STATE_UT"].astype(str).str.strip()
    elif "STATE_LGD" in out.columns and "state_name" not in out.columns:
        out["state_name"] = out["STATE_LGD"].astype(str)
    if "state_name" not in out.columns:
        out["state_name"] = "Unknown"

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
    out = ensure_adm2_columns(out)

    out["geometry"] = out["geometry"].simplify(tolerance, preserve_topology=True)
    out = out[out.geometry.area > float(min_area)].reset_index(drop=True)
    return out


def load_local_adm2(
    path: PathLike,
    *,
    tolerance: float,
    bbox: Optional[BBox],
    min_area: float,
) -> gpd.GeoDataFrame:
    """
    Load ADM2 from file and apply standard preparation steps.

    Args:
        path: Path to ADM2 file (GeoJSON/Shapefile/etc.)
        tolerance: Simplification tolerance in degrees
        bbox: Cropping bbox (min_lon, min_lat, max_lon, max_lat)
        min_area: Minimum polygon area threshold (degrees^2)

    Returns:
        Prepared ADM2 GeoDataFrame in EPSG:4326 with district_name/state_name.
    """
    gdf = gpd.read_file(str(path))
    gdf = simplify_and_filter(gdf, tolerance=tolerance, min_area=min_area)
    gdf = crop_to_bbox(gdf, bbox)
    return gdf.reset_index(drop=True)


def ensure_key_column(
    gdf: gpd.GeoDataFrame,
    *,
    district_col: str,
    alias_fn: Callable[[str], str],
    key_col: str = "__key",
) -> gpd.GeoDataFrame:
    """
    Ensure a deterministic join key column exists.
    """
    out = gdf.copy()
    if key_col not in out.columns:
        out[key_col] = out[district_col].astype(str).map(alias_fn)
    return out


def featurecollections_by_state(
    gdf: gpd.GeoDataFrame,
    *,
    state_col: str,
    normalize_state_fn: Callable[[str], str],
    keep_cols: Optional[list[str]] = None,
) -> dict[str, dict]:
    """
    Build FeatureCollection per state (plus "all") using only identifiers + geometry.

    Returns:
        dict[state_key] -> FeatureCollection
        dict["all"] -> FeatureCollection
    """
    base = gdf.copy()
    if keep_cols is None:
        keep_cols = ["district_name", "state_name", "__key", "geometry"]

    keep_cols = [c for c in keep_cols if c in base.columns]
    base = base[keep_cols].copy()

    fc_all = json.loads(base.to_json())
    by_state: dict[str, dict] = {}

    for feat in fc_all.get("features", []):
        props = feat.get("properties") or {}
        state_name = props.get(state_col, "Unknown")
        state_key = normalize_state_fn(str(state_name)) or "unknown"
        by_state.setdefault(state_key, {"type": "FeatureCollection", "features": []})
        by_state[state_key]["features"].append(feat)

    by_state["all"] = fc_all
    return by_state


def build_adm1_from_adm2(adm2_gdf: gpd.GeoDataFrame, *, state_col: str = "state_name") -> gpd.GeoDataFrame:
    """
    Derive an ADM1 (state) GeoDataFrame by dissolving ADM2 boundaries.
    """
    adm2 = adm2_gdf.copy()
    adm1 = adm2.dissolve(by=state_col, as_index=False)
    if state_col not in adm1.columns and "index" in adm1.columns:
        adm1 = adm1.rename(columns={"index": state_col})
    if "shapeName" not in adm1.columns:
        adm1["shapeName"] = adm1[state_col]
    return adm1.reset_index(drop=True)


def enrich_adm2_with_state_names(
    adm2_gdf: gpd.GeoDataFrame,
    adm1_gdf: gpd.GeoDataFrame,
    *,
    state_col: str = "state_name",
    adm1_name_col: str = "shapeName",
) -> gpd.GeoDataFrame:
    """
    Best-effort enrichment of ADM2 state_name via a single spatial join.
    """
    adm2 = adm2_gdf.copy()
    adm1 = adm1_gdf.copy()

    if state_col not in adm2.columns:
        adm2[state_col] = "Unknown"

    pts = adm2.copy()
    pts["geometry"] = pts.geometry.representative_point()

    try:
        joined = gpd.sjoin(
            pts[["geometry"]],
            adm1[["geometry", adm1_name_col]],
            how="left",
            predicate="within",
        )
        if adm1_name_col in joined.columns:
            mapping = joined[adm1_name_col].to_dict()
            for idx, val in mapping.items():
                if pd.notna(val):
                    adm2.at[idx, state_col] = str(val).strip()
    except Exception:
        pass

    missing = adm2[state_col].isna() | (adm2[state_col].astype(str).str.strip() == "")
    adm2.loc[missing, state_col] = "Unknown"
    return adm2

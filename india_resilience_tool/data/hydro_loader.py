"""
Hydro boundary loading and preparation utilities for IRT.

This module standardizes canonical basin and sub-basin GeoJSON inputs for
runtime and pipeline use. It is intentionally Streamlit-free.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Literal, Optional, Tuple, Union

import geopandas as gpd
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform


PathLike = Union[str, Path]
HydroLevel = Literal["basin", "sub_basin"]
BBox = Tuple[float, float, float, float]  # (min_lon, min_lat, max_lon, max_lat)

_BASIN_REQUIRED = ["basin_id", "basin_name", "hydro_level", "geometry"]
_SUBBASIN_REQUIRED = [
    "basin_id",
    "basin_name",
    "subbasin_id",
    "subbasin_code",
    "subbasin_name",
    "hydro_level",
    "geometry",
]


def drop_z(geom: BaseGeometry) -> BaseGeometry:
    """Drop Z dimension from geometries if present."""
    try:
        return transform(lambda x, y, z=None: (x, y), geom)
    except Exception:
        return geom


def ensure_epsg4326(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ensure the GeoDataFrame is in EPSG:4326."""
    if gdf.crs is None:
        return gdf.set_crs("EPSG:4326")
    return gdf.to_crs("EPSG:4326")


def crop_to_bbox(gdf: gpd.GeoDataFrame, bbox: Optional[BBox]) -> gpd.GeoDataFrame:
    """Crop to bbox in EPSG:4326."""
    if bbox is None:
        return gdf

    min_lon, min_lat, max_lon, max_lat = bbox
    try:
        return gdf.cx[min_lon:max_lon, min_lat:max_lat]
    except Exception:
        pts = gdf.geometry.centroid
        mask = pts.x.between(min_lon, max_lon) & pts.y.between(min_lat, max_lat)
        return gdf[mask]


def _assert_required_columns(gdf: gpd.GeoDataFrame, *, level: HydroLevel) -> None:
    required = _SUBBASIN_REQUIRED if level == "sub_basin" else _BASIN_REQUIRED
    missing = [col for col in required if col not in gdf.columns]
    if missing:
        raise ValueError(
            f"Hydro {level} layer is missing required columns: {missing}. "
            "Use the canonical exported GeoJSON files."
        )


def _validate_geometries(gdf: gpd.GeoDataFrame, *, level: HydroLevel) -> None:
    if gdf.geometry.isna().any():
        raise ValueError(f"Hydro {level} layer contains null geometries.")
    if gdf.geometry.is_empty.any():
        raise ValueError(f"Hydro {level} layer contains empty geometries.")
    bad = ~gdf.geom_type.isin(["Polygon", "MultiPolygon"])
    if bad.any():
        bad_types = sorted(gdf.loc[bad].geom_type.astype(str).unique().tolist())
        raise ValueError(
            f"Hydro {level} layer contains non-areal geometries: {bad_types}."
        )


def ensure_hydro_columns(
    gdf: gpd.GeoDataFrame,
    *,
    level: HydroLevel,
) -> gpd.GeoDataFrame:
    """Validate and normalize the canonical hydro columns."""
    out = gdf.copy()
    _assert_required_columns(out, level=level)
    _validate_geometries(out, level=level)

    for column in ("basin_id", "basin_name", "hydro_level"):
        out[column] = out[column].astype(str).str.strip()

    if level == "sub_basin":
        for column in ("subbasin_id", "subbasin_code", "subbasin_name"):
            out[column] = out[column].astype(str).str.strip()

    return out


def simplify_hydro_for_render(
    gdf: gpd.GeoDataFrame,
    *,
    level: HydroLevel,
    tolerance: float,
) -> gpd.GeoDataFrame:
    """Return a render-only simplified hydro GeoDataFrame in EPSG:4326."""
    out = gdf.copy()
    out["geometry"] = out["geometry"].apply(drop_z)
    out = ensure_epsg4326(out)
    out = ensure_hydro_columns(out, level=level)

    tol = float(tolerance)
    if tol > 0.0:
        out["geometry"] = out["geometry"].simplify(tol, preserve_topology=True)

    _validate_geometries(out, level=level)
    return out.reset_index(drop=True)


def ensure_hydro_key_column(
    gdf: gpd.GeoDataFrame,
    *,
    level: HydroLevel,
    alias_fn: Callable[[str], str],
    key_col: str = "__key",
) -> gpd.GeoDataFrame:
    """Ensure a deterministic join key exists for hydro boundaries."""
    out = gdf.copy()
    if key_col in out.columns:
        return out

    if level == "sub_basin":
        out[key_col] = out["subbasin_id"].astype(str).map(alias_fn)
    else:
        out[key_col] = out["basin_id"].astype(str).map(alias_fn)
    return out


def filter_subbasins_for_basin(
    gdf: gpd.GeoDataFrame,
    basin_id: str,
    *,
    alias_fn: Callable[[str], str],
) -> gpd.GeoDataFrame:
    """Filter the sub-basin GeoDataFrame to a parent basin."""
    basin_key = alias_fn(basin_id)
    mask = gdf["basin_id"].astype(str).map(alias_fn) == basin_key
    return gdf[mask].copy()


def load_local_hydro(
    path: PathLike,
    *,
    level: HydroLevel,
    bbox: Optional[BBox] = None,
) -> gpd.GeoDataFrame:
    """
    Load a hydro boundary file and apply standard preparation steps.

    Args:
        path: Path to basin or sub-basin GeoJSON.
        level: "basin" or "sub_basin".
        bbox: Optional EPSG:4326 crop bbox.

    Returns:
        Prepared hydro GeoDataFrame in EPSG:4326.
    """
    gdf = gpd.read_file(str(path))
    gdf["geometry"] = gdf["geometry"].apply(drop_z)
    gdf = ensure_epsg4326(gdf)
    gdf = ensure_hydro_columns(gdf, level=level)
    gdf = crop_to_bbox(gdf, bbox)
    return gdf.reset_index(drop=True)

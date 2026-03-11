"""
River-network display loading and filtering utilities for IRT.

This module is intentionally Streamlit-free. It handles only the cleaned
display artifact used for hydro map overlays, not the full canonical parquet.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional, Tuple, Union

import geopandas as gpd
import pandas as pd

from shapely.geometry.base import BaseGeometry
from shapely.ops import transform


PathLike = Union[str, Path]
BBox = Tuple[float, float, float, float]

_DISPLAY_REQUIRED = [
    "river_feature_id",
    "source_uid_river",
    "river_name_clean",
    "basin_name_clean",
    "subbasin_name_clean",
    "state_names_clean",
    "length_km_source",
    "geometry",
]
_RECON_REQUIRED = [
    "hydro_basin_name",
    "hydro_basin_id",
    "river_basin_name",
    "match_status",
    "notes",
]
_RECON_STATUSES = {"matched", "no_source_rivers", "review_required"}


def drop_z(geom: BaseGeometry) -> BaseGeometry:
    """Drop Z values if present while keeping XY coordinates unchanged."""
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


def _validate_line_geometries(gdf: gpd.GeoDataFrame) -> None:
    if gdf.geometry.isna().any():
        raise ValueError("River display layer contains null geometries.")
    if gdf.geometry.is_empty.any():
        raise ValueError("River display layer contains empty geometries.")
    bad = ~gdf.geom_type.isin(["LineString", "MultiLineString"])
    if bad.any():
        bad_types = sorted(gdf.loc[bad].geom_type.astype(str).unique().tolist())
        raise ValueError(f"River display layer contains non-line geometries: {bad_types}.")


def ensure_river_display_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Validate and normalize canonical river display columns."""
    missing = [col for col in _DISPLAY_REQUIRED if col not in gdf.columns]
    if missing:
        raise ValueError(
            f"River display layer is missing required columns: {missing}. "
            "Use the cleaned river_network_display.geojson artifact."
        )

    out = gdf.copy()
    _validate_line_geometries(out)

    for column in (
        "river_feature_id",
        "source_uid_river",
        "river_name_clean",
        "basin_name_clean",
        "subbasin_name_clean",
        "state_names_clean",
    ):
        out[column] = out[column].astype(str).str.strip()

    if out["river_feature_id"].duplicated().any():
        raise ValueError("River display layer contains duplicate river_feature_id values.")

    return out


def ensure_river_key_column(
    gdf: gpd.GeoDataFrame,
    *,
    alias_fn: Callable[[str], str],
    key_col: str = "__key",
) -> gpd.GeoDataFrame:
    """Ensure a deterministic feature key exists for river display features."""
    out = gdf.copy()
    if key_col in out.columns:
        return out
    out[key_col] = out["river_feature_id"].astype(str).map(alias_fn)
    return out


def canonicalize_river_hydro_name(value: str) -> str:
    """Normalize hydro names for river display matching.

    The cleaned river network uses basin names like "Godavari" while the hydro
    polygon layer uses names like "Godavari Basin". We match them by trimming
    whitespace and removing a trailing " Basin" suffix when present.
    """
    text = str(value or "").strip()
    if text.lower().endswith(" basin"):
        text = text[:-6].strip()
    return text


def filter_rivers_for_basin(
    gdf: gpd.GeoDataFrame,
    basin_name: str,
    *,
    alias_fn: Callable[[str], str],
) -> gpd.GeoDataFrame:
    """Filter the river display layer to one basin name."""
    basin_key = alias_fn(canonicalize_river_hydro_name(basin_name))
    mask = gdf["basin_name_clean"].astype(str).map(canonicalize_river_hydro_name).map(alias_fn) == basin_key
    return gdf[mask].copy()


def filter_rivers_for_subbasin(
    gdf: gpd.GeoDataFrame,
    subbasin_name: str,
    *,
    alias_fn: Callable[[str], str],
) -> gpd.GeoDataFrame:
    """Filter the river display layer to one sub-basin name."""
    subbasin_key = alias_fn(subbasin_name)
    mask = gdf["subbasin_name_clean"].astype(str).map(alias_fn) == subbasin_key
    return gdf[mask].copy()


def ensure_river_basin_reconciliation(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize the canonical river-basin reconciliation table."""
    missing = [col for col in _RECON_REQUIRED if col not in df.columns]
    if missing:
        raise ValueError(
            f"River basin reconciliation is missing required columns: {missing}."
        )

    out = df.copy()
    for col in ("hydro_basin_name", "hydro_basin_id", "match_status", "notes"):
        out[col] = out[col].fillna("").astype(str).str.strip()

    out["river_basin_name"] = out["river_basin_name"].fillna("").astype(str).str.strip()

    if out["hydro_basin_name"].eq("").any():
        raise ValueError("River basin reconciliation contains blank hydro_basin_name values.")
    if out["hydro_basin_id"].eq("").any():
        raise ValueError("River basin reconciliation contains blank hydro_basin_id values.")
    if out["hydro_basin_name"].duplicated().any():
        raise ValueError("River basin reconciliation contains duplicate hydro_basin_name values.")
    if out["hydro_basin_id"].duplicated().any():
        raise ValueError("River basin reconciliation contains duplicate hydro_basin_id values.")

    bad_status = sorted(set(out["match_status"]) - _RECON_STATUSES)
    if bad_status:
        raise ValueError(f"River basin reconciliation contains invalid match_status values: {bad_status}.")

    matched_blank = out["match_status"].eq("matched") & out["river_basin_name"].eq("")
    if matched_blank.any():
        raise ValueError("Matched river basin reconciliation rows must include river_basin_name.")

    unresolved_named = out["match_status"].ne("matched") & out["river_basin_name"].ne("")
    if unresolved_named.any():
        raise ValueError(
            "Only matched reconciliation rows may set river_basin_name; unresolved rows must leave it blank."
        )

    return out.reset_index(drop=True)


def load_river_basin_reconciliation(path: PathLike) -> pd.DataFrame:
    """Load the canonical hydro-basin to river-basin reconciliation CSV."""
    df = pd.read_csv(str(path))
    return ensure_river_basin_reconciliation(df)


def resolve_river_basin_reconciliation(
    *,
    hydro_basin_name: str,
    reconciliation_df: Optional[pd.DataFrame],
    alias_fn: Callable[[str], str],
) -> dict[str, Optional[str]]:
    """Resolve one hydro basin against the reconciliation table."""
    default_review = {
        "status": "review_required",
        "river_basin_name": None,
        "message": "River overlay for this basin is pending basin-name reconciliation.",
    }
    if reconciliation_df is None or reconciliation_df.empty:
        return dict(default_review)

    hydro_key = alias_fn(str(hydro_basin_name or "").strip())
    work = reconciliation_df.copy()
    work["__hydro_key"] = work["hydro_basin_name"].astype(str).map(alias_fn)
    row = work.loc[work["__hydro_key"] == hydro_key]
    if row.empty:
        return dict(default_review)

    rec = row.iloc[0]
    status = str(rec.get("match_status", "")).strip()
    river_basin_name = str(rec.get("river_basin_name", "")).strip() or None
    if status == "matched" and river_basin_name:
        return {
            "status": "matched",
            "river_basin_name": river_basin_name,
            "message": None,
        }
    if status == "no_source_rivers":
        return {
            "status": status,
            "river_basin_name": None,
            "message": "No river features are currently mapped to this basin in the cleaned river source.",
        }
    return dict(default_review)


def load_local_river_display(
    path: PathLike,
    *,
    bbox: Optional[BBox] = None,
) -> gpd.GeoDataFrame:
    """Load the cleaned river display artifact in EPSG:4326."""
    gdf = gpd.read_file(str(path))
    gdf["geometry"] = gdf["geometry"].apply(drop_z)
    gdf = ensure_epsg4326(gdf)
    gdf = ensure_river_display_columns(gdf)
    gdf = crop_to_bbox(gdf, bbox)
    return gdf.reset_index(drop=True)

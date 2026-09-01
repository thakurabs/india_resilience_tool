"""
ADM2 loading and preparation utilities for IRT.

This module is intentionally Streamlit-free (no st.cache_data here).
Caching should be applied in the Streamlit layer.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Tuple, Union

import geopandas as gpd
import pandas as pd
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform


PathLike = Union[str, Path]
BBox = Tuple[float, float, float, float]  # (min_lon, min_lat, max_lon, max_lat)
logger = logging.getLogger(__name__)


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

    state_source_cols = (
        "state_name",
        "STATE_UT",
        "STATE",
        "ST_NM",
        "state",
        "adm1_name",
        "shapeName_0",
        "shapeGroup",
        "STATE_LGD",
    )
    state_values = None
    for col in state_source_cols:
        if col not in out.columns:
            continue
        values = out[col].astype("string").str.strip()
        valid = values.notna() & (values != "") & ~values.str.lower().isin({"<na>", "nan", "none", "unknown"})
        if state_values is None:
            state_values = values.where(valid)
        else:
            state_values = state_values.where(state_values.notna(), values.where(valid))
    if state_values is not None:
        out["state_name"] = state_values.fillna("Unknown").astype(str)
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
    state_col: str = "state_name",
    district_col: str,
    alias_fn: Callable[[str], str],
    key_col: str = "__key",
) -> gpd.GeoDataFrame:
    """
    Ensure a deterministic district join key column exists.

    Key format: ``"{normalized_state}|{normalized_district}"``.
    District names are not globally unique across India, so state-aware keys are
    required for correct boundary/master joins and map property patching.
    """
    out = gdf.copy()
    if key_col not in out.columns:
        state_norm = out[state_col].astype(str).map(alias_fn)
        district_norm = out[district_col].astype(str).map(alias_fn)
        out[key_col] = state_norm.str.cat(district_norm, sep="|")
    return out


def featurecollections_by_state(
    gdf,
    *,
    state_col: str,
    normalize_state_fn,
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


def load_local_adm1_artifact(path: PathLike) -> gpd.GeoDataFrame:
    """Read the precomputed ADM1 (state polygons) artifact.

    Output contract matches ``build_adm1_from_adm2``: EPSG:4326 with columns
    ``state_name`` and ``shapeName``.
    """
    gdf = gpd.read_file(str(path))
    gdf = ensure_epsg4326(gdf)
    if "state_name" not in gdf.columns and "shapeName" in gdf.columns:
        gdf["state_name"] = gdf["shapeName"].astype(str).str.strip()
    if "shapeName" not in gdf.columns and "state_name" in gdf.columns:
        gdf["shapeName"] = gdf["state_name"].astype(str).str.strip()
    missing = {"state_name", "shapeName"} - set(gdf.columns)
    if missing:
        raise ValueError(
            "ADM1 artifact must contain state_name and shapeName columns; "
            f"missing: {sorted(missing)}"
        )
    return gdf.reset_index(drop=True)


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
    Best-effort enrichment of blank/Unknown ADM2 state names via point-in-state matching.

    Existing source-derived state names are preserved. Only rows with missing,
    blank, or ``Unknown`` values in ``state_col`` are spatially joined using each
    district geometry's representative point, which is guaranteed to lie inside
    the district polygon. If the spatial join fails, the source-derived state
    names are returned and the failure is logged.
    """
    adm2 = adm2_gdf.copy()
    adm1 = adm1_gdf.copy()

    if state_col not in adm2.columns:
        adm2[state_col] = "Unknown"

    state_values = adm2[state_col].astype("string").str.strip()
    missing = state_values.isna() | (state_values == "") | state_values.str.lower().isin(
        {"<na>", "nan", "none", "unknown"}
    )
    if not bool(missing.any()):
        return adm2

    pts = adm2.loc[missing].copy()
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
    except Exception as exc:
        logger.warning(
            "ADM2 state-name spatial enrichment failed; preserving source-derived state names: %s",
            exc,
        )
        return adm2

    state_values = adm2[state_col].astype("string").str.strip()
    missing = state_values.isna() | (state_values == "") | state_values.str.lower().isin(
        {"<na>", "nan", "none", "unknown"}
    )
    adm2.loc[missing, state_col] = "Unknown"
    if bool(missing.any()):
        count = int(missing.sum())
        total = max(int(len(adm2)), 1)
        logger.warning(
            "ADM2 state-name enrichment left %d/%d rows (%.1f%%) as Unknown.",
            count,
            total,
            count / total * 100.0,
        )
    return adm2

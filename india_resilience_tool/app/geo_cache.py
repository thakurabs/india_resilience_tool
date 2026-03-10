"""
Streamlit-cached geo loaders and FeatureCollection builders.

This module exists to keep the dashboard orchestrator thin while preserving the
runtime behavior (same caching, tolerances, and keying strategy).
"""

from __future__ import annotations

import geopandas as gpd
import streamlit as st

from india_resilience_tool.config.constants import (
    MAX_LAT,
    MAX_LON,
    MIN_LAT,
    MIN_LON,
    SIMPLIFY_TOL_ADM2,
    SIMPLIFY_TOL_ADM3,
    SIMPLIFY_TOL_BASIN_RENDER,
    SIMPLIFY_TOL_SUBBASIN_RENDER,
)
from india_resilience_tool.data.adm2_loader import (
    build_adm1_from_adm2 as _build_adm1_from_adm2,
    enrich_adm2_with_state_names as _enrich_adm2_with_state_names,
    ensure_key_column as _ensure_key_column,
    featurecollections_by_state as _featurecollections_by_state,
    load_local_adm2 as _load_local_adm2,
)
from india_resilience_tool.data.adm3_loader import load_local_adm3 as _load_local_adm3
from india_resilience_tool.data.hydro_loader import (
    ensure_hydro_key_column as _ensure_hydro_key_column,
    load_local_hydro as _load_local_hydro,
    simplify_hydro_for_render as _simplify_hydro_for_render,
)
from india_resilience_tool.utils.naming import alias, normalize_name


def _featurecollections_by_selector(
    gdf: gpd.GeoDataFrame,
    *,
    selector_col: str,
    keep_cols: list[str],
) -> dict[str, dict]:
    """Build geometry-only FeatureCollections keyed by a normalized selector column."""
    by_selector = _featurecollections_by_state(
        gdf,
        state_col=selector_col,
        normalize_state_fn=alias,
        keep_cols=keep_cols,
    )
    if "all" not in by_selector:
        all_features: list[dict] = []
        for fc in by_selector.values():
            all_features.extend((fc or {}).get("features", []) or [])
        by_selector = dict(by_selector)
        by_selector["all"] = {"type": "FeatureCollection", "features": all_features}
    return by_selector


@st.cache_data
def load_local_adm2(path: str, tolerance: float = SIMPLIFY_TOL_ADM2) -> gpd.GeoDataFrame:
    gdf = _load_local_adm2(
        path=path,
        tolerance=float(tolerance),
        bbox=(MIN_LON, MIN_LAT, MAX_LON, MAX_LAT),
        min_area=0.0003,
    )
    return gdf


@st.cache_data(ttl=3600)
def load_local_adm3(path: str, tolerance: float = SIMPLIFY_TOL_ADM3) -> gpd.GeoDataFrame:
    """
    Load ADM3 (blocks) with the same bbox + simplification strategy as ADM2.

    Notes:
      - tolerant of large files via caching
      - does NOT require a __key column (merge.py builds composite keys for blocks)
    """
    gdf = _load_local_adm3(
        path=path,
        tolerance=float(tolerance),
        bbox=(MIN_LON, MIN_LAT, MAX_LON, MAX_LAT),
        min_area=0.00005,
    )
    return gdf


@st.cache_data(ttl=3600)
def load_local_basin(path: str) -> gpd.GeoDataFrame:
    """Load basin GeoJSON with canonical hydro columns."""
    return _load_local_hydro(
        path=path,
        level="basin",
        bbox=(MIN_LON, MIN_LAT, MAX_LON, MAX_LAT),
    )


@st.cache_data(ttl=3600)
def load_local_subbasin(path: str) -> gpd.GeoDataFrame:
    """Load sub-basin GeoJSON with canonical hydro columns."""
    return _load_local_hydro(
        path=path,
        level="sub_basin",
        bbox=(MIN_LON, MIN_LAT, MAX_LON, MAX_LAT),
    )


@st.cache_data(ttl=3600)
def load_local_basin_render(
    path: str,
    tolerance: float = SIMPLIFY_TOL_BASIN_RENDER,
) -> gpd.GeoDataFrame:
    """Load basin GeoJSON and apply render-only simplification."""
    gdf = load_local_basin(path)
    return _simplify_hydro_for_render(gdf, level="basin", tolerance=float(tolerance))


@st.cache_data(ttl=3600)
def load_local_subbasin_render(
    path: str,
    tolerance: float = SIMPLIFY_TOL_SUBBASIN_RENDER,
) -> gpd.GeoDataFrame:
    """Load sub-basin GeoJSON and apply render-only simplification."""
    gdf = load_local_subbasin(path)
    return _simplify_hydro_for_render(gdf, level="sub_basin", tolerance=float(tolerance))


@st.cache_data(ttl=3600)
def load_basin_selector_index(path: str) -> dict[str, object]:
    """Return cached basin selector metadata for hydro sidebar controls."""
    gdf = load_local_basin(path)
    basin_names = sorted(gdf["basin_name"].astype(str).str.strip().unique().tolist())
    return {
        "basin_names": basin_names,
    }


@st.cache_data(ttl=3600)
def load_subbasin_selector_index(path: str) -> dict[str, object]:
    """Return cached sub-basin selector metadata keyed by basin name."""
    gdf = load_local_subbasin(path)
    basin_names = sorted(gdf["basin_name"].astype(str).str.strip().unique().tolist())
    subbasins_all = sorted(gdf["subbasin_name"].astype(str).str.strip().unique().tolist())

    subbasins_by_basin: dict[str, list[str]] = {}
    for basin_name, basin_df in gdf.groupby(gdf["basin_name"].astype(str).str.strip(), dropna=False):
        basin_key = str(basin_name).strip()
        subbasins_by_basin[basin_key] = sorted(
            basin_df["subbasin_name"].astype(str).str.strip().unique().tolist()
        )

    return {
        "basin_names": basin_names,
        "subbasins_all": subbasins_all,
        "subbasins_by_basin": subbasins_by_basin,
    }


@st.cache_data(ttl=3600)
def build_adm2_geojson_by_state(
    path: str,
    tolerance: float,
    mtime: float,
) -> dict[str, dict]:
    """
    Build and cache an ADM2 FeatureCollection per state (geometry + identifiers only).

    Cached by (path, tolerance, mtime) so it invalidates automatically when the
    source GeoJSON changes or simplification tolerance is updated.
    """
    _ = mtime  # used only to invalidate Streamlit's cache

    gdf = load_local_adm2(path, tolerance=tolerance)
    if "__key" not in gdf.columns:
        gdf = _ensure_key_column(gdf, district_col="district_name", alias_fn=alias, key_col="__key")

    by_state = _featurecollections_by_state(
        gdf,
        state_col="state_name",
        normalize_state_fn=normalize_name,
        keep_cols=["district_name", "state_name", "__key", "geometry"],
    )
    return by_state


@st.cache_data(ttl=3600)
def build_adm3_geojson_by_state(
    path: str,
    tolerance: float,
    mtime: float,
) -> dict[str, dict]:
    """
    Build and cache an ADM3 FeatureCollection per state (geometry + identifiers only).

    Cached by (path, tolerance, mtime) so it invalidates automatically when the
    source GeoJSON changes or simplification tolerance is updated.
    """
    _ = mtime  # used only to invalidate Streamlit's cache

    gdf = load_local_adm3(path, tolerance=tolerance)

    # Tolerate alternate ADM3 naming conventions.
    if "block_name" not in gdf.columns:
        for c in ("block", "adm3_name", "subdistrict_name", "name"):
            if c in gdf.columns:
                gdf["block_name"] = gdf[c]
                break
    if "district_name" not in gdf.columns:
        for c in ("district", "adm2_name", "shapeName_2", "shapeName_1"):
            if c in gdf.columns:
                gdf["district_name"] = gdf[c]
                break
    if "state_name" not in gdf.columns:
        for c in ("state", "adm1_name", "shapeName_0", "shapeGroup"):
            if c in gdf.columns:
                gdf["state_name"] = gdf[c]
                break

    # Build a composite key: state|district|block (normalized via alias)
    if "__bkey" not in gdf.columns:

        def _mk_bkey(r) -> str:
            return (
                f"{alias(r.get('state_name', ''))}|"
                f"{alias(r.get('district_name', ''))}|"
                f"{alias(r.get('block_name', ''))}"
            )

        gdf["__bkey"] = gdf.apply(_mk_bkey, axis=1)

    by_state = _featurecollections_by_state(
        gdf,
        state_col="state_name",
        normalize_state_fn=normalize_name,
        keep_cols=["block_name", "district_name", "state_name", "__bkey", "geometry"],
    )
    return by_state


@st.cache_data(ttl=3600)
def build_basin_geojson_all(
    path: str,
    mtime: float,
    tolerance: float = SIMPLIFY_TOL_BASIN_RENDER,
) -> dict[str, dict]:
    """Build a single nationwide basin FeatureCollection cache."""
    _ = mtime
    gdf = load_local_basin_render(path, tolerance=tolerance)
    gdf = _ensure_hydro_key_column(gdf, level="basin", alias_fn=alias, key_col="__key")
    return _featurecollections_by_state(
        gdf.assign(state_name="All"),
        state_col="state_name",
        normalize_state_fn=normalize_name,
        keep_cols=["basin_id", "basin_name", "hydro_level", "__key", "geometry"],
    )


@st.cache_data(ttl=3600)
def build_basin_geojson_by_basin(
    path: str,
    mtime: float,
    tolerance: float = SIMPLIFY_TOL_BASIN_RENDER,
) -> dict[str, dict]:
    """Build basin FeatureCollections keyed by normalized basin name."""
    _ = mtime
    gdf = load_local_basin_render(path, tolerance=tolerance)
    gdf = _ensure_hydro_key_column(gdf, level="basin", alias_fn=alias, key_col="__key")
    gdf = gdf.copy()
    gdf["__selector"] = gdf["basin_name"].astype(str).map(alias)
    return _featurecollections_by_selector(
        gdf,
        selector_col="__selector",
        keep_cols=["basin_id", "basin_name", "hydro_level", "__key", "geometry"],
    )


@st.cache_data(ttl=3600)
def build_subbasin_geojson_all(
    path: str,
    mtime: float,
    tolerance: float = SIMPLIFY_TOL_SUBBASIN_RENDER,
) -> dict[str, dict]:
    """Build a single nationwide sub-basin FeatureCollection cache."""
    _ = mtime
    gdf = load_local_subbasin_render(path, tolerance=tolerance)
    gdf = _ensure_hydro_key_column(gdf, level="sub_basin", alias_fn=alias, key_col="__key")
    return _featurecollections_by_state(
        gdf.assign(state_name="All"),
        state_col="state_name",
        normalize_state_fn=normalize_name,
        keep_cols=[
            "basin_id",
            "basin_name",
            "subbasin_id",
            "subbasin_code",
            "subbasin_name",
            "hydro_level",
            "__key",
            "geometry",
        ],
    )


@st.cache_data(ttl=3600)
def build_subbasin_geojson_by_basin(
    path: str,
    mtime: float,
    tolerance: float = SIMPLIFY_TOL_SUBBASIN_RENDER,
) -> dict[str, dict]:
    """Build sub-basin FeatureCollections keyed by normalized parent basin name."""
    _ = mtime
    gdf = load_local_subbasin_render(path, tolerance=tolerance)
    gdf = _ensure_hydro_key_column(gdf, level="sub_basin", alias_fn=alias, key_col="__key")
    gdf = gdf.copy()
    gdf["__selector"] = gdf["basin_name"].astype(str).map(alias)
    return _featurecollections_by_selector(
        gdf,
        selector_col="__selector",
        keep_cols=[
            "basin_id",
            "basin_name",
            "subbasin_id",
            "subbasin_code",
            "subbasin_name",
            "hydro_level",
            "__key",
            "geometry",
        ],
    )


@st.cache_data
def build_adm1_from_adm2(_adm2_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Streamlit cannot reliably hash GeoDataFrames; the leading underscore tells
    # Streamlit to exclude this parameter from the cache key.
    return _build_adm1_from_adm2(_adm2_gdf, state_col="state_name")


@st.cache_data
def enrich_adm2_with_state_names(
    _adm2_gdf: gpd.GeoDataFrame, _adm1_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    # Streamlit cannot reliably hash GeoDataFrames; the leading underscores tell
    # Streamlit to exclude these parameters from the cache key.
    return _enrich_adm2_with_state_names(
        _adm2_gdf, _adm1_gdf, state_col="state_name", adm1_name_col="shapeName"
    )


@st.cache_data(ttl=180)
def list_available_states_from_processed_root_cached(processed_root_str: str) -> list[str]:
    from india_resilience_tool.app.geography import list_available_states_from_processed_root

    return list_available_states_from_processed_root(processed_root_str)

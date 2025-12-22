"""
Streamlit-cached ADM2 loaders for IRT.

This module keeps Streamlit caching inside app/ while delegating pure geo logic
to india_resilience_tool.data.adm2_loader.

It also guarantees the legacy contract that geojson_by_state contains an "all"
FeatureCollection key.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import streamlit as st

from india_resilience_tool.data.adm2_loader import (
    ensure_key_column,
    featurecollections_by_state,
    load_local_adm2 as _load_local_adm2,
)
from india_resilience_tool.utils.naming import alias, normalize_name


DEFAULT_BBOX: Tuple[float, float, float, float] = (68.0, 5.0, 97.5, 45.0)
DEFAULT_MIN_AREA: float = 0.0003


def ensure_all_featurecollection(by_state: Dict[str, dict]) -> Dict[str, dict]:
    """
    Ensure the legacy contract: by_state["all"] exists.

    Args:
        by_state: mapping normalized_state -> FeatureCollection

    Returns:
        A dict that includes "all" FeatureCollection.
    """
    if "all" in by_state:
        return by_state

    all_features: List[dict] = []
    for key in sorted(by_state.keys()):
        fc = by_state.get(key) or {}
        all_features.extend(fc.get("features", []) or [])

    out = dict(by_state)
    out["all"] = {"type": "FeatureCollection", "features": all_features}
    return out


@st.cache_data
def load_local_adm2(
    path: str,
    tolerance: float,
    *,
    bbox: Tuple[float, float, float, float] = DEFAULT_BBOX,
    min_area: float = DEFAULT_MIN_AREA,
):
    """
    Streamlit-cached ADM2 load.

    Args:
        path: path to ADM2 GeoJSON
        tolerance: simplification tolerance
        bbox: (min_lon, min_lat, max_lon, max_lat)
        min_area: minimum polygon area threshold

    Returns:
        GeoDataFrame (kept untyped here to avoid geopandas import in app layer)
    """
    return _load_local_adm2(
        path=path,
        tolerance=float(tolerance),
        bbox=bbox,
        min_area=float(min_area),
    )


@st.cache_data(ttl=3600)
def build_adm2_geojson_by_state(
    path: str,
    tolerance: float,
    mtime: float,
    *,
    bbox: Tuple[float, float, float, float] = DEFAULT_BBOX,
    min_area: float = DEFAULT_MIN_AREA,
) -> Dict[str, dict]:
    """
    Build and cache an ADM2 FeatureCollection per state.

    Cache key includes (path, tolerance, mtime, bbox, min_area) so it invalidates
    when the source file changes or settings change.

    Contract:
      - includes district_name/state_name/__key in feature.properties
      - ALWAYS includes by_state["all"] FeatureCollection fallback

    Args:
        path: path to ADM2 GeoJSON
        tolerance: simplification tolerance
        mtime: file mtime used only for cache invalidation
        bbox: (min_lon, min_lat, max_lon, max_lat)
        min_area: minimum polygon area threshold

    Returns:
        Dict[str, FeatureCollection]
    """
    _ = mtime  # used only to invalidate Streamlit cache

    gdf = load_local_adm2(path, tolerance=tolerance, bbox=bbox, min_area=min_area)

    if "__key" not in getattr(gdf, "columns", []):
        gdf = ensure_key_column(
            gdf,
            district_col="district_name",
            alias_fn=alias,
            key_col="__key",
        )

    by_state = featurecollections_by_state(
        gdf,
        state_col="state_name",
        normalize_state_fn=normalize_name,
        keep_cols=["district_name", "state_name", "__key", "geometry"],
    )

    return ensure_all_featurecollection(by_state)

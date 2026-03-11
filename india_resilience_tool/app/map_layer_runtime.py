"""
Map-layer runtime helpers (Folium + GeoJSON patching) for IRT.

This module is app-layer (Folium is OK) but Streamlit-free: it builds a Folium
map object for the current selection by:
- loading a cached per-state FeatureCollection (geometry-only)
- patching per-feature properties from the current merged dataframe
- attaching a tooltip/highlight function
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Tuple

from india_resilience_tool.config.constants import (
    SIMPLIFY_TOL_BASIN_RENDER,
    SIMPLIFY_TOL_SUBBASIN_RENDER,
)


def build_folium_map_for_selection(
    *,
    level: str,
    merged: Any,
    display_gdf: Any,
    selected_state: str,
    selected_district: str,
    selected_basin: str,
    selected_subbasin: str,
    map_mode: str,
    baseline_col: Optional[str],
    rank_scope_label: str,
    metric_col: str,
    map_value_col: str,
    alias_fn: Callable[[str], str],
    normalize_state_fn: Callable[[str], str],
    adm1: Any,
    map_center: list[float],
    map_zoom: float,
    bounds_latlon: list[list[float]],
    hover_enabled: bool,
    # GeoJSON cache inputs
    adm2_geojson_path: Path,
    adm3_geojson_path: Path,
    basin_geojson_path: Path,
    subbasin_geojson_path: Path,
    simplify_tolerance_adm2: float,
    simplify_tolerance_adm3: float,
    crosswalk_overlay: Optional[Mapping[str, Any]] = None,
) -> Any:
    from india_resilience_tool.app.geo_cache import (
        build_adm2_geojson_by_state,
        build_adm3_geojson_by_state,
        build_basin_geojson_all,
        build_basin_geojson_by_basin,
        build_subbasin_geojson_all,
        build_subbasin_geojson_by_basin,
    )
    from india_resilience_tool.app.views.map_view import build_choropleth_map_with_geojson_layer
    from india_resilience_tool.viz.folium_featurecollection import (
        build_geojson_tooltip,
        build_props_map_from_gdf,
        ensure_geojson_by_state_has_all,
        filter_fc_by_district,
        filter_fc_by_feature_keys,
        patch_fc_properties,
    )

    level_norm = str(level).strip().lower()
    if level_norm not in {"district", "block", "basin", "sub_basin"}:
        level_norm = "district"

    # -------------------------
    # GeoJSON-by-state cache (geometry cached; properties patched per rerun)
    # -------------------------
    if level_norm == "sub_basin":
        subbasin_mtime = float(subbasin_geojson_path.stat().st_mtime)
        if selected_basin != "All":
            geojson_by_state = build_subbasin_geojson_by_basin(
                path=str(subbasin_geojson_path),
                mtime=subbasin_mtime,
                tolerance=SIMPLIFY_TOL_SUBBASIN_RENDER,
            )
        else:
            geojson_by_state = build_subbasin_geojson_all(
                path=str(subbasin_geojson_path),
                mtime=subbasin_mtime,
                tolerance=SIMPLIFY_TOL_SUBBASIN_RENDER,
            )
    elif level_norm == "basin":
        basin_mtime = float(basin_geojson_path.stat().st_mtime)
        if selected_basin != "All":
            geojson_by_state = build_basin_geojson_by_basin(
                path=str(basin_geojson_path),
                mtime=basin_mtime,
                tolerance=SIMPLIFY_TOL_BASIN_RENDER,
            )
        else:
            geojson_by_state = build_basin_geojson_all(
                path=str(basin_geojson_path),
                mtime=basin_mtime,
                tolerance=SIMPLIFY_TOL_BASIN_RENDER,
            )
    elif level_norm == "block":
        adm3_mtime = float(adm3_geojson_path.stat().st_mtime)
        geojson_by_state = build_adm3_geojson_by_state(
            path=str(adm3_geojson_path),
            tolerance=simplify_tolerance_adm3,
            mtime=adm3_mtime,
        )
    else:
        adm2_mtime = float(adm2_geojson_path.stat().st_mtime)
        geojson_by_state = build_adm2_geojson_by_state(
            path=str(adm2_geojson_path),
            tolerance=simplify_tolerance_adm2,
            mtime=adm2_mtime,
        )

    if level_norm in {"basin", "sub_basin"}:
        selection_key = alias_fn(selected_basin) if selected_basin != "All" else "all"
        fc_source = geojson_by_state.get(selection_key, geojson_by_state.get("all"))
        geojson_by_state = ensure_geojson_by_state_has_all(geojson_by_state)
        fc = copy.deepcopy(fc_source or geojson_by_state["all"])
    else:
        state_key = "all" if selected_state == "All" else (normalize_state_fn(selected_state) or "unknown")
        geojson_by_state = ensure_geojson_by_state_has_all(geojson_by_state)
        fc = copy.deepcopy(geojson_by_state.get(state_key, geojson_by_state["all"]))
    fc = filter_fc_by_district(
        fc,
        selected_district=selected_district,
        selected_basin=selected_basin,
        selected_subbasin=selected_subbasin,
        level=level_norm,
        alias_fn=alias_fn,
    )

    prop_gdf = display_gdf if getattr(display_gdf, "empty", False) is False else merged
    feature_key_col = "__bkey" if level_norm == "block" else "__key"

    props_map, _value_cols, _text_cols = build_props_map_from_gdf(
        prop_gdf,
        level=level_norm,
        alias_fn=alias_fn,
        feature_key_col=feature_key_col,
        metric_col=metric_col,
        map_value_col=map_value_col,
    )
    fc = patch_fc_properties(
        fc,
        level=level_norm,
        alias_fn=alias_fn,
        feature_key_col=feature_key_col,
        props_map=props_map,
    )

    reference_fc = None
    reference_level = None
    reference_layer_name = None
    overlay_level = str((crosswalk_overlay or {}).get("level", "")).strip().lower()
    overlay_feature_keys = list((crosswalk_overlay or {}).get("feature_keys", []) or [])
    if overlay_level in {"district", "block", "basin", "sub_basin"} and overlay_feature_keys:
        if overlay_level == "district":
            adm2_mtime = float(adm2_geojson_path.stat().st_mtime)
            overlay_geojson_by_state = build_adm2_geojson_by_state(
                path=str(adm2_geojson_path),
                tolerance=simplify_tolerance_adm2,
                mtime=adm2_mtime,
            )
            overlay_geojson_by_state = ensure_geojson_by_state_has_all(overlay_geojson_by_state)
            reference_fc = filter_fc_by_feature_keys(
                overlay_geojson_by_state["all"],
                feature_keys=overlay_feature_keys,
                level="district",
                alias_fn=alias_fn,
            )
            reference_level = "district"
        elif overlay_level == "block":
            adm3_mtime = float(adm3_geojson_path.stat().st_mtime)
            overlay_geojson_by_state = build_adm3_geojson_by_state(
                path=str(adm3_geojson_path),
                tolerance=simplify_tolerance_adm3,
                mtime=adm3_mtime,
            )
            overlay_geojson_by_state = ensure_geojson_by_state_has_all(overlay_geojson_by_state)
            reference_fc = filter_fc_by_feature_keys(
                overlay_geojson_by_state["all"],
                feature_keys=overlay_feature_keys,
                level="block",
                alias_fn=alias_fn,
                key_col="__bkey",
            )
            reference_level = "block"
        elif overlay_level == "basin":
            basin_mtime = float(basin_geojson_path.stat().st_mtime)
            overlay_geojson_by_state = build_basin_geojson_all(
                path=str(basin_geojson_path),
                mtime=basin_mtime,
                tolerance=SIMPLIFY_TOL_BASIN_RENDER,
            )
            overlay_geojson_by_state = ensure_geojson_by_state_has_all(overlay_geojson_by_state)
            reference_fc = filter_fc_by_feature_keys(
                overlay_geojson_by_state["all"],
                feature_keys=overlay_feature_keys,
                level="basin",
                alias_fn=alias_fn,
            )
            reference_level = "basin"
        else:
            subbasin_mtime = float(subbasin_geojson_path.stat().st_mtime)
            overlay_geojson_by_state = build_subbasin_geojson_all(
                path=str(subbasin_geojson_path),
                mtime=subbasin_mtime,
                tolerance=SIMPLIFY_TOL_SUBBASIN_RENDER,
            )
            overlay_geojson_by_state = ensure_geojson_by_state_has_all(overlay_geojson_by_state)
            reference_fc = filter_fc_by_feature_keys(
                overlay_geojson_by_state["all"],
                feature_keys=overlay_feature_keys,
                level="sub_basin",
                alias_fn=alias_fn,
            )
            reference_level = "sub_basin"

        reference_layer_name = str((crosswalk_overlay or {}).get("label", "")).strip() or "Related units"

    highlight_fn = None
    tooltip = None
    if level_norm == "sub_basin":
        layer_name = "Sub-basins"
    elif level_norm == "basin":
        layer_name = "Basins"
    else:
        layer_name = "Blocks" if level_norm == "block" else "Districts"

    if hover_enabled:
        tooltip = build_geojson_tooltip(
            level=level_norm,
            map_mode=map_mode,
            has_baseline=bool(baseline_col and (baseline_col in getattr(merged, "columns", []))),
            rank_scope_label=rank_scope_label,
        )

        highlight_fn = lambda _f: {
            "fillColor": "#ffff00",
            "color": "#000",
            "weight": 2,
            "fillOpacity": 0.9,
        }

    m = build_choropleth_map_with_geojson_layer(
        fc=fc,
        map_center=map_center,
        map_zoom=map_zoom,
        bounds_latlon=bounds_latlon,
        adm1=adm1,
        selected_state=selected_state,
        selected_district=selected_district,
        layer_name=layer_name,
        tooltip=tooltip,
        highlight_function=highlight_fn,
        reference_fc=reference_fc,
        reference_level=reference_level,
        reference_layer_name=reference_layer_name,
    )
    return m

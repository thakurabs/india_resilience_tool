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
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

from india_resilience_tool.config.constants import (
    SIMPLIFY_TOL_BASIN_RENDER,
    SIMPLIFY_TOL_SUBBASIN_RENDER,
)


def _empty_fc() -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": []}


def _union_featurecollections(collections: Sequence[Optional[Mapping[str, Any]]]) -> dict[str, Any]:
    """Return a shallow union of FeatureCollection features."""
    features: list[dict[str, Any]] = []
    for fc in collections:
        if not fc:
            continue
        features.extend(list((fc or {}).get("features", []) or []))
    return {"type": "FeatureCollection", "features": features}


def build_folium_map_for_selection(
    *,
    level: str,
    merged: Any,
    display_gdf: Any,
    session_state: Any,
    render_signature: Tuple[Any, ...],
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
    river_display_geojson_path: Path,
    simplify_tolerance_adm2: float,
    simplify_tolerance_adm3: float,
    crosswalk_overlay: Optional[Mapping[str, Any]] = None,
    show_river_network: bool = False,
    resolved_river_basin_name: Optional[str] = None,
    perf_section: Optional[Callable[[str], Any]] = None,
) -> Any:
    from india_resilience_tool.app.geo_cache import (
        build_adm2_geojson_by_state,
        build_adm3_geojson_by_district,
        build_adm3_geojson_by_state,
        build_basin_geojson_all,
        build_basin_geojson_by_basin,
        build_river_geojson_by_basin,
        build_river_geojson_by_subbasin,
        build_subbasin_geojson_all,
        build_subbasin_geojson_by_basin,
    )
    from india_resilience_tool.app.views.map_view import (
        add_reference_overlay_layer,
        add_river_overlay_layer,
        build_base_choropleth_map_with_geojson_layer,
    )
    from india_resilience_tool.viz.folium_featurecollection import (
        build_geojson_tooltip,
        build_props_map_from_gdf,
        clone_featurecollection_for_patch,
        ensure_geojson_by_state_has_all,
        filter_fc_by_district,
        filter_fc_by_feature_keys,
        patch_fc_properties,
        props_map_signature,
    )

    def _patched_fc_cache() -> dict[tuple[Any, ...], dict[str, Any]]:
        cache = session_state.get("_patched_fc_cache")
        if not isinstance(cache, dict):
            cache = {}
            session_state["_patched_fc_cache"] = cache
        return cache

    def _folium_base_map_cache() -> dict[tuple[Any, ...], Any]:
        cache = session_state.get("_folium_base_map_cache")
        if not isinstance(cache, dict):
            cache = {}
            session_state["_folium_base_map_cache"] = cache
        return cache

    level_norm = str(level).strip().lower()
    if level_norm not in {"district", "block", "basin", "sub_basin"}:
        level_norm = "district"

    # -------------------------
    # GeoJSON-by-state cache (geometry cached; properties patched per rerun)
    # -------------------------
    geojson_signature: tuple[Any, ...]
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
        geojson_signature = ("sub_basin", subbasin_mtime, alias_fn(selected_basin), alias_fn(selected_subbasin))
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
        geojson_signature = ("basin", basin_mtime, alias_fn(selected_basin))
    elif level_norm == "block":
        adm3_mtime = float(adm3_geojson_path.stat().st_mtime)
        if selected_state != "All" and selected_district != "All":
            geojson_by_state = build_adm3_geojson_by_district(
                path=str(adm3_geojson_path),
                tolerance=simplify_tolerance_adm3,
                mtime=adm3_mtime,
            )
            geojson_signature = (
                "block",
                "district",
                adm3_mtime,
                alias_fn(selected_state),
                alias_fn(selected_district),
            )
        else:
            geojson_by_state = build_adm3_geojson_by_state(
                path=str(adm3_geojson_path),
                tolerance=simplify_tolerance_adm3,
                mtime=adm3_mtime,
            )
            geojson_signature = ("block", "state", adm3_mtime, alias_fn(selected_state))
    else:
        adm2_mtime = float(adm2_geojson_path.stat().st_mtime)
        geojson_by_state = build_adm2_geojson_by_state(
            path=str(adm2_geojson_path),
            tolerance=simplify_tolerance_adm2,
            mtime=adm2_mtime,
        )
        geojson_signature = ("district", adm2_mtime, alias_fn(selected_state))

    if level_norm in {"basin", "sub_basin"}:
        selection_key = alias_fn(selected_basin) if selected_basin != "All" else "all"
        fc_source = geojson_by_state.get(selection_key, geojson_by_state.get("all"))
        geojson_by_state = ensure_geojson_by_state_has_all(geojson_by_state)
    else:
        geojson_by_state = ensure_geojson_by_state_has_all(geojson_by_state)
        if level_norm == "block" and selected_state != "All" and selected_district != "All":
            selector_key = f"{alias_fn(selected_state)}|{alias_fn(selected_district)}"
            fc_source = geojson_by_state.get(selector_key, geojson_by_state.get("all"))
        else:
            state_key = "all" if selected_state == "All" else (normalize_state_fn(selected_state) or "unknown")
            fc_source = geojson_by_state.get(state_key, geojson_by_state["all"])

    base_fc = filter_fc_by_district(
        fc_source or geojson_by_state["all"],
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
    prop_signature = props_map_signature(props_map)
    patched_cache_key = (
        "patched_fc",
        *render_signature,
        *geojson_signature,
        feature_key_col,
        prop_signature,
    )
    patched_fc_cache = _patched_fc_cache()
    cached_fc = patched_fc_cache.get(patched_cache_key)
    if cached_fc is None:
        fc = clone_featurecollection_for_patch(base_fc)
        fc = patch_fc_properties(
            fc,
            level=level_norm,
            alias_fn=alias_fn,
            feature_key_col=feature_key_col,
            props_map=props_map,
        )
        patched_fc_cache[patched_cache_key] = clone_featurecollection_for_patch(fc)
        while len(patched_fc_cache) > 24:
            patched_fc_cache.pop(next(iter(patched_fc_cache)))
    else:
        fc = clone_featurecollection_for_patch(cached_fc)

    reference_fc = None
    reference_level = None
    reference_layer_name = None
    river_fc = None
    river_layer_name = None
    overlay_spec = crosswalk_overlay or {}
    overlay_level = str(overlay_spec.get("level", "")).strip().lower()
    overlay_feature_keys = list(overlay_spec.get("feature_keys", []) or [])
    overlay_scope_dimension = str(overlay_spec.get("scope_dimension", "")).strip().lower()
    overlay_scope_values = [str(v).strip() for v in (overlay_spec.get("scope_values") or []) if str(v).strip()]
    if overlay_level in {"district", "block", "basin", "sub_basin"} and overlay_feature_keys:
        if overlay_level == "district":
            adm2_mtime = float(adm2_geojson_path.stat().st_mtime)
            overlay_geojson_by_state = build_adm2_geojson_by_state(
                path=str(adm2_geojson_path),
                tolerance=simplify_tolerance_adm2,
                mtime=adm2_mtime,
            )
            overlay_geojson_by_state = ensure_geojson_by_state_has_all(overlay_geojson_by_state)
            overlay_source_fc = overlay_geojson_by_state["all"]
            if overlay_scope_dimension == "state_name" and overlay_scope_values:
                overlay_source_fc = _union_featurecollections(
                    overlay_geojson_by_state.get(normalize_state_fn(state_name))
                    for state_name in overlay_scope_values
                )
            reference_fc = filter_fc_by_feature_keys(
                overlay_source_fc,
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
            overlay_source_fc = overlay_geojson_by_state["all"]
            if overlay_scope_dimension == "state_name" and overlay_scope_values:
                overlay_source_fc = _union_featurecollections(
                    overlay_geojson_by_state.get(normalize_state_fn(state_name))
                    for state_name in overlay_scope_values
                )
            reference_fc = filter_fc_by_feature_keys(
                overlay_source_fc,
                feature_keys=overlay_feature_keys,
                level="block",
                alias_fn=alias_fn,
                key_col="__bkey",
            )
            reference_level = "block"
        elif overlay_level == "basin":
            basin_mtime = float(basin_geojson_path.stat().st_mtime)
            if overlay_scope_dimension == "basin_name" and overlay_scope_values:
                overlay_geojson_by_state = build_basin_geojson_by_basin(
                    path=str(basin_geojson_path),
                    mtime=basin_mtime,
                    tolerance=SIMPLIFY_TOL_BASIN_RENDER,
                )
                overlay_geojson_by_state = ensure_geojson_by_state_has_all(overlay_geojson_by_state)
                overlay_source_fc = _union_featurecollections(
                    overlay_geojson_by_state.get(alias_fn(basin_name))
                    for basin_name in overlay_scope_values
                )
            else:
                overlay_geojson_by_state = build_basin_geojson_all(
                    path=str(basin_geojson_path),
                    mtime=basin_mtime,
                    tolerance=SIMPLIFY_TOL_BASIN_RENDER,
                )
                overlay_geojson_by_state = ensure_geojson_by_state_has_all(overlay_geojson_by_state)
                overlay_source_fc = overlay_geojson_by_state["all"]
            reference_fc = filter_fc_by_feature_keys(
                overlay_source_fc,
                feature_keys=overlay_feature_keys,
                level="basin",
                alias_fn=alias_fn,
            )
            reference_level = "basin"
        else:
            subbasin_mtime = float(subbasin_geojson_path.stat().st_mtime)
            if overlay_scope_dimension == "basin_name" and overlay_scope_values:
                overlay_geojson_by_state = build_subbasin_geojson_by_basin(
                    path=str(subbasin_geojson_path),
                    mtime=subbasin_mtime,
                    tolerance=SIMPLIFY_TOL_SUBBASIN_RENDER,
                )
                overlay_geojson_by_state = ensure_geojson_by_state_has_all(overlay_geojson_by_state)
                overlay_source_fc = _union_featurecollections(
                    overlay_geojson_by_state.get(alias_fn(basin_name))
                    for basin_name in overlay_scope_values
                )
            else:
                overlay_geojson_by_state = build_subbasin_geojson_all(
                    path=str(subbasin_geojson_path),
                    mtime=subbasin_mtime,
                    tolerance=SIMPLIFY_TOL_SUBBASIN_RENDER,
                )
                overlay_geojson_by_state = ensure_geojson_by_state_has_all(overlay_geojson_by_state)
                overlay_source_fc = overlay_geojson_by_state["all"]
            reference_fc = filter_fc_by_feature_keys(
                overlay_source_fc,
                feature_keys=overlay_feature_keys,
                level="sub_basin",
                alias_fn=alias_fn,
            )
            reference_level = "sub_basin"

        reference_layer_name = str(overlay_spec.get("label", "")).strip() or "Related units"

    if (
        show_river_network
        and level_norm in {"basin", "sub_basin"}
        and river_display_geojson_path.exists()
        and selected_basin != "All"
    ):
        river_mtime = float(river_display_geojson_path.stat().st_mtime)
        if level_norm == "sub_basin" and selected_subbasin != "All":
            river_geojson_by_selector = build_river_geojson_by_subbasin(
                path=str(river_display_geojson_path),
                mtime=river_mtime,
            )
            selector_key = alias_fn(selected_subbasin)
            river_fc = clone_featurecollection_for_patch(
                river_geojson_by_selector.get(selector_key, _empty_fc())
            )
            river_layer_name = "River network"
        else:
            river_geojson_by_selector = build_river_geojson_by_basin(
                path=str(river_display_geojson_path),
                mtime=river_mtime,
            )
            selector_key = alias_fn(str(resolved_river_basin_name or "").strip())
            river_fc = clone_featurecollection_for_patch(
                river_geojson_by_selector.get(selector_key, _empty_fc())
            )
            river_layer_name = "River network"

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

    base_map_cache_key = ("folium_base_map", *patched_cache_key, layer_name)
    base_map_cache = _folium_base_map_cache()
    cached_base_map = base_map_cache.get(base_map_cache_key)

    if cached_base_map is None:
        ctx = perf_section("map: build base folium map [cache_miss]") if perf_section is not None else nullcontext()
        with ctx:
            m = build_base_choropleth_map_with_geojson_layer(
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
            )
        try:
            base_map_cache[base_map_cache_key] = copy.deepcopy(m)
            while len(base_map_cache) > 12:
                base_map_cache.pop(next(iter(base_map_cache)))
        except Exception:
            base_map_cache.pop(base_map_cache_key, None)
    else:
        try:
            ctx = perf_section("map: clone cached base map [cache_hit]") if perf_section is not None else nullcontext()
            with ctx:
                m = copy.deepcopy(cached_base_map)
        except Exception:
            base_map_cache.pop(base_map_cache_key, None)
            ctx = perf_section("map: build base folium map [clone_fallback_rebuild]") if perf_section is not None else nullcontext()
            with ctx:
                m = build_base_choropleth_map_with_geojson_layer(
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
                )
            try:
                base_map_cache[base_map_cache_key] = copy.deepcopy(m)
                while len(base_map_cache) > 12:
                    base_map_cache.pop(next(iter(base_map_cache)))
            except Exception:
                base_map_cache.pop(base_map_cache_key, None)

    if reference_fc and list((reference_fc or {}).get("features", []) or []):
        ctx = perf_section("map: add related overlay") if perf_section is not None else nullcontext()
        with ctx:
            m = add_reference_overlay_layer(
                m,
                reference_fc=reference_fc,
                reference_level=reference_level,
                reference_layer_name=reference_layer_name,
            )

    if river_fc and list((river_fc or {}).get("features", []) or []):
        ctx = perf_section("map: add river overlay") if perf_section is not None else nullcontext()
        with ctx:
            m = add_river_overlay_layer(
                m,
                river_fc=river_fc,
                river_layer_name=river_layer_name,
            )
    return m

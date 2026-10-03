"""
Map-layer runtime helpers (Folium + GeoJSON patching) for IRT.

This module is app-layer (Folium is OK) but Streamlit-free: it builds a Folium
map object for the current selection by:
- loading a cached per-state FeatureCollection (geometry-only)
- patching per-feature properties from the current merged dataframe
- attaching a tooltip/highlight function
"""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from india_resilience_tool.config.constants import (
    SIMPLIFY_TOL_BASIN_RENDER,
    SIMPLIFY_TOL_SUBBASIN_RENDER,
)
from india_resilience_tool.data.admin_coverage import CoverageDiagnostics, compute_coverage_diagnostics


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


@dataclass(frozen=True)
class MapBuildResult:
    """Built map object plus authoritative render-time coverage diagnostics."""

    folium_map: Any
    coverage_diagnostics: Optional[CoverageDiagnostics]


def build_folium_map_for_selection(
    *,
    level: str,
    master_df: Optional[Any],
    merged: Any,
    display_gdf: Any,
    selected_state: str,
    selected_district: str,
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
    overlay_layers: tuple[Any, ...] = (),
    perf_section: Optional[Callable[[str], Any]] = None,
) -> MapBuildResult:
    from india_resilience_tool.app.geo_cache import (
        build_adm2_geojson_by_state,
        build_adm3_geojson_by_district,
        build_adm3_geojson_by_state,
        build_basin_geojson_all,
        build_basin_geojson_by_basin,
        build_subbasin_geojson_all,
        build_subbasin_geojson_by_basin,
    )
    from india_resilience_tool.app.views.map_view import (
        ADMIN_OUTLINE_DISTRICT_WEIGHT,
        SELECTED_UNIT_OUTLINE_COLOR,
        SELECTED_UNIT_OUTLINE_WEIGHT,
        add_admin_outline_layer,
        add_overlay_render_layers,
        add_reference_overlay_layer,
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
    )

    level_norm = str(level).strip().lower()
    if level_norm not in {"district", "block"}:
        level_norm = "district"

    # Load the level-appropriate FeatureCollection cache. The geometry-only
    # FeatureCollection is itself memoized in `geo_cache` keyed on (path, mtime,
    # tolerance); we only need to pick the right builder here.
    if level_norm == "block":
        adm3_mtime = float(adm3_geojson_path.stat().st_mtime)
        if selected_state != "All" and selected_district != "All":
            geojson_by_state = build_adm3_geojson_by_district(
                path=str(adm3_geojson_path),
                tolerance=simplify_tolerance_adm3,
                mtime=adm3_mtime,
            )
        else:
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
        level=level_norm,
        alias_fn=alias_fn,
    )

    prop_gdf = display_gdf if getattr(display_gdf, "empty", False) is False else merged
    feature_key_col = "__bkey" if level_norm == "block" else "__key"

    ctx = perf_section("map: build props map") if perf_section is not None else nullcontext()
    with ctx:
        props_map, _value_cols, _text_cols = build_props_map_from_gdf(
            prop_gdf,
            level=level_norm,
            alias_fn=alias_fn,
            feature_key_col=feature_key_col,
            metric_col=metric_col,
            map_value_col=map_value_col,
        )

    coverage_diagnostics = compute_coverage_diagnostics(
        feature_collection=base_fc,
        level=level_norm,
        alias_fn=alias_fn,
        feature_key_col=feature_key_col,
        props_map=props_map,
        master_df=master_df if getattr(master_df, "empty", False) is False else prop_gdf,
        map_value_col=map_value_col,
        metric_col=metric_col,
        baseline_col=baseline_col,
    )

    # Build the patched FC fresh every render. The previous session-state cache
    # was net negative: its SHA-1 prop-signature key cost more than the patch step
    # it gated, and the cache rarely hit because the render signature also flipped
    # on every scenario/period/stat change.
    ctx = perf_section("map: patch featurecollection") if perf_section is not None else nullcontext()
    with ctx:
        fc = clone_featurecollection_for_patch(base_fc)
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

    highlight_fn = None
    tooltip = None
    layer_name = "Blocks" if level_norm == "block" else "Districts"

    if hover_enabled:
        tooltip = build_geojson_tooltip(
            level=level_norm,
            map_mode=map_mode,
            has_baseline=bool(baseline_col and (baseline_col in getattr(merged, "columns", []))),
            rank_scope_label=rank_scope_label,
            metric_slug=metric_col,
        )

        # Hover: darken the outline only; the fill keeps its class color so the
        # hovered value stays readable (no yellow flash).
        highlight_fn = lambda _f: {
            "color": "#333333",
            "weight": 2.0,
        }

    # Build the base folium map fresh every render. A prior implementation cached
    # the built map in session_state and `copy.deepcopy`d it on retrieval; the deepcopy
    # of a folium.Map with embedded GeoJSON was consistently slower than rebuilding,
    # and the cache key (which included a content signature) rarely hit anyway.
    ctx = perf_section("map: build base folium map") if perf_section is not None else nullcontext()
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

    # Block view: district outlines sit between the block hairlines and the
    # state outlines so the admin hierarchy stays legible (CHG-0256).
    district_outline_fc = None
    if level_norm == "block" and selected_state != "All":
        try:
            adm2_mtime = float(adm2_geojson_path.stat().st_mtime)
            district_outline_by_state = ensure_geojson_by_state_has_all(
                build_adm2_geojson_by_state(
                    path=str(adm2_geojson_path),
                    tolerance=simplify_tolerance_adm2,
                    mtime=adm2_mtime,
                )
            )
            state_key = normalize_state_fn(selected_state) or "unknown"
            district_outline_fc = district_outline_by_state.get(state_key)
            if district_outline_fc and selected_district != "All":
                district_outline_fc = filter_fc_by_district(
                    district_outline_fc,
                    selected_district=selected_district,
                    level="district",
                    alias_fn=alias_fn,
                )
            add_admin_outline_layer(
                m,
                outline_fc=district_outline_fc,
                name="District outlines",
                weight=ADMIN_OUTLINE_DISTRICT_WEIGHT,
            )
        except Exception:
            district_outline_fc = None

    # Persistent emphasis outline around the selected district (CHG-0259) —
    # visually distinct from the transient hover highlight.
    if selected_district != "All":
        try:
            selected_outline_fc = district_outline_fc if level_norm == "block" else base_fc
            add_admin_outline_layer(
                m,
                outline_fc=selected_outline_fc,
                name="Selected district",
                color=SELECTED_UNIT_OUTLINE_COLOR,
                weight=SELECTED_UNIT_OUTLINE_WEIGHT,
                opacity=1.0,
            )
        except Exception:
            pass

    if reference_fc and list((reference_fc or {}).get("features", []) or []):
        ctx = perf_section("map: add related overlay") if perf_section is not None else nullcontext()
        with ctx:
            m = add_reference_overlay_layer(
                m,
                reference_fc=reference_fc,
                reference_level=reference_level,
                reference_layer_name=reference_layer_name,
            )

    if overlay_layers:
        ctx = perf_section("map: add reference overlays") if perf_section is not None else nullcontext()
        with ctx:
            m = add_overlay_render_layers(
                m,
                overlay_layers=tuple(overlay_layers),
            )
    return MapBuildResult(
        folium_map=m,
        coverage_diagnostics=coverage_diagnostics,
    )

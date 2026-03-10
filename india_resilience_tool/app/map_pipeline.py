"""
Map + rankings pipeline for the IRT Streamlit dashboard.

This module extracts the "merged dataframe → enriched columns → color scale →
folium map → rankings table" block from the legacy monolith so the main
`run_app()` orchestrator can stay small.

Notes:
    - This is app-layer code and may use Streamlit (widgets/warnings).
    - Scientific transforms (baseline/delta, rank/percentile/risk, tooltips)
      live in Streamlit-free modules under `analysis/` and `viz/`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

import numpy as np
import pandas as pd
import streamlit as st

from india_resilience_tool.analysis.map_enrichment import (
    add_current_baseline_delta,
    add_rank_percentile_risk,
    add_tooltip_strings,
)
from india_resilience_tool.analysis.metrics import risk_class_from_percentile
from india_resilience_tool.app.color_range_controls import compute_color_range_defaults
from india_resilience_tool.app.map_layer_runtime import build_folium_map_for_selection
from india_resilience_tool.data.master_columns import find_baseline_column_for_stat
from india_resilience_tool.data.merge import (
    get_or_build_merged_for_index_cached as _get_or_build_merged_for_index_cached,
)
from india_resilience_tool.utils.naming import alias
from india_resilience_tool.viz.colors import (
    apply_fillcolor_binned,
    build_vertical_binned_legend_block_html,
)
from india_resilience_tool.viz.tables import build_rankings_table_df as _build_rankings_table_df
from india_resilience_tool.viz.charts import period_display_label


@dataclass(frozen=True)
class MapArtifacts:
    merged: Any
    table_df: Any
    has_baseline: bool
    folium_map: Any
    legend_block_html: Optional[str]
    baseline_col: Optional[str]
    map_mode: str
    map_value_col: str
    pretty_metric_label: str
    cmap_name: str
    rank_scope_label: str


def _build_legend_title(varcfg: Mapping[str, Any]) -> str:
    """Return the minimal legend title text derived from metric units."""
    return str(varcfg.get("unit") or varcfg.get("units") or "").strip()


def _level_aware_merge(
    *,
    adm2: Any,
    adm3: Any,
    df: pd.DataFrame,
    variable_slug: str,
    master_csv_path: Path,
    level: str,
) -> Any:
    level_norm = str(level or "district").strip().lower()
    boundary_gdf = adm3 if level_norm in {"block", "basin", "sub_basin"} else adm2
    if boundary_gdf is None:
        raise ValueError(f"Boundary GeoDataFrame is required for level={level_norm!r}")

    return _get_or_build_merged_for_index_cached(
        boundary_gdf,
        df,
        slug=variable_slug,
        master_path=master_csv_path,
        session_state=st.session_state,
        alias_fn=alias,
        adm2_state_col="state_name",
        master_state_col="state",
        level=level_norm,
    )


def build_map_and_rankings(
    *,
    adm_level: str,
    adm1: Any,
    adm2: Any,
    adm3: Any,
    df: pd.DataFrame,
    master_csv_path: Path,
    variable_slug: str,
    varcfg: Mapping[str, Any],
    sel_metric: str,
    sel_scenario_display: str,
    sel_period: str,
    sel_stat: str,
    metric_col: str,
    map_mode: str,
    selected_state: str,
    selected_district: str,
    selected_block: str,
    selected_basin: str,
    selected_subbasin: str,
    spatial_family: str,
    crosswalk_overlay: Optional[Mapping[str, Any]],
    hover_enabled: bool,
    map_center: list[float],
    map_zoom: float,
    bounds_latlon: list[list[float]],
    pending_block_zoom: Optional[Mapping[str, str]],
    normalize_state_fn: Any,
    adm2_geojson_path: Path,
    adm3_geojson_path: Path,
    basin_geojson_path: Path,
    subbasin_geojson_path: Path,
    simplify_tol_adm2: float,
    simplify_tol_adm3: float,
    map_height: int,
    color_slider_placeholder: Any,
    perf_section: Any,
    render_perf_panel_safe: Any,
) -> MapArtifacts:
    """
    Build the full map + rankings artifacts for the current selection.

    This function preserves the legacy behavior: it warns and stops the Streamlit
    run when required inputs are missing.
    """
    level_norm = str(adm_level or "district").strip().lower()

    if level_norm in {"district", "block"} and "district" not in df.columns:
        st.error("Master CSV must contain a 'district' column.")
        render_perf_panel_safe()
        st.stop()

    if level_norm == "block":
        block_col_candidates = ["block", "block_name"]
        block_col = next((c for c in block_col_candidates if c in df.columns), None)
        if block_col is None:
            st.error("Block mode requires master CSV to contain a 'block' (or 'block_name') column.")
            render_perf_panel_safe()
            st.stop()

    if level_norm == "basin" and "basin_id" not in df.columns:
        st.error("Basin mode requires master CSV to contain a 'basin_id' column.")
        render_perf_panel_safe()
        st.stop()

    if level_norm == "sub_basin" and "subbasin_id" not in df.columns:
        st.error("Sub-basin mode requires master CSV to contain a 'subbasin_id' column.")
        render_perf_panel_safe()
        st.stop()

        if adm3 is None:
            st.error("Block mode requires ADM3 boundaries to be loaded.")
            render_perf_panel_safe()
            st.stop()

    with perf_section("merge: build merged gdf"):
        with st.spinner("Preparing merged geometries with CSV attributes..."):
            merged = _level_aware_merge(
                adm2=adm2,
                adm3=adm3,
                df=df,
                variable_slug=variable_slug,
                master_csv_path=master_csv_path,
                level=level_norm,
            )

    # Handle pending block zoom (needs merged GeoDataFrame with block geometries)
    if pending_block_zoom and "block_name" in getattr(merged, "columns", []):
        zoom_state = str(pending_block_zoom.get("state", "")).strip()
        zoom_district = str(pending_block_zoom.get("district", "")).strip()
        zoom_block = str(pending_block_zoom.get("block", "")).strip()

        try:
            block_mask = (
                (merged["state_name"].astype(str).str.strip().str.lower() == zoom_state.lower())
                & (merged["district_name"].astype(str).str.strip().str.lower() == zoom_district.lower())
                & (merged["block_name"].astype(str).str.strip().str.lower() == zoom_block.lower())
            )
            block_rows = merged[block_mask]
            if not block_rows.empty:
                block_geom = block_rows.iloc[0].geometry
                if block_geom is not None:
                    centroid = block_geom.centroid
                    map_center = [float(centroid.y), float(centroid.x)]
                    map_zoom = 11.0
                    st.session_state["map_center"] = map_center
                    st.session_state["map_zoom"] = map_zoom
        except Exception:
            pass

    # --- Baseline column for this metric + stat (used by map & table) ---
    baseline_col = find_baseline_column_for_stat(
        df.columns,
        base_metric=sel_metric,
        stat=sel_stat,
    )

    # --- Compute current/baseline/delta columns once (used by map + tooltip) ---
    with perf_section("map: compute current/baseline/delta"):
        merged = add_current_baseline_delta(
            merged,
            metric_col=metric_col,
            baseline_col=baseline_col,
        )

    # --- Decide which column the map will actually show ---
    map_value_col = metric_col  # default: absolute values

    if map_mode == "Change from 1990-2010 baseline":
        if baseline_col and (baseline_col in merged.columns):
            map_value_col = "_delta_abs"
        else:
            st.warning(
                "Baseline (historical 1990-2010) column not found for this metric/stat; "
                "showing absolute values instead."
            )
            map_mode = "Absolute value"
            st.session_state["map_mode"] = map_mode
            map_value_col = metric_col

    # --- Compute rank/percentile/risk class per state for tooltip quick-glance ---
    with perf_section("map: compute rank + risk class"):
        rank_higher_is_worse = bool(varcfg.get("rank_higher_is_worse", True))
        merged, rank_scope_label = add_rank_percentile_risk(
            merged,
            admin_level=level_norm,
            rank_higher_is_worse=rank_higher_is_worse,
            alias_fn=alias,
            risk_class_from_percentile_fn=risk_class_from_percentile,
        )

    with perf_section("map: build tooltip strings"):
        merged = add_tooltip_strings(merged, map_mode=map_mode)

    # Compute color scale defaults from *visible* units (matches the map filter),
    # then default to a robust p2–p98 range so outliers don't collapse the palette.
    scale_gdf = merged
    if selected_state != "All" and "state_name" in scale_gdf.columns:
        state_mask = scale_gdf["state_name"].astype(str).str.strip() == selected_state
        if not state_mask.any():
            state_mask = (
                scale_gdf["state_name"]
                .astype(str)
                .str.contains(selected_state, case=False, na=False)
            )
        scale_gdf = scale_gdf[state_mask]

    if selected_district != "All" and "district_name" in scale_gdf.columns:
        scale_gdf = scale_gdf[scale_gdf["district_name"].astype(str) == selected_district]

    if level_norm == "block" and selected_block != "All" and "block_name" in scale_gdf.columns:
        scale_gdf = scale_gdf[scale_gdf["block_name"].astype(str) == selected_block]
    if level_norm == "basin" and selected_basin != "All" and "basin_name" in scale_gdf.columns:
        scale_gdf = scale_gdf[scale_gdf["basin_name"].astype(str) == selected_basin]
    if level_norm == "sub_basin":
        if selected_basin != "All" and "basin_name" in scale_gdf.columns:
            scale_gdf = scale_gdf[scale_gdf["basin_name"].astype(str) == selected_basin]
        if (
            selected_subbasin != "All"
            and "subbasin_name" in scale_gdf.columns
        ):
            scale_gdf = scale_gdf[
                scale_gdf["subbasin_name"].astype(str) == selected_subbasin
            ]

    scale_vals = pd.to_numeric(
        scale_gdf.get(map_value_col, pd.Series([], dtype=float)), errors="coerce"
    )
    scale_vals = scale_vals.replace([np.inf, -np.inf], np.nan).dropna()

    if scale_vals.empty:
        st.error("No numeric values found for the current map selection.")
        render_perf_panel_safe()
        st.stop()

    data_min, data_max, vmin_default, vmax_default = compute_color_range_defaults(scale_vals)

    with st.sidebar:
        vmin_vmax = color_slider_placeholder.slider(
            "Color range (min → max)",
            min_value=float(data_min),
            max_value=float(data_max),
            value=(vmin_default, vmax_default),
            step=max((data_max - data_min) / 200.0, 0.001),
            key="color_range_slider",
        )

    vmin, vmax = float(vmin_vmax[0]), float(vmin_vmax[1])

    # Choose colormap: sequential for absolute, diverging for change
    if map_mode == "Change from 1990-2010 baseline":
        cmap_name = "RdBu_r"  # blue-negative, red-positive
        pretty_metric_label = (
            f"Δ {str(varcfg.get('label') or variable_slug)} vs 1990–2010 · "
            f"{sel_scenario_display} · {period_display_label(sel_period)} · {sel_stat}"
        )
    else:
        cmap_name = "Reds"
        pretty_metric_label = (
            f"{str(varcfg.get('label') or variable_slug)} · {sel_scenario_display} · {period_display_label(sel_period)} · {sel_stat}"
        )
    legend_title = _build_legend_title(varcfg)

    with perf_section("colors: apply_fillcolor_binned"):
        with st.spinner("Computing colors..."):
            merged = apply_fillcolor_binned(
                merged,
                map_value_col,
                vmin,
                vmax,
                cmap_name=cmap_name,
                nlevels=15,
            )

    # -------------------------
    # Build ranking table
    # -------------------------
    with perf_section("rank_table: build"):
        if level_norm == "sub_basin":
            unit_col = "subbasin_name"
        elif level_norm == "basin":
            unit_col = "basin_name"
        elif level_norm == "block":
            unit_col = "block_name"
        else:
            unit_col = "district_name"
        table_df, has_baseline = _build_rankings_table_df(
            merged,
            metric_col=metric_col,
            baseline_col=baseline_col,
            selected_state=selected_state,
            risk_class_from_percentile=risk_class_from_percentile,
            district_col=unit_col,
            state_col="state_name",
            aspirational_col="aspirational",
        )

    # Filter for map display (preserves legacy behavior: block selection does not
    # hide other blocks; it only affects the details panel).
    display_gdf = merged
    if selected_state != "All":
        state_mask = display_gdf["state_name"].astype(str).str.strip() == selected_state
        if not state_mask.any():
            state_mask = (
                display_gdf["state_name"]
                .astype(str)
                .str.contains(selected_state, case=False, na=False)
            )
        display_gdf = display_gdf[state_mask]

    if selected_district != "All":
        display_gdf = display_gdf[display_gdf["district_name"].astype(str) == selected_district]
    if level_norm == "basin" and selected_basin != "All":
        display_gdf = display_gdf[display_gdf["basin_name"].astype(str) == selected_basin]
    if level_norm == "sub_basin":
        if selected_basin != "All":
            display_gdf = display_gdf[
                display_gdf["basin_name"].astype(str) == selected_basin
            ]
        if selected_subbasin != "All":
            display_gdf = display_gdf[
                display_gdf["subbasin_name"].astype(str) == selected_subbasin
            ]

    with perf_section("map: GeoJSON serialize+add layer"):
        folium_map = build_folium_map_for_selection(
            level=level_norm,
            merged=merged,
            display_gdf=display_gdf,
            selected_state=selected_state,
            selected_district=selected_district,
            selected_basin=selected_basin,
            selected_subbasin=selected_subbasin,
            map_mode=map_mode,
            baseline_col=baseline_col,
            rank_scope_label=rank_scope_label,
            metric_col=metric_col,
            map_value_col=map_value_col,
            alias_fn=alias,
            normalize_state_fn=normalize_state_fn,
            adm1=adm1,
            map_center=map_center,
            map_zoom=map_zoom,
            bounds_latlon=bounds_latlon,
            hover_enabled=bool(hover_enabled),
            adm2_geojson_path=adm2_geojson_path,
            adm3_geojson_path=adm3_geojson_path,
            basin_geojson_path=basin_geojson_path,
            subbasin_geojson_path=subbasin_geojson_path,
            simplify_tolerance_adm2=simplify_tol_adm2,
            simplify_tolerance_adm3=simplify_tol_adm3,
            crosswalk_overlay=crosswalk_overlay,
        )

    legend_block_html = build_vertical_binned_legend_block_html(
        legend_title=legend_title,
        vmin=vmin,
        vmax=vmax,
        cmap_name=cmap_name,
        nlevels=15,
        nticks=5,
        include_zero_tick=True,
        map_height=map_height,
        bar_width_px=18,
    )

    return MapArtifacts(
        merged=merged,
        table_df=table_df,
        has_baseline=bool(has_baseline),
        folium_map=folium_map,
        legend_block_html=legend_block_html,
        baseline_col=baseline_col,
        map_mode=map_mode,
        map_value_col=map_value_col,
        pretty_metric_label=pretty_metric_label,
        cmap_name=cmap_name,
        rank_scope_label=rank_scope_label,
    )

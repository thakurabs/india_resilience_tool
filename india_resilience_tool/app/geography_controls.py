"""
Geography & analysis-focus controls for the Streamlit sidebar.

This module keeps the dashboard runtime smaller while preserving stable widget
keys and introducing a family-aware geography flow:
  - Admin -> state -> district -> block
  - Hydro -> basin -> sub-basin
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st

from india_resilience_tool.app.geo_cache import (
    load_admin_block_selector_index,
    list_available_states_from_processed_root_cached,
    load_basin_selector_index,
    load_hydro_subbasin_selector_index,
    load_subbasin_selector_index,
)
from india_resilience_tool.app.sidebar import render_analysis_mode_selector
from india_resilience_tool.config.variables import VARIABLES
from india_resilience_tool.data.adm3_loader import (
    get_blocks_for_district as _get_blocks_for_district,
)
from india_resilience_tool.data.optimized_bundle import optimized_context_path
from india_resilience_tool.utils.naming import alias


@dataclass(frozen=True)
class GeographyContext:
    analysis_mode: str
    analysis_ready: bool
    selected_state: str
    selected_district: str
    selected_block: str
    selected_basin: str
    selected_subbasin: str
    show_river_network: bool
    gdf_state_districts: Any


def _analysis_mode_options(spatial_family: str, admin_level: str) -> list[str]:
    """Return level-aware analysis-mode options for the sidebar selector."""
    family_norm = str(spatial_family).strip().lower()
    level_norm = str(admin_level).strip().lower()
    if family_norm == "hydro":
        if level_norm == "sub_basin":
            return ["Single sub-basin focus", "Multi-sub-basin portfolio"]
        return ["Single basin focus", "Multi-basin portfolio"]
    if level_norm == "block":
        return ["Single block focus", "Multi-block portfolio"]
    return ["Single district focus", "Multi-district portfolio"]


def _resolve_available_admin_states(processed_root: Optional[Path]) -> tuple[list[str], bool]:
    """Return state options and whether the processed root has usable admin data."""
    if processed_root is None:
        return ["All"], False

    processed_root_resolved = processed_root.resolve()
    discovered_states = list_available_states_from_processed_root_cached(str(processed_root_resolved))
    if (
        (not processed_root_resolved.exists())
        or (not processed_root_resolved.is_dir())
        or (not discovered_states)
    ):
        return ["All"], False
    return ["All"] + discovered_states, True


def _supported_admin_states_for_selected_metric() -> list[str]:
    slug = str(st.session_state.get("selected_var", "")).strip()
    if not slug or slug not in VARIABLES:
        return []
    supported = VARIABLES[slug].get("supported_admin_states") or []
    return [str(state).strip() for state in supported if str(state).strip()]


def _build_admin_geography(
    *,
    analysis_ready: bool,
    analysis_mode: str,
    processed_root: Optional[Path],
    adm1: Any,
    adm2: Any,
    adm3_geojson: Path,
    simplify_tol_adm3: float,
    admin_level: str,
) -> tuple[str, str, str, Any]:
    metric_ready_for_geography = processed_root is not None
    restricted_states = _supported_admin_states_for_selected_metric()

    if not metric_ready_for_geography:
        st.info(
            "Select a **Risk domain** and **Metric** in the ribbon above the map to load available states."
        )
        available_states = ["All"]
    else:
        available_states, has_available_data = _resolve_available_admin_states(processed_root)
        if restricted_states:
            available_states = restricted_states
        if not has_available_data:
            processed_root_resolved = processed_root.resolve()
            st.warning(
                f"No processed data found under IRT_PROCESSED_ROOT={processed_root_resolved}"
            )

    metric_slug = str(st.session_state.get("selected_var", "")).strip()
    previous_metric_slug = str(st.session_state.get("__admin_geography_metric_slug", "")).strip()
    restricted_states = _supported_admin_states_for_selected_metric()
    if restricted_states and metric_slug != previous_metric_slug:
        st.session_state["selected_state"] = restricted_states[0]
        st.session_state["selected_district"] = "All"
        st.session_state["selected_block"] = "All"
    st.session_state["__admin_geography_metric_slug"] = metric_slug

    if st.session_state.get("selected_state") not in available_states:
        st.session_state["selected_state"] = available_states[0]

    selected_state = st.selectbox(
        "State",
        options=available_states,
        index=available_states.index(st.session_state["selected_state"]),
        key="selected_state",
        disabled=(not analysis_ready) or (not metric_ready_for_geography),
    )

    if restricted_states:
        st.caption("This metric is currently available for Telangana only.")

    if selected_state == "All":
        gdf_state_districts = adm2.copy()
    else:
        sel_state_norm = selected_state.strip().lower()
        state_row = adm1[
            adm1["shapeName"].astype(str).str.strip().str.lower() == sel_state_norm
        ]
        if state_row.empty:
            state_row = adm1[
                adm1["shapeName"]
                .astype(str)
                .str.strip()
                .str.lower()
                .str.contains(sel_state_norm, na=False)
            ]

        if not state_row.empty:
            state_geom = state_row.iloc[0].geometry
            try:
                gdf_state_districts = adm2[
                    adm2.geometry.within(state_geom.buffer(0.001))
                ].copy()
            except Exception:
                gdf_state_districts = adm2[
                    adm2.geometry.centroid.within(state_geom.buffer(0.001))
                ].copy()
        else:
            gdf_state_districts = pd.DataFrame()

        if getattr(gdf_state_districts, "empty", True):
            gdf_state_districts = adm2[
                adm2["state_name"]
                .astype(str)
                .str.strip()
                .str.lower()
                .str.contains(sel_state_norm, na=False)
            ].copy()

    districts = ["All"] + sorted(
        gdf_state_districts["district_name"].astype(str).unique().tolist()
    )
    if st.session_state.get("selected_district") not in districts:
        st.session_state["selected_district"] = "All"

    admin_level_from_state = st.session_state.get("admin_level", admin_level)
    if "Multi" in analysis_mode and str(admin_level_from_state) != "block":
        st.session_state["selected_district"] = "All"

    selected_district = st.selectbox(
        "District",
        options=districts,
        index=districts.index(st.session_state["selected_district"]),
        key="selected_district",
        disabled=not analysis_ready,
    )

    selected_block = "All"
    if str(admin_level_from_state) == "block":
        block_options = ["All"]
        if selected_district != "All":
            data_dir = adm3_geojson.parent
            block_index_path = optimized_context_path("admin_block_index.parquet", data_dir=data_dir)
            if block_index_path.exists():
                selector_index = load_admin_block_selector_index(str(block_index_path))
                selector_key = f"{alias(selected_state)}|{alias(selected_district)}"
                block_options = ["All"] + [str(v) for v in selector_index.get("blocks_by_selector", {}).get(selector_key, [])]
            else:
                if not adm3_geojson.exists():
                    st.error(
                        f"ADM3 geojson not found at {adm3_geojson}. Please provide block_4326.geojson."
                    )
                    st.stop()
                from india_resilience_tool.app.geo_cache import load_local_adm3

                adm3_sidebar = load_local_adm3(str(adm3_geojson), tolerance=simplify_tol_adm3)
                try:
                    blocks = _get_blocks_for_district(
                        adm3_sidebar,
                        selected_state,
                        selected_district,
                        normalize_fn=alias,
                    )
                    block_options = ["All"] + sorted(
                        [str(b).strip() for b in blocks if str(b).strip()]
                    )
                except Exception:
                    block_options = ["All"]
        else:
            st.caption("Select a district to see blocks")

        if st.session_state.get("selected_block") not in block_options:
            st.session_state["selected_block"] = "All"

        selected_block = st.selectbox(
            "Block",
            options=block_options,
            index=block_options.index(st.session_state["selected_block"]),
            key="selected_block",
            disabled=not analysis_ready,
        )
    else:
        st.session_state["selected_block"] = "All"

    st.session_state["selected_basin"] = "All"
    st.session_state["selected_subbasin"] = "All"

    return selected_state, selected_district, selected_block, gdf_state_districts


def _build_hydro_geography(
    *,
    analysis_ready: bool,
    admin_level: str,
    basins_geojson: Path,
    subbasins_geojson: Path,
) -> tuple[str, str]:
    if not basins_geojson.exists():
        st.error(
            f"Hydro basin geojson not found at {basins_geojson}. Please provide basins.geojson."
        )
        st.stop()
    if not subbasins_geojson.exists():
        st.error(
            f"Hydro sub-basin geojson not found at {subbasins_geojson}. Please provide subbasins.geojson."
        )
        st.stop()

    level_norm = str(admin_level).strip().lower()
    data_dir = basins_geojson.parent
    hydro_index_path = optimized_context_path("hydro_subbasin_index.parquet", data_dir=data_dir)
    if hydro_index_path.exists():
        selector_index = load_hydro_subbasin_selector_index(str(hydro_index_path))
    else:
        selector_index = (
            load_subbasin_selector_index(str(subbasins_geojson))
            if level_norm == "sub_basin"
            else load_basin_selector_index(str(basins_geojson))
        )

    basin_names = selector_index.get("basin_names", [])
    subbasins_by_basin = selector_index.get("subbasins_by_basin", {})
    subbasins_all = selector_index.get("subbasins_all", [])

    basin_options = ["All"] + [str(v) for v in basin_names]
    if st.session_state.get("selected_basin") not in basin_options:
        st.session_state["selected_basin"] = "All"

    selected_basin = st.selectbox(
        "Basin",
        options=basin_options,
        index=basin_options.index(st.session_state["selected_basin"]),
        key="selected_basin",
        disabled=not analysis_ready,
    )

    subbasin_options = ["All"]
    if selected_basin != "All":
        subbasin_options = ["All"] + [str(v) for v in subbasins_by_basin.get(str(selected_basin).strip(), [])]
    elif level_norm == "sub_basin":
        subbasin_options = ["All"] + [str(v) for v in subbasins_all]

    if st.session_state.get("selected_subbasin") not in subbasin_options:
        st.session_state["selected_subbasin"] = "All"

    if level_norm == "sub_basin":
        selected_subbasin = st.selectbox(
            "Sub-basin",
            options=subbasin_options,
            index=subbasin_options.index(st.session_state["selected_subbasin"]),
            key="selected_subbasin",
            disabled=not analysis_ready,
        )
    else:
        selected_subbasin = "All"
        st.session_state["selected_subbasin"] = "All"

    st.session_state["selected_state"] = "All"
    st.session_state["selected_district"] = "All"
    st.session_state["selected_block"] = "All"

    return selected_basin, selected_subbasin


def render_geography_and_analysis_focus(
    *,
    state_placeholder: Any,
    spatial_family: str,
    admin_level: str,
    processed_root: Optional[Path],
    sel_placeholder: str,
    view_map: str,
    view_rankings: str,
    adm1: Any,
    adm2: Any,
    adm3_geojson: Path,
    basins_geojson: Path,
    subbasins_geojson: Path,
    river_display_geojson: Path,
    simplify_tol_adm3: float,
) -> GeographyContext:
    """Render the geography controls and return the active selection context."""
    with state_placeholder.container():
        with st.expander("Geography & analysis focus", expanded=True):
            family_norm = str(spatial_family).strip().lower()
            level_norm = str(admin_level).strip().lower()
            analysis_options = _analysis_mode_options(family_norm, level_norm)
            analysis_mode = render_analysis_mode_selector(
                label="Analysis focus",
                options=analysis_options,
                placeholder=sel_placeholder,
                index=0,
                help_text=(
                    "Choose a single-unit focus to explore one unit at a time, "
                    "or portfolio mode to build and compare a set of units."
                ),
                label_visibility="collapsed",
                use_markdown_header=True,
                level=admin_level,
            )

            analysis_ready = analysis_mode != sel_placeholder
            if not analysis_ready:
                st.info("Select **Analysis focus** above to enable geography and map settings.")

            prev_mode_for_jump_reset = st.session_state.get(
                "_analysis_mode_prev_for_jump_reset", analysis_mode
            )
            if prev_mode_for_jump_reset != analysis_mode:
                st.session_state["_analysis_mode_prev_for_jump_reset"] = analysis_mode
                st.session_state["portfolio_build_route"] = None
                st.session_state["jump_to_rankings"] = False
                st.session_state["jump_to_map"] = False

            unit_singular = "block" if admin_level == "block" else "district"
            unit_plural = "blocks" if admin_level == "block" else "districts"
            if str(spatial_family).strip().lower() == "hydro":
                unit_singular = "sub-basin" if admin_level == "sub_basin" else "basin"
                unit_plural = "sub-basins" if admin_level == "sub_basin" else "basins"

            if analysis_mode == sel_placeholder:
                st.caption("Select an analysis focus to continue.")
            elif "Single" in analysis_mode:
                st.caption(
                    f"Inspect one {unit_singular} at a time. Use the dropdowns below "
                    f"to pick which {unit_singular} you want to explore in detail."
                )
            else:
                st.markdown(
                    f"<div style='font-size:0.9rem; margin-top:0.25rem; margin-bottom:0.1rem;'>"
                    f"In <strong>Multi-{unit_singular} portfolio</strong> mode you build a set of {unit_plural} "
                    f"for comparison. {unit_plural.title()} are added from the <em>{view_map}</em>, the "
                    f"<em>{view_rankings}</em>, or from saved point locations. "
                    f"</div>",
                    unsafe_allow_html=True,
                )

            selected_state = "All"
            selected_district = "All"
            selected_block = "All"
            selected_basin = "All"
            selected_subbasin = "All"
            show_river_network = bool(st.session_state.get("show_river_network", False))
            gdf_state_districts = adm2.copy()

            if str(spatial_family).strip().lower() == "hydro":
                selected_basin, selected_subbasin = _build_hydro_geography(
                    analysis_ready=analysis_ready,
                    admin_level=admin_level,
                    basins_geojson=basins_geojson,
                    subbasins_geojson=subbasins_geojson,
                )
                if selected_basin == "All":
                    st.session_state["show_river_network"] = False
                show_river_network = st.checkbox(
                    "Show river network",
                    key="show_river_network",
                    value=bool(st.session_state.get("show_river_network", False)),
                    disabled=(not analysis_ready) or (selected_basin == "All"),
                    help=(
                        "Show cleaned river lines for the selected basin or sub-basin. "
                        "Available only when a specific basin is selected."
                    ),
                )
                if not river_display_geojson.exists():
                    st.session_state["show_river_network"] = False
                    show_river_network = False
                    st.caption("River overlay unavailable: river_network_display.geojson not found.")
            else:
                (
                    selected_state,
                    selected_district,
                    selected_block,
                    gdf_state_districts,
                ) = _build_admin_geography(
                    analysis_ready=analysis_ready,
                    analysis_mode=analysis_mode,
                    processed_root=processed_root,
                    adm1=adm1,
                    adm2=adm2,
                    adm3_geojson=adm3_geojson,
                    simplify_tol_adm3=simplify_tol_adm3,
                    admin_level=admin_level,
                )
                st.session_state["show_river_network"] = False
                show_river_network = False

    return GeographyContext(
        analysis_mode=analysis_mode,
        analysis_ready=analysis_ready,
        selected_state=str(selected_state or "All"),
        selected_district=str(selected_district or "All"),
        selected_block=str(selected_block or "All"),
        selected_basin=str(selected_basin or "All"),
        selected_subbasin=str(selected_subbasin or "All"),
        show_river_network=bool(show_river_network),
        gdf_state_districts=gdf_state_districts,
    )

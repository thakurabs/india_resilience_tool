"""
Geography & analysis-focus controls for the Streamlit sidebar.

This module exists to keep the dashboard runtime smaller while preserving the
legacy dashboard behavior and widget keys.

Widget keys (must remain stable):
- analysis_mode
- selected_state
- selected_district
- selected_block
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st

from india_resilience_tool.data.adm3_loader import get_blocks_for_district as _get_blocks_for_district
from india_resilience_tool.utils.naming import alias

from india_resilience_tool.app.geo_cache import (
    list_available_states_from_processed_root_cached,
    load_local_adm3,
)
from india_resilience_tool.app.sidebar import render_analysis_mode_selector


@dataclass(frozen=True)
class GeographyContext:
    analysis_mode: str
    analysis_ready: bool
    selected_state: str
    selected_district: str
    selected_block: str
    gdf_state_districts: Any


def render_geography_and_analysis_focus(
    *,
    state_placeholder: Any,
    admin_level: str,
    processed_root: Optional[Path],
    sel_placeholder: str,
    view_map: str,
    view_rankings: str,
    adm1: Any,
    adm2: Any,
    adm3_geojson: Path,
    simplify_tol_adm3: float,
) -> GeographyContext:
    """
    Render the legacy "Geography & analysis focus" expander and return selections.

    Notes:
        - This function intentionally preserves the legacy behavior and session_state
          interactions, including portfolio-mode district freezing and selection
          validation.
        - `adm1`/`adm2` are GeoDataFrames in practice, but are typed as Any here to
          avoid importing geopandas types at import-time for lightweight tooling.
    """
    with state_placeholder.container():
        with st.expander("Geography & analysis focus", expanded=True):
            # Option A UX: disable downstream geography widgets until Analysis focus is chosen.
            # Render Analysis focus FIRST so the user understands why controls are disabled.

            # ---- Step 0: Analysis focus (single vs portfolio; labels depend on admin_level) ----
            analysis_options = (
                ["Single block focus", "Multi-block portfolio"]
                if admin_level == "block"
                else ["Single district focus", "Multi-district portfolio"]
            )

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

            # Reset jump flags when switching analysis focus modes (keep behavior stable).
            prev_mode_for_jump_reset = st.session_state.get(
                "_analysis_mode_prev_for_jump_reset", analysis_mode
            )
            if prev_mode_for_jump_reset != analysis_mode:
                st.session_state["_analysis_mode_prev_for_jump_reset"] = analysis_mode
                st.session_state["portfolio_build_route"] = None
                st.session_state["jump_to_rankings"] = False
                st.session_state["jump_to_map"] = False

            # Brief helper text so the mode explains itself (level-aware)
            unit_singular = "block" if admin_level == "block" else "district"
            unit_plural = "blocks" if admin_level == "block" else "districts"

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

            # ---- Step 1: State selection (data-driven from processed root) ----

            metric_ready_for_geography = processed_root is not None

            if not metric_ready_for_geography:
                st.info(
                    "Select a **Risk domain** and **Metric** in the ribbon above the map to load available states."
                )
                processed_root_resolved = None
                available_states = ["All"]
            else:
                processed_root_resolved = processed_root.resolve()
                available_states = list_available_states_from_processed_root_cached(
                    str(processed_root_resolved)
                )

                if (
                    (not processed_root_resolved.exists())
                    or (not processed_root_resolved.is_dir())
                    or (not available_states)
                ):
                    st.warning(
                        f"No processed data found under IRT_PROCESSED_ROOT={processed_root_resolved}"
                    )
                    available_states = ["All"]

            prev_state = st.session_state.get("selected_state")

            if prev_state and prev_state not in available_states:
                st.warning(
                    f"Previously selected state '{prev_state}' is no longer available in processed data. "
                    f"Resetting to '{available_states[0]}'."
                )

            if (
                "selected_state" not in st.session_state
                or st.session_state["selected_state"] not in available_states
            ):
                st.session_state["selected_state"] = available_states[0]

            selected_state = st.selectbox(
                "State",
                options=available_states,
                index=available_states.index(st.session_state["selected_state"]),
                key="selected_state",
                disabled=(not analysis_ready) or (not metric_ready_for_geography),
            )

            # Build per-state district GeoDataFrame
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

                    if gdf_state_districts.empty:
                        gdf_state_districts = adm2[
                            adm2["state_name"]
                            .astype(str)
                            .str.strip()
                            .str.lower()
                            .str.contains(sel_state_norm, na=False)
                        ].copy()
                else:
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

            # Ensure we always have a valid district in session state
            if (
                "selected_district" not in st.session_state
                or st.session_state["selected_district"] not in districts
            ):
                st.session_state["selected_district"] = "All"

            # ---- Step 2: District selection (always shown, required before block in block mode) ----
            # Ensure we always have a valid district in session state
            if (
                "selected_district" not in st.session_state
                or st.session_state["selected_district"] not in districts
            ):
                st.session_state["selected_district"] = "All"

            # Portfolio mode behavior for district selection:
            # - In district-level portfolio mode: freeze district to "All"
            # - In block-level portfolio mode: allow district selection (needed to navigate blocks)
            # Check this BEFORE creating the widget to avoid Streamlit session state errors
            _current_analysis_mode = st.session_state.get("analysis_mode", "Single district focus")
            admin_level_from_state = st.session_state.get("admin_level", admin_level)
            if "Multi" in _current_analysis_mode and str(admin_level_from_state) != "block":
                st.session_state["selected_district"] = "All"

            selected_district = st.selectbox(
                "District",
                options=districts,
                index=districts.index(st.session_state["selected_district"]),
                key="selected_district",
                disabled=not analysis_ready,
            )

            # ---- Step 3: Block selection (only when admin_level == block AND district selected) ----
            selected_block = "All"
            if str(admin_level_from_state) == "block":
                if not adm3_geojson.exists():
                    st.error(
                        f"ADM3 geojson not found at {adm3_geojson}. Please provide block_4326.geojson."
                    )
                    st.stop()

                # Load ADM3 boundaries for block selection
                adm3_sidebar = load_local_adm3(str(adm3_geojson), tolerance=simplify_tol_adm3)

                block_options = ["All"]
                if selected_district != "All":
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

                    if (
                        "selected_block" not in st.session_state
                        or st.session_state["selected_block"] not in block_options
                    ):
                        st.session_state["selected_block"] = "All"

                    selected_block = st.selectbox(
                        "Block",
                        options=block_options,
                        index=block_options.index(st.session_state.get("selected_block", "All")),
                        key="selected_block",
                        disabled=not analysis_ready,
                    )
                else:
                    # Show disabled/info when district not selected
                    if selected_district == "All":
                        st.caption("Select a district to see blocks")
                    st.session_state["selected_block"] = "All"
            else:
                st.session_state.pop("selected_block", None)

            # Note: Portfolio mode behavior for district selection is now handled BEFORE
            # the selectbox widget is created to avoid Streamlit session state modification errors.
            # In district-level portfolio mode: district is frozen to "All"
            # In block-level portfolio mode: district selection is allowed (needed to navigate blocks)
            if "Multi" in analysis_mode and str(admin_level_from_state) != "block":
                # Just update the local variable; session_state was already set before widget
                selected_district = "All"

    # Normalize any potential None values to strings (defensive)
    if selected_state is None or (isinstance(selected_state, float) and pd.isna(selected_state)):
        selected_state = "All"
    if selected_district is None or (isinstance(selected_district, float) and pd.isna(selected_district)):
        selected_district = "All"
    if selected_block is None or (isinstance(selected_block, float) and pd.isna(selected_block)):
        selected_block = "All"

    return GeographyContext(
        analysis_mode=analysis_mode,
        analysis_ready=analysis_ready,
        selected_state=str(selected_state),
        selected_district=str(selected_district),
        selected_block=str(selected_block),
        gdf_state_districts=gdf_state_districts,
    )

"""
Geography & analysis-focus controls for the Streamlit sidebar.

This module keeps the dashboard runtime smaller while preserving stable widget
keys for the admin geography flow:
  - Admin -> state -> district -> block
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st

from india_resilience_tool.app.geo_cache import (
    load_admin_block_selector_index,
    load_local_adm2,
    list_available_states_from_processed_root_cached,
)
from india_resilience_tool.app.overlays import (
    OverlayControlState,
    ensure_overlay_session_state,
    resolve_overlay_control_states,
)
from india_resilience_tool.app.sidebar import render_analysis_mode_selector
from india_resilience_tool.config.variables import VARIABLES
from india_resilience_tool.data.adm3_loader import (
    get_blocks_for_district as _get_blocks_for_district,
)
from india_resilience_tool.data.optimized_bundle import optimized_context_path, optimized_geometry_path
from india_resilience_tool.utils.naming import alias


@dataclass(frozen=True)
class GeographyContext:
    analysis_mode: str
    analysis_ready: bool
    selected_state: str
    selected_district: str
    selected_block: str
    overlay_states: dict[str, OverlayControlState]
    gdf_state_districts: Any


def _analysis_mode_options(spatial_family: str, admin_level: str) -> list[str]:
    """Return level-aware analysis-mode options for the sidebar selector."""
    level_norm = str(admin_level).strip().lower()
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


def _empty_adm2_like(adm2: Any) -> Any:
    """Return an empty district frame without dropping GeoDataFrame metadata."""
    if adm2 is None:
        return pd.DataFrame(columns=["district_name", "state_name"])
    return adm2.iloc[0:0].copy()


def _valid_state_name_mask(values: pd.Series) -> pd.Series:
    """Return rows with a concrete, non-placeholder state name."""
    text = values.astype("string").str.strip()
    return text.notna() & (text != "") & ~text.str.lower().isin({"<na>", "nan", "none", "unknown"})


def _resolve_selected_state_geometry(adm1: Any, selected_state: str) -> Any:
    """Return the exact ADM1 geometry for ``selected_state``, or ``None``."""
    if adm1 is None or getattr(adm1, "empty", True):
        return None
    selected_key = alias(selected_state)
    for col in ("state_name", "shapeName"):
        if col not in adm1.columns:
            continue
        mask = adm1[col].astype(str).map(alias) == selected_key
        matches = adm1[mask]
        if not matches.empty:
            return matches.iloc[0].geometry
    return None


def _districts_for_selected_state(adm2: Any, adm1: Any, selected_state: str) -> Any:
    """
    Resolve district options for a state using exact state attribution first.

    Blank/Unknown rows are supplemented with a representative-point test against
    the selected full-fidelity state geometry. If exact attribution finds no
    districts at all, the same representative-point test is allowed as a full
    fallback so older artifacts still produce usable options. Substring state
    matching is intentionally not used.
    """
    if adm2 is None or getattr(adm2, "empty", True):
        return _empty_adm2_like(adm2)
    if "district_name" not in adm2.columns or "state_name" not in adm2.columns:
        return _empty_adm2_like(adm2)

    selected_key = alias(selected_state)
    state_values = adm2["state_name"].astype("string").str.strip()
    valid_state = _valid_state_name_mask(adm2["state_name"])
    exact = adm2[valid_state & (state_values.map(alias) == selected_key)].copy()

    state_geom = _resolve_selected_state_geometry(adm1, selected_state)
    if state_geom is None:
        return exact if not exact.empty else _empty_adm2_like(adm2)

    try:
        point_mask = adm2.geometry.representative_point().within(state_geom.buffer(0.001))
    except Exception:
        return exact if not exact.empty else _empty_adm2_like(adm2)

    if exact.empty:
        return adm2[point_mask].copy()

    supplement = adm2[(~valid_state) & point_mask].copy()
    if supplement.empty:
        return exact
    return pd.concat([exact, supplement], axis=0).drop_duplicates(subset=["district_name"]).copy()


def _load_state_adm2_shard_for_sidebar(*, selected_state: str, data_dir: Path) -> Any:
    """Load an optimized district shard for sidebar options when full ADM2 is deferred."""
    shard_path = optimized_geometry_path(level="district", state=selected_state, data_dir=data_dir)
    if not shard_path.exists():
        return None
    return load_local_adm2(str(shard_path), cache_version="adm2_state_v2")


def _build_admin_geography(
    *,
    analysis_ready: bool,
    analysis_mode: str,
    processed_root: Optional[Path],
    adm1: Any,
    adm2: Optional[Any],
    adm3_geojson: Path,
    data_dir: Path,
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

    adm2_for_options = adm2
    if adm2_for_options is None and selected_state != "All":
        adm2_for_options = _load_state_adm2_shard_for_sidebar(
            selected_state=selected_state,
            data_dir=data_dir,
        )

    if adm2_for_options is None:
        gdf_state_districts = _empty_adm2_like(adm2_for_options)
    elif selected_state == "All":
        gdf_state_districts = adm2_for_options.copy()
    else:
        gdf_state_districts = _districts_for_selected_state(adm2_for_options, adm1, selected_state)
        if getattr(gdf_state_districts, "empty", True):
            shard_adm2 = _load_state_adm2_shard_for_sidebar(
                selected_state=selected_state,
                data_dir=data_dir,
            )
            if shard_adm2 is not None:
                gdf_state_districts = _districts_for_selected_state(shard_adm2, adm1, selected_state)

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
    adm2: Optional[Any],
    adm3_geojson: Path,
    river_display_geojson: Path,
    data_dir: Path,
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
            gdf_state_districts = (
                adm2.copy()
                if adm2 is not None
                else pd.DataFrame(columns=["district_name", "state_name"])
            )
            ensure_overlay_session_state(st.session_state)

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
                data_dir=data_dir,
            )

            overlay_states = resolve_overlay_control_states(
                session_state=st.session_state,
                admin_level=admin_level,
                selected_state=selected_state,
                river_display_geojson_path=river_display_geojson,
                data_dir=data_dir,
                selected_district=selected_district,
            )
            visible_overlay_states = [state for state in overlay_states.values() if state.visible]
            if visible_overlay_states:
                st.markdown("#### Reference overlays")
                for overlay_state in visible_overlay_states:
                    st.checkbox(
                        overlay_state.label,
                        key=overlay_state.enabled_key,
                        disabled=(not analysis_ready) or (not overlay_state.available),
                    )
                    if overlay_state.category_key and overlay_state.category_choices:
                        current_category = overlay_state.selected_category or overlay_state.category_choices[0]
                        index = (
                            overlay_state.category_choices.index(current_category)
                            if current_category in overlay_state.category_choices
                            else 0
                        )
                        st.selectbox(
                            f"{overlay_state.label} category",
                            options=list(overlay_state.category_choices),
                            index=index,
                            key=overlay_state.category_key,
                            disabled=(not analysis_ready) or (not overlay_state.available),
                        )
                    st.slider(
                        overlay_state.slider_label,
                        min_value=0,
                        max_value=100,
                        value=int(overlay_state.opacity_pct),
                        step=5,
                        key=overlay_state.opacity_key,
                        disabled=(not analysis_ready) or (not overlay_state.available),
                    )
                    if overlay_state.unavailable_caption:
                        st.caption(overlay_state.unavailable_caption)
                    if overlay_state.availability_reason:
                        st.caption(overlay_state.availability_reason)
                overlay_states = {
                    overlay_id: OverlayControlState(
                        overlay_id=state.overlay_id,
                        label=state.label,
                        slider_label=state.slider_label,
                        enabled_key=state.enabled_key,
                        opacity_key=state.opacity_key,
                        category_key=state.category_key,
                        selected_category=(
                            str(st.session_state.get(state.category_key, state.selected_category))
                            if state.category_key
                            else None
                        ),
                        category_choices=state.category_choices,
                        visible=state.visible,
                        available=state.available,
                        enabled=bool(st.session_state.get(state.enabled_key, False)) and state.available,
                        opacity_pct=max(0, min(100, int(st.session_state.get(state.opacity_key, state.opacity_pct)))),
                        unavailable_caption=state.unavailable_caption,
                        availability_reason=state.availability_reason,
                    )
                    for overlay_id, state in overlay_states.items()
                }

    return GeographyContext(
        analysis_mode=analysis_mode,
        analysis_ready=analysis_ready,
        selected_state=str(selected_state or "All"),
        selected_district=str(selected_district or "All"),
        selected_block=str(selected_block or "All"),
        overlay_states=overlay_states,
        gdf_state_districts=gdf_state_districts,
    )

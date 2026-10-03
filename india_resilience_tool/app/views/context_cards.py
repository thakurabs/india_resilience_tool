"""Compact admin context cards for the right-side Climate Profile panel."""

from __future__ import annotations

import hashlib
import math
from typing import Any, Mapping, Optional

import pandas as pd

from india_resilience_tool.data.hydro_summary import parse_hydro_intersections


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _as_float(value: object) -> Optional[float]:
    if _is_missing(value):
        return None
    try:
        v = float(value)
    except Exception:
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _text(value: object) -> str:
    if _is_missing(value):
        return ""
    return str(value).strip()


def _format_population(value: object) -> str:
    v = _as_float(value)
    if v is None:
        return "Not available"
    if abs(v) >= 1_000_000:
        return f"{v / 1_000_000:.2f} million".replace(".00 million", " million")
    return f"{v:,.0f}"


def _format_count(value: object) -> str:
    v = _as_float(value)
    if v is None:
        return "Not available"
    return f"{v:,.0f}"


def _format_rate(value: object) -> str:
    v = _as_float(value)
    if v is None:
        return "Not available"
    return f"{v:,.1f}".rstrip("0").rstrip(".")


def _format_pct(value: object, *, fraction: bool = False, decimals: int = 1) -> str:
    v = _as_float(value)
    if v is None:
        return "Not available"
    if fraction:
        v *= 100.0
    if abs(v - round(v)) < 0.05:
        return f"{v:.0f}%"
    return f"{v:.{decimals}f}%"


def _button_key(prefix: str, *parts: object) -> str:
    raw = "|".join(str(p or "") for p in parts)
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def _toggle_overlay(payload: Mapping[str, Any]) -> None:
    import streamlit as st

    current = st.session_state.get("active_hydro_boundary_overlay")
    if isinstance(current, dict) and current == dict(payload):
        st.session_state["active_hydro_boundary_overlay"] = None
    else:
        st.session_state["active_hydro_boundary_overlay"] = dict(payload)
    st.rerun()


def render_exposure_snapshot_card(
    *,
    exposure_summary_row: Mapping[str, Any],
    level: str,
) -> None:
    """Render population, facilities, built-up, and agricultural LULC exposure."""
    import streamlit as st

    pop = exposure_summary_row.get("pop_2020")
    built_up_area = exposure_summary_row.get("built_up_area_km2")
    built_up_share = exposure_summary_row.get("built_up_area_share_pct")
    lulc_agri_area = exposure_summary_row.get("lulc_agri_area_km2")
    lulc_agri_share = exposure_summary_row.get("lulc_agri_share_pct")
    rural_total = _as_float(exposure_summary_row.get("rural_facilities_total_count"))
    has_population = _as_float(pop) is not None
    has_built_up = _as_float(built_up_area) is not None or _as_float(built_up_share) is not None
    has_lulc_agri = _as_float(lulc_agri_area) is not None or _as_float(lulc_agri_share) is not None
    if not (has_population or has_built_up or has_lulc_agri or rural_total is not None):
        return

    level_norm = str(level or "district").strip().lower()
    parent_label = "district" if level_norm == "block" else "state"
    share = exposure_summary_row.get("population_share_parent_pct")

    with st.expander("Exposure Snapshot", expanded=True):
        if has_population:
            c1, c2 = st.columns(2)
            with c1:
                st.caption("Population")
                st.markdown(f"**{_format_population(pop)}**")
            with c2:
                st.caption("Share of parent geography")
                share_txt = _format_pct(share, fraction=False)
                if share_txt == "Not available":
                    st.markdown("**Not available**")
                else:
                    st.markdown(f"**{share_txt}** of {parent_label} population")

        if rural_total is not None:
            st.divider()
            r1, r2 = st.columns(2)
            with r1:
                st.caption("Rural facilities")
                st.markdown(f"**{_format_count(rural_total)}**")
            with r2:
                st.caption("Rural facilities per 100k people")
                st.markdown(f"**{_format_rate(exposure_summary_row.get('rural_facilities_total_count_per_100k'))}**")

            category_values = [
                ("Agro", exposure_summary_row.get("rural_facilities_agro_count")),
                ("Education", exposure_summary_row.get("rural_facilities_education_count")),
                ("Health", exposure_summary_row.get("rural_facilities_health_count")),
                ("Service", exposure_summary_row.get("rural_facilities_service_count")),
            ]
            if any(_as_float(value) is not None for _, value in category_values):
                cols = st.columns(4)
                for col, (label, value) in zip(cols, category_values):
                    with col:
                        st.caption(label)
                        st.markdown(f"**{_format_count(value)}**")

        if has_built_up:
            st.divider()
            b1, b2 = st.columns(2)
            with b1:
                st.caption("Built-up area")
                area_val = _as_float(built_up_area)
                st.markdown("**Not available**" if area_val is None else f"**{area_val:,.1f} km²**")
            with b2:
                st.caption("Built-up area share")
                st.markdown(f"**{_format_pct(built_up_share, fraction=False)}**")

        if has_lulc_agri:
            st.divider()
            l1, l2 = st.columns(2)
            with l1:
                st.caption("Agricultural LULC area")
                area_val = _as_float(lulc_agri_area)
                st.markdown("**Not available**" if area_val is None else f"**{area_val:,.1f} km²**")
            with l2:
                st.caption("Agricultural LULC share")
                st.markdown(f"**{_format_pct(lulc_agri_share, fraction=False)}**")


def _hydro_chip(
    *,
    label: str,
    pct_text: str,
    payload: Mapping[str, Any],
    disabled: bool = False,
) -> None:
    import streamlit as st

    active = st.session_state.get("active_hydro_boundary_overlay")
    is_active = isinstance(active, dict) and active == dict(payload)
    button_label = f"● {label}" if is_active else label
    c1, c2 = st.columns([0.72, 0.28], gap="small")
    with c1:
        if st.button(
            button_label,
            key=_button_key(
                "hydro_boundary",
                payload.get("admin_key"),
                payload.get("hydro_level"),
                payload.get("basin_id"),
                payload.get("subbasin_id"),
            ),
            help="Show/clear this hydro boundary on the map",
            disabled=disabled,
            use_container_width=True,
        ):
            _toggle_overlay(payload)
    with c2:
        st.markdown(f"**{pct_text}**")


def render_hydrological_context_card(
    *,
    hydro_summary_row: Mapping[str, Any],
    level: str,
    admin_key: str,
) -> None:
    """Render compact hydrological context and clickable boundary chips."""
    import streamlit as st

    if not admin_key:
        return

    basin_id = _text(hydro_summary_row.get("basin_id"))
    basin_name = _text(hydro_summary_row.get("basin_name"))
    basin_frac = hydro_summary_row.get("basin_frac")
    if _as_float(basin_frac) is None and "dominant_frac" in hydro_summary_row:
        basin_frac = hydro_summary_row.get("dominant_frac")

    subbasin_id = _text(hydro_summary_row.get("subbasin_id"))
    subbasin_name = _text(hydro_summary_row.get("subbasin_name"))
    subbasin_frac = hydro_summary_row.get("subbasin_frac")
    hydro_type = _text(hydro_summary_row.get("hydro_type")) or "Hydro context available"
    primary_river = _text(hydro_summary_row.get("primary_river"))
    drainage = _as_float(hydro_summary_row.get("drainage_area_km2"))

    has_any = bool(basin_id and basin_name) or bool(subbasin_id and subbasin_name)
    if not has_any:
        return

    level_norm = str(level or "district").strip().lower()

    with st.expander("Hydrological Context", expanded=True):
        if basin_id and basin_name:
            st.caption("Dominant basin")
            _hydro_chip(
                label=basin_name,
                pct_text=_format_pct(basin_frac, fraction=True),
                payload={
                    "hydro_level": "basin",
                    "basin_id": basin_id,
                    "subbasin_id": None,
                    "hydro_name": basin_name,
                    "admin_level": level_norm,
                    "admin_key": admin_key,
                },
            )

        if subbasin_id and subbasin_name:
            st.caption("Dominant sub-basin")
            _hydro_chip(
                label=subbasin_name,
                pct_text=_format_pct(subbasin_frac, fraction=True),
                payload={
                    "hydro_level": "sub_basin",
                    "basin_id": basin_id,
                    "subbasin_id": subbasin_id,
                    "hydro_name": subbasin_name,
                    "admin_level": level_norm,
                    "admin_key": admin_key,
                },
            )

        also = parse_hydro_intersections(hydro_summary_row.get("also_intersects_basin_json"))
        also = [x for x in also if _as_float(x.get("basin_frac", x.get("overlap_frac"))) is not None]
        also = sorted(
            also,
            key=lambda x: _as_float(x.get("basin_frac", x.get("overlap_frac"))) or 0,
            reverse=True,
        )[:2]
        if also:
            st.caption("Also intersects")
            for item in also:
                other_id = _text(item.get("basin_id"))
                other_name = _text(item.get("basin_name"))
                other_frac = item.get("basin_frac", item.get("overlap_frac"))
                if not other_id or not other_name:
                    continue
                _hydro_chip(
                    label=other_name,
                    pct_text=_format_pct(other_frac, fraction=True),
                    payload={
                        "hydro_level": "basin",
                        "basin_id": other_id,
                        "subbasin_id": None,
                        "hydro_name": other_name,
                        "admin_level": level_norm,
                        "admin_key": admin_key,
                    },
                )

        st.caption("Hydro type")
        st.markdown(f"**{hydro_type}**")

        meta = []
        if primary_river:
            meta.append(f"Primary river: **{primary_river}**")
        if drainage is not None:
            meta.append(f"Drainage area: **{drainage:,.0f} km²**")
        if meta:
            st.caption(" • ".join(meta))

        active = st.session_state.get("active_hydro_boundary_overlay")
        if isinstance(active, dict) and active.get("admin_key") == admin_key:
            if st.button(
                "Clear hydro overlay",
                key=_button_key("clear_hydro_boundary", admin_key),
                use_container_width=True,
            ):
                st.session_state["active_hydro_boundary_overlay"] = None
                st.rerun()


def render_admin_context_cards(
    *,
    exposure_summary_row: Optional[Mapping[str, Any]],
    hydro_summary_row: Optional[Mapping[str, Any]],
    level: str,
    admin_key: Optional[str],
    spatial_family: str = "admin",
) -> None:
    """Render all compact context cards for admin district/block focus."""
    level_norm = str(level or "").strip().lower()
    spatial_family_norm = str(spatial_family or "admin").strip().lower()
    if spatial_family_norm != "admin" or level_norm not in {"district", "block"}:
        return

    if exposure_summary_row is not None:
        render_exposure_snapshot_card(
            exposure_summary_row=exposure_summary_row,
            level=level_norm,
        )

    if hydro_summary_row is not None and admin_key:
        render_hydrological_context_card(
            hydro_summary_row=hydro_summary_row,
            level=level_norm,
            admin_key=admin_key,
        )

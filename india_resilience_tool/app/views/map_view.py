"""
Map view (Folium + st_folium) rendering and event extraction.

This module intentionally keeps Streamlit + Folium interactions inside app/.
Heavy imports are inside render_map_view() so tests remain fast.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any, Callable, Mapping, Optional, Tuple


def extract_clicked_district_state(ret: Optional[Mapping[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract (district_name, state_name) from st_folium return payload.

    The payload can contain different keys depending on st_folium/folium versions.
    We scan common keys and then inspect properties for likely district/state fields.

    Returns:
        (district_name, state_name) if found else (None, None)
    """
    if not ret:
        return None, None

    candidates = (
        "last_object_clicked",
        "clicked_feature",
        "last_active_drawing",
        "last_object",
    )

    for key in candidates:
        feat = ret.get(key)  # type: ignore[union-attr]
        if not isinstance(feat, dict):
            continue

        props = feat.get("properties") if isinstance(feat.get("properties"), dict) else feat
        if not isinstance(props, dict):
            continue

        # District name keys observed across ADM2 sources + folium outputs
        for pk in ("district_name", "shapeName", "NAME", "name", "SHAPE_NAME"):
            val = props.get(pk)
            if val:
                state_val = props.get("state_name") or props.get("shapeGroup") or props.get("shapeName_0")
                return str(val), (str(state_val) if state_val else None)

    return None, None


def render_map_view(
    *,
    m: Any,
    variable_slug: str,
    map_mode: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    selected_state: str,
    selected_district: str,
    map_width: int,
    map_height: int,
    perf_section: Optional[Callable[[str], Any]] = None,
) -> Tuple[Mapping[str, Any], Optional[str], Optional[str]]:
    """
    Render the folium map inside Streamlit using st_folium, and extract click info.

    This function preserves the legacy dashboard behavior:
      - Uses a deterministic st_folium key (map_key) tied to variable/scenario/period/stat and selection
      - Adds portfolio-mode point markers from session_state if present
      - Returns the st_folium payload and extracted clicked (district,state)

    Args:
        m: Pre-built folium.Map (including GeoJson layer, styles, etc.)
        variable_slug: current index slug (VARIABLE_SLUG)
        map_mode: "District" / "State average" etc (whatever your dashboard uses)
        sel_scenario: selected scenario key
        sel_period: selected period string
        sel_stat: selected stat key (mean/p05/p95 etc)
        selected_state: currently selected state (string)
        selected_district: currently selected district (string)
        map_width: width passed to st_folium
        map_height: height passed to st_folium
        perf_section: optional perf context manager factory (label -> context manager)

    Returns:
        returned: st_folium return dict (empty dict if None)
        clicked_district: extracted district name (if any)
        clicked_state: extracted state name (if any)
    """
    import streamlit as st
    import folium  # noqa: F401  # used for Marker
    from streamlit_folium import st_folium

    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")

    # In Multi-district portfolio mode, draw multi-point markers (if any)
    if analysis_mode == "Multi-district portfolio":
        points = st.session_state.get("point_query_points", [])
        if isinstance(points, list):
            for idx, pt in enumerate(points, start=1):
                if not isinstance(pt, dict):
                    continue
                try:
                    lat_p = float(pt.get("lat"))
                    lon_p = float(pt.get("lon"))
                except (TypeError, ValueError):
                    continue

                folium.Marker(
                    location=[lat_p, lon_p],
                    tooltip=f"Point {idx}: {lat_p:.4f}, {lon_p:.4f}",
                ).add_to(m)

        # Active point (current point used in the Climate Profile)
        point_query = st.session_state.get("point_query_latlon")
        if isinstance(point_query, dict):
            try:
                lat_a = float(point_query.get("lat"))
                lon_a = float(point_query.get("lon"))
                folium.Marker(
                    location=[lat_a, lon_a],
                    tooltip=f"Active point: {lat_a:.4f}, {lon_a:.4f}",
                ).add_to(m)
            except (TypeError, ValueError):
                pass

    ctx = perf_section("map: render st_folium") if perf_section is not None else nullcontext()

    with ctx:
        _state_key = str(selected_state).strip().lower().replace(" ", "_")
        _district_key = str(selected_district).strip().lower().replace(" ", "_")

        map_key = (
            f"main_map_{variable_slug}_{map_mode}_"
            f"{sel_scenario}_{sel_period}_{sel_stat}_"
            f"{_state_key}_{_district_key}"
        )

        returned = st_folium(
            m,
            width=map_width,
            height=map_height,
            returned_objects=[
                "last_object_clicked",
                "last_clicked",
            ],
            key=map_key,
        )

    if not isinstance(returned, dict):
        returned = {}

    clicked_district, clicked_state = extract_clicked_district_state(returned)
    return returned, clicked_district, clicked_state

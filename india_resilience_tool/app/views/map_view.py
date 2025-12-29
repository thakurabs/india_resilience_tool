"""
Map view (Folium + st_folium) rendering and event extraction.

This module provides:
- Map rendering with st_folium
- Click event extraction
- Portfolio district highlighting (blue borders)
- Inline add-to-portfolio controls
- Saved point markers

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
        feat = ret.get(key)
        if not isinstance(feat, dict):
            continue

        props = feat.get("properties") if isinstance(feat.get("properties"), dict) else feat
        if not isinstance(props, dict):
            continue

        for pk in ("district_name", "shapeName", "NAME", "name", "SHAPE_NAME"):
            val = props.get(pk)
            if val:
                state_val = props.get("state_name") or props.get("shapeGroup") or props.get("shapeName_0")
                return str(val), (str(state_val) if state_val else None)

    return None, None


def extract_click_coordinates(ret: Optional[Mapping[str, Any]]) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract (lat, lon) from st_folium return payload.
    
    Returns:
        (lat, lon) if found else (None, None)
    """
    if not ret:
        return None, None
    
    last_click = ret.get("last_object_clicked") or ret.get("last_clicked")
    if isinstance(last_click, dict):
        lat = last_click.get("lat")
        lng = last_click.get("lng") or last_click.get("lon")
        if lat is not None and lng is not None:
            try:
                return float(lat), float(lng)
            except (TypeError, ValueError):
                pass
    
    return None, None


def find_district_at_coordinates(
    merged: Any,  # GeoDataFrame
    lat: float,
    lon: float,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Find district containing or nearest to given coordinates.
    
    Args:
        merged: GeoDataFrame with district geometries and district_name/state_name columns
        lat: Latitude
        lon: Longitude
    
    Returns:
        (district_name, state_name) if found else (None, None)
    """
    from shapely.geometry import Point
    
    try:
        pt = Point(float(lon), float(lat))
        
        # Try exact containment first
        mask = merged.geometry.contains(pt)
        if mask.any():
            row = merged[mask].iloc[0]
        else:
            # Fall back to nearest centroid
            dists = merged.geometry.centroid.distance(pt)
            row = merged.loc[dists.idxmin()]
        
        district = str(row.get("district_name", "")).strip()
        state = str(row.get("state_name", "")).strip()
        
        if district and state:
            return district, state
    except Exception:
        pass
    
    return None, None


def create_portfolio_style_function(
    portfolio_keys: set,
    normalize_fn: Callable[[str], str],
    *,
    portfolio_border_color: str = "#2563eb",
    portfolio_border_weight: int = 3,
    default_border_color: str = "#666666",
    default_border_weight: float = 0.3,
) -> Callable[[dict], dict]:
    """
    Create a style function that highlights portfolio districts.
    
    Portfolio districts get a distinct blue border.
    
    Args:
        portfolio_keys: Set of (normalized_state, normalized_district) tuples
        normalize_fn: Function to normalize state/district names
        portfolio_border_color: Border color for portfolio districts
        portfolio_border_weight: Border weight for portfolio districts
        default_border_color: Border color for non-portfolio districts
        default_border_weight: Border weight for non-portfolio districts
    
    Returns:
        Style function for folium.GeoJson
    """
    def style_fn(feature: dict) -> dict:
        props = feature.get("properties", {})
        fill_color = props.get("fillColor", "#cccccc")
        
        state_name = props.get("state_name", "")
        district_name = props.get("district_name", "")
        
        key = (normalize_fn(state_name), normalize_fn(district_name))
        is_in_portfolio = key in portfolio_keys
        
        if is_in_portfolio:
            return {
                "fillColor": fill_color,
                "color": portfolio_border_color,
                "weight": portfolio_border_weight,
                "fillOpacity": 0.8,
                "dashArray": None,
            }
        else:
            return {
                "fillColor": fill_color,
                "color": default_border_color,
                "weight": default_border_weight,
                "fillOpacity": 0.7,
            }
    
    return style_fn


def add_portfolio_legend_to_map(
    m: Any,
    portfolio_count: int,
    portfolio_border_color: str = "#2563eb",
) -> None:
    """
    Add a legend item indicating portfolio districts.
    """
    import folium
    
    if portfolio_count == 0:
        return
    
    legend_html = f"""
    <div style="
        position: fixed;
        bottom: 50px;
        left: 10px;
        z-index: 1000;
        background: white;
        padding: 8px 12px;
        border-radius: 4px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.3);
        font-size: 12px;
    ">
        <div style="display: flex; align-items: center; gap: 8px;">
            <div style="
                width: 20px;
                height: 14px;
                border: 3px solid {portfolio_border_color};
                background: #f0f0f0;
            "></div>
            <span>In portfolio ({portfolio_count})</span>
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))


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
      - Uses a deterministic st_folium key tied to variable/scenario/period/stat and selection
      - Adds portfolio-mode point markers from session_state if present
      - Adds portfolio legend in portfolio mode
      - Returns the st_folium payload and extracted clicked (district, state)

    Args:
        m: Pre-built folium.Map (including GeoJson layer, styles, etc.)
        variable_slug: current index slug
        map_mode: "Absolute value" / "Change from baseline"
        sel_scenario: selected scenario key
        sel_period: selected period string
        sel_stat: selected stat key
        selected_state: currently selected state
        selected_district: currently selected district
        map_width: width passed to st_folium
        map_height: height passed to st_folium
        perf_section: optional perf context manager factory

    Returns:
        returned: st_folium return dict (empty dict if None)
        clicked_district: extracted district name (if any)
        clicked_state: extracted state name (if any)
    """
    import streamlit as st
    import folium
    from streamlit_folium import st_folium

    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")

    # In Multi-district portfolio mode, draw markers and legend
    if analysis_mode == "Multi-district portfolio":
        # Saved point markers (blue)
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
                
                label = pt.get("label") or f"Point {idx}"
                district = pt.get("district", "")
                tooltip_text = f"{label}: {district}" if district else f"{label}: {lat_p:.4f}, {lon_p:.4f}"

                folium.Marker(
                    location=[lat_p, lon_p],
                    tooltip=tooltip_text,
                    icon=folium.Icon(color="blue", icon="info-sign"),
                ).add_to(m)

        # Active point marker (legacy)
        point_query = st.session_state.get("point_query_latlon")
        if isinstance(point_query, dict):
            try:
                lat_a = float(point_query.get("lat"))
                lon_a = float(point_query.get("lon"))
                folium.Marker(
                    location=[lat_a, lon_a],
                    tooltip=f"Active: {lat_a:.4f}, {lon_a:.4f}",
                    icon=folium.Icon(color="red", icon="star"),
                ).add_to(m)
            except (TypeError, ValueError):
                pass
        
        # Single preview marker (red star) - from "Show on map" button
        preview_marker = st.session_state.get("map_preview_marker")
        if isinstance(preview_marker, dict):
            try:
                lat_m = float(preview_marker.get("lat"))
                lon_m = float(preview_marker.get("lon"))
                district = preview_marker.get("district", "")
                state = preview_marker.get("state", "")
                tooltip_text = f"📍 {district}, {state}" if district else f"📍 {lat_m:.4f}, {lon_m:.4f}"
                
                folium.Marker(
                    location=[lat_m, lon_m],
                    tooltip=tooltip_text,
                    popup=f"<b>{district}</b><br>{state}<br>({lat_m:.4f}, {lon_m:.4f})",
                    icon=folium.Icon(color="red", icon="star"),
                ).add_to(m)
            except (TypeError, ValueError):
                pass
        
        # Multiple preview markers (green) - from batch "Show all on map" button
        preview_markers = st.session_state.get("map_preview_markers")
        if isinstance(preview_markers, list):
            for idx, marker in enumerate(preview_markers, start=1):
                if not isinstance(marker, dict):
                    continue
                try:
                    lat_m = float(marker.get("lat"))
                    lon_m = float(marker.get("lon"))
                except (TypeError, ValueError):
                    continue
                
                label = marker.get("label") or f"#{idx}"
                district = marker.get("district", "")
                state = marker.get("state", "")
                tooltip_text = f"📍 {label}: {district}" if district else f"📍 {label}: {lat_m:.4f}, {lon_m:.4f}"
                
                folium.Marker(
                    location=[lat_m, lon_m],
                    tooltip=tooltip_text,
                    popup=f"<b>{label}</b><br>{district}, {state}<br>({lat_m:.4f}, {lon_m:.4f})",
                    icon=folium.Icon(color="green", icon="map-marker"),
                ).add_to(m)
        
        # Portfolio legend
        portfolio = st.session_state.get("portfolio_districts", [])
        add_portfolio_legend_to_map(m, len(portfolio))

    ctx = perf_section("map: render st_folium") if perf_section is not None else nullcontext()

    with ctx:
        map_key = f"map_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}_{selected_state}_{selected_district}"

        returned = st_folium(
            m,
            width=map_width,
            height=map_height,
            returned_objects=["last_object_clicked"],
            use_container_width=False,
            key=map_key,
        )

    if not isinstance(returned, dict):
        returned = {}

    clicked_district, clicked_state = extract_clicked_district_state(returned)
    return returned, clicked_district, clicked_state


def render_district_add_to_portfolio(
    *,
    clicked_district: Optional[str],
    clicked_state: Optional[str],
    selected_state: str,
    portfolio_add_fn: Callable[[str, str], None],
    portfolio_remove_fn: Callable[[str, str], None],
    portfolio_contains_fn: Callable[[str, str], bool],
    normalize_fn: Callable[[str], str],
    # New optional parameters for coordinate-based lookup
    returned: Optional[Mapping[str, Any]] = None,
    merged: Optional[Any] = None,  # GeoDataFrame
) -> bool:
    """
    Render inline add/remove button for a clicked district in portfolio mode.
    
    If clicked_district is None but returned and merged are provided,
    will attempt to find the district using click coordinates.
    
    Returns True if portfolio was changed.
    """
    import streamlit as st
    
    # If no district from properties, try coordinate lookup
    if not clicked_district and returned is not None and merged is not None:
        lat, lon = extract_click_coordinates(returned)
        if lat is not None and lon is not None:
            clicked_district, clicked_state = find_district_at_coordinates(merged, lat, lon)
    
    if not clicked_district:
        return False
    
    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")
    if analysis_mode != "Multi-district portfolio":
        return False
    
    state_for_add = clicked_state or selected_state
    if not state_for_add or state_for_add == "All":
        return False
    
    is_in_portfolio = portfolio_contains_fn(state_for_add, clicked_district)
    
    # Action box
    bg_color = "#e8f4e8" if not is_in_portfolio else "#fff3cd"
    border_color = "#28a745" if not is_in_portfolio else "#ffc107"
    
    st.markdown(
        f"""<div style="
            padding: 12px;
            background: {bg_color};
            border-radius: 8px;
            margin: 8px 0;
            border-left: 4px solid {border_color};
        ">
            <strong>{clicked_district}</strong>, {state_for_add}
        </div>""",
        unsafe_allow_html=True,
    )
    
    col1, col2 = st.columns(2)
    key_suffix = f"{normalize_fn(state_for_add)}_{normalize_fn(clicked_district)}"
    
    with col1:
        if is_in_portfolio:
            if st.button(
                "✓ Remove from portfolio",
                key=f"map_remove_{key_suffix}",
                type="secondary",
                use_container_width=True,
            ):
                portfolio_remove_fn(state_for_add, clicked_district)
                st.success(f"Removed {clicked_district}")
                st.rerun()
                return True
        else:
            if st.button(
                "+ Add to portfolio",
                key=f"map_add_{key_suffix}",
                type="primary",
                use_container_width=True,
            ):
                portfolio_add_fn(state_for_add, clicked_district)
                st.success(f"Added {clicked_district}")
                st.rerun()
                return True
    
    with col2:
        portfolio = st.session_state.get("portfolio_districts", [])
        st.markdown(
            f"<div style='text-align: center; padding: 8px; color: #666;'>"
            f"📋 {len(portfolio)} in portfolio</div>",
            unsafe_allow_html=True,
        )
    
    return False
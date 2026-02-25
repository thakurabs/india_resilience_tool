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
                state_val = (
                    props.get("state_name")
                    or props.get("state")
                    or props.get("STATE")
                    or props.get("shapeGroup")
                    or props.get("shapeName_0")
                )
                return str(val), (str(state_val) if state_val else None)

    return None, None


def extract_clicked_block_district_state(
    ret: Optional[Mapping[str, Any]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract (block_name, district_name, state_name) from st_folium return payload.

    Notes:
        st_folium payloads vary by version. We scan common keys and then inspect
        properties for likely block/district/state fields.

    Returns:
        (block_name, district_name, state_name) if found else (None, None, None)
    """
    if not ret:
        return None, None, None

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

        # Block name candidates (ADM3)
        block_val: Optional[str] = None
        for pk in (
            "block_name",
            "subdistrict_name",
            "adm3_name",
            "NAME_3",
            "name_3",
            "NAME3",
            "shapeName_3",
        ):
            v = props.get(pk)
            if v:
                block_val = str(v).strip()
                break

        if block_val:
            district_val = props.get("district_name") or props.get("district") or props.get("shapeName_1") or props.get("shapeName_2")
            state_val = props.get("state_name") or props.get("state") or props.get("shapeGroup") or props.get("shapeName_0")
            return block_val, (str(district_val).strip() if district_val else None), (str(state_val).strip() if state_val else None)

    return None, None, None


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


def find_block_at_coordinates(
    merged: Any,  # GeoDataFrame
    lat: float,
    lon: float,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Find block (ADM3) containing or nearest to given coordinates.

    Args:
        merged: GeoDataFrame with block geometries and block_name/district_name/state_name columns
        lat: Latitude
        lon: Longitude

    Returns:
        (block_name, district_name, state_name) if found else (None, None, None)
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

        block = str(row.get("block_name", "")).strip()
        district = str(row.get("district_name", "")).strip()
        state = str(row.get("state_name", "")).strip()

        if block and district and state:
            return block, district, state
    except Exception:
        pass

    return None, None, None


def create_portfolio_style_function(
    portfolio_keys: set,
    normalize_fn: Callable[[str], str],
    *,
    level: str = "district",
    portfolio_border_color: str = "#2563eb",
    portfolio_border_weight: int = 3,
    default_border_color: str = "#666666",
    default_border_weight: float = 0.3,
) -> Callable[[dict], dict]:
    """
    Create a style function that highlights portfolio units.

    District mode:
        portfolio_keys contains (state, district)

    Block mode:
        portfolio_keys contains (state, district, block)
    """
    level_norm = str(level).strip().lower()

    def style_fn(feature: dict) -> dict:
        props = feature.get("properties", {})
        fill_color = props.get("fillColor", "#cccccc")

        state_name = props.get("state_name", "")
        district_name = props.get("district_name", "")

        if level_norm == "block":
            block_name = props.get("block_name", "") or props.get("subdistrict_name", "") or props.get("adm3_name", "")
            key = (
                normalize_fn(state_name),
                normalize_fn(district_name),
                normalize_fn(str(block_name)),
            )
        else:
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

        return {
            "fillColor": fill_color,
            "color": default_border_color,
            "weight": default_border_weight,
            "fillOpacity": 0.8,
            "dashArray": None,
        }

    return style_fn


def add_portfolio_legend_to_map(
    m: Any,
    portfolio_count: int,
    *,
    level: str = "district",
    portfolio_border_color: str = "#2563eb",
) -> None:
    """
    Add a legend item indicating portfolio units.
    """
    import folium

    if portfolio_count == 0:
        return

    unit_label = "block" if str(level).strip().lower() == "block" else "district"

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
            <span>In portfolio ({portfolio_count} {unit_label}{'s' if portfolio_count != 1 else ''})</span>
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
    legend_block_html: Optional[str] = None,
    selected_block: str = "All",
    level: str = "district",
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

    # In Multi-district/block portfolio mode, draw markers and legend
    if "Multi" in analysis_mode:
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
                tooltip_text = f"Location: {district}, {state}" if district else f"Location: {lat_m:.4f}, {lon_m:.4f}"
                
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
                tooltip_text = f"Location: {label}: {district}" if district else f"Location: {label}: {lat_m:.4f}, {lon_m:.4f}"
                
                folium.Marker(
                    location=[lat_m, lon_m],
                    tooltip=tooltip_text,
                    popup=f"<b>{label}</b><br>{district}, {state}<br>({lat_m:.4f}, {lon_m:.4f})",
                    icon=folium.Icon(color="green", icon="map-marker"),
                ).add_to(m)
        
        # Portfolio legend
        portfolio_state_key = "portfolio_blocks" if str(level).strip().lower() == "block" else "portfolio_districts"
        portfolio = st.session_state.get(portfolio_state_key, [])
        add_portfolio_legend_to_map(m, len(portfolio) if isinstance(portfolio, list) else 0, level=level)

    ctx = perf_section("map: render st_folium") if perf_section is not None else nullcontext()

    with ctx:
        map_key = f"map_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}_{selected_state}_{selected_district}_{selected_block}_{str(level).strip().lower()}"

        if legend_block_html:
            map_col, legend_col = st.columns([18, 3])
            with map_col:
                returned = st_folium(
                    m,
                    width=map_width,
                    height=map_height,
                    returned_objects=["last_object_clicked"],
                    use_container_width=True,
                    key=map_key,
                )
            with legend_col:
                st.markdown(legend_block_html, unsafe_allow_html=True)
        else:
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

    # Default (district) click extraction
    clicked_district, clicked_state = extract_clicked_district_state(returned)

    # Block-aware click extraction (stores clicked_block in session_state)
    clicked_block: Optional[str] = None
    if str(level).strip().lower() == "block":
        b, d, s = extract_clicked_block_district_state(returned)
        clicked_block = b
        clicked_district = d or clicked_district
        clicked_state = s or clicked_state
        st.session_state["clicked_block"] = clicked_block
    else:
        # Ensure stale value isn't carried across toggles
        if "clicked_block" in st.session_state:
            st.session_state.pop("clicked_block")

    return returned, clicked_district, clicked_state


def render_unit_add_to_portfolio(
    *,
    clicked_district: Optional[str],
    clicked_state: Optional[str],
    clicked_block: Optional[str] = None,
    selected_state: str,
    portfolio_add_fn: Callable[..., None],
    portfolio_remove_fn: Callable[..., None],
    portfolio_contains_fn: Callable[..., bool],
    normalize_fn: Callable[[str], str],
    # Optional parameters for coordinate-based lookup
    returned: Optional[Mapping[str, Any]] = None,
    merged: Optional[Any] = None,  # GeoDataFrame
    level: str = "district",
) -> bool:
    """
    Render inline add/remove button for a clicked unit in portfolio mode.

    District mode:
        Uses (state, district)

    Block mode:
        Uses (state, district, block)
    """
    import streamlit as st

    level_norm = str(level).strip().lower()

    # Resolve click coordinates if needed
    resolved_district = clicked_district
    resolved_state = clicked_state
    resolved_block = clicked_block

    if level_norm == "block":
        if not resolved_block and returned is not None:
            b, d, s = extract_clicked_block_district_state(returned)
            resolved_block = b or resolved_block
            resolved_district = d or resolved_district
            resolved_state = s or resolved_state

        if (
            (merged is not None)
            and (returned is not None)
            and (
                (not resolved_block)
                or (not resolved_district)
                or (not resolved_state)
                or (str(resolved_state).strip() == "All")
            )
        ):
            lat, lon = extract_click_coordinates(returned)
            if lat is not None and lon is not None:
                b2, d2, s2 = find_block_at_coordinates(merged, lat, lon)
                if not resolved_block and b2:
                    resolved_block = b2
                if not resolved_district and d2:
                    resolved_district = d2
                if (not resolved_state or str(resolved_state).strip() == "All") and s2:
                    if (not resolved_district) or (d2 and normalize_fn(d2) == normalize_fn(resolved_district)):
                        resolved_state = s2
    else:
        if (
            (merged is not None)
            and (returned is not None)
            and (
                (not resolved_district)
                or (not resolved_state)
                or (str(resolved_state).strip() == "All")
            )
        ):
            lat, lon = extract_click_coordinates(returned)
            if lat is not None and lon is not None:
                d2, s2 = find_district_at_coordinates(merged, lat, lon)
                if not resolved_district and d2:
                    resolved_district = d2
                if (not resolved_state or str(resolved_state).strip() == "All") and s2:
                    if (not resolved_district) or (d2 and normalize_fn(d2) == normalize_fn(resolved_district)):
                        resolved_state = s2

    if level_norm == "block":
        if not resolved_block or not resolved_district:
            return False
    else:
        if not resolved_district:
            return False

    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")
    if "Multi" not in analysis_mode:
        return False

    state_for_add = (resolved_state or selected_state or "").strip()
    if not state_for_add or state_for_add == "All":
        return False

    unit_label = "block" if level_norm == "block" else "district"
    name_for_display = resolved_block if level_norm == "block" else resolved_district

    # Portfolio membership check
    if level_norm == "block":
        is_in_portfolio = bool(portfolio_contains_fn(state_for_add, resolved_district, resolved_block))
        key_suffix = f"{normalize_fn(state_for_add)}_{normalize_fn(resolved_district)}_{normalize_fn(resolved_block)}"
    else:
        is_in_portfolio = bool(portfolio_contains_fn(state_for_add, resolved_district))
        key_suffix = f"{normalize_fn(state_for_add)}_{normalize_fn(resolved_district)}"

    st.markdown(f"**{name_for_display}** ({state_for_add})")

    col1, col2 = st.columns([2, 1])
    with col1:
        if is_in_portfolio:
            if st.button(
                f"Remove {unit_label} from portfolio",
                key=f"map_remove_{key_suffix}",
                type="secondary",
                use_container_width=True,
            ):
                if level_norm == "block":
                    portfolio_remove_fn(state_for_add, resolved_district, resolved_block)
                else:
                    portfolio_remove_fn(state_for_add, resolved_district)
                st.success(f"Removed {name_for_display}")
                st.rerun()
                return True
        else:
            if st.button(
                f"+ Add {unit_label} to portfolio",
                key=f"map_add_{key_suffix}",
                type="primary",
                use_container_width=True,
            ):
                if level_norm == "block":
                    portfolio_add_fn(state_for_add, resolved_district, resolved_block)
                else:
                    portfolio_add_fn(state_for_add, resolved_district)
                st.success(f"Added {name_for_display}")
                st.rerun()
                return True

    with col2:
        st.caption("Portfolio mode")

    return False


def render_district_add_to_portfolio(
    *,
    clicked_district: Optional[str],
    clicked_state: Optional[str],
    selected_state: str,
    portfolio_add_fn: Callable[[str, str], None],
    portfolio_remove_fn: Callable[[str, str], None],
    portfolio_contains_fn: Callable[[str, str], bool],
    normalize_fn: Callable[[str], str],
    returned: Optional[Mapping[str, Any]] = None,
    merged: Optional[Any] = None,  # GeoDataFrame
) -> bool:
    """
    Backward-compatible wrapper for district-mode inline portfolio controls.
    """
    return render_unit_add_to_portfolio(
        clicked_district=clicked_district,
        clicked_state=clicked_state,
        clicked_block=None,
        selected_state=selected_state,
        portfolio_add_fn=portfolio_add_fn,
        portfolio_remove_fn=portfolio_remove_fn,
        portfolio_contains_fn=portfolio_contains_fn,
        normalize_fn=normalize_fn,
        returned=returned,
        merged=merged,
        level="district",
    )

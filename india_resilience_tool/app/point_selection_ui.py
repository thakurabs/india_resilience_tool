"""
Point selection UI for IRT portfolio mode.

This module renders coordinate-based district lookup and saved points management.

Features:
- Enter coordinates → Preview district → Add to portfolio
- Show on map: Place a marker on the map to visualize location
- Batch coordinate input: Paste multiple coordinates at once
- Save points for later batch adding

Widget keys preserved:
- btn_save_point
- btn_clear_saved_points

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import re
from typing import Any, Callable, List, Optional, Tuple

import pandas as pd
from shapely.geometry import Point


def find_district_at_point(
    merged: Any,
    lat: float,
    lon: float,
) -> Optional[Tuple[str, str]]:
    """
    Find district containing or nearest to a point.
    
    Returns (state_name, district_name) or None if not found.
    """
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
        
        state = str(row.get("state_name", "")).strip()
        district = str(row.get("district_name", "")).strip()
        
        if state and district:
            return (state, district)
    except Exception:
        pass
    
    return None


def parse_batch_coordinates(text: str) -> List[Tuple[float, float, Optional[str]]]:
    """
    Parse batch coordinate input.
    
    Supports formats:
    - lat, lon
    - lat, lon, label
    - lat lon
    - One coordinate pair per line
    
    Returns list of (lat, lon, label) tuples.
    """
    results = []
    lines = text.strip().split("\n")
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Try to parse: lat, lon, label or lat, lon or lat lon
        # Remove any quotes
        line = line.replace('"', '').replace("'", "")
        
        # Split by comma or whitespace
        if "," in line:
            parts = [p.strip() for p in line.split(",")]
        else:
            parts = line.split()
        
        if len(parts) >= 2:
            try:
                lat = float(parts[0])
                lon = float(parts[1])
                label = parts[2] if len(parts) > 2 else None
                
                # Basic validation for India bounds
                if 6.0 <= lat <= 38.0 and 68.0 <= lon <= 98.0:
                    results.append((lat, lon, label))
            except ValueError:
                continue
    
    return results


def render_point_selection_panel(
    *,
    merged: Any,
    portfolio_add_fn: Callable[[str, str], None],
    portfolio_key_fn: Callable[[str, str], Tuple[str, str]],
    portfolio_set_flash_fn: Callable[[str, str], None],
) -> bool:
    """
    Render the point selection panel for portfolio mode.

    Features:
    1. Single coordinate entry with preview
    2. Show on map button to visualize location
    3. Batch coordinate input for multiple points
    4. Save points for later batch adding

    Args:
        merged: GeoDataFrame with district geometries
        portfolio_add_fn: Function to add a district to portfolio
        portfolio_key_fn: Function to create a normalized key
        portfolio_set_flash_fn: Function to set a flash message

    Returns:
        clear_clicked: Always False (kept for backward compatibility)
    """
    import streamlit as st

    # Initialize session state
    if "point_query_points" not in st.session_state:
        st.session_state["point_query_points"] = []
    if "map_preview_marker" not in st.session_state:
        st.session_state["map_preview_marker"] = None

    saved_points = st.session_state["point_query_points"]

    # Get bounds for validation
    try:
        minx, miny, maxx, maxy = merged.total_bounds
        default_lat = float((miny + maxy) / 2.0)
        default_lon = float((minx + maxx) / 2.0)
    except Exception:
        miny, maxy = 6.0, 38.0
        minx, maxx = 68.0, 98.0
        default_lat, default_lon = 17.385, 78.4867

    st.subheader("📍 Add by Location")
    
    # Create tabs for single vs batch input
    tab_single, tab_batch = st.tabs(["Single Coordinate", "Batch Input"])
    
    # =========================================================================
    # TAB 1: Single Coordinate Entry
    # =========================================================================
    with tab_single:
        st.caption("Enter coordinates to find and add a district to your portfolio.")
        
        # Coordinate input
        col_lat, col_lon = st.columns(2)
        with col_lat:
            lat_input = st.number_input(
                "Latitude",
                min_value=float(miny),
                max_value=float(maxy),
                value=float(st.session_state.get("point_query_lat", default_lat)),
                format="%.4f",
                key="_point_lat",
            )
        with col_lon:
            lon_input = st.number_input(
                "Longitude",
                min_value=float(minx),
                max_value=float(maxx),
                value=float(st.session_state.get("point_query_lon", default_lon)),
                format="%.4f",
                key="_point_lon",
            )

        # Preview district
        result = find_district_at_point(merged, lat_input, lon_input)
        
        if result:
            state_name, district_name = result
            st.info(f"📍 This location is in **{district_name}**, {state_name}")
            
            # Action buttons - 3 columns
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("➕ Add to portfolio", key="_point_add_direct", type="primary", use_container_width=True):
                    portfolio_add_fn(state_name, district_name)
                    portfolio_set_flash_fn(f"Added {district_name}, {state_name}", "success")
                    st.rerun()
            
            with col2:
                if st.button("🗺️ Show on map", key="_point_show_map", use_container_width=True):
                    # Set the marker for the map to display
                    st.session_state["map_preview_marker"] = {
                        "lat": lat_input,
                        "lon": lon_input,
                        "district": district_name,
                        "state": state_name,
                    }
                    portfolio_set_flash_fn(f"📍 Showing {district_name} on map", "info")
                    # Set flag to jump to map view
                    st.session_state["jump_to_map"] = True
                    st.rerun()
            
            with col3:
                if st.button("💾 Save point", key="btn_save_point", use_container_width=True):
                    # Check for duplicates
                    is_dup = any(
                        abs(p.get("lat", 0) - lat_input) < 1e-6 and 
                        abs(p.get("lon", 0) - lon_input) < 1e-6
                        for p in saved_points
                    )
                    if is_dup:
                        st.warning("This point is already saved")
                    else:
                        saved_points.append({
                            "lat": lat_input, 
                            "lon": lon_input,
                            "label": None,
                            "district": district_name,
                            "state": state_name,
                        })
                        st.session_state["point_query_points"] = saved_points
                        st.success("Point saved!")
                        st.rerun()
        else:
            st.warning("Could not identify a district at this location")
        
        # Show current map marker status
        current_marker = st.session_state.get("map_preview_marker")
        if current_marker:
            st.caption(
                f"🗺️ Map marker active: {current_marker.get('district')}, "
                f"{current_marker.get('state')} ({current_marker.get('lat'):.4f}, {current_marker.get('lon'):.4f})"
            )
            if st.button("✕ Clear map marker", key="_clear_map_marker", type="secondary"):
                st.session_state["map_preview_marker"] = None
                st.rerun()

    # =========================================================================
    # TAB 2: Batch Coordinate Input
    # =========================================================================
    with tab_batch:
        st.caption("Paste multiple coordinates to add several districts at once.")
        
        st.markdown("""
        **Supported formats:**
        - `lat, lon` (one per line)
        - `lat, lon, label` (with optional label)
        - `lat lon` (space-separated)
        """)
        
        batch_input = st.text_area(
            "Paste coordinates",
            placeholder="17.3850, 78.4867\n18.1124, 79.0193, Warangal Office\n16.5062, 80.6480",
            height=120,
            key="_batch_coords_input",
        )
        
        if batch_input.strip():
            parsed = parse_batch_coordinates(batch_input)
            
            if parsed:
                st.success(f"✓ Parsed {len(parsed)} coordinate(s)")
                
                # Preview parsed coordinates
                preview_data = []
                for lat, lon, label in parsed:
                    result = find_district_at_point(merged, lat, lon)
                    if result:
                        state_name, district_name = result
                        preview_data.append({
                            "Label": label or "—",
                            "Lat": f"{lat:.4f}",
                            "Lon": f"{lon:.4f}",
                            "District": district_name,
                            "State": state_name,
                        })
                    else:
                        preview_data.append({
                            "Label": label or "—",
                            "Lat": f"{lat:.4f}",
                            "Lon": f"{lon:.4f}",
                            "District": "Not found",
                            "State": "—",
                        })
                
                st.dataframe(
                    pd.DataFrame(preview_data),
                    hide_index=True,
                    use_container_width=True,
                )
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("➕ Add all to portfolio", key="_batch_add_all", type="primary", use_container_width=True):
                        added = 0
                        for lat, lon, label in parsed:
                            result = find_district_at_point(merged, lat, lon)
                            if result:
                                portfolio_add_fn(result[0], result[1])
                                added += 1
                        
                        if added > 0:
                            portfolio_set_flash_fn(f"Added {added} district(s) to portfolio", "success")
                        else:
                            portfolio_set_flash_fn("No districts could be identified", "warning")
                        st.rerun()
                
                with col2:
                    if st.button("🗺️ Show all on map", key="_batch_show_map", use_container_width=True):
                        # Add all points as markers
                        markers = []
                        for lat, lon, label in parsed:
                            result = find_district_at_point(merged, lat, lon)
                            if result:
                                markers.append({
                                    "lat": lat,
                                    "lon": lon,
                                    "label": label,
                                    "district": result[1],
                                    "state": result[0],
                                })
                        if markers:
                            st.session_state["map_preview_markers"] = markers
                            portfolio_set_flash_fn(f"📍 Showing {len(markers)} location(s) on map", "info")
                            st.session_state["jump_to_map"] = True
                            st.rerun()
                
                with col3:
                    if st.button("💾 Save all points", key="_batch_save_all", use_container_width=True):
                        saved_count = 0
                        for lat, lon, label in parsed:
                            # Check for duplicates
                            is_dup = any(
                                abs(p.get("lat", 0) - lat) < 1e-6 and 
                                abs(p.get("lon", 0) - lon) < 1e-6
                                for p in saved_points
                            )
                            if not is_dup:
                                result = find_district_at_point(merged, lat, lon)
                                saved_points.append({
                                    "lat": lat,
                                    "lon": lon,
                                    "label": label,
                                    "district": result[1] if result else None,
                                    "state": result[0] if result else None,
                                })
                                saved_count += 1
                        
                        st.session_state["point_query_points"] = saved_points
                        if saved_count > 0:
                            st.success(f"Saved {saved_count} new point(s)")
                            st.rerun()
                        else:
                            st.info("All points were already saved")
            else:
                st.warning("Could not parse any valid coordinates. Please check the format.")

    # =========================================================================
    # Saved Points Section (shown in both tabs)
    # =========================================================================
    if saved_points:
        st.markdown("---")
        st.markdown(f"**📌 Saved Points ({len(saved_points)})**")
        
        points_df_data = []
        for idx, pt in enumerate(saved_points):
            lat = pt.get("lat")
            lon = pt.get("lon")
            label = pt.get("label")
            # Use cached district/state if available, otherwise look up
            district = pt.get("district")
            state = pt.get("state")
            if not district or not state:
                result = find_district_at_point(merged, lat, lon) if lat and lon else None
                if result:
                    state, district = result
            
            district_info = f"{district}, {state}" if district and state else "Unknown"
            points_df_data.append({
                "#": idx + 1,
                "Label": label or "—",
                "Lat": f"{lat:.4f}" if lat else "—",
                "Lon": f"{lon:.4f}" if lon else "—",
                "District": district_info,
            })
        
        st.dataframe(
            pd.DataFrame(points_df_data),
            hide_index=True,
            use_container_width=True,
        )

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("➕ Add all to portfolio", key="_points_add_all", type="primary", use_container_width=True):
                added = 0
                for pt in saved_points:
                    state = pt.get("state")
                    district = pt.get("district")
                    if not state or not district:
                        result = find_district_at_point(merged, pt.get("lat"), pt.get("lon"))
                        if result:
                            state, district = result
                    if state and district:
                        portfolio_add_fn(state, district)
                        added += 1
                
                if added > 0:
                    portfolio_set_flash_fn(f"Added {added} district(s) to portfolio", "success")
                else:
                    portfolio_set_flash_fn("No districts could be identified", "warning")
                st.rerun()
        
        with col2:
            if st.button("🗺️ Show on map", key="_points_show_map", use_container_width=True):
                markers = []
                for pt in saved_points:
                    lat = pt.get("lat")
                    lon = pt.get("lon")
                    if lat and lon:
                        markers.append({
                            "lat": lat,
                            "lon": lon,
                            "label": pt.get("label"),
                            "district": pt.get("district"),
                            "state": pt.get("state"),
                        })
                if markers:
                    st.session_state["map_preview_markers"] = markers
                    portfolio_set_flash_fn(f"📍 Showing {len(markers)} saved point(s) on map", "info")
                    st.session_state["jump_to_map"] = True
                    st.rerun()
        
        with col3:
            if st.button("🗑 Clear all", key="btn_clear_saved_points", use_container_width=True):
                st.session_state["point_query_points"] = []
                st.session_state["map_preview_markers"] = None
                st.rerun()
    else:
        st.caption("💡 Use **Save point** to build a list of locations for batch adding.")

    return False
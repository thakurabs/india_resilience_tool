"""
Point selection UI for IRT portfolio mode.

This module renders coordinate-based district lookup and saved points management.
Simplified from the original version:
- Removed "active point" vs "saved points" distinction
- Single flow: enter coords → preview → add or save
- Cleaner UI with fewer buttons

Widget keys preserved:
- btn_save_point
- btn_clear_saved_points

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

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


def render_point_selection_panel(
    *,
    merged: Any,
    portfolio_add_fn: Callable[[str, str], None],
    portfolio_key_fn: Callable[[str, str], Tuple[str, str]],
    portfolio_set_flash_fn: Callable[[str, str], None],
) -> bool:
    """
    Render the point selection panel for portfolio mode.

    Simplified flow:
    1. Enter coordinates
    2. See preview of district at that location
    3. Add directly to portfolio OR save point for later

    Args:
        merged: GeoDataFrame with district geometries
        portfolio_add_fn: Function to add a district to portfolio
        portfolio_key_fn: Function to create a normalized key
        portfolio_set_flash_fn: Function to set a flash message

    Returns:
        clear_clicked: Always False (kept for backward compatibility)
    """
    import streamlit as st

    # Initialize saved points
    if "point_query_points" not in st.session_state:
        st.session_state["point_query_points"] = []

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
    st.caption("Enter coordinates to find and add districts to your portfolio.")

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
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("➕ Add to portfolio", key="_point_add_direct", type="primary", use_container_width=True):
                portfolio_add_fn(state_name, district_name)
                portfolio_set_flash_fn(f"Added {district_name}, {state_name}", "success")
                st.rerun()
        with col2:
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
                    saved_points.append({"lat": lat_input, "lon": lon_input})
                    st.session_state["point_query_points"] = saved_points
                    st.success("Point saved!")
                    st.rerun()
    else:
        st.warning("Could not identify a district at this location")

    # Saved points section
    if saved_points:
        st.markdown("---")
        st.markdown(f"**Saved Points ({len(saved_points)})**")
        
        points_df_data = []
        for idx, pt in enumerate(saved_points):
            lat = pt.get("lat")
            lon = pt.get("lon")
            result = find_district_at_point(merged, lat, lon) if lat and lon else None
            district_info = f"{result[1]}, {result[0]}" if result else "Unknown"
            points_df_data.append({
                "#": idx + 1,
                "Lat": f"{lat:.4f}",
                "Lon": f"{lon:.4f}",
                "District": district_info,
            })
        
        st.dataframe(
            pd.DataFrame(points_df_data),
            hide_index=True,
            use_container_width=True,
        )

        col1, col2 = st.columns(2)
        with col1:
            if st.button("➕ Add all to portfolio", key="_points_add_all", type="primary", use_container_width=True):
                added = 0
                for pt in saved_points:
                    result = find_district_at_point(merged, pt.get("lat"), pt.get("lon"))
                    if result:
                        portfolio_add_fn(result[0], result[1])
                        added += 1
                
                if added > 0:
                    portfolio_set_flash_fn(f"Added {added} district(s) to portfolio", "success")
                else:
                    portfolio_set_flash_fn("No districts could be identified", "warning")
                st.rerun()
        
        with col2:
            if st.button("🗑 Clear saved", key="btn_clear_saved_points", use_container_width=True):
                st.session_state["point_query_points"] = []
                st.rerun()
    else:
        st.caption("Use **Save point** to build a list of locations for batch adding.")

    return False
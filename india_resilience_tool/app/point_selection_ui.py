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


def find_block_at_point(
    merged: Any,
    lat: float,
    lon: float,
) -> Optional[Tuple[str, str, str]]:
    """
    Find block containing or nearest to a point.

    Returns (state_name, district_name, block_name) or None if not found.
    """
    try:
        pt = Point(float(lon), float(lat))

        # Try exact containment first
        mask = merged.geometry.contains(pt)
        if mask.any():
            row = merged[mask].iloc[0]
        else:
            dists = merged.geometry.centroid.distance(pt)
            row = merged.loc[dists.idxmin()]

        def _first_nonempty(keys: list[str]) -> str:
            for k in keys:
                if k in row and str(row.get(k, "")).strip():
                    return str(row.get(k, "")).strip()
            return ""

        state = _first_nonempty(["state_name", "state", "adm1_name", "STATE"])
        district = _first_nonempty(["district_name", "district", "adm2_name", "DISTRICT"])
        block = _first_nonempty(["block_name", "block", "adm3_name", "BLOCK"])

        if state and district and block:
            return (state, district, block)
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
    portfolio_add_fn: Callable[..., None],
    portfolio_key_fn: Callable[..., tuple],
    portfolio_set_flash_fn: Callable[[str, str], None],
    level: str = "district",
) -> bool:
    """
    Render the point selection panel for portfolio mode.

    Supports both district and block admin levels:
      - district: resolves (state, district)
      - block: resolves (state, district, block)
    """
    import streamlit as st

    level_norm = (level or "district").strip().lower()
    is_block = level_norm == "block"

    # Initialize saved points in session state
    saved_points = st.session_state.get("point_query_points", [])
    if saved_points is None:
        saved_points = []
        st.session_state["point_query_points"] = saved_points

    # Determine bounds for input defaults from merged geometry
    try:
        bounds = merged.total_bounds  # [minx, miny, maxx, maxy]
        minx, miny, maxx, maxy = bounds
        default_lat = float((miny + maxy) / 2.0)
        default_lon = float((minx + maxx) / 2.0)
    except Exception:
        miny, maxy = 6.0, 38.0
        minx, maxx = 68.0, 98.0
        default_lat, default_lon = 17.385, 78.4867

    def _find_unit(lat: float, lon: float) -> Optional[tuple[str, str, Optional[str]]]:
        if is_block:
            ret = find_block_at_point(merged, lat, lon)
            if not ret:
                return None
            st_name, dist_name, blk_name = ret
            return (st_name, dist_name, blk_name)
        ret = find_district_at_point(merged, lat, lon)
        if not ret:
            return None
        st_name, dist_name = ret
        return (st_name, dist_name, None)

    st.subheader("Add by Location")

    tab_single, tab_batch = st.tabs(["Single Coordinate", "Batch Input"])

    # =========================================================================
    # TAB 1: Single Coordinate Entry
    # =========================================================================
    with tab_single:
        col_lat, col_lon = st.columns(2)

        with col_lat:
            lat_input = st.number_input(
                "Latitude",
                min_value=float(miny),
                max_value=float(maxy),
                value=float(st.session_state.get("point_query_lat", default_lat)),
                format="%.4f",
                key="_point_lat",
                help="Enter latitude in decimal degrees within the dataset bounds.",
            )
        with col_lon:
            lon_input = st.number_input(
                "Longitude",
                min_value=float(minx),
                max_value=float(maxx),
                value=float(st.session_state.get("point_query_lon", default_lon)),
                format="%.4f",
                key="_point_lon",
                help="Enter longitude in decimal degrees within the dataset bounds.",
            )

        result = _find_unit(lat_input, lon_input)

        if result:
            state_name, district_name, block_name = result

            if is_block and block_name:
                st.info(f"This location is in **{block_name}** (District: {district_name}), {state_name}")
            else:
                st.info(f"This location is in **{district_name}**, {state_name}")

            col1, col2, col3 = st.columns(3)

            with col1:
                if st.button("Add to portfolio", key="_point_add_direct", type="primary", use_container_width=True):
                    try:
                        if is_block and block_name:
                            portfolio_add_fn(state_name, district_name, block_name)
                            portfolio_set_flash_fn(f"Added {block_name} ({district_name}), {state_name}", "success")
                        else:
                            portfolio_add_fn(state_name, district_name)
                            portfolio_set_flash_fn(f"Added {district_name}, {state_name}", "success")
                    except TypeError:
                        portfolio_add_fn(state_name, district_name)
                        portfolio_set_flash_fn(f"Added {district_name}, {state_name}", "success")
                    st.rerun()

            with col2:
                if st.button("Show on map", key="_point_show_map", use_container_width=True):
                    st.session_state["map_preview_marker"] = {
                        "lat": lat_input,
                        "lon": lon_input,
                        "district": district_name,
                        "state": state_name,
                        "block": block_name,
                    }
                    if is_block and block_name:
                        portfolio_set_flash_fn(f"Showing {block_name} on map", "info")
                    else:
                        portfolio_set_flash_fn(f"Showing {district_name} on map", "info")
                    st.session_state["jump_to_map"] = True
                    st.rerun()

            with col3:
                if st.button("Save point", key="btn_save_point", use_container_width=True):
                    is_dup = any(
                        abs(p.get("lat", 0) - lat_input) < 1e-6 and abs(p.get("lon", 0) - lon_input) < 1e-6
                        for p in saved_points
                    )
                    if is_dup:
                        st.warning("This point is already saved")
                    else:
                        entry = {
                            "lat": lat_input,
                            "lon": lon_input,
                            "label": None,
                            "district": district_name,
                            "state": state_name,
                        }
                        if is_block and block_name:
                            entry["block"] = block_name
                        saved_points.append(entry)
                        st.session_state["point_query_points"] = saved_points
                        st.success("Point saved!")
                        st.rerun()
        else:
            st.warning("Could not identify a unit at this location")

        current_marker = st.session_state.get("map_preview_marker")
        if current_marker:
            label = current_marker.get("block") or current_marker.get("district")
            if label:
                st.caption(f"Map preview active: {label}. Switch to Map view to see marker.")

    # =========================================================================
    # TAB 2: Batch Coordinate Entry
    # =========================================================================
    with tab_batch:
        batch_text = st.text_area(
            "Batch coordinates",
            value=st.session_state.get("point_query_batch_text", ""),
            height=140,
            key="_point_batch_text",
            placeholder="17.3850, 78.4867, Home\n16.5062, 80.6480, Site B",
            help=(
                "Supported formats (one per line): lat, lon | lat lon | lat, lon, label. "
                "Values should be decimal degrees."
            ),
        )

        colA, colB = st.columns(2)
        with colA:
            if st.button("Preview batch", key="btn_preview_batch", use_container_width=True):
                pts = parse_batch_coordinates(batch_text)
                previews = []
                for lat, lon, label in pts:
                    hit = _find_unit(lat, lon)
                    if not hit:
                        continue
                    st_name, dist_name, blk_name = hit
                    row = {"lat": lat, "lon": lon, "label": label, "state": st_name, "district": dist_name}
                    if is_block and blk_name:
                        row["block"] = blk_name
                    previews.append(row)
                st.session_state["point_query_batch_preview"] = previews
                st.success(f"Previewed {len(previews)} point(s)")

        with colB:
            if st.button("Add batch to saved points", key="btn_batch_save_points", use_container_width=True):
                pts = parse_batch_coordinates(batch_text)
                added = 0
                for lat, lon, label in pts:
                    hit = _find_unit(lat, lon)
                    if not hit:
                        continue
                    st_name, dist_name, blk_name = hit
                    is_dup = any(abs(p.get("lat", 0) - lat) < 1e-6 and abs(p.get("lon", 0) - lon) < 1e-6 for p in saved_points)
                    if is_dup:
                        continue
                    entry = {"lat": lat, "lon": lon, "label": label, "district": dist_name, "state": st_name}
                    if is_block and blk_name:
                        entry["block"] = blk_name
                    saved_points.append(entry)
                    added += 1
                st.session_state["point_query_points"] = saved_points
                st.success(f"Saved {added} point(s)")
                st.rerun()

        preview_rows = st.session_state.get("point_query_batch_preview", [])
        if preview_rows:
            st.markdown("**Preview**")
            st.dataframe(preview_rows, hide_index=True, use_container_width=True)

    st.markdown("---")
    st.markdown("#### Saved points")

    if saved_points:
        display_rows = []
        for p in saved_points:
            row = {
                "Label": p.get("label") or "",
                "Lat": p.get("lat"),
                "Lon": p.get("lon"),
                "State": p.get("state"),
                "District": p.get("district"),
            }
            if is_block:
                row["Block"] = p.get("block", "")
            display_rows.append(row)

        st.dataframe(display_rows, hide_index=True, use_container_width=True)

        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Add all to portfolio", key="btn_add_all_saved_points", type="primary", use_container_width=True):
                added = 0
                for pt in saved_points:
                    st_name = pt.get("state")
                    dist_name = pt.get("district")
                    blk_name = pt.get("block")
                    if not st_name or not dist_name:
                        continue
                    try:
                        if is_block and blk_name:
                            portfolio_add_fn(st_name, dist_name, blk_name)
                        else:
                            portfolio_add_fn(st_name, dist_name)
                        added += 1
                    except TypeError:
                        portfolio_add_fn(st_name, dist_name)
                        added += 1
                portfolio_set_flash_fn(f"Added {added} item(s) to portfolio", "success")
                st.rerun()

        with col2:
            if st.button("Show on map", key="_points_show_map", use_container_width=True):
                markers = []
                for pt in saved_points:
                    lat = pt.get("lat")
                    lon = pt.get("lon")
                    if lat is None or lon is None:
                        continue
                    markers.append(
                        {
                            "lat": lat,
                            "lon": lon,
                            "label": pt.get("label"),
                            "district": pt.get("district"),
                            "state": pt.get("state"),
                            "block": pt.get("block"),
                        }
                    )

                if markers:
                    st.session_state["map_preview_markers"] = markers
                    portfolio_set_flash_fn(f"Showing {len(markers)} saved point(s) on map", "info")
                    st.session_state["jump_to_map"] = True
                    st.rerun()

        with col3:
            if st.button("Clear all", key="btn_clear_saved_points", use_container_width=True):
                st.session_state["point_query_points"] = []
                st.session_state["map_preview_markers"] = None
                st.rerun()
    return False

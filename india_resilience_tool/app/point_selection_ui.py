"""
Point selection UI for IRT portfolio mode.

This module renders the "Saved points" panel shown when:
- analysis_mode is "Multi-district portfolio"
- portfolio_route is "saved_points"

It includes:
- Coordinate input (lat/lon)
- Show on map / Save point buttons
- Map selection mode toggle
- Saved points list with portfolio integration

Widget keys preserved:
- btn_use_latlon
- btn_save_point
- btn_select_on_map
- btn_clear_point
- btn_clear_saved_points
- btn_points_to_portfolio

Session state keys used:
- point_query_lat
- point_query_lon
- point_query_latlon
- point_query_points
- point_query_select_on_map
- jump_to_map
- jump_to_rankings
- active_view

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Any, Callable, Tuple

import pandas as pd
from shapely.geometry import Point


def render_point_selection_panel(
    *,
    merged: Any,  # GeoDataFrame with geometry
    portfolio_add_fn: Callable[[str, str], None],
    portfolio_key_fn: Callable[[str, str], Tuple[str, str]],
    portfolio_set_flash_fn: Callable[[str, str], None],
) -> bool:
    """
    Render the point selection panel for the "saved points" route in portfolio mode.

    Args:
        merged: GeoDataFrame with district geometries
        portfolio_add_fn: Function to add a district to portfolio
        portfolio_key_fn: Function to create a normalized key for (state, district)
        portfolio_set_flash_fn: Function to set a flash message (message, level)

    Returns:
        clear_clicked: True if the "Clear active point" button was clicked
    """
    import streamlit as st

    clear_clicked = False

    # Container for multi-point saved list
    if "point_query_points" not in st.session_state:
        st.session_state["point_query_points"] = []

    # --- Get bounds for coordinate validation ---
    try:
        minx, miny, maxx, maxy = merged.total_bounds
        default_lat = float((miny + maxy) / 2.0)
        default_lon = float((minx + maxx) / 2.0)
    except Exception:
        # Fallback to broad defaults if geometry bounds are unavailable
        miny, maxy = -90.0, 90.0
        minx, maxx = -180.0, 180.0
        default_lat, default_lon = 20.0, 78.0

    st.subheader("Saved points")

    with st.expander("📍 Point selection", expanded=True):
        st.caption(
            "Choose locations either by typing coordinates or by clicking on the map, "
            "and optionally save them as candidate points for your portfolio."
        )

        # -------------------------------
        # Option 1 — Type coordinates
        # -------------------------------
        st.markdown("**Option 1 — Type coordinates**")

        col_lat, col_lon = st.columns(2)
        with col_lat:
            lat_input = st.number_input(
                "Latitude",
                min_value=float(miny),
                max_value=float(maxy),
                value=float(st.session_state.get("point_query_lat", default_lat)),
                format="%.4f",
            )
        with col_lon:
            lon_input = st.number_input(
                "Longitude",
                min_value=float(minx),
                max_value=float(maxx),
                value=float(st.session_state.get("point_query_lon", default_lon)),
                format="%.4f",
            )

        # Actions for the typed point
        col_set_active, col_save_point = st.columns(2)

        # 1) Show this point on the map (set active point)
        with col_set_active:
            if st.button("Show on map", key="btn_use_latlon"):
                try:
                    lat_f = float(lat_input)
                    lon_f = float(lon_input)
                except (TypeError, ValueError):
                    portfolio_set_flash_fn("Invalid latitude/longitude.", "warning")
                    st.rerun()

                st.session_state["point_query_lat"] = lat_f
                st.session_state["point_query_lon"] = lon_f
                st.session_state["point_query_latlon"] = {"lat": lat_f, "lon": lon_f}

                # Ensure the user is looking at the map
                st.session_state["jump_to_map"] = True
                st.session_state["jump_to_rankings"] = False
                st.session_state["active_view"] = "🗺 Map view"
                st.rerun()

        # 2) Add current typed point to saved list
        with col_save_point:
            if st.button("Save point", key="btn_save_point"):
                try:
                    lat_f = float(lat_input)
                    lon_f = float(lon_input)
                except (TypeError, ValueError):
                    lat_f, lon_f = None, None

                if lat_f is not None and lon_f is not None:
                    pts = st.session_state.get("point_query_points", [])
                    # Avoid exact duplicates
                    exists = any(
                        abs(p.get("lat") - lat_f) < 1e-6
                        and abs(p.get("lon") - lon_f) < 1e-6
                        for p in pts
                    )
                    if not exists:
                        pts.append({"lat": lat_f, "lon": lon_f})
                        st.session_state["point_query_points"] = pts

        st.markdown("---")

        # -------------------------------
        # Option 2 — Select from map
        # -------------------------------
        st.markdown("**Option 2 — Select from map**")

        col_pick_on_map, col_clear_point = st.columns(2)

        # 3) Enable one-shot map selection
        with col_pick_on_map:
            if st.button("Click on map to choose", key="btn_select_on_map"):
                st.session_state["point_query_select_on_map"] = True

        # 4) Clear current active point
        with col_clear_point:
            if st.button("Clear active point", key="btn_clear_point"):
                for _k in (
                    "point_query_lat",
                    "point_query_lon",
                    "point_query_latlon",
                    "point_query_select_on_map",
                ):
                    st.session_state.pop(_k, None)
                clear_clicked = True

        # Helper text when map-selection mode is active
        if st.session_state.get("point_query_select_on_map", False):
            st.info(
                "Map selection active: click once on the map to choose a point. "
                "The next click will set the point and turn off selection."
            )

        # ---- Saved multi-point list + portfolio glue ----
        saved_points = st.session_state.get("point_query_points", [])
        if saved_points:
            st.markdown("**Saved points for portfolio selection**")
            st.caption(
                "These points remember locations you care about. "
                "You can map them to districts and add those districts to the portfolio."
            )
            saved_points_df = pd.DataFrame(saved_points)
            saved_points_df.index = saved_points_df.index + 1
            st.dataframe(
                saved_points_df.rename(columns={"lat": "Latitude", "lon": "Longitude"}),
                use_container_width=True,
            )

            col_sp1, col_sp2 = st.columns(2)
            with col_sp1:
                if st.button("Clear saved points", key="btn_clear_saved_points"):
                    st.session_state["point_query_points"] = []

            with col_sp2:
                if st.button(
                    "Add saved points' districts to portfolio",
                    key="btn_points_to_portfolio",
                ):
                    _add_saved_points_to_portfolio(
                        merged=merged,
                        portfolio_add_fn=portfolio_add_fn,
                        portfolio_key_fn=portfolio_key_fn,
                        portfolio_set_flash_fn=portfolio_set_flash_fn,
                    )
        else:
            st.caption(
                "Use **Save point** to build a list of locations and then "
                "send their districts into the multi-district portfolio."
            )

    return clear_clicked


def _add_saved_points_to_portfolio(
    *,
    merged: Any,
    portfolio_add_fn: Callable[[str, str], None],
    portfolio_key_fn: Callable[[str, str], Tuple[str, str]],
    portfolio_set_flash_fn: Callable[[str, str], None],
) -> None:
    """Add districts corresponding to saved points to the portfolio."""
    import streamlit as st

    pts = st.session_state.get("point_query_points", [])
    if not pts:
        portfolio_set_flash_fn(
            "No saved points found. Save at least one point first.",
            "warning",
        )
        st.rerun()

    # Track what was already in the portfolio
    before_items = st.session_state.get("portfolio_districts", [])
    before_keys = set()
    for it in before_items:
        if isinstance(it, dict):
            before_keys.add(portfolio_key_fn(it.get("state"), it.get("district")))

    added_new = 0

    for p in pts:
        plat = p.get("lat")
        plon = p.get("lon")
        if plat is None or plon is None:
            continue
        try:
            pt = Point(float(plon), float(plat))
        except (TypeError, ValueError):
            continue

        # Use geometry logic to find the district
        try:
            contains_mask = merged.geometry.contains(pt)
            if contains_mask.any():
                row = merged[contains_mask].iloc[0]
            else:
                centroids = merged.geometry.centroid
                dists = centroids.distance(pt)
                idx = dists.idxmin()
                row = merged.loc[idx]
        except Exception:
            continue

        state_name = str(row.get("state_name", "")).strip()
        district_name = str(row.get("district_name", "")).strip()
        if not (state_name and district_name):
            continue

        k = portfolio_key_fn(state_name, district_name)
        if k not in before_keys:
            before_keys.add(k)
            added_new += 1

        portfolio_add_fn(state_name, district_name)

    if added_new > 0:
        portfolio_set_flash_fn(
            f"Added {added_new} new district(s) to the portfolio from saved points.",
            "success",
        )
    else:
        portfolio_set_flash_fn(
            "No new districts were added (they may already be in the portfolio).",
            "info",
        )

    # Force a rerun so the Portfolio analysis panel re-renders
    st.rerun()
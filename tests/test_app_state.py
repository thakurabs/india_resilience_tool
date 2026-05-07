"""
Unit tests for app.state defaults.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app.state import ensure_session_state


def test_ensure_session_state_sets_defaults() -> None:
    ss = {}
    ensure_session_state(ss)
    assert ss["analysis_mode"] == "Single district focus"
    assert ss["active_view"] == "Map view"
    assert ss["main_view_selector"] == "Map view"
    assert ss["landing_active"] is True
    assert ss["landing_bundle"] == "Heat Risk"
    assert ss["landing_scenario"] == "ssp585"
    assert ss["landing_period"] == "2040-2060"
    assert ss["landing_focus_level"] == "india"
    assert ss["landing_selected_state"] is None
    assert ss["landing_selected_district"] is None
    assert ss["landing_selected_block"] is None
    assert ss["landing_tab"] == "Rankings"
    assert isinstance(ss["portfolio_districts"], list)
    assert isinstance(ss["portfolio_blocks"], list)
    assert isinstance(ss["portfolio_basins"], list)
    assert isinstance(ss["portfolio_subbasins"], list)
    assert ss["jump_to_rankings"] is False
    assert ss["jump_to_map"] is False
    assert ss["landing_search"] == ""
    assert ss["landing_search_selection"] is None
    assert ss["landing_search_last_applied"] is None
    assert ss["landing_search_reset_pending"] is False
    assert ss["landing_context_pair"] == ("ssp585", "2040-2060")
    assert isinstance(ss["landing_compare_selection"], list)
    assert ss["crosswalk_overlay"] is None
    assert ss["hydro_admin_context_level"] == "district"
    assert ss["overlay_rp100_flood_depth_raster_enabled"] is False
    assert ss["overlay_rp100_flood_depth_raster_opacity_pct"] == 65
    assert ss["overlay_rural_facilities_density_enabled"] is False
    assert ss["overlay_rural_facilities_density_opacity_pct"] == 55
    assert ss["overlay_rural_facilities_density_category"] == "total"
    assert ss["overlay_built_up_area_current_raster_enabled"] is False
    assert ss["overlay_built_up_area_current_raster_opacity_pct"] == 55
    assert ss["overlay_river_network_enabled"] is False
    assert ss["overlay_river_network_opacity_pct"] == 75
    assert ss["_pending_crosswalk_navigation"] is None


def test_ensure_session_state_does_not_override_existing() -> None:
    ss = {"analysis_mode": "Multi-district portfolio", "portfolio_districts": [("X", "Y")]}
    ensure_session_state(ss)
    assert ss["analysis_mode"] == "Multi-district portfolio"
    assert ss["portfolio_districts"] == [("X", "Y")]

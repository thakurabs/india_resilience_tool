"""Focused tests for map-pipeline performance guards and non-spatial helpers."""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.app.map_pipeline import (
    _build_nonspatial_details_source_df,
    _filter_frame_by_selection_value,
    _stack_legend_blocks,
    blocked_drilldown_message,
    details_require_geometry,
)
from india_resilience_tool.app.overlays import RP100_FLOOD_DEPTH_BINS
from india_resilience_tool.viz.colors import build_rp100_flood_depth_legend_html


def test_blocked_drilldown_message_requires_narrowing_for_fine_grain_views() -> None:
    assert (
        blocked_drilldown_message(
            adm_level="block",
            spatial_family="admin",
            selected_state="All",
            selected_basin="All",
        )
        == "Select a state to render block maps and rankings."
    )
    assert (
        blocked_drilldown_message(
            adm_level="sub_basin",
            spatial_family="hydro",
            selected_state="All",
            selected_basin="All",
        )
        == "Select a basin to render sub-basin maps and rankings."
    )
    assert (
        blocked_drilldown_message(
            adm_level="district",
            spatial_family="admin",
            selected_state="All",
            selected_basin="All",
        )
        is None
    )


def test_details_require_geometry_only_for_summary_flows() -> None:
    assert details_require_geometry(
        adm_level="district",
        spatial_family="admin",
        selected_state="Telangana",
        selected_district="All",
        selected_block="All",
        selected_basin="All",
        selected_subbasin="All",
    )
    assert not details_require_geometry(
        adm_level="district",
        spatial_family="admin",
        selected_state="Telangana",
        selected_district="Hyderabad",
        selected_block="All",
        selected_basin="All",
        selected_subbasin="All",
    )
    assert details_require_geometry(
        adm_level="block",
        spatial_family="admin",
        selected_state="Telangana",
        selected_district="Hyderabad",
        selected_block="All",
        selected_basin="All",
        selected_subbasin="All",
    )
    assert not details_require_geometry(
        adm_level="sub_basin",
        spatial_family="hydro",
        selected_state="All",
        selected_district="All",
        selected_block="All",
        selected_basin="Godavari",
        selected_subbasin="Sabari",
    )


def test_build_nonspatial_details_source_df_normalizes_admin_and_hydro_columns() -> None:
    admin_df = pd.DataFrame(
        {
            "state": ["Telangana"],
            "district": ["Hyderabad"],
            "block": ["Shaikpet"],
            "value": [1.0],
        }
    )
    admin_out = _build_nonspatial_details_source_df(admin_df, level="block", spatial_family="admin")

    assert admin_out.columns.tolist() == ["state_name", "district_name", "block_name", "value"]
    assert admin_out.loc[0, "state_name"] == "Telangana"
    assert admin_out.loc[0, "district_name"] == "Hyderabad"
    assert admin_out.loc[0, "block_name"] == "Shaikpet"

    hydro_df = pd.DataFrame({"basin_name": ["Godavari"], "value": [2.0]})
    hydro_out = _build_nonspatial_details_source_df(hydro_df, level="basin", spatial_family="hydro")

    assert hydro_out.loc[0, "state_name"] == "Hydro"
    assert hydro_out.loc[0, "basin_name"] == "Godavari"


def test_filter_frame_by_selection_value_handles_case_and_alias_mismatch() -> None:
    df = pd.DataFrame(
        {
            "district_name": ["Adilabad", "Nirmal"],
            "block_name": ["Adilabad Rural", "Laxmanchanda"],
            "basin_name": ["Upper Godavari", "Krishna"],
        }
    )

    district_out = _filter_frame_by_selection_value(
        df,
        column="district_name",
        selected_value="ADILABAD",
    )
    assert district_out["district_name"].tolist() == ["Adilabad"]

    block_out = _filter_frame_by_selection_value(
        df,
        column="block_name",
        selected_value="ADILABAD RURAL",
    )
    assert block_out["block_name"].tolist() == ["Adilabad Rural"]

    basin_out = _filter_frame_by_selection_value(
        df,
        column="basin_name",
        selected_value="upper godavari",
    )
    assert basin_out["basin_name"].tolist() == ["Upper Godavari"]


def test_stack_legend_blocks_keeps_both_legend_fragments() -> None:
    html = _stack_legend_blocks('<div id="main"></div>', '<div id="rp100"></div>', map_height=560)

    assert 'id="main"' in html
    assert 'id="rp100"' in html
    assert "flex-direction:row" in html
    assert "gap:28px" in html
    assert "height:560px" in html


def test_rp100_overlay_legend_uses_responsive_map_height() -> None:
    html = build_rp100_flood_depth_legend_html(bins=RP100_FLOOD_DEPTH_BINS, map_height=560)

    assert "height: 419px" in html

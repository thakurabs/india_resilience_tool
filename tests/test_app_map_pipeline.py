"""Focused tests for map-pipeline performance guards and non-spatial helpers."""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.app.map_pipeline import (
    _build_map_render_signature,
    _build_nonspatial_details_source_df,
    _filter_frame_by_selection_value,
    blocked_drilldown_message,
    details_require_geometry,
)


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


def test_build_map_render_signature_ignores_overlay_and_river_context_changes() -> None:
    base = _build_map_render_signature(
        level="block",
        selected_state="Telangana",
        selected_district="Adilabad",
        selected_block="All",
        selected_basin="All",
        selected_subbasin="All",
        metric_col="tas__ssp245",
        map_value_col="_delta_abs",
        baseline_col="tas__baseline",
        map_mode="Change from 1990-2010 baseline",
        hover_enabled=True,
        crosswalk_overlay={"level": "district", "feature_keys": ["Telangana::Adilabad"]},
        show_river_network=False,
        resolved_river_basin_name=None,
    )
    overlay_changed = _build_map_render_signature(
        level="block",
        selected_state="Telangana",
        selected_district="Adilabad",
        selected_block="All",
        selected_basin="All",
        selected_subbasin="All",
        metric_col="tas__ssp245",
        map_value_col="_delta_abs",
        baseline_col="tas__baseline",
        map_mode="Change from 1990-2010 baseline",
        hover_enabled=True,
        crosswalk_overlay={"level": "district", "feature_keys": ["Telangana::Nirmal"]},
        show_river_network=False,
        resolved_river_basin_name=None,
    )
    river_changed = _build_map_render_signature(
        level="block",
        selected_state="Telangana",
        selected_district="Adilabad",
        selected_block="All",
        selected_basin="All",
        selected_subbasin="All",
        metric_col="tas__ssp245",
        map_value_col="_delta_abs",
        baseline_col="tas__baseline",
        map_mode="Change from 1990-2010 baseline",
        hover_enabled=True,
        crosswalk_overlay={"level": "district", "feature_keys": ["Telangana::Adilabad"]},
        show_river_network=True,
        resolved_river_basin_name="Godavari",
    )

    assert base == overlay_changed
    assert base == river_changed

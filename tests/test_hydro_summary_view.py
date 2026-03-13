"""Regression tests for hydro summary routing and aggregation."""

from __future__ import annotations

import pandas as pd


def test_resolve_summary_target_uses_hydro_basin_summary_for_subbasin_all() -> None:
    from india_resilience_tool.app.details_runtime import _resolve_summary_target

    target = _resolve_summary_target(
        admin_level="sub_basin",
        spatial_family="hydro",
        selected_state="All",
        selected_district="All",
        selected_block="All",
        selected_basin="Godavari Basin",
        selected_subbasin="All",
    )
    assert target == "hydro_basin"


def test_resolve_summary_target_keeps_admin_state_summary() -> None:
    from india_resilience_tool.app.details_runtime import _resolve_summary_target

    target = _resolve_summary_target(
        admin_level="district",
        spatial_family="admin",
        selected_state="Telangana",
        selected_district="All",
        selected_block="All",
        selected_basin="All",
        selected_subbasin="All",
    )
    assert target == "state"


def test_build_hydro_scenario_panel_uses_subbasin_area_weights() -> None:
    from india_resilience_tool.app.views.hydro_summary_view import (
        _build_hydro_scenario_panel,
        _with_hydro_weights,
    )

    df = pd.DataFrame(
        {
            "basin_name": ["Demo Basin", "Demo Basin"],
            "subbasin_name": ["A", "B"],
            "subbasin_area_km2": [1.0, 3.0],
            "aq_water_stress__historical__1979-2019__mean": [1.0, 5.0],
            "aq_water_stress__opt__2080__mean": [2.0, 10.0],
        }
    )

    weighted = _with_hydro_weights(df)
    panel_df = _build_hydro_scenario_panel(
        df=weighted,
        base_metric="aq_water_stress",
        sel_stat="mean",
    )

    assert panel_df.shape[0] == 2
    hist = panel_df.loc[panel_df["scenario"].astype(str) == "historical", "value"].iloc[0]
    opt_2080 = panel_df.loc[panel_df["scenario"].astype(str) == "opt", "value"].iloc[0]

    assert hist == 4.0
    assert opt_2080 == 8.0

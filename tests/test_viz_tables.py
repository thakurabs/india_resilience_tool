"""
Unit tests for viz.tables.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.viz.tables import build_rankings_table_df


def _risk(p: float) -> str:
    return "Very High" if p >= 80 else "Other"


def test_build_rankings_table_filters_state_and_ranks() -> None:
    merged = pd.DataFrame(
        {
            "district_name": ["A", "B", "C"],
            "state_name": ["Telangana", "Telangana", "Karnataka"],
            "m": [10.0, 30.0, 999.0],
        }
    )

    table, has_base = build_rankings_table_df(
        merged,
        metric_col="m",
        baseline_col=None,
        selected_state="Telangana",
        risk_class_from_percentile=_risk,
    )

    assert has_base is False
    assert table.shape[0] == 2
    assert set(table["district_name"]) == {"A", "B"}

    # B (30) should rank 1, A (10) rank 2
    b_rank = int(table.loc[table["district_name"] == "B", "rank_value"].iloc[0])
    a_rank = int(table.loc[table["district_name"] == "A", "rank_value"].iloc[0])
    assert b_rank == 1
    assert a_rank == 2


def test_build_rankings_table_with_baseline_and_delta_rank() -> None:
    merged = pd.DataFrame(
        {
            "district_name": ["A", "B"],
            "state_name": ["Telangana", "Telangana"],
            "cur": [20.0, 12.0],
            "base": [10.0, 10.0],
        }
    )

    table, has_base = build_rankings_table_df(
        merged,
        metric_col="cur",
        baseline_col="base",
        selected_state="Telangana",
        risk_class_from_percentile=_risk,
    )

    assert has_base is True
    assert "delta_abs" in table.columns
    assert "delta_pct" in table.columns
    assert "rank_delta" in table.columns

    # A: delta 10, B: delta 2 => A rank_delta 1
    a_rank_delta = int(table.loc[table["district_name"] == "A", "rank_delta"].iloc[0])
    b_rank_delta = int(table.loc[table["district_name"] == "B", "rank_delta"].iloc[0])
    assert a_rank_delta == 1
    assert b_rank_delta == 2


def test_build_rankings_table_preserves_hydro_context_columns() -> None:
    merged = pd.DataFrame(
        {
            "subbasin_name": ["Pranhita", "Wardha"],
            "subbasin_id": ["SB01", "SB02"],
            "basin_name": ["Godavari", "Godavari"],
            "basin_id": ["B01", "B01"],
            "state_name": ["Hydro", "Hydro"],
            "m": [20.0, 10.0],
        }
    )

    table, has_base = build_rankings_table_df(
        merged,
        metric_col="m",
        baseline_col=None,
        selected_state="All",
        risk_class_from_percentile=_risk,
        district_col="subbasin_name",
        state_col="state_name",
        extra_cols=["basin_name", "basin_id", "subbasin_id"],
    )

    assert has_base is False
    assert set(["subbasin_name", "basin_name", "basin_id", "subbasin_id"]).issubset(table.columns)
    assert table.loc[table["subbasin_name"] == "Pranhita", "basin_name"].iloc[0] == "Godavari"
    assert table.loc[table["subbasin_name"] == "Wardha", "subbasin_id"].iloc[0] == "SB02"

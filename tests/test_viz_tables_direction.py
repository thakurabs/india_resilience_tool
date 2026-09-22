"""
Direction-aware ranking tests for viz.tables.build_rankings_table_df.

The default (higher_is_worse=True) must preserve the historical
"1 = highest" behavior used by every non-Cold-Risk bundle.
The cold-magnitude branch (higher_is_worse=False) flips ranking
to ascending and inverts percentile.
"""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.viz.tables import build_rankings_table_df


def _risk(p: float) -> str:
    return "Very High" if p >= 80 else "Other"


def _three_district_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "district_name": ["A", "B", "C"],
            "state_name": ["Telangana", "Telangana", "Telangana"],
            "m": [10.0, 20.0, 30.0],
        }
    )


def test_higher_is_worse_default_unchanged() -> None:
    """Default branch matches pre-Phase-5 behavior: highest value -> rank 1."""
    merged = _three_district_frame()
    table, _ = build_rankings_table_df(
        merged,
        metric_col="m",
        baseline_col=None,
        selected_state="Telangana",
        risk_class_from_percentile=_risk,
    )
    by_name = table.set_index("district_name")["rank_value"].to_dict()
    assert by_name == {"C": 1, "B": 2, "A": 3}


def test_lower_is_worse_flips_rank_and_percentile() -> None:
    """For cold-magnitude metrics, lowest value -> rank 1 (worst)."""
    merged = _three_district_frame()
    table, _ = build_rankings_table_df(
        merged,
        metric_col="m",
        baseline_col=None,
        selected_state="Telangana",
        risk_class_from_percentile=_risk,
        higher_is_worse=False,
    )
    by_name = table.set_index("district_name")["rank_value"].to_dict()
    assert by_name == {"A": 1, "B": 2, "C": 3}

    # Percentile inverts: smallest value gets the highest (worst) percentile.
    pct = table.set_index("district_name")["percentile_value"].to_dict()
    assert pct["A"] > pct["B"] > pct["C"]


def test_baseline_delta_rank_respects_direction() -> None:
    """When lower is worse, the most-negative delta is worst."""
    merged = pd.DataFrame(
        {
            "district_name": ["A", "B"],
            "state_name": ["Telangana", "Telangana"],
            "cur": [8.0, 12.0],
            "base": [10.0, 10.0],
        }
    )
    table, _ = build_rankings_table_df(
        merged,
        metric_col="cur",
        baseline_col="base",
        selected_state="Telangana",
        risk_class_from_percentile=_risk,
        higher_is_worse=False,
    )
    by_name = table.set_index("district_name")["rank_delta"].to_dict()
    # A: delta -2 (worse for cold), B: delta +2 → A gets rank 1.
    assert by_name == {"A": 1, "B": 2}


def test_heat_regression_no_bleed() -> None:
    """A pre-Phase-5 caller (no higher_is_worse kwarg) must see identical
    rank/percentile output for a representative heat-style metric."""
    merged = pd.DataFrame(
        {
            "district_name": ["A", "B", "C", "D"],
            "state_name": ["Telangana"] * 4,
            "m": [25.0, 40.0, 35.0, 30.0],
        }
    )
    table, _ = build_rankings_table_df(
        merged,
        metric_col="m",
        baseline_col=None,
        selected_state="Telangana",
        risk_class_from_percentile=_risk,
    )
    rank_by_name = table.set_index("district_name")["rank_value"].to_dict()
    assert rank_by_name == {"B": 1, "C": 2, "D": 3, "A": 4}
    # Percentile in default branch must remain monotonic-with-value (ascending).
    pct = table.set_index("district_name")["percentile_value"].to_dict()
    assert pct["B"] > pct["C"] > pct["D"] > pct["A"]

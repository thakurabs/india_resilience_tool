"""
Unit tests for analysis.metrics helpers.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.analysis.metrics import (
    compute_percentile_in_state,
    compute_rank_and_percentile,
    compute_rank_descending,
    risk_class_from_percentile,
)


def test_risk_class_mapping() -> None:
    assert risk_class_from_percentile(float("nan")) == "Unknown"
    assert risk_class_from_percentile(80.0) == "Very High"
    assert risk_class_from_percentile(60.0) == "High"
    assert risk_class_from_percentile(40.0) == "Medium"
    assert risk_class_from_percentile(20.0) == "Low"
    assert risk_class_from_percentile(0.0) == "Very Low"


def test_percentile_methods() -> None:
    vals = pd.Series([1, 2, 3, 4, 5])
    assert compute_percentile_in_state(vals, 3, method="le") == 60.0  # <=3 : 3/5
    assert compute_percentile_in_state(vals, 3, method="lt") == 40.0  # <3  : 2/5


def test_rank_descending() -> None:
    vals = pd.Series([10, 20, 30, 40])
    assert compute_rank_descending(vals, 40) == 1
    assert compute_rank_descending(vals, 30) == 2
    assert compute_rank_descending(vals, 10) == 4


def test_compute_rank_and_percentile_filters_state() -> None:
    df = pd.DataFrame(
        {
            "state": ["Telangana", "Telangana", "Karnataka"],
            "m": [10.0, 30.0, 999.0],
        }
    )

    rank, pct = compute_rank_and_percentile(df, "Telangana", "m", 10.0, percentile_method="le")
    assert rank == 2  # (30 > 10) + 1
    assert pct == 50.0  # (<=10) : 1/2

"""
Unit tests for the shared area-weighting helpers.

These guard the single definition of the state headline KPI's area-weighted
mean so the live view and the offline precompute tool cannot drift apart.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from india_resilience_tool.analysis.area_weighting import (
    AREA_WEIGHT_COL,
    weighted_state_mean,
    with_area_weights,
)


def test_with_area_weights_prefers_area_m2() -> None:
    df = pd.DataFrame({"v": [1.0, 2.0], "area_m2": [10.0, 30.0]})
    out = with_area_weights(df)
    assert AREA_WEIGHT_COL in out.columns
    assert out[AREA_WEIGHT_COL].tolist() == [10.0, 30.0]


def test_with_area_weights_prefers_existing_weight_column() -> None:
    df = pd.DataFrame({"v": [1.0], AREA_WEIGHT_COL: [42.0], "area_m2": [1.0]})
    out = with_area_weights(df)
    # An already-attached weight column wins over a raw area_m2 column.
    assert out[AREA_WEIGHT_COL].tolist() == [42.0]


def test_with_area_weights_none_returns_empty() -> None:
    assert with_area_weights(None).empty


def test_weighted_state_mean_basic() -> None:
    df = with_area_weights(pd.DataFrame({"v": [1.0, 3.0], "area_m2": [10.0, 30.0]}))
    value, n = weighted_state_mean(df, "v")
    # (1*10 + 3*30) / 40 = 2.5
    assert value == pytest.approx(2.5)
    assert n == 2


def test_weighted_state_mean_drops_nan_value_and_nonpositive_area() -> None:
    df = with_area_weights(
        pd.DataFrame(
            {
                "v": [1.0, 3.0, np.nan, 5.0],
                "area_m2": [10.0, 30.0, 5.0, 0.0],  # last has area 0 -> dropped
            }
        )
    )
    value, n = weighted_state_mean(df, "v")
    # NaN value and area<=0 rows drop -> same as the basic case.
    assert value == pytest.approx(2.5)
    assert n == 2


def test_weighted_state_mean_value_and_count_share_one_mask() -> None:
    # n_units must reflect exactly the rows that contribute to value.
    df = with_area_weights(
        pd.DataFrame({"v": [np.nan, np.nan, 4.0], "area_m2": [10.0, 20.0, 5.0]})
    )
    value, n = weighted_state_mean(df, "v")
    assert value == pytest.approx(4.0)
    assert n == 1


def test_weighted_state_mean_empty_returns_none_zero() -> None:
    assert weighted_state_mean(pd.DataFrame(), "v") == (None, 0)


def test_weighted_state_mean_missing_column_returns_none_zero() -> None:
    df = with_area_weights(pd.DataFrame({"v": [1.0], "area_m2": [1.0]}))
    assert weighted_state_mean(df, "missing") == (None, 0)
    assert weighted_state_mean(df, None) == (None, 0)


def test_weighted_state_mean_all_nan_metric_returns_none_zero() -> None:
    # Coverage-cliff pattern (e.g. arid SPI): metric all-NaN but areas valid.
    df = with_area_weights(pd.DataFrame({"v": [np.nan, np.nan], "area_m2": [10.0, 20.0]}))
    assert weighted_state_mean(df, "v") == (None, 0)

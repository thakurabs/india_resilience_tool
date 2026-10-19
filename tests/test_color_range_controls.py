"""
Unit tests for app.color_range_controls.compute_color_range_defaults.

Regression guard for CHG-0120: a single-value fraction metric (display_scale=100)
must not produce a colorbar domain of [-99%, +101%]; and a non-negative metric must
never produce a negative lower bound. Backward-compat for unit-scale metrics and
diverging (negative-bearing) metrics is preserved.

Author: Abu Bakar Siddiqui Thakur
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from india_resilience_tool.app.color_range_controls import compute_color_range_defaults


def test_single_fraction_value_scale_100_has_no_negative_bound() -> None:
    # Mirrors the J&K RP-100 Flood Extent case: only Kathua (~0.0085 fraction) has a
    # value, all other districts NaN. display_scale=100 -> floor is 1% not 100%.
    vals = pd.Series([0.0085, np.nan, np.nan])
    data_min, data_max, vmin_default, vmax_default = compute_color_range_defaults(
        vals, display_scale=100.0
    )

    assert data_min == 0.0  # clamped to 0 (non-negative metric)
    assert vmin_default >= 0.0
    # padding floor 1/100 = 0.01 -> upper bound ~ value + 0.01 (well under 1.0 == 100%)
    assert data_max == 0.0085 + 0.01
    assert data_max < 0.05  # nowhere near the old +1.0 (=100%) blow-up


def test_single_unit_scale_value_preserves_floor_1() -> None:
    # Depth-style metric (display_scale=1.0): the legacy floor of 1.0 is retained.
    vals = pd.Series([8.32])
    data_min, data_max, _vmin, _vmax = compute_color_range_defaults(vals, display_scale=1.0)

    assert data_min == 8.32 - 1.0
    assert data_max == 8.32 + 1.0


def test_negative_bearing_series_not_clamped() -> None:
    # Diverging (baseline-change) data has real negatives -> lower bound preserved.
    vals = pd.Series([-3.0, -1.0, 2.0, 4.0])
    data_min, data_max, vmin_default, vmax_default = compute_color_range_defaults(
        vals, display_scale=1.0
    )

    assert data_min == -3.0
    assert data_max == 4.0
    assert vmin_default <= 0.0 < vmax_default


def test_multi_value_non_negative_unchanged() -> None:
    # Normal multi-district metric: bounds equal full data min/max (regression guard).
    vals = pd.Series([0.0, 0.1, 0.2, 0.3, 0.4])
    data_min, data_max, vmin_default, vmax_default = compute_color_range_defaults(
        vals, display_scale=100.0
    )

    assert data_min == 0.0
    assert data_max == 0.4
    assert 0.0 <= vmin_default <= vmax_default <= 0.4

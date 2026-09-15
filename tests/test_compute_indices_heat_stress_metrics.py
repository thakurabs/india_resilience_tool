"""
Unit tests for heat-stress compute helpers.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr


def _repo_root() -> Path:
    """Find repository root (assumes tests/ is directly under repo root)."""
    return Path(__file__).resolve().parents[1]


_ROOT = _repo_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.pipeline import compute_indices_multiprocess as CMP  # noqa: E402


def test_wet_bulb_seasonal_mean_stull_uses_requested_months(monkeypatch: pytest.MonkeyPatch) -> None:
    wet_bulb = xr.DataArray(
        np.array([20.0, 30.0, 40.0, 80.0], dtype=float),
        coords={"time": pd.date_range("2000-01-01", periods=4, freq="MS")},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_wet_bulb_daily_mean_c", lambda *args, **kwargs: wet_bulb)

    result = CMP.wet_bulb_seasonal_mean_stull(None, None, None, months=[1, 2, 3])

    assert result == pytest.approx(30.0)


def test_wet_bulb_depression_days_range_stull_counts_only_requested_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wbd = xr.DataArray(
        np.array([2.5, 3.0, 3.1, 4.5, 6.0, 6.1], dtype=float),
        coords={"time": pd.date_range("2000-01-01", periods=6, freq="D")},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_wet_bulb_depression_daily_mean_c", lambda *args, **kwargs: wbd)

    result = CMP.wet_bulb_depression_days_range_stull(
        None,
        None,
        None,
        lower_c=3.0,
        upper_c=6.0,
        lower_inclusive=False,
        upper_inclusive=True,
    )

    assert result == 3


def test_wet_bulb_depression_longest_run_le_threshold_stull_respects_min_spell_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wbd = xr.DataArray(
        np.array([2.0, 2.5, 3.5, 2.0, 2.0, 2.0, 4.0], dtype=float),
        coords={"time": pd.date_range("2000-01-01", periods=7, freq="D")},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_wet_bulb_depression_daily_mean_c", lambda *args, **kwargs: wbd)

    result = CMP.wet_bulb_depression_longest_run_le_threshold_stull(
        None,
        None,
        None,
        thresh_c=3.0,
        min_spell_days=3,
    )

    assert result == 3


def test_wet_bulb_days_ge_28_is_monotonic_relative_to_ge_30(monkeypatch: pytest.MonkeyPatch) -> None:
    twb = xr.DataArray(
        np.array([27.0, 28.0, 29.0, 30.0, 31.0], dtype=float),
        coords={"time": pd.date_range("2000-01-01", periods=5, freq="D")},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_wet_bulb_daily_mean_c", lambda *args, **kwargs: twb)

    ge_28 = CMP.wet_bulb_days_ge_threshold_stull(None, None, None, thresh_c=28.0)
    ge_30 = CMP.wet_bulb_days_ge_threshold_stull(None, None, None, thresh_c=30.0)

    assert ge_28 == 4
    assert ge_30 == 2
    assert ge_28 >= ge_30

def test_wbgt_shade_stull_annual_mean_uses_stull_wet_bulb_and_air_temperature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    time = pd.date_range("2000-01-01", periods=3, freq="D")
    twb = xr.DataArray(np.array([24.0, 26.0, 28.0], dtype=float), coords={"time": time}, dims=("time",))
    tas_k = xr.DataArray(
        np.array([30.0, 32.0, 34.0], dtype=float) + 273.15,
        coords={"time": time},
        dims=("time",),
    )

    monkeypatch.setattr(CMP, "_wet_bulb_daily_mean_c", lambda *args, **kwargs: twb)
    monkeypatch.setattr(CMP, "_get_district_daily_mean", lambda *args, **kwargs: tas_k)

    result = CMP.wbgt_shade_stull_annual_mean(None, None, None)

    expected_daily = 0.7 * np.array([24.0, 26.0, 28.0]) + 0.3 * np.array([30.0, 32.0, 34.0])
    assert result == pytest.approx(float(expected_daily.mean()))


def test_wbgt_shade_stull_threshold_counts_are_monotonic(monkeypatch: pytest.MonkeyPatch) -> None:
    time = pd.date_range("2000-01-01", periods=5, freq="D")
    wbgt = xr.DataArray(
        np.array([27.0, 28.0, 29.0, 30.0, 32.0], dtype=float),
        coords={"time": time},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_wbgt_shade_stull_daily_mean_c", lambda *args, **kwargs: wbgt)

    ge_28 = CMP.wbgt_shade_stull_days_ge_threshold(None, None, None, thresh_c=28.0)
    ge_30 = CMP.wbgt_shade_stull_days_ge_threshold(None, None, None, thresh_c=30.0)
    ge_32 = CMP.wbgt_shade_stull_days_ge_threshold(None, None, None, thresh_c=32.0)

    assert ge_28 == 4
    assert ge_30 == 2
    assert ge_32 == 1
    assert ge_28 >= ge_30 >= ge_32


def test_swbgt_empirical_annual_mean_uses_vapour_pressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    time = pd.date_range("2000-01-01", periods=2, freq="D")
    tas_c = np.array([30.0, 35.0], dtype=float)
    rh_pct = np.array([50.0, 60.0], dtype=float)
    tas_k = xr.DataArray(tas_c + 273.15, coords={"time": time}, dims=("time",))
    rh = xr.DataArray(rh_pct, coords={"time": time}, dims=("time",))

    def fake_daily_mean(da, mask):
        return tas_k if da == "tas" else rh

    monkeypatch.setattr(CMP, "_get_district_daily_mean", fake_daily_mean)

    result = CMP.swbgt_empirical_annual_mean("tas", "hurs", None)

    es = 6.112 * np.exp((17.67 * tas_c) / (tas_c + 243.5))
    e = (rh_pct / 100.0) * es
    expected = 0.567 * tas_c + 0.393 * e + 3.94
    assert result == pytest.approx(float(expected.mean()))


def test_swbgt_empirical_threshold_counts_treat_nan_as_non_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    time = pd.date_range("2000-01-01", periods=4, freq="D")
    swbgt = xr.DataArray(
        np.array([27.0, 28.0, np.nan, 30.0], dtype=float),
        coords={"time": time},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_swbgt_empirical_daily_mean_c", lambda *args, **kwargs: swbgt)

    ge_28 = CMP.swbgt_empirical_days_ge_threshold(None, None, None, thresh_c=28.0)
    ge_30 = CMP.swbgt_empirical_days_ge_threshold(None, None, None, thresh_c=30.0)

    assert ge_28 == 2
    assert ge_30 == 1
    assert ge_28 >= ge_30


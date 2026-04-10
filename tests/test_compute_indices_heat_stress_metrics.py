from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
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
        coords={"time": xr.date_range("2000-01-01", periods=4, freq="MS", use_cftime=True)},
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
        coords={"time": xr.date_range("2000-01-01", periods=6, freq="D", use_cftime=True)},
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
        coords={"time": xr.date_range("2000-01-01", periods=7, freq="D", use_cftime=True)},
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
        coords={"time": xr.date_range("2000-01-01", periods=5, freq="D", use_cftime=True)},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_wet_bulb_daily_mean_c", lambda *args, **kwargs: twb)

    ge_28 = CMP.wet_bulb_days_ge_threshold_stull(None, None, None, thresh_c=28.0)
    ge_30 = CMP.wet_bulb_days_ge_threshold_stull(None, None, None, thresh_c=30.0)

    assert ge_28 == 4
    assert ge_30 == 2
    assert ge_28 >= ge_30

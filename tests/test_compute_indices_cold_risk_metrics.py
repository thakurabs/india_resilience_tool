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


def test_seasonal_min_respects_requested_months() -> None:
    da = xr.DataArray(
        np.array([280.15, 270.15, 275.15, 260.15], dtype=float).reshape(4, 1, 1),
        coords={
            "time": xr.date_range("2000-01-01", periods=4, freq="MS", use_cftime=True),
            "lat": [17.0],
            "lon": [78.0],
        },
        dims=("time", "lat", "lon"),
    )
    mask = xr.DataArray(np.array([[True]]), coords={"lat": [17.0], "lon": [78.0]}, dims=("lat", "lon"))

    result = CMP.seasonal_min(da, mask, months=[1, 2, 3])

    assert result == pytest.approx(-3.0)


def test_count_days_le_threshold_is_inclusive(monkeypatch: pytest.MonkeyPatch) -> None:
    da = xr.DataArray(
        np.array([282.15, 283.15, 284.15], dtype=float),
        coords={"time": xr.date_range("2000-01-01", periods=3, freq="D", use_cftime=True)},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_get_district_daily_mean", lambda *args, **kwargs: da)

    result = CMP.count_days_le_threshold(None, None, thresh_k=283.15)

    assert result == 2


def test_longest_consecutive_run_le_threshold_returns_longest_streak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    da = xr.DataArray(
        np.array([284.15, 283.15, 282.15, 285.15, 281.15, 280.15, 279.15], dtype=float),
        coords={"time": xr.date_range("2000-01-01", periods=7, freq="D", use_cftime=True)},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_get_district_daily_mean", lambda *args, **kwargs: da)

    result = CMP.longest_consecutive_run_le_threshold(None, None, thresh_k=283.15, min_len=1)

    assert result == 3


def test_tnle5_is_monotonic_relative_to_tnle10(monkeypatch: pytest.MonkeyPatch) -> None:
    da = xr.DataArray(
        np.array([276.15, 278.15, 281.15, 283.15, 285.15], dtype=float),
        coords={"time": xr.date_range("2000-01-01", periods=5, freq="D", use_cftime=True)},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_get_district_daily_mean", lambda *args, **kwargs: da)

    le_5 = CMP.count_days_le_threshold(None, None, thresh_k=278.15)
    le_10 = CMP.count_days_le_threshold(None, None, thresh_k=283.15)

    assert le_5 == 2
    assert le_10 == 4
    assert le_5 <= le_10

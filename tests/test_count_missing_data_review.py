"""F-05 diagnostic contract: known defects are strict xfails, not accepted zeros.

Synthetic daily inputs exercise production reducers without changing calculations.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from india_resilience_tool.compute import cold_risk_gridfirst as cold
from india_resilience_tool.compute import heat_risk_gridfirst as heat
from india_resilience_tool.compute import heat_stress_gridfirst as stress
from india_resilience_tool.compute.gridfirst_spatial import aggregate_cell_values, subcell_idw_fill

ROUTES = ["heat_ge", "heat_gt", "heat_le", "heat_lt", "cold_le", "cold_lt", "cold_run", "night28", "twb28", "twb30"]


def _daily(values: list[float] | np.ndarray) -> xr.DataArray:
    values = np.asarray(values, dtype=float)
    if values.ndim == 1:
        values = values[:, None, None]
    return xr.DataArray(values, dims=("time", "lat", "lon"), coords={
        "time": pd.date_range("2021-01-01", periods=len(values)),
        "lat": np.arange(values.shape[1]) + 10.0,
        "lon": np.arange(values.shape[2]) + 72.0,
    })


def _reduce(route: str, values: xr.DataArray, monkeypatch: pytest.MonkeyPatch) -> xr.DataArray:
    """Use zero-relative temperatures so the same fixtures cover every route."""
    if route in {"night28", "twb28", "twb30"}:
        threshold = 30.0 if route == "twb30" else 28.0
        slug = "tasmin_tropical_nights_gt28" if route == "night28" else f"twb_days_ge_{int(threshold)}"
        monkeypatch.setattr(stress, "_concat_year_var", lambda *a, **k: values + threshold + 273.15)
        monkeypatch.setattr(stress, "_twb_daily_for_year", lambda *a, **k: values + threshold)
        return stress._cell_values_for_metric({"slug": slug, "params": {"thresh_c": threshold}}, {}, 2021)
    mode = route.split("_")[1]
    compute = {"ge": "count_days_ge_threshold", "gt": "count_days_above_threshold", "le": "count_days_le_threshold", "lt": "count_days_below_threshold", "run": "longest_consecutive_run_le_threshold"}[mode]
    metric = {"slug": route, "compute": compute, "params": {"thresh_k": 273.15}}
    if route.startswith("cold"):
        return cold._cold_risk_cell_values(metric=metric, eval_da=values + 273.15, prev_da=None, threshold=None)
    return heat._metric_cell_values(metric=metric, eval_da=values + 273.15, threshold=None)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("values", [[], [np.nan], [np.nan] * 365], ids=["empty", "single_missing", "all_year_missing"])
@pytest.mark.xfail(strict=True, raises=AssertionError, reason="F-05: missing count/run inputs currently reduce to zero")
def test_no_observations_must_remain_missing(route, values, monkeypatch):
    assert np.isnan(_reduce(route, _daily(values), monkeypatch).item())


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("case", ["no_events", "all_events", "single", "partial", "threshold", "extremes"])
def test_observed_counts_and_run_boundaries(route, case, monkeypatch):
    low = route in {"heat_le", "heat_lt", "cold_le", "cold_lt", "cold_run"}
    event = -2.0 if low else 2.0
    non_event = -event
    strict = route in {"heat_gt", "heat_lt", "cold_lt", "night28"}
    fixtures = {
        "no_events": ([non_event] * 5, 0),
        "all_events": ([event] * 5, 5),
        "single": ([event], 1),
        "partial": ([event, np.nan, event, event], 2 if route == "cold_run" else 3),
        "threshold": ([0.0], 0 if strict else 1),
        "extremes": ([-100.0, 100.0], 1),
    }
    values, expected = fixtures[case]
    assert _reduce(route, _daily(values), monkeypatch).item() == expected


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.xfail(strict=True, raises=AssertionError, reason="F-05: zero from a missing cell suppresses IDW fallback")
def test_missing_island_cell_allows_finite_donor_fill(route, monkeypatch):
    field = _reduce(route, _daily(np.array([[[np.nan, 2.0], [2.0, 2.0]]])), monkeypatch)
    weights = pd.DataFrame({"unit_key": ["island"], "cell_index": [0], "lat_index": [0], "lon_index": [0], "area_m2": [100.0]})
    assert "island" in subcell_idw_fill(field, weights)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.xfail(strict=True, raises=AssertionError, reason="F-05: missing-cell zero biases mixed polygon area mean")
def test_missing_cell_does_not_dilute_polygon_count(route, monkeypatch):
    event = -2.0 if route in {"heat_le", "heat_lt", "cold_le", "cold_lt", "cold_run"} else 2.0
    field = _reduce(route, _daily(np.array([[[np.nan, event]]])), monkeypatch)
    weights = pd.DataFrame({"unit_key": ["coast", "coast"], "cell_index": [0, 1], "area_m2": [1.0, 1.0]})
    assert aggregate_cell_values(field, weights)["coast"] == 1.0


def test_nan_field_idw_control_and_no_donor_control():
    field = _daily(np.array([[[np.nan, 3.0], [3.0, 3.0]]])).isel(time=0, drop=True)
    weights = pd.DataFrame({"unit_key": ["island"], "cell_index": [0], "lat_index": [0], "lon_index": [0], "area_m2": [100.0]})
    assert subcell_idw_fill(field, weights)["island"] == pytest.approx(3.0)
    assert subcell_idw_fill(field * np.nan, weights) == {}
    assert np.isnan(aggregate_cell_values(field, weights)["island"])

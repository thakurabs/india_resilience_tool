import numpy as np
import pandas as pd
import pytest
import xarray as xr

from india_resilience_tool.compute.drought_risk_gridfirst import (
    aggregate_grid_values_with_retention,
    annual_spi_metric_grid,
    period_rollup_grid,
)


def _spi(values, start="2000-01-01"):
    time = pd.date_range(start, periods=len(values), freq="MS")
    arr = np.asarray(values, dtype=float).reshape(len(values), 1, 1)
    return xr.DataArray(arr, coords={"time": time, "lat": [0.0], "lon": [77.0]}, dims=("time", "lat", "lon"))


def test_year_boundary_spell_is_truncated_to_calendar_year():
    values = [1] * 9 + [-2] * 3 + [-2] * 9 + [1] * 3
    annual = annual_spi_metric_grid(
        _spi(values),
        annual_aggregation="max_spell_lt",
        threshold=-1.0,
        min_months_per_year=9,
        min_event_months=1,
    )["value"]

    assert float(annual.sel(year=2000).item()) == 3.0
    assert float(annual.sel(year=2001).item()) == 9.0

    period = period_rollup_grid(
        annual,
        period_name="2000-2001",
        years=(2000, 2001),
        rollup="period_max",
        min_years_per_period_fraction=0.75,
    )
    assert float(period["value"].item()) == 9.0
    assert float(period["value"].item()) != 12.0


def test_period_max_before_polygon_aggregation_is_non_commutative():
    annual = xr.DataArray(
        np.asarray([[[3.0, 9.0]], [[8.0, 2.0]], [[2.0, 4.0]]]),
        coords={"year": [2000, 2001, 2002], "lat": [0.0], "lon": [77.0, 78.0]},
        dims=("year", "lat", "lon"),
    )
    weights = pd.DataFrame(
        {
            "unit_key": ["A", "A"],
            "cell_index": [0, 1],
            "area_m2": [0.5, 0.5],
        }
    )
    period = period_rollup_grid(
        annual,
        period_name="2000-2002",
        years=(2000, 2002),
        rollup="period_max",
        min_years_per_period_fraction=0.75,
    )
    out = aggregate_grid_values_with_retention(period["value"], weights)
    assert out["A"][0] == 8.5


def test_nan_cells_are_renormalized_with_retained_weight_floor():
    values = xr.DataArray(
        np.asarray([[6.0, np.nan, 9.0]]),
        coords={"lat": [0.0], "lon": [77.0, 78.0, 79.0]},
        dims=("lat", "lon"),
    )
    weights = pd.DataFrame(
        {
            "unit_key": ["A", "A", "A"],
            "cell_index": [0, 1, 2],
            "area_m2": [0.4, 0.4, 0.2],
        }
    )
    out = aggregate_grid_values_with_retention(values, weights, min_polygon_cell_weight_fraction=0.50)
    assert out["A"][0] == pytest.approx(7.0)
    assert out["A"][1] == 0.6000000000000001


def test_spatial_weighted_mean_fixture():
    values = xr.DataArray(
        np.asarray([[4.0, 9.0]]),
        coords={"lat": [0.0], "lon": [77.0, 78.0]},
        dims=("lat", "lon"),
    )
    weights = pd.DataFrame(
        {
            "unit_key": ["A", "A"],
            "cell_index": [0, 1],
            "area_m2": [0.6, 0.4],
        }
    )
    out = aggregate_grid_values_with_retention(values, weights)
    assert out["A"][0] == 6.0

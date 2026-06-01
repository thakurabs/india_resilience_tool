from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from india_resilience_tool.compute.drought_risk_gridfirst import (
    _to_contiguous_monthly_index,
    _trim_to_full_calendar_years,
    aggregate_grid_counts,
    aggregate_grid_values_with_retention,
    annual_spi_metric_grid,
    compute_spi_grid,
    is_drought_gridfirst,
    period_rollup_grid,
)
from india_resilience_tool.compute.gridfirst_spatial import read_grid_metric_cache, write_grid_metric_cache


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


def test_polygon_year_counts_are_weighted_per_polygon():
    counts = xr.DataArray(
        np.asarray([[21.0, 9.0, 4.0]]),
        coords={"lat": [0.0], "lon": [77.0, 78.0, 79.0]},
        dims=("lat", "lon"),
    )
    weights = pd.DataFrame(
        {
            "unit_key": ["A", "A", "B"],
            "cell_index": [0, 1, 2],
            "area_m2": [0.5, 0.5, 1.0],
        }
    )
    out = aggregate_grid_counts(counts, weights)
    assert out["A"][0] == 15
    assert out["B"][0] == 4


def test_strict_threshold_equal_minus_one_does_not_count():
    annual = annual_spi_metric_grid(
        _spi([-1.0, -1.01, -0.5] + [0.0] * 9),
        annual_aggregation="count_events_lt",
        threshold=-1.0,
        min_months_per_year=9,
        min_event_months=1,
    )["value"]

    assert float(annual.sel(year=2000).item()) == 1.0


def test_count_months_lt_counts_total_months_not_events_or_max_spell() -> None:
    spi = _spi([-2.0, -2.0, 0.0, -2.0, 0.0, -2.0, -2.0, -2.0, 0.0, 0.0, 0.0, 0.0])

    count_months = annual_spi_metric_grid(
        spi,
        annual_aggregation="count_months_lt",
        threshold=-1.0,
        min_months_per_year=9,
        min_event_months=1,
    )["value"]
    count_events = annual_spi_metric_grid(
        spi,
        annual_aggregation="count_events_lt",
        threshold=-1.0,
        min_months_per_year=9,
        min_event_months=1,
    )["value"]
    max_spell = annual_spi_metric_grid(
        spi,
        annual_aggregation="max_spell_lt",
        threshold=-1.0,
        min_months_per_year=9,
        min_event_months=1,
    )["value"]

    assert float(count_months.sel(year=2000).item()) == 5.0
    assert float(count_events.sel(year=2000).item()) == 3.0
    assert float(max_spell.sel(year=2000).item()) == 3.0
    assert float(count_months.sel(year=2000).item()) != float(count_events.sel(year=2000).item())
    assert float(count_months.sel(year=2000).item()) != float(max_spell.sel(year=2000).item())


def test_drought_gridfirst_helper_keeps_spi3_month_count_admin_only() -> None:
    assert is_drought_gridfirst("spi3_count_events_lt_minus1", "district") is True
    assert is_drought_gridfirst("spi3_count_events_lt_minus1", "basin") is False
    assert is_drought_gridfirst("spi3_count_months_lt_minus1", "district") is True
    assert is_drought_gridfirst("spi3_count_months_lt_minus1", "block") is True
    assert is_drought_gridfirst("spi3_count_months_lt_minus1", "basin") is False
    assert is_drought_gridfirst("spi3_count_months_lt_minus1", "sub_basin") is False


def test_nan_breaks_drought_event_runs():
    annual = annual_spi_metric_grid(
        _spi([-2.0, -2.0, np.nan, -2.0, -2.0] + [0.0] * 7),
        annual_aggregation="count_events_lt",
        threshold=-1.0,
        min_months_per_year=9,
        min_event_months=1,
    )["value"]

    assert float(annual.sel(year=2000).item()) == 2.0


def test_drought_free_finite_year_emits_zero_and_all_nan_year_emits_nan():
    drought_free = annual_spi_metric_grid(
        _spi([0.0] * 12),
        annual_aggregation="max_spell_lt",
        threshold=-1.0,
        min_months_per_year=9,
        min_event_months=1,
    )["value"]
    assert float(drought_free.sel(year=2000).item()) == 0.0

    all_nan = annual_spi_metric_grid(
        _spi([np.nan] * 12),
        annual_aggregation="max_spell_lt",
        threshold=-1.0,
        min_months_per_year=9,
        min_event_months=1,
    )["value"]
    assert np.isnan(float(all_nan.sel(year=2000).item()))


def test_valid_month_floor_requires_nine_months():
    eight_valid = annual_spi_metric_grid(
        _spi([-2.0] * 8 + [np.nan] * 4),
        annual_aggregation="count_events_lt",
        threshold=-1.0,
        min_months_per_year=9,
        min_event_months=1,
    )["value"]
    nine_valid = annual_spi_metric_grid(
        _spi([-2.0] * 9 + [np.nan] * 3),
        annual_aggregation="count_events_lt",
        threshold=-1.0,
        min_months_per_year=9,
        min_event_months=1,
    )["value"]

    assert np.isnan(float(eight_valid.sel(year=2000).item()))
    assert float(nine_valid.sel(year=2000).item()) == 1.0


def test_period_max_ignores_nan_and_all_nan_period_stays_nan():
    annual = xr.DataArray(
        np.asarray([[[3.0]], [[np.nan]], [[7.0]], [[0.0]], [[5.0]]]),
        coords={"year": [2000, 2001, 2002, 2003, 2004], "lat": [0.0], "lon": [77.0]},
        dims=("year", "lat", "lon"),
    )
    period = period_rollup_grid(
        annual,
        period_name="2000-2004",
        years=(2000, 2004),
        rollup="period_max",
        min_years_per_period_fraction=0.75,
    )
    assert float(period["value"].item()) == 7.0

    sparse = period_rollup_grid(
        annual,
        period_name="2000-2004",
        years=(2000, 2004),
        rollup="period_max",
        min_years_per_period_fraction=1.0,
    )
    assert np.isnan(float(sparse["value"].item()))


def test_to_contiguous_monthly_index_nan_fills_gap_between_baseline_and_scenario():
    baseline_times = pd.date_range("1990-01-01", "1991-12-01", freq="MS")
    scenario_times = pd.date_range("1996-01-01", "1996-12-01", freq="MS")
    times = baseline_times.append(scenario_times)
    da = xr.DataArray(
        np.full((len(times), 1, 1), 100.0, dtype=float),
        coords={"time": times, "lat": [0.0], "lon": [77.0]},
        dims=("time", "lat", "lon"),
    )

    out = _to_contiguous_monthly_index(da)

    expected = pd.date_range("1990-01-01", "1996-12-01", freq="MS")
    assert np.array_equal(out["time"].values, expected.values)
    assert bool(np.all(np.isnan(out.sel(time=slice("1992-01-01", "1995-12-01")).values[:, 0, 0])))


def test_to_contiguous_monthly_index_accepts_cftime_like_values():
    class NoLeapLike:
        def __init__(self, year, month):
            self.year = year
            self.month = month

    times = np.asarray(
        [NoLeapLike(1990, 1), NoLeapLike(1990, 2), NoLeapLike(1990, 4)],
        dtype=object,
    )
    da = xr.DataArray(
        np.asarray([[[1.0]], [[2.0]], [[4.0]]]),
        coords={"time": times, "lat": [0.0], "lon": [77.0]},
        dims=("time", "lat", "lon"),
    )

    out = _to_contiguous_monthly_index(da)

    assert np.array_equal(out["time"].values, pd.date_range("1990-01-01", "1990-04-01", freq="MS").values)
    assert np.isnan(float(out.sel(time="1990-03-01").item()))


def test_trim_to_full_calendar_years_drops_partial_jan_dec_edges():
    times = pd.date_range("1990-03-01", "1995-09-01", freq="MS")
    da = xr.DataArray(
        np.ones((len(times), 1, 1)),
        coords={"time": times, "lat": [0.0], "lon": [77.0]},
        dims=("time", "lat", "lon"),
    )

    out = _trim_to_full_calendar_years(da)

    expected_times = pd.date_range("1991-01-01", "1994-12-01", freq="MS")
    assert np.array_equal(out["time"].values, expected_times.values)


def test_compute_spi_grid_passes_nan_filled_array_to_climate_indices():
    baseline_times = pd.date_range("1990-01-01", "1991-12-01", freq="MS")
    scenario_times = pd.date_range("1996-01-01", "1997-12-01", freq="MS")
    times = baseline_times.append(scenario_times)
    da = xr.DataArray(
        np.full((len(times), 1, 1), 50.0, dtype=float),
        coords={"time": times, "lat": [0.0], "lon": [77.0]},
        dims=("time", "lat", "lon"),
    )
    captured = {}

    def fake_spi(*, monthly_precip, data_start_year, **_kwargs):
        captured["array"] = np.asarray(monthly_precip, dtype=float).copy()
        captured["data_start_year"] = data_start_year
        return np.full(captured["array"].shape, np.nan, dtype=float)

    with patch(
        "india_resilience_tool.compute.drought_risk_gridfirst.compute_spi_climate_indices",
        side_effect=fake_spi,
    ):
        compute_spi_grid(da, baseline_years=(1990, 1991), scale_months=3)

    series = captured["array"]
    assert captured["data_start_year"] == 1990
    assert series.size == 96
    assert bool(np.all(series[:24] == 50.0))
    assert bool(np.all(np.isnan(series[24:72])))
    assert bool(np.all(series[72:] == 50.0))


def test_grid_metric_cache_invalidates_when_input_file_hash_changes(tmp_path):
    cube = xr.Dataset(
        {
            "value": xr.DataArray(
                np.asarray([[[1.0]]], dtype=float),
                dims=("year", "lat", "lon"),
                coords={"year": [2020], "lat": [0.0], "lon": [77.0]},
            )
        }
    )
    sidecar = {
        "methodology_version": "drought-risk-v2-gridfirst-1",
        "climate_indices_version": "2.2.0",
        "slug": "spi3_max_spell_lt_minus1",
        "model": "m1",
        "scenario": "ssp585",
        "year": 2020,
        "input_file_hashes": {"/data/pr_2020.nc": "deadbeef"},
    }
    path = tmp_path / "cache.nc"
    write_grid_metric_cache(cube, path, sidecar=sidecar)

    hit = read_grid_metric_cache(path, expected_sidecar=sidecar)
    assert hit is not None
    assert float(hit["value"].sel(year=2020).item()) == 1.0

    mutated = dict(sidecar)
    mutated["input_file_hashes"] = {"/data/pr_2020.nc": "cafebabe"}
    assert read_grid_metric_cache(path, expected_sidecar=mutated) is None

    different_set = dict(sidecar)
    different_set["input_file_hashes"] = {"/data/pr_2021.nc": "deadbeef"}
    assert read_grid_metric_cache(path, expected_sidecar=different_set) is None

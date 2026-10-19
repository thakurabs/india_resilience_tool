"""Scientific contracts for conservative support and distinct threshold metrics."""
import numpy as np
import pandas as pd
import pytest

from tools.diagnostics.wbgt_route_b import (
    conservative_matrix, regrid_reference, fixed_aggregate, annual_metrics, paired_outputs,
)


def test_conservative_constant_and_missing_support():
    lat = np.array([.075, .025])
    lon = np.array([70.025, 70.075])
    matrix = conservative_matrix(lat, lon, .05, np.array([.05]), np.array([70.05]), .1)
    value, support = regrid_reference(np.full((2, 2), 31.), matrix)
    assert value[0] == pytest.approx(31)
    assert support[0] == pytest.approx(1)
    value, support = regrid_reference(np.array([[31., np.nan], [31., np.nan]]), matrix)
    assert value[0] == pytest.approx(31)
    assert support[0] == pytest.approx(.5)
    value, support = regrid_reference(np.full((2, 2), np.nan), matrix)
    assert np.isnan(value[0]) and support[0] == 0


def test_uncovered_target_fraction_remains_visible():
    matrix = conservative_matrix(np.array([.05]), np.array([70.05]), .1,
                                 np.array([.05]), np.array([70.1]), .2)
    value, support = regrid_reference(np.array([[10.]]), matrix)
    assert value[0] == 10
    assert 0 < support[0] < 1


def test_missing_cells_do_not_poison_zero_weight_units_or_renormalize():
    daily = np.array([[10., np.nan], [20., 30.]])
    out = fixed_aggregate(daily, np.array([[1., 0.], [.5, .5]]))
    assert out[0, 0] == 10
    assert np.isnan(out[0, 1])
    assert out[1, 1] == 25


def test_count_first_differs_from_threshold_of_spatial_mean():
    dates = pd.date_range("2001-01-01", "2001-12-31")
    cell = np.tile([25., 35.], (365, 1))
    weights = np.array([[.5, .5]])
    out = paired_outputs(dates, cell, np.full((365, 1), 30.), weights)
    assert out["grid_first_series"]["days_ge_32"][0, 0] == 0
    assert out["grid_first_cell_metrics"]["days_ge_32"][0, 0] == 182.5
    assert out["region_first_series"]["days_ge_30"][0, 0] == 365


def test_leap_day_dropped_and_incomplete_year_is_nan():
    dates = pd.date_range("2000-01-01", "2000-12-31")
    values = np.full((366, 2), 30.)
    values[59, :] = np.nan  # Feb 29 does not invalidate IRT reporting year
    values[12, 1] = np.nan
    out = annual_metrics(dates, values)
    assert out["days_ge_30"][0, 0] == 365
    assert np.isnan(out["days_ge_30"][0, 1])
    assert np.isnan(annual_metrics(dates[1:], values[1:])["mean_c"]).all()


def test_extremes_single_day_and_duplicate_dates():
    dates = pd.date_range("2001-01-01", "2001-12-31")
    assert annual_metrics(dates, np.full(365, -40.))["days_ge_28"][0, 0] == 0
    assert annual_metrics(dates, np.full(365, 60.))["days_ge_32"][0, 0] == 365
    assert np.isnan(annual_metrics(dates[:1], [30.])["days_ge_28"][0, 0])
    with pytest.raises(ValueError, match="unique"):
        annual_metrics([dates[0], dates[0]], [30., 31.])
    with pytest.raises(ValueError):
        annual_metrics([], [])

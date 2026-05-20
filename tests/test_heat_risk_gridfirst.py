from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import box
from shapely.geometry import Polygon

from india_resilience_tool.compute.heat_risk_gridfirst import (
    GridSpec,
    HEAT_RISK_GRIDFIRST_SLUGS,
    _cellwise_percent_days,
    aggregate_daily_area_mean,
    aggregate_percent_days,
    build_area_weights,
    compute_doy_thresholds,
    compute_heat_risk_rows_for_metric,
)
from tools.pipeline import compute_indices_multiprocess as CMP


def _unit_gdf(geometries: list[object], names: list[str] | None = None) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"district_name": names or [f"D{i}" for i in range(len(geometries))]},
        geometry=geometries,
        crs="EPSG:4326",
    )


def test_area_weights_full_and_partial_cells() -> None:
    grid = GridSpec(lat=(0.5,), lon=(0.5, 1.5))
    weights = build_area_weights(
        _unit_gdf([box(0.0, 0.0, 1.0, 1.0), box(0.0, 0.0, 0.5, 1.0)], ["full", "half"]),
        grid,
    )

    full_area = weights.loc[weights["unit_key"] == "full", "area_m2"].sum()
    half_area = weights.loc[weights["unit_key"] == "half", "area_m2"].sum()

    assert len(weights[weights["unit_key"] == "full"]) == 1
    assert len(weights[weights["unit_key"] == "half"]) == 1
    assert half_area / full_area == pytest.approx(0.5, rel=1e-6)


def test_area_weights_polygon_with_hole_excludes_inner_cell() -> None:
    grid = GridSpec(lat=(0.5, 1.5), lon=(0.5, 1.5))
    outer = [(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)]
    hole = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]
    weights = build_area_weights(_unit_gdf([Polygon(shell=outer, holes=[hole])], ["donut"]), grid)

    assert set(weights["cell_index"]) == {1, 2, 3}
    assert len(weights) == 3


def test_daily_area_mean_matches_area_mean_first_for_uniform_mean_fixture() -> None:
    grid = GridSpec(lat=(0.5,), lon=(0.5, 1.5))
    weights = build_area_weights(_unit_gdf([box(0.0, 0.0, 2.0, 1.0)], ["both"]), grid)
    da = xr.DataArray(
        np.asarray([[[10.0, 20.0]], [[30.0, 50.0]]]),
        dims=("time", "lat", "lon"),
        coords={"time": pd.date_range("2020-01-01", periods=2), "lat": [0.5], "lon": [0.5, 1.5]},
    )

    means = aggregate_daily_area_mean(da, weights)["both"]

    np.testing.assert_allclose(means.values, [15.0, 40.0], rtol=1e-6)


def test_tx90p_linear_quantile_and_strict_greater_than_under_ties() -> None:
    baseline_time = pd.date_range("1990-01-01", "2010-12-31", freq="D")
    baseline_values = np.full((len(baseline_time), 1, 1), 10.0)
    baseline_values[0, 0, 0] = 20.0
    baseline = xr.DataArray(
        baseline_values,
        dims=("time", "lat", "lon"),
        coords={"time": baseline_time, "lat": [0.5], "lon": [0.5]},
    )
    threshold = compute_doy_thresholds(
        baseline,
        percentile=90,
        window_days=1,
        quantile_method="linear",
    )
    eval_time = pd.date_range("2020-01-01", "2020-12-31", freq="D")
    eval_values = np.full((len(eval_time), 1, 1), 10.0)
    eval_values[0, 0, 0] = 20.0
    eva = xr.DataArray(eval_values, dims=("time", "lat", "lon"), coords={"time": eval_time, "lat": [0.5], "lon": [0.5]})

    exceed, valid = _cellwise_percent_days(eva, threshold, exceed_ge=False)
    pct = aggregate_percent_days(
        exceed,
        valid,
        pd.DataFrame({"unit_key": ["D"], "cell_index": [0], "area_m2": [1.0]}),
    )

    assert pct["D"] == pytest.approx(100.0 / 365.0)


def test_doy_thresholds_keep_all_nan_cells_as_nan() -> None:
    baseline_time = pd.date_range("1990-01-01", "1990-12-31", freq="D")
    baseline_values = np.full((len(baseline_time), 1, 2), np.nan)
    baseline_values[:, 0, 1] = 10.0
    baseline = xr.DataArray(
        baseline_values,
        dims=("time", "lat", "lon"),
        coords={"time": baseline_time, "lat": [0.5], "lon": [0.5, 1.5]},
    )

    threshold = compute_doy_thresholds(
        baseline,
        percentile=90,
        window_days=5,
        quantile_method="linear",
    )

    assert threshold.dims == ("doy", "lat", "lon")
    assert threshold.shape == (365, 1, 2)
    assert np.isnan(threshold.isel(lon=0)).all()
    np.testing.assert_allclose(threshold.isel(lon=1).values, 10.0)


def test_gridfirst_smoke_preserves_pipeline_output_contract(tmp_path: Path, monkeypatch) -> None:
    metric = {
        "slug": "txx_annual_max",
        "var": "tasmax",
        "value_col": "txx_annual_max_C",
        "compute": "annual_max_temperature",
        "params": {},
    }
    time = pd.date_range("2020-01-01", periods=2)
    da = xr.DataArray(
        np.asarray([[[300.0]], [[305.0]]]),
        dims=("time", "lat", "lon"),
        coords={"time": time, "lat": [0.5], "lon": [0.5]},
        name="tasmax",
    )
    nc_path = tmp_path / "tasmax_2020.nc"
    da.to_dataset().to_netcdf(nc_path)
    weights = pd.DataFrame({"unit_key": ["Hyderabad"], "cell_index": [0], "area_m2": [1.0]})

    rows = compute_heat_risk_rows_for_metric(
        metric=metric,
        model="MODEL",
        scenario="ssp585",
        year_to_paths={2020: {"tasmax": nc_path}},
        baseline_year_to_paths={2020: {"tasmax": nc_path}},
        weights=weights,
        level="district",
    )
    monkeypatch.setattr(CMP, "MIN_YEARS_ABSOLUTE", 1)
    monkeypatch.setattr(CMP, "MIN_YEARS_REQUIRED_FRACTION", 0.0)
    out_root = tmp_path / "processed" / "txx_annual_max"
    result = CMP._write_metric_rows_outputs(
        rows=rows,
        coverage_df=pd.DataFrame(),
        metric_root_path=out_root,
        state_name="Telangana",
        level="district",
        slug="txx_annual_max",
        model="MODEL",
        scenario="ssp585",
        scenario_conf={"periods": {"2020-2040": (2020, 2040)}},
        value_col="txx_annual_max_C",
        year_to_paths={2020: {"tasmax": nc_path}},
    )

    yearly = out_root / "Telangana" / "districts" / "Hyderabad" / "MODEL" / "ssp585" / "Hyderabad_yearly.csv"
    periods = out_root / "Telangana" / "districts" / "Hyderabad" / "MODEL" / "ssp585" / "Hyderabad_periods.csv"
    assert result == {"yearly_file_count": 1, "period_file_count": 1}
    assert yearly.exists()
    assert periods.exists()
    df = pd.read_csv(yearly)
    assert {"district", "year", "value", "txx_annual_max_C", "source_file", "model", "scenario"} <= set(df.columns)
    assert df.loc[0, "txx_annual_max_C"] == pytest.approx(31.85)


def test_gridfirst_persists_and_reuses_annual_cell_metric_cache(tmp_path: Path, monkeypatch) -> None:
    metric = {
        "slug": "tx90p_hot_days_pct",
        "var": "tasmax",
        "value_col": "tx90p_pct",
        "compute": "tx90p_etccdi",
        "params": {
            "baseline_years": (1990, 1990),
            "percentile": 90,
            "window_days": 1,
            "quantile_method": "linear",
            "exceed_ge": False,
        },
    }
    baseline_time = pd.date_range("1990-01-01", "1990-12-31", freq="D")
    eval_time = pd.date_range("2020-01-01", "2020-12-31", freq="D")
    baseline_da = xr.DataArray(
        np.full((len(baseline_time), 1, 1), 10.0),
        dims=("time", "lat", "lon"),
        coords={"time": baseline_time, "lat": [0.5], "lon": [0.5]},
        name="tasmax",
    )
    eval_values = np.full((len(eval_time), 1, 1), 10.0)
    eval_values[:36, 0, 0] = 11.0
    eval_da = xr.DataArray(
        eval_values,
        dims=("time", "lat", "lon"),
        coords={"time": eval_time, "lat": [0.5], "lon": [0.5]},
        name="tasmax",
    )
    baseline_path = tmp_path / "1990.nc"
    eval_path = tmp_path / "2020.nc"
    baseline_da.to_dataset().to_netcdf(baseline_path)
    eval_da.to_dataset().to_netcdf(eval_path)
    weights = pd.DataFrame({"unit_key": ["D"], "cell_index": [0], "area_m2": [1.0]})
    cache_root = tmp_path / "cache"

    rows = compute_heat_risk_rows_for_metric(
        metric=metric,
        model="MODEL",
        scenario="ssp585",
        year_to_paths={2020: {"tasmax": eval_path}},
        baseline_year_to_paths={1990: {"tasmax": baseline_path}},
        weights=weights,
        cache_root=cache_root,
    )

    grid_path = cache_root / "grid_metrics" / "tx90p_hot_days_pct" / "MODEL" / "ssp585" / "2020.nc"
    assert grid_path.exists()
    assert grid_path.with_suffix(".nc.json").exists()
    with xr.open_dataset(grid_path) as ds:
        cached = ds.load()
    assert {"tx90p_pct", "exceed_days", "valid_days"} <= set(cached.data_vars)
    assert float(cached["exceed_days"].isel(lat=0, lon=0)) == pytest.approx(36.0)
    assert float(cached["valid_days"].isel(lat=0, lon=0)) == pytest.approx(365.0)
    assert rows[0]["tx90p_pct"] == pytest.approx(100.0 * 36.0 / 365.0)

    def _raise_if_recomputed(**_: object) -> xr.DataArray:
        raise AssertionError("grid metric cache was not reused")

    monkeypatch.setattr(
        "india_resilience_tool.compute.heat_risk_gridfirst._metric_cell_values",
        _raise_if_recomputed,
    )
    rows_from_cache = compute_heat_risk_rows_for_metric(
        metric=metric,
        model="MODEL",
        scenario="ssp585",
        year_to_paths={2020: {"tasmax": eval_path}},
        baseline_year_to_paths={1990: {"tasmax": baseline_path}},
        weights=weights,
        cache_root=cache_root,
    )
    assert rows_from_cache[0]["tx90p_pct"] == pytest.approx(rows[0]["tx90p_pct"])


def test_gridfirst_threshold_day_metrics_cover_txge35_and_tnle10_without_baseline(tmp_path: Path) -> None:
    assert {"txge35_extreme_heat_days", "tnle10_cold_nights"} <= HEAT_RISK_GRIDFIRST_SLUGS
    eval_time = pd.date_range("2020-01-01", periods=4, freq="D")
    tasmax = xr.DataArray(
        np.asarray([[[309.0]], [[308.15]], [[307.0]], [[np.nan]]]),
        dims=("time", "lat", "lon"),
        coords={"time": eval_time, "lat": [0.5], "lon": [0.5]},
        name="tasmax",
    )
    tasmin = xr.DataArray(
        np.asarray([[[283.15]], [[282.0]], [[284.0]], [[np.nan]]]),
        dims=("time", "lat", "lon"),
        coords={"time": eval_time, "lat": [0.5], "lon": [0.5]},
        name="tasmin",
    )
    tasmax_path = tmp_path / "tasmax_2020.nc"
    tasmin_path = tmp_path / "tasmin_2020.nc"
    tasmax.to_dataset().to_netcdf(tasmax_path)
    tasmin.to_dataset().to_netcdf(tasmin_path)
    weights = pd.DataFrame({"unit_key": ["D"], "cell_index": [0], "area_m2": [1.0]})

    tx_rows = compute_heat_risk_rows_for_metric(
        metric={
            "slug": "txge35_extreme_heat_days",
            "var": "tasmax",
            "value_col": "days_tx_ge_35C",
            "compute": "count_days_ge_threshold",
            "params": {"thresh_k": 35.0 + 273.15},
        },
        model="MODEL",
        scenario="ssp585",
        year_to_paths={2020: {"tasmax": tasmax_path}},
        baseline_year_to_paths={},
        weights=weights,
        cache_root=tmp_path / "cache",
    )
    tn_rows = compute_heat_risk_rows_for_metric(
        metric={
            "slug": "tnle10_cold_nights",
            "var": "tasmin",
            "value_col": "days_tn_le_10C",
            "compute": "count_days_le_threshold",
            "params": {"thresh_k": 10.0 + 273.15},
        },
        model="MODEL",
        scenario="ssp585",
        year_to_paths={2020: {"tasmin": tasmin_path}},
        baseline_year_to_paths={},
        weights=weights,
        cache_root=tmp_path / "cache",
    )

    assert tx_rows[0]["days_tx_ge_35C"] == pytest.approx(2.0)
    assert tn_rows[0]["days_tn_le_10C"] == pytest.approx(2.0)

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

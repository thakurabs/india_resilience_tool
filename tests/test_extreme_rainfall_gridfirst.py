from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import geopandas as gpd
from shapely.geometry import box

from india_resilience_tool.compute.drought_risk_gridfirst import aggregate_grid_values_with_retention
from india_resilience_tool.compute.extreme_rainfall_gridfirst import (
    EXTREME_RAINFALL_GRIDFIRST_METHOD_VERSION,
    R95P_BASELINE_YEARS,
    R95P_QUANTILE_METHOD,
    R95P_STRICT_EXCEEDANCE,
    annual_extreme_rainfall_grid,
    compute_extreme_rainfall_rows_for_metric,
    compute_r95p_threshold_grid,
    extreme_rainfall_grid_metric_cache_path,
    r95p_threshold_cache_path,
)
from india_resilience_tool.compute.gridfirst_spatial import read_grid_metric_cache, write_grid_metric_cache
from tools.pipeline import compute_indices_multiprocess as CMP


def _daily(values: np.ndarray, *, start: str = "2020-01-01") -> xr.DataArray:
    arr = np.asarray(values, dtype=float)
    return xr.DataArray(
        arr,
        dims=("time", "lat", "lon"),
        coords={
            "time": pd.date_range(start, periods=arr.shape[0], freq="D"),
            "lat": [0.5],
            "lon": [0.5 + i for i in range(arr.shape[2])],
        },
        name="pr",
        attrs={"units": "mm/day"},
    )


def _write_pr(path: Path, values: np.ndarray, *, start: str = "2020-01-01") -> Path:
    _daily(values, start=start).to_dataset().to_netcdf(path)
    return path


def test_rx1day_grid_first_exceeds_legacy_polygon_average_for_out_of_phase_peaks() -> None:
    da = _daily(np.asarray([[[100.0, 0.0]], [[0.0, 100.0]], [[1.0, 1.0]]]))
    weights = pd.DataFrame({"unit_key": ["D", "D"], "cell_index": [0, 1], "area_m2": [1.0, 1.0]})

    ds = annual_extreme_rainfall_grid(da, slug="pr_max_1day_precip")
    grid_first = aggregate_grid_values_with_retention(ds["value"], weights)["D"][0]
    legacy_polygon_first = float(da.mean(dim=("lat", "lon")).max())

    assert grid_first == pytest.approx(100.0)
    assert grid_first > legacy_polygon_first


def test_rx5day_single_nan_invalidates_windows_containing_it() -> None:
    da = _daily(np.asarray([[[10.0]], [[10.0]], [[np.nan]], [[10.0]], [[10.0]], [[10.0]]]))

    ds = annual_extreme_rainfall_grid(da, slug="pr_max_5day_precip")

    assert np.isnan(float(ds["value"].isel(lat=0, lon=0)))


def test_r20mm_counts_exactly_twenty_mm() -> None:
    da = _daily(np.asarray([[[19.99]], [[20.0]], [[21.0]]]))

    ds = annual_extreme_rainfall_grid(da, slug="r20mm_very_heavy_precip_days")

    assert float(ds["value"].isel(lat=0, lon=0)) == 2.0


def test_cwd_nan_breaks_runs_all_dry_zero_and_coverage_fail_nan() -> None:
    run_da = _daily(np.asarray([[[1.0]], [[2.0]], [[np.nan]], [[3.0]], [[4.0]], [[5.0]], [[0.0]], [[0.0]], [[0.0]], [[0.0]]]))
    dry_da = _daily(np.zeros((10, 1, 1)))
    fail_da = _daily(np.asarray([[[np.nan]], [[5.0]], [[5.0]], [[5.0]], [[5.0]]]))

    assert float(annual_extreme_rainfall_grid(run_da, slug="cwd_consecutive_wet_days")["value"].isel(lat=0, lon=0)) == 3.0
    assert float(annual_extreme_rainfall_grid(dry_da, slug="cwd_consecutive_wet_days")["value"].isel(lat=0, lon=0)) == 0.0
    assert np.isnan(float(annual_extreme_rainfall_grid(fail_da, slug="cwd_consecutive_wet_days")["value"].isel(lat=0, lon=0)))


def test_cdd_nan_breaks_runs_all_wet_zero_and_coverage_fail_nan() -> None:
    run_da = _daily(np.asarray([[[0.0]], [[0.0]], [[np.nan]], [[0.0]], [[0.0]], [[0.0]], [[5.0]], [[5.0]], [[5.0]], [[5.0]]]))
    wet_da = _daily(np.full((10, 1, 1), 5.0))
    fail_da = _daily(np.asarray([[[np.nan]], [[0.0]], [[0.0]], [[0.0]], [[0.0]]]))

    assert float(annual_extreme_rainfall_grid(run_da, slug="pr_consecutive_dry_days_lt1mm")["value"].isel(lat=0, lon=0)) == 3.0
    assert float(annual_extreme_rainfall_grid(wet_da, slug="pr_consecutive_dry_days_lt1mm")["value"].isel(lat=0, lon=0)) == 0.0
    assert np.isnan(float(annual_extreme_rainfall_grid(fail_da, slug="pr_consecutive_dry_days_lt1mm")["value"].isel(lat=0, lon=0)))


def test_cdd_grid_first_exceeds_legacy_polygon_average_for_out_of_phase_dry_spells() -> None:
    # Two cells with 5-day dry runs staggered in time: cell A dry on days 1-5, cell B dry on days 6-10.
    # Polygon-mean precipitation never has more than 1 consecutive day with mean < 1.0, so polygon-first CDD is short.
    # Per-cell grid-first CDD is 5 at both cells; area-weighted to the polygon, grid-first is 5.
    values = np.asarray(
        [
            [[0.0, 5.0]],
            [[0.0, 5.0]],
            [[0.0, 5.0]],
            [[0.0, 5.0]],
            [[0.0, 5.0]],
            [[5.0, 0.0]],
            [[5.0, 0.0]],
            [[5.0, 0.0]],
            [[5.0, 0.0]],
            [[5.0, 0.0]],
        ]
    )
    da = _daily(values)
    weights = pd.DataFrame({"unit_key": ["D", "D"], "cell_index": [0, 1], "area_m2": [1.0, 1.0]})

    grid_first = aggregate_grid_values_with_retention(
        annual_extreme_rainfall_grid(da, slug="pr_consecutive_dry_days_lt1mm")["value"],
        weights,
    )["D"][0]
    polygon_mean = da.mean(dim=("lat", "lon")).values
    longest_polygon_first_dry = 0
    run = 0
    for v in polygon_mean:
        if np.isfinite(v) and v < 1.0:
            run += 1
            longest_polygon_first_dry = max(longest_polygon_first_dry, run)
        else:
            run = 0

    assert grid_first == pytest.approx(5.0)
    assert grid_first > longest_polygon_first_dry


def test_r95p_admin_v2_threshold_semantics_and_no_wet_baseline_cell() -> None:
    baseline_values = np.stack([[[float(day), 0.0]] for day in range(1, 21)], axis=0)
    threshold = compute_r95p_threshold_grid(_daily(baseline_values, start="1990-01-01"))["value"]
    eval_da = _daily(np.asarray([[[19.05, 50.0]], [[20.0, 60.0]]]))

    ds = annual_extreme_rainfall_grid(eval_da, slug="r95p_very_wet_precip", threshold=threshold)

    assert R95P_BASELINE_YEARS == (1990, 2010)
    assert R95P_QUANTILE_METHOD == "linear"
    assert R95P_STRICT_EXCEEDANCE is True
    assert float(threshold.isel(lat=0, lon=0)) == pytest.approx(19.05)
    assert float(ds["value"].isel(lat=0, lon=0)) == pytest.approx(20.0)
    assert np.isnan(float(threshold.isel(lat=0, lon=1)))
    assert np.isnan(float(ds["value"].isel(lat=0, lon=1)))


def test_r95p_polygon_retention_drops_nan_baseline_cell() -> None:
    cell_values = xr.DataArray([[10.0, np.nan]], dims=("lat", "lon"), coords={"lat": [0.5], "lon": [0.5, 1.5]})
    weights = pd.DataFrame({"unit_key": ["D", "D"], "cell_index": [0, 1], "area_m2": [49.0, 51.0]})

    value, retained = aggregate_grid_values_with_retention(cell_values, weights)["D"]

    assert retained == pytest.approx(0.49)
    assert np.isnan(value)


def test_r95ptot_distinguishes_no_wet_days_zero_from_coverage_fail_nan() -> None:
    threshold = xr.DataArray([[10.0]], dims=("lat", "lon"), coords={"lat": [0.5], "lon": [0.5]})
    no_wet = _daily(np.zeros((10, 1, 1)))
    fail = _daily(np.full((10, 1, 1), np.nan))

    no_wet_value = annual_extreme_rainfall_grid(no_wet, slug="r95ptot_contribution_pct", threshold=threshold)["value"]
    fail_value = annual_extreme_rainfall_grid(fail, slug="r95ptot_contribution_pct", threshold=threshold)["value"]

    assert float(no_wet_value.isel(lat=0, lon=0)) == 0.0
    assert np.isnan(float(fail_value.isel(lat=0, lon=0)))


def test_extreme_rainfall_cache_invalidation(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    base_path = _write_pr(tmp_path / "base.nc", np.asarray([[[1.0]], [[20.0]]]), start="1990-01-01")
    metric = {"slug": "r95p_very_wet_precip", "var": "pr", "value_col": "r95p_mm", "params": {"grid_id": "g1"}}
    eval_path = _write_pr(tmp_path / "eval.nc", np.asarray([[[25.0]], [[30.0]]]))
    weights = pd.DataFrame({"unit_key": ["D"], "cell_index": [0], "area_m2": [1.0]})

    rows = compute_extreme_rainfall_rows_for_metric(
        metric=metric,
        model="MODEL",
        scenario="historical",
        year_to_paths={2020: {"pr": eval_path}},
        baseline_year_to_paths={1990: {"pr": base_path}},
        weights=weights,
        cache_root=cache_root,
    )
    assert rows[0]["value"] == pytest.approx(55.0)

    threshold_path = r95p_threshold_cache_path(cache_root, model="MODEL", grid_id="g1")
    old_sidecar = json.loads(threshold_path.with_suffix(".nc.json").read_text(encoding="utf-8"))
    base_path.unlink()
    _write_pr(base_path, np.asarray([[[1.0]], [[100.0]]]), start="1990-01-01")
    rows_changed = compute_extreme_rainfall_rows_for_metric(
        metric=metric,
        model="MODEL",
        scenario="historical",
        year_to_paths={2020: {"pr": eval_path}},
        baseline_year_to_paths={1990: {"pr": base_path}},
        weights=weights,
        cache_root=cache_root,
    )
    new_sidecar = json.loads(threshold_path.with_suffix(".nc.json").read_text(encoding="utf-8"))
    assert old_sidecar["input_file_hashes"] != new_sidecar["input_file_hashes"]
    assert rows_changed[0]["value"] == pytest.approx(0.0)

    annual_path = extreme_rainfall_grid_metric_cache_path(
        cache_root,
        slug="r95p_very_wet_precip",
        model="MODEL",
        grid_id="g1",
        scenario="historical",
        year=2020,
    )
    sidecar = json.loads(annual_path.with_suffix(".nc.json").read_text(encoding="utf-8"))
    sidecar["method_version"] = "old"
    annual_path.with_suffix(".nc.json").write_text(json.dumps(sidecar), encoding="utf-8")
    assert read_grid_metric_cache(annual_path, expected_sidecar={"method_version": EXTREME_RAINFALL_GRIDFIRST_METHOD_VERSION}) is None

    torn_path = tmp_path / "torn.nc"
    write_grid_metric_cache(xr.Dataset({"value": xr.DataArray([[1.0]], dims=("lat", "lon"))}), torn_path, sidecar={"method_version": "x"})
    torn_path.with_suffix(".nc.json").unlink()
    assert read_grid_metric_cache(torn_path, expected_sidecar={"method_version": "x"}) is None


def test_legacy_count_rainy_days_supports_inclusive_r20_and_strict_rainy_days() -> None:
    da = _daily(np.asarray([[[2.5]], [[20.0]], [[20.1]]]))
    mask = xr.DataArray([[True]], dims=("lat", "lon"), coords={"lat": [0.5], "lon": [0.5]})

    assert CMP.count_rainy_days(da, mask, thresh_mm=2.5) == 2
    assert CMP.count_rainy_days(da, mask, thresh_mm=20.0, exceed_ge=True) == 2


def test_pipeline_dispatches_admin_extreme_rainfall_to_gridfirst(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sample_path = _write_pr(tmp_path / "2020.nc", np.asarray([[[10.0]], [[20.0]]]))
    metric = {"slug": "pr_max_1day_precip", "var": "pr", "value_col": "max_1day_precip_mm", "compute": "rx1day", "params": {}}
    gdf = gpd.GeoDataFrame({"district_name": ["D"]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326")
    called: dict[str, object] = {}

    monkeypatch.setattr(CMP, "var_data_dir", lambda *_args, **_kwargs: tmp_path)
    monkeypatch.setattr(CMP, "validated_year_files_for_var", lambda *_args, **_kwargs: ({2020: sample_path}, {}))
    monkeypatch.setattr(CMP, "read_gridfirst_spatial_weights_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        CMP,
        "build_gridfirst_area_weights",
        lambda *_args, **_kwargs: pd.DataFrame({"unit_key": ["D"], "cell_index": [0], "area_m2": [1.0]}),
    )
    monkeypatch.setattr(CMP, "write_gridfirst_spatial_weights_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        CMP,
        "gridfirst_coverage_from_weights",
        lambda *_args, **_kwargs: pd.DataFrame({"unit_key": ["D"], "coverage_ok": [True], "district": ["D"]}),
    )
    def fake_writer(**kwargs):
        called["write"] = kwargs
        return {"ok": 1}

    monkeypatch.setattr(CMP, "_write_metric_rows_outputs", fake_writer)

    def fake_compute(**kwargs):
        called["compute"] = kwargs
        return [{"district": "D", "year": 2020, "value": 20.0, "max_1day_precip_mm": 20.0}]

    monkeypatch.setattr(CMP, "compute_extreme_rainfall_rows_for_metric", fake_compute)

    result = CMP.process_metric_for_model_scenario(
        metric,
        "MODEL",
        "historical",
        {"subdir": "historical/tas", "periods": {"2020-2020": (2020, 2020)}},
        gdf,
        level="district",
        state_name="State",
    )

    assert result == {"ok": 1}
    assert called["compute"]["level"] == "district"
    assert called["compute"]["baseline_year_to_paths"] == {}


def test_pipeline_keeps_hydro_extreme_rainfall_on_legacy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sample_path = _write_pr(tmp_path / "2020.nc", np.asarray([[[10.0, 12.0]], [[20.0, 22.0]]]))
    metric = {"slug": "pr_max_1day_precip", "var": "pr", "value_col": "max_1day_precip_mm", "compute": "rx1day", "params": {}}
    gdf = gpd.GeoDataFrame({"basin_id": ["1"], "basin_name": ["B"]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326")
    mask = xr.DataArray([[True, True]], dims=("lat", "lon"), coords={"lat": [0.5], "lon": [0.5, 1.5]})
    called = {"legacy": False}

    monkeypatch.setattr(CMP, "var_data_dir", lambda *_args, **_kwargs: tmp_path)
    monkeypatch.setattr(CMP, "validated_year_files_for_var", lambda *_args, **_kwargs: ({2020: sample_path}, {}))
    monkeypatch.setattr(
        CMP,
        "compute_extreme_rainfall_rows_for_metric",
        lambda **_kwargs: pytest.fail("hydro level should not use admin v2 grid-first dispatch"),
    )
    monkeypatch.setattr(
        CMP,
        "build_unit_masks",
        lambda *_args, **_kwargs: (
            {"B": mask},
            pd.DataFrame({"unit_key": ["B"], "coverage_ok": [True], "eligible_for_processing": [True], "basin_name": ["B"]}),
        ),
    )
    monkeypatch.setattr(CMP, "MIN_YEARS_ABSOLUTE", 1)
    monkeypatch.setattr(CMP, "MIN_YEARS_REQUIRED_FRACTION", 0.0)
    monkeypatch.setattr(CMP, "metric_root", lambda _slug: tmp_path / "processed" / _slug)

    original_rx1day = CMP.rx1day

    def recording_rx1day(*args, **kwargs):
        called["legacy"] = True
        return original_rx1day(*args, **kwargs)

    monkeypatch.setattr(CMP, "rx1day", recording_rx1day)

    result = CMP.process_metric_for_model_scenario(
        metric,
        "MODEL",
        "historical",
        {"subdir": "historical/tas", "periods": {"2020-2020": (2020, 2020)}},
        gdf,
        level="basin",
        state_name="State",
    )

    assert called["legacy"] is True
    assert result is None or result["yearly_file_count"] == 1

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import box

from india_resilience_tool.compute.gridfirst_spatial import read_grid_metric_cache
from india_resilience_tool.compute.heat_stress_gridfirst import (
    HEAT_STRESS_GRIDFIRST_METHOD_VERSION,
    HEAT_STRESS_GRIDFIRST_SLUGS,
    compute_heat_stress_rows_for_metric,
    heat_stress_grid_metric_cache_path,
    stull_twb_c,
)
from tools.pipeline import compute_indices_multiprocess as CMP


def _write_da(path: Path, name: str, values: np.ndarray, time: pd.DatetimeIndex) -> Path:
    da = xr.DataArray(
        values,
        dims=("time", "lat", "lon"),
        coords={"time": time, "lat": [0.5], "lon": np.arange(values.shape[2]) + 0.5},
        name=name,
    )
    da.to_dataset().to_netcdf(path)
    return path


def test_stull_twb_helper_matches_legacy_one_cell_behavior() -> None:
    tas_c = xr.DataArray([30.0], dims=("time",))
    rh = xr.DataArray([0.70], dims=("time",))

    expected = CMP._wet_bulb_stull_c(tas_c, rh)
    result = stull_twb_c(tas_c, rh)

    xr.testing.assert_allclose(result, expected)


def test_twb_summer_mean_requires_months_param(tmp_path: Path) -> None:
    time = pd.date_range("2020-03-01", periods=2, freq="D")
    tas = _write_da(tmp_path / "tas.nc", "tas", np.full((2, 1, 1), 303.15), time)
    hurs = _write_da(tmp_path / "hurs.nc", "hurs", np.full((2, 1, 1), 60.0), time)

    with pytest.raises(ValueError, match="months"):
        compute_heat_stress_rows_for_metric(
            metric={
                "slug": "twb_summer_mean",
                "value_col": "summer_twb_mean_C",
                "compute": "wet_bulb_seasonal_mean_stull",
                "params": {},
            },
            model="MODEL",
            scenario="ssp585",
            year_to_paths={2020: {"tas": tas, "hurs": hurs}},
            weights=pd.DataFrame({"unit_key": ["D"], "cell_index": [0], "area_m2": [1.0]}),
        )


def test_tasmin_tropical_nights_gt28_drops_feb29_strict_and_nan_non_event(tmp_path: Path) -> None:
    time = pd.to_datetime(["2020-02-28", "2020-02-29", "2020-03-01", "2020-03-02"])
    values = np.asarray([[[301.15]], [[305.15]], [[301.16]], [[np.nan]]])
    tasmin = _write_da(tmp_path / "tasmin.nc", "tasmin", values, time)

    rows = compute_heat_stress_rows_for_metric(
        metric={
            "slug": "tasmin_tropical_nights_gt28",
            "var": "tasmin",
            "value_col": "tropical_nights_gt_28C",
            "compute": "count_days_above_threshold",
            "params": {"thresh_k": 301.15},
        },
        model="MODEL",
        scenario="ssp585",
        year_to_paths={2020: {"tasmin": tasmin}},
        weights=pd.DataFrame({"unit_key": ["D"], "cell_index": [0], "area_m2": [1.0]}),
    )

    assert rows[0]["tropical_nights_gt_28C"] == pytest.approx(1.0)


def test_two_cell_twb_gridfirst_differs_from_polygon_mean_first(tmp_path: Path) -> None:
    time = pd.date_range("2020-01-01", periods=1, freq="D")
    tas_c = np.asarray([[[25.0, 45.0]]])
    rh = np.asarray([[[90.0, 20.0]]])
    tas = _write_da(tmp_path / "tas.nc", "tas", tas_c + 273.15, time)
    hurs = _write_da(tmp_path / "hurs.nc", "hurs", rh, time)
    weights = pd.DataFrame(
        {"unit_key": ["D", "D"], "cell_index": [0, 1], "area_m2": [1.0, 1.0]}
    )

    rows = compute_heat_stress_rows_for_metric(
        metric={
            "slug": "twb_annual_mean",
            "var": "tas",
            "value_col": "twb_annual_mean_C",
            "compute": "wet_bulb_annual_mean_stull",
            "params": {},
        },
        model="MODEL",
        scenario="ssp585",
        year_to_paths={2020: {"tas": tas, "hurs": hurs}},
        weights=weights,
    )

    grid_first = rows[0]["twb_annual_mean_C"]
    polygon_mean_first = float(stull_twb_c(xr.DataArray([35.0]), xr.DataArray([55.0])).item())
    assert abs(grid_first - polygon_mean_first) > 0.01


def test_two_cell_day_count_aggregation_remains_fractional() -> None:
    cell_values = xr.DataArray([[10.0, 30.0]], dims=("lat", "lon"), coords={"lat": [0.5], "lon": [0.5, 1.5]})
    weights = pd.DataFrame(
        {"unit_key": ["D", "D"], "cell_index": [0, 1], "area_m2": [1.0, 1.0]}
    )

    from india_resilience_tool.compute.heat_risk_gridfirst import aggregate_cell_values

    assert aggregate_cell_values(cell_values, weights)["D"] == pytest.approx(20.0)


def test_cache_sidecar_method_version_mismatch_is_cache_miss(tmp_path: Path) -> None:
    time = pd.date_range("2020-01-01", periods=1, freq="D")
    tas = _write_da(tmp_path / "tas.nc", "tas", np.asarray([[[303.15]]]), time)
    hurs = _write_da(tmp_path / "hurs.nc", "hurs", np.asarray([[[60.0]]]), time)
    cache_root = tmp_path / "cache"

    compute_heat_stress_rows_for_metric(
        metric={
            "slug": "twb_annual_mean",
            "var": "tas",
            "value_col": "twb_annual_mean_C",
            "compute": "wet_bulb_annual_mean_stull",
            "params": {"grid_id": "grid"},
        },
        model="MODEL",
        scenario="ssp585",
        year_to_paths={2020: {"tas": tas, "hurs": hurs}},
        weights=pd.DataFrame({"unit_key": ["D"], "cell_index": [0], "area_m2": [1.0]}),
        cache_root=cache_root,
    )
    cache_path = heat_stress_grid_metric_cache_path(
        cache_root,
        slug="twb_annual_mean",
        model="MODEL",
        grid_id="grid",
        scenario="ssp585",
        year=2020,
    )
    sidecar_path = cache_path.with_suffix(".nc.json")
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["method_version"] = "old"
    sidecar_path.write_text(json.dumps(sidecar), encoding="utf-8")

    assert read_grid_metric_cache(cache_path, expected_sidecar={"method_version": HEAT_STRESS_GRIDFIRST_METHOD_VERSION}) is None


def test_pipeline_dispatches_heat_stress_only_metric_to_gridfirst(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tas_path = _write_da(tmp_path / "tas.nc", "tas", np.asarray([[[303.15]]]), pd.date_range("2020-01-01", periods=1))
    hurs_path = _write_da(tmp_path / "hurs.nc", "hurs", np.asarray([[[60.0]]]), pd.date_range("2020-01-01", periods=1))
    metric = {
        "slug": "twb_annual_mean",
        "var": "tas",
        "vars": ["tas", "hurs"],
        "value_col": "twb_annual_mean_C",
        "compute": "wet_bulb_annual_mean_stull",
        "params": {},
    }
    gdf = gpd.GeoDataFrame({"district_name": ["D"]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326")
    called: dict[str, object] = {}

    monkeypatch.setattr(CMP, "var_data_dir", lambda *_args, **_kwargs: tmp_path)
    monkeypatch.setattr(
        CMP,
        "validated_year_files_for_var",
        lambda _dir, var: ({2020: tas_path if var == "tas" else hurs_path}, {}),
    )
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
        return [{"district": "D", "year": 2020, "value": 1.0, "twb_annual_mean_C": 1.0}]

    monkeypatch.setattr(CMP, "compute_heat_stress_rows_for_metric", fake_compute)

    result = CMP.process_metric_for_model_scenario(
        metric,
        "MODEL",
        "ssp585",
        {"subdir": "ssp585", "periods": {"2020-2020": (2020, 2020)}},
        gdf,
        level="district",
        state_name="State",
    )

    assert result == {"ok": 1}
    assert called["compute"]["metric"]["params"]["grid_id"]
    assert called["compute"]["level"] == "district"


def test_shared_heat_risk_slugs_are_not_heat_stress_gridfirst() -> None:
    assert "tn90p_warm_nights_pct" not in HEAT_STRESS_GRIDFIRST_SLUGS
    assert "wsdi_warm_spell_days" not in HEAT_STRESS_GRIDFIRST_SLUGS
    assert "wbd_le_3" not in HEAT_STRESS_GRIDFIRST_SLUGS

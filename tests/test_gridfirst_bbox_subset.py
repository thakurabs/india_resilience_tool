"""Tests for the per-state bounding-box grid subset (memory fix).

Covers the shared helpers in ``gridfirst_spatial``: direction-agnostic index
range computation, positional subsetting, the cell/grid alignment guard, the
spatial-weights state token, the threshold-cache grid_id signature, and the
headline guarantee that a bbox-subset aggregation reproduces the full-grid
per-unit values bit-for-bit (within fp tolerance).
"""

from __future__ import annotations

import errno
import os
import threading
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import box

from india_resilience_tool.compute import gridfirst_spatial as gs


# --- bbox_to_index_range: direction-agnostic + guards (G2, G9) ----------------

def test_bbox_index_range_ascending():
    lat = [10.0, 11.0, 12.0, 13.0, 14.0]
    lon = [77.0, 78.0, 79.0, 80.0, 81.0]
    # bbox squarely over the middle row/col; no buffer to test core selection.
    rng = gs.bbox_to_index_range(lat, lon, (77.5, 10.5, 80.5, 13.5), buffer_cells=0)
    assert rng == (1, 4, 1, 4)


def test_bbox_index_range_descending_lat_and_lon():
    lat = [14.0, 13.0, 12.0, 11.0, 10.0]  # descending
    lon = [81.0, 80.0, 79.0, 78.0, 77.0]  # descending
    rng = gs.bbox_to_index_range(lat, lon, (77.5, 10.5, 80.5, 13.5), buffer_cells=0)
    # 11,12,13 are positions 3,2,1 -> contiguous [1,4); same for lon.
    assert rng == (1, 4, 1, 4)


def test_bbox_index_range_non_monotonic_raises():
    lat = [10.0, 12.0, 11.0, 13.0]
    lon = [77.0, 78.0, 79.0, 80.0]
    with pytest.raises(ValueError, match="not monotonic"):
        gs.bbox_to_index_range(lat, lon, (77.5, 10.5, 79.5, 12.5), buffer_cells=0)


def test_bbox_index_range_disjoint_raises():
    lat = [10.0, 11.0, 12.0]
    lon = [77.0, 78.0, 79.0]
    with pytest.raises(ValueError, match="zero grid cells"):
        gs.bbox_to_index_range(lat, lon, (77.0, 50.0, 79.0, 60.0), buffer_cells=0)


def test_bbox_buffer_uses_grid_spacing():
    lat = [10.0, 11.0, 12.0, 13.0, 14.0]
    lon = [77.0, 78.0, 79.0, 80.0, 81.0]
    # Tight bbox on a single cell, buffer_cells=1 should pull in the neighbours.
    rng = gs.bbox_to_index_range(lat, lon, (79.0, 12.0, 79.0, 12.0), buffer_cells=1)
    assert rng == (1, 4, 1, 4)


# --- subset_grid_by_index -----------------------------------------------------

def test_subset_grid_by_index_slices_and_passthrough():
    da = xr.DataArray(
        np.arange(25.0).reshape(5, 5),
        dims=("lat", "lon"),
        coords={"lat": [10, 11, 12, 13, 14], "lon": [77, 78, 79, 80, 81]},
    )
    sub = gs.subset_grid_by_index(da, (1, 4, 1, 4))
    assert sub.shape == (3, 3)
    assert list(sub["lat"].values) == [11, 12, 13]
    assert list(sub["lon"].values) == [78, 79, 80]
    assert gs.subset_grid_by_index(da, None) is da


# --- alignment guard (G1) -----------------------------------------------------

def _small_district():
    return gpd.GeoDataFrame(
        {"district_name": ["D1"]},
        geometry=[box(78.3, 11.3, 79.7, 12.7)],
        crs="EPSG:4326",
    )


def test_alignment_guard_raises_on_coordinate_drift():
    grid = gs.GridSpec(lat=(11.0, 12.0, 13.0), lon=(78.0, 79.0, 80.0))
    weights = gs.build_area_weights(_small_district(), grid, level="district")
    field = xr.DataArray(
        np.ones((3, 3)),
        dims=("lat", "lon"),
        coords={"lat": [11.0, 12.0, 13.0], "lon": [78.0, 79.0, 80.0]},
    )
    # Same shape, but the reference GridSpec coords drift sub-cell -> must raise.
    drifted = gs.GridSpec(lat=(11.001, 12.0, 13.0), lon=(78.0, 79.0, 80.0))
    with pytest.raises(ValueError, match="coordinates differ"):
        gs.aggregate_cell_values(field, weights, grid=drifted)


def test_alignment_guard_raises_on_shape_mismatch():
    grid = gs.GridSpec(lat=(11.0, 12.0, 13.0), lon=(78.0, 79.0, 80.0))
    weights = gs.build_area_weights(_small_district(), grid, level="district")
    # Field one row short of the weights' grid -> cell_index runs out of range.
    field = xr.DataArray(
        np.ones((2, 3)),
        dims=("lat", "lon"),
        coords={"lat": [11.0, 12.0], "lon": [78.0, 79.0, 80.0]},
    )
    with pytest.raises(ValueError, match="cell_index"):
        gs.aggregate_cell_values(field, weights)


# --- headline identity guarantee ---------------------------------------------

def test_bbox_subset_matches_full_grid_aggregation():
    lat = [10.0, 11.0, 12.0, 13.0, 14.0]
    lon = [77.0, 78.0, 79.0, 80.0, 81.0]
    values = (np.arange(25.0).reshape(5, 5) + 1.0) * 3.0  # arbitrary finite field
    field = xr.DataArray(values, dims=("lat", "lon"), coords={"lat": lat, "lon": lon})
    gdf = _small_district()

    # Full-grid path.
    grid_full = gs.GridSpec(lat=tuple(lat), lon=tuple(lon))
    weights_full = gs.build_area_weights(gdf, grid_full, level="district")
    full = gs.aggregate_cell_values(field, weights_full, grid=grid_full)

    # Subset path: derive index range from the polygon, subset, rebuild weights.
    index_range = gs.bbox_to_index_range(lat, lon, tuple(gdf.total_bounds), buffer_cells=1)
    assert index_range == (1, 4, 1, 4)  # a genuine subset, not the full grid
    sub = gs.subset_grid_by_index(field, index_range)
    grid_sub = gs.GridSpec(
        lat=tuple(float(v) for v in sub["lat"].values),
        lon=tuple(float(v) for v in sub["lon"].values),
    )
    weights_sub = gs.build_area_weights(gdf, grid_sub, level="district")
    subset = gs.aggregate_cell_values(sub, weights_sub, grid=grid_sub)

    assert set(full) == set(subset)
    for unit, val in full.items():
        assert subset[unit] == pytest.approx(val, rel=1e-9, abs=1e-9)


# --- spatial-weights state token (G3) ----------------------------------------

def test_spatial_weights_cache_state_token_prevents_cross_state_bleed(tmp_path: Path):
    grid = gs.GridSpec(lat=(11.0, 12.0), lon=(78.0, 79.0))
    weights = pd.DataFrame(
        {"unit_key": ["A"], "cell_index": [0], "lat_index": [0], "lon_index": [0], "area_m2": [1.0]}
    )
    out = tmp_path / "district__telangana__grid.parquet"
    gs.write_spatial_weights_cache(
        weights, output_path=out, grid=grid, level="district", state="Telangana"
    )
    # Same grid/level, different state -> must miss.
    assert gs.read_spatial_weights_cache(out, grid=grid, level="district", state="Maharashtra") is None
    same = gs.read_spatial_weights_cache(out, grid=grid, level="district", state="Telangana")
    assert same is not None and list(same["unit_key"]) == ["A"]


# --- threshold cache grid_id signature (G5) ----------------------------------

def test_threshold_cache_grid_id_invalidation(tmp_path: Path):
    threshold = xr.DataArray(
        np.ones((3, 2, 2)),
        dims=("doy", "lat", "lon"),
        coords={"doy": [1, 2, 3], "lat": [11.0, 12.0], "lon": [78.0, 79.0]},
    )
    path = gs.threshold_cache_path(tmp_path, model="ModelA", var="tasmax", baseline_label="1981-2010", grid_id="abc123")
    kw = dict(
        input_signature="sig",
        baseline_years=(1981, 2010),
        percentile=90,
        window_days=5,
        quantile_method="linear",
    )
    gs.write_threshold_cache(threshold, path, grid_id="abc123", **kw)

    assert gs.read_threshold_cache(path, grid_id="zzz999", **kw) is None
    hit = gs.read_threshold_cache(path, grid_id="abc123", **kw)
    assert hit is not None and hit.shape == (3, 2, 2)


# --- Phase 2: shared-hardening guards (H1-H4) --------------------------------

def test_grid_metric_cache_path_grid_id_segment(tmp_path: Path):
    # H1: a grid_id segment isolates per-grid annual caches; None is legacy.
    legacy = gs.grid_metric_cache_path(tmp_path, slug="s", model="m", scenario="ssp245", year=2050)
    keyed = gs.grid_metric_cache_path(tmp_path, slug="s", model="m", scenario="ssp245", year=2050, grid_id="abc123")
    other = gs.grid_metric_cache_path(tmp_path, slug="s", model="m", scenario="ssp245", year=2050, grid_id="def456")
    assert legacy != keyed and keyed != other
    assert "abc123" in str(keyed) and "abc123" not in str(legacy)


def test_threshold_cache_path_full_signature(tmp_path: Path):
    # H2: percentile/window/method/smooth/grid_id all distinguish the path so
    # two metrics sharing model/var/baseline never race on one file.
    base = dict(cache_root=tmp_path, model="m", var="tasmax", baseline_label="1990-2010")
    p_a = gs.threshold_cache_path(**base, grid_id="g1", percentile=90, window_days=5, quantile_method="linear")
    p_b = gs.threshold_cache_path(**base, grid_id="g1", percentile=10, window_days=5, quantile_method="linear")
    p_c = gs.threshold_cache_path(**base, grid_id="g1", percentile=90, window_days=15, quantile_method="linear")
    p_d = gs.threshold_cache_path(**base, grid_id="g1", percentile=90, window_days=5, quantile_method="nearest")
    p_e = gs.threshold_cache_path(**base, grid_id="g2", percentile=90, window_days=5, quantile_method="linear")
    p_f = gs.threshold_cache_path(**base, grid_id="g1", percentile=90, window_days=5, quantile_method="linear", smooth=31)
    assert len({p_a, p_b, p_c, p_d, p_e, p_f}) == 6


def test_threshold_cache_atomic_roundtrip_with_smooth(tmp_path: Path):
    threshold = xr.DataArray(
        np.ones((3, 2, 2)),
        dims=("doy", "lat", "lon"),
        coords={"doy": [1, 2, 3], "lat": [11.0, 12.0], "lon": [78.0, 79.0]},
    )
    path = gs.threshold_cache_path(
        tmp_path, model="m", var="tasmax", baseline_label="1990-2010",
        grid_id="g1", percentile=90, window_days=5, quantile_method="linear", smooth=31,
    )
    kw = dict(input_signature="sig", baseline_years=(1990, 2010), percentile=90, window_days=5, quantile_method="linear")
    gs.write_threshold_cache(threshold, path, grid_id="g1", smooth=31, **kw)
    # No stale temp files left behind by the atomic write.
    assert not list(path.parent.glob("*.tmp"))
    hit = gs.read_threshold_cache(path, grid_id="g1", **kw)
    assert hit is not None and hit.shape == (3, 2, 2)


def test_assert_grid_matches_passes_and_raises():
    grid = gs.GridSpec(lat=(11.0, 12.0, 13.0), lon=(78.0, 79.0))
    ok = xr.DataArray(np.ones((3, 2)), dims=("lat", "lon"), coords={"lat": [11.0, 12.0, 13.0], "lon": [78.0, 79.0]})
    gs.assert_grid_matches(ok, grid, name="ok")  # no raise
    gs.assert_grid_matches(ok, None)  # None grid is a no-op

    shape_bad = xr.DataArray(np.ones((2, 2)), dims=("lat", "lon"), coords={"lat": [11.0, 12.0], "lon": [78.0, 79.0]})
    with pytest.raises(ValueError, match="shape"):
        gs.assert_grid_matches(shape_bad, grid, name="shape")

    drift = xr.DataArray(np.ones((3, 2)), dims=("lat", "lon"), coords={"lat": [11.001, 12.0, 13.0], "lon": [78.0, 79.0]})
    with pytest.raises(ValueError, match="differ"):
        gs.assert_grid_matches(drift, grid, name="drift")

    no_coords = xr.DataArray(np.ones((3, 2)), dims=("lat", "lon"))
    with pytest.raises(ValueError, match="missing lat/lon"):
        gs.assert_grid_matches(no_coords, grid, name="nocoords")


def test_to_lat_lon_field_transposes_and_rejects_extra_dim():
    grid = gs.GridSpec(lat=(11.0, 12.0, 13.0), lon=(78.0, 79.0, 80.0))
    weights = gs.build_area_weights(_small_district(), grid, level="district")
    # A (lon, lat) field must aggregate identically to (lat, lon): the helper
    # transposes before the C-order flatten so cell_index stays valid.
    values = (np.arange(9.0).reshape(3, 3) + 1.0)
    canonical = xr.DataArray(values, dims=("lat", "lon"), coords={"lat": [11.0, 12.0, 13.0], "lon": [78.0, 79.0, 80.0]})
    transposed = canonical.transpose("lon", "lat")
    a = gs.aggregate_cell_values(canonical, weights, grid=grid)
    b = gs.aggregate_cell_values(transposed, weights, grid=grid)
    assert set(a) == set(b)
    for k in a:
        assert a[k] == pytest.approx(b[k], rel=1e-9, abs=1e-9)

    extra = xr.DataArray(
        np.ones((2, 3, 3)),
        dims=("time", "lat", "lon"),
        coords={"time": [0, 1], "lat": [11.0, 12.0, 13.0], "lon": [78.0, 79.0, 80.0]},
    )
    with pytest.raises(ValueError, match="non-singleton"):
        gs.aggregate_cell_values(extra, weights, grid=grid)


def test_aggregate_percent_days_validates_denominator_grid():
    grid = gs.GridSpec(lat=(11.0, 12.0, 13.0), lon=(78.0, 79.0, 80.0))
    weights = gs.build_area_weights(_small_district(), grid, level="district")
    exceed = xr.DataArray(
        np.ones((3, 3)), dims=("lat", "lon"), coords={"lat": [11.0, 12.0, 13.0], "lon": [78.0, 79.0, 80.0]}
    )
    # valid_days on a coordinate-drifted grid must raise rather than silently
    # divide exceed-days of one cell by valid-days of another.
    valid_drift = xr.DataArray(
        np.full((3, 3), 10.0), dims=("lat", "lon"), coords={"lat": [11.0, 12.0, 13.5], "lon": [78.0, 79.0, 80.0]}
    )
    with pytest.raises(ValueError):
        gs.aggregate_percent_days(exceed, valid_drift, weights, grid=grid)


# --- Phase 2: per-family bbox-subset identity (forwarding guard) -------------
# Each family's compute fn must produce identical per-unit values whether it
# loads the full grid (index_range=None) or only the bbox subset. This is the
# headline guarantee for the memory fix extended to heat/cold/extreme/heat-stress.

from india_resilience_tool.compute import cold_risk_gridfirst as _cr  # noqa: E402
from india_resilience_tool.compute import extreme_rainfall_gridfirst as _er  # noqa: E402
from india_resilience_tool.compute import heat_risk_gridfirst as _hr  # noqa: E402
from india_resilience_tool.compute import heat_stress_gridfirst as _hs  # noqa: E402

_GRID_LAT = [10.0, 11.0, 12.0, 13.0, 14.0]
_GRID_LON = [77.0, 78.0, 79.0, 80.0, 81.0]


def _mk_year(path: Path, var: str, year: int, *, ndays: int = 60, kind: str = "temp") -> Path:
    times = pd.date_range(f"{year}-01-01", periods=ndays, freq="D")
    rng = np.random.default_rng(abs(hash((var, int(year)))) % (2**31))
    if kind == "humidity":
        data = np.clip(60.0 + rng.normal(0.0, 15.0, size=(ndays, 5, 5)), 0.0, 100.0)
    elif kind == "precip":  # kg m-2 s-1, a few wet days
        data = np.abs(rng.normal(0.0, 3e-5, size=(ndays, 5, 5)))
    else:  # temperature in Kelvin
        data = 300.0 + rng.normal(0.0, 6.0, size=(ndays, 5, 5))
    da = xr.DataArray(
        data, dims=("time", "lat", "lon"), coords={"time": times, "lat": _GRID_LAT, "lon": _GRID_LON}, name=var
    )
    if kind == "precip":
        da.attrs["units"] = "kg m-2 s-1"
    da.to_dataset(name=var).to_netcdf(path)
    return path


def _subset_grid():
    gdf = _small_district()
    idx = gs.bbox_to_index_range(_GRID_LAT, _GRID_LON, tuple(gdf.total_bounds), buffer_cells=1)
    assert idx == (1, 4, 1, 4)  # genuine subset
    sub_lat = _GRID_LAT[idx[0]:idx[1]]
    sub_lon = _GRID_LON[idx[2]:idx[3]]
    return gdf, idx, sub_lat, sub_lon


def _rows_to_dict(rows: list[dict]) -> dict[str, float]:
    return {str(r["district"]): r["value"] for r in rows}


def _assert_same_units(full: dict, subset: dict) -> None:
    assert full and set(full) == set(subset)
    for unit, val in full.items():
        if np.isnan(val):
            assert np.isnan(subset[unit])
        else:
            assert subset[unit] == pytest.approx(val, rel=1e-9, abs=1e-9)


def test_heat_count_metric_subset_identity(tmp_path: Path):
    p = _mk_year(tmp_path / "tasmax_2050.nc", "tasmax", 2050)
    y2p = {2050: {"tasmax": p}}
    metric = {
        "slug": "txge35_extreme_heat_days", "var": "tasmax", "value_col": "value",
        "compute": "count_days_ge_threshold", "params": {"thresh_k": 301.0},
    }
    gdf, idx, sub_lat, sub_lon = _subset_grid()
    g_full = gs.GridSpec(lat=tuple(_GRID_LAT), lon=tuple(_GRID_LON))
    g_sub = gs.GridSpec(lat=tuple(sub_lat), lon=tuple(sub_lon))
    full = _rows_to_dict(_hr.compute_heat_risk_rows_for_metric(
        metric=metric, model="M", scenario="ssp245", year_to_paths=y2p,
        baseline_year_to_paths=y2p, weights=gs.build_area_weights(gdf, g_full, level="district"),
        level="district", index_range=None, grid=g_full,
    ))
    subset = _rows_to_dict(_hr.compute_heat_risk_rows_for_metric(
        metric=metric, model="M", scenario="ssp245", year_to_paths=y2p,
        baseline_year_to_paths=y2p, weights=gs.build_area_weights(gdf, g_sub, level="district"),
        level="district", index_range=idx, grid=g_sub,
    ))
    _assert_same_units(full, subset)


def test_heat_tx90p_threshold_subset_identity(tmp_path: Path):
    base = {y: {"tasmax": _mk_year(tmp_path / f"tasmax_{y}.nc", "tasmax", y)} for y in (2048, 2049)}
    evalp = {2050: {"tasmax": _mk_year(tmp_path / "tasmax_2050.nc", "tasmax", 2050)}}
    metric = {
        "slug": "tx90p_hot_days_pct", "var": "tasmax", "value_col": "value", "compute": "tx90p_etccdi",
        "params": {"percentile": 90, "window_days": 5, "quantile_method": "linear",
                   "exceed_ge": False, "direction": "above", "baseline_years": (2048, 2049)},
    }
    gdf, idx, sub_lat, sub_lon = _subset_grid()
    g_full = gs.GridSpec(lat=tuple(_GRID_LAT), lon=tuple(_GRID_LON))
    g_sub = gs.GridSpec(lat=tuple(sub_lat), lon=tuple(sub_lon))
    full = _rows_to_dict(_hr.compute_heat_risk_rows_for_metric(
        metric=metric, model="M", scenario="ssp245", year_to_paths=evalp, baseline_year_to_paths=base,
        weights=gs.build_area_weights(gdf, g_full, level="district"), level="district", index_range=None, grid=g_full,
    ))
    subset = _rows_to_dict(_hr.compute_heat_risk_rows_for_metric(
        metric=metric, model="M", scenario="ssp245", year_to_paths=evalp, baseline_year_to_paths=base,
        weights=gs.build_area_weights(gdf, g_sub, level="district"), level="district", index_range=idx, grid=g_sub,
    ))
    _assert_same_units(full, subset)


def test_cold_annual_min_subset_identity(tmp_path: Path):
    p = _mk_year(tmp_path / "tasmin_2050.nc", "tasmin", 2050)
    y2p = {2050: {"tasmin": p}}
    metric = {
        "slug": "tnn_annual_min", "var": "tasmin", "value_col": "value",
        "compute": "annual_min_temperature", "params": {},
    }
    gdf, idx, sub_lat, sub_lon = _subset_grid()
    g_full = gs.GridSpec(lat=tuple(_GRID_LAT), lon=tuple(_GRID_LON))
    g_sub = gs.GridSpec(lat=tuple(sub_lat), lon=tuple(sub_lon))
    full = _rows_to_dict(_cr.compute_cold_risk_rows_for_metric(
        metric=metric, model="M", scenario="ssp245", year_to_paths=y2p, baseline_year_to_paths=y2p,
        weights=gs.build_area_weights(gdf, g_full, level="district"), level="district", index_range=None, grid=g_full,
    ))
    subset = _rows_to_dict(_cr.compute_cold_risk_rows_for_metric(
        metric=metric, model="M", scenario="ssp245", year_to_paths=y2p, baseline_year_to_paths=y2p,
        weights=gs.build_area_weights(gdf, g_sub, level="district"), level="district", index_range=idx, grid=g_sub,
    ))
    _assert_same_units(full, subset)


def test_extreme_pr_max_subset_identity(tmp_path: Path):
    p = _mk_year(tmp_path / "pr_2050.nc", "pr", 2050, kind="precip")
    y2p = {2050: {"pr": p}}
    metric = {"slug": "pr_max_1day_precip", "var": "pr", "value_col": "value", "params": {}}
    gdf, idx, sub_lat, sub_lon = _subset_grid()
    g_full = gs.GridSpec(lat=tuple(_GRID_LAT), lon=tuple(_GRID_LON))
    g_sub = gs.GridSpec(lat=tuple(sub_lat), lon=tuple(sub_lon))
    full = _rows_to_dict(_er.compute_extreme_rainfall_rows_for_metric(
        metric={**metric, "params": {"grid_id": g_full.grid_id}}, model="M", scenario="ssp245",
        year_to_paths=y2p, baseline_year_to_paths={}, weights=gs.build_area_weights(gdf, g_full, level="district"),
        level="district", index_range=None, grid=g_full,
    ))
    subset = _rows_to_dict(_er.compute_extreme_rainfall_rows_for_metric(
        metric={**metric, "params": {"grid_id": g_sub.grid_id}}, model="M", scenario="ssp245",
        year_to_paths=y2p, baseline_year_to_paths={}, weights=gs.build_area_weights(gdf, g_sub, level="district"),
        level="district", index_range=idx, grid=g_sub,
    ))
    _assert_same_units(full, subset)


def test_heat_stress_twb_multivar_subset_identity(tmp_path: Path):
    tas = _mk_year(tmp_path / "tas_2050.nc", "tas", 2050)
    hurs = _mk_year(tmp_path / "hurs_2050.nc", "hurs", 2050, kind="humidity")
    y2p = {2050: {"tas": tas, "hurs": hurs}}
    metric = {"slug": "twb_annual_mean", "var": "tas", "value_col": "value", "params": {}}
    gdf, idx, sub_lat, sub_lon = _subset_grid()
    g_full = gs.GridSpec(lat=tuple(_GRID_LAT), lon=tuple(_GRID_LON))
    g_sub = gs.GridSpec(lat=tuple(sub_lat), lon=tuple(sub_lon))
    full = _rows_to_dict(_hs.compute_heat_stress_rows_for_metric(
        metric=metric, model="M", scenario="ssp245", year_to_paths=y2p,
        weights=gs.build_area_weights(gdf, g_full, level="district"), level="district", index_range=None, grid=g_full,
    ))
    subset = _rows_to_dict(_hs.compute_heat_stress_rows_for_metric(
        metric=metric, model="M", scenario="ssp245", year_to_paths=y2p,
        weights=gs.build_area_weights(gdf, g_sub, level="district"), level="district", index_range=idx, grid=g_sub,
    ))
    _assert_same_units(full, subset)


def test_failure_before_write_leaves_no_cache(tmp_path: Path):
    # H3: a coord-drifted eval file must raise *and* not persist an annual cache.
    p = _mk_year(tmp_path / "tas_2050.nc", "tas", 2050)
    hurs = _mk_year(tmp_path / "hurs_2050.nc", "hurs", 2050, kind="humidity")
    y2p = {2050: {"tas": p, "hurs": hurs}}
    metric = {"slug": "twb_annual_mean", "var": "tas", "value_col": "value", "params": {"grid_id": "g1"}}
    gdf, idx, sub_lat, sub_lon = _subset_grid()
    # Mismatched GridSpec: claim a different latitude than the file actually has.
    wrong = gs.GridSpec(lat=(sub_lat[0] + 0.5, *sub_lat[1:]), lon=tuple(sub_lon))
    cache_root = tmp_path / "cache"
    with pytest.raises(ValueError):
        _hs.compute_heat_stress_rows_for_metric(
            metric=metric, model="M", scenario="ssp245", year_to_paths=y2p,
            weights=gs.build_area_weights(gdf, wrong, level="district"),
            level="district", cache_root=cache_root, index_range=idx, grid=wrong,
        )
    assert not list(cache_root.rglob("*.nc"))


# --- _atomic_replace: Windows sharing-violation retry/accept (CHG-0062) -------

def _sharing_error(msg: str = "sharing violation") -> OSError:
    """An OSError that looks like a Windows ERROR_ACCESS_DENIED to the helper."""
    err = OSError(errno.EACCES, msg)
    err.winerror = 5  # ERROR_ACCESS_DENIED (set explicitly so it is testable on Linux)
    return err


def test_atomic_replace_retries_then_succeeds(tmp_path: Path, monkeypatch):
    src = tmp_path / "src.bin"
    dst = tmp_path / "dst.bin"
    src.write_bytes(b"payload")
    real_replace = os.replace
    calls = {"n": 0}

    def flaky(a, b):
        calls["n"] += 1
        if calls["n"] <= 3:
            raise _sharing_error()
        return real_replace(a, b)

    monkeypatch.setattr(gs.os, "replace", flaky)
    moved = gs._atomic_replace(src, dst, accept_existing=True, base_delay=0.0, max_delay=0.0)
    assert moved is True
    assert calls["n"] == 4
    assert dst.read_bytes() == b"payload"
    assert not src.exists()


def test_atomic_replace_accepts_identical_existing(tmp_path: Path, monkeypatch):
    src = tmp_path / "src.bin"
    dst = tmp_path / "dst.bin"
    src.write_bytes(b"payload")
    dst.write_bytes(b"payload")  # concurrent winner published byte-identical content

    monkeypatch.setattr(gs.os, "replace", lambda a, b: (_ for _ in ()).throw(_sharing_error()))
    moved = gs._atomic_replace(src, dst, accept_existing=True, retries=3, base_delay=0.0, max_delay=0.0)
    assert moved is False  # accepted the existing equivalent payload
    assert dst.read_bytes() == b"payload"


def test_atomic_replace_rejects_differing_existing(tmp_path: Path, monkeypatch):
    src = tmp_path / "src.bin"
    dst = tmp_path / "dst.bin"
    src.write_bytes(b"ours")
    dst.write_bytes(b"theirs-different")  # exists but content differs -> must raise

    monkeypatch.setattr(gs.os, "replace", lambda a, b: (_ for _ in ()).throw(_sharing_error()))
    with pytest.raises(OSError):
        gs._atomic_replace(src, dst, accept_existing=True, retries=3, base_delay=0.0, max_delay=0.0)


def test_atomic_replace_raises_on_non_sharing_error(tmp_path: Path, monkeypatch):
    src = tmp_path / "src.bin"
    dst = tmp_path / "dst.bin"
    src.write_bytes(b"payload")
    calls = {"n": 0}

    def boom(a, b):
        calls["n"] += 1
        raise OSError(errno.ENOSPC, "no space")  # no winerror -> not a sharing race

    monkeypatch.setattr(gs.os, "replace", boom)
    with pytest.raises(OSError):
        gs._atomic_replace(src, dst, accept_existing=True, base_delay=0.0, max_delay=0.0)
    assert calls["n"] == 1  # immediate raise, no retry


def test_atomic_replace_sidecar_lands_or_raises(tmp_path: Path, monkeypatch):
    # accept_existing=False (the sidecar path): never silently accept a stale dst.
    src = tmp_path / "src.json"
    dst = tmp_path / "dst.json"
    src.write_text("{}")

    monkeypatch.setattr(gs.os, "replace", lambda a, b: (_ for _ in ()).throw(_sharing_error()))
    with pytest.raises(OSError):
        gs._atomic_replace(src, dst, retries=3, base_delay=0.0, max_delay=0.0)


def test_concurrent_grid_metric_writes_smoke(tmp_path: Path):
    # Extra coverage: many threads publishing the same cache path must not raise,
    # and the final cache must read back valid.
    path = tmp_path / "grid_metrics" / "slug" / "M" / "g1" / "ssp245" / "2050.nc"
    sidecar = {"method_version": gs.GRIDFIRST_SPATIAL_METHOD_VERSION, "grid_id": "g1"}
    ds = xr.Dataset(
        {"value": (("lat", "lon"), np.arange(4.0).reshape(2, 2))},
        coords={"lat": [10.0, 11.0], "lon": [77.0, 78.0]},
    )
    errors: list[BaseException] = []

    def worker():
        try:
            gs.write_grid_metric_cache(ds.copy(deep=True), path, sidecar=sidecar)
        except BaseException as exc:  # noqa: BLE001 - surface any escape
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    out = gs.read_grid_metric_cache(path, expected_sidecar=sidecar)
    assert out is not None
    np.testing.assert_array_equal(out["value"].values, ds["value"].values)

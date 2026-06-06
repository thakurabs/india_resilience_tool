"""Tests for the per-state bounding-box grid subset (memory fix).

Covers the shared helpers in ``gridfirst_spatial``: direction-agnostic index
range computation, positional subsetting, the cell/grid alignment guard, the
spatial-weights state token, the threshold-cache grid_id signature, and the
headline guarantee that a bbox-subset aggregation reproduces the full-grid
per-unit values bit-for-bit (within fp tolerance).
"""

from __future__ import annotations

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

"""Tests for the sub-grid-cell IDW climate fill (CHG-0305) and its provenance
plumbing (CHG-0306).

Sub-grid-cell coral atolls (e.g. Lakshadweep's Andrott/Bitra/Chetlat/Kiltan) sit
entirely inside a single ocean-masked (all-NaN) climate grid cell, so the base
area-weight aggregators either drop the unit or NaN it. ``subcell_idw_fill``
estimates a value for exactly those units from the finite cells on the same
state-cropped grid, and stamps ``climate_fill_method="idw"`` so the fill is
recorded in-data (native everywhere else). These tests cover:

* the fill fires for a sub-cell polygon whose only cell is NaN, with a
  hand-computable IDW value (both the cell-values and retention read paths);
* the negative guard — a multi-cell all-NaN polygon (arid-SPI3 analogue) and a
  full-cell polygon are NOT sub-cell, so they are left NaN;
* the size test is computed in the analysis CRS (holds at high latitude);
* the provenance survives the two silent master drops (``_collect_file_frame``
  reindex and the ``_build_wide_master`` value collapse) and the optimised
  publish whitelist (``_select_master_columns``).

The composite-propagation case lives in ``test_composite_metrics.py`` where the
component-master fixtures already exist.
"""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import box

from india_resilience_tool.compute import gridfirst_spatial as gs
from india_resilience_tool.compute import master_builder as mb


# --- fixtures -----------------------------------------------------------------

# 3x3 grid, 1-degree spacing, cell centres on integers. Cell (i, j) covers
# [centre-0.5, centre+0.5]. cell_index = i * n_lon + j.
_LAT = (11.0, 12.0, 13.0)
_LON = (78.0, 79.0, 80.0)


def _grid() -> gs.GridSpec:
    return gs.GridSpec(lat=_LAT, lon=_LON)


def _atoll_gdf(district_name: str = "ATOLL") -> gpd.GeoDataFrame:
    # A 0.2x0.2-degree polygon fully inside the centre cell (12, 79): ~1/25 of a
    # 1x1-degree cell -> unambiguously sub-cell.
    return gpd.GeoDataFrame(
        {"district_name": [district_name]},
        geometry=[box(78.9, 11.9, 79.1, 12.1)],
        crs="EPSG:4326",
    )


def _field(values_by_cell: dict[tuple[int, int], float]) -> xr.DataArray:
    """Build a (lat, lon) field, NaN everywhere except the given cells."""
    arr = np.full((3, 3), np.nan, dtype=float)
    for (i, j), v in values_by_cell.items():
        arr[i, j] = v
    return xr.DataArray(arr, dims=("lat", "lon"), coords={"lat": list(_LAT), "lon": list(_LON)})


# --- the fill fires (hand-computable IDW) -------------------------------------

def test_single_donor_fill_equals_that_donor():
    """One finite donor cell -> IDW is that donor's value regardless of distance."""
    grid = _grid()
    weights = gs.build_area_weights(_atoll_gdf(), grid, level="district")
    field = _field({(0, 0): 5.0})  # centre cell (1,1) is NaN; one donor at (0,0)
    fills = gs.subcell_idw_fill(field, weights, grid=grid)
    assert set(fills) == {"ATOLL"}
    assert fills["ATOLL"] == pytest.approx(5.0)


def test_two_equidistant_donors_average():
    """Two donors symmetric about the atoll centroid -> equal IDW weights -> mean."""
    grid = _grid()
    weights = gs.build_area_weights(_atoll_gdf(), grid, level="district")
    # Atoll centroid sits at the centre cell (12, 79); donors at (12, 78) and
    # (12, 80) are one degree of longitude either side -> equal great-circle
    # distance -> equal weight -> plain mean of 4 and 8.
    field = _field({(1, 0): 4.0, (1, 2): 8.0})
    fills = gs.subcell_idw_fill(field, weights, grid=grid)
    assert fills["ATOLL"] == pytest.approx(6.0)


def test_cell_values_path_drops_atoll_that_fill_recovers():
    """aggregate_cell_values drops the all-NaN-cell atoll; the fill re-adds it."""
    grid = _grid()
    weights = gs.build_area_weights(_atoll_gdf(), grid, level="district")
    field = _field({(0, 0): 5.0})
    base = gs.aggregate_cell_values(field, weights, grid=grid)
    assert "ATOLL" not in base  # symptom: row-absent
    fills = gs.subcell_idw_fill(field, weights, grid=grid)
    assert fills["ATOLL"] == pytest.approx(5.0)


def test_retention_path_nans_atoll_that_fill_recovers():
    """The retention floor NaNs the atoll (retained ~0); the fill recovers it."""
    from india_resilience_tool.compute.drought_risk_gridfirst import (
        aggregate_grid_values_with_retention,
    )

    grid = _grid()
    weights = gs.build_area_weights(_atoll_gdf(), grid, level="district")
    field = _field({(0, 0): 5.0})
    base = aggregate_grid_values_with_retention(field, weights, grid=grid)
    value, retained = base["ATOLL"]
    assert np.isnan(value) and retained == pytest.approx(0.0)  # symptom: row-present NaN
    fills = gs.subcell_idw_fill(field, weights, grid=grid)
    assert fills["ATOLL"] == pytest.approx(5.0)


# --- negative guards ----------------------------------------------------------

def test_no_fill_when_overlapping_cell_is_finite():
    """A sub-cell unit whose one cell has data is left to the base aggregator."""
    grid = _grid()
    weights = gs.build_area_weights(_atoll_gdf(), grid, level="district")
    field = _field({(1, 1): 3.0, (0, 0): 5.0})  # atoll's own cell (1,1) is finite
    assert gs.subcell_idw_fill(field, weights, grid=grid) == {}


def test_no_fill_for_multi_cell_all_nan_polygon():
    """Arid-SPI3 analogue: a multi-cell all-NaN block is not sub-cell -> no fill."""
    grid = _grid()
    # Spans all three cells of the middle row -> poly area ~= 3 cells, never < 0.5
    # of a single cell.
    multi = gpd.GeoDataFrame(
        {"district_name": ["BIG"]},
        geometry=[box(77.6, 11.6, 80.4, 12.4)],
        crs="EPSG:4326",
    )
    weights = gs.build_area_weights(multi, grid, level="district")
    field = _field({(0, 0): 5.0})  # its own row-1 cells are all NaN; donor elsewhere
    assert gs.subcell_idw_fill(field, weights, grid=grid) == {}


def test_no_fill_for_full_cell_polygon():
    """A polygon covering a whole cell has ratio ~1.0 -> not sub-cell (CRS-consistent)."""
    grid = _grid()
    full = gpd.GeoDataFrame(
        {"district_name": ["FULL"]},
        geometry=[box(78.5, 11.5, 79.5, 12.5)],  # exactly the centre cell
        crs="EPSG:4326",
    )
    weights = gs.build_area_weights(full, grid, level="district")
    field = _field({(0, 0): 5.0})
    assert gs.subcell_idw_fill(field, weights, grid=grid) == {}


def test_no_fill_when_no_finite_donor_anywhere():
    """A globally-NaN field cannot be filled; base NaN is preserved."""
    grid = _grid()
    weights = gs.build_area_weights(_atoll_gdf(), grid, level="district")
    field = _field({})  # entirely NaN
    assert gs.subcell_idw_fill(field, weights, grid=grid) == {}


# --- in-CRS size test (high latitude) -----------------------------------------

def test_subcell_size_test_holds_at_high_latitude():
    """The size test uses projected (m^2) areas, so a tiny high-latitude polygon is
    still classified sub-cell rather than mis-sized by a degree-area shortcut."""
    lat = (69.0, 70.0, 71.0)
    lon = (20.0, 21.0, 22.0)
    grid = gs.GridSpec(lat=lat, lon=lon)
    atoll = gpd.GeoDataFrame(
        {"district_name": ["POLAR"]},
        geometry=[box(20.9, 69.9, 21.1, 70.1)],
        crs="EPSG:4326",
    )
    weights = gs.build_area_weights(atoll, grid, level="district")
    field = xr.DataArray(
        np.array([[7.0, np.nan, np.nan], [np.nan, np.nan, np.nan], [np.nan, np.nan, np.nan]]),
        dims=("lat", "lon"),
        coords={"lat": list(lat), "lon": list(lon)},
    )
    fills = gs.subcell_idw_fill(field, weights, grid=grid)
    assert fills.get("POLAR") == pytest.approx(7.0)


# --- provenance plumbing: master_builder --------------------------------------

def _yearly_cols_block() -> list[str]:
    return ["block", "district", "state", "model", "scenario", "year", "value"]


def test_collect_file_frame_preserves_climate_fill_method(tmp_path):
    csv = tmp_path / "y.csv"
    pd.DataFrame(
        {
            "value": [1.0, 2.0],
            "year": [2020, 2021],
            "climate_fill_method": ["idw", "idw"],
        }
    ).to_csv(csv, index=False)
    frame = mb._collect_file_frame(csv, ["value"], "year", {"block": "B", "district": "D", "state": "S", "model": "M", "scenario": "ssp245"}, _yearly_cols_block())
    assert "climate_fill_method" in frame.columns
    assert list(frame["climate_fill_method"]) == ["idw", "idw"]


def test_collect_file_frame_absent_when_csv_lacks_column(tmp_path):
    csv = tmp_path / "y.csv"
    pd.DataFrame({"value": [1.0], "year": [2020]}).to_csv(csv, index=False)
    frame = mb._collect_file_frame(csv, ["value"], "year", {"block": "B", "district": "D", "state": "S", "model": "M", "scenario": "ssp245"}, _yearly_cols_block())
    assert "climate_fill_method" not in frame.columns


def _df_all_block() -> pd.DataFrame:
    # Two units: ATOLL (idw across all its rows) and MAIN (native). Multiple
    # scenario/period/model rows per unit to exercise the per-unit reduction.
    rows = []
    for scenario, period in [("historical", "1990-2010"), ("ssp585", "2040-2060")]:
        for model in ("m1", "m2"):
            rows.append({"block": "ATOLL", "district": "D", "state": "S", "model": model, "scenario": scenario, "period": period, "value": 5.0, "climate_fill_method": "idw"})
            rows.append({"block": "MAIN", "district": "D", "state": "S", "model": model, "scenario": scenario, "period": period, "value": 3.0, "climate_fill_method": "native"})
    return pd.DataFrame(rows)


def test_build_wide_master_carries_per_unit_provenance():
    master = mb._build_wide_master(_df_all_block(), "value", "block", verbose=False)
    assert "climate_fill_method" in master.columns
    by_block = dict(zip(master["block"], master["climate_fill_method"]))
    assert by_block == {"ATOLL": "idw", "MAIN": "native"}


def test_build_wide_master_any_idw_wins_for_mixed_unit():
    df = _df_all_block()
    # Flip one of ATOLL's rows to native: the per-unit reduction must still read idw.
    idx = df.index[(df["block"] == "ATOLL")][0]
    df.loc[idx, "climate_fill_method"] = "native"
    master = mb._build_wide_master(df, "value", "block", verbose=False)
    by_block = dict(zip(master["block"], master["climate_fill_method"]))
    assert by_block["ATOLL"] == "idw"


def test_build_wide_master_no_column_when_absent():
    df = _df_all_block().drop(columns=["climate_fill_method"])
    master = mb._build_wide_master(df, "value", "block", verbose=False)
    assert "climate_fill_method" not in master.columns


# --- provenance plumbing: optimised publish whitelist -------------------------

def test_select_master_columns_keeps_climate_fill_method():
    from tools.optimized import build_processed_optimised as bpo

    df = pd.DataFrame(
        {
            "block": ["ATOLL"],
            "district": ["D"],
            "state": ["S"],
            "tas_annual_mean__ssp585__2040-2060__mean": [1.0],
            "climate_fill_method": ["idw"],
        }
    )
    out = bpo._select_master_columns(df, slug="tas_annual_mean", level="block", supported_stats=["mean"])
    assert "climate_fill_method" in out.columns
    assert list(out["climate_fill_method"]) == ["idw"]

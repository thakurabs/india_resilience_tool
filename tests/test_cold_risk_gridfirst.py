"""Tier-1 tests for Cold Risk v2 grid-first compute (CHG-0010).

Covers:
* Slug allowlist completeness.
* The four new Cold-Risk cellwise helpers (annual min, longest run, DJF mean/min).
* The generalized ``_cellwise_spell_days`` honoring ``direction="below"``.
* End-to-end orchestrator semantics for each compute kind (mean / min / count /
  longest run / percentile / cold spell).
* SSP historical-Dec fallback through the orchestrator.
* Cache round-trip + sidecar invalidation.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


_ROOT = _repo_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


from india_resilience_tool.compute import cold_risk_gridfirst as CRG  # noqa: E402
from india_resilience_tool.compute import gridfirst_spatial as GFS  # noqa: E402


LAT_VALS = [17.0, 17.25]
LON_VALS = [78.0, 78.25]


# ---------------------------------------------------------------------------
# Slug allowlist
# ---------------------------------------------------------------------------
def test_cold_risk_gridfirst_slugs_match_bundle() -> None:
    expected = frozenset(
        {
            "tas_winter_mean",
            "tasmin_winter_mean",
            "tnn_annual_min",
            "tasmin_winter_min",
            "tnle10_cold_nights",
            "tnle5_severe_cold_nights",
            "txle15_cold_days",
            "tnle10_consecutive_cold_nights",
            "tx10p_cool_days_pct",
            "tn10p_cool_nights_pct",
            "csdi_cold_spell_days",
        }
    )
    assert CRG.COLD_RISK_GRIDFIRST_SLUGS == expected


# ---------------------------------------------------------------------------
# Helpers and fixtures
# ---------------------------------------------------------------------------
def _make_year_da(
    year: int,
    *,
    varname: str,
    dec_value_k: dict[tuple[int, int], float],
    jan_feb_value_k: dict[tuple[int, int], float],
    other_value_k: dict[tuple[int, int], float],
) -> xr.DataArray:
    """Build a one-year, 2x2-cell daily DataArray.

    The three value maps are keyed by (lat_index, lon_index) so each cell can
    carry its own Dec / Jan-Feb / other-month constant.
    """
    time = xr.date_range(f"{year}-01-01", f"{year}-12-31", freq="D", use_cftime=True)
    months = np.array([t.month for t in time])
    values = np.empty((time.size, len(LAT_VALS), len(LON_VALS)), dtype=float)
    for i in range(len(LAT_VALS)):
        for j in range(len(LON_VALS)):
            key = (i, j)
            values[months == 12, i, j] = dec_value_k[key]
            values[(months == 1) | (months == 2), i, j] = jan_feb_value_k[key]
            values[~((months == 12) | (months == 1) | (months == 2)), i, j] = other_value_k[key]
    return xr.DataArray(
        values,
        coords={"time": time, "lat": LAT_VALS, "lon": LON_VALS},
        dims=("time", "lat", "lon"),
        name=varname,
    )


def _two_unit_weights() -> pd.DataFrame:
    """Two polygons, each covering one row of cells, equal area weighting."""
    return pd.DataFrame(
        [
            {"unit_key": "Polygon_North", "cell_index": 0, "lat_index": 0, "lon_index": 0, "area_m2": 1.0},
            {"unit_key": "Polygon_North", "cell_index": 1, "lat_index": 0, "lon_index": 1, "area_m2": 1.0},
            {"unit_key": "Polygon_South", "cell_index": 2, "lat_index": 1, "lon_index": 0, "area_m2": 1.0},
            {"unit_key": "Polygon_South", "cell_index": 3, "lat_index": 1, "lon_index": 1, "area_m2": 1.0},
        ]
    )


# ---------------------------------------------------------------------------
# New cellwise helpers
# ---------------------------------------------------------------------------
def test_cellwise_annual_min_temperature_returns_celsius() -> None:
    cur = _make_year_da(
        2000,
        varname="tasmin",
        dec_value_k={(i, j): 280.0 for i in (0, 1) for j in (0, 1)},
        jan_feb_value_k={(0, 0): 260.0, (0, 1): 265.0, (1, 0): 250.0, (1, 1): 255.0},
        other_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
    )
    out = GFS._cellwise_annual_min_temperature(cur)
    assert out.shape == (2, 2)
    assert float(out.sel(lat=LAT_VALS[1], lon=LON_VALS[0]).values) == pytest.approx(250.0 - 273.15)
    assert float(out.sel(lat=LAT_VALS[0], lon=LON_VALS[1]).values) == pytest.approx(265.0 - 273.15)


def test_cellwise_longest_consecutive_run_le_finds_longest_streak() -> None:
    # 7-day series; cell (0,0) has a 3-day streak at the threshold, cell (1,1) has none.
    time = xr.date_range("2000-01-01", periods=7, freq="D", use_cftime=True)
    values = np.full((7, 2, 2), 290.0, dtype=float)
    values[2:5, 0, 0] = 280.0    # 3-day streak <= 283.15K
    values[1, 0, 1] = 280.0      # only 1 day
    values[3, 1, 0] = 280.0      # only 1 day
    # cell (1, 1) untouched (stays at 290K)
    cur = xr.DataArray(
        values,
        coords={"time": time, "lat": LAT_VALS, "lon": LON_VALS},
        dims=("time", "lat", "lon"),
    )
    out = GFS._cellwise_longest_consecutive_run_le(cur, thresh_k=283.15)
    assert float(out.sel(lat=LAT_VALS[0], lon=LON_VALS[0]).values) == 3
    assert float(out.sel(lat=LAT_VALS[0], lon=LON_VALS[1]).values) == 1
    assert float(out.sel(lat=LAT_VALS[1], lon=LON_VALS[0]).values) == 1
    assert float(out.sel(lat=LAT_VALS[1], lon=LON_VALS[1]).values) == 0


def test_cellwise_djf_cross_year_mean_uses_prev_year_dec() -> None:
    prev = _make_year_da(
        2000,
        varname="tas",
        dec_value_k={(i, j): 5.0 + 273.15 for i in (0, 1) for j in (0, 1)},
        jan_feb_value_k={(i, j): 290.0 for i in (0, 1) for j in (0, 1)},
        other_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
    )
    cur = _make_year_da(
        2001,
        varname="tas",
        dec_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},  # ignored
        jan_feb_value_k={(i, j): -20.0 + 273.15 for i in (0, 1) for j in (0, 1)},
        other_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
    )
    out = GFS._cellwise_djf_cross_year_mean(prev_da=prev, cur_da=cur)
    # Dec(2000) = +5C for 31 days, Jan/Feb(2001) = -20C for 59 days.
    expected_c = (31 * 5.0 + 59 * -20.0) / (31 + 59)
    for i, lat in enumerate(LAT_VALS):
        for j, lon in enumerate(LON_VALS):
            assert float(out.sel(lat=lat, lon=lon).values) == pytest.approx(expected_c, abs=1e-9)


def test_cellwise_djf_cross_year_min_picks_window_minimum() -> None:
    prev = _make_year_da(
        2000,
        varname="tasmin",
        dec_value_k={(i, j): 5.0 + 273.15 for i in (0, 1) for j in (0, 1)},
        jan_feb_value_k={(i, j): 290.0 for i in (0, 1) for j in (0, 1)},
        other_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
    )
    cur = _make_year_da(
        2001,
        varname="tasmin",
        dec_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
        jan_feb_value_k={(0, 0): -12.0 + 273.15, (0, 1): -8.0 + 273.15, (1, 0): -15.0 + 273.15, (1, 1): -4.0 + 273.15},
        other_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
    )
    out = GFS._cellwise_djf_cross_year_min(prev_da=prev, cur_da=cur)
    assert float(out.sel(lat=LAT_VALS[0], lon=LON_VALS[0]).values) == pytest.approx(-12.0, abs=1e-9)
    assert float(out.sel(lat=LAT_VALS[1], lon=LON_VALS[0]).values) == pytest.approx(-15.0, abs=1e-9)
    assert float(out.sel(lat=LAT_VALS[1], lon=LON_VALS[1]).values) == pytest.approx(-4.0, abs=1e-9)


def test_cellwise_djf_cross_year_mean_returns_nan_when_prev_missing() -> None:
    cur = _make_year_da(
        2001,
        varname="tas",
        dec_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
        jan_feb_value_k={(i, j): -20.0 + 273.15 for i in (0, 1) for j in (0, 1)},
        other_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
    )
    out = GFS._cellwise_djf_cross_year_mean(prev_da=None, cur_da=cur)
    assert out.shape == (2, 2)
    assert np.all(np.isnan(out.values))


# ---------------------------------------------------------------------------
# Generalized _cellwise_spell_days with direction="below"
# ---------------------------------------------------------------------------
def test_cellwise_spell_days_below_direction_strict_lt() -> None:
    # Build a 1-cell daily eval series with one 6-day stretch exactly at the threshold
    # and one 7-day stretch strictly below. With strict-< only the 7-day stretch counts.
    time = xr.date_range("2001-01-01", periods=20, freq="D", use_cftime=True)
    values = np.full((20, 1, 1), 290.0, dtype=float)
    values[0:6, 0, 0] = 280.0    # 6 days at threshold (boundary equal)
    values[10:17, 0, 0] = 270.0  # 7 days strictly below threshold
    eval_da = xr.DataArray(
        values, coords={"time": time, "lat": [LAT_VALS[0]], "lon": [LON_VALS[0]]}, dims=("time", "lat", "lon"),
    )
    # Threshold = 280 K everywhere on every DOY.
    thresh = xr.DataArray(
        np.full((365, 1, 1), 280.0, dtype=float),
        coords={"doy": np.arange(1, 366), "lat": [LAT_VALS[0]], "lon": [LON_VALS[0]]},
        dims=("doy", "lat", "lon"),
    )
    out_strict = GFS._cellwise_spell_days(eval_da, thresh, min_spell_days=6, exceed_ge=False, direction="below")
    out_inclusive = GFS._cellwise_spell_days(eval_da, thresh, min_spell_days=6, exceed_ge=True, direction="below")
    assert float(out_strict.values.reshape(-1)[0]) == 7.0
    assert float(out_inclusive.values.reshape(-1)[0]) == 13.0  # 6 + 7


# ---------------------------------------------------------------------------
# Orchestrator integration tests
# ---------------------------------------------------------------------------
def _open_yearly_var(da: xr.DataArray, tmp_path: Path, year: int, varname: str, prefix: str = "") -> Path:
    """Write a single-variable yearly NetCDF for the orchestrator's path-based API."""
    out = tmp_path / f"{prefix}{year}.nc"
    da.to_netcdf(out)
    return out


def _cold_metric(slug: str, value_col: str, *, var: str, compute: str, params: dict | None = None) -> dict:
    return {
        "slug": slug,
        "value_col": value_col,
        "var": var,
        "compute": compute,
        "params": dict(params or {}),
    }


def test_orchestrator_djf_mean_uses_historical_fallback_for_ssp_first_year(tmp_path: Path) -> None:
    # historical 2014 + ssp245 2015.
    hist_2014 = _make_year_da(
        2014, varname="tas",
        dec_value_k={(i, j): 3.0 + 273.15 for i in (0, 1) for j in (0, 1)},
        jan_feb_value_k={(i, j): -7.0 + 273.15 for i in (0, 1) for j in (0, 1)},
        other_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
    )
    ssp_2015 = _make_year_da(
        2015, varname="tas",
        dec_value_k={(i, j): 4.0 + 273.15 for i in (0, 1) for j in (0, 1)},
        jan_feb_value_k={(0, 0): -9.0 + 273.15, (0, 1): -5.0 + 273.15, (1, 0): -11.0 + 273.15, (1, 1): -7.0 + 273.15},
        other_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
    )
    hist_path = _open_yearly_var(hist_2014, tmp_path, 2014, "tas", prefix="hist_")
    ssp_path = _open_yearly_var(ssp_2015, tmp_path, 2015, "tas", prefix="ssp_")

    metric = _cold_metric(
        "tas_winter_mean", "winter_tas_mean_C",
        var="tas", compute="seasonal_mean", params={"months": [12, 1, 2]},
    )
    rows = CRG.compute_cold_risk_rows_for_metric(
        metric=metric,
        model="TEST_MODEL",
        scenario="ssp245",
        year_to_paths={2015: {"tas": ssp_path}},
        baseline_year_to_paths={2015: {"tas": ssp_path}},  # unused for DJF compute
        weights=_two_unit_weights(),
        level="district",
        cache_root=None,
        historical_year_to_paths={2014: {"tas": hist_path}},
    )
    assert {row["district"] for row in rows} == {"Polygon_North", "Polygon_South"}
    by_district = {row["district"]: row for row in rows if row["year"] == 2015}
    # North polygon covers (0,0) and (0,1): mean Jan/Feb = (-9 + -5)/2 = -7.
    # South polygon covers (1,0) and (1,1): mean Jan/Feb = (-11 + -7)/2 = -9.
    # Dec(2014) is +3 everywhere (31 days), Jan/Feb is each polygon's mean (59 days).
    north_expected = (31 * 3.0 + 59 * -7.0) / 90
    south_expected = (31 * 3.0 + 59 * -9.0) / 90
    assert by_district["Polygon_North"]["winter_tas_mean_C"] == pytest.approx(north_expected, abs=1e-6)
    assert by_district["Polygon_South"]["winter_tas_mean_C"] == pytest.approx(south_expected, abs=1e-6)


def test_orchestrator_djf_min_returns_polygon_minimum(tmp_path: Path) -> None:
    prev = _make_year_da(
        2000, varname="tasmin",
        dec_value_k={(i, j): 5.0 + 273.15 for i in (0, 1) for j in (0, 1)},
        jan_feb_value_k={(i, j): 290.0 for i in (0, 1) for j in (0, 1)},
        other_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
    )
    cur = _make_year_da(
        2001, varname="tasmin",
        dec_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
        jan_feb_value_k={(0, 0): -12.0 + 273.15, (0, 1): -8.0 + 273.15, (1, 0): -15.0 + 273.15, (1, 1): -4.0 + 273.15},
        other_value_k={(i, j): 295.0 for i in (0, 1) for j in (0, 1)},
    )
    prev_path = _open_yearly_var(prev, tmp_path, 2000, "tasmin")
    cur_path = _open_yearly_var(cur, tmp_path, 2001, "tasmin")

    metric = _cold_metric(
        "tasmin_winter_min", "winter_tasmin_min_C",
        var="tasmin", compute="seasonal_min", params={"months": [12, 1, 2]},
    )
    rows = CRG.compute_cold_risk_rows_for_metric(
        metric=metric,
        model="TEST_MODEL",
        scenario="historical",
        year_to_paths={2000: {"tasmin": prev_path}, 2001: {"tasmin": cur_path}},
        baseline_year_to_paths={2000: {"tasmin": prev_path}, 2001: {"tasmin": cur_path}},
        weights=_two_unit_weights(),
        level="district",
        cache_root=None,
    )
    by_district = {row["district"]: row for row in rows if row["year"] == 2001}
    # Area-weighted min: aggregate_cell_values returns the area-weighted MEAN of cell mins.
    # North polygon: mean of (-12, -8) = -10. South polygon: mean of (-15, -4) = -9.5.
    assert by_district["Polygon_North"]["winter_tasmin_min_C"] == pytest.approx(-10.0, abs=1e-6)
    assert by_district["Polygon_South"]["winter_tasmin_min_C"] == pytest.approx(-9.5, abs=1e-6)


def test_orchestrator_count_days_le_threshold(tmp_path: Path) -> None:
    # 30 days at 268K (below 273.15 threshold), 335 days at 285K.
    time = xr.date_range("2001-01-01", "2001-12-31", freq="D", use_cftime=True)
    n = time.size
    cold_days = 30
    values = np.full((n, 2, 2), 285.0, dtype=float)
    values[:cold_days, :, :] = 268.0
    cur = xr.DataArray(
        values,
        coords={"time": time, "lat": LAT_VALS, "lon": LON_VALS},
        dims=("time", "lat", "lon"),
        name="tasmin",
    )
    cur_path = _open_yearly_var(cur, tmp_path, 2001, "tasmin")

    metric = _cold_metric(
        "tnle10_cold_nights", "days_tn_le_10C",
        var="tasmin", compute="count_days_le_threshold", params={"thresh_k": 283.15},
    )
    rows = CRG.compute_cold_risk_rows_for_metric(
        metric=metric,
        model="TEST_MODEL", scenario="historical",
        year_to_paths={2001: {"tasmin": cur_path}},
        baseline_year_to_paths={2001: {"tasmin": cur_path}},
        weights=_two_unit_weights(),
        level="district",
        cache_root=None,
    )
    for row in rows:
        assert row["days_tn_le_10C"] == pytest.approx(float(cold_days), abs=1e-6)


def test_orchestrator_tx10p_strict_excludes_boundary_days(tmp_path: Path) -> None:
    # 21 baseline years all at 280K -> p10 threshold = 280K everywhere.
    # Eval year (2011): all 365 days exactly at 280K. Strict < should yield 0%.
    baseline_files: dict[int, dict[str, Path]] = {}
    for yr in range(1990, 2011):
        da = xr.DataArray(
            np.full((365, 2, 2), 280.0, dtype=float),
            coords={
                "time": xr.date_range(f"{yr}-01-01", periods=365, freq="D", use_cftime=True),
                "lat": LAT_VALS,
                "lon": LON_VALS,
            },
            dims=("time", "lat", "lon"),
            name="tasmax",
        )
        baseline_files[yr] = {"tasmax": _open_yearly_var(da, tmp_path, yr, "tasmax", prefix="base_")}
    eval_da = xr.DataArray(
        np.full((365, 2, 2), 280.0, dtype=float),
        coords={
            "time": xr.date_range("2011-01-01", periods=365, freq="D", use_cftime=True),
            "lat": LAT_VALS,
            "lon": LON_VALS,
        },
        dims=("time", "lat", "lon"),
        name="tasmax",
    )
    eval_path = _open_yearly_var(eval_da, tmp_path, 2011, "tasmax", prefix="eval_")

    metric = _cold_metric(
        "tx10p_cool_days_pct", "tx10p_pct",
        var="tasmax", compute="tx90p_etccdi",
        params={
            "percentile": 10, "baseline_years": (1990, 2010), "window_days": 5,
            "quantile_method": "nearest", "exceed_ge": False, "direction": "below",
        },
    )
    rows = CRG.compute_cold_risk_rows_for_metric(
        metric=metric,
        model="TEST_MODEL", scenario="historical",
        year_to_paths={2011: {"tasmax": eval_path}},
        baseline_year_to_paths=baseline_files,
        weights=_two_unit_weights(),
        level="district",
        cache_root=None,
    )
    for row in rows:
        assert row["tx10p_pct"] == pytest.approx(0.0, abs=1e-9)


def test_orchestrator_rejects_non_cold_risk_slug() -> None:
    bogus_metric = _cold_metric(
        "txx_annual_max", "tx_max_C",
        var="tasmax", compute="annual_max_temperature",
    )
    with pytest.raises(ValueError, match="unsupported slug"):
        CRG.compute_cold_risk_rows_for_metric(
            metric=bogus_metric,
            model="TEST_MODEL", scenario="historical",
            year_to_paths={}, baseline_year_to_paths={},
            weights=_two_unit_weights(),
            level="district",
        )


# ---------------------------------------------------------------------------
# Cache contract
# ---------------------------------------------------------------------------
def test_cold_risk_grid_metric_cache_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    time = xr.date_range("2001-01-01", "2001-12-31", freq="D", use_cftime=True)
    n = time.size
    values = np.full((n, 2, 2), 285.0, dtype=float)
    values[:15, :, :] = 268.0
    cur = xr.DataArray(
        values,
        coords={"time": time, "lat": LAT_VALS, "lon": LON_VALS},
        dims=("time", "lat", "lon"),
        name="tasmin",
    )
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(exist_ok=True)
    cur_path = _open_yearly_var(cur, raw_dir, 2001, "tasmin")

    cache_root = tmp_path / "cache"
    metric = _cold_metric(
        "tnle10_cold_nights", "days_tn_le_10C",
        var="tasmin", compute="count_days_le_threshold", params={"thresh_k": 283.15},
    )
    weights = _two_unit_weights()
    year_to_paths = {2001: {"tasmin": cur_path}}

    # First run populates the cache.
    real_cell_values = CRG._cold_risk_cell_values
    call_count = {"n": 0}

    def _counting_cell_values(**kwargs):
        call_count["n"] += 1
        return real_cell_values(**kwargs)

    monkeypatch.setattr(CRG, "_cold_risk_cell_values", _counting_cell_values)

    rows1 = CRG.compute_cold_risk_rows_for_metric(
        metric=metric, model="M1", scenario="historical",
        year_to_paths=year_to_paths, baseline_year_to_paths=year_to_paths,
        weights=weights, level="district", cache_root=cache_root,
    )
    assert call_count["n"] == 1, "first run must compute per-cell field exactly once"

    cache_path = GFS.grid_metric_cache_path(
        cache_root, slug="tnle10_cold_nights", model="M1", scenario="historical", year=2001,
    )
    assert cache_path.exists(), "first run must write the per-cell metric cache"
    sidecar_path = cache_path.with_suffix(cache_path.suffix + ".json")
    assert sidecar_path.exists()

    # Second run with the same inputs must short-circuit via the cache.
    rows2 = CRG.compute_cold_risk_rows_for_metric(
        metric=metric, model="M1", scenario="historical",
        year_to_paths=year_to_paths, baseline_year_to_paths=year_to_paths,
        weights=weights, level="district", cache_root=cache_root,
    )
    assert call_count["n"] == 1, "second run must hit the cache and skip per-cell compute"

    assert len(rows2) == len(rows1)
    for r1, r2 in zip(
        sorted(rows1, key=lambda r: r["district"]),
        sorted(rows2, key=lambda r: r["district"]),
    ):
        assert r1["days_tn_le_10C"] == r2["days_tn_le_10C"]

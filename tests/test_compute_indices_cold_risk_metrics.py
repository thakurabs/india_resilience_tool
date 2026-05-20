from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest
import xarray as xr


def _repo_root() -> Path:
    """Find repository root (assumes tests/ is directly under repo root)."""
    return Path(__file__).resolve().parents[1]


_ROOT = _repo_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.pipeline import compute_indices_multiprocess as CMP  # noqa: E402


def test_seasonal_min_respects_requested_months() -> None:
    da = xr.DataArray(
        np.array([280.15, 270.15, 275.15, 260.15], dtype=float).reshape(4, 1, 1),
        coords={
            "time": xr.date_range("2000-01-01", periods=4, freq="MS", use_cftime=True),
            "lat": [17.0],
            "lon": [78.0],
        },
        dims=("time", "lat", "lon"),
    )
    mask = xr.DataArray(np.array([[True]]), coords={"lat": [17.0], "lon": [78.0]}, dims=("lat", "lon"))

    result = CMP.seasonal_min(da, mask, months=[1, 2, 3])

    assert result == pytest.approx(-3.0)


def test_count_days_le_threshold_is_inclusive(monkeypatch: pytest.MonkeyPatch) -> None:
    da = xr.DataArray(
        np.array([282.15, 283.15, 284.15], dtype=float),
        coords={"time": xr.date_range("2000-01-01", periods=3, freq="D", use_cftime=True)},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_get_district_daily_mean", lambda *args, **kwargs: da)

    result = CMP.count_days_le_threshold(None, None, thresh_k=283.15)

    assert result == 2


def test_longest_consecutive_run_le_threshold_returns_longest_streak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    da = xr.DataArray(
        np.array([284.15, 283.15, 282.15, 285.15, 281.15, 280.15, 279.15], dtype=float),
        coords={"time": xr.date_range("2000-01-01", periods=7, freq="D", use_cftime=True)},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_get_district_daily_mean", lambda *args, **kwargs: da)

    result = CMP.longest_consecutive_run_le_threshold(None, None, thresh_k=283.15, min_len=1)

    assert result == 3


def test_tnle5_is_monotonic_relative_to_tnle10(monkeypatch: pytest.MonkeyPatch) -> None:
    da = xr.DataArray(
        np.array([276.15, 278.15, 281.15, 283.15, 285.15], dtype=float),
        coords={"time": xr.date_range("2000-01-01", periods=5, freq="D", use_cftime=True)},
        dims=("time",),
    )
    monkeypatch.setattr(CMP, "_get_district_daily_mean", lambda *args, **kwargs: da)

    le_5 = CMP.count_days_le_threshold(None, None, thresh_k=278.15)
    le_10 = CMP.count_days_le_threshold(None, None, thresh_k=283.15)

    assert le_5 == 2
    assert le_10 == 4
    assert le_5 <= le_10


# =============================================================================
# DJF cross-year helper tests (CHG-0003 / audit issue B7-B9)
# =============================================================================
LAT = 17.0
LON = 78.0


def _write_year_netcdf(
    path: Path,
    *,
    varname: str,
    year: int,
    dec_value_k: float,
    jan_feb_value_k: float,
    other_value_k: float,
) -> None:
    """Write a one-year, one-cell daily NetCDF file used by DJF helper tests.

    Days in December receive ``dec_value_k`` (Kelvin), days in January/February
    receive ``jan_feb_value_k``, and all other days receive ``other_value_k``.
    """
    time = xr.date_range(f"{year}-01-01", f"{year}-12-31", freq="D", use_cftime=True)
    values = np.full((time.size, 1, 1), float(other_value_k), dtype=float)
    months = np.array([t.month for t in time])
    values[months == 12, 0, 0] = float(dec_value_k)
    values[(months == 1) | (months == 2), 0, 0] = float(jan_feb_value_k)

    da = xr.DataArray(
        values,
        coords={"time": time, "lat": [LAT], "lon": [LON]},
        dims=("time", "lat", "lon"),
        name=varname,
    )
    da.to_netcdf(path)


def _djf_metric(slug: str, value_col: str, *, var: str, compute: str) -> dict:
    return {
        "slug": slug,
        "value_col": value_col,
        "var": var,
        "compute": compute,
        "params": {"months": [12, 1, 2]},
    }


def _single_cell_mask() -> dict[str, xr.DataArray]:
    mask = xr.DataArray(
        np.array([[True]]),
        coords={"lat": [LAT], "lon": [LON]},
        dims=("lat", "lon"),
    )
    return {"TestDistrict": mask}


def _rows_to_dict(rows: list[dict], value_col: str) -> dict[int, float]:
    return {int(r["year"]): r[value_col] for r in rows}


def test_djf_cross_year_mean_uses_prev_year_december(tmp_path: Path) -> None:
    # year 2000 has Jan/Feb cold, Dec warm; year 2001 has Jan/Feb very cold, Dec mild.
    # DJF(2001) should average Dec(2000)=+5°C, Jan-Feb(2001)=-20°C, NOT Dec(2001).
    file_2000 = tmp_path / "2000.nc"
    file_2001 = tmp_path / "2001.nc"
    _write_year_netcdf(
        file_2000, varname="tas", year=2000,
        dec_value_k=5.0 + 273.15, jan_feb_value_k=-10.0 + 273.15, other_value_k=15.0 + 273.15,
    )
    _write_year_netcdf(
        file_2001, varname="tas", year=2001,
        dec_value_k=0.0 + 273.15, jan_feb_value_k=-20.0 + 273.15, other_value_k=15.0 + 273.15,
    )

    metric = _djf_metric("tas_winter_mean", "winter_tas_mean_C", var="tas", compute="seasonal_mean")
    year_to_paths = {2000: {"tas": file_2000}, 2001: {"tas": file_2001}}

    rows = CMP._compute_seasonal_mean_djf_cross_year_rows_for_metric(
        metric=metric,
        model="TEST_MODEL",
        scenario="historical",
        year_to_paths=year_to_paths,
        masks=_single_cell_mask(),
        level="district",
    )

    values = _rows_to_dict(rows, "winter_tas_mean_C")
    # year 2000 has no prev-year file -> NaN.
    assert np.isnan(values[2000])
    # year 2001: Dec(2000) = +5°C (31 days), Jan/Feb(2001) = -20°C (31+28 days).
    # Weighted mean = (31 * 5 + 59 * -20) / 90 = (155 - 1180) / 90 = -11.388...
    expected = (31 * 5.0 + 59 * -20.0) / (31 + 59)
    assert values[2001] == pytest.approx(expected, abs=1e-6)


def test_djf_cross_year_min_picks_coldest_across_window(tmp_path: Path) -> None:
    file_2000 = tmp_path / "2000.nc"
    file_2001 = tmp_path / "2001.nc"
    _write_year_netcdf(
        file_2000, varname="tasmin", year=2000,
        dec_value_k=5.0 + 273.15, jan_feb_value_k=-3.0 + 273.15, other_value_k=15.0 + 273.15,
    )
    _write_year_netcdf(
        file_2001, varname="tasmin", year=2001,
        dec_value_k=2.0 + 273.15, jan_feb_value_k=-12.0 + 273.15, other_value_k=15.0 + 273.15,
    )

    metric = _djf_metric("tasmin_winter_min", "winter_tasmin_min_C", var="tasmin", compute="seasonal_min")
    year_to_paths = {2000: {"tasmin": file_2000}, 2001: {"tasmin": file_2001}}

    rows = CMP._compute_seasonal_min_djf_cross_year_rows_for_metric(
        metric=metric,
        model="TEST_MODEL",
        scenario="historical",
        year_to_paths=year_to_paths,
        masks=_single_cell_mask(),
        level="district",
    )

    values = _rows_to_dict(rows, "winter_tasmin_min_C")
    # DJF(2001) min spans Dec(2000)=+5, Jan/Feb(2001)=-12 -> coldest is -12.
    assert values[2001] == pytest.approx(-12.0, abs=1e-6)
    # year 2000 still NaN (no prev-year file).
    assert np.isnan(values[2000])


def test_djf_cross_year_first_year_without_history_is_nan(tmp_path: Path) -> None:
    file_2001 = tmp_path / "2001.nc"
    _write_year_netcdf(
        file_2001, varname="tas", year=2001,
        dec_value_k=0.0 + 273.15, jan_feb_value_k=-5.0 + 273.15, other_value_k=15.0 + 273.15,
    )

    metric = _djf_metric("tas_winter_mean", "winter_tas_mean_C", var="tas", compute="seasonal_mean")
    year_to_paths = {2001: {"tas": file_2001}}

    rows = CMP._compute_seasonal_mean_djf_cross_year_rows_for_metric(
        metric=metric,
        model="TEST_MODEL",
        scenario="historical",
        year_to_paths=year_to_paths,
        masks=_single_cell_mask(),
        level="district",
    )

    values = _rows_to_dict(rows, "winter_tas_mean_C")
    # No previous-year file at all, no historical fallback supplied -> NaN.
    assert np.isnan(values[2001])


def test_djf_cross_year_uses_historical_fallback_for_ssp_first_year(tmp_path: Path) -> None:
    # Simulate the SSP boundary: historical archive holds 2014, SSP holds 2015+.
    hist_2014 = tmp_path / "historical_2014.nc"
    ssp_2015 = tmp_path / "ssp_2015.nc"
    _write_year_netcdf(
        hist_2014, varname="tas", year=2014,
        dec_value_k=3.0 + 273.15, jan_feb_value_k=-7.0 + 273.15, other_value_k=15.0 + 273.15,
    )
    _write_year_netcdf(
        ssp_2015, varname="tas", year=2015,
        dec_value_k=4.0 + 273.15, jan_feb_value_k=-9.0 + 273.15, other_value_k=15.0 + 273.15,
    )

    metric = _djf_metric("tas_winter_mean", "winter_tas_mean_C", var="tas", compute="seasonal_mean")
    ssp_year_to_paths = {2015: {"tas": ssp_2015}}
    historical_year_to_paths = {2014: {"tas": hist_2014}}

    rows = CMP._compute_seasonal_mean_djf_cross_year_rows_for_metric(
        metric=metric,
        model="TEST_MODEL",
        scenario="ssp245",
        year_to_paths=ssp_year_to_paths,
        masks=_single_cell_mask(),
        level="district",
        historical_year_to_paths=historical_year_to_paths,
    )

    values = _rows_to_dict(rows, "winter_tas_mean_C")
    # DJF(2015) = Dec(2014)=+3 (31 days), Jan/Feb(2015)=-9 (59 days).
    expected = (31 * 3.0 + 59 * -9.0) / (31 + 59)
    assert values[2015] == pytest.approx(expected, abs=1e-6)


def test_djf_cross_year_ssp_first_year_without_historical_fallback_is_nan(tmp_path: Path) -> None:
    ssp_2015 = tmp_path / "ssp_2015.nc"
    _write_year_netcdf(
        ssp_2015, varname="tas", year=2015,
        dec_value_k=4.0 + 273.15, jan_feb_value_k=-9.0 + 273.15, other_value_k=15.0 + 273.15,
    )

    metric = _djf_metric("tas_winter_mean", "winter_tas_mean_C", var="tas", compute="seasonal_mean")
    ssp_year_to_paths = {2015: {"tas": ssp_2015}}

    rows = CMP._compute_seasonal_mean_djf_cross_year_rows_for_metric(
        metric=metric,
        model="TEST_MODEL",
        scenario="ssp245",
        year_to_paths=ssp_year_to_paths,
        masks=_single_cell_mask(),
        level="district",
        historical_year_to_paths=None,
    )

    values = _rows_to_dict(rows, "winter_tas_mean_C")
    # Regression guard: without the historical-fallback inventory wired in,
    # the first SSP year must remain NaN. Catches accidental rewiring drops.
    assert np.isnan(values[2015])


# =============================================================================
# TX10p / TN10p strict-< boundary behavior
# CHG-0004 (audit issue D11) — exceed_ge=False must produce eva < threshold.
# =============================================================================
def _constant_daily_series(value_k: float, start: str, periods: int) -> xr.DataArray:
    """Build a constant-value daily series with cftime no-leap-friendly coords."""
    time = xr.date_range(start, periods=periods, freq="D", use_cftime=True)
    return xr.DataArray(
        np.full(periods, float(value_k), dtype=float),
        coords={"time": time},
        dims=("time",),
    )


def test_tx10p_etccdi_strict_excludes_boundary_days() -> None:
    # Baseline: constant 280K across (1990, 2010). p10 threshold equals 280K on every doy.
    # Eval year (2011): all days exactly equal to threshold.
    # Strict-< must yield 0% of days; inclusive (<=) would yield ~100%.
    baseline = _constant_daily_series(280.0, "1990-01-01", periods=21 * 365)
    evaluation = _constant_daily_series(280.0, "2011-01-01", periods=365)
    series = xr.concat([baseline, evaluation], dim="time")

    strict = CMP._compute_tx90p_etccdi_yearly(
        series=series,
        baseline_years=(1990, 2010),
        eval_years=[2011],
        percentile=10,
        window_days=5,
        exceed_ge=False,
        quantile_method="nearest",
        direction="below",
    )
    inclusive = CMP._compute_tx90p_etccdi_yearly(
        series=series,
        baseline_years=(1990, 2010),
        eval_years=[2011],
        percentile=10,
        window_days=5,
        exceed_ge=True,
        quantile_method="nearest",
        direction="below",
    )

    assert strict[2011] == pytest.approx(0.0, abs=1e-9), (
        "strict TX10p must exclude boundary-equal days"
    )
    assert inclusive[2011] > 50.0, (
        "sanity: inclusive variant should include the boundary-equal days"
    )


def test_tx10p_etccdi_strict_includes_below_threshold_days() -> None:
    # Half of eval days clearly below baseline, half above.
    baseline = _constant_daily_series(280.0, "1990-01-01", periods=21 * 365)
    # First 180 eval days are 5K colder; remaining 185 are 5K warmer.
    eval_time = xr.date_range("2011-01-01", periods=365, freq="D", use_cftime=True)
    eval_vals = np.where(np.arange(365) < 180, 275.0, 285.0)
    evaluation = xr.DataArray(eval_vals, coords={"time": eval_time}, dims=("time",))
    series = xr.concat([baseline, evaluation], dim="time")

    strict = CMP._compute_tx90p_etccdi_yearly(
        series=series,
        baseline_years=(1990, 2010),
        eval_years=[2011],
        percentile=10,
        window_days=5,
        exceed_ge=False,
        quantile_method="nearest",
        direction="below",
    )

    # 180 strictly-below days out of 365 -> ~49.3%.
    assert strict[2011] == pytest.approx(180.0 / 365.0 * 100.0, abs=1e-6)


def test_djf_cross_year_helper_rejects_non_djf_months(tmp_path: Path) -> None:
    metric = {
        "slug": "tas_summer_mean",
        "value_col": "summer_tas_mean_C",
        "var": "tas",
        "compute": "seasonal_mean",
        "params": {"months": [6, 7, 8]},
    }
    with pytest.raises(ValueError, match="non-DJF months"):
        CMP._compute_seasonal_mean_djf_cross_year_rows_for_metric(
            metric=metric,
            model="TEST_MODEL",
            scenario="historical",
            year_to_paths={2000: {"tas": tmp_path / "ignored.nc"}},
            masks=_single_cell_mask(),
            level="district",
        )

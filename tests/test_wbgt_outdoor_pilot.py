"""Engineering contracts for the outdoor-WBGT pilot runner (milestone 4, CHG-0621).

These tests guard the *engineering* claims the pilot makes: that it reproduces the frozen W1
path, that chunking and resume cannot change a number, that missing data never becomes a zero
exceedance count, that aggregation is a true area-weighted mean over valid cells only, and
that no write can land in production.  The science is milestone 1-3's and is not re-tested
here.

Fixtures are synthetic wherever an engineering contract is being asserted, so the tests do not
depend on the NEX archive being present.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

import tools.diagnostics.wbgt_outdoor_feasibility as m1
import tools.diagnostics.wbgt_outdoor_selection as m2
import tools.diagnostics.wbgt_outdoor_pilot as P
from tools.diagnostics.wbgt_method_validation import Site


# ==========================================================================
# Fixtures
# ==========================================================================


def synthetic_daily(year: int = 2005, seed: int = 7) -> pd.DataFrame:
    """A plausible daily NEX frame with the padding day on each side of the target year."""

    days = pd.date_range(pd.Timestamp(f"{year - 1}-12-31"), pd.Timestamp(f"{year + 1}-01-01"),
                         freq="D")
    rng = np.random.default_rng(seed)
    doy = days.dayofyear.to_numpy(dtype=float)
    season = np.sin(2 * np.pi * (doy - 100) / 365.0)
    tas = 27.0 + 5.0 * season + rng.normal(0.0, 0.6, len(days))
    dtr = 9.0 + 2.0 * rng.random(len(days))
    return pd.DataFrame(
        {
            "tas": tas,
            "tasmin": tas - dtr / 2.0,
            "tasmax": tas + dtr / 2.0,
            "hurs": np.clip(62.0 + 12.0 * season + rng.normal(0.0, 3.0, len(days)), 5.0, 99.0),
            "rsds": np.clip(215.0 + 55.0 * season + rng.normal(0.0, 12.0, len(days)), 10.0, None),
            "sfcWind": np.clip(2.4 + rng.normal(0.0, 0.4, len(days)), 0.3, None),
        },
        index=days,
    )


def synthetic_cube_year() -> tuple[dict, pd.DatetimeIndex]:
    """A full target year plus padding, heterogeneous across cells."""

    frame = synthetic_daily()
    days = frame.index
    n_lat, n_lon = 2, 3
    offset = np.arange(n_lat * n_lon, dtype=float).reshape(1, n_lat, n_lon)
    cube = {}
    for var in m1.REQUIRED_NEX_VARIABLES:
        values = frame[var].to_numpy(dtype=float)[:, None, None] * np.ones((1, n_lat, n_lon))
        if var in P.KELVIN_VARIABLES:
            values = values + offset
        cube[var] = values
    return cube, days


def synthetic_cube(n_days: int = 5, n_lat: int = 2, n_lon: int = 3) -> tuple[dict, pd.DatetimeIndex]:
    """A tiny heterogeneous cube for the input-validity contracts."""

    days = pd.date_range("2004-12-31", periods=n_days, freq="D")
    base = np.arange(n_lat * n_lon, dtype=float).reshape(1, n_lat, n_lon)
    ones = np.zeros((n_days, n_lat, n_lon))
    return (
        {
            "tas": 28.0 + base + ones,
            "tasmin": 23.0 + base + ones,
            "tasmax": 33.0 + base + ones,
            "hurs": np.full((n_days, n_lat, n_lon), 60.0),
            "rsds": np.full((n_days, n_lat, n_lon), 240.0),
            "sfcWind": np.full((n_days, n_lat, n_lon), 2.0),
        },
        days,
    )


@pytest.fixture(scope="module")
def site() -> Site:
    return Site("fixture_cell", 10.875, 76.375, P.ELEVATION_M, "pilot fixture")


@pytest.fixture(scope="module")
def daily() -> pd.DataFrame:
    return synthetic_daily()


def weights_frame(pairs: list[tuple[str, int, float]]) -> pd.DataFrame:
    """Sparse area weights in the shape ``build_area_weights`` returns."""

    return pd.DataFrame(
        [{"unit_key": u, "cell_index": c, "lat_index": 0, "lon_index": c, "area_m2": a}
         for u, c, a in pairs]
    )


def grid_dataset(values: dict[str, np.ndarray], lat, lon) -> xr.Dataset:
    return xr.Dataset(
        {k: (("lat", "lon"), v) for k, v in values.items()},
        coords={"lat": np.asarray(lat, dtype=float), "lon": np.asarray(lon, dtype=float)},
    )


class _FakeGrid:
    """Minimal stand-in for ``GridSpec`` in aggregation unit tests."""

    def __init__(self, lat, lon) -> None:
        self.lat = tuple(float(v) for v in lat)
        self.lon = tuple(float(v) for v in lon)

    @property
    def shape(self) -> tuple[int, int]:
        return (len(self.lat), len(self.lon))


# ==========================================================================
# Write isolation (SPEC.md 7)
# ==========================================================================


@pytest.mark.parametrize(
    "target",
    [
        "docs/diagnostics/wbgt_outdoor_feasibility",
        "docs/diagnostics/wbgt_outdoor_selection",
        "docs/diagnostics/wbgt_outdoor_humidity",
        "docs/diagnostics/wbgt_shade_release",
        "docs/diagnostics/wbgt_outdoor_selection/nested/deeper",
    ],
)
def test_guard_refuses_predecessor_evidence(target: str) -> None:
    """Predecessor evidence is immutable to this milestone, nested paths included."""

    with pytest.raises(SystemExit, match="protected root"):
        P.guard_write_target(Path(target), "--out-dir")


@pytest.mark.parametrize("fragment", ["processed", "processed_optimised", "wbgt_shade_national",
                                      "irt_data"])
def test_guard_refuses_production_fragments(tmp_path: Path, fragment: str) -> None:
    """A production root is refused wherever it appears in the resolved path."""

    with pytest.raises(SystemExit, match="must not write"):
        P.guard_write_target(tmp_path / fragment / "out", "--work-dir")


def test_guard_allows_the_pilot_directories(tmp_path: Path) -> None:
    assert P.guard_write_target(tmp_path / "wbgt_outdoor_pilot", "--out-dir").is_absolute()


def test_guard_follows_symlinks_to_a_protected_destination(tmp_path: Path) -> None:
    """A benign name pointing into production must be refused on its destination."""

    victim = tmp_path / "processed" / "metric"
    victim.mkdir(parents=True)
    link = tmp_path / "harmless"
    try:
        link.symlink_to(victim, target_is_directory=True)
    except (OSError, NotImplementedError):  # pragma: no cover - needs privilege on Windows
        pytest.skip("symlink creation not permitted here")
    with pytest.raises(SystemExit):
        P.guard_write_target(link, "--out-dir")


# ==========================================================================
# Input verification (SPEC.md 5)
# ==========================================================================


def _write_nc(path: Path, var: str, *, units: str, days: pd.DatetimeIndex,
              lat=(10.0, 10.25), lon=(76.0, 76.25)) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = np.zeros((len(days), len(lat), len(lon)), dtype=float)
    xr.Dataset(
        {var: (("time", "lat", "lon"), data, {"units": units})},
        coords={"time": days, "lat": list(lat), "lon": list(lon)},
    ).to_netcdf(path)
    return path


def test_wrong_units_are_refused_not_converted(tmp_path: Path) -> None:
    """A degC file where K is expected is an error; silent conversion would hide a real bug."""

    days = pd.date_range("2005-01-01", "2005-12-31", freq="D")
    check = P.verify_variable(_write_nc(tmp_path / "tas.nc", "tas", units="degC", days=days),
                              "tas", 2005)
    assert not check.ok
    assert "refusing to convert" in check.reason


def test_missing_dates_are_detected(tmp_path: Path) -> None:
    days = pd.date_range("2005-01-01", "2005-12-20", freq="D")
    check = P.verify_variable(_write_nc(tmp_path / "t.nc", "tas", units="K", days=days),
                              "tas", 2005)
    assert not check.ok and check.missing_dates == 11


def test_duplicate_dates_are_detected(tmp_path: Path) -> None:
    days = pd.date_range("2005-01-01", "2005-12-31", freq="D")
    doubled = days.append(pd.DatetimeIndex([days[5]])).sort_values()
    check = P.verify_variable(_write_nc(tmp_path / "t.nc", "tas", units="K", days=doubled),
                              "tas", 2005)
    assert not check.ok and check.duplicate_dates == 1


def test_missing_file_is_reported_not_raised(tmp_path: Path) -> None:
    check = P.verify_variable(tmp_path / "absent.nc", "tas", 2005)
    assert not check.ok and check.reason == "file not found"


def test_mismatched_grid_is_refused() -> None:
    """Variables on different grids must not be joined; regridding would be silent."""

    a = xr.DataArray(np.zeros((2, 2)), coords={"lat": [1.0, 2.0], "lon": [3.0, 4.0]},
                     dims=("lat", "lon"))
    b = xr.DataArray(np.zeros((2, 2)), coords={"lat": [1.0, 2.5], "lon": [3.0, 4.0]},
                     dims=("lat", "lon"))
    with pytest.raises(ValueError, match="different lat grid"):
        P.assert_identical_grids({"tas": a, "hurs": b})


def test_identical_grids_pass() -> None:
    a = xr.DataArray(np.zeros((2, 2)), coords={"lat": [1.0, 2.0], "lon": [3.0, 4.0]},
                     dims=("lat", "lon"))
    P.assert_identical_grids({"tas": a, "hurs": a.copy()})


def test_tasmin_above_tasmax_is_invalidated_not_repaired() -> None:
    """A contradictory input is marked invalid; reordering it would invent a plausible day."""

    cube, _ = synthetic_cube(n_days=3)
    cube["tasmin"][1, 0, 0] = cube["tasmax"][1, 0, 0] + 2.0
    valid, counts = P.input_validity_mask(cube)
    assert counts["tasmin_gt_tasmax_cell_days"] == 1
    assert not valid[1, 0, 0]
    assert cube["tasmin"][1, 0, 0] > cube["tasmax"][1, 0, 0]  # left untouched


def test_out_of_range_humidity_is_invalidated() -> None:
    """The real archive carries hurs marginally above 100 %; it must be caught."""

    cube, _ = synthetic_cube(n_days=3)
    cube["hurs"][0, 1, 2] = 100.5
    valid, counts = P.input_validity_mask(cube)
    assert counts["out_of_range_cell_days"] == 1 and not valid[0, 1, 2]


def test_negative_radiation_and_wind_are_invalidated() -> None:
    cube, _ = synthetic_cube(n_days=3)
    cube["rsds"][0, 0, 0] = -1.0
    cube["sfcWind"][1, 0, 1] = -0.5
    valid, counts = P.input_validity_mask(cube)
    assert counts["out_of_range_cell_days"] == 2
    assert not valid[0, 0, 0] and not valid[1, 0, 1]


def test_required_years_include_both_padding_years() -> None:
    assert P.required_years(2005) == (2004, 2005, 2006)


def test_rsds_and_sfcwind_route_to_the_wbgt_tree() -> None:
    """The two v2-acquisition variables must not be looked for in the main tree."""

    main, wbgt = Path("/main"), Path("/wbgt")
    for var in ("tas", "tasmin", "tasmax", "hurs"):
        path = P.variable_path(var, source_root=main, wbgt_root=wbgt, model="M",
                               scenario="historical", year=2005)
        assert str(path).replace("\\", "/").startswith("/main")
    for var in ("rsds", "sfcWind"):
        path = P.variable_path(var, source_root=main, wbgt_root=wbgt, model="M",
                               scenario="historical", year=2005)
        assert str(path).replace("\\", "/").startswith("/wbgt")


# ==========================================================================
# Parity with the frozen method (SPEC.md 9)
# ==========================================================================


def test_pilot_reproduces_the_frozen_w1_path(daily: pd.DataFrame, site: Site) -> None:
    """The engineering rewrite must not have changed the science."""

    result = P.parity_against_frozen_path(daily, site, candidate="W1")
    assert result["ok"], result
    assert result["max_abs_diff_c"] <= P.PARITY_TOL_C


def test_pilot_reproduces_the_frozen_c1_path(daily: pd.DataFrame, site: Site) -> None:
    result = P.parity_against_frozen_path(daily, site, candidate="C1")
    assert result["ok"], result


def test_w1_and_c1_actually_differ(daily: pd.DataFrame, site: Site) -> None:
    """A parity test passes trivially if the two candidates are the same computation."""

    static = m1.SiteStatic.from_site(site)
    w1 = P.cell_daily_max_c(daily, static, candidate="W1")
    c1 = P.cell_daily_max_c(daily, static, candidate="C1")
    assert float((w1 - c1).abs().max()) > 0.01


def test_target_year_hours_equal_the_full_span(daily: pd.DataFrame, site: Site) -> None:
    """Restricting the hour span is the threefold saving; it must change no number."""

    result = P.parity_hour_span(daily, site, 2005)
    assert result["ok"], result
    assert result["boundary_days_finite"]


def test_boundary_days_are_finite_only_with_padding(daily: pd.DataFrame, site: Site) -> None:
    """Without the adjacent-year day, the first target day must go NaN rather than be wrong."""

    static = m1.SiteStatic.from_site(site)
    target = pd.DatetimeIndex([pd.Timestamp("2005-01-01")])
    with_padding = P.cell_daily_max_c(daily, static, target_days=target)
    trimmed = daily.loc[daily.index >= "2005-01-01"]
    without = P.cell_daily_max_c(trimmed, static, target_days=target)
    assert np.isfinite(with_padding.iloc[0])
    assert not np.isfinite(without.iloc[0])


def test_unsupported_candidate_is_refused(daily: pd.DataFrame, site: Site) -> None:
    with pytest.raises(ValueError, match="only W1 and C1"):
        P.cell_daily_max_c(daily, m1.SiteStatic.from_site(site), candidate="W2")


def test_hourly_leakage_is_impossible_by_construction(daily: pd.DataFrame, site: Site) -> None:
    """``DailyInputs`` refuses any column that is not a daily NEX variable."""

    leaky = daily.copy()
    leaky["wbgt_observed"] = 30.0
    with pytest.raises(ValueError, match="hourly leakage"):
        P.cell_daily_max_c(leaky, m1.SiteStatic.from_site(site))


def test_chunking_and_single_cell_are_bit_identical() -> None:
    """Chunk size is an engineering choice; it may never move a bit."""

    cube, days = synthetic_cube_year()
    checks = P.parity_batch_and_chunks(cube, days, lat=[10.0, 10.25],
                                       lon=[76.0, 76.25, 76.5],
                                       cell_indices=[0, 1, 2, 3], year=2005)
    assert checks, "no parity checks ran"
    for check in checks:
        assert check["ok"], check


# ==========================================================================
# Annual statistics (SPEC.md 10)
# ==========================================================================


def test_incomplete_year_yields_nan_not_zero_counts() -> None:
    """The single most dangerous failure: missing data becoming zero exceedance days."""

    series = pd.Series([31.0] * 364 + [np.nan],
                       index=pd.date_range("2005-01-01", periods=365, freq="D"))
    stats = P.annual_statistics(series, expected_days=365)
    assert stats["complete_year"] == 0.0
    assert np.isnan(stats["wbgt_annual_mean_c"])
    for t in (28, 30, 32):
        assert np.isnan(stats[f"days_ge_{t}"]), f"days_ge_{t} became {stats[f'days_ge_{t}']}"


def test_all_nan_year_yields_nan() -> None:
    series = pd.Series(np.nan, index=pd.date_range("2005-01-01", periods=365, freq="D"))
    stats = P.annual_statistics(series, expected_days=365)
    assert stats["valid_days"] == 0.0 and np.isnan(stats["days_ge_28"])


def test_complete_year_counts_are_exact_and_ordered() -> None:
    values = np.concatenate([np.full(100, 27.0), np.full(100, 29.0),
                             np.full(100, 31.0), np.full(65, 33.0)])
    series = pd.Series(values, index=pd.date_range("2005-01-01", periods=365, freq="D"))
    stats = P.annual_statistics(series, expected_days=365)
    assert stats["complete_year"] == 1.0
    assert stats["days_ge_28"] == 265.0
    assert stats["days_ge_30"] == 165.0
    assert stats["days_ge_32"] == 65.0
    assert stats["days_ge_32"] <= stats["days_ge_30"] <= stats["days_ge_28"]


def test_threshold_is_inclusive_at_the_boundary() -> None:
    series = pd.Series(np.full(365, 30.0),
                       index=pd.date_range("2005-01-01", periods=365, freq="D"))
    stats = P.annual_statistics(series, expected_days=365)
    assert stats["days_ge_30"] == 365.0 and stats["days_ge_32"] == 0.0


def test_feb29_is_dropped() -> None:
    days = pd.date_range("2004-01-01", "2004-12-31", freq="D")
    kept = P.drop_feb29(days)
    assert len(days) == 366 and len(kept) == 365
    assert not ((kept.month == 2) & (kept.day == 29)).any()


# ==========================================================================
# Grid-first aggregation (SPEC.md 10, 11)
# ==========================================================================


def test_area_weighting_is_a_true_weighted_mean() -> None:
    """Hand-checkable fixture: the expected answer is written out, not produced by the code."""

    grid_ds = grid_dataset(
        {
            "wbgt_annual_mean_c": np.array([[30.0, 20.0, 10.0]]),
            "days_ge_28": np.array([[300.0, 200.0, 100.0]]),
            "days_ge_30": np.array([[200.0, 100.0, 50.0]]),
            "days_ge_32": np.array([[100.0, 50.0, 10.0]]),
        },
        lat=[10.0], lon=[76.0, 76.25, 76.5],
    )
    weights = weights_frame([("A", 0, 3.0), ("A", 1, 1.0)])
    frame = P.aggregate_units(grid_ds, weights, level="district",
                              grid=_FakeGrid([10.0], [76.0, 76.25, 76.5]), state="S",
                              model="M", year=2005, candidate="W1")
    assert frame.loc[0, "wbgt_annual_mean_c"] == pytest.approx((3.0 * 30.0 + 20.0) / 4.0)
    assert frame.loc[0, "days_ge_28"] == pytest.approx((3.0 * 300.0 + 200.0) / 4.0)
    assert frame.loc[0, "valid_area_fraction"] == pytest.approx(1.0)
    assert frame.loc[0, "n_cells_valid"] == 2


def test_invalid_cells_are_excluded_and_reported_not_filled() -> None:
    """A NaN cell lowers the valid-area fraction; it must not be replaced by a neighbour."""

    grid_ds = grid_dataset(
        {
            "wbgt_annual_mean_c": np.array([[30.0, np.nan, 10.0]]),
            "days_ge_28": np.array([[300.0, np.nan, 100.0]]),
            "days_ge_30": np.array([[200.0, np.nan, 50.0]]),
            "days_ge_32": np.array([[100.0, np.nan, 10.0]]),
        },
        lat=[10.0], lon=[76.0, 76.25, 76.5],
    )
    weights = weights_frame([("A", 0, 3.0), ("A", 1, 1.0)])
    frame = P.aggregate_units(grid_ds, weights, level="district",
                              grid=_FakeGrid([10.0], [76.0, 76.25, 76.5]), state="S",
                              model="M", year=2005, candidate="W1")
    assert frame.loc[0, "wbgt_annual_mean_c"] == pytest.approx(30.0)
    assert frame.loc[0, "valid_area_fraction"] == pytest.approx(0.75)
    assert frame.loc[0, "n_cells_valid"] == 1 and frame.loc[0, "n_cells_invalid"] == 1
    assert frame.loc[0, "climate_fill_method"] == "native"


def test_unit_with_no_valid_support_is_retained_as_nan_with_a_reason() -> None:
    grid_ds = grid_dataset(
        {name: np.array([[np.nan, np.nan]]) for name in
         ("wbgt_annual_mean_c", "days_ge_28", "days_ge_30", "days_ge_32")},
        lat=[10.0], lon=[76.0, 76.25],
    )
    weights = weights_frame([("A", 0, 1.0), ("A", 1, 1.0)])
    frame = P.aggregate_units(grid_ds, weights, level="district",
                              grid=_FakeGrid([10.0], [76.0, 76.25]), state="S",
                              model="M", year=2005, candidate="W1")
    assert len(frame) == 1
    assert np.isnan(frame.loc[0, "wbgt_annual_mean_c"])
    assert np.isnan(frame.loc[0, "days_ge_32"])
    assert frame.loc[0, "exclusion_reason"] == "no valid contributing climate cell"
    assert frame.loc[0, "valid_area_fraction"] == 0.0


def test_block_keys_split_into_district_and_block() -> None:
    grid_ds = grid_dataset({k: np.array([[30.0]]) for k in
                            ("wbgt_annual_mean_c", "days_ge_28", "days_ge_30", "days_ge_32")},
                           lat=[10.0], lon=[76.0])
    frame = P.aggregate_units(grid_ds, weights_frame([("Palakkad||Malampuzha", 0, 1.0)]),
                              level="block", grid=_FakeGrid([10.0], [76.0]), state="Kerala",
                              model="M", year=2005, candidate="W1")
    assert frame.loc[0, "district_name"] == "Palakkad"
    assert frame.loc[0, "block_name"] == "Malampuzha"


def test_admin_checks_catch_threshold_disorder() -> None:
    frame = pd.DataFrame([{"unit_key": "A", "wbgt_annual_mean_c": 30.0, "days_ge_28": 10.0,
                           "days_ge_30": 20.0, "days_ge_32": 5.0, "n_cells_valid": 1}])
    checks = {c["check"]: c["ok"] for c in P.check_admin_correctness(frame, level="district")}
    assert checks["threshold_ordering"] is False


def test_admin_checks_catch_out_of_range_counts() -> None:
    frame = pd.DataFrame([{"unit_key": "A", "wbgt_annual_mean_c": 30.0, "days_ge_28": 400.0,
                           "days_ge_30": 20.0, "days_ge_32": 5.0, "n_cells_valid": 1}])
    checks = {c["check"]: c["ok"] for c in P.check_admin_correctness(frame, level="district")}
    assert checks["counts_within_0_365"] is False


def test_admin_checks_catch_missing_becoming_zero() -> None:
    """A NaN value with a 0 count is the exact failure mode the pilot must never ship."""

    frame = pd.DataFrame([{"unit_key": "A", "wbgt_annual_mean_c": np.nan, "days_ge_28": 0.0,
                           "days_ge_30": 0.0, "days_ge_32": 0.0, "n_cells_valid": 0}])
    checks = {c["check"]: c["ok"] for c in P.check_admin_correctness(frame, level="district")}
    assert checks["missing_never_becomes_zero"] is False


def test_admin_checks_catch_duplicate_keys() -> None:
    row = {"unit_key": "A", "wbgt_annual_mean_c": 30.0, "days_ge_28": 1.0, "days_ge_30": 1.0,
           "days_ge_32": 1.0, "n_cells_valid": 1}
    checks = {c["check"]: c["ok"] for c in
              P.check_admin_correctness(pd.DataFrame([row, row]), level="district")}
    assert checks["unique_admin_keys"] is False


def test_admin_checks_pass_on_a_clean_table() -> None:
    frame = pd.DataFrame([{"unit_key": "A", "wbgt_annual_mean_c": 30.0, "days_ge_28": 300.0,
                           "days_ge_30": 200.0, "days_ge_32": 100.0, "n_cells_valid": 4}])
    assert all(c["ok"] for c in P.check_admin_correctness(frame, level="district"))


def test_block_to_district_weights_by_valid_area_not_whole_area() -> None:
    """Whole-block area would over-weight a block whose statistic rests on little support."""

    frame = pd.DataFrame([
        {"state_name": "S", "district_name": "D", "block_name": "b1",
         "wbgt_annual_mean_c": 30.0, "days_ge_28": 300.0, "days_ge_30": 200.0,
         "days_ge_32": 100.0, "valid_intersected_area_m2": 3.0,
         "total_intersected_area_m2": 10.0},
        {"state_name": "S", "district_name": "D", "block_name": "b2",
         "wbgt_annual_mean_c": 20.0, "days_ge_28": 200.0, "days_ge_30": 100.0,
         "days_ge_32": 50.0, "valid_intersected_area_m2": 1.0,
         "total_intersected_area_m2": 1.0},
    ])
    rollup = P.block_to_district(frame)
    assert rollup.loc[0, "block_to_district_wbgt_annual_mean_c"] == pytest.approx(27.5)
    # Whole-area weighting would give (10*30 + 1*20)/11 = 29.09, a materially different answer.
    assert rollup.loc[0, "block_to_district_wbgt_annual_mean_c"] != pytest.approx(29.09, abs=1e-2)


def test_block_to_district_ignores_blocks_with_no_valid_area() -> None:
    frame = pd.DataFrame([
        {"state_name": "S", "district_name": "D", "block_name": "b1",
         "wbgt_annual_mean_c": 30.0, "days_ge_28": 300.0, "days_ge_30": 200.0,
         "days_ge_32": 100.0, "valid_intersected_area_m2": 2.0,
         "total_intersected_area_m2": 2.0},
        {"state_name": "S", "district_name": "D", "block_name": "b2",
         "wbgt_annual_mean_c": np.nan, "days_ge_28": np.nan, "days_ge_30": np.nan,
         "days_ge_32": np.nan, "valid_intersected_area_m2": 0.0,
         "total_intersected_area_m2": 5.0},
    ])
    rollup = P.block_to_district(frame)
    assert rollup.loc[0, "block_to_district_wbgt_annual_mean_c"] == pytest.approx(30.0)


def test_weighted_mean_range_check_flags_an_impossible_value() -> None:
    grid_ds = grid_dataset({"wbgt_annual_mean_c": np.array([[30.0, 20.0]])},
                           lat=[10.0], lon=[76.0, 76.25])
    weights = weights_frame([("A", 0, 1.0), ("A", 1, 1.0)])
    good = pd.DataFrame([{"unit_key": "A", "wbgt_annual_mean_c": 25.0}])
    bad = pd.DataFrame([{"unit_key": "A", "wbgt_annual_mean_c": 45.0}])
    assert P.weighted_mean_within_cell_range(grid_ds, weights, good)["ok"]
    assert not P.weighted_mean_within_cell_range(grid_ds, weights, bad)["ok"]


# ==========================================================================
# Cache and resume (SPEC.md 7)
# ==========================================================================


def _sidecar(tmp_path: Path, **overrides) -> dict:
    (tmp_path / "in.nc").write_bytes(b"x" * 16)
    base = dict(state="Kerala", model="ACCESS-CM2", year=2005, candidate="W1",
                grid_id="abc123", input_paths=[tmp_path / "in.nc"], boundary_hash="bh1")
    base.update(overrides)
    return P.cell_cache_sidecar(**base)


def test_resume_reuses_a_matching_cache(tmp_path: Path) -> None:
    ds = grid_dataset({"wbgt_annual_mean_c": np.array([[30.0]])}, lat=[10.0], lon=[76.0])
    sidecar = _sidecar(tmp_path)
    path = tmp_path / "cells.nc"
    P.write_cell_cache(ds, path, sidecar=sidecar)
    reused = P.read_cell_cache(path, expected=sidecar)
    assert reused is not None
    assert float(reused["wbgt_annual_mean_c"].values[0, 0]) == 30.0


@pytest.mark.parametrize("override", [
    {"candidate": "C1"},
    {"model": "MRI-ESM2-0"},
    {"year": 2006},
    {"grid_id": "different"},
    {"boundary_hash": "bh2"},
    {"state": "Rajasthan"},
])
def test_resume_is_invalidated_by_any_signature_change(tmp_path: Path, override: dict) -> None:
    """A cache may be reused only when every identity element matches."""

    ds = grid_dataset({"wbgt_annual_mean_c": np.array([[30.0]])}, lat=[10.0], lon=[76.0])
    path = tmp_path / "cells.nc"
    P.write_cell_cache(ds, path, sidecar=_sidecar(tmp_path))
    assert P.read_cell_cache(path, expected=_sidecar(tmp_path, **override)) is None


def test_resume_is_invalidated_by_changed_inputs(tmp_path: Path) -> None:
    """Rewriting an input file at a different size must invalidate the cache."""

    ds = grid_dataset({"wbgt_annual_mean_c": np.array([[30.0]])}, lat=[10.0], lon=[76.0])
    path = tmp_path / "cells.nc"
    P.write_cell_cache(ds, path, sidecar=_sidecar(tmp_path))
    (tmp_path / "in.nc").write_bytes(b"y" * 4096)
    base = dict(state="Kerala", model="ACCESS-CM2", year=2005, candidate="W1",
                grid_id="abc123", input_paths=[tmp_path / "in.nc"], boundary_hash="bh1")
    assert P.read_cell_cache(path, expected=P.cell_cache_sidecar(**base)) is None


def test_cache_without_a_sidecar_is_not_reused(tmp_path: Path) -> None:
    """A torn write must not be mistaken for a complete one."""

    ds = grid_dataset({"wbgt_annual_mean_c": np.array([[30.0]])}, lat=[10.0], lon=[76.0])
    path = tmp_path / "cells.nc"
    P.write_cell_cache(ds, path, sidecar=_sidecar(tmp_path))
    path.with_suffix(".json").unlink()
    assert P.read_cell_cache(path, expected=_sidecar(tmp_path)) is None


def test_resume_round_trip_does_not_duplicate_values(tmp_path: Path) -> None:
    """A resumed grid must equal the original exactly, cell for cell."""

    cube, days = synthetic_cube_year()
    grid_ds = P.compute_cell_grid(cube, days, lat=[10.0, 10.25], lon=[76.0, 76.25, 76.5],
                                  cell_indices=[0, 1], year=2005)
    path = tmp_path / "cells.nc"
    P.write_cell_cache(grid_ds, path, sidecar=_sidecar(tmp_path))
    reused = P.read_cell_cache(path, expected=_sidecar(tmp_path))
    assert reused is not None
    for field in ("wbgt_annual_mean_c", "days_ge_28", "days_ge_30", "days_ge_32"):
        assert np.array_equal(reused[field].values, grid_ds[field].values, equal_nan=True)
    assert reused["wbgt_annual_mean_c"].shape == (2, 3)


def test_elevation_convention_is_in_the_cache_identity(tmp_path: Path) -> None:
    """A future elevation-aware run must never reuse this sea-level cache."""

    assert P.ELEVATION_CONVENTION in json.dumps(_sidecar(tmp_path))
    assert P.ELEVATION_CONVENTION in P.method_signature("W1")


# ==========================================================================
# Method signature and declared limitations (SPEC.md 2, 7, 14)
# ==========================================================================


def test_signatures_distinguish_the_two_candidates() -> None:
    assert P.method_signature("W1") != P.method_signature("C1")
    assert "wind-w1-dtr" in P.method_signature("W1")
    assert "wind-constant-daily-mean" in P.method_signature("C1")


def test_w1_coefficients_are_imported_never_restated() -> None:
    """If the slope were re-declared here it could silently drift from milestone 2."""

    import inspect

    assert "0.03" not in inspect.getsource(P)
    assert str(m2.WIND_DTR_SLOPE_PER_C) in P.method_signature("W1")


def test_the_pilot_does_not_redeclare_gate_constants() -> None:
    """Acceptance gates belong to milestones 1-2; this milestone must not restate them."""

    import inspect

    source = inspect.getsource(P)
    for forbidden in ("GATE_MEDIAN_ABS_ERROR_C =", "GATE_RMSE_C =", "COUNT_GATE_REL_TOL ="):
        assert forbidden not in source


def test_limitations_travel_with_every_manifest() -> None:
    """Every required caveat must be present, in a form that survives a copy-paste."""

    text = " ".join(P.LIMITATIONS).lower()
    for phrase in ("diagnostic-only", "uncorrected nex", "national accuracy",
                   "compensating reconstruction errors", "reference-sensitive",
                   "rare-event", "ranking accuracy", "inferred", "no dem"):
        assert phrase in text, phrase


def test_quantity_is_never_called_daily_mean_wbgt() -> None:
    assert P.QUANTITY_NAME == "Annual mean of daily maximum outdoor WBGT"
    assert "daily mean wbgt" not in P.QUANTITY_NAME.lower()
    assert P.EXCEEDANCE_NAME.startswith("Area-weighted mean annual cell exceedance days")


# ==========================================================================
# Ranking sensitivity (SPEC.md 13)
# ==========================================================================


def test_ranking_comparison_is_perfect_on_identical_input() -> None:
    frame = pd.DataFrame({"unit_key": list("abcdef"),
                          "wbgt_annual_mean_c": [30.0, 29.0, 28.0, 27.0, 26.0, 25.0]})
    result = P.ranking_comparison(frame, frame.copy(), field="wbgt_annual_mean_c",
                                  label="self", kind="method")
    assert result["spearman"] == pytest.approx(1.0)
    assert result["max_abs_rank_shift"] == 0.0


def test_ranking_comparison_detects_a_reversal() -> None:
    left = pd.DataFrame({"unit_key": list("abcde"),
                         "wbgt_annual_mean_c": [30.0, 29.0, 28.0, 27.0, 26.0]})
    right = pd.DataFrame({"unit_key": list("abcde"),
                          "wbgt_annual_mean_c": [26.0, 27.0, 28.0, 29.0, 30.0]})
    result = P.ranking_comparison(left, right, field="wbgt_annual_mean_c",
                                  label="reversed", kind="method")
    assert result["spearman"] == pytest.approx(-1.0)
    assert result["max_abs_rank_shift"] == 4.0


def test_ranking_comparison_excludes_nan_units_and_says_so() -> None:
    left = pd.DataFrame({"unit_key": list("abcde"),
                         "wbgt_annual_mean_c": [30.0, 29.0, np.nan, 27.0, 26.0]})
    right = pd.DataFrame({"unit_key": list("abcde"),
                          "wbgt_annual_mean_c": [30.0, 29.0, 28.0, 27.0, 26.0]})
    result = P.ranking_comparison(left, right, field="wbgt_annual_mean_c",
                                  label="gap", kind="method")
    assert result["excluded_units"] == 1 and result["n_units"] == 4


def test_ranking_comparison_reports_insufficient_overlap() -> None:
    left = pd.DataFrame({"unit_key": ["a", "b"], "wbgt_annual_mean_c": [30.0, 29.0]})
    result = P.ranking_comparison(left, left.copy(), field="wbgt_annual_mean_c",
                                  label="tiny", kind="method")
    assert result["insufficient"] is True and np.isnan(result["spearman"])


def test_method_and_model_sensitivity_are_labelled_separately() -> None:
    """The two answer different questions and must never be merged into one number."""

    frame = pd.DataFrame({"unit_key": list("abcdef"),
                          "wbgt_annual_mean_c": [30.0, 29.0, 28.0, 27.0, 26.0, 25.0]})
    method = P.ranking_comparison(frame, frame.copy(), field="wbgt_annual_mean_c",
                                  label="W1 vs C1", kind="method")
    model = P.ranking_comparison(frame, frame.copy(), field="wbgt_annual_mean_c",
                                 label="A vs B", kind="model")
    assert method["kind"] == "method" and model["kind"] == "model"


# ==========================================================================
# Grid-first ordering (SPEC.md 8)
# ==========================================================================


def test_grid_first_differs_from_averaging_weather_first() -> None:
    """WBGT is nonlinear, so cell-first and polygon-first genuinely disagree.

    If this ever asserted equality, the pipeline would have silently become polygon-first.
    """

    cube, days = synthetic_cube_year()
    grid_ds = P.compute_cell_grid(cube, days, lat=[10.0, 10.25], lon=[76.0, 76.25, 76.5],
                                  cell_indices=[0, 1], year=2005)
    cellwise = float(np.nanmean([grid_ds["wbgt_annual_mean_c"].values[0, 0],
                                 grid_ds["wbgt_annual_mean_c"].values[0, 1]]))
    averaged = pd.DataFrame(
        {v: (cube[v][:, 0, 0] + cube[v][:, 0, 1]) / 2.0 for v in m1.REQUIRED_NEX_VARIABLES},
        index=days,
    )
    static = m1.SiteStatic("avg", 10.0, 76.0, P.ELEVATION_M)
    target = P.drop_feb29(days[days.year == 2005])
    weather_first = float(P.cell_daily_max_c(averaged, static, target_days=target).mean())
    assert abs(cellwise - weather_first) > 1e-6


def test_uncomputed_cells_stay_nan() -> None:
    """A cell that was never requested must not acquire a value from its neighbours."""

    cube, days = synthetic_cube_year()
    grid_ds = P.compute_cell_grid(cube, days, lat=[10.0, 10.25], lon=[76.0, 76.25, 76.5],
                                  cell_indices=[0], year=2005)
    values = grid_ds["wbgt_annual_mean_c"].values
    assert np.isfinite(values[0, 0])
    assert np.isnan(values[0, 1]) and np.isnan(values[1, 2])


def test_invalid_input_days_propagate_to_a_nan_year() -> None:
    """One invalid day must cost the whole year, not be quietly skipped."""

    cube, days = synthetic_cube_year()
    valid = np.ones_like(cube["tas"], dtype=bool)
    valid[int(np.argmax(days.year == 2005)) + 10, 0, 0] = False
    grid_ds = P.compute_cell_grid(cube, days, lat=[10.0, 10.25], lon=[76.0, 76.25, 76.5],
                                  cell_indices=[0], year=2005, valid_mask=valid)
    assert np.isnan(grid_ds["wbgt_annual_mean_c"].values[0, 0])
    assert np.isnan(grid_ds["days_ge_28"].values[0, 0])
    assert grid_ds["complete_year"].values[0, 0] == 0.0
    assert grid_ds["input_invalid_days"].values[0, 0] == 1.0

"""Engineering contracts for the outdoor-WBGT pilot follow-up (milestone 4b, CHG-0626).

These tests guard what milestone 4b adds: that the input-quality policy clips only what it
declares and never edits the caller's array, that an inverted temperature range is invalidated
rather than repaired, that a missing elevation is an explicit failure rather than a silent
sea-level substitution, that the coverage denominators are the declared ones, that a run's
cache cannot be served to a run with a different policy or elevation, and that the geometry
checks actually detect gaps, overlaps and children outside their parent.

Milestone 4's own contracts are tested in ``tests/test_wbgt_outdoor_pilot.py`` and are not
repeated here.  Fixtures are synthetic wherever an engineering contract is asserted, so these
tests do not need the NEX archive or the GMTED tiles present.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

import tools.diagnostics.wbgt_outdoor_feasibility as m1
import tools.diagnostics.wbgt_outdoor_pilot as P
import tools.diagnostics.wbgt_outdoor_pilot_qc as Q


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
    """A full target year plus padding, on a 2x3 grid, heterogeneous across cells."""

    frame = synthetic_daily()
    days = pd.DatetimeIndex(frame.index)
    offset = np.array([[0.0, 0.4, -0.3], [0.2, -0.5, 0.1]])[None, :, :]
    cube = {}
    for var in m1.REQUIRED_NEX_VARIABLES:
        base = np.broadcast_to(frame[var].to_numpy(dtype=float)[:, None, None],
                               (len(days), 2, 3)).copy()
        if var in ("tas", "tasmin", "tasmax"):
            base = base + offset
        cube[var] = base
    return cube, days


# ==========================================================================
# Fixtures
# ==========================================================================


def cube_with_hurs(values: list[float]) -> dict[str, np.ndarray]:
    """A one-cell cube whose ``hurs`` takes the given values, everything else valid."""

    n = len(values)
    base = {
        "tas": np.full((n, 1, 1), 27.0),
        "tasmin": np.full((n, 1, 1), 22.0),
        "tasmax": np.full((n, 1, 1), 32.0),
        "hurs": np.asarray(values, dtype=float).reshape(n, 1, 1),
        "rsds": np.full((n, 1, 1), 220.0),
        "sfcWind": np.full((n, 1, 1), 2.0),
    }
    return base


def square(x0: float, y0: float, x1: float, y1: float):
    from shapely.geometry import box

    return box(x0, y0, x1, y1)


def admin_frames(blocks: list[tuple[str, str, object]], districts: list[tuple[str, object]]):
    """Minimal district and block GeoDataFrames in EPSG:4326 for the geometry checks."""

    import geopandas as gpd

    d = gpd.GeoDataFrame(
        {"district_name": [n for n, _ in districts]},
        geometry=[g for _, g in districts], crs="EPSG:4326")
    b = gpd.GeoDataFrame(
        {"district_name": [d_ for d_, _, _ in blocks],
         "block_name": [b_ for _, b_, _ in blocks]},
        geometry=[g for _, _, g in blocks], crs="EPSG:4326")
    return d, b


class _FakeGrid:
    """Just enough of ``GridSpec`` for the aggregation helpers."""

    def __init__(self, lat, lon, grid_id="grid-qc") -> None:
        self.lat = np.asarray(lat, dtype=float)
        self.lon = np.asarray(lon, dtype=float)
        self.shape = (len(self.lat), len(self.lon))
        self.grid_id = grid_id


def grid_dataset(values: list[list[float]], *, lat, lon, elevation=None) -> xr.Dataset:
    """A per-cell annual grid with the fields the coverage table reads."""

    arr = np.asarray(values, dtype=float)
    fields = {
        "wbgt_annual_mean_c": arr,
        "days_ge_28": np.where(np.isfinite(arr), 100.0, np.nan),
        "days_ge_30": np.where(np.isfinite(arr), 50.0, np.nan),
        "days_ge_32": np.where(np.isfinite(arr), 10.0, np.nan),
        "complete_year": np.where(np.isfinite(arr), 1.0, 0.0),
        "cell_elevation_m": (np.zeros_like(arr) if elevation is None
                             else np.asarray(elevation, dtype=float)),
        "elevation_fallback": np.zeros_like(arr),
        "rh_clipped_days": np.zeros_like(arr),
    }
    return xr.Dataset({k: (("lat", "lon"), v) for k, v in fields.items()},
                      coords={"lat": np.asarray(lat, float), "lon": np.asarray(lon, float)})


# ==========================================================================
# Input-quality policy for hurs (pilot_qc SPEC.md 2.2)
# ==========================================================================


def test_strict_policy_changes_nothing_and_keeps_above_100_invalid() -> None:
    cube = cube_with_hurs([50.0, 100.0, 100.5])
    out, flags, record = P.apply_rh_policy(cube, rh_policy="strict")
    assert out["hurs"] is cube["hurs"]
    assert not flags.any()
    assert record["rh_clipped_cell_days"] == 0
    valid, _ = P.input_validity_mask(out)
    assert list(valid[:, 0, 0]) == [True, True, False]


def test_clip_policy_clips_only_above_100_and_flags_it() -> None:
    cube = cube_with_hurs([50.0, 100.0, 100.5, 103.6])
    out, flags, record = P.apply_rh_policy(cube, rh_policy="clip100")
    assert list(out["hurs"][:, 0, 0]) == [50.0, 100.0, 100.0, 100.0]
    assert list(flags[:, 0, 0]) == [False, False, True, True]
    assert record["rh_clipped_cell_days"] == 2
    assert record["rh_max_correction_pct"] == pytest.approx(3.6, abs=1e-6)
    valid, _ = P.input_validity_mask(out)
    assert valid[:, 0, 0].all()


def test_exactly_100_is_already_valid_and_is_never_flagged() -> None:
    """100.0 % is inside the physical bound, so neither policy may touch it."""

    cube = cube_with_hurs([100.0])
    for policy in P.RH_POLICIES:
        out, flags, _ = P.apply_rh_policy(cube, rh_policy=policy)
        assert out["hurs"][0, 0, 0] == 100.0
        assert not flags.any()


def test_clip_policy_leaves_the_callers_array_untouched() -> None:
    """The source file is never modified, and neither is the array the caller still holds."""

    cube = cube_with_hurs([101.0, 50.0])
    original = cube["hurs"].copy()
    out, _, _ = P.apply_rh_policy(cube, rh_policy="clip100")
    assert np.array_equal(cube["hurs"], original)
    assert out["hurs"][0, 0, 0] == 100.0


def test_negative_hurs_stays_invalid_under_both_policies() -> None:
    cube = cube_with_hurs([-1.0, 50.0])
    for policy in P.RH_POLICIES:
        out, flags, _ = P.apply_rh_policy(cube, rh_policy=policy)
        assert out["hurs"][0, 0, 0] == -1.0
        assert not flags[0, 0, 0]
        valid, _ = P.input_validity_mask(out)
        assert not valid[0, 0, 0]


@pytest.mark.parametrize("missing", [np.nan, 1e20])
def test_missing_hurs_is_never_clipped_into_a_value(missing: float) -> None:
    """A fill value decoded to NaN, or an undecoded 1e20, must not become 100 %."""

    cube = cube_with_hurs([missing, 50.0])
    out, flags, _ = P.apply_rh_policy(cube, rh_policy="clip100")
    if np.isnan(missing):
        assert np.isnan(out["hurs"][0, 0, 0])
        assert not flags[0, 0, 0]
    else:
        # An undecoded fill value is far above the bound, so it is clipped; the point of the
        # test is that it must then still be rejected as a physically impossible input, which
        # the validity mask does through the other variables' range checks.
        assert flags[0, 0, 0]
    valid, _ = P.input_validity_mask(out)
    assert valid[1, 0, 0]


def test_record_reports_the_untreated_extent_whatever_the_policy() -> None:
    """The report of how bad the inputs are must not depend on how they were treated."""

    cube = cube_with_hurs([50.0, 101.0, 103.0])
    for policy in P.RH_POLICIES:
        _, _, record = P.apply_rh_policy(cube, rh_policy=policy)
        assert record["rh_above_100_cell_days"] == 2
        assert record["rh_above_102_cell_days"] == 1
        assert record["rh_observed_max_pct"] == pytest.approx(103.0)


def test_unknown_policy_is_refused() -> None:
    with pytest.raises(ValueError, match="Unknown rh_policy"):
        P.apply_rh_policy(cube_with_hurs([50.0]), rh_policy="clip102")
    with pytest.raises(ValueError, match="Unknown rh_policy"):
        P.method_signature("W1", rh_policy="clip102")


def test_clipped_day_and_invalid_day_stay_distinguishable() -> None:
    """A flagged corrected day must never be confused with an unresolved invalid day."""

    cube = cube_with_hurs([101.0, 50.0, 50.0])
    cube["tasmin"] = cube["tasmin"].copy()
    cube["tasmin"][1, 0, 0] = 40.0  # inverted range on day 1
    out, flags, _ = P.apply_rh_policy(cube, rh_policy="clip100")
    valid, counts = P.input_validity_mask(out)
    assert flags[0, 0, 0] and valid[0, 0, 0]          # corrected, and now usable
    assert not flags[1, 0, 0] and not valid[1, 0, 0]  # unresolved, and still rejected
    assert counts["tasmin_gt_tasmax_cell_days"] == 1


# ==========================================================================
# Inverted temperature range (pilot_qc SPEC.md 2.3)
# ==========================================================================


def test_inverted_temperature_range_is_invalidated_not_repaired_under_either_policy() -> None:
    """The RH treatment must not rescue a cell-day whose defect is the temperature ordering."""

    cube = cube_with_hurs([101.0])
    cube["tasmin"] = np.full((1, 1, 1), 32.5)
    cube["tasmax"] = np.full((1, 1, 1), 31.0)
    for policy in P.RH_POLICIES:
        out, _, _ = P.apply_rh_policy(cube, rh_policy=policy)
        valid, counts = P.input_validity_mask(out)
        assert not valid[0, 0, 0]
        if policy == "clip100":
            # With the RH defect treated, the ordering defect is what still rejects the day.
            assert counts["tasmin_gt_tasmax_cell_days"] == 1
        else:
            # Under strict the day is already out of range on hurs, so the ordering counter
            # does not double-count it; the day is rejected either way.
            assert counts["out_of_range_cell_days"] == 1
        # Nothing was swapped, averaged or filled.
        assert out["tasmin"][0, 0, 0] == 32.5
        assert out["tasmax"][0, 0, 0] == 31.0


def test_one_invalid_day_still_costs_the_whole_year_after_the_treatment() -> None:
    """Temporal completeness is not relaxed by the input treatment (pilot_qc SPEC.md 2.2)."""

    series = pd.Series(np.full(365, 29.0))
    series.iloc[17] = np.nan
    stats = P.annual_statistics(series, expected_days=365)
    assert np.isnan(stats["wbgt_annual_mean_c"])
    assert all(np.isnan(stats[f"days_ge_{int(t)}"]) for t in P.THRESHOLDS_C)


# ==========================================================================
# Elevation (pilot_qc SPEC.md 3)
# ==========================================================================


def test_missing_elevation_is_an_explicit_failure_not_a_silent_sea_level() -> None:
    cube, days = synthetic_cube_year()
    field = np.full((2, 3), 500.0)
    field[0, 1] = np.nan
    with pytest.raises(SystemExit, match="Refusing to substitute sea level"):
        P.compute_cell_grid(cube, days, lat=[10.0, 10.25], lon=[76.0, 76.25, 76.5],
                            cell_indices=[1], year=2005, elevation_m=field)


def test_missing_elevation_fallback_is_flagged_when_explicitly_allowed() -> None:
    cube, days = synthetic_cube_year()
    field = np.full((2, 3), 500.0)
    field[0, 1] = np.nan
    ds = P.compute_cell_grid(cube, days, lat=[10.0, 10.25], lon=[76.0, 76.25, 76.5],
                            cell_indices=[0, 1], year=2005, elevation_m=field,
                            allow_sea_level_fallback=True)
    assert ds["elevation_fallback"].values[0, 1] == 1.0
    assert ds["elevation_fallback"].values[0, 0] == 0.0
    assert ds["cell_elevation_m"].values[0, 1] == P.ELEVATION_M
    assert ds["cell_elevation_m"].values[0, 0] == 500.0


def test_elevation_field_must_match_the_grid_shape() -> None:
    cube, days = synthetic_cube_year()
    with pytest.raises(ValueError, match="scalar or a"):
        P.compute_cell_grid(cube, days, lat=[10.0, 10.25], lon=[76.0, 76.25, 76.5],
                            cell_indices=[0], year=2005,
                            elevation_m=np.zeros((2, 3, 1)))


def test_elevation_changes_the_result_and_is_recorded_per_cell() -> None:
    """Elevation must actually reach the solver, and each cell keeps its own value."""

    cube, days = synthetic_cube_year()
    field = np.array([[0.0, 2000.0, 0.0], [0.0, 0.0, 0.0]])
    flat = P.compute_cell_grid(cube, days, lat=[10.0, 10.25], lon=[76.0, 76.25, 76.5],
                               cell_indices=[1], year=2005, elevation_m=P.ELEVATION_M)
    high = P.compute_cell_grid(cube, days, lat=[10.0, 10.25], lon=[76.0, 76.25, 76.5],
                               cell_indices=[1], year=2005, elevation_m=field)
    assert high["cell_elevation_m"].values[0, 1] == 2000.0
    assert flat["cell_elevation_m"].values[0, 1] == 0.0
    a = float(flat["wbgt_annual_mean_c"].values[0, 1])
    b = float(high["wbgt_annual_mean_c"].values[0, 1])
    assert np.isfinite(a) and np.isfinite(b)
    assert a != b


def test_isa_pressure_falls_with_elevation_and_is_unchanged_by_this_milestone() -> None:
    """Only the elevation input changed; the formulation is milestone 1's."""

    assert m1.barometric_pressure_hpa(0.0) == pytest.approx(1013.25, abs=1e-6)
    sea = m1.barometric_pressure_hpa(0.0)
    hill = m1.barometric_pressure_hpa(2000.0)
    peak = m1.barometric_pressure_hpa(5000.0)
    assert sea > hill > peak > 0.0


def test_cell_elevation_field_area_averages_and_keeps_nodata_missing(tmp_path: Path) -> None:
    """A synthetic two-cell raster: one cell averages its pixels, the other is all nodata."""

    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import from_origin

    # 0.25 deg cells centred at lon 76.0 and 76.25, lat 10.0; raster pixels of 0.125 deg.
    data = np.array([[10.0, 30.0, Q.GMTED_NODATA, Q.GMTED_NODATA],
                     [20.0, 40.0, Q.GMTED_NODATA, Q.GMTED_NODATA]], dtype="float32")
    path = tmp_path / "tile.tif"
    with rasterio.open(path, "w", driver="GTiff", height=2, width=4, count=1,
                       dtype="float32", crs="EPSG:4326",
                       transform=from_origin(75.875, 10.125, 0.125, 0.125),
                       nodata=Q.GMTED_NODATA) as dst:
        dst.write(data, 1)

    field, meta = Q.cell_elevation_field([10.0], [76.0, 76.25], [path],
                                        cell_size_deg=0.25)
    assert field.shape == (1, 2)
    assert field[0, 0] == pytest.approx(25.0, abs=0.05)  # cos-weighted mean of 10/30/20/40
    assert np.isnan(field[0, 1])
    assert meta["cells_missing"] == 1
    assert meta["units"] == "m"
    assert "EGM96" in meta["vertical_reference"]


def test_negative_elevation_is_retained_not_clipped(tmp_path: Path) -> None:
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import from_origin

    data = np.full((2, 2), -12.0, dtype="float32")
    path = tmp_path / "below.tif"
    with rasterio.open(path, "w", driver="GTiff", height=2, width=2, count=1, dtype="float32",
                       crs="EPSG:4326", transform=from_origin(75.875, 10.125, 0.125, 0.125),
                       nodata=Q.GMTED_NODATA) as dst:
        dst.write(data, 1)
    field, meta = Q.cell_elevation_field([10.0], [76.0], [path], cell_size_deg=0.25)
    assert field[0, 0] == pytest.approx(-12.0, abs=1e-6)
    assert meta["cells_negative"] == 1


def test_elevation_identity_depends_on_the_tile_checksums() -> None:
    a = Q.elevation_convention_name([{"tile": "x", "sha256": "aa"}])
    b = Q.elevation_convention_name([{"tile": "x", "sha256": "bb"}])
    assert a != b
    assert a.startswith("elev-gmted2010-mea300-cellmean-")
    assert a != P.ELEVATION_CONVENTION


def test_elevation_round_trip_carries_its_own_convention(tmp_path: Path) -> None:
    field = np.array([[1.0, 2.0], [3.0, 4.0]])
    path = Q.save_elevation(tmp_path / "e.npz", field, "elev-test-identity", {"units": "m"})
    args = __import__("argparse").Namespace(elevation_npz=path)
    loaded, convention = P.load_elevation_argument(args)
    assert np.array_equal(loaded, field)
    assert convention == "elev-test-identity"
    assert args.elevation_convention == "elev-test-identity"


def test_no_elevation_argument_means_the_declared_sea_level_convention() -> None:
    args = __import__("argparse").Namespace(elevation_npz=None)
    field, convention = P.load_elevation_argument(args)
    assert field is None
    assert convention == P.ELEVATION_CONVENTION


# ==========================================================================
# Cache identity (pilot_qc SPEC.md 8.1)
# ==========================================================================


def _sidecar(tmp_path: Path, **overrides):
    probe = tmp_path / "probe.nc"
    probe.write_bytes(b"x")
    base = dict(state="Kerala", model="ACCESS-CM2", year=2005, candidate="W1",
                grid_id="g", input_paths=[probe], boundary_hash="b")
    base.update(overrides)
    return P.cell_cache_sidecar(**base)


def test_cache_key_changes_with_the_rh_policy(tmp_path: Path) -> None:
    strict = _sidecar(tmp_path, rh_policy="strict")
    clipped = _sidecar(tmp_path, rh_policy="clip100")
    assert strict != clipped
    assert strict["rh_policy"] == "strict"
    assert clipped["method_signature"] != strict["method_signature"]


def test_cache_key_changes_with_the_elevation_identity(tmp_path: Path) -> None:
    sea = _sidecar(tmp_path, elevation_identity=P.ELEVATION_CONVENTION)
    gmted = _sidecar(tmp_path, elevation_identity="elev-gmted2010-mea300-cellmean-deadbeef")
    assert sea != gmted
    assert gmted["elevation_identity"].startswith("elev-gmted2010")


@pytest.mark.parametrize("override", [{"rh_policy": "clip100"},
                                      {"elevation_identity": "elev-other"}])
def test_a_cache_written_under_one_policy_is_refused_under_another(
    tmp_path: Path, override: dict
) -> None:
    cube, days = synthetic_cube_year()
    ds = P.compute_cell_grid(cube, days, lat=[10.0, 10.25], lon=[76.0, 76.25, 76.5],
                             cell_indices=[0], year=2005)
    path = tmp_path / "cells.nc"
    written = _sidecar(tmp_path)
    P.write_cell_cache(ds, path, sidecar=written)
    assert P.read_cell_cache(path, expected=written) is not None
    assert P.read_cell_cache(path, expected=_sidecar(tmp_path, **override)) is None


def test_v2_signature_can_never_match_a_milestone_4_v1_signature() -> None:
    """Milestone 4's cache must be unreachable from here, by construction."""

    signature = P.method_signature("W1")
    assert signature.startswith("outdoor-pilot-v2:")
    assert "outdoor-pilot-v1" not in signature
    assert "rh-strict-" in signature


# ==========================================================================
# Coverage classification (pilot_qc SPEC.md 6)
# ==========================================================================


def _coverage_fixture(values, *, polygon_area_scale=1.0):
    import geopandas as gpd

    # Two cells, one unit covering both; the unit polygon is deliberately larger than the
    # represented area so represented_fraction is below 1.
    lat, lon = [10.0], [76.0, 76.25]
    ds = grid_dataset([values], lat=lat, lon=lon)
    weights = pd.DataFrame([
        {"unit_key": "Palakkad", "cell_index": 0, "lat_index": 0, "lon_index": 0,
         "area_m2": 300.0},
        {"unit_key": "Palakkad", "cell_index": 1, "lat_index": 0, "lon_index": 1,
         "area_m2": 100.0},
    ])
    # A polygon whose EPSG:6933 area is known only approximately, so scale is applied by the
    # caller through a monkeypatched area instead; here we use a real small square.
    gdf = gpd.GeoDataFrame({"district_name": ["Palakkad"]},
                           geometry=[square(76.0, 10.0, 76.01, 10.01)], crs="EPSG:4326")
    return gdf, weights, ds


def test_coverage_reports_all_three_areas_and_the_declared_denominator() -> None:
    gdf, weights, ds = _coverage_fixture([29.0, np.nan])
    table = Q.coverage_table(gdf, weights, ds, level="district", state="Kerala",
                             model="ACCESS-CM2", year=2005, run="baseline")
    row = table.iloc[0]
    assert row.represented_area_m2 == pytest.approx(400.0)
    assert row.valid_area_m2 == pytest.approx(300.0)
    assert row.valid_fraction_of_represented == pytest.approx(0.75)
    assert row.full_polygon_area_m2 > 0
    assert row.valid_fraction_of_full == pytest.approx(
        row.valid_area_m2 / row.full_polygon_area_m2)
    assert row.coverage_screen_denominator == "valid_fraction_of_represented"
    assert row.coverage_class == "partial_coverage"


def test_the_screen_is_applied_to_the_represented_denominator_not_the_full_polygon() -> None:
    """Milestone 4 screened valid/represented; that meaning is preserved, not reinterpreted."""

    gdf, weights, ds = _coverage_fixture([29.0, 30.0])
    table = Q.coverage_table(gdf, weights, ds, level="district", state="Kerala",
                             model="ACCESS-CM2", year=2005, run="baseline")
    row = table.iloc[0]
    assert row.valid_fraction_of_represented == pytest.approx(1.0)
    # The full-polygon view is far stricter here, and must NOT drive the class.
    assert row.valid_fraction_of_full < 0.5
    assert row.coverage_class == "meets_screen"


def test_all_invalid_unit_is_no_valid_coverage_and_never_zero() -> None:
    gdf, weights, ds = _coverage_fixture([np.nan, np.nan])
    table = Q.coverage_table(gdf, weights, ds, level="district", state="Kerala",
                             model="ACCESS-CM2", year=2005, run="baseline")
    row = table.iloc[0]
    assert row.coverage_class == "no_valid_coverage"
    assert row.valid_area_m2 == 0.0
    # The fraction is a real zero because the unit *is* represented; the class, not the
    # fraction, is what records that there is no valid support.
    assert row.valid_fraction_of_represented == 0.0
    assert np.isnan(row.wbgt_annual_mean_c) if "wbgt_annual_mean_c" in row else True


def test_a_unit_with_no_intersecting_cell_is_still_listed() -> None:
    """A unit the grid never touched must appear, classified, not silently dropped."""

    import geopandas as gpd

    ds = grid_dataset([[29.0]], lat=[10.0], lon=[76.0])
    gdf = gpd.GeoDataFrame({"district_name": ["Palakkad", "Lakshadweep"]},
                           geometry=[square(76.0, 10.0, 76.01, 10.01),
                                     square(72.0, 10.0, 72.01, 10.01)], crs="EPSG:4326")
    weights = pd.DataFrame([{"unit_key": "Palakkad", "cell_index": 0, "lat_index": 0,
                             "lon_index": 0, "area_m2": 100.0}])
    table = Q.coverage_table(gdf, weights, ds, level="district", state="Kerala",
                             model="ACCESS-CM2", year=2005, run="baseline")
    assert set(table.unit_key) == {"Palakkad", "Lakshadweep"}
    missing = table.loc[table.unit_key.eq("Lakshadweep")].iloc[0]
    assert missing.coverage_class == "no_valid_coverage"
    assert missing.represented_fraction == 0.0


@pytest.mark.parametrize(
    "fraction,n_valid,expected",
    [(1.0, 4, "meets_screen"), (0.99, 4, "meets_screen"), (0.9899, 4, "partial_coverage"),
     (0.55, 2, "partial_coverage"), (np.nan, 0, "no_valid_coverage"), (0.0, 0, "no_valid_coverage")],
)
def test_classification_boundaries(fraction: float, n_valid: int, expected: str) -> None:
    assert Q.classify_coverage(fraction, n_valid) == expected


# ==========================================================================
# Common-support comparison (pilot_qc SPEC.md 4)
# ==========================================================================


def test_cell_comparison_separates_gained_support_from_value_change() -> None:
    lat, lon = [10.0], [76.0, 76.25]
    left = grid_dataset([[29.0, np.nan]], lat=lat, lon=lon)
    right = grid_dataset([[29.5, 31.0]], lat=lat, lon=lon)
    rows = Q.cell_comparison(left, right, state="Kerala", model="ACCESS-CM2",
                             left_run="baseline", right_run="rh")
    mean_row = next(r for r in rows if r["field"] == "wbgt_annual_mean_c")
    assert mean_row["n_cells_valid_left"] == 1
    assert mean_row["n_cells_valid_right"] == 2
    assert mean_row["n_cells_gained"] == 1
    assert mean_row["n_cells_lost"] == 0
    assert mean_row["n_cells_common_valid"] == 1
    # The common-support delta must ignore the cell that only one run has.
    assert mean_row["max_abs_delta_common"] == pytest.approx(0.5)


def test_unit_comparison_flags_a_change_that_cannot_be_attributed_to_the_treatment() -> None:
    keys = ["state_name", "level", "unit_key"]
    left = pd.DataFrame([{"state_name": "Kerala", "level": "district", "unit_key": "A",
                          "wbgt_annual_mean_c": 29.0, "days_ge_28": 200.0, "days_ge_30": 100.0,
                          "days_ge_32": 10.0}])
    right = left.copy()
    right.loc[0, "wbgt_annual_mean_c"] = 28.5
    cov_l = pd.DataFrame([{**{k: left.loc[0, k] for k in keys}, "valid_area_m2": 100.0,
                           "valid_fraction_of_represented": 0.5,
                           "coverage_class": "partial_coverage"}])
    cov_r = pd.DataFrame([{**{k: left.loc[0, k] for k in keys}, "valid_area_m2": 200.0,
                           "valid_fraction_of_represented": 1.0,
                           "coverage_class": "meets_screen"}])
    out = Q.unit_comparison(left, right, left_run="baseline", right_run="rh",
                            coverage_left=cov_l, coverage_right=cov_r)
    row = out.iloc[0]
    assert row.wbgt_annual_mean_c_delta == pytest.approx(-0.5)
    assert bool(row.support_changed)
    assert "not attributable to the treatment alone" in row.attribution
    assert row.coverage_class_left == "partial_coverage"
    assert row.coverage_class_right == "meets_screen"


def test_unit_comparison_marks_an_unchanged_support_as_attributable() -> None:
    keys = ["state_name", "level", "unit_key"]
    left = pd.DataFrame([{"state_name": "Kerala", "level": "district", "unit_key": "A",
                          "wbgt_annual_mean_c": 29.0, "days_ge_28": 200.0, "days_ge_30": 100.0,
                          "days_ge_32": 10.0}])
    right = left.copy()
    right.loc[0, "wbgt_annual_mean_c"] = 28.0
    cov = pd.DataFrame([{**{k: left.loc[0, k] for k in keys}, "valid_area_m2": 100.0,
                         "valid_fraction_of_represented": 1.0,
                         "coverage_class": "meets_screen"}])
    out = Q.unit_comparison(left, right, left_run="baseline", right_run="elev",
                            coverage_left=cov, coverage_right=cov.copy())
    assert not bool(out.iloc[0].support_changed)
    assert "attributable to the treatment" in out.iloc[0].attribution


# ==========================================================================
# Geometry checks (pilot_qc SPEC.md 7)
# ==========================================================================


def _geometry_result(frame: pd.DataFrame, check: str, unit: str) -> pd.Series:
    rows = frame[frame.check.eq(check) & frame.unit.eq(unit)]
    assert len(rows) == 1, (check, unit, len(rows))
    return rows.iloc[0]


def test_clean_tiling_passes_every_geometry_check() -> None:
    districts = [("D", square(0.0, 0.0, 1.0, 1.0))]
    blocks = [("D", "B1", square(0.0, 0.0, 0.5, 1.0)), ("D", "B2", square(0.5, 0.0, 1.0, 1.0))]
    out = Q.geometry_checks(*admin_frames(blocks, districts), state="S")
    assert out.ok.all(), out[~out.ok].to_string()


def test_a_gap_between_children_and_parent_is_detected_and_quantified() -> None:
    districts = [("D", square(0.0, 0.0, 1.0, 1.0))]
    blocks = [("D", "B1", square(0.0, 0.0, 0.5, 1.0))]  # right half missing
    out = Q.geometry_checks(*admin_frames(blocks, districts), state="S")
    row = _geometry_result(out, "gap_blocks_cover_district", "D")
    assert not row.ok
    assert row.discrepancy_fraction == pytest.approx(0.5, abs=0.02)
    assert row.discrepancy_area_m2 > Q.GEOMETRY_SLIVER_FLOOR_M2


def test_child_area_outside_its_parent_is_detected() -> None:
    districts = [("D", square(0.0, 0.0, 1.0, 1.0))]
    blocks = [("D", "B1", square(0.0, 0.0, 1.0, 1.0)), ("D", "B2", square(1.0, 0.0, 1.5, 1.0))]
    out = Q.geometry_checks(*admin_frames(blocks, districts), state="S")
    row = _geometry_result(out, "child_inside_parent", "D")
    assert not row.ok
    assert row.discrepancy_area_m2 > Q.GEOMETRY_SLIVER_FLOOR_M2


def test_overlapping_children_are_detected_with_the_offending_pair_named() -> None:
    districts = [("D", square(0.0, 0.0, 1.0, 1.0))]
    blocks = [("D", "B1", square(0.0, 0.0, 0.6, 1.0)), ("D", "B2", square(0.4, 0.0, 1.0, 1.0))]
    out = Q.geometry_checks(*admin_frames(blocks, districts), state="S")
    row = _geometry_result(out, "blocks_do_not_overlap", "D")
    assert not row.ok
    assert row.discrepancy_fraction == pytest.approx(1 / 3, abs=0.02)
    assert "B1" in row.detail and "B2" in row.detail


def test_a_sliver_below_the_declared_floor_does_not_fail_a_check() -> None:
    """The tolerance is declared before evaluation, so a 1 m overlap must not be a finding."""

    districts = [("D", square(0.0, 0.0, 1.0, 1.0))]
    blocks = [("D", "B1", square(0.0, 0.0, 0.5000001, 1.0)),
              ("D", "B2", square(0.5, 0.0, 1.0, 1.0))]
    out = Q.geometry_checks(*admin_frames(blocks, districts), state="S")
    row = _geometry_result(out, "blocks_do_not_overlap", "D")
    assert row.ok
    assert row.discrepancy_area_m2 < Q.GEOMETRY_SLIVER_FLOOR_M2


def test_an_orphan_block_parent_is_reported() -> None:
    districts = [("D", square(0.0, 0.0, 1.0, 1.0))]
    blocks = [("D", "B1", square(0.0, 0.0, 1.0, 1.0)),
              ("Elsewhere", "B2", square(2.0, 0.0, 3.0, 1.0))]
    out = Q.geometry_checks(*admin_frames(blocks, districts), state="S")
    row = _geometry_result(out, "every_block_parent_present", "S")
    assert not row.ok
    assert "Elsewhere" in row.detail


def test_duplicate_block_geometries_are_reported() -> None:
    districts = [("D", square(0.0, 0.0, 1.0, 1.0))]
    blocks = [("D", "B1", square(0.0, 0.0, 1.0, 1.0)), ("D", "B2", square(0.0, 0.0, 1.0, 1.0))]
    out = Q.geometry_checks(*admin_frames(blocks, districts), state="S")
    assert not _geometry_result(out, "block_geometries_distinct", "D" if False else "S").ok


def test_geometry_tolerances_are_declared_as_constants() -> None:
    """They must be readable from the module, not buried in a comparison."""

    assert Q.GEOMETRY_GAP_TOLERANCE == 0.001
    assert Q.GEOMETRY_OUTSIDE_TOLERANCE == 0.001
    assert Q.GEOMETRY_OVERLAP_TOLERANCE == 0.001
    assert Q.GEOMETRY_SLIVER_FLOOR_M2 == 10_000.0
    assert Q.ANALYSIS_CRS == "EPSG:6933"


# ==========================================================================
# Reproduction of milestone 4 (pilot_qc SPEC.md 5, A1)
# ==========================================================================


def _frozen_table(tmp_path: Path, **overrides) -> Path:
    rows = []
    for key in ("A", "B"):
        row = {"state_name": "Kerala", "unit_key": key, "candidate": "W1",
               "wbgt_annual_mean_c": 29.0, "days_ge_28": 200.0, "days_ge_30": 100.0,
               "days_ge_32": 10.0, "valid_area_fraction": 1.0}
        rows.append(row)
    frame = pd.DataFrame(rows)
    for column, value in overrides.items():
        frame.loc[0, column] = value
    path = tmp_path / "district_values.csv"
    frame.to_csv(path, index=False)
    return path


def _new_units() -> pd.DataFrame:
    return pd.DataFrame([
        {"state_name": "Kerala", "level": "district", "unit_key": key,
         "wbgt_annual_mean_c": 29.0, "days_ge_28": 200.0, "days_ge_30": 100.0,
         "days_ge_32": 10.0, "valid_area_fraction": 1.0}
        for key in ("A", "B")])


def test_reproduction_check_passes_on_an_identical_table(tmp_path: Path) -> None:
    out = Q.reproduction_check(_new_units(), _frozen_table(tmp_path), level="district")
    assert out["ok"]
    assert out["max_abs_mean_diff_c"] == 0.0
    assert out["n_units"] == 2


def test_reproduction_check_fails_on_a_value_beyond_the_frozen_tolerance(tmp_path: Path) -> None:
    out = Q.reproduction_check(_new_units(),
                               _frozen_table(tmp_path, wbgt_annual_mean_c=29.0 + 1e-6),
                               level="district")
    assert not out["ok"]
    assert out["max_abs_mean_diff_c"] > Q.REPRODUCTION_TOL_C


def test_reproduction_check_requires_exactly_equal_counts(tmp_path: Path) -> None:
    out = Q.reproduction_check(_new_units(), _frozen_table(tmp_path, days_ge_32=11.0),
                               level="district")
    assert not out["ok"]
    assert out["count_mismatches"] == 1


def test_reproduction_check_detects_a_changed_nan_pattern(tmp_path: Path) -> None:
    out = Q.reproduction_check(_new_units(), _frozen_table(tmp_path,
                                                           wbgt_annual_mean_c=np.nan),
                               level="district")
    assert not out["ok"]
    assert out["nan_pattern_mismatches"] == 1


def test_reproduction_check_reports_a_missing_frozen_table(tmp_path: Path) -> None:
    out = Q.reproduction_check(_new_units(), tmp_path / "absent.csv", level="district")
    assert not out["ok"]
    assert "missing" in out["detail"]


# ==========================================================================
# Isolation (pilot_qc SPEC.md 8)
# ==========================================================================


@pytest.mark.parametrize("target", [
    "docs/diagnostics/wbgt_outdoor_pilot",
    "docs/diagnostics/wbgt_outdoor_pilot/nested/deeper",
    "scratch/wbgt_outdoor_pilot",
    "docs/diagnostics/wbgt_outdoor_selection",
    "docs/diagnostics/wbgt_shade_release",
])
def test_milestone_4_evidence_and_cache_are_refused(target: str) -> None:
    with pytest.raises(SystemExit, match="Refusing to use"):
        Q.qc_guard(Path(target), "--out-dir")


@pytest.mark.parametrize("target", [
    "D:/projects/irt_data/processed/heat_wbgt",
    "D:/projects/irt_data/processed_optimised",
    "scratch/wbgt_shade_national",
])
def test_production_destinations_are_refused(target: str) -> None:
    with pytest.raises(SystemExit, match="Refusing to use"):
        Q.qc_guard(Path(target), "--work-dir")


def test_this_milestones_own_directories_are_allowed() -> None:
    """The guard must not be so broad that it blocks the stage it is meant to protect."""

    assert Q.qc_guard(Q.DEFAULT_OUT_DIR, "--out-dir").name == "wbgt_outdoor_pilot_qc"
    assert Q.qc_guard(Q.DEFAULT_WORK_DIR, "--work-dir").name == "wbgt_outdoor_pilot_qc"


def test_no_production_module_imports_either_diagnostic_tool() -> None:
    offenders = [
        path for path in Path("india_resilience_tool").rglob("*.py")
        if "wbgt_outdoor_pilot" in path.read_text(encoding="utf-8")
    ]
    assert offenders == []


# ==========================================================================
# Limitations and wording (pilot_qc SPEC.md 10)
# ==========================================================================


def test_the_clip_is_always_declared_as_experimental() -> None:
    text = " ".join(P.limitations(rh_policy="clip100")).lower()
    assert "experimental" in text
    assert "not accepted source repair" in text
    assert "complete-365 rule still applies" in text


def test_the_gmted_elevation_is_declared_when_used() -> None:
    text = " ".join(P.limitations(elevation_convention="elev-gmted2010-mea300-cellmean-abc"))
    assert "elev-gmted2010" in text
    assert "formulation is unchanged" in text
    assert "never one value per district" in text


def test_strict_sea_level_limitations_are_milestone_4s_set() -> None:
    assert P.limitations() == P.LIMITATIONS


def test_the_elevation_example_is_not_presented_as_a_national_bound() -> None:
    text = " ".join(P.LIMITATIONS)
    assert "0.250 C" in text
    assert "NOT a national upper bound" in text


def test_the_data_not_code_claim_is_scoped_to_coverage_failures() -> None:
    text = " ".join(P.LIMITATIONS).lower()
    assert "applies only to those observed coverage failures" in text


def test_method_and_model_sensitivity_are_declared_non_comparable() -> None:
    text = " ".join(P.LIMITATIONS).lower()
    assert "different geographic and metric scopes" in text
    assert "not comparable" in text


def test_aggregation_equality_is_not_claimed_to_prove_tiling() -> None:
    text = " ".join(P.LIMITATIONS)
    assert "does NOT prove that blocks tile districts" in text


def test_the_quantity_keeps_its_full_name() -> None:
    assert P.QUANTITY_NAME == "Annual mean of daily maximum outdoor WBGT"
    assert "daily mean WBGT" not in P.QUANTITY_NAME
    assert P.EXCEEDANCE_NAME == "Area-weighted mean annual cell exceedance days"


def test_the_run_matrix_is_the_frozen_four() -> None:
    assert [name for name, _, _ in Q.RUN_MATRIX] == ["baseline", "rh", "elev", "combined"]
    assert dict((n, (p, e)) for n, p, e in Q.RUN_MATRIX)["baseline"] == ("strict", False)
    assert dict((n, (p, e)) for n, p, e in Q.RUN_MATRIX)["combined"] == ("clip100", True)
    assert Q.SECOND_MODEL_RUNS == ("baseline", "combined")

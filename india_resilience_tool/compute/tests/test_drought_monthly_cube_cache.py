"""CHG-0110 — parity & contract tests for the drought monthly-cube disk cache.

Covers ``load_or_build_monthly_cube`` (CHG-0108): cold-vs-warm bit-parity, NaN-mask
survival through the NetCDF round-trip, grid_id↔index_range collision safety, the
``cache_root=None`` functional path, baseline-input invalidation (the union-hash
contribution), method-version gating, and year-span path splitting. Plus a
registry-invariant guard that all drought grid-first slugs share one cube key.

Run: python -m pytest india_resilience_tool/compute/tests/test_drought_monthly_cube_cache.py -q
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from india_resilience_tool.compute import drought_risk_gridfirst as drg
from india_resilience_tool.compute.drought_risk_gridfirst import (
    DROUGHT_GRIDFIRST_ADMIN_ONLY_SLUGS,
    DROUGHT_GRIDFIRST_SLUGS,
    drought_monthly_cube_cache_path,
    load_or_build_monthly_cube,
)


MODEL = "TestModel"
SCENARIO = "historical"
GRID_ID = "test-grid"
LAT = np.array([20.0, 20.25, 20.5], dtype=float)
LON = np.array([78.0, 78.25, 78.5], dtype=float)


def _write_year(
    out_dir: Path,
    year: int,
    *,
    seed: int,
    nan_month: int | None = None,
    nan_keep_days: int = 5,
) -> Path:
    """Write a synthetic daily ``pr`` NetCDF for one calendar year; return its path.

    ``nan_month`` (1-12) blanks all but ``nan_keep_days`` days of that month so the
    monthly coverage floor (0.90) masks it to NaN.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    time = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
    rng = np.random.default_rng(seed)
    data = rng.uniform(0.0, 10.0, size=(time.size, LAT.size, LON.size)).astype("float64")
    if nan_month is not None:
        month_mask = time.month == nan_month
        idx = np.nonzero(month_mask)[0]
        blank = idx[nan_keep_days:]  # keep only the first few finite days -> sub-coverage
        data[blank, :, :] = np.nan
    da = xr.DataArray(
        data,
        coords={"time": time, "lat": LAT, "lon": LON},
        dims=("time", "lat", "lon"),
        name="pr",
        attrs={"units": "mm/day"},
    )
    path = out_dir / f"pr_{year}.nc"
    da.to_dataset(name="pr").to_netcdf(path)
    return path


def _make_inputs(tmp_path: Path, *, nan_month: int | None = 1) -> tuple[dict, dict, list[int]]:
    """Build (baseline_year_to_paths, year_to_paths, years_needed) for a 3-year cube.

    Baseline years 2018-2019 + scenario year 2020. The sub-coverage NaN month is
    injected into a baseline year so it survives into the union cube.
    """
    base_dir = tmp_path / "inputs" / "baseline"
    scen_dir = tmp_path / "inputs" / "scenario"
    baseline_year_to_paths = {
        2018: {"pr": _write_year(base_dir, 2018, seed=1, nan_month=nan_month)},
        2019: {"pr": _write_year(base_dir, 2019, seed=2)},
    }
    year_to_paths = {
        2020: {"pr": _write_year(scen_dir, 2020, seed=3)},
    }
    years_needed = sorted(set(baseline_year_to_paths) | set(year_to_paths))
    return baseline_year_to_paths, year_to_paths, years_needed


def _call(cache_root, baseline, scenario, years, *, index_range=None, grid_id=GRID_ID):
    return load_or_build_monthly_cube(
        model=MODEL,
        scenario=SCENARIO,
        grid_id=grid_id,
        index_range=index_range,
        baseline_year_to_paths=baseline,
        year_to_paths=scenario,
        years_needed=years,
        min_daily_coverage=0.90,
        cache_root=cache_root,
    )


# --------------------------------------------------------------------------- #
# 1. Cold-vs-warm bit-parity
# --------------------------------------------------------------------------- #
def test_cold_warm_bit_parity(tmp_path):
    baseline, scenario, years = _make_inputs(tmp_path)
    cache_root = tmp_path / "cache"

    cold = _call(cache_root, baseline, scenario, years)  # writes the cube
    # Cube file must now exist at the expected path.
    cube_path = drought_monthly_cube_cache_path(
        cache_root, model=MODEL, scenario=SCENARIO, grid_id=GRID_ID, years_needed=years
    )
    assert cube_path.exists()

    warm = _call(cache_root, baseline, scenario, years)  # reads the cube
    xr.testing.assert_identical(cold, warm)


# --------------------------------------------------------------------------- #
# 2. NaN-mask survival through the NetCDF round-trip
# --------------------------------------------------------------------------- #
def test_nan_mask_survives_round_trip(tmp_path):
    baseline, scenario, years = _make_inputs(tmp_path, nan_month=1)
    cache_root = tmp_path / "cache"

    cold = _call(cache_root, baseline, scenario, years)
    warm = _call(cache_root, baseline, scenario, years)

    # The sub-coverage Jan-2018 month must be NaN, and the NaN mask must be identical
    # pre- and post-round-trip (no coord/NaN drift through NetCDF).
    jan = cold.sel(time="2018-01-01")
    assert bool(np.isnan(jan).all())
    assert np.array_equal(np.isnan(cold.values), np.isnan(warm.values))
    # And there must be at least one finite month (the cube is not all-NaN).
    assert bool(np.isfinite(cold.values).any())


# --------------------------------------------------------------------------- #
# 3. grid_id-collision guard (G1: index_range in the sidecar)
# --------------------------------------------------------------------------- #
def test_grid_id_collision_does_not_cross_serve(tmp_path):
    baseline, scenario, years = _make_inputs(tmp_path)
    cache_root = tmp_path / "cache"

    # Same grid_id, full extent -> writes a 3x3 cube.
    full = _call(cache_root, baseline, scenario, years, index_range=None)
    assert full.sizes["lat"] == 3 and full.sizes["lon"] == 3

    # Same grid_id, a 2x2 subset extent -> MUST miss (index_range differs in sidecar)
    # and rebuild its own extent rather than cross-serving the 3x3 cube.
    subset = _call(cache_root, baseline, scenario, years, index_range=(0, 2, 0, 2))
    assert subset.sizes["lat"] == 2 and subset.sizes["lon"] == 2

    # Inverse: same grid_id + same index_range + same inputs -> serves the cached extent.
    subset_again = _call(cache_root, baseline, scenario, years, index_range=(0, 2, 0, 2))
    xr.testing.assert_identical(subset, subset_again)


# --------------------------------------------------------------------------- #
# 4. cache_root=None functional parity (no file written, same array)
# --------------------------------------------------------------------------- #
def test_cache_root_none_functional_parity(tmp_path):
    baseline, scenario, years = _make_inputs(tmp_path)
    cache_root = tmp_path / "cache"

    # cache_root=None must not write any cube; only the explicit cached call does.
    uncached = _call(None, baseline, scenario, years)
    cached = _call(cache_root, baseline, scenario, years)
    cube_files = list((cache_root / "monthly_cube").rglob("pr_monthly.nc"))
    assert len(cube_files) == 1

    xr.testing.assert_identical(cached, uncached)


# --------------------------------------------------------------------------- #
# 5. Changed baseline input -> cache miss (the union-hash contribution)
# --------------------------------------------------------------------------- #
def test_changed_baseline_input_invalidates(tmp_path):
    baseline, scenario, years = _make_inputs(tmp_path, nan_month=None)
    cache_root = tmp_path / "cache"

    first = _call(cache_root, baseline, scenario, years)

    # Rewrite ONE baseline-year file with perturbed values at the SAME path. The
    # scenario-only hash would not see this; the union hash must.
    base_path = baseline[2018]["pr"]
    da = xr.open_dataset(base_path)["pr"].load()
    da = da + 100.0  # perturb every cell (arithmetic drops attrs)
    da.attrs["units"] = "mm/day"  # restore the units attr the round-trip build requires
    base_path.unlink()
    da.to_dataset(name="pr").to_netcdf(base_path)

    second = _call(cache_root, baseline, scenario, years)

    # The cube must reflect the perturbed baseline (a miss + rebuild), not the stale read.
    assert not np.allclose(
        np.nan_to_num(first.values), np.nan_to_num(second.values)
    )


# --------------------------------------------------------------------------- #
# 6. Method-version bump -> cache miss
# --------------------------------------------------------------------------- #
def test_method_version_bump_invalidates(tmp_path, monkeypatch):
    baseline, scenario, years = _make_inputs(tmp_path)
    cache_root = tmp_path / "cache"

    _call(cache_root, baseline, scenario, years)  # cold write under v1

    # Bump the cube method version; the prior sidecar must no longer match -> miss.
    monkeypatch.setattr(drg, "DROUGHT_MONTHLY_CUBE_METHOD_VERSION", "drought-monthly-cube-TEST-2")
    cube_path = drought_monthly_cube_cache_path(
        cache_root, model=MODEL, scenario=SCENARIO, grid_id=GRID_ID, years_needed=years
    )
    sidecar_path = cube_path.with_suffix(cube_path.suffix + ".json")
    before_mtime = sidecar_path.stat().st_mtime_ns

    _call(cache_root, baseline, scenario, years)  # should miss and rewrite

    # A rewrite (miss) updates the sidecar; a hit would leave it untouched.
    assert sidecar_path.stat().st_mtime_ns != before_mtime
    import json

    written = json.loads(sidecar_path.read_text(encoding="utf-8"))
    assert written["cube_method_version"] == "drought-monthly-cube-TEST-2"


# --------------------------------------------------------------------------- #
# 7. Year-span path split (G4)
# --------------------------------------------------------------------------- #
def test_year_span_path_split(tmp_path):
    baseline, scenario, years = _make_inputs(tmp_path)
    cache_root = tmp_path / "cache"

    # Full span 2018-2020.
    _call(cache_root, baseline, scenario, years)

    # A divergent window (drop the scenario year) -> different span -> distinct path.
    narrow_years = sorted(baseline)
    _call(cache_root, baseline, {}, narrow_years)

    p_full = drought_monthly_cube_cache_path(
        cache_root, model=MODEL, scenario=SCENARIO, grid_id=GRID_ID, years_needed=years
    )
    p_narrow = drought_monthly_cube_cache_path(
        cache_root, model=MODEL, scenario=SCENARIO, grid_id=GRID_ID, years_needed=narrow_years
    )
    assert p_full != p_narrow
    assert p_full.exists() and p_narrow.exists()


# --------------------------------------------------------------------------- #
# Registry invariant — all drought grid-first slugs share one cube key
# --------------------------------------------------------------------------- #
def test_drought_slugs_share_baseline_years():
    """All grid-first drought slugs must declare identical ``baseline_years`` so they
    share a single monthly-cube path (the load-bearing precondition for cube reuse).
    A future divergence is a safe separate file (G4 year-span path), but it should be
    surfaced loudly here rather than silently splitting the cache.
    """
    from india_resilience_tool.config.metrics_registry import ALL_METRICS_RAW

    slugs = set(DROUGHT_GRIDFIRST_SLUGS) | set(DROUGHT_GRIDFIRST_ADMIN_ONLY_SLUGS)
    by_slug = {
        str(m.get("slug", "")).strip(): dict(m.get("params") or {})
        for m in ALL_METRICS_RAW
        if str(m.get("slug", "")).strip() in slugs
    }
    missing = slugs - set(by_slug)
    assert not missing, f"drought grid-first slugs absent from registry: {sorted(missing)}"

    baselines = {
        slug: tuple(int(v) for v in params.get("baseline_years", (1981, 2010)))
        for slug, params in by_slug.items()
    }
    distinct = set(baselines.values())
    assert len(distinct) == 1, f"drought slugs do not share baseline_years: {baselines}"

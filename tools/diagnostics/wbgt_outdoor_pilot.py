#!/usr/bin/env python
"""Outdoor-WBGT engineering pilot: frozen W1 on actual NEX cells, aggregated to admin units.

Milestone 4.  The scientific method is **frozen** and imported unchanged from milestone 1
(``wbgt_outdoor_feasibility``) and milestone 2 (``wbgt_outdoor_selection``): this module adds
no physics.  What it adds is the engineering path — reading real NASA NEX files, computing
grid-first on climate cells, and area-weighting to districts and blocks — and measures whether
that path is correct and affordable.

Every output of this tool is DIAGNOSTIC-ONLY.  Nothing here enters the dashboard, the
production metric registry, composites, frozen rulers or published maps.  See
``docs/diagnostics/wbgt_outdoor_pilot/SPEC.md``, which was frozen before any result existed.

This module imports production helpers read-only and modifies none of them.  No production
module imports this tool.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

import tools.diagnostics.wbgt_outdoor_feasibility as m1
import tools.diagnostics.wbgt_outdoor_selection as m2
from tools.diagnostics.wbgt_method_validation import cos_solar_zenith

# ==========================================================================
# Frozen identities (SPEC.md 2, 3, 6)
# ==========================================================================

PILOT_CANDIDATE = "W1"
COMPARISON_CANDIDATE = "C1"

#: No local DEM or ``orog`` field exists, so every cell runs at sea level.  This is named in
#: the method signature so that a future elevation-aware cache can never be reused as this one.
ELEVATION_CONVENTION = "elev-sea-level-constant-no-dem"
ELEVATION_M = 0.0

PILOT_STATES = ("Kerala", "Rajasthan", "Himachal Pradesh")
PILOT_LEVELS = ("district", "block")
PILOT_MODEL = "ACCESS-CM2"
PILOT_SECOND_MODEL = "MRI-ESM2-0"
PILOT_SECOND_MODEL_STATES = ("Himachal Pradesh",)
PILOT_SCENARIO = "historical"
PILOT_YEAR = 2005
PILOT_MEMBER = "r1i1p1f1"

THRESHOLDS_C = (28.0, 30.0, 32.0)

#: Units the pilot accepts without conversion beyond K->degC (SPEC.md 4).
EXPECTED_UNITS = {
    "tas": ("K",),
    "tasmin": ("K",),
    "tasmax": ("K",),
    "hurs": ("%",),
    "rsds": ("W m-2", "W m**-2"),
    "sfcWind": ("m s-1", "m s**-1"),
}
KELVIN_VARIABLES = ("tas", "tasmin", "tasmax")
SUPPORTED_CALENDARS = ("proleptic_gregorian", "standard", "gregorian", "noleap")

#: Physical validity bounds used only to mark invalid inputs (SPEC.md 5.6).
VALID_T_RANGE_C = (-90.0, 60.0)

# Parity tolerances, frozen before scoring (SPEC.md 9).
PARITY_TOL_C = 1e-9

# Budget ceilings, declared before the run (SPEC.md 12).
BUDGET_WALL_SECONDS = 3600.0
BUDGET_PEAK_RSS_BYTES = 4 * 1024**3
BUDGET_ARTIFACT_BYTES = 5 * 1024**3

DEFAULT_OUT_DIR = Path("docs/diagnostics/wbgt_outdoor_pilot")
DEFAULT_WORK_DIR = Path("scratch/wbgt_outdoor_pilot")
DEFAULT_SOURCE_ROOT = Path("D:/projects/irt_data/r1i1p1f1")
DEFAULT_WBGT_ROOT = Path("D:/projects/irt_data/nex_gddp_cmip6_v2_wbgt/r1i1p1f1")
DEFAULT_BOUNDARY_ROOT = Path("D:/projects/irt_data")

#: Variables served by the WBGT-v2 acquisition tree rather than the main tree.
WBGT_TREE_VARIABLES = m1.NEX_WBGT_TREE_VARIABLES

#: Predecessor evidence directories, immutable to this milestone (SPEC.md 7).
PROTECTED_DIRS = (
    "docs/diagnostics/wbgt_outdoor_feasibility",
    "docs/diagnostics/wbgt_outdoor_selection",
    "docs/diagnostics/wbgt_outdoor_humidity",
    "docs/diagnostics/wbgt_shade_release",
)
#: Path fragments that must never appear in a resolved write target (SPEC.md 7).
FORBIDDEN_WRITE_FRAGMENTS = m1.FORBIDDEN_WRITE_FRAGMENTS + ("processed", "wbgt_shade_national")

#: Text stamped onto every manifest and every table-bearing report (SPEC.md 14).
LIMITATIONS = (
    "DIAGNOSTIC-ONLY: no output enters the dashboard, production metric registry, composites, "
    "frozen rulers or published maps.",
    "Uncorrected NEX inputs; no bias correction is applied.",
    "Six-site reconstruction evidence does not establish national accuracy.",
    "Remaining NEX distribution and count errors are unresolved.",
    "W1 may benefit from compensating reconstruction errors; its wind shape is a declared "
    "assumption, not an established physical reconstruction.",
    ">=32 C outcomes include reference-sensitive results.",
    "Rare-event regimes remain insufficiently evaluated.",
    "Spatial ranking accuracy is NOT established.",
    "Daily time-boundary semantics remain INFERRED; no NEX file publishes time_bnds.",
    f"Cell elevation is a sea-level constant ({ELEVATION_CONVENTION}); no DEM is available. "
    "Measured WBGT sensitivity is 0.250 C between 0 m and 2276 m at fixed drivers.",
    "All thresholds, including >=28 and >=30 C, remain diagnostic-only on NEX.",
)

QUANTITY_NAME = "Annual mean of daily maximum outdoor WBGT"
EXCEEDANCE_NAME = "Area-weighted mean annual cell exceedance days"


def method_signature(candidate: str = PILOT_CANDIDATE) -> str:
    """The full frozen identity of one pilot candidate.

    Every element that could change a number appears here, so a cache written under one
    signature can never be reused under another (SPEC.md 7).
    """

    wind = (
        f"wind-w1-dtr-slope{m2.WIND_DTR_SLOPE_PER_C}"
        f"-amp{m2.WIND_DTR_AMP_MIN}-{m2.WIND_DTR_AMP_MAX}"
        if candidate == "W1"
        else "wind-constant-daily-mean"
    )
    params = m1.CANDIDATE_PARAMS["C1"]
    return (
        f"outdoor-pilot-v1:{candidate}"
        f":parton-logan-a{params.parton_logan_a_h}-b{params.parton_logan_b}"
        f":humidity-{params.humidity_invariant}"
        f":magnus-{m1.MAGNUS_A_HPA}-{m1.MAGNUS_B}-{m1.MAGNUS_C_C}"
        f":toa-shape-erbs1982"
        f":{wind}"
        f":pressure-isa-{ELEVATION_CONVENTION}"
        f":liljegren-thermofeel{_thermofeel_version()}"
        f":day-nex-normalised-complete365-feb29dropped"
    )


def _thermofeel_version() -> str:
    try:
        import thermofeel

        return str(getattr(thermofeel, "__version__", "unknown"))
    except Exception:  # pragma: no cover - environment probe only
        return "unknown"


# ==========================================================================
# Write isolation (SPEC.md 7)
# ==========================================================================


def guard_write_target(path: Path, label: str, *, extra_protected: Sequence[Path] = ()) -> Path:
    """Resolve ``path`` and refuse it if it lands in a protected root.

    Resolution happens first, so a symlink into production is refused on its destination
    rather than on its name.  Raises ``SystemExit`` so a mistake stops the run instead of
    being caught and continued.
    """

    resolved = Path(path).resolve()
    lowered = str(resolved).replace("\\", "/").lower()
    for fragment in FORBIDDEN_WRITE_FRAGMENTS:
        if f"/{fragment}/" in f"{lowered}/" or lowered.endswith(f"/{fragment}"):
            raise SystemExit(
                f"Refusing to use {label} {resolved}: it resolves inside '{fragment}', "
                "which this milestone must not write to (SPEC.md 7)."
            )
    protected = [Path(p) for p in PROTECTED_DIRS] + list(extra_protected)
    for candidate in protected:
        try:
            other = Path(candidate).resolve()
        except OSError:  # pragma: no cover - unreadable path
            continue
        if resolved == other or other in resolved.parents:
            raise SystemExit(
                f"Refusing to use {label} {resolved}: it is inside protected root {other} "
                "(SPEC.md 7)."
            )
    return resolved


# ==========================================================================
# Input loading and verification (SPEC.md 4, 5)
# ==========================================================================


def variable_path(
    var: str, *, source_root: Path, wbgt_root: Path, model: str, scenario: str, year: int
) -> Path:
    """Locate one yearly NEX file, routing rsds/sfcWind to the WBGT-v2 acquisition tree."""

    root = wbgt_root if var in WBGT_TREE_VARIABLES else source_root
    return Path(root) / scenario / var / model / f"{year}.nc"


def required_years(year: int) -> tuple[int, int, int]:
    """The target year and the two padding years the temperature reconstruction reads.

    ``reconstruct_temperature_c`` needs ``tasmin``/``tasmax`` at the previous and next day, so
    the first and last target days need a neighbour from the adjacent year (SPEC.md 3).
    """

    return (year - 1, year, year + 1)


@dataclass(frozen=True)
class InputCheck:
    """Outcome of the pre-compute verification of one variable."""

    variable: str
    path: str
    units: str | None
    calendar: str
    days: int
    duplicate_dates: int
    missing_dates: int
    ok: bool
    reason: str


def verify_variable(
    path: Path, var: str, year: int, *, expect_days: int | None = None
) -> InputCheck:
    """Verify one yearly file's units, calendar and day coverage (SPEC.md 5.1-5.5)."""

    if not path.is_file():
        return InputCheck(var, str(path), None, "", 0, 0, 0, False, "file not found")
    with xr.open_dataset(path) as ds:
        if var not in ds:
            return InputCheck(var, str(path), None, "", 0, 0, 0, False, f"variable {var} absent")
        units = ds[var].attrs.get("units")
        calendar = str(ds.time.dt.calendar)
        times = pd.DatetimeIndex(ds.indexes["time"].to_datetimeindex()
                                 if hasattr(ds.indexes["time"], "to_datetimeindex")
                                 else ds.indexes["time"]).normalize()
        days = int(times.size)
    if units not in EXPECTED_UNITS[var]:
        return InputCheck(var, str(path), units, calendar, days, 0, 0, False,
                          f"units {units!r} not in {EXPECTED_UNITS[var]}; refusing to convert")
    if calendar not in SUPPORTED_CALENDARS:
        return InputCheck(var, str(path), units, calendar, days, 0, 0, False,
                          f"calendar {calendar!r} unsupported; refusing to convert")
    duplicates = int(times.duplicated().sum())
    expected = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
    if calendar == "noleap":
        expected = expected[~((expected.month == 2) & (expected.day == 29))]
    missing = int(len(expected.difference(times)))
    ok = duplicates == 0 and missing == 0
    if expect_days is not None and days != expect_days:
        ok = False
    reason = "ok" if ok else f"{duplicates} duplicate and {missing} missing dates"
    return InputCheck(var, str(path), units, calendar, days, duplicates, missing, ok, reason)


def verify_inputs(
    *, source_root: Path, wbgt_root: Path, model: str, scenario: str, year: int
) -> tuple[list[InputCheck], list[str]]:
    """Verify every required variable for the target year and both padding years."""

    checks: list[InputCheck] = []
    problems: list[str] = []
    for y in required_years(year):
        for var in m1.REQUIRED_NEX_VARIABLES:
            path = variable_path(var, source_root=source_root, wbgt_root=wbgt_root,
                                 model=model, scenario=scenario, year=y)
            check = verify_variable(path, var, y)
            checks.append(check)
            if not check.ok:
                problems.append(f"{var} {y}: {check.reason}")
    return checks, problems


def assert_identical_grids(arrays: Mapping[str, xr.DataArray]) -> None:
    """Refuse a silent regrid: every variable must carry identical lat/lon (SPEC.md 5.2)."""

    items = list(arrays.items())
    ref_name, ref = items[0]
    for name, arr in items[1:]:
        for axis in ("lat", "lon"):
            a = np.asarray(ref[axis].values, dtype=float)
            b = np.asarray(arr[axis].values, dtype=float)
            if a.shape != b.shape or not np.array_equal(a, b):
                raise ValueError(
                    f"{name} is on a different {axis} grid from {ref_name}; refusing to "
                    "regrid silently (SPEC.md 5.2)"
                )


def load_variable_span(
    var: str,
    *,
    source_root: Path,
    wbgt_root: Path,
    model: str,
    scenario: str,
    year: int,
    index_range: tuple[int, int, int, int] | None,
) -> xr.DataArray:
    """Load one variable over the target year plus the single padding day on each side.

    Whole adjacent years are opened but only their boundary day is retained, so the padding
    the temperature reconstruction needs is present without tripling the loaded volume.
    """

    from india_resilience_tool.compute.gridfirst_spatial import (
        normalize_lat_lon,
        subset_grid_by_index,
    )

    prev_year, target, next_year = required_years(year)
    parts: list[xr.DataArray] = []
    for y, keep in ((prev_year, "last"), (target, "all"), (next_year, "first")):
        path = variable_path(var, source_root=source_root, wbgt_root=wbgt_root,
                             model=model, scenario=scenario, year=y)
        with xr.open_dataset(path) as ds:
            ds = normalize_lat_lon(ds)
            arr = subset_grid_by_index(ds[var], index_range)
            if keep == "last":
                arr = arr.isel(time=[-1])
            elif keep == "first":
                arr = arr.isel(time=[0])
            parts.append(arr.load())
    out = xr.concat(parts, dim="time")
    if var in KELVIN_VARIABLES:
        out = out - 273.15
    return out


def load_daily_cube(
    *,
    source_root: Path,
    wbgt_root: Path,
    model: str,
    scenario: str,
    year: int,
    index_range: tuple[int, int, int, int] | None,
) -> tuple[dict[str, np.ndarray], pd.DatetimeIndex]:
    """Load all six variables onto one identical grid and normalise the day index.

    Returns ``(cube, days)`` where each cube entry is ``(n_days, n_lat, n_lon)``.  The daily
    timestamp is at 12:00 in the source and is normalised to midnight here, preserving the
    INFERRED day convention carried forward from milestones 1-3 (SPEC.md 5).
    """

    arrays = {
        var: load_variable_span(var, source_root=source_root, wbgt_root=wbgt_root,
                                model=model, scenario=scenario, year=year,
                                index_range=index_range)
        for var in m1.REQUIRED_NEX_VARIABLES
    }
    assert_identical_grids(arrays)
    ref = arrays["tas"]
    days = pd.DatetimeIndex(np.asarray(ref["time"].values)).normalize()
    for var, arr in arrays.items():
        other = pd.DatetimeIndex(np.asarray(arr["time"].values)).normalize()
        if not other.equals(days):
            raise ValueError(
                f"{var} day index differs from tas; refusing to join mismatched days "
                "(SPEC.md 5)"
            )
    cube = {var: np.asarray(arr.transpose("time", "lat", "lon").values, dtype=float)
            for var, arr in arrays.items()}
    return cube, days


def input_validity_mask(cube: Mapping[str, np.ndarray]) -> tuple[np.ndarray, dict[str, int]]:
    """Flag physically invalid cell-days without repairing them (SPEC.md 5.6, 5.7).

    ``tasmin > tasmax`` days are marked invalid rather than reordered or clipped, so a
    contradictory input can never silently become a plausible WBGT.
    """

    finite = np.ones_like(cube["tas"], dtype=bool)
    for arr in cube.values():
        finite &= np.isfinite(arr)
    lo, hi = VALID_T_RANGE_C
    in_range = finite.copy()
    for var in KELVIN_VARIABLES:
        in_range &= (cube[var] >= lo) & (cube[var] <= hi)
    in_range &= (cube["hurs"] >= 0.0) & (cube["hurs"] <= 100.0)
    in_range &= cube["rsds"] >= 0.0
    in_range &= cube["sfcWind"] >= 0.0
    ordered = cube["tasmin"] <= cube["tasmax"]
    valid = in_range & ordered
    counts = {
        "non_finite_cell_days": int((~finite).sum()),
        "out_of_range_cell_days": int((finite & ~in_range).sum()),
        "tasmin_gt_tasmax_cell_days": int((finite & in_range & ~ordered).sum()),
        "invalid_cell_days": int((~valid).sum()),
        "total_cell_days": int(valid.size),
    }
    return valid, counts


# ==========================================================================
# The frozen W1 path, applied to one cell (SPEC.md 2, 8, 9)
# ==========================================================================


def cell_daily_max_c(
    daily: pd.DataFrame,
    static: m1.SiteStatic,
    *,
    candidate: str = PILOT_CANDIDATE,
    target_days: pd.DatetimeIndex | None = None,
) -> pd.Series:
    """Daily maximum outdoor WBGT for one cell, using the frozen milestone-2 path.

    ``daily`` must carry exactly the six NEX variables indexed by normalised day, including
    the padding day on each side of ``target_days``.  Hours are built only for ``target_days``:
    each reconstructed hour depends on the daily values at its own day and its two neighbours,
    so restricting the hour span leaves the target days bit-identical while cutting the solver
    cost roughly threefold.  That equivalence is asserted in the companion test module.

    No hourly observation of any kind enters this function; ``m1.DailyInputs`` refuses any
    column that is not a daily NEX variable, so leakage is prevented by construction.
    """

    inputs = m1.DailyInputs(daily)
    days = pd.DatetimeIndex(daily.index) if target_days is None else pd.DatetimeIndex(target_days)
    times = pd.DatetimeIndex(
        np.concatenate([pd.date_range(d, periods=24, freq="h").to_numpy() for d in days])
    )
    local_day = pd.DatetimeIndex(times.normalize()).to_numpy()
    cz_mid = cos_solar_zenith(static.lat, times + m1.RADIATION_MIDPOINT_OFFSET, static.lon)
    base = m1.reconstruct_hourly(
        times, local_day, inputs, static, m1.CANDIDATE_PARAMS["C1"], cossza=cz_mid
    )
    if candidate == "W1":
        wind = m2.wind_w1_dtr_ms(daily, local_day, cz_mid)
    elif candidate == "C1":
        wind = base["wind_10m_ms"].to_numpy(dtype=float)
    else:
        raise ValueError(f"This pilot runs only W1 and C1; got {candidate!r}")
    wbgt = m1.liljegren_wbgt_c(
        base["t_c"].to_numpy(),
        base["rh_pct"].to_numpy(),
        base["pressure_hpa"].to_numpy(),
        wind,
        base["ssrd_w_m2"].to_numpy(),
        base["fdir_frac"].to_numpy(),
        cz_mid,
    )
    return m1.daily_max(wbgt, local_day, days)


def annual_statistics(daily_max: pd.Series, *, expected_days: int) -> dict[str, float]:
    """Annual mean of daily maxima and annual threshold counts for one cell (SPEC.md 10).

    An incomplete year yields NaN for every statistic.  Missing data never becomes a
    zero-exceedance year: the counts are NaN, not 0.
    """

    values = pd.to_numeric(daily_max, errors="coerce")
    valid = int(np.isfinite(values).sum())
    complete = valid == expected_days
    out: dict[str, float] = {
        "expected_days": float(expected_days),
        "valid_days": float(valid),
        "complete_year": float(bool(complete)),
    }
    if not complete:
        out["wbgt_annual_mean_c"] = np.nan
        for t in THRESHOLDS_C:
            out[f"days_ge_{int(t)}"] = np.nan
        return out
    out["wbgt_annual_mean_c"] = float(values.mean())
    for t in THRESHOLDS_C:
        out[f"days_ge_{int(t)}"] = float((values >= t).sum())
    return out


def drop_feb29(days: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Apply the frozen complete-365 / drop-Feb-29 policy (SPEC.md 10)."""

    return days[~((days.month == 2) & (days.day == 29))]


def compute_cell_grid(
    cube: Mapping[str, np.ndarray],
    days: pd.DatetimeIndex,
    *,
    lat: Sequence[float],
    lon: Sequence[float],
    cell_indices: Sequence[int],
    year: int,
    candidate: str = PILOT_CANDIDATE,
    valid_mask: np.ndarray | None = None,
    progress: object | None = None,
) -> xr.Dataset:
    """Compute per-cell annual statistics for the requested flat cell indices.

    Grid-first: WBGT is solved at every reconstructed hour of every requested cell and reduced
    to a daily maximum *before* any polygon ever sees it (SPEC.md 8).  Cells that are not
    requested stay NaN, which is how a polygon with no valid support stays NaN rather than
    acquiring a spatially filled value.
    """

    n_lat, n_lon = len(lat), len(lon)
    target = drop_feb29(days[days.year == year])
    fields = ["wbgt_annual_mean_c", "expected_days", "valid_days", "complete_year"] + [
        f"days_ge_{int(t)}" for t in THRESHOLDS_C
    ]
    out = {name: np.full((n_lat, n_lon), np.nan, dtype=float) for name in fields}
    out["input_invalid_days"] = np.full((n_lat, n_lon), np.nan, dtype=float)
    out["solver_invalid_days"] = np.full((n_lat, n_lon), np.nan, dtype=float)

    for position, flat in enumerate(cell_indices):
        i, j = divmod(int(flat), n_lon)
        frame = pd.DataFrame(
            {var: cube[var][:, i, j] for var in m1.REQUIRED_NEX_VARIABLES}, index=days
        )
        if valid_mask is not None:
            bad = ~valid_mask[:, i, j]
            if bad.any():
                frame.loc[bad, :] = np.nan
        static = m1.SiteStatic(f"cell_{i}_{j}", float(lat[i]), float(lon[j]), ELEVATION_M)
        series = cell_daily_max_c(frame, static, candidate=candidate, target_days=target)
        stats = annual_statistics(series, expected_days=len(target))
        for name, value in stats.items():
            out[name][i, j] = value
        if valid_mask is not None:
            in_target = pd.DatetimeIndex(days).isin(target)
            input_bad = int((~valid_mask[:, i, j][in_target]).sum())
        else:
            input_bad = 0
        out["input_invalid_days"][i, j] = float(input_bad)
        out["solver_invalid_days"][i, j] = float(
            max(0, len(target) - int(stats["valid_days"]) - input_bad)
        )
        if progress is not None:
            progress(position + 1, len(cell_indices))

    coords = {"lat": np.asarray(lat, dtype=float), "lon": np.asarray(lon, dtype=float)}
    ds = xr.Dataset({k: (("lat", "lon"), v) for k, v in out.items()}, coords=coords)
    ds.attrs["method_signature"] = method_signature(candidate)
    ds.attrs["quantity"] = QUANTITY_NAME
    ds.attrs["diagnostic_only"] = "true"
    return ds


# ==========================================================================
# Admin aggregation (SPEC.md 10, 11)
# ==========================================================================


def aggregate_units(
    grid_ds: xr.Dataset,
    weights: pd.DataFrame,
    *,
    level: str,
    grid,
    state: str,
    model: str,
    year: int,
    candidate: str,
) -> pd.DataFrame:
    """Area-weight per-cell annual statistics onto admin units.

    Uses the production intersection-area weights, so a unit's value is a true area-weighted
    mean over its *valid* intersecting cells.  Units with no valid support are retained as NaN
    with a reason; nothing is filled spatially (SPEC.md 10).
    """

    from india_resilience_tool.compute.gridfirst_spatial import aggregate_cell_values

    if weights.empty:
        return pd.DataFrame()
    fields = ["wbgt_annual_mean_c"] + [f"days_ge_{int(t)}" for t in THRESHOLDS_C]
    values = {f: aggregate_cell_values(grid_ds[f], weights, grid=grid) for f in fields}
    finite = np.isfinite(grid_ds["wbgt_annual_mean_c"])
    valid_area = aggregate_cell_values(finite.astype(float), weights, grid=grid)

    flat_valid = np.asarray(finite.transpose("lat", "lon").values).reshape(-1)
    rows: list[dict[str, object]] = []
    for unit_key, group in weights.groupby("unit_key", sort=False):
        idx = group["cell_index"].to_numpy(dtype=int)
        area = group["area_m2"].to_numpy(dtype=float)
        good = flat_valid[idx]
        total_area = float(area.sum())
        good_area = float(area[good].sum())
        fraction = good_area / total_area if total_area > 0 else 0.0
        row: dict[str, object] = {
            "state_name": state,
            "level": level,
            "unit_key": str(unit_key),
            "model": model,
            "scenario": PILOT_SCENARIO,
            "year": int(year),
            "candidate": candidate,
            "method_signature": method_signature(candidate),
            "valid_intersected_area_m2": good_area,
            "total_intersected_area_m2": total_area,
            "valid_area_fraction": fraction,
            "n_cells_total": int(len(idx)),
            "n_cells_valid": int(good.sum()),
            "n_cells_invalid": int((~good).sum()),
            "spatial_support_method": "intersection_area_weighted_valid_cells_only",
            "climate_fill_method": "native",
            "exclusion_reason": "" if good.any() else "no valid contributing climate cell",
        }
        for field in fields:
            row[field] = values[field].get(str(unit_key), np.nan)
        if level == "block" and "||" in str(unit_key):
            row["district_name"], row["block_name"] = str(unit_key).split("||", 1)
        else:
            row["district_name"] = str(unit_key)
            row["block_name"] = ""
        rows.append(row)
    frame = pd.DataFrame(rows)
    frame.attrs["exceedance_label"] = EXCEEDANCE_NAME
    return frame


def check_admin_correctness(frame: pd.DataFrame, *, level: str) -> list[dict[str, object]]:
    """Structural checks on an aggregated table (SPEC.md 11)."""

    checks: list[dict[str, object]] = []

    def record(name: str, ok: bool, detail: str = "") -> None:
        checks.append({"level": level, "check": name, "ok": bool(ok), "detail": detail})

    dupes = int(frame["unit_key"].duplicated().sum())
    record("unique_admin_keys", dupes == 0, f"{dupes} duplicate keys")

    ordering_ok = True
    detail = ""
    have = frame[["days_ge_28", "days_ge_30", "days_ge_32"]].dropna()
    if not have.empty:
        bad = int(
            ((have.days_ge_32 > have.days_ge_30 + 1e-9)
             | (have.days_ge_30 > have.days_ge_28 + 1e-9)).sum()
        )
        ordering_ok = bad == 0
        detail = f"{bad} rows violate days_ge_32 <= days_ge_30 <= days_ge_28"
    record("threshold_ordering", ordering_ok, detail)

    bounds_ok = True
    n_bad = 0
    for col in ("days_ge_28", "days_ge_30", "days_ge_32"):
        series = frame[col].dropna()
        n_bad += int(((series < 0) | (series > 365)).sum())
    bounds_ok = n_bad == 0
    record("counts_within_0_365", bounds_ok, f"{n_bad} rows outside [0, 365]")

    nan_rows = frame["wbgt_annual_mean_c"].isna()
    zero_counts = frame.loc[nan_rows, ["days_ge_28", "days_ge_30", "days_ge_32"]].fillna(-1)
    leaked = int((zero_counts == 0).any(axis=1).sum())
    record("missing_never_becomes_zero", leaked == 0,
           f"{leaked} NaN-value rows carry a zero count")

    unsupported = frame["n_cells_valid"].eq(0)
    retained = frame.loc[unsupported, "wbgt_annual_mean_c"].isna().all() if unsupported.any() else True
    record("unsupported_units_retained_as_nan", bool(retained),
           f"{int(unsupported.sum())} unsupported units")
    return checks


def weighted_mean_within_cell_range(
    grid_ds: xr.Dataset, weights: pd.DataFrame, frame: pd.DataFrame
) -> dict[str, object]:
    """Every weighted mean must lie inside the range of its contributing valid cells."""

    flat = np.asarray(grid_ds["wbgt_annual_mean_c"].transpose("lat", "lon").values).reshape(-1)
    violations = 0
    checked = 0
    for unit_key, group in weights.groupby("unit_key", sort=False):
        values = flat[group["cell_index"].to_numpy(dtype=int)]
        values = values[np.isfinite(values)]
        if values.size == 0:
            continue
        row = frame.loc[frame.unit_key.eq(str(unit_key)), "wbgt_annual_mean_c"]
        if row.empty or not np.isfinite(row.iloc[0]):
            continue
        checked += 1
        if not (values.min() - 1e-9 <= float(row.iloc[0]) <= values.max() + 1e-9):
            violations += 1
    return {"check": "weighted_mean_within_cell_range", "ok": violations == 0,
            "detail": f"{violations} of {checked} units outside their cell range"}


def block_to_district(frame: pd.DataFrame) -> pd.DataFrame:
    """Re-aggregate block rows to districts using VALID intersected area as the weight.

    Weighting by whole-block area would be wrong: the block statistic itself was formed over
    valid intersected area only, so the district re-aggregation must use the same support
    (SPEC.md 11).
    """

    rows: list[dict[str, object]] = []
    fields = ["wbgt_annual_mean_c"] + [f"days_ge_{int(t)}" for t in THRESHOLDS_C]
    for (state, district), group in frame.groupby(["state_name", "district_name"], sort=False):
        weight = group["valid_intersected_area_m2"].to_numpy(dtype=float)
        row: dict[str, object] = {"state_name": state, "district_name": district,
                                  "n_blocks": int(len(group))}
        for field in fields:
            values = group[field].to_numpy(dtype=float)
            good = np.isfinite(values) & (weight > 0)
            row[f"block_to_district_{field}"] = (
                float(np.average(values[good], weights=weight[good])) if good.any() else np.nan
            )
        rows.append(row)
    return pd.DataFrame(rows)


# ==========================================================================
# Parity (SPEC.md 9)
# ==========================================================================


def parity_against_frozen_path(
    daily: pd.DataFrame, site, *, candidate: str = PILOT_CANDIDATE
) -> dict[str, object]:
    """Compare the pilot cell path with ``m2.nex_candidate_daily_max`` on identical inputs.

    This is the check that the engineering rewrite did not change the science.  Both sides
    consume the same ``DailyInputs``; only the surrounding plumbing differs.
    """

    sample = daily.copy()
    sample.insert(0, "model", "parity")
    sample.insert(1, "scenario", PILOT_SCENARIO)
    sample.insert(2, "site", site.name)
    reference = m2.nex_candidate_daily_max(sample, [site], candidate)
    ref = reference["wbgt_daily_max_c"]
    ref.index = pd.DatetimeIndex(ref.index)

    static = m1.SiteStatic.from_site(site)
    pilot = cell_daily_max_c(daily, static, candidate=candidate)

    common = ref.index.intersection(pilot.index)
    diff = (pilot.reindex(common) - ref.reindex(common)).abs()
    finite = diff[np.isfinite(diff)]
    return {
        "check": "pilot_vs_frozen_nex_path",
        "candidate": candidate,
        "n_common_days": int(len(common)),
        "max_abs_diff_c": float(finite.max()) if len(finite) else 0.0,
        "tolerance_c": PARITY_TOL_C,
        "ok": bool(len(finite) and float(finite.max()) <= PARITY_TOL_C),
    }


def parity_hour_span(daily: pd.DataFrame, site, year: int) -> dict[str, object]:
    """Target-year-only hours must equal the full-span result on the target year.

    This is what licenses the threefold solver saving in ``cell_daily_max_c``, and it is also
    the first/last-target-day padding test: those are precisely the days whose reconstruction
    reads the adjacent year.
    """

    static = m1.SiteStatic.from_site(site)
    full = cell_daily_max_c(daily, static)
    target = drop_feb29(pd.DatetimeIndex(daily.index)[pd.DatetimeIndex(daily.index).year == year])
    trimmed = cell_daily_max_c(daily, static, target_days=target)
    diff = (trimmed - full.reindex(target)).abs()
    finite = diff[np.isfinite(diff)]
    boundary = [target[0], target[-1]]
    bdiff = (trimmed.reindex(boundary) - full.reindex(boundary)).abs()
    return {
        "check": "target_year_hours_equal_full_span",
        "n_days": int(len(target)),
        "max_abs_diff_c": float(finite.max()) if len(finite) else np.nan,
        "boundary_days_max_abs_diff_c": float(np.nanmax(bdiff.to_numpy())) if len(bdiff) else np.nan,
        "boundary_days_finite": bool(np.isfinite(trimmed.reindex(boundary)).all()),
        "tolerance_c": PARITY_TOL_C,
        "ok": bool(len(finite) and float(finite.max()) <= PARITY_TOL_C
                   and np.isfinite(trimmed.reindex(boundary)).all()),
    }


def parity_batch_and_chunks(
    cube: Mapping[str, np.ndarray],
    days: pd.DatetimeIndex,
    *,
    lat: Sequence[float],
    lon: Sequence[float],
    cell_indices: Sequence[int],
    year: int,
) -> list[dict[str, object]]:
    """One cell alone, and several chunk sizes, must be bit-identical (SPEC.md 9)."""

    checks: list[dict[str, object]] = []
    whole = compute_cell_grid(cube, days, lat=lat, lon=lon, cell_indices=cell_indices, year=year)
    field = "wbgt_annual_mean_c"

    single = compute_cell_grid(cube, days, lat=lat, lon=lon,
                               cell_indices=[cell_indices[0]], year=year)
    i, j = divmod(int(cell_indices[0]), len(lon))
    a = float(single[field].values[i, j])
    b = float(whole[field].values[i, j])
    checks.append({
        "check": "single_cell_equals_batch",
        "bit_identical": bool((np.isnan(a) and np.isnan(b)) or a == b),
        "detail": f"{a!r} vs {b!r}",
        "ok": bool((np.isnan(a) and np.isnan(b)) or a == b),
    })

    for size in (1, 3, len(cell_indices)):
        pieces = [list(cell_indices)[k:k + size] for k in range(0, len(cell_indices), size)]
        merged = np.full_like(whole[field].values, np.nan)
        counts = np.full_like(whole["days_ge_32"].values, np.nan)
        for piece in pieces:
            part = compute_cell_grid(cube, days, lat=lat, lon=lon, cell_indices=piece, year=year)
            mask = np.isfinite(part[field].values)
            merged[mask] = part[field].values[mask]
            cmask = np.isfinite(part["days_ge_32"].values)
            counts[cmask] = part["days_ge_32"].values[cmask]
        same = np.array_equal(merged, whole[field].values, equal_nan=True)
        same_counts = np.array_equal(counts, whole["days_ge_32"].values, equal_nan=True)
        checks.append({
            "check": f"chunk_size_{size}_equals_whole",
            "bit_identical": bool(same),
            "counts_exactly_equal": bool(same_counts),
            "ok": bool(same and same_counts),
        })
    return checks


# ==========================================================================
# Caching and resume (SPEC.md 7)
# ==========================================================================


def _hash_files(paths: Sequence[Path]) -> str:
    """Identity of the input files: size and mtime, hashed, in a stable order."""

    digest = hashlib.sha256()
    for path in sorted(str(p) for p in paths):
        p = Path(path)
        stat = p.stat()
        digest.update(f"{p.name}:{stat.st_size}:{int(stat.st_mtime)}".encode())
    return digest.hexdigest()[:16]


def cell_cache_sidecar(
    *, state: str, model: str, year: int, candidate: str, grid_id: str,
    input_paths: Sequence[Path], boundary_hash: str,
) -> dict[str, object]:
    """Everything that must match before a cached cell grid may be reused."""

    return {
        "method_signature": method_signature(candidate),
        "state": state,
        "model": model,
        "scenario": PILOT_SCENARIO,
        "year": int(year),
        "candidate": candidate,
        "grid_id": grid_id,
        "input_identity": _hash_files(input_paths),
        "boundary_hash": boundary_hash,
        "elevation_convention": ELEVATION_CONVENTION,
    }


def read_cell_cache(path: Path, *, expected: Mapping[str, object]) -> xr.Dataset | None:
    """Return the cached grid only when every signature element matches."""

    sidecar = path.with_suffix(".json")
    if not path.is_file() or not sidecar.is_file():
        return None
    try:
        stored = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if stored != dict(expected):
        return None
    with xr.open_dataset(path) as ds:
        return ds.load()


def write_cell_cache(ds: xr.Dataset, path: Path, *, sidecar: Mapping[str, object]) -> None:
    """Write the grid and its sidecar, the sidecar last so a torn write is not reused."""

    path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(path)
    path.with_suffix(".json").write_text(json.dumps(dict(sidecar), indent=2), encoding="utf-8")


# ==========================================================================
# Ranking sensitivity (SPEC.md 13)
# ==========================================================================


def ranking_comparison(
    left: pd.DataFrame, right: pd.DataFrame, *, field: str, label: str, kind: str
) -> dict[str, object]:
    """Compare two rankings of the same units. Diagnostic only.

    ``kind`` is ``"method"`` or ``"model"``; the two are reported separately and never merged,
    because they answer different questions.  Rank stability is not rank accuracy, and this
    function does not claim otherwise.
    """

    a = left.set_index("unit_key")[field]
    b = right.set_index("unit_key")[field]
    common = a.index.intersection(b.index)
    a, b = a.reindex(common), b.reindex(common)
    good = np.isfinite(a) & np.isfinite(b)
    excluded = int((~good).sum())
    a, b = a[good], b[good]
    if len(a) < 3:
        return {"comparison": label, "kind": kind, "field": field, "n_units": int(len(a)),
                "excluded_units": excluded, "spearman": np.nan, "insufficient": True}
    ra, rb = a.rank(method="average"), b.rank(method="average")
    shift = (ra - rb).abs()
    order = shift.sort_values(ascending=False)
    near_tie = int((a.diff().abs().dropna() < 0.01).sum())
    return {
        "comparison": label,
        "kind": kind,
        "field": field,
        "n_units": int(len(a)),
        "excluded_units": excluded,
        "spearman": float(ra.corr(rb, method="pearson")),
        "mean_abs_rank_shift": float(shift.mean()),
        "max_abs_rank_shift": float(shift.max()),
        "n_units_shift_gt_5": int((shift > 5).sum()),
        "largest_change_unit": str(order.index[0]) if len(order) else "",
        "largest_change_shift": float(order.iloc[0]) if len(order) else np.nan,
        "largest_change_values": f"{float(a.loc[order.index[0]]):.3f} -> "
                                 f"{float(b.loc[order.index[0]]):.3f}" if len(order) else "",
        "near_tie_pairs_within_0.01": near_tie,
        "insufficient": False,
    }


# ==========================================================================
# Runner
# ==========================================================================


class _PeakMemory:
    """Sample this process's RSS on a background thread for the duration of a stage."""

    def __init__(self) -> None:
        import psutil

        self._process = psutil.Process()
        self.peak = self._process.memory_info().rss
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "_PeakMemory":
        def sample() -> None:
            while not self._stop.wait(0.1):
                self.peak = max(self.peak, self._process.memory_info().rss)

        self._thread = threading.Thread(target=sample, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


def _directory_bytes(path: Path) -> int:
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())


def state_setup(
    state: str,
    *,
    boundary_root: Path,
    source_root: Path,
    wbgt_root: Path,
    model: str,
    year: int,
):
    """Boundaries, grid subset and area weights for one state, at both levels."""

    import geopandas as gpd
    from india_resilience_tool.compute.gridfirst_spatial import (
        bbox_to_index_range,
        build_area_weights,
        boundary_content_hash,
        dataset_grid_spec,
        normalize_lat_lon,
        subset_grid_by_index,
    )

    frames: dict[str, object] = {}
    hashes: dict[str, str] = {}
    for level in PILOT_LEVELS:
        path = Path(boundary_root) / f"{level}s_4326.geojson"
        gdf = gpd.read_file(path)
        subset = gdf[gdf.state_name.eq(state)].copy()
        if subset.empty:
            raise ValueError(f"No {level} boundaries for {state}")
        frames[level] = subset
        hashes[level] = boundary_content_hash(path)

    bounds = tuple(frames["district"].total_bounds)
    reference = variable_path("tas", source_root=source_root, wbgt_root=wbgt_root,
                              model=model, scenario=PILOT_SCENARIO, year=year)
    with xr.open_dataset(reference) as ds:
        ds = normalize_lat_lon(ds)
        index_range = bbox_to_index_range(ds.lat, ds.lon, bounds)
        grid = dataset_grid_spec(subset_grid_by_index(ds, index_range))

    weights = {level: build_area_weights(frames[level], grid, level=level)
               for level in PILOT_LEVELS}
    cells = sorted({int(v) for level in PILOT_LEVELS
                    for v in weights[level]["cell_index"].to_numpy(dtype=int)})
    return frames, hashes, index_range, grid, weights, cells


def run_manifest(args: argparse.Namespace, extra: Mapping[str, object]) -> dict[str, object]:
    """Git snapshot, package versions, exact command and the frozen identities."""

    import subprocess
    import sys

    def git(*cmd: str) -> str:
        try:
            return subprocess.run(["git", *cmd], capture_output=True, text=True,
                                  check=True).stdout.strip()
        except Exception:  # pragma: no cover - git absent
            return "unknown"

    import geopandas
    import psutil

    manifest = {
        "tool": "tools/diagnostics/wbgt_outdoor_pilot.py",
        "spec": "docs/diagnostics/wbgt_outdoor_pilot/SPEC.md",
        "generated_utc": pd.Timestamp.utcnow().isoformat(),
        "git_branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "git_sha": git("rev-parse", "--short", "HEAD"),
        "git_dirty": bool(git("status", "--porcelain")),
        "command": " ".join([str(Path(__file__)), *(os.sys.argv[1:])]),
        "python": sys.version.split()[0],
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "xarray": xr.__version__,
        "geopandas": geopandas.__version__,
        "psutil": psutil.__version__,
        "thermofeel": _thermofeel_version(),
        "method_signature_w1": method_signature("W1"),
        "method_signature_c1": method_signature("C1"),
        "frozen_method": "milestone 2 W1 == milestone 3 candidate B, unchanged",
        "scope": {
            "states": list(args.states),
            "levels": list(args.levels),
            "model": args.model,
            "scenario": PILOT_SCENARIO,
            "member": PILOT_MEMBER,
            "year": int(args.year),
        },
        "elevation_convention": ELEVATION_CONVENTION,
        "day_boundary": "INFERRED from the CMIP6 daily convention; no NEX file publishes "
                        "time_bnds, so this is not verified locally",
        "budget": {
            "wall_seconds": BUDGET_WALL_SECONDS,
            "peak_rss_bytes": BUDGET_PEAK_RSS_BYTES,
            "artifact_bytes": BUDGET_ARTIFACT_BYTES,
            "workers": int(args.workers),
        },
        "limitations": list(LIMITATIONS),
    }
    manifest.update(dict(extra))
    return manifest


def _write_csv(frame: pd.DataFrame, path: Path, *, verbose: bool = True) -> None:
    frame.to_csv(path, index=False)
    if verbose:
        print(f"  wrote {path} ({len(frame)} rows)", flush=True)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the staged engineering pilot and write its isolated artifacts."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--wbgt-root", type=Path, default=DEFAULT_WBGT_ROOT)
    parser.add_argument("--boundary-root", type=Path, default=DEFAULT_BOUNDARY_ROOT)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument("--states", nargs="+", default=list(PILOT_STATES))
    parser.add_argument("--levels", nargs="+", default=list(PILOT_LEVELS))
    parser.add_argument("--model", default=PILOT_MODEL)
    parser.add_argument("--year", type=int, default=PILOT_YEAR)
    parser.add_argument("--sample-cells", type=int, default=0,
                        help="Stage B: compute only this many cells per state.")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-sensitivity", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    verbose = not args.quiet

    out_dir = guard_write_target(args.out_dir, "--out-dir")
    work_dir = guard_write_target(args.work_dir, "--work-dir")
    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    if verbose:
        print("Outdoor-WBGT engineering pilot, milestone 4")
        print(f"  spec       docs/diagnostics/wbgt_outdoor_pilot/SPEC.md")
        print(f"  method     {method_signature(PILOT_CANDIDATE)}")
        print(f"  scope      {', '.join(args.states)} | {args.model} | {args.year}")
        print(f"  out-dir    {out_dir}")
        print(f"  work-dir   {work_dir}")
        print(f"  workers    {args.workers}")
        print("  DIAGNOSTIC-ONLY; no output enters production.\n")

    started_all = time.perf_counter()

    # ---------------- Stage A: preflight ----------------
    checks, problems = verify_inputs(source_root=args.source_root, wbgt_root=args.wbgt_root,
                                     model=args.model, scenario=PILOT_SCENARIO, year=args.year)
    input_frame = pd.DataFrame([c.__dict__ for c in checks])
    if verbose:
        print(f"Stage A preflight: {len(checks)} variable-years checked, "
              f"{len(problems)} problem(s)")
    if problems:
        for problem in problems:
            print(f"  FAIL {problem}", flush=True)
        _write_csv(input_frame, out_dir / "input_checks.csv", verbose=verbose)
        raise SystemExit("Input verification failed (SPEC.md 5); stopping before compute.")

    if args.dry_run:
        _write_csv(input_frame, out_dir / "input_checks.csv", verbose=verbose)
        (out_dir / "run_manifest_dry_run.json").write_text(
            json.dumps(run_manifest(args, {"stage": "dry-run", "problems": problems}), indent=2),
            encoding="utf-8")
        print("Dry run complete; nothing computed.")
        return 0

    manifest_extra: dict[str, object] = {}
    all_units: list[pd.DataFrame] = []
    all_checks: list[dict[str, object]] = []
    coverage_rows: list[dict[str, object]] = []
    timings: list[dict[str, object]] = []
    parity_rows: list[dict[str, object]] = []
    grids: dict[tuple[str, str, str], xr.Dataset] = {}
    setups: dict[str, object] = {}

    with _PeakMemory() as memory:
        for state in args.states:
            elapsed_total = time.perf_counter() - started_all
            if elapsed_total > BUDGET_WALL_SECONDS:
                print(f"  BUDGET: {elapsed_total:.0f} s exceeds {BUDGET_WALL_SECONDS:.0f} s; "
                      f"checkpointing before {state}.", flush=True)
                manifest_extra["budget_stop"] = {
                    "state_not_started": state,
                    "elapsed_seconds": elapsed_total,
                    "continue_command": (
                        f"python -m tools.diagnostics.wbgt_outdoor_pilot --states {state} "
                        f"--model {args.model} --year {args.year} --resume"
                    ),
                }
                break

            started = time.perf_counter()
            frames, hashes, index_range, grid, weights, cells = state_setup(
                state, boundary_root=args.boundary_root, source_root=args.source_root,
                wbgt_root=args.wbgt_root, model=args.model, year=args.year)
            setups[state] = (frames, hashes, index_range, grid, weights, cells)
            setup_seconds = time.perf_counter() - started
            if args.sample_cells:
                cells = cells[: args.sample_cells]
            if verbose:
                print(f"{state}: grid {grid.shape} | intersecting cells {len(cells)} | "
                      f"districts {len(frames['district'])} blocks {len(frames['block'])} | "
                      f"setup {setup_seconds:.1f} s", flush=True)

            started = time.perf_counter()
            cube, days = load_daily_cube(source_root=args.source_root, wbgt_root=args.wbgt_root,
                                         model=args.model, scenario=PILOT_SCENARIO,
                                         year=args.year, index_range=index_range)
            valid_mask, invalid_counts = input_validity_mask(cube)
            load_seconds = time.perf_counter() - started
            coverage_rows.append({"state": state, **invalid_counts,
                                  "load_seconds": load_seconds,
                                  "grid_cells": int(np.prod(grid.shape)),
                                  "intersecting_cells": len(cells)})

            input_paths = [variable_path(v, source_root=args.source_root,
                                         wbgt_root=args.wbgt_root, model=args.model,
                                         scenario=PILOT_SCENARIO, year=y)
                           for y in required_years(args.year)
                           for v in m1.REQUIRED_NEX_VARIABLES]

            candidates = [PILOT_CANDIDATE] if args.skip_sensitivity else [PILOT_CANDIDATE,
                                                                         COMPARISON_CANDIDATE]
            for candidate in candidates:
                sidecar = cell_cache_sidecar(
                    state=state, model=args.model, year=args.year, candidate=candidate,
                    grid_id=grid.grid_id, input_paths=input_paths,
                    boundary_hash=hashes["district"])
                cache_path = work_dir / f"cells_{state.replace(' ', '_')}_{args.model}_" \
                                        f"{args.year}_{candidate}.nc"
                grid_ds = None
                if args.resume and not args.overwrite:
                    grid_ds = read_cell_cache(cache_path, expected=sidecar)
                    if grid_ds is not None and verbose:
                        print(f"  {candidate}: reused cached cell grid", flush=True)
                if grid_ds is None:
                    if cache_path.exists() and not (args.overwrite or args.resume):
                        raise SystemExit(
                            f"Refusing to overwrite {cache_path}; pass --overwrite or --resume.")
                    started = time.perf_counter()
                    grid_ds = compute_cell_grid(cube, days, lat=grid.lat, lon=grid.lon,
                                                cell_indices=cells, year=args.year,
                                                candidate=candidate, valid_mask=valid_mask)
                    compute_seconds = time.perf_counter() - started
                    write_cell_cache(grid_ds, cache_path, sidecar=sidecar)
                    timings.append({"state": state, "candidate": candidate,
                                    "cells": len(cells), "setup_seconds": setup_seconds,
                                    "load_seconds": load_seconds,
                                    "compute_seconds": compute_seconds,
                                    "seconds_per_cell": compute_seconds / max(1, len(cells))})
                    if verbose:
                        print(f"  {candidate}: {len(cells)} cells in {compute_seconds:.1f} s "
                              f"({compute_seconds / max(1, len(cells)):.2f} s/cell)", flush=True)
                grids[(state, args.model, candidate)] = grid_ds

                for level in args.levels:
                    frame = aggregate_units(grid_ds, weights[level], level=level, grid=grid,
                                            state=state, model=args.model, year=args.year,
                                            candidate=candidate)
                    all_units.append(frame)
                    if candidate == PILOT_CANDIDATE:
                        for check in check_admin_correctness(frame, level=level):
                            all_checks.append({"state": state, "model": args.model, **check})
                        range_check = weighted_mean_within_cell_range(
                            grid_ds, weights[level], frame)
                        all_checks.append({"state": state, "model": args.model,
                                           "level": level, **range_check})

    if not all_units:
        raise SystemExit("No units aggregated; nothing to report.")

    units = pd.concat(all_units, ignore_index=True)
    _write_csv(units[units.level.eq("district")], out_dir / "district_values.csv", verbose=verbose)
    _write_csv(units[units.level.eq("block")], out_dir / "block_values.csv", verbose=verbose)
    _write_csv(pd.DataFrame(all_checks), out_dir / "admin_checks.csv", verbose=verbose)
    _write_csv(pd.DataFrame(coverage_rows), out_dir / "coverage.csv", verbose=verbose)
    _write_csv(input_frame, out_dir / "input_checks.csv", verbose=verbose)
    if timings:
        _write_csv(pd.DataFrame(timings), out_dir / "timings.csv", verbose=verbose)

    # District vs block-to-district reconciliation (SPEC.md 11).
    pilot_units = units[units.candidate.eq(PILOT_CANDIDATE)]
    blocks = pilot_units[pilot_units.level.eq("block")]
    districts = pilot_units[pilot_units.level.eq("district")]
    if not blocks.empty and not districts.empty:
        rollup = block_to_district(blocks)
        merged = districts.merge(rollup, on=["state_name", "district_name"], how="outer")
        merged["difference_c"] = (merged["block_to_district_wbgt_annual_mean_c"]
                                  - merged["wbgt_annual_mean_c"])
        _write_csv(merged[["state_name", "district_name", "n_blocks", "wbgt_annual_mean_c",
                           "block_to_district_wbgt_annual_mean_c", "difference_c",
                           "valid_area_fraction"]],
                   out_dir / "district_vs_block_rollup.csv", verbose=verbose)

    # Ranking sensitivity (SPEC.md 13), method only; model handled by a second invocation.
    sensitivity: list[dict[str, object]] = []
    if not args.skip_sensitivity:
        for level in args.levels:
            left = units[(units.level.eq(level)) & (units.candidate.eq(PILOT_CANDIDATE))]
            right = units[(units.level.eq(level)) & (units.candidate.eq(COMPARISON_CANDIDATE))]
            if left.empty or right.empty:
                continue
            for field in ("wbgt_annual_mean_c", "days_ge_32"):
                sensitivity.append({"level": level, **ranking_comparison(
                    left, right, field=field, label=f"W1 vs C1 ({level})", kind="method")})
    if sensitivity:
        _write_csv(pd.DataFrame(sensitivity), out_dir / "ranking_sensitivity.csv",
                   verbose=verbose)

    # Parity, on a real pilot cell rather than a synthetic one (SPEC.md 9).
    if setups:
        state = args.states[0]
        frames, hashes, index_range, grid, weights, cells = setups[state]
        cube, days = load_daily_cube(source_root=args.source_root, wbgt_root=args.wbgt_root,
                                     model=args.model, scenario=PILOT_SCENARIO,
                                     year=args.year, index_range=index_range)
        i, j = divmod(int(cells[0]), len(grid.lon))
        daily = pd.DataFrame({v: cube[v][:, i, j] for v in m1.REQUIRED_NEX_VARIABLES},
                             index=days)
        from tools.diagnostics.wbgt_method_validation import Site

        site = Site("parity_cell", float(grid.lat[i]), float(grid.lon[j]), ELEVATION_M, "pilot")
        parity_rows.append(parity_against_frozen_path(daily, site, candidate=PILOT_CANDIDATE))
        parity_rows.append(parity_against_frozen_path(daily, site, candidate=COMPARISON_CANDIDATE))
        parity_rows.append(parity_hour_span(daily, site, args.year))
        parity_rows.extend(parity_batch_and_chunks(cube, days, lat=grid.lat, lon=grid.lon,
                                                   cell_indices=cells[:4], year=args.year))
        _write_csv(pd.DataFrame(parity_rows), out_dir / "parity_checks.csv", verbose=verbose)

    wall = time.perf_counter() - started_all
    artifact_bytes = _directory_bytes(work_dir) + _directory_bytes(out_dir)
    manifest_extra.update({
        "stage": "all",
        "wall_seconds": wall,
        "peak_rss_bytes": int(memory.peak),
        "artifact_bytes": int(artifact_bytes),
        "budget_respected": {
            "wall": wall <= BUDGET_WALL_SECONDS,
            "peak_rss": memory.peak <= BUDGET_PEAK_RSS_BYTES,
            "artifacts": artifact_bytes <= BUDGET_ARTIFACT_BYTES,
        },
        "parity": parity_rows,
        "admin_checks_failed": int(sum(1 for c in all_checks if not c["ok"])),
        "quantity_name": QUANTITY_NAME,
        "exceedance_label": EXCEEDANCE_NAME,
    })
    (out_dir / "run_manifest.json").write_text(
        json.dumps(run_manifest(args, manifest_extra), indent=2, default=str), encoding="utf-8")
    if verbose:
        print(f"\nwall {wall:.1f} s | peak RSS {memory.peak / 1024**3:.2f} GB | "
              f"artifacts {artifact_bytes / 1024**2:.1f} MB")
        print(f"admin checks failed: {manifest_extra['admin_checks_failed']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

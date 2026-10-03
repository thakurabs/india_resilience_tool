"""Outdoor-WBGT humidity consistency: one input-consistent humidity formulation, two winds.

Frozen contract: ``docs/diagnostics/wbgt_outdoor_humidity/SPEC.md`` (CHG-0614). Everything this
module measures was declared there before any score was computed.

This is milestone 3 of the physically reconstructed open-sky WBGT. It **imports** milestone 1
(``wbgt_outdoor_feasibility``: references, gates, scoring, reconstruction) and milestone 2
(``wbgt_outdoor_selection``: the W1 wind shape and the R3 reference treatment) rather than
restating them, so no carried-forward definition can drift. Both predecessors' artifacts are
read-only here.

The four candidates (SPEC.md 2) are::

    A = existing humidity + C1 constant daily-mean wind      (= milestone 2 cand_C1, unchanged)
    B = existing humidity + W1 DTR-dependent wind            (= milestone 2 cand_W1, unchanged)
    C = input-consistent humidity + C1 wind                  (new)
    D = input-consistent humidity + W1 wind                  (new)

The new humidity formulation is **diagnostic-only**. It is an explicit method option in this
module; no existing function's behaviour is changed, and nothing here touches production
computation, metric registration, the national shade rebuild, its stage, its caches,
``processed``, ``processed_optimised`` or ``irt_data``. It never downloads and never installs.

Usage::

    python -m tools.diagnostics.wbgt_outdoor_humidity --self-test
    python -m tools.diagnostics.wbgt_outdoor_humidity --dry-run
    python -m tools.diagnostics.wbgt_outdoor_humidity --stage all --workers 1
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in (None, ""):  # pragma: no cover - direct-script fallback
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.diagnostics import wbgt_outdoor_feasibility as m1
from tools.diagnostics import wbgt_outdoor_selection as m2
from tools.diagnostics.wbgt_method_validation import SITES, Site, cos_solar_zenith

# ==========================================================================
# Frozen contract constants (SPEC.md). Gates, references and the W1 wind are
# IMPORTED, never restated, so that they cannot drift.
# ==========================================================================

#: SPEC.md 4.1 -- milestone 1's humidity invariant, unchanged. Constant daily vapour pressure.
HUMIDITY_OLD = "vapour_pressure"

#: SPEC.md 4.2 -- the one new formulation: a daily vapour-pressure parameter chosen so that the
#: reconstructed hourly RH, averaged over the same hours, reproduces the supplied daily ``hurs``.
HUMIDITY_NEW = "input_consistent_daily_rh"

#: SPEC.md 4.2 -- numerical tolerances, declared before scoring.
RH_RESIDUAL_TOL_PP = 1e-6
SOLVER_MAX_ITER = 200

#: SPEC.md 2 -- the four candidates: (humidity formulation, wind treatment).
CANDIDATES: Mapping[str, tuple[str, str]] = {
    "A": (HUMIDITY_OLD, "C1"),
    "B": (HUMIDITY_OLD, "W1"),
    "C": (HUMIDITY_NEW, "C1"),
    "D": (HUMIDITY_NEW, "W1"),
}
CANDIDATE_IDS: tuple[str, ...] = ("A", "B", "C", "D")
OLD_HUMIDITY_IDS: tuple[str, ...] = ("A", "B")
NEW_HUMIDITY_IDS: tuple[str, ...] = ("C", "D")

CANDIDATE_NAMES: Mapping[str, str] = {
    "A": "A existing humidity (constant daily e) + C1 constant daily-mean wind [milestone 2 C1]",
    "B": "B existing humidity (constant daily e) + W1 DTR-dependent wind [milestone 2 W1]",
    "C": "C input-consistent daily RH + C1 constant daily-mean wind [new]",
    "D": "D input-consistent daily RH + W1 DTR-dependent wind [new]",
}

#: The milestone 2 column each old-humidity candidate must reproduce exactly (SPEC.md 5).
MILESTONE2_EQUIVALENT: Mapping[str, str] = {"A": "cand_C1", "B": "cand_W1"}
MILESTONE2_DAILY_SERIES = Path("docs/diagnostics/wbgt_outdoor_selection/daily_series.parquet")

#: SPEC.md 6 -- both references, at all six sites. ref_legacy is excluded (contaminated tail).
REFERENCE_AUDITED = "ref_audited"
REFERENCE_R3 = "ref_R3_midpoint"
REFERENCES: tuple[str, ...] = (REFERENCE_AUDITED, REFERENCE_R3)

#: SPEC.md 8 -- robustness classes. NOT EVALUATED is never converted into a pass or a fail.
CLASS_ROBUST_PASS = "ROBUST PASS"
CLASS_ROBUST_FAIL = "ROBUST FAIL"
CLASS_REFERENCE_SENSITIVE = "REFERENCE-SENSITIVE"
CLASS_NOT_EVALUATED = "NOT EVALUATED"

#: SPEC.md 9.3 -- paired year-block resampling.
BOOTSTRAP_DRAWS = m1.BOOTSTRAP_DRAWS
BOOTSTRAP_SEED = m1.BOOTSTRAP_SEED

#: SPEC.md 10 -- the limited NEX transfer check, reusing milestone 2's cached sample only.
NEX_MODELS = m2.NEX_MODELS
NEX_SCENARIO = m2.NEX_SCENARIO
NEX_MATCHED_YEARS = m2.NEX_MATCHED_YEARS
MILESTONE2_WORK_DIR = Path("scratch/wbgt_outdoor_selection")

DEFAULT_OUT_DIR = Path("docs/diagnostics/wbgt_outdoor_humidity")
DEFAULT_WORK_DIR = Path("scratch/wbgt_outdoor_humidity")

#: Bumped whenever a cached per-site parquet's meaning changes (SPEC.md 5).
CACHE_SCHEMA = "wbgt-outdoor-humidity-cache-v1"

#: Directories this milestone must never write into, on top of milestone 1's list.
PROTECTED_DIRS: tuple[str, ...] = (
    "docs/diagnostics/wbgt_outdoor_feasibility",
    "docs/diagnostics/wbgt_outdoor_selection",
    "docs/diagnostics/wbgt_shade_release",
)
EXTRA_FORBIDDEN_FRAGMENTS: tuple[str, ...] = ("processed", "wbgt_shade_national")


def thermofeel_version() -> str:
    """The solver version, part of every candidate signature (SPEC.md 5)."""

    try:
        import thermofeel

        return str(getattr(thermofeel, "__version__", "unknown"))
    except Exception:  # pragma: no cover - reported, never silent
        return "unavailable"


def candidate_signature(cid: str, *, day_convention: str = "ist") -> str:
    """Immutable identity of one candidate: humidity, wind, solver, calendar, completeness.

    Distinguishes old from input-consistent humidity, C1 from W1 wind, the solver version, and
    the day/completeness rules. A cache is reusable only if this string matches (SPEC.md 5) --
    a matching reference signature is never sufficient on its own.
    """

    if cid not in CANDIDATES:
        raise ValueError(f"Unknown candidate: {cid!r}")
    humidity, wind = CANDIDATES[cid]
    if humidity == HUMIDITY_NEW:
        humidity_sig = f"{HUMIDITY_NEW}-tol{RH_RESIDUAL_TOL_PP:g}pp-iter{SOLVER_MAX_ITER}"
    else:
        humidity_sig = HUMIDITY_OLD
    if wind == "C1":
        wind_sig = "wind-constant-daily-mean"
    else:
        wind_sig = (
            f"wind-dtr-slope{m2.WIND_DTR_SLOPE_PER_C}"
            f"-amp{m2.WIND_DTR_AMP_MIN}-{m2.WIND_DTR_AMP_MAX}"
        )
    return (
        f"outdoor-humidity-v1:{cid}"
        f":parton-logan-a{m1.PARTON_LOGAN_A_H}-b{m1.PARTON_LOGAN_B}"
        f":humidity-{humidity_sig}"
        f":magnus-{m1.MAGNUS_A_HPA}-{m1.MAGNUS_B}-{m1.MAGNUS_C_C}"
        f":toa-shape-erbs1982"
        f":{wind_sig}"
        f":pressure-isa-from-site-elevation"
        f":liljegren-thermofeel{thermofeel_version()}"
        f":day-{day_convention}-complete24h-feb29dropped"
    )


def signature_bundle(day_convention: str = "ist") -> dict[str, object]:
    """Every method parameter and input identity a cache must match (SPEC.md 5)."""

    return {
        "schema": CACHE_SCHEMA,
        "candidates": {cid: candidate_signature(cid, day_convention=day_convention) for cid in CANDIDATE_IDS},
        "references": {
            REFERENCE_AUDITED: m1.REFERENCE_SIGNATURES[m1.REFERENCE_AUDITED],
            REFERENCE_R3: m2.REFERENCE_SIGNATURE_MIDPOINT,
        },
        "gates": {
            "median_abs_error_c": m1.GATE_MEDIAN_ABS_ERROR_C,
            "rmse_c": m1.GATE_RMSE_C,
            "count_min_ref_per_year": m1.COUNT_GATE_MIN_REF_PER_YEAR,
            "count_rel_tol": m1.COUNT_GATE_REL_TOL,
            "count_abs_floor_per_year": m1.COUNT_GATE_ABS_FLOOR_PER_YEAR,
        },
        "thresholds_c": list(m1.THRESHOLDS_C),
        "day_convention": day_convention,
    }


# ==========================================================================
# The input-consistent humidity solve (SPEC.md 4.2)
# ==========================================================================


def _day_matrix(values: np.ndarray, local_day: np.ndarray) -> tuple[np.ndarray, pd.DatetimeIndex]:
    """Reshape an hour-ordered series into ``(n_days, n_hours)``.

    Raises rather than guessing if the hours are not contiguous equal-length local-day blocks:
    the solve must run over a whole day's hours, never over a shortened set (SPEC.md 4.2).
    """

    day_index = pd.DatetimeIndex(local_day)
    codes, uniques = pd.factorize(day_index, sort=False)
    if codes.size == 0:
        return np.empty((0, 0), dtype=float), pd.DatetimeIndex([])
    if np.any(np.diff(codes) < 0):
        raise ValueError(
            "hours are not ordered in contiguous local-day blocks; refusing to reshape"
        )
    counts = np.bincount(codes)
    if not np.all(counts == counts[0]):
        raise ValueError(
            "local days carry differing hour counts "
            f"(min {int(counts.min())}, max {int(counts.max())}); "
            "the input-consistent solve requires whole days only"
        )
    matrix = np.asarray(values, dtype=float).reshape(len(counts), int(counts[0]))
    return matrix, pd.DatetimeIndex(uniques)


def constraint_mean_rh_pct(e_hpa: np.ndarray, es_hpa: np.ndarray) -> np.ndarray:
    """``mean_h[clip(100 e / es(T_h), 0, 100)]`` per day, the quantity constrained to ``hurs``.

    Non-decreasing in ``e`` for positive ``es``, which is what makes the bounded solve valid.
    """

    e = np.asarray(e_hpa, dtype=float)
    es = np.asarray(es_hpa, dtype=float)
    if es.ndim != 2:
        raise ValueError("es_hpa must be (n_days, n_hours)")
    if e.shape != (es.shape[0],):
        raise ValueError("e_hpa must be one value per day")
    with np.errstate(divide="ignore", invalid="ignore"):
        rh = 100.0 * e[:, None] / es
    return np.mean(np.clip(rh, 0.0, 100.0), axis=1)


@dataclass(frozen=True)
class HumiditySolution:
    """Per-day outcome of the input-consistency solve. Invalid days stay invalid."""

    e_star_hpa: np.ndarray
    residual_pp: np.ndarray
    iterations: np.ndarray
    valid: np.ndarray
    reason: np.ndarray
    path: np.ndarray


def solve_daily_vapour_pressure_hpa(
    es_hpa: np.ndarray,
    hurs_pct: np.ndarray,
    *,
    tol_pp: float = RH_RESIDUAL_TOL_PP,
    max_iter: int = SOLVER_MAX_ITER,
) -> HumiditySolution:
    """Choose one non-negative daily vapour pressure reproducing the supplied daily RH.

    Solves ``mean_h[clip(100 e / es(T_h), 0, 100)] = hurs`` per day, by the analytic expression
    where no hour saturates and by bounded bisection on ``[0, max_h es]`` where the clip binds.
    Endpoints ``hurs == 0`` and ``hurs == 100`` are handled exactly; ``hurs`` outside
    ``[0, 100]`` is invalid input and is never coerced. Non-convergence yields an explicit
    invalid day with a reason, never a fallback value (SPEC.md 4.2).
    """

    es = np.asarray(es_hpa, dtype=float)
    if es.ndim != 2:
        raise ValueError("es_hpa must be (n_days, n_hours)")
    hurs = np.asarray(hurs_pct, dtype=float)
    if hurs.shape != (es.shape[0],):
        raise ValueError("hurs_pct must carry exactly one value per day")
    if max_iter < 1:
        raise ValueError("max_iter must be at least 1")

    n_days = es.shape[0]
    e_star = np.full(n_days, np.nan)
    residual = np.full(n_days, np.nan)
    iterations = np.zeros(n_days, dtype=int)
    reason = np.array([""] * n_days, dtype=object)
    path = np.array([""] * n_days, dtype=object)
    if n_days == 0 or es.shape[1] == 0:
        return HumiditySolution(
            e_star, residual, iterations, np.zeros(n_days, dtype=bool), reason, path
        )

    es_ok = np.isfinite(es).all(axis=1) & (es > 0.0).all(axis=1)
    hurs_finite = np.isfinite(hurs)
    hurs_in_range = hurs_finite & (hurs >= 0.0) & (hurs <= 100.0)

    reason[~es_ok] = "invalid: non-finite or non-positive saturation vapour pressure"
    reason[es_ok & ~hurs_finite] = "invalid: non-finite daily hurs"
    reason[es_ok & hurs_finite & ~hurs_in_range] = "invalid: daily hurs outside [0, 100] percent"
    live = es_ok & hurs_in_range

    idx = np.flatnonzero(live)
    if idx.size:
        sub_es = es[idx]
        sub_h = hurs[idx]
        sub_min = sub_es.min(axis=1)
        sub_max = sub_es.max(axis=1)
        sub_e = np.full(idx.size, np.nan)
        sub_path = np.array([""] * idx.size, dtype=object)
        sub_it = np.zeros(idx.size, dtype=int)

        at_zero = sub_h <= 0.0
        sub_e[at_zero] = 0.0
        sub_path[at_zero] = "endpoint: daily hurs == 0"

        at_sat = sub_h >= 100.0
        sub_e[at_sat] = sub_max[at_sat]
        sub_path[at_sat] = "endpoint: daily hurs == 100, minimum e saturating every sampled hour"

        interior = ~at_zero & ~at_sat
        if interior.any():
            analytic = np.full(idx.size, np.nan)
            analytic[interior] = (sub_h[interior] / 100.0) / np.mean(
                1.0 / sub_es[interior], axis=1
            )
            unsaturated = interior & (analytic <= sub_min)
            sub_e[unsaturated] = analytic[unsaturated]
            sub_path[unsaturated] = "analytic: no sampled hour saturates"

            needs_solve = interior & ~unsaturated
            if needs_solve.any():
                es_need = sub_es[needs_solve]
                h_need = sub_h[needs_solve]
                lo = np.zeros(int(needs_solve.sum()), dtype=float)
                hi = sub_max[needs_solve].astype(float).copy()
                it = np.zeros(lo.size, dtype=int)
                mid = 0.5 * (lo + hi)
                for step in range(max_iter):
                    f = constraint_mean_rh_pct(mid, es_need) - h_need
                    done = np.abs(f) <= tol_pp
                    it[~done] = step + 1
                    if bool(done.all()):
                        break
                    above = f > 0.0
                    hi = np.where(above, mid, hi)
                    lo = np.where(above, lo, mid)
                    mid = 0.5 * (lo + hi)
                sub_e[needs_solve] = mid
                sub_it[needs_solve] = it
                sub_path[needs_solve] = "bisection: clipping at 100 percent binds"

        e_star[idx] = sub_e
        path[idx] = sub_path
        iterations[idx] = sub_it

        finite = idx[np.isfinite(e_star[idx])]
        if finite.size:
            residual[finite] = constraint_mean_rh_pct(e_star[finite], es[finite]) - hurs[finite]

    converged = np.isfinite(residual) & (np.abs(residual) <= tol_pp)
    valid = live & converged
    reason[valid] = "ok"
    failed = live & ~converged
    reason[failed] = (
        f"invalid: root solve did not reach |residual| <= {RH_RESIDUAL_TOL_PP:g} pp "
        f"within {max_iter} iterations"
    )
    e_star[failed] = np.nan
    return HumiditySolution(e_star, residual, iterations, valid, reason, path)


def reconstruct_humidity_input_consistent(
    t_hourly_c: np.ndarray,
    daily: pd.DataFrame,
    local_day: np.ndarray,
    *,
    tol_pp: float = RH_RESIDUAL_TOL_PP,
    max_iter: int = SOLVER_MAX_ITER,
) -> tuple[np.ndarray, np.ndarray, pd.DataFrame]:
    """Hourly RH (percent), a clip mask, and per-day diagnostics, from daily ``tas``/``hurs``.

    The temperature reconstruction is read, never altered. An invalid day yields NaN at every
    one of its hours, so its daily maximum is NaN and missing input can never become a
    zero-exceedance day (SPEC.md 4.2).
    """

    t_c = np.asarray(t_hourly_c, dtype=float)
    es_hourly = m1.saturation_pressure_hpa(t_c)
    es_matrix, day_index = _day_matrix(es_hourly, local_day)
    n_hours = int(es_matrix.shape[1]) if es_matrix.size else 0

    hurs = daily["hurs"].reindex(day_index).to_numpy(dtype=float)
    tas = daily["tas"].reindex(day_index).to_numpy(dtype=float)
    solution = solve_daily_vapour_pressure_hpa(
        es_matrix, hurs, tol_pp=tol_pp, max_iter=max_iter
    )

    e_old = (hurs / 100.0) * m1.saturation_pressure_hpa(tas)
    e_day = np.where(solution.valid, solution.e_star_hpa, np.nan)
    e_hourly = np.repeat(e_day, n_hours) if n_hours else np.empty(0, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        rh_raw = 100.0 * e_hourly / es_hourly
    clipped = np.isfinite(rh_raw) & ((rh_raw < 0.0) | (rh_raw > 100.0))
    rh_pct = np.clip(rh_raw, 0.0, 100.0)

    if es_matrix.size:
        saturated = np.mean(solution.e_star_hpa[:, None] >= es_matrix, axis=1)
        old_clip = np.mean(e_old[:, None] > es_matrix, axis=1)
    else:  # pragma: no cover - empty input
        saturated = np.empty(0, dtype=float)
        old_clip = np.empty(0, dtype=float)

    if es_matrix.size:
        # SPEC.md 9.1 requires the daily-RH residual for EVERY candidate, so the existing
        # method's miss is measured on the same days rather than left implicit.
        residual_old = constraint_mean_rh_pct(e_old, es_matrix) - hurs
    else:  # pragma: no cover - empty input
        residual_old = np.empty(0, dtype=float)

    per_day = pd.DataFrame(
        {
            "daily_hurs_pct": hurs,
            "e_old_hpa": e_old,
            "residual_old_pp": residual_old,
            "abs_residual_old_pp": np.abs(residual_old),
            "e_star_hpa": solution.e_star_hpa,
            "delta_e_hpa": solution.e_star_hpa - e_old,
            "residual_pp": solution.residual_pp,
            "abs_residual_pp": np.abs(solution.residual_pp),
            "iterations": solution.iterations,
            "valid": solution.valid,
            "reason": solution.reason,
            "solve_path": solution.path,
            "saturated_hour_fraction_new": saturated,
            "clipped_hour_fraction_old": old_clip,
        },
        index=day_index,
    )
    per_day.index.name = "local_day"
    return rh_pct, clipped, per_day


def effective_vapour_pressure_hpa(rh_pct: np.ndarray, t_c: np.ndarray) -> np.ndarray:
    """``e_eff = (RH/100) es(T)``.

    For the input-consistent method this equals ``min(e*, es(T_h))``: the effective vapour
    pressure is NOT constant at saturated hours, and the report says so (SPEC.md 4.2).
    """

    return (np.asarray(rh_pct, dtype=float) / 100.0) * m1.saturation_pressure_hpa(t_c)


# ==========================================================================
# Per-site experiment
# ==========================================================================


@dataclass
class HumidityResult:
    """One site-window run: daily maxima per identity plus its diagnostics."""

    site: str
    window: str
    daily: pd.DataFrame
    diagnostics: dict[str, object] = field(default_factory=dict)


def _peak_positions(frame: pd.DataFrame, column: str) -> pd.Series:
    """Positional index of each local day's maximum of ``column``; days with any NaN dropped."""

    grouped = frame.groupby("day")[column]
    complete = grouped.apply(lambda s: bool(np.isfinite(s).all()))
    picked = grouped.idxmax()
    picked = picked[complete.reindex(picked.index).fillna(False).to_numpy(dtype=bool)]
    return picked.dropna().astype(int)


def hourly_humidity_diagnostics(
    *,
    site: str,
    window: str,
    times: pd.DatetimeIndex,
    local_day: np.ndarray,
    rh_actual_pct: np.ndarray,
    t_actual_c: np.ndarray,
    t_recon_c: np.ndarray,
    wbgt_reference_hourly_c: np.ndarray,
    methods: Mapping[str, np.ndarray],
) -> pd.DataFrame:
    """Reconstructed-versus-observed hourly humidity, per method, season and peak hour.

    Hourly reference data is used here for **diagnosis only**; no candidate reads it
    (SPEC.md 9.1). The actual temperature peak and the reference WBGT peak are distinct hours
    and are reported separately -- they need not coincide.
    """

    seasons = m1.season_of(pd.DatetimeIndex(times))
    e_actual = effective_vapour_pressure_hpa(rh_actual_pct, t_actual_c)
    positions = pd.DataFrame(
        {
            "day": local_day,
            "t_actual": np.asarray(t_actual_c, dtype=float),
            "wbgt_ref": np.asarray(wbgt_reference_hourly_c, dtype=float),
        }
    ).reset_index(drop=True)
    t_peak = _peak_positions(positions, "t_actual")
    w_peak = _peak_positions(positions, "wbgt_ref")

    rows: list[dict[str, object]] = []
    for method, rh_recon in methods.items():
        rh_recon = np.asarray(rh_recon, dtype=float)
        e_recon = effective_vapour_pressure_hpa(rh_recon, t_recon_c)
        d_rh = rh_recon - np.asarray(rh_actual_pct, dtype=float)
        d_e = e_recon - e_actual
        for season in ("ALL", *m1.SEASONS):
            mask = (
                np.ones(len(times), dtype=bool)
                if season == "ALL"
                else (seasons == season).to_numpy()
            )
            rh_slice = d_rh[mask]
            e_slice = d_e[mask]
            finite_rh = np.isfinite(rh_slice)
            finite_e = np.isfinite(e_slice)
            row: dict[str, object] = {
                "site": site,
                "window": window,
                "humidity_method": method,
                "season": season,
                "n_hours": int(mask.sum()),
                "n_valid_hours": int(finite_rh.sum()),
                "rh_bias_pp": float(np.mean(rh_slice[finite_rh])) if finite_rh.any() else np.nan,
                "rh_mean_abs_error_pp": (
                    float(np.mean(np.abs(rh_slice[finite_rh]))) if finite_rh.any() else np.nan
                ),
                "rh_rmse_pp": (
                    float(np.sqrt(np.mean(rh_slice[finite_rh] ** 2))) if finite_rh.any() else np.nan
                ),
                "e_eff_bias_hpa": float(np.mean(e_slice[finite_e])) if finite_e.any() else np.nan,
                "e_eff_mean_abs_error_hpa": (
                    float(np.mean(np.abs(e_slice[finite_e]))) if finite_e.any() else np.nan
                ),
            }
            if season == "ALL":
                for label, picked in (
                    ("at_actual_temperature_peak", t_peak),
                    ("at_reference_wbgt_peak", w_peak),
                ):
                    take = picked.to_numpy(dtype=int)
                    sub_rh = d_rh[take]
                    sub_e = d_e[take]
                    ok_rh = np.isfinite(sub_rh)
                    ok_e = np.isfinite(sub_e)
                    row[f"n_days_{label}"] = int(len(take))
                    row[f"rh_bias_pp_{label}"] = (
                        float(np.mean(sub_rh[ok_rh])) if ok_rh.any() else np.nan
                    )
                    row[f"rh_mean_abs_error_pp_{label}"] = (
                        float(np.mean(np.abs(sub_rh[ok_rh]))) if ok_rh.any() else np.nan
                    )
                    row[f"e_eff_bias_hpa_{label}"] = (
                        float(np.mean(sub_e[ok_e])) if ok_e.any() else np.nan
                    )
                row["frac_days_peaks_coincide"] = (
                    float(
                        np.mean(
                            t_peak.reindex(w_peak.index).to_numpy(dtype=float)
                            == w_peak.to_numpy(dtype=float)
                        )
                    )
                    if len(w_peak)
                    else np.nan
                )
            rows.append(row)
    return pd.DataFrame(rows)


def run_site_window(
    site: Site,
    years: Sequence[int],
    *,
    cache_dir: Path,
    day_convention: str = "ist",
    verbose: bool = True,
) -> HumidityResult:
    """Build both references and all four candidates for one site-window.

    Every driver except humidity and the C1/W1 wind choice is shared by construction: the base
    reconstruction is computed once with milestone 1's C1 parameters and reused (SPEC.md 2.1).
    """

    if day_convention not in ("ist", "utc"):
        raise ValueError(f"Unknown day convention: {day_convention!r}")
    static = m1.SiteStatic.from_site(site)
    started = time.monotonic()
    raw = m1.load_hourly_cache(site, years, cache_dir)
    hourly = m1.attach_geometry(site, raw)
    if day_convention == "utc":
        hourly["local_day"] = pd.DatetimeIndex(pd.DatetimeIndex(hourly.index).date)

    keep = m1.complete_local_days(hourly)
    hourly = hourly[hourly["local_day"].isin(keep)]
    local_day = hourly["local_day"].to_numpy()
    times = pd.DatetimeIndex(hourly.index)

    t_actual = hourly["temperature_2m"].to_numpy(dtype=float)
    rh_actual = hourly["relative_humidity_2m"].to_numpy(dtype=float)
    p_actual = hourly["surface_pressure"].to_numpy(dtype=float)
    wind_actual = hourly["wind_speed_10m"].to_numpy(dtype=float)
    ssrd_actual = hourly["shortwave_radiation"].to_numpy(dtype=float)
    fdir_actual = hourly["fdir_frac"].to_numpy(dtype=float)
    cz_mid = hourly["cossza_mid"].to_numpy(dtype=float)

    series: dict[str, pd.Series] = {}
    peaks: dict[str, pd.Series] = {}
    diagnostics: dict[str, object] = {
        "site": site.name,
        "regime": site.regime,
        "elevation_m": site.elevation_m,
        "years": [int(y) for y in years],
        "day_convention": day_convention,
        "n_hours": int(len(hourly)),
        "n_complete_days": int(len(keep)),
        "signatures": signature_bundle(day_convention),
        "nan_rate": {},
        "wind": {},
    }

    def add(name: str, values: np.ndarray) -> None:
        series[name] = m1.daily_max(values, local_day, keep)
        peaks[name] = m2.peak_hour_utc(values, local_day, times)
        diagnostics["nan_rate"][name] = float(np.mean(~np.isfinite(values)))

    # ---- Reference 1: carried forward unchanged ----
    ref_audited_hourly = m1.liljegren_wbgt_c(
        t_actual, rh_actual, p_actual, wind_actual, ssrd_actual, fdir_actual, cz_mid
    )
    add(REFERENCE_AUDITED, ref_audited_hourly)

    # ---- Reference 2: R3, now at all six sites (SPEC.md 6) ----
    shifted = m2.interpolate_drivers_to_midpoint(hourly)
    add(
        REFERENCE_R3,
        m1.liljegren_wbgt_c(
            shifted["temperature_2m"].to_numpy(dtype=float),
            shifted["relative_humidity_2m"].to_numpy(dtype=float),
            shifted["surface_pressure"].to_numpy(dtype=float),
            shifted["wind_speed_10m"].to_numpy(dtype=float),
            ssrd_actual,
            fdir_actual,
            cz_mid,
        ),
    )

    # ---- Daily inputs: the only weather a deployable candidate may read ----
    daily_inputs = m1.aggregate_daily_inputs(hourly)
    daily = daily_inputs.frame

    base = m1.reconstruct_hourly(
        times, local_day, daily_inputs, static, m1.CANDIDATE_PARAMS["C1"], cossza=cz_mid
    )
    t_recon = base["t_c"].to_numpy(dtype=float)
    pressure = base["pressure_hpa"].to_numpy(dtype=float)
    ssrd_recon = base["ssrd_w_m2"].to_numpy(dtype=float)
    fdir_recon = base["fdir_frac"].to_numpy(dtype=float)

    # ---- The two humidity formulations, on the SAME reconstructed temperature ----
    rh_old = base["rh_pct"].to_numpy(dtype=float)
    rh_new, clipped_new, per_day = reconstruct_humidity_input_consistent(
        t_recon, daily, local_day
    )
    humidity: dict[str, np.ndarray] = {HUMIDITY_OLD: rh_old, HUMIDITY_NEW: rh_new}
    diagnostics["humidity_per_day"] = per_day
    diagnostics["humidity_consistency"] = humidity_consistency_summary(
        per_day, rh_old=rh_old, rh_new=rh_new, clipped_new=clipped_new, base=base
    )

    # ---- The two wind treatments, inherited verbatim ----
    winds: dict[str, np.ndarray] = {
        "C1": base["wind_10m_ms"].to_numpy(dtype=float),
        "W1": m2.wind_w1_dtr_ms(daily, local_day, cz_mid),
    }
    for wid, wind in winds.items():
        diagnostics["wind"][wid] = m2.wind_diagnostics(wind, daily, local_day)
    diagnostics["wind"]["ACTUAL"] = m2.wind_diagnostics(wind_actual, daily, local_day)

    # ---- The four candidates ----
    for cid in CANDIDATE_IDS:
        humidity_key, wind_key = CANDIDATES[cid]
        add(
            f"cand_{cid}",
            m1.liljegren_wbgt_c(
                t_recon,
                humidity[humidity_key],
                pressure,
                winds[wind_key],
                ssrd_recon,
                fdir_recon,
                cz_mid,
            ),
        )

    diagnostics["hourly_humidity"] = hourly_humidity_diagnostics(
        site=site.name,
        window=f"{min(years)}-{max(years)}",
        times=times,
        local_day=local_day,
        rh_actual_pct=rh_actual,
        t_actual_c=t_actual,
        t_recon_c=t_recon,
        wbgt_reference_hourly_c=ref_audited_hourly,
        methods=humidity,
    )

    diagnostics["wall_seconds"] = round(time.monotonic() - started, 2)
    if verbose:
        print(
            f"    {site.name} {min(years)}-{max(years)} [{day_convention}]: "
            f"{len(keep)} complete days, {diagnostics['wall_seconds']:.1f} s"
        )
    frame = pd.DataFrame(series)
    frame.index = pd.DatetimeIndex(frame.index, name="local_day")
    peak_frame = pd.DataFrame(peaks)
    peak_frame.index = pd.DatetimeIndex(peak_frame.index, name="local_day")
    diagnostics["peak_hour_frame"] = peak_frame
    diagnostics["peak_hour_mean_utc"] = {
        c: float(peak_frame[c].mean()) for c in peak_frame.columns
    }
    return HumidityResult(site.name, f"{min(years)}-{max(years)}", frame, diagnostics)



def daily_rh_residual_comparison(
    site: Site, years: Sequence[int], *, cache_dir: Path, verbose: bool = True
) -> pd.DataFrame:
    """Both humidity methods' daily-RH residual for one site-window (SPEC.md 9.1).

    Reuses the same reconstruction, saturation and constraint functions as the full run and
    skips the Liljegren solver, because the residual does not depend on it. That keeps this a
    cheap completion of the per-candidate residual requirement rather than a second code path
    that could drift from the one that produced the scores.
    """

    raw = m1.load_hourly_cache(site, years, cache_dir)
    hourly = m1.attach_geometry(site, raw)
    keep = m1.complete_local_days(hourly)
    hourly = hourly[hourly["local_day"].isin(keep)]
    local_day = hourly["local_day"].to_numpy()
    times = pd.DatetimeIndex(hourly.index)
    daily_inputs = m1.aggregate_daily_inputs(hourly)
    daily = daily_inputs.frame

    t_c = m1.reconstruct_temperature_c(
        times, daily, m1.SiteStatic.from_site(site), m1.CANDIDATE_PARAMS["C1"]
    )
    es_matrix, day_index = _day_matrix(m1.saturation_pressure_hpa(t_c), local_day)
    hurs = daily["hurs"].reindex(day_index).to_numpy(dtype=float)
    tas = daily["tas"].reindex(day_index).to_numpy(dtype=float)
    e_old = (hurs / 100.0) * m1.saturation_pressure_hpa(tas)
    solution = solve_daily_vapour_pressure_hpa(es_matrix, hurs)

    residual_old = constraint_mean_rh_pct(e_old, es_matrix) - hurs
    residual_new = solution.residual_pp
    valid = solution.valid
    window = f"{min(years)}-{max(years)}"
    rows = []
    for method, residual in (
        (HUMIDITY_OLD, residual_old),
        (HUMIDITY_NEW, residual_new),
    ):
        r = np.asarray(residual, dtype=float)[valid]
        rows.append(
            {
                "site": site.name,
                "window": window,
                "humidity_method": method,
                "candidates": "A, B" if method == HUMIDITY_OLD else "C, D",
                "n_days_valid": int(valid.sum()),
                "mean_residual_pp": float(np.nanmean(r)),
                "mean_abs_residual_pp": float(np.nanmean(np.abs(r))),
                "median_abs_residual_pp": float(np.nanmedian(np.abs(r))),
                "p95_abs_residual_pp": float(np.nanpercentile(np.abs(r), 95)),
                "max_abs_residual_pp": float(np.nanmax(np.abs(r))),
                "frac_days_abs_residual_gt_1pp": float(np.nanmean(np.abs(r) > 1.0)),
                "frac_days_abs_residual_gt_5pp": float(np.nanmean(np.abs(r) > 5.0)),
                "tolerance_pp": RH_RESIDUAL_TOL_PP,
            }
        )
    if verbose:
        print(
            f"    {site.name} {window}: existing mean |residual| "
            f"{rows[0]['mean_abs_residual_pp']:.3f} pp (max {rows[0]['max_abs_residual_pp']:.3f}), "
            f"input-consistent max {rows[1]['max_abs_residual_pp']:.2e} pp"
        )
    return pd.DataFrame(rows)


def humidity_consistency_summary(
    per_day: pd.DataFrame,
    *,
    rh_old: np.ndarray,
    rh_new: np.ndarray,
    clipped_new: np.ndarray,
    base: pd.DataFrame,
) -> dict[str, object]:
    """Residuals, invalid days, solver failures, saturation and the ``e*`` shift (SPEC.md 9.1)."""

    valid = per_day["valid"].to_numpy(dtype=bool)
    residual = per_day["abs_residual_pp"].to_numpy(dtype=float)
    reasons = per_day["reason"].astype(str)
    failures = reasons.str.startswith("invalid: root solve")
    delta = per_day["delta_e_hpa"].to_numpy(dtype=float)
    old_clip = np.asarray(base["rh_clipped"], dtype=bool)
    return {
        "n_days": int(len(per_day)),
        "n_days_valid": int(valid.sum()),
        "n_days_invalid": int((~valid).sum()),
        "n_solver_failures": int(failures.sum()),
        "invalid_reasons": {
            str(k): int(v)
            for k, v in reasons[~valid].value_counts().items()
        },
        "solve_paths": {
            str(k): int(v) for k, v in per_day["solve_path"].astype(str).value_counts().items()
        },
        "mean_abs_residual_pp": float(np.nanmean(residual[valid])) if valid.any() else np.nan,
        "max_abs_residual_pp": float(np.nanmax(residual[valid])) if valid.any() else np.nan,
        # The EXISTING method's miss on the same days, for contrast (SPEC.md 9.1).
        "existing_mean_residual_pp": (
            float(np.nanmean(per_day["residual_old_pp"].to_numpy(dtype=float)[valid]))
            if valid.any() and "residual_old_pp" in per_day
            else np.nan
        ),
        "existing_mean_abs_residual_pp": (
            float(np.nanmean(per_day["abs_residual_old_pp"].to_numpy(dtype=float)[valid]))
            if valid.any() and "abs_residual_old_pp" in per_day
            else np.nan
        ),
        "existing_max_abs_residual_pp": (
            float(np.nanmax(per_day["abs_residual_old_pp"].to_numpy(dtype=float)[valid]))
            if valid.any() and "abs_residual_old_pp" in per_day
            else np.nan
        ),
        "residual_tolerance_pp": RH_RESIDUAL_TOL_PP,
        "max_iterations_used": int(per_day["iterations"].max()) if len(per_day) else 0,
        "iteration_limit": SOLVER_MAX_ITER,
        "mean_delta_e_hpa": float(np.nanmean(delta)),
        "median_delta_e_hpa": float(np.nanmedian(delta)),
        "min_delta_e_hpa": float(np.nanmin(delta)) if len(delta) else np.nan,
        "max_delta_e_hpa": float(np.nanmax(delta)) if len(delta) else np.nan,
        "frac_days_e_star_above_e_old": float(np.nanmean(delta > 0.0)),
        "mean_saturated_hour_fraction_new": float(
            np.nanmean(per_day["saturated_hour_fraction_new"].to_numpy(dtype=float))
        ),
        "mean_clipped_hour_fraction_old": float(
            np.nanmean(per_day["clipped_hour_fraction_old"].to_numpy(dtype=float))
        ),
        "frac_hours_clipped_new": float(np.mean(np.asarray(clipped_new, dtype=bool))),
        "frac_hours_clipped_old": float(np.mean(old_clip)),
        "mean_rh_old_pct": float(np.nanmean(rh_old)),
        "mean_rh_new_pct": float(np.nanmean(rh_new)),
    }


# ==========================================================================
# Old-candidate reproduction against milestone 2 (SPEC.md 5)
# ==========================================================================


def reproduce_old_candidates(
    result: HumidityResult, milestone2_series: Path = MILESTONE2_DAILY_SERIES
) -> dict[str, object]:
    """Compare A and B, day for day, with milestone 2's committed ``cand_C1``/``cand_W1``.

    The expectation is exact equality: identical code path, identical inputs. A non-zero
    difference is a defect to investigate, never an improvement.
    """

    out: dict[str, object] = {
        "site": result.site,
        "window": result.window,
        "source": str(milestone2_series),
        "expectation": "exact equality (identical code path and inputs)",
    }
    if not Path(milestone2_series).exists():
        out["status"] = f"NOT EVALUATED: {milestone2_series} is absent"
        return out
    frame = pd.read_parquet(milestone2_series)
    block = frame[
        (frame["site"].astype(str) == result.site) & (frame["window"].astype(str) == result.window)
    ]
    if block.empty:
        out["status"] = "NOT EVALUATED: milestone 2 carries no series for this site-window"
        return out
    comparisons: dict[str, object] = {}
    worst = 0.0
    for cid, column in MILESTONE2_EQUIVALENT.items():
        mine = result.daily.get(f"cand_{cid}")
        theirs = block.get(column)
        if mine is None or theirs is None:
            comparisons[cid] = {"status": f"NOT EVALUATED: {column} absent"}
            continue
        paired = pd.DataFrame({"mine": mine, "theirs": theirs}).dropna()
        diff = (paired["mine"] - paired["theirs"]).abs()
        max_abs = float(diff.max()) if len(diff) else np.nan
        worst = max(worst, 0.0 if not np.isfinite(max_abs) else max_abs)
        comparisons[cid] = {
            "milestone2_column": column,
            "n_common_days": int(len(paired)),
            "n_days_mine": int(mine.notna().sum()),
            "n_days_milestone2": int(theirs.notna().sum()),
            "max_abs_difference_c": max_abs,
            "n_days_differing": int((diff > 0.0).sum()) if len(diff) else 0,
            "exact": bool(len(diff) and max_abs == 0.0),
        }
    out["comparisons"] = comparisons
    out["worst_max_abs_difference_c"] = worst
    out["status"] = (
        "REPRODUCED EXACTLY"
        if all(isinstance(v, dict) and v.get("exact") for v in comparisons.values())
        else "DIFFERENCE FOUND -- investigate before trusting any comparison"
    )
    return out


# ==========================================================================
# Scoring tables, one per reference (SPEC.md 9.2)
# ==========================================================================


def build_score_table(results: Sequence[HumidityResult]) -> pd.DataFrame:
    """Daily scores for every candidate against BOTH references, per season and pooled.

    Each candidate series is computed once (``run_site_window``) and scored against each
    reference; candidate timing never depends on the reference (SPEC.md 6).
    """

    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        seasons = m1.season_of(pd.DatetimeIndex(daily.index))
        for reference in REFERENCES:
            if reference not in daily:
                continue
            for cid in CANDIDATE_IDS:
                column = f"cand_{cid}"
                if column not in daily:
                    continue
                for season in ("ALL", *m1.SEASONS):
                    mask = (
                        slice(None) if season == "ALL" else (seasons == season).to_numpy()
                    )
                    row = m1.score_pair(
                        daily[reference][mask],
                        daily[column][mask],
                        site=result.site,
                        window=result.window,
                        season=season,
                        reference_name=reference,
                        candidate_name=column,
                        kind="deployable candidate",
                    )
                    row["candidate_id"] = cid
                    row["humidity_method"] = CANDIDATES[cid][0]
                    row["wind_treatment"] = CANDIDATES[cid][1]
                    rows.append(row)
        # The two reference treatments against each other, for context.
        if all(r in daily for r in REFERENCES):
            rows.append(
                {
                    **m1.score_pair(
                        daily[REFERENCE_AUDITED],
                        daily[REFERENCE_R3],
                        site=result.site,
                        window=result.window,
                        season="ALL",
                        reference_name=REFERENCE_AUDITED,
                        candidate_name=REFERENCE_R3,
                        kind="reference versus reference",
                    ),
                    "candidate_id": "",
                    "humidity_method": "",
                    "wind_treatment": "",
                }
            )
    return pd.DataFrame(rows)


def build_annual_table(results: Sequence[HumidityResult]) -> pd.DataFrame:
    """Annual means, threshold counts, count errors and the imported count gate, per reference."""

    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        for reference in REFERENCES:
            if reference not in daily:
                continue
            ref_series = daily[reference]
            ref_years_all = m1.complete_years(ref_series)
            for cid in CANDIDATE_IDS:
                column = f"cand_{cid}"
                if column not in daily:
                    continue
                years = m2.matched_complete_years(ref_series, daily[column])
                base = {
                    "site": result.site,
                    "window": result.window,
                    "reference": reference,
                    "candidate": column,
                    "candidate_id": cid,
                    "humidity_method": CANDIDATES[cid][0],
                    "wind_treatment": CANDIDATES[cid][1],
                    "n_complete_years_reference": len(ref_years_all),
                    "n_complete_years_candidate": len(m1.complete_years(daily[column])),
                    "n_complete_years": len(years),
                    "years": ",".join(str(y) for y in years),
                }
                if not years:
                    rows.append(
                        {
                            **base,
                            "threshold_c": np.nan,
                            "statistic": "annual_mean_c",
                            "reference_per_year": np.nan,
                            "candidate_per_year": np.nan,
                            "signed_error": np.nan,
                            "absolute_error": np.nan,
                            "relative_error": np.nan,
                            "count_gate": f"{CLASS_NOT_EVALUATED} (no matched complete year)",
                            "count_gate_tolerance_per_year": np.nan,
                        }
                    )
                    continue
                ref_annual = m1.annual_counts(ref_series, years)
                cand_annual = m1.annual_counts(daily[column], years)
                ref_mean = float(ref_annual["annual_mean_c"].mean())
                cand_mean = float(cand_annual["annual_mean_c"].mean())
                rows.append(
                    {
                        **base,
                        "threshold_c": np.nan,
                        "statistic": "annual_mean_c",
                        "reference_per_year": ref_mean,
                        "candidate_per_year": cand_mean,
                        "signed_error": cand_mean - ref_mean,
                        "absolute_error": abs(cand_mean - ref_mean),
                        "relative_error": np.nan,
                        "count_gate": "",
                        "count_gate_tolerance_per_year": np.nan,
                    }
                )
                for threshold in m1.THRESHOLDS_C:
                    col = f"days_ge_{threshold:g}"
                    ref_per_year = float(ref_annual[col].mean())
                    cand_per_year = float(cand_annual[col].mean())
                    verdict, tol = m1.count_gate(ref_per_year, cand_per_year)
                    rows.append(
                        {
                            **base,
                            "threshold_c": threshold,
                            "statistic": f"days_ge_{threshold:g}_per_year",
                            "reference_per_year": ref_per_year,
                            "candidate_per_year": cand_per_year,
                            "signed_error": cand_per_year - ref_per_year,
                            "absolute_error": abs(cand_per_year - ref_per_year),
                            "relative_error": (
                                (cand_per_year - ref_per_year) / ref_per_year
                                if ref_per_year > 0
                                else np.nan
                            ),
                            "count_gate": verdict,
                            "count_gate_tolerance_per_year": tol,
                        }
                    )
    return pd.DataFrame(rows)


def build_per_year_table(results: Sequence[HumidityResult]) -> pd.DataFrame:
    """Signed and absolute count error for EACH complete year, so cancellation is visible."""

    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        for reference in REFERENCES:
            if reference not in daily:
                continue
            for cid in CANDIDATE_IDS:
                column = f"cand_{cid}"
                if column not in daily:
                    continue
                years = m2.matched_complete_years(daily[reference], daily[column])
                if not years:
                    continue
                ref = m1.annual_counts(daily[reference], years)
                cand = m1.annual_counts(daily[column], years)
                for threshold in m1.THRESHOLDS_C:
                    col = f"days_ge_{threshold:g}"
                    ref_mean = float(ref[col].mean())
                    verdict, tol = m1.count_gate(ref_mean, float(cand[col].mean()))
                    errors = (cand[col] - ref[col]).to_numpy(dtype=float)
                    for year, error in zip(years, errors, strict=True):
                        rows.append(
                            {
                                "site": result.site,
                                "window": result.window,
                                "reference": reference,
                                "candidate": column,
                                "candidate_id": cid,
                                "threshold_c": threshold,
                                "year": int(year),
                                "reference_count": float(ref[col].loc[year]),
                                "candidate_count": float(cand[col].loc[year]),
                                "signed_error": float(error),
                                "absolute_error": float(abs(error)),
                                "window_mean_gate": verdict,
                                "gate_tolerance_per_year": tol,
                                "outside_tolerance": (
                                    bool(abs(error) > tol) if np.isfinite(tol) else False
                                ),
                            }
                        )
    return pd.DataFrame(rows)


def per_year_summary(per_year: pd.DataFrame) -> pd.DataFrame:
    """Collapse the per-year table to one row per site/window/reference/candidate/threshold."""

    if per_year.empty:
        return pd.DataFrame()
    keys = ["site", "window", "reference", "candidate", "candidate_id", "threshold_c"]
    grouped = per_year.groupby(keys, dropna=False)
    out = grouped.agg(
        n_complete_years=("year", "size"),
        window_mean_gate=("window_mean_gate", "first"),
        gate_tolerance_per_year=("gate_tolerance_per_year", "first"),
        reference_mean_per_year=("reference_count", "mean"),
        candidate_mean_per_year=("candidate_count", "mean"),
        mean_signed_error=("signed_error", "mean"),
        mean_absolute_error=("absolute_error", "mean"),
        min_signed_error=("signed_error", "min"),
        max_signed_error=("signed_error", "max"),
        n_years_outside_tolerance=("outside_tolerance", "sum"),
    ).reset_index()
    return out


# ==========================================================================
# Robustness across the two reference treatments (SPEC.md 8)
# ==========================================================================


def classify(verdict_audited: str, verdict_r3: str) -> str:
    """Map a pair of gate verdicts onto the predeclared robustness classes."""

    passes = {"PASS"}
    fails = {"FAIL"}
    if verdict_audited in passes and verdict_r3 in passes:
        return CLASS_ROBUST_PASS
    if verdict_audited in fails and verdict_r3 in fails:
        return CLASS_ROBUST_FAIL
    if {verdict_audited, verdict_r3} <= (passes | fails):
        return CLASS_REFERENCE_SENSITIVE
    return CLASS_NOT_EVALUATED


def build_robustness_table(results: Sequence[HumidityResult]) -> pd.DataFrame:
    """Gate each candidate against both references on COMMON dates and COMMON complete years.

    Exclusions are listed explicitly and the absolute errors and gate margins under each
    reference stay visible: a classification alone is not evidence (SPEC.md 8).
    """

    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        if not all(r in daily for r in REFERENCES):
            continue
        audited = daily[REFERENCE_AUDITED]
        r3 = daily[REFERENCE_R3]
        for cid in CANDIDATE_IDS:
            column = f"cand_{cid}"
            if column not in daily:
                continue
            common = pd.DataFrame(
                {"a": audited, "b": r3, "c": daily[column]}
            ).dropna()
            excluded_dates = int(len(daily) - len(common))
            years = sorted(
                set(m2.matched_complete_years(audited, daily[column]))
                & set(m2.matched_complete_years(r3, daily[column]))
            )
            years_audited_only = sorted(
                set(m2.matched_complete_years(audited, daily[column])) - set(years)
            )
            years_r3_only = sorted(
                set(m2.matched_complete_years(r3, daily[column])) - set(years)
            )
            base = {
                "site": result.site,
                "window": result.window,
                "candidate": column,
                "candidate_id": cid,
                "humidity_method": CANDIDATES[cid][0],
                "wind_treatment": CANDIDATES[cid][1],
                "n_common_valid_days": int(len(common)),
                "n_excluded_days": excluded_dates,
                "n_common_complete_years": len(years),
                "common_years": ",".join(str(y) for y in years),
                "years_excluded_audited_only": ",".join(str(y) for y in years_audited_only),
                "years_excluded_R3_only": ",".join(str(y) for y in years_r3_only),
            }

            # Daily gate on common valid dates, under each reference.
            daily_rows = {}
            for label, ref_column in (("audited", "a"), ("R3", "b")):
                scored = m1.score_pair(
                    common[ref_column],
                    common["c"],
                    site=result.site,
                    window=result.window,
                    season="ALL",
                    reference_name=label,
                    candidate_name=column,
                    kind="robustness",
                )
                daily_rows[label] = scored
            rows.append(
                {
                    **base,
                    "statistic": "daily_gate",
                    "threshold_c": np.nan,
                    "gate_audited": daily_rows["audited"].get("daily_gate", CLASS_NOT_EVALUATED),
                    "gate_R3": daily_rows["R3"].get("daily_gate", CLASS_NOT_EVALUATED),
                    "robustness": classify(
                        str(daily_rows["audited"].get("daily_gate")),
                        str(daily_rows["R3"].get("daily_gate")),
                    ),
                    "median_abs_error_audited_c": daily_rows["audited"].get("median_abs_error_c"),
                    "rmse_audited_c": daily_rows["audited"].get("rmse_c"),
                    "median_abs_error_R3_c": daily_rows["R3"].get("median_abs_error_c"),
                    "rmse_R3_c": daily_rows["R3"].get("rmse_c"),
                }
            )

            if not years:
                for threshold in m1.THRESHOLDS_C:
                    rows.append(
                        {
                            **base,
                            "statistic": f"days_ge_{threshold:g}_per_year",
                            "threshold_c": threshold,
                            "gate_audited": CLASS_NOT_EVALUATED,
                            "gate_R3": CLASS_NOT_EVALUATED,
                            "robustness": CLASS_NOT_EVALUATED,
                        }
                    )
                continue
            counts_audited = m1.annual_counts(audited, years)
            counts_r3 = m1.annual_counts(r3, years)
            counts_cand = m1.annual_counts(daily[column], years)
            for threshold in m1.THRESHOLDS_C:
                col = f"days_ge_{threshold:g}"
                ref_a = float(counts_audited[col].mean())
                ref_b = float(counts_r3[col].mean())
                cand = float(counts_cand[col].mean())
                verdict_a, tol_a = m1.count_gate(ref_a, cand)
                verdict_b, tol_b = m1.count_gate(ref_b, cand)
                rows.append(
                    {
                        **base,
                        "statistic": f"days_ge_{threshold:g}_per_year",
                        "threshold_c": threshold,
                        "candidate_per_year": cand,
                        "reference_per_year_audited": ref_a,
                        "reference_per_year_R3": ref_b,
                        "signed_error_audited": cand - ref_a,
                        "signed_error_R3": cand - ref_b,
                        "gate_tolerance_audited": tol_a,
                        "gate_tolerance_R3": tol_b,
                        "gate_margin_audited": (
                            tol_a - abs(cand - ref_a) if np.isfinite(tol_a) else np.nan
                        ),
                        "gate_margin_R3": (
                            tol_b - abs(cand - ref_b) if np.isfinite(tol_b) else np.nan
                        ),
                        "gate_audited": verdict_a,
                        "gate_R3": verdict_b,
                        "robustness": classify(verdict_a, verdict_b),
                    }
                )
    return pd.DataFrame(rows)


# ==========================================================================
# Paired year-block uncertainty on old versus new (SPEC.md 9.3)
# ==========================================================================


def paired_year_block_daily(
    results: Sequence[HumidityResult],
    *,
    old_id: str,
    new_id: str,
    reference: str = REFERENCE_AUDITED,
    seed: int = BOOTSTRAP_SEED,
    draws: int = BOOTSTRAP_DRAWS,
) -> pd.DataFrame:
    """Paired interval for ``new - old`` in the daily statistics, resampling whole years.

    Both candidates are evaluated on the SAME drawn years, so the interval is on the paired
    difference and not on two independently resampled quantities. Reference-method uncertainty
    (SPEC.md 8) is reported separately and never combined with this.
    """

    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        needed = [reference, f"cand_{old_id}", f"cand_{new_id}"]
        if not all(c in daily for c in needed):
            continue
        paired = daily[needed].dropna()
        paired.columns = ["ref", "old", "new"]
        if paired.empty:
            continue
        years = pd.DatetimeIndex(paired.index).year
        blocks = [paired[years == y] for y in sorted(set(years))]
        if len(blocks) < 2:
            continue
        rng = np.random.default_rng(seed)

        def statistic(block: pd.DataFrame, column: str, name: str) -> float:
            d = block[column] - block["ref"]
            if name == "bias_c":
                return float(d.mean())
            if name == "rmse_c":
                return float(np.sqrt((d**2).mean()))
            if name == "median_abs_error_c":
                return float(d.abs().median())
            raise ValueError(f"Unsupported statistic: {name!r}")

        samples: dict[str, np.ndarray] = {
            name: np.empty(draws, dtype=float)
            for name in ("bias_c", "median_abs_error_c", "rmse_c")
        }
        n = len(blocks)
        for i in range(draws):
            pick = pd.concat([blocks[j] for j in rng.integers(0, n, n)])
            for name, store in samples.items():
                store[i] = statistic(pick, "new", name) - statistic(pick, "old", name)
        for name, store in samples.items():
            rows.append(
                {
                    "site": result.site,
                    "window": result.window,
                    "reference": reference,
                    "comparison": f"{new_id} minus {old_id}",
                    "statistic": name,
                    "point_estimate": statistic(paired, "new", name)
                    - statistic(paired, "old", name),
                    "ci_lo": float(np.percentile(store, 2.5)),
                    "ci_hi": float(np.percentile(store, 97.5)),
                    "excludes_zero": bool(
                        np.percentile(store, 2.5) > 0.0 or np.percentile(store, 97.5) < 0.0
                    ),
                    "draws": draws,
                    "seed": seed,
                    "method": "paired year-block bootstrap, whole years with replacement",
                    "scope": "temporal sampling only; reference-method uncertainty is separate",
                }
            )
    return pd.DataFrame(rows)


def paired_year_block_counts(
    results: Sequence[HumidityResult],
    *,
    old_id: str,
    new_id: str,
    reference: str = REFERENCE_AUDITED,
    seed: int = BOOTSTRAP_SEED,
    draws: int = BOOTSTRAP_DRAWS,
) -> pd.DataFrame:
    """Paired interval for the change in mean annual count error, resampling complete years."""

    rows: list[dict[str, object]] = []
    for result in results:
        daily = result.daily
        needed = [reference, f"cand_{old_id}", f"cand_{new_id}"]
        if not all(c in daily for c in needed):
            continue
        years = sorted(
            set(m2.matched_complete_years(daily[reference], daily[f"cand_{old_id}"]))
            & set(m2.matched_complete_years(daily[reference], daily[f"cand_{new_id}"]))
        )
        if len(years) < 2:
            continue
        ref = m1.annual_counts(daily[reference], years)
        old = m1.annual_counts(daily[f"cand_{old_id}"], years)
        new = m1.annual_counts(daily[f"cand_{new_id}"], years)
        rng = np.random.default_rng(seed)
        picks = rng.integers(0, len(years), (draws, len(years)))
        for threshold in m1.THRESHOLDS_C:
            col = f"days_ge_{threshold:g}"
            e_old = (old[col] - ref[col]).to_numpy(dtype=float)
            e_new = (new[col] - ref[col]).to_numpy(dtype=float)
            point = float(np.abs(np.mean(e_new)) - np.abs(np.mean(e_old)))
            draws_arr = np.abs(e_new[picks].mean(axis=1)) - np.abs(e_old[picks].mean(axis=1))
            rows.append(
                {
                    "site": result.site,
                    "window": result.window,
                    "reference": reference,
                    "comparison": f"{new_id} minus {old_id}",
                    "statistic": f"abs_mean_count_error_{threshold:g}",
                    "n_complete_years": len(years),
                    "reference_per_year": float(ref[col].mean()),
                    "point_estimate": point,
                    "ci_lo": float(np.percentile(draws_arr, 2.5)),
                    "ci_hi": float(np.percentile(draws_arr, 97.5)),
                    "excludes_zero": bool(
                        np.percentile(draws_arr, 2.5) > 0.0
                        or np.percentile(draws_arr, 97.5) < 0.0
                    ),
                    "draws": draws,
                    "seed": seed,
                    "method": "paired year-block bootstrap on complete years",
                    "scope": "temporal sampling only; reference-method uncertainty is separate",
                }
            )
    return pd.DataFrame(rows)


# ==========================================================================
# Selection rule (SPEC.md 11)
# ==========================================================================


def apply_selection_rule(
    scores: pd.DataFrame,
    robustness: pd.DataFrame,
    per_year: pd.DataFrame,
    consistency: pd.DataFrame,
) -> dict[str, object]:
    """Rank the four candidates by the predeclared rule. Absent evidence stays absent."""

    ranked: list[dict[str, object]] = []
    for cid in CANDIDATE_IDS:
        column = f"cand_{cid}"
        daily = scores[
            (scores["candidate"] == column)
            & (scores["season"] == "ALL")
            & (scores["reference"] == REFERENCE_AUDITED)
        ]
        rob = robustness[robustness["candidate"] == column]
        counts = rob[rob["statistic"] != "daily_gate"]
        gated = counts[counts["robustness"] != CLASS_NOT_EVALUATED]
        years_out = per_year[
            (per_year["candidate"] == column)
            & (per_year["reference"] == REFERENCE_AUDITED)
            & (per_year["window_mean_gate"].isin(["PASS", "FAIL"]))
        ]
        cons = consistency[consistency["candidate_id"] == cid]
        if daily.empty:
            ranked.append({"candidate": cid, "status": CLASS_NOT_EVALUATED})
            continue
        entry: dict[str, object] = {
            "candidate": cid,
            "name": CANDIDATE_NAMES[cid],
            "humidity_method": CANDIDATES[cid][0],
            "wind_treatment": CANDIDATES[cid][1],
            "daily_pairs": int(len(daily)),
            "daily_pairs_passing": int((daily["daily_gate"] == "PASS").sum()),
            "daily_gate": "PASS" if (daily["daily_gate"] == "PASS").all() else "FAIL",
            "worst_median_abs_error_c": float(daily["median_abs_error_c"].max()),
            "worst_rmse_c": float(daily["rmse_c"].max()),
            "count_pairs_classified": int(len(gated)),
            "count_pairs_robust_pass": int((gated["robustness"] == CLASS_ROBUST_PASS).sum()),
            "count_pairs_robust_fail": int((gated["robustness"] == CLASS_ROBUST_FAIL).sum()),
            "count_pairs_reference_sensitive": int(
                (gated["robustness"] == CLASS_REFERENCE_SENSITIVE).sum()
            ),
            "count_pairs_not_evaluated": int(
                (counts["robustness"] == CLASS_NOT_EVALUATED).sum()
            ),
            "worst_count_error_per_year_audited": (
                float(gated["signed_error_audited"].abs().max()) if not gated.empty else np.nan
            ),
            "n_individual_years_outside_tolerance": (
                int(years_out["outside_tolerance"].sum()) if not years_out.empty else -1
            ),
        }
        if CANDIDATES[cid][0] == HUMIDITY_NEW:
            entry["max_abs_residual_pp"] = (
                float(cons["max_abs_residual_pp"].max()) if not cons.empty else np.nan
            )
            entry["n_solver_failures"] = (
                int(cons["n_solver_failures"].sum()) if not cons.empty else -1
            )
            entry["input_consistency"] = (
                "PASS"
                if not cons.empty
                and float(cons["max_abs_residual_pp"].max()) <= RH_RESIDUAL_TOL_PP
                and int(cons["n_solver_failures"].sum()) == 0
                else "FAIL"
            )
        else:
            entry["input_consistency"] = "N/A (existing humidity, not input-consistent by design)"
        ranked.append(entry)

    evaluated = [r for r in ranked if r.get("status") != CLASS_NOT_EVALUATED]
    survivors = [r for r in evaluated if r["daily_gate"] == "PASS"]
    eliminated = [
        {"candidate": r["candidate"], "reason": "failed the daily gate"}
        for r in evaluated
        if r["daily_gate"] != "PASS"
    ]
    for r in evaluated:
        if r.get("input_consistency") == "FAIL":
            eliminated.append(
                {
                    "candidate": r["candidate"],
                    "reason": "did not satisfy the input-consistency constraint",
                }
            )
    consistent_ids = {
        r["candidate"] for r in evaluated if r.get("input_consistency") != "FAIL"
    }
    survivors = [r for r in survivors if r["candidate"] in consistent_ids]

    def rank_key(entry: Mapping[str, object]) -> tuple:
        return (
            int(entry["count_pairs_robust_fail"]),
            int(entry["count_pairs_robust_fail"]) + int(entry["count_pairs_reference_sensitive"]),
            float(entry["worst_count_error_per_year_audited"]),
            int(entry["n_individual_years_outside_tolerance"]),
            CANDIDATE_IDS.index(str(entry["candidate"])),
        )

    survivors_sorted = sorted(survivors, key=rank_key)

    # Step 4: the no-material-deterioration screen, old humidity -> new humidity.
    deterioration = deterioration_screen(robustness)

    selected = survivors_sorted[0]["candidate"] if survivors_sorted else None
    if selected in NEW_HUMIDITY_IDS and deterioration["n_pairs_lost"] > 0:
        # A ROBUST PASS turned ROBUST FAIL is material deterioration; the preference is blocked
        # and the old-humidity counterpart is selected instead (SPEC.md 11 step 4 and step 7).
        fallback = {"C": "A", "D": "B"}[str(selected)]
        alternative = next((r for r in survivors_sorted if r["candidate"] == fallback), None)
        if alternative is not None:
            selected = fallback
            blocked = True
        else:  # pragma: no cover - the counterpart always survives if the new one does
            blocked = True
    else:
        blocked = False

    passing_all = [
        r
        for r in survivors_sorted
        if r["count_pairs_robust_fail"] == 0 and r["count_pairs_reference_sensitive"] == 0
    ]
    if not survivors_sorted:
        status = "NONE (no candidate passed the daily gate)"
    elif passing_all and selected == passing_all[0]["candidate"]:
        status = "SELECTED (daily gate and every classified count pair pass under both references)"
    else:
        status = "BEST DEVELOPMENT CANDIDATE (count gate not passed under both references)"

    return {
        "rule": "SPEC.md 11, predeclared",
        "selected": selected,
        "status": status,
        "new_humidity_preference_blocked_by_deterioration": blocked,
        "deterioration_screen": deterioration,
        "ranking": survivors_sorted,
        "eliminated": eliminated,
        "all_candidates": ranked,
    }


def deterioration_screen(robustness: pd.DataFrame) -> dict[str, object]:
    """Which ROBUST PASS pairs the new humidity turns into ROBUST FAIL (SPEC.md 11 step 4)."""

    out: dict[str, object] = {
        "definition": "a site-window-threshold pair that is ROBUST PASS under the old humidity "
        "and ROBUST FAIL under the new one, at the same wind treatment",
        "pairs_lost": [],
        "pairs_gained": [],
    }
    if robustness.empty:
        out["n_pairs_lost"] = 0
        out["n_pairs_gained"] = 0
        return out
    counts = robustness[robustness["statistic"] != "daily_gate"]
    keys = ["site", "window", "statistic", "wind_treatment"]
    for (site, window, statistic, wind), block in counts.groupby(keys, dropna=False):
        old = block[block["humidity_method"] == HUMIDITY_OLD]
        new = block[block["humidity_method"] == HUMIDITY_NEW]
        if old.empty or new.empty:
            continue
        old_class = str(old["robustness"].iloc[0])
        new_class = str(new["robustness"].iloc[0])
        record = {
            "site": site,
            "window": window,
            "statistic": statistic,
            "wind_treatment": wind,
            "old_humidity": old_class,
            "new_humidity": new_class,
        }
        if old_class == CLASS_ROBUST_PASS and new_class == CLASS_ROBUST_FAIL:
            out["pairs_lost"].append(record)
        elif old_class == CLASS_ROBUST_FAIL and new_class == CLASS_ROBUST_PASS:
            out["pairs_gained"].append(record)
    out["n_pairs_lost"] = len(out["pairs_lost"])
    out["n_pairs_gained"] = len(out["pairs_gained"])
    return out


# ==========================================================================
# Limited NEX transfer check (SPEC.md 10)
# ==========================================================================


def nex_candidate_daily_max(
    sample: pd.DataFrame, sites: Sequence[Site], candidate: str
) -> pd.DataFrame:
    """Run one candidate on cached NEX daily inputs. A DISTRIBUTION comparison only.

    NEX carries no weather for a particular ERA5 date, so no same-date difference, RMSE or
    correlation against ERA5 is computed anywhere in this function (SPEC.md 10).
    """

    if candidate not in CANDIDATES:
        raise ValueError(f"Unknown candidate: {candidate!r}")
    if sample.empty:
        return pd.DataFrame()
    humidity_key, wind_key = CANDIDATES[candidate]
    by_name = {s.name: s for s in sites}
    frames: list[pd.DataFrame] = []
    for (model, scenario, site_name), block in sample.groupby(["model", "scenario", "site"]):
        site = by_name[site_name]
        static = m1.SiteStatic.from_site(site)
        daily = block[list(m1.REQUIRED_NEX_VARIABLES)].copy()
        daily.index = pd.DatetimeIndex(daily.index).normalize()
        daily = daily[~daily.index.duplicated(keep="first")].sort_index()
        inputs = m1.DailyInputs(daily)

        times = pd.DatetimeIndex(
            np.concatenate(
                [pd.date_range(day, periods=24, freq="h").to_numpy() for day in daily.index]
            )
        )
        local_day = pd.DatetimeIndex(times.normalize()).to_numpy()
        cz_mid = cos_solar_zenith(site.lat, times + m1.RADIATION_MIDPOINT_OFFSET, site.lon)
        base = m1.reconstruct_hourly(
            times, local_day, inputs, static, m1.CANDIDATE_PARAMS["C1"], cossza=cz_mid
        )
        t_recon = base["t_c"].to_numpy(dtype=float)
        if humidity_key == HUMIDITY_OLD:
            rh = base["rh_pct"].to_numpy(dtype=float)
        else:
            rh, _, _ = reconstruct_humidity_input_consistent(t_recon, daily, local_day)
        wind = (
            base["wind_10m_ms"].to_numpy(dtype=float)
            if wind_key == "C1"
            else m2.wind_w1_dtr_ms(daily, local_day, cz_mid)
        )
        wbgt = m1.liljegren_wbgt_c(
            t_recon,
            rh,
            base["pressure_hpa"].to_numpy(dtype=float),
            wind,
            base["ssrd_w_m2"].to_numpy(dtype=float),
            base["fdir_frac"].to_numpy(dtype=float),
            cz_mid,
        )
        frame = m1.daily_max(wbgt, local_day, daily.index).to_frame("wbgt_daily_max_c")
        frame.insert(0, "model", model)
        frame.insert(1, "scenario", scenario)
        frame.insert(2, "site", site_name)
        frame.insert(3, "candidate", candidate)
        frames.append(frame)
    out = pd.concat(frames)
    out.index.name = "nex_day"
    return out


def nex_transfer_table(
    nex_daily: pd.DataFrame,
    era5_results: Sequence[HumidityResult],
    years: Sequence[int],
) -> pd.DataFrame:
    """Per-model distribution and annual-count rows for all four candidates on NEX.

    Reported per model before any ensemble summary, with the old-humidity counterpart's values
    carried alongside so the change attributable to the humidity formulation is visible.
    """

    if nex_daily.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    era5_by_site: dict[str, pd.Series] = {}
    for result in era5_results:
        ref = result.daily[REFERENCE_AUDITED]
        ref = ref[pd.DatetimeIndex(ref.index).year.isin(list(years))]
        era5_by_site[result.site] = (
            pd.concat([era5_by_site[result.site], ref]).sort_index()
            if result.site in era5_by_site
            else ref
        )

    for site, ref in era5_by_site.items():
        ref_years = m1.complete_years(ref)
        reference_row = m2._distribution_row(ref, ref_years)
        rows.append(
            {
                "site": site,
                "source": "ERA5 ref_audited (IST civil day)",
                "model": "",
                "candidate": "",
                "humidity_method": "",
                "wind_treatment": "",
                "years_requested": f"{min(years)}-{max(years)}",
                **reference_row,
            }
        )
        block_site = nex_daily[nex_daily["site"] == site]
        for (model, candidate), block in block_site.groupby(["model", "candidate"]):
            series = block["wbgt_daily_max_c"]
            nex_years = m1.complete_years(series)
            row = m2._distribution_row(series, nex_years)
            entry: dict[str, object] = {
                "site": site,
                "source": "NEX candidate (inferred NEX day)",
                "model": model,
                "candidate": candidate,
                "humidity_method": CANDIDATES[str(candidate)][0],
                "wind_treatment": CANDIDATES[str(candidate)][1],
                "years_requested": f"{min(years)}-{max(years)}",
                "common_complete_years": len(set(nex_years) & set(ref_years)),
                **row,
            }
            for threshold in m1.THRESHOLDS_C:
                key = f"days_ge_{threshold:g}_per_year"
                ref_value, nex_value = reference_row[key], row[key]
                entry[f"signed_error_{threshold:g}"] = nex_value - ref_value
                entry[f"relative_error_{threshold:g}"] = (
                    (nex_value - ref_value) / ref_value if ref_value > 0 else np.nan
                )
            entry["annual_mean_signed_error_c"] = (
                row["annual_mean_c"] - reference_row["annual_mean_c"]
            )
            rows.append(entry)
    table = pd.DataFrame(rows)

    # Change relative to the old-humidity counterpart at the same wind treatment.
    counterpart = {"C": "A", "D": "B"}
    if not table.empty:
        keyed = table[table["candidate"] != ""].set_index(["site", "model", "candidate"])
        deltas = []
        for (site, model, candidate), row in keyed.iterrows():
            if candidate not in counterpart:
                continue
            old_key = (site, model, counterpart[candidate])
            if old_key not in keyed.index:
                continue
            old = keyed.loc[old_key]
            record = {
                "site": site,
                "model": model,
                "candidate": candidate,
                "old_counterpart": counterpart[candidate],
                "wind_treatment": CANDIDATES[str(candidate)][1],
                "annual_mean_change_c": float(row["annual_mean_c"] - old["annual_mean_c"]),
                "q99_change_c": float(row["q99_c"] - old["q99_c"]),
            }
            for threshold in m1.THRESHOLDS_C:
                key = f"days_ge_{threshold:g}_per_year"
                record[f"count_change_{threshold:g}"] = float(row[key] - old[key])
                record[f"abs_error_change_{threshold:g}"] = float(
                    abs(row[f"signed_error_{threshold:g}"])
                    - abs(old[f"signed_error_{threshold:g}"])
                )
            deltas.append(record)
        if deltas:
            table = pd.concat(
                [table, pd.DataFrame(deltas).assign(source="change versus old humidity")],
                ignore_index=True,
            )
    return table


# ==========================================================================
# Write guards, manifest, self-test
# ==========================================================================


def _guard_write_target(path: Path, label: str) -> Path:
    resolved = path.resolve()
    lowered = str(resolved).replace("\\", "/").lower()
    for fragment in (*m1.FORBIDDEN_WRITE_FRAGMENTS, *EXTRA_FORBIDDEN_FRAGMENTS):
        if f"/{fragment}" in lowered or lowered.endswith(f"/{fragment}"):
            raise SystemExit(
                f"Refusing to use {label} {resolved}: it resolves inside '{fragment}', which "
                "this milestone must not write to (SPEC.md 13)."
            )
    for protected in PROTECTED_DIRS:
        target = Path(protected).resolve()
        if resolved == target or target in resolved.parents:
            raise SystemExit(
                f"Refusing to use {label} {resolved}: {protected} is immutable here "
                "(SPEC.md 13)."
            )
    return resolved


def run_manifest(args: argparse.Namespace, extra: Mapping[str, object]) -> dict[str, object]:
    """Git snapshot, package versions, frozen settings, input identities and the exact command."""

    import subprocess

    def git(*cmd: str) -> str:
        try:
            return subprocess.run(
                ["git", *cmd], capture_output=True, text=True, check=True, timeout=30
            ).stdout.strip()
        except Exception:  # pragma: no cover - git absent or detached
            return "unavailable"

    return {
        "tool": "tools/diagnostics/wbgt_outdoor_humidity.py",
        "spec": "docs/diagnostics/wbgt_outdoor_humidity/SPEC.md",
        "milestone_1_spec": "docs/diagnostics/wbgt_outdoor_feasibility/SPEC.md",
        "milestone_2_spec": "docs/diagnostics/wbgt_outdoor_selection/SPEC.md",
        "generated_utc": pd.Timestamp.utcnow().isoformat(),
        "git_branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "git_sha": git("rev-parse", "--short", "HEAD"),
        "git_dirty": bool(git("status", "--porcelain")),
        "command": " ".join(sys.argv),
        "python": sys.version.split()[0],
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "thermofeel": thermofeel_version(),
        "inputs": {
            "era5_hourly_cache": str(args.era5_cache),
            "nex_sample_cache": str(args.nex_sample or "(milestone 2 work dir)"),
            "milestone2_daily_series": str(MILESTONE2_DAILY_SERIES),
        },
        "candidates": {
            cid: {
                "name": CANDIDATE_NAMES[cid],
                "humidity": CANDIDATES[cid][0],
                "wind": CANDIDATES[cid][1],
                "signature": candidate_signature(cid),
            }
            for cid in CANDIDATE_IDS
        },
        "humidity_solver": {
            "residual_tolerance_pp": RH_RESIDUAL_TOL_PP,
            "iteration_limit": SOLVER_MAX_ITER,
            "bracket": "[0, max_h es(T_h)]",
            "analytic_branch": "e = (hurs/100) / mean_h[1/es(T_h)] where no hour saturates",
        },
        "signatures": signature_bundle(),
        "w1_inherited": {
            "slope_per_c": m2.WIND_DTR_SLOPE_PER_C,
            "amplitude_bounds": [m2.WIND_DTR_AMP_MIN, m2.WIND_DTR_AMP_MAX],
            "status": "inherited unchanged from milestone 2; a declared assumption, not a "
            "published wind reconstruction",
        },
        "gates": {
            "source": "imported from wbgt_outdoor_feasibility, unchanged and not widened",
            "median_abs_error_c": m1.GATE_MEDIAN_ABS_ERROR_C,
            "rmse_c": m1.GATE_RMSE_C,
            "count_min_ref_per_year": m1.COUNT_GATE_MIN_REF_PER_YEAR,
            "count_rel_tol": m1.COUNT_GATE_REL_TOL,
            "count_abs_floor_per_year": m1.COUNT_GATE_ABS_FLOOR_PER_YEAR,
        },
        "shade": "UNTOUCHED: no production shade computation, method signature, cache, stage or "
        "release configuration was read for writing or modified",
        **dict(extra),
    }


def _synthetic_es(n_days: int, n_hours: int = 24, spread: float = 8.0) -> np.ndarray:
    hours = np.arange(n_hours)
    shape = np.sin(2 * np.pi * (hours - 3) / n_hours)
    temps = 25.0 + spread * shape[None, :] + np.linspace(-2.0, 2.0, n_days)[:, None]
    return m1.saturation_pressure_hpa(temps)


def self_test() -> int:
    """Contract checks that need no cache, no network and no NEX tree."""

    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'ok  ' if ok else 'FAIL'} {name}" + (f" -- {detail}" if detail else ""))
        if not ok:
            failures.append(name)

    print("wbgt_outdoor_humidity self-test (SPEC.md)")

    # 1. Analytic branch at constant temperature.
    es = np.full((1, 24), m1.saturation_pressure_hpa(30.0))
    sol = solve_daily_vapour_pressure_hpa(es, np.array([60.0]))
    check(
        "constant-temperature analytic solution",
        bool(sol.valid[0]) and abs(sol.e_star_hpa[0] - 0.6 * es[0, 0]) < 1e-9,
        f"e*={sol.e_star_hpa[0]:.6f}",
    )

    # 2. Variable temperature, unsaturated.
    es = _synthetic_es(5)
    sol = solve_daily_vapour_pressure_hpa(es, np.full(5, 40.0))
    check(
        "variable-temperature unsaturated analytic solution",
        bool(sol.valid.all())
        and bool(np.all(np.abs(sol.residual_pp) <= RH_RESIDUAL_TOL_PP))
        and all("analytic" in p for p in sol.path),
        f"max residual {np.max(np.abs(sol.residual_pp)):.2e} pp",
    )

    # 3. Saturation-clipped root solve.
    sol = solve_daily_vapour_pressure_hpa(es, np.full(5, 97.0))
    check(
        "saturation-clipped root solution",
        bool(sol.valid.all())
        and bool(np.all(np.abs(sol.residual_pp) <= RH_RESIDUAL_TOL_PP))
        and any("bisection" in p for p in sol.path),
        f"paths {sorted(set(sol.path))}",
    )

    # 4. Endpoints.
    sol = solve_daily_vapour_pressure_hpa(es[:2], np.array([0.0, 100.0]))
    check(
        "0 and 100 percent endpoints",
        bool(sol.valid.all())
        and sol.e_star_hpa[0] == 0.0
        and abs(sol.e_star_hpa[1] - es[1].max()) < 1e-12,
        f"e*={sol.e_star_hpa}",
    )

    # 5. Out-of-range RH is invalid, never coerced.
    sol = solve_daily_vapour_pressure_hpa(es[:2], np.array([-1.0, 101.0]))
    check(
        "out-of-range RH is invalid, not coerced",
        (not sol.valid.any()) and all("outside [0, 100]" in r for r in sol.reason),
    )

    # 6. Monotonicity of the constraint function.
    grid = np.linspace(0.0, float(es[0].max()) * 1.2, 40)
    values = np.array([constraint_mean_rh_pct(np.array([e]), es[:1])[0] for e in grid])
    check("constraint function is non-decreasing", bool(np.all(np.diff(values) >= -1e-12)))

    # 7. Empty and all-NaN input.
    empty = solve_daily_vapour_pressure_hpa(np.empty((0, 24)), np.empty(0))
    nan_sol = solve_daily_vapour_pressure_hpa(np.full((2, 24), np.nan), np.array([50.0, 50.0]))
    check(
        "empty and all-NaN inputs",
        empty.e_star_hpa.size == 0
        and (not nan_sol.valid.any())
        and all("non-finite" in r for r in nan_sol.reason),
    )

    # 8. Signatures distinguish humidity and wind.
    signatures = {cid: candidate_signature(cid) for cid in CANDIDATE_IDS}
    check(
        "candidate signatures are distinct and name the method",
        len(set(signatures.values())) == 4
        and HUMIDITY_NEW in signatures["C"]
        and HUMIDITY_NEW not in signatures["A"]
        and "wind-dtr" in signatures["B"]
        and "wind-constant" in signatures["A"],
    )

    # 9. Robustness classification.
    check(
        "robustness classification",
        classify("PASS", "PASS") == CLASS_ROBUST_PASS
        and classify("FAIL", "FAIL") == CLASS_ROBUST_FAIL
        and classify("PASS", "FAIL") == CLASS_REFERENCE_SENSITIVE
        and classify("PASS", "NOT GATED (rare event)") == CLASS_NOT_EVALUATED,
    )

    # 10. Gates and references are imported, not restated.
    check(
        "gates and references are imported from milestone 1/2",
        signature_bundle()["gates"]["median_abs_error_c"] == m1.GATE_MEDIAN_ABS_ERROR_C
        and signature_bundle()["references"][REFERENCE_R3] == m2.REFERENCE_SIGNATURE_MIDPOINT,
    )

    # 11. Write guards.
    for bad in ("irt_data/x", "docs/diagnostics/wbgt_outdoor_selection", "scratch/wbgt_shade_national"):
        try:
            _guard_write_target(Path(bad), "test")
            check(f"write guard rejects {bad}", False)
        except SystemExit:
            check(f"write guard rejects {bad}", True)

    print(f"\n{len(failures)} failure(s)" if failures else "\nall checks passed")
    return 1 if failures else 0


# ==========================================================================
# CLI
# ==========================================================================

STAGES = ("windows", "nex", "residuals", "all")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.diagnostics.wbgt_outdoor_humidity",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--stage", choices=STAGES, default="all")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument("--era5-cache", type=Path, default=m1.DEFAULT_ERA5_CACHE)
    parser.add_argument(
        "--nex-sample",
        type=Path,
        default=None,
        help="cached NEX daily sample parquet; defaults to milestone 2's cached sample",
    )
    parser.add_argument("--sites", default="all")
    parser.add_argument(
        "--windows", default=f"{m1.DEFAULT_PRIMARY_WINDOW};{m1.DEFAULT_CONTINUITY_WINDOW}"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="bounded at 4; default 1 because the national shade rebuild owns the disk",
    )
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--resume", action="store_true", help="reuse signature-matched caches")
    group.add_argument("--overwrite", action="store_true", help="recompute every per-site parquet")
    return parser


def select_sites(spec: str) -> tuple[Site, ...]:
    if spec.strip().lower() == "all":
        return tuple(SITES)
    wanted = [s.strip().lower() for s in spec.split(",") if s.strip()]
    chosen = tuple(s for s in SITES if s.name.lower() in wanted)
    if len(chosen) != len(wanted):
        raise SystemExit(f"Unknown site(s) in {spec!r}; known: {[s.name for s in SITES]}")
    return chosen


def _cache_path(work_dir: Path, site: str, window: str) -> Path:
    return work_dir / f"{site.lower()}_{window}.parquet"


def _json_default(value: object) -> object:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (pd.DataFrame, pd.Series)):
        return "omitted"
    return str(value)


def _serialisable_diagnostics(diagnostics: Mapping[str, object]) -> dict[str, object]:
    """Diagnostics minus the frames, which are persisted as parquet beside the daily series."""

    out = dict(diagnostics)
    out.pop("humidity_per_day", None)
    out.pop("hourly_humidity", None)
    peaks = out.pop("peak_hour_frame", None)
    if isinstance(peaks, pd.DataFrame):
        out["peak_hour_records"] = peaks.to_dict(orient="list")
    return out


def _run_one(payload: tuple) -> tuple[HumidityResult]:
    site, years, cache_dir = payload
    return (run_site_window(site, years, cache_dir=cache_dir, verbose=True),)


def _store(result: HumidityResult, work_dir: Path) -> None:
    path = _cache_path(work_dir, result.site, result.window)
    result.daily.to_parquet(path)
    per_day = result.diagnostics.get("humidity_per_day")
    if isinstance(per_day, pd.DataFrame):
        per_day.to_parquet(path.with_name(path.stem + "_humidity_per_day.parquet"))
    hourly = result.diagnostics.get("hourly_humidity")
    if isinstance(hourly, pd.DataFrame):
        hourly.to_parquet(path.with_name(path.stem + "_hourly_humidity.parquet"))
    path.with_suffix(".json").write_text(
        json.dumps(_serialisable_diagnostics(result.diagnostics), indent=2, default=_json_default)
    )


def _load(site: str, window: str, work_dir: Path) -> HumidityResult | None:
    """Reload a cached result, but only if its FULL signature bundle matches (SPEC.md 5)."""

    path = _cache_path(work_dir, site, window)
    meta = path.with_suffix(".json")
    if not path.exists() or not meta.exists():
        return None
    diagnostics = json.loads(meta.read_text())
    if diagnostics.get("signatures") != signature_bundle(
        str(diagnostics.get("day_convention", "ist"))
    ):
        print(f"    {site} {window}: cache signature mismatch, recomputing")
        return None
    daily = pd.read_parquet(path)
    for key, suffix in (
        ("humidity_per_day", "_humidity_per_day.parquet"),
        ("hourly_humidity", "_hourly_humidity.parquet"),
    ):
        side = path.with_name(path.stem + suffix)
        if side.exists():
            diagnostics[key] = pd.read_parquet(side)
    return HumidityResult(site, window, daily, diagnostics)


def consistency_table(results: Sequence[HumidityResult]) -> pd.DataFrame:
    """One row per site/window/new-humidity candidate: residuals, failures, saturation, e* shift."""

    rows: list[dict[str, object]] = []
    for result in results:
        summary = result.diagnostics.get("humidity_consistency")
        if not isinstance(summary, Mapping):
            continue
        for cid in NEW_HUMIDITY_IDS:
            rows.append(
                {
                    "site": result.site,
                    "window": result.window,
                    "candidate_id": cid,
                    "candidate": f"cand_{cid}",
                    "wind_treatment": CANDIDATES[cid][1],
                    **{
                        k: (json.dumps(v) if isinstance(v, Mapping) else v)
                        for k, v in summary.items()
                    },
                }
            )
    return pd.DataFrame(rows)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.self_test:
        return self_test()

    out_dir = _guard_write_target(args.out_dir, "out-dir")
    work_dir = _guard_write_target(args.work_dir, "work-dir")
    sites = select_sites(args.sites)
    windows = m2.parse_windows(args.windows)
    window_labels = [m2._window_label(w) for w in windows]
    workers = max(1, min(4, int(args.workers)))

    print("Outdoor-WBGT humidity consistency, milestone 3")
    print("  spec        docs/diagnostics/wbgt_outdoor_humidity/SPEC.md")
    print(f"  stage       {args.stage}")
    print(f"  out-dir     {out_dir}")
    print(f"  work-dir    {work_dir}")
    print(f"  era5 cache  {args.era5_cache}")
    print(f"  sites       {', '.join(s.name for s in sites)}")
    print(f"  windows     {', '.join(window_labels)}")
    print("  candidates  A/B (existing humidity), C/D (input-consistent humidity)")
    print("  references  ref_audited and R3, both, at every site")
    print(f"  workers     {workers}")

    missing = [
        (s.name, y)
        for s in sites
        for w in windows
        for y in w
        if not (args.era5_cache / f"{s.name.lower()}_{y}.parquet").exists()
    ]
    if missing:
        raise SystemExit(
            f"Hourly ERA5 cache incomplete under {args.era5_cache}: {missing[:6]}"
            f"{' ...' if len(missing) > 6 else ''}. This tool never downloads."
        )
    if args.dry_run:
        print("\nPreflight OK (dry run). Nothing computed, nothing written.")
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    print("\nPreflight OK.\n")

    started = time.monotonic()
    extra: dict[str, object] = {"stages_run": []}
    results: list[HumidityResult] = []

    # ---------------- Stage: daily-RH residual for BOTH methods (SPEC.md 9.1) ----------------
    if args.stage in ("residuals", "all"):
        print("Measuring the daily-RH residual of both humidity methods")
        residual_frames = [
            daily_rh_residual_comparison(site, years, cache_dir=args.era5_cache)
            for years in windows
            for site in sites
        ]
        residuals = pd.concat(residual_frames, ignore_index=True)
        residuals.to_csv(out_dir / "humidity_residual_comparison.csv", index=False)
        print(f"  wrote {out_dir / 'humidity_residual_comparison.csv'} ({len(residuals)} rows)")
        extra["stages_run"].append("residuals")
        extra["daily_rh_residual"] = {
            method: {
                "mean_abs_residual_pp": float(block["mean_abs_residual_pp"].mean()),
                "max_abs_residual_pp": float(block["max_abs_residual_pp"].max()),
                "worst_site_window": (
                    block.loc[block["max_abs_residual_pp"].idxmax(), ["site", "window"]].to_dict()
                ),
            }
            for method, block in residuals.groupby("humidity_method")
        }
        if args.stage == "residuals":
            manifest = run_manifest(args, extra)
            (out_dir / "run_manifest_residuals.json").write_text(
                json.dumps(manifest, indent=2, default=_json_default)
            )
            print(f"\nwrote {out_dir / 'run_manifest_residuals.json'}")
            return 0

    # ---------------- Old-candidate reproduction, BEFORE the full run (SPEC.md 5) ----------------
    print("Reproducing milestone 2's C1/W1 on one site-window before the full run")
    first_site, first_years = sites[0], windows[0]
    first_label = m2._window_label(first_years)
    first = None if args.overwrite else _load(first_site.name, first_label, work_dir)
    if first is None:
        first = run_site_window(first_site, first_years, cache_dir=args.era5_cache)
        _store(first, work_dir)
    else:
        print(f"    {first_site.name} {first_label}: reused signature-matched cache")
    reproduction = reproduce_old_candidates(first)
    (out_dir / "old_candidate_reproduction.json").write_text(
        json.dumps(reproduction, indent=2, default=_json_default)
    )
    print(f"  {reproduction['status']}")
    for cid, entry in (reproduction.get("comparisons") or {}).items():
        if isinstance(entry, Mapping) and "max_abs_difference_c" in entry:
            print(
                f"    {cid} vs {entry['milestone2_column']}: "
                f"max |diff| {entry['max_abs_difference_c']:.3e} C "
                f"over {entry['n_common_days']} common days"
            )
    extra["old_candidate_reproduction"] = reproduction
    results.append(first)

    # ---------------- Stage: the four-candidate comparison ----------------
    print("\nBuilding per-site daily series")
    jobs: list[tuple] = []
    for years in windows:
        label = m2._window_label(years)
        for site in sites:
            if site.name == first_site.name and label == first_label:
                continue
            cached = None if args.overwrite else _load(site.name, label, work_dir)
            if cached is not None:
                results.append(cached)
                print(f"    {site.name} {label}: reused cache ({len(cached.daily)} days)")
                continue
            jobs.append((site, years, args.era5_cache))
    if jobs:
        if workers == 1:
            for payload in jobs:
                (result,) = _run_one(payload)
                _store(result, work_dir)
                results.append(result)
        else:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                for (result,) in pool.map(_run_one, jobs):
                    _store(result, work_dir)
                    results.append(result)
    results.sort(key=lambda r: (r.window, r.site))
    extra["stages_run"].append("windows")

    scores = build_score_table(results)
    annual = build_annual_table(results)
    per_year = build_per_year_table(results)
    per_year_sum = per_year_summary(per_year)
    robustness = build_robustness_table(results)
    consistency = consistency_table(results)
    humidity_hourly = pd.concat(
        [
            r.diagnostics["hourly_humidity"]
            for r in results
            if isinstance(r.diagnostics.get("hourly_humidity"), pd.DataFrame)
        ],
        ignore_index=True,
    )
    per_day_all = pd.concat(
        [
            r.diagnostics["humidity_per_day"].assign(site=r.site, window=r.window)
            for r in results
            if isinstance(r.diagnostics.get("humidity_per_day"), pd.DataFrame)
        ]
    )
    uncertainty = pd.concat(
        [
            paired_year_block_daily(results, old_id=old, new_id=new)
            for old, new in (("A", "C"), ("B", "D"))
        ]
        + [
            paired_year_block_counts(results, old_id=old, new_id=new)
            for old, new in (("A", "C"), ("B", "D"))
        ],
        ignore_index=True,
    )
    wind_rows = [
        {"site": r.site, "window": r.window, "wind": wid, **diag}
        for r in results
        for wid, diag in (r.diagnostics.get("wind") or {}).items()
    ]

    for name, table in (
        ("candidate_scores.csv", scores),
        ("annual_counts.csv", annual),
        ("per_year_count_errors.csv", per_year),
        ("per_year_count_summary.csv", per_year_sum),
        ("robustness_classification.csv", robustness),
        ("humidity_consistency.csv", consistency),
        ("humidity_hourly_diagnostics.csv", humidity_hourly),
        ("uncertainty_paired.csv", uncertainty),
        ("wind_diagnostics.csv", pd.DataFrame(wind_rows)),
    ):
        table.to_csv(out_dir / name, index=False)
        print(f"  wrote {out_dir / name} ({len(table)} rows)")

    per_day_all.to_parquet(out_dir / "humidity_per_day.parquet", compression="zstd")
    combined = pd.concat([r.daily.assign(site=r.site, window=r.window) for r in results])
    for column in ("site", "window"):
        combined[column] = combined[column].astype("category")
    combined.to_parquet(out_dir / "daily_series.parquet", compression="zstd", index=True)

    selection = apply_selection_rule(scores, robustness, per_year, consistency)
    (out_dir / "selection.json").write_text(
        json.dumps(selection, indent=2, default=_json_default)
    )
    print(f"\nSelection rule (SPEC.md 11): {selection['status']}")
    print(f"  selected: {selection['selected']}")
    for entry in selection["all_candidates"]:
        if entry.get("status") == CLASS_NOT_EVALUATED:
            print(f"    {entry['candidate']}: NOT EVALUATED")
            continue
        print(
            f"    {entry['candidate']}: daily {entry['daily_gate']} "
            f"({entry['daily_pairs_passing']}/{entry['daily_pairs']}), "
            f"counts {entry['count_pairs_robust_pass']} robust pass / "
            f"{entry['count_pairs_robust_fail']} robust fail / "
            f"{entry['count_pairs_reference_sensitive']} reference-sensitive / "
            f"{entry['count_pairs_not_evaluated']} not evaluated, "
            f"worst |count error| {entry['worst_count_error_per_year_audited']:.1f} d/yr, "
            f"consistency {entry['input_consistency']}"
        )
    print(
        f"  deterioration screen: {selection['deterioration_screen']['n_pairs_lost']} pair(s) lost, "
        f"{selection['deterioration_screen']['n_pairs_gained']} gained"
    )
    extra["selection"] = selection

    # ---------------- Stage: limited NEX transfer check (SPEC.md 10) ----------------
    if args.stage in ("nex", "all"):
        sample_path = args.nex_sample or (
            MILESTONE2_WORK_DIR
            / f"nex_sample_{NEX_SCENARIO}_{min(NEX_MATCHED_YEARS)}-{max(NEX_MATCHED_YEARS)}.parquet"
        )
        if not Path(sample_path).exists():
            extra["nex"] = {
                "status": "NOT RUN",
                "limitation": f"the cached NEX sample {sample_path} is absent; reusing it was the "
                "only authorized route and no download or new reader is in scope (SPEC.md 10)",
            }
            print(f"\nNEX transfer check: NOT RUN -- {sample_path} is absent")
        else:
            print(f"\nLimited NEX transfer check (SPEC.md 10), reusing {sample_path}")
            sample = pd.read_parquet(sample_path)
            frames: list[pd.DataFrame] = []
            for cid in CANDIDATE_IDS:
                path = work_dir / f"nex_{cid}_daily_max.parquet"
                if args.resume and path.exists():
                    frames.append(pd.read_parquet(path))
                    print(f"    {cid}: reused cache")
                else:
                    frame = nex_candidate_daily_max(sample, sites, cid)
                    frame.to_parquet(path)
                    frames.append(frame)
                    print(f"    {cid}: {len(frame)} NEX days")
            nex_daily = pd.concat(frames)
            era5_subset = [r for r in results if r.window == m1.DEFAULT_PRIMARY_WINDOW]
            transfer = nex_transfer_table(nex_daily, era5_subset, NEX_MATCHED_YEARS)
            transfer.to_csv(out_dir / "nex_transfer.csv", index=False)
            validity = m1.nex_site_validity(sample)
            validity.to_csv(out_dir / "nex_sample_validity.csv", index=False)
            extra["nex"] = {
                "status": "RUN",
                "models": sorted(set(sample["model"].astype(str))),
                "scenario": NEX_SCENARIO,
                "years": f"{min(NEX_MATCHED_YEARS)}-{max(NEX_MATCHED_YEARS)}",
                "sample": str(sample_path),
                "day_boundary": "INFERRED from the CMIP6 daily convention; no file publishes "
                "time_bnds, so this is not verified locally",
                "scope": "distribution and annual counts only; no same-date NEX-vs-ERA5 RMSE or "
                "correlation, no QDM, no new inventory, no national scan",
            }
            extra["stages_run"].append("nex")
            print(f"  wrote {out_dir / 'nex_transfer.csv'} ({len(transfer)} rows)")

    extra["runtime"] = {
        "wall_seconds": round(time.monotonic() - started, 1),
        "workers": workers,
        "per_site_window_seconds": {
            f"{r.site} {r.window}": r.diagnostics.get("wall_seconds") for r in results
        },
    }
    manifest = run_manifest(args, extra)
    (out_dir / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, default=_json_default)
    )
    print(f"\nwrote {out_dir / 'run_manifest.json'}")
    print(f"total wall time {extra['runtime']['wall_seconds']:.1f} s")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

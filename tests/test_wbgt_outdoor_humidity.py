"""Tests for the outdoor-WBGT humidity-consistency harness (CHG-0616).

These cover only what milestone 3 *changes* relative to milestones 1 and 2: the
input-consistent daily-RH solve, its validity and endpoint handling, the candidate/cache
signatures that distinguish it, the both-reference robustness verdicts, and the guarantee that
the old A/B behaviour is preserved.  The predecessors' contracts are covered by
``tests/test_wbgt_outdoor_feasibility.py`` and ``tests/test_wbgt_outdoor_selection.py`` and are
not restated.

Assertions are properties from ``docs/diagnostics/wbgt_outdoor_humidity/SPEC.md`` -- invariants
of the constraint being solved, not the solver's own arithmetic echoed back.  Everything runs on
small synthetic inputs, with no network, no NEX tree and no hourly cache.
"""

from __future__ import annotations

import inspect

import numpy as np
import pandas as pd
import pytest

from tools.diagnostics import wbgt_outdoor_feasibility as M1
from tools.diagnostics import wbgt_outdoor_selection as M2
from tools.diagnostics import wbgt_outdoor_humidity as H
from tools.diagnostics.wbgt_method_validation import SITES

KOCHI = next(s for s in SITES if s.name == "Kochi")
BIKANER = next(s for s in SITES if s.name == "Bikaner")

pytest.importorskip("thermofeel", reason="physical solver checks need thermofeel")


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


def _es_matrix(n_days: int = 6, n_hours: int = 24, spread: float = 9.0, base: float = 28.0):
    """Saturation pressures for a diurnally varying synthetic temperature field."""

    hours = np.arange(n_hours)
    shape = np.sin(2.0 * np.pi * (hours - 3) / n_hours)
    temps = base + spread * shape[None, :] + np.linspace(-3.0, 3.0, n_days)[:, None]
    return M1.saturation_pressure_hpa(temps), temps


def _chain(site, days: int = 40, seed: int = 11):
    """Synthetic hourly series -> geometry -> complete days -> daily inputs and base recon."""

    hourly = M1.attach_geometry(site, M1._synthetic_hourly(site, days=days, seed=seed))
    keep = M1.complete_local_days(hourly)
    hourly = hourly[hourly["local_day"].isin(keep)]
    daily_inputs = M1.aggregate_daily_inputs(hourly)
    times = pd.DatetimeIndex(hourly.index)
    local_day = hourly["local_day"].to_numpy()
    cz_mid = hourly["cossza_mid"].to_numpy(dtype=float)
    base = M1.reconstruct_hourly(
        times,
        local_day,
        daily_inputs,
        M1.SiteStatic.from_site(site),
        M1.CANDIDATE_PARAMS["C1"],
        cossza=cz_mid,
    )
    return hourly, keep, daily_inputs, times, local_day, cz_mid, base


# --------------------------------------------------------------------------
# The constraint being solved (SPEC.md 4.2)
# --------------------------------------------------------------------------


def test_constraint_is_monotone_non_decreasing_in_e():
    """The solve is only well posed because the constraint never decreases in ``e``."""

    es, _ = _es_matrix(n_days=4)
    grid = np.linspace(0.0, float(es.max()) * 1.3, 120)
    for row in range(es.shape[0]):
        values = np.array(
            [H.constraint_mean_rh_pct(np.array([e]), es[row : row + 1])[0] for e in grid]
        )
        assert np.all(np.diff(values) >= -1e-12)
        # And it spans the whole achievable RH range, which is what makes the bracket valid.
        assert values[0] == pytest.approx(0.0)
        assert values[-1] == pytest.approx(100.0)


def test_constant_temperature_reduces_to_the_supplied_rh():
    """With one temperature all day, ``e* = (hurs/100) es(T)`` and hourly RH IS ``hurs``."""

    es = np.full((1, 24), float(M1.saturation_pressure_hpa(31.0)))
    for hurs in (5.0, 42.0, 88.0):
        solution = H.solve_daily_vapour_pressure_hpa(es, np.array([hurs]))
        assert bool(solution.valid[0])
        assert solution.e_star_hpa[0] == pytest.approx((hurs / 100.0) * es[0, 0], rel=1e-12)
        assert H.constraint_mean_rh_pct(solution.e_star_hpa, es)[0] == pytest.approx(hurs)


def test_variable_temperature_unsaturated_uses_the_analytic_branch_and_is_exact():
    """Where no hour saturates the answer is closed form, so the residual is machine zero."""

    es, _ = _es_matrix()
    hurs = np.full(es.shape[0], 35.0)
    solution = H.solve_daily_vapour_pressure_hpa(es, hurs)
    assert bool(solution.valid.all())
    assert all("analytic" in path for path in solution.path)
    assert np.max(np.abs(solution.residual_pp)) < 1e-9
    # The analytic branch is the harmonic-style mean, NOT es of the mean temperature.
    expected = (hurs / 100.0) / np.mean(1.0 / es, axis=1)
    assert np.allclose(solution.e_star_hpa, expected, rtol=1e-12)


def test_saturation_clipped_days_take_the_root_solve_and_still_reproduce_hurs():
    """When the 100 % cap binds the analytic form is wrong; the bounded solve still matches."""

    es, _ = _es_matrix(spread=14.0)
    hurs = np.full(es.shape[0], 96.0)
    solution = H.solve_daily_vapour_pressure_hpa(es, hurs)
    assert bool(solution.valid.all())
    assert any("bisection" in path for path in solution.path)
    assert np.max(np.abs(solution.residual_pp)) <= H.RH_RESIDUAL_TOL_PP
    # The analytic expression would have overshot, which is why the solve exists.
    analytic = (hurs / 100.0) / np.mean(1.0 / es, axis=1)
    assert np.any(analytic > es.min(axis=1))


def test_solution_always_reproduces_the_supplied_daily_rh_within_tolerance():
    """The one property the whole milestone rests on, swept across the RH range."""

    es, _ = _es_matrix(n_days=8, spread=12.0)
    for value in (0.0, 1.0, 17.5, 50.0, 80.0, 99.0, 99.999, 100.0):
        hurs = np.full(es.shape[0], value)
        solution = H.solve_daily_vapour_pressure_hpa(es, hurs)
        assert bool(solution.valid.all()), value
        achieved = H.constraint_mean_rh_pct(solution.e_star_hpa, es)
        assert np.max(np.abs(achieved - hurs)) <= H.RH_RESIDUAL_TOL_PP, value


def test_endpoints_are_exact_and_not_approximated():
    """0 % gives zero vapour pressure; 100 % gives the minimum ``e`` saturating every hour."""

    es, _ = _es_matrix(n_days=3)
    solution = H.solve_daily_vapour_pressure_hpa(es, np.array([0.0, 100.0, 100.0]))
    assert solution.e_star_hpa[0] == 0.0
    assert np.allclose(solution.e_star_hpa[1:], es[1:].max(axis=1))
    # Minimality: any smaller e leaves at least one hour below saturation, so the mean drops.
    smaller = es[1:].max(axis=1) * (1.0 - 1e-6)
    assert np.all(H.constraint_mean_rh_pct(smaller, es[1:]) < 100.0)


def test_out_of_range_rh_is_invalid_and_never_coerced():
    es, _ = _es_matrix(n_days=4)
    solution = H.solve_daily_vapour_pressure_hpa(es, np.array([-0.1, 100.1, np.nan, 50.0]))
    assert list(solution.valid) == [False, False, False, True]
    assert "outside [0, 100]" in solution.reason[0]
    assert "outside [0, 100]" in solution.reason[1]
    assert "non-finite daily hurs" in solution.reason[2]
    assert np.isnan(solution.e_star_hpa[:3]).all()


def test_non_finite_and_non_positive_saturation_pressures_invalidate_the_day():
    es, _ = _es_matrix(n_days=3)
    es[0, 5] = np.nan
    es[1, 9] = 0.0
    solution = H.solve_daily_vapour_pressure_hpa(es, np.full(3, 55.0))
    assert list(solution.valid) == [False, False, True]
    assert all("saturation vapour pressure" in solution.reason[i] for i in (0, 1))


def test_empty_input_is_handled_without_raising():
    solution = H.solve_daily_vapour_pressure_hpa(np.empty((0, 24)), np.empty(0))
    assert solution.e_star_hpa.size == 0
    assert solution.valid.size == 0


def test_non_convergence_is_reported_as_invalid_not_as_a_fallback_value():
    """A starved iteration budget must yield an explicit invalid day, never a silent guess."""

    es, _ = _es_matrix(n_days=3, spread=14.0)
    hurs = np.full(3, 96.0)  # forces the bisection branch
    solution = H.solve_daily_vapour_pressure_hpa(es, hurs, max_iter=1)
    assert not solution.valid.any()
    assert all("did not reach" in r for r in solution.reason)
    assert np.isnan(solution.e_star_hpa).all()


def test_iteration_limit_is_validated():
    es, _ = _es_matrix(n_days=1)
    with pytest.raises(ValueError):
        H.solve_daily_vapour_pressure_hpa(es, np.array([50.0]), max_iter=0)


def test_partial_days_are_refused_rather_than_solved_over_fewer_hours():
    """SPEC.md 4.2: the solve is never performed over a shortened set of valid hours."""

    days = pd.DatetimeIndex(["2000-01-01"] * 24 + ["2000-01-02"] * 20)
    with pytest.raises(ValueError, match="differing hour counts"):
        H._day_matrix(np.ones(len(days)), days.to_numpy())


def test_day_blocks_must_be_contiguous():
    days = pd.DatetimeIndex(["2000-01-01", "2000-01-02", "2000-01-01", "2000-01-02"])
    with pytest.raises(ValueError, match="contiguous"):
        H._day_matrix(np.ones(4), days.to_numpy())


# --------------------------------------------------------------------------
# The reconstruction wrapper (SPEC.md 4.2)
# --------------------------------------------------------------------------


def test_reconstruction_preserves_the_supplied_daily_hurs():
    """End to end on a synthetic site: the daily mean of reconstructed RH IS ``hurs``."""

    _, _, daily_inputs, times, local_day, _, base = _chain(KOCHI)
    rh, _, per_day = H.reconstruct_humidity_input_consistent(
        base["t_c"].to_numpy(dtype=float), daily_inputs.frame, local_day
    )
    got = pd.Series(rh, index=pd.DatetimeIndex(local_day)).groupby(level=0).mean()
    want = daily_inputs.frame["hurs"].reindex(got.index)
    valid = per_day["valid"].reindex(got.index).to_numpy(dtype=bool)
    assert valid.any()
    assert np.max(np.abs((got - want).to_numpy()[valid])) <= H.RH_RESIDUAL_TOL_PP
    assert float(per_day.loc[valid, "abs_residual_pp"].max()) <= H.RH_RESIDUAL_TOL_PP


def test_existing_method_generally_does_not_preserve_daily_hurs():
    """The premise of the experiment: the old invariant is not input-consistent."""

    _, _, daily_inputs, _, local_day, _, base = _chain(KOCHI)
    got = (
        pd.Series(base["rh_pct"].to_numpy(dtype=float), index=pd.DatetimeIndex(local_day))
        .groupby(level=0)
        .mean()
    )
    want = daily_inputs.frame["hurs"].reindex(got.index)
    assert np.nanmax(np.abs((got - want).to_numpy())) > 0.5


def test_invalid_day_yields_nan_hours_so_a_daily_maximum_is_never_a_zero_exceedance():
    """Missing or invalid input must never become a zero threshold count (SPEC.md 4.2)."""

    _, keep, daily_inputs, _, local_day, _, base = _chain(BIKANER)
    daily = daily_inputs.frame.copy()
    broken_day = daily.index[3]
    daily.loc[broken_day, "hurs"] = 140.0  # out of range -> invalid, not coerced
    rh, _, per_day = H.reconstruct_humidity_input_consistent(
        base["t_c"].to_numpy(dtype=float), daily, local_day
    )
    assert not bool(per_day.loc[broken_day, "valid"])
    mask = pd.DatetimeIndex(local_day) == broken_day
    assert np.isnan(rh[mask]).all()
    # ... and the daily maximum of anything built from it is NaN, not a number.
    maxima = M1.daily_max(rh, local_day, keep)
    assert np.isnan(maxima.loc[broken_day])
    assert maxima.drop(index=broken_day).notna().any()


def test_effective_vapour_pressure_is_capped_at_saturation():
    """Where RH is clipped the effective vapour pressure is ``min(e*, es)``, not ``e*``."""

    es, temps = _es_matrix(n_days=2, spread=14.0)
    solution = H.solve_daily_vapour_pressure_hpa(es, np.full(2, 98.0))
    rh = np.clip(100.0 * solution.e_star_hpa[:, None] / es, 0.0, 100.0)
    e_eff = H.effective_vapour_pressure_hpa(rh.ravel(), temps.ravel()).reshape(es.shape)
    expected = np.minimum(solution.e_star_hpa[:, None], es)
    assert np.allclose(e_eff, expected, rtol=1e-12)
    assert np.any(e_eff < solution.e_star_hpa[:, None] - 1e-9)  # saturation actually binds


# --------------------------------------------------------------------------
# No leakage of hourly weather into a candidate (SPEC.md 4.2)
# --------------------------------------------------------------------------


def test_new_humidity_reads_only_daily_inputs():
    """The signature accepts no hourly humidity, dew point or WBGT."""

    parameters = set(
        inspect.signature(H.reconstruct_humidity_input_consistent).parameters
    )
    assert parameters == {"t_hourly_c", "daily", "local_day", "tol_pp", "max_iter"}
    for forbidden in ("rh_hourly", "dewpoint", "wbgt", "hourly"):
        assert not any(forbidden in p for p in parameters - {"t_hourly_c"})


def test_daily_inputs_contract_still_refuses_extra_columns():
    """The structural leakage guard the candidates are built on is still in force."""

    _, _, daily_inputs, _, _, _, _ = _chain(KOCHI)
    frame = daily_inputs.frame.copy()
    frame["relative_humidity_2m"] = 50.0
    with pytest.raises(ValueError, match="hourly leakage"):
        M1.DailyInputs(frame)


# --------------------------------------------------------------------------
# Candidate identity and cache signatures (SPEC.md 5)
# --------------------------------------------------------------------------


def test_candidates_differ_only_in_humidity_and_wind():
    assert H.CANDIDATES["A"] == (H.HUMIDITY_OLD, "C1")
    assert H.CANDIDATES["B"] == (H.HUMIDITY_OLD, "W1")
    assert H.CANDIDATES["C"] == (H.HUMIDITY_NEW, "C1")
    assert H.CANDIDATES["D"] == (H.HUMIDITY_NEW, "W1")
    # A/C and B/D share a wind; A/B and C/D share a humidity. Nothing else varies.
    assert H.CANDIDATES["A"][1] == H.CANDIDATES["C"][1]
    assert H.CANDIDATES["B"][1] == H.CANDIDATES["D"][1]
    assert H.CANDIDATES["A"][0] == H.CANDIDATES["B"][0]
    assert H.CANDIDATES["C"][0] == H.CANDIDATES["D"][0]


def test_signatures_distinguish_every_method_axis():
    signatures = {cid: H.candidate_signature(cid) for cid in H.CANDIDATE_IDS}
    assert len(set(signatures.values())) == len(H.CANDIDATE_IDS)
    assert H.HUMIDITY_NEW in signatures["C"] and H.HUMIDITY_NEW in signatures["D"]
    assert H.HUMIDITY_NEW not in signatures["A"] and H.HUMIDITY_NEW not in signatures["B"]
    assert "wind-constant" in signatures["A"] and "wind-dtr" in signatures["B"]
    # Tolerance, iteration limit, solver and calendar all appear, so a cache cannot be
    # reused across a change in any of them.
    assert f"tol{H.RH_RESIDUAL_TOL_PP:g}pp" in signatures["C"]
    assert f"iter{H.SOLVER_MAX_ITER}" in signatures["C"]
    assert "liljegren-thermofeel" in signatures["A"]
    assert "complete24h-feb29dropped" in signatures["A"]
    assert H.candidate_signature("A", day_convention="utc") != signatures["A"]


def test_signature_bundle_carries_references_and_gates_not_just_the_reference():
    """SPEC.md 5: a matching reference signature is never sufficient to reuse a cache."""

    bundle = H.signature_bundle()
    assert set(bundle["candidates"]) == set(H.CANDIDATE_IDS)
    assert bundle["references"][H.REFERENCE_AUDITED] == M1.REFERENCE_SIGNATURES["audited"]
    assert bundle["references"][H.REFERENCE_R3] == M2.REFERENCE_SIGNATURE_MIDPOINT
    assert bundle["gates"]["median_abs_error_c"] == M1.GATE_MEDIAN_ABS_ERROR_C
    assert bundle["thresholds_c"] == list(M1.THRESHOLDS_C)


def test_gates_are_imported_and_not_widened():
    """The whole point of importing them: milestone 3 cannot loosen a gate."""

    assert M1.GATE_MEDIAN_ABS_ERROR_C == 1.0
    assert M1.GATE_RMSE_C == 1.5
    assert M1.COUNT_GATE_REL_TOL == 0.20
    assert M1.COUNT_GATE_ABS_FLOOR_PER_YEAR == 2.0
    assert M1.COUNT_GATE_MIN_REF_PER_YEAR == 5.0
    source = inspect.getsource(H)
    for name in ("GATE_MEDIAN_ABS_ERROR_C =", "GATE_RMSE_C =", "COUNT_GATE_REL_TOL ="):
        assert name not in source


def test_old_candidates_reuse_milestone_code_paths_unchanged():
    """A and B must be milestone 1/2 functions called as they stand (SPEC.md 2)."""

    source = inspect.getsource(H.run_site_window)
    assert 'm1.CANDIDATE_PARAMS["C1"]' in source
    assert "m2.wind_w1_dtr_ms" in source
    # W1's coefficients are inherited, never restated here.
    assert "0.03" not in inspect.getsource(H)


# --------------------------------------------------------------------------
# Robustness verdicts across the two references (SPEC.md 8)
# --------------------------------------------------------------------------


def test_classification_never_invents_a_pass_or_a_fail():
    assert H.classify("PASS", "PASS") == H.CLASS_ROBUST_PASS
    assert H.classify("FAIL", "FAIL") == H.CLASS_ROBUST_FAIL
    assert H.classify("PASS", "FAIL") == H.CLASS_REFERENCE_SENSITIVE
    assert H.classify("FAIL", "PASS") == H.CLASS_REFERENCE_SENSITIVE
    rare = "NOT GATED (rare event)"
    assert H.classify("PASS", rare) == H.CLASS_NOT_EVALUATED
    assert H.classify(rare, rare) == H.CLASS_NOT_EVALUATED
    assert H.classify(rare, "FAIL") == H.CLASS_NOT_EVALUATED


def test_rare_event_pairs_are_not_counted_as_passes_by_the_selection_rule():
    """A rare-event pair is NOT EVALUATED and must not inflate the robust-pass tally."""

    scores = pd.DataFrame(
        [
            {
                "site": "X",
                "window": "1990-2004",
                "season": "ALL",
                "reference": H.REFERENCE_AUDITED,
                "candidate": f"cand_{cid}",
                "candidate_id": cid,
                "daily_gate": "PASS",
                "median_abs_error_c": 0.5,
                "rmse_c": 0.9,
            }
            for cid in H.CANDIDATE_IDS
        ]
    )
    robustness = pd.DataFrame(
        [
            {
                "site": "X",
                "window": "1990-2004",
                "candidate": f"cand_{cid}",
                "candidate_id": cid,
                "humidity_method": H.CANDIDATES[cid][0],
                "wind_treatment": H.CANDIDATES[cid][1],
                "statistic": "days_ge_32_per_year",
                "threshold_c": 32.0,
                "robustness": H.CLASS_NOT_EVALUATED,
                "gate_audited": "NOT GATED (rare event)",
                "gate_R3": "NOT GATED (rare event)",
                "signed_error_audited": 1.0,
            }
            for cid in H.CANDIDATE_IDS
        ]
    )
    consistency = pd.DataFrame(
        [
            {"candidate_id": cid, "max_abs_residual_pp": 0.0, "n_solver_failures": 0}
            for cid in H.NEW_HUMIDITY_IDS
        ]
    )
    result = H.apply_selection_rule(
        scores, robustness, pd.DataFrame(columns=["candidate", "reference", "window_mean_gate", "outside_tolerance"]), consistency
    )
    for entry in result["all_candidates"]:
        assert entry["count_pairs_robust_pass"] == 0
        assert entry["count_pairs_not_evaluated"] == 1
        assert entry["count_pairs_classified"] == 0


def test_deterioration_screen_names_lost_robust_passes():
    """SPEC.md 11 step 4: a ROBUST PASS turning ROBUST FAIL blocks the new-humidity preference."""

    rows = []
    for cid, verdict in (("A", H.CLASS_ROBUST_PASS), ("C", H.CLASS_ROBUST_FAIL)):
        rows.append(
            {
                "site": "X",
                "window": "1990-2004",
                "statistic": "days_ge_30_per_year",
                "wind_treatment": "C1",
                "humidity_method": H.CANDIDATES[cid][0],
                "robustness": verdict,
            }
        )
    for cid, verdict in (("B", H.CLASS_ROBUST_FAIL), ("D", H.CLASS_ROBUST_PASS)):
        rows.append(
            {
                "site": "X",
                "window": "1990-2004",
                "statistic": "days_ge_30_per_year",
                "wind_treatment": "W1",
                "humidity_method": H.CANDIDATES[cid][0],
                "robustness": verdict,
            }
        )
    screen = H.deterioration_screen(pd.DataFrame(rows))
    assert screen["n_pairs_lost"] == 1
    assert screen["pairs_lost"][0]["wind_treatment"] == "C1"
    assert screen["n_pairs_gained"] == 1
    assert screen["pairs_gained"][0]["wind_treatment"] == "W1"


def test_input_consistency_failure_eliminates_a_new_humidity_candidate():
    scores = pd.DataFrame(
        [
            {
                "site": "X",
                "window": "1990-2004",
                "season": "ALL",
                "reference": H.REFERENCE_AUDITED,
                "candidate": f"cand_{cid}",
                "candidate_id": cid,
                "daily_gate": "PASS",
                "median_abs_error_c": 0.5,
                "rmse_c": 0.9,
            }
            for cid in H.CANDIDATE_IDS
        ]
    )
    robustness = pd.DataFrame(
        [
            {
                "site": "X",
                "window": "1990-2004",
                "candidate": f"cand_{cid}",
                "candidate_id": cid,
                "humidity_method": H.CANDIDATES[cid][0],
                "wind_treatment": H.CANDIDATES[cid][1],
                "statistic": "days_ge_30_per_year",
                "threshold_c": 30.0,
                "robustness": H.CLASS_ROBUST_PASS,
                "gate_audited": "PASS",
                "gate_R3": "PASS",
                "signed_error_audited": 0.5,
            }
            for cid in H.CANDIDATE_IDS
        ]
    )
    consistency = pd.DataFrame(
        [
            {"candidate_id": "C", "max_abs_residual_pp": 1.0, "n_solver_failures": 4},
            {"candidate_id": "D", "max_abs_residual_pp": 0.0, "n_solver_failures": 0},
        ]
    )
    result = H.apply_selection_rule(
        scores,
        robustness,
        pd.DataFrame(columns=["candidate", "reference", "window_mean_gate", "outside_tolerance"]),
        consistency,
    )
    assert any(e["reason"].startswith("did not satisfy") for e in result["eliminated"])
    assert "C" not in [r["candidate"] for r in result["ranking"]]
    assert result["selected"] != "C"


# --------------------------------------------------------------------------
# Write isolation (SPEC.md 13)
# --------------------------------------------------------------------------


def test_write_guard_protects_predecessors_shade_and_production():
    from pathlib import Path

    for bad in (
        "docs/diagnostics/wbgt_outdoor_feasibility",
        "docs/diagnostics/wbgt_outdoor_selection",
        "docs/diagnostics/wbgt_outdoor_selection/sub",
        "scratch/wbgt_shade_national",
        "some/processed_optimised/x",
    ):
        with pytest.raises(SystemExit):
            H._guard_write_target(Path(bad), "test")
    assert H._guard_write_target(Path(H.DEFAULT_OUT_DIR), "out-dir").name == "wbgt_outdoor_humidity"
    assert "wbgt_outdoor_humidity" in str(H.DEFAULT_WORK_DIR)


# --------------------------------------------------------------------------
# Per-candidate daily-RH residual (SPEC.md 9.1)
# --------------------------------------------------------------------------


def test_existing_method_residual_is_measured_not_assumed():
    """SPEC.md 9.1 asks for the residual of EVERY candidate, including the existing humidity."""

    _, _, daily_inputs, _, local_day, _, base = _chain(KOCHI)
    _, _, per_day = H.reconstruct_humidity_input_consistent(
        base["t_c"].to_numpy(dtype=float), daily_inputs.frame, local_day
    )
    assert {"residual_old_pp", "abs_residual_old_pp"} <= set(per_day.columns)
    valid = per_day["valid"].to_numpy(dtype=bool)
    # The existing invariant genuinely misses the target, and by much more than the new one.
    assert float(per_day.loc[valid, "abs_residual_old_pp"].max()) > 0.5
    assert float(per_day.loc[valid, "abs_residual_pp"].max()) <= H.RH_RESIDUAL_TOL_PP


def test_residual_of_the_existing_method_equals_the_constraint_it_fails():
    """The reported old residual is the same functional the new method drives to zero."""

    es, _ = _es_matrix(n_days=5)
    hurs = np.full(5, 62.0)
    e_old = np.full(5, 20.0)
    residual = H.constraint_mean_rh_pct(e_old, es) - hurs
    solution = H.solve_daily_vapour_pressure_hpa(es, hurs)
    # Same functional, same days: one is off, the other is at zero within tolerance.
    assert np.any(np.abs(residual) > 1.0)
    assert np.max(np.abs(solution.residual_pp)) <= H.RH_RESIDUAL_TOL_PP

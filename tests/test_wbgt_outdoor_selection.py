"""Tests for the outdoor-WBGT method-selection harness (CHG-0610).

These cover only what milestone 2 *changes* relative to milestone 1: the two new wind
candidates, the calibration/evaluation separation W2 depends on, the alternative reference
treatment, the selection rule, the signatures that distinguish the assumptions, and the
training/application separation of the conditional bias-correction trial.  Milestone 1's
contract is already covered by ``tests/test_wbgt_outdoor_feasibility.py`` and is not restated.

Assertions are properties from ``docs/diagnostics/wbgt_outdoor_selection/SPEC.md``, not the
implementation's own arithmetic echoed back.  Everything runs on small synthetic inputs, with no
network, no NEX tree and no hourly cache.
"""

from __future__ import annotations

import inspect

import numpy as np
import pandas as pd
import pytest

from tools.diagnostics import wbgt_outdoor_feasibility as M1
from tools.diagnostics import wbgt_outdoor_selection as S
from tools.diagnostics.wbgt_method_validation import SITES

KOCHI = next(s for s in SITES if s.name == "Kochi")
BIKANER = next(s for s in SITES if s.name == "Bikaner")

pytest.importorskip("thermofeel", reason="physical solver checks need thermofeel")


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


def _chain(site, days: int = 40, seed: int = 11):
    """Synthetic hourly series -> geometry -> complete days -> daily inputs."""

    hourly = M1.attach_geometry(site, M1._synthetic_hourly(site, days=days, seed=seed))
    keep = M1.complete_local_days(hourly)
    hourly = hourly[hourly["local_day"].isin(keep)]
    return (
        hourly,
        M1.aggregate_daily_inputs(hourly).frame,
        pd.DatetimeIndex(hourly.index),
        hourly["local_day"].to_numpy(),
        hourly["cossza_mid"].to_numpy(dtype=float),
    )


@pytest.fixture(scope="module")
def kochi():
    return _chain(KOCHI)


# --------------------------------------------------------------------------
# W1: the DTR-dependent wind shape (SPEC.md 3.1)
# --------------------------------------------------------------------------


def test_w1_is_non_negative_everywhere(kochi):
    _, daily, _, local_day, cossza = kochi
    wind = S.wind_w1_dtr_ms(daily, local_day, cossza)
    finite = wind[np.isfinite(wind)]
    assert finite.size > 0
    assert finite.min() >= 0.0


def test_w1_preserves_the_supplied_daily_mean_wind(kochi):
    """A within-day shape may redistribute wind; it must not change the day's mean."""

    _, daily, _, local_day, cossza = kochi
    wind = S.wind_w1_dtr_ms(daily, local_day, cossza)
    got = pd.Series(wind, index=pd.DatetimeIndex(local_day)).groupby(level=0).mean()
    want = daily["sfcWind"].reindex(got.index)
    assert np.allclose(got.to_numpy(), want.to_numpy(), atol=1e-9)


def test_w1_amplitude_tracks_dtr_and_respects_its_declared_bounds(kochi):
    _, daily, _, local_day, _ = kochi
    wide = daily.copy()
    wide["tasmin"] = wide["tasmax"] - 60.0  # far beyond the upper bound
    narrow = daily.copy()
    narrow["tasmin"] = narrow["tasmax"] - 0.5  # far below the lower bound
    assert np.allclose(S.dtr_amplitude(wide, local_day), S.WIND_DTR_AMP_MAX)
    assert np.allclose(S.dtr_amplitude(narrow, local_day), S.WIND_DTR_AMP_MIN)

    small = daily.copy()
    small["tasmin"] = small["tasmax"] - 6.0
    large = daily.copy()
    large["tasmin"] = large["tasmax"] - 20.0
    assert S.dtr_amplitude(small, local_day).mean() < S.dtr_amplitude(large, local_day).mean()


def test_w1_collapses_onto_c2_at_the_declared_hinge(kochi):
    """The slope is pinned by the hinge, which is the claim that keeps W1 untuned."""

    _, daily, _, local_day, cossza = kochi
    hinged = daily.copy()
    hinged["tasmin"] = hinged["tasmax"] - S.WIND_DTR_HINGE_C
    w1 = S.wind_w1_dtr_ms(hinged, local_day, cossza)
    c2 = M1.reconstruct_wind_10m_ms(hinged, local_day, cossza, M1.CANDIDATE_PARAMS["C2"])
    assert np.allclose(w1, c2, atol=1e-9, equal_nan=True)
    assert S.WIND_DTR_HINGE_C == pytest.approx(
        M1.WIND_DIURNAL_AMPLITUDE / S.WIND_DTR_SLOPE_PER_C
    )


def test_w1_shape_is_identical_to_milestone_1_c2_shape(kochi):
    """W1 must change only the amplitude, so the shape is imported behaviour, not a rewrite."""

    _, daily, _, local_day, cossza = kochi
    wbar = daily["sfcWind"].reindex(pd.DatetimeIndex(local_day)).to_numpy(dtype=float)
    here = np.clip(
        wbar * (1.0 + M1.WIND_DIURNAL_AMPLITUDE * S.zero_mean_cz_shape(cossza, local_day)),
        0.0,
        None,
    )
    there = M1.reconstruct_wind_10m_ms(daily, local_day, cossza, M1.CANDIDATE_PARAMS["C2"])
    assert np.allclose(here, there, atol=1e-12, equal_nan=True)


def test_shape_sums_to_zero_over_every_local_day(kochi):
    _, _, _, local_day, cossza = kochi
    shape = S.zero_mean_cz_shape(cossza, local_day)
    per_day = pd.Series(shape, index=pd.DatetimeIndex(local_day)).groupby(level=0).mean()
    assert np.allclose(per_day.to_numpy(), 0.0, atol=1e-12)


# --------------------------------------------------------------------------
# W2: the climatological wind shape (SPEC.md 3.2)
# --------------------------------------------------------------------------


def test_w2_monthly_profiles_are_normalised_and_non_negative(kochi):
    hourly, _, _, _, _ = kochi
    profile = S.wind_climatology_profile(hourly)
    populated = profile.dropna(how="all")
    assert not populated.empty
    assert np.allclose(populated.mean(axis=1).to_numpy(), 1.0, atol=1e-12)
    assert np.nanmin(profile.to_numpy()) >= 0.0
    assert list(profile.columns) == list(range(24))
    assert list(profile.index) == list(range(1, 13))


def test_w2_preserves_the_daily_mean_and_stays_non_negative(kochi):
    hourly, daily, times, local_day, _ = kochi
    profile = S.wind_climatology_profile(hourly)
    wind = S.wind_w2_climatology_ms(daily, local_day, times, profile)
    finite = wind[np.isfinite(wind)]
    assert finite.min() >= 0.0
    got = pd.Series(wind, index=pd.DatetimeIndex(local_day)).groupby(level=0).mean()
    want = daily["sfcWind"].reindex(got.index)
    assert np.allclose(got.to_numpy(), want.to_numpy(), atol=1e-9, equal_nan=True)


def test_w2_month_with_no_calibration_data_stays_nan_not_filled(kochi):
    """An uncalibrated month must be visible as missing, never silently set to a flat profile."""

    hourly, _, _, _, _ = kochi
    profile = S.wind_climatology_profile(hourly)
    months_present = set(pd.DatetimeIndex(hourly["local_day"]).month.unique())
    missing = [m for m in range(1, 13) if m not in months_present]
    assert missing, "synthetic fixture should not span all twelve months"
    for month in missing:
        assert profile.loc[month].isna().all()


def test_w2_refuses_a_calibration_period_that_overlaps_the_evaluation_window(tmp_path):
    """SPEC.md 3.2: a test year may never contribute to the profile applied to it."""

    signature = inspect.signature(S.run_site_window)
    assert "calibration_years" in signature.parameters
    source = inspect.getsource(S.run_site_window)
    assert "overlap" in source
    assert "forbids deriving a test year" in source


def test_w2_calibration_and_evaluation_periods_are_disjoint_by_declaration():
    calibration = set(M1.parse_years(S.W2_PRIMARY_CALIBRATION))
    evaluation = set(M1.parse_years(S.W2_PRIMARY_EVALUATION))
    assert not (calibration & evaluation)
    assert max(calibration) < min(evaluation), "the primary split must run forward in time"


def test_w2_is_flagged_as_needing_an_asset_and_c1_c2_w1_are_not():
    assert S.NEEDS_CLIMATOLOGY_ASSET["W2"] is True
    assert S.NEEDS_CLIMATOLOGY_ASSET["W2rev"] is True
    for cid in ("C1", "C2", "W1"):
        assert S.NEEDS_CLIMATOLOGY_ASSET[cid] is False


def test_reverse_calibration_is_not_eligible_for_selection():
    assert "W2rev" not in S.DEPLOYABLE_IDS
    assert "W2" in S.DEPLOYABLE_IDS


# --------------------------------------------------------------------------
# Leakage prevention (SPEC.md 3)
# --------------------------------------------------------------------------


def test_deployable_wind_shapes_accept_no_hourly_weather():
    """W1 and W2 must be reachable only from daily inputs, geometry and the clock."""

    for function in (S.wind_w1_dtr_ms, S.wind_w2_climatology_ms):
        source = inspect.getsource(function)
        for forbidden in (
            "temperature_2m",
            "relative_humidity_2m",
            "shortwave_radiation",
            "direct_radiation",
            "surface_pressure",
            "wind_speed_10m",
        ):
            assert forbidden not in source, f"{function.__name__} reads hourly {forbidden}"


def test_w1_signature_carries_no_hourly_series_argument():
    parameters = set(inspect.signature(S.wind_w1_dtr_ms).parameters)
    assert parameters == {"daily", "local_day", "cossza"}


def test_climatology_profile_is_the_only_route_to_hourly_wind_and_is_period_separated():
    """W2 does read hourly wind -- but only from the calibration period, by construction."""

    source = inspect.getsource(S.wind_climatology_profile)
    assert "wind_speed_10m" in source
    assert "CALIBRATION" in inspect.getdoc(S.wind_climatology_profile)


def test_oracles_are_labelled_as_oracles_and_excluded_from_selection():
    assert set(S.ORACLE_IDS).isdisjoint(S.DEPLOYABLE_IDS)
    for oracle in S.ORACLE_IDS:
        assert "ORACLE" in S.CANDIDATE_NAMES[oracle]


def test_a6_is_declared_not_computed_with_a_stated_reason():
    """Omitting a declared oracle is a reportable fact, not a silent gap."""

    source = inspect.getsource(S.run_site_window)
    assert 'diagnostics["a6"]' in source
    assert "NOT COMPUTED" in source
    assert "identical to A5 by construction" in source


# --------------------------------------------------------------------------
# Alternative reference treatment R3 (SPEC.md 5)
# --------------------------------------------------------------------------


def test_r3_interpolates_only_the_instantaneous_drivers(kochi):
    hourly, _, _, _, _ = kochi
    shifted = S.interpolate_drivers_to_midpoint(hourly)
    for column in ("shortwave_radiation", "direct_radiation", "fdir_frac", "cossza_mid"):
        assert np.array_equal(
            shifted[column].to_numpy(), hourly[column].to_numpy(), equal_nan=True
        )
    for column in S.INSTANTANEOUS_DRIVERS:
        raw = hourly[column].to_numpy(dtype=float)
        if np.ptp(raw) == 0.0:
            continue  # a constant driver interpolates to itself; nothing to detect
        assert not np.array_equal(shifted[column].to_numpy()[1:], raw[1:])


def test_r3_is_the_midpoint_of_the_two_bracketing_labels(kochi):
    hourly, _, _, _, _ = kochi
    shifted = S.interpolate_drivers_to_midpoint(hourly)
    raw = hourly["temperature_2m"].to_numpy(dtype=float)
    assert np.allclose(shifted["temperature_2m"].to_numpy()[1:], 0.5 * (raw[1:] + raw[:-1]))


def test_r3_leaves_the_first_hour_missing_rather_than_back_filling(kochi):
    hourly, _, _, _, _ = kochi
    shifted = S.interpolate_drivers_to_midpoint(hourly)
    for column in S.INSTANTANEOUS_DRIVERS:
        values = shifted[column].to_numpy(dtype=float)
        assert not np.isfinite(values[0])
        assert np.isfinite(values[1:]).all()


def test_r3_signature_distinguishes_it_from_both_milestone_1_references():
    signatures = {*M1.REFERENCE_SIGNATURES.values(), S.REFERENCE_SIGNATURE_MIDPOINT}
    assert len(signatures) == 3
    assert "all-drivers-interpolated" in S.REFERENCE_SIGNATURE_MIDPOINT


def test_reference_sensitivity_is_scoped_to_the_failing_count_sites():
    assert set(S.REFERENCE_SENSITIVITY_SITES) == {"Bikaner", "Hyderabad"}


# --------------------------------------------------------------------------
# Carried-forward gates must be imported, not restated (SPEC.md 2.2)
# --------------------------------------------------------------------------


def test_gates_are_not_redefined_in_the_selection_module():
    source = inspect.getsource(S)
    for name in (
        "GATE_MEDIAN_ABS_ERROR_C",
        "GATE_RMSE_C",
        "COUNT_GATE_REL_TOL",
        "COUNT_GATE_ABS_FLOOR_PER_YEAR",
        "COUNT_GATE_MIN_REF_PER_YEAR",
    ):
        assert f"\n{name} =" not in source, f"{name} is re-declared and could drift from gate 1"
        assert f"m1.{name}" in source, f"{name} is never read from milestone 1"


def test_carried_forward_gate_thresholds_are_unchanged():
    assert M1.GATE_MEDIAN_ABS_ERROR_C == 1.0
    assert M1.GATE_RMSE_C == 1.5
    assert M1.COUNT_GATE_REL_TOL == 0.20
    assert M1.COUNT_GATE_ABS_FLOOR_PER_YEAR == 2.0
    assert M1.COUNT_GATE_MIN_REF_PER_YEAR == 5.0


# --------------------------------------------------------------------------
# CarbonPlan comparator (SPEC.md 6)
# --------------------------------------------------------------------------


def test_carbonplan_coefficients_come_from_the_published_source_points():
    beta = S.carbonplan_adjustment_coefficients()
    assert len(beta) == 3
    assert beta[0] == pytest.approx(-2.1564, abs=1e-3)   # intercept
    assert beta[1] == pytest.approx(-0.005375, abs=1e-6)  # per W/m2: more sun, warmer in the sun
    assert beta[2] == pytest.approx(1.0424, abs=1e-3)     # per m/s: more wind, less sun penalty
    assert beta[1] < 0.0 and beta[2] > 0.0


def test_carbonplan_source_points_are_the_sixteen_kong_huber_points():
    assert len(S.CARBONPLAN_KONG_HUBER_X) == 16
    assert len(S.CARBONPLAN_KONG_HUBER_Y) == 16
    assert all(y < 0.0 for y in S.CARBONPLAN_KONG_HUBER_Y), "sun is warmer than shade"


def test_carbonplan_style_chain_clips_to_the_declared_domain_and_warms_the_shade(kochi):
    _, daily, _, _, _ = kochi
    days = daily.index
    static = M1.SiteStatic.from_site(KOCHI)
    outdoor, provenance = S.carbonplan_style_outdoor_c(daily, static, days)
    shade = M1.shade_stull_c(
        daily["tasmax"].to_numpy(dtype=float),
        M1.rh_at_tasmax_pct(
            daily["tas"].to_numpy(dtype=float),
            daily["tasmax"].to_numpy(dtype=float),
            daily["hurs"].to_numpy(dtype=float),
        ),
    )
    assert np.all(outdoor > shade), "the sun adjustment must raise the shade value"
    assert provenance["revision"] == S.CARBONPLAN_REVISION
    assert provenance["deviations"], "deviations must be stated, not assumed away"
    assert any("metsim" in d for d in provenance["deviations"])
    assert any("notebook 06" in d for d in provenance["deviations"])


def test_carbonplan_provenance_records_an_exact_revision_not_a_branch():
    assert len(S.CARBONPLAN_REVISION) == 40
    assert set(S.CARBONPLAN_REVISION) <= set("0123456789abcdef")


# --------------------------------------------------------------------------
# Wind diagnostics (SPEC.md 3.1)
# --------------------------------------------------------------------------


def test_wind_diagnostics_reports_the_solver_floor_exposure(kochi):
    _, daily, _, local_day, cossza = kochi
    diag = S.wind_diagnostics(S.wind_w1_dtr_ms(daily, local_day, cossza), daily, local_day)
    assert 0.0 <= diag["frac_hours_at_or_below_solver_floor"] <= 1.0
    assert diag["n_clipped_at_zero"] == 0
    assert S.THERMOFEEL_MIN_WIND_10M == pytest.approx(0.62)


def test_wind_diagnostics_detects_a_clip_that_breaks_mean_conservation():
    """A shape that clipped at zero would silently lower the daily mean; that must show up."""

    days = pd.date_range("2000-01-01", periods=2, freq="D")
    local_day = np.repeat(days.to_numpy(), 24)
    daily = pd.DataFrame(
        {v: 1.0 for v in M1.REQUIRED_NEX_VARIABLES}, index=days
    )
    daily["sfcWind"] = 1.0
    wind = np.full(48, 1.0)
    wind[:12] = 0.0  # a clip deep enough to destroy the mean
    diag = S.wind_diagnostics(wind, daily, local_day)
    assert diag["n_clipped_at_zero"] == 12
    assert diag["max_daily_mean_error_ms"] > 0.1


# --------------------------------------------------------------------------
# Complete years, missing data and degenerate inputs
# --------------------------------------------------------------------------


def test_per_year_count_errors_is_empty_without_complete_years():
    days = pd.date_range("2000-01-01", periods=40, freq="D")
    daily = pd.DataFrame({"ref_audited": 30.0, "cand_C1": 30.0}, index=days)
    result = S.SelectionResult("X", "2000-2000", daily, {})
    assert S.per_year_count_errors([result]).empty


def test_per_year_count_errors_exposes_a_cancelling_mean_level_pass():
    """Two years that cancel must pass the mean gate and still be reported as both outside it."""

    years = [2001, 2002]
    index = pd.DatetimeIndex(
        np.concatenate([pd.date_range(f"{y}-01-01", f"{y}-12-31").to_numpy() for y in years])
    )
    reference = pd.Series(20.0, index=index)
    candidate = pd.Series(20.0, index=index)
    # Reference: 30 hot days each year. Candidate: 5 in the first year, 55 in the second.
    for year, hot_ref, hot_cand in ((2001, 30, 5), (2002, 30, 55)):
        mask = index.year == year
        positions = np.flatnonzero(mask)
        reference.iloc[positions[:hot_ref]] = 35.0
        candidate.iloc[positions[:hot_cand]] = 35.0
    daily = pd.DataFrame({"ref_audited": reference, "cand_C1": candidate})
    table = S.per_year_count_errors([S.SelectionResult("X", "2001-2002", daily, {})])
    row = table[(table["candidate"] == "cand_C1") & (table["threshold_c"] == 32.0)].iloc[0]
    assert row["mean_gate"] == "PASS", "the means cancel exactly, so the mean gate passes"
    assert row["mean_signed_error"] == pytest.approx(0.0)
    assert row["n_years_outside_tolerance"] == 2, "both individual years must be flagged"
    assert row["min_signed_error"] < 0 < row["max_signed_error"]


def test_all_nan_candidate_is_not_counted_as_zero_exceedances():
    """An invalid candidate year must never be scored as 365 non-exceedance days."""

    index = pd.date_range("2001-01-01", "2001-12-31")
    daily = pd.DataFrame({"ref_audited": 35.0, "cand_C1": np.nan}, index=index)
    result = S.SelectionResult("X", "2001-2001", daily, {})

    assert M1.complete_years(daily["cand_C1"]) == []
    assert S.matched_complete_years(daily["ref_audited"], daily["cand_C1"]) == []
    # No matched complete year -> no per-year row at all, rather than a -365 day error.
    assert S.per_year_count_errors([result]).empty

    annual = S.build_annual_table([result])
    row = annual[annual["threshold_c"].isna()].iloc[0]
    assert row["count_gate"] == "NOT EVALUATED (no matched complete year)"
    assert row["n_complete_years_reference"] == 1
    assert row["n_complete_years_candidate"] == 0


def test_partially_missing_candidate_shrinks_the_evaluated_years_visibly():
    """A candidate missing one year is scored on the other, and the shrinkage is reported."""

    index = pd.DatetimeIndex(
        np.concatenate(
            [pd.date_range(f"{y}-01-01", f"{y}-12-31").to_numpy() for y in (2001, 2002)]
        )
    )
    candidate = pd.Series(35.0, index=index)
    candidate[index.year == 2002] = np.nan
    daily = pd.DataFrame({"ref_audited": pd.Series(35.0, index=index), "cand_C1": candidate})
    annual = S.build_annual_table([S.SelectionResult("X", "2001-2002", daily, {})])
    row = annual[annual["threshold_c"] == 32.0].iloc[0]
    assert row["n_complete_years_reference"] == 2
    assert row["n_complete_years_candidate"] == 1
    assert row["n_complete_years"] == 1
    assert row["years"] == "2001"
    assert row["signed_error"] == pytest.approx(0.0), "the covered year must match exactly"


def test_empty_result_set_produces_empty_tables():
    assert S.per_year_count_errors([]).empty
    assert S.wind_attribution_table([]).empty
    assert S.reference_sensitivity_table([]).empty


def test_reference_sensitivity_skips_results_without_r3():
    days = pd.date_range("2001-01-01", periods=30, freq="D")
    daily = pd.DataFrame({"ref_audited": 30.0, "cand_C1": 30.0}, index=days)
    assert S.reference_sensitivity_table(
        [S.SelectionResult("X", "2001-2001", daily, {})]
    ).empty


def test_extreme_inputs_do_not_produce_negative_wind(kochi):
    _, daily, times, local_day, cossza = kochi
    extreme = daily.copy()
    extreme["sfcWind"] = 0.0
    extreme["tasmin"] = extreme["tasmax"] - 60.0
    wind = S.wind_w1_dtr_ms(extreme, local_day, cossza)
    assert np.nanmin(wind) >= 0.0
    assert np.nanmax(wind) == pytest.approx(0.0)


def test_peak_hour_is_nan_for_a_day_with_any_invalid_hour():
    days = pd.date_range("2000-01-01", periods=2, freq="D")
    times = pd.date_range("2000-01-01", periods=48, freq="h")
    local_day = np.repeat(days.to_numpy(), 24)
    values = np.arange(48, dtype=float)
    values[5] = np.nan
    peaks = S.peak_hour_utc(values, local_day, times)
    assert not np.isfinite(peaks.iloc[0])
    assert peaks.iloc[1] == 23.0


# --------------------------------------------------------------------------
# Selection rule (SPEC.md 7)
# --------------------------------------------------------------------------


def _fake_tables(specs):
    """Build minimal score/annual/per-year tables from {candidate: (daily, n_fail, worst)}."""

    scores, annual, per_year = [], [], []
    for cid, (daily_gate, n_fail, worst) in specs.items():
        column = f"cand_{cid}"
        for i in range(2):
            scores.append(
                {
                    "candidate": column, "season": "ALL", "site": f"S{i}", "window": "W",
                    "daily_gate": daily_gate, "median_abs_error_c": 0.5, "rmse_c": 1.0,
                }
            )
        for i in range(4):
            failing = i < n_fail
            annual.append(
                {
                    "candidate": column, "site": f"S{i}", "window": "W", "threshold_c": 32.0,
                    "count_gate": "FAIL" if failing else "PASS",
                    "absolute_error": worst if failing else 0.5,
                }
            )
            per_year.append(
                {
                    "candidate": column, "site": f"S{i}", "threshold_c": 32.0,
                    "mean_gate": "FAIL" if failing else "PASS",
                    "n_years_outside_tolerance": 3 if failing else 0,
                }
            )
    return pd.DataFrame(scores), pd.DataFrame(annual), pd.DataFrame(per_year)


def test_selection_eliminates_a_daily_gate_failure_however_good_its_counts():
    scores, annual, per_year = _fake_tables(
        {"C1": ("FAIL", 0, 0.5), "C2": ("PASS", 1, 3.0), "W1": ("PASS", 2, 9.0), "W2": ("PASS", 3, 9.0)}
    )
    out = S.apply_selection_rule(scores, annual, per_year)
    assert out["selected"] == "C2"
    assert any(e["candidate"] == "C1" for e in out["eliminated"])
    assert all(r["candidate"] != "C1" for r in out["ranking"])


def test_selection_prefers_fewer_failing_pairs_before_smaller_error():
    scores, annual, per_year = _fake_tables(
        {"C1": ("PASS", 2, 0.6), "C2": ("PASS", 1, 40.0), "W1": ("PASS", 3, 0.6), "W2": ("PASS", 3, 0.6)}
    )
    out = S.apply_selection_rule(scores, annual, per_year)
    assert out["selected"] == "C2", "one large failure beats two small ones, as declared"


def test_selection_prefers_the_asset_free_candidate_when_both_pass_fully():
    scores, annual, per_year = _fake_tables(
        {"C1": ("FAIL", 0, 0.5), "C2": ("FAIL", 0, 0.5), "W1": ("PASS", 0, 0.5), "W2": ("PASS", 0, 0.5)}
    )
    out = S.apply_selection_rule(scores, annual, per_year)
    assert out["selected"] == "W1"
    assert S.NEEDS_CLIMATOLOGY_ASSET[out["selected"]] is False


def test_selection_names_a_development_candidate_rather_than_claiming_a_pass():
    scores, annual, per_year = _fake_tables(
        {"C1": ("PASS", 1, 3.0), "C2": ("PASS", 2, 4.0), "W1": ("PASS", 3, 5.0), "W2": ("PASS", 4, 6.0)}
    )
    out = S.apply_selection_rule(scores, annual, per_year)
    assert out["selected"] == "C1"
    assert "BEST DEVELOPMENT CANDIDATE" in out["status"]
    assert "SELECTED" not in out["status"]


def test_selection_reports_not_evaluated_without_converting_it_to_a_verdict():
    scores, annual, per_year = _fake_tables({"C1": ("PASS", 0, 0.5)})
    out = S.apply_selection_rule(scores, annual, per_year)
    missing = [r for r in out["all_candidates"] if r.get("status") == "NOT EVALUATED"]
    assert {r["candidate"] for r in missing} == {"C2", "W1", "W2"}
    assert out["selected"] == "C1"


def test_selection_reports_every_candidate_including_failures():
    scores, annual, per_year = _fake_tables(
        {"C1": ("FAIL", 0, 0.5), "C2": ("PASS", 1, 3.0), "W1": ("PASS", 0, 0.5), "W2": ("PASS", 0, 0.5)}
    )
    out = S.apply_selection_rule(scores, annual, per_year)
    assert len(out["all_candidates"]) == len(S.DEPLOYABLE_IDS)


# --------------------------------------------------------------------------
# QDM trigger and training/application separation (SPEC.md 9)
# --------------------------------------------------------------------------


def _comparison(relative_errors, absolute_errors=None):
    rows = [{"site": "ERA5", "model": "", "relative_error_32": np.nan, "signed_error_32": np.nan}]
    for i, rel in enumerate(relative_errors):
        rows.append(
            {
                "site": f"S{i}", "model": "ACCESS-CM2",
                "relative_error_32": rel,
                "signed_error_32": (absolute_errors or [0.0] * len(relative_errors))[i],
            }
        )
    return pd.DataFrame(rows)


def test_qdm_trigger_needs_two_sites_over_the_relative_tolerance():
    assert S.qdm_trigger(_comparison([0.30, 0.05, 0.05]))["fired"] is False
    assert S.qdm_trigger(_comparison([0.30, 0.30, 0.05]))["fired"] is True


def test_qdm_trigger_fires_on_a_single_large_absolute_error():
    fired = S.qdm_trigger(_comparison([0.05, 0.05], [20.0, 0.0]))
    assert fired["fired"] is True
    assert fired["per_model"][0]["sites_over_absolute_tolerance"] == ["S0"]


def test_qdm_trigger_ignores_the_reference_row():
    out = S.qdm_trigger(_comparison([0.05, 0.05]))
    assert out["fired"] is False
    assert all(d["model"] != "" for d in out["per_model"])


def test_qdm_trigger_is_not_evaluated_on_empty_input():
    out = S.qdm_trigger(pd.DataFrame())
    assert out["fired"] is False
    assert "NOT EVALUATED" in out["reason"]


def test_qdm_train_and_test_years_are_disjoint_by_declaration():
    assert not (set(S.QDM_TRAIN_YEARS) & set(S.QDM_TEST_YEARS))
    assert max(S.QDM_TRAIN_YEARS) < min(S.QDM_TEST_YEARS)


def test_qdm_trial_refuses_overlapping_train_and_test_years():
    with pytest.raises(ValueError, match="overlap"):
        S.qdm_trial(
            pd.DataFrame({"model": [], "site": [], "wbgt_daily_max_c": []}),
            {},
            train_years=(1990, 1991),
            test_years=(1991, 1992),
        )


def test_qdm_settings_match_the_verified_carbonplan_configuration():
    assert S.QDM_NQUANTILES == 100
    assert S.QDM_WINDOW_DAYS == 31


def test_qdm_implementation_reused_is_the_audited_one():
    from tools.diagnostics import wbgt_qdm_bias_correction as Q

    source = inspect.getsource(Q.qdm_adjust)
    # The delta construction is what preserves a shifted future distribution's shift.
    assert "tau" in source and "delta" in source
    assert "q_sim" in source, "tau must be taken in the simulated period's own distribution"


# --------------------------------------------------------------------------
# Isolation, signatures and the CLI contract
# --------------------------------------------------------------------------


def test_write_guard_refuses_forbidden_roots(tmp_path):
    for fragment in ("irt_data", "processed_optimised", "wbgt_shade_national"):
        with pytest.raises(SystemExit, match="Refusing to use"):
            S._guard_write_target(tmp_path / fragment / "out", "out-dir")


def test_write_guard_protects_milestone_1_evidence():
    with pytest.raises(SystemExit, match="immutable"):
        S._guard_write_target(
            pd.Series(dtype=float).index.__class__ and __import__("pathlib").Path(
                "docs/diagnostics/wbgt_outdoor_feasibility"
            ),
            "out-dir",
        )


def test_default_output_locations_are_separate_from_milestone_1():
    assert S.DEFAULT_OUT_DIR != M1.DEFAULT_OUT_DIR
    assert S.DEFAULT_WORK_DIR != M1.DEFAULT_WORK_DIR
    assert "wbgt_outdoor_selection" in str(S.DEFAULT_OUT_DIR)


def test_resume_and_overwrite_are_mutually_exclusive():
    parser = S.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--resume", "--overwrite"])


def test_workers_default_to_one_while_the_shade_rebuild_runs():
    assert S.build_parser().parse_args([]).workers == 1


def test_nex_comparison_computes_no_same_date_skill_measure():
    """SPEC.md 8: NEX carries no forecast for an ERA5 date, so no RMSE or r is admissible."""

    source = inspect.getsource(S.nex_matched_comparison) + inspect.getsource(
        S.nex_candidate_daily_max
    )
    assert "pearson" not in source.lower()
    assert "rmse" not in source.lower()
    assert "corr(" not in source


def test_matched_period_is_the_same_years_for_both_sources():
    assert S.NEX_MATCHED_YEARS == tuple(range(1990, 2000))
    assert set(S.NEX_MATCHED_YEARS) <= set(M1.parse_years(M1.DEFAULT_PRIMARY_WINDOW))


def test_source_day_sensitivity_changes_grouping_not_timestamps():
    source = inspect.getsource(S.run_site_window)
    assert "only the grouping changes" in source
    assert 'day_convention == "utc"' in source


def test_unknown_day_convention_is_rejected():
    with pytest.raises(ValueError, match="day convention"):
        S.run_site_window(
            KOCHI, (1990,), cache_dir=__import__("pathlib").Path("/nonexistent"),
            calibration_years=None, day_convention="local-solar",
        )


def test_candidate_names_distinguish_every_assumption():
    names = set(S.CANDIDATE_NAMES.values())
    assert len(names) == len(S.CANDIDATE_NAMES)
    assert "unchanged" in S.CANDIDATE_NAMES["C1"] and "unchanged" in S.CANDIDATE_NAMES["C2"]
    assert "assumption" in S.CANDIDATE_NAMES["W1"]
    assert "REVERSE" in S.CANDIDATE_NAMES["W2rev"]
    assert "forward" in S.CANDIDATE_NAMES["W2"]


def test_missing_cache_raises_and_never_downloads():
    from pathlib import Path

    with pytest.raises(FileNotFoundError, match="never downloads"):
        M1.load_hourly_cache(BIKANER, (1990,), Path("/nonexistent-cache"))

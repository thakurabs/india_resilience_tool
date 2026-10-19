"""Contracts of the CarbonPlan shade-WBGT held-out validation (CHG-0647).

These guard the decisions ``docs/diagnostics/wbgt_shade_carbonplan_validation/SPEC.md``
freezes before scoring: held-out reference data never reach training, each
application window is adjusted in its own call, IRT's reporting calendar drops
Feb 29 and refuses incomplete years, missing days stay missing instead of
becoming zero exceedances, thresholds are inclusive and unrounded, threshold
flips are attributed only by the declared rules, and admin values are
count-first area averages with coverage on the original admin area.

Fixtures are synthetic; QDM itself (xclim) is replaced by recording stand-ins,
because what is tested is the separation and windowing around it.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tools.diagnostics import wbgt_shade_cp_validation as V


def daily(start: str, end: str, value=29.0, columns=("a",)) -> pd.DataFrame:
    idx = pd.date_range(start, end, freq="D")
    return pd.DataFrame({c: np.full(len(idx), value, float) for c in columns}, index=idx)


def da(start: str, end: str, seed: int = 0) -> xr.DataArray:
    idx = pd.date_range(start, end, freq="D")
    return xr.DataArray(np.random.default_rng(seed).normal(28, 2, len(idx)), dims="time",
                        coords={"time": idx})


# --------------------------------------------------------------------------- separation and windows

def test_training_never_sees_held_out_reference_or_model_years():
    seen = {}

    def train(ref, raw):
        seen["ref"] = pd.DatetimeIndex(ref.time.values)
        seen["raw"] = pd.DatetimeIndex(raw.time.values)
        return "model"

    applied = []
    V.fit_and_apply(train, lambda m, ts: applied.append(pd.DatetimeIndex(ts.time.values)) or ts,
                    da("1985-01-01", "2014-12-31", 1), da("1985-01-01", "2014-12-31", 2),
                    V.Window(1985, 1999), [V.Window(2000, 2014)])
    assert seen["ref"].year.max() == 1999 and seen["raw"].year.max() == 1999
    assert seen["ref"].year.min() == 1985
    assert applied[0].year.min() == 2000 and applied[0].year.max() == 2014


def test_each_application_window_is_its_own_call():
    calls = []
    out = V.fit_and_apply(lambda r, m: "model",
                          lambda m, ts: calls.append((int(ts.time.dt.year.min()), int(ts.time.dt.year.max()))) or ts,
                          da("1985-01-01", "2014-12-31"), da("1985-01-01", "2014-12-31"),
                          V.Window(1985, 1999), [V.Window(2000, 2007), V.Window(2008, 2014)])
    assert calls == [(2000, 2007), (2008, 2014)]
    assert set(out) == {"2000-2007", "2008-2014"}


def test_overlapping_application_window_is_refused():
    with pytest.raises(ValueError, match="overlaps training"):
        V.fit_and_apply(lambda r, m: "model", lambda m, ts: ts, da("1985-01-01", "2014-12-31"),
                        da("1985-01-01", "2014-12-31"), V.Window(1985, 1999), [V.Window(1999, 2014)])


def test_window_missing_a_year_is_refused():
    full = da("1985-01-01", "2014-12-31")
    gap = full.isel(time=np.flatnonzero(full.time.dt.year.values != 1990))
    with pytest.raises(ValueError, match="lacks years \\[1990\\]"):
        V.select_window(gap, V.Window(1985, 1999))


# --------------------------------------------------------------------------- calendar and counts

def test_irt_calendar_drops_feb29_and_keeps_complete_leap_year():
    d, valid = V.irt_calendar(daily("2000-01-01", "2000-12-31"), V.Window(2000, 2000))
    assert len(d) == 365
    assert not ((d.index.month == 2) & (d.index.day == 29)).any()
    assert bool(valid.loc[2000, "a"])


def test_incomplete_year_is_invalid_not_zero():
    frame = daily("2001-01-01", "2002-12-31", 35.0).drop(pd.Timestamp("2002-07-01"))
    d, valid = V.irt_calendar(frame, V.Window(2001, 2002))
    counts = V.annual_counts(d, valid)[32]
    assert counts.loc[2001, "a"] == 365
    assert np.isnan(counts.loc[2002, "a"])


def test_nan_day_makes_unit_year_missing_not_zero_count():
    frame = daily("2001-01-01", "2001-12-31", 20.0, columns=("a", "b"))
    frame.loc["2001-03-03", "b"] = np.nan
    d, valid = V.irt_calendar(frame, V.Window(2001, 2001))
    counts = V.annual_counts(d, valid)[28]
    assert counts.loc[2001, "a"] == 0
    assert np.isnan(counts.loc[2001, "b"])


def test_all_nan_and_empty_inputs_give_missing_years():
    frame = daily("2001-01-01", "2001-12-31", np.nan)
    d, valid = V.irt_calendar(frame, V.Window(2001, 2001))
    assert np.isnan(V.annual_counts(d, valid)[28].loc[2001, "a"])
    d, valid = V.irt_calendar(frame.iloc[:0], V.Window(2001, 2001))
    assert not valid.to_numpy().any()


def test_threshold_is_inclusive_and_unrounded():
    frame = daily("2001-01-01", "2001-12-31", 27.0)
    frame.iloc[0, 0] = 28.0
    frame.iloc[1, 0] = 27.99995
    frame.iloc[2, 0] = 1e6
    d, valid = V.irt_calendar(frame, V.Window(2001, 2001))
    assert V.annual_counts(d, valid)[28].loc[2001, "a"] == 2


# --------------------------------------------------------------------------- scoring

def test_support_classes_follow_reference_days_not_model():
    assert list(V.support_class(np.array([0, 3, 15, np.nan]), 15)) == ["zero", "sparse", "supported", "missing"]


def test_zero_zero_is_excluded_from_improvement_classes():
    labels = V.classify_change(np.array([0.0, 2.0, 1.0, np.nan]), np.array([0.0, 0.5, 1.2, 1.0]), 0.5,
                               zero_zero=np.array([True, False, False, False]))
    assert list(labels) == ["zero/zero", "improved", "unchanged", "missing"]


def test_primary_errors_on_constant_and_tied_series():
    x = np.full((730, 1), 30.0)
    ref = np.full((730, 1), 30.0)
    months = np.tile(pd.date_range("2001-01-01", periods=365).month.to_numpy(), 2)
    e = V.primary_errors(x, ref, months, months, 2)
    assert all(np.allclose(v, 0) for v in e.values())
    assert e["days_ge_30_error"][0] == 0


def test_primary_errors_refuse_non_finite_input():
    x = np.full((365, 1), 30.0)
    x[5] = np.nan
    m = pd.date_range("2001-01-01", periods=365).month.to_numpy()
    with pytest.raises(ValueError, match="finite"):
        V.primary_errors(x, x.copy(), m, m, 1)


def test_screening_needs_interval_and_share_not_just_median():
    raw = np.array([1.0, 1.0, 1.0])
    qdm = np.array([0.1, 0.1, 2.0])
    labels = V.classify_change(raw, qdm, 0.05)
    assert V.screening_verdict(raw, qdm, labels, (-0.9, -0.1)) == "improves"
    assert V.screening_verdict(raw, qdm, labels, (-0.9, 0.2)) == "mixed/inconclusive"


def test_bootstrap_is_repeatable_and_resamples_whole_years():
    years = np.repeat(np.arange(2000, 2015), 365)
    months = np.tile(pd.date_range("2001-01-01", periods=365).month.to_numpy(), 15)
    rng = np.random.default_rng(3)
    ref = rng.normal(28, 2, (len(years), 4))
    raw, qdm = ref - 1.5, ref + 0.1
    masks = {"p95_bias": np.ones(4, bool)}
    a = V.bootstrap_median_change(raw, qdm, ref, years, years, months, months, masks, n_boot=50)
    b = V.bootstrap_median_change(raw, qdm, ref, years, years, months, months, masks, n_boot=50)
    assert a == b
    idx = V.year_resample_indices(years, np.random.default_rng(0))
    assert len(idx) == len(years) and (np.bincount(years[idx] - 2000, minlength=15) % 365 == 0).all()


# --------------------------------------------------------------------------- fidelity attribution

def test_flip_attribution_proximity_tie_and_unexplained():
    idx = pd.date_range("2001-01-01", periods=40)
    raw = pd.Series(np.linspace(20, 21, 40), index=idx)
    raw.iloc[15] = raw.iloc[10] + 5e-5                       # tie partner of day 10
    mine = pd.Series(25.0, index=idx)
    pub = mine.copy()
    mine.iloc[0], pub.iloc[0] = 28.00002, 27.99995           # proximity flip
    mine.iloc[10], pub.iloc[10] = 28.2, 27.9                 # tie flip (+0.3)
    mine.iloc[15], pub.iloc[15] = 25.0, 25.3                 # its equal-and-opposite partner
    mine.iloc[20], pub.iloc[20] = 30.5, 29.5                 # unexplained
    f = V.threshold_flips(mine, pub, raw, thresholds=(28, 30)).set_index(["date", "threshold"])
    assert f.loc[(idx[0], 28), "explanation"] == "threshold_proximity"
    assert f.loc[(idx[10], 28), "explanation"] == "tie_pair"
    assert f.loc[(idx[20], 30), "explanation"] == "unexplained"
    assert f.loc[(idx[0], 28), "direction"] == 1


# --------------------------------------------------------------------------- transfer

def crosswalk() -> pd.DataFrame:
    return pd.DataFrame({
        "admin": ["A", "A", "B", "C"],
        "processing_id": [1, 2, 2, -1],
        "intersection_m2": [30.0, 50.0, 100.0, 0.0],
        "unit_area_m2": [100.0, 100.0, 100.0, 50.0],
        "union_m2": [80.0, 80.0, 100.0, 0.0]})


def nested_crosswalk() -> pd.DataFrame:
    """Admin square 10x10 with source 1 covering all of it and source 2 nested inside 1."""
    shapely = pytest.importorskip("shapely")
    from shapely.geometry import box

    admin = box(0, 0, 10, 10)
    sources = {1: box(0, 0, 10, 10), 2: box(2, 2, 7, 7)}
    total, union = V.union_intersection_area(admin, list(sources.values()))
    assert shapely is not None and np.isclose(total, 125.0) and np.isclose(union, 100.0)
    rows = [{"admin": "N", "processing_id": pid, "intersection_m2": admin.intersection(g).area,
             "unit_area_m2": admin.area, "union_m2": union} for pid, g in sources.items()]
    rows.append({"admin": "M", "processing_id": 1, "intersection_m2": 100.0, "unit_area_m2": 100.0,
                 "union_m2": 100.0})
    return pd.DataFrame(rows)


def test_nested_sources_give_union_coverage_not_summed_coverage():
    w = V.crosswalk_weights(nested_crosswalk(), ["admin"])
    n = w[w.admin == "N"]
    # the summed denominator reported 125 % here (CHG-0652); the union caps it at 100 %
    assert np.isclose(n.coverage.iloc[0], 1.0) and (w.coverage <= 1.0 + 1e-12).all()
    assert np.isclose(n.overlap_fraction.iloc[0], 0.25)
    assert np.isclose(w[w.admin == "M"].overlap_fraction.iloc[0], 0.0)


def test_summed_coverage_is_refused_without_union():
    with pytest.raises(ValueError, match="union_m2"):
        V.crosswalk_weights(crosswalk().drop(columns="union_m2"), ["admin"])


def test_overlapping_admin_is_excluded_with_missing_values_not_renormalised():
    w = V.crosswalk_weights(nested_crosswalk(), ["admin"])
    status = V.overlap_status(w, ["admin"]).set_index("admin")
    assert status.loc["N", "excluded"] and not status.loc["M", "excluded"]
    assert status.loc["N", "exclusion_reason"] == V.OVERLAP_EXCLUSION
    values = pd.DataFrame({"processing_id": [1, 2], "slug": "days_ge_30", "year": 2001, "value": [10.0, 20.0]})
    out = V.count_first_transfer(values, w, ["admin"], excluded=status.reset_index()).set_index("admin")
    assert np.isnan(out.loc["N", "value"]) and out.loc["N", "exclusion_reason"] == V.OVERLAP_EXCLUSION
    assert np.isclose(out.loc["M", "value"], 10.0) and out.loc["M", "exclusion_reason"] == ""


def test_overlap_below_tolerance_is_reported_but_not_excluded():
    cw = pd.DataFrame({"admin": ["T", "T"], "processing_id": [1, 2], "intersection_m2": [60.0, 40.00001],
                       "unit_area_m2": [100.0, 100.0], "union_m2": [100.0, 100.0]})
    status = V.overlap_status(V.crosswalk_weights(cw, ["admin"]), ["admin"])
    assert status.overlap_m2.iloc[0] > 0 and not status.excluded.iloc[0]


def test_crosswalk_normalises_over_cover_and_reports_original_denominator():
    w = V.crosswalk_weights(crosswalk(), ["admin"])
    a = w[w.admin == "A"]
    assert np.isclose(a.w.sum(), 1.0)
    assert np.isclose(a.coverage.iloc[0], 0.8)
    c = w[w.admin == "C"]
    assert c.reason.iloc[0] == "no coverage" and np.isnan(c.w.iloc[0])


def test_transfer_is_count_first_and_missing_unit_is_uncovered():
    w = V.crosswalk_weights(crosswalk(), ["admin"])
    # unit 1 is always hot, unit 2 never: count-first gives the area mix, not a count of a mean.
    values = pd.DataFrame({"processing_id": [1, 2, 1, 2], "slug": "days_ge_30",
                           "year": [2001, 2001, 2002, 2002], "value": [365.0, 0.0, np.nan, 0.0]})
    out = V.count_first_transfer(values, w, ["admin"]).set_index(["admin", "year"])
    assert np.isclose(out.loc[("A", 2001), "value"], 365 * 30 / 80)
    assert np.isclose(out.loc[("A", 2002), "value"], 0.0)
    assert np.isclose(out.loc[("A", 2002), "series_coverage"], 0.5)
    assert ("C", 2001) not in out.index

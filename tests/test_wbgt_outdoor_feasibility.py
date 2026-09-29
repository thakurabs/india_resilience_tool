"""Tests for the outdoor-WBGT reconstruction feasibility harness (CHG-0604).

These assert physical and data-contract properties from
``docs/diagnostics/wbgt_outdoor_feasibility/SPEC.md``, not the implementation's own arithmetic
restated.  Every test runs on small synthetic inputs with no network and no NEX access.
"""

from __future__ import annotations

import inspect

import numpy as np
import pandas as pd
import pytest

from tools.diagnostics import wbgt_outdoor_feasibility as M
from tools.diagnostics.wbgt_method_validation import SITES

KOCHI = next(s for s in SITES if s.name == "Kochi")
SHIMLA = next(s for s in SITES if s.name == "Shimla")

pytest.importorskip("thermofeel", reason="physical solver checks need thermofeel")


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


def _chain(site, days: int = 30, seed: int = 11):
    """Synthetic hourly ERA5-like series -> geometry -> daily inputs -> reconstruction."""

    hourly = M.attach_geometry(site, M._synthetic_hourly(site, days=days, seed=seed))
    keep = M.complete_local_days(hourly)
    hourly = hourly[hourly["local_day"].isin(keep)]
    inputs = M.aggregate_daily_inputs(hourly)
    times = pd.DatetimeIndex(hourly.index)
    local_day = hourly["local_day"].to_numpy()
    cossza = hourly["cossza_mid"].to_numpy(dtype=float)
    static = M.SiteStatic.from_site(site)
    recon = M.reconstruct_hourly(
        times, local_day, inputs, static, M.CANDIDATE_PARAMS["C1"], cossza=cossza
    )
    return hourly, keep, inputs, times, local_day, cossza, static, recon


@pytest.fixture(scope="module")
def chain():
    return _chain(KOCHI)


# --------------------------------------------------------------------------
# Units and wind-height treatment
# --------------------------------------------------------------------------


def test_liljegren_wrapper_is_documented_to_take_10m_wind_and_converts_internally():
    """The wrapper must NOT pre-convert to 2 m: thermofeel does it, so we would double-count."""

    from thermofeel import calculate_wbgt_liljegren
    from thermofeel import liljegren as lj

    doc = calculate_wbgt_liljegren.__doc__ or ""
    assert "wind speed at 10 metres" in doc
    # And the library owns both guards, so the caller must not apply either.
    assert lj.MIN_WIND_10M == pytest.approx(0.62)
    assert lj.MIN_SPEED == pytest.approx(0.13)

    # A 10 m input is scaled DOWN internally: the 2 m speed is strictly smaller for a
    # well-above-floor wind, which is only true if the conversion happens inside.
    speed_2m = lj.wind_speed_2m(np.array([5.0]), np.array([0.8]), np.array([800.0]))
    assert 0.13 < speed_2m[0] < 5.0


def test_pressure_unit_is_hpa_not_pa():
    """Passing pascals instead of hectopascals must not silently produce a plausible number."""

    args = (
        np.array([308.15]),  # K
        np.array([55.0]),  # %
        np.array([1000.0]),  # hPa
        np.array([2.0]),  # m/s at 10 m
        np.array([800.0]),  # W/m2
        np.array([0.7]),
        np.array([0.9]),
    )
    good = M.liljegren_wbgt_c(args[0] - 273.15, args[1], args[2], *args[3:])
    bad = M.liljegren_wbgt_c(args[0] - 273.15, args[1], args[2] * 100.0, *args[3:])
    assert np.isfinite(good[0])
    assert not np.isclose(good[0], bad[0], atol=0.5)


def test_barometric_pressure_matches_isa_and_falls_with_elevation():
    assert M.barometric_pressure_hpa(0.0) == pytest.approx(1013.25, abs=1e-6)
    assert M.barometric_pressure_hpa(2276.0) < M.barometric_pressure_hpa(505.0) < M.barometric_pressure_hpa(3.0)
    # Roughly 12 hPa per 100 m near sea level.
    assert 10.0 < M.barometric_pressure_hpa(0.0) - M.barometric_pressure_hpa(100.0) < 14.0


def test_high_elevation_pressure_changes_the_answer():
    """Pressure is physical, not cosmetic: Shimla's 768 hPa must move WBGT measurably."""

    base = dict(
        t_c=np.array([25.0]),
        rh_pct=np.array([40.0]),
        wind_10m_ms=np.array([1.5]),
        ssrd_w_m2=np.array([850.0]),
        fdir_frac=np.array([0.75]),
        cossza=np.array([0.9]),
    )
    sea = M.liljegren_wbgt_c(pressure_hpa=np.array([1013.25]), **base)
    hill = M.liljegren_wbgt_c(
        pressure_hpa=M.barometric_pressure_hpa(np.array([SHIMLA.elevation_m])), **base
    )
    assert np.isfinite(sea[0]) and np.isfinite(hill[0])
    assert abs(sea[0] - hill[0]) > 0.05


# --------------------------------------------------------------------------
# Radiation: energy conservation, night zeros, partition
# --------------------------------------------------------------------------


def test_radiation_energy_is_conserved_to_declared_tolerance(chain):
    _, keep, inputs, times, local_day, _, _, recon = chain
    recon_mean = (
        pd.Series(recon["ssrd_w_m2"].to_numpy(), index=times).groupby(local_day).mean()
    )
    target = inputs.frame["rsds"].reindex(pd.DatetimeIndex(recon_mean.index))
    rel = (recon_mean.to_numpy() - target.to_numpy()) / target.to_numpy()
    assert np.nanmax(np.abs(rel)) < M.RADIATION_ENERGY_RTOL


def test_night_shortwave_is_exactly_zero_and_daytime_is_non_negative(chain):
    _, _, _, _, _, _, _, recon = chain
    ssrd = recon["ssrd_w_m2"].to_numpy()
    toa = recon["toa_w_m2"].to_numpy()
    assert np.all(ssrd[toa <= 0.0] == 0.0)
    assert np.all(ssrd >= 0.0)
    assert np.all(recon["fdir_frac"].to_numpy()[toa <= 0.0] == 0.0)


def test_direct_fraction_stays_in_unit_interval(chain):
    _, _, _, _, _, _, _, recon = chain
    fdir = recon["fdir_frac"].to_numpy()
    assert np.all((fdir >= 0.0) & (fdir <= 1.0))


def test_erbs_diffuse_fraction_bounds_and_endpoint_values():
    kt = np.linspace(0.0, 1.2, 241)
    kd = M.erbs_diffuse_fraction(kt)
    assert np.all((kd >= 0.0) & (kd <= 1.0))
    assert M.erbs_diffuse_fraction(np.array([0.0]))[0] == pytest.approx(1.0)
    assert M.erbs_diffuse_fraction(np.array([0.95]))[0] == pytest.approx(0.165)
    # Clearer skies mean less diffuse.
    assert M.erbs_diffuse_fraction(np.array([0.15]))[0] > M.erbs_diffuse_fraction(np.array([0.65]))[0]


def test_zero_insolation_day_returns_zeros_not_nan():
    """A day whose TOA shape is zero must return zeros rather than dividing by zero."""

    times = pd.date_range("2001-01-01", periods=24, freq="h")
    day = pd.DatetimeIndex(np.repeat(pd.Timestamp("2001-01-01"), 24))
    daily = pd.DataFrame(
        {
            "tas": [20.0],
            "tasmin": [15.0],
            "tasmax": [25.0],
            "hurs": [60.0],
            "rsds": [200.0],
            "sfcWind": [2.0],
        },
        index=pd.DatetimeIndex(["2001-01-01"]),
    )
    # Latitude 89.9 N in January: the sun never rises, so the shape is identically zero.
    static = M.SiteStatic("PolarNight", 89.9, 0.0, 0.0)
    ssrd, fdir, toa = M.reconstruct_radiation_w_m2(times, daily, static, day.to_numpy())
    assert np.all(toa == 0.0)
    assert np.all(ssrd == 0.0)
    assert np.all(fdir == 0.0)
    assert np.all(np.isfinite(ssrd))


# --------------------------------------------------------------------------
# Time alignment and day boundaries
# --------------------------------------------------------------------------


def test_local_day_uses_ist_and_partial_boundary_days_are_dropped():
    hourly = M.attach_geometry(KOCHI, M._synthetic_hourly(KOCHI, days=5))
    times = pd.DatetimeIndex(hourly.index)
    expected = pd.DatetimeIndex((times + M.IST_OFFSET).date)
    assert list(hourly["local_day"]) == list(expected)

    keep = M.complete_local_days(hourly)
    counts = hourly.groupby("local_day").size()
    assert set(keep) == {d for d, n in counts.items() if n == 24}
    assert counts.min() < 24  # the synthetic record does have partial boundary days
    assert all(counts[d] == 24 for d in keep)


def test_audited_cossza_leads_the_labelled_one_in_the_morning():
    """The audited identity evaluates geometry half an hour earlier than the label."""

    hourly = M.attach_geometry(KOCHI, M._synthetic_hourly(KOCHI, days=3))
    label = hourly["cossza_label"].to_numpy()
    mid = hourly["cossza_mid"].to_numpy()
    assert not np.allclose(label, mid)
    # Before solar noon the sun is climbing, so the earlier instant is lower.
    morning = hourly.index.hour < 6  # Kochi solar noon is ~06:55 UTC
    rising = morning & (label > 0.05)
    assert np.all(mid[rising] < label[rising])
    assert M.RADIATION_MIDPOINT_OFFSET == pd.Timedelta(minutes=-30)


def test_solar_events_ordered_and_seasonal_daylength_varies():
    dates = pd.DatetimeIndex(["2001-06-21", "2001-12-21"])
    events = M.solar_events_utc(SHIMLA.lat, SHIMLA.lon, dates)
    assert (events["sunrise_h"] < events["noon_h"]).all()
    assert (events["noon_h"] < events["sunset_h"]).all()
    assert not events["polar"].any()
    june = events["sunset_h"].iloc[0] - events["sunrise_h"].iloc[0]
    december = events["sunset_h"].iloc[1] - events["sunrise_h"].iloc[1]
    assert june > december  # 31 N: longer days at the June solstice


def test_polar_case_is_flagged_rather_than_producing_garbage():
    events = M.solar_events_utc(89.0, 0.0, pd.DatetimeIndex(["2001-01-15"]))
    assert bool(events["polar"].iloc[0])


# --------------------------------------------------------------------------
# Temperature and humidity reconstruction
# --------------------------------------------------------------------------


def test_reconstructed_peak_recovers_the_daily_tasmax(chain):
    _, keep, inputs, times, local_day, _, _, recon = chain
    interior = keep[1:-1]
    peak = pd.Series(recon["t_c"].to_numpy(), index=times).groupby(local_day).max().reindex(interior)
    assert (peak - inputs.frame["tasmax"].reindex(interior)).abs().max() < 0.35


def test_temperature_nan_is_confined_to_the_record_boundary(chain):
    _, keep, _, _, local_day, _, _, recon = chain
    bad = ~np.isfinite(recon["t_c"].to_numpy())
    nan_days = set(pd.DatetimeIndex(local_day[bad]).unique())
    assert nan_days.issubset({keep[0], keep[-1]})


def test_humidity_bounds_and_the_reported_consistency_diagnostic(chain):
    _, keep, inputs, times, local_day, _, _, recon = chain
    rh = recon["rh_pct"].to_numpy()
    finite = rh[np.isfinite(rh)]
    assert np.all((finite >= 0.0) & (finite <= 100.0))
    # The constant-e assumption does not preserve daily-mean RH; that residual must be
    # measurable, which is why the harness reports it rather than asserting it is zero.
    recon_hurs = pd.Series(rh, index=times).groupby(local_day).mean().reindex(keep[1:-1])
    residual = recon_hurs - inputs.frame["hurs"].reindex(keep[1:-1])
    assert np.isfinite(residual).all()
    assert residual.abs().max() > 0.0


def test_constant_relative_humidity_variant_reproduces_the_daily_mean_exactly(chain):
    _, keep, inputs, times, local_day, cossza, static, _ = chain
    recon = M.reconstruct_hourly(
        times, local_day, inputs, static, M.CANDIDATE_PARAMS["C3"], cossza=cossza
    )
    hourly_mean = (
        pd.Series(recon["rh_pct"].to_numpy(), index=times).groupby(local_day).mean()
    )
    target = inputs.frame["hurs"].reindex(pd.DatetimeIndex(hourly_mean.index))
    assert np.nanmax(np.abs(hourly_mean.to_numpy() - target.to_numpy())) < 1e-9


def test_very_dry_and_very_humid_conditions_stay_physical():
    times = pd.date_range("2001-05-01", periods=24, freq="h")
    day = pd.DatetimeIndex(np.repeat(pd.Timestamp("2001-05-01"), 24)).to_numpy()
    t_hourly = np.linspace(25.0, 48.0, 24)
    for hurs in (2.0, 99.5):
        daily = pd.DataFrame(
            {"tas": [35.0], "hurs": [hurs]},
            index=pd.DatetimeIndex(["2001-05-01"]),
        )
        rh, clipped = M.reconstruct_humidity_pct(
            t_hourly, daily, day, M.CANDIDATE_PARAMS["C1"]
        )
        assert np.all((rh >= 0.0) & (rh <= 100.0))
        assert clipped.dtype == bool
    del times


# --------------------------------------------------------------------------
# Wind
# --------------------------------------------------------------------------


def test_constant_wind_treatment_is_literally_the_daily_mean(chain):
    _, _, inputs, times, local_day, cossza, _, _ = chain
    wind = M.reconstruct_wind_10m_ms(
        inputs.frame, local_day, cossza, M.CANDIDATE_PARAMS["C1"]
    )
    expected = inputs.frame["sfcWind"].reindex(pd.DatetimeIndex(local_day)).to_numpy()
    assert np.allclose(wind, expected, equal_nan=True)


def test_diurnal_wind_shape_preserves_the_daily_mean_and_peaks_by_day(chain):
    _, _, inputs, times, local_day, cossza, _, _ = chain
    wind = M.reconstruct_wind_10m_ms(
        inputs.frame, local_day, cossza, M.CANDIDATE_PARAMS["C2"]
    )
    series = pd.Series(wind, index=times)
    per_day = series.groupby(local_day).mean()
    target = inputs.frame["sfcWind"].reindex(pd.DatetimeIndex(per_day.index))
    assert np.nanmax(np.abs(per_day.to_numpy() - target.to_numpy())) < 1e-9
    # Daytime wind must exceed night-time wind under the declared shape.
    daytime = cossza > 0.5
    assert np.nanmean(wind[daytime]) > np.nanmean(wind[~daytime])


def test_very_low_wind_reaches_the_library_floor_rather_than_diverging():
    calm = M.liljegren_wbgt_c(
        np.array([38.0]),
        np.array([60.0]),
        np.array([1000.0]),
        np.array([0.0]),  # dead calm at 10 m
        np.array([900.0]),
        np.array([0.8]),
        np.array([0.95]),
    )
    breezy = M.liljegren_wbgt_c(
        np.array([38.0]),
        np.array([60.0]),
        np.array([1000.0]),
        np.array([0.62]),  # exactly the library's 10 m floor
        np.array([900.0]),
        np.array([0.8]),
        np.array([0.95]),
    )
    assert np.isfinite(calm[0])
    assert calm[0] == pytest.approx(breezy[0], abs=1e-9)


# --------------------------------------------------------------------------
# Daily reduction and missing data
# --------------------------------------------------------------------------


def test_daily_maximum_is_taken_after_the_hourly_calculation(chain):
    """max(WBGT(hour)) must differ from WBGT(max T, max RH, max rsds) -- the forbidden shortcut."""

    hourly, keep, _, times, local_day, cossza, _, recon = chain
    per_hour = M.liljegren_wbgt_c(
        recon["t_c"].to_numpy(),
        recon["rh_pct"].to_numpy(),
        recon["pressure_hpa"].to_numpy(),
        recon["wind_10m_ms"].to_numpy(),
        recon["ssrd_w_m2"].to_numpy(),
        recon["fdir_frac"].to_numpy(),
        cossza,
    )
    proper = M.daily_max(per_hour, local_day, keep)

    frame = recon.assign(day=local_day, cz=cossza)
    grouped = frame.groupby("day")
    shortcut = M.liljegren_wbgt_c(
        grouped["t_c"].max().reindex(keep).to_numpy(),
        grouped["rh_pct"].max().reindex(keep).to_numpy(),
        grouped["pressure_hpa"].max().reindex(keep).to_numpy(),
        grouped["wind_10m_ms"].max().reindex(keep).to_numpy(),
        grouped["ssrd_w_m2"].max().reindex(keep).to_numpy(),
        grouped["fdir_frac"].max().reindex(keep).to_numpy(),
        grouped["cz"].max().reindex(keep).to_numpy(),
    )
    interior = proper.iloc[1:-1].to_numpy()
    assert np.isfinite(interior).all()
    assert not np.allclose(interior, shortcut[1:-1], atol=0.25)


def test_one_invalid_hour_makes_the_day_nan_not_a_non_exceedance():
    day = pd.DatetimeIndex(np.repeat(pd.Timestamp("2001-05-01"), 24)).to_numpy()
    keep = pd.DatetimeIndex(["2001-05-01"])
    values = np.full(24, 35.0)
    assert M.daily_max(values, day, keep).iloc[0] == pytest.approx(35.0)
    values[7] = np.nan
    holed = M.daily_max(values, day, keep)
    assert pd.isna(holed.iloc[0])
    # And the count of a NaN day is not silently a zero exceedance.
    counts = M.annual_counts(holed, [2001])
    assert counts.empty or counts["days_ge_32"].sum() == 0
    assert M.complete_years(holed) == []


def test_all_nan_and_empty_inputs_do_not_raise():
    day = pd.DatetimeIndex(np.repeat(pd.Timestamp("2001-05-01"), 24)).to_numpy()
    keep = pd.DatetimeIndex(["2001-05-01"])
    assert pd.isna(M.daily_max(np.full(24, np.nan), day, keep).iloc[0])

    empty = pd.Series(dtype=float, index=pd.DatetimeIndex([]))
    row = M.score_pair(
        empty,
        empty,
        site="X",
        window="w",
        season="ALL",
        reference_name="r",
        candidate_name="c",
        kind="k",
    )
    assert row["n_valid_days"] == 0
    assert "rmse_c" not in row
    assert M.complete_years(empty) == []


def test_single_day_series_scores_without_error():
    index = pd.DatetimeIndex(["2001-05-01"])
    row = M.score_pair(
        pd.Series([30.0], index=index),
        pd.Series([31.0], index=index),
        site="X",
        window="w",
        season="ALL",
        reference_name="r",
        candidate_name="c",
        kind="k",
    )
    assert row["n_valid_days"] == 1
    assert row["bias_c"] == pytest.approx(1.0)
    assert np.isnan(row["pearson_r"])
    assert row["daily_gate"] == "FAIL"  # 1.0 C is not < 1.0 C


def test_leap_day_is_dropped_and_a_leap_year_still_needs_365_valid_days():
    index = pd.date_range("2004-01-01", "2004-12-31", freq="D")  # 366 days
    series = pd.Series(np.full(len(index), 33.0), index=index)
    assert M.complete_years(series) == [2004]
    counts = M.annual_counts(series, [2004])
    assert counts.loc[2004, "days_ge_32"] == 365.0  # 29 February excluded

    holed = series.copy()
    holed.loc["2004-07-01"] = np.nan
    assert M.complete_years(holed) == []


def test_short_year_is_excluded_from_annual_counts():
    index = pd.date_range("2001-01-01", periods=300, freq="D")
    series = pd.Series(np.full(300, 33.0), index=index)
    assert M.complete_years(series) == []


# --------------------------------------------------------------------------
# Leakage prevention
# --------------------------------------------------------------------------


def test_daily_inputs_refuses_hourly_columns():
    frame = pd.DataFrame(
        {v: [1.0] for v in M.REQUIRED_NEX_VARIABLES},
        index=pd.DatetimeIndex(["2001-01-01"]),
    )
    M.DailyInputs(frame)  # accepted
    with pytest.raises(ValueError, match="refusing hourly leakage"):
        M.DailyInputs(frame.assign(temperature_2m=[30.0]))
    with pytest.raises(ValueError, match="missing required columns"):
        M.DailyInputs(frame.drop(columns=["rsds"]))


def test_reconstruction_signature_accepts_no_hourly_weather():
    """A deployable candidate may read daily inputs, static site data and solar geometry only."""

    params = set(inspect.signature(M.reconstruct_hourly).parameters)
    assert params == {"times", "local_day", "daily_inputs", "static", "params", "cossza"}
    for name in ("reconstruct_temperature_c", "reconstruct_radiation_w_m2"):
        text = inspect.getsource(getattr(M, name))
        for forbidden in (
            "temperature_2m",
            "relative_humidity_2m",
            "shortwave_radiation",
            "direct_radiation",
            "surface_pressure",
            "wind_speed_10m",
            "dew_point_2m",
        ):
            assert forbidden not in text, f"{name} reads hourly field {forbidden}"


def test_static_site_information_carries_no_weather():
    static = M.SiteStatic.from_site(KOCHI)
    assert {f for f in static.__dataclass_fields__} == {"name", "lat", "lon", "elevation_m"}


# --------------------------------------------------------------------------
# Scoring contract
# --------------------------------------------------------------------------


def test_quantile_difference_and_conditional_error_are_different_statistics():
    index = pd.date_range("2001-01-01", periods=400, freq="D")
    rng = np.random.default_rng(3)
    reference = pd.Series(rng.normal(30.0, 3.0, len(index)), index=index)
    # A candidate that is right on average but wrong specifically on the hottest days.
    candidate = reference + np.where(reference > reference.quantile(0.99), -4.0, 0.04)
    row = M.score_pair(
        reference,
        candidate,
        site="X",
        window="w",
        season="ALL",
        reference_name="r",
        candidate_name="c",
        kind="k",
    )
    assert row["cond_mean_error_hottest_1pct_c"] < -3.0
    assert row["q99_difference_c"] > row["cond_mean_error_hottest_1pct_c"]
    assert abs(row["bias_c"]) < 0.2


def test_median_abs_error_matches_the_legacy_harness_definition():
    """Continuity: the earlier harness's median_abs_bias_c is median(|cand - ref|)."""

    index = pd.date_range("2001-01-01", periods=5, freq="D")
    reference = pd.Series([30.0, 30.0, 30.0, 30.0, 30.0], index=index)
    candidate = pd.Series([31.0, 29.0, 33.0, 30.5, 28.0], index=index)
    row = M.score_pair(
        reference,
        candidate,
        site="X",
        window="w",
        season="ALL",
        reference_name="r",
        candidate_name="c",
        kind="k",
    )
    assert row["median_abs_error_c"] == pytest.approx(np.median([1.0, 1.0, 3.0, 0.5, 2.0]))
    assert row["rmse_c"] == pytest.approx(np.sqrt(np.mean([1.0, 1.0, 9.0, 0.25, 4.0])))
    assert row["bias_c"] == pytest.approx(np.mean([1.0, -1.0, 3.0, 0.5, -2.0]))


def test_count_gate_applies_the_predeclared_policy():
    # Rare events are never scored as a pass or a fail.
    verdict, tol = M.count_gate(3.0, 99.0)
    assert verdict == "NOT GATED (rare event)"
    assert np.isnan(tol)
    # The absolute floor governs small but gated bases.
    assert M.count_gate(6.0, 7.9)[0] == "PASS"
    assert M.count_gate(6.0, 8.1)[0] == "FAIL"
    assert M.count_gate(6.0, 0.0)[1] == pytest.approx(M.COUNT_GATE_ABS_FLOOR_PER_YEAR)
    # The relative band governs large bases.
    assert M.count_gate(100.0, 119.0)[0] == "PASS"
    assert M.count_gate(100.0, 121.0)[0] == "FAIL"
    assert M.count_gate(np.nan, 5.0)[0] == "NOT GATED (rare event)"


def test_year_block_bootstrap_resamples_years_not_days():
    index = pd.date_range("2001-01-01", "2005-12-31", freq="D")
    years = pd.DatetimeIndex(index).year
    reference = pd.Series(np.zeros(len(index)), index=index)
    # Each year is internally constant, so a day-level bootstrap would give a near-zero
    # interval while a year-block bootstrap must span the between-year spread.
    candidate = pd.Series((years - 2001).astype(float), index=index)
    lo, hi = M.year_block_bootstrap(reference, candidate, statistic="bias_c")
    assert hi - lo > 0.5
    assert lo >= 0.0 and hi <= 4.0


def test_bootstrap_needs_more_than_one_year_block():
    index = pd.date_range("2001-01-01", periods=100, freq="D")
    series = pd.Series(np.zeros(100), index=index)
    assert all(np.isnan(v) for v in M.year_block_bootstrap(series, series + 1.0, statistic="bias_c"))


def test_unsupported_bootstrap_statistic_raises():
    index = pd.date_range("2001-01-01", periods=400, freq="D")
    series = pd.Series(np.zeros(400), index=index)
    with pytest.raises(ValueError, match="Unsupported bootstrap statistic"):
        M.year_block_bootstrap(series, series, statistic="mode")


# --------------------------------------------------------------------------
# Inventory, resume and write safety
# --------------------------------------------------------------------------


def test_required_variable_intersection_marks_incomplete_model_years():
    presence = pd.DataFrame(
        [
            {"scenario": "historical", "variable": v, "model": "M1", "year": 1990, "present": True, "note": ""}
            for v in M.REQUIRED_NEX_VARIABLES
        ]
        + [
            {"scenario": "historical", "variable": "tas", "model": "M2", "year": 1990, "present": True, "note": ""},
        ]
    )
    out = M.nex_required_intersection(presence)
    complete = out.set_index(["model", "year"])["complete"]
    assert bool(complete[("M1", 1990)])
    assert not bool(complete[("M2", 1990)])


def test_required_variables_cover_the_physical_candidate():
    assert set(M.REQUIRED_NEX_VARIABLES) == {"tas", "tasmin", "tasmax", "hurs", "rsds", "sfcWind"}
    assert "ps" not in M.REQUIRED_NEX_VARIABLES  # NEX publishes none; ISA is used instead
    assert M.NEX_WBGT_TREE_VARIABLES == {"rsds", "sfcWind"}


def test_write_targets_inside_production_or_the_shade_stage_are_refused(tmp_path):
    for fragment in ("irt_data", "processed_optimised", "wbgt_shade_national"):
        with pytest.raises(SystemExit, match="Refusing to use"):
            M._guard_write_target(tmp_path / fragment / "out", "--out-dir")
    assert M._guard_write_target(tmp_path / "safe", "--out-dir").name == "safe"


def test_resume_and_overwrite_are_mutually_exclusive():
    with pytest.raises(SystemExit, match="mutually exclusive"):
        M.main(["--resume", "--overwrite"])


def test_worker_count_is_bounded():
    with pytest.raises(SystemExit, match="bounded at 4"):
        M.main(["--workers", "36"])
    with pytest.raises(SystemExit, match="must be >= 1"):
        M.main(["--workers", "0"])


def test_year_parsing_rejects_descending_ranges_and_handles_lists():
    assert M.parse_years("1990-1992") == (1990, 1991, 1992)
    assert M.parse_years("1990,1995") == (1990, 1995)
    with pytest.raises(ValueError, match="Descending"):
        M.parse_years("2004-1990")
    with pytest.raises(ValueError, match="No years parsed"):
        M.parse_years(" ")


def test_missing_hourly_cache_year_raises_and_never_downloads(tmp_path):
    with pytest.raises(FileNotFoundError, match="never downloads"):
        M.load_hourly_cache(KOCHI, [1990], tmp_path)


def test_candidate_parameters_are_declared_and_signed():
    assert set(M.CANDIDATE_PARAMS) == set(M.CANDIDATE_IDS)
    signatures = {cid: M.CANDIDATE_PARAMS[cid].signature() for cid in M.CANDIDATE_IDS}
    assert len(set(signatures.values())) == len(signatures)
    assert "vapour_pressure" in signatures["C1"]
    assert "wind-diurnal" in signatures["C2"]
    assert "relative_humidity" in signatures["C3"]


def test_self_test_passes():
    assert M.self_test() == 0

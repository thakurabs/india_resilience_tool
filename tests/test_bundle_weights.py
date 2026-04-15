from __future__ import annotations

import math

from india_resilience_tool.config.bundle_weights import (
    LANDING_BUNDLE_WEIGHTS,
    get_bundle_weights,
    validate_bundle_weights,
)


def test_heat_risk_bundle_weights_are_stable_and_sum_to_one() -> None:
    entries = get_bundle_weights("Heat Risk")

    assert [entry.metric_slug for entry in entries] == [
        "tas_annual_mean",
        "tasmax_summer_mean",
        "tas_summer_mean",
        "txx_annual_max",
        "tn90p_warm_nights_pct",
        "hwa_heatwave_amplitude",
        "txge30_hot_days",
        "txge35_extreme_heat_days",
        "tasmin_tropical_nights_gt25",
        "hwfi_tmean_90p",
        "hwfi_events_tmean_90p",
        "wsdi_warm_spell_days",
        "tnx_annual_max",
        "tx90p_hot_days_pct",
    ]
    assert math.isclose(sum(entry.weight for entry in entries), 1.0, rel_tol=0.0, abs_tol=1e-9)


def test_heat_stress_bundle_weights_are_stable_and_sum_to_one() -> None:
    entries = get_bundle_weights("Heat Stress")

    assert [entry.metric_slug for entry in entries] == [
        "twb_annual_mean",
        "twb_summer_mean",
        "twb_annual_max",
        "twb_days_ge_30",
        "wbd_le_3",
        "wbd_gt3_le6",
        "tasmin_tropical_nights_gt28",
        "tn90p_warm_nights_pct",
        "wbd_le_3_consecutive_days",
        "wsdi_warm_spell_days",
        "twb_days_ge_28",
    ]
    assert math.isclose(sum(entry.weight for entry in entries), 1.0, rel_tol=0.0, abs_tol=1e-9)


def test_cold_risk_bundle_weights_are_stable_and_sum_to_one() -> None:
    entries = get_bundle_weights("Cold Risk")

    assert [entry.metric_slug for entry in entries] == [
        "tas_winter_mean",
        "tasmin_winter_mean",
        "tnn_annual_min",
        "tasmin_winter_min",
        "tnle10_cold_nights",
        "tnle5_severe_cold_nights",
        "txle15_cold_days",
        "tx10p_cool_days_pct",
        "tn10p_cool_nights_pct",
        "csdi_cold_spell_days",
        "tnle10_consecutive_cold_nights",
    ]
    assert math.isclose(sum(entry.weight for entry in entries), 1.0, rel_tol=0.0, abs_tol=1e-9)


def test_drought_risk_bundle_weights_are_stable_and_sum_to_one() -> None:
    entries = get_bundle_weights("Drought Risk")

    assert [entry.metric_slug for entry in entries] == [
        "spi3_count_events_lt_minus1",
        "spi6_count_events_lt_minus1",
        "spi12_count_events_lt_minus1",
    ]
    assert [entry.weight for entry in entries] == [0.20, 0.30, 0.50]
    assert math.isclose(sum(entry.weight for entry in entries), 1.0, rel_tol=0.0, abs_tol=1e-9)


def test_flood_bundle_weights_are_stable_and_sum_to_one() -> None:
    entries = get_bundle_weights("Flood & Extreme Rainfall Risk")

    assert [entry.metric_slug for entry in entries] == [
        "pr_max_1day_precip",
        "pr_max_5day_precip",
        "r20mm_very_heavy_precip_days",
        "r95p_very_wet_precip",
        "r95ptot_contribution_pct",
        "cwd_consecutive_wet_days",
    ]
    assert math.isclose(sum(entry.weight for entry in entries), 1.0, rel_tol=0.0, abs_tol=1e-9)


def test_agriculture_bundle_weights_are_stable_and_sum_to_one() -> None:
    entries = get_bundle_weights("Agriculture & Growing Conditions")

    assert [entry.metric_slug for entry in entries] == [
        "gsl_growing_season",
        "tasmax_summer_mean",
        "txge35_extreme_heat_days",
        "wsdi_warm_spell_days",
        "tasmin_winter_mean",
        "tnle10_cold_nights",
        "spi3_drought_index",
        "prcptot_annual_total",
        "dtr_daily_temp_range",
    ]
    assert math.isclose(sum(entry.weight for entry in entries), 1.0, rel_tol=0.0, abs_tol=1e-9)


def test_validate_bundle_weights_reports_no_issues() -> None:
    assert validate_bundle_weights() == []


def test_all_visible_glance_climate_bundles_have_custom_weights_in_this_pass() -> None:
    assert set(LANDING_BUNDLE_WEIGHTS) == {
        "Heat Risk",
        "Heat Stress",
        "Cold Risk",
        "Drought Risk",
        "Flood & Extreme Rainfall Risk",
        "Agriculture & Growing Conditions",
    }

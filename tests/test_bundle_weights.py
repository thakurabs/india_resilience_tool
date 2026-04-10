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


def test_validate_bundle_weights_reports_no_issues() -> None:
    assert validate_bundle_weights() == []


def test_only_heat_risk_and_heat_stress_have_custom_weights_in_this_pass() -> None:
    assert set(LANDING_BUNDLE_WEIGHTS) == {"Heat Risk", "Heat Stress"}

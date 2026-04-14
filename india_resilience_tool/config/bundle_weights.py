"""Declarative landing bundle weights for Glance / landing bundle scores."""

from __future__ import annotations

from dataclasses import dataclass
from math import isclose
from typing import Optional


@dataclass(frozen=True)
class BundleWeightEntry:
    """One approved per-metric bundle weight entry for landing scoring."""

    bundle_domain: str
    metric_slug: str
    weight: float
    source_note: str
    substitution_note: str = ""
    workbook_group: Optional[str] = None


LANDING_BUNDLE_WEIGHTS: dict[str, tuple[BundleWeightEntry, ...]] = {
    "Heat Risk": (
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="tas_annual_mean",
            weight=0.2 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Mean & Background Heat",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="tasmax_summer_mean",
            weight=0.2 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Mean & Background Heat",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="tas_summer_mean",
            weight=0.2 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Mean & Background Heat",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="txx_annual_max",
            weight=0.25 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Extremes",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="tn90p_warm_nights_pct",
            weight=0.25 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Extremes",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="hwa_heatwave_amplitude",
            weight=0.25 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Extremes",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="txge30_hot_days",
            weight=0.2 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Threshold-based Frequency",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="txge35_extreme_heat_days",
            weight=0.2 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Threshold-based Frequency",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="tasmin_tropical_nights_gt25",
            weight=0.2 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            substitution_note="Uses TN > 25°C for Indian context instead of the legacy TN > 20°C metric.",
            workbook_group="Threshold-based Frequency",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="hwfi_tmean_90p",
            weight=0.15 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Percentile Extremes",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="hwfi_events_tmean_90p",
            weight=0.15 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Percentile Extremes",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="wsdi_warm_spell_days",
            weight=0.2 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Heatwave Characteristics",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="tnx_annual_max",
            weight=0.2 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Heatwave Characteristics",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="tx90p_hot_days_pct",
            weight=0.2 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Heatwave Characteristics",
        ),
    ),
    "Heat Stress": (
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="twb_annual_mean",
            weight=0.20 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Heat Stress",
            workbook_group="Background Heat Stress",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="twb_summer_mean",
            weight=0.20 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Heat Stress",
            workbook_group="Background Heat Stress",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="twb_annual_max",
            weight=0.25 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Heat Stress",
            workbook_group="Extreme Heat Stress",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="twb_days_ge_30",
            weight=0.25 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Heat Stress",
            workbook_group="Extreme Heat Stress",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="wbd_le_3",
            weight=0.15 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Heat Stress",
            workbook_group="Humidity Constraint",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="wbd_gt3_le6",
            weight=0.15 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Heat Stress",
            workbook_group="Humidity Constraint",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="tasmin_tropical_nights_gt28",
            weight=0.15 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Heat Stress",
            workbook_group="Night-time Heat Stress",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="tn90p_warm_nights_pct",
            weight=0.15 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Heat Stress",
            workbook_group="Night-time Heat Stress",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="wbd_le_3_consecutive_days",
            weight=0.25 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Stress",
            workbook_group="Persistence / Duration",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="wsdi_warm_spell_days",
            weight=0.25 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Stress",
            workbook_group="Persistence / Duration",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="twb_days_ge_28",
            weight=0.25 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Stress",
            workbook_group="Persistence / Duration",
        ),
    ),
    "Cold Risk": (
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="tas_winter_mean",
            weight=0.20 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Background Cold",
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="tasmin_winter_mean",
            weight=0.20 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Background Cold",
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="tnn_annual_min",
            weight=0.20 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Absolute Extremes",
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="tasmin_winter_min",
            weight=0.20 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Absolute Extremes",
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="tnle10_cold_nights",
            weight=0.25 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Threshold-based Cold Days",
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="tnle5_severe_cold_nights",
            weight=0.25 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Threshold-based Cold Days",
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="txle15_cold_days",
            weight=0.25 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Threshold-based Cold Days",
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="tx10p_cool_days_pct",
            weight=0.15 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Relative Cold",
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="tn10p_cool_nights_pct",
            weight=0.15 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Relative Cold",
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="csdi_cold_spell_days",
            weight=0.20 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Cold Spell Characteristics",
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="tnle10_consecutive_cold_nights",
            weight=0.20 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Cold Spell Characteristics",
        ),
    ),
    "Drought Risk": (
        BundleWeightEntry(
            bundle_domain="Drought Risk",
            metric_slug="spi3_count_events_lt_minus1",
            weight=0.20,
            source_note="Approved drought bundle / current weighted pass",
            substitution_note="Uses SPI drought-event counts for seasonal drought.",
            workbook_group="Seasonal Drought",
        ),
        BundleWeightEntry(
            bundle_domain="Drought Risk",
            metric_slug="spi6_count_events_lt_minus1",
            weight=0.30,
            source_note="Approved drought bundle / current weighted pass",
            substitution_note="Uses SPI drought-event counts for meteorological drought.",
            workbook_group="Meteorological Drought",
        ),
        BundleWeightEntry(
            bundle_domain="Drought Risk",
            metric_slug="spi12_count_events_lt_minus1",
            weight=0.50,
            source_note="Approved drought bundle / current weighted pass",
            substitution_note="Uses SPI drought-event counts for long-term drought.",
            workbook_group="Long-term Drought",
        ),
    ),
    "Flood & Extreme Rainfall Risk": (
        BundleWeightEntry(
            bundle_domain="Flood & Extreme Rainfall Risk",
            metric_slug="pr_max_1day_precip",
            weight=0.25 / 2.0,
            source_note="Approved flood bundle / current available metrics pass",
            substitution_note="Flood Depth Index remains deferred; weights cover the active six-metric flood bundle only.",
            workbook_group="Peak Intensity",
        ),
        BundleWeightEntry(
            bundle_domain="Flood & Extreme Rainfall Risk",
            metric_slug="pr_max_5day_precip",
            weight=0.25 / 2.0,
            source_note="Approved flood bundle / current available metrics pass",
            workbook_group="Peak Intensity",
        ),
        BundleWeightEntry(
            bundle_domain="Flood & Extreme Rainfall Risk",
            metric_slug="r20mm_very_heavy_precip_days",
            weight=0.25,
            source_note="Approved flood bundle / current available metrics pass",
            workbook_group="Heavy Rain Frequency",
        ),
        BundleWeightEntry(
            bundle_domain="Flood & Extreme Rainfall Risk",
            metric_slug="r95p_very_wet_precip",
            weight=0.25 / 2.0,
            source_note="Approved flood bundle / current available metrics pass",
            workbook_group="Very Wet Contribution",
        ),
        BundleWeightEntry(
            bundle_domain="Flood & Extreme Rainfall Risk",
            metric_slug="r95ptot_contribution_pct",
            weight=0.25 / 2.0,
            source_note="Approved flood bundle / current available metrics pass",
            workbook_group="Very Wet Contribution",
        ),
        BundleWeightEntry(
            bundle_domain="Flood & Extreme Rainfall Risk",
            metric_slug="cwd_consecutive_wet_days",
            weight=0.25,
            source_note="Approved flood bundle / current available metrics pass",
            workbook_group="Wet-spell Persistence",
        ),
    ),
    "Agriculture & Growing Conditions": (
        BundleWeightEntry(
            bundle_domain="Agriculture & Growing Conditions",
            metric_slug="gsl_growing_season",
            weight=0.20,
            source_note="Approved agriculture bundle / current weighted pass",
            workbook_group="Growing Season / Phenology",
        ),
        BundleWeightEntry(
            bundle_domain="Agriculture & Growing Conditions",
            metric_slug="tasmax_summer_mean",
            weight=0.20 / 3.0,
            source_note="Approved agriculture bundle / current weighted pass",
            workbook_group="Heat Burden",
        ),
        BundleWeightEntry(
            bundle_domain="Agriculture & Growing Conditions",
            metric_slug="txge35_extreme_heat_days",
            weight=0.20 / 3.0,
            source_note="Approved agriculture bundle / current weighted pass",
            workbook_group="Heat Burden",
        ),
        BundleWeightEntry(
            bundle_domain="Agriculture & Growing Conditions",
            metric_slug="wsdi_warm_spell_days",
            weight=0.20 / 3.0,
            source_note="Approved agriculture bundle / current weighted pass",
            workbook_group="Heat Burden",
        ),
        BundleWeightEntry(
            bundle_domain="Agriculture & Growing Conditions",
            metric_slug="tasmin_winter_mean",
            weight=0.20 / 2.0,
            source_note="Approved agriculture bundle / current weighted pass",
            workbook_group="Cold Burden",
        ),
        BundleWeightEntry(
            bundle_domain="Agriculture & Growing Conditions",
            metric_slug="tnle10_cold_nights",
            weight=0.20 / 2.0,
            source_note="Approved agriculture bundle / current weighted pass",
            workbook_group="Cold Burden",
        ),
        BundleWeightEntry(
            bundle_domain="Agriculture & Growing Conditions",
            metric_slug="spi3_drought_index",
            weight=0.20 / 2.0,
            source_note="Approved agriculture bundle / current weighted pass",
            workbook_group="Water Availability / Drought",
        ),
        BundleWeightEntry(
            bundle_domain="Agriculture & Growing Conditions",
            metric_slug="prcptot_annual_total",
            weight=0.20 / 2.0,
            source_note="Approved agriculture bundle / current weighted pass",
            substitution_note="Uses PRCPTOT as the approved rainfall proxy for this agriculture pass.",
            workbook_group="Water Availability / Drought",
        ),
        BundleWeightEntry(
            bundle_domain="Agriculture & Growing Conditions",
            metric_slug="dtr_daily_temp_range",
            weight=0.20,
            source_note="Approved agriculture bundle / current weighted pass",
            workbook_group="Temperature Variability / Suitability",
        ),
    ),
}


def get_bundle_weights(bundle_domain: str) -> tuple[BundleWeightEntry, ...]:
    """Return approved per-metric bundle weights for a landing bundle."""
    return LANDING_BUNDLE_WEIGHTS.get(str(bundle_domain).strip(), ())


def has_bundle_weights(bundle_domain: str) -> bool:
    """Return whether a landing bundle has approved custom weights."""
    return bool(get_bundle_weights(bundle_domain))


def validate_bundle_weights() -> list[str]:
    """Return validation issues for configured landing bundle weights."""
    from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG

    issues: list[str] = []
    for bundle_domain, entries in LANDING_BUNDLE_WEIGHTS.items():
        if not entries:
            issues.append(f"Bundle {bundle_domain!r} has no weight entries.")
            continue

        total = 0.0
        seen: set[str] = set()
        for entry in entries:
            if entry.bundle_domain != bundle_domain:
                issues.append(
                    f"Bundle {bundle_domain!r} contains entry with mismatched bundle_domain {entry.bundle_domain!r}."
                )
            if not str(entry.metric_slug).strip():
                issues.append(f"Bundle {bundle_domain!r} has an entry with an empty metric_slug.")
            if entry.metric_slug in seen:
                issues.append(f"Bundle {bundle_domain!r} repeats metric slug {entry.metric_slug!r}.")
            seen.add(entry.metric_slug)
            if entry.metric_slug not in METRICS_BY_SLUG:
                issues.append(f"Bundle {bundle_domain!r} references unknown metric slug {entry.metric_slug!r}.")
            if float(entry.weight) <= 0.0:
                issues.append(f"Bundle {bundle_domain!r} has non-positive weight for {entry.metric_slug!r}.")
            total += float(entry.weight)

        if not isclose(total, 1.0, rel_tol=0.0, abs_tol=1e-9):
            issues.append(f"Bundle {bundle_domain!r} weights sum to {total:.12f}, expected 1.0.")

    return issues

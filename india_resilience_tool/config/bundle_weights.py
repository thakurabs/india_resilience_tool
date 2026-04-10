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

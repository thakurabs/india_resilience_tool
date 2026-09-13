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
    is_attribute: bool = False
    #: True when the metric's value is defined *relative to the unit's own
    #: baseline distribution* (ETCCDI percentile and percentile-spell indices)
    #: rather than against an absolute physical threshold or level. The frozen
    #: national ruler publishes the absolute half only, so this partition
    #: decides which metrics carry the headline composite (CHG-0367b).
    is_baseline_referenced: bool = False


#: Configured headline weight a bundle must carry, i.e. the sum of its
#: non-attribute, non-baseline-referenced weights. Only bundles published against
#: a frozen national ruler are pinned; the rest have no headline split.
EXPECTED_HEADLINE_WEIGHT_TOTALS: dict[str, float] = {
    "Heat Risk": 0.2 / 3.0 * 3 + 0.25 / 3.0 * 2 + 0.2 / 3.0 * 3 + 0.2 / 3.0,
    "Heat Stress": 0.70,
    "Riverine Flood": 1.0,
    "Extreme Rainfall | Flash Flood Risk": 0.75,
    "Cold Risk": 0.75,
}


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
            is_baseline_referenced=True,
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
            is_baseline_referenced=True,
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="hwfi_events_tmean_90p",
            weight=0.15 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Percentile Extremes",
            is_baseline_referenced=True,
        ),
        BundleWeightEntry(
            bundle_domain="Heat Risk",
            metric_slug="wsdi_warm_spell_days",
            weight=0.2 / 3.0,
            source_note="Bundles_comp_Score.xlsx / Heat Risk",
            workbook_group="Heatwave Characteristics",
            is_baseline_referenced=True,
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
            is_baseline_referenced=True,
        ),
    ),
    "Heat Stress": (
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="twb_annual_mean",
            weight=0.20 / 2.0,
            source_note="Heat Stress v2 grid-first bundle",
            workbook_group="Background humid heat",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="twb_summer_mean",
            weight=0.20 / 2.0,
            source_note="Heat Stress v2 grid-first bundle",
            workbook_group="Background humid heat",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="twb_annual_max",
            weight=0.40 / 3.0,
            source_note="Heat Stress v2 grid-first bundle",
            workbook_group="Extreme / threshold humid heat",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="twb_days_ge_28",
            weight=0.40 / 3.0,
            source_note="Heat Stress v2 grid-first bundle",
            workbook_group="Extreme / threshold humid heat",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="twb_days_ge_30",
            weight=0.40 / 3.0,
            source_note="Heat Stress v2 grid-first bundle",
            workbook_group="Extreme / threshold humid heat",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="tasmin_tropical_nights_gt28",
            weight=0.20 / 2.0,
            source_note="Heat Stress v2 grid-first bundle",
            workbook_group="Night-time recovery stress",
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="tn90p_warm_nights_pct",
            weight=0.20 / 2.0,
            source_note="Heat Stress v2 grid-first bundle",
            workbook_group="Night-time recovery stress",
            is_baseline_referenced=True,
        ),
        BundleWeightEntry(
            bundle_domain="Heat Stress",
            metric_slug="wsdi_warm_spell_days",
            weight=0.20 / 1.0,
            source_note="Heat Stress v2 grid-first bundle",
            workbook_group="Persistence",
            is_baseline_referenced=True,
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
            # TX10p counts days below the unit's own 1990-2010 10th-percentile
            # daily maximum, so the trigger temperature differs by district.
            is_baseline_referenced=True,
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="tn10p_cool_nights_pct",
            weight=0.15 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Relative Cold",
            # TN10p shares TX10p's unit-specific percentile threshold.
            is_baseline_referenced=True,
        ),
        BundleWeightEntry(
            bundle_domain="Cold Risk",
            metric_slug="csdi_cold_spell_days",
            weight=0.20 / 2.0,
            source_note="Bundles_comp_Score.xlsx / Coldrisk",
            workbook_group="Cold Spell Characteristics",
            # CSDI counts days in spells below the unit's own 10th-percentile
            # daily minimum, the cold-side twin of WSDI in Heat Risk.
            is_baseline_referenced=True,
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
            weight=0.08,
            source_note="Approved Drought Risk v2 bundle",
            substitution_note="Uses SPI drought-event counts for seasonal drought.",
            workbook_group="Seasonal Drought",
        ),
        BundleWeightEntry(
            bundle_domain="Drought Risk",
            metric_slug="spi6_count_events_lt_minus1",
            weight=0.12,
            source_note="Approved Drought Risk v2 bundle",
            substitution_note="Uses SPI drought-event counts for meteorological drought.",
            workbook_group="Meteorological Drought",
        ),
        BundleWeightEntry(
            bundle_domain="Drought Risk",
            metric_slug="spi12_count_events_lt_minus1",
            weight=0.20,
            source_note="Approved Drought Risk v2 bundle",
            substitution_note="Uses SPI drought-event counts for long-term drought.",
            workbook_group="Long-term Drought",
        ),
        BundleWeightEntry(
            bundle_domain="Drought Risk",
            metric_slug="spi3_max_spell_lt_minus1",
            weight=0.12,
            source_note="Approved Drought Risk v2 bundle",
            substitution_note="Uses longest within-year SPI3 drought spell length.",
            workbook_group="Seasonal Drought",
        ),
        BundleWeightEntry(
            bundle_domain="Drought Risk",
            metric_slug="spi6_max_spell_lt_minus1",
            weight=0.18,
            source_note="Approved Drought Risk v2 bundle",
            substitution_note="Uses longest within-year SPI6 drought spell length.",
            workbook_group="Meteorological Drought",
        ),
        BundleWeightEntry(
            bundle_domain="Drought Risk",
            metric_slug="spi12_max_spell_lt_minus1",
            weight=0.30,
            source_note="Approved Drought Risk v2 bundle",
            substitution_note="Uses longest within-year SPI12 drought spell length.",
            workbook_group="Long-term Drought",
        ),
    ),
    "Riverine Flood": (
        BundleWeightEntry(
            bundle_domain="Riverine Flood",
            metric_slug="jrc_flood_depth_index_rp100",
            weight=1.0,
            source_note="RP-100 severity index (depth × extent matrix); depth and extent shown as inline glance attributes",
            workbook_group="Inundation Severity",
        ),
        BundleWeightEntry(
            bundle_domain="Riverine Flood",
            metric_slug="jrc_flood_depth_rp100",
            weight=0.0,
            source_note="",
            is_attribute=True,
            workbook_group="Inundation Depth",
        ),
        BundleWeightEntry(
            bundle_domain="Riverine Flood",
            metric_slug="jrc_flood_extent_rp100",
            weight=0.0,
            source_note="",
            is_attribute=True,
            workbook_group="Inundation Extent",
        ),
    ),
    "Water Risk": (
        BundleWeightEntry(
            bundle_domain="Water Risk",
            metric_slug="water_scarcity_percapita",
            weight=1.0,
            source_note="NITI Aayog present-day (2025) per-capita water-scarcity class; 2050 projection and deterioration shown as inline glance attributes",
            workbook_group="Water Scarcity",
        ),
        BundleWeightEntry(
            bundle_domain="Water Risk",
            metric_slug="water_scarcity_percapita_2050",
            weight=0.0,
            source_note="",
            is_attribute=True,
            workbook_group="Water Scarcity (2050 projection)",
        ),
        BundleWeightEntry(
            bundle_domain="Water Risk",
            metric_slug="water_scarcity_deterioration_2050",
            weight=0.0,
            source_note="",
            is_attribute=True,
            workbook_group="Water Scarcity Deterioration",
        ),
    ),
    "Extreme Rainfall | Flash Flood Risk": (
        BundleWeightEntry(
            bundle_domain="Extreme Rainfall | Flash Flood Risk",
            metric_slug="pr_max_1day_precip",
            weight=0.25 / 2.0,
            source_note="Approved flood bundle / current available metrics pass",
            substitution_note="Riverine/JRC flood depth is handled by the separate Riverine Flood bundle.",
            workbook_group="Peak Intensity",
        ),
        BundleWeightEntry(
            bundle_domain="Extreme Rainfall | Flash Flood Risk",
            metric_slug="pr_max_5day_precip",
            weight=0.25 / 2.0,
            source_note="Approved flood bundle / current available metrics pass",
            workbook_group="Peak Intensity",
        ),
        BundleWeightEntry(
            bundle_domain="Extreme Rainfall | Flash Flood Risk",
            metric_slug="r20mm_very_heavy_precip_days",
            weight=0.25,
            source_note="Approved flood bundle / current available metrics pass",
            workbook_group="Heavy Rain Frequency",
        ),
        BundleWeightEntry(
            bundle_domain="Extreme Rainfall | Flash Flood Risk",
            metric_slug="r95p_very_wet_precip",
            weight=0.25 / 2.0,
            source_note="Approved flood bundle / current available metrics pass",
            workbook_group="Very Wet Contribution",
            # R95p counts rainfall above the *unit's own* baseline 95th
            # percentile of wet-day precipitation, so an arid district and a
            # coastal one are measured against different millimetre thresholds.
            is_baseline_referenced=True,
        ),
        BundleWeightEntry(
            bundle_domain="Extreme Rainfall | Flash Flood Risk",
            metric_slug="r95ptot_contribution_pct",
            weight=0.25 / 2.0,
            source_note="Approved flood bundle / current available metrics pass",
            workbook_group="Very Wet Contribution",
            # R95pTOT shares R95p's unit-specific percentile threshold and is in
            # addition a *share* of that unit's own wet-day total, so it is
            # doubly relative to the unit rather than to an absolute depth.
            is_baseline_referenced=True,
        ),
        BundleWeightEntry(
            bundle_domain="Extreme Rainfall | Flash Flood Risk",
            metric_slug="cwd_consecutive_wet_days",
            weight=0.25,
            source_note="Approved flood bundle / current available metrics pass",
            workbook_group="Wet-spell Persistence",
        ),
    ),
}


def get_bundle_weights(bundle_domain: str) -> tuple[BundleWeightEntry, ...]:
    """Return approved per-metric bundle weights for a landing bundle."""
    return LANDING_BUNDLE_WEIGHTS.get(str(bundle_domain).strip(), ())


def has_bundle_weights(bundle_domain: str) -> bool:
    """Return whether a landing bundle has approved custom weights."""
    return bool(get_bundle_weights(bundle_domain))


def get_bundle_headline_weights(bundle_domain: str) -> tuple[BundleWeightEntry, ...]:
    """Return the entries that carry a bundle's published headline composite.

    The headline is the non-attribute, non-baseline-referenced half: metrics
    scored against absolute physical thresholds or levels. Their configured
    weights sum to less than 1.0 by construction and are renormalized at scoring
    time, so this accessor returns the configured weights unchanged (CHG-0367b).
    """
    return tuple(
        e
        for e in get_bundle_weights(bundle_domain)
        if not e.is_attribute and not e.is_baseline_referenced
    )


def get_bundle_baseline_referenced_slugs(bundle_domain: str) -> tuple[str, ...]:
    """Return metric slugs whose value is defined against the unit's own baseline."""
    return tuple(
        e.metric_slug
        for e in get_bundle_weights(bundle_domain)
        if not e.is_attribute and e.is_baseline_referenced
    )


def get_bundle_headline_weight_total(bundle_domain: str) -> float:
    """Configured headline weight of a bundle, i.e. the coverage denominator.

    This is the *configured* total, not the total of whatever fitted. Using the
    fitted sum would let a row missing a metric entirely report full coverage
    against a smaller universe than the gate claims (CHG-0350).
    """
    return float(sum(e.weight for e in get_bundle_headline_weights(bundle_domain)))


def get_bundle_attribute_slugs(bundle_domain: str) -> tuple[str, ...]:
    """Return metric slugs declared as inline glance attributes for a bundle."""
    return tuple(
        e.metric_slug
        for e in LANDING_BUNDLE_WEIGHTS.get(str(bundle_domain).strip(), ())
        if e.is_attribute
    )


def validate_bundle_weights() -> list[str]:
    """Return validation issues for configured landing bundle weights."""
    from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG, get_metrics_for_bundle

    issues: list[str] = []
    baseline_flags_by_slug: dict[str, list[tuple[str, bool]]] = {}
    for bundle_domain, entries in LANDING_BUNDLE_WEIGHTS.items():
        for entry in entries:
            baseline_flags_by_slug.setdefault(entry.metric_slug, []).append(
                (bundle_domain, entry.is_baseline_referenced)
            )
    for metric_slug, assignments in sorted(baseline_flags_by_slug.items()):
        if len({flag for _, flag in assignments}) > 1:
            detail = ", ".join(
                f"{bundle_domain}={flag}"
                for bundle_domain, flag in assignments
            )
            issues.append(
                f"Metric slug {metric_slug!r} has divergent is_baseline_referenced "
                f"flags across bundles: {detail}."
            )

    for bundle_domain, entries in LANDING_BUNDLE_WEIGHTS.items():
        if not entries:
            issues.append(f"Bundle {bundle_domain!r} has no weight entries.")
            continue
        available_bundle_metrics = set(
            get_metrics_for_bundle(bundle_domain, spatial_family="admin", level="district")
        )

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
            elif not entry.is_attribute and entry.metric_slug not in available_bundle_metrics:
                issues.append(
                    f"Bundle {bundle_domain!r} references metric slug {entry.metric_slug!r} "
                    "that is not available for admin/district Glance scoring in this bundle."
                )
            if not entry.is_attribute and float(entry.weight) <= 0.0:
                issues.append(f"Bundle {bundle_domain!r} has non-positive weight for {entry.metric_slug!r}.")
            if entry.is_attribute and float(entry.weight) != 0.0:
                issues.append(
                    f"Bundle {bundle_domain!r} attribute entry {entry.metric_slug!r} must have weight=0.0."
                )
            total += float(entry.weight)

        non_attr_weights = sum(float(e.weight) for e in entries if not e.is_attribute)
        if not isclose(non_attr_weights, 1.0, rel_tol=0.0, abs_tol=1e-9):
            issues.append(
                f"Bundle {bundle_domain!r} non-attribute weights sum to {non_attr_weights:.12f}, expected 1.0."
            )

        # The frozen national ruler publishes the absolute half of Heat Risk and
        # renormalizes it 0.6333... -> 1.0. If that configured total drifts, every
        # published score changes silently under an unchanged column name, so it
        # is pinned here rather than merely documented (CHG-0367b).
        expected_headline = EXPECTED_HEADLINE_WEIGHT_TOTALS.get(bundle_domain)
        if expected_headline is not None:
            headline_total = sum(
                float(e.weight)
                for e in entries
                if not e.is_attribute and not e.is_baseline_referenced
            )
            if not isclose(headline_total, expected_headline, rel_tol=0.0, abs_tol=1e-9):
                issues.append(
                    f"Bundle {bundle_domain!r} headline (absolute-threshold) weights sum to "
                    f"{headline_total:.12f}, expected {expected_headline:.12f}."
                )

    return issues

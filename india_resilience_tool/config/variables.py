"""
Variable/Index registry and configuration for IRT.

This module defines the climate indices available in the dashboard,
their metadata, and display groupings.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Any, Dict

# ---- Variable/Index registry ----
# Each entry maps an "index slug" to:
#  - label: what the user sees in the Index dropdown
#  - periods_metric_col: the base metric name in master CSV (<metric>__<scenario>__<period>__<stat>)
#  - description: human-readable definition used in tooltips
#  - file patterns for district/state yearly series discovery
VARIABLES: Dict[str, Dict[str, Any]] = {
    "tas_gt32": {
        "label": "Summer Days",
        "group": "temperature",
        "periods_metric_col": "days_gt_32C",
        "description": (
            "Number of days in a year on which the district-average daily maximum near-surface "
            "air temperature exceeds 30 °C."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmax_csd_gt30": {
        "label": "Consecutive Summer Days",
        "group": "temperature",
        "periods_metric_col": "consec_summer_days_gt_30C",
        "description": (
            "For each year, the maximum length (in days) of any spell of consecutive days "
            "on which the district-average daily maximum temperature exceeds 30 °C."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmax_csd_events_gt30": {
        "label": "Consecutive Summer Day Events",
        "group": "temperature",
        "periods_metric_col": "csd_events_gt_30C",
        "description": (
            "Number of distinct 'Consecutive Summer Day' spells per year, where each spell "
            "is a run of at least 5 consecutive days on which the district-average daily "
            "maximum temperature exceeds 30 °C."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmin_tropical_nights_gt20": {
        "label": "Tropical Nights",
        "group": "temperature",
        "periods_metric_col": "tropical_nights_gt_20C",
        "description": (
            "Number of nights in a year on which the district-average daily minimum "
            "temperature exceeds 20 °C."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "hwdi_tasmax_plus5C": {
        "label": "Heat Wave Duration Index (HWDI, #Days)",
        "group": "temperature",
        "periods_metric_col": "hwdi_max_spell_len",
        "description": (
            "For each year, the length (in days) of the longest heat-wave spell. "
            "Heat-wave spells are defined from days on which the district-average daily "
            "maximum temperature is at least about 5 °C warmer than its local historical "
            "normal (i.e. persistent, unusually hot days)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "hwfi_tmean_90p": {
        "label": "Heat Wave Frequency Index (HWFI, #Days)",
        "group": "temperature",
        "periods_metric_col": "hwfi_days_in_spells",
        "description": (
            "For each year, the total number of days that occur inside heat-wave spells. "
            "Heat-wave spells are identified using the district-average daily mean "
            "temperature exceeding a high threshold (around the 90th percentile of the "
            "historical distribution) and persisting for several consecutive days."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "hwdi_events_tasmax_plus5C": {
        "label": "Heat Wave Duration Index (HWDI, #Events)",
        "group": "temperature",
        "periods_metric_col": "hwdi_events_count",
        "description": (
            "Number of distinct heat-wave spells per year for the HWDI definition "
            "(spells of unusually hot days based on daily maximum temperature being "
            "roughly ≥5 °C above its local historical normal)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "hwfi_events_tmean_90p": {
        "label": "Heat Wave Frequency Index (HWFI, #Events)",
        "group": "temperature",
        "periods_metric_col": "hwfi_events_count",
        "description": (
            "Number of distinct heat-wave spells per year for the HWFI definition "
            "(spells of several consecutive days on which the district-average daily "
            "mean temperature exceeds a high percentile threshold, ~90th percentile)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmax_annual_mean": {
        "label": "Annual Max Temperature",
        "group": "temperature",
        "periods_metric_col": "annual_tasmax_mean_C",
        "description": (
            "Annual mean of daily maximum near-surface air temperature (tasmax), in °C, "
            "averaged over all days in the year for each district."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmax_summer_mean": {
        "label": "Summer Max Temperature",
        "group": "temperature",
        "periods_metric_col": "summer_tasmax_mean_C",
        "description": (
            "Mean of daily maximum temperature (tasmax), in °C, averaged over the "
            "summer season (March–May) for each year and district."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmin_annual_mean": {
        "label": "Annual Min Temperature",
        "group": "temperature",
        "periods_metric_col": "annual_tasmin_mean_C",
        "description": (
            "Annual mean of daily minimum near-surface air temperature (tasmin), in °C, "
            "averaged over all days in the year for each district."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "tasmin_winter_mean": {
        "label": "Winter Min Temperature",
        "group": "temperature",
        "periods_metric_col": "winter_tasmin_mean_C",
        "description": (
            "Mean of daily minimum temperature (tasmin), in °C, averaged over the winter "
            "season (December–February) for each year and district."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "rain_gt_2p5mm": {
        "label": "Rainy days",
        "group": "rain",
        "periods_metric_col": "days_rain_gt_2p5mm",
        "description": (
            "Number of days in a year on which the district-average daily rainfall "
            "exceeds 2.5 mm/day."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_simple_daily_intensity": {
        "label": "Simple Daily Intensity",
        "group": "rain",
        "periods_metric_col": "simple_daily_intensity_mm_per_day",
        "description": (
            "Ratio of total precipitation to the number of days with precipitation "
            "≥ 1 mm, over the selected period (mm/day)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_max_1day_precip": {
        "label": "Maximum 1-day Precipitation",
        "group": "rain",
        "periods_metric_col": "max_1day_precip_mm",
        "description": (
            "Seasonal or period-wise maximum of district-average daily precipitation "
            "over any single day (mm)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_max_5day_precip": {
        "label": "Highest Consecutive 5-day Precipitation",
        "group": "rain",
        "periods_metric_col": "max_5day_precip_mm",
        "description": (
            "Maximum total precipitation accumulated over any consecutive 5-day period "
            "within the selected years (mm)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_5day_precip_events_gt50mm": {
        "label": "Consecutive 5-day Precipitation Events (> 50 mm)",
        "group": "rain",
        "periods_metric_col": "consec_5day_precip_events",
        "description": (
            "Number of separate 5-day periods in which the total precipitation "
            "exceeds 50 mm (events per period)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_heavy_precip_days_gt10mm": {
        "label": "Heavy Precipitation Days (> 10 mm)",
        "group": "rain",
        "periods_metric_col": "heavy_precip_days_gt_10mm",
        "description": (
            "Number of days in the year with district-average daily precipitation "
            "greater than 10 mm."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_very_heavy_precip_days_gt25mm": {
        "label": "Very Heavy Precipitation Days (> 25 mm)",
        "group": "rain",
        "periods_metric_col": "very_heavy_precip_days_gt_25mm",
        "description": (
            "Number of days in the year with district-average daily precipitation "
            "greater than 25 mm."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_consecutive_dry_days_lt1mm": {
        "label": "Consecutive Dry Days (< 1 mm)",
        "group": "rain",
        "periods_metric_col": "consecutive_dry_days",
        "description": (
            "Longest stretch within the period of consecutive dry days with "
            "daily precipitation less than 1 mm."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
    "pr_consecutive_dry_day_events_gt5": {
        "label": "Consecutive Dry Day Events (> 5 days)",
        "group": "rain",
        "periods_metric_col": "consecutive_dry_day_events",
        "description": (
            "Number of separate periods with more than 5 consecutive dry days "
            "(daily precipitation < 1 mm)."
        ),
        "district_yearly_candidates": [
            "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
        ],
        "state_yearly_candidates": [
            "{root}/{state}/state_yearly_ensemble_stats.csv"
        ],
    },
}

INDEX_GROUP_LABELS: Dict[str, str] = {
    "temperature": "Temperature",
    "rain": "Rainfall",
}


def get_index_groups() -> list[str]:
    """Return ordered list of index groups (Temperature first, then Rainfall, then others)."""
    raw_groups = {cfg.get("group", "other") for cfg in VARIABLES.values()}
    preferred_order = ["temperature", "rain"]
    all_groups: list[str] = []
    for g in preferred_order:
        if g in raw_groups:
            all_groups.append(g)
    for g in sorted(raw_groups):
        if g not in all_groups:
            all_groups.append(g)
    return all_groups


def get_indices_for_group(group: str) -> list[str]:
    """Return list of index slugs for a given group."""
    return [slug for slug, cfg in VARIABLES.items() if cfg.get("group", "other") == group]
"""
Shared metrics registry for the India Resilience Tool (IRT).

This module unifies:
- Dashboard metric registry needs (slug/label/group/periods_metric_col + discovery templates)
- Pipeline metric specs (var/value_col/compute/params/units)

Includes all standard Climdex indices plus custom IRT indices.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence


def infer_group_from_var(var: str) -> str:
    """
    Infer a registry group from the CMIP variable name.
    """
    v = (var or "").strip().lower()
    if v in {"tas", "tasmax", "tasmin", "tmean"}:
        return "temperature"
    if v in {"pr", "precip", "precipitation"}:
        return "rain"
    return "other"


@dataclass(frozen=True)
class MetricSpec:
    """
    Unified metric specification used by both dashboard and pipeline.
    """

    slug: str
    label: str
    group: str
    periods_metric_col: str

    # Pipeline fields
    var: Optional[str] = None
    value_col: Optional[str] = None
    units: Optional[str] = None
    compute: Optional[str] = None
    params: Mapping[str, Any] = field(default_factory=dict)

    # Optional metadata
    name: Optional[str] = None
    description: Optional[str] = None
    aliases: Sequence[str] = field(default_factory=tuple)

    # Discovery templates
    district_yearly_candidates: Optional[Sequence[str]] = None
    state_yearly_candidates: Optional[Sequence[str]] = None

    @staticmethod
    def from_pipeline_dict(d: Mapping[str, Any]) -> "MetricSpec":
        """Create a MetricSpec from a pipeline metric dict."""
        slug = str(d["slug"])
        var = str(d.get("var", "") or "")
        value_col = str(d.get("value_col", "") or "")
        label = str(d.get("label") or d.get("name") or slug)
        group = str(d.get("group") or infer_group_from_var(var))
        periods_metric_col = str(d.get("periods_metric_col") or value_col)

        return MetricSpec(
            slug=slug,
            label=label,
            group=group,
            periods_metric_col=periods_metric_col,
            var=var or None,
            value_col=value_col or None,
            units=str(d.get("units") or "") or None,
            compute=str(d.get("compute") or "") or None,
            params=d.get("params") or {},
            name=str(d.get("name") or "") or None,
            description=str(d.get("description") or "") or None,
            aliases=tuple(d.get("aliases") or ()),
            district_yearly_candidates=d.get("district_yearly_candidates"),
            state_yearly_candidates=d.get("state_yearly_candidates"),
        )


def find_duplicate_slugs(pipeline_metrics: Sequence[Mapping[str, Any]]) -> list[str]:
    """Return a sorted list of slugs that appear more than once."""
    counts: dict[str, int] = {}
    for m in pipeline_metrics:
        slug = str(m.get("slug", "")).strip()
        if not slug:
            continue
        counts[slug] = counts.get(slug, 0) + 1
    return sorted([s for s, c in counts.items() if c > 1])


def build_registry_from_pipeline(
    pipeline_metrics: Sequence[Mapping[str, Any]],
) -> dict[str, MetricSpec]:
    """Build a slug -> MetricSpec mapping from pipeline metrics."""
    out: dict[str, MetricSpec] = {}
    for m in pipeline_metrics:
        spec = MetricSpec.from_pipeline_dict(m)
        out[spec.slug] = spec
    return out


def validate_registry_against_pipeline(
    registry_by_slug: Mapping[str, MetricSpec],
    pipeline_metrics: Sequence[Mapping[str, Any]],
) -> list[str]:
    """Validate registry/pipeline consistency."""
    issues: list[str] = []

    dupes = find_duplicate_slugs(pipeline_metrics)
    if dupes:
        issues.append(
            "Duplicate pipeline metric slugs detected: " + ", ".join(dupes)
        )

    for pm in pipeline_metrics:
        slug = str(pm.get("slug", "")).strip()
        if not slug:
            issues.append("Pipeline metric missing slug.")
            continue

        if slug not in registry_by_slug:
            issues.append(f"Pipeline slug '{slug}' missing from registry.")
            continue

        reg = registry_by_slug[slug]
        pm_value_col = str(pm.get("value_col") or "").strip()
        if pm_value_col and reg.periods_metric_col != pm_value_col:
            issues.append(
                f"Mismatch for slug '{slug}': registry periods_metric_col='{reg.periods_metric_col}' "
                f"but pipeline value_col='{pm_value_col}'."
            )

    return issues


# -----------------------------------------------------------------------------
# PIPELINE METRICS (single source of truth)
# -----------------------------------------------------------------------------
# Organized by category:
#   1. HEAT RISK / THERMAL STRESS INDICES
#   2. COLD RISK INDICES
#   3. PRECIPITATION / FLOOD-RELATED INDICES
#   4. DROUGHT / DRYNESS INDICES
#   5. OTHER USEFUL INDICES

PIPELINE_METRICS_RAW: list[dict[str, Any]] = [
    # # =========================================================================
    # # 1. HEAT RISK / THERMAL STRESS INDICES
    # # =========================================================================
    
    # # --- Temperature Extremes ---
    # {
    #     "name": "Annual Maximum Temperature (TXx)",
    #     "slug": "txx_annual_max",
    #     "var": "tasmax",
    #     "value_col": "txx_annual_max_C",
    #     "units": "°C",
    #     "compute": "annual_max_temperature",
    #     "params": {},
    #     "group": "temperature",
    #     "description": (
    #         "The highest daily maximum temperature recorded in the year (°C). "
    #         "Climdex index TXx."
    #     ),
    # },
    # {
    #     "name": "Annual Maximum of Daily Minimum Temperature (TNx)",
    #     "slug": "tnx_annual_max",
    #     "var": "tasmin",
    #     "value_col": "tnx_annual_max_C",
    #     "units": "°C",
    #     "compute": "annual_max_temperature",
    #     "params": {},
    #     "group": "temperature",
    #     "description": (
    #         "The highest daily minimum temperature recorded in the year (°C). "
    #         "Indicates warmest night. Climdex index TNx."
    #     ),
    # },
    # {
    #     "name": "Annual Minimum of Daily Maximum Temperature (TXn)",
    #     "slug": "txn_annual_min",
    #     "var": "tasmax",
    #     "value_col": "txn_annual_min_C",
    #     "units": "°C",
    #     "compute": "annual_min_temperature",
    #     "params": {},
    #     "group": "temperature",
    #     "description": (
    #         "The lowest daily maximum temperature recorded in the year (°C). "
    #         "Indicates coldest daytime. Climdex index TXn."
    #     ),
    # },
    # {
    #     "name": "Annual Minimum of Daily Minimum Temperature (TNn)",
    #     "slug": "tnn_annual_min",
    #     "var": "tasmin",
    #     "value_col": "tnn_annual_min_C",
    #     "units": "°C",
    #     "compute": "annual_min_temperature",
    #     "params": {},
    #     "group": "temperature",
    #     "description": (
    #         "The lowest daily minimum temperature recorded in the year (°C). "
    #         "Indicates coldest night. Climdex index TNn."
    #     ),
    # },
    
    # # --- Percentile-based Temperature Indices ---
    # {
    #     "name": "Hot Days (TX90p)",
    #     "slug": "tx90p_hot_days_pct",
    #     "var": "tasmax",
    #     "value_col": "tx90p_pct",
    #     "units": "%",
    #     "compute": "percentile_days_above",
    #     "params": {"percentile": 90, "baseline_years": (1985, 2014)},
    #     "group": "temperature",
    #     "description": (
    #         "Percentage of days when daily maximum temperature exceeds the 90th "
    #         "percentile of the baseline period. Climdex index TX90p."
    #     ),
    # },
    # {
    #     "name": "Warm Nights (TN90p)",
    #     "slug": "tn90p_warm_nights_pct",
    #     "var": "tasmin",
    #     "value_col": "tn90p_pct",
    #     "units": "%",
    #     "compute": "percentile_days_above",
    #     "params": {"percentile": 90, "baseline_years": (1985, 2014)},
    #     "group": "temperature",
    #     "description": (
    #         "Percentage of days when daily minimum temperature exceeds the 90th "
    #         "percentile of the baseline period. Climdex index TN90p."
    #     ),
    # },
    # {
    #     "name": "Cool Days (TX10p)",
    #     "slug": "tx10p_cool_days_pct",
    #     "var": "tasmax",
    #     "value_col": "tx10p_pct",
    #     "units": "%",
    #     "compute": "percentile_days_below",
    #     "params": {"percentile": 10, "baseline_years": (1985, 2014)},
    #     "group": "temperature",
    #     "description": (
    #         "Percentage of days when daily maximum temperature is below the 10th "
    #         "percentile of the baseline period. Climdex index TX10p."
    #     ),
    # },
    # {
    #     "name": "Cool Nights (TN10p)",
    #     "slug": "tn10p_cool_nights_pct",
    #     "var": "tasmin",
    #     "value_col": "tn10p_pct",
    #     "units": "%",
    #     "compute": "percentile_days_below",
    #     "params": {"percentile": 10, "baseline_years": (1985, 2014)},
    #     "group": "temperature",
    #     "description": (
    #         "Percentage of days when daily minimum temperature is below the 10th "
    #         "percentile of the baseline period. Climdex index TN10p."
    #     ),
    # },
    
    # # --- Threshold-based Heat Indices ---
    # {
    #     "name": "Summer Days (SU, TX > 25°C)",
    #     "slug": "su_summer_days_gt25",
    #     "var": "tasmax",
    #     "value_col": "su_days_gt_25C",
    #     "units": "days",
    #     "compute": "count_days_above_threshold",
    #     "params": {"thresh_k": 25.0 + 273.15},
    #     "group": "temperature",
    #     "description": (
    #         "Number of days when daily maximum temperature exceeds 25°C. "
    #         "Standard Climdex SU index."
    #     ),
    # },
    {
        "name": "Summer Days (TX > 32°C)",
        "slug": "tas_gt32",
        "var": "tasmax",
        "value_col": "days_gt_32C",
        "units": "days",
        "compute": "count_days_above_threshold",
        "params": {"thresh_k": 32.0 + 273.15},
        "group": "temperature",
        "description": (
            "Number of days when daily maximum temperature exceeds 32°C. "
            "India-specific higher threshold for summer days."
        ),
    },
    # {
    #     "name": "Hot Days (TX ≥ 30°C)",
    #     "slug": "txge30_hot_days",
    #     "var": "tasmax",
    #     "value_col": "days_tx_ge_30C",
    #     "units": "days",
    #     "compute": "count_days_ge_threshold",
    #     "params": {"thresh_k": 30.0 + 273.15},
    #     "group": "temperature",
    #     "description": (
    #         "Number of days when daily maximum temperature is at or above 30°C. "
    #         "Climdex index TXge30."
    #     ),
    # },
    # {
    #     "name": "Extreme Heat Days (TX ≥ 35°C)",
    #     "slug": "txge35_extreme_heat_days",
    #     "var": "tasmax",
    #     "value_col": "days_tx_ge_35C",
    #     "units": "days",
    #     "compute": "count_days_ge_threshold",
    #     "params": {"thresh_k": 35.0 + 273.15},
    #     "group": "temperature",
    #     "description": (
    #         "Number of days when daily maximum temperature is at or above 35°C. "
    #         "Climdex index TXge35. Critical for heat stress."
    #     ),
    # },
    # {
    #     "name": "Tropical Nights (TR, TN > 20°C)",
    #     "slug": "tasmin_tropical_nights_gt20",
    #     "var": "tasmin",
    #     "value_col": "tropical_nights_gt_20C",
    #     "units": "days",
    #     "compute": "count_days_above_threshold",
    #     "params": {"thresh_k": 20.0 + 273.15},
    #     "group": "temperature",
    #     "description": (
    #         "Number of nights when daily minimum temperature exceeds 20°C. "
    #         "Climdex index TR."
    #     ),
    # },
    
    # # --- Warm/Heat Spell Indices ---
    # {
    #     "name": "Warm Spell Duration Index (WSDI)",
    #     "slug": "wsdi_warm_spell_days",
    #     "var": "tasmax",
    #     "value_col": "wsdi_days",
    #     "units": "days",
    #     "compute": "warm_spell_duration_index",
    #     "params": {"percentile": 90, "min_spell_days": 6, "baseline_years": (1985, 2014)},
    #     "group": "temperature",
    #     "description": (
    #         "Annual count of days contributing to warm spells, where a warm spell "
    #         "is ≥6 consecutive days with TX > 90th percentile. Climdex WSDI."
    #     ),
    # },
    # {
    #     "name": "Consecutive Summer Days (TX > 30°C)",
    #     "slug": "tasmax_csd_gt30",
    #     "var": "tasmax",
    #     "value_col": "consec_summer_days_gt_30C",
    #     "units": "days",
    #     "compute": "longest_consecutive_run_above_threshold",
    #     "params": {"thresh_k": 30.0 + 273.15},
    #     "group": "temperature",
    #     "description": (
    #         "Maximum length of consecutive days when daily maximum temperature "
    #         "exceeds 30°C."
    #     ),
    # },
    # {
    #     "name": "Consecutive Summer Day Events (TX > 30°C)",
    #     "slug": "tasmax_csd_events_gt30",
    #     "var": "tasmax",
    #     "value_col": "csd_events_gt_30C",
    #     "units": "events",
    #     "compute": "consecutive_run_events_above_threshold",
    #     "params": {"thresh_k": 30.0 + 273.15, "min_event_days": 6},
    #     "group": "temperature",
    #     "description": (
    #         "Number of distinct spells of ≥6 consecutive days when daily maximum "
    #         "temperature exceeds 30°C."
    #     ),
    # },
    # {
    #     "name": "Heat Wave Duration Index (HWDI, #Days)",
    #     "slug": "hwdi_tasmax_plus5C",
    #     "var": "tasmax",
    #     "value_col": "hwdi_max_spell_len",
    #     "units": "days",
    #     "compute": "heatwave_duration_index",
    #     "params": {"baseline_years": (1985, 2014), "delta_c": 5.0},
    #     "group": "temperature",
    #     "description": (
    #         "Length of the longest heat-wave spell in the year. Heat-wave days "
    #         "defined as TX ≥ 5°C above historical normal."
    #     ),
    # },
    # {
    #     "name": "Heat Wave Frequency Index (HWFI, #Days)",
    #     "slug": "hwfi_tmean_90p",
    #     "var": "tas",
    #     "value_col": "hwfi_days_in_spells",
    #     "units": "days",
    #     "compute": "heatwave_frequency_percentile",
    #     "params": {"baseline_years": (1985, 2014), "pct": 90},
    #     "group": "temperature",
    #     "description": (
    #         "Total days inside heat-wave spells, where spells are consecutive "
    #         "days with mean temperature > 90th percentile."
    #     ),
    # },
    # {
    #     "name": "Heat Wave Duration Index (HWDI, #Events)",
    #     "slug": "hwdi_events_tasmax_plus5C",
    #     "var": "tasmax",
    #     "value_col": "hwdi_events_count",
    #     "units": "events",
    #     "compute": "heatwave_event_count",
    #     "params": {"baseline_years": (1985, 2014), "delta_c": 5.0},
    #     "group": "temperature",
    #     "description": (
    #         "Number of distinct heat-wave spells per year (HWDI definition)."
    #     ),
    # },
    # {
    #     "name": "Heat Wave Frequency Index (HWFI, #Events)",
    #     "slug": "hwfi_events_tmean_90p",
    #     "var": "tas",
    #     "value_col": "hwfi_events_count",
    #     "units": "events",
    #     "compute": "heatwave_event_count_percentile",
    #     "params": {"baseline_years": (1985, 2014), "pct": 90},
    #     "group": "temperature",
    #     "description": (
    #         "Number of distinct heat-wave spells per year (HWFI definition)."
    #     ),
    # },
    # {
    #     "name": "Heatwave Magnitude (HWM)",
    #     "slug": "hwm_heatwave_magnitude",
    #     "var": "tasmax",
    #     "value_col": "hwm_mean_temp_C",
    #     "units": "°C",
    #     "compute": "heatwave_magnitude",
    #     "params": {"baseline_years": (1985, 2014), "min_spell_days": 3},
    #     "group": "temperature",
    #     "description": (
    #         "Mean temperature across all heatwave days in the year. "
    #         "Climdex HWM index."
    #     ),
    # },
    # {
    #     "name": "Heatwave Amplitude (HWA)",
    #     "slug": "hwa_heatwave_amplitude",
    #     "var": "tasmax",
    #     "value_col": "hwa_peak_temp_C",
    #     "units": "°C",
    #     "compute": "heatwave_amplitude",
    #     "params": {"baseline_years": (1985, 2014), "min_spell_days": 3},
    #     "group": "temperature",
    #     "description": (
    #         "Peak daily temperature in the hottest heatwave of the year. "
    #         "Climdex HWA index."
    #     ),
    # },
    
    # # --- Mean Temperature Indices ---
    # {
    #     "name": "Annual Max Temperature (Mean)",
    #     "slug": "tasmax_annual_mean",
    #     "var": "tasmax",
    #     "value_col": "annual_tasmax_mean_C",
    #     "units": "°C",
    #     "compute": "annual_mean",
    #     "params": {},
    #     "group": "temperature",
    #     "description": (
    #         "Annual mean of daily maximum temperature (°C). Climdex TXm."
    #     ),
    # },
    # {
    #     "name": "Summer Max Temperature (MAM Mean)",
    #     "slug": "tasmax_summer_mean",
    #     "var": "tasmax",
    #     "value_col": "summer_tasmax_mean_C",
    #     "units": "°C",
    #     "compute": "seasonal_mean",
    #     "params": {"months": [3, 4, 5]},
    #     "group": "temperature",
    #     "description": (
    #         "Mean of daily maximum temperature during summer (March–May)."
    #     ),
    # },
    # {
    #     "name": "Annual Min Temperature (Mean)",
    #     "slug": "tasmin_annual_mean",
    #     "var": "tasmin",
    #     "value_col": "annual_tasmin_mean_C",
    #     "units": "°C",
    #     "compute": "annual_mean",
    #     "params": {},
    #     "group": "temperature",
    #     "description": (
    #         "Annual mean of daily minimum temperature (°C). Climdex TNm."
    #     ),
    # },
    # {
    #     "name": "Winter Min Temperature (DJF Mean)",
    #     "slug": "tasmin_winter_mean",
    #     "var": "tasmin",
    #     "value_col": "winter_tasmin_mean_C",
    #     "units": "°C",
    #     "compute": "seasonal_mean",
    #     "params": {"months": [12, 1, 2]},
    #     "group": "temperature",
    #     "description": (
    #         "Mean of daily minimum temperature during winter (December–February)."
    #     ),
    # },
    # {
    #     "name": "Daily Temperature Range (DTR)",
    #     "slug": "dtr_daily_temp_range",
    #     "var": "tasmax",  # Requires both tasmax and tasmin
    #     "value_col": "dtr_mean_C",
    #     "units": "°C",
    #     "compute": "daily_temperature_range",
    #     "params": {},
    #     "group": "temperature",
    #     "description": (
    #         "Mean difference between daily maximum and minimum temperature. "
    #         "Climdex DTR index."
    #     ),
    # },
    # {
    #     "name": "Extreme Temperature Range (ETR)",
    #     "slug": "etr_extreme_temp_range",
    #     "var": "tasmax",  # Requires both tasmax and tasmin
    #     "value_col": "etr_range_C",
    #     "units": "°C",
    #     "compute": "extreme_temperature_range",
    #     "params": {},
    #     "group": "temperature",
    #     "description": (
    #         "Difference between highest TX and lowest TN in the year. "
    #         "Climdex ETR index."
    #     ),
    # },
    
    # # =========================================================================
    # # 2. COLD RISK INDICES
    # # =========================================================================
    # {
    #     "name": "Frost Days (FD, TN < 0°C)",
    #     "slug": "fd_frost_days",
    #     "var": "tasmin",
    #     "value_col": "frost_days",
    #     "units": "days",
    #     "compute": "count_days_below_threshold",
    #     "params": {"thresh_k": 0.0 + 273.15},
    #     "group": "temperature",
    #     "description": (
    #         "Number of days when daily minimum temperature is below 0°C. "
    #         "Climdex FD index. Critical for agriculture."
    #     ),
    # },
    # {
    #     "name": "Icing Days (ID, TX < 0°C)",
    #     "slug": "id_icing_days",
    #     "var": "tasmax",
    #     "value_col": "icing_days",
    #     "units": "days",
    #     "compute": "count_days_below_threshold",
    #     "params": {"thresh_k": 0.0 + 273.15},
    #     "group": "temperature",
    #     "description": (
    #         "Number of days when daily maximum temperature is below 0°C. "
    #         "Climdex ID index. Indicates severe cold."
    #     ),
    # },
    # {
    #     "name": "Cold Nights (TN < 2°C)",
    #     "slug": "tnlt2_cold_nights",
    #     "var": "tasmin",
    #     "value_col": "days_tn_lt_2C",
    #     "units": "days",
    #     "compute": "count_days_below_threshold",
    #     "params": {"thresh_k": 2.0 + 273.15},
    #     "group": "temperature",
    #     "description": (
    #         "Number of days when daily minimum temperature is below 2°C. "
    #         "Climdex TNlt2 index."
    #     ),
    # },
    # {
    #     "name": "Very Cold Nights (TN < -2°C)",
    #     "slug": "tnltm2_very_cold_nights",
    #     "var": "tasmin",
    #     "value_col": "days_tn_lt_m2C",
    #     "units": "days",
    #     "compute": "count_days_below_threshold",
    #     "params": {"thresh_k": -2.0 + 273.15},
    #     "group": "temperature",
    #     "description": (
    #         "Number of days when daily minimum temperature is below -2°C. "
    #         "Climdex TNltm2 index."
    #     ),
    # },
    # {
    #     "name": "Cold Spell Duration Index (CSDI)",
    #     "slug": "csdi_cold_spell_days",
    #     "var": "tasmin",
    #     "value_col": "csdi_days",
    #     "units": "days",
    #     "compute": "cold_spell_duration_index",
    #     "params": {"percentile": 10, "min_spell_days": 6, "baseline_years": (1985, 2014)},
    #     "group": "temperature",
    #     "description": (
    #         "Annual count of days contributing to cold spells, where a cold spell "
    #         "is ≥6 consecutive days with TN < 10th percentile. Climdex CSDI."
    #     ),
    # },
    # {
    #     "name": "Growing Season Length (GSL)",
    #     "slug": "gsl_growing_season",
    #     "var": "tas",
    #     "value_col": "gsl_days",
    #     "units": "days",
    #     "compute": "growing_season_length",
    #     "params": {"thresh_k": 5.0 + 273.15, "min_spell_days": 6},
    #     "group": "temperature",
    #     "description": (
    #         "Number of days between first span of ≥6 days with TM > 5°C and "
    #         "first span after July 1 of ≥6 days with TM < 5°C. Climdex GSL."
    #     ),
    # },
    
    # # =========================================================================
    # # 3. PRECIPITATION / FLOOD-RELATED INDICES
    # # =========================================================================
    
    # # --- Precipitation Extremes ---
    # {
    #     "name": "Maximum 1-day Precipitation (Rx1day)",
    #     "slug": "pr_max_1day_precip",
    #     "var": "pr",
    #     "value_col": "max_1day_precip_mm",
    #     "units": "mm",
    #     "compute": "rx1day",
    #     "params": {},
    #     "group": "rain",
    #     "description": (
    #         "Maximum precipitation recorded on any single day in the year. "
    #         "Climdex Rx1day index."
    #     ),
    # },
    # {
    #     "name": "Maximum 5-day Precipitation (Rx5day)",
    #     "slug": "pr_max_5day_precip",
    #     "var": "pr",
    #     "value_col": "max_5day_precip_mm",
    #     "units": "mm",
    #     "compute": "rx5day",
    #     "params": {},
    #     "group": "rain",
    #     "description": (
    #         "Maximum total precipitation over any consecutive 5-day period. "
    #         "Climdex Rx5day index."
    #     ),
    # },
    
    # # --- Precipitation Threshold Indices ---
    # {
    #     "name": "Rainy Days (PR > 2.5mm)",
    #     "slug": "rain_gt_2p5mm",
    #     "var": "pr",
    #     "value_col": "days_rain_gt_2p5mm",
    #     "units": "days",
    #     "compute": "count_rainy_days",
    #     "params": {"thresh_mm": 2.5},
    #     "group": "rain",
    #     "description": (
    #         "Number of days with precipitation exceeding 2.5mm."
    #     ),
    # },
    # {
    #     "name": "Heavy Precipitation Days (R10mm)",
    #     "slug": "pr_heavy_precip_days_gt10mm",
    #     "var": "pr",
    #     "value_col": "heavy_precip_days_gt_10mm",
    #     "units": "days",
    #     "compute": "count_rainy_days",
    #     "params": {"thresh_mm": 10.0},
    #     "group": "rain",
    #     "description": (
    #         "Number of days with precipitation ≥ 10mm. Climdex R10mm index."
    #     ),
    # },
    # {
    #     "name": "Very Heavy Precipitation Days (R20mm)",
    #     "slug": "r20mm_very_heavy_precip_days",
    #     "var": "pr",
    #     "value_col": "r20mm_days",
    #     "units": "days",
    #     "compute": "count_rainy_days",
    #     "params": {"thresh_mm": 20.0},
    #     "group": "rain",
    #     "description": (
    #         "Number of days with precipitation ≥ 20mm. Climdex R20mm index."
    #     ),
    # },
    # {
    #     "name": "Extreme Precipitation Days (PR > 25mm)",
    #     "slug": "pr_very_heavy_precip_days_gt25mm",
    #     "var": "pr",
    #     "value_col": "very_heavy_precip_days_gt_25mm",
    #     "units": "days",
    #     "compute": "count_rainy_days",
    #     "params": {"thresh_mm": 25.0},
    #     "group": "rain",
    #     "description": (
    #         "Number of days with precipitation exceeding 25mm."
    #     ),
    # },
    
    # # --- Percentile-based Precipitation Indices ---
    # {
    #     "name": "Very Wet Day Precipitation (R95p)",
    #     "slug": "r95p_very_wet_precip",
    #     "var": "pr",
    #     "value_col": "r95p_mm",
    #     "units": "mm",
    #     "compute": "percentile_precipitation_total",
    #     "params": {"percentile": 95, "baseline_years": (1985, 2014)},
    #     "group": "rain",
    #     "description": (
    #         "Total precipitation on days exceeding the 95th percentile of "
    #         "wet-day precipitation. Climdex R95p index."
    #     ),
    # },
    # {
    #     "name": "Extremely Wet Day Precipitation (R99p)",
    #     "slug": "r99p_extreme_wet_precip",
    #     "var": "pr",
    #     "value_col": "r99p_mm",
    #     "units": "mm",
    #     "compute": "percentile_precipitation_total",
    #     "params": {"percentile": 99, "baseline_years": (1985, 2014)},
    #     "group": "rain",
    #     "description": (
    #         "Total precipitation on days exceeding the 99th percentile of "
    #         "wet-day precipitation. Climdex R99p index."
    #     ),
    # },
    # {
    #     "name": "Very Wet Day Contribution (R95pTOT)",
    #     "slug": "r95ptot_contribution_pct",
    #     "var": "pr",
    #     "value_col": "r95ptot_pct",
    #     "units": "%",
    #     "compute": "percentile_precipitation_contribution",
    #     "params": {"percentile": 95, "baseline_years": (1985, 2014)},
    #     "group": "rain",
    #     "description": (
    #         "Percentage of total precipitation from very wet days (> 95th pctl). "
    #         "Climdex R95pTOT = 100 × R95p / PRCPTOT."
    #     ),
    # },
    # {
    #     "name": "Extremely Wet Day Contribution (R99pTOT)",
    #     "slug": "r99ptot_contribution_pct",
    #     "var": "pr",
    #     "value_col": "r99ptot_pct",
    #     "units": "%",
    #     "compute": "percentile_precipitation_contribution",
    #     "params": {"percentile": 99, "baseline_years": (1985, 2014)},
    #     "group": "rain",
    #     "description": (
    #         "Percentage of total precipitation from extremely wet days (> 99th pctl). "
    #         "Climdex R99pTOT = 100 × R99p / PRCPTOT."
    #     ),
    # },
    
    # # --- Precipitation Intensity & Totals ---
    # {
    #     "name": "Simple Daily Intensity Index (SDII)",
    #     "slug": "pr_simple_daily_intensity",
    #     "var": "pr",
    #     "value_col": "simple_daily_intensity_mm_per_day",
    #     "units": "mm/day",
    #     "compute": "simple_daily_intensity_index",
    #     "params": {"wet_day_thresh_mm": 1.0},
    #     "group": "rain",
    #     "description": (
    #         "Mean precipitation on wet days (days with ≥ 1mm). Climdex SDII index."
    #     ),
    # },
    # {
    #     "name": "Total Wet-Day Precipitation (PRCPTOT)",
    #     "slug": "prcptot_annual_total",
    #     "var": "pr",
    #     "value_col": "prcptot_mm",
    #     "units": "mm",
    #     "compute": "total_wet_day_precipitation",
    #     "params": {"wet_thresh_mm": 1.0},
    #     "group": "rain",
    #     "description": (
    #         "Total precipitation from all wet days (≥ 1mm) in the year. "
    #         "Climdex PRCPTOT index."
    #     ),
    # },
    
    # # --- Wet/Dry Spell Indices ---
    # {
    #     "name": "Consecutive Wet Days (CWD)",
    #     "slug": "cwd_consecutive_wet_days",
    #     "var": "pr",
    #     "value_col": "cwd_max_spell_len",
    #     "units": "days",
    #     "compute": "consecutive_wet_days",
    #     "params": {"wet_thresh_mm": 1.0},
    #     "group": "rain",
    #     "description": (
    #         "Maximum length of consecutive days with precipitation ≥ 1mm. "
    #         "Climdex CWD index."
    #     ),
    # },
    # {
    #     "name": "Consecutive 5-day Precipitation Events (> 50mm)",
    #     "slug": "pr_5day_precip_events_gt50mm",
    #     "var": "pr",
    #     "value_col": "consec_5day_precip_events",
    #     "units": "events",
    #     "compute": "rx5day_events_over_threshold",
    #     "params": {"event_thresh_mm": 50.0},
    #     "group": "rain",
    #     "description": (
    #         "Number of 5-day periods where total precipitation exceeds 50mm."
    #     ),
    # },
    
    # # =========================================================================
    # # 4. DROUGHT / DRYNESS INDICES
    # # =========================================================================
    # {
    #     "name": "Consecutive Dry Days (CDD)",
    #     "slug": "pr_consecutive_dry_days_lt1mm",
    #     "var": "pr",
    #     "value_col": "consecutive_dry_days",
    #     "units": "days",
    #     "compute": "consecutive_dry_days",
    #     "params": {"dry_thresh_mm": 1.0},
    #     "group": "rain",
    #     "description": (
    #         "Maximum consecutive days with precipitation < 1mm. Climdex CDD index."
    #     ),
    # },
    # {
    #     "name": "Consecutive Dry Day Events (> 5 days)",
    #     "slug": "pr_consecutive_dry_day_events_gt5",
    #     "var": "pr",
    #     "value_col": "consecutive_dry_day_events",
    #     "units": "events",
    #     "compute": "consecutive_dry_day_events",
    #     "params": {"dry_thresh_mm": 1.0, "min_event_days": 6},
    #     "group": "rain",
    #     "description": (
    #         "Number of dry spells lasting more than 5 consecutive days."
    #     ),
    # },
    # {
    #     "name": "Standardised Precipitation Index 3-month (SPI3)",
    #     "slug": "spi3_drought_index",
    #     "var": "pr",
    #     "value_col": "spi3_index",
    #     "units": "index",
    #     "compute": "standardised_precipitation_index",
    #     "params": {"scale_months": 3, "baseline_years": (1985, 2014)},
    #     "group": "rain",
    #     "description": (
    #         "3-month Standardised Precipitation Index. Measures short-term "
    #         "drought conditions. Climdex SPI."
    #     ),
    # },
    # {
    #     "name": "Standardised Precipitation Index 6-month (SPI6)",
    #     "slug": "spi6_drought_index",
    #     "var": "pr",
    #     "value_col": "spi6_index",
    #     "units": "index",
    #     "compute": "standardised_precipitation_index",
    #     "params": {"scale_months": 6, "baseline_years": (1985, 2014)},
    #     "group": "rain",
    #     "description": (
    #         "6-month Standardised Precipitation Index. Measures medium-term "
    #         "drought conditions. Climdex SPI."
    #     ),
    # },
    # {
    #     "name": "Standardised Precipitation Index 12-month (SPI12)",
    #     "slug": "spi12_drought_index",
    #     "var": "pr",
    #     "value_col": "spi12_index",
    #     "units": "index",
    #     "compute": "standardised_precipitation_index",
    #     "params": {"scale_months": 12, "baseline_years": (1985, 2014)},
    #     "group": "rain",
    #     "description": (
    #         "12-month Standardised Precipitation Index. Measures long-term "
    #         "drought conditions. Climdex SPI."
    #     ),
    # },
    # {
    #     "name": "Standardised Precip-Evapotranspiration Index 3-month (SPEI3)",
    #     "slug": "spei3_drought_index",
    #     "var": "pr",  # Also requires tasmax/tasmin for ET calculation
    #     "value_col": "spei3_index",
    #     "units": "index",
    #     "compute": "standardised_precipitation_evapotranspiration_index",
    #     "params": {"scale_months": 3, "baseline_years": (1985, 2014)},
    #     "group": "rain",
    #     "description": (
    #         "3-month SPEI incorporating evapotranspiration. More comprehensive "
    #         "drought measure than SPI. Climdex SPEI."
    #     ),
    # },
    # {
    #     "name": "Standardised Precip-Evapotranspiration Index 6-month (SPEI6)",
    #     "slug": "spei6_drought_index",
    #     "var": "pr",
    #     "value_col": "spei6_index",
    #     "units": "index",
    #     "compute": "standardised_precipitation_evapotranspiration_index",
    #     "params": {"scale_months": 6, "baseline_years": (1985, 2014)},
    #     "group": "rain",
    #     "description": (
    #         "6-month SPEI incorporating evapotranspiration. Climdex SPEI."
    #     ),
    # },
    # {
    #     "name": "Standardised Precip-Evapotranspiration Index 12-month (SPEI12)",
    #     "slug": "spei12_drought_index",
    #     "var": "pr",
    #     "value_col": "spei12_index",
    #     "units": "index",
    #     "compute": "standardised_precipitation_evapotranspiration_index",
    #     "params": {"scale_months": 12, "baseline_years": (1985, 2014)},
    #     "group": "rain",
    #     "description": (
    #         "12-month SPEI incorporating evapotranspiration. Climdex SPEI."
    #     ),
    # },
]

# Typed views derived from pipeline metrics
PIPELINE_METRICS: list[MetricSpec] = [MetricSpec.from_pipeline_dict(m) for m in PIPELINE_METRICS_RAW]
METRICS_BY_SLUG: dict[str, MetricSpec] = build_registry_from_pipeline(PIPELINE_METRICS_RAW)


# -----------------------------------------------------------------------------
# DASHBOARD DISCOVERY TEMPLATES
# -----------------------------------------------------------------------------
# These templates define where to look for yearly ensemble CSVs.
# The actual data structure is:
#   {root}/{state}/districts/ensembles/{district}/{scenario}/{district}_yearly_ensemble.csv
DEFAULT_DISTRICT_YEARLY_CANDIDATES = [
    # NEW structure (current data layout)
    "{root}/{state}/districts/ensembles/{district}/{scenario}/{district}_yearly_ensemble.csv",
    "{root}/{state}/districts/ensembles/{district_underscored}/{scenario}/{district_underscored}_yearly_ensemble.csv",
    # OLD structure (legacy fallback)
    "{root}/{state}/{district_underscored}/ensembles/{scenario}/{district_underscored}_yearly_ensemble.csv",
    "{root}/{state}/{district}/ensembles/{scenario}/{district}_yearly_ensemble.csv",
    # Legacy single-file structure
    "{root}/{state}/{district_underscored}/district_yearly_ensemble_stats.csv",
    "{root}/{state}/{district}/district_yearly_ensemble_stats.csv",
]

DEFAULT_STATE_YEARLY_CANDIDATES = [
    "{root}/{state}/state_yearly_ensemble_stats.csv",
]


def get_dashboard_variables() -> dict[str, dict[str, Any]]:
    """
    Generate the VARIABLES dict for the dashboard from the unified registry.
    """
    variables = {}
    for slug, spec in METRICS_BY_SLUG.items():
        variables[slug] = {
            "label": spec.label,
            "group": spec.group,
            "periods_metric_col": spec.periods_metric_col,
            "description": spec.description or "",
            "district_yearly_candidates": list(spec.district_yearly_candidates or DEFAULT_DISTRICT_YEARLY_CANDIDATES),
            "state_yearly_candidates": list(spec.state_yearly_candidates or DEFAULT_STATE_YEARLY_CANDIDATES),
        }
    return variables


def get_metrics_by_group() -> dict[str, list[str]]:
    """Return metrics organized by group."""
    groups: dict[str, list[str]] = {}
    for slug, spec in METRICS_BY_SLUG.items():
        group = spec.group or "other"
        if group not in groups:
            groups[group] = []
        groups[group].append(slug)
    return groups


def get_metric_count() -> dict[str, int]:
    """Return count of metrics per group."""
    groups = get_metrics_by_group()
    return {g: len(slugs) for g, slugs in groups.items()}
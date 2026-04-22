"""
Shared metrics registry for the India Resilience Tool (IRT).

This module unifies:
- Dashboard metric registry needs (slug/label/group/periods_metric_col + discovery templates)
- Pipeline metric specs (var/value_col/compute/params/units)
- Dashboard taxonomy metadata (assessment pillars + domains)

Includes all standard Climdex indices plus custom IRT indices.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

from india_resilience_tool.config.composite_metrics import VISIBLE_GLANCE_COMPOSITES


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


SEVERITY_CLASS_LABELS: dict[int, str] = {
    1: "VeryLow",
    2: "Low",
    3: "Moderate",
    4: "High",
    5: "Extreme",
}


@dataclass(frozen=True)
class MetricSpec:
    """
    Unified metric specification used by both dashboard and pipeline.
    """

    slug: str
    label: str
    group: str
    periods_metric_col: str

    # Ranking metadata (dashboard "Position in state" / risk quick-glance)
    # If True: higher values are ranked as worse (rank 1 = highest value).
    # If False: lower values are ranked as worse (rank 1 = lowest value).
    rank_higher_is_worse: bool = True

    # Pipeline fields
    var: Optional[str] = None
    vars: Optional[Sequence[str]] = None
    value_col: Optional[str] = None
    units: Optional[str] = None
    display_units: Optional[str] = None
    display_scale: float = 1.0
    class_labels: Optional[Mapping[int, str]] = None
    class_display_mode: Optional[str] = None
    compute: Optional[str] = None
    params: Mapping[str, Any] = field(default_factory=dict)

    # Optional metadata
    name: Optional[str] = None
    description: Optional[str] = None
    aliases: Sequence[str] = field(default_factory=tuple)
    source_type: str = "pipeline"
    supports_yearly_trend: bool = True
    selection_mode: str = "scenario_period"
    fixed_scenario: Optional[str] = None
    fixed_period: Optional[str] = None
    supported_statistics: Sequence[str] = field(default_factory=lambda: ("mean", "median"))
    supports_baseline_comparison: bool = True
    supports_scenario_comparison: bool = True
    admin_rebuild_command: Optional[str] = None
    hydro_rebuild_command: Optional[str] = None
    supported_scenarios: Sequence[str] = field(default_factory=tuple)
    preferred_period_order: Sequence[str] = field(default_factory=tuple)
    supported_spatial_families: Sequence[str] = field(default_factory=tuple)
    supported_levels: Sequence[str] = field(default_factory=tuple)
    supported_admin_states: Sequence[str] = field(default_factory=tuple)

    # Discovery templates
    district_yearly_candidates: Optional[Sequence[str]] = None
    state_yearly_candidates: Optional[Sequence[str]] = None

    @staticmethod
    def from_pipeline_dict(d: Mapping[str, Any]) -> "MetricSpec":
        """Create a MetricSpec from a pipeline metric dict."""
        slug = str(d["slug"])
        var = str(d.get("var", "") or "")
        vars_raw = d.get("vars")
        vars_list = [str(v) for v in vars_raw] if isinstance(vars_raw, (list, tuple)) else None
        value_col = str(d.get("value_col", "") or "")
        label = str(d.get("label") or d.get("name") or slug)
        group = str(d.get("group") or infer_group_from_var(var))
        periods_metric_col = str(d.get("periods_metric_col") or value_col)

        return MetricSpec(
            slug=slug,
            label=label,
            group=group,
            periods_metric_col=periods_metric_col,
            rank_higher_is_worse=bool(d.get("rank_higher_is_worse", True)),
            var=var or None,
            vars=tuple(vars_list) if vars_list else None,
            value_col=value_col or None,
            units=str(d.get("units") or "") or None,
            display_units=str(d.get("display_units") or "") or None,
            display_scale=float(d.get("display_scale", 1.0) or 1.0),
            class_labels=d.get("class_labels"),
            class_display_mode=str(d.get("class_display_mode") or "").strip() or None,
            compute=str(d.get("compute") or "") or None,
            params=d.get("params") or {},
            name=str(d.get("name") or "") or None,
            description=str(d.get("description") or "") or None,
            aliases=tuple(d.get("aliases") or ()),
            source_type=str(d.get("source_type") or "pipeline").strip() or "pipeline",
            supports_yearly_trend=bool(d.get("supports_yearly_trend", True)),
            selection_mode=str(d.get("selection_mode") or "scenario_period").strip() or "scenario_period",
            fixed_scenario=str(d.get("fixed_scenario") or "").strip() or None,
            fixed_period=str(d.get("fixed_period") or "").strip() or None,
            supported_statistics=tuple(
                str(v) for v in (d.get("supported_statistics") or ("mean", "median"))
            ),
            supports_baseline_comparison=bool(d.get("supports_baseline_comparison", True)),
            supports_scenario_comparison=bool(d.get("supports_scenario_comparison", True)),
            admin_rebuild_command=str(d.get("admin_rebuild_command") or "").strip() or None,
            hydro_rebuild_command=str(d.get("hydro_rebuild_command") or "").strip() or None,
            supported_scenarios=tuple(str(v) for v in (d.get("supported_scenarios") or ())),
            preferred_period_order=tuple(str(v) for v in (d.get("preferred_period_order") or ())),
            supported_spatial_families=tuple(str(v) for v in (d.get("supported_spatial_families") or ())),
            supported_levels=tuple(str(v) for v in (d.get("supported_levels") or ())),
            supported_admin_states=tuple(str(v) for v in (d.get("supported_admin_states") or ())),
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
    # =========================================================================
    # 1. HEAT RISK / THERMAL STRESS INDICES
    # =========================================================================
    
    # --- Temperature Extremes ---
    {
        "name": "Annual Maximum Temperature (TXx)",
        "slug": "txx_annual_max",
        "var": "tasmax",
        "value_col": "txx_annual_max_C",
        "units": "°C",
        "compute": "annual_max_temperature",
        "params": {},
        "group": "temperature",
        "description": (
            "The highest daily maximum temperature recorded in the year (°C). "
            "Climdex index TXx."
        ),
    },
    {
        "name": "Warmest Night ",
        "slug": "tnx_annual_max",
        "var": "tasmin",
        "value_col": "tnx_annual_max_C",
        "units": "°C",
        "compute": "annual_max_temperature",
        "params": {},
        "group": "temperature",
        "description": (
            "The highest daily minimum temperature recorded in the year (°C). "
            "Indicates warmest night. Climdex index TNx."
        ),
    },
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
    {
        "name": "Annual Minimum of Daily Minimum Temperature (TNn)",
        "slug": "tnn_annual_min",
        "var": "tasmin",
        "value_col": "tnn_annual_min_C",
        "units": "°C",
        "compute": "annual_min_temperature",
        "params": {},
        "group": "temperature",
        "description": (
            "The lowest daily minimum temperature recorded in the year (°C). "
            "Indicates coldest night. Climdex index TNn."
        ),
    },
    
    # --- Percentile-based Temperature Indices ---
    {
        "name": "Hot Days (TX90p)",
        "slug": "tx90p_hot_days_pct",
        "var": "tasmax",
        "value_col": "tx90p_pct",
        "units": "%",
        # ETCCDI-aligned TX90p (calendar-day percentile threshold using a moving window)
        "compute": "tx90p_etccdi",
        "params": {
            "percentile": 90,
            # Match ETCCDI reference baseline you validated against
            "baseline_years": (1981, 2010),
            # ETCCDI-style moving window (5-day = +/-2 days around day-of-year)
            "window_days": 5,
            # Quantile method matters for exact matching (esp. small samples)
            "quantile_method": "nearest",
            # ETCCDI convention is strictly "above" the percentile; set True only if you
            # found the reference behaves like >= for your dataset (keep False by default)
            "exceed_ge": True,
            # Optional: if your final best match used smoothing on daily thresholds,
            # set an integer window (e.g., 5). Otherwise omit or keep None.
            # "smooth": 5,
        },
        "group": "temperature",
        "description": (
            "Percentage of days when daily maximum temperature exceeds the 90th "
            "percentile threshold computed per calendar day from the baseline period "
            "using a moving window (ETCCDI TX90p)."
        ),
    },
    {
        "name": "Warm Nights (TN90p)",
        "slug": "tn90p_warm_nights_pct",
        "var": "tasmin",
        "value_col": "tn90p_pct",
        "units": "%",
        # Route through the ETCCDI multi-year baseline workflow (same as TX90p)
        "compute": "tx90p_etccdi",
        "params": {
            "percentile": 90,
            "baseline_years": (1981, 2010),
            "window_days": 5,
            "quantile_method": "nearest",
            # ETCCDI convention is ">" not ">="; keep False unless you intentionally chose otherwise
            "exceed_ge": True,
            # optional smoothing if needed later
            # "smooth": 5,
        },
        "group": "temperature",
        "description": (
            "Percentage of days when daily minimum temperature exceeds the 90th "
            "percentile threshold computed per calendar day from the baseline period "
            "using a moving window (ETCCDI TN90p-style)."
        ),
    },
    {
        "name": "Cool Days (TX10p)",
        "slug": "tx10p_cool_days_pct",
        "var": "tasmax",
        "value_col": "tx10p_pct",
        "units": "%",
        # ETCCDI-aligned TX10p using the same multi-year baseline workflow as TX90p
        "compute": "tx90p_etccdi",
        "params": {
            "percentile": 10,
            "baseline_years": (1981, 2010),
            "window_days": 5,
            "quantile_method": "nearest",
            # For "below-percentile" indices, exceed_ge=True means inclusive (<= threshold)
            "exceed_ge": True,
            "direction": "below",
            # "smooth": 5,
        },
        "group": "temperature",
        "description": (
            "Percentage of days when daily maximum temperature is below the 10th "
            "percentile threshold computed per calendar day from the baseline period "
            "using a moving window (ETCCDI TX10p)."
        ),
    },
    {
        "name": "Cool Nights (TN10p)",
        "slug": "tn10p_cool_nights_pct",
        "var": "tasmin",
        "value_col": "tn10p_pct",
        "units": "%",
        # ETCCDI-aligned TN10p using the same multi-year baseline workflow as TN90p
        "compute": "tx90p_etccdi",
        "params": {
            "percentile": 10,
            "baseline_years": (1981, 2010),
            "window_days": 5,
            "quantile_method": "nearest",
            # For "below-percentile" indices, exceed_ge=True means inclusive (<= threshold)
            "exceed_ge": True,
            "direction": "below",
            # "smooth": 5,
        },
        "group": "temperature",
        "description": (
            "Percentage of days when daily minimum temperature is below the 10th "
            "percentile threshold computed per calendar day from the baseline period "
            "using a moving window (ETCCDI TN10p)."
        ),
    },
    
    # --- Threshold-based Heat Indices ---
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
    # {
    #     "name": "Summer Days (TX > 32°C)",
    #     "slug": "tas_gt32",
    #     "var": "tasmax",
    #     "value_col": "days_gt_32C",
    #     "units": "days",
    #     "compute": "count_days_above_threshold",
    #     "params": {"thresh_k": 32.0 + 273.15},
    #     "group": "temperature",
    #     "description": (
    #         "Number of days when daily maximum temperature exceeds 32°C. "
    #         "India-specific higher threshold for summer days."
    #     ),
    # },
{
    "name": "Wet-Bulb Temperature (Annual Mean)",
    "slug": "twb_annual_mean",
    "var": "tas",
    "vars": ["tas", "hurs"],
    "value_col": "twb_annual_mean_C",
    "units": "°C",
    "compute": "wet_bulb_annual_mean_stull",
    "params": {},
    "group": "temperature",
    "description": (
        "Annual mean wet-bulb temperature (°C) derived from near-surface air temperature (tas) "
        "and relative humidity (hurs) using the Stull (2011) approximation."
    ),
},
{
    "name": "Wet-Bulb Temperature (Summer Mean; MAM Mean)",
    "slug": "twb_summer_mean",
    "var": "tas",
    "vars": ["tas", "hurs"],
    "value_col": "summer_twb_mean_C",
    "units": "°C",
    "compute": "wet_bulb_seasonal_mean_stull",
    "params": {"months": [3, 4, 5]},
    "group": "temperature",
    "description": (
        "Mean wet-bulb temperature (°C) during summer (March-May), derived from "
        "near-surface air temperature (tas) and relative humidity (hurs) using "
        "the Stull (2011) approximation."
    ),
},
{
    "name": "Wet-Bulb Temperature (Annual Max)",
    "slug": "twb_annual_max",
    "var": "tas",
    "vars": ["tas", "hurs"],
    "value_col": "twb_annual_max_C",
    "units": "°C",
    "compute": "wet_bulb_annual_max_stull",
    "params": {},
    "group": "temperature",
    "description": (
        "Annual maximum wet-bulb temperature (°C) derived from near-surface air temperature (tas) "
        "and relative humidity (hurs) using the Stull (2011) approximation."
    ),
},
{
    "name": "Wet-Bulb Days (Twb ≥ 30°C)",
    "slug": "twb_days_ge_30",
    "var": "tas",
    "vars": ["tas", "hurs"],
    "value_col": "twb_days_ge_30_days",
    "units": "days",
    "compute": "wet_bulb_days_ge_threshold_stull",
    "params": {"thresh_c": 30.0},
    "group": "temperature",
    "description": (
        "Number of days per year with wet-bulb temperature ≥ 30°C, derived from tas and hurs "
        "using the Stull (2011) approximation."
    ),
},
{
    "name": "Severe Humid-Heat Days (WBD ≤ 3°C)",
    "slug": "wbd_le_3",
    "var": "tas",
    "vars": ["tas", "hurs"],
    "value_col": "wbd_le_3_days",
    "units": "days",
    "compute": "wet_bulb_depression_days_le_threshold_stull",
    "params": {"thresh_c": 3.0},
    "group": "temperature",
    "description": (
        "Number of days per year where wet-bulb depression (tas − Twb) is ≤ 3°C, "
        "derived from tas and hurs using the Stull (2011) approximation. Low depression "
        "indicates very humid conditions and reduced evaporative cooling, increasing heat stress."
    ),
},
{
    "name": "Humid-Heat Days (WBD ≤ 6°C)",
    "slug": "wbd_le_6",
    "var": "tas",
    "vars": ["tas", "hurs"],
    "value_col": "wbd_le_6_days",
    "units": "days",
    "compute": "wet_bulb_depression_days_le_threshold_stull",
    "params": {"thresh_c": 6.0},
    "group": "temperature",
    "description": (
        "Number of days per year where wet-bulb depression (tas − Twb) is ≤ 6°C, "
        "derived from tas and hurs using the Stull (2011) approximation. Low depression "
        "indicates humid conditions and reduced evaporative cooling."
    ),
},
{
    "name": "Moderate Humid-Heat Days (3°C < WBD ≤ 6°C)",
    "slug": "wbd_gt3_le6",
    "var": "tas",
    "vars": ["tas", "hurs"],
    "value_col": "wbd_gt_3_le_6_days",
    "units": "days",
    "compute": "wet_bulb_depression_days_range_stull",
    "params": {
        "lower_c": 3.0,
        "upper_c": 6.0,
        "lower_inclusive": False,
        "upper_inclusive": True,
    },
    "group": "temperature",
    "description": (
        "Number of days per year where wet-bulb depression (tas - Twb) falls in the "
        "moderate humid-heat range 3°C < WBD <= 6°C, derived from tas and hurs using "
        "the Stull (2011) approximation."
    ),
},
    {
        "name": "Hot Days (TX ≥ 30°C)",
        "slug": "txge30_hot_days",
        "var": "tasmax",
        "value_col": "days_tx_ge_30C",
        "units": "days",
        "compute": "count_days_ge_threshold",
        "params": {"thresh_k": 30.0 + 273.15},
        "group": "temperature",
        "description": (
            "Number of days when daily maximum temperature is at or above 30°C. "
            "Climdex index TXge30."
        ),
    },
    {
        "name": "Extreme Heat Days (TX ≥ 35°C)",
        "slug": "txge35_extreme_heat_days",
        "var": "tasmax",
        "value_col": "days_tx_ge_35C",
        "units": "days",
        "compute": "count_days_ge_threshold",
        "params": {"thresh_k": 35.0 + 273.15},
        "group": "temperature",
        "description": (
            "Number of days when daily maximum temperature is at or above 35°C. "
            "Climdex index TXge35. Critical for heat stress."
        ),
    },
    {
        "name": "Tropical Nights (TR, TN > 20°C)",
        "slug": "tasmin_tropical_nights_gt20",
        "var": "tasmin",
        "value_col": "tropical_nights_gt_20C",
        "units": "days",
        "compute": "count_days_above_threshold",
        "params": {"thresh_k": 20.0 + 273.15},
        "group": "temperature",
        "description": (
            "Number of nights when daily minimum temperature exceeds 20°C. "
            "Climdex index TR."
        ),
    },
    {
        "name": "Tropical Nights (TR, TN > 25°C)",
        "slug": "tasmin_tropical_nights_gt25",
        "var": "tasmin",
        "value_col": "tropical_nights_gt_25C",
        "units": "days",
        "compute": "count_days_above_threshold",
        "params": {"thresh_k": 25.0 + 273.15},
        "group": "temperature",
        "description": (
            "Number of nights when daily minimum temperature exceeds 25°C. "
            "This higher tropical-nights threshold is used for the Indian Heat Risk bundle context."
        ),
    },
    {
        "name": "Tropical Nights (TR, TN > 28°C)",
        "slug": "tasmin_tropical_nights_gt28",
        "var": "tasmin",
        "value_col": "tropical_nights_gt_28C",
        "units": "days",
        "compute": "count_days_above_threshold",
        "params": {"thresh_k": 28.0 + 273.15},
        "group": "temperature",
        "description": (
            "Number of nights when daily minimum temperature exceeds 28°C. "
            "This higher threshold is used for the Heat Stress bundle's night-time heat component."
        ),
    },
    {
        "name": "Heat Stress Days (Twb ≥ 28°C)",
        "slug": "twb_days_ge_28",
        "var": "tas",
        "vars": ["tas", "hurs"],
        "value_col": "twb_days_ge_28_days",
        "units": "days",
        "compute": "wet_bulb_days_ge_threshold_stull",
        "params": {"thresh_c": 28.0},
        "group": "temperature",
        "description": (
            "Number of days per year with wet-bulb temperature >= 28°C, derived from tas "
            "and hurs using the Stull (2011) approximation."
        ),
    },
    {
        "name": "Consecutive Wet-Bulb Stress Days (WBD ≤ 3°C)",
        "slug": "wbd_le_3_consecutive_days",
        "var": "tas",
        "vars": ["tas", "hurs"],
        "value_col": "wbd_le_3_consecutive_days",
        "units": "days",
        "compute": "wet_bulb_depression_longest_run_le_threshold_stull",
        "params": {"thresh_c": 3.0, "min_spell_days": 3},
        "group": "temperature",
        "description": (
            "Maximum length of a humid-heat spell where wet-bulb depression (tas - Twb) "
            "stays <= 3°C for at least 3 consecutive days, derived from tas and hurs using "
            "the Stull (2011) approximation."
        ),
    },
    
    # --- Warm/Heat Spell Indices ---
    {
        "name": "Warm Spell Duration Index (WSDI)",
        "slug": "wsdi_warm_spell_days",
        "var": "tasmax",
        "value_col": "wsdi_days",
        "units": "days",
        "compute": "warm_spell_duration_index",
        "params": {
            "baseline_years": (1981, 2010),
            "percentile": 90,
            "window_days": 5,
            "quantile_method": "nearest",
            "exceed_ge": True,
            "smooth": None,
            "min_spell_days": 6,
            "direction": "above",
        },
        "group": "temperature",
        "description": (
            "Annual count of days contributing to warm spells, where a warm spell "
            "is ≥6 consecutive days with TX > 90th percentile. Climdex WSDI."
        ),
    },
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
    #     "value_col": "hwdi_spell_days",
    #     "units": "days",
    #     "compute": "heatwave_duration_index",
    #     "params": {
    #         "baseline_years": (1981, 2010),
    #         "delta_c": 5.0,
    #         "abs_thresh_k": 313.15,
    #         "min_spell_days": 5,
    #     },
    #     "group": "temperature",
    #     "description": (
    #         "Total days inside heat-wave spells (≥5 consecutive days) where TX "
    #         "exceeds max(absolute threshold, baseline mean + 5°C), with the baseline "
    #         "mean computed from the historical reference period."
    #     ),
    # },
    {
        "name": "Heat Wave Frequency Index (HWFI, #Days)",
        "slug": "hwfi_tmean_90p",
        "var": "tas",
        "value_col": "hwfi_days_in_spells",
        "units": "days",
        "compute": "heatwave_frequency_percentile",
        "params": {
            "baseline_years": (1981, 2010),
            "pct": 90,
            "window_days": 5,
            "quantile_method": "nearest",
            "exceed_ge": True,
            "smooth": None,
            "min_spell_days": 5,
        },
        "group": "temperature",
        "description": (
            "Total days inside heat-wave spells, where spells are consecutive days "
            "with mean temperature above a 90th-percentile threshold calibrated from "
            "the baseline period (multi-year ETCCDI-style day-of-year thresholds)."
        ),
    },
    # {
    #     "name": "Heat Wave Duration Index (HWDI, #Events)",
    #     "slug": "hwdi_events_tasmax_plus5C",
    #     "var": "tasmax",
    #     "value_col": "hwdi_events_count",
    #     "units": "events",
    #     "compute": "heatwave_event_count",
    #     "params": {
    #         "baseline_years": (1981, 2010),
    #         "delta_c": 5.0,
    #         "abs_thresh_k": 313.15,
    #         "min_spell_days": 5,
    #     },
    #     "group": "temperature",
    #     "description": (
    #         "Number of distinct heat-wave spells per year where TX exceeds "
    #         "max(absolute threshold, baseline mean + 5°C) for ≥5 consecutive days."
    #     ),
    # },
    {
        "name": "Heat Wave Frequency (tasmax 90p, #Events)",
        "slug": "hwfi_events_tmean_90p",
        "var": "tasmax",
        "value_col": "hwfi_events_count",
        "units": "events",
        "compute": "heatwave_event_count_percentile",
        "params": {
            "baseline_years": (1981, 2010),
            "pct": 90,
            "window_days": 5,
            "quantile_method": "nearest",
            "exceed_ge": True,
            "smooth": None,
            "min_spell_days": 5,
        },
        "group": "temperature",
        "description": (
            "Number of distinct heatwave spells per year, where spells are runs of "
            ">= 5 consecutive days with daily maximum temperature (tasmax) above a "
            "baseline-calibrated day-of-year 90th-percentile threshold (moving-window, "
            "multi-year baseline)."
        ),
    },
    # {
    #     "name": "Heatwave Intensity (mean exceedance)",
    #     "slug": "hwm_heatwave_magnitude",
    #     "var": "tasmax",
    #     "value_col": "hwm_mean_exceedance_C",
    #     "units": "°C above threshold",
    #     "compute": "heatwave_magnitude",
    #     "params": {
    #         "baseline_years": (1981, 2010),
    #         "pct": 90,
    #         "window_days": 5,
    #         "quantile_method": "nearest",
    #         "exceed_ge": True,
    #         "smooth": None,
    #         "min_spell_days": 5,
    #     },
    #     "group": "temperature",
    #     "description": (
    #         "Maximum mean exceedance (°C above threshold) across heatwave spells of "
    #         ">= 5 consecutive days, where the threshold is the baseline-calibrated "
    #         "day-of-year 90th-percentile (tasmax, moving-window, multi-year baseline)."
    #     ),
    # },
    {
        "name": "Heatwave Amplitude (peak day)",
        "slug": "hwa_heatwave_amplitude",
        "var": "tasmax",
        "value_col": "hwa_peak_temp_C",
        "units": "°C",
        "compute": "heatwave_amplitude",
        "params": {
            "baseline_years": (1981, 2010),
            "pct": 90,
            "window_days": 5,
            "quantile_method": "nearest",
            "exceed_ge": True,
            "smooth": None,
            "min_spell_days": 5,
        },
        "group": "temperature",
        "description": (
            "Peak daily maximum temperature (°C) within the hottest heatwave spell "
            "(the spell with the highest mean exceedance above the baseline DOY 90p "
            "threshold), using tasmax and >= 5-day spells."
        ),
    },    
    # --- Mean Temperature Indices ---
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
    {
        "name": "Summer Max Temperature (MAM Mean)",
        "slug": "tasmax_summer_mean",
        "var": "tasmax",
        "value_col": "summer_tasmax_mean_C",
        "units": "°C",
        "compute": "seasonal_mean",
        "params": {"months": [3, 4, 5]},
        "group": "temperature",
        "description": (
            "Mean of daily maximum temperature during summer (March–May)."
        ),
    },
    {
        "name": "Annual Mean Temperature (TM Mean)",
        "slug": "tas_annual_mean",
        "var": "tas",
        "value_col": "annual_tas_mean_C",
        "units": "°C",
        "compute": "annual_mean",
        "params": {},
        "group": "temperature",
        "description": (
            "Annual mean of daily mean near-surface air temperature (°C). "
            "This is the mean daily mean temperature (TM)."
        ),
    },
    {
        "name": "Summer Mean Temperature (TM; MAM Mean)",
        "slug": "tas_summer_mean",
        "var": "tas",
        "value_col": "summer_tas_mean_C",
        "units": "°C",
        "compute": "seasonal_mean",
        "params": {"months": [3, 4, 5]},
        "group": "temperature",
        "description": (
            "Mean of daily mean temperature during summer (March–May). "
            "This is the seasonal TM mean."
        ),
    },
    {
        "name": "Winter Mean Temperature (TM; DJF Mean)",
        "slug": "tas_winter_mean",
        "var": "tas",
        "value_col": "winter_tas_mean_C",
        "units": "°C",
        "compute": "seasonal_mean",
        "params": {"months": [12, 1, 2]},
        "group": "temperature",
        "description": (
            "Mean of daily mean temperature during winter (December–February). "
            "This is the seasonal TM mean."
        ),
    },
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
    {
        "name": "Winter Min Temperature (DJF Mean)",
        "slug": "tasmin_winter_mean",
        "var": "tasmin",
        "value_col": "winter_tasmin_mean_C",
        "units": "°C",
        "compute": "seasonal_mean",
        "params": {"months": [12, 1, 2]},
        "group": "temperature",
        "description": (
            "Mean of daily minimum temperature during winter (December–February)."
        ),
    },
    {
        "name": "Winter Minimum Tmin (DJF Min TN)",
        "slug": "tasmin_winter_min",
        "var": "tasmin",
        "value_col": "winter_tasmin_min_C",
        "units": "°C",
        "compute": "seasonal_min",
        "params": {"months": [12, 1, 2]},
        "group": "temperature",
        "description": (
            "Minimum of daily minimum temperature during winter (December-February). "
            "This captures the coldest winter night in the season."
        ),
    },
    {
        "name": "Daily Temperature Range (DTR)",
        "slug": "dtr_daily_temp_range",
        "var": "tasmax",  # Primary var (also requires tasmin)
        "vars": ["tasmax", "tasmin"],
        "value_col": "dtr_mean_C",
        "units": "°C",
        "compute": "daily_temperature_range",
        "params": {},
        "group": "temperature",
        "description": (
            "Mean difference between daily maximum and minimum temperature. "
            "Climdex DTR index."
        ),
    },
    {
        "name": "Extreme Temperature Range (ETR)",
        "slug": "etr_extreme_temp_range",
        "var": "tasmax",  # Primary var (also requires tasmin)
        "vars": ["tasmax", "tasmin"],
        "value_col": "etr_range_C",
        "units": "°C",
        "compute": "extreme_temperature_range",
        "params": {},
        "group": "temperature",
        "description": (
            "Difference between highest TX and lowest TN in the year. "
            "Climdex ETR index."
        ),
    },
    
    # =========================================================================
    # 2. COLD RISK INDICES
    # =========================================================================
    {
        "name": "Frost Days (FD, TN < 0°C)",
        "slug": "fd_frost_days",
        "var": "tasmin",
        "value_col": "frost_days",
        "units": "days",
        "compute": "count_days_below_threshold",
        "params": {"thresh_k": 0.0 + 273.15},
        "group": "temperature",
        "description": (
            "Number of days when daily minimum temperature is below 0°C. "
            "Climdex FD index. Critical for agriculture."
        ),
    },
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
    {
        "name": "Cold Nights (TN < 2°C)",
        "slug": "tnlt2_cold_nights",
        "var": "tasmin",
        "value_col": "days_tn_lt_2C",
        "units": "days",
        "compute": "count_days_below_threshold",
        "params": {"thresh_k": 2.0 + 273.15},
        "group": "temperature",
        "description": (
            "Number of days when daily minimum temperature is below 2°C. "
            "Climdex TNlt2 index."
        ),
    },
    {
        "name": "Cold Nights (TN <= 10°C)",
        "slug": "tnle10_cold_nights",
        "var": "tasmin",
        "value_col": "days_tn_le_10C",
        "units": "days",
        "compute": "count_days_le_threshold",
        "params": {"thresh_k": 10.0 + 273.15},
        "group": "temperature",
        "description": (
            "Number of days when daily minimum temperature is at or below 10°C. "
            "This workbook-aligned threshold captures cold nights relevant to plains and central India."
        ),
    },
    {
        "name": "Severe Cold Nights (TN <= 5°C)",
        "slug": "tnle5_severe_cold_nights",
        "var": "tasmin",
        "value_col": "days_tn_le_5C",
        "units": "days",
        "compute": "count_days_le_threshold",
        "params": {"thresh_k": 5.0 + 273.15},
        "group": "temperature",
        "description": (
            "Number of days when daily minimum temperature is at or below 5°C. "
            "This workbook-aligned threshold captures more severe night-time cold."
        ),
    },
    {
        "name": "Cold Days (TX <= 15°C)",
        "slug": "txle15_cold_days",
        "var": "tasmax",
        "value_col": "days_tx_le_15C",
        "units": "days",
        "compute": "count_days_le_threshold",
        "params": {"thresh_k": 15.0 + 273.15},
        "group": "temperature",
        "description": (
            "Number of days when daily maximum temperature is at or below 15°C. "
            "This workbook-aligned daytime cold metric strengthens the threshold-based cold group."
        ),
    },
    {
        "name": "Consecutive Cold Nights (TN <= 10°C)",
        "slug": "tnle10_consecutive_cold_nights",
        "var": "tasmin",
        "value_col": "tn_le_10C_consecutive_days",
        "units": "days",
        "compute": "longest_consecutive_run_le_threshold",
        "params": {"thresh_k": 10.0 + 273.15, "min_len": 1},
        "group": "temperature",
        "description": (
            "Maximum consecutive run length of nights when daily minimum temperature is at "
            "or below 10°C."
        ),
    },
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
    {
        "name": "Cold Spell Duration Index (CSDI)",
        "slug": "csdi_cold_spell_days",
        "var": "tasmin",
        "value_col": "csdi_days",
        "units": "days",
        "compute": "cold_spell_duration_index",
        "params": {
            "baseline_years": (1981, 2010),
            "percentile": 10,
            "window_days": 5,
            "quantile_method": "nearest",
            "exceed_ge": True,
            "smooth": None,
            "min_spell_days": 6,
            "direction": "below",
        },
        "group": "temperature",
        "description": (
            "Annual count of days contributing to cold spells, where a cold spell "
            "is ≥6 consecutive days with TN < 10th percentile. Climdex CSDI."
        ),
    },
    {
        "name": "Growing Season Length (GSL)",
        "slug": "gsl_growing_season",
        "rank_higher_is_worse": False,
        "var": "tas",
        "value_col": "gsl_days",
        "units": "days",
        "compute": "growing_season_length",
        "params": {"thresh_k": 5.0 + 273.15, "min_spell_days": 6},
        "group": "temperature",
        "description": (
            "Number of days between first span of ≥6 days with TM > 5°C and "
            "first span after July 1 of ≥6 days with TM < 5°C. Climdex GSL."
        ),
    },
    
    # =========================================================================
    # 3. PRECIPITATION / FLOOD-RELATED INDICES
    # =========================================================================
    
    # --- Precipitation Extremes ---
    {
        "name": "Maximum 1-day Precipitation (Rx1day)",
        "slug": "pr_max_1day_precip",
        "var": "pr",
        "value_col": "max_1day_precip_mm",
        "units": "mm",
        "compute": "rx1day",
        "params": {},
        "group": "rain",
        "description": (
            "Maximum precipitation recorded on any single day in the year. "
            "Climdex Rx1day index."
        ),
    },
    {
        "name": "Maximum 5-day Precipitation (Rx5day)",
        "slug": "pr_max_5day_precip",
        "var": "pr",
        "value_col": "max_5day_precip_mm",
        "units": "mm",
        "compute": "rx5day",
        "params": {},
        "group": "rain",
        "description": (
            "Maximum total precipitation over any consecutive 5-day period. "
            "Climdex Rx5day index."
        ),
    },
    
    # --- Precipitation Threshold Indices ---
    {
        "name": "Rainy Days (PR > 2.5mm)",
        "slug": "rain_gt_2p5mm",
        "var": "pr",
        "value_col": "days_rain_gt_2p5mm",
        "units": "days",
        "compute": "count_rainy_days",
        "params": {"thresh_mm": 2.5},
        "group": "rain",
        "description": (
            "Number of days with precipitation exceeding 2.5mm."
        ),
    },
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
    {
        "name": "Very Heavy Precipitation Days (R20mm)",
        "slug": "r20mm_very_heavy_precip_days",
        "var": "pr",
        "value_col": "r20mm_days",
        "units": "days",
        "compute": "count_rainy_days",
        "params": {"thresh_mm": 20.0},
        "group": "rain",
        "description": (
            "Number of days with precipitation ≥ 20mm. Climdex R20mm index."
        ),
    },
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
    
    # --- Percentile-based Precipitation Indices ---
    {
        "name": "Very Wet Day Precipitation (R95p)",
        "slug": "r95p_very_wet_precip",
        "var": "pr",
        "value_col": "r95p_mm",
        "units": "mm",
        "compute": "percentile_precipitation_total",
        "params": {
            "percentile": 95,
            "baseline_years": (1981, 2010),
            "quantile_method": "nearest",
            # exceed_ge=True means include ties (>= threshold) for wet-day exceedance
            "exceed_ge": True,
            # ETCCDI wet-day threshold convention (mm/day)
            "wet_day_mm": 1.0,
        },
        "group": "rain",
        "description": (
            "Total precipitation from very wet days, defined as days with precipitation "
            "exceeding the 95th percentile of wet-day precipitation in the baseline period "
            "(ETCCDI R95p)."
        ),
    },
    # {
    #     "name": "Extremely Wet Day Precipitation (R99p)",
    #     "slug": "r99p_extreme_wet_precip",
    #     "var": "pr",
    #     "value_col": "r99p_mm",
    #     "units": "mm",
    #     "compute": "percentile_precipitation_total",
    #     "params": {"percentile": 99, "baseline_years": (1981, 2010)},
    #     "group": "rain",
    #     "description": (
    #         "Total precipitation on days exceeding the 99th percentile of "
    #         "wet-day precipitation. Climdex R99p index."
    #     ),
    # },
    {
        "name": "Very Wet Day Contribution (R95pTOT)",
        "slug": "r95ptot_contribution_pct",
        "var": "pr",
        "value_col": "r95ptot_pct",
        "units": "%",
        "compute": "percentile_precipitation_contribution",
        "params": {
            "percentile": 95,
            "baseline_years": (1981, 2010),
            "quantile_method": "nearest",
            "exceed_ge": True,
            "wet_day_mm": 1.0,
        },
        "group": "rain",
        "description": (
            "Percentage of wet-day precipitation contributed by very wet days, where "
            "very wet days exceed the 95th percentile of baseline wet-day precipitation "
            "(ETCCDI R95pTOT)."
        ),
    },
    # {
    #     "name": "Extremely Wet Day Contribution (R99pTOT)",
    #     "slug": "r99ptot_contribution_pct",
    #     "var": "pr",
    #     "value_col": "r99ptot_pct",
    #     "units": "%",
    #     "compute": "percentile_precipitation_contribution",
    #     "params": {"percentile": 99, "baseline_years": (1981, 2010)},
    #     "group": "rain",
    #     "description": (
    #         "Percentage of total precipitation from extremely wet days (> 99th pctl). "
    #         "Climdex R99pTOT = 100 × R99p / PRCPTOT."
    #     ),
    # },
    
    # --- Precipitation Intensity & Totals ---
    {
        "name": "Simple Daily Intensity Index (SDII)",
        "slug": "pr_simple_daily_intensity",
        "var": "pr",
        "value_col": "simple_daily_intensity_mm_per_day",
        "units": "mm/day",
        "compute": "simple_daily_intensity_index",
        "params": {"wet_day_thresh_mm": 1.0},
        "group": "rain",
        "description": (
            "Mean precipitation on wet days (days with ≥ 1mm). Climdex SDII index."
        ),
    },
    {
        "name": "Total Wet-Day Precipitation (PRCPTOT)",
        "slug": "prcptot_annual_total",
        "rank_higher_is_worse": False,
        "var": "pr",
        "value_col": "prcptot_mm",
        "units": "mm",
        "compute": "total_wet_day_precipitation",
        "params": {"wet_thresh_mm": 1.0},
        "group": "rain",
        "description": (
            "Total precipitation from all wet days (≥ 1mm) in the year. "
            "Climdex PRCPTOT index."
        ),
    },
    
    # --- Wet/Dry Spell Indices ---
    {
        "name": "Consecutive Wet Days (CWD)",
        "slug": "cwd_consecutive_wet_days",
        "var": "pr",
        "value_col": "cwd_max_spell_len",
        "units": "days",
        "compute": "consecutive_wet_days",
        "params": {"wet_thresh_mm": 1.0},
        "group": "rain",
        "description": (
            "Maximum length of consecutive days with precipitation ≥ 1mm. "
            "Climdex CWD index."
        ),
    },
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
    
    # =========================================================================
    # 4. DROUGHT / DRYNESS INDICES
    # =========================================================================
    {
        "name": "Consecutive Dry Days (CDD)",
        "slug": "pr_consecutive_dry_days_lt1mm",
        "var": "pr",
        "value_col": "consecutive_dry_days",
        "units": "days",
        "compute": "consecutive_dry_days",
        "params": {"dry_thresh_mm": 1.0},
        "group": "rain",
        "description": (
            "Maximum consecutive days with precipitation < 1mm. Climdex CDD index."
        ),
    },
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
{
        "name": "Standardised Precipitation Index 3-month (SPI3)",
        "slug": "spi3_drought_index",
        "rank_higher_is_worse": False,
        "var": "pr",
        "value_col": "spi3_index",
        "units": "index",
        "compute": "standardised_precipitation_index",
        "params": {"scale_months": 3, "baseline_years": (1981, 2010)},
        "group": "rain",
        "description": (
            "3-month Standardised Precipitation Index. Measures short-term "
            "drought conditions. Climdex SPI."
        ),
    },
    {
        "name": "Standardised Precipitation Index 6-month (SPI6)",
        "slug": "spi6_drought_index",
        "rank_higher_is_worse": False,
        "var": "pr",
        "value_col": "spi6_index",
        "units": "index",
        "compute": "standardised_precipitation_index",
        "params": {"scale_months": 6, "baseline_years": (1981, 2010)},
        "group": "rain",
        "description": (
            "6-month Standardised Precipitation Index. Measures medium-term "
            "drought conditions. Climdex SPI."
        ),
    },
    {
        "name": "Standardised Precipitation Index 12-month (SPI12)",
        "slug": "spi12_drought_index",
        "rank_higher_is_worse": False,
        "var": "pr",
        "value_col": "spi12_index",
        "units": "index",
        "compute": "standardised_precipitation_index",
        "params": {"scale_months": 12, "baseline_years": (1981, 2010)},
        "group": "rain",
        "description": (
            "12-month Standardised Precipitation Index. Measures long-term "
            "drought conditions. Climdex SPI."
        ),
    },

    # ------------------------------------------------------------------
    # SPI persistence metrics: annual count of months crossing thresholds
    # (computed from monthly SPI using climate-indices adapter)
    # ------------------------------------------------------------------

    # SPI3 counts
    {
        "name": "SPI3: Count of months with SPI < -1 (moderate drought)",
        "slug": "spi3_count_months_lt_minus1",
        "var": "pr",
        "value_col": "spi3_months_lt_minus1",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 3,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_lt",
            "threshold": -1.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI3 below -1 (moderate meteorological drought persistence).",
    },
    {
        "name": "SPI3: Count of drought events with SPI < -1",
        "slug": "spi3_count_events_lt_minus1",
        "var": "pr",
        "value_col": "spi3_events_lt_minus1",
        "units": "events",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 3,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_events_lt",
            "threshold": -1.0,
        },
        "group": "rain",
        "description": "Annual count of contiguous SPI3 drought events below -1 (moderate seasonal drought episodes).",
    },
    {
        "name": "SPI3: Count of months with SPI < -2 (severe drought)",
        "slug": "spi3_count_months_lt_minus2",
        "var": "pr",
        "value_col": "spi3_months_lt_minus2",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 3,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_lt",
            "threshold": -2.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI3 below -2 (severe meteorological drought persistence).",
    },
    {
        "name": "SPI3: Count of months with SPI > +1 (moderately wet)",
        "slug": "spi3_count_months_gt_plus1",
        "var": "pr",
        "value_col": "spi3_months_gt_plus1",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 3,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_gt",
            "threshold": 1.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI3 above +1 (wet persistence).",
    },
    {
        "name": "SPI3: Count of months with SPI > +2 (severely wet)",
        "slug": "spi3_count_months_gt_plus2",
        "var": "pr",
        "value_col": "spi3_months_gt_plus2",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 3,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_gt",
            "threshold": 2.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI3 above +2 (extremely wet persistence).",
    },

    # SPI6 counts
    {
        "name": "SPI6: Count of months with SPI < -1 (moderate drought)",
        "slug": "spi6_count_months_lt_minus1",
        "var": "pr",
        "value_col": "spi6_months_lt_minus1",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 6,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_lt",
            "threshold": -1.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI6 below -1 (moderate meteorological drought persistence).",
    },
    {
        "name": "SPI6: Count of drought events with SPI < -1",
        "slug": "spi6_count_events_lt_minus1",
        "var": "pr",
        "value_col": "spi6_events_lt_minus1",
        "units": "events",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 6,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_events_lt",
            "threshold": -1.0,
        },
        "group": "rain",
        "description": "Annual count of contiguous SPI6 drought events below -1 (meteorological drought episodes).",
    },
    {
        "name": "SPI6: Count of months with SPI < -2 (severe drought)",
        "slug": "spi6_count_months_lt_minus2",
        "var": "pr",
        "value_col": "spi6_months_lt_minus2",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 6,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_lt",
            "threshold": -2.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI6 below -2 (severe meteorological drought persistence).",
    },
    {
        "name": "SPI6: Count of months with SPI > +1 (moderately wet)",
        "slug": "spi6_count_months_gt_plus1",
        "var": "pr",
        "value_col": "spi6_months_gt_plus1",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 6,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_gt",
            "threshold": 1.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI6 above +1 (wet persistence).",
    },
    {
        "name": "SPI6: Count of months with SPI > +2 (severely wet)",
        "slug": "spi6_count_months_gt_plus2",
        "var": "pr",
        "value_col": "spi6_months_gt_plus2",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 6,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_gt",
            "threshold": 2.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI6 above +2 (extremely wet persistence).",
    },

    # SPI12 counts
    {
        "name": "SPI12: Count of months with SPI < -1 (moderate drought)",
        "slug": "spi12_count_months_lt_minus1",
        "var": "pr",
        "value_col": "spi12_months_lt_minus1",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 12,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_lt",
            "threshold": -1.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI12 below -1 (moderate long-term drought persistence).",
    },
    {
        "name": "SPI12: Count of drought events with SPI < -1",
        "slug": "spi12_count_events_lt_minus1",
        "var": "pr",
        "value_col": "spi12_events_lt_minus1",
        "units": "events",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 12,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_events_lt",
            "threshold": -1.0,
        },
        "group": "rain",
        "description": "Annual count of contiguous SPI12 drought events below -1 (long-term drought episodes).",
    },
    {
        "name": "SPI12: Count of months with SPI < -2 (severe drought)",
        "slug": "spi12_count_months_lt_minus2",
        "var": "pr",
        "value_col": "spi12_months_lt_minus2",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 12,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_lt",
            "threshold": -2.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI12 below -2 (severe long-term drought persistence).",
    },
    {
        "name": "SPI12: Count of months with SPI > +1 (moderately wet)",
        "slug": "spi12_count_months_gt_plus1",
        "var": "pr",
        "value_col": "spi12_months_gt_plus1",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 12,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_gt",
            "threshold": 1.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI12 above +1 (wet persistence).",
    },
    {
        "name": "SPI12: Count of months with SPI > +2 (severely wet)",
        "slug": "spi12_count_months_gt_plus2",
        "var": "pr",
        "value_col": "spi12_months_gt_plus2",
        "units": "months",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 12,
            "baseline_years": (1981, 2010),
            "annual_aggregation": "count_months_gt",
            "threshold": 2.0,
        },
        "group": "rain",
        "description": "Annual count of months with SPI12 above +2 (extremely wet persistence).",
    },
    # {
    #     "name": "Standardised Precip-Evapotranspiration Index 3-month (SPEI3)",
    #     "slug": "spei3_drought_index",
    #     "var": "pr",  # Also requires tasmax/tasmin for ET calculation
    #     "value_col": "spei3_index",
    #     "units": "index",
    #     "compute": "standardised_precipitation_evapotranspiration_index",
    #     "params": {"scale_months": 3, "baseline_years": (1981, 2010)},
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
    #     "params": {"scale_months": 6, "baseline_years": (1981, 2010)},
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
    #     "params": {"scale_months": 12, "baseline_years": (1981, 2010)},
    #     "group": "rain",
    #     "description": (
    #         "12-month SPEI incorporating evapotranspiration. Climdex SPEI."
    #     ),
    # },
]

# Dashboard-only source-backed metrics that do not participate in the climate
# compute pipeline. These still use the same wide-master contract so the app can
# treat them like first-class metrics.
DASHBOARD_ONLY_METRICS_RAW: list[dict[str, Any]] = [
    {
        "name": "Aqueduct Water Stress",
        "slug": "aq_water_stress",
        "label": "Aqueduct Water Stress",
        "group": "water",
        "value_col": "aq_water_stress",
        "periods_metric_col": "aq_water_stress",
        "units": "index",
        "description": (
            "Aqueduct 4.0 annual water stress transferred from HydroSHEDS Level 6 "
            "onto Survey of India basin and sub-basin units using area-weighted overlap."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "scenario_period",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": True,
        "supports_scenario_comparison": True,
        "admin_rebuild_command": "python -m tools.geodata.build_aqueduct_admin_masters --overwrite",
        "hydro_rebuild_command": "python -m tools.geodata.build_aqueduct_hydro_masters --overwrite",
        "supported_scenarios": ("historical", "bau", "opt", "pes"),
        "preferred_period_order": ("1979-2019", "2030", "2050", "2080"),
        "supported_spatial_families": ("admin", "hydro"),
        "supported_levels": ("district", "block", "basin", "sub_basin"),
        "rank_higher_is_worse": True,
    },
    {
        "name": "Aqueduct Interannual Variability",
        "slug": "aq_interannual_variability",
        "label": "Aqueduct Interannual Variability",
        "group": "water",
        "value_col": "aq_interannual_variability",
        "periods_metric_col": "aq_interannual_variability",
        "units": "index",
        "description": (
            "Aqueduct 4.0 interannual variability transferred from HydroSHEDS Level 6 "
            "onto Survey of India basin and sub-basin units using area-weighted overlap."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "scenario_period",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": True,
        "supports_scenario_comparison": True,
        "admin_rebuild_command": "python -m tools.geodata.build_aqueduct_admin_masters --overwrite",
        "hydro_rebuild_command": "python -m tools.geodata.build_aqueduct_hydro_masters --overwrite",
        "supported_scenarios": ("historical", "bau", "opt", "pes"),
        "preferred_period_order": ("1979-2019", "2030", "2050", "2080"),
        "supported_spatial_families": ("admin", "hydro"),
        "supported_levels": ("district", "block", "basin", "sub_basin"),
        "rank_higher_is_worse": True,
    },
    {
        "name": "Aqueduct Seasonal Variability",
        "slug": "aq_seasonal_variability",
        "label": "Aqueduct Seasonal Variability",
        "group": "water",
        "value_col": "aq_seasonal_variability",
        "periods_metric_col": "aq_seasonal_variability",
        "units": "index",
        "description": (
            "Aqueduct 4.0 seasonal variability transferred from HydroSHEDS Level 6 "
            "onto Survey of India basin and sub-basin units using area-weighted overlap."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "scenario_period",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": True,
        "supports_scenario_comparison": True,
        "admin_rebuild_command": "python -m tools.geodata.build_aqueduct_admin_masters --overwrite",
        "hydro_rebuild_command": "python -m tools.geodata.build_aqueduct_hydro_masters --overwrite",
        "supported_scenarios": ("historical", "bau", "opt", "pes"),
        "preferred_period_order": ("1979-2019", "2030", "2050", "2080"),
        "supported_spatial_families": ("admin", "hydro"),
        "supported_levels": ("district", "block", "basin", "sub_basin"),
        "rank_higher_is_worse": True,
    },
    {
        "name": "Aqueduct Water Depletion",
        "slug": "aq_water_depletion",
        "label": "Aqueduct Water Depletion",
        "group": "water",
        "value_col": "aq_water_depletion",
        "periods_metric_col": "aq_water_depletion",
        "units": "index",
        "description": (
            "Aqueduct 4.0 water depletion transferred from HydroSHEDS Level 6 "
            "onto Survey of India basin and sub-basin units using area-weighted overlap."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "scenario_period",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": True,
        "supports_scenario_comparison": True,
        "admin_rebuild_command": "python -m tools.geodata.build_aqueduct_admin_masters --overwrite",
        "hydro_rebuild_command": "python -m tools.geodata.build_aqueduct_hydro_masters --overwrite",
        "supported_scenarios": ("historical", "bau", "opt", "pes"),
        "preferred_period_order": ("1979-2019", "2030", "2050", "2080"),
        "supported_spatial_families": ("admin", "hydro"),
        "supported_levels": ("district", "block", "basin", "sub_basin"),
        "rank_higher_is_worse": True,
    },
    {
        "name": "Total Population",
        "slug": "population_total",
        "label": "Total Population",
        "group": "other",
        "value_col": "population_total",
        "periods_metric_col": "population_total",
        "units": "people",
        "description": (
            "2025 population totals aggregated from the 1 km population raster onto "
            "canonical district and block units."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "2025",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": "python -m tools.geodata.build_population_admin_masters --overwrite",
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("2025",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district", "block"),
        "rank_higher_is_worse": True,
    },
    {
        "name": "Population Density",
        "slug": "population_density",
        "label": "Population Density",
        "group": "other",
        "value_col": "population_density",
        "periods_metric_col": "population_density",
        "units": "people/km2",
        "description": (
            "2025 population density derived from raster-aggregated population totals "
            "and canonical district/block polygon area."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "2025",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": "python -m tools.geodata.build_population_admin_masters --overwrite",
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("2025",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district", "block"),
        "rank_higher_is_worse": True,
    },
    {
        "name": "Stage of Ground Water Extraction",
        "slug": "gw_stage_extraction_pct",
        "label": "Stage of Ground Water Extraction",
        "group": "water",
        "value_col": "gw_stage_extraction_pct",
        "periods_metric_col": "gw_stage_extraction_pct",
        "units": "%",
        "description": (
            "2024-2025 GEC district snapshot of total stage of groundwater extraction."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "2024-2025",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": "python -m tools.geodata.build_groundwater_district_masters --overwrite",
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("2024-2025",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district",),
        "rank_higher_is_worse": True,
    },
    {
        "name": "Net Annual Ground Water Availability for Future Use",
        "slug": "gw_future_availability_ham",
        "label": "Net Annual Ground Water Availability for Future Use",
        "group": "water",
        "value_col": "gw_future_availability_ham",
        "periods_metric_col": "gw_future_availability_ham",
        "units": "ham",
        "description": (
            "2024-2025 GEC district snapshot of net annual groundwater availability for future use."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "2024-2025",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": "python -m tools.geodata.build_groundwater_district_masters --overwrite",
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("2024-2025",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district",),
        "rank_higher_is_worse": False,
    },
    {
        "name": "Annual Extractable Ground Water Resource",
        "slug": "gw_extractable_resource_ham",
        "label": "Annual Extractable Ground Water Resource",
        "group": "water",
        "value_col": "gw_extractable_resource_ham",
        "periods_metric_col": "gw_extractable_resource_ham",
        "units": "ham",
        "description": (
            "2024-2025 GEC district snapshot of annual extractable groundwater resource."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "2024-2025",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": "python -m tools.geodata.build_groundwater_district_masters --overwrite",
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("2024-2025",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district",),
        "rank_higher_is_worse": False,
    },
    {
        "name": "Ground Water Extraction for All Uses",
        "slug": "gw_total_extraction_ham",
        "label": "Ground Water Extraction for All Uses",
        "group": "water",
        "value_col": "gw_total_extraction_ham",
        "periods_metric_col": "gw_total_extraction_ham",
        "units": "ha.m",
        "description": (
            "2024-2025 GEC district snapshot of total groundwater extraction for all uses."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "2024-2025",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": "python -m tools.geodata.build_groundwater_district_masters --overwrite",
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("2024-2025",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district",),
        "rank_higher_is_worse": True,
    },
    {
        "name": "Flood Severity Index (RP-100)",
        "slug": "jrc_flood_depth_index_rp100",
        "label": "Flood Severity Index (RP-100)",
        "group": "water",
        "value_col": "jrc_flood_depth_index_rp100",
        "periods_metric_col": "jrc_flood_depth_index_rp100",
        "units": "severity class (1-5)",
        "display_units": "",
        "class_labels": SEVERITY_CLASS_LABELS,
        "class_display_mode": "label_with_score",
        "description": (
            "Telangana-only ordinal severity class derived from RP-100 JRC flood depth "
            "and RP-100 flood extent using a fixed 5x5 depth-by-extent scoring matrix. "
            "Depth uses flooded-cell p95 block depth and flooded-area-weighted district "
            "depth; extent uses the share of total polygon area covered by positive "
            "modeled depth. Class 1 is VeryLow and class 5 is Extreme. This is an "
            "externally sourced snapshot severity index, not a climate scenario projection."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "Current",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": (
            "python -m tools.runs.prepare_dashboard jrc-flood-depth "
            "--source-dir <JRC_DIR> --assume-units m --overwrite"
        ),
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("Current",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district", "block"),
        "supported_admin_states": ("Telangana",),
        "rank_higher_is_worse": True,
    },
    {
        "name": "RP-100 Flood Extent",
        "slug": "jrc_flood_extent_rp100",
        "label": "RP-100 Flood Extent",
        "group": "water",
        "value_col": "jrc_flood_extent_rp100",
        "periods_metric_col": "jrc_flood_extent_rp100",
        "units": "fraction",
        "display_units": "%",
        "display_scale": 100.0,
        "description": (
            "Telangana-only JRC RP-100 flood extent, defined as the share of total polygon "
            "area covered by positive modeled flood depth. Block and district values are "
            "both based on total polygon area, while raster-supported area is retained as QA."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "Current",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": (
            "python -m tools.runs.prepare_dashboard jrc-flood-depth "
            "--source-dir <JRC_DIR> --assume-units m --overwrite"
        ),
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("Current",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district", "block"),
        "supported_admin_states": ("Telangana",),
        "rank_higher_is_worse": True,
    },
    {
        "name": "RP-10 Flood Depth",
        "slug": "jrc_flood_depth_rp10",
        "label": "RP-10 Flood Depth",
        "group": "water",
        "value_col": "jrc_flood_depth_rp10",
        "periods_metric_col": "jrc_flood_depth_rp10",
        "units": "m",
        "description": (
            "Telangana-only JRC flood-depth snapshot for the 10-year return period. "
            "Block values use flooded-cell p95 depth and district values use flooded-area-"
            "weighted means of child block flooded-cell p95 depth. This is an externally "
            "sourced inundation layer, not a climate scenario projection."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "Current",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": (
            "python -m tools.runs.prepare_dashboard jrc-flood-depth "
            "--source-dir <JRC_DIR> --assume-units m --overwrite"
        ),
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("Current",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district", "block"),
        "supported_admin_states": ("Telangana",),
        "rank_higher_is_worse": True,
    },
    {
        "name": "RP-50 Flood Depth",
        "slug": "jrc_flood_depth_rp50",
        "label": "RP-50 Flood Depth",
        "group": "water",
        "value_col": "jrc_flood_depth_rp50",
        "periods_metric_col": "jrc_flood_depth_rp50",
        "units": "m",
        "description": (
            "Telangana-only JRC flood-depth snapshot for the 50-year return period. "
            "Block values use flooded-cell p95 depth and district values use flooded-area-"
            "weighted means of child block flooded-cell p95 depth. This is an externally "
            "sourced inundation layer, not a climate scenario projection."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "Current",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": (
            "python -m tools.runs.prepare_dashboard jrc-flood-depth "
            "--source-dir <JRC_DIR> --assume-units m --overwrite"
        ),
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("Current",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district", "block"),
        "supported_admin_states": ("Telangana",),
        "rank_higher_is_worse": True,
    },
    {
        "name": "RP-100 Flood Depth",
        "slug": "jrc_flood_depth_rp100",
        "label": "RP-100 Flood Depth",
        "group": "water",
        "value_col": "jrc_flood_depth_rp100",
        "periods_metric_col": "jrc_flood_depth_rp100",
        "units": "m",
        "description": (
            "Telangana-only JRC flood-depth snapshot for the 100-year return period. "
            "Block values use flooded-cell p95 depth and district values use flooded-area-"
            "weighted means of child block flooded-cell p95 depth. This is an externally "
            "sourced inundation layer, not a climate scenario projection."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "Current",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": (
            "python -m tools.runs.prepare_dashboard jrc-flood-depth "
            "--source-dir <JRC_DIR> --assume-units m --overwrite"
        ),
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("Current",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district", "block"),
        "supported_admin_states": ("Telangana",),
        "rank_higher_is_worse": True,
    },
    {
        "name": "RP-500 Flood Depth",
        "slug": "jrc_flood_depth_rp500",
        "label": "RP-500 Flood Depth",
        "group": "water",
        "value_col": "jrc_flood_depth_rp500",
        "periods_metric_col": "jrc_flood_depth_rp500",
        "units": "m",
        "description": (
            "Telangana-only JRC flood-depth snapshot for the 500-year return period. "
            "Block values use flooded-cell p95 depth and district values use flooded-area-"
            "weighted means of child block flooded-cell p95 depth. This is an externally "
            "sourced inundation layer, not a climate scenario projection."
        ),
        "source_type": "external",
        "supports_yearly_trend": False,
        "selection_mode": "static_snapshot",
        "fixed_scenario": "snapshot",
        "fixed_period": "Current",
        "supported_statistics": ("mean",),
        "supports_baseline_comparison": False,
        "supports_scenario_comparison": False,
        "admin_rebuild_command": (
            "python -m tools.runs.prepare_dashboard jrc-flood-depth "
            "--source-dir <JRC_DIR> --assume-units m --overwrite"
        ),
        "supported_scenarios": ("snapshot",),
        "preferred_period_order": ("Current",),
        "supported_spatial_families": ("admin",),
        "supported_levels": ("district", "block"),
        "supported_admin_states": ("Telangana",),
        "rank_higher_is_worse": True,
    },
    *[
        {
            "name": spec.composite_label,
            "slug": spec.composite_slug,
            "label": spec.composite_label,
            "group": "other",
            "value_col": spec.composite_slug,
            "periods_metric_col": spec.composite_slug,
            "units": "score",
            "display_units": "score",
            "display_scale": 1.0,
            "description": (
                f"Persisted weighted composite hazard score for the {spec.bundle_domain} bundle. "
                "Computed offline from approved bundle weights and per-scenario-period normalization."
            ),
            "source_type": "derived",
            "supports_yearly_trend": False,
            "selection_mode": "scenario_period",
            "supported_statistics": ("mean",),
            "supports_baseline_comparison": False,
            "supports_scenario_comparison": False,
            "admin_rebuild_command": "python -m tools.pipeline.build_composite_metrics",
            "supported_scenarios": ("ssp245", "ssp585"),
            "preferred_period_order": ("2020-2040", "2040-2060", "2060-2080"),
            "supported_spatial_families": spec.supported_spatial_families,
            "supported_levels": spec.supported_levels,
            "rank_higher_is_worse": True,
        }
        for spec in VISIBLE_GLANCE_COMPOSITES
    ],
]

ALL_METRICS_RAW: list[dict[str, Any]] = PIPELINE_METRICS_RAW + DASHBOARD_ONLY_METRICS_RAW
PIPELINE_SLUGS: set[str] = {str(m.get("slug", "")).strip() for m in PIPELINE_METRICS_RAW if str(m.get("slug", "")).strip()}

# Typed views derived from metric registries
PIPELINE_METRICS: list[MetricSpec] = [MetricSpec.from_pipeline_dict(m) for m in PIPELINE_METRICS_RAW]
METRICS_BY_SLUG: dict[str, MetricSpec] = build_registry_from_pipeline(ALL_METRICS_RAW)


# -----------------------------------------------------------------------------
# DASHBOARD TAXONOMY
# -----------------------------------------------------------------------------
# The dashboard now uses:
#   Assessment pillar -> Domain -> Metric
#
# The older bundle terminology is kept as a compatibility alias because portfolio,
# exports, and some helper functions still reference "bundles".

DOMAINS: dict[str, list[str]] = {
    "Heat Risk": [
        "composite_heat_risk",
        "tas_annual_mean",
        "txx_annual_max",        
        # Heat thresholds (absolute temperature thresholds)
        # "tas_gt32",
        "txge30_hot_days",
        
        "txge35_extreme_heat_days",
        # "su_summer_days_gt25",
        "tasmin_tropical_nights_gt25",
        # Wet-bulb thermal stress
        
        
        
        # Heat percentiles (relative to baseline)
        "tx90p_hot_days_pct",
        "tn90p_warm_nights_pct",
        # Heatwaves & persistence
        "wsdi_warm_spell_days",
        # "tasmax_csd_gt30",
        # "tasmax_csd_events_gt30",
        # "hwdi_tasmax_plus5C",
        "hwfi_tmean_90p",
        # "hwdi_events_tasmax_plus5C",
        "hwfi_events_tmean_90p",
        # "hwm_heatwave_magnitude",
        "hwa_heatwave_amplitude",
        # Heat baseline context (annual/seasonal means and extremes)
        
        "tnx_annual_max",
        
        "tasmax_summer_mean",
        
        "tas_summer_mean",
        # "tasmax_annual_mean",
        # "tasmin_annual_mean",
    ],
    "Heat Stress": [
        "composite_heat_stress",
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
    ],
    "Cold Risk": [
        "composite_cold_risk",
        "tas_winter_mean",
        "tasmin_winter_mean",
        "tnn_annual_min",
        "tasmin_winter_min",
        # Threshold-based cold days
        "tnle10_cold_nights",
        "tnle5_severe_cold_nights",
        "txle15_cold_days",
        # Relative cold
        "tx10p_cool_days_pct",
        "tn10p_cool_nights_pct",
        # Cold spell characteristics
        "csdi_cold_spell_days",
        "tnle10_consecutive_cold_nights",
    ],
    "Agriculture & Growing Conditions": [
        "composite_agriculture_growing_conditions",
        "gsl_growing_season",
        "tasmax_summer_mean",
        "tasmin_winter_mean",
        "dtr_daily_temp_range",
        "txge35_extreme_heat_days",
        "tnle10_cold_nights",
        "wsdi_warm_spell_days",
        "spi3_drought_index",
        "prcptot_annual_total",
    ],
    "Flood & Extreme Rainfall Risk": [
        "composite_flood_extreme_rainfall_risk",
        # Peak intensity
        "pr_max_1day_precip",
        "pr_max_5day_precip",
        # Heavy rain day frequency
        # "pr_heavy_precip_days_gt10mm",
        "r20mm_very_heavy_precip_days",
        # "pr_very_heavy_precip_days_gt25mm",
        # "rain_gt_2p5mm",
        # Very wet / extremely wet contribution
        "r95p_very_wet_precip",
        # "r99p_extreme_wet_precip",
        "r95ptot_contribution_pct",
        # "r99ptot_contribution_pct",
        # Wet-spell persistence
        "cwd_consecutive_wet_days",
        # "pr_5day_precip_events_gt50mm",
    ],
    "Rainfall Totals & Typical Wetness": [
        # Annual totals and intensity
        "prcptot_annual_total",
        "pr_simple_daily_intensity",
        "rain_gt_2p5mm",
    ],
    "Drought Risk": [
        "composite_drought_risk",
        "spi3_count_events_lt_minus1",
        "spi6_count_events_lt_minus1",
        "spi12_count_events_lt_minus1",
    ],
    "Drought Risk (Advanced)": [
        # Short-term vs long-term SPI + severity splits (keep available but not default)
        "spi3_drought_index",
        "spi3_count_months_lt_minus1",
        "spi3_count_months_lt_minus2",

        "spi6_drought_index",
        "spi6_count_months_lt_minus1",
        "spi6_count_months_lt_minus2",

        "spi12_drought_index",
        "spi12_count_months_lt_minus1",
        "spi12_count_months_lt_minus2",

        # Climatic water-balance drought (SPEI) – optional (currently disabled)
        # "spei3_drought_index",
        # "spei6_drought_index",
        # "spei12_drought_index",
    ],
    "Temperature Variability": [
        # Daily and annual variability
        "dtr_daily_temp_range",
        "etr_extreme_temp_range",
    ],
    "Population Exposure": [
        "population_total",
        "population_density",
    ],
    "Aqueduct Water Risk": [
        "aq_water_stress",
        "aq_interannual_variability",
        "aq_seasonal_variability",
        "aq_water_depletion",
    ],
    "Groundwater Status & Availability": [
        "gw_stage_extraction_pct",
        "gw_future_availability_ham",
        "gw_extractable_resource_ham",
        "gw_total_extraction_ham",
    ],
    "Flood Inundation Depth (JRC)": [
        "jrc_flood_depth_index_rp100",
        "jrc_flood_extent_rp100",
        "jrc_flood_depth_rp10",
        "jrc_flood_depth_rp50",
        "jrc_flood_depth_rp100",
        "jrc_flood_depth_rp500",
    ],
}

# Domain display order for UI
DOMAIN_ORDER: list[str] = [
    "Heat Risk",
    "Heat Stress",
    "Cold Risk",
    "Agriculture & Growing Conditions",
    "Flood & Extreme Rainfall Risk",
    "Rainfall Totals & Typical Wetness",
    "Drought Risk",
    "Drought Risk (Advanced)",
    "Temperature Variability",
    "Population Exposure",
    "Aqueduct Water Risk",
    "Groundwater Status & Availability",
    "Flood Inundation Depth (JRC)",
]

PILLAR_DOMAINS: dict[str, list[str]] = {
    "Climate Hazards": [
        "Heat Risk",
        "Heat Stress",
        "Cold Risk",
        "Agriculture & Growing Conditions",
        "Flood & Extreme Rainfall Risk",
        "Rainfall Totals & Typical Wetness",
        "Drought Risk",
        "Drought Risk (Advanced)",
        "Temperature Variability",
    ],
    "Bio-physical Hazards": [
        "Aqueduct Water Risk",
        "Groundwater Status & Availability",
        "Flood Inundation Depth (JRC)",
    ],
    "Exposure": [
        "Population Exposure",
    ],
    "Vulnerability": [],
    "Adaptive Capacity": [],
}

PILLAR_ORDER: list[str] = [
    "Climate Hazards",
    "Bio-physical Hazards",
    "Exposure",
    "Vulnerability",
    "Adaptive Capacity",
]

DOMAIN_TO_PILLAR: dict[str, str] = {
    domain: pillar
    for pillar, domains in PILLAR_DOMAINS.items()
    for domain in domains
}

LEGACY_DOMAIN_ALIASES: dict[str, str] = {
    "Water Risk": "Aqueduct Water Risk",
}

# Defaults for single-focus mode
DEFAULT_PILLAR: str = "Climate Hazards"
DEFAULT_DOMAIN: str = "Heat Risk"

# Domain descriptions for UI tooltips/help text
DOMAIN_DESCRIPTIONS: dict[str, str] = {
    "Aqueduct Water Risk": (
        "Hydrologic water-risk metrics derived from Aqueduct and displayed on "
        "admin and hydro units through audited overlap transfer workflows."
    ),
    "Population Exposure": (
        "Static population exposure layers derived from the 2025 population raster "
        "and aggregated onto canonical district and block units."
    ),
    "Groundwater Status & Availability": (
        "District groundwater assessment layers from the 2024-2025 GEC workbook, "
        "covering extraction stage, extractable resource, total extraction, and "
        "future groundwater availability."
    ),
    "Flood Inundation Depth (JRC)": (
        "Telangana-only JRC flood snapshot domain covering the derived RP-100 "
        "Flood Severity Index, RP-100 Flood Extent, plus RP-10, RP-50, RP-100, "
        "and RP-500 depth layers. Flood extent uses total polygon area, while "
        "depth layers use flooded-cell p95 block depth and flooded-area-weighted "
        "district rollups."
    ),
    "Heat Risk": (
        "Metrics related to extreme heat, heatwaves, and thermal stress. "
        "Includes threshold-based indices, percentile extremes, and heatwave persistence."
    ),
    "Cold Risk": (
        "Metrics related to cold extremes, frost, and cold spells. "
        "Includes frost days, icing days, and cold spell duration."
    ),
    "Agriculture & Growing Conditions": (
        "Metrics relevant to crop suitability and growing season length. "
        "Useful for non-disaster framing of climate impacts on agriculture."
    ),
    "Flood & Extreme Rainfall Risk": (
        "Metrics related to extreme precipitation events and flood risk. "
        "Includes peak intensity, heavy rain frequency, and wet spell persistence."
    ),
    "Rainfall Totals & Typical Wetness": (
        "Metrics for overall water availability and typical rainfall patterns. "
        "Distinct from flood extremes; useful for water resource planning."
    ),
    "Drought Risk": (
        "Metrics related to dry spells and drought conditions. "
        "Includes SPI and SPEI indices at multiple timescales."
    ),
    "Temperature Variability": (
        "Metrics for daily and annual temperature variability. "
        "Useful for understanding climate stability and interpreting heat stress."
    ),
}

PILLAR_DESCRIPTIONS: dict[str, str] = {
    "Climate Hazards": (
        "Climate-model-derived hazard and climate-condition layers, including heat, "
        "cold, rainfall, flood, drought, and variability metrics."
    ),
    "Bio-physical Hazards": (
        "Physical hazard layers from externally sourced geospatial products, kept "
        "separate from climate hazard indices to make provenance clear."
    ),
    "Exposure": (
        "People, assets, land use, and other layers that describe what is present "
        "in places potentially affected by hazards."
    ),
    "Vulnerability": (
        "Future placeholder for sensitivity and vulnerability layers."
    ),
    "Adaptive Capacity": (
        "Future placeholder for coping, readiness, and adaptive capacity layers."
    ),
}

# Compatibility aliases (older code still imports bundle names/constants)
BUNDLES: dict[str, list[str]] = DOMAINS
BUNDLE_ORDER: list[str] = DOMAIN_ORDER
DEFAULT_BUNDLE: str = DEFAULT_DOMAIN
BUNDLE_DESCRIPTIONS: dict[str, str] = DOMAIN_DESCRIPTIONS


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
    "{root}/{state}/state_yearly_ensemble_stats_{level}.csv",
]


def get_dashboard_variables() -> dict[str, dict[str, Any]]:
    """
    Generate the VARIABLES dict for the dashboard from the unified registry.

    Notes:
        The dashboard primarily needs:
          - label/group (UI)
          - periods_metric_col (master CSV schema)
          - units (axis/legend/tooltip formatting)
          - discovery templates (yearly files)

        Adding keys here is backwards compatible for callers that only read a subset.
    """
    variables: dict[str, dict[str, Any]] = {}
    for slug, spec in METRICS_BY_SLUG.items():
        units = str(spec.units or "").strip()
        domains_for_slug = get_domains_for_metric(slug)
        pillars_for_slug = list(
            dict.fromkeys(
                get_pillar_for_domain(domain)
                for domain in domains_for_slug
                if get_pillar_for_domain(domain)
            )
        )
        district_yearly_candidates = (
            list(spec.district_yearly_candidates)
            if spec.district_yearly_candidates is not None
            else ([] if not spec.supports_yearly_trend else list(DEFAULT_DISTRICT_YEARLY_CANDIDATES))
        )
        state_yearly_candidates = (
            list(spec.state_yearly_candidates)
            if spec.state_yearly_candidates is not None
            else ([] if not spec.supports_yearly_trend else list(DEFAULT_STATE_YEARLY_CANDIDATES))
        )
        variables[slug] = {
            "label": spec.label,
            "group": spec.group,
            "periods_metric_col": spec.periods_metric_col,
            "rank_higher_is_worse": bool(spec.rank_higher_is_worse),
            # Backwards compatible: some codepaths look for "unit"
            "units": units,
            "unit": units,
            "display_units": str(spec.display_units or "").strip(),
            "display_scale": float(spec.display_scale),
            "class_labels": dict(spec.class_labels or {}),
            "class_display_mode": spec.class_display_mode,
            "description": spec.description or "",
            "source_type": spec.source_type,
            "supports_yearly_trend": bool(spec.supports_yearly_trend),
            "selection_mode": spec.selection_mode,
            "fixed_scenario": spec.fixed_scenario,
            "fixed_period": spec.fixed_period,
            "supported_statistics": list(spec.supported_statistics),
            "supports_baseline_comparison": bool(spec.supports_baseline_comparison),
            "supports_scenario_comparison": bool(spec.supports_scenario_comparison),
            "admin_rebuild_command": spec.admin_rebuild_command,
            "hydro_rebuild_command": spec.hydro_rebuild_command,
            "supported_scenarios": list(spec.supported_scenarios),
            "preferred_period_order": list(spec.preferred_period_order),
            "supported_spatial_families": list(spec.supported_spatial_families),
            "supported_levels": list(spec.supported_levels),
            "supported_admin_states": list(spec.supported_admin_states),
            "domains": domains_for_slug,
            "pillars": pillars_for_slug,
            "district_yearly_candidates": district_yearly_candidates,
            "state_yearly_candidates": state_yearly_candidates,
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


def _metric_supported_in_context(
    spec: MetricSpec,
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> bool:
    """Return True when a metric is valid for the requested app context."""
    family = str(spatial_family or "").strip().lower()
    level_norm = str(level or "").strip().lower()

    if spec.supported_spatial_families:
        allowed_families = {str(v).strip().lower() for v in spec.supported_spatial_families if str(v).strip()}
        if family and family not in allowed_families:
            return False

    if spec.supported_levels:
        allowed_levels = {str(v).strip().lower() for v in spec.supported_levels if str(v).strip()}
        if level_norm and level_norm not in allowed_levels:
            return False

    return True


def get_pipeline_bundles() -> dict[str, list[str]]:
    """Return bundle contents restricted to pipeline-backed metrics."""
    out: dict[str, list[str]] = {}
    for domain, slugs in DOMAINS.items():
        filtered = [slug for slug in slugs if slug in PIPELINE_SLUGS]
        if filtered:
            out[domain] = filtered
    return out


def get_metric_count() -> dict[str, int]:
    """Return count of metrics per group."""
    groups = get_metrics_by_group()
    return {g: len(slugs) for g, slugs in groups.items()}


# -----------------------------------------------------------------------------
# TAXONOMY HELPER FUNCTIONS
# -----------------------------------------------------------------------------

def normalize_domain_name(domain: str) -> str:
    """Return the canonical domain name for legacy/alias input."""
    raw = str(domain or "").strip()
    return LEGACY_DOMAIN_ALIASES.get(raw, raw)


def get_pillars(
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> list[str]:
    """Return populated assessment pillars for the current context."""
    pillars: list[str] = []
    for pillar in PILLAR_ORDER:
        if get_domains_for_pillar(pillar, spatial_family=spatial_family, level=level):
            pillars.append(pillar)
    return pillars


def get_domains_for_pillar(
    pillar: str,
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> list[str]:
    """Return populated domains for a pillar in display order."""
    canonical_pillar = str(pillar or "").strip()
    domains: list[str] = []
    for domain in PILLAR_DOMAINS.get(canonical_pillar, []):
        if get_metrics_for_domain(domain, spatial_family=spatial_family, level=level):
            domains.append(domain)
    return domains


def get_domains(
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> list[str]:
    """Return populated domain names in display order."""
    domains: list[str] = []
    for domain in DOMAIN_ORDER:
        if get_metrics_for_domain(domain, spatial_family=spatial_family, level=level):
            domains.append(domain)
    return domains


def get_metrics_for_domain(
    domain: str,
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> list[str]:
    """Return metric slugs for a domain in the current context."""
    canonical_domain = normalize_domain_name(domain)
    slugs = list(DOMAINS.get(canonical_domain, []))
    filtered: list[str] = []
    for slug in slugs:
        spec = METRICS_BY_SLUG.get(slug)
        if spec is None:
            continue
        if _metric_supported_in_context(spec, spatial_family=spatial_family, level=level):
            filtered.append(slug)
    return filtered


def get_domains_for_metric(
    slug: str,
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> list[str]:
    """Return populated domains containing a given metric slug."""
    spec = METRICS_BY_SLUG.get(slug)
    if spec is None or not _metric_supported_in_context(spec, spatial_family=spatial_family, level=level):
        return []
    return [
        domain
        for domain, slugs in DOMAINS.items()
        if slug in slugs and get_metrics_for_domain(domain, spatial_family=spatial_family, level=level)
    ]


def get_domain_description(domain: str) -> str:
    """Return description for a domain."""
    return DOMAIN_DESCRIPTIONS.get(normalize_domain_name(domain), "")


def get_pillar_for_domain(domain: str) -> str:
    """Return the assessment pillar containing a domain."""
    return DOMAIN_TO_PILLAR.get(normalize_domain_name(domain), "")


def get_pillar_description(pillar: str) -> str:
    """Return description for an assessment pillar."""
    return PILLAR_DESCRIPTIONS.get(str(pillar or "").strip(), "")


def get_default_pillar(
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> str:
    """Return the default populated pillar for the current context."""
    pillars = get_pillars(spatial_family=spatial_family, level=level)
    if DEFAULT_PILLAR in pillars:
        return DEFAULT_PILLAR
    return pillars[0] if pillars else DEFAULT_PILLAR


def get_default_domain(
    *,
    pillar: Optional[str] = None,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> str:
    """Return the default populated domain for the current context."""
    if pillar:
        domains = get_domains_for_pillar(pillar, spatial_family=spatial_family, level=level)
        if DEFAULT_DOMAIN in domains:
            return DEFAULT_DOMAIN
        return domains[0] if domains else DEFAULT_DOMAIN
    domains = get_domains(spatial_family=spatial_family, level=level)
    if DEFAULT_DOMAIN in domains:
        return DEFAULT_DOMAIN
    return domains[0] if domains else DEFAULT_DOMAIN


def get_metric_options_for_domain(
    domain: str,
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> list[tuple[str, str]]:
    """Return (slug, label) tuples for metrics in a domain."""
    slugs = get_metrics_for_domain(domain, spatial_family=spatial_family, level=level)
    options = []
    for slug in slugs:
        spec = METRICS_BY_SLUG.get(slug)
        if spec:
            options.append((slug, spec.label))
    return options


def get_bundles(
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> list[str]:
    """Compatibility alias: return populated domains in display order."""
    return get_domains(spatial_family=spatial_family, level=level)


def get_metrics_for_bundle(
    bundle: str,
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> list[str]:
    """Compatibility alias for get_metrics_for_domain."""
    return get_metrics_for_domain(bundle, spatial_family=spatial_family, level=level)


def get_bundle_for_metric(
    slug: str,
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> list[str]:
    """Compatibility alias for get_domains_for_metric."""
    return get_domains_for_metric(slug, spatial_family=spatial_family, level=level)


def get_bundle_description(bundle: str) -> str:
    """Compatibility alias for get_domain_description."""
    return get_domain_description(bundle)


def get_default_bundle(
    *,
    pillar: Optional[str] = None,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> str:
    """Compatibility alias for get_default_domain."""
    return get_default_domain(pillar=pillar, spatial_family=spatial_family, level=level)


def get_metric_options_for_bundle(
    bundle: str,
    *,
    spatial_family: Optional[str] = None,
    level: Optional[str] = None,
) -> list[tuple[str, str]]:
    """Compatibility alias for get_metric_options_for_domain."""
    return get_metric_options_for_domain(bundle, spatial_family=spatial_family, level=level)


def validate_domains() -> list[str]:
    """Validate that domain and pillar definitions are consistent with the registry."""
    issues: list[str] = []

    for domain, slugs in DOMAINS.items():
        for slug in slugs:
            if slug not in METRICS_BY_SLUG:
                issues.append(f"Domain '{domain}' references unknown slug: '{slug}'")

    for domain in DOMAIN_ORDER:
        if domain not in DOMAINS:
            issues.append(f"DOMAIN_ORDER contains unknown domain: '{domain}'")

    for domain in DOMAINS:
        if domain not in DOMAIN_ORDER:
            issues.append(f"Domain '{domain}' missing from DOMAIN_ORDER")

    for pillar in PILLAR_ORDER:
        if pillar not in PILLAR_DOMAINS:
            issues.append(f"PILLAR_ORDER contains unknown pillar: '{pillar}'")

    for pillar, domains in PILLAR_DOMAINS.items():
        if pillar not in PILLAR_ORDER:
            issues.append(f"Pillar '{pillar}' missing from PILLAR_ORDER")
        for domain in domains:
            if domain not in DOMAINS:
                issues.append(f"Pillar '{pillar}' references unknown domain: '{domain}'")
            elif DOMAIN_TO_PILLAR.get(domain) != pillar:
                issues.append(f"Domain '{domain}' is not mapped back to pillar '{pillar}'.")

    if DEFAULT_PILLAR not in PILLAR_DOMAINS:
        issues.append(f"DEFAULT_PILLAR '{DEFAULT_PILLAR}' not in PILLAR_DOMAINS")

    if DEFAULT_DOMAIN not in DOMAINS:
        issues.append(f"DEFAULT_DOMAIN '{DEFAULT_DOMAIN}' not in DOMAINS")

    return issues


def validate_bundles() -> list[str]:
    """Compatibility alias for validate_domains."""
    return validate_domains()


def print_bundle_summary() -> None:
    """Print a summary of the dashboard taxonomy."""
    print(f"\n{'='*60}")
    print("India Resilience Tool - Taxonomy Summary")
    print(f"{'='*60}")
    print(f"Total pillars: {len(PILLAR_ORDER)}")
    print(f"Default pillar: {DEFAULT_PILLAR}")
    print(f"Default domain: {DEFAULT_DOMAIN}")

    print(f"\n{'='*60}")
    print("Pillar -> domain contents:")
    print(f"{'='*60}")

    for pillar in PILLAR_ORDER:
        domains = PILLAR_DOMAINS.get(pillar, [])
        if not domains:
            continue
        print(f"\n{pillar}:")
        for domain in domains:
            slugs = DOMAINS.get(domain, [])
            print(f"  {domain} ({len(slugs)} metrics):")
            for slug in slugs:
                spec = METRICS_BY_SLUG.get(slug)
                label = spec.label if spec else "(unknown)"
                print(f"    - {label} ({slug})")


if __name__ == "__main__":
    # Run validation and print summary when executed directly
    print("Validating metrics registry...")
    issues = validate_bundles()
    if issues:
        print("Taxonomy validation issues found:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("All taxonomy definitions validated successfully!")
    
    print_bundle_summary()

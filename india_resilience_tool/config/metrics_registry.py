"""
Shared metrics registry for the India Resilience Tool (IRT).

This module unifies:
- Dashboard metric registry needs (slug/label/group/periods_metric_col + discovery templates)
- Pipeline metric specs (var/value_col/compute/params/units)

Important:
- The pipeline still relies on ordered metric specs; duplicates are preserved in PIPELINE_METRICS_RAW
  to avoid changing existing behavior.
- A slug->spec mapping can be derived (last-wins) for dashboard-style lookup.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence


def infer_group_from_var(var: str) -> str:
    """
    Infer a registry group from the CMIP variable name.

    This is a convenience to populate group for pipeline metrics where group is not
    explicitly defined.
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

    Required fields (dashboard contract):
      - slug
      - label
      - group
      - periods_metric_col

    Pipeline fields (optional but expected for pipeline-driven metrics):
      - var
      - value_col
      - units
      - compute (string key resolved by pipeline runtime)
      - params

    Discovery fields (optional; dashboard can use these later):
      - district_yearly_candidates
      - state_yearly_candidates
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
    aliases: Sequence[str] = field(default_factory=tuple)

    # Discovery templates (optional; filled during dashboard extraction)
    district_yearly_candidates: Optional[Sequence[str]] = None
    state_yearly_candidates: Optional[Sequence[str]] = None

    @staticmethod
    def from_pipeline_dict(d: Mapping[str, Any]) -> "MetricSpec":
        """
        Create a MetricSpec from a pipeline metric dict.

        Contract:
          - periods_metric_col defaults to value_col (this prevents the common
            periods_metric_col ↔ value_col mismatch that causes None/Unknown outputs).
        """
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
            aliases=tuple(d.get("aliases") or ()),
            district_yearly_candidates=d.get("district_yearly_candidates"),
            state_yearly_candidates=d.get("state_yearly_candidates"),
        )


def find_duplicate_slugs(pipeline_metrics: Sequence[Mapping[str, Any]]) -> list[str]:
    """
    Return a sorted list of slugs that appear more than once in the pipeline metric list.
    """
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
    """
    Build a slug -> MetricSpec mapping from pipeline metrics.

    Note:
      - If duplicates exist, this mapping is "last-wins" for that slug.
      - The ordered list is preserved separately (PIPELINE_METRICS_RAW).
    """
    out: dict[str, MetricSpec] = {}
    for m in pipeline_metrics:
        spec = MetricSpec.from_pipeline_dict(m)
        out[spec.slug] = spec
    return out


def validate_registry_against_pipeline(
    registry_by_slug: Mapping[str, MetricSpec],
    pipeline_metrics: Sequence[Mapping[str, Any]],
) -> list[str]:
    """
    Validate registry/pipeline consistency and return a list of human-readable issues.

    This is intentionally strict about the most fragile contract:
      periods_metric_col should match the pipeline's value_col unless explicitly overridden.
    """
    issues: list[str] = []

    dupes = find_duplicate_slugs(pipeline_metrics)
    if dupes:
        issues.append(
            "Duplicate pipeline metric slugs detected (behavior preserved, but risky): "
            + ", ".join(dupes)
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

        pm_var = str(pm.get("var") or "").strip()
        if pm_var and (reg.var or "").strip() and reg.var != pm_var:
            issues.append(
                f"Mismatch for slug '{slug}': registry var='{reg.var}' but pipeline var='{pm_var}'."
            )

    return issues


# -----------------------------------------------------------------------------
# PIPELINE METRICS (single source of truth)
# -----------------------------------------------------------------------------
# These are moved from the legacy root-level compute_indices.py to avoid duplication.
# Keep order and duplicates to preserve existing processing behavior.

PIPELINE_METRICS_RAW: list[dict[str, Any]] = [
    {
        "name": "Summer Days",
        "slug": "tasmax_gt30",
        "var": "tasmax",
        "value_col": "days_gt_30C",
        "units": "days",
        "compute": "count_days_above_threshold",
        "params": {"thresh_k": 30.0 + 273.15},  # 305.15 K
    },
    {
        "name": "Consecutive Summer Days (tasmax > 30 °C)",
        "slug": "tasmax_csd_gt30",
        "var": "tasmax",
        "value_col": "consec_summer_days_gt_30C",
        "units": "days",
        "compute": "longest_consecutive_run_above_threshold",
        "params": {"thresh_k": 30.0 + 273.15},
    },
    {
        "name": "Tropical Nights (tasmin > 20 °C)",
        "slug": "tasmin_tropical_nights_gt20",
        "var": "tasmin",
        "value_col": "tropical_nights_gt_20C",
        "units": "days",
        "compute": "count_days_above_threshold",
        "params": {"thresh_k": 20.0 + 273.15},
    },
    {
        "name": "Heat Wave Duration Index (HWDI)",
        "slug": "hwdi_tasmax_plus5C",       # or whatever slug you're using
        "var": "tasmax",
        "value_col": "hwdi_max_spell_len",
        "units": "days",
        "compute": "heatwave_duration_index",
        "params": {"baseline_years": (1985, 2014), "delta_c": 5.0},
    },
    {
        "name": "Heat Wave Frequency Index (HWF, tmean > 90p)",
        "slug": "hwfi_tmean_90p",
        "var": "tas",
        "value_col": "hwf_days_above_90p",
        "units": "days",
        "compute": "heatwave_frequency_percentile",
        "params": {"baseline_years": (1985, 2014), "pct": 90},
    },
    {
        "name": "HWDI (# Events)",
        "slug": "hwdi_events_tasmax_plus5C",
        "var": "tasmax",
        "value_col": "hwdi_events",
        "units": "events",
        "compute": "heatwave_event_count",
        "params": {"baseline_years": (1985, 2014), "delta_c": 5.0},
    },
    {
        "name": "HWF (# Events, tmean > 90p)",
        "slug": "hwfi_events_tmean_90p",
        "var": "tas",
        "value_col": "hwf_events_above_90p",
        "units": "events",
        "compute": "heatwave_event_count_percentile",
        "params": {"baseline_years": (1985, 2014), "pct": 90},
    },
    {
        "name": "Consecutive Summer Days (# Events, tasmax > 30 °C)",
        "slug": "tasmax_csd_events_gt30",
        "var": "tasmax",
        "value_col": "consec_summer_day_events_gt_30C",
        "units": "events",
        "compute": "consecutive_run_events_above_threshold",
        "params": {"thresh_k": 30.0 + 273.15, "min_event_days": 6},
    },
    {
        "name": "Annual Mean Tmax",
        "slug": "tasmax_annual_mean",
        "var": "tasmax",
        "value_col": "tasmax_annual_mean",
        "units": "K",
        "compute": "annual_mean",
        "params": {},
    },
    {
        "name": "Summer Mean Tmax (MAM)",
        "slug": "tasmax_summer_mean",
        "var": "tasmax",
        "value_col": "tasmax_mam_mean",
        "units": "K",
        "compute": "seasonal_mean",
        "params": {"months": [3, 4, 5]},
    },
    {
        "name": "Annual Mean Tmin",
        "slug": "tasmin_annual_mean",
        "var": "tasmin",
        "value_col": "tasmin_annual_mean",
        "units": "K",
        "compute": "annual_mean",
        "params": {},
    },
    {
        "name": "Winter Mean Tmin (DJF)",
        "slug": "tasmin_winter_mean",
        "var": "tasmin",
        "value_col": "tasmin_djf_mean",
        "units": "K",
        "compute": "seasonal_mean",
        "params": {"months": [12, 1, 2]},
    },
    {
        "name": "Rainy Days (pr > 2.5 mm)",
        "slug": "rain_gt_2p5mm",
        "var": "pr",
        "value_col": "rainy_days_gt_2p5mm",
        "units": "days",
        "compute": "count_rainy_days",
        "params": {"thresh_mm": 2.5},
    },
    {
        "name": "Simple Daily Intensity Index (SDII)",
        "slug": "pr_simple_daily_intensity",
        "var": "pr",
        "value_col": "sdii_mm_per_day",
        "units": "mm/day",
        "compute": "simple_daily_intensity_index",
        "params": {"wet_day_thresh_mm": 1.0},
    },
    {
        "name": "Max 1-day Precipitation (Rx1day)",
        "slug": "pr_max_1day_precip",
        "var": "pr",
        "value_col": "rx1day_mm",
        "units": "mm",
        "compute": "rx1day",
        "params": {},
    },
    {
        "name": "Max 5-day Precipitation (Rx5day)",
        "slug": "pr_max_5day_precip",
        "var": "pr",
        "value_col": "rx5day_mm",
        "units": "mm",
        "compute": "rx5day",
        "params": {},
    },
    {
        "name": "5-day Precipitation Events (sum(pr) > 50 mm)",
        "slug": "pr_5day_precip_events_gt50mm",
        "var": "pr",
        "value_col": "rx5day_events_gt_50mm",
        "units": "events",
        "compute": "rx5day_events_over_threshold",
        "params": {"event_thresh_mm": 50.0},
    },
    {
        "name": "Heavy Precipitation Days (pr > 10 mm)",
        "slug": "pr_heavy_precip_days_gt10mm",
        "var": "pr",
        "value_col": "heavy_precip_days_gt_10mm",
        "units": "days",
        "compute": "count_rainy_days",
        "params": {"thresh_mm": 10.0},
    },
    {
        "name": "Very Heavy Precipitation Days (pr > 25 mm)",
        "slug": "pr_very_heavy_precip_days_gt25mm",
        "var": "pr",
        "value_col": "very_heavy_precip_days_gt_25mm",
        "units": "days",
        "compute": "count_rainy_days",
        "params": {"thresh_mm": 25.0},
    },
    {
        "name": "Consecutive Dry Days (CDD)",
        "slug": "pr_consecutive_dry_days_lt1mm",
        "var": "pr",
        "value_col": "cdd_max_spell_len",
        "units": "days",
        "compute": "consecutive_dry_days",
        "params": {"dry_thresh_mm": 1.0},
    },
    {
        "name": "Consecutive Dry Day Events (CDD events)",
        "slug": "pr_consecutive_dry_day_events_lt1mm",
        "var": "pr",
        "value_col": "consecutive_dry_day_events",
        "units": "events",
        "compute": "consecutive_dry_day_events",
        "params": {"dry_thresh_mm": 1.0, "min_event_days": 6},
    },
]

# Typed views derived from pipeline metrics
PIPELINE_METRICS: list[MetricSpec] = [MetricSpec.from_pipeline_dict(m) for m in PIPELINE_METRICS_RAW]
METRICS_BY_SLUG: dict[str, MetricSpec] = build_registry_from_pipeline(PIPELINE_METRICS_RAW)

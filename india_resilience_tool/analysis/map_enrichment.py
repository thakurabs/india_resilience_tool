"""
Map enrichment helpers (Streamlit-free).

These functions prepare a merged (Geo)DataFrame for:
- choropleth mapping (baseline/delta columns)
- tooltip quick-glance (rank/percentile/risk label + formatted strings)

Contracts:
- This module must not import Streamlit.
- Functions are written to be robust to NaNs/missing columns; they prefer returning
  "Unknown"/"—" rather than raising, unless inputs are structurally invalid.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Tuple

import pandas as pd

from india_resilience_tool.viz.formatting import format_metric_compact


def add_current_baseline_delta(
    merged: pd.DataFrame,
    *,
    metric_col: str,
    baseline_col: Optional[str],
) -> pd.DataFrame:
    """
    Add numeric current/baseline/delta columns used by the map and tooltips.

    Adds columns:
    - `_current_value`: numeric coercion of `metric_col`
    - `_baseline_value`: numeric coercion of `baseline_col` (or all-NA if missing)
    - `_delta_abs`: `_current_value - _baseline_value` (or all-NA if baseline missing)
    - `_delta_pct`: percent change vs baseline (baseline==0 -> NA; baseline missing -> all-NA)

    Missing data behavior:
    - If `metric_col` is missing: `_current_value` becomes all-NA.
    - If `baseline_col` is None or missing from `merged`: baseline/delta columns become all-NA.
    """
    merged["_current_value"] = pd.to_numeric(merged.get(metric_col), errors="coerce")

    if baseline_col and (baseline_col in merged.columns):
        merged["_baseline_value"] = pd.to_numeric(merged.get(baseline_col), errors="coerce")
        merged["_delta_abs"] = merged["_current_value"] - merged["_baseline_value"]

        denom = merged["_baseline_value"].where(merged["_baseline_value"] != 0)
        merged["_delta_pct"] = (merged["_delta_abs"] / denom) * 100.0
    else:
        merged["_baseline_value"] = pd.Series([pd.NA] * len(merged), index=merged.index, dtype="Float64")
        merged["_delta_abs"] = pd.Series([pd.NA] * len(merged), index=merged.index, dtype="Float64")
        merged["_delta_pct"] = pd.Series([pd.NA] * len(merged), index=merged.index, dtype="Float64")

    return merged


def add_rank_percentile_risk(
    merged: pd.DataFrame,
    *,
    admin_level: str,
    rank_higher_is_worse: bool,
    alias_fn: Callable[[str], str],
    risk_class_from_percentile_fn: Callable[[float], str],
    state_col: str = "state_name",
    district_col: str = "district_name",
) -> Tuple[pd.DataFrame, str]:
    """
    Add rank/percentile/risk label columns for tooltip quick-glance.

    Adds columns:
    - `_rank_in_state`: rank within group (1 = worst, direction-aware)
    - `_percentile_state`: inclusive percentile within group (0..100, higher = worse)
    - `_risk_class`: string label derived from percentile (or "Unknown")

    Grouping scope:
    - District admin: group by state (as stored in `state_col`)
    - Block admin: group by state|district (normalized via `alias_fn`)

    Missing data behavior:
    - If grouping columns are missing: uses a single "Unknown" group.
    - If `_current_value` is missing: treats it as all-NA.
    - NA percentiles -> `_risk_class == "Unknown"`.
    """
    v = merged.get("_current_value")
    if v is None:
        v = pd.to_numeric(pd.Series([pd.NA] * len(merged), index=merged.index), errors="coerce")
        merged["_current_value"] = v

    state_series = merged.get(state_col)
    if state_series is None:
        state_series = pd.Series(["Unknown"] * len(merged), index=merged.index)
    state_series = state_series.astype(str).fillna("Unknown")

    admin_level_norm = str(admin_level or "").strip().lower()
    rank_scope_label = "state"

    if admin_level_norm == "block" and district_col in merged.columns:
        district_series = merged[district_col].astype(str).fillna("Unknown")
        group_key = state_series.map(alias_fn) + "|" + district_series.map(alias_fn)
        rank_scope_label = "district"
    else:
        group_key = state_series

    rank_ascending = not bool(rank_higher_is_worse)
    percentile_ascending = bool(rank_higher_is_worse)

    merged["_rank_in_state"] = v.groupby(group_key).rank(method="min", ascending=rank_ascending)
    merged["_percentile_state"] = (
        v.groupby(group_key).rank(pct=True, method="max", ascending=percentile_ascending) * 100.0
    )

    def _risk_label(p: float) -> str:
        try:
            if pd.isna(p):
                return "Unknown"
            return str(risk_class_from_percentile_fn(float(p)))
        except Exception:
            return "Unknown"

    merged["_risk_class"] = merged["_percentile_state"].apply(_risk_label)
    return merged, rank_scope_label


#: Machine exclusion reasons rendered for a reader. Only reasons that explain a
#: *missing* value appear here; "eligible" deliberately does not, because an
#: eligible unit with no value is missing for some other reason and this function
#: must not guess which. An unrecognised reason is passed through verbatim rather
#: than dropped, so a new reason surfaces as itself instead of silently vanishing.
_EXCLUSION_REASON_LABELS: dict[str, str] = {
    "coverage_below_gate_reference_support_dominant": (
        "No data: reference coverage below the required share of this unit's area"
    ),
    "coverage_below_gate_grid_gap_dominant": (
        "No data: climate grid coverage below the required share of this unit's area"
    ),
}


def format_exclusion_reason(reason: Any, *, coverage: Any = None) -> str:
    """Render one per-unit exclusion reason, with its retained share when known."""
    text = str(reason or "").strip()
    if not text or text.lower() == "eligible":
        return ""
    label = _EXCLUSION_REASON_LABELS.get(text.lower(), text)
    try:
        if coverage is not None and not pd.isna(coverage):
            return f"{label} ({float(coverage):.0%} covered)"
    except (TypeError, ValueError):
        pass
    return label


def summarise_missing_selection(
    frame: pd.DataFrame,
    *,
    reason_col: str = "exclusion_reason",
    coverage_col: str = "support_coverage",
) -> str:
    """Explain why a whole selection has no values, or return "" if it cannot.

    A selection with nothing to map is not always a malfunction. Where every unit
    carries a recorded exclusion reason -- the two island territories, where the
    reference support genuinely does not reach the gate -- the absence is the
    documented result, and the honest presentation is the reason, not an error.

    The empty string is returned deliberately whenever the data does not account
    for the absence: no reason column, no rows at all, or any unit marked
    ``eligible`` yet still carrying no value. That last case is a real anomaly and
    the caller should keep reporting it as an error rather than explaining it
    away.
    """
    if frame is None or len(frame) == 0 or reason_col not in frame.columns:
        return ""
    reasons = frame[reason_col].astype(str).str.strip()
    if reasons.eq("").all():
        return ""
    if reasons.str.lower().eq("eligible").any():
        return ""

    counts = reasons.value_counts()
    clauses = []
    for reason, count in counts.items():
        label = format_exclusion_reason(reason)
        if not label:
            continue
        clause = label.split(": ", 1)[1] if label.startswith("No data: ") else label
        clauses.append(f"{clause} ({int(count)} of {len(frame)} units)")
    if not clauses:
        return ""

    message = "No data for this selection. " + "; ".join(clauses) + "."
    if coverage_col in frame.columns:
        coverage = pd.to_numeric(frame[coverage_col], errors="coerce").dropna()
        if not coverage.empty:
            message += (f" Highest retained coverage in this selection is "
                        f"{float(coverage.max()):.0%}.")
    return message


def add_tooltip_strings(
    merged: pd.DataFrame,
    *,
    map_mode: str,
    variable_slug: Optional[str] = None,
    model_count_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    Add human-friendly tooltip string columns (Streamlit-free).

    Adds columns:
    - `_tooltip_value`, `_tooltip_value_label`
    - `_tooltip_baseline`, `_tooltip_delta`
    - `_tooltip_rank`
    - `_tooltip_n_models`, only when `model_count_col` names a column that is
      actually present. A metric whose master carries no contributing-model count
      gets no such column, and the tooltip then omits the row entirely rather than
      showing a blank or a guessed roster size.

    Missing data behavior:
    - Any missing numeric inputs render as "—".
    """

    def _fmt_number(x) -> str:
        return format_metric_compact(x, metric_slug=variable_slug)

    if map_mode == "Change from 1990-2010 baseline":
        merged["_tooltip_value"] = merged.get("_delta_abs", pd.Series([pd.NA] * len(merged), index=merged.index)).apply(
            _fmt_number
        )
        merged["_tooltip_value_label"] = "Δ vs 1990–2010"
    else:
        merged["_tooltip_value"] = merged.get(
            "_current_value", pd.Series([pd.NA] * len(merged), index=merged.index)
        ).apply(_fmt_number)
        merged["_tooltip_value_label"] = "Value"

    merged["_tooltip_baseline"] = merged.get(
        "_baseline_value", pd.Series([pd.NA] * len(merged), index=merged.index)
    ).apply(_fmt_number)
    merged["_tooltip_delta"] = merged.get("_delta_abs", pd.Series([pd.NA] * len(merged), index=merged.index)).apply(
        _fmt_number
    )

    def _fmt_rank(r) -> str:
        if r is None or pd.isna(r):
            return "—"
        try:
            return str(int(round(float(r))))
        except Exception:
            return "—"

    merged["_tooltip_rank"] = merged.get("_rank_in_state", pd.Series([pd.NA] * len(merged), index=merged.index)).apply(
        _fmt_rank
    )

    # Contributing-model count for the displayed slice. It counts the models that
    # produced a finite value for this unit, scenario and period -- the same
    # aggregation level as the value above it -- so a unit whose roster thinned
    # says so.
    #
    # A count of zero renders as "—" rather than "0". The count exists to qualify a
    # published value, and zero contributors means there is no value to qualify: the
    # row sits beside a "—" value, where a literal 0 reads as a measurement and
    # invites the inference that the models are missing. They may not be -- a unit
    # excluded for insufficient reference support has every model available and is
    # simply not eligible, which the metric's coverage table explains and a tooltip
    # cannot. An absent column is a third, separate statement: no row at all.
    if model_count_col and str(model_count_col) in merged.columns:
        counts = pd.to_numeric(merged[str(model_count_col)], errors="coerce")
        merged["_tooltip_n_models"] = counts.where(counts > 0).apply(_fmt_rank)

    # Why a value is missing, where the metric publishes a per-unit reason. Added
    # only when the column exists, so a metric without one gets no row at all.
    if "exclusion_reason" in merged.columns:
        coverage = (
            merged["support_coverage"]
            if "support_coverage" in merged.columns
            else pd.Series([None] * len(merged), index=merged.index)
        )
        merged["_tooltip_no_data_reason"] = [
            format_exclusion_reason(reason, coverage=cov)
            for reason, cov in zip(merged["exclusion_reason"], coverage)
        ]
    return merged

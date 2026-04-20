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

from typing import Callable, Optional, Tuple

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


def add_tooltip_strings(
    merged: pd.DataFrame,
    *,
    map_mode: str,
    variable_slug: Optional[str] = None,
) -> pd.DataFrame:
    """
    Add human-friendly tooltip string columns (Streamlit-free).

    Adds columns:
    - `_tooltip_value`, `_tooltip_value_label`
    - `_tooltip_baseline`, `_tooltip_delta`
    - `_tooltip_rank`

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
    return merged

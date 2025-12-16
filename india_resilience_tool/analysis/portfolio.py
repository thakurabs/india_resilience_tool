"""
Portfolio helpers for IRT.

This module centralizes:
- portfolio normalization + keying
- add/remove/contains/clear operations on a session_state-like mapping
- building the multi-index portfolio comparison table

This module is intentionally Streamlit-free: pass `session_state=st.session_state`
from the app layer.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import difflib
from typing import Any, Callable, Mapping, MutableMapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


PortfolioItem = Mapping[str, Any]  # expects keys: "state", "district"
NormalizeFn = Callable[[str], str]


def portfolio_normalize(text: str, *, alias_fn: Callable[[str], str]) -> str:
    """
    Normalize a state/district name for robust comparison across sources.

    Contract (matches dashboard logic):
      - alias_fn(text) is assumed to do ascii fold + lowercase + aliases
      - remove spaces to handle "Sanga Reddy" vs "Sangareddy"
    """
    norm = alias_fn(text)
    return str(norm).replace(" ", "")


def portfolio_key(
    state_name: str,
    district_name: str,
    *,
    normalize_fn: NormalizeFn,
) -> tuple[str, str]:
    """
    Normalized key used to compare/uniquify portfolio items.
    """
    return (normalize_fn(state_name), normalize_fn(district_name))


def portfolio_add(
    session_state: MutableMapping[str, Any],
    state_name: str,
    district_name: str,
    *,
    normalize_fn: NormalizeFn,
    state_key: str = "portfolio_districts",
) -> None:
    """
    Add a (state, district) pair to the portfolio if not already present.

    Preserves dashboard contract:
      - ignore empty / district == "All"
      - store list items as {"state": <>, "district": <>}
      - de-duplicate using normalized key
    """
    if not state_name or not district_name or district_name == "All":
        return

    items = session_state.get(state_key, [])
    if not isinstance(items, list):
        items = []

    new_norm = portfolio_key(state_name, district_name, normalize_fn=normalize_fn)
    for item in items:
        if not isinstance(item, Mapping):
            continue
        if portfolio_key(
            str(item.get("state", "")),
            str(item.get("district", "")),
            normalize_fn=normalize_fn,
        ) == new_norm:
            return

    items.append({"state": state_name, "district": district_name})
    session_state[state_key] = items


def portfolio_remove(
    session_state: MutableMapping[str, Any],
    state_name: str,
    district_name: str,
    *,
    normalize_fn: NormalizeFn,
    state_key: str = "portfolio_districts",
) -> None:
    """
    Remove a (state, district) pair from the portfolio.
    """
    items = session_state.get(state_key, [])
    if not isinstance(items, list):
        session_state[state_key] = []
        return

    norm = portfolio_key(state_name, district_name, normalize_fn=normalize_fn)

    new_items: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        if portfolio_key(
            str(item.get("state", "")),
            str(item.get("district", "")),
            normalize_fn=normalize_fn,
        ) != norm:
            new_items.append({"state": str(item.get("state", "")), "district": str(item.get("district", ""))})

    session_state[state_key] = new_items


def portfolio_contains(
    session_state: Mapping[str, Any],
    state_name: str,
    district_name: str,
    *,
    normalize_fn: NormalizeFn,
    state_key: str = "portfolio_districts",
) -> bool:
    """
    Return True if (state, district) already exists in portfolio_districts.
    """
    if not state_name or not district_name or district_name == "All":
        return False

    items = session_state.get(state_key, [])
    if not isinstance(items, list):
        return False

    norm = portfolio_key(state_name, district_name, normalize_fn=normalize_fn)
    for item in items:
        if not isinstance(item, Mapping):
            continue
        if portfolio_key(
            str(item.get("state", "")),
            str(item.get("district", "")),
            normalize_fn=normalize_fn,
        ) == norm:
            return True
    return False


def portfolio_clear(
    session_state: MutableMapping[str, Any],
    *,
    state_key: str = "portfolio_districts",
) -> None:
    """
    Clear all portfolio districts.
    """
    session_state[state_key] = []


def build_portfolio_multiindex_df(
    *,
    portfolio: Sequence[Any],
    selected_slugs: Sequence[str],
    variables: Mapping[str, Mapping[str, Any]],
    index_group_labels: Mapping[str, str],
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    load_master_and_schema_for_slug: Callable[[str], tuple[pd.DataFrame, Any, list[str], Any]],
    resolve_metric_column: Callable[[pd.DataFrame, str, str, str, str], Optional[str]],
    find_baseline_column_for_stat: Callable[[Sequence[Any], str, str], Optional[str]],
    match_row_idx: Callable[[pd.DataFrame, str, str], Optional[int]],
    compute_rank_and_percentile: Callable[..., tuple[Optional[int], Optional[float]]],
    risk_class_from_percentile: Callable[[float], str],
    normalize_fn: NormalizeFn,
) -> pd.DataFrame:
    """
    Build the portfolio multi-index comparison table (same semantics as current dashboard).

    Notes:
      - This function intentionally keeps the existing fuzzy fallback logic for metric selection.
      - Column naming is resolved by injected `resolve_metric_column`.
      - Baseline column is resolved by injected `find_baseline_column_for_stat`.
      - Rank/percentile computed by injected `compute_rank_and_percentile` (dashboard wrapper).
    """
    rows_out: list[dict[str, Any]] = []

    for item in portfolio:
        if isinstance(item, Mapping):
            st_name = str(item.get("state", "")).strip()
            dist_name = str(item.get("district", "")).strip()
        else:
            try:
                st_name, dist_name = item  # type: ignore[misc]
                st_name = str(st_name).strip()
                dist_name = str(dist_name).strip()
            except Exception:
                st_name, dist_name = "", ""

        for slug in selected_slugs:
            cfg = variables.get(slug, {})
            idx_label = cfg.get("label", str(slug))
            idx_group = cfg.get("group", "other")
            idx_group_label = index_group_labels.get(idx_group, str(idx_group).title())
            registry_metric_i = str(cfg.get("periods_metric_col", "")).strip()

            df_local, _, metrics_local, _ = load_master_and_schema_for_slug(str(slug))

            used_metric = registry_metric_i
            metric_col = None
            if used_metric:
                metric_col = resolve_metric_column(df_local, used_metric, sel_scenario, sel_period, sel_stat)

            # Fuzzy fallback: if configured metric base isn't available, pick closest base metric.
            if metric_col is None and registry_metric_i and metrics_local:
                base_norm = normalize_fn(registry_metric_i)
                candidates = [m for m in metrics_local if base_norm and base_norm in normalize_fn(m)]
                if not candidates:
                    candidates = difflib.get_close_matches(registry_metric_i, metrics_local, n=1, cutoff=0.6)
                if candidates:
                    used_metric = candidates[0]
                    metric_col = resolve_metric_column(df_local, used_metric, sel_scenario, sel_period, sel_stat)

            baseline_col = None
            if used_metric and df_local is not None and not df_local.empty:
                baseline_col = find_baseline_column_for_stat(df_local.columns, used_metric, sel_stat)

            # Defaults
            value = np.nan
            baseline = np.nan
            delta_abs = np.nan
            delta_pct = np.nan
            rank_in_state: Optional[int] = None
            percentile_in_state = np.nan
            risk_class = "Unknown"

            if df_local is not None and not df_local.empty and metric_col and metric_col in df_local.columns:
                idx_row = match_row_idx(df_local, st_name, dist_name)
                if idx_row is not None:
                    try:
                        value = float(pd.to_numeric(df_local.loc[idx_row, metric_col], errors="coerce"))
                    except Exception:
                        value = np.nan

                    if baseline_col and baseline_col in df_local.columns:
                        try:
                            baseline = float(pd.to_numeric(df_local.loc[idx_row, baseline_col], errors="coerce"))
                        except Exception:
                            baseline = np.nan

                    if not pd.isna(value) and not pd.isna(baseline):
                        delta_abs = value - baseline
                        if baseline != 0:
                            delta_pct = (delta_abs / baseline) * 100.0

                    rank_in_state, percentile = compute_rank_and_percentile(
                        df_local=df_local,
                        st_name=st_name,
                        metric_col=metric_col,
                        value=value,
                    )
                    if percentile is not None:
                        percentile_in_state = float(percentile)
                        risk_class = risk_class_from_percentile(percentile_in_state)

            rows_out.append(
                {
                    "State": st_name,
                    "District": dist_name,
                    "Index": idx_label,
                    "Group": idx_group_label,
                    "Current value": value,
                    "Baseline": baseline,
                    "Δ": delta_abs,
                    "%Δ": delta_pct,
                    "Rank in state": rank_in_state,
                    "Percentile": percentile_in_state,
                    "Risk class": risk_class,
                }
            )

    return pd.DataFrame(rows_out)

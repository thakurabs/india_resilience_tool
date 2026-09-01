"""
Table assembly helpers for IRT visualizations.

This module centralizes DataFrame assembly used by the Streamlit rankings view,
including:
- filtering to selected state
- absolute value ranking
- optional baseline + deltas ranking
- percentiles + risk class mapping

Streamlit-free: caching belongs in the app layer.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Callable, Optional, Tuple

import numpy as np
import pandas as pd


def build_rankings_table_df(
    merged_df: pd.DataFrame,
    *,
    metric_col: str,
    baseline_col: Optional[str],
    selected_state: str,
    risk_class_from_percentile: Callable[[float], str],
    district_col: str = "district_name",
    state_col: str = "state_name",
    aspirational_col: str = "aspirational",
    extra_cols: Optional[list[str]] = None,
    higher_is_worse: bool = True,
    bundle_score_col: Optional[str] = None,
) -> Tuple[pd.DataFrame, bool]:
    """
    Build the district-level rankings table used by the dashboard.

    The ``risk_class`` label is normally assigned from the metric's ordinal
    percentile (``value.rank(pct=True)``), which forces an even quintile spread
    even when metric values are tightly clustered (Method A). When
    ``bundle_score_col`` names a column of 0-100 bundle composite scores
    (already higher-worse, non-ordinal), the label is instead assigned by
    classifying those composite scores directly (Method B), so a near-flat
    cohort no longer sweeps the full Very High -> Very Low range.

    ``percentile_value`` is always computed and returned as the true ordinal
    percentile (a displayed column) regardless of which label source is used.

    Method-B engagement is **table-wide and self-defensive**: if any surviving
    ``bundle_score`` value is NaN, the entire table falls back to the ordinal
    label source, so the methodology is never mixed per row.

    Args:
        bundle_score_col: Optional name of a column on ``merged_df`` holding the
            0-100 bundle composite score. When present and fully populated for
            the surviving rows, it becomes the ``risk_class`` source (Method B).

    Returns:
        (table_df, has_baseline)
    """
    if merged_df is None or merged_df.empty:
        return pd.DataFrame(), False

    if district_col not in merged_df.columns:
        return pd.DataFrame(), False

    ranking_source = merged_df.copy()
    if state_col not in ranking_source.columns:
        ranking_source[state_col] = "Hydro"

    # Filter for ranking: respect selected_state
    if selected_state != "All":
        rank_mask = (
            ranking_source[state_col].astype(str).str.strip().str.lower()
            == str(selected_state).strip().lower()
        )
        ranking_df = ranking_source.loc[rank_mask].copy()
    else:
        ranking_df = ranking_source.copy()

    if ranking_df.empty:
        return pd.DataFrame(), False

    if metric_col not in ranking_df.columns:
        return pd.DataFrame(), False

    columns_to_keep: list[str] = [district_col, state_col]
    for col in list(extra_cols or []):
        if col in ranking_df.columns and col not in columns_to_keep:
            columns_to_keep.append(col)

    table_df = ranking_df[columns_to_keep].copy()

    # Absolute value
    value_series = pd.to_numeric(ranking_df[metric_col], errors="coerce")
    table_df["value"] = value_series

    # Optional bundle composite score (Method B label source). Added aligned by
    # index alongside `value` and BEFORE the NaN-value dropna below, so it rides
    # through the same row filtering and stays index-aligned with the rest of
    # the table.
    if bundle_score_col and bundle_score_col in ranking_df.columns:
        table_df["bundle_score"] = pd.to_numeric(
            ranking_df.loc[:, bundle_score_col], errors="coerce"
        )

    # Baseline & changes
    has_baseline = bool(baseline_col) and (baseline_col in ranking_df.columns)
    if has_baseline:
        baseline_series = pd.to_numeric(ranking_df[baseline_col], errors="coerce")
        table_df["baseline"] = baseline_series

        table_df["delta_abs"] = table_df["value"] - table_df["baseline"]
        table_df["delta_pct"] = np.where(
            (baseline_series != 0) & (~baseline_series.isna()),
            100.0 * table_df["delta_abs"] / baseline_series,
            np.nan,
        )
    else:
        has_baseline = False

    # Drop rows with NaN value (cannot rank)
    table_df = table_df[~table_df["value"].isna()].copy()
    if table_df.empty:
        return pd.DataFrame(), has_baseline

    # Rank by absolute value: 1 = worst.
    # When higher_is_worse=True (registry default), worst = highest → rank descending.
    # When higher_is_worse=False (cold-magnitude metrics), worst = lowest → rank ascending.
    rank_descending = bool(higher_is_worse)
    table_df["rank_value"] = (
        table_df["value"].rank(ascending=not rank_descending, method="min").astype(int)
    )

    # Percentile (0..100), direction-aware: higher percentile always means higher risk.
    # Always computed: it is the true ordinal and a displayed ("Percentile") column,
    # independent of which source drives the risk_class label below.
    pct_ascending = table_df["value"].rank(pct=True) * 100.0
    table_df["percentile_value"] = pct_ascending if higher_is_worse else (100.0 - pct_ascending)

    # Risk class. Method B (bundle composite 0-100, already higher-worse so no
    # direction flip) when a fully-populated bundle_score is present for every
    # surviving row; otherwise Method A (ordinal percentile). The all-or-nothing
    # guard keeps the methodology uniform table-wide.
    use_bundle = (
        "bundle_score" in table_df.columns and table_df["bundle_score"].notna().all()
    )
    label_source = table_df["bundle_score"] if use_bundle else table_df["percentile_value"]
    table_df["risk_class"] = label_source.apply(risk_class_from_percentile)

    # Rank by increase if baseline present.
    # For "higher is worse" metrics, biggest positive delta = worst (descending).
    # For "lower is worse" metrics, most negative delta = worst (ascending).
    if has_baseline and "delta_abs" in table_df.columns:
        delta_rank = table_df["delta_abs"].rank(
            ascending=not rank_descending,
            method="min",
        )
        if delta_rank.notna().any():
            table_df["rank_delta"] = delta_rank.astype("Int64")

    # Carry aspirational flag if present
    if aspirational_col in ranking_df.columns:
        table_df[aspirational_col] = ranking_df[aspirational_col].values

    return table_df, has_baseline

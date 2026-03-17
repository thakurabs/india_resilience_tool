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
) -> Tuple[pd.DataFrame, bool]:
    """
    Build the district-level rankings table used by the dashboard.

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

    # Rank by absolute value (1 = highest)
    table_df["rank_value"] = (
        table_df["value"].rank(ascending=False, method="min").astype(int)
    )

    # Percentile (0..100)
    table_df["percentile_value"] = table_df["value"].rank(pct=True) * 100.0

    # Risk class
    table_df["risk_class"] = table_df["percentile_value"].apply(risk_class_from_percentile)

    # Rank by increase if baseline present
    if has_baseline and "delta_abs" in table_df.columns:
        if table_df["delta_abs"].notna().any():
            table_df["rank_delta"] = (
                table_df["delta_abs"].rank(ascending=False, method="min").astype(int)
            )

    # Carry aspirational flag if present
    if aspirational_col in ranking_df.columns:
        table_df[aspirational_col] = ranking_df[aspirational_col].values

    return table_df, has_baseline

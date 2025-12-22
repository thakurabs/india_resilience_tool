"""
Core metric analytics helpers for IRT.

This module centralizes:
- rank within state (descending)
- percentile within state (<= or < variants)
- risk class mapping from percentile

This is intentionally Streamlit-free and UI-agnostic.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Sequence, Tuple

import pandas as pd


def risk_class_from_percentile(p: float) -> str:
    """
    Map percentile (0..100) to UI risk class labels.

    Contract (must match dashboard expectations exactly):
      - NaN -> "Unknown"
      - >=80 -> "Very High"
      - >=60 -> "High"
      - >=40 -> "Medium"
      - >=20 -> "Low"
      - else -> "Very Low"
    """
    if pd.isna(p):
        return "Unknown"
    if p >= 80:
        return "Very High"
    if p >= 60:
        return "High"
    if p >= 40:
        return "Medium"
    if p >= 20:
        return "Low"
    return "Very Low"


def _default_normalize(s: str) -> str:
    return str(s).strip().lower().replace(" ", "")


def compute_percentile_in_state(
    values: pd.Series,
    value: float,
    *,
    method: str = "le",
) -> Optional[float]:
    """
    Compute percentile of `value` within `values`.

    Args:
        values: Numeric-like series; NaNs ignored.
        value: The value whose percentile to compute.
        method:
            - "le": fraction(values <= value) * 100  (inclusive; matches rankings table helper)
            - "lt": fraction(values <  value) * 100  (strict; matches portfolio record builder)

    Returns:
        Percentile in [0,100] or None if undefined.
    """
    if values is None:
        return None

    vals = pd.to_numeric(values, errors="coerce").dropna()
    if vals.empty or pd.isna(value):
        return None

    try:
        v = float(value)
    except Exception:
        return None

    if method not in {"le", "lt"}:
        raise ValueError("method must be 'le' or 'lt'")

    try:
        if method == "le":
            return float((vals <= v).mean() * 100.0)
        return float((vals < v).mean() * 100.0)
    except Exception:
        return None


def compute_rank_descending(values: pd.Series, value: float) -> Optional[int]:
    """
    Rank of `value` among `values` in descending order (higher => rank 1).

    This matches the dashboard nested helper behavior:
      rank = count(values > value) + 1

    Returns:
        Rank (1..N) or None.
    """
    if values is None:
        return None

    vals = pd.to_numeric(values, errors="coerce").dropna()
    if vals.empty or pd.isna(value):
        return None

    try:
        v = float(value)
    except Exception:
        return None

    try:
        return int((vals > v).sum() + 1)
    except Exception:
        return None


def compute_rank_and_percentile(
    df_local: pd.DataFrame,
    st_name: str,
    metric_col: str,
    value: float,
    *,
    state_col: str = "state",
    normalize_fn: Optional[Callable[[str], str]] = None,
    percentile_method: str = "le",
) -> Tuple[Optional[int], Optional[float]]:
    """
    Compute (rank_in_state, percentile_in_state) for a value given a master-like table.

    Args:
        df_local: DataFrame containing at least `state_col` and `metric_col`.
        st_name: State name to filter by.
        metric_col: Column containing the metric values.
        value: Value for which rank/percentile is computed.
        state_col: Name of the state column (default "state").
        normalize_fn: Function to normalize state names for matching.
                      If None, uses a conservative default (lower + strip + remove spaces).
        percentile_method: "le" or "lt" for percentile computation.

    Returns:
        (rank, percentile) where each may be None.
    """
    if df_local is None or df_local.empty:
        return None, None
    if state_col not in df_local.columns:
        return None, None
    if metric_col not in df_local.columns:
        return None, None
    if pd.isna(value):
        return None, None

    norm = normalize_fn or _default_normalize
    try:
        st_norm = norm(st_name)
        state_norm = df_local[state_col].astype(str).map(norm)
    except Exception:
        return None, None

    try:
        m_state = state_norm == st_norm
        vals = pd.to_numeric(df_local.loc[m_state, metric_col], errors="coerce").dropna()
    except Exception:
        return None, None

    if vals.empty:
        return None, None

    rank = compute_rank_descending(vals, value)
    percentile = compute_percentile_in_state(vals, value, method=percentile_method)
    return rank, percentile

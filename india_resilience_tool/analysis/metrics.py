"""
Core metric analytics helpers for IRT.

This module centralizes:
- Direction-aware rank/percentile within a comparison group
- Percentile helpers used across the dashboard
- Risk class mapping from percentile

This is intentionally Streamlit-free and UI-agnostic.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class PositionStats:
    """Rank/percentile summary within a comparison group.

    Fields:
        rank:
            1..N where 1 indicates the *worst* value per `higher_is_worse`.
            - If higher_is_worse=True: rank 1 = highest value
            - If higher_is_worse=False: rank 1 = lowest value
        n:
            Number of non-missing values used.
        percentile:
            0..100, where higher = worse (direction-aware).

    Notes:
        - For UI stability, the default percentile definition is *inclusive*
          so worst ties map to 100.
        - Rank uses a competition-style definition (ties share the best rank).
    """

    rank: Optional[int]
    n: Optional[int]
    percentile: Optional[float]


def risk_class_from_percentile(p: float) -> str:
    """Map percentile (0..100) to UI risk class labels.

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


def compute_percentile_in_state(
    state_vals: pd.Series,
    value: float,
    *,
    method: str = "le",
) -> Optional[float]:
    """Compute percentile rank within a state distribution.

    Args:
        state_vals: Series of values in the comparison group (NaNs allowed).
        value: Value to locate within the distribution.
        method:
            - "le": percentile = fraction <= value (inclusive)
            - "lt": percentile = fraction < value  (exclusive)

    Returns:
        Percentile as a float in [0, 100], or None if inputs are insufficient.

    Notes:
        This helper is retained for backwards compatibility. For a direction-aware,
        rank-consistent percentile, use `compute_position_stats(...).percentile` instead.
    """
    if state_vals is None:
        return None
    if value is None or pd.isna(value):
        return None

    v = pd.to_numeric(state_vals, errors="coerce").dropna()
    if v.empty:
        return None

    method = str(method or "le").strip().lower()
    if method not in {"le", "lt"}:
        raise ValueError("method must be 'le' or 'lt'")

    if method == "le":
        frac = float((v <= value).sum()) / float(len(v))
    else:
        frac = float((v < value).sum()) / float(len(v))
    return frac * 100.0


def compute_rank_descending(values: pd.Series, value: float) -> Optional[int]:
    """Compute descending rank (1=highest) within values.

    Args:
        values: Series of comparison values.
        value: Value to rank.

    Returns:
        Rank as int (1..N) or None.
    """
    if values is None:
        return None
    if value is None or pd.isna(value):
        return None
    v = pd.to_numeric(values, errors="coerce").dropna()
    if v.empty:
        return None
    return int((v > value).sum() + 1)


def compute_rank_and_percentile(
    df: pd.DataFrame,
    state_name: str,
    metric_col: str,
    value: float,
    *,
    state_col: str = "state",
    percentile_method: str = "le",
    normalize_fn: Optional[Callable[[str], str]] = None,
) -> Tuple[Optional[int], Optional[float]]:
    """Compute rank (descending) and percentile within a state.

    This helper matches the earlier dashboard behavior (rank 1 = highest).
    For direction-aware logic (e.g., SPI where lower is worse), use
    `compute_position_stats`.

    Args:
        normalize_fn:
            Optional normalization function to apply to state names before matching.
            If not provided, matching falls back to strip + lower.
    """
    if df is None or df.empty:
        return None, None
    if metric_col not in df.columns or state_col not in df.columns:
        return None, None

    if normalize_fn is None:
        left = df[state_col].astype(str).str.strip().str.lower()
        right = str(state_name).strip().lower()
    else:
        def _safe_norm(x: object) -> str:
            try:
                return normalize_fn(str(x))
            except Exception:
                return str(x).strip().lower()

        left = df[state_col].astype(str).map(_safe_norm)
        right = _safe_norm(state_name)

    mask = left == right
    state_vals = pd.to_numeric(df.loc[mask, metric_col], errors="coerce").dropna()
    if state_vals.empty:
        return None, None

    rank = compute_rank_descending(state_vals, value)
    pct = compute_percentile_in_state(state_vals, value, method=percentile_method)
    return rank, pct


def compute_position_stats(
    values: pd.Series,
    value: Optional[float],
    *,
    higher_is_worse: bool = True,
    percentile_inclusive: bool = True,
) -> PositionStats:
    """Compute direction-aware rank and percentile for a value.

    Args:
        values:
            Comparison distribution (NaNs allowed).
        value:
            Value to rank. If None/NaN -> all outputs None.
        higher_is_worse:
            If True, higher values are considered worse.
            If False, lower values are considered worse.
        percentile_inclusive:
            If True, percentiles are inclusive of ties at the worst end, so worst ties map to 100.
            If False, percentiles exclude equality (slightly smaller percentiles for ties).

    Returns:
        PositionStats with rank, n, percentile. Percentile is always defined such that
        higher = worse (0..100).
    """
    if values is None:
        return PositionStats(rank=None, n=None, percentile=None)

    v = pd.to_numeric(values, errors="coerce").dropna()
    if v.empty:
        return PositionStats(rank=None, n=None, percentile=None)

    if value is None or pd.isna(value):
        return PositionStats(rank=None, n=int(len(v)), percentile=None)

    value_f = float(value)
    n = int(len(v))

    # Rank: 1 = worst
    if higher_is_worse:
        rank = int((v > value_f).sum() + 1)
        if percentile_inclusive:
            percentile = float((v <= value_f).sum()) / float(n) * 100.0
        else:
            percentile = float((v < value_f).sum()) / float(n) * 100.0
    else:
        rank = int((v < value_f).sum() + 1)
        if percentile_inclusive:
            percentile = float((v >= value_f).sum()) / float(n) * 100.0
        else:
            percentile = float((v > value_f).sum()) / float(n) * 100.0

    return PositionStats(rank=rank, n=n, percentile=percentile)


def rank_series_within_group(
    series: pd.Series,
    group_key: pd.Series,
    *,
    higher_is_worse: bool = True,
) -> pd.Series:
    """Vectorized, direction-aware rank within group.

    Returns a pandas Series with ranks (float) aligned to the input series.
    NaNs in the input remain NaN in the output.
    """
    rank_ascending = not higher_is_worse
    return series.groupby(group_key).rank(method="min", ascending=rank_ascending)


def percentile_series_within_group(
    series: pd.Series,
    group_key: pd.Series,
    *,
    higher_is_worse: bool = True,
    inclusive: bool = True,
) -> pd.Series:
    """Vectorized, direction-aware percentile within group.

    Percentile is always returned on 0..100, where higher = worse.
    """
    # For percentiles, we want higher = worse.
    # If higher_is_worse: ascending=True makes larger values have larger ranks.
    # If lower_is_worse: ascending=False makes smaller values have larger ranks.
    percentile_ascending = higher_is_worse
    method = "max" if inclusive else "average"
    return series.groupby(group_key).rank(pct=True, method=method, ascending=percentile_ascending) * 100.0


def safe_apply_numeric(
    series: pd.Series,
    fn: Callable[[pd.Series], Optional[float]],
) -> Optional[float]:
    """Apply a numeric reducer safely after coercing to numeric and dropping NaNs."""
    if series is None:
        return None
    v = pd.to_numeric(series, errors="coerce").dropna()
    if v.empty:
        return None
    try:
        return fn(v)
    except Exception:
        return None

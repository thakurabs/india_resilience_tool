"""
Bundle-level landing score helpers for the India Resilience Tool (IRT).

These helpers are intentionally Streamlit-free so the landing-page scoring
method can be tested independently of the UI.

Methodology
-----------
- Input metrics may have different units and numeric ranges.
- Each metric is first normalized onto a 0-100 scale.
- Higher values always mean worse hazard after normalization.
- Bundle scores are weighted means of available normalized metrics only.
- When custom bundle weights are not supplied, equal weights are used.
- Missing metrics yield partial results; geographies with no valid metrics
  receive NaN bundle scores.

V1 landing aggregation
----------------------
- District bundle scores are treated as the primary geography-level score.
- State bundle scores are derived as the unweighted mean of district bundle
  scores within each state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class BundleMetricSpec:
    """Metadata required to normalize and aggregate one bundle metric."""

    slug: str
    label: str
    column: str
    weight: float = 1.0
    higher_is_worse: bool = True


def normalized_metric_column(slug: str) -> str:
    """Return the stable normalized-column name for one metric slug."""
    return f"{str(slug).strip()}__landing_norm"


def normalize_metric_series(
    values: pd.Series,
    *,
    higher_is_worse: bool,
) -> pd.Series:
    """
    Normalize one metric series onto a 0-100 higher-worse scale.

    Missing-data behavior:
    - Non-numeric or missing values become NaN.
    - If all values are missing, the output is all NaN.
    - If all finite values are identical, all finite rows receive `50.0`.

    Args:
        values: Raw metric values for a single geography family.
        higher_is_worse: Registry directionality flag.

    Returns:
        Series aligned to the input index with normalized scores in `[0, 100]`
        or NaN where no usable value is available.
    """
    numeric = pd.to_numeric(values, errors="coerce")
    out = pd.Series(np.nan, index=numeric.index, dtype=float)

    finite = numeric[np.isfinite(numeric)]
    if finite.empty:
        return out

    lo = float(finite.min())
    hi = float(finite.max())
    if hi == lo:
        out.loc[finite.index] = 50.0
        return out

    scaled = (finite - lo) / (hi - lo)
    if not higher_is_worse:
        scaled = 1.0 - scaled

    out.loc[scaled.index] = scaled * 100.0
    return out


def compute_bundle_score_frame(
    df: pd.DataFrame,
    *,
    metric_specs: Sequence[BundleMetricSpec],
    id_columns: Sequence[str],
) -> pd.DataFrame:
    """
    Compute weighted bundle scores from a wide metric frame.

    Missing-data behavior:
    - Metrics missing from `df` are skipped.
    - Rows with some valid normalized metrics receive the weighted mean of those values.
    - Weights are renormalized across available metrics within each row.
    - Rows with no valid normalized metrics receive NaN bundle scores.

    Args:
        df: Wide dataframe containing geography IDs and one raw column per metric.
        metric_specs: Metric metadata including source columns and directionality.
        id_columns: Geography-identifying columns to preserve in the output.

    Returns:
        DataFrame containing the requested ID columns, normalized metric columns,
        `bundle_score`, and `available_metric_count`.
    """
    if not id_columns:
        raise ValueError("At least one id column is required to compute bundle scores.")

    out = df.loc[:, [col for col in id_columns if col in df.columns]].copy()
    normalized_columns: list[str] = []
    normalized_weights: list[float] = []

    for spec in metric_specs:
        if spec.column not in df.columns:
            continue
        norm_col = normalized_metric_column(spec.slug)
        out[norm_col] = normalize_metric_series(
            df[spec.column],
            higher_is_worse=bool(spec.higher_is_worse),
        )
        normalized_columns.append(norm_col)
        normalized_weights.append(float(spec.weight))

    if not normalized_columns:
        out["bundle_score"] = np.nan
        out["available_metric_count"] = 0
        return out

    norm_frame = out[normalized_columns]
    weight_series = pd.Series(normalized_weights, index=normalized_columns, dtype=float)
    available_weights = norm_frame.notna().mul(weight_series, axis=1).sum(axis=1)
    weighted_sum = norm_frame.mul(weight_series, axis=1).sum(axis=1, skipna=True)
    out["bundle_score"] = weighted_sum.div(available_weights.where(available_weights > 0.0))
    out["available_metric_count"] = norm_frame.notna().sum(axis=1).astype(int)
    out.loc[out["available_metric_count"] == 0, "bundle_score"] = np.nan
    return out


def aggregate_state_bundle_scores(
    district_scores: pd.DataFrame,
    *,
    state_col: str = "state_name",
    score_col: str = "bundle_score",
) -> pd.DataFrame:
    """
    Derive state bundle scores from district bundle scores.

    V1 contract:
    - Uses a simple unweighted mean of district bundle scores within each state.
    - Districts with NaN bundle scores are excluded from the state mean.

    Args:
        district_scores: District-level score frame.
        state_col: State-name column.
        score_col: Bundle-score column.

    Returns:
        DataFrame with one row per state and the aggregated bundle score.
    """
    if state_col not in district_scores.columns:
        raise ValueError(f"Missing required state column: {state_col!r}")

    score_frame = district_scores[[state_col, score_col]].copy()
    score_frame[score_col] = pd.to_numeric(score_frame[score_col], errors="coerce")

    grouped = (
        score_frame.groupby(state_col, as_index=False, dropna=False)[score_col]
        .mean()
        .reset_index(drop=True)
    )
    return grouped


def compute_metric_driver_frame(
    score_frame: pd.DataFrame,
    *,
    metric_specs: Sequence[BundleMetricSpec],
) -> pd.DataFrame:
    """
    Summarize normalized metric drivers for a selected geography scope.

    Missing-data behavior:
    - Metrics with no finite normalized values in the supplied frame are skipped.

    Args:
        score_frame: District- or geography-scoped score frame containing
            normalized metric columns.
        metric_specs: Metric metadata used to label the output.

    Returns:
        DataFrame sorted by descending normalized contribution with columns:
        `metric_slug`, `metric_label`, `normalized_score`.
    """
    rows: list[dict[str, object]] = []
    for spec in metric_specs:
        norm_col = normalized_metric_column(spec.slug)
        if norm_col not in score_frame.columns:
            continue

        values = pd.to_numeric(score_frame[norm_col], errors="coerce").dropna()
        if values.empty:
            continue

        rows.append(
            {
                "metric_slug": spec.slug,
                "metric_label": spec.label,
                "normalized_score": float(values.mean()),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["metric_slug", "metric_label", "normalized_score"])

    driver_df = pd.DataFrame(rows).sort_values(
        "normalized_score",
        ascending=False,
        kind="stable",
    )
    return driver_df.reset_index(drop=True)

"""
Master-column helpers (Streamlit-free).

These helpers interpret the wide master schema convention:
  {metric}__{scenario}__{period}__{stat}

They are used by the dashboard for baseline discovery and labeling.
"""

from __future__ import annotations

import re
from typing import Any, Optional, Sequence


def resolve_metric_column(
    df_or_cols: Any,
    base_metric: str,
    scenario: str,
    period: str,
    stat: str,
    *,
    strict: bool = False,
) -> Optional[str]:
    """
    Resolve the master CSV column name for a metric/scenario/period/stat.

    Master columns are expected to be normalized to:
        <metric>__<scenario>__<period>__<stat>

    This function is deliberately permissive about `period` formatting
    (e.g., "1990_2010" vs "1990-2010").

    Args:
        df_or_cols: DataFrame/GeoDataFrame (with `.columns`) or an iterable of column names.
        base_metric: metric slug/base (left-most token in the master schema).
        scenario: scenario key (e.g., "historical", "ssp245").
        period: period token (e.g., "1990-2010", "2020-2040").
        stat: stat token (e.g., "mean", "median", "p95").
        strict: When True, the requested period must match the column's period
            after normalization (`_` <-> `-`). The permissive fallback that
            returned the first scenario+stat-matching column when the period
            did not match is disabled, so callers receive ``None`` instead of
            a silently-substituted column. Period-format tolerance is preserved.

    Returns:
        The matching column name (preserving original casing) if found, else None.
    """
    metric = str(base_metric or "").strip()
    if not metric:
        return None

    try:
        cols = list(df_or_cols.columns)  # type: ignore[attr-defined]
    except Exception:
        try:
            cols = list(df_or_cols)
        except Exception:
            return None

    scen = str(scenario).strip().lower()
    per = str(period).strip().replace("_", "-").replace("–", "-")
    stt = str(stat).strip().lower()

    col_map = {str(c).lower(): str(c) for c in cols}
    candidate = f"{metric}__{scen}__{per}__{stt}".lower()
    if candidate in col_map:
        return col_map[candidate]

    # Fallback: match by pieces (handles minor period formatting differences).
    try:
        pat = re.compile(
            rf"^{re.escape(metric)}__{re.escape(scen)}__(?P<period>.+)__{re.escape(stt)}$",
            flags=re.IGNORECASE,
        )
        matches: list[tuple[str, str]] = []
        for c in cols:
            m = pat.match(str(c))
            if m:
                matches.append((str(c), m.group("period")))
        if not matches:
            return None

        per_norm = per.lower()
        for c, col_period in matches:
            col_period_norm = col_period.strip().replace("_", "-").replace("–", "-").lower()
            if col_period_norm == per_norm:
                return c
        if strict:
            return None
        # Legacy permissive fallback: previously this returned the first scenario+stat
        # match even when the period did not match, which produced silent substitution.
        # Retained for non-strict callers (dashboard, optimized-bundle paths).
        per_substr = per_norm
        for c, _col_period in matches:
            if per_substr in c.lower():
                return c
        return matches[0][0]
    except Exception:
        return None


def find_baseline_column_for_stat(
    df_cols: Sequence[str],
    base_metric: str,
    stat: str,
    preferred_period: str = "1990-2010",
) -> Optional[str]:
    """
    Find a historical baseline column for a metric + stat.

    Columns are expected in the form:
        <metric>__<scenario>__<period>__<stat>

    Contract (legacy):
      - Only considers `historical` scenario
      - Prefers `preferred_period` when present (accepts minor variants like 1990_2010)
      - Otherwise returns the lexicographically earliest historical period
    """
    metric = str(base_metric or "").strip()
    stt = str(stat or "").strip()
    if not metric or not stt:
        return None

    pat = re.compile(
        rf"^{re.escape(metric)}__(?P<scenario>[^_]+)__(?P<period>[^_]+)__{re.escape(stt)}$"
    )
    candidates: list[tuple[str, str]] = []
    for c in df_cols:
        m = pat.match(str(c))
        if not m:
            continue
        scen = m.group("scenario").strip().lower()
        if scen != "historical":
            continue
        period = m.group("period").strip()
        candidates.append((str(c), period))

    if not candidates:
        return None

    pref = str(preferred_period).strip().replace("_", "-")
    for c, p in candidates:
        if p.replace("_", "-") == pref:
            return c

    candidates.sort(key=lambda x: x[1])
    return candidates[0][0]


def find_baseline_column_for_metric(
    df_cols: Sequence[str],
    *,
    base_metric: str,
    preferred_period_tokens: Sequence[str] = ("1995-2014", "1995_2014", "1985-2014"),
) -> Optional[str]:
    """
    Find a historical baseline column for the given metric (legacy heuristic).

    Contract (legacy):
      - Only considers columns ending in `__mean`
      - Only considers `historical` scenario
      - Prefers a small set of historical periods when present
      - Otherwise returns the lexicographically earliest historical period

    Returns:
        Column name or None if no suitable baseline column is found.
    """
    metric = str(base_metric or "").strip()
    if not metric:
        return None

    pat = re.compile(
        rf"^{re.escape(metric)}__(?P<scenario>[^_]+)__(?P<period>[^_]+)__mean$"
    )

    candidates: list[tuple[str, str]] = []
    for c in df_cols:
        m = pat.match(str(c))
        if m and m.group("scenario").lower() == "historical":
            candidates.append((str(c), str(m.group("period"))))

    if not candidates:
        return None

    pref = {str(p).replace(" ", "") for p in preferred_period_tokens if str(p).strip()}
    for c, p in candidates:
        if p.replace(" ", "") in pref:
            return c

    candidates.sort(key=lambda x: x[1])
    return candidates[0][0]

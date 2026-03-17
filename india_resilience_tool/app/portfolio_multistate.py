"""
Streamlit-free helpers for multi-state portfolio comparison.

This module contains pure functions used by the app-layer portfolio UI to:
  - extract portfolio states
  - compute summary-strip stats

Kept free of Streamlit imports so it can be unit-tested with pytest.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

import pandas as pd


def extract_states_in_portfolio(
    portfolio: Sequence[Any],
    *,
    fallback_state: Optional[str] = None,
) -> list[str]:
    """
    Extract unique state names represented in a portfolio.

    Notes:
      - Preserves first-seen casing/spelling for each state (case-insensitive uniqueness).
      - Drops empty/"All" values.
      - Uses fallback_state when extraction yields no valid states.
    """
    seen: set[str] = set()
    out: list[str] = []

    for item in portfolio:
        st_name = None
        if isinstance(item, dict):
            st_name = item.get("state")
        else:
            try:
                tup = tuple(item)
                st_name = tup[0] if len(tup) > 0 else None
            except Exception:
                st_name = None

        s = str(st_name).strip() if st_name is not None else ""
        if not s or s == "All":
            continue
        k = s.strip().lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)

    if not out and isinstance(fallback_state, str) and fallback_state.strip() and fallback_state != "All":
        out = [fallback_state.strip()]

    return out


def compute_portfolio_summary_stats(
    display_df: pd.DataFrame,
    *,
    level: str = "district",
) -> dict:
    """
    Compute summary-strip stats for a portfolio comparison table.

    Returns dict with keys:
      - units_count
      - states_count
      - metrics_count
      - risk_counts (dict[str, int])
    """
    level_norm = (level or "district").strip().lower()
    is_block = level_norm == "block"
    is_basin = level_norm == "basin"
    is_subbasin = level_norm == "sub_basin"

    if display_df is None or display_df.empty:
        return {"units_count": 0, "states_count": 0, "metrics_count": 0, "risk_counts": {}}

    state_col = "State" if "State" in display_df.columns else None
    dist_col = "District" if "District" in display_df.columns else None
    blk_col = "Block" if is_block and "Block" in display_df.columns else None
    basin_col = "Basin" if ("Basin" in display_df.columns) else None
    subbasin_col = "Sub-basin" if is_subbasin and ("Sub-basin" in display_df.columns) else None
    idx_col = "Index" if "Index" in display_df.columns else None
    risk_col = "Risk class" if "Risk class" in display_df.columns else None

    if is_subbasin:
        unit_cols = [c for c in (basin_col, subbasin_col) if c]
    elif is_basin:
        unit_cols = [c for c in (basin_col,) if c]
    else:
        unit_cols = [c for c in (state_col, dist_col, blk_col) if c]
    units_count = int(display_df[unit_cols].drop_duplicates().shape[0]) if unit_cols else 0
    if basin_col and not state_col:
        states_count = int(display_df[basin_col].dropna().nunique())
    else:
        states_count = int(display_df[state_col].dropna().nunique()) if state_col else 0
    metrics_count = int(display_df[idx_col].dropna().nunique()) if idx_col else 0

    risk_counts: dict[str, int] = {}
    if risk_col:
        s = display_df[risk_col].fillna("Unknown").astype(str)
        vc = s.value_counts(dropna=False).to_dict()
        risk_counts = {str(k): int(v) for k, v in vc.items()}

    return {
        "units_count": units_count,
        "states_count": states_count,
        "metrics_count": metrics_count,
        "risk_counts": risk_counts,
    }

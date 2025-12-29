"""
Portfolio helpers for IRT.

This module centralizes:
- Portfolio state management (PortfolioState class)
- Normalization and keying functions
- Add/remove/contains/clear operations
- Building the multi-index portfolio comparison table

This module is intentionally Streamlit-free: pass `session_state=st.session_state`
from the app layer.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import difflib
import time
from typing import Any, Callable, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


PortfolioItem = Mapping[str, Any]  # expects keys: "state", "district"
NormalizeFn = Callable[[str], str]


# =============================================================================
# PortfolioState Class - Unified State Management
# =============================================================================

class PortfolioState:
    """
    Unified portfolio state manager.
    
    Wraps Streamlit session_state but provides a cleaner API and
    handles all portfolio-related state in one place.
    
    Usage:
        state = PortfolioState(st.session_state, normalize_fn=my_normalize)
        state.add_district("Telangana", "Hyderabad")
        if state.contains_district("Telangana", "Hyderabad"):
            ...
        state.remove_district("Telangana", "Hyderabad")
    """
    
    # Session state keys
    KEY_DISTRICTS = "portfolio_districts"
    KEY_SAVED_POINTS = "point_query_points"  # Legacy key for compatibility
    KEY_SELECTED_INDICES = "portfolio_multiindex_selection"
    KEY_COMPARISON_TABLE = "portfolio_multiindex_df"
    KEY_TABLE_CONTEXT = "portfolio_multiindex_context"
    KEY_LAST_MODIFIED = "portfolio_last_modified"
    KEY_FLASH_MESSAGE = "_portfolio_flash"
    KEY_CONFIRM_CLEAR = "confirm_clear_portfolio"
    
    def __init__(
        self,
        session_state: MutableMapping[str, Any],
        normalize_fn: NormalizeFn,
    ):
        self._ss = session_state
        self._normalize = normalize_fn
        self._ensure_defaults()
    
    def _ensure_defaults(self) -> None:
        """Ensure all keys have default values."""
        self._ss.setdefault(self.KEY_DISTRICTS, [])
        self._ss.setdefault(self.KEY_SAVED_POINTS, [])
        self._ss.setdefault(self.KEY_SELECTED_INDICES, [])
        self._ss.setdefault(self.KEY_COMPARISON_TABLE, None)
        self._ss.setdefault(self.KEY_TABLE_CONTEXT, None)
        self._ss.setdefault(self.KEY_LAST_MODIFIED, 0.0)
        self._ss.setdefault(self.KEY_FLASH_MESSAGE, None)
        self._ss.setdefault(self.KEY_CONFIRM_CLEAR, False)
    
    def _make_key(self, state: str, district: str) -> Tuple[str, str]:
        """Create normalized key for comparison."""
        return (self._normalize(state), self._normalize(district))
    
    def _mark_modified(self) -> None:
        """Mark portfolio as modified (triggers table rebuild)."""
        self._ss[self.KEY_LAST_MODIFIED] = time.time()
        self._ss[self.KEY_COMPARISON_TABLE] = None
    
    # -------------------------
    # District Management
    # -------------------------
    
    @property
    def districts(self) -> List[Dict[str, str]]:
        """Get list of portfolio districts as dicts."""
        items = self._ss.get(self.KEY_DISTRICTS, [])
        return [d for d in items if isinstance(d, dict)]
    
    @property
    def district_count(self) -> int:
        """Number of districts in portfolio."""
        return len(self.districts)
    
    def add_district(self, state: str, district: str) -> bool:
        """Add district to portfolio. Returns True if added."""
        state = str(state).strip()
        district = str(district).strip()
        
        if not state or not district or district == "All":
            return False
        
        if self.contains_district(state, district):
            return False
        
        items = self._ss.get(self.KEY_DISTRICTS, [])
        if not isinstance(items, list):
            items = []
        
        items.append({"state": state, "district": district})
        self._ss[self.KEY_DISTRICTS] = items
        self._mark_modified()
        return True
    
    def remove_district(self, state: str, district: str) -> bool:
        """Remove district from portfolio. Returns True if removed."""
        key = self._make_key(state, district)
        items = self._ss.get(self.KEY_DISTRICTS, [])
        
        new_items = []
        removed = False
        for item in items:
            if not isinstance(item, dict):
                continue
            item_key = self._make_key(item.get("state", ""), item.get("district", ""))
            if item_key == key:
                removed = True
            else:
                new_items.append(item)
        
        self._ss[self.KEY_DISTRICTS] = new_items
        if removed:
            self._mark_modified()
        return removed
    
    def contains_district(self, state: str, district: str) -> bool:
        """Check if district is in portfolio."""
        if not state or not district or district == "All":
            return False
        
        key = self._make_key(state, district)
        for item in self.districts:
            item_key = self._make_key(item.get("state", ""), item.get("district", ""))
            if item_key == key:
                return True
        return False
    
    def clear_districts(self) -> int:
        """Clear all districts. Returns count removed."""
        count = self.district_count
        self._ss[self.KEY_DISTRICTS] = []
        self._mark_modified()
        return count
    
    def toggle_district(self, state: str, district: str) -> bool:
        """Toggle district. Returns True if now in portfolio."""
        if self.contains_district(state, district):
            self.remove_district(state, district)
            return False
        else:
            self.add_district(state, district)
            return True
    
    def get_district_keys(self) -> set:
        """Get set of normalized (state, district) keys."""
        keys = set()
        for item in self.districts:
            keys.add(self._make_key(item.get("state", ""), item.get("district", "")))
        return keys
    
    # -------------------------
    # Saved Points Management  
    # -------------------------
    
    @property
    def saved_points(self) -> List[Dict[str, Any]]:
        """Get list of saved points."""
        points = self._ss.get(self.KEY_SAVED_POINTS, [])
        return [p for p in points if isinstance(p, dict)]
    
    def add_saved_point(self, lat: float, lon: float, label: Optional[str] = None) -> bool:
        """Add a saved point. Returns False if duplicate."""
        points = self.saved_points
        for p in points:
            if abs(p.get("lat", 0) - lat) < 1e-6 and abs(p.get("lon", 0) - lon) < 1e-6:
                return False
        points.append({"lat": lat, "lon": lon, "label": label})
        self._ss[self.KEY_SAVED_POINTS] = points
        return True
    
    def remove_saved_point(self, index: int) -> bool:
        """Remove saved point by index."""
        points = self.saved_points
        if 0 <= index < len(points):
            points.pop(index)
            self._ss[self.KEY_SAVED_POINTS] = points
            return True
        return False
    
    def clear_saved_points(self) -> int:
        """Clear all saved points. Returns count removed."""
        count = len(self.saved_points)
        self._ss[self.KEY_SAVED_POINTS] = []
        return count
    
    # -------------------------
    # Flash Messages
    # -------------------------
    
    def set_flash(self, message: str, level: str = "success") -> None:
        """Set a one-shot flash message."""
        self._ss[self.KEY_FLASH_MESSAGE] = {"message": message, "level": level}
    
    def pop_flash(self) -> Optional[Dict[str, str]]:
        """Get and clear flash message."""
        return self._ss.pop(self.KEY_FLASH_MESSAGE, None)
    
    # -------------------------
    # Comparison Table Cache
    # -------------------------
    
    @property
    def comparison_table(self) -> Any:
        """Get cached comparison table (DataFrame or None)."""
        return self._ss.get(self.KEY_COMPARISON_TABLE)
    
    @comparison_table.setter
    def comparison_table(self, df: Any) -> None:
        self._ss[self.KEY_COMPARISON_TABLE] = df
    
    def needs_table_rebuild(self, context: Dict[str, Any]) -> bool:
        """Check if table needs rebuild given current context."""
        if self.comparison_table is None:
            return True
        if self._ss.get(self.KEY_TABLE_CONTEXT) != context:
            return True
        return False
    
    def set_table_context(self, context: Dict[str, Any]) -> None:
        """Set the table build context for cache invalidation."""
        self._ss[self.KEY_TABLE_CONTEXT] = context


# =============================================================================
# Standalone Functions (for backward compatibility)
# =============================================================================

def portfolio_normalize(text: str, *, alias_fn: Callable[[str], str]) -> str:
    """
    Normalize a state/district name for robust comparison across sources.
    """
    norm = alias_fn(text)
    return str(norm).replace(" ", "")


def portfolio_key(
    state_name: str,
    district_name: str,
    *,
    normalize_fn: NormalizeFn,
) -> tuple[str, str]:
    """Normalized key used to compare/uniquify portfolio items."""
    return (normalize_fn(state_name), normalize_fn(district_name))


def portfolio_add(
    session_state: MutableMapping[str, Any],
    state_name: str,
    district_name: str,
    *,
    normalize_fn: NormalizeFn,
    state_key: str = "portfolio_districts",
) -> None:
    """Add a (state, district) pair to the portfolio if not already present."""
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
    """Remove a (state, district) pair from the portfolio."""
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
            new_items.append({
                "state": str(item.get("state", "")),
                "district": str(item.get("district", ""))
            })

    session_state[state_key] = new_items


def portfolio_contains(
    session_state: Mapping[str, Any],
    state_name: str,
    district_name: str,
    *,
    normalize_fn: NormalizeFn,
    state_key: str = "portfolio_districts",
) -> bool:
    """Return True if (state, district) already exists in portfolio_districts."""
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
    """Clear all portfolio districts."""
    session_state[state_key] = []


def get_portfolio_district_keys(
    session_state: Mapping[str, Any],
    normalize_fn: NormalizeFn,
    state_key: str = "portfolio_districts",
) -> set:
    """
    Get normalized keys for all districts in the portfolio.
    Returns a set of (normalized_state, normalized_district) tuples.
    """
    portfolio = session_state.get(state_key, [])
    keys = set()
    
    for item in portfolio:
        if isinstance(item, dict):
            state = str(item.get("state", "")).strip()
            district = str(item.get("district", "")).strip()
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            state, district = str(item[0]).strip(), str(item[1]).strip()
        else:
            continue
        
        if state and district:
            keys.add((normalize_fn(state), normalize_fn(district)))
    
    return keys


# =============================================================================
# Multi-Index Comparison Table Builder
# =============================================================================

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
    Build the portfolio multi-index comparison table.
    """
    rows_out: list[dict[str, Any]] = []

    for item in portfolio:
        if isinstance(item, Mapping):
            st_name = str(item.get("state", "")).strip()
            dist_name = str(item.get("district", "")).strip()
        else:
            try:
                st_name, dist_name = item
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

            # Fuzzy fallback
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

            rows_out.append({
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
            })

    return pd.DataFrame(rows_out)
# india_resilience_tool/analysis/portfolio.py
"""
Portfolio helpers for IRT.

This module centralizes:
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
from typing import Any, Callable, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# -----------------------------------------------------------------------------
# Session-state storage keys (kept as module-level constants for reuse)
# -----------------------------------------------------------------------------
KEY_DISTRICTS = "portfolio_districts"
KEY_BLOCKS = "portfolio_blocks"
KEY_BASINS = "portfolio_basins"
KEY_SUBBASINS = "portfolio_subbasins"
KEY_SAVED_POINTS = "point_query_points"  # legacy key used elsewhere


PortfolioItem = Mapping[str, Any]
NormalizeFn = Callable[[str], str]


def get_portfolio_storage_key(level: str) -> str:
    """
    Resolve the session_state list key used to store portfolio items.

    Args:
        level: `district`, `block`, `basin`, or `sub_basin`.

    Returns:
        Session-state key string, e.g. `portfolio_districts` or `portfolio_subbasins`.
    """
    level_norm = str(level).strip().lower()
    if level_norm == "block":
        return KEY_BLOCKS
    if level_norm == "basin":
        return KEY_BASINS
    if level_norm == "sub_basin":
        return KEY_SUBBASINS
    return KEY_DISTRICTS


# =============================================================================
# Standalone Functions (for backward compatibility)
# =============================================================================

def portfolio_normalize(text: str, *, alias_fn: Callable[[str], str]) -> str:
    """
    Normalize a state/district/block name for robust comparison across sources.
    """
    norm = alias_fn(text)
    return str(norm).replace(" ", "")


def portfolio_key(
    state_name: str,
    district_name: str,
    *,
    normalize_fn: NormalizeFn,
) -> tuple[str, str]:
    """Normalized key used to compare/uniquify district portfolio items."""
    return (normalize_fn(state_name), normalize_fn(district_name))


def portfolio_key_block(
    state_name: str,
    district_name: str,
    block_name: str,
    *,
    normalize_fn: NormalizeFn,
) -> tuple[str, str, str]:
    """Normalized key used to compare/uniquify block portfolio items."""
    return (normalize_fn(state_name), normalize_fn(district_name), normalize_fn(block_name))


def portfolio_key_basin(
    basin_name: str,
    *,
    basin_id: Optional[str],
    normalize_fn: NormalizeFn,
) -> tuple[str, str]:
    """Normalized key used to compare/uniquify basin portfolio items."""
    return (normalize_fn(basin_id or ""), normalize_fn(basin_name))


def portfolio_key_subbasin(
    basin_name: str,
    subbasin_name: str,
    *,
    basin_id: Optional[str],
    subbasin_id: Optional[str],
    normalize_fn: NormalizeFn,
) -> tuple[str, str, str, str]:
    """Normalized key used to compare/uniquify sub-basin portfolio items."""
    return (
        normalize_fn(basin_id or ""),
        normalize_fn(basin_name),
        normalize_fn(subbasin_id or ""),
        normalize_fn(subbasin_name),
    )


def _basin_matches(
    *,
    item_basin_name: str,
    item_basin_id: str,
    basin_name: str,
    basin_id: Optional[str],
    normalize_fn: NormalizeFn,
) -> bool:
    item_key = portfolio_key_basin(item_basin_name, basin_id=item_basin_id, normalize_fn=normalize_fn)
    query_key = portfolio_key_basin(basin_name, basin_id=basin_id, normalize_fn=normalize_fn)
    if item_key[0] and query_key[0]:
        return item_key[0] == query_key[0]
    return bool(item_key[1] and query_key[1] and item_key[1] == query_key[1])


def _subbasin_matches(
    *,
    item_basin_name: str,
    item_basin_id: str,
    item_subbasin_name: str,
    item_subbasin_id: str,
    basin_name: str,
    basin_id: Optional[str],
    subbasin_name: str,
    subbasin_id: Optional[str],
    normalize_fn: NormalizeFn,
) -> bool:
    item_key = portfolio_key_subbasin(
        item_basin_name,
        item_subbasin_name,
        basin_id=item_basin_id,
        subbasin_id=item_subbasin_id,
        normalize_fn=normalize_fn,
    )
    query_key = portfolio_key_subbasin(
        basin_name,
        subbasin_name,
        basin_id=basin_id,
        subbasin_id=subbasin_id,
        normalize_fn=normalize_fn,
    )
    basin_match = (
        item_key[0] == query_key[0]
        if item_key[0] and query_key[0]
        else bool(item_key[1] and query_key[1] and item_key[1] == query_key[1])
    )
    subbasin_match = (
        item_key[2] == query_key[2]
        if item_key[2] and query_key[2]
        else bool(item_key[3] and query_key[3] and item_key[3] == query_key[3])
    )
    return basin_match and subbasin_match


def portfolio_add(
    session_state: MutableMapping[str, Any],
    state_name: str = "",
    district_name: str = "",
    *,
    normalize_fn: NormalizeFn,
    block_name: Optional[str] = None,
    basin_name: Optional[str] = None,
    basin_id: Optional[str] = None,
    subbasin_name: Optional[str] = None,
    subbasin_id: Optional[str] = None,
    level: str = "district",
    state_key: Optional[str] = None,
) -> None:
    """
    Add a portfolio item if not already present.

    Backward compatible:
        portfolio_add(ss, state, district, normalize_fn=...)

    For blocks:
        portfolio_add(ss, state, district, block_name=block, level="block", normalize_fn=...)
    """
    level_norm = str(level).strip().lower()
    storage_key = state_key or get_portfolio_storage_key(level_norm)

    if level_norm == "basin":
        basin_name_clean = str(basin_name or "").strip()
        if not basin_name_clean or basin_name_clean == "All":
            return
    elif level_norm == "sub_basin":
        basin_name_clean = str(basin_name or "").strip()
        subbasin_name_clean = str(subbasin_name or "").strip()
        if (
            not basin_name_clean
            or basin_name_clean == "All"
            or not subbasin_name_clean
            or subbasin_name_clean == "All"
        ):
            return
    else:
        if not state_name or not district_name or district_name == "All":
            return
        if level_norm == "block":
            if not block_name or str(block_name).strip() == "" or str(block_name).strip() == "All":
                return

    items = session_state.get(storage_key, [])
    if not isinstance(items, list):
        items = []

    if level_norm == "basin":
        for item in items:
            if not isinstance(item, Mapping):
                continue
            if _basin_matches(
                item_basin_name=str(item.get("basin_name", "")),
                item_basin_id=str(item.get("basin_id", "")),
                basin_name=basin_name_clean,
                basin_id=basin_id,
                normalize_fn=normalize_fn,
            ):
                return
        items.append({"basin_id": str(basin_id or ""), "basin_name": basin_name_clean})
        session_state[storage_key] = items
        return

    if level_norm == "sub_basin":
        for item in items:
            if not isinstance(item, Mapping):
                continue
            if _subbasin_matches(
                item_basin_name=str(item.get("basin_name", "")),
                item_basin_id=str(item.get("basin_id", "")),
                item_subbasin_name=str(item.get("subbasin_name", "")),
                item_subbasin_id=str(item.get("subbasin_id", "")),
                basin_name=basin_name_clean,
                basin_id=basin_id,
                subbasin_name=subbasin_name_clean,
                subbasin_id=subbasin_id,
                normalize_fn=normalize_fn,
            ):
                return
        items.append(
            {
                "basin_id": str(basin_id or ""),
                "basin_name": basin_name_clean,
                "subbasin_id": str(subbasin_id or ""),
                "subbasin_name": subbasin_name_clean,
            }
        )
        session_state[storage_key] = items
        return

    if level_norm == "block":
        new_norm = portfolio_key_block(state_name, district_name, str(block_name), normalize_fn=normalize_fn)
        for item in items:
            if not isinstance(item, Mapping):
                continue
            if portfolio_key_block(
                str(item.get("state", "")),
                str(item.get("district", "")),
                str(item.get("block", "")),
                normalize_fn=normalize_fn,
            ) == new_norm:
                return
        items.append({"state": state_name, "district": district_name, "block": str(block_name)})
        session_state[storage_key] = items
        return

    # district
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
    session_state[storage_key] = items


def portfolio_remove(
    session_state: MutableMapping[str, Any],
    state_name: str = "",
    district_name: str = "",
    *,
    normalize_fn: NormalizeFn,
    block_name: Optional[str] = None,
    basin_name: Optional[str] = None,
    basin_id: Optional[str] = None,
    subbasin_name: Optional[str] = None,
    subbasin_id: Optional[str] = None,
    level: str = "district",
    state_key: Optional[str] = None,
) -> None:
    """
    Remove a portfolio item.

    Backward compatible:
        portfolio_remove(ss, state, district, normalize_fn=...)

    For blocks:
        portfolio_remove(ss, state, district, block_name=block, level="block", normalize_fn=...)
    """
    level_norm = str(level).strip().lower()
    storage_key = state_key or get_portfolio_storage_key(level_norm)

    items = session_state.get(storage_key, [])
    if not isinstance(items, list):
        session_state[storage_key] = []
        return

    if level_norm == "basin":
        basin_name_clean = str(basin_name or "").strip()
        if not basin_name_clean:
            return
        new_items_basin: list[dict[str, str]] = []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            if not _basin_matches(
                item_basin_name=str(item.get("basin_name", "")),
                item_basin_id=str(item.get("basin_id", "")),
                basin_name=basin_name_clean,
                basin_id=basin_id,
                normalize_fn=normalize_fn,
            ):
                new_items_basin.append(
                    {
                        "basin_id": str(item.get("basin_id", "")),
                        "basin_name": str(item.get("basin_name", "")),
                    }
                )
        session_state[storage_key] = new_items_basin
        return

    if level_norm == "sub_basin":
        basin_name_clean = str(basin_name or "").strip()
        subbasin_name_clean = str(subbasin_name or "").strip()
        if not basin_name_clean or not subbasin_name_clean:
            return
        new_items_subbasin: list[dict[str, str]] = []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            if not _subbasin_matches(
                item_basin_name=str(item.get("basin_name", "")),
                item_basin_id=str(item.get("basin_id", "")),
                item_subbasin_name=str(item.get("subbasin_name", "")),
                item_subbasin_id=str(item.get("subbasin_id", "")),
                basin_name=basin_name_clean,
                basin_id=basin_id,
                subbasin_name=subbasin_name_clean,
                subbasin_id=subbasin_id,
                normalize_fn=normalize_fn,
            ):
                new_items_subbasin.append(
                    {
                        "basin_id": str(item.get("basin_id", "")),
                        "basin_name": str(item.get("basin_name", "")),
                        "subbasin_id": str(item.get("subbasin_id", "")),
                        "subbasin_name": str(item.get("subbasin_name", "")),
                    }
                )
        session_state[storage_key] = new_items_subbasin
        return

    if level_norm == "block":
        if not block_name:
            return
        norm = portfolio_key_block(state_name, district_name, str(block_name), normalize_fn=normalize_fn)
        new_items: list[dict[str, str]] = []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            if portfolio_key_block(
                str(item.get("state", "")),
                str(item.get("district", "")),
                str(item.get("block", "")),
                normalize_fn=normalize_fn,
            ) != norm:
                new_items.append(
                    {
                        "state": str(item.get("state", "")),
                        "district": str(item.get("district", "")),
                        "block": str(item.get("block", "")),
                    }
                )
        session_state[storage_key] = new_items
        return

    # district
    norm = portfolio_key(state_name, district_name, normalize_fn=normalize_fn)
    new_items2: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        if portfolio_key(
            str(item.get("state", "")),
            str(item.get("district", "")),
            normalize_fn=normalize_fn,
        ) != norm:
            new_items2.append(
                {
                    "state": str(item.get("state", "")),
                    "district": str(item.get("district", "")),
                }
            )
    session_state[storage_key] = new_items2


def portfolio_contains(
    session_state: Mapping[str, Any],
    state_name: str = "",
    district_name: str = "",
    *,
    normalize_fn: NormalizeFn,
    block_name: Optional[str] = None,
    basin_name: Optional[str] = None,
    basin_id: Optional[str] = None,
    subbasin_name: Optional[str] = None,
    subbasin_id: Optional[str] = None,
    level: str = "district",
    state_key: Optional[str] = None,
) -> bool:
    """
    Return True if item exists in portfolio.

    Backward compatible:
        portfolio_contains(ss, state, district, normalize_fn=...)

    For blocks:
        portfolio_contains(ss, state, district, block_name=block, level="block", normalize_fn=...)
    """
    level_norm = str(level).strip().lower()
    storage_key = state_key or get_portfolio_storage_key(level_norm)

    if level_norm == "basin":
        if not basin_name or str(basin_name).strip() in {"", "All"}:
            return False
    elif level_norm == "sub_basin":
        if (
            not basin_name
            or str(basin_name).strip() in {"", "All"}
            or not subbasin_name
            or str(subbasin_name).strip() in {"", "All"}
        ):
            return False
    else:
        if not state_name or not district_name or district_name == "All":
            return False
        if level_norm == "block":
            if not block_name or str(block_name).strip() == "" or str(block_name).strip() == "All":
                return False

    items = session_state.get(storage_key, [])
    if not isinstance(items, list):
        return False

    if level_norm == "basin":
        for item in items:
            if not isinstance(item, Mapping):
                continue
            if _basin_matches(
                item_basin_name=str(item.get("basin_name", "")),
                item_basin_id=str(item.get("basin_id", "")),
                basin_name=str(basin_name or ""),
                basin_id=basin_id,
                normalize_fn=normalize_fn,
            ):
                return True
        return False

    if level_norm == "sub_basin":
        for item in items:
            if not isinstance(item, Mapping):
                continue
            if _subbasin_matches(
                item_basin_name=str(item.get("basin_name", "")),
                item_basin_id=str(item.get("basin_id", "")),
                item_subbasin_name=str(item.get("subbasin_name", "")),
                item_subbasin_id=str(item.get("subbasin_id", "")),
                basin_name=str(basin_name or ""),
                basin_id=basin_id,
                subbasin_name=str(subbasin_name or ""),
                subbasin_id=subbasin_id,
                normalize_fn=normalize_fn,
            ):
                return True
        return False

    if level_norm == "block":
        norm = portfolio_key_block(state_name, district_name, str(block_name), normalize_fn=normalize_fn)
        for item in items:
            if not isinstance(item, Mapping):
                continue
            if portfolio_key_block(
                str(item.get("state", "")),
                str(item.get("district", "")),
                str(item.get("block", "")),
                normalize_fn=normalize_fn,
            ) == norm:
                return True
        return False

    norm2 = portfolio_key(state_name, district_name, normalize_fn=normalize_fn)
    for item in items:
        if not isinstance(item, Mapping):
            continue
        if portfolio_key(
            str(item.get("state", "")),
            str(item.get("district", "")),
            normalize_fn=normalize_fn,
        ) == norm2:
            return True
    return False


def portfolio_clear(
    session_state: MutableMapping[str, Any],
    *,
    level: str = "district",
    state_key: Optional[str] = None,
) -> None:
    """Clear all portfolio items for the given level."""
    storage_key = state_key or get_portfolio_storage_key(level)
    session_state[storage_key] = []


def get_portfolio_unit_keys(
    session_state: Mapping[str, Any],
    normalize_fn: NormalizeFn,
    *,
    level: str = "district",
    state_key: Optional[str] = None,
) -> set:
    """
    Get normalized keys for all items in the portfolio.

    Returns:
        - level="district": set of (normalized_state, normalized_district)
        - level="block": set of (normalized_state, normalized_district, normalized_block)
        - level="basin": set of (normalized_basin_id, normalized_basin_name)
        - level="sub_basin": set of (normalized_basin_id, normalized_basin_name, normalized_subbasin_id, normalized_subbasin_name)
    """
    level_norm = str(level).strip().lower()
    storage_key = state_key or get_portfolio_storage_key(level_norm)

    portfolio = session_state.get(storage_key, [])
    keys: set = set()

    for item in portfolio:
        if isinstance(item, Mapping):
            state = str(item.get("state", "")).strip()
            district = str(item.get("district", "")).strip()
            block = str(item.get("block", "")).strip() if level_norm == "block" else ""
            basin = str(item.get("basin_name", "")).strip()
            basin_id = str(item.get("basin_id", "")).strip()
            subbasin = str(item.get("subbasin_name", "")).strip() if level_norm == "sub_basin" else ""
            subbasin_id = str(item.get("subbasin_id", "")).strip() if level_norm == "sub_basin" else ""
        elif isinstance(item, (list, tuple)):
            if level_norm == "sub_basin" and len(item) >= 4:
                basin_id, basin, subbasin_id, subbasin = (
                    str(item[0]).strip(),
                    str(item[1]).strip(),
                    str(item[2]).strip(),
                    str(item[3]).strip(),
                )
                state = district = block = ""
            elif level_norm == "basin" and len(item) >= 2:
                basin_id, basin = str(item[0]).strip(), str(item[1]).strip()
                state = district = block = subbasin = subbasin_id = ""
            elif level_norm == "block" and len(item) >= 3:
                state, district, block = str(item[0]).strip(), str(item[1]).strip(), str(item[2]).strip()
                basin = basin_id = subbasin = subbasin_id = ""
            elif level_norm != "block" and len(item) >= 2:
                state, district = str(item[0]).strip(), str(item[1]).strip()
                block = basin = basin_id = subbasin = subbasin_id = ""
            else:
                continue
        else:
            continue

        if level_norm == "basin":
            if not basin and not basin_id:
                continue
            keys.add(portfolio_key_basin(basin, basin_id=basin_id, normalize_fn=normalize_fn))
            continue

        if level_norm == "sub_basin":
            if not basin and not basin_id:
                continue
            if not subbasin and not subbasin_id:
                continue
            keys.add(
                portfolio_key_subbasin(
                    basin,
                    subbasin,
                    basin_id=basin_id,
                    subbasin_id=subbasin_id,
                    normalize_fn=normalize_fn,
                )
            )
            continue

        if not state or not district:
            continue

        if level_norm == "block":
            if not block:
                continue
            keys.add((normalize_fn(state), normalize_fn(district), normalize_fn(block)))
        else:
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
    match_row_idx: Callable[..., Optional[int]],
    compute_rank_and_percentile: Callable[..., tuple[Optional[int], Optional[float]]],
    risk_class_from_percentile: Callable[[float], str],
    normalize_fn: NormalizeFn,
    sel_scenarios: Optional[Sequence[str]] = None,
    level: str = "district",
) -> pd.DataFrame:
    """
    Build the portfolio multi-index comparison table.

    Notes:
        - For level="district", items are (state, district).
        - For level="block", items are (state, district, block).
        - For level="basin", items are keyed by basin.
        - For level="sub_basin", items are keyed by basin + sub-basin.

    Scenario comparison:
        - When ``sel_scenarios`` is provided (e.g., ["ssp245", "ssp585"]),
          the output includes a ``Scenario`` column and contains one row per
          (unit × index × scenario).
        - Baseline is always computed from the *historical* baseline column
          via ``find_baseline_column_for_stat``. This means Δ and %Δ are
          *scenario values vs historical baseline*.

    Missing/invalid data behavior:
        - If a unit row cannot be matched, or a required metric column is absent,
          numeric outputs (Current value/Baseline/Δ/%Δ/Percentile) are NaN and
          Rank/Risk class are left as (None/"Unknown").

    The row match and ranking functions are injected (dependency injection).
    For block mode, we attempt to call the injected functions with block-aware
    arguments and fall back to the older district-only signatures if needed.
    """
    level_norm = str(level).strip().lower()
    rows_out: list[dict[str, Any]] = []

    # Determine scenario list (single by default)
    if sel_scenarios is None:
        scenario_list = [str(sel_scenario).strip()]
        include_scenario_col = False
    else:
        scenario_list = [str(s).strip() for s in sel_scenarios if str(s).strip()]
        if not scenario_list:
            scenario_list = [str(sel_scenario).strip()]
        include_scenario_col = True

    # Cache per-slug master loads (the loader may already cache; this avoids re-calls)
    master_cache: dict[str, tuple[pd.DataFrame, list[str]]] = {}

    def _parse_item(item: Any) -> tuple[str, str, str, str, str, str, str]:
        blk = ""
        basin = ""
        basin_id = ""
        subbasin = ""
        subbasin_id = ""
        if isinstance(item, Mapping):
            st = str(item.get("state", "")).strip()
            dist = str(item.get("district", "")).strip()
            if level_norm == "block":
                blk = str(item.get("block", "")).strip()
            elif level_norm == "basin":
                basin = str(item.get("basin_name", "")).strip()
                basin_id = str(item.get("basin_id", "")).strip()
            elif level_norm == "sub_basin":
                basin = str(item.get("basin_name", "")).strip()
                basin_id = str(item.get("basin_id", "")).strip()
                subbasin = str(item.get("subbasin_name", "")).strip()
                subbasin_id = str(item.get("subbasin_id", "")).strip()
            return st, dist, blk, basin, basin_id, subbasin, subbasin_id

        try:
            tup = tuple(item)
        except Exception:
            return "", "", "", "", "", "", ""

        if level_norm == "sub_basin":
            if len(tup) < 4:
                return "", "", "", "", "", "", ""
            return (
                "",
                "",
                "",
                str(tup[1]).strip(),
                str(tup[0]).strip(),
                str(tup[3]).strip(),
                str(tup[2]).strip(),
            )

        if level_norm == "basin":
            if len(tup) < 2:
                return "", "", "", "", "", "", ""
            return "", "", "", str(tup[1]).strip(), str(tup[0]).strip(), "", ""

        if level_norm == "block":
            if len(tup) < 3:
                return "", "", "", "", "", "", ""
            st, dist, blk = tup[0], tup[1], tup[2]
            return str(st).strip(), str(dist).strip(), str(blk).strip(), "", "", "", ""

        if len(tup) < 2:
            return "", "", "", "", "", "", ""
        st, dist = tup[0], tup[1]
        return str(st).strip(), str(dist).strip(), "", "", "", "", ""

    for item in portfolio:
        st_name, dist_name, blk_name, basin_name, basin_id, subbasin_name, subbasin_id = _parse_item(item)

        for slug in selected_slugs:
            cfg = variables.get(slug, {})
            idx_label = cfg.get("label", str(slug))
            idx_group = cfg.get("group", "other")
            idx_group_label = index_group_labels.get(idx_group, str(idx_group).title())
            registry_metric_i = str(cfg.get("periods_metric_col", "")).strip()

            # Load master only once per slug
            slug_s = str(slug)
            if slug_s not in master_cache:
                df_local, _, metrics_local, _ = load_master_and_schema_for_slug(slug_s)
                master_cache[slug_s] = (df_local, list(metrics_local or []))
            df_local, metrics_local = master_cache[slug_s]

            # Determine the base metric name (used to resolve scenario/period columns)
            used_metric = registry_metric_i
            if not used_metric and metrics_local:
                used_metric = str(metrics_local[0])

            # Fuzzy fallback for the base metric name (handles minor naming differences)
            if used_metric and metrics_local and used_metric not in metrics_local:
                base_norm = normalize_fn(used_metric)
                candidates = [m for m in metrics_local if base_norm and base_norm in normalize_fn(m)]
                if not candidates:
                    candidates = difflib.get_close_matches(used_metric, metrics_local, n=1, cutoff=0.6)
                if candidates:
                    used_metric = str(candidates[0])

            # Resolve baseline column once (independent of scenario)
            baseline_col = None
            if used_metric and df_local is not None and not df_local.empty:
                baseline_col = find_baseline_column_for_stat(df_local.columns, used_metric, sel_stat)

            # Match the unit row once per (unit × slug)
            idx_row = None
            if df_local is not None and not df_local.empty:
                if level_norm == "sub_basin":
                    try:
                        idx_row = match_row_idx(df_local, basin_name, subbasin_name, basin_id, subbasin_id)
                    except TypeError:
                        idx_row = match_row_idx(df_local, basin_name, subbasin_name)
                elif level_norm == "basin":
                    try:
                        idx_row = match_row_idx(df_local, basin_name, basin_id)
                    except TypeError:
                        idx_row = match_row_idx(df_local, basin_name)
                elif level_norm == "block":
                    try:
                        idx_row = match_row_idx(df_local, st_name, dist_name, blk_name)
                    except TypeError:
                        idx_row = match_row_idx(df_local, st_name, dist_name)
                else:
                    idx_row = match_row_idx(df_local, st_name, dist_name)

            # Baseline value once per (unit × slug)
            baseline = float("nan")
            if idx_row is not None and baseline_col and baseline_col in df_local.columns:
                try:
                    baseline = float(pd.to_numeric(df_local.loc[idx_row, baseline_col], errors="coerce"))
                except Exception:
                    baseline = float("nan")

            for scen in scenario_list:
                # Defaults
                value = float("nan")
                delta_abs = float("nan")
                delta_pct = float("nan")
                rank_in_state: Optional[int] = None
                percentile_in_state = float("nan")
                risk_class = "Unknown"

                metric_col = None
                if used_metric:
                    metric_col = resolve_metric_column(df_local, used_metric, scen, sel_period, sel_stat)

                if (
                    df_local is not None
                    and not df_local.empty
                    and idx_row is not None
                    and metric_col
                    and metric_col in df_local.columns
                ):
                    try:
                        value = float(pd.to_numeric(df_local.loc[idx_row, metric_col], errors="coerce"))
                    except Exception:
                        value = float("nan")

                    if not pd.isna(value) and not pd.isna(baseline):
                        delta_abs = value - baseline
                        if baseline != 0:
                            delta_pct = (delta_abs / baseline) * 100.0

                    # Rank/percentile: try block-aware kwargs, then fall back
                    try:
                        kwargs: dict[str, Any] = {
                            "df_local": df_local,
                            "st_name": st_name if level_norm in {"district", "block"} else basin_name,
                            "metric_col": metric_col,
                            "value": value,
                        }
                        if level_norm == "block":
                            kwargs["district_name"] = dist_name
                            kwargs["block_name"] = blk_name
                        elif level_norm == "basin":
                            kwargs["basin_name"] = basin_name
                            kwargs["basin_id"] = basin_id
                        elif level_norm == "sub_basin":
                            kwargs["basin_name"] = basin_name
                            kwargs["basin_id"] = basin_id
                            kwargs["subbasin_name"] = subbasin_name
                            kwargs["subbasin_id"] = subbasin_id
                        rank_in_state, percentile = compute_rank_and_percentile(**kwargs)
                    except TypeError:
                        rank_in_state, percentile = compute_rank_and_percentile(
                            df_local=df_local,
                            st_name=st_name if level_norm in {"district", "block"} else basin_name,
                            metric_col=metric_col,
                            value=value,
                        )

                    if percentile is not None:
                        percentile_in_state = float(percentile)
                        risk_class = risk_class_from_percentile(percentile_in_state)

                row: dict[str, Any] = {
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
                if level_norm == "sub_basin":
                    row["Basin"] = basin_name
                    row["Sub-basin"] = subbasin_name
                elif level_norm == "basin":
                    row["Basin"] = basin_name
                else:
                    row["State"] = st_name
                    row["District"] = dist_name
                if level_norm == "block":
                    row["Block"] = blk_name
                if include_scenario_col:
                    row["Scenario"] = scen

                rows_out.append(row)

    return pd.DataFrame(rows_out)

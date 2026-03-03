"""
Multi-district portfolio panel (right column) for IRT.

This module renders the portfolio-mode right-column UI including:
- Portfolio summary badge (always visible)
- Portfolio district list with remove buttons
- Bundle-first index selection for comparison (NEW)
- Multi-index comparison table (auto-rebuilds)
- Coordinate-based district lookup

Key improvements over previous version:
- Removed mandatory route selection (rankings/map/saved_points)
- All add methods available simultaneously
- Auto-rebuild comparison table on changes
- Portfolio list always visible
- Simplified point selection integrated
- Bundle-first metric selection with optional manual refinement (NEW)

Widget keys preserved:
- portfolio_multiindex_selection
- portfolio_bundle_selection (NEW)
- portfolio_manual_refinement (NEW)
- btn_portfolio_remove_all
- btn_portfolio_remove_all_confirm
- btn_portfolio_remove_all_cancel
- btn_portfolio_remove_{state}_{district}

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import re

import html
from pathlib import Path
from typing import Any, Callable, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd


# =============================================================================
# Portfolio Badge & Summary
# =============================================================================

def render_portfolio_badge(portfolio_count: int, level: str = "district") -> None:
    """
    Render a compact portfolio summary badge.

    Args:
        portfolio_count: Number of units in the portfolio.
        level: "district" or "block".
    """
    import streamlit as st

    level_norm = (level or "district").strip().lower()
    unit_singular = "block" if level_norm == "block" else "district"
    unit_plural = "blocks" if level_norm == "block" else "districts"

    if portfolio_count == 0:
        st.markdown(
            f"""<div style="padding: 8px 12px; background: #f0f2f6; border-radius: 8px;
            text-align: center; color: #666;">
            <span style="font-size: 1.1em;">Portfolio</span>
            <strong>Portfolio empty</strong> — Click {unit_plural} to add
            </div>""",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"""<div style="padding: 8px 12px; background: #e8f4e8; border-radius: 8px;
            text-align: center; color: #2d5a2d;">
            <span style="font-size: 1.1em;">Portfolio</span>
            <strong>{portfolio_count} {unit_singular}{'s' if portfolio_count != 1 else ''}</strong> in portfolio
            </div>""",
            unsafe_allow_html=True,
        )

# =============================================================================
# Portfolio District List
# =============================================================================

def _portfolio_items_to_dicts(portfolio: Sequence[Any], *, is_block: bool) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for d in portfolio:
        if isinstance(d, dict):
            items.append(
                {
                    "state": str(d.get("state", "")).strip(),
                    "district": str(d.get("district", "")).strip(),
                    "block": str(d.get("block", "")).strip(),
                }
            )
        elif isinstance(d, (list, tuple)):
            if len(d) >= 3:
                items.append({"state": str(d[0]).strip(), "district": str(d[1]).strip(), "block": str(d[2]).strip()})
            elif len(d) >= 2:
                items.append({"state": str(d[0]).strip(), "district": str(d[1]).strip(), "block": ""})

    items = [x for x in items if x.get("state") and x.get("district") and (not is_block or x.get("block"))]
    return items


def _filter_portfolio_items(
    items: Sequence[dict[str, str]],
    *,
    query: str,
    normalize_fn: Callable[[str], str],
) -> list[dict[str, str]]:
    qn = normalize_fn(str(query))
    if not qn:
        return list(items)

    def _hit(it: dict[str, str]) -> bool:
        base = " ".join([it.get("state", ""), it.get("district", ""), it.get("block", "")])
        return qn in normalize_fn(base)

    return [it for it in items if _hit(it)]


def _group_portfolio_items_by_state(
    items: Sequence[dict[str, str]],
    *,
    normalize_fn: Callable[[str], str],
) -> dict[str, list[dict[str, str]]]:
    by_state: dict[str, list[dict[str, str]]] = {}
    for it in items:
        by_state.setdefault(it["state"], []).append(it)

    for st_name, lst in by_state.items():
        lst.sort(key=lambda r: (normalize_fn(r.get("district", "")), normalize_fn(r.get("block", ""))))

    return dict(sorted(by_state.items(), key=lambda kv: normalize_fn(kv[0])))


def _group_portfolio_items_by_state_and_district(
    items: Sequence[dict[str, str]],
    *,
    normalize_fn: Callable[[str], str],
) -> dict[str, dict[str, list[dict[str, str]]]]:
    """
    Group portfolio items into a nested State -> District -> [Items] mapping.

    Sorting:
      - States by normalize(state)
      - Districts by normalize(district)
      - Items (blocks) by normalize(block)
    """
    by_state: dict[str, dict[str, list[dict[str, str]]]] = {}
    for it in items:
        st_name = it.get("state", "")
        dist_name = it.get("district", "")
        by_state.setdefault(st_name, {}).setdefault(dist_name, []).append(it)

    # Sort blocks within each district; sort districts and states
    out: dict[str, dict[str, list[dict[str, str]]]] = {}
    for st_name, by_district in by_state.items():
        dist_sorted: dict[str, list[dict[str, str]]] = {}
        for dist_name, lst in by_district.items():
            lst_sorted = sorted(lst, key=lambda r: normalize_fn(r.get("block", "")))
            dist_sorted[dist_name] = lst_sorted
        out[st_name] = dict(sorted(dist_sorted.items(), key=lambda kv: normalize_fn(kv[0])))

    return dict(sorted(out.items(), key=lambda kv: normalize_fn(kv[0])))


def render_portfolio_list(
    *,
    portfolio: Sequence[Any],
    portfolio_remove_fn: Callable[..., None],
    normalize_fn: Callable[[str], str],
    max_visible: int = 8,
    level: str = "district",
) -> None:
    """
    Render the portfolio unit list (districts or blocks) with remove buttons.
    """
    import streamlit as st

    level_norm = (level or "district").strip().lower()
    is_block = level_norm == "block"

    if not portfolio:
        st.caption(
            "No blocks selected yet. Add blocks from the map, rankings table, or by coordinates."
            if is_block
            else "No districts selected yet. Add districts from the map, rankings table, or by coordinates."
        )
        return

    items_all = _portfolio_items_to_dicts(portfolio, is_block=is_block)

    # Search (kept within the manage panel for large portfolios)
    search_key = f"portfolio_manage_search_{level_norm}"
    q = st.text_input(
        "Search",
        value=str(st.session_state.get(search_key, "")),
        key=search_key,
        placeholder="Type to filter…",
    )
    items = _filter_portfolio_items(items_all, query=q, normalize_fn=normalize_fn)
    if not items:
        st.caption("No matching items. Clear search to see all.")
        return

    total_items = len(items)

    show_all = st.session_state.get("_portfolio_show_all", False) or total_items <= max_visible
    remaining_budget = total_items if show_all else max_visible

    def _render_indented_leaf(label: str, *, padding_px: int) -> None:
        safe = html.escape(str(label))
        st.markdown(
            f"<div style=\"padding-left: {int(padding_px)}px;\"><strong>{safe}</strong></div>",
            unsafe_allow_html=True,
        )

    def _render_remove_all_in_state(st_name: str, *, state_items_all: Sequence[dict[str, str]]) -> None:
        # Optional: remove all in state (confirm-inline)
        confirm_key = f"confirm_remove_state_{level_norm}_{normalize_fn(st_name)}"
        confirm = bool(st.session_state.get(confirm_key, False))
        c1, c2 = st.columns([3, 2])
        with c1:
            pass
        with c2:
            if not confirm:
                if st.button(
                    "Remove all in state",
                    key=f"btn_remove_state_{level_norm}_{normalize_fn(st_name)}",
                    type="secondary",
                ):
                    st.session_state[confirm_key] = True
                    st.rerun()
            else:
                st.warning("Remove all?")
                y, n = st.columns(2)
                with y:
                    if st.button(
                        "Yes",
                        key=f"btn_remove_state_yes_{level_norm}_{normalize_fn(st_name)}",
                        type="primary",
                    ):
                        for it in list(state_items_all):
                            try:
                                if is_block:
                                    portfolio_remove_fn(it["state"], it["district"], it["block"])
                                else:
                                    portfolio_remove_fn(it["state"], it["district"])
                            except TypeError:
                                portfolio_remove_fn(it["state"], it["district"])
                        st.session_state[confirm_key] = False
                        st.rerun()
                with n:
                    if st.button("No", key=f"btn_remove_state_no_{level_norm}_{normalize_fn(st_name)}"):
                        st.session_state[confirm_key] = False
                        st.rerun()

    if is_block:
        # Block mode: State -> District -> Blocks
        tree_all = _group_portfolio_items_by_state_and_district(items_all, normalize_fn=normalize_fn)
        tree = _group_portfolio_items_by_state_and_district(items, normalize_fn=normalize_fn)

        for st_name, by_district in tree.items():
            if remaining_budget <= 0:
                break

            all_districts = tree_all.get(st_name, {})
            state_total = sum(len(v) for v in all_districts.values())

            with st.expander(f"{st_name} ({state_total})", expanded=True):
                state_items_all = [it for lst in all_districts.values() for it in lst]
                if state_items_all:
                    _render_remove_all_in_state(st_name, state_items_all=state_items_all)

                for dist_name, block_items in by_district.items():
                    if remaining_budget <= 0:
                        break

                    district_total = len(all_districts.get(dist_name, []))
                    with st.expander(f"{dist_name} ({district_total})", expanded=True):
                        for it in block_items:
                            if remaining_budget <= 0:
                                break
                            remaining_budget -= 1

                            state_i = it.get("state", "")
                            district_i = it.get("district", "")
                            block_i = it.get("block", "")

                            col1, col2 = st.columns([5, 1])
                            with col1:
                                _render_indented_leaf(block_i, padding_px=16)
                            with col2:
                                key_parts = [normalize_fn(state_i), normalize_fn(district_i), normalize_fn(block_i)]
                                key = "btn_portfolio_remove_" + "_".join(key_parts)
                                if st.button("×", key=key, help=f"Remove {block_i}"):
                                    try:
                                        portfolio_remove_fn(state_i, district_i, block_i)
                                    except TypeError:
                                        portfolio_remove_fn(state_i, district_i)
                                    st.rerun()

                st.markdown("")
    else:
        # District mode: State -> Districts
        by_state_all = _group_portfolio_items_by_state(items_all, normalize_fn=normalize_fn)
        by_state = _group_portfolio_items_by_state(items, normalize_fn=normalize_fn)

        for st_name, state_items in by_state.items():
            if remaining_budget <= 0:
                break

            state_total = len(by_state_all.get(st_name, []))

            with st.expander(f"{st_name} ({state_total})", expanded=True):
                state_items_all = by_state_all.get(st_name, [])
                if state_items_all:
                    _render_remove_all_in_state(st_name, state_items_all=state_items_all)

                for it in state_items:
                    if remaining_budget <= 0:
                        break
                    remaining_budget -= 1

                    state_i = it.get("state", "")
                    district_i = it.get("district", "")

                    col1, col2 = st.columns([5, 1])
                    with col1:
                        _render_indented_leaf(district_i, padding_px=16)
                    with col2:
                        key_parts = [normalize_fn(state_i), normalize_fn(district_i)]
                        key = "btn_portfolio_remove_" + "_".join(key_parts)
                        if st.button("×", key=key, help=f"Remove {district_i}"):
                            try:
                                portfolio_remove_fn(state_i, district_i)
                            except TypeError:
                                portfolio_remove_fn(state_i, district_i)
                            st.rerun()

            st.markdown("")

    # Show more/less toggle (preserve existing session keys)
    if total_items > max_visible:
        remaining = total_items - max_visible
        if show_all:
            if st.button("Show less", key="_portfolio_show_less"):
                st.session_state["_portfolio_show_all"] = False
                st.rerun()
        else:
            if st.button(f"Show {remaining} more...", key="_portfolio_show_more"):
                st.session_state["_portfolio_show_all"] = True
                st.rerun()


# =============================================================================
# Clear Portfolio Button
# =============================================================================

def render_clear_portfolio_button(
    *,
    portfolio_count: int,
    clear_fn: Callable[[], None],
    set_flash_fn: Callable[[str, str], None],
    level: str = "district",
) -> None:
    """Render clear all button with inline confirmation."""
    import streamlit as st

    if portfolio_count == 0:
        return

    level_norm = (level or "district").strip().lower()
    unit_plural = "blocks" if level_norm == "block" else "districts"

    confirm_state = st.session_state.get("confirm_clear_portfolio", False)

    if not confirm_state:
        if st.button("Clear all", key="btn_portfolio_remove_all", type="secondary"):
            st.session_state["confirm_clear_portfolio"] = True
            st.rerun()
    else:
        st.warning(f"Remove all {portfolio_count} {unit_plural}?")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Yes, clear", key="btn_portfolio_remove_all_confirm", type="primary"):
                clear_fn()
                st.session_state["confirm_clear_portfolio"] = False
                set_flash_fn("Portfolio cleared.", "success")
                st.rerun()
        with col2:
            if st.button("Cancel", key="btn_portfolio_remove_all_cancel"):
                st.session_state["confirm_clear_portfolio"] = False
                st.rerun()


# =============================================================================
# Index Selector (Bundle-first with optional manual refinement)
# =============================================================================

def render_index_selector(
    *,
    variables: Mapping[str, Mapping[str, Any]],
    current_slug: str,
    selected_slugs: List[str],
) -> List[str]:
    """
    Render bundle-first index selection for portfolio comparison.
    
    Pattern 2 implementation:
    1. User selects one or more bundles (multi-select)
    2. Metrics from selected bundles are auto-expanded
    3. Optional: user can manually refine the metric list
    
    Ensures the current index is always included by default.
    
    Widget keys used:
    - portfolio_bundle_selection: selected bundles
    - portfolio_manual_refinement: whether manual mode is enabled
    - portfolio_multiindex_selection: final metric selection
    """
    import streamlit as st
    
    # Import bundle functions
    from india_resilience_tool.config.variables import (
        get_bundles,
        get_metrics_for_bundle,
        get_bundle_for_metric,
        get_default_bundle,
    )
    
    all_bundles = get_bundles()
    available_slugs = list(variables.keys())
    
    # --- Determine default bundle(s) based on current metric ---
    # If user has an existing bundle selection, use that
    # Otherwise, select the bundle(s) containing the current metric
    current_bundle_selection = st.session_state.get("portfolio_bundle_selection")
    
    if current_bundle_selection is None:
        # First load: select bundle(s) containing the current metric
        bundles_for_current = get_bundle_for_metric(current_slug)
        if bundles_for_current:
            default_bundles = [bundles_for_current[0]]  # Use first matching bundle
        else:
            default_bundles = [get_default_bundle()]
    else:
        # Use existing selection, filtering out invalid bundles
        default_bundles = [b for b in current_bundle_selection if b in all_bundles]
        if not default_bundles:
            default_bundles = [get_default_bundle()]
    
    # --- Bundle multi-select ---
    selected_bundles = st.multiselect(
        "Select risk domains to compare",
        options=all_bundles,
        default=default_bundles,
        key="portfolio_bundle_selection",
        help="Select one or more risk domains. Metrics from all selected domains will be included.",
    )
    
    # --- Expand bundles to metrics ---
    if selected_bundles:
        expanded_slugs: list[str] = []
        for bundle in selected_bundles:
            for slug in get_metrics_for_bundle(bundle):
                if slug in available_slugs and slug not in expanded_slugs:
                    expanded_slugs.append(slug)
        
        # Show count of expanded metrics
        st.caption(f"{len(expanded_slugs)} metrics from {len(selected_bundles)} domain(s)")
    else:
        # No bundles selected - fall back to current metric only
        expanded_slugs = [current_slug] if current_slug in available_slugs else []
    
    # --- Optional manual refinement (kept out of the main flow) ---
    st.caption("Metrics are auto-included from selected domains. Use **Advanced metrics** to refine.")
    with st.expander("Advanced metrics", expanded=False):
        st.checkbox(
            "Manually refine metric selection",
            value=st.session_state.get("portfolio_manual_refinement", False),
            key="portfolio_manual_refinement",
            help="Enable to manually add/remove individual metrics from the comparison",
        )

    manual_mode = bool(st.session_state.get("portfolio_manual_refinement", False))
    if manual_mode:
        # Show multi-select with expanded slugs as default
        # But allow user to pick any metric
        
        # Determine default for manual selection
        existing_manual = st.session_state.get("portfolio_multiindex_selection", [])
        if existing_manual and any(s in available_slugs for s in existing_manual):
            # User has made manual selections - preserve them
            default_manual = [s for s in existing_manual if s in available_slugs]
        else:
            # Use expanded slugs as default
            default_manual = expanded_slugs
        
        # Ensure current slug is in the default
        if current_slug in available_slugs and current_slug not in default_manual:
            default_manual = [current_slug] + default_manual
        
        selected = st.multiselect(
            "Metrics to compare",
            options=available_slugs,
            default=default_manual,
            format_func=lambda s: variables[s]["label"] if s in variables else s,
            key="portfolio_multiindex_selection",
            help="Select specific metrics to include in the comparison",
        )
        
        # If user clears all, fall back to current slug
        if not selected and current_slug in available_slugs:
            return [current_slug]
        
        return selected
    else:
        # Auto mode: use expanded slugs directly
        # Ensure current slug is included
        if current_slug in available_slugs and current_slug not in expanded_slugs:
            expanded_slugs = [current_slug] + expanded_slugs
        
        # Update session state for cache invalidation tracking
        st.session_state["portfolio_multiindex_selection"] = expanded_slugs
        
        # Show which metrics are included (collapsed by default)
        if expanded_slugs:
            with st.expander(f"View {len(expanded_slugs)} included metrics", expanded=False):
                # Group by bundle for display
                for bundle in selected_bundles:
                    bundle_metrics = [s for s in get_metrics_for_bundle(bundle) if s in expanded_slugs]
                    if bundle_metrics:
                        st.markdown(f"**{bundle}** ({len(bundle_metrics)})")
                        for slug in bundle_metrics:
                            label = variables.get(slug, {}).get("label", slug)
                            st.caption(f"  • {label}")
        
        return expanded_slugs


# =============================================================================
# Comparison Table (Auto-rebuild)
# =============================================================================

def build_comparison_df(
    *,
    portfolio: Sequence[Any],
    selected_slugs: Sequence[str],
    variables: Mapping[str, Mapping[str, Any]],
    index_group_labels: Mapping[str, str],
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    sel_scenarios: Optional[Sequence[str]] = None,
    pilot_state: str,
    data_dir: Path,
    load_master_csv_fn: Callable[[str], pd.DataFrame],
    normalize_master_columns_fn: Callable[[pd.DataFrame], pd.DataFrame],
    parse_master_schema_fn: Callable[[Any], tuple[list, list, dict]],
    resolve_metric_column_fn: Callable[..., Optional[str]],
    find_baseline_column_for_stat_fn: Callable[..., Optional[str]],
    risk_class_from_percentile_fn: Callable[[float], str],
    normalize_fn: Callable[[str], str],
    build_portfolio_multiindex_df_fn: Callable[..., pd.DataFrame],
    level: str = "district",
) -> Optional[pd.DataFrame]:
    """
    Build the portfolio comparison dataframe with auto-rebuild on changes.

    District mode:
      - loads master_metrics_by_district.csv
      - matches rows on (state, district)

    Block mode:
      - loads master_metrics_by_block.csv
      - matches rows on (state, district, block)
    """
    import streamlit as st
    import os
    from india_resilience_tool.analysis.metrics import compute_rank_and_percentile
    from india_resilience_tool.app.portfolio_multistate import (
        extract_states_in_portfolio,
    )

    level_norm = (level or "district").strip().lower()
    is_block = level_norm == "block"

    if not portfolio or not selected_slugs:
        return None

    states_for_master = extract_states_in_portfolio(portfolio, fallback_state=pilot_state)

    # Build context for cache invalidation (level-aware)
    def _unit_tuple(item: Any) -> tuple:
        if isinstance(item, dict):
            st_name = item.get("state")
            dist_name = item.get("district")
            blk_name = item.get("block")
        else:
            tup = tuple(item)
            st_name = tup[0] if len(tup) > 0 else None
            dist_name = tup[1] if len(tup) > 1 else None
            blk_name = tup[2] if len(tup) > 2 else None

        return (st_name, dist_name, blk_name) if is_block else (st_name, dist_name)

    scenarios_for_context = list(sel_scenarios) if sel_scenarios is not None else [sel_scenario]

    context = {
        "level": level_norm,
        "units": [_unit_tuple(d) for d in portfolio],
        "states": list(states_for_master),
        "slugs": list(selected_slugs),
        "scenario": sel_scenario,
        "scenarios": list(scenarios_for_context),
        "period": sel_period,
        "stat": sel_stat,
    }

    prev_context = st.session_state.get("portfolio_multiindex_context")
    cached_df = st.session_state.get("portfolio_multiindex_df")
    needs_rebuild = cached_df is None or prev_context != context

    missing_master_by_slug: dict[str, list[str]] = {}

    # Helper functions
    def _resolve_proc_root_for_slug(slug: str) -> Path:
        env_root = os.getenv("IRT_PROCESSED_ROOT")
        if env_root:
            base_path = Path(env_root)
            if base_path.name == slug:
                return base_path.resolve()
            return (base_path / slug).resolve()
        return (data_dir / "processed" / slug).resolve()

    def _resolve_state_dir(proc_root: Path, state_name: str) -> str:
        """
        Resolve a portfolio state label to an on-disk directory name under proc_root.

        This improves robustness against casing/whitespace differences.
        """
        key = str(proc_root.resolve()) if proc_root else str(proc_root)
        cache = st.session_state.setdefault("_portfolio_proc_state_dirs", {})
        mapping = cache.get(key)
        if not isinstance(mapping, dict):
            mapping = {}
            try:
                if proc_root.exists() and proc_root.is_dir():
                    for p in proc_root.iterdir():
                        if p.is_dir():
                            mapping[normalize_fn(p.name)] = p.name
            except Exception:
                mapping = {}
            cache[key] = mapping

        return str(mapping.get(normalize_fn(state_name), state_name))

    def _load_master_and_schema_for_slug(slug: str):
        proc_root = _resolve_proc_root_for_slug(slug)
        master_fname = "master_metrics_by_block.csv" if is_block else "master_metrics_by_district.csv"

        file_cache = st.session_state.setdefault("_portfolio_master_cache", {})
        concat_cache = st.session_state.setdefault("_portfolio_master_concat_cache", {})

        # Resolve state directory names once per slug build.
        state_dirs = [_resolve_state_dir(proc_root, s) for s in states_for_master] or ([pilot_state] if pilot_state else [])
        # Cache key should be stable across portfolio ordering (output does not depend on state load order).
        state_dirs_key = "|".join(sorted([normalize_fn(s) for s in state_dirs]))
        concat_key = f"{slug}::{level_norm}::{master_fname}::{state_dirs_key}"

        signature: list[tuple[str, Optional[float]]] = []
        dfs: list[pd.DataFrame] = []
        missing_states: list[str] = []

        for st_name in state_dirs:
            master_path = proc_root / str(st_name) / master_fname
            path_s = str(master_path)
            try:
                mtime = master_path.stat().st_mtime
            except Exception:
                mtime = None
            signature.append((path_s, mtime))

        signature_sorted = tuple(sorted(signature, key=lambda x: x[0]))
        concat_entry = concat_cache.get(concat_key)
        if isinstance(concat_entry, dict) and concat_entry.get("signature") == signature_sorted:
            missing = concat_entry.get("missing_states")
            if isinstance(missing, list) and missing:
                missing_master_by_slug[str(slug)] = [str(x) for x in missing if str(x).strip()]
            return concat_entry["df"], concat_entry["schema_items"], concat_entry["metrics"], concat_entry["by_metric"]

        for st_name in state_dirs:
            master_path = proc_root / str(st_name) / master_fname

            file_key = f"{slug}::{master_path}"
            try:
                mtime = master_path.stat().st_mtime
            except Exception:
                mtime = None

            entry = file_cache.get(file_key)
            if entry and entry.get("mtime") == mtime:
                df_state = entry["df"]
                if df_state is not None and not df_state.empty:
                    dfs.append(df_state)
                continue

            if not master_path.exists():
                missing_states.append(str(st_name))
                empty = pd.DataFrame()
                file_cache[file_key] = {"df": empty, "schema_items": [], "metrics": [], "by_metric": {}, "mtime": mtime}
                continue

            df_state = load_master_csv_fn(str(master_path))
            df_state = normalize_master_columns_fn(df_state)
            schema_items_s, metrics_s, by_metric_s = parse_master_schema_fn(df_state.columns)
            file_cache[file_key] = {
                "df": df_state,
                "schema_items": schema_items_s,
                "metrics": metrics_s,
                "by_metric": by_metric_s,
                "mtime": mtime,
            }
            if df_state is not None and not df_state.empty:
                dfs.append(df_state)

        if not dfs:
            empty = pd.DataFrame()
            concat_cache[concat_key] = {"df": empty, "schema_items": [], "metrics": [], "by_metric": {}, "signature": signature_sorted, "missing_states": missing_states}
            if missing_states:
                missing_master_by_slug[str(slug)] = list(missing_states)
            return empty, [], [], {}

        df_all = pd.concat(dfs, axis=0, ignore_index=True, sort=False)
        schema_items, metrics, by_metric = parse_master_schema_fn(df_all.columns)
        concat_cache[concat_key] = {
            "df": df_all,
            "schema_items": schema_items,
            "metrics": metrics,
            "by_metric": by_metric,
            "signature": signature_sorted,
            "missing_states": missing_states,
        }
        if missing_states:
            missing_master_by_slug[str(slug)] = list(missing_states)
        return df_all, schema_items, metrics, by_metric

    def _match_row_idx(df_local, st_name, dist_name, blk_name: Optional[str] = None):
        if df_local is None or df_local.empty:
            return None

        # Support both naming styles (some loaders may use *_name)
        state_col_name = "state" if "state" in df_local.columns else ("state_name" if "state_name" in df_local.columns else None)
        district_col_name = "district" if "district" in df_local.columns else ("district_name" if "district_name" in df_local.columns else None)
        block_col_name = None
        if is_block:
            block_col_name = "block" if "block" in df_local.columns else ("block_name" if "block_name" in df_local.columns else None)

        if state_col_name is None or district_col_name is None:
            return None
        if is_block and block_col_name is None:
            return None

        st_norm = normalize_fn(st_name)
        dist_norm = normalize_fn(dist_name)
        state_col = df_local[state_col_name].astype(str).map(normalize_fn)
        dist_col = df_local[district_col_name].astype(str).map(normalize_fn)

        if is_block:
            if not blk_name:
                return None
            blk_norm = normalize_fn(blk_name)
            blk_col = df_local[block_col_name].astype(str).map(normalize_fn)

            exact = (state_col == st_norm) & (dist_col == dist_norm) & (blk_col == blk_norm)
            if exact.any():
                return int(df_local.index[exact][0])

            try:
                contains = blk_col.str.contains(blk_norm, na=False)
                fallback = (state_col == st_norm) & (dist_col == dist_norm) & contains
                if fallback.any():
                    return int(df_local.index[fallback][0])
            except Exception:
                pass

            return None

        exact = (state_col == st_norm) & (dist_col == dist_norm)
        if exact.any():
            return int(df_local.index[exact][0])

        try:
            contains = dist_col.str.contains(dist_norm, na=False)
            fallback = (state_col == st_norm) & contains
            if fallback.any():
                return int(df_local.index[fallback][0])
        except Exception:
            pass

        return None

    def _compute_rank_and_percentile(df_local, st_name, metric_col, value):
        state_col_name = "state" if "state" in df_local.columns else ("state_name" if "state_name" in df_local.columns else "state")
        return compute_rank_and_percentile(
            df_local,
            st_name,
            metric_col,
            value,
            state_col=state_col_name,
            normalize_fn=normalize_fn,
            percentile_method="le",
        )

    # Build or use cached table
    if needs_rebuild:
        with st.spinner("Building comparison table..."):
            kwargs = dict(
                portfolio=portfolio,
                selected_slugs=selected_slugs,
                variables=variables,
                index_group_labels=index_group_labels,
                sel_scenario=sel_scenario,
                sel_period=sel_period,
                sel_stat=sel_stat,
                load_master_and_schema_for_slug=_load_master_and_schema_for_slug,
                resolve_metric_column=resolve_metric_column_fn,
                find_baseline_column_for_stat=find_baseline_column_for_stat_fn,
                match_row_idx=_match_row_idx,
                compute_rank_and_percentile=_compute_rank_and_percentile,
                risk_class_from_percentile=risk_class_from_percentile_fn,
                normalize_fn=normalize_fn,
            )

            if sel_scenarios is not None:
                kwargs["sel_scenarios"] = list(sel_scenarios)

            # Prefer passing level if the builder supports it; fallback if not
            try:
                df = build_portfolio_multiindex_df_fn(**kwargs, level=level_norm)
            except TypeError:
                df = build_portfolio_multiindex_df_fn(**kwargs)

            st.session_state["portfolio_multiindex_df"] = df
            st.session_state["portfolio_multiindex_context"] = context
            cached_df = df

        if missing_master_by_slug:
            # Show a compact warning once per rebuild (avoids repeating on unrelated reruns).
            examples: list[str] = []
            for slug, states_missing in list(missing_master_by_slug.items())[:3]:
                sm = [s for s in states_missing if isinstance(s, str) and s.strip()]
                if not sm:
                    continue
                examples.append(f"{slug}: {', '.join(sm[:4])}" + ("…" if len(sm) > 4 else ""))
            if examples:
                st.warning(
                    "Some portfolio states are missing master metrics for one or more indices; "
                    "those rows may show blank values. " + " | ".join(examples)
                )

    if cached_df is not None and not cached_df.empty:
        return cached_df
    return None


def render_comparison_table_ui(
    df: pd.DataFrame,
    *,
    level: str = "district",
) -> None:
    """Render the comparison table UI (wide/long + downloads) for a pre-built comparison dataframe."""
    import streamlit as st

    if df is None or df.empty:
        st.info("No comparison results to display.")
        return

    level_norm = (level or "district").strip().lower()
    is_block = level_norm == "block"

    long_df = df.copy()
    has_scenario = ("Scenario" in long_df.columns) and long_df["Scenario"].notna().any()

    def _reorder_long_df(df_in: pd.DataFrame, *, include_scenario: bool) -> pd.DataFrame:
        out = df_in.copy()
        preferred: list[str] = []

        for c in ("State", "District"):
            if c in out.columns:
                preferred.append(c)

        if is_block and "Block" in out.columns:
            preferred.append("Block")

        for c in ("Index", "Group"):
            if c in out.columns:
                preferred.append(c)

        if include_scenario and "Scenario" in out.columns:
            preferred.append("Scenario")

        for c in (
            "Current value",
            "Baseline",
            "Δ",
            "%Δ",
            "Rank in state",
            "Percentile",
            "Risk class",
        ):
            if c in out.columns and c not in preferred:
                preferred.append(c)

        remaining = [c for c in out.columns if c not in preferred]
        return out[preferred + remaining]

    def _pivot_scenario_wide(df_in: pd.DataFrame, *, value_col: str) -> pd.DataFrame:
        if df_in is None or df_in.empty or "Scenario" not in df_in.columns:
            return df_in

        id_cols: list[str] = []
        for c in ("State", "District"):
            if c in df_in.columns:
                id_cols.append(c)

        if is_block and "Block" in df_in.columns:
            id_cols.append("Block")

        for c in ("Index", "Group"):
            if c in df_in.columns:
                id_cols.append(c)

        base_cols = list(id_cols)
        if "Baseline" in df_in.columns and "Baseline" not in base_cols:
            base_cols.append("Baseline")

        base = df_in[base_cols].drop_duplicates(subset=id_cols).set_index(id_cols)

        if value_col not in df_in.columns:
            value_col = "Percentile" if "Percentile" in df_in.columns else "Current value"

        pv = df_in.pivot_table(
            index=id_cols,
            columns="Scenario",
            values=value_col,
            aggfunc="first",
        )

        pv.columns = [f"{value_col} ({str(c)})" for c in pv.columns]
        wide = base.join(pv, how="left").reset_index()
        return wide

    view = "Long"
    value_col_for_wide = "Percentile"
    if has_scenario:
        st.caption(
            "Baseline is historical; Δ and %Δ are computed vs baseline. "
            "Use **Wide** view to compare scenarios side-by-side."
        )
        view_key = f"_portfolio_scenario_view_{level_norm}"
        value_key = f"_portfolio_scenario_value_{level_norm}"

        value_options_all = [
            "Current value",
            "Δ",
            "%Δ",
            "Percentile",
            "Risk class",
            "Rank in state",
        ]
        value_options = [c for c in value_options_all if c in long_df.columns]
        if not value_options:
            value_options = ["Percentile"] if "Percentile" in long_df.columns else ["Current value"]

        v1, v2 = st.columns([1, 2])
        with v1:
            view = st.radio(
                "Scenario view",
                options=["Wide", "Long"],
                horizontal=True,
                key=view_key,
            )
        with v2:
            default_value = st.session_state.get(value_key, "Percentile")
            if default_value not in value_options:
                default_value = value_options[0]
            value_col_for_wide = st.selectbox(
                "Value to compare",
                options=value_options,
                index=value_options.index(default_value),
                key=value_key,
            )

    # Choose display dataframe
    if has_scenario and view == "Wide":
        display_df = _pivot_scenario_wide(long_df, value_col=value_col_for_wide)
    else:
        display_df = _reorder_long_df(long_df, include_scenario=has_scenario)

    st.dataframe(display_df, hide_index=True, use_container_width=True)

    def _safe_stub(s: str) -> str:
        s = str(s).strip().lower()
        s = s.replace("%", "pct")
        s = s.replace("Δ", "delta")
        s = re.sub(r"[^a-z0-9_\-]+", "_", s)
        return s.strip("_")

    if has_scenario:
        dl1, dl2 = st.columns(2)
        with dl1:
            st.download_button(
                "Download long CSV",
                data=_reorder_long_df(long_df, include_scenario=True).to_csv(index=False).encode("utf-8"),
                file_name=f"portfolio_comparison_{level_norm}_long.csv",
                mime="text/csv",
            )
        with dl2:
            wide_df = _pivot_scenario_wide(long_df, value_col=value_col_for_wide)
            st.download_button(
                f"Download wide CSV ({value_col_for_wide})",
                data=wide_df.to_csv(index=False).encode("utf-8"),
                file_name=f"portfolio_comparison_{level_norm}_wide_{_safe_stub(value_col_for_wide)}.csv",
                mime="text/csv",
            )
    else:
        st.download_button(
            "Download as CSV",
            data=display_df.to_csv(index=False).encode("utf-8"),
            file_name=f"portfolio_comparison_{level_norm}.csv",
            mime="text/csv",
        )


def render_comparison_table(
    *,
    portfolio: Sequence[Any],
    selected_slugs: Sequence[str],
    variables: Mapping[str, Mapping[str, Any]],
    index_group_labels: Mapping[str, str],
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    sel_scenarios: Optional[Sequence[str]] = None,
    pilot_state: str,
    data_dir: Path,
    load_master_csv_fn: Callable[[str], pd.DataFrame],
    normalize_master_columns_fn: Callable[[pd.DataFrame], pd.DataFrame],
    parse_master_schema_fn: Callable[[Any], tuple[list, list, dict]],
    resolve_metric_column_fn: Callable[..., Optional[str]],
    find_baseline_column_for_stat_fn: Callable[..., Optional[str]],
    risk_class_from_percentile_fn: Callable[[float], str],
    normalize_fn: Callable[[str], str],
    build_portfolio_multiindex_df_fn: Callable[..., pd.DataFrame],
    level: str = "district",
) -> Optional[pd.DataFrame]:
    """
    Backward-compatible wrapper: build + render the comparison table UI.

    Prefer calling `build_comparison_df` and `render_comparison_table_ui` separately for layouts
    that need the dataframe for multiple views (e.g., tabs).
    """
    df = build_comparison_df(
        portfolio=portfolio,
        selected_slugs=selected_slugs,
        variables=variables,
        index_group_labels=index_group_labels,
        sel_scenario=sel_scenario,
        sel_period=sel_period,
        sel_stat=sel_stat,
        sel_scenarios=sel_scenarios,
        pilot_state=pilot_state,
        data_dir=data_dir,
        load_master_csv_fn=load_master_csv_fn,
        normalize_master_columns_fn=normalize_master_columns_fn,
        parse_master_schema_fn=parse_master_schema_fn,
        resolve_metric_column_fn=resolve_metric_column_fn,
        find_baseline_column_for_stat_fn=find_baseline_column_for_stat_fn,
        risk_class_from_percentile_fn=risk_class_from_percentile_fn,
        normalize_fn=normalize_fn,
        build_portfolio_multiindex_df_fn=build_portfolio_multiindex_df_fn,
        level=level,
    )
    if df is not None and not df.empty:
        render_comparison_table_ui(df, level=level)
    return df


# =============================================================================
# Portfolio Visualizations
# =============================================================================

# Available value columns for visualization
VIZ_VALUE_OPTIONS = {
    "Current value": "Current Value",
    "Percentile": "Percentile (within state)",
    "%Δ": "% Change from Baseline",
    "Δ": "Absolute Change",
}

# Available chart types
CHART_TYPES = {
    "heatmap": "Heatmap",
    "grouped_bar": "Grouped Bar Chart",
    "both": "Both Charts",
}


def render_portfolio_visualizations(
    df: pd.DataFrame,
    *,
    default_value_col: str = "Percentile",
    default_chart_type: str = "heatmap",
) -> None:
    """
    Render interactive portfolio comparison visualizations.
    
    Args:
        df: DataFrame from build_portfolio_multiindex_df with columns:
            State, District, Index, Group, Current value, Baseline, Δ, %Δ,
            Rank in state, Percentile, Risk class
        default_value_col: Default value column to visualize
        default_chart_type: Default chart type to show
    """
    import streamlit as st
    
    if df is None or df.empty:
        st.info("Add districts and select indices to see visualizations.")
        return

    has_scenario = ("Scenario" in df.columns) and df["Scenario"].notna().any()
    is_block = ("Block" in df.columns) and df["Block"].notna().any()
    scope = "block" if is_block else "district"
    
    # Check minimum data requirements
    n_districts = df["District"].nunique() if "District" in df.columns else 0
    n_indices = df["Index"].nunique() if "Index" in df.columns else 0
    n_units = df["Block"].nunique() if is_block and "Block" in df.columns else n_districts
    
    if n_units < 1 or n_indices < 1:
        st.info("Need at least 1 unit and 1 index for visualizations.")
        return
    
    # Filter value options to those with valid data
    available_value_cols = {}
    for col_key, col_label in VIZ_VALUE_OPTIONS.items():
        if col_key in df.columns:
            # Check if column has any non-null values
            if df[col_key].notna().any():
                available_value_cols[col_key] = col_label
    
    if not available_value_cols:
        st.warning("No valid data columns available for visualization.")
        return

    from india_resilience_tool.viz.charts import SCENARIO_DISPLAY, SCENARIO_ORDER

    def _scenario_label(s: str) -> str:
        s_norm = str(s or "").strip().lower()
        return str(SCENARIO_DISPLAY.get(s_norm, s)).strip()

    def _scenario_sort(s: str) -> tuple[int, int, str]:
        s_norm = str(s or "").strip().lower()
        if s_norm in SCENARIO_ORDER:
            return (0, int(SCENARIO_ORDER.index(s_norm)), s_norm)
        return (1, 10_000, s_norm)

    scenario_options: list[str] = []
    if has_scenario:
        scenario_options = [
            s
            for s in (df["Scenario"].dropna().astype(str).map(str.strip).tolist() if "Scenario" in df.columns else [])
            if s
        ]
        # De-dupe while preserving order
        seen = set()
        scenario_options = [s for s in scenario_options if not (s in seen or seen.add(s))]
        scenario_options = sorted(scenario_options, key=_scenario_sort)

    viz_mode = "Across indices (single scenario)"
    if has_scenario and len(scenario_options) >= 2:
        viz_mode = st.radio(
            "Visualization mode",
            options=["Scenario compare", "Across indices (single scenario)"],
            horizontal=True,
            index=0,
            key=f"_viz_mode_{scope}",
        )
    elif has_scenario:
        st.caption("Only one scenario present in the table; scenario-compare visualizations are disabled.")

    # Import chart functions
    from india_resilience_tool.viz.charts import (
        make_portfolio_grouped_bar,
        make_portfolio_heatmap,
        make_portfolio_heatmap_robust_min_percentile,
        make_portfolio_heatmap_scenario_panels,
    )

    value_col_options = list(available_value_cols.keys())
    default_idx = value_col_options.index(default_value_col) if default_value_col in value_col_options else 0

    if viz_mode == "Scenario compare":
        st.markdown("#### Scenario compare")

        chart_key = f"_viz_scenario_chart_{scope}"
        scenario_chart = st.selectbox(
            "Scenario chart",
            options=[
                "Heatmap — Scenario panels + robust (stacked)",
                "Heatmap — Robust risk (min percentile)",
                "Heatmap — Scenario panels",
            ],
            index=0,
            key=chart_key,
        )

        value_col = st.selectbox(
            "Show values",
            options=value_col_options,
            format_func=lambda x: available_value_cols[x],
            index=default_idx,
            key=f"_viz_scenario_value_col_{scope}",
        )

        if scenario_chart.startswith("Heatmap — Scenario panels + robust"):
            st.caption(
                "Scenario panels provide context; Robust risk summarizes the **min percentile** across scenarios (risk that remains high even under the less severe scenario)."
            )
            if value_col != "Percentile":
                st.info("Stacked view is currently defined for **Percentile** only. Switch **Show values** to Percentile.")
                return
            sel_scens = list(scenario_options)
            if len(sel_scens) < 2:
                st.info("Scenario compare requires at least 2 scenarios.")
                return
            if len(sel_scens) > 2:
                st.warning("Showing the first 2 scenarios for readability. Reduce scenarios in Scenario mode to change this.")
                sel_scens = sel_scens[:2]
            st.caption("Using scenarios: " + ", ".join([_scenario_label(s) for s in sel_scens]))

            # Context headers
            ctx = st.session_state.get("portfolio_multiindex_context") or {}
            period = str(ctx.get("period") or "").strip()
            stat = str(ctx.get("stat") or "").strip()
            ctx_bits = []
            if period:
                ctx_bits.append(period)
            if stat:
                ctx_bits.append(stat)
            ctx_suffix = (" • " + " • ".join(ctx_bits)) if ctx_bits else ""

            st.markdown("##### Scenario panels (Percentile)" + ctx_suffix)
            st.caption("Each panel shows percentile (within state) for one scenario; colors indicate risk class.")

            fig_panels = make_portfolio_heatmap_scenario_panels(
                df,
                value_col="Percentile",
                scenarios=sel_scens,
                normalize_per_index=False,
                layout="vertical",
                hide_xticklabels_except_last=True,
                hspace=0.12,
            )
            if fig_panels is not None:
                st.pyplot(fig_panels)
                _offer_figure_download(fig_panels, "portfolio_heatmap_scenario_panels_stacked.png", "Download heatmap")
            else:
                st.warning("Could not generate scenario panels heatmap. Check data availability.")

            st.markdown("##### Robust risk (min percentile)" + ctx_suffix)
            st.caption("Robust risk = min percentile across selected scenarios.")

            fig_robust = make_portfolio_heatmap_robust_min_percentile(
                df,
                scenarios=sel_scens,
            )
            if fig_robust is not None:
                st.pyplot(fig_robust)
                _offer_figure_download(fig_robust, "portfolio_heatmap_robust_min_percentile.png", "Download heatmap")
            else:
                st.warning("Could not generate robust risk heatmap. Check data availability.")

        elif scenario_chart.startswith("Heatmap — Robust risk"):
            st.caption(
                "Robust risk = **min percentile** across selected scenarios (risk that remains high even under the less severe scenario)."
            )
            if value_col != "Percentile":
                st.info("Robust risk is currently defined for **Percentile** only. Switch **Show values** to Percentile.")
                return
            robust_scens = list(scenario_options)
            if len(robust_scens) < 2:
                st.info("Scenario compare requires at least 2 scenarios.")
                return
            st.caption("Using scenarios: " + ", ".join([_scenario_label(s) for s in robust_scens]))

            fig = make_portfolio_heatmap_robust_min_percentile(
                df,
                scenarios=robust_scens,
            )
            if fig is not None:
                st.pyplot(fig)
                _offer_figure_download(fig, "portfolio_heatmap_robust_min_percentile.png", "Download heatmap")
            else:
                st.warning("Could not generate robust risk heatmap. Check data availability.")

        elif scenario_chart.startswith("Heatmap — Scenario panels"):
            sel_scens = list(scenario_options)
            if len(sel_scens) > 3:
                st.warning("Showing the first 3 scenarios for readability. Reduce scenarios in Scenario mode to change this.")
                sel_scens = sel_scens[:3]
            st.caption("Using scenarios: " + ", ".join([_scenario_label(s) for s in sel_scens]))

            with st.expander("Heatmap options", expanded=False):
                col1, col2, col3 = st.columns(3)
                with col1:
                    layout = st.radio(
                        "Layout",
                        options=["horizontal", "vertical"],
                        horizontal=True,
                        index=0,
                        key=f"_viz_scenario_panels_layout_{scope}",
                    )
                with col2:
                    normalize = st.checkbox(
                        "Normalize per index",
                        value=True if value_col == "Current value" else False,
                        disabled=value_col != "Current value",
                        help="For Current value only: normalize per index across all selected scenarios (shared scale).",
                        key=f"_viz_scenario_panels_norm_{scope}",
                    )
                with col3:
                    cmap = "RdYlGn_r"
                    if value_col == "Percentile":
                        st.caption("Colors represent risk classes (Very Low → Very High); numbers show percentiles.")
                    else:
                        cmap = st.selectbox(
                            "Color scheme",
                            options=["RdYlGn_r", "RdYlBu_r", "YlOrRd", "Blues", "Reds", "viridis"],
                            index=0,
                            key=f"_viz_scenario_panels_cmap_{scope}",
                            disabled=value_col in ("%Δ", "Δ"),
                        )

            fig = make_portfolio_heatmap_scenario_panels(
                df,
                value_col=value_col,
                scenarios=sel_scens,
                normalize_per_index=bool(normalize) if value_col == "Current value" else False,
                cmap=cmap,
                layout=layout,
            )
            if fig is not None:
                st.pyplot(fig)
                _offer_figure_download(fig, "portfolio_heatmap_scenario_panels.png", "Download heatmap")
            else:
                st.warning("Could not generate scenario panels heatmap. Check data availability.")

        else:
            st.warning("Unknown scenario chart option.")

        return

    # -------------------------
    # Across indices (single scenario) — legacy charts
    # -------------------------
    df_single = df
    if has_scenario and scenario_options:
        scen_filtered = st.selectbox(
            "Scenario to visualize",
            options=scenario_options,
            index=0,
            format_func=_scenario_label,
            key=f"_viz_single_scenario_{scope}",
            help="Legacy charts require a single scenario selection.",
        )
        df_single = df[df["Scenario"].astype(str).str.strip() == str(scen_filtered).strip()].copy()

    col1, col2 = st.columns(2)
    with col1:
        chart_type = st.selectbox(
            "Chart type",
            options=list(CHART_TYPES.keys()),
            format_func=lambda x: CHART_TYPES[x],
            index=list(CHART_TYPES.keys()).index(default_chart_type) if default_chart_type in CHART_TYPES else 0,
            key=f"_viz_chart_type_{scope}",
        )
    with col2:
        value_col = st.selectbox(
            "Show values",
            options=value_col_options,
            format_func=lambda x: available_value_cols[x],
            index=default_idx,
            key=f"_viz_value_col_{scope}",
        )

    if chart_type in ("heatmap", "both"):
        _render_heatmap_section(
            df_single,
            value_col,
            make_portfolio_heatmap,
            key_prefix=f"{scope}_single",
        )

    if chart_type in ("grouped_bar", "both"):
        _render_grouped_bar_section(
            df_single,
            value_col,
            make_portfolio_grouped_bar,
            key_prefix=f"{scope}_single",
        )


def _render_heatmap_section(
    df: pd.DataFrame,
    value_col: str,
    make_heatmap_fn: Callable,
    *,
    key_prefix: str = "",
) -> None:
    """Render the heatmap visualization section."""
    import streamlit as st
    
    st.markdown("#### Heatmap")
    
    # Heatmap-specific options
    with st.expander("Heatmap options", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            normalize = st.checkbox(
                "Normalize per index",
                value=True if value_col == "Current value" else False,
                disabled=value_col != "Current value",
                help="Scale values within each index column for better comparison",
                key=f"{key_prefix}_heatmap_normalize",
            )
        with col2:
            cmap = st.selectbox(
                "Color scheme",
                options=["RdYlGn_r", "RdYlBu_r", "YlOrRd", "Blues", "Reds", "viridis"],
                index=0,
                key=f"{key_prefix}_heatmap_cmap",
                disabled=value_col in ("%Δ", "Δ"),  # Diverging maps forced for change
            )
    
    # Generate heatmap
    try:
        fig = make_heatmap_fn(
            df,
            value_col=value_col,
            normalize_per_index=normalize if value_col == "Current value" else False,
            cmap=cmap,
        )
        
        if fig is not None:
            st.pyplot(fig)
            
            # Download button for the figure
            _offer_figure_download(fig, "portfolio_heatmap.png", "Download heatmap")
        else:
            st.warning("Could not generate heatmap. Check data availability.")
    except Exception as e:
        st.error(f"Error generating heatmap: {e}")


def _render_grouped_bar_section(
    df: pd.DataFrame,
    value_col: str,
    make_bar_fn: Callable,
    *,
    key_prefix: str = "",
) -> None:
    """Render the grouped bar chart visualization section."""
    import streamlit as st
    
    st.markdown("#### Grouped Bar Chart")
    
    # Count data dimensions
    n_districts = df["District"].nunique() if "District" in df.columns else 0
    n_indices = df["Index"].nunique() if "Index" in df.columns else 0
    
    # Bar chart-specific options
    with st.expander("Bar chart options", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            max_districts_max = min(15, max(n_districts, 1))
            if max_districts_max <= 1:
                max_districts = 1
                st.caption("Max districts: 1")
            else:
                max_districts = st.slider(
                    "Max districts",
                    min_value=1,
                    max_value=max_districts_max,
                    value=min(10, max_districts_max),
                    key=f"{key_prefix}_bar_max_districts",
                )
        with col2:
            max_indices_max = min(10, max(n_indices, 1))
            if max_indices_max <= 1:
                max_indices = 1
                st.caption("Max indices: 1")
            else:
                max_indices = st.slider(
                    "Max indices",
                    min_value=1,
                    max_value=max_indices_max,
                    value=min(6, max_indices_max),
                    key=f"{key_prefix}_bar_max_indices",
                )
        with col3:
            horizontal = st.checkbox(
                "Horizontal bars",
                value=n_districts > 5,
                help="Better for many districts",
                key=f"{key_prefix}_bar_horizontal",
            )
        
        show_values = st.checkbox(
            "Show values on bars",
            value=True,
            key=f"{key_prefix}_bar_show_values",
        )
    
    # Generate bar chart
    try:
        fig = make_bar_fn(
            df,
            value_col=value_col,
            max_districts=max_districts,
            max_indices=max_indices,
            horizontal=horizontal,
            show_values=show_values,
        )
        
        if fig is not None:
            st.pyplot(fig)
            
            # Download button for the figure
            _offer_figure_download(fig, "portfolio_bar_chart.png", "Download bar chart")
        else:
            st.warning("Could not generate bar chart. Check data availability.")
    except Exception as e:
        st.error(f"Error generating bar chart: {e}")


def _offer_figure_download(fig: Any, filename: str, label: str) -> None:
    """Offer a download button for a matplotlib figure."""
    import streamlit as st
    import io
    
    try:
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="white")
        buf.seek(0)
        
        st.download_button(
            f"Download {label}",
            data=buf.getvalue(),
            file_name=filename,
            mime="image/png",
            key=f"_download_{filename}",
        )
    except Exception:
        pass  # Silently fail download if there's an issue


# =============================================================================
# Coordinate Lookup - Import from point_selection_ui
# =============================================================================

# The full coordinate lookup with tabs, batch input, and show-on-map features
# is in point_selection_ui.py. We import and wrap it here for use in the panel.

def render_coordinate_lookup(
    *,
    merged: Any,
    portfolio_add_fn: Callable[..., None],
    set_flash_fn: Callable[[str, str], None],
    level: str = "district",
) -> None:
    """
    Render coordinate-based unit lookup.

    This wraps render_point_selection_panel from point_selection_ui.py
    and forwards admin level so the panel can resolve district vs block.
    """
    from india_resilience_tool.app.point_selection_ui import render_point_selection_panel

    level_norm = (level or "district").strip().lower()
    is_block = level_norm == "block"

    def _portfolio_key_fn(state: str, district: str, block: Optional[str] = None) -> tuple:
        s = state.lower().replace(" ", "")
        d = district.lower().replace(" ", "")
        if is_block:
            b = (block or "").lower().replace(" ", "")
            return (s, d, b)
        return (s, d)

    render_point_selection_panel(
        merged=merged,
        level=level_norm,
        portfolio_add_fn=portfolio_add_fn,
        portfolio_key_fn=_portfolio_key_fn,
        portfolio_set_flash_fn=set_flash_fn,
    )


# =============================================================================
# Main Portfolio Panel
# =============================================================================

def render_portfolio_panel(
    *,
    # State/selection context
    selected_state: str,
    portfolio_route: Optional[str],  # Kept for backward compat, ignored
    level: str = "district",
    # Variable/metric context
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
    index_group_labels: Mapping[str, str],
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    metric_col: str,
    # Data
    merged: Any,
    adm1: Any,
    # Config
    pilot_state: str,
    data_dir: Path,
    # Callable dependencies
    compute_state_metrics_fn: Callable[..., tuple[dict, Any, Any]],
    load_master_csv_fn: Callable[[str], pd.DataFrame],
    normalize_master_columns_fn: Callable[[pd.DataFrame], pd.DataFrame],
    parse_master_schema_fn: Callable[[Any], tuple[list, list, dict]],
    resolve_metric_column_fn: Callable[..., Optional[str]],
    find_baseline_column_for_stat_fn: Callable[..., Optional[str]],
    risk_class_from_percentile_fn: Callable[[float], str],
    portfolio_normalize_fn: Callable[[str], str],
    portfolio_remove_fn: Callable[..., None],
    portfolio_remove_all_fn: Callable[[], None],
    build_portfolio_multiindex_df_fn: Callable[..., pd.DataFrame],
) -> None:
    """
    Render the complete portfolio panel (district or block).

    Note: portfolio_route parameter is kept for backward compatibility but ignored.
    """
    import streamlit as st
    from india_resilience_tool.analysis.portfolio import portfolio_add, portfolio_contains

    level_norm = (level or "district").strip().lower()
    is_block = level_norm == "block"
    unit_plural = "blocks" if is_block else "districts"

    def _add(state: str, district: str, block: Optional[str] = None) -> None:
        try:
            portfolio_add(st.session_state, state, district, block_name=block, level=level_norm, normalize_fn=portfolio_normalize_fn)
        except TypeError:
            portfolio_add(st.session_state, state, district, normalize_fn=portfolio_normalize_fn)

    def _contains(state: str, district: str, block: Optional[str] = None) -> bool:
        try:
            return bool(portfolio_contains(st.session_state, state, district, block_name=block, level=level_norm, normalize_fn=portfolio_normalize_fn))
        except TypeError:
            return bool(portfolio_contains(st.session_state, state, district, normalize_fn=portfolio_normalize_fn))

    def _set_flash(msg: str, level_: str) -> None:
        st.session_state["_portfolio_flash"] = {"message": msg, "level": level_}

    storage_key = "portfolio_blocks" if is_block else "portfolio_districts"
    portfolio = st.session_state.get(storage_key, [])

    flash = st.session_state.pop("_portfolio_flash", None)
    if flash:
        lvl = flash.get("level", "success")
        msg = flash.get("message", "")
        if lvl == "success":
            st.success(msg)
        elif lvl == "warning":
            st.warning(msg)
        elif lvl == "error":
            st.error(msg)
        else:
            st.info(msg)

    st.markdown("### Your Portfolio")
    render_portfolio_badge(len(portfolio), level=level_norm)

    if portfolio:
        with st.expander(f"Manage portfolio {unit_plural}", expanded=False):
            render_portfolio_list(
                portfolio=portfolio,
                portfolio_remove_fn=portfolio_remove_fn,
                normalize_fn=portfolio_normalize_fn,
                max_visible=8,
                level=level_norm,
            )
            st.markdown("---")
            render_clear_portfolio_button(
                portfolio_count=len(portfolio),
                clear_fn=portfolio_remove_all_fn,
                set_flash_fn=_set_flash,
                level=level_norm,
            )

    st.markdown("---")

    # Split the right panel into a clean, low-scroll flow.
    tab_key = f"portfolio_rhs_tab_{level_norm}"
    default_tab = "Add units" if not portfolio else "Compare"
    if tab_key not in st.session_state:
        st.session_state[tab_key] = default_tab

    tab = st.radio(
        "Portfolio panel",
        options=["Compare", "Add units"],
        key=tab_key,
        horizontal=True,
        label_visibility="collapsed",
    )

    st.markdown("---")

    if tab == "Add units":
        st.markdown(f"#### How to add {unit_plural}")
        st.markdown(
            f"""
        **From the map:** Click any {('block' if is_block else 'district')}, then click **+ Add to portfolio**

        **From rankings:** Use the **+ Add** buttons in the rankings table

        **By coordinates:** Use the location panel below
        """
        )

        render_coordinate_lookup(
            merged=merged,
            level=level_norm,
            portfolio_add_fn=_add,
            set_flash_fn=_set_flash,
        )
        return

    # -------------------------
    # Compare tab
    # -------------------------
    if not portfolio:
        st.info("Your portfolio is empty. Use the **Add units** tab to add districts/blocks.")
        return

    st.markdown("### Portfolio Comparison")

    current_selection = st.session_state.get("portfolio_multiindex_selection", [variable_slug])
    selected_slugs = render_index_selector(
        variables=variables,
        current_slug=variable_slug,
        selected_slugs=current_selection,
    )

    if not selected_slugs:
        st.info("Select at least one index to compare.")
        return

    # --- Scenario mode (single shared control for table + visualizations) ---
    compare_key = f"portfolio_compare_scenarios_{level_norm}"  # kept for backward-compat session state
    scen_key = f"portfolio_scenario_selection_{level_norm}"
    mode_key = f"portfolio_scenario_mode_{level_norm}"

    default_mode = "Compare scenarios" if bool(st.session_state.get(compare_key, False)) else "Single scenario"
    if mode_key not in st.session_state:
        st.session_state[mode_key] = default_mode

    st.radio(
        "Scenario mode",
        options=["Single scenario", "Compare scenarios"],
        horizontal=True,
        key=mode_key,
        help="Single scenario uses the global scenario selector. Compare scenarios expands the table and enables scenario-compare charts.",
    )

    compare_scenarios = str(st.session_state.get(mode_key, "Single scenario")).strip().lower().startswith("compare")
    st.session_state[compare_key] = bool(compare_scenarios)

    sel_scenarios: Optional[List[str]] = None
    if compare_scenarios:
        # Conservative defaults aligned with current data coverage
        base_opts = ["ssp245", "ssp585"]
        if isinstance(sel_scenario, str) and sel_scenario.strip() and sel_scenario not in base_opts:
            base_opts = [sel_scenario.strip()] + base_opts

        default_sel: list[str] = []
        if isinstance(sel_scenario, str) and sel_scenario.strip():
            default_sel.append(sel_scenario.strip())
        if sel_scenario.strip().lower() == "ssp245":
            default_sel.append("ssp585")
        elif sel_scenario.strip().lower() == "ssp585":
            default_sel.append("ssp245")

        # De-dupe while preserving order
        seen = set()
        default_sel = [x for x in default_sel if not (x in seen or seen.add(x))]

        chosen = st.multiselect(
            "Scenarios to compare",
            options=base_opts,
            default=default_sel,
            key=scen_key,
        )

        if not chosen:
            chosen = [sel_scenario]
        sel_scenarios = [str(x).strip() for x in chosen if str(x).strip()]

    cached_df = build_comparison_df(
        portfolio=portfolio,
        selected_slugs=selected_slugs,
        variables=variables,
        index_group_labels=index_group_labels,
        sel_scenario=sel_scenario,
        sel_period=sel_period,
        sel_stat=sel_stat,
        sel_scenarios=sel_scenarios,
        pilot_state=pilot_state,
        data_dir=data_dir,
        load_master_csv_fn=load_master_csv_fn,
        normalize_master_columns_fn=normalize_master_columns_fn,
        parse_master_schema_fn=parse_master_schema_fn,
        resolve_metric_column_fn=resolve_metric_column_fn,
        find_baseline_column_for_stat_fn=find_baseline_column_for_stat_fn,
        risk_class_from_percentile_fn=risk_class_from_percentile_fn,
        normalize_fn=portfolio_normalize_fn,
        build_portfolio_multiindex_df_fn=build_portfolio_multiindex_df_fn,
        level=level_norm,
    )

    scen_caption = ", ".join(sel_scenarios) if sel_scenarios else sel_scenario
    st.caption(
        f"Comparing {len(portfolio)} {unit_plural} across {len(selected_slugs)} indices • "
        f"{scen_caption} • {sel_period} • {sel_stat}"
    )

    if cached_df is not None and not cached_df.empty:
        from india_resilience_tool.app.portfolio_multistate import compute_portfolio_summary_stats

        summary = compute_portfolio_summary_stats(cached_df, level=level_norm)
        c1, c2, c3 = st.columns(3)
        c1.metric("Units", int(summary.get("units_count", 0)))
        c2.metric("States", int(summary.get("states_count", 0)))
        c3.metric("Metrics", int(summary.get("metrics_count", 0)))

        risk_counts = summary.get("risk_counts") or {}
        if isinstance(risk_counts, dict) and risk_counts:
            rc_parts = [f"{k}: {int(v)}" for k, v in risk_counts.items()]
            st.caption("Risk class • " + " • ".join(rc_parts))

        tab_table, tab_viz = st.tabs(["Table", "Visualizations"])
        with tab_table:
            render_comparison_table_ui(cached_df, level=level_norm)

        with tab_viz:
            show_viz_key = f"portfolio_show_visualizations_{level_norm}"
            show_viz = st.checkbox(
                "Enable visualizations",
                value=bool(st.session_state.get(show_viz_key, False)),
                key=show_viz_key,
                help="Charts are generated on demand and can be slower than the table.",
            )
            if show_viz:
                render_portfolio_visualizations(
                    cached_df,
                    default_value_col="Percentile",
                    default_chart_type="heatmap",
                )
            else:
                st.info("Enable visualizations to generate charts for this comparison.")

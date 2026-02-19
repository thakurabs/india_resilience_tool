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
            <span style="font-size: 1.1em;">📋</span>
            <strong>Portfolio empty</strong> — Click {unit_plural} to add
            </div>""",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"""<div style="padding: 8px 12px; background: #e8f4e8; border-radius: 8px;
            text-align: center; color: #2d5a2d;">
            <span style="font-size: 1.1em;">📋</span>
            <strong>{portfolio_count} {unit_singular}{'s' if portfolio_count != 1 else ''}</strong> in portfolio
            </div>""",
            unsafe_allow_html=True,
        )

# =============================================================================
# Portfolio District List
# =============================================================================

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

    # Normalize portfolio items into dicts: {"state": ..., "district": ..., "block": ...}
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

    show_all = st.session_state.get("_portfolio_show_all", False) or len(items) <= max_visible
    display_items = items if show_all else items[:max_visible]

    for d in display_items:
        state_i = d.get("state", "")
        district_i = d.get("district", "")
        block_i = d.get("block", "")

        col1, col2 = st.columns([5, 1])
        with col1:
            if is_block:
                st.markdown(f"**{block_i}** ({district_i}, {state_i})")
            else:
                st.markdown(f"**{district_i}**, {state_i}")
        with col2:
            key_parts = [normalize_fn(state_i), normalize_fn(district_i)]
            if is_block:
                key_parts.append(normalize_fn(block_i))
            key = "btn_portfolio_remove_" + "_".join(key_parts)

            if st.button("×", key=key, help=f"Remove {block_i if is_block else district_i}"):
                try:
                    if is_block:
                        portfolio_remove_fn(state_i, district_i, block_i)
                    else:
                        portfolio_remove_fn(state_i, district_i)
                except TypeError:
                    # Backward-compat: older remove functions only accept (state, district)
                    portfolio_remove_fn(state_i, district_i)
                st.rerun()

    # Show more/less toggle
    if len(items) > max_visible:
        remaining = len(items) - max_visible
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
        if st.button("🗑 Clear all", key="btn_portfolio_remove_all", type="secondary"):
            st.session_state["confirm_clear_portfolio"] = True
            st.rerun()
    else:
        st.warning(f"Remove all {portfolio_count} {unit_plural}?")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✓ Yes, clear", key="btn_portfolio_remove_all_confirm", type="primary"):
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
        st.caption(f"📊 {len(expanded_slugs)} metrics from {len(selected_bundles)} domain(s)")
    else:
        # No bundles selected - fall back to current metric only
        expanded_slugs = [current_slug] if current_slug in available_slugs else []
    
    # --- Optional manual refinement ---
    manual_mode = st.checkbox(
        "Manually refine metric selection",
        value=st.session_state.get("portfolio_manual_refinement", False),
        key="portfolio_manual_refinement",
        help="Enable to manually add/remove individual metrics from the comparison",
    )
    
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

def render_comparison_table(
    *,
    portfolio: Sequence[Any],
    selected_slugs: Sequence[str],
    variables: Mapping[str, Mapping[str, Any]],
    index_group_labels: Mapping[str, str],
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
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
    Render the comparison table with auto-rebuild on changes.

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

    level_norm = (level or "district").strip().lower()
    is_block = level_norm == "block"

    if not portfolio or not selected_slugs:
        return None

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

    context = {
        "level": level_norm,
        "units": [_unit_tuple(d) for d in portfolio],
        "slugs": list(selected_slugs),
        "scenario": sel_scenario,
        "period": sel_period,
        "stat": sel_stat,
    }

    prev_context = st.session_state.get("portfolio_multiindex_context")
    cached_df = st.session_state.get("portfolio_multiindex_df")
    needs_rebuild = cached_df is None or prev_context != context

    # Helper functions
    def _resolve_proc_root_for_slug(slug: str) -> Path:
        # Single source of truth for processed root resolution (supports IRT_PROCESSED_SUBDIR).
        from india_resilience_tool.config.paths import resolve_processed_root

        return resolve_processed_root(slug, data_dir=data_dir, mode="portfolio")

    def _load_master_and_schema_for_slug(slug: str):
        proc_root = _resolve_proc_root_for_slug(slug)
        from paths import resolve_master_metrics_path

        master_path = resolve_master_metrics_path(proc_root / pilot_state, "block" if is_block else "district")

        cache = st.session_state.setdefault("_portfolio_master_cache", {})
        cache_key = f"{slug}::{master_path}"

        try:
            mtime = master_path.stat().st_mtime
        except Exception:
            mtime = None

        entry = cache.get(cache_key)
        if entry and entry.get("mtime") == mtime:
            return entry["df"], entry["schema_items"], entry["metrics"], entry["by_metric"]

        if not master_path.exists():
            empty = pd.DataFrame()
            cache[cache_key] = {"df": empty, "schema_items": [], "metrics": [], "by_metric": {}, "mtime": mtime}
            return empty, [], [], {}

        df = load_master_csv_fn(str(master_path))
        df = normalize_master_columns_fn(df)

        # Your schema parser expects columns, keep that contract
        schema_items, metrics, by_metric = parse_master_schema_fn(df.columns)

        cache[cache_key] = {"df": df, "schema_items": schema_items, "metrics": metrics, "by_metric": by_metric, "mtime": mtime}
        return df, schema_items, metrics, by_metric

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

            # Prefer passing level if the builder supports it; fallback if not
            try:
                df = build_portfolio_multiindex_df_fn(**kwargs, level=level_norm)
            except TypeError:
                df = build_portfolio_multiindex_df_fn(**kwargs)

            st.session_state["portfolio_multiindex_df"] = df
            st.session_state["portfolio_multiindex_context"] = context
            cached_df = df

    # Display table
    if cached_df is not None and not cached_df.empty:
        # Reorder columns for nicer UX (esp. block mode)
        display_df = cached_df.copy()
        preferred: list[str] = []

        for c in ("State", "District"):
            if c in display_df.columns:
                preferred.append(c)

        if is_block and "Block" in display_df.columns:
            preferred.append("Block")

        for c in (
            "Index",
            "Group",
            "Current value",
            "Baseline",
            "Δ",
            "%Δ",
            "Rank in state",
            "Percentile",
            "Risk class",
        ):
            if c in display_df.columns and c not in preferred:
                preferred.append(c)

        remaining = [c for c in display_df.columns if c not in preferred]
        display_df = display_df[preferred + remaining]

        st.dataframe(display_df, hide_index=True, use_container_width=True)

        st.download_button(
            "⬇️ Download as CSV",
            data=display_df.to_csv(index=False).encode("utf-8"),
            file_name=f"portfolio_comparison_{level_norm}.csv",
            mime="text/csv",
        )

        return cached_df

    return None


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
    "heatmap": "🔥 Heatmap",
    "grouped_bar": "📊 Grouped Bar Chart",
    "both": "📈 Both Charts",
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
    
    # Check minimum data requirements
    n_districts = df["District"].nunique() if "District" in df.columns else 0
    n_indices = df["Index"].nunique() if "Index" in df.columns else 0
    
    if n_districts < 1 or n_indices < 1:
        st.info("Need at least 1 district and 1 index for visualizations.")
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
    
    # Controls row
    col1, col2 = st.columns(2)
    
    with col1:
        # Chart type selector
        chart_type = st.selectbox(
            "Chart type",
            options=list(CHART_TYPES.keys()),
            format_func=lambda x: CHART_TYPES[x],
            index=list(CHART_TYPES.keys()).index(default_chart_type) if default_chart_type in CHART_TYPES else 0,
            key="_viz_chart_type",
        )
    
    with col2:
        # Value column selector
        value_col_options = list(available_value_cols.keys())
        default_idx = value_col_options.index(default_value_col) if default_value_col in value_col_options else 0
        
        value_col = st.selectbox(
            "Show values",
            options=value_col_options,
            format_func=lambda x: available_value_cols[x],
            index=default_idx,
            key="_viz_value_col",
        )
    
    # Import chart functions
    from india_resilience_tool.viz.charts import (
        make_portfolio_heatmap,
        make_portfolio_grouped_bar,
    )
    
    # Render selected chart(s)
    if chart_type in ("heatmap", "both"):
        _render_heatmap_section(df, value_col, make_portfolio_heatmap)
    
    if chart_type in ("grouped_bar", "both"):
        _render_grouped_bar_section(df, value_col, make_portfolio_grouped_bar)


def _render_heatmap_section(
    df: pd.DataFrame,
    value_col: str,
    make_heatmap_fn: Callable,
) -> None:
    """Render the heatmap visualization section."""
    import streamlit as st
    
    st.markdown("#### 🔥 Heatmap")
    
    # Heatmap-specific options
    with st.expander("Heatmap options", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            normalize = st.checkbox(
                "Normalize per index",
                value=True if value_col == "Current value" else False,
                disabled=value_col != "Current value",
                help="Scale values within each index column for better comparison",
                key="_heatmap_normalize",
            )
        with col2:
            cmap = st.selectbox(
                "Color scheme",
                options=["RdYlGn_r", "RdYlBu_r", "YlOrRd", "Blues", "Reds", "viridis"],
                index=0,
                key="_heatmap_cmap",
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
) -> None:
    """Render the grouped bar chart visualization section."""
    import streamlit as st
    
    st.markdown("#### 📊 Grouped Bar Chart")
    
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
                    key="_bar_max_districts",
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
                    key="_bar_max_indices",
                )
        with col3:
            horizontal = st.checkbox(
                "Horizontal bars",
                value=n_districts > 5,
                help="Better for many districts",
                key="_bar_horizontal",
            )
        
        show_values = st.checkbox(
            "Show values on bars",
            value=True,
            key="_bar_show_values",
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
            f"⬇️ {label}",
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

    st.markdown("### 📋 Your Portfolio")
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

    if not portfolio:
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

    if portfolio:
        st.markdown("### 📊 Portfolio Comparison")

        current_selection = st.session_state.get("portfolio_multiindex_selection", [variable_slug])
        selected_slugs = render_index_selector(
            variables=variables,
            current_slug=variable_slug,
            selected_slugs=current_selection,
        )

        if not selected_slugs:
            st.info("Select at least one index to compare.")
        else:
            cached_df = render_comparison_table(
                portfolio=portfolio,
                selected_slugs=selected_slugs,
                variables=variables,
                index_group_labels=index_group_labels,
                sel_scenario=sel_scenario,
                sel_period=sel_period,
                sel_stat=sel_stat,
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

            st.caption(
                f"Comparing {len(portfolio)} {unit_plural} across {len(selected_slugs)} indices • "
                f"{sel_scenario} • {sel_period} • {sel_stat}"
            )

            if cached_df is not None and not cached_df.empty:
                st.markdown("---")
                with st.expander("📈 Visualizations", expanded=True):
                    render_portfolio_visualizations(
                        cached_df,
                        default_value_col="Percentile",
                        default_chart_type="heatmap",
                    )

        st.markdown("---")
        render_coordinate_lookup(
            merged=merged,
            level=level_norm,
            portfolio_add_fn=_add,
            set_flash_fn=_set_flash,
        )

"""
Multi-district portfolio panel (right column) for IRT.

This module renders the portfolio-mode right-column UI including:
- Portfolio summary badge (always visible)
- Portfolio district list with remove buttons
- Index selection for comparison
- Multi-index comparison table (auto-rebuilds)
- Coordinate-based district lookup

Key improvements over previous version:
- Removed mandatory route selection (rankings/map/saved_points)
- All add methods available simultaneously
- Auto-rebuild comparison table on changes
- Portfolio list always visible
- Simplified point selection integrated

Widget keys preserved:
- portfolio_multiindex_selection
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

def render_portfolio_badge(portfolio_count: int) -> None:
    """
    Render a compact portfolio summary badge.
    """
    import streamlit as st
    
    if portfolio_count == 0:
        st.markdown(
            """<div style="padding: 8px 12px; background: #f0f2f6; border-radius: 8px; 
            text-align: center; color: #666;">
            <span style="font-size: 1.1em;">📋</span> 
            <strong>Portfolio empty</strong> — Click districts to add
            </div>""",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"""<div style="padding: 8px 12px; background: #e8f4e8; border-radius: 8px; 
            text-align: center; color: #2d5a2d;">
            <span style="font-size: 1.1em;">📋</span> 
            <strong>{portfolio_count} district{'s' if portfolio_count != 1 else ''}</strong> in portfolio
            </div>""",
            unsafe_allow_html=True,
        )


# =============================================================================
# Portfolio District List
# =============================================================================

def render_portfolio_list(
    *,
    portfolio: Sequence[Any],
    portfolio_remove_fn: Callable[[str, str], None],
    normalize_fn: Callable[[str], str],
    max_visible: int = 8,
) -> None:
    """
    Render the portfolio district list with remove buttons.
    """
    import streamlit as st
    
    if not portfolio:
        st.caption("No districts selected yet. Add districts from the map or rankings table.")
        return
    
    # Convert to list of dicts
    districts = []
    for d in portfolio:
        if isinstance(d, dict):
            districts.append(d)
        elif isinstance(d, (list, tuple)) and len(d) >= 2:
            districts.append({"state": d[0], "district": d[1]})
    
    # Determine what to show
    show_all = st.session_state.get("_portfolio_show_all", False) or len(districts) <= max_visible
    display_districts = districts if show_all else districts[:max_visible]
    
    for d in display_districts:
        state_i = str(d.get("state", "")).strip()
        district_i = str(d.get("district", "")).strip()
        
        col1, col2 = st.columns([5, 1])
        with col1:
            st.markdown(f"**{district_i}**, {state_i}")
        with col2:
            key = f"btn_portfolio_remove_{normalize_fn(state_i)}_{normalize_fn(district_i)}"
            if st.button("×", key=key, help=f"Remove {district_i}"):
                portfolio_remove_fn(state_i, district_i)
                st.rerun()
    
    # Show more/less toggle
    if len(districts) > max_visible:
        remaining = len(districts) - max_visible
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
) -> None:
    """Render clear all button with inline confirmation."""
    import streamlit as st
    
    if portfolio_count == 0:
        return
    
    confirm_state = st.session_state.get("confirm_clear_portfolio", False)
    
    if not confirm_state:
        if st.button("🗑 Clear all", key="btn_portfolio_remove_all", type="secondary"):
            st.session_state["confirm_clear_portfolio"] = True
            st.rerun()
    else:
        st.warning(f"Remove all {portfolio_count} districts?")
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
# Index Selector
# =============================================================================

def render_index_selector(
    *,
    variables: Mapping[str, Mapping[str, Any]],
    current_slug: str,
    selected_slugs: List[str],
) -> List[str]:
    """
    Render index multi-select for portfolio comparison.
    
    Ensures the current index is always selected by default.
    """
    import streamlit as st
    
    available = [(slug, meta["label"]) for slug, meta in variables.items()]
    available_slugs = [s for s, _ in available]
    
    # Determine default: use existing selection if valid, otherwise current slug
    if selected_slugs and any(s in available_slugs for s in selected_slugs):
        default = [s for s in selected_slugs if s in available_slugs]
    else:
        # Default to current index
        default = [current_slug] if current_slug in available_slugs else []
    
    # Ensure we always have at least the current slug selected initially
    if not default and current_slug in available_slugs:
        default = [current_slug]
    
    selected = st.multiselect(
        "Compare across indices",
        options=available_slugs,
        default=default,
        format_func=lambda s: variables[s]["label"] if s in variables else s,
        key="portfolio_multiindex_selection",
        help="Select one or more climate indices to compare across your portfolio districts",
    )
    
    # If user clears all selections, return current slug as fallback
    # This ensures table always has something to show
    if not selected and current_slug in available_slugs:
        return [current_slug]
    
    return selected


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
) -> Optional[pd.DataFrame]:
    """
    Render the comparison table with auto-rebuild on changes.
    """
    import streamlit as st
    import os
    from india_resilience_tool.analysis.metrics import compute_rank_and_percentile
    
    if not portfolio or not selected_slugs:
        return None
    
    # Build context for cache invalidation
    context = {
        "districts": [
            (d.get("state"), d.get("district")) if isinstance(d, dict) else tuple(d[:2])
            for d in portfolio
        ],
        "slugs": list(selected_slugs),
        "scenario": sel_scenario,
        "period": sel_period,
        "stat": sel_stat,
    }
    
    # Check if rebuild needed
    prev_context = st.session_state.get("portfolio_multiindex_context")
    cached_df = st.session_state.get("portfolio_multiindex_df")
    needs_rebuild = cached_df is None or prev_context != context
    
    # Helper functions
    def _resolve_proc_root_for_slug(slug: str) -> Path:
        env_root = os.getenv("IRT_PROCESSED_ROOT")
        if env_root:
            base_path = Path(env_root)
            if base_path.name == slug:
                return base_path.resolve()
            return (base_path / slug).resolve()
        return (data_dir / "processed" / slug).resolve()

    def _load_master_and_schema_for_slug(slug: str):
        proc_root = _resolve_proc_root_for_slug(slug)
        master_path = proc_root / pilot_state / "master_metrics_by_district.csv"
        
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
        schema, metrics, by_metric = parse_master_schema_fn(df.columns)
        
        cache[cache_key] = {"df": df, "schema_items": schema, "metrics": metrics, "by_metric": by_metric, "mtime": mtime}
        return df, schema, metrics, by_metric

    def _match_row_idx(df_local, st_name, dist_name):
        if df_local is None or df_local.empty:
            return None
        if "state" not in df_local.columns or "district" not in df_local.columns:
            return None
        
        st_norm = normalize_fn(st_name)
        dist_norm = normalize_fn(dist_name)
        state_col = df_local["state"].astype(str).map(normalize_fn)
        dist_col = df_local["district"].astype(str).map(normalize_fn)
        
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
        return compute_rank_and_percentile(
            df_local, st_name, metric_col, value,
            state_col="state", normalize_fn=normalize_fn, percentile_method="le"
        )

    # Build or use cached table
    if needs_rebuild:
        with st.spinner("Building comparison table..."):
            df = build_portfolio_multiindex_df_fn(
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
            st.session_state["portfolio_multiindex_df"] = df
            st.session_state["portfolio_multiindex_context"] = context
            cached_df = df
    
    # Display table
    if cached_df is not None and not cached_df.empty:
        st.dataframe(cached_df, hide_index=True, use_container_width=True)
        
        st.download_button(
            "⬇️ Download as CSV",
            data=cached_df.to_csv(index=False).encode("utf-8"),
            file_name="portfolio_comparison.csv",
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
            max_districts = st.slider(
                "Max districts",
                min_value=1,
                max_value=min(15, max(n_districts, 1)),
                value=min(10, n_districts),
                key="_bar_max_districts",
            )
        with col2:
            max_indices = st.slider(
                "Max indices",
                min_value=1,
                max_value=min(10, max(n_indices, 1)),
                value=min(6, n_indices),
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
    portfolio_add_fn: Callable[[str, str], None],
    set_flash_fn: Callable[[str, str], None],
) -> None:
    """
    Render coordinate-based district lookup.
    
    This wraps the full render_point_selection_panel from point_selection_ui.py
    which includes:
    - Single coordinate entry with preview
    - Batch coordinate input (paste multiple)
    - Show on map functionality
    - Saved points management
    """
    from india_resilience_tool.app.point_selection_ui import render_point_selection_panel
    
    # Create a key function (required by render_point_selection_panel but not used for much)
    def _portfolio_key_fn(state: str, district: str) -> tuple:
        return (state.lower().replace(" ", ""), district.lower().replace(" ", ""))
    
    render_point_selection_panel(
        merged=merged,
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
    portfolio_remove_fn: Callable[[str, str], None],
    portfolio_remove_all_fn: Callable[[], None],
    build_portfolio_multiindex_df_fn: Callable[..., pd.DataFrame],
) -> None:
    """
    Render the complete multi-district portfolio panel.
    
    Note: portfolio_route parameter is kept for backward compatibility but ignored.
    The new design doesn't require mandatory route selection.
    """
    import streamlit as st
    from india_resilience_tool.analysis.portfolio import portfolio_add, portfolio_contains
    
    # Helper functions for add/contains
    def _add(state: str, district: str) -> None:
        portfolio_add(st.session_state, state, district, normalize_fn=portfolio_normalize_fn)
    
    def _contains(state: str, district: str) -> bool:
        return portfolio_contains(st.session_state, state, district, normalize_fn=portfolio_normalize_fn)
    
    def _set_flash(msg: str, level: str) -> None:
        st.session_state["_portfolio_flash"] = {"message": msg, "level": level}
    
    portfolio = st.session_state.get("portfolio_districts", [])
    
    # Flash messages
    flash = st.session_state.pop("_portfolio_flash", None)
    if flash:
        level = flash.get("level", "success")
        msg = flash.get("message", "")
        if level == "success":
            st.success(msg)
        elif level == "warning":
            st.warning(msg)
        elif level == "error":
            st.error(msg)
        else:
            st.info(msg)
    
    # Section 1: Portfolio Summary
    st.markdown("### 📋 Your Portfolio")
    render_portfolio_badge(len(portfolio))
    
    if portfolio:
        with st.expander("Manage portfolio districts", expanded=False):
            render_portfolio_list(
                portfolio=portfolio,
                portfolio_remove_fn=portfolio_remove_fn,
                normalize_fn=portfolio_normalize_fn,
                max_visible=8,
            )
            st.markdown("---")
            render_clear_portfolio_button(
                portfolio_count=len(portfolio),
                clear_fn=portfolio_remove_all_fn,
                set_flash_fn=_set_flash,
            )
    
    st.markdown("---")
    
    # Section 2: How to Add (when empty)
    if not portfolio:
        st.markdown("#### How to add districts")
        st.markdown("""
        **From the map:** Click any district, then click **+ Add to portfolio**
        
        **From rankings:** Use the **+ Add** buttons in the rankings table
        
        **By coordinates:** Use the location panel below
        """)
        
        render_coordinate_lookup(
            merged=merged,
            portfolio_add_fn=_add,
            set_flash_fn=_set_flash,
        )
    
    # Section 3: Comparison Analysis (when portfolio has districts)
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
            )
            
            st.caption(
                f"Comparing {len(portfolio)} districts across {len(selected_slugs)} indices • "
                f"{sel_scenario} • {sel_period} • {sel_stat}"
            )
            
            # Section 4: Visualizations (when we have comparison data)
            if cached_df is not None and not cached_df.empty:
                st.markdown("---")
                with st.expander("📈 Visualizations", expanded=True):
                    render_portfolio_visualizations(
                        cached_df,
                        default_value_col="Percentile",
                        default_chart_type="heatmap",
                    )
        
        # Coordinate lookup also available when portfolio not empty
        st.markdown("---")
        render_coordinate_lookup(
            merged=merged,
            portfolio_add_fn=_add,
            set_flash_fn=_set_flash,
        )
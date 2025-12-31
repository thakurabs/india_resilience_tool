"""
Rankings table view for IRT.

This module provides:
- District rankings table with sorting options
- Per-row add/remove buttons in portfolio mode
- Visual indicators for portfolio membership
- Download functionality

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Any, Callable, Mapping, Optional

import pandas as pd


def render_rankings_view(
    *,
    view: str,
    table_df: Optional[pd.DataFrame],
    has_baseline: bool,
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    selected_state: str,
    portfolio_add: Callable[..., None],
    portfolio_contains: Optional[Callable[..., bool]] = None,
    portfolio_remove: Optional[Callable[..., None]] = None,
    level: str = "district",
) -> None:
    """
    Render the Rankings view.

    In portfolio mode, shows per-row add/remove buttons instead of
    the checkbox + batch button approach.

    Contract:
    - Preserves widget key "rank_mode"
    - Detects portfolio mode from session_state["analysis_mode"]
    """
    import streamlit as st

    if view != "📊 Rankings table":
        return

    level_norm = str(level).strip().lower()
    st.subheader("Block Rankings" if level_norm == "block" else "District Rankings")

    if table_df is None or table_df.empty:
        st.caption("No ranking data available for this selection.")
        return

    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")
    is_portfolio_mode = analysis_mode == "Multi-district portfolio"

    if is_portfolio_mode:
        _render_portfolio_rankings(
            table_df=table_df,
            has_baseline=has_baseline,
            variables=variables,
            variable_slug=variable_slug,
            sel_scenario=sel_scenario,
            sel_period=sel_period,
            sel_stat=sel_stat,
            selected_state=selected_state,
            portfolio_add_fn=portfolio_add,
            portfolio_contains_fn=portfolio_contains,
            portfolio_remove_fn=portfolio_remove,
            level=level,
        )
    else:
        _render_simple_rankings(
            table_df=table_df,
            has_baseline=has_baseline,
            variables=variables,
            variable_slug=variable_slug,
            sel_scenario=sel_scenario,
            sel_period=sel_period,
            sel_stat=sel_stat,
            selected_state=selected_state,
            level=level,
        )



def _render_simple_rankings(
    *,
    table_df: pd.DataFrame,
    has_baseline: bool,
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    selected_state: str,
    level: str = "district",
) -> None:
    """Render simple rankings view for non-portfolio mode."""
    import streamlit as st

    # Mode selector
    options = ["Top 20 biggest increases", "All"]
    rank_mode = st.radio(
        "Show:",
        options=options,
        index=0,
        key="rank_mode",
        horizontal=True,
    )

    df = table_df.copy()

    if rank_mode == "Top 20 biggest increases":
        if has_baseline and "rank_delta" in df.columns:
            df = df.dropna(subset=["delta_abs"]).copy()
            if df.empty:
                st.info("No valid baseline/change values. Showing by value instead.")
                df = table_df.sort_values("value", ascending=False).head(20)
            else:
                df = df.sort_values("rank_delta").head(20)
        else:
            st.info("Baseline not available. Showing by value instead.")
            if "rank_value" in df.columns:
                df = df.sort_values("rank_value").head(20)
            else:
                df = df.sort_values("value", ascending=False).head(20)
    else:
        if "rank_value" in df.columns:
            df = df.sort_values("rank_value")
        else:
            df = df.sort_values("value", ascending=False)

    # Build display columns
    level_norm = str(level).strip().lower()
    if level_norm == "block" and "block_name" in df.columns:
        display_cols = ["rank_value", "block_name", "district_name", "state_name", "value"]
    else:
        display_cols = ["rank_value", "district_name", "state_name", "value"]
    if has_baseline and "baseline" in df.columns:
        display_cols += ["delta_abs", "delta_pct"]
    if "percentile_value" in df.columns:
        display_cols.append("percentile_value")
    if "risk_class" in df.columns:
        display_cols.append("risk_class")
    if "aspirational" in df.columns:
        display_cols.append("aspirational")

    display_cols = [c for c in display_cols if c in df.columns]

    df_display = df[display_cols].rename(columns={
        "rank_value": "Rank (value)",
        "block_name": "Block",
        "district_name": "District",
        "state_name": "State",
        "value": "Index value",
        "baseline": "Baseline (1990–2010)",
        "delta_abs": "Δ vs baseline",
        "delta_pct": "%Δ vs baseline",
        "percentile_value": "Percentile",
        "risk_class": "Risk class",
        "aspirational": "Aspirational",
    })

    st.dataframe(df_display, hide_index=True, use_container_width=True)

    metric_label = variables.get(variable_slug, {}).get("label", variable_slug)
    st.caption(
        f"**{metric_label}** • {sel_scenario} • {sel_period} • {sel_stat}"
        + (f" • Filtered to {selected_state}" if selected_state != "All" else "")
    )


def _render_portfolio_rankings(
    *,
    table_df: pd.DataFrame,
    has_baseline: bool,
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    selected_state: str,
    portfolio_add_fn: Callable[..., None],
    portfolio_contains_fn: Optional[Callable[..., bool]] = None,
    portfolio_remove_fn: Optional[Callable[..., None]] = None,
    level: str = "district",
) -> None:
    """Render rankings view with per-row add/remove buttons using st.data_editor."""
    import streamlit as st
    from india_resilience_tool.utils.naming import alias

    level_norm = str(level).strip().lower()
    unit_label = "block" if level_norm == "block" else "district"
    plural_label = f"{unit_label}s"

    def _normalize(s: str) -> str:
        return alias(s).replace(" ", "")

    # Default to district-level portfolio functions if not provided
    if portfolio_contains_fn is None or portfolio_remove_fn is None:
        from india_resilience_tool.analysis.portfolio import portfolio_contains, portfolio_remove

        if portfolio_contains_fn is None:
            def portfolio_contains_fn(state: str, district: str, block: Optional[str] = None) -> bool:  # type: ignore[misc]
                return portfolio_contains(st.session_state, state, district, normalize_fn=_normalize)

        if portfolio_remove_fn is None:
            def portfolio_remove_fn(state: str, district: str, block: Optional[str] = None) -> None:  # type: ignore[misc]
                portfolio_remove(st.session_state, state, district, normalize_fn=_normalize)

    # Work on a copy
    df_to_show = table_df.copy()

    # Mode selector - use same options as original
    options = ["Top 20 biggest increases", "All"]
    rank_mode = st.radio(
        "Show:",
        options=options,
        index=0,
        key="rank_mode",
        horizontal=True,
    )

    if rank_mode == "Top 20 biggest increases":
        if has_baseline and "rank_delta" in df_to_show.columns:
            df_to_show = df_to_show.dropna(subset=["delta_abs"]).copy()
            if df_to_show.empty:
                st.info("No valid baseline/change values to rank by increase.")
            else:
                df_to_show = df_to_show.sort_values("rank_delta").head(20)
        else:
            st.info("Baseline not available for this index/stat; showing absolute-value ranking instead.")
            if "rank_value" in df_to_show.columns:
                df_to_show = df_to_show.sort_values("rank_value").head(20)
            else:
                df_to_show = df_to_show.sort_values("value", ascending=False).head(20)
    else:
        if "rank_value" in df_to_show.columns:
            df_to_show = df_to_show.sort_values("rank_value")
        else:
            df_to_show = df_to_show.sort_values("value", ascending=False)

    # Determine name columns
    is_block_table = level_norm == "block" and "block_name" in df_to_show.columns

    # Add portfolio status columns
    df_port = df_to_show.copy()
    in_portfolio_status: list[bool] = []

    for _, row in df_port.iterrows():
        state = str(row.get("state_name", "")).strip()
        district = str(row.get("district_name", "")).strip()
        block = str(row.get("block_name", "")).strip() if is_block_table else None

        if not state or not district:
            in_portfolio_status.append(False)
            continue

        if is_block_table and block:
            in_portfolio_status.append(bool(portfolio_contains_fn(state, district, block)))
        else:
            in_portfolio_status.append(bool(portfolio_contains_fn(state, district)))

    df_port.insert(0, "In portfolio", in_portfolio_status)
    df_port["Add to portfolio"] = False  # Checkbox column for adding

    # Portfolio stats
    portfolio_state_key = "portfolio_blocks" if level_norm == "block" else "portfolio_districts"
    portfolio_items = st.session_state.get(portfolio_state_key, [])
    if not isinstance(portfolio_items, list):
        portfolio_items = []

    st.markdown(
        f"<div style='text-align: right; color: #666; margin-bottom: 8px;'>"
        f"📋 {len(portfolio_items)} {unit_label}{'s' if len(portfolio_items) != 1 else ''} in portfolio"
        f"</div>",
        unsafe_allow_html=True,
    )

    edited_df = st.data_editor(
        df_port,
        use_container_width=True,
        key=f"rankings_portfolio_editor_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}_{level_norm}",
        num_rows="fixed",
        disabled=[c for c in df_port.columns if c not in ("Add to portfolio",)],
        column_config={
            "In portfolio": st.column_config.CheckboxColumn(
                "✓",
                help="Already in portfolio",
                disabled=True,
            ),
            "Add to portfolio": st.column_config.CheckboxColumn(
                "Add",
                help=f"Check to add {unit_label} to portfolio",
            ),
        },
        hide_index=True,
    )

    metric_label = variables.get(variable_slug, {}).get("label", variable_slug)
    caption_text = (
        f"**{metric_label}** • {sel_scenario} • {sel_period} • {sel_stat}"
        + (f" • Filtered to {selected_state}" if selected_state != "All" else "")
    )
    st.caption(caption_text)

    st.markdown("---")

    if st.button(
        f"➕ Add checked {plural_label} to portfolio",
        key=f"btn_add_portfolio_from_table_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}_{level_norm}",
        type="primary",
    ):
        added = 0

        for _, row in edited_df.iterrows():
            if not bool(row.get("Add to portfolio")):
                continue

            state_label = str(row.get("state_name", "")).strip()
            district_label = str(row.get("district_name", "")).strip()
            if not state_label or not district_label:
                continue

            if is_block_table:
                block_label = str(row.get("block_name", "")).strip()
                if not block_label:
                    continue
                if not bool(portfolio_contains_fn(state_label, district_label, block_label)):
                    portfolio_add_fn(state_label, district_label, block_label)
                    added += 1
            else:
                if not bool(portfolio_contains_fn(state_label, district_label)):
                    portfolio_add_fn(state_label, district_label)
                    added += 1

        if added > 0:
            st.success(f"Added {added} {unit_label}{'s' if added != 1 else ''} to portfolio.")
            st.rerun()
        else:
            st.info(f"No new {plural_label} were added (they may already be in the portfolio).")

    from india_resilience_tool.utils.naming import alias

    def _normalize(s: str) -> str:
        return alias(s).replace(" ", "")

    def _contains(state: str, district: str) -> bool:
        return portfolio_contains(st.session_state, state, district, normalize_fn=_normalize)

    def _remove(state: str, district: str) -> None:
        portfolio_remove(st.session_state, state, district, normalize_fn=_normalize)

    # Mode selector - use same options as original
    options = ["Top 20 biggest increases", "All"]
    rank_mode = st.radio(
        "Show:",
        options=options,
        index=0,
        key="rank_mode",
        horizontal=True,
    )

    df_to_show = table_df.copy()

    if rank_mode == "Top 20 biggest increases":
        if has_baseline and "rank_delta" in df_to_show.columns:
            df_to_show = df_to_show.dropna(subset=["delta_abs"]).copy()
            if df_to_show.empty:
                st.info("No valid baseline/change values to rank by increase.")
            else:
                df_to_show = df_to_show.sort_values("rank_delta").head(20)
        else:
            st.info("Baseline not available for this index/stat; showing absolute-value ranking instead.")
            if "rank_value" in df_to_show.columns:
                df_to_show = df_to_show.sort_values("rank_value").head(20)
            else:
                df_to_show = df_to_show.sort_values("value", ascending=False).head(20)
    else:
        if "rank_value" in df_to_show.columns:
            df_to_show = df_to_show.sort_values("rank_value")
        else:
            df_to_show = df_to_show.sort_values("value", ascending=False)

    # Build display columns - include all relevant columns like original
    display_cols = ["rank_value", "district_name", "state_name", "value"]
    if has_baseline and "baseline" in df_to_show.columns:
        display_cols += ["delta_abs", "delta_pct"]
    if "percentile_value" in df_to_show.columns:
        display_cols.append("percentile_value")
    if "risk_class" in df_to_show.columns:
        display_cols.append("risk_class")
    if "aspirational" in df_to_show.columns:
        display_cols.append("aspirational")

    display_cols = [c for c in display_cols if c in df_to_show.columns]

    # Reset index to ensure alignment between df_to_show and df_display/df_port
    df_to_show = df_to_show.reset_index(drop=True)

    df_display = df_to_show[display_cols].rename(
        columns={
            "rank_value": "Rank (value)",
            "district_name": "District",
            "state_name": "State",
            "value": "Index value",
            "baseline": "Baseline (1990–2010)",
            "delta_abs": "Δ vs baseline",
            "delta_pct": "%Δ vs baseline",
            "percentile_value": "Percentile",
            "risk_class": "Risk class",
            "aspirational": "Aspirational",
        }
    )

    metric_label = variables.get(variable_slug, {}).get("label", variable_slug)
    caption_text = (
        f"Ranking based on **{metric_label}**, "
        f"**{sel_scenario}**, **{sel_period}**, **{sel_stat}**. "
        f"Change vs baseline uses historical **1990–2010** where available. "
        + (f"Filtered to state: **{selected_state}**." if selected_state != "All" else "Showing all states.")
    )

    # Add "In portfolio" column to show current status
    df_port = df_display.copy()
    
    # Check portfolio status for each row
    in_portfolio_status = []
    for _, row in df_to_show.iterrows():
        district = str(row.get("district_name", "")).strip()
        state = str(row.get("state_name", "")).strip()
        in_portfolio_status.append(_contains(state, district))
    
    df_port.insert(0, "In portfolio", in_portfolio_status)
    df_port["Add to portfolio"] = False  # Checkbox column for adding

    # Portfolio stats
    portfolio = st.session_state.get("portfolio_districts", [])
    st.markdown(
        f"<div style='text-align: right; color: #666; margin-bottom: 8px;'>"
        f"📋 {len(portfolio)} district{'s' if len(portfolio) != 1 else ''} in portfolio"
        f"</div>",
        unsafe_allow_html=True,
    )

    # Use data_editor for better table experience
    edited_df = st.data_editor(
        df_port,
        use_container_width=True,
        key=f"rankings_portfolio_editor_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}",
        num_rows="fixed",
        disabled=[c for c in df_port.columns if c not in ("Add to portfolio",)],
        column_config={
            "In portfolio": st.column_config.CheckboxColumn(
                "✓",
                help="Already in portfolio",
                disabled=True,
            ),
            "Add to portfolio": st.column_config.CheckboxColumn(
                "Add",
                help="Check to add to portfolio",
            ),
            "Percentile": st.column_config.NumberColumn(
                "Percentile",
                format="%.1f",
            ),
            "Index value": st.column_config.NumberColumn(
                "Index value",
                format="%.2f",
            ),
            "Δ vs baseline": st.column_config.NumberColumn(
                "Δ vs baseline",
                format="%.2f",
            ),
            "%Δ vs baseline": st.column_config.NumberColumn(
                "%Δ vs baseline",
                format="%.1f%%",
            ),
        },
        hide_index=True,
    )

    st.caption(caption_text)

    st.markdown("---")

    # Add button to add checked districts
    col1, col2 = st.columns([2, 1])
    with col1:
        if st.button(
            "➕ Add checked districts to portfolio",
            key=f"btn_add_portfolio_from_table_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}",
            type="primary",
        ):
            added = 0
            
            for i, row in edited_df.iterrows():
                if not row.get("Add to portfolio"):
                    continue
                # Get original data for this row - indices are aligned since we reset earlier
                if i >= len(df_to_show):
                    continue
                orig_row = df_to_show.iloc[i]
                district_label = str(orig_row.get("district_name", "")).strip()
                state_label = str(orig_row.get("state_name", "")).strip()
                if not district_label or not state_label:
                    continue
                if not _contains(state_label, district_label):
                    portfolio_add_fn(state_label, district_label)
                    added += 1

            if added > 0:
                st.success(f"Added {added} district(s) to portfolio.")
                st.rerun()
            else:
                st.info("No new districts were added (they may already be in the portfolio).")

    # Show current portfolio summary
    if portfolio:
        with col2:
            st.markdown(f"**{len(portfolio)}** in portfolio")
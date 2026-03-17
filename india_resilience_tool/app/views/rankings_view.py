"""
Rankings table view for IRT.

This module provides:
- District/Block rankings table with sorting options
- Per-row add/remove buttons in portfolio mode
- Visual indicators for portfolio membership
- Download functionality

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Any, Callable, Literal, Mapping, Optional

import pandas as pd

from india_resilience_tool.app.state import VIEW_RANKINGS

AdminLevel = Literal["district", "block", "basin", "sub_basin"]


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
    level: AdminLevel = "district",
) -> None:
    """
    Render the Rankings view.

    In portfolio mode, shows per-row add/remove buttons instead of
    the checkbox + batch button approach.

    Contract:
    - Detects portfolio mode from session_state["analysis_mode"]
    - Uses unique widget keys that include level to avoid collisions
    """
    import streamlit as st

    if view != VIEW_RANKINGS:
        return

    level_norm = str(level).strip().lower()
    if level_norm == "sub_basin":
        unit_label = "Sub-basin"
    elif level_norm == "basin":
        unit_label = "Basin"
    else:
        unit_label = "Block" if level_norm == "block" else "District"
    st.subheader(f"{unit_label} Rankings")

    if table_df is None or table_df.empty:
        st.caption("No ranking data available for this selection.")
        return

    if level_norm == "sub_basin":
        default_mode = "Single sub-basin focus"
    elif level_norm == "basin":
        default_mode = "Single basin focus"
    else:
        default_mode = "Single block focus" if level_norm == "block" else "Single district focus"
    analysis_mode = st.session_state.get("analysis_mode", default_mode)
    is_portfolio_mode = "Multi" in str(analysis_mode)

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
            level=level_norm,
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
            level=level_norm,
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

    level_norm = str(level).strip().lower()

    # Mode selector - unique key per level
    options = ["Top 20 biggest increases", "All"]
    rank_mode = st.radio(
        "Show:",
        options=options,
        index=0,
        key=f"rank_mode_simple_{level_norm}",
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

    # Build display columns based on level
    if level_norm == "sub_basin" and "subbasin_name" in df.columns:
        display_cols = ["rank_value", "subbasin_name", "basin_name", "value"]
    elif level_norm == "basin" and "basin_name" in df.columns:
        display_cols = ["rank_value", "basin_name", "value"]
    elif level_norm == "block" and "block_name" in df.columns:
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
        "subbasin_name": "Sub-basin",
        "basin_name": "Basin",
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
    from india_resilience_tool.analysis.portfolio import get_portfolio_storage_key

    level_norm = str(level).strip().lower()
    if level_norm == "sub_basin":
        unit_label = "sub-basin"
    elif level_norm == "basin":
        unit_label = "basin"
    else:
        unit_label = "block" if level_norm == "block" else "district"
    plural_label = f"{unit_label}s"

    def _normalize(s: str) -> str:
        return alias(s).replace(" ", "")

    # Default to district-level portfolio functions if not provided
    if portfolio_contains_fn is None or portfolio_remove_fn is None:
        from india_resilience_tool.analysis.portfolio import portfolio_contains, portfolio_remove

        if portfolio_contains_fn is None:
            def portfolio_contains_fn(state: str = "", district: str = "", block: Optional[str] = None, **kwargs: Any) -> bool:  # type: ignore[misc]
                if level_norm == "sub_basin":
                    return portfolio_contains(
                        st.session_state,
                        normalize_fn=_normalize,
                        level="sub_basin",
                        basin_name=str(kwargs.get("basin_name", "")),
                        basin_id=kwargs.get("basin_id"),
                        subbasin_name=str(kwargs.get("subbasin_name", "")),
                        subbasin_id=kwargs.get("subbasin_id"),
                    )
                if level_norm == "basin":
                    return portfolio_contains(
                        st.session_state,
                        normalize_fn=_normalize,
                        level="basin",
                        basin_name=str(kwargs.get("basin_name", "")),
                        basin_id=kwargs.get("basin_id"),
                    )
                if level_norm == "block" and block:
                    return portfolio_contains(
                        st.session_state, state, district, 
                        block_name=block, level="block", normalize_fn=_normalize
                    )
                return portfolio_contains(st.session_state, state, district, normalize_fn=_normalize)

        if portfolio_remove_fn is None:
            def portfolio_remove_fn(state: str = "", district: str = "", block: Optional[str] = None, **kwargs: Any) -> None:  # type: ignore[misc]
                if level_norm == "sub_basin":
                    portfolio_remove(
                        st.session_state,
                        normalize_fn=_normalize,
                        level="sub_basin",
                        basin_name=str(kwargs.get("basin_name", "")),
                        basin_id=kwargs.get("basin_id"),
                        subbasin_name=str(kwargs.get("subbasin_name", "")),
                        subbasin_id=kwargs.get("subbasin_id"),
                    )
                    return
                if level_norm == "basin":
                    portfolio_remove(
                        st.session_state,
                        normalize_fn=_normalize,
                        level="basin",
                        basin_name=str(kwargs.get("basin_name", "")),
                        basin_id=kwargs.get("basin_id"),
                    )
                    return
                if level_norm == "block" and block:
                    portfolio_remove(
                        st.session_state, state, district,
                        block_name=block, level="block", normalize_fn=_normalize
                    )
                else:
                    portfolio_remove(st.session_state, state, district, normalize_fn=_normalize)

    # Work on a copy
    df_to_show = table_df.copy()

    # Filter / search (helps when adding 20–50 units)
    filter_key = f"rankings_filter_{level_norm}"
    q = st.text_input(
        "Filter",
        value=str(st.session_state.get(filter_key, "")),
        key=filter_key,
        placeholder=f"Type to filter {plural_label}…",
    )
    qn = _normalize(q)
    if qn:
        if level_norm == "sub_basin":
            desired = ["basin_name", "subbasin_name"]
        elif level_norm == "basin":
            desired = ["basin_name"]
        else:
            desired = ["state_name", "district_name"]
            if level_norm == "block" and "block_name" in df_to_show.columns:
                desired.append("block_name")
        cols = [c for c in desired if c in df_to_show.columns]
        if cols:
            work = df_to_show[cols].astype(str).apply(lambda s: s.map(_normalize))
            mask = False
            for c in cols:
                mask = mask | work[c].str.contains(qn, na=False)
            df_to_show = df_to_show[mask].copy()
            if df_to_show.empty:
                st.info("No rows match the current filter.")
                return

    # Mode selector - unique key per level
    options = ["Top 20 biggest increases", "All"]
    rank_mode = st.radio(
        "Show:",
        options=options,
        index=0,
        key=f"rank_mode_portfolio_{level_norm}",
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
    is_basin_table = level_norm == "basin" and "basin_name" in df_to_show.columns
    is_subbasin_table = level_norm == "sub_basin" and "subbasin_name" in df_to_show.columns

    # Add portfolio status columns
    df_port = df_to_show.copy()
    in_portfolio_status: list[bool] = []

    for _, row in df_port.iterrows():
        if is_subbasin_table:
            basin = str(row.get("basin_name", "")).strip()
            basin_id = str(row.get("basin_id", "")).strip() or None
            subbasin = str(row.get("subbasin_name", "")).strip()
            subbasin_id = str(row.get("subbasin_id", "")).strip() or None
            if not basin or not subbasin:
                in_portfolio_status.append(False)
                continue
            in_portfolio_status.append(
                bool(
                    portfolio_contains_fn(
                        basin_name=basin,
                        basin_id=basin_id,
                        subbasin_name=subbasin,
                        subbasin_id=subbasin_id,
                    )
                )
            )
            continue

        if is_basin_table:
            basin = str(row.get("basin_name", "")).strip()
            basin_id = str(row.get("basin_id", "")).strip() or None
            if not basin:
                in_portfolio_status.append(False)
                continue
            in_portfolio_status.append(bool(portfolio_contains_fn(basin_name=basin, basin_id=basin_id)))
            continue

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
    portfolio_state_key = get_portfolio_storage_key(level_norm)
    portfolio_items = st.session_state.get(portfolio_state_key, [])
    if not isinstance(portfolio_items, list):
        portfolio_items = []

    st.markdown(
        f"<div style='text-align: right; color: #666; margin-bottom: 8px;'>"
        f"{len(portfolio_items)} {unit_label}{'s' if len(portfolio_items) != 1 else ''} in portfolio"
        f"</div>",
        unsafe_allow_html=True,
    )

    editor_key = f"rankings_portfolio_editor_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}_{level_norm}"
    action_slot = st.empty()

    edited_df = st.data_editor(
        df_port,
        use_container_width=True,
        key=editor_key,
        num_rows="fixed",
        disabled=[c for c in df_port.columns if c not in ("Add to portfolio",)],
        column_config={
            "In portfolio": st.column_config.CheckboxColumn(
                "Yes",
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

    if "Add to portfolio" in edited_df.columns:
        selected_n = int(
            pd.to_numeric(edited_df["Add to portfolio"], errors="coerce")
            .fillna(0)
            .astype(bool)
            .sum()
        )
    else:
        selected_n = 0

    with action_slot.container():
        c1, c2 = st.columns([3, 2])
        with c1:
            st.caption(f"Selected to add: **{selected_n}**")
        with c2:
            if st.button(
                "Clear selections",
                key=f"btn_rankings_clear_add_checks_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}_{level_norm}",
                type="secondary",
                use_container_width=True,
            ):
                st.session_state.pop(editor_key, None)
                st.rerun()

    if st.button(
        f"Add checked {plural_label} to portfolio",
        key=f"btn_add_portfolio_from_table_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}_{level_norm}",
        type="primary",
        disabled=(selected_n == 0),
        use_container_width=True,
    ):
        added = 0

        for _, row in edited_df.iterrows():
            if not bool(row.get("Add to portfolio")):
                continue

            if is_subbasin_table:
                basin_label = str(row.get("basin_name", "")).strip()
                basin_id = str(row.get("basin_id", "")).strip() or None
                subbasin_label = str(row.get("subbasin_name", "")).strip()
                subbasin_id = str(row.get("subbasin_id", "")).strip() or None
                if not basin_label or not subbasin_label:
                    continue
                if not bool(
                    portfolio_contains_fn(
                        basin_name=basin_label,
                        basin_id=basin_id,
                        subbasin_name=subbasin_label,
                        subbasin_id=subbasin_id,
                    )
                ):
                    portfolio_add_fn(
                        basin_name=basin_label,
                        basin_id=basin_id,
                        subbasin_name=subbasin_label,
                        subbasin_id=subbasin_id,
                    )
                    added += 1
                continue

            if is_basin_table:
                basin_label = str(row.get("basin_name", "")).strip()
                basin_id = str(row.get("basin_id", "")).strip() or None
                if not basin_label:
                    continue
                if not bool(portfolio_contains_fn(basin_name=basin_label, basin_id=basin_id)):
                    portfolio_add_fn(basin_name=basin_label, basin_id=basin_id)
                    added += 1
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

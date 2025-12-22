"""
Rankings table view (full UI).

Includes:
- rank-mode selector ("Top 20 biggest increases" vs "All") with key="rank_mode"
- display column selection + renaming
- portfolio-mode table editor + add button + portfolio summary
- single-district mode simple dataframe

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
    portfolio_add: Callable[[str, str], None],
) -> None:
    """
    Render the Rankings view. Designed to be called from dashboard_unfactored_impl.py.

    Contract:
    - Preserves widget keys:
        - "rank_mode"
        - rankings editor key: f"rankings_portfolio_editor_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}"
        - add button key: f"btn_add_portfolio_from_table_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}"
    - Preserves session_state keys used elsewhere:
        - "analysis_mode"
        - "portfolio_districts"
    """
    import streamlit as st

    if view != "📊 Rankings table":
        return

    st.subheader("District rankings")

    if table_df is None or table_df.empty:
        st.caption("No ranking data available for this index, scenario, period and selection.")
        return

    # Ranking mode selector (key preserved)
    options = ["Top 20 biggest increases", "All"]
    rank_mode = st.radio(
        "Show:",
        options=options,
        index=0,
        key="rank_mode",
    )

    df_to_show = table_df.copy()

    if rank_mode == "Top 20 biggest increases":
        if has_baseline and ("rank_delta" in df_to_show.columns):
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

    # Decide which columns to display (preserve your exact logic)
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

    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")

    if analysis_mode == "Multi-district portfolio":
        df_port = df_display.copy()
        if "Add to portfolio" not in df_port.columns:
            df_port["Add to portfolio"] = False

        edited_df = st.data_editor(
            df_port,
            width="stretch",
            key=f"rankings_portfolio_editor_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}",
            num_rows="fixed",
            disabled=[c for c in df_port.columns if c != "Add to portfolio"],
        )

        st.caption(caption_text)

        st.markdown("---")
        st.markdown("#### Portfolio builder (from rankings table)")

        if st.button(
            "➕ Add checked districts to portfolio",
            key=f"btn_add_portfolio_from_table_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}",
        ):
            added = 0
            for _, row in edited_df.iterrows():
                if not row.get("Add to portfolio"):
                    continue
                district_label = row.get("District")
                state_label = row.get("State")
                if pd.isna(district_label) or pd.isna(state_label):
                    continue
                portfolio_add(str(state_label), str(district_label))
                added += 1

            if added > 0:
                st.success(f"Added {added} district(s) to portfolio.")
            else:
                st.info("No new districts were added to the portfolio.")

        portfolio = st.session_state.get("portfolio_districts", [])
        if portfolio:
            st.markdown("**Current portfolio (districts)**")
            try:
                if isinstance(portfolio[0], dict):
                    port_df = pd.DataFrame(portfolio).rename(columns={"state": "State", "district": "District"})
                else:
                    port_df = pd.DataFrame(portfolio, columns=["State", "District"])
            except Exception:
                port_df = pd.DataFrame(columns=["State", "District"])

            st.dataframe(port_df, hide_index=True, use_container_width=True)
        else:
            st.caption(
                "No districts in portfolio yet. Check one or more rows in the table "
                "above and click **Add checked districts to portfolio**."
            )
    else:
        st.dataframe(df_display, width="stretch")
        st.caption(caption_text)

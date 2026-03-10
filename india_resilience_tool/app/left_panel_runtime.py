"""
Left-panel runtime (Streamlit UI): Map vs Rankings for IRT.

This module extracts the "view selector + map/rankings rendering" block from
the app runtime so the orchestrator can stay small and delegate UI work
to focused modules.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping, Optional, Tuple


def render_left_panel(
    *,
    col: Any,
    # Map inputs
    m: Any,
    legend_block_html: Optional[str],
    map_mode: str,
    map_width: int,
    map_height: int,
    perf_section: Optional[Callable[[str], Any]],
    # Selection context
    variable_slug: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    selected_state: str,
    selected_district: str,
    selected_block: str,
    selected_basin: str,
    selected_subbasin: str,
    level: str,
    # Rankings inputs
    table_df: Any,
    has_baseline: bool,
    variables: Mapping[str, Mapping[str, Any]],
    variable_slug_for_rankings: str,
    # Portfolio callables (used by rankings + map inline add/remove)
    portfolio_add_fn: Callable[..., None],
    portfolio_contains_fn: Callable[..., bool],
    portfolio_remove_fn: Callable[..., None],
    portfolio_normalize_fn: Callable[[str], str],
    # Data (for coordinate fallback in add-to-portfolio)
    merged: Any,
) -> Tuple[Mapping[str, Any], str]:
    """
    Render the left panel and return the st_folium payload (if Map view ran).

    Returns:
        (returned, view)
    """
    import streamlit as st

    from india_resilience_tool.app.state import VIEW_MAP, VIEW_RANKINGS
    from india_resilience_tool.app.views.map_view import render_map_view, render_unit_add_to_portfolio
    from india_resilience_tool.app.views.rankings_view import render_rankings_view
    from india_resilience_tool.app.sidebar import render_view_selector

    returned: Mapping[str, Any] = {}

    with col:
        # Ribbon is shown above; keep only the reset action here (right-aligned)
        _, reset_col = st.columns([4, 1])
        with reset_col:
            if st.button("⟲ Reset View", key="reset_map_view"):
                st.session_state["pending_selected_state"] = "All"
                st.session_state["pending_selected_district"] = "All"
                st.session_state["selected_basin"] = "All"
                st.session_state["selected_subbasin"] = "All"
                st.session_state["crosswalk_overlay"] = None
                st.session_state["map_reset_requested"] = True

        # Main view selector: Map vs Rankings (replaces tabs)
        view = render_view_selector(label="View", horizontal=True)

        # ---------- VIEW 1: MAP ----------
        if view == VIEW_MAP:
            # In portfolio mode, reserve a slot ABOVE the map so the add/remove control
            # is visible even when the user is scrolling inside the right panel.
            analysis_mode = st.session_state.get("analysis_mode", "Single district focus")
            portfolio_action_slot = st.empty() if "Multi" in str(analysis_mode) else None

            returned, clicked_district, clicked_state = render_map_view(
                m=m,
                variable_slug=variable_slug,
                map_mode=map_mode,
                sel_scenario=sel_scenario,
                sel_period=sel_period,
                sel_stat=sel_stat,
                selected_state=selected_state,
                selected_district=selected_district,
                selected_block=selected_block,
                selected_basin=selected_basin,
                selected_subbasin=selected_subbasin,
                map_width=map_width,
                map_height=map_height,
                legend_block_html=legend_block_html,
                perf_section=perf_section,
                level=level,
            )

            # Show add-to-portfolio button when a unit is clicked in portfolio mode
            if portfolio_action_slot is not None:
                with portfolio_action_slot.container():
                    clicked_block = (
                        st.session_state.get("clicked_block")
                        if str(level).strip().lower() == "block"
                        else None
                    )
                    render_unit_add_to_portfolio(
                        clicked_district=clicked_district,
                        clicked_state=clicked_state,
                        clicked_block=clicked_block,
                        selected_state=selected_state,
                        portfolio_add_fn=portfolio_add_fn,
                        portfolio_remove_fn=portfolio_remove_fn,
                        portfolio_contains_fn=portfolio_contains_fn,
                        normalize_fn=portfolio_normalize_fn,
                        returned=returned,
                        merged=merged,
                        level=level,
                    )

            if clicked_district and str(level).strip().lower() in {"district", "block"}:
                st.session_state["pending_selected_district"] = clicked_district
                if clicked_state:
                    st.session_state["pending_selected_state"] = clicked_state

        # ---------- VIEW 2: RANKINGS ----------
        elif view == VIEW_RANKINGS:
            render_rankings_view(
                view=view,
                table_df=table_df,
                has_baseline=has_baseline,
                variables=variables,
                variable_slug=variable_slug_for_rankings,
                sel_scenario=sel_scenario,
                sel_period=sel_period,
                sel_stat=sel_stat,
                selected_state=selected_state,
                portfolio_add=portfolio_add_fn,
                portfolio_contains=portfolio_contains_fn,
                portfolio_remove=portfolio_remove_fn,
                level=level,
            )

    return returned, view

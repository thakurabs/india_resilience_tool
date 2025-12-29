"""
Sidebar controls for IRT (routing + view selection).

This module provides:
- Analysis mode selector (single vs multi-district)
- View selector (map vs rankings)
- Portfolio quick stats display
- Hover toggle for portfolio mode
- Jump-once flag handling for view navigation

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Optional

# Re-export state module constants for convenience
from india_resilience_tool.app.state import (
    ANALYSIS_MODE_PORTFOLIO,
    ANALYSIS_MODE_SINGLE,
    VIEW_MAP,
    VIEW_RANKINGS,
)


def apply_jump_once_flags() -> None:
    """
    Honor jump-once flags before creating the sidebar view radio.

    Contract:
    - If jump_to_rankings is True: set active_view + main_view_selector to Rankings
    - If jump_to_map is True: set active_view + main_view_selector to Map
    - Reset jump flags immediately so the jump only applies once.
    - If both flags are (accidentally) set, Rankings wins (legacy precedence).
    """
    import streamlit as st

    ss = st.session_state

    if ss.get("jump_to_rankings"):
        ss["active_view"] = VIEW_RANKINGS
        ss["main_view_selector"] = VIEW_RANKINGS
        ss["jump_to_rankings"] = False
        ss["jump_to_map"] = False
    elif ss.get("jump_to_map"):
        ss["active_view"] = VIEW_MAP
        ss["main_view_selector"] = VIEW_MAP
        ss["jump_to_map"] = False


def render_analysis_mode_selector(
    *,
    label: str = "Analysis focus",
    options: Optional[list[str]] = None,
    index: int = 0,
    help_text: Optional[str] = None,
    label_visibility: str = "visible",
    use_markdown_header: bool = False,
) -> str:
    """
    Render analysis mode selector with the stable widget key 'analysis_mode'.

    This is UI-preserving for the legacy dashboard:
    - can optionally render the bold markdown header
    - can collapse the widget label for accessibility while keeping non-empty label text
    """
    import streamlit as st

    opts = options or [ANALYSIS_MODE_SINGLE, ANALYSIS_MODE_PORTFOLIO]

    if use_markdown_header:
        st.markdown(
            "<div style='font-weight:600; font-size:1rem; "
            "margin-top:0.5rem; margin-bottom:-0.35rem;'>"
            "Analysis focus</div>",
            unsafe_allow_html=True,
        )

    current = st.session_state.get("analysis_mode", ANALYSIS_MODE_SINGLE)
    try:
        idx = opts.index(current)
    except Exception:
        idx = int(index)

    mode = st.radio(
        label,
        options=opts,
        index=idx,
        key="analysis_mode",
        label_visibility=label_visibility,
        help=help_text,
    )
    
    # Handle mode transitions - clear route-based state
    prev_mode = st.session_state.get("_analysis_mode_prev")
    if prev_mode != mode:
        st.session_state["_analysis_mode_prev"] = mode
        # Clear old route-based state (no longer needed but clean up)
        st.session_state.pop("portfolio_build_route", None)
    
    return mode


def render_hover_toggle_if_portfolio(
    analysis_mode: str,
    *,
    label: str = "Enable hover highlight & tooltip",
) -> Optional[bool]:
    """
    Render hover toggle checkbox.

    Contract:
      - key must remain 'hover_enabled'
      - checkbox is always rendered (regardless of mode)
      - defaults to True if not set

    Returns:
        hover_enabled value
    """
    import streamlit as st

    hover_enabled = st.checkbox(
        label,
        value=bool(st.session_state.get("hover_enabled", True)),
        key="hover_enabled",
    )
    return hover_enabled


def render_view_selector(
    *,
    label: str = "Main view",
    horizontal: bool = False,
) -> str:
    """
    Render view selector (Map vs Rankings) with stable widget key.

    Contract:
    - Must call apply_jump_once_flags() BEFORE creating the radio
    - key must remain 'main_view_selector'
    - Must also set 'active_view' to match selection
    """
    import streamlit as st

    apply_jump_once_flags()

    ss = st.session_state
    options = [VIEW_MAP, VIEW_RANKINGS]
    current = ss.get("active_view", VIEW_MAP)
    try:
        index = options.index(current)
    except Exception:
        index = 0

    choice = st.radio(
        label,
        options=options,
        index=index,
        key="main_view_selector",
        horizontal=horizontal,
    )

    ss["active_view"] = choice
    return choice


def render_portfolio_quick_stats() -> None:
    """
    Render quick portfolio statistics in the sidebar.
    
    Shows district count and states represented.
    Only displays in portfolio mode.
    """
    import streamlit as st
    
    analysis_mode = st.session_state.get("analysis_mode", ANALYSIS_MODE_SINGLE)
    if analysis_mode != ANALYSIS_MODE_PORTFOLIO:
        return
    
    portfolio = st.session_state.get("portfolio_districts", [])
    count = len(portfolio)
    
    if count == 0:
        st.markdown(
            '<div style="padding: 8px; background: #f5f5f5; border-radius: 4px; '
            'text-align: center; color: #666; font-size: 0.85em;">'
            '📋 Portfolio empty</div>',
            unsafe_allow_html=True,
        )
    else:
        # Count unique states
        states = set()
        for d in portfolio:
            if isinstance(d, dict):
                states.add(d.get("state", ""))
        
        state_text = f" from {len(states)} state{'s' if len(states) != 1 else ''}" if states else ""
        
        st.markdown(
            f'<div style="padding: 8px; background: #e8f4e8; border-radius: 4px; '
            f'text-align: center; color: #2d5a2d; font-size: 0.85em;">'
            f'📋 <strong>{count}</strong> district{"s" if count != 1 else ""}{state_text}</div>',
            unsafe_allow_html=True,
        )


def render_portfolio_mode_hint(analysis_mode: str) -> None:
    """
    Render contextual help text based on analysis mode.
    """
    import streamlit as st
    
    if analysis_mode == ANALYSIS_MODE_SINGLE:
        st.caption(
            "Explore one district at a time. Select a district from the dropdown "
            "or click on the map to see detailed climate metrics."
        )
    else:
        st.caption(
            "Build a portfolio of districts for comparison. Add districts by "
            "clicking on the map or using the rankings table."
        )


def get_portfolio_summary() -> dict:
    """
    Get a summary of the current portfolio state.
    
    Returns dict with:
    - count: number of districts
    - states: list of unique states
    - districts: list of (state, district) tuples
    """
    import streamlit as st
    
    portfolio = st.session_state.get("portfolio_districts", [])
    
    states = set()
    districts = []
    
    for d in portfolio:
        if isinstance(d, dict):
            state = d.get("state", "")
            district = d.get("district", "")
            if state and district:
                states.add(state)
                districts.append((state, district))
    
    return {
        "count": len(districts),
        "states": sorted(states),
        "districts": districts,
    }
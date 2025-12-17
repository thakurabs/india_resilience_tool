"""
Sidebar controls for IRT (routing + view selection).

This module is intentionally small in Step 15:
- analysis mode selector (key: analysis_mode)
- optional hover toggle (key: hover_enabled) only in portfolio mode
- jump-once flags honored BEFORE view selector is created
- view selector (key: main_view_selector) and active_view synchronization

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Optional

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
    - Always reset the corresponding jump flag to False immediately
    """
    import streamlit as st

    ss = st.session_state

    if ss.get("jump_to_rankings"):
        ss["active_view"] = VIEW_RANKINGS
        ss["main_view_selector"] = VIEW_RANKINGS
        ss["jump_to_rankings"] = False

    if ss.get("jump_to_map"):
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
        label,  # keep non-empty label for accessibility
        options=opts,
        index=idx,
        key="analysis_mode",
        label_visibility=label_visibility,
        help=help_text,
    )
    return mode


def render_hover_toggle_if_portfolio(
    analysis_mode: str,
    *,
    label: str = "Enable hover highlight & tooltip",
) -> Optional[bool]:
    """
    Render hover toggle ONLY in portfolio mode.

    Contract:
      - key must remain 'hover_enabled'
      - in single-district mode, do not show the checkbox

    Returns:
        hover_enabled if rendered, else None
    """
    import streamlit as st

    if analysis_mode != ANALYSIS_MODE_PORTFOLIO:
        return None

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

    # Use st.radio (not st.sidebar.radio) so callers can control placement consistently.
    # If you prefer sidebar-only, switch to st.sidebar.radio and keep the same key.
    choice = st.radio(
        label,
        options=options,
        index=index,
        key="main_view_selector",
        horizontal=horizontal,
    )

    ss["active_view"] = choice
    return choice

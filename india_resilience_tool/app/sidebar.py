# india_resilience_tool/app/sidebar.py
"""
Sidebar controls for IRT (routing + view selection).

This module provides:
- Administrative level selector (District vs Block)
- Analysis mode selector (single vs portfolio)
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
    ADMIN_LEVEL_BLOCK,
    ADMIN_LEVEL_DISTRICT,
    ANALYSIS_MODE_PORTFOLIO,
    ANALYSIS_MODE_SINGLE,
    VIEW_MAP,
    VIEW_RANKINGS,
    get_current_level,
    get_level_display_name,
    get_level_display_name_plural,
    set_level,
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


def render_admin_level_selector(
    *,
    label: str = "Administrative level",
    label_visibility: str = "collapsed",
    use_markdown_header: bool = False,
) -> str:
    """
    Render District/Block toggle and return selected level.

    Contract:
    - key must remain 'admin_level'
    - when switching level, dependent selections + caches must be reset (via set_level)
    """
    import streamlit as st

    options = [ADMIN_LEVEL_DISTRICT, ADMIN_LEVEL_BLOCK]
    current = st.session_state.get("admin_level", ADMIN_LEVEL_DISTRICT)
    try:
        idx = options.index(current)
    except Exception:
        idx = 0

    if use_markdown_header:
        st.markdown(
            "<div style='font-weight:600; font-size:1rem; "
            "margin-top:0.5rem; margin-bottom:-0.35rem;'>"
            "Administrative level</div>",
            unsafe_allow_html=True,
        )

    def _fmt(opt: str) -> str:
        return "Block" if opt == ADMIN_LEVEL_BLOCK else "District"

    selected = st.radio(
        label,
        options=options,
        index=idx,
        key="admin_level",
        horizontal=True,
        label_visibility=label_visibility,
        format_func=_fmt,
    )

    prev = st.session_state.get("_admin_level_prev", current)
    if prev != selected:
        st.session_state["_admin_level_prev"] = selected
        set_level(session_state=st.session_state, level=selected)

    return selected


def render_block_selector(
    blocks_list: list[str],
    selected_district: str,
    *,
    label: str = "Block",
    label_visibility: str = "collapsed",
    help_text: Optional[str] = None,
) -> str:
    """
    Render block dropdown (only when admin_level == 'block').

    Args:
        blocks_list: List of blocks for the currently selected district (or full list;
            caller may filter).
        selected_district: Current district selection; if 'All', block selection is forced to 'All'.

    Returns:
        Selected block (string). Uses the stable widget key 'selected_block'.
    """
    import streamlit as st

    if not selected_district or selected_district == "All":
        st.session_state["selected_block"] = "All"
        return "All"

    # Reset block if district changed
    prev_dist = st.session_state.get("_selected_district_prev_for_block")
    if prev_dist != selected_district:
        st.session_state["_selected_district_prev_for_block"] = selected_district
        st.session_state["selected_block"] = "All"

    options = ["All"] + sorted({b for b in blocks_list if isinstance(b, str) and b.strip() != ""})
    current = st.session_state.get("selected_block", "All")
    if current not in options:
        current = "All"
        st.session_state["selected_block"] = "All"

    try:
        idx = options.index(current)
    except Exception:
        idx = 0

    selected = st.selectbox(
        label,
        options=options,
        index=idx,
        key="selected_block",
        label_visibility=label_visibility,
        help=help_text,
    )
    return selected


def render_analysis_mode_selector(
    *,
    label: str = "Analysis focus",
    options: Optional[list[str]] = None,
    index: int = 0,
    help_text: Optional[str] = None,
    label_visibility: str = "visible",
    use_markdown_header: bool = False,
    level: Optional[str] = None,
) -> str:
    """
    Render analysis mode selector with the stable widget key 'analysis_mode'.

    This is UI-preserving for the legacy dashboard:
    - can optionally render the bold markdown header
    - can collapse the widget label for accessibility while keeping non-empty label text

    Notes:
        We keep stored values as the legacy constants (which include the word 'district')
        but use a format_func to display 'block' variants when level == 'block'.
    """
    import streamlit as st

    opts = options or [ANALYSIS_MODE_SINGLE, ANALYSIS_MODE_PORTFOLIO]

    if level is None:
        level = get_current_level(st.session_state)

    level_norm = str(level).strip().lower()
    unit = get_level_display_name(level_norm)  # "District" / "Block"

    if help_text is None:
        if level_norm == "block":
            help_text = (
                "Choose “Single block focus” to explore one block at a time, "
                "or “Multi-block portfolio” to build and compare a set of blocks."
            )
        else:
            help_text = (
                "Choose “Single district focus” to explore one district at a time, "
                "or “Multi-district portfolio” to build and compare a set of districts."
            )

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

    def _fmt(opt: str) -> str:
        # Preserve stored values; only change display label.
        if level_norm != "block":
            return opt
        if opt == ANALYSIS_MODE_SINGLE:
            return f"Single {unit.lower()} focus"
        if opt == ANALYSIS_MODE_PORTFOLIO:
            return f"Multi-{unit.lower()} portfolio"
        # Generic fallback: best-effort replacement
        return str(opt).replace("districts", "blocks").replace("district", "block")

    mode = st.radio(
        label,
        options=opts,
        index=idx,
        key="analysis_mode",
        label_visibility=label_visibility,
        help=help_text,
        format_func=_fmt,
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


def render_portfolio_quick_stats(*, level: Optional[str] = None) -> None:
    """
    Render quick portfolio statistics in the sidebar.

    Shows unit count and states represented.
    Only displays in portfolio mode.
    """
    import streamlit as st

    analysis_mode = st.session_state.get("analysis_mode", ANALYSIS_MODE_SINGLE)
    if analysis_mode != ANALYSIS_MODE_PORTFOLIO:
        return

    if level is None:
        level = get_current_level(st.session_state)

    level_norm = str(level).strip().lower()
    unit_singular = get_level_display_name(level_norm).lower()
    unit_plural = get_level_display_name_plural(level_norm).lower()

    storage_key = "portfolio_blocks" if level_norm == "block" else "portfolio_districts"
    portfolio = st.session_state.get(storage_key, [])

    # Count valid items and unique states
    states: set[str] = set()
    valid_count = 0

    for item in portfolio:
        if not isinstance(item, dict):
            continue
        state = str(item.get("state", "")).strip()
        district = str(item.get("district", "")).strip()

        if level_norm == "block":
            block = str(item.get("block", "")).strip()
            if state and district and block:
                valid_count += 1
                states.add(state)
        else:
            if state and district:
                valid_count += 1
                states.add(state)

    if valid_count == 0:
        st.markdown(
            '<div style="padding: 8px; background: #f5f5f5; border-radius: 4px; '
            'text-align: center; color: #666; font-size: 0.85em;">'
            "📋 Portfolio empty</div>",
            unsafe_allow_html=True,
        )
        return

    state_text = (
        f" from {len(states)} state{'s' if len(states) != 1 else ''}"
        if states
        else ""
    )

    # Choose singular/plural label
    unit_label = unit_singular if valid_count == 1 else unit_plural

    st.markdown(
        f'<div style="padding: 8px; background: #e8f4e8; border-radius: 4px; '
        f'text-align: center; color: #2d5a2d; font-size: 0.85em;">'
        f'📋 <strong>{valid_count}</strong> {unit_label}{state_text}</div>',
        unsafe_allow_html=True,
    )


def render_portfolio_mode_hint(analysis_mode: str, *, level: Optional[str] = None) -> None:
    """
    Render contextual help text based on analysis mode.
    """
    import streamlit as st

    if level is None:
        level = get_current_level(st.session_state)

    level_norm = str(level).strip().lower()
    unit = get_level_display_name(level_norm).lower()
    unit_plural = get_level_display_name_plural(level_norm).lower()

    if analysis_mode == ANALYSIS_MODE_SINGLE:
        st.caption(
            f"Explore one {unit} at a time. Select a {unit} from the dropdown "
            "or click on the map to see detailed climate metrics."
        )
    else:
        st.caption(
            f"Build a portfolio of {unit_plural} for comparison. Add {unit_plural} by "
            "clicking on the map or using the rankings table."
        )


def get_portfolio_summary(*, level: Optional[str] = None) -> dict:
    """
    Get a summary of the current portfolio state.

    Returns dict with (always present):
    - count: number of items (valid units)
    - states: list of unique states
    - districts: list of (state, district) tuples  [legacy key; for block level this is the parent district list]

    Additionally for block level:
    - blocks: list of (state, district, block) tuples
    """
    import streamlit as st

    if level is None:
        level = get_current_level(st.session_state)

    level_norm = str(level).strip().lower()
    storage_key = "portfolio_blocks" if level_norm == "block" else "portfolio_districts"
    portfolio = st.session_state.get(storage_key, [])

    states: set[str] = set()
    districts: list[tuple[str, str]] = []
    blocks: list[tuple[str, str, str]] = []

    for item in portfolio:
        if not isinstance(item, dict):
            continue
        state = str(item.get("state", "")).strip()
        district = str(item.get("district", "")).strip()
        if not state or not district:
            continue

        states.add(state)
        districts.append((state, district))

        if level_norm == "block":
            block = str(item.get("block", "")).strip()
            if block:
                blocks.append((state, district, block))

    out = {
        "count": len(blocks) if level_norm == "block" else len(districts),
        "states": sorted(states),
        "districts": districts,
    }
    if level_norm == "block":
        out["blocks"] = blocks

    return out

"""
Tests for the admin-level switch reset (CHG-0277).

Guards the session-state contract: switching administrative level must clear
every level-dependent key, including the portfolio multiindex selection, while
a first render (no previous level recorded) must not reset anything.
"""

from __future__ import annotations

from india_resilience_tool.app.state import (
    ADMIN_LEVEL_BLOCK,
    ADMIN_LEVEL_DISTRICT,
    reset_district_option_state,
    reset_level_dependent_state,
    set_level,
)


def _seeded_session() -> dict:
    """Session state mid-way through a district-level analysis."""
    return {
        "admin_level": ADMIN_LEVEL_DISTRICT,
        "selected_district": "Adilabad",
        "selected_block": "All",
        "portfolio_districts": ["Adilabad", "Nirmal"],
        "portfolio_blocks": [],
        "portfolio_multiindex_df": object(),
        "portfolio_multiindex_context": {"bundle": "Heat Risk"},
        "portfolio_multiindex_selection": ["Adilabad", "Nirmal"],
        "_merged_cache": {"key": "cached"},
        "selected_district_option": "Adilabad",
        "_district_effective_state": "Telangana",
    }


def test_reset_level_dependent_state_clears_everything() -> None:
    ss = _seeded_session()
    # Simulate the widget having already written the new level.
    ss["admin_level"] = ADMIN_LEVEL_BLOCK

    reset_level_dependent_state(ss)

    assert ss["selected_district"] == "All"
    assert ss["selected_block"] == "All"
    assert ss["portfolio_districts"] == []
    assert ss["portfolio_blocks"] == []
    assert ss["portfolio_multiindex_df"] is None
    assert ss["portfolio_multiindex_context"] is None
    assert ss["portfolio_multiindex_selection"] == []
    assert ss["_merged_cache"] == {}
    assert "selected_district_option" not in ss
    assert "_district_effective_state" not in ss
    # The level itself is untouched (the widget owns it).
    assert ss["admin_level"] == ADMIN_LEVEL_BLOCK


def test_first_render_seeding_does_not_reset() -> None:
    """Mirror of render_admin_level_selector's first-render branch: when
    _admin_level_prev is absent we only seed it and never reset."""
    ss = _seeded_session()
    selected = ss["admin_level"]

    prev = ss.get("_admin_level_prev")
    if prev is None:
        ss["_admin_level_prev"] = selected
    elif prev != selected:
        reset_level_dependent_state(ss)

    assert ss["_admin_level_prev"] == ADMIN_LEVEL_DISTRICT
    assert ss["selected_district"] == "Adilabad"
    assert ss["portfolio_districts"] == ["Adilabad", "Nirmal"]
    assert ss["portfolio_multiindex_selection"] == ["Adilabad", "Nirmal"]


def test_level_switch_after_seed_resets() -> None:
    ss = _seeded_session()
    ss["_admin_level_prev"] = ADMIN_LEVEL_DISTRICT
    # Widget writes the new level before the prev-detection runs.
    ss["admin_level"] = ADMIN_LEVEL_BLOCK
    selected = ss["admin_level"]

    prev = ss.get("_admin_level_prev")
    if prev is None:
        ss["_admin_level_prev"] = selected
    elif prev != selected:
        ss["_admin_level_prev"] = selected
        reset_level_dependent_state(ss)

    assert ss["_admin_level_prev"] == ADMIN_LEVEL_BLOCK
    assert ss["selected_district"] == "All"
    assert ss["portfolio_multiindex_selection"] == []


def test_set_level_still_resets_for_non_widget_callers() -> None:
    ss = _seeded_session()

    set_level(ss, ADMIN_LEVEL_BLOCK)

    assert ss["admin_level"] == ADMIN_LEVEL_BLOCK
    assert ss["selected_district"] == "All"
    assert ss["portfolio_multiindex_selection"] == []


def test_set_level_noop_when_level_unchanged() -> None:
    ss = _seeded_session()

    set_level(ss, ADMIN_LEVEL_DISTRICT)

    assert ss["selected_district"] == "Adilabad"


def test_reset_district_option_state_tolerates_absence() -> None:
    ss: dict = {}
    reset_district_option_state(ss)
    assert ss == {}

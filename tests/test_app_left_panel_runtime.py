"""Focused tests for left-panel click navigation short-circuiting."""

from __future__ import annotations

from india_resilience_tool.app.left_panel_runtime import _queue_pending_map_navigation


def test_queue_pending_map_navigation_queues_changed_district_and_state() -> None:
    session_state: dict[str, str] = {}

    changed = _queue_pending_map_navigation(
        session_state=session_state,
        level="district",
        clicked_district="Adilabad",
        clicked_state="Telangana",
        selected_state="All",
        selected_district="All",
        analysis_mode="Single district focus",
    )

    assert changed is True
    assert session_state["pending_selected_state"] == "Telangana"
    assert session_state["pending_selected_district"] == "Adilabad"


def test_queue_pending_map_navigation_noops_for_same_selection() -> None:
    session_state: dict[str, str] = {}

    changed = _queue_pending_map_navigation(
        session_state=session_state,
        level="district",
        clicked_district="Adilabad",
        clicked_state="Telangana",
        selected_state="Telangana",
        selected_district="Adilabad",
        analysis_mode="Single district focus",
    )

    assert changed is False
    assert session_state == {}


def test_queue_pending_map_navigation_skips_portfolio_mode() -> None:
    session_state: dict[str, str] = {}

    changed = _queue_pending_map_navigation(
        session_state=session_state,
        level="district",
        clicked_district="Adilabad",
        clicked_state="Telangana",
        selected_state="All",
        selected_district="All",
        analysis_mode="Multi district comparison",
    )

    assert changed is False
    assert session_state == {}


def test_queue_pending_map_navigation_ignores_hydro_levels() -> None:
    session_state: dict[str, str] = {}

    changed = _queue_pending_map_navigation(
        session_state=session_state,
        level="basin",
        clicked_district="Adilabad",
        clicked_state="Telangana",
        selected_state="All",
        selected_district="All",
        analysis_mode="Single district focus",
    )

    assert changed is False
    assert session_state == {}

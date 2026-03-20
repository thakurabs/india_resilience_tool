"""
Unit tests for app.state defaults.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app.state import ensure_session_state


def test_ensure_session_state_sets_defaults() -> None:
    ss = {}
    ensure_session_state(ss)
    assert ss["analysis_mode"] == "Single district focus"
    assert ss["active_view"] == "Map view"
    assert ss["main_view_selector"] == "Map view"
    assert isinstance(ss["portfolio_districts"], list)
    assert ss["jump_to_rankings"] is False
    assert ss["jump_to_map"] is False


def test_ensure_session_state_does_not_override_existing() -> None:
    ss = {"analysis_mode": "Multi-district portfolio", "portfolio_districts": [("X", "Y")]}
    ensure_session_state(ss)
    assert ss["analysis_mode"] == "Multi-district portfolio"
    assert ss["portfolio_districts"] == [("X", "Y")]

"""
Smoke import test for app.sidebar.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app import sidebar


def test_sidebar_module_has_expected_functions() -> None:
    assert callable(sidebar.apply_jump_once_flags)
    assert callable(sidebar.render_analysis_mode_selector)
    assert callable(sidebar.render_hover_toggle_if_portfolio)
    assert callable(sidebar.render_view_selector)

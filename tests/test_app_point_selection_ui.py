"""
Smoke tests for app.point_selection_ui.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations


def test_point_selection_ui_module_imports() -> None:
    """Test that the point_selection_ui module imports without Streamlit dependency."""
    from india_resilience_tool.app import point_selection_ui

    assert point_selection_ui is not None


def test_point_selection_ui_exports_renderer() -> None:
    """Test that the main renderer is exported and callable."""
    from india_resilience_tool.app.point_selection_ui import render_point_selection_panel

    assert callable(render_point_selection_panel)
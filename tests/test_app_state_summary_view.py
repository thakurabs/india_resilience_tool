"""
Smoke tests for app.views.state_summary_view.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations


def test_state_summary_view_module_imports() -> None:
    """Test that the state_summary_view module imports without Streamlit dependency."""
    from india_resilience_tool.app.views import state_summary_view

    assert state_summary_view is not None


def test_state_summary_view_exports_renderer() -> None:
    """Test that the main renderer is exported and callable."""
    from india_resilience_tool.app.views.state_summary_view import render_state_summary_view

    assert callable(render_state_summary_view)
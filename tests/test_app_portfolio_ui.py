"""
Smoke tests for app.portfolio_ui.

We intentionally avoid importing Streamlit at module import time; the module should
import cleanly in test environments.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations


def test_portfolio_ui_module_imports() -> None:
    """Test that the portfolio_ui module imports without Streamlit dependency."""
    from india_resilience_tool.app import portfolio_ui

    assert portfolio_ui is not None


def test_portfolio_ui_exports_render_portfolio_panel() -> None:
    """Test that the main renderer is exported and callable."""
    from india_resilience_tool.app.portfolio_ui import render_portfolio_panel

    assert callable(render_portfolio_panel)


def test_portfolio_ui_exports_subrenderers() -> None:
    """Test that all sub-renderers are exported and callable."""
    from india_resilience_tool.app.portfolio_ui import (
        render_index_selection,
        render_multiindex_comparison,
        render_portfolio_editor,
        render_route_chooser,
        render_route_hints,
        render_state_summary,
    )

    assert callable(render_state_summary)
    assert callable(render_route_chooser)
    assert callable(render_route_hints)
    assert callable(render_index_selection)
    assert callable(render_multiindex_comparison)
    assert callable(render_portfolio_editor)
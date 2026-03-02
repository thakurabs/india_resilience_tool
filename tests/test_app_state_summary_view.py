"""Tests for app.views.state_summary_view."""

from __future__ import annotations


def test_state_summary_view_module_imports() -> None:
    """Test that the state_summary_view module imports without Streamlit dependency."""
    from india_resilience_tool.app.views import state_summary_view

    assert state_summary_view is not None


def test_state_summary_view_exports_renderer() -> None:
    """Test that the main renderer is exported and callable."""
    from india_resilience_tool.app.views.state_summary_view import render_state_summary_view

    assert callable(render_state_summary_view)


def test_compute_position_in_india_single_state_returns_na_rank() -> None:
    from india_resilience_tool.app.views.state_summary_view import _compute_position_in_india

    rank, n = _compute_position_in_india(
        {"telangana": 12.0},
        "Telangana",
        higher_is_worse=True,
    )
    assert rank is None
    assert n == 1


def test_compute_position_in_india_rank_is_never_greater_than_n() -> None:
    from india_resilience_tool.app.views.state_summary_view import _compute_position_in_india

    rank, n = _compute_position_in_india(
        {"telangana": 12.0, "maharashtra": 8.0},
        "Telangana",
        higher_is_worse=True,
    )
    assert rank is not None
    assert n == 2
    assert rank <= n

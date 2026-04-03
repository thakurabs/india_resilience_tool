"""Tests for pre-render view resolution in the app runtime."""

from __future__ import annotations

from india_resilience_tool.app.runtime import _resolve_pre_render_view


def test_resolve_pre_render_view_prefers_widget_backed_view_over_stale_active_view() -> None:
    session_state = {
        "main_view_selector": "Map view",
        "active_view": "Rankings table",
    }

    assert _resolve_pre_render_view(session_state, default_view="Map view") == "Map view"


def test_resolve_pre_render_view_falls_back_to_active_view_then_default() -> None:
    assert (
        _resolve_pre_render_view(
            {"active_view": "Rankings table"},
            default_view="Map view",
        )
        == "Rankings table"
    )
    assert _resolve_pre_render_view({}, default_view="Map view") == "Map view"

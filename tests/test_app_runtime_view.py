"""Tests for pre-render view resolution in the app runtime."""

from __future__ import annotations

from india_resilience_tool.app.runtime import TOP_VIEW_DASHBOARD, TOP_VIEW_DOCS, _resolve_pre_render_view, _top_view_selector


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


def test_top_view_selector_uses_segmented_control(monkeypatch) -> None:
    class StubStreamlit:
        @staticmethod
        def segmented_control(label, options, default, key, label_visibility):
            assert label == "View"
            assert options == (TOP_VIEW_DASHBOARD, TOP_VIEW_DOCS)
            assert default == TOP_VIEW_DASHBOARD
            assert key == "irt_top_view"
            assert label_visibility == "collapsed"
            return TOP_VIEW_DOCS

    import india_resilience_tool.app.runtime as runtime

    monkeypatch.setitem(__import__("sys").modules, "streamlit", StubStreamlit)

    assert runtime._top_view_selector() == TOP_VIEW_DOCS


def test_top_view_selector_radio_fallback_and_invalid_selection(monkeypatch) -> None:
    class StubStreamlit:
        @staticmethod
        def radio(label, options, index, key, horizontal, label_visibility):
            assert label == "View"
            assert options == (TOP_VIEW_DASHBOARD, TOP_VIEW_DOCS)
            assert index == 0
            assert key == "irt_top_view"
            assert horizontal is True
            assert label_visibility == "collapsed"
            return "Other"

    import india_resilience_tool.app.runtime as runtime

    monkeypatch.setitem(__import__("sys").modules, "streamlit", StubStreamlit)

    assert runtime._top_view_selector() == TOP_VIEW_DASHBOARD

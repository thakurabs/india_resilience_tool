from __future__ import annotations


from india_resilience_tool.app.state import ensure_session_state


def test_ensure_session_state_sets_right_panel_collapsed_default() -> None:
    ss: dict[str, object] = {}
    ensure_session_state(ss)
    assert ss.get("right_panel_collapsed") is False


def test_ensure_session_state_does_not_clobber_right_panel_collapsed() -> None:
    ss: dict[str, object] = {"right_panel_collapsed": True}
    ensure_session_state(ss)
    assert ss.get("right_panel_collapsed") is True


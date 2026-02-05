"""Tests for modular dashboard performance helpers."""

from __future__ import annotations

from types import SimpleNamespace

from india_resilience_tool.app import perf


class _DummySidebar:
    def empty(self):
        return _DummyContainer()


class _DummyContainer:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyExpander(_DummyContainer):
    pass


def test_perf_reset_and_recording(monkeypatch) -> None:
    fake_st = SimpleNamespace(session_state={"perf_enabled": True})
    monkeypatch.setattr(perf, "st", fake_st)

    perf.perf_reset()
    assert fake_st.session_state["_perf_records"] == []

    token = perf.perf_start("x")
    assert token is not None
    perf.perf_end("x", token)

    records = fake_st.session_state["_perf_records"]
    assert len(records) == 1
    assert records[0]["section"] == "x"
    assert isinstance(records[0]["seconds"], float)


def test_perf_noop_when_disabled(monkeypatch) -> None:
    fake_st = SimpleNamespace(session_state={"perf_enabled": False})
    monkeypatch.setattr(perf, "st", fake_st)

    assert perf.perf_start("x") is None
    perf.perf_end("x", None)
    assert "_perf_records" not in fake_st.session_state


def test_render_perf_panel_safe_uses_sidebar_placeholder(monkeypatch) -> None:
    calls = {"rendered": 0}

    fake_st = SimpleNamespace(
        session_state={"perf_enabled": True},
        sidebar=_DummySidebar(),
        empty=lambda: _DummyContainer(),
    )
    monkeypatch.setattr(perf, "st", fake_st)
    monkeypatch.setattr(perf, "render_perf_panel", lambda container: calls.__setitem__("rendered", 1))

    perf.render_perf_panel_safe()
    assert calls["rendered"] == 1

from __future__ import annotations

from pathlib import Path

from india_resilience_tool.app.geography_controls import _resolve_available_admin_states


def test_resolve_available_admin_states_preserves_all_for_flat_admin_masters(tmp_path: Path) -> None:
    (tmp_path / "Telangana").mkdir(parents=True)
    (tmp_path / "Telangana" / "master_metrics_by_district.csv").write_text("state,district\n", encoding="utf-8")
    (tmp_path / "Odisha").mkdir(parents=True)
    (tmp_path / "Odisha" / "master_metrics_by_district.csv").write_text("state,district\n", encoding="utf-8")

    available_states, has_available_data = _resolve_available_admin_states(tmp_path)

    assert has_available_data is True
    assert available_states == ["All", "Odisha", "Telangana"]


def test_resolve_available_admin_states_returns_all_when_missing(tmp_path: Path) -> None:
    available_states, has_available_data = _resolve_available_admin_states(tmp_path / "missing")

    assert has_available_data is False
    assert available_states == ["All"]

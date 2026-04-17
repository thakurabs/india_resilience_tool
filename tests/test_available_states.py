from __future__ import annotations

import types
from pathlib import Path

from india_resilience_tool.app.geography import list_available_states_from_processed_root


def test_available_states_new_structure(tmp_path: Path) -> None:
    (tmp_path / "Telangana" / "districts" / "Karimnagar" / "ssp245").mkdir(parents=True)
    (tmp_path / "Odisha" / "blocks" / "Khordha" / "Bhubaneswar" / "ssp245").mkdir(parents=True)
    (tmp_path / "EmptyState").mkdir()

    states = list_available_states_from_processed_root(str(tmp_path))
    assert states == ["Odisha", "Telangana"]


def test_available_states_old_structure(tmp_path: Path) -> None:
    (tmp_path / "Telangana" / "Hyderabad" / "ssp245").mkdir(parents=True)
    (tmp_path / "Kerala" / "districts").mkdir(parents=True)

    states = list_available_states_from_processed_root(str(tmp_path))
    assert states == ["Telangana"]


def test_available_states_flat_admin_master_structure(tmp_path: Path) -> None:
    (tmp_path / "Telangana").mkdir(parents=True)
    (tmp_path / "Telangana" / "master_metrics_by_district.csv").write_text("state,district\n", encoding="utf-8")
    (tmp_path / "Odisha").mkdir(parents=True)
    (tmp_path / "Odisha" / "master_metrics_by_block.csv").write_text("state,district,block\n", encoding="utf-8")

    states = list_available_states_from_processed_root(str(tmp_path))
    assert states == ["Odisha", "Telangana"]


def test_available_states_ignores_reserved_and_nan_like_dirs(tmp_path: Path) -> None:
    (tmp_path / "Telangana").mkdir(parents=True)
    (tmp_path / "Telangana" / "master_metrics_by_district.csv").write_text("state,district\n", encoding="utf-8")
    (tmp_path / "hydro" / "DemoBasin" / "ssp245").mkdir(parents=True)
    (tmp_path / "nan").mkdir(parents=True)
    (tmp_path / "nan" / "master_metrics_by_district.csv").write_text("state,district\n", encoding="utf-8")

    states = list_available_states_from_processed_root(str(tmp_path))
    assert states == ["Telangana"]


def test_available_states_missing_or_empty_root(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"
    assert list_available_states_from_processed_root(str(missing)) == []

    assert list_available_states_from_processed_root(str(tmp_path)) == []


def test_jrc_metric_supports_only_telangana_in_geography_controls(monkeypatch) -> None:
    from india_resilience_tool.app import geography_controls

    monkeypatch.setattr(
        geography_controls,
        "st",
        types.SimpleNamespace(session_state={"selected_var": "jrc_flood_depth_rp100"}),
    )
    assert geography_controls._supported_admin_states_for_selected_metric() == ["Telangana"]

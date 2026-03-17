from __future__ import annotations

from pathlib import Path

from india_resilience_tool.app.ribbon import _resolve_external_admin_master_sources


def test_resolve_external_admin_master_sources_for_all_uses_all_valid_states(tmp_path: Path) -> None:
    (tmp_path / "Telangana").mkdir(parents=True)
    (tmp_path / "Telangana" / "master_metrics_by_district.csv").write_text("state,district\n", encoding="utf-8")
    (tmp_path / "Odisha").mkdir(parents=True)
    (tmp_path / "Odisha" / "master_metrics_by_district.csv").write_text("state,district\n", encoding="utf-8")
    (tmp_path / "hydro" / "DemoBasin" / "ssp245").mkdir(parents=True)

    paths = _resolve_external_admin_master_sources(
        tmp_path,
        level="district",
        selected_state="All",
    )

    assert paths == (
        tmp_path / "Odisha" / "master_metrics_by_district.csv",
        tmp_path / "Telangana" / "master_metrics_by_district.csv",
    )


def test_resolve_external_admin_master_sources_for_single_state_returns_one_path(tmp_path: Path) -> None:
    expected = tmp_path / "Telangana" / "master_metrics_by_block.csv"

    paths = _resolve_external_admin_master_sources(
        tmp_path,
        level="block",
        selected_state="Telangana",
    )

    assert paths == (expected,)

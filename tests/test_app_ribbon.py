from __future__ import annotations

from pathlib import Path

import pandas as pd

from india_resilience_tool.app.ribbon import (
    _hydro_master_contract_ready,
    _resolve_external_admin_master_sources,
    _resolve_hydro_master_source,
)


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


def test_resolve_hydro_master_source_falls_back_to_legacy_when_optimized_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    slug = "txx_annual_max"
    optimized_root = tmp_path / "processed_optimised" / "metrics" / slug
    (optimized_root / "masters").mkdir(parents=True)

    legacy_root = tmp_path / "processed" / slug
    legacy_hydro = legacy_root / "hydro"
    legacy_hydro.mkdir(parents=True)
    (legacy_hydro / "master_metrics_by_basin.csv").write_text(
        "basin_id,basin_name,txx_annual_max__ssp245__2030-2040__mean\nGODAVARI,Godavari Basin,1.0\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("IRT_PROCESSED_ROOT", raising=False)
    monkeypatch.delenv("IRT_PROCESSED_OPTIMISED_ROOT", raising=False)

    resolved_root, master_path, legacy_checked = _resolve_hydro_master_source(
        optimized_root,
        variable_slug=slug,
        level="basin",
        data_dir=tmp_path,
    )

    assert resolved_root == legacy_root.resolve()
    assert master_path == (legacy_root / "hydro" / "master_metrics_by_basin.csv").resolve()
    assert legacy_checked == legacy_root.resolve()


def test_hydro_master_contract_ready_accepts_optimized_parquet(tmp_path: Path) -> None:
    master_path = tmp_path / "metrics" / "tas_annual_mean" / "masters" / "hydro" / "basin" / "master.parquet"
    master_path.parent.mkdir(parents=True)
    pd.DataFrame(
        {
            "basin_id": ["godavari"],
            "tas_annual_mean__ssp245__2060-2080__median": [28.83],
        }
    ).to_parquet(master_path, index=False)

    assert _hydro_master_contract_ready(master_path, "basin") is True

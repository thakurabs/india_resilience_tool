"""
Unit tests for safe pruning of legacy per-unit CSV outputs.

These tests do not require pandas/pyarrow because pruning is filesystem-based.
"""

from __future__ import annotations

from pathlib import Path

from india_resilience_tool.compute.master_builder import _prune_legacy_yearly_period_csvs


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")


def test_prune_legacy_csv_requires_parquet_gates(tmp_path: Path) -> None:
    state_root = tmp_path / "Telangana"
    state_root.mkdir(parents=True, exist_ok=True)

    # Legacy files (should NOT be deleted because gates are missing)
    legacy_y = state_root / "districts" / "Alpha" / "M1" / "ssp585" / "Alpha_yearly.csv"
    legacy_p = state_root / "districts" / "Alpha" / "M1" / "ssp585" / "Alpha_periods.csv"
    _touch(legacy_y)
    _touch(legacy_p)

    deleted = _prune_legacy_yearly_period_csvs(state_root=state_root, level="district", verbose=False)
    assert deleted == []
    assert legacy_y.exists()
    assert legacy_p.exists()


def test_prune_legacy_csv_deletes_only_yearly_and_periods(tmp_path: Path) -> None:
    state_root = tmp_path / "Telangana"
    state_root.mkdir(parents=True, exist_ok=True)

    # Gates: parquet master + consolidated parquet ensembles
    _touch(state_root / "master_metrics_by_district.parquet")
    _touch(
        state_root
        / "districts"
        / "ensembles"
        / "yearly"
        / "scenario=ssp585"
        / "data.parquet"
    )

    # Legacy per-unit CSVs to delete
    legacy_y = state_root / "districts" / "Alpha" / "M1" / "ssp585" / "Alpha_yearly.csv"
    legacy_p = state_root / "districts" / "Alpha" / "M1" / "ssp585" / "Alpha_periods.csv"
    _touch(legacy_y)
    _touch(legacy_p)

    # Decoys that must be preserved
    legacy_ensemble = state_root / "districts" / "ensembles" / "Alpha" / "ssp585" / "Alpha_yearly_ensemble.csv"
    _touch(legacy_ensemble)
    raw_decoy = state_root / "districts" / "raw" / "periods" / "model=M1" / "scenario=ssp585" / "Alpha_periods.csv"
    _touch(raw_decoy)

    deleted = _prune_legacy_yearly_period_csvs(state_root=state_root, level="district", verbose=False)
    deleted_set = {p.resolve() for p in deleted}

    assert legacy_y.resolve() in deleted_set
    assert legacy_p.resolve() in deleted_set
    assert not legacy_y.exists()
    assert not legacy_p.exists()

    # Ensure we did not touch decoys
    assert legacy_ensemble.exists()
    assert raw_decoy.exists()

"""Focused regression tests for admin ensemble accounting."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


def _repo_root() -> Path:
    """Find repository root (assumes tests/ is directly under repo root)."""
    return Path(__file__).resolve().parents[1]


_ROOT = _repo_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.pipeline import compute_indices_multiprocess as CMP  # noqa: E402


def _write_yearly_csv(path: Path, *, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def test_compute_district_ensembles_counts_expected_and_written_outputs(tmp_path: Path) -> None:
    level_root = tmp_path / "Telangana" / "districts"
    ensembles_root = level_root / "ensembles"
    _write_yearly_csv(
        level_root / "ADILABAD" / "ModelA" / "historical" / "ADILABAD_yearly.csv",
        rows=[
            {"year": 2000, "value": 1.0},
            {"year": 2001, "value": 2.0},
        ],
    )
    _write_yearly_csv(
        level_root / "ADILABAD" / "ModelB" / "historical" / "ADILABAD_yearly.csv",
        rows=[
            {"year": 2000, "value": 3.0},
            {"year": 2001, "value": 5.0},
        ],
    )

    stats = CMP._compute_district_ensembles(level_root, ensembles_root)

    assert stats.expected_output_count == 1
    assert stats.written_count == 1
    assert stats.missing_expected_output_count == 0
    assert stats.failure_count == 0
    assert (ensembles_root / "ADILABAD" / "historical" / "ADILABAD_yearly_ensemble.csv").exists()


def test_compute_district_ensembles_marks_missing_when_inputs_invalid(tmp_path: Path) -> None:
    level_root = tmp_path / "Telangana" / "districts"
    ensembles_root = level_root / "ensembles"
    _write_yearly_csv(
        level_root / "ADILABAD" / "ModelA" / "historical" / "ADILABAD_yearly.csv",
        rows=[
            {"district": "ADILABAD", "model": "ModelA", "scenario": "historical", "source_file": "x"},
        ],
    )

    stats = CMP._compute_district_ensembles(level_root, ensembles_root)

    assert stats.expected_output_count == 1
    assert stats.written_count == 0
    assert stats.missing_expected_output_count == 1
    assert stats.skipped_input_count == 1
    assert any("no valid filtered yearly inputs" in message for message in stats.errors)


def test_compute_block_ensembles_counts_expected_and_written_outputs(tmp_path: Path) -> None:
    level_root = tmp_path / "Telangana" / "blocks"
    ensembles_root = level_root / "ensembles"
    first_model_path = (
        level_root
        / "ADILABAD"
        / "BLOCK_A"
        / "ModelA"
        / "historical"
        / "BLOCK_A_yearly.csv"
    )
    second_model_path = (
        level_root
        / "ADILABAD"
        / "BLOCK_A"
        / "ModelB"
        / "historical"
        / "BLOCK_A_yearly.csv"
    )
    _write_yearly_csv(
        first_model_path,
        rows=[
            {"year": 2000, "value": 1.0},
            {"year": 2001, "value": 2.0},
        ],
    )
    _write_yearly_csv(
        second_model_path,
        rows=[
            {"year": 2000, "value": 3.0},
            {"year": 2001, "value": 4.0},
        ],
    )

    stats = CMP._compute_block_ensembles(level_root, ensembles_root)

    assert stats.expected_output_count == 1
    assert stats.written_count == 1
    assert stats.missing_expected_output_count == 0
    assert stats.failure_count == 0
    assert (ensembles_root / "ADILABAD" / "BLOCK_A" / "historical" / "BLOCK_A_yearly_ensemble.csv").exists()
    assert not first_model_path.exists()
    assert not second_model_path.exists()


def test_compute_block_ensembles_marks_missing_when_inputs_invalid(tmp_path: Path) -> None:
    level_root = tmp_path / "Telangana" / "blocks"
    ensembles_root = level_root / "ensembles"
    _write_yearly_csv(
        level_root / "ADILABAD" / "BLOCK_A" / "ModelA" / "historical" / "BLOCK_A_yearly.csv",
        rows=[
            {
                "district": "ADILABAD",
                "block": "BLOCK_A",
                "model": "ModelA",
                "scenario": "historical",
                "source_file": "x",
            },
        ],
    )

    stats = CMP._compute_block_ensembles(level_root, ensembles_root)

    assert stats.expected_output_count == 1
    assert stats.written_count == 0
    assert stats.missing_expected_output_count == 1
    assert stats.skipped_input_count == 1
    assert any("no valid filtered yearly inputs" in message for message in stats.errors)

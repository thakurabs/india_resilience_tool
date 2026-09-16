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


def test_compute_district_ensembles_treats_all_nan_unit_as_benign_empty(tmp_path: Path) -> None:
    level_root = tmp_path / "Gujarat" / "districts"
    ensembles_root = level_root / "ensembles"
    for model in ("ModelA", "ModelB"):
        _write_yearly_csv(
            level_root / "Banas_Kantha" / model / "historical" / "Banas_Kantha_yearly.csv",
            rows=[
                {"year": 2000, "value": None},
                {"year": 2001, "value": None},
            ],
        )

    stats = CMP._compute_district_ensembles(level_root, ensembles_root)

    assert stats.empty_admin_output_count == 1
    assert stats.expected_output_count == 0
    assert stats.missing_expected_output_count == 0
    assert stats.failure_count == 0
    assert stats.skipped_input_count == 2  # two models, each -> no_numeric_rows
    assert not (
        ensembles_root / "Banas_Kantha" / "historical" / "Banas_Kantha_yearly_ensemble.csv"
    ).exists()


def test_compute_district_ensembles_mixed_reasons_stays_hard_miss(tmp_path: Path) -> None:
    # G3: a benign no_numeric_rows co-occurring with a non-benign missing_year must NOT be benign.
    level_root = tmp_path / "Gujarat" / "districts"
    ensembles_root = level_root / "ensembles"
    _write_yearly_csv(  # ModelA: benign no_numeric_rows
        level_root / "Patan" / "ModelA" / "historical" / "Patan_yearly.csv",
        rows=[{"year": 2000, "value": None}],
    )
    _write_yearly_csv(  # ModelB: missing_year (non-benign data-contract failure)
        level_root / "Patan" / "ModelB" / "historical" / "Patan_yearly.csv",
        rows=[{"district": "Patan", "model": "ModelB", "source_file": "x"}],
    )

    stats = CMP._compute_district_ensembles(level_root, ensembles_root)

    assert stats.empty_admin_output_count == 0
    assert stats.expected_output_count == 1
    assert stats.missing_expected_output_count == 1
    assert stats.failure_count == 0


def test_compute_district_ensembles_respect_model_and_scenario_filters(tmp_path: Path) -> None:
    level_root = tmp_path / "Telangana" / "districts"
    ensembles_root = level_root / "ensembles"
    _write_yearly_csv(
        level_root / "ADILABAD" / "ModelA" / "historical" / "ADILABAD_yearly.csv",
        rows=[{"year": 2000, "value": 1.0}],
    )
    _write_yearly_csv(
        level_root / "ADILABAD" / "ModelA" / "ssp245" / "ADILABAD_yearly.csv",
        rows=[{"year": 2030, "value": 2.0}],
    )
    _write_yearly_csv(
        level_root / "ADILABAD" / "ModelB" / "historical" / "ADILABAD_yearly.csv",
        rows=[{"year": 2000, "value": 3.0}],
    )

    stats = CMP._compute_district_ensembles(
        level_root,
        ensembles_root,
        allowed_models=("ModelA",),
        allowed_scenarios=("historical",),
    )

    assert stats.expected_output_count == 1
    assert stats.written_count == 1
    assert (ensembles_root / "ADILABAD" / "historical" / "ADILABAD_yearly_ensemble.csv").exists()
    assert not (ensembles_root / "ADILABAD" / "ssp245" / "ADILABAD_yearly_ensemble.csv").exists()


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


def test_compute_block_ensembles_treats_all_nan_unit_as_benign_empty(tmp_path: Path) -> None:
    level_root = tmp_path / "Gujarat" / "blocks"
    ensembles_root = level_root / "ensembles"
    for model in ("ModelA", "ModelB"):
        _write_yearly_csv(
            level_root / "Banas_Kantha" / "BLOCK_A" / model / "historical" / "BLOCK_A_yearly.csv",
            rows=[
                {"year": 2000, "value": None},
                {"year": 2001, "value": None},
            ],
        )

    stats = CMP._compute_block_ensembles(level_root, ensembles_root)

    assert stats.empty_admin_output_count == 1
    assert stats.expected_output_count == 0
    assert stats.missing_expected_output_count == 0
    assert stats.failure_count == 0
    assert stats.skipped_input_count == 2  # two models, each -> no_numeric_rows
    assert not (
        ensembles_root / "Banas_Kantha" / "BLOCK_A" / "historical" / "BLOCK_A_yearly_ensemble.csv"
    ).exists()


def test_compute_block_ensembles_mixed_reasons_stays_hard_miss(tmp_path: Path) -> None:
    # G3 (block, the G1 regression-prone half): no_numeric_rows + missing_year must NOT be benign.
    level_root = tmp_path / "Gujarat" / "blocks"
    ensembles_root = level_root / "ensembles"
    _write_yearly_csv(  # ModelA: benign no_numeric_rows
        level_root / "Patan" / "BLOCK_A" / "ModelA" / "historical" / "BLOCK_A_yearly.csv",
        rows=[{"year": 2000, "value": None}],
    )
    _write_yearly_csv(  # ModelB: missing_year (non-benign data-contract failure)
        level_root / "Patan" / "BLOCK_A" / "ModelB" / "historical" / "BLOCK_A_yearly.csv",
        rows=[{"district": "Patan", "block": "BLOCK_A", "model": "ModelB", "source_file": "x"}],
    )

    stats = CMP._compute_block_ensembles(level_root, ensembles_root)

    assert stats.empty_admin_output_count == 0
    assert stats.expected_output_count == 1
    assert stats.missing_expected_output_count == 1
    assert stats.failure_count == 0


def test_compute_block_ensembles_respect_model_and_scenario_filters(tmp_path: Path) -> None:
    level_root = tmp_path / "Telangana" / "blocks"
    ensembles_root = level_root / "ensembles"
    _write_yearly_csv(
        level_root / "ADILABAD" / "BLOCK_A" / "ModelA" / "historical" / "BLOCK_A_yearly.csv",
        rows=[{"year": 2000, "value": 1.0}],
    )
    _write_yearly_csv(
        level_root / "ADILABAD" / "BLOCK_A" / "ModelA" / "ssp245" / "BLOCK_A_yearly.csv",
        rows=[{"year": 2030, "value": 2.0}],
    )
    _write_yearly_csv(
        level_root / "ADILABAD" / "BLOCK_A" / "ModelB" / "historical" / "BLOCK_A_yearly.csv",
        rows=[{"year": 2000, "value": 3.0}],
    )

    stats = CMP._compute_block_ensembles(
        level_root,
        ensembles_root,
        allowed_models=("ModelA",),
        allowed_scenarios=("historical",),
    )

    assert stats.expected_output_count == 1
    assert stats.written_count == 1
    assert (ensembles_root / "ADILABAD" / "BLOCK_A" / "historical" / "BLOCK_A_yearly_ensemble.csv").exists()
    assert not (ensembles_root / "ADILABAD" / "BLOCK_A" / "ssp245" / "BLOCK_A_yearly_ensemble.csv").exists()


def test_compute_block_ensembles_preserve_keeps_valid_contributing_yearly_inputs(tmp_path: Path) -> None:
    level_root = tmp_path / "Telangana" / "blocks"
    ensembles_root = level_root / "ensembles"
    first_model_path = level_root / "ADILABAD" / "BLOCK_A" / "ModelA" / "historical" / "BLOCK_A_yearly.csv"
    second_model_path = level_root / "ADILABAD" / "BLOCK_A" / "ModelB" / "historical" / "BLOCK_A_yearly.csv"
    invalid_model_path = level_root / "ADILABAD" / "BLOCK_A" / "ModelC" / "historical" / "BLOCK_A_yearly.csv"
    _write_yearly_csv(first_model_path, rows=[{"year": 2000, "value": 1.0}])
    _write_yearly_csv(second_model_path, rows=[{"year": 2000, "value": 3.0}])
    _write_yearly_csv(invalid_model_path, rows=[{"district": "ADILABAD", "block": "BLOCK_A"}])

    stats = CMP._compute_block_ensembles(level_root, ensembles_root, yearly_cleanup_policy="preserve")

    assert stats.expected_output_count == 1
    assert stats.written_count == 1
    assert stats.failure_count == 0
    assert first_model_path.exists()
    assert second_model_path.exists()
    assert invalid_model_path.exists()

"""Focused regression tests for compute-marker validation semantics."""

from __future__ import annotations

import sys
from pathlib import Path


def _repo_root() -> Path:
    """Find repository root (assumes tests/ is directly under repo root)."""
    return Path(__file__).resolve().parents[1]


_ROOT = _repo_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.pipeline import compute_indices_multiprocess as CMP  # noqa: E402


def _task(*, level: str = "block") -> CMP.ProcessingTask:
    return CMP.ProcessingTask(
        metric_idx=0,
        slug="tas_winter_mean",
        model="CanESM5",
        scenario="historical",
        scenario_conf={"periods": {"1990-2010": (1990, 2010)}},
        task_id=0,
        total_tasks=1,
        level=level,
        state_name="Telangana",
        required_vars=("tas",),
        common_years_hash="abc123",
        scope_name="Telangana",
        source_signatures={"eval": "sig-eval"},
        yearly_cleanup_policy=CMP._compute_marker_yearly_cleanup_policy(level),
    )


def test_block_compute_marker_remains_valid_after_yearly_cleanup(monkeypatch) -> None:
    task = _task(level="block")
    monkeypatch.setattr(
        CMP,
        "_load_marker_json",
        lambda _path: {
            "schema_version": CMP.COMPUTE_MARKER_SCHEMA_VERSION,
            "slug": task.slug,
            "level": task.level,
            "scope": task.scope_name,
            "model": task.model,
            "scenario": task.scenario,
            "required_vars": list(task.required_vars),
            "common_years_hash": task.common_years_hash,
            "source_signatures": dict(task.source_signatures),
            "boundary_path": "/tmp/boundary.geojson",
            "boundary_mtime_ns": 123,
            "yearly_file_count": 620,
            "period_file_count": 620,
            "yearly_cleanup_policy": "delete_after_ensemble",
        },
    )
    monkeypatch.setattr(CMP, "_boundary_signature", lambda _level, _state: ("/tmp/boundary.geojson", 123))
    monkeypatch.setattr(CMP, "_task_output_file_counts", lambda **_kwargs: (0, 620))

    status = CMP.task_completion_marker_status(task)

    assert status.valid is True
    assert status.reason == "ok"


def test_block_compute_marker_still_requires_matching_period_counts(monkeypatch) -> None:
    task = _task(level="block")
    monkeypatch.setattr(
        CMP,
        "_load_marker_json",
        lambda _path: {
            "schema_version": CMP.COMPUTE_MARKER_SCHEMA_VERSION,
            "slug": task.slug,
            "level": task.level,
            "scope": task.scope_name,
            "model": task.model,
            "scenario": task.scenario,
            "required_vars": list(task.required_vars),
            "common_years_hash": task.common_years_hash,
            "source_signatures": dict(task.source_signatures),
            "boundary_path": "/tmp/boundary.geojson",
            "boundary_mtime_ns": 123,
            "yearly_file_count": 620,
            "period_file_count": 620,
            "yearly_cleanup_policy": "delete_after_ensemble",
        },
    )
    monkeypatch.setattr(CMP, "_boundary_signature", lambda _level, _state: ("/tmp/boundary.geojson", 123))
    monkeypatch.setattr(CMP, "_task_output_file_counts", lambda **_kwargs: (0, 619))

    status = CMP.task_completion_marker_status(task)

    assert status.valid is False
    assert status.reason == "compute_marker_output_count_mismatch"


def test_district_compute_marker_reports_missing_preserved_yearly_files(monkeypatch) -> None:
    task = _task(level="district")
    monkeypatch.setattr(
        CMP,
        "_load_marker_json",
        lambda _path: {
            "schema_version": CMP.COMPUTE_MARKER_SCHEMA_VERSION,
            "slug": task.slug,
            "level": task.level,
            "scope": task.scope_name,
            "model": task.model,
            "scenario": task.scenario,
            "required_vars": list(task.required_vars),
            "common_years_hash": task.common_years_hash,
            "source_signatures": dict(task.source_signatures),
            "boundary_path": "/tmp/boundary.geojson",
            "boundary_mtime_ns": 123,
            "yearly_file_count": 33,
            "period_file_count": 33,
            "yearly_cleanup_policy": "preserve",
        },
    )
    monkeypatch.setattr(CMP, "_boundary_signature", lambda _level, _state: ("/tmp/boundary.geojson", 123))
    monkeypatch.setattr(CMP, "_task_output_file_counts", lambda **_kwargs: (0, 33))

    status = CMP.task_completion_marker_status(task)

    assert status.valid is False
    assert status.reason == "yearly_files_missing_under_preserve_policy"


def test_compute_marker_rejects_source_signature_drift(monkeypatch) -> None:
    task = _task(level="district")
    monkeypatch.setattr(
        CMP,
        "_load_marker_json",
        lambda _path: {
            "schema_version": CMP.COMPUTE_MARKER_SCHEMA_VERSION,
            "slug": task.slug,
            "level": task.level,
            "scope": task.scope_name,
            "model": task.model,
            "scenario": task.scenario,
            "required_vars": list(task.required_vars),
            "common_years_hash": task.common_years_hash,
            "source_signatures": {"eval": "stale-signature"},
            "boundary_path": "/tmp/boundary.geojson",
            "boundary_mtime_ns": 123,
            "yearly_file_count": 33,
            "period_file_count": 33,
            "yearly_cleanup_policy": "preserve",
        },
    )
    monkeypatch.setattr(CMP, "_boundary_signature", lambda _level, _state: ("/tmp/boundary.geojson", 123))
    monkeypatch.setattr(CMP, "_task_output_file_counts", lambda **_kwargs: (33, 33))

    status = CMP.task_completion_marker_status(task)

    assert status.valid is False
    assert status.reason == "compute_marker_source_signatures_mismatch"


def test_compute_marker_rejects_old_schema_even_with_matching_outputs(monkeypatch) -> None:
    task = _task(level="block")
    monkeypatch.setattr(
        CMP,
        "_load_marker_json",
        lambda _path: {
            "schema_version": CMP.COMPUTE_MARKER_SCHEMA_VERSION - 1,
            "slug": task.slug,
            "level": task.level,
            "scope": task.scope_name,
            "model": task.model,
            "scenario": task.scenario,
            "required_vars": list(task.required_vars),
            "common_years_hash": task.common_years_hash,
            "source_signatures": dict(task.source_signatures),
            "boundary_path": "/tmp/boundary.geojson",
            "boundary_mtime_ns": 123,
            "yearly_file_count": 620,
            "period_file_count": 620,
            "yearly_cleanup_policy": task.yearly_cleanup_policy,
        },
    )
    monkeypatch.setattr(CMP, "_boundary_signature", lambda _level, _state: ("/tmp/boundary.geojson", 123))
    monkeypatch.setattr(CMP, "_task_output_file_counts", lambda **_kwargs: (620, 620))

    status = CMP.task_completion_marker_status(task)

    assert status.valid is False
    assert status.reason == "compute_marker_schema_mismatch"


def test_compute_marker_rejects_policy_mismatch(monkeypatch) -> None:
    task = _task(level="block")
    monkeypatch.setattr(
        CMP,
        "_load_marker_json",
        lambda _path: {
            "schema_version": CMP.COMPUTE_MARKER_SCHEMA_VERSION,
            "slug": task.slug,
            "level": task.level,
            "scope": task.scope_name,
            "model": task.model,
            "scenario": task.scenario,
            "required_vars": list(task.required_vars),
            "common_years_hash": task.common_years_hash,
            "source_signatures": dict(task.source_signatures),
            "boundary_path": "/tmp/boundary.geojson",
            "boundary_mtime_ns": 123,
            "yearly_file_count": 620,
            "period_file_count": 620,
            "yearly_cleanup_policy": "preserve",
        },
    )
    monkeypatch.setattr(CMP, "_boundary_signature", lambda _level, _state: ("/tmp/boundary.geojson", 123))
    monkeypatch.setattr(CMP, "_task_output_file_counts", lambda **_kwargs: (620, 620))

    status = CMP.task_completion_marker_status(task)

    assert status.valid is False
    assert status.reason == "yearly_cleanup_policy_mismatch"


def test_preserve_compute_marker_reports_missing_yearly_files(monkeypatch) -> None:
    task = _task(level="block")
    task.yearly_cleanup_policy = "preserve"
    monkeypatch.setattr(
        CMP,
        "_load_marker_json",
        lambda _path: {
            "schema_version": CMP.COMPUTE_MARKER_SCHEMA_VERSION,
            "slug": task.slug,
            "level": task.level,
            "scope": task.scope_name,
            "model": task.model,
            "scenario": task.scenario,
            "required_vars": list(task.required_vars),
            "common_years_hash": task.common_years_hash,
            "source_signatures": dict(task.source_signatures),
            "boundary_path": "/tmp/boundary.geojson",
            "boundary_mtime_ns": 123,
            "yearly_file_count": 620,
            "period_file_count": 620,
            "yearly_cleanup_policy": "preserve",
        },
    )
    monkeypatch.setattr(CMP, "_boundary_signature", lambda _level, _state: ("/tmp/boundary.geojson", 123))
    monkeypatch.setattr(CMP, "_task_output_file_counts", lambda **_kwargs: (0, 620))

    status = CMP.task_completion_marker_status(task)

    assert status.valid is False
    assert status.reason == "yearly_files_missing_under_preserve_policy"


def test_aridity_marker_requires_current_method_version(monkeypatch) -> None:
    """A successful old run must not skip a PET-validity correction."""
    from dataclasses import replace

    task = replace(_task(), slug="aridity_index_p_over_pet")
    monkeypatch.setattr(CMP, "_load_marker_json", lambda _path: {"schema_version": CMP.COMPUTE_MARKER_SCHEMA_VERSION})
    status = CMP.task_completion_marker_status(task)
    assert not status.valid
    assert status.reason == "compute_marker_aridity_method_mismatch"


def test_aridity_marker_writer_records_method_version(monkeypatch) -> None:
    from dataclasses import replace

    task = replace(_task(), slug="aridity_index_p_over_pet")
    captured = {}
    monkeypatch.setattr(CMP, "_boundary_signature", lambda *_args: ("boundary", 123))
    monkeypatch.setattr(CMP, "_write_marker_json", lambda path, payload: captured.update(payload))
    CMP._write_task_completion_marker(task)
    assert captured["aridity_method_version"] == CMP.ARIDITY_GRIDFIRST_METHOD_VERSION


def test_aridity_ensemble_marker_requires_current_method_version(monkeypatch) -> None:
    """Old ensembles must be rebuilt after a PET-validity correction."""
    monkeypatch.setattr(CMP, "_load_marker_json", lambda _path: {"schema_version": CMP.ENSEMBLE_MARKER_SCHEMA_VERSION})
    status = CMP.ensemble_completion_marker_status(
        slug="aridity_index_p_over_pet", level="district", scope_name="Lakshadweep",
    )
    assert not status.valid
    assert status.reason == "ensemble_marker_aridity_method_mismatch"


def test_aridity_ensemble_marker_writer_records_method_version(monkeypatch) -> None:
    captured = {}
    monkeypatch.setattr(CMP, "_boundary_signature", lambda *_args: ("boundary", 123))
    monkeypatch.setattr(CMP, "_write_marker_json", lambda path, payload: captured.update(payload))
    CMP._write_ensemble_completion_marker(
        slug="aridity_index_p_over_pet", level="district", scope_name="Lakshadweep", expected_output_count=3,
    )
    assert captured["aridity_method_version"] == CMP.ARIDITY_GRIDFIRST_METHOD_VERSION

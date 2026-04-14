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


def test_district_compute_marker_still_requires_exact_yearly_counts(monkeypatch) -> None:
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
    assert status.reason == "compute_marker_output_count_mismatch"

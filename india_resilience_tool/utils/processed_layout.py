"""
Shared layout helpers for Parquet-native processed metric storage.

These helpers define the canonical build/published/archive structure under
`processed_parquet/<metric>/...` while keeping legacy `processed/` untouched.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal


AdminLevel = Literal["district", "block", "basin", "sub_basin"]


def get_level_folder(level: AdminLevel) -> str:
    """Return the canonical on-disk folder name for a spatial level."""
    if level == "sub_basin":
        return "sub_basins"
    if level == "basin":
        return "basins"
    if level == "block":
        return "blocks"
    return "districts"


def metric_contract_root(metric_root: Path) -> Path:
    """Return the metric contract root regardless of whether a child root was passed."""
    root = Path(metric_root)
    if root.name in {"build", "published", "archive"}:
        return root.parent
    return root


def metric_build_root(metric_root: Path) -> Path:
    """Return the canonical build root for a metric."""
    return metric_contract_root(metric_root) / "build"


def metric_published_root(metric_root: Path) -> Path:
    """Return the canonical published root for a metric."""
    return metric_contract_root(metric_root) / "published"


def metric_archive_root(metric_root: Path, timestamp: str) -> Path:
    """Return the canonical archive root for one publish timestamp."""
    return metric_contract_root(metric_root) / "archive" / str(timestamp).strip()


def summary_root(base_root: Path, *, state: str, level: AdminLevel) -> Path:
    """Return the root that holds master/state-summary artifacts for a level."""
    root = Path(base_root)
    if level in {"basin", "sub_basin"}:
        return root / "hydro"
    return root / str(state)


def level_root(base_root: Path, *, state: str, level: AdminLevel) -> Path:
    """Return the root that holds level-scoped datasets for a level."""
    return summary_root(base_root, state=state, level=level) / get_level_folder(level)


def model_yearly_dataset_root(base_root: Path, *, state: str, level: AdminLevel) -> Path:
    """Return the canonical dataset root for per-model yearly rows."""
    return level_root(base_root, state=state, level=level) / "models" / "yearly"


def model_period_dataset_root(base_root: Path, *, state: str, level: AdminLevel) -> Path:
    """Return the canonical dataset root for per-model period rows."""
    return level_root(base_root, state=state, level=level) / "models" / "periods"


def ensemble_yearly_dataset_root(base_root: Path, *, state: str, level: AdminLevel) -> Path:
    """Return the canonical dataset root for yearly ensemble rows."""
    return level_root(base_root, state=state, level=level) / "ensembles" / "yearly"


def coverage_qc_dataset_root(base_root: Path, *, state: str, level: AdminLevel) -> Path:
    """Return the canonical dataset root for spatial coverage QC rows."""
    return level_root(base_root, state=state, level=level) / "coverage_qc"

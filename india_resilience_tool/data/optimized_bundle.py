"""
Path helpers and minimal contracts for the `processed_optimised` runtime bundle.

The optimized bundle is the compact, deployable dashboard-serving contract. It is
kept separate from the legacy `processed/` tree so the old outputs can coexist
during migration and validation.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal, Optional

from india_resilience_tool.config.paths import get_paths_config


AdminLevel = Literal["district", "block", "basin", "sub_basin"]
SpatialFamily = Literal["admin", "hydro"]

OPTIMIZED_DIRNAME = "processed_optimised"
_METRICS_DIRNAME = "metrics"
_GEOMETRY_DIRNAME = "geometry"
_CONTEXT_DIRNAME = "context"
_MANIFEST_FILENAME = "bundle_manifest.json"


def resolve_optimized_bundle_root(*, data_dir: Optional[Path] = None) -> Path:
    """
    Return the root directory of the optimized runtime bundle.

    Environment override:
      - `IRT_PROCESSED_OPTIMISED_ROOT`
    """
    env_root = os.getenv("IRT_PROCESSED_OPTIMISED_ROOT")
    if env_root:
        return Path(env_root).expanduser().resolve()

    if data_dir is None:
        data_dir = get_paths_config().data_dir
    return (Path(data_dir) / OPTIMIZED_DIRNAME).resolve()


def resolve_optimized_metric_root(
    slug: str,
    *,
    data_dir: Optional[Path] = None,
) -> Path:
    """Return the optimized runtime root for one metric slug."""
    root = resolve_optimized_bundle_root(data_dir=data_dir)
    return (root / _METRICS_DIRNAME / str(slug).strip()).resolve()


def _metric_root_path(metric_root: Path | str) -> Path:
    return Path(metric_root).expanduser().resolve()


def bundle_manifest_path(*, data_dir: Optional[Path] = None) -> Path:
    """Return the optimized bundle manifest path."""
    return resolve_optimized_bundle_root(data_dir=data_dir) / _MANIFEST_FILENAME


def optimized_master_path(
    slug: str,
    *,
    level: AdminLevel,
    state: Optional[str] = None,
    data_dir: Optional[Path] = None,
) -> Path:
    """
    Return the optimized master Parquet path for a metric/level selection.

    Admin levels are stored one file per state:
      - `metrics/<slug>/masters/admin/<level>/state=<STATE>.parquet`

    Hydro levels are stored one file per level:
      - `metrics/<slug>/masters/hydro/<level>/master.parquet`
    """
    metric_root = resolve_optimized_metric_root(slug, data_dir=data_dir)
    return optimized_master_path_from_metric_root(metric_root, level=level, state=state)


def optimized_master_path_from_metric_root(
    metric_root: Path | str,
    *,
    level: AdminLevel,
    state: Optional[str] = None,
) -> Path:
    """Return the optimized master path when the metric root is already known."""
    metric_root = _metric_root_path(metric_root)
    level_norm = str(level).strip().lower()
    if level_norm in {"district", "block"}:
        if not state:
            raise ValueError(f"State is required for optimized admin master level={level_norm!r}")
        return metric_root / "masters" / "admin" / level_norm / f"state={str(state).strip()}.parquet"
    return metric_root / "masters" / "hydro" / level_norm / "master.parquet"


def optimized_yearly_ensemble_path(
    slug: str,
    *,
    level: AdminLevel,
    state: Optional[str] = None,
    data_dir: Optional[Path] = None,
) -> Path:
    """
    Return the optimized yearly-ensemble Parquet path for a metric/level selection.
    """
    metric_root = resolve_optimized_metric_root(slug, data_dir=data_dir)
    return optimized_yearly_ensemble_path_from_metric_root(metric_root, level=level, state=state)


def optimized_yearly_ensemble_path_from_metric_root(
    metric_root: Path | str,
    *,
    level: AdminLevel,
    state: Optional[str] = None,
) -> Path:
    """Return the optimized yearly-ensemble path when the metric root is already known."""
    metric_root = _metric_root_path(metric_root)
    level_norm = str(level).strip().lower()
    if level_norm in {"district", "block"}:
        if not state:
            raise ValueError(f"State is required for optimized yearly ensemble level={level_norm!r}")
        return (
            metric_root
            / "yearly_ensemble"
            / "admin"
            / level_norm
            / f"state={str(state).strip()}.parquet"
        )
    return metric_root / "yearly_ensemble" / "hydro" / level_norm / "master.parquet"


def optimized_yearly_models_path(
    slug: str,
    *,
    level: AdminLevel,
    state: Optional[str] = None,
    data_dir: Optional[Path] = None,
) -> Path:
    """
    Return the optimized yearly-model Parquet path for a metric/level selection.
    """
    metric_root = resolve_optimized_metric_root(slug, data_dir=data_dir)
    return optimized_yearly_models_path_from_metric_root(metric_root, level=level, state=state)


def optimized_yearly_models_path_from_metric_root(
    metric_root: Path | str,
    *,
    level: AdminLevel,
    state: Optional[str] = None,
) -> Path:
    """Return the optimized yearly-model path when the metric root is already known."""
    metric_root = _metric_root_path(metric_root)
    level_norm = str(level).strip().lower()
    if level_norm in {"district", "block"}:
        if not state:
            raise ValueError(f"State is required for optimized yearly models level={level_norm!r}")
        return (
            metric_root
            / "yearly_models"
            / "admin"
            / level_norm
            / f"state={str(state).strip()}.parquet"
        )
    return metric_root / "yearly_models" / "hydro" / level_norm / "master.parquet"


def optimized_geometry_path(
    *,
    level: AdminLevel,
    state: Optional[str] = None,
    basin_id: Optional[str] = None,
    data_dir: Optional[Path] = None,
) -> Path:
    """
    Return the optimized display-geometry GeoJSON path for one spatial slice.
    """
    root = resolve_optimized_bundle_root(data_dir=data_dir) / _GEOMETRY_DIRNAME
    level_norm = str(level).strip().lower()
    if level_norm == "district":
        if not state:
            raise ValueError("State is required for district geometry.")
        return root / "admin" / "district" / f"state={str(state).strip()}.geojson"
    if level_norm == "block":
        if not state:
            raise ValueError("State is required for block geometry.")
        return root / "admin" / "block" / f"state={str(state).strip()}.geojson"
    if level_norm == "basin":
        return root / "hydro" / "basin.geojson"
    if level_norm == "sub_basin":
        if not basin_id:
            raise ValueError("basin_id is required for sub-basin geometry.")
        return root / "hydro" / "sub_basin" / f"basin_id={str(basin_id).strip()}.geojson"
    raise ValueError(f"Unsupported geometry level: {level!r}")


def optimized_context_path(name: str, *, data_dir: Optional[Path] = None) -> Path:
    """Return a named context artifact path inside the optimized bundle."""
    return resolve_optimized_bundle_root(data_dir=data_dir) / _CONTEXT_DIRNAME / str(name).strip()


def optimized_master_sources_for_level(
    slug: str,
    *,
    level: AdminLevel,
    selected_state: str,
    data_dir: Optional[Path] = None,
) -> tuple[Path, ...]:
    """
    Return one or many optimized master paths for the requested UI selection.

    For admin levels:
      - a concrete state returns one state Parquet
      - `All` returns every `state=*.parquet` file for the level

    Hydro levels always return one file.
    """
    level_norm = str(level).strip().lower()
    metric_root = resolve_optimized_metric_root(slug, data_dir=data_dir)
    return optimized_master_sources_from_metric_root(
        metric_root,
        level=level_norm,
        selected_state=selected_state,
    )


def optimized_master_sources_from_metric_root(
    metric_root: Path | str,
    *,
    level: AdminLevel,
    selected_state: str,
) -> tuple[Path, ...]:
    """Return one or many optimized master paths when the metric root is already known."""
    metric_root = _metric_root_path(metric_root)
    level_norm = str(level).strip().lower()
    if level_norm in {"basin", "sub_basin"}:
        return (optimized_master_path_from_metric_root(metric_root, level=level_norm),)

    level_dir = metric_root / "masters" / "admin" / level_norm
    state_norm = str(selected_state or "All").strip() or "All"
    if state_norm != "All":
        return (level_dir / f"state={state_norm}.parquet",)

    return tuple(sorted(level_dir.glob("state=*.parquet")))


def list_optimized_states_for_level(
    slug: str,
    *,
    level: AdminLevel,
    data_dir: Optional[Path] = None,
) -> list[str]:
    """
    List states available for one optimized admin metric level.
    """
    level_norm = str(level).strip().lower()
    if level_norm not in {"district", "block"}:
        return []
    metric_root = resolve_optimized_metric_root(slug, data_dir=data_dir)
    return list_optimized_states_for_metric_root(metric_root, level=level_norm)


def list_optimized_states_for_metric_root(
    metric_root: Path | str,
    *,
    level: AdminLevel,
) -> list[str]:
    """List states available for one optimized admin metric level from a metric root."""
    metric_root = _metric_root_path(metric_root)
    level_norm = str(level).strip().lower()
    if level_norm not in {"district", "block"}:
        return []
    level_dir = metric_root / "masters" / "admin" / level_norm
    out: list[str] = []
    for path in sorted(level_dir.glob("state=*.parquet")):
        stem = path.stem
        if not stem.startswith("state="):
            continue
        state = stem.split("=", 1)[1].strip()
        if state:
            out.append(state)
    return out


def is_optimized_metric_root(path: Path) -> bool:
    """
    Return True when the path looks like an optimized metric root.
    """
    p = Path(path)
    if p.parent.name != _METRICS_DIRNAME:
        return False
    return any(
        (p / child).exists()
        for child in ("masters", "yearly_ensemble", "yearly_models")
    )

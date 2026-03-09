"""
Central path and environment-variable semantics for the India Resilience Tool (IRT).

This module is the single source of truth for:
- repo-root discovery
- DATA_DIR default behavior (and optional IRT_DATA_DIR override)
- processed-root resolution semantics (including portfolio multi-index behavior)
- pilot state/debug environment defaults
- ADM2 (district) and ADM3 (block/subdistrict) boundary paths

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional


def find_repo_root(start: Optional[Path] = None) -> Path:
    """
    Walk up from `start` until we find something that looks like the repo root.

    Markers:
      - .git
      - pyproject.toml
      - setup.cfg
      - requirements.txt

    Falls back to the directory of this file if no marker is found.
    """
    if start is None:
        start = Path(__file__).resolve()

    markers = [".git", "pyproject.toml", "setup.cfg", "requirements.txt"]

    for parent in [start] + list(start.parents):
        if any((parent / m).exists() for m in markers):
            return parent

    return Path(__file__).resolve().parent


@dataclass(frozen=True)
class PathsConfig:
    """
    Typed container for commonly used IRT paths.

    Notes:
      - `data_dir` defaults to <projects_root>/irt_data unless IRT_DATA_DIR is set.
      - Keep these fields stable once downstream modules depend on them.
    """

    repo_root: Path
    projects_root: Path
    data_dir: Path
    data_root: Path
    districts_path: Path
    blocks_path: Path  # NEW: ADM3 block/subdistrict boundaries
    basins_path: Path
    subbasins_path: Path
    base_output_root: Path


def _get_data_dir(repo_root: Path) -> Path:
    """
    Resolve DATA_DIR with optional IRT_DATA_DIR override.

    Contract:
      - If IRT_DATA_DIR is set, use it as DATA_DIR.
      - Otherwise, default to <repo_root.parent>/irt_data (matches current repo behavior).
    """
    env_data_dir = os.getenv("IRT_DATA_DIR")
    if env_data_dir:
        return Path(env_data_dir).expanduser().resolve()

    projects_root = repo_root.parent
    return (projects_root / "irt_data").resolve()


def get_paths_config() -> PathsConfig:
    """
    Build a PathsConfig from the current environment.

    This is safe to call from both dashboard and pipeline code.
    """
    repo_root = find_repo_root()
    projects_root = repo_root.parent
    data_dir = _get_data_dir(repo_root)

    return PathsConfig(
        repo_root=repo_root,
        projects_root=projects_root,
        data_dir=data_dir,
        data_root=data_dir / "r1i1p1f1",
        districts_path=data_dir / "districts_4326.geojson",
        blocks_path=data_dir / "blocks_4326.geojson",  # NEW
        basins_path=data_dir / "basins.geojson",
        subbasins_path=data_dir / "subbasins.geojson",
        base_output_root=data_dir / "processed",
    )


def pilot_state_default() -> str:
    """
    Return the pilot state with contractual default.

    Contract:
      - IRT_PILOT_STATE default is "Telangana"
    """
    return os.getenv("IRT_PILOT_STATE", "Telangana")


def debug_enabled_default() -> bool:
    """
    Return debug flag based on IRT_DEBUG environment variable.

    Contract:
      - Truthy values: "1", "true", "yes", "y" (case-insensitive)
      - Default: False
    """
    val = str(os.getenv("IRT_DEBUG", "")).strip().lower()
    return val in {"1", "true", "yes", "y"}


ProcessedRootMode = Literal["single", "portfolio"]

# NEW: Administrative level type
SpatialFamily = Literal["admin", "hydro"]
AdminLevel = Literal["district", "block", "basin", "sub_basin"]


def resolve_processed_root(
    slug: str,
    *,
    data_dir: Optional[Path] = None,
    mode: ProcessedRootMode = "single",
) -> Path:
    """
    Resolve processed root for a given index slug.

    Contract (must preserve existing dashboard behavior):

    1) If IRT_PROCESSED_ROOT is set:
       - mode == "single":
           return Path(IRT_PROCESSED_ROOT) as-is (NO slug append)
           (This matches current single-index code path.)
       - mode == "portfolio":
           treat env root as either:
             a) already pointing to .../<slug>  -> use it
             b) a base dir                      -> append /<slug>

    2) If IRT_PROCESSED_ROOT is not set:
       default to DATA_DIR/processed/<slug>
       where DATA_DIR is:
         - provided via `data_dir`, else
         - derived via get_paths_config().data_dir

    Returns:
      Absolute resolved Path to processed root for the requested slug.
    """
    env_root = os.getenv("IRT_PROCESSED_ROOT")
    if env_root:
        base_path = Path(env_root).expanduser()
        if mode == "portfolio":
            proc_root = base_path if base_path.name == slug else (base_path / slug)
        else:
            proc_root = base_path
        return proc_root.resolve()

    if data_dir is None:
        data_dir = get_paths_config().data_dir

    return (data_dir / "processed" / slug).resolve()


def get_boundary_path(level: AdminLevel) -> Path:
    """
    Get the boundary file path for a given administrative level.
    
    Args:
        level: "district", "block", "basin", or "sub_basin"
        
    Returns:
        Path to the GeoJSON boundary file
    """
    cfg = get_paths_config()
    if level == "sub_basin":
        return cfg.subbasins_path
    if level == "basin":
        return cfg.basins_path
    if level == "block":
        return cfg.blocks_path
    return cfg.districts_path


def get_master_csv_filename(level: AdminLevel) -> str:
    """
    Get the master CSV filename for a given administrative level.
    
    Args:
        level: "district", "block", "basin", or "sub_basin"
        
    Returns:
        Filename string (e.g., "master_metrics_by_district.csv")
    """
    if level == "sub_basin":
        return "master_metrics_by_sub_basin.csv"
    if level == "basin":
        return "master_metrics_by_basin.csv"
    if level == "block":
        return "master_metrics_by_block.csv"
    return "master_metrics_by_district.csv"


def get_unit_name_column(level: AdminLevel) -> str:
    """Get the canonical primary display column for a spatial level."""
    if level == "sub_basin":
        return "subbasin_name"
    if level == "basin":
        return "basin_name"
    if level == "block":
        return "block_name"
    return "district_name"


# Convenience constants matching legacy root-level paths.py exports
_CFG = get_paths_config()
REPO_ROOT: Path = _CFG.repo_root
PROJECTS_ROOT: Path = _CFG.projects_root
DATA_DIR: Path = _CFG.data_dir
DATA_ROOT: Path = _CFG.data_root
DISTRICTS_PATH: Path = _CFG.districts_path
BLOCKS_PATH: Path = _CFG.blocks_path  # NEW
BASINS_PATH: Path = _CFG.basins_path
SUBBASINS_PATH: Path = _CFG.subbasins_path
BASE_OUTPUT_ROOT: Path = _CFG.base_output_root

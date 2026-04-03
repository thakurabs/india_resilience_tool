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
    river_network_path: Path
    river_network_display_path: Path
    river_basin_reconciliation_path: Path
    river_subbasin_diagnostics_path: Path
    river_reaches_path: Path
    river_nodes_path: Path
    river_adjacency_path: Path
    river_topology_qa_path: Path
    river_missing_assignments_path: Path
    river_missing_assignments_geojson_path: Path
    district_subbasin_crosswalk_path: Path
    block_subbasin_crosswalk_path: Path
    district_basin_crosswalk_path: Path
    block_basin_crosswalk_path: Path
    base_output_root: Path
    optimized_output_root: Path


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
        river_network_path=data_dir / "river_network.parquet",
        river_network_display_path=data_dir / "river_network_display.geojson",
        river_basin_reconciliation_path=data_dir / "river_basin_name_reconciliation.csv",
        river_subbasin_diagnostics_path=data_dir / "river_subbasin_diagnostics.csv",
        river_reaches_path=data_dir / "river_reaches.parquet",
        river_nodes_path=data_dir / "river_nodes.parquet",
        river_adjacency_path=data_dir / "river_adjacency.parquet",
        river_topology_qa_path=data_dir / "river_topology_qa.csv",
        river_missing_assignments_path=data_dir / "river_missing_assignments.csv",
        river_missing_assignments_geojson_path=data_dir / "river_missing_assignments.geojson",
        district_subbasin_crosswalk_path=data_dir / "district_subbasin_crosswalk.csv",
        block_subbasin_crosswalk_path=data_dir / "block_subbasin_crosswalk.csv",
        district_basin_crosswalk_path=data_dir / "district_basin_crosswalk.csv",
        block_basin_crosswalk_path=data_dir / "block_basin_crosswalk.csv",
        base_output_root=data_dir / "processed",
        optimized_output_root=data_dir / "processed_optimised",
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


def resolve_processed_optimised_root(
    slug: str,
    *,
    data_dir: Optional[Path] = None,
    mode: ProcessedRootMode = "portfolio",
) -> Path:
    """
    Resolve the optimized processed root for one metric slug.

    Contract:
      - If `IRT_PROCESSED_OPTIMISED_ROOT` is set and already points at `<slug>`,
        use it directly.
      - If it is set to a base bundle root, append `/metrics/<slug>`.
      - Otherwise default to `DATA_DIR/processed_optimised/metrics/<slug>`.

    Compatibility:
      - Accepts the legacy `mode` keyword so callers can swap between
        `resolve_processed_root(...)` and `resolve_processed_optimised_root(...)`
        without branching on function signatures.
      - `mode` does not affect optimized-root resolution.
    """
    _ = mode
    env_root = os.getenv("IRT_PROCESSED_OPTIMISED_ROOT")
    if env_root:
        base_path = Path(env_root).expanduser()
        if base_path.name == slug:
            return base_path.resolve()
        if base_path.name == "metrics":
            return (base_path / slug).resolve()
        return (base_path / "metrics" / slug).resolve()

    if data_dir is None:
        data_dir = get_paths_config().data_dir

    return (data_dir / "processed_optimised" / "metrics" / slug).resolve()


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
RIVER_NETWORK_DISPLAY_PATH: Path = _CFG.river_network_display_path
RIVER_BASIN_RECONCILIATION_PATH: Path = _CFG.river_basin_reconciliation_path
RIVER_NETWORK_PATH: Path = _CFG.river_network_path
RIVER_SUBBASIN_DIAGNOSTICS_PATH: Path = _CFG.river_subbasin_diagnostics_path
RIVER_REACHES_PATH: Path = _CFG.river_reaches_path
RIVER_NODES_PATH: Path = _CFG.river_nodes_path
RIVER_ADJACENCY_PATH: Path = _CFG.river_adjacency_path
RIVER_TOPOLOGY_QA_PATH: Path = _CFG.river_topology_qa_path
RIVER_MISSING_ASSIGNMENTS_PATH: Path = _CFG.river_missing_assignments_path
RIVER_MISSING_ASSIGNMENTS_GEOJSON_PATH: Path = _CFG.river_missing_assignments_geojson_path
DISTRICT_SUBBASIN_CROSSWALK_PATH: Path = _CFG.district_subbasin_crosswalk_path
BLOCK_SUBBASIN_CROSSWALK_PATH: Path = _CFG.block_subbasin_crosswalk_path
DISTRICT_BASIN_CROSSWALK_PATH: Path = _CFG.district_basin_crosswalk_path
BLOCK_BASIN_CROSSWALK_PATH: Path = _CFG.block_basin_crosswalk_path
BASE_OUTPUT_ROOT: Path = _CFG.base_output_root
OPTIMIZED_OUTPUT_ROOT: Path = _CFG.optimized_output_root

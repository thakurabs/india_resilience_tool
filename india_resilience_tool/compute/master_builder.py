# build_master_metrics.py
#!/usr/bin/env python3
"""
build_master_metrics.py

Build master CSVs for processed metric outputs at admin or hydro levels.
Includes progress reporting for large datasets.

Parallelism:
    The wide-master build (row construction per admin unit) can run in parallel.
    Control worker processes via --workers / -w (default: 75% of available CPUs).

Supports BOTH folder structures for districts:
- NEW: {state}/districts/{district}/{model}/{scenario}/
- OLD: {state}/{district}/{model}/{scenario}/ (backward compatible)

For blocks, only supports NEW structure:
- {state}/blocks/{district}/{block}/{model}/{scenario}/

For hydro, uses the fixed hydro root:
- hydro/basins/{basin}/{model}/{scenario}/
- hydro/sub_basins/{basin}/{sub_basin}/{model}/{scenario}/

Usage:
    python build_master_metrics.py                         # Default: district + block
    python build_master_metrics.py --level district         # District only
    python build_master_metrics.py --level block            # Block only
    python build_master_metrics.py --state Telangana        # Filter to an admin state (batch mode)
    python build_master_metrics.py --metrics tx90p tn90p     # Filter to a metric set (batch mode)
    python build_master_metrics.py --level basin --metrics tas_annual_mean  # Hydro basin masters
    python build_master_metrics.py --workers 8               # Use 8 worker processes

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path
import argparse
import json
import math
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from paths import get_boundary_path

from india_resilience_tool.data.hydro_loader import load_local_hydro


def _get_mp_module():
    """Return a multiprocessing-compatible module.

    Prefers the external `multiprocess` library (better pickling on some
    platforms), but falls back to Python's built-in `multiprocessing`.
    """
    try:
        import multiprocess as mp  # type: ignore
        return mp
    except Exception:
        import multiprocessing as mp
        return mp


def default_workers_75pct() -> int:
    """Return default workers as 75% of available CPUs (min 1).

    Uses floor semantics (consistent with `int(cpu * 0.75)`), but guards
    against returning 0.
    """
    try:
        cpu = os.cpu_count() or 1
    except Exception:
        cpu = 1
    return max(1, int(cpu * 0.75))


AdminLevel = Literal["district", "block", "basin", "sub_basin"]
CLILevel = Literal["district", "block", "basin", "sub_basin", "both"]

# Folder names for clean separation
DISTRICT_FOLDER = "districts"
BLOCK_FOLDER = "blocks"
BASIN_FOLDER = "basins"
SUB_BASIN_FOLDER = "sub_basins"
HYDRO_ROOT_NAME = "hydro"


# -----------------------------------------------------------------------------
# Imports with fallbacks
# -----------------------------------------------------------------------------
def _try_import_registries() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Return (VARIABLES, METRICS_BY_SLUG) with robust import fallbacks.

    Tries package-first imports (preferred), then legacy root-level modules.
    On failure, raises an ImportError that includes the underlying causes.
    """
    import importlib

    def _import_symbol(module_candidates: Sequence[str], symbol: str) -> Any:
        errors: List[str] = []
        for mod_name in module_candidates:
            try:
                mod = importlib.import_module(mod_name)
                return getattr(mod, symbol)
            except Exception as e:
                errors.append(f"{mod_name}.{symbol}: {type(e).__name__}: {e}")
        joined = "\n  - " + "\n  - ".join(errors) if errors else ""
        raise ImportError(f"Could not import {symbol}. Tried:{joined}")

    # Prefer the packaged modules; keep legacy fallbacks in case someone has root-level copies.
    variables_obj = _import_symbol(
        (
            "india_resilience_tool.config.variables",
            "config.variables",
            "variables",
        ),
        "VARIABLES",
    )
    metrics_obj = _import_symbol(
        (
            "india_resilience_tool.config.metrics_registry",
            "config.metrics_registry",
            "metrics_registry",
        ),
        "METRICS_BY_SLUG",
    )

    variables = dict(variables_obj)
    metrics_by_slug = dict(metrics_obj)

    return variables, metrics_by_slug


def _try_import_processed_root() -> Optional[Path]:
    """Try to import BASE_OUTPUT_ROOT from paths.py."""
    try:
        from paths import BASE_OUTPUT_ROOT
        return Path(BASE_OUTPUT_ROOT)
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def safe_read_csv(path: Path) -> pd.DataFrame:
    """Try robust CSV read with encoding fallbacks."""
    path = Path(path)
    try:
        return pd.read_csv(path)
    except Exception:
        try:
            return pd.read_csv(path, encoding="ISO-8859-1")
        except Exception:
            return pd.read_csv(path, encoding="utf-8", errors="replace")


def sanitize_colname(s: str) -> str:
    """Return a machine-friendly column string."""
    s = str(s).strip()
    s = s.replace(" ", "_").replace("-", "_").replace("/", "_").replace("%", "pct")
    s = s.replace("(", "").replace(")", "").replace(",", "").replace(":", "").replace("'", "")
    while "__" in s:
        s = s.replace("__", "_")
    return s


def hydro_folder_name(text: str) -> str:
    """Return the folder-safe hydro token used by the compute pipeline."""
    return str(text).replace(" ", "_").replace("/", "_").strip()


def _is_hydro_level(level: AdminLevel) -> bool:
    """Return True when a level uses the fixed hydro root rather than a state root."""
    return level in {"basin", "sub_basin"}


def _resolve_scope_name(
    level: AdminLevel,
    state: Optional[str],
    *,
    verbose: bool = False,
) -> str:
    """Resolve the effective directory scope name for admin vs hydro levels."""
    if _is_hydro_level(level):
        if state and str(state).strip() and verbose:
            print(
                f"NOTE: --state is ignored for hydro level '{level}'; "
                f"using '{HYDRO_ROOT_NAME}' root."
            )
        return HYDRO_ROOT_NAME

    state_name = str(state or "").strip()
    if not state_name:
        raise ValueError(f"Level '{level}' requires a real admin state.")
    return state_name


def _build_basin_lookup() -> Dict[str, Dict[str, str]]:
    """Build a lookup from basin folder token to canonical basin identifiers."""
    gdf = load_local_hydro(get_boundary_path("basin"), level="basin")
    lookup: Dict[str, Dict[str, str]] = {}
    for _, row in gdf.iterrows():
        folder_key = hydro_folder_name(row.get("basin_name", ""))
        if not folder_key:
            continue
        lookup[folder_key] = {
            "basin_id": str(row.get("basin_id", "")).strip(),
            "basin_name": str(row.get("basin_name", "")).strip(),
        }
    return lookup


def _build_subbasin_lookup() -> Dict[Tuple[str, str], Dict[str, str]]:
    """Build a lookup from basin/sub-basin folder tokens to canonical identifiers."""
    gdf = load_local_hydro(get_boundary_path("sub_basin"), level="sub_basin")
    lookup: Dict[Tuple[str, str], Dict[str, str]] = {}
    for _, row in gdf.iterrows():
        basin_key = hydro_folder_name(row.get("basin_name", ""))
        subbasin_key = hydro_folder_name(row.get("subbasin_name", ""))
        if not basin_key or not subbasin_key:
            continue
        lookup[(basin_key, subbasin_key)] = {
            "basin_id": str(row.get("basin_id", "")).strip(),
            "basin_name": str(row.get("basin_name", "")).strip(),
            "subbasin_id": str(row.get("subbasin_id", "")).strip(),
            "subbasin_code": str(row.get("subbasin_code", "")).strip(),
            "subbasin_name": str(row.get("subbasin_name", "")).strip(),
        }
    return lookup


def compute_ensemble_stats(values_list: Sequence[float]) -> Optional[Dict[str, float]]:
    """Given a list of numeric values (per-model), return dict of stats."""
    arr = np.array(list(values_list), dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return None
    return {
        "mean": float(np.nanmean(arr)),
        "std": float(np.nanstd(arr, ddof=0)),
        "median": float(np.nanmedian(arr)),
        "p05": float(np.percentile(arr, 5)),
        "p95": float(np.percentile(arr, 95)),
    }


def _build_unit_row_worker(
    args: Tuple[Dict[str, Any], Dict[Tuple[str, str], Dict[str, float]], str]
) -> Dict[str, Any]:
    """Worker: build a single wide-master row.

    Args:
        args: (unit_ident, scen_period_to_models, metric_col_name)

    Returns:
        Row dict for the wide-format master.
    """
    unit_ident, scen_period_to_models, metric_col_name = args
    base_row: Dict[str, Any] = dict(unit_ident)

    for (scenario, period), model_map in scen_period_to_models.items():
        values = list(model_map.values())
        stats = compute_ensemble_stats(values)
        if not stats:
            continue

        col_prefix = f"{metric_col_name}__{scenario}__{period}"
        for stat_name, stat_val in stats.items():
            base_row[f"{col_prefix}__{stat_name}"] = stat_val
        base_row[f"{col_prefix}__n_models"] = len(values)
        base_row[f"{col_prefix}__values_per_model"] = json.dumps(model_map)

    return base_row


def _first_existing_metric_col(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """Return the first metric column found in df from candidates."""
    cols = list(df.columns)
    col_lc = {str(c).strip().lower(): c for c in cols}

    for cand in candidates:
        if not cand:
            continue
        cand_str = str(cand).strip()
        if cand_str in cols:
            return cand_str
        cand_lc = cand_str.lower()
        if cand_lc in col_lc:
            return str(col_lc[cand_lc])
        sanitized_target = sanitize_colname(cand_str).lower()
        for c in cols:
            if sanitize_colname(c).lower() == sanitized_target:
                return str(c)
    return None


def get_master_csv_filename(level: AdminLevel) -> str:
    """Get the master CSV filename for a given administrative level."""
    if level == "sub_basin":
        return "master_metrics_by_sub_basin.csv"
    if level == "basin":
        return "master_metrics_by_basin.csv"
    return "master_metrics_by_block.csv" if level == "block" else "master_metrics_by_district.csv"


def get_unit_column_name(level: AdminLevel) -> str:
    """Get the unit column name based on level."""
    if level == "sub_basin":
        return "subbasin_name"
    if level == "basin":
        return "basin_name"
    return "block" if level == "block" else "district"


def get_level_folder(level: AdminLevel) -> str:
    """Get the subfolder name for a given level."""
    if level == "sub_basin":
        return SUB_BASIN_FOLDER
    if level == "basin":
        return BASIN_FOLDER
    return BLOCK_FOLDER if level == "block" else DISTRICT_FOLDER


# -----------------------------------------------------------------------------
# Legacy CSV pruning (filesystem-based; optional maintenance)
# -----------------------------------------------------------------------------
def _prune_legacy_yearly_period_csvs(
    *,
    state_root: Path,
    level: AdminLevel = "district",
    verbose: bool = True,
) -> List[Path]:
    """
    Delete legacy per-unit yearly/period CSVs only when Parquet "gates" exist.

    This is a safety-first cleanup helper intended to avoid deleting legacy CSVs
    before newer consolidated Parquet outputs are present.

    Gates required (both):
      1) `{state_root}/master_metrics_by_{level}.parquet`
      2) Any consolidated yearly ensembles Parquet under:
           `{state_root}/{level_folder}/ensembles/yearly/**/data.parquet`

    Only deletes files matching:
      - `*_yearly.csv`
      - `*_periods.csv`
    while excluding any paths containing known non-legacy folders such as
    `ensembles/` or `raw/`.

    Returns:
        List of Paths that were deleted.
    """
    state_root = Path(state_root)
    level_folder = get_level_folder(level)

    master_gate = state_root / f"master_metrics_by_{level}.parquet"
    ensembles_gate_root = state_root / level_folder / "ensembles" / "yearly"
    ensembles_gate_ok = False
    try:
        ensembles_gate_ok = ensembles_gate_root.exists() and any(
            p.name == "data.parquet" for p in ensembles_gate_root.rglob("data.parquet")
        )
    except Exception:
        ensembles_gate_ok = False

    if not (master_gate.exists() and ensembles_gate_ok):
        if verbose:
            print(
                f"[prune] gates missing for state_root={state_root} level={level}: "
                f"master_parquet={master_gate.exists()} ensembles_parquet={ensembles_gate_ok}"
            )
        return []

    root = state_root / level_folder
    if not root.exists():
        return []

    excluded_parts = {
        "ensembles",
        "raw",
        "plots",
        "pdf_plots",
        "validation_reports",
        "__pycache__",
    }

    deleted: List[Path] = []
    for p in root.rglob("*.csv"):
        name = p.name
        if not (name.endswith("_yearly.csv") or name.endswith("_periods.csv")):
            continue

        parts = set(p.parts)
        if parts & excluded_parts:
            continue
        # Avoid structured "scenario=..." paths used by partitioned stores
        if any("scenario=" in part for part in p.parts):
            continue

        try:
            p.unlink()
            deleted.append(p)
        except Exception as e:
            if verbose:
                print(f"[prune] failed to delete {p}: {e}")

    return deleted


# -----------------------------------------------------------------------------
# Progress helper
# -----------------------------------------------------------------------------
class ProgressReporter:
    def __init__(self, total: int, prefix: str = "Progress", report_every: int = 50):
        self.total = total
        self.prefix = prefix
        self.report_every = report_every
        self.current = 0
        self.start_time = time.time()

    def update(self, n: int = 1):
        self.current += n
        if self.current % self.report_every == 0 or self.current == self.total:
            elapsed = time.time() - self.start_time
            rate = self.current / elapsed if elapsed > 0 else 0
            eta = (self.total - self.current) / rate if rate > 0 else 0
            print(
                f"  {self.prefix}: {self.current}/{self.total} "
                f"({100*self.current/self.total:.1f}%) - {rate:.1f}/s - ETA: {eta:.0f}s"
            )


# -----------------------------------------------------------------------------
# Structure detection
# -----------------------------------------------------------------------------
def _is_model_directory(path: Path) -> bool:
    """Check if a directory looks like a model directory (contains scenario subdirs)."""
    if not path.is_dir():
        return False
    scenarios = {"historical", "ssp245", "ssp585"}
    subdirs = {p.name for p in path.iterdir() if p.is_dir()}
    return bool(subdirs & scenarios)


def _detect_district_structure(state_root: Path, verbose: bool = False) -> Tuple[Optional[Path], str]:
    """Detect whether district data uses new or old folder structure.

    Returns:
        (data_root, structure_type) where structure_type is "new" or "old"
    """
    # Check for new structure: state/districts/{district}/{model}/{scenario}/
    new_path = state_root / DISTRICT_FOLDER
    if new_path.exists():
        for district_dir in new_path.iterdir():
            if not district_dir.is_dir() or district_dir.name == "ensembles":
                continue
            # district_dir should contain model dirs
            for subdir in district_dir.iterdir():
                if subdir.is_dir() and _is_model_directory(subdir):
                    return new_path, "new"

    # Check for old structure: state/{district}/{model}/{scenario}/
    skip_dirs = {"blocks", "districts", "ensembles", "validation_reports", "pdf_plots", "plots"}

    for item in state_root.iterdir():
        if not item.is_dir() or item.name in skip_dirs:
            continue
        # item is a district dir, should contain model dirs
        for subdir in item.iterdir():
            if subdir.is_dir() and _is_model_directory(subdir):
                return state_root, "old"

    return None, "unknown"


# -----------------------------------------------------------------------------
# Data collection with progress (supporting both structures)
# -----------------------------------------------------------------------------
def _collect_district_data(
    state_root: Path,
    state: str,
    metric_col_candidates: Sequence[str],
    verbose: bool = True,
) -> Tuple[List[Dict], List[Dict]]:
    """Collect data from district-level directory structure.

    Supports both old and new folder structures.
    """
    all_rows: List[Dict] = []
    yearly_rows: List[Dict] = []

    # Detect structure
    data_root, structure = _detect_district_structure(state_root, verbose)

    if data_root is None:
        if verbose:
            print(f"  ERROR: Could not detect district data structure in {state_root}")
        return [], []

    if verbose:
        print(f"  Detected {structure.upper()} folder structure")
        if structure == "new":
            print(f"  Data root: {data_root}")
        else:
            print(f"  Data root: {data_root} (old structure - district folders at state level)")

    skip_dirs = {"ensembles", "blocks", "districts", "validation_reports", "pdf_plots", "plots"}
    district_dirs = [p for p in data_root.iterdir() if p.is_dir() and p.name not in skip_dirs]

    # Filter to only actual district directories (those containing model subdirs)
    district_dirs = [
        d for d in district_dirs if any(_is_model_directory(p) for p in d.iterdir() if p.is_dir())
    ]

    if verbose:
        print(f"  Found {len(district_dirs)} district directories")

    progress = ProgressReporter(len(district_dirs), "Districts") if verbose else None

    for ddir in district_dirs:
        district = ddir.name
        model_dirs = [p for p in ddir.iterdir() if p.is_dir() and _is_model_directory(p)]

        for mdir in model_dirs:
            model = mdir.name
            scenario_dirs = [p for p in mdir.iterdir() if p.is_dir()]

            for sdir in scenario_dirs:
                scenario = sdir.name

                # Read periods CSV
                periods_csv = sdir / f"{district}_periods.csv"
                if periods_csv.exists():
                    df_p = safe_read_csv(periods_csv)
                    metric_col = _first_existing_metric_col(df_p, metric_col_candidates)
                    if metric_col and metric_col in df_p.columns:
                        for _, row in df_p.iterrows():
                            all_rows.append(
                                {
                                    "district": district.replace("_", " "),
                                    "state": state,
                                    "model": model,
                                    "scenario": scenario,
                                    "period": row.get("period", ""),
                                    "value": row[metric_col],
                                }
                            )

                # Read yearly CSV
                yearly_csv = sdir / f"{district}_yearly.csv"
                if yearly_csv.exists():
                    df_y = safe_read_csv(yearly_csv)
                    metric_col = _first_existing_metric_col(df_y, metric_col_candidates)
                    if metric_col and metric_col in df_y.columns:
                        for _, row in df_y.iterrows():
                            yearly_rows.append(
                                {
                                    "district": district.replace("_", " "),
                                    "state": state,
                                    "model": model,
                                    "scenario": scenario,
                                    "year": row.get("year", ""),
                                    "value": row[metric_col],
                                }
                            )

        if progress:
            progress.update()

    return all_rows, yearly_rows


def _collect_block_data(
    state_root: Path,
    state: str,
    metric_col_candidates: Sequence[str],
    verbose: bool = True,
) -> Tuple[List[Dict], List[Dict]]:
    """Collect data from block-level directory structure with progress reporting.

    Structure: state/blocks/{district}/{block}/{model}/{scenario}/*.csv
    """
    all_rows: List[Dict] = []
    yearly_rows: List[Dict] = []

    # Blocks only support new structure
    level_root = state_root / BLOCK_FOLDER

    if not level_root.exists():
        if verbose:
            print(f"  ERROR: Block folder not found: {level_root}")
        return [], []

    skip_dirs = {"ensembles"}
    district_dirs = [p for p in level_root.iterdir() if p.is_dir() and p.name not in skip_dirs]

    if verbose:
        print(f"  Found {len(district_dirs)} district directories in blocks/")

    # Count total blocks for progress
    total_blocks = 0
    district_block_map: Dict[Path, List[Path]] = {}
    for ddir in district_dirs:
        block_dirs = [p for p in ddir.iterdir() if p.is_dir()]
        district_block_map[ddir] = block_dirs
        total_blocks += len(block_dirs)

    if verbose:
        print(f"  Found {total_blocks} total blocks across all districts")

    progress = ProgressReporter(total_blocks, "Blocks", report_every=20) if verbose else None

    for ddir, block_dirs in district_block_map.items():
        district = ddir.name

        for bdir in block_dirs:
            block = bdir.name
            # IMPORTANT: only treat real model directories as models
            model_dirs = [p for p in bdir.iterdir() if p.is_dir() and _is_model_directory(p)]

            for mdir in model_dirs:
                model = mdir.name
                scenario_dirs = [p for p in mdir.iterdir() if p.is_dir()]

                for sdir in scenario_dirs:
                    scenario = sdir.name

                    # Read periods CSV
                    periods_csv = sdir / f"{block}_periods.csv"
                    if periods_csv.exists():
                        df_p = safe_read_csv(periods_csv)
                        metric_col = _first_existing_metric_col(df_p, metric_col_candidates)
                        if metric_col and metric_col in df_p.columns:
                            for _, row in df_p.iterrows():
                                all_rows.append(
                                    {
                                        "block": block.replace("_", " "),
                                        "district": district.replace("_", " "),
                                        "state": state,
                                        "model": model,
                                        "scenario": scenario,
                                        "period": row.get("period", ""),
                                        "value": row[metric_col],
                                    }
                                )

                    # Read yearly CSV
                    yearly_csv = sdir / f"{block}_yearly.csv"
                    if yearly_csv.exists():
                        df_y = safe_read_csv(yearly_csv)
                        metric_col = _first_existing_metric_col(df_y, metric_col_candidates)
                        if metric_col and metric_col in df_y.columns:
                            for _, row in df_y.iterrows():
                                yearly_rows.append(
                                    {
                                        "block": block.replace("_", " "),
                                        "district": district.replace("_", " "),
                                        "state": state,
                                        "model": model,
                                        "scenario": scenario,
                                        "year": row.get("year", ""),
                                        "value": row[metric_col],
                                    }
                                )

            if progress:
                progress.update()

    return all_rows, yearly_rows


def _collect_basin_data(
    state_root: Path,
    state: str,
    metric_col_candidates: Sequence[str],
    verbose: bool = True,
) -> Tuple[List[Dict], List[Dict]]:
    """Collect data from basin-level directory structure."""
    all_rows: List[Dict] = []
    yearly_rows: List[Dict] = []
    basin_lookup = _build_basin_lookup()

    level_root = state_root / BASIN_FOLDER
    if not level_root.exists():
        if verbose:
            print(f"  ERROR: Basin folder not found: {level_root}")
        return [], []

    basin_dirs = [p for p in level_root.iterdir() if p.is_dir() and p.name != "ensembles"]
    for bdir in basin_dirs:
        basin = bdir.name
        basin_meta = basin_lookup.get(basin)
        if basin_meta is None:
            if verbose:
                print(f"  WARNING: Basin folder '{basin}' not found in canonical basin lookup")
            basin_meta = {"basin_id": "", "basin_name": basin.replace("_", " ")}
        model_dirs = [p for p in bdir.iterdir() if p.is_dir() and _is_model_directory(p)]
        for mdir in model_dirs:
            model = mdir.name
            for sdir in [p for p in mdir.iterdir() if p.is_dir()]:
                scenario = sdir.name
                periods_csv = sdir / f"{basin}_periods.csv"
                if periods_csv.exists():
                    df_p = safe_read_csv(periods_csv)
                    metric_col = _first_existing_metric_col(df_p, metric_col_candidates)
                    if metric_col and metric_col in df_p.columns:
                        for _, row in df_p.iterrows():
                            all_rows.append(
                                {
                                    "basin": basin_meta["basin_name"],
                                    "basin_id": basin_meta["basin_id"],
                                    "basin_name": basin_meta["basin_name"],
                                    "state": state,
                                    "model": model,
                                    "scenario": scenario,
                                    "period": row.get("period", ""),
                                    "value": row[metric_col],
                                }
                            )

                yearly_csv = sdir / f"{basin}_yearly.csv"
                if yearly_csv.exists():
                    df_y = safe_read_csv(yearly_csv)
                    metric_col = _first_existing_metric_col(df_y, metric_col_candidates)
                    if metric_col and metric_col in df_y.columns:
                        for _, row in df_y.iterrows():
                            yearly_rows.append(
                                {
                                    "basin": basin_meta["basin_name"],
                                    "basin_id": basin_meta["basin_id"],
                                    "basin_name": basin_meta["basin_name"],
                                    "state": state,
                                    "model": model,
                                    "scenario": scenario,
                                    "year": row.get("year", ""),
                                    "value": row[metric_col],
                                }
                            )

    return all_rows, yearly_rows


def _collect_sub_basin_data(
    state_root: Path,
    state: str,
    metric_col_candidates: Sequence[str],
    verbose: bool = True,
) -> Tuple[List[Dict], List[Dict]]:
    """Collect data from sub-basin-level directory structure."""
    all_rows: List[Dict] = []
    yearly_rows: List[Dict] = []
    subbasin_lookup = _build_subbasin_lookup()

    level_root = state_root / SUB_BASIN_FOLDER
    if not level_root.exists():
        if verbose:
            print(f"  ERROR: Sub-basin folder not found: {level_root}")
        return [], []

    basin_dirs = [p for p in level_root.iterdir() if p.is_dir() and p.name != "ensembles"]
    for basin_dir in basin_dirs:
        basin = basin_dir.name
        sub_basin_dirs = [p for p in basin_dir.iterdir() if p.is_dir()]
        for sbdir in sub_basin_dirs:
            sub_basin = sbdir.name
            subbasin_meta = subbasin_lookup.get((basin, sub_basin))
            if subbasin_meta is None:
                if verbose:
                    print(
                        "  WARNING: Sub-basin folder "
                        f"'{basin}/{sub_basin}' not found in canonical sub-basin lookup"
                    )
                subbasin_meta = {
                    "basin_id": "",
                    "basin_name": basin.replace("_", " "),
                    "subbasin_id": "",
                    "subbasin_code": "",
                    "subbasin_name": sub_basin.replace("_", " "),
                }
            model_dirs = [p for p in sbdir.iterdir() if p.is_dir() and _is_model_directory(p)]
            for mdir in model_dirs:
                model = mdir.name
                for sdir in [p for p in mdir.iterdir() if p.is_dir()]:
                    scenario = sdir.name
                    periods_csv = sdir / f"{sub_basin}_periods.csv"
                    if periods_csv.exists():
                        df_p = safe_read_csv(periods_csv)
                        metric_col = _first_existing_metric_col(df_p, metric_col_candidates)
                        if metric_col and metric_col in df_p.columns:
                            for _, row in df_p.iterrows():
                                all_rows.append(
                                {
                                    "sub_basin": subbasin_meta["subbasin_name"],
                                    "basin": subbasin_meta["basin_name"],
                                    "basin_id": subbasin_meta["basin_id"],
                                    "basin_name": subbasin_meta["basin_name"],
                                    "subbasin_id": subbasin_meta["subbasin_id"],
                                    "subbasin_code": subbasin_meta["subbasin_code"],
                                    "subbasin_name": subbasin_meta["subbasin_name"],
                                    "state": state,
                                    "model": model,
                                    "scenario": scenario,
                                        "period": row.get("period", ""),
                                        "value": row[metric_col],
                                    }
                                )

                    yearly_csv = sdir / f"{sub_basin}_yearly.csv"
                    if yearly_csv.exists():
                        df_y = safe_read_csv(yearly_csv)
                        metric_col = _first_existing_metric_col(df_y, metric_col_candidates)
                        if metric_col and metric_col in df_y.columns:
                            for _, row in df_y.iterrows():
                                yearly_rows.append(
                                {
                                    "sub_basin": subbasin_meta["subbasin_name"],
                                    "basin": subbasin_meta["basin_name"],
                                    "basin_id": subbasin_meta["basin_id"],
                                    "basin_name": subbasin_meta["basin_name"],
                                    "subbasin_id": subbasin_meta["subbasin_id"],
                                    "subbasin_code": subbasin_meta["subbasin_code"],
                                    "subbasin_name": subbasin_meta["subbasin_name"],
                                    "state": state,
                                    "model": model,
                                    "scenario": scenario,
                                        "year": row.get("year", ""),
                                        "value": row[metric_col],
                                    }
                                )

    return all_rows, yearly_rows


# -----------------------------------------------------------------------------
# Master building
# -----------------------------------------------------------------------------
def _build_wide_master(
    df_all: pd.DataFrame,
    _metric_col_name: str,
    level: AdminLevel,
    num_workers: int = 1,
    verbose: bool = True,
) -> pd.DataFrame:
    """Build wide-format master DataFrame with ensemble statistics.

    This is typically the most CPU-intensive part of the workflow. When
    ``num_workers > 1``, the per-unit row construction is parallelized.
    """
    unit_col = get_unit_column_name(level)

    if level == "sub_basin":
        id_cols = ["subbasin_id", "subbasin_code", "subbasin_name", "basin_id", "basin_name", "state"]
        units = df_all[id_cols].drop_duplicates()
    elif level == "basin":
        id_cols = ["basin_id", "basin_name", "state"]
        units = df_all[id_cols].drop_duplicates()
    elif level == "block":
        id_cols = ["block", "district", "state"]
        units = df_all[id_cols].drop_duplicates()
    else:
        id_cols = ["district", "state"]
        units = df_all[id_cols].drop_duplicates()

    if verbose:
        print(f"  Building wide master for {len(units)} {unit_col}s...")

    progress = ProgressReporter(len(units), "Units", report_every=50) if verbose else None

    # Pre-aggregate: one value per (unit, scenario, period, model)
    # This dramatically reduces duplicated work when multiple CSV rows exist.
    df_grp = df_all.groupby(id_cols + ["scenario", "period", "model"], as_index=False)["value"].mean()

    def _unit_iter() -> Iterable[Tuple[Dict[str, Any], Dict[Tuple[str, str], Dict[str, float]]]]:
        """Yield (unit_ident, scen_period_to_model_values)."""
        for unit_key, unit_df in df_grp.groupby(id_cols):
            if not isinstance(unit_key, tuple):
                unit_key = (unit_key,)
            unit_ident = {c: v for c, v in zip(id_cols, unit_key)}

            mapping: Dict[Tuple[str, str], Dict[str, float]] = {}
            for (scenario, period), grp in unit_df.groupby(["scenario", "period"]):
                model_map: Dict[str, float] = {}
                for _, r in grp.iterrows():
                    model = str(r.get("model", "") or "")
                    val = r.get("value")
                    if model and pd.notna(val):
                        model_map[model] = float(val)
                if model_map:
                    mapping[(str(scenario), str(period))] = model_map

            yield unit_ident, mapping

    tasks = [(unit_ident, mapping, _metric_col_name) for unit_ident, mapping in _unit_iter()]

    effective_workers = max(1, int(num_workers))
    if verbose:
        if effective_workers > 1:
            print(f"  Using {effective_workers} worker(s) for wide master build")
        else:
            print("  Using single worker for wide master build")

    rows: List[Dict[str, Any]] = []

    if effective_workers <= 1 or len(tasks) <= 1:
        for unit_ident, mapping, mcol in tasks:
            rows.append(_build_unit_row_worker((unit_ident, mapping, mcol)))
            if progress:
                progress.update()
        return pd.DataFrame(rows)

    mp = _get_mp_module()
    # Use spawn for better cross-platform reliability (esp. Windows).
    try:
        ctx = mp.get_context("spawn")
    except Exception:
        ctx = mp

    # Chunking: small, predictable chunks to keep progress responsive.
    chunksize = max(1, int(math.ceil(len(tasks) / (effective_workers * 8))))

    with ctx.Pool(processes=effective_workers) as pool:
        for row in pool.imap_unordered(_build_unit_row_worker, tasks, chunksize=chunksize):
            rows.append(row)
            if progress:
                progress.update()

    return pd.DataFrame(rows)


def _unique_unit_count(df: pd.DataFrame, level: AdminLevel) -> int:
    """Count unique admin units in a dataframe chunk."""
    if df.empty:
        return 0
    if level == "sub_basin":
        return int(df[["subbasin_id", "basin_id", "state"]].drop_duplicates().shape[0])
    if level == "basin":
        return int(df[["basin_id", "state"]].drop_duplicates().shape[0])
    if level == "block":
        return int(df[["block", "district", "state"]].drop_duplicates().shape[0])
    return int(df[["district", "state"]].drop_duplicates().shape[0])


def _build_state_summaries(
    df_all: pd.DataFrame,
    df_yearly: pd.DataFrame,
    _metric_col_name: str,
    level: AdminLevel,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build all state-level summary DataFrames."""
    def _coerce_year_int(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "year" not in df.columns:
            return df
        out = df.copy()
        out["year"] = pd.to_numeric(out["year"], errors="coerce")
        out = out.dropna(subset=["year"])
        if out.empty:
            return out
        out["year"] = out["year"].astype(int)
        return out

    # State model averages (period data)
    state_model_rows: List[Dict[str, Any]] = []
    for (scenario, period, model), grp in df_all.groupby(["scenario", "period", "model"]):
        values = grp["value"].dropna().tolist()
        if values:
            state_model_rows.append(
                {
                    "scenario": scenario,
                    "period": period,
                    "model": model,
                    "value": float(np.mean(values)),
                    "n_units": _unique_unit_count(grp, level),
                }
            )
    state_model_df = pd.DataFrame(state_model_rows)

    # State ensemble stats (period data)
    state_ensemble_rows: List[Dict[str, Any]] = []
    for (scenario, period), grp in df_all.groupby(["scenario", "period"]):
        model_means = grp.groupby("model")["value"].mean().dropna().tolist()
        stats = compute_ensemble_stats(model_means)
        if stats:
            state_ensemble_rows.append(
                {
                    "scenario": scenario,
                    "period": period,
                    **{f"ensemble_{k}": v for k, v in stats.items()},
                    "n_models": len(model_means),
                    "n_units": _unique_unit_count(grp, level),
                }
            )
    state_ensemble_df = pd.DataFrame(state_ensemble_rows)

    # Yearly summaries
    state_yearly_model_df = pd.DataFrame()
    state_yearly_ensemble_df = pd.DataFrame()

    if not df_yearly.empty:
        yearly_model_rows: List[Dict[str, Any]] = []
        for (scenario, year, model), grp in df_yearly.groupby(["scenario", "year", "model"]):
            values = grp["value"].dropna().tolist()
            if values:
                yearly_model_rows.append(
                    {
                        "scenario": scenario,
                        "year": year,
                        "model": model,
                        "value": float(np.mean(values)),
                        "n_units": _unique_unit_count(grp, level),
                    }
                )
        state_yearly_model_df = _coerce_year_int(pd.DataFrame(yearly_model_rows))

        yearly_ensemble_rows: List[Dict[str, Any]] = []
        for (scenario, year), grp in df_yearly.groupby(["scenario", "year"]):
            model_means = grp.groupby("model")["value"].mean().dropna().tolist()
            stats = compute_ensemble_stats(model_means)
            if stats:
                yearly_ensemble_rows.append(
                    {
                        "scenario": scenario,
                        "year": year,
                        **{f"ensemble_{k}": v for k, v in stats.items()},
                        "n_models": len(model_means),
                        "n_units": _unique_unit_count(grp, level),
                    }
                )
        state_yearly_ensemble_df = _coerce_year_int(pd.DataFrame(yearly_ensemble_rows))

    return state_model_df, state_ensemble_df, state_yearly_model_df, state_yearly_ensemble_df


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------
def build_master_metrics(
    output_root: str,
    state: str | None,
    metric_col_in_periods: str = "days_gt_32C",
    out_path: str | None = None,
    attach_centroid_geojson: str | None = None,
    verbose: bool = True,
    metric_col_candidates: Sequence[str] | None = None,
    level: AdminLevel = "district",
    num_workers: int = 1,
) -> pd.DataFrame:
    """Build a master CSV for a single metric scope (state for admin, hydro for hydro)."""
    root = Path(output_root)
    scope_name = _resolve_scope_name(level, state, verbose=verbose)
    state_root = root / scope_name

    if not state_root.exists():
        if verbose:
            label = "Hydro root" if _is_hydro_level(level) else "State root"
            print(f"ERROR: {label} not found: {state_root}", file=sys.stderr)
        return pd.DataFrame()

    if metric_col_candidates is None:
        metric_col_candidates = [metric_col_in_periods, "value"]

    start_time = time.time()

    if verbose:
        print(f"\n{'='*60}")
        print("Building master CSV")
        print(f"{'='*60}")
        print(f"Level: {level}")
        if _is_hydro_level(level):
            print(f"Scope: {scope_name}")
            print(f"Hydro root: {state_root}")
        else:
            print(f"State: {scope_name}")
            print(f"State root: {state_root}")
        print(f"Metric column: {metric_col_in_periods}")
        print()

    # Collect data
    if verbose:
        print("[Step 1/3] Collecting data from CSV files...")

    if level == "sub_basin":
        all_rows, yearly_rows = _collect_sub_basin_data(state_root, scope_name, metric_col_candidates, verbose)
    elif level == "basin":
        all_rows, yearly_rows = _collect_basin_data(state_root, scope_name, metric_col_candidates, verbose)
    elif level == "block":
        all_rows, yearly_rows = _collect_block_data(state_root, scope_name, metric_col_candidates, verbose)
    else:
        all_rows, yearly_rows = _collect_district_data(state_root, scope_name, metric_col_candidates, verbose)

    if not all_rows:
        if verbose:
            label = "scope" if _is_hydro_level(level) else "state"
            print(f"ERROR: No data found for {label}={scope_name} at {level} level", file=sys.stderr)
        return pd.DataFrame()

    if verbose:
        print(f"  Collected {len(all_rows)} period rows, {len(yearly_rows)} yearly rows")
        print()

    df_all = pd.DataFrame(all_rows)
    df_yearly = pd.DataFrame(yearly_rows) if yearly_rows else pd.DataFrame()

    # Build master
    if verbose:
        print("[Step 2/3] Building wide-format master...")

    master = _build_wide_master(
        df_all,
        metric_col_in_periods,
        level,
        num_workers=num_workers,
        verbose=verbose,
    )

    if verbose:
        print()
        print("[Step 3/3] Building state summaries...")

    state_model_df, state_ensemble_df, state_yearly_model_df, state_yearly_ensemble_df = _build_state_summaries(
        df_all, df_yearly, metric_col_in_periods, level
    )

    # Write outputs (master CSV goes in state root)
    if out_path:
        outp = Path(out_path)
        outp.parent.mkdir(parents=True, exist_ok=True)

        if verbose:
            print()
            print("Writing output files...")

        master.to_csv(outp, index=False)
        master.to_parquet(outp.with_suffix(".parquet"), index=False)
        state_model_df.to_csv(outp.parent / f"state_model_averages_{level}.csv", index=False)
        state_ensemble_df.to_csv(outp.parent / f"state_ensemble_stats_{level}.csv", index=False)
        state_yearly_model_df.to_csv(outp.parent / f"state_yearly_model_averages_{level}.csv", index=False)
        state_yearly_ensemble_df.to_csv(outp.parent / f"state_yearly_ensemble_stats_{level}.csv", index=False)

        if verbose:
            print(f"  Master CSV -> {outp}")
            print(f"  Rows: {len(master)}, Columns: {len(master.columns)}")

    elapsed = time.time() - start_time
    if verbose:
        print()
        print(f"{'='*60}")
        print(f"Complete! Total time: {elapsed:.1f}s")
        print(f"{'='*60}")

    return master


# -----------------------------------------------------------------------------
# Directory detection for batch mode
# -----------------------------------------------------------------------------
def _discover_metric_dirs(processed_root: Path) -> List[Path]:
    """Return immediate subdirectories under processed_root."""
    if not processed_root.exists():
        return []
    return sorted([p for p in processed_root.iterdir() if p.is_dir()])


def _looks_like_state_dir(state_dir: Path, level: AdminLevel) -> bool:
    """Check if directory looks like a state directory for given level."""
    if not state_dir.is_dir():
        return False

    if _is_hydro_level(level):
        return False

    if level == "block":
        # Only new structure for blocks
        level_path = state_dir / BLOCK_FOLDER
        if not level_path.exists():
            return False
        patterns = (
            "*/*/*/*/*_periods.csv",
            "*/*/*/*/*_yearly.csv",
            "ensembles/*/*/*/*_yearly_ensemble.csv",
        )
        for pat in patterns:
            try:
                for _ in level_path.glob(pat):
                    return True
            except Exception:
                continue
        return False

    # Districts: check for new structure first
    level_path = state_dir / DISTRICT_FOLDER
    if level_path.exists():
        patterns = ("*/*/*/*_periods.csv", "*/*/*/*_yearly.csv")
        for pat in patterns:
            try:
                for _ in level_path.glob(pat):
                    return True
            except Exception:
                continue

    # Districts: old structure
    patterns = ("*/*/*/*_periods.csv", "*/*/*/*_yearly.csv")
    for pat in patterns:
        try:
            for _ in state_dir.glob(pat):
                return True
        except Exception:
            continue

    return False


def _looks_like_hydro_root(scope_dir: Path, level: AdminLevel) -> bool:
    """Check if a hydro root contains data for the requested hydro level."""
    if not scope_dir.is_dir() or not _is_hydro_level(level):
        return False

    level_path = scope_dir / get_level_folder(level)
    if not level_path.exists():
        return False

    patterns = (
        ("*/*/*/*_periods.csv", "*/*/*/*_yearly.csv", "ensembles/*/*/*_yearly_ensemble.csv")
        if level == "basin"
        else ("*/*/*/*/*_periods.csv", "*/*/*/*/*_yearly.csv", "ensembles/*/*/*/*_yearly_ensemble.csv")
    )
    for pat in patterns:
        try:
            for _ in level_path.glob(pat):
                return True
        except Exception:
            continue
    return False


def _discover_states(metric_root: Path, level: AdminLevel = "district") -> List[str]:
    """Return state directories under a metric root."""
    states: List[str] = []
    for p in metric_root.iterdir():
        if p.is_dir() and _looks_like_state_dir(p, level):
            states.append(p.name)
    return sorted(states)


def _discover_scopes(metric_root: Path, level: AdminLevel = "district") -> List[str]:
    """Return the effective directory scopes for the requested level."""
    if _is_hydro_level(level):
        hydro_root = metric_root / HYDRO_ROOT_NAME
        return [HYDRO_ROOT_NAME] if _looks_like_hydro_root(hydro_root, level) else []
    return _discover_states(metric_root, level)


# -----------------------------------------------------------------------------
# Batch mode
# -----------------------------------------------------------------------------
def build_all_master_metrics(
    processed_root: Path,
    *,
    level: AdminLevel = "district",
    metrics_filter: Optional[Sequence[str]] = None,
    state_filter: Optional[Sequence[str]] = None,
    district_geojson: Optional[str] = None,
    verbose: bool = True,
    skip_existing: bool = False,
    num_workers: int = 1,
) -> None:
    """Build master CSVs for all metrics under processed_root."""
    variables, metrics_by_slug = _try_import_registries()

    metric_dirs = _discover_metric_dirs(processed_root)
    on_disk_slugs = {p.name for p in metric_dirs}

    variables_slugs = set(variables.keys())
    registry_slugs = set(metrics_by_slug.keys())
    eligible_slugs = sorted(on_disk_slugs & variables_slugs & registry_slugs)

    if metrics_filter:
        filt = {str(s).strip() for s in metrics_filter if str(s).strip()}
        if filt:
            eligible_set = set(eligible_slugs)
            missing = sorted(filt - eligible_set)
            eligible_slugs = [s for s in eligible_slugs if s in filt]
            if verbose and missing:
                print(f"[BATCH] requested metrics not eligible / not found: {', '.join(missing)}")

    if verbose:
        print(f"[BATCH] level = {level}")
        print(f"[BATCH] processed_root = {processed_root}")
        print(f"[BATCH] eligible metrics = {len(eligible_slugs)}")

    if not eligible_slugs:
        print("[BATCH] No eligible metrics found.", file=sys.stderr)
        return

    state_filter_norm = {str(s).strip() for s in state_filter if str(s).strip()} if state_filter else None
    if _is_hydro_level(level) and state_filter_norm and verbose:
        print(
            f"[BATCH] NOTE: --state is ignored for hydro level '{level}'; "
            f"using '{HYDRO_ROOT_NAME}' root when present."
        )
        state_filter_norm = None
    master_filename = get_master_csv_filename(level)

    for slug in eligible_slugs:
        metric_root = processed_root / slug
        scopes = _discover_scopes(metric_root, level)

        if state_filter_norm:
            scopes = [s for s in scopes if s in state_filter_norm]

        if not scopes:
            if verbose:
                print(f"[BATCH] {slug}: no matching scopes; skipping")
            continue

        vcfg = variables.get(slug, {}) or {}
        periods_metric_col = str(vcfg.get("periods_metric_col") or "").strip()

        reg = metrics_by_slug.get(slug)
        reg_value_col = ""
        try:
            reg_value_col = str(getattr(reg, "value_col", "") or "").strip()
        except Exception:
            pass

        out_metric_name = periods_metric_col or reg_value_col or "value"
        read_candidates = [c for c in [periods_metric_col, reg_value_col, "value"] if c]

        for scope_name in scopes:
            out_path = metric_root / scope_name / master_filename

            if skip_existing and out_path.exists():
                if verbose:
                    print(f"[BATCH] {slug}/{scope_name}: exists; skipping")
                continue

            if verbose:
                print(f"\n[BATCH] Building {slug}/{scope_name} ({level} level)")

            build_master_metrics(
                str(metric_root),
                None if _is_hydro_level(level) else scope_name,
                metric_col_in_periods=out_metric_name,
                out_path=str(out_path),
                attach_centroid_geojson=district_geojson,
                verbose=verbose,
                metric_col_candidates=read_candidates,
                level=level,
                num_workers=num_workers,
            )


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Build master_metrics CSV(s) from processed outputs. "
            "Hydro levels automatically use processed/{metric}/hydro/."
        )
    )

    p.add_argument(
        "--level",
        "-l",
        choices=["district", "block", "basin", "sub_basin", "both"],
        default="both",
        help="Spatial level (default: both = district + block)",
    )
    p.add_argument("--processed-root", "-p", default=None, help="Processed root directory")
    p.add_argument(
        "--state",
        "-s",
        default=None,
        help="Admin state filter (comma-separated); ignored for hydro levels",
    )
    p.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Batch mode: filter to specific metric slugs (space-separated)",
    )
    p.add_argument(
        "--list-metrics",
        action="store_true",
        help="Batch mode: list eligible metric slugs under processed root and exit",
    )
    p.add_argument("--skip-existing", action="store_true", help="Skip if master CSV already exists")
    p.add_argument("--output-root", "-r", default=None, help="Single-metric mode: processed variable root")
    p.add_argument("--metric", "-m", default=None, help="Single-metric mode: metric column name")
    p.add_argument("--district-geojson", "-g", default=None, help="Optional path to district GeoJSON for centroids")
    p.add_argument("--quiet", action="store_true", help="Reduce output")
    p.add_argument(
        "--workers",
        "-w",
        type=int,
        default=default_workers_75pct(),
        help=(
            "Number of worker processes to use for wide master build "
            "(default: 75% of available CPUs). Set to 1 to disable parallelism."
        ),
    )

    return p.parse_args()


def main() -> None:
    args = parse_args()
    verbose = not bool(args.quiet)

    cli_level: CLILevel = args.level
    levels_to_run: List[AdminLevel] = ["district", "block"] if cli_level == "both" else [cli_level]  # type: ignore[list-item]

    state_filter = [s.strip() for s in str(args.state).split(",") if s.strip()] if args.state else None

    def _print_run_banner(run_idx: int, total: int, lvl: AdminLevel) -> None:
        if not verbose:
            return
        print("#" * 78)
        print(f"RUN {run_idx}/{total}: {lvl.upper()} LEVEL")
        print("#" * 78)

    # Single-metric mode
    if args.output_root:
        if not args.state and any(not _is_hydro_level(level) for level in levels_to_run):
            raise SystemExit("Single-metric mode requires --state for district/block levels")

        metric_col = args.metric or "value"
        total_runs = len(levels_to_run)

        for run_idx, level in enumerate(levels_to_run, start=1):
            _print_run_banner(run_idx, total_runs, level)
            scope_name = _resolve_scope_name(level, args.state, verbose=verbose)
            master_filename = get_master_csv_filename(level)
            default_out = Path(args.output_root) / scope_name / master_filename

            build_master_metrics(
                args.output_root,
                args.state,
                metric_col_in_periods=metric_col,
                out_path=str(default_out),
                attach_centroid_geojson=args.district_geojson,
                verbose=verbose,
                metric_col_candidates=[metric_col, "value"],
                level=level,
                num_workers=int(args.workers),
            )
        return

    # Batch mode
    # Batch mode
    processed_root = Path(args.processed_root) if args.processed_root else _try_import_processed_root()
    if processed_root is None:
        raise SystemExit("Batch mode needs --processed-root or paths.BASE_OUTPUT_ROOT")

    if bool(args.list_metrics):
        variables, metrics_by_slug = _try_import_registries()
        metric_dirs = _discover_metric_dirs(processed_root)
        on_disk_slugs = {p.name for p in metric_dirs}
        eligible_slugs = sorted(on_disk_slugs & set(variables.keys()) & set(metrics_by_slug.keys()))
        print("Eligible metrics:")
        for slug in eligible_slugs:
            print(f"  {slug}")
        print(f"Total: {len(eligible_slugs)}")
        return

    total_runs = len(levels_to_run)
    for run_idx, level in enumerate(levels_to_run, start=1):
        _print_run_banner(run_idx, total_runs, level)
        build_all_master_metrics(
            processed_root,
            level=level,
            metrics_filter=args.metrics,
            state_filter=state_filter,
            district_geojson=args.district_geojson,
            verbose=verbose,
            skip_existing=bool(args.skip_existing),
            num_workers=int(args.workers),
        )


if __name__ == "__main__":
    main()

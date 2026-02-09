# build_master_metrics.py
#!/usr/bin/env python3
"""
build_master_metrics.py

Build master CSVs for processed metric outputs at district or block level.
Includes progress reporting for large datasets.

Supports BOTH folder structures for districts:
- NEW: {state}/districts/{district}/{model}/{scenario}/
- OLD: {state}/{district}/{model}/{scenario}/ (backward compatible)

For blocks, only supports NEW structure:
- {state}/blocks/{district}/{block}/{model}/{scenario}/

Usage:
    python build_master_metrics.py                         # Default: district + block
    python build_master_metrics.py --level district         # District only
    python build_master_metrics.py --level block            # Block only
    python build_master_metrics.py --state Telangana        # Filter to a state (batch mode)

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


AdminLevel = Literal["district", "block"]
CLILevel = Literal["district", "block", "both"]

# Folder names for clean separation
DISTRICT_FOLDER = "districts"
BLOCK_FOLDER = "blocks"


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
    return "master_metrics_by_block.csv" if level == "block" else "master_metrics_by_district.csv"


def get_unit_column_name(level: AdminLevel) -> str:
    """Get the unit column name based on level."""
    return "block" if level == "block" else "district"


def get_level_folder(level: AdminLevel) -> str:
    """Get the subfolder name for a given level."""
    return BLOCK_FOLDER if level == "block" else DISTRICT_FOLDER


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


# -----------------------------------------------------------------------------
# Master building
# -----------------------------------------------------------------------------
def _build_wide_master(
    df_all: pd.DataFrame,
    metric_col_name: str,
    level: AdminLevel,
    num_workers: int = 1,
    verbose: bool = True,
) -> pd.DataFrame:
    """Build wide-format master DataFrame with ensemble statistics.

    This is typically the most CPU-intensive part of the workflow. When
    ``num_workers > 1``, the per-unit row construction is parallelized.
    """
    unit_col = get_unit_column_name(level)

    if level == "block":
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

    tasks = [(unit_ident, mapping, metric_col_name) for unit_ident, mapping in _unit_iter()]

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
    if level == "block":
        return int(df[["block", "district", "state"]].drop_duplicates().shape[0])
    return int(df[["district", "state"]].drop_duplicates().shape[0])


def _build_state_summaries(
    df_all: pd.DataFrame,
    df_yearly: pd.DataFrame,
    metric_col_name: str,
    level: AdminLevel,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build all state-level summary DataFrames."""
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
                    f"{metric_col_name}_mean": float(np.mean(values)),
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
                    **{f"{metric_col_name}__{k}": v for k, v in stats.items()},
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
                        f"{metric_col_name}_mean": float(np.mean(values)),
                        "n_units": _unique_unit_count(grp, level),
                    }
                )
        state_yearly_model_df = pd.DataFrame(yearly_model_rows)

        yearly_ensemble_rows: List[Dict[str, Any]] = []
        for (scenario, year), grp in df_yearly.groupby(["scenario", "year"]):
            model_means = grp.groupby("model")["value"].mean().dropna().tolist()
            stats = compute_ensemble_stats(model_means)
            if stats:
                yearly_ensemble_rows.append(
                    {
                        "scenario": scenario,
                        "year": year,
                        **{f"{metric_col_name}__{k}": v for k, v in stats.items()},
                        "n_models": len(model_means),
                        "n_units": _unique_unit_count(grp, level),
                    }
                )
        state_yearly_ensemble_df = pd.DataFrame(yearly_ensemble_rows)

    return state_model_df, state_ensemble_df, state_yearly_model_df, state_yearly_ensemble_df


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------
def build_master_metrics(
    output_root: str,
    state: str,
    metric_col_in_periods: str = "days_gt_32C",
    out_path: str | None = None,
    attach_centroid_geojson: str | None = None,
    verbose: bool = True,
    metric_col_candidates: Sequence[str] | None = None,
    level: AdminLevel = "district",
    num_workers: int = 1,
) -> pd.DataFrame:
    """Build master CSV for a single metric/state combination."""
    root = Path(output_root)
    state_root = root / state

    if not state_root.exists():
        if verbose:
            print(f"ERROR: State root not found: {state_root}", file=sys.stderr)
        return pd.DataFrame()

    if metric_col_candidates is None:
        metric_col_candidates = [metric_col_in_periods, "value"]

    start_time = time.time()

    if verbose:
        print(f"\n{'='*60}")
        print("Building master CSV")
        print(f"{'='*60}")
        print(f"Level: {level}")
        print(f"State: {state}")
        print(f"Metric column: {metric_col_in_periods}")
        print(f"State root: {state_root}")
        print()

    # Collect data
    if verbose:
        print("[Step 1/3] Collecting data from CSV files...")

    if level == "block":
        all_rows, yearly_rows = _collect_block_data(state_root, state, metric_col_candidates, verbose)
    else:
        all_rows, yearly_rows = _collect_district_data(state_root, state, metric_col_candidates, verbose)

    if not all_rows:
        if verbose:
            print(f"ERROR: No data found for {state} at {level} level", file=sys.stderr)
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
        state_model_df.to_csv(outp.parent / "state_model_averages.csv", index=False)
        state_ensemble_df.to_csv(outp.parent / "state_ensemble_stats.csv", index=False)
        state_yearly_model_df.to_csv(outp.parent / "state_yearly_model_averages.csv", index=False)
        state_yearly_ensemble_df.to_csv(outp.parent / "state_yearly_ensemble_stats.csv", index=False)

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

    if level == "block":
        # Only new structure for blocks
        level_path = state_dir / BLOCK_FOLDER
        if not level_path.exists():
            return False
        patterns = ("*/*/*/*/*_periods.csv", "*/*/*/*/*_yearly.csv")
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


def _discover_states(metric_root: Path, level: AdminLevel = "district") -> List[str]:
    """Return state directories under a metric root."""
    states: List[str] = []
    for p in metric_root.iterdir():
        if p.is_dir() and _looks_like_state_dir(p, level):
            states.append(p.name)
    return sorted(states)


# -----------------------------------------------------------------------------
# Batch mode
# -----------------------------------------------------------------------------
def build_all_master_metrics(
    processed_root: Path,
    *,
    level: AdminLevel = "district",
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

    if verbose:
        print(f"[BATCH] level = {level}")
        print(f"[BATCH] processed_root = {processed_root}")
        print(f"[BATCH] eligible metrics = {len(eligible_slugs)}")

    if not eligible_slugs:
        print("[BATCH] No eligible metrics found.", file=sys.stderr)
        return

    state_filter_norm = {str(s).strip() for s in state_filter if str(s).strip()} if state_filter else None
    master_filename = get_master_csv_filename(level)

    for slug in eligible_slugs:
        metric_root = processed_root / slug
        states = _discover_states(metric_root, level)

        if state_filter_norm:
            states = [s for s in states if s in state_filter_norm]

        if not states:
            if verbose:
                print(f"[BATCH] {slug}: no matching states; skipping")
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

        for state in states:
            out_path = metric_root / state / master_filename

            if skip_existing and out_path.exists():
                if verbose:
                    print(f"[BATCH] {slug}/{state}: exists; skipping")
                continue

            if verbose:
                print(f"\n[BATCH] Building {slug}/{state} ({level} level)")

            build_master_metrics(
                str(metric_root),
                state,
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
    p = argparse.ArgumentParser(description="Build master_metrics CSV(s) from processed outputs.")

    p.add_argument(
        "--level",
        "-l",
        choices=["district", "block", "both"],
        default="both",
        help="Administrative level (default: both)",
    )
    p.add_argument("--processed-root", "-p", default=None, help="Processed root directory")
    p.add_argument("--state", "-s", default=None, help="State filter (comma-separated)")
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
        if not args.state:
            raise SystemExit("Single-metric mode requires --state")

        metric_col = args.metric or "value"
        total_runs = len(levels_to_run)

        for run_idx, level in enumerate(levels_to_run, start=1):
            _print_run_banner(run_idx, total_runs, level)
            master_filename = get_master_csv_filename(level)
            default_out = Path(args.output_root) / str(args.state) / master_filename

            build_master_metrics(
                args.output_root,
                str(args.state),
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
    processed_root = Path(args.processed_root) if args.processed_root else _try_import_processed_root()
    if processed_root is None:
        raise SystemExit("Batch mode needs --processed-root or paths.BASE_OUTPUT_ROOT")

    total_runs = len(levels_to_run)
    for run_idx, level in enumerate(levels_to_run, start=1):
        _print_run_banner(run_idx, total_runs, level)
        build_all_master_metrics(
            processed_root,
            level=level,
            state_filter=state_filter,
            district_geojson=args.district_geojson,
            verbose=verbose,
            skip_existing=bool(args.skip_existing),
            num_workers=int(args.workers),
        )


if __name__ == "__main__":
    main()
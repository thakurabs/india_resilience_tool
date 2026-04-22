"""
Build the compact `processed_optimised` runtime bundle from existing legacy data.

This tool is intentionally non-destructive: it reads from the current
`IRT_DATA_DIR/processed` tree and writes a new optimized bundle under
`IRT_DATA_DIR/processed_optimised`.
"""

from __future__ import annotations

import argparse
import os
import json
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional, TypeVar

import geopandas as gpd
import pandas as pd
from tqdm.auto import tqdm

from india_resilience_tool.config.constants import (
    SIMPLIFY_TOL_ADM2,
    SIMPLIFY_TOL_ADM3,
    SIMPLIFY_TOL_BASIN_RENDER,
    SIMPLIFY_TOL_SUBBASIN_RENDER,
)
from india_resilience_tool.config.paths import get_paths_config, resolve_processed_root
from india_resilience_tool.config.variables import VARIABLES
from india_resilience_tool.data.adm2_loader import ensure_adm2_columns
from india_resilience_tool.data.adm3_loader import ensure_adm3_columns
from india_resilience_tool.data.discovery import (
    iter_block_yearly_ensemble_files,
    iter_district_yearly_ensemble_files,
    iter_hydro_yearly_ensemble_files,
    iter_hydro_yearly_model_files,
)
from india_resilience_tool.data.hydro_loader import ensure_hydro_columns
from india_resilience_tool.data.optimized_bundle import (
    OPTIMIZED_DIRNAME,
    bundle_manifest_path,
    optimized_context_path,
    optimized_geometry_path,
    optimized_master_path,
    optimized_yearly_ensemble_path,
    optimized_yearly_models_path,
    resolve_optimized_bundle_root,
)
from india_resilience_tool.utils.naming import alias
from india_resilience_tool.utils.processed_io import read_table, remove_tree, unlink_file


LEGACY_MASTER_FILENAMES = {
    "district": "master_metrics_by_district.csv",
    "block": "master_metrics_by_block.csv",
    "basin": "master_metrics_by_basin.csv",
    "sub_basin": "master_metrics_by_sub_basin.csv",
}

ADMIN_ID_COLS = {
    "district": ["district", "state"],
    "block": ["block", "district", "state"],
}

HYDRO_ID_COLS = {
    "basin": ["basin_id", "basin_name"],
    "sub_basin": ["subbasin_id", "basin_id", "subbasin_name", "basin_name"],
}

CONTEXT_FILENAMES = {
    "district_subbasin.parquet": "district_subbasin_crosswalk.csv",
    "block_subbasin.parquet": "block_subbasin_crosswalk.csv",
    "district_basin.parquet": "district_basin_crosswalk.csv",
    "block_basin.parquet": "block_basin_crosswalk.csv",
    "river_reaches.parquet": "river_reaches.parquet",
    "river_network_display.geojson": "river_network_display.geojson",
    "river_basin_name_reconciliation.parquet": "river_basin_name_reconciliation.csv",
    "river_subbasin_diagnostics.parquet": "river_subbasin_diagnostics.csv",
}

LEVEL_SELECTIONS = {
    "all": ("district", "block", "basin", "sub_basin"),
    "admin": ("district", "block"),
    "hydro": ("basin", "sub_basin"),
    "district": ("district",),
    "block": ("block",),
    "basin": ("basin",),
    "sub_basin": ("sub_basin",),
}

YEARLY_PARALLEL_CHUNK_SIZE = 64
MANIFEST_ARTIFACT_VERSION = 2
PARITY_REPORT_FILENAME = "parity_report.json"
T = TypeVar("T")


@dataclass(frozen=True)
class BuildTask:
    stage: str
    label: str
    slug: Optional[str] = None
    state: Optional[str] = None
    level: Optional[str] = None
    source_path: Optional[Path] = None
    target_path: Optional[Path] = None


@dataclass(frozen=True)
class MetricBundleSummary:
    slug: str
    source_type: str
    wrote_masters: bool
    wrote_yearly_ensemble: bool
    wrote_yearly_models: bool


@dataclass(frozen=True)
class YearlyModelsJob:
    slug: str
    state: str
    level: str
    csv_paths: tuple[Path, ...]
    models_path: Path


@dataclass(frozen=True)
class YearlyEnsembleSource:
    scenario: str
    csv_path: Path
    name_1: str
    name_2: Optional[str] = None
    model: Optional[str] = None


@dataclass(frozen=True)
class YearlyEnsembleJob:
    slug: str
    level: str
    target_path: Path
    state: Optional[str] = None
    source_mode: str = "legacy_ensemble"
    sources: tuple[YearlyEnsembleSource, ...] = ()


@dataclass(frozen=True)
class BuildPlan:
    summaries_seed: tuple[MetricBundleSummary, ...]
    master_tasks: tuple[BuildTask, ...]
    yearly_model_jobs: tuple[YearlyModelsJob, ...]
    yearly_ensemble_jobs: tuple[YearlyEnsembleJob, ...]
    context_tasks: tuple[BuildTask, ...]
    geometry_tasks: tuple[BuildTask, ...]
    manifest_task: BuildTask

    def stage_totals(self) -> dict[str, int]:
        return {
            "masters": len(self.master_tasks),
            "yearly-models": sum(len(job.csv_paths) + 1 for job in self.yearly_model_jobs),
            "yearly-ensemble": sum(len(job.sources) + 1 for job in self.yearly_ensemble_jobs),
            "context": len(self.context_tasks),
            "geometry": len(self.geometry_tasks),
            "manifest": 1,
        }

    @property
    def total_tasks(self) -> int:
        return sum(self.stage_totals().values())


class BuildProgress:
    """Track exact build progress with one overall and one stage bar."""

    def __init__(self, plan: BuildPlan, *, enabled: bool) -> None:
        self._plan = plan
        self._enabled = bool(enabled)
        self._stage_totals = plan.stage_totals()
        self._stage_completed = {stage: 0 for stage in self._stage_totals}
        self._completed_total = 0
        self._current_task: Optional[BuildTask] = None
        self._overall_bar: Optional[tqdm] = None
        self._stage_bar: Optional[tqdm] = None
        self._stage_name: Optional[str] = None

        if self._enabled:
            self._overall_bar = tqdm(
                total=plan.total_tasks,
                desc="processed_optimised",
                unit="task",
                position=0,
                leave=True,
                dynamic_ncols=True,
            )

    def print_plan_summary(self) -> None:
        totals = self._plan.stage_totals()
        joined = ", ".join(f"{stage}={count}" for stage, count in totals.items())
        print(f"PLANNED TASKS total={self._plan.total_tasks} ({joined})")

    def _ensure_stage(self, *, stage: str, label: str) -> None:
        if not self._enabled:
            return
        if stage != self._stage_name:
            if self._stage_bar is not None:
                self._stage_bar.close()
            self._stage_name = stage
            self._stage_bar = tqdm(
                total=self._stage_totals[stage],
                desc=stage,
                unit="task",
                position=1,
                leave=False,
                dynamic_ncols=True,
            )
            completed = self._stage_completed[stage]
            if completed:
                self._stage_bar.update(completed)
        if self._overall_bar is not None:
            self._overall_bar.set_postfix_str(label)
        if self._stage_bar is not None:
            self._stage_bar.set_postfix_str(label)

    def start_task(self, task: BuildTask) -> None:
        self._current_task = task
        self._ensure_stage(stage=task.stage, label=task.label)

    def finish_task(self, task: BuildTask, *, count: int = 1) -> None:
        self._stage_completed[task.stage] += count
        self._completed_total += count
        self._current_task = None
        if not self._enabled:
            return
        if self._overall_bar is not None:
            self._overall_bar.update(count)
        if self._stage_bar is not None:
            self._stage_bar.update(count)

    def advance_stage(self, *, stage: str, label: str, count: int) -> None:
        if count <= 0:
            return
        self._current_task = BuildTask(stage=stage, label=label)
        self._ensure_stage(stage=stage, label=label)
        self._stage_completed[stage] += count
        self._completed_total += count
        if not self._enabled:
            return
        if self._overall_bar is not None:
            self._overall_bar.update(count)
        if self._stage_bar is not None:
            self._stage_bar.update(count)

    def failure_summary(self) -> str:
        remaining = self._plan.total_tasks - self._completed_total
        if self._current_task is None:
            stage = self._stage_name or "unknown"
            current = "unknown"
        else:
            stage = self._current_task.stage
            current = self._current_task.label
        return (
            "PROCESSED OPTIMISED FAILED "
            f"(stage={stage}, completed_tasks={self._completed_total}, "
            f"remaining_tasks={remaining}, current={current})"
        )

    def close(self) -> None:
        if self._stage_bar is not None:
            self._stage_bar.close()
            self._stage_bar = None
        if self._overall_bar is not None:
            self._overall_bar.close()
            self._overall_bar = None


def _run_task(task: BuildTask, progress: BuildProgress, action) -> None:
    progress.start_task(task)
    try:
        action()
    except Exception:
        raise
    progress.finish_task(task)


def default_build_workers_80pct() -> int:
    """Return the default worker count as 80% of available logical CPUs."""
    try:
        cpu_count = os.cpu_count() or 1
    except Exception:
        cpu_count = 1
    return max(1, int(cpu_count * 0.8))


def resolve_build_workers(workers: Optional[int]) -> int:
    """Resolve the effective worker count for the optimized builder."""
    if workers is None:
        return default_build_workers_80pct()
    resolved = int(workers)
    if resolved < 1:
        raise ValueError(f"workers must be >= 1, got {workers!r}")
    return resolved


def _chunk_tuple(items: tuple[T, ...], *, chunk_size: int) -> tuple[tuple[T, ...], ...]:
    """Split a tuple into stable, contiguous chunks."""
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be >= 1, got {chunk_size!r}")
    if not items:
        return tuple()
    return tuple(items[i : i + chunk_size] for i in range(0, len(items), chunk_size))


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, compression="zstd")


def _safe_numeric_downcast(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_float_dtype(out[col]):
            out[col] = pd.to_numeric(out[col], downcast="float")
        elif pd.api.types.is_integer_dtype(out[col]):
            out[col] = pd.to_numeric(out[col], downcast="integer")
    return out


def _metric_value_cols(df: pd.DataFrame, *, supported_stats: Iterable[str]) -> list[str]:
    keep_stats = {str(v).strip().lower() for v in supported_stats if str(v).strip()}
    out: list[str] = []
    for col in df.columns:
        parts = str(col).split("__")
        if len(parts) != 4:
            continue
        stat = parts[-1].strip().lower()
        if stat in keep_stats:
            out.append(str(col))
    return out


def _admin_keys(df: pd.DataFrame, *, level: str) -> pd.DataFrame:
    out = df.copy()
    if level == "district":
        out["district_key"] = (
            out["state"].astype(str).map(alias).str.cat(out["district"].astype(str).map(alias), sep="|")
        )
        return out
    out["block_key"] = (
        out["state"].astype(str)
        .map(alias)
        .str.cat(out["district"].astype(str).map(alias), sep="|")
        .str.cat(out["block"].astype(str).map(alias), sep="|")
    )
    return out


def _select_master_columns(
    df: pd.DataFrame,
    *,
    level: str,
    supported_stats: Iterable[str],
) -> pd.DataFrame:
    id_cols = list(ADMIN_ID_COLS[level]) if level in ADMIN_ID_COLS else list(HYDRO_ID_COLS[level])
    keep_cols = [c for c in id_cols if c in df.columns]
    keep_cols.extend(_metric_value_cols(df, supported_stats=supported_stats))
    out = df[keep_cols].copy()
    if level in {"district", "block"}:
        out = _admin_keys(out, level=level)
    return _safe_numeric_downcast(out)


def _read_legacy_master(path: Path) -> pd.DataFrame:
    parquet_path = path.with_suffix(".parquet")
    if parquet_path.exists():
        return read_table(parquet_path)
    return read_table(path)


def _legacy_master_source(path: Path) -> Optional[Path]:
    parquet_path = path.with_suffix(".parquet")
    if parquet_path.exists():
        return parquet_path
    if path.exists():
        return path
    return None


def _iter_state_dirs(metric_root: Path) -> list[Path]:
    out: list[Path] = []
    for child in sorted(metric_root.iterdir()):
        if not child.is_dir():
            continue
        if child.name.lower() == "hydro":
            continue
        out.append(child)
    return out


def _iter_yearly_csv_paths(state_root: Path, *, level: str) -> tuple[Path, ...]:
    if level == "district":
        pattern = "districts/*/*/*/*_yearly.csv"
    else:
        pattern = "blocks/*/*/*/*/*_yearly.csv"
    return tuple(sorted(state_root.glob(pattern)))


def _read_yearly_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _normalize_legacy_ensemble_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize legacy yearly-ensemble CSV columns to the optimized schema."""
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    rename_map = {
        "ensemble_mean": "mean",
        "ensemble_value": "mean",
        "value": "mean",
        "ensemble_median": "median",
        "ensemble_std": "std",
        "ensemble_p05": "p05",
        "ensemble_p95": "p95",
    }
    out = out.rename(columns={k: v for k, v in rename_map.items() if k in out.columns})
    if "mean" not in out.columns and "median" in out.columns:
        out["mean"] = out["median"]
    if "year" in out.columns:
        out["year"] = pd.to_numeric(out["year"], errors="coerce")
    if "mean" in out.columns:
        out["mean"] = pd.to_numeric(out["mean"], errors="coerce")
    if "median" in out.columns:
        out["median"] = pd.to_numeric(out["median"], errors="coerce")
    keep_cols = [c for c in ("year", "mean", "median") if c in out.columns]
    if "year" not in keep_cols or "mean" not in keep_cols:
        return pd.DataFrame()
    out = out[keep_cols].dropna(subset=["year", "mean"]).sort_values("year").reset_index(drop=True)
    return out


def _extract_value_column(df: pd.DataFrame) -> Optional[str]:
    if "value" in df.columns:
        return "value"
    ignore = {"year", "district", "block", "scenario", "model", "source_file", "state"}
    for col in df.columns:
        if str(col) in ignore:
            continue
        if pd.to_numeric(df[col], errors="coerce").notna().any():
            return str(col)
    return None


def _load_one_legacy_admin_yearly_model(
    csv_path: Path,
    *,
    state_name: str,
    level: str,
) -> pd.DataFrame:
    """Load one admin yearly model CSV into the optimized yearly-model schema."""
    df = _read_yearly_csv(csv_path)
    if df.empty:
        return pd.DataFrame()

    value_col = _extract_value_column(df)
    if value_col is None or "year" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["value"] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=["year", "value"])
    if df.empty:
        return pd.DataFrame()

    if level == "district":
        district_name = csv_path.parts[-4]
        df["district_key"] = _normalized_key(state_name, district_name)
        key_col = "district_key"
    else:
        district_name = csv_path.parts[-5]
        block_name = csv_path.parts[-4]
        df["block_key"] = _normalized_key(state_name, district_name, block_name)
        key_col = "block_key"

    if "scenario" not in df.columns:
        df["scenario"] = csv_path.parts[-2]
    if "model" not in df.columns:
        df["model"] = csv_path.parts[-3]

    keep_cols = ["year", "value", "scenario", "model", key_col]
    return df[keep_cols]


def _load_legacy_admin_yearly_models_chunk(
    chunk_index: int,
    path_strings: tuple[str, ...],
    *,
    state_name: str,
    level: str,
) -> tuple[int, pd.DataFrame]:
    """Load one chunk of admin yearly-model CSVs in stable source order."""
    rows: list[pd.DataFrame] = []
    for path_string in path_strings:
        row_df = _load_one_legacy_admin_yearly_model(Path(path_string), state_name=state_name, level=level)
        if not row_df.empty:
            rows.append(row_df)
    if not rows:
        return chunk_index, pd.DataFrame()
    return chunk_index, pd.concat(rows, ignore_index=True, sort=False)


def _execute_parallel_chunks(
    *,
    progress: BuildProgress,
    stage: str,
    label_prefix: str,
    chunks: tuple[tuple[str, ...], ...],
    worker_count: int,
    worker_fn,
    worker_kwargs: dict[str, object],
) -> list[tuple[int, pd.DataFrame]]:
    """Execute chunked worker tasks and preserve deterministic chunk ordering."""
    if not chunks:
        return []

    max_workers = max(1, min(int(worker_count), len(chunks)))
    results: list[tuple[int, pd.DataFrame]] = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(worker_fn, chunk_index, chunk, **worker_kwargs): (chunk_index, len(chunk))
            for chunk_index, chunk in enumerate(chunks)
        }
        for future in as_completed(future_map):
            chunk_index, chunk_size = future_map[future]
            chunk_result = future.result()
            progress.advance_stage(
                stage=stage,
                label=f"{label_prefix} | chunk {chunk_index + 1}/{len(chunks)}",
                count=chunk_size,
            )
            results.append(chunk_result)
    results.sort(key=lambda item: item[0])
    return results


def _load_legacy_admin_yearly_models(
    *,
    slug: str,
    state_name: str,
    level: str,
    csv_paths: tuple[Path, ...],
    progress: BuildProgress,
    workers: int = 1,
) -> pd.DataFrame:
    if workers <= 1 or len(csv_paths) <= 1:
        rows: list[pd.DataFrame] = []
        for csv_path in csv_paths:
            task = BuildTask(
                stage="yearly-models",
                label=f"{slug} | {state_name} | {level} | {csv_path.name}",
                slug=slug,
                state=state_name,
                level=level,
                source_path=csv_path,
            )
            progress.start_task(task)
            try:
                row_df = _load_one_legacy_admin_yearly_model(csv_path, state_name=state_name, level=level)
                if not row_df.empty:
                    rows.append(row_df)
            except Exception:
                raise
            progress.finish_task(task)
        if not rows:
            return pd.DataFrame()
        out = pd.concat(rows, ignore_index=True, sort=False)
        out["scenario"] = out["scenario"].astype(str).str.strip().str.lower()
        out["model"] = out["model"].astype(str).str.strip()
        return _safe_numeric_downcast(out)

    parallel_task = BuildTask(
        stage="yearly-models",
        label=f"{slug} | {state_name} | {level} | parallel yearly-model reads",
        slug=slug,
        state=state_name,
        level=level,
    )
    progress.start_task(parallel_task)
    path_chunks = _chunk_tuple(tuple(str(path) for path in csv_paths), chunk_size=YEARLY_PARALLEL_CHUNK_SIZE)
    chunk_results = _execute_parallel_chunks(
        progress=progress,
        stage="yearly-models",
        label_prefix=f"{slug} | {state_name} | {level}",
        chunks=path_chunks,
        worker_count=workers,
        worker_fn=_load_legacy_admin_yearly_models_chunk,
        worker_kwargs={"state_name": state_name, "level": level},
    )
    rows = [chunk_df for _, chunk_df in chunk_results if not chunk_df.empty]
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True, sort=False)
    out["scenario"] = out["scenario"].astype(str).str.strip().str.lower()
    out["model"] = out["model"].astype(str).str.strip()
    return _safe_numeric_downcast(out)


def _build_yearly_ensemble_from_models(model_df: pd.DataFrame, *, level: str) -> pd.DataFrame:
    if model_df.empty:
        return pd.DataFrame()
    key_col = "district_key" if level == "district" else "block_key"
    grouped = (
        model_df.groupby([key_col, "scenario", "year"], as_index=False)["value"]
        .agg(mean="mean", median="median")
        .reset_index(drop=True)
    )
    return _safe_numeric_downcast(grouped)


def _load_legacy_hydro_yearly_models(
    *,
    level: str,
    sources: tuple[YearlyEnsembleSource, ...],
    basin_map: dict[str, tuple[str, str]],
    subbasin_map: dict[str, tuple[str, str, str, str]],
    progress: BuildProgress,
    label_prefix: str = "hydro",
    workers: int = 1,
) -> pd.DataFrame:
    """Load hydro model-yearly rows that can be aggregated into optimized ensembles."""
    if level not in {"basin", "sub_basin"}:
        return pd.DataFrame()

    if workers <= 1 or len(sources) <= 1:
        rows: list[pd.DataFrame] = []
        for source in sources:
            row_df = _load_one_legacy_hydro_yearly_model_source(
                source=source,
                level=level,
                basin_map=basin_map,
                subbasin_map=subbasin_map,
            )
            if not row_df.empty:
                rows.append(row_df)
        if not rows:
            return pd.DataFrame()
        return _safe_numeric_downcast(pd.concat(rows, ignore_index=True, sort=False))

    progress.start_task(BuildTask(stage="yearly-ensemble", label=f"{label_prefix} | parallel hydro model reads"))
    source_chunks = _chunk_tuple(sources, chunk_size=YEARLY_PARALLEL_CHUNK_SIZE)
    chunk_results = _execute_parallel_chunks(
        progress=progress,
        stage="yearly-ensemble",
        label_prefix=label_prefix,
        chunks=source_chunks,
        worker_count=workers,
        worker_fn=_load_legacy_hydro_yearly_models_chunk,
        worker_kwargs={
            "level": level,
            "basin_map": basin_map,
            "subbasin_map": subbasin_map,
        },
    )
    rows = [chunk_df for _, chunk_df in chunk_results if not chunk_df.empty]
    if not rows:
        return pd.DataFrame()
    return _safe_numeric_downcast(pd.concat(rows, ignore_index=True, sort=False))


def _build_hydro_yearly_ensemble_from_models(model_df: pd.DataFrame, *, level: str) -> pd.DataFrame:
    """Aggregate hydro model-yearly rows into optimized hydro yearly ensembles."""
    if model_df.empty:
        return pd.DataFrame()

    if level == "basin":
        grouped = (
            model_df.groupby(["basin_id", "basin_name", "scenario", "year"], as_index=False)["value"]
            .agg(mean="mean", median="median")
            .sort_values(["basin_name", "scenario", "year"], kind="stable")
            .reset_index(drop=True)
        )
        return _safe_numeric_downcast(grouped)

    grouped = (
        model_df.groupby(
            ["basin_id", "basin_name", "subbasin_id", "subbasin_name", "scenario", "year"],
            as_index=False,
        )["value"]
        .agg(mean="mean", median="median")
        .sort_values(["basin_name", "subbasin_name", "scenario", "year"], kind="stable")
        .reset_index(drop=True)
    )
    return _safe_numeric_downcast(grouped)


def _normalized_key(*parts: str) -> str:
    return "|".join(alias(part) for part in parts)


@lru_cache(maxsize=None)
def _hydro_name_maps_cached(
    data_dir_str: str,
    slug: str,
) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str, str, str]]]:
    """Build normalized hydro name -> id lookup maps from the legacy hydro masters."""
    data_dir = Path(data_dir_str)
    legacy_root = resolve_processed_root(slug, data_dir=data_dir, mode="portfolio")
    hydro_root = legacy_root / "hydro"
    basin_map: dict[str, tuple[str, str]] = {}
    subbasin_map: dict[str, tuple[str, str, str, str]] = {}

    basin_source = _legacy_master_source(hydro_root / LEGACY_MASTER_FILENAMES["basin"])
    if basin_source is not None:
        basin_df = _read_legacy_master(hydro_root / LEGACY_MASTER_FILENAMES["basin"])
        if not basin_df.empty and {"basin_id", "basin_name"}.issubset(set(basin_df.columns)):
            for _, row in basin_df[["basin_id", "basin_name"]].dropna().drop_duplicates().iterrows():
                basin_name = str(row["basin_name"]).strip()
                basin_map[alias(basin_name)] = (str(row["basin_id"]).strip(), basin_name)

    sub_source = _legacy_master_source(hydro_root / LEGACY_MASTER_FILENAMES["sub_basin"])
    if sub_source is not None:
        sub_df = _read_legacy_master(hydro_root / LEGACY_MASTER_FILENAMES["sub_basin"])
        expected = {"basin_id", "basin_name", "subbasin_id", "subbasin_name"}
        if not sub_df.empty and expected.issubset(set(sub_df.columns)):
            for _, row in sub_df[list(expected)].dropna().drop_duplicates().iterrows():
                basin_name = str(row["basin_name"]).strip()
                sub_name = str(row["subbasin_name"]).strip()
                key = _normalized_key(basin_name, sub_name)
                subbasin_map[key] = (
                    str(row["basin_id"]).strip(),
                    basin_name,
                    str(row["subbasin_id"]).strip(),
                    sub_name,
                )
                basin_map.setdefault(alias(basin_name), (str(row["basin_id"]).strip(), basin_name))

    return basin_map, subbasin_map


def _hydro_name_maps(
    *,
    data_dir: Path,
    slug: str,
    level: str,
) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, str, str, str]]]:
    _ = level
    basin_map, subbasin_map = _hydro_name_maps_cached(str(Path(data_dir).resolve()), slug)
    return dict(basin_map), dict(subbasin_map)


def _load_one_legacy_yearly_ensemble_source(
    *,
    source: YearlyEnsembleSource,
    level: str,
    state_name: Optional[str],
    basin_map: Optional[dict[str, tuple[str, str]]] = None,
    subbasin_map: Optional[dict[str, tuple[str, str, str, str]]] = None,
) -> pd.DataFrame:
    """Load one optimized yearly-ensemble frame from a legacy yearly source."""
    df = _normalize_legacy_ensemble_df(_read_yearly_csv(source.csv_path))
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["scenario"] = str(source.scenario).strip().lower()
    if level == "district":
        df["district_key"] = _normalized_key(str(state_name or ""), source.name_1)
    elif level == "block":
        df["block_key"] = _normalized_key(str(state_name or ""), source.name_1, str(source.name_2 or ""))
    elif level == "basin":
        basin_lookup = basin_map or {}
        basin_name = str(source.name_1).strip()
        basin_id, basin_name_out = basin_lookup.get(alias(basin_name), ("", basin_name))
        df["basin_id"] = basin_id
        df["basin_name"] = basin_name_out
    else:
        sub_lookup = subbasin_map or {}
        basin_name = str(source.name_1).strip()
        sub_name = str(source.name_2 or "").strip()
        basin_id, basin_name_out, sub_id, sub_name_out = sub_lookup.get(
            _normalized_key(basin_name, sub_name),
            ("", basin_name, "", sub_name),
        )
        df["basin_id"] = basin_id
        df["basin_name"] = basin_name_out
        df["subbasin_id"] = sub_id
        df["subbasin_name"] = sub_name_out
    return df


def _load_legacy_yearly_ensemble_chunk(
    chunk_index: int,
    sources: tuple[YearlyEnsembleSource, ...],
    *,
    level: str,
    state_name: Optional[str],
    basin_map: Optional[dict[str, tuple[str, str]]] = None,
    subbasin_map: Optional[dict[str, tuple[str, str, str, str]]] = None,
) -> tuple[int, pd.DataFrame]:
    """Load one chunk of legacy yearly-ensemble sources in stable order."""
    rows: list[pd.DataFrame] = []
    for source in sources:
        row_df = _load_one_legacy_yearly_ensemble_source(
            source=source,
            level=level,
            state_name=state_name,
            basin_map=basin_map,
            subbasin_map=subbasin_map,
        )
        if not row_df.empty:
            rows.append(row_df)
    if not rows:
        return chunk_index, pd.DataFrame()
    return chunk_index, pd.concat(rows, ignore_index=True, sort=False)


def _load_one_legacy_hydro_yearly_model_source(
    *,
    source: YearlyEnsembleSource,
    level: str,
    basin_map: dict[str, tuple[str, str]],
    subbasin_map: dict[str, tuple[str, str, str, str]],
) -> pd.DataFrame:
    """Load one hydro yearly model CSV into the optimized model-fallback schema."""
    df = _read_yearly_csv(source.csv_path)
    if df.empty:
        return pd.DataFrame()
    value_col = _extract_value_column(df)
    if value_col is None or "year" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["value"] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=["year", "value"])
    if df.empty:
        return pd.DataFrame()

    df["scenario"] = str(source.scenario).strip().lower()
    df["model"] = str(source.model or "").strip()

    if level == "basin":
        basin_name = str(source.name_1).strip()
        basin_id, basin_name_out = basin_map.get(alias(basin_name), ("", basin_name))
        df["basin_id"] = basin_id
        df["basin_name"] = basin_name_out
        return df[["basin_id", "basin_name", "scenario", "model", "year", "value"]]

    basin_name = str(source.name_1).strip()
    sub_name = str(source.name_2 or "").strip()
    basin_id, basin_name_out, sub_id, sub_name_out = subbasin_map.get(
        _normalized_key(basin_name, sub_name),
        ("", basin_name, "", sub_name),
    )
    df["basin_id"] = basin_id
    df["basin_name"] = basin_name_out
    df["subbasin_id"] = sub_id
    df["subbasin_name"] = sub_name_out
    return df[["basin_id", "basin_name", "subbasin_id", "subbasin_name", "scenario", "model", "year", "value"]]


def _load_legacy_hydro_yearly_models_chunk(
    chunk_index: int,
    sources: tuple[YearlyEnsembleSource, ...],
    *,
    level: str,
    basin_map: dict[str, tuple[str, str]],
    subbasin_map: dict[str, tuple[str, str, str, str]],
) -> tuple[int, pd.DataFrame]:
    """Load one chunk of hydro yearly model sources in stable order."""
    rows: list[pd.DataFrame] = []
    for source in sources:
        row_df = _load_one_legacy_hydro_yearly_model_source(
            source=source,
            level=level,
            basin_map=basin_map,
            subbasin_map=subbasin_map,
        )
        if not row_df.empty:
            rows.append(row_df)
    if not rows:
        return chunk_index, pd.DataFrame()
    return chunk_index, pd.concat(rows, ignore_index=True, sort=False)


def _load_legacy_yearly_ensemble(
    *,
    level: str,
    state_name: Optional[str],
    sources: tuple[YearlyEnsembleSource, ...],
    basin_map: Optional[dict[str, tuple[str, str]]] = None,
    subbasin_map: Optional[dict[str, tuple[str, str, str, str]]] = None,
    progress: BuildProgress,
    label_prefix: str = "ensemble",
    workers: int = 1,
) -> pd.DataFrame:
    """Load optimized yearly-ensemble rows directly from legacy ensemble CSVs."""
    basin_lookup = basin_map or {}
    subbasin_lookup = subbasin_map or {}
    if workers <= 1 or len(sources) <= 1:
        rows: list[pd.DataFrame] = []
        for source in sources:
            row_df = _load_one_legacy_yearly_ensemble_source(
                source=source,
                level=level,
                state_name=state_name,
                basin_map=basin_lookup,
                subbasin_map=subbasin_lookup,
            )
            if not row_df.empty:
                rows.append(row_df)
        if not rows:
            return pd.DataFrame()
        return _safe_numeric_downcast(pd.concat(rows, ignore_index=True, sort=False))

    progress.start_task(BuildTask(stage="yearly-ensemble", label=f"{label_prefix} | parallel ensemble reads"))
    source_chunks = _chunk_tuple(sources, chunk_size=YEARLY_PARALLEL_CHUNK_SIZE)
    chunk_results = _execute_parallel_chunks(
        progress=progress,
        stage="yearly-ensemble",
        label_prefix=label_prefix,
        chunks=source_chunks,
        worker_count=workers,
        worker_fn=_load_legacy_yearly_ensemble_chunk,
        worker_kwargs={
            "level": level,
            "state_name": state_name,
            "basin_map": basin_lookup,
            "subbasin_map": subbasin_lookup,
        },
    )
    rows = [chunk_df for _, chunk_df in chunk_results if not chunk_df.empty]
    if not rows:
        return pd.DataFrame()
    return _safe_numeric_downcast(pd.concat(rows, ignore_index=True, sort=False))


def _simplify_geometry(
    gdf: gpd.GeoDataFrame,
    *,
    keep_cols: list[str],
    tolerance: float,
) -> gpd.GeoDataFrame:
    out = gdf[keep_cols + ["geometry"]].copy()
    if "area_m2" not in out.columns:
        from pyproj import Geod

        geod = Geod(ellps="WGS84")
        areas: list[float] = []
        for geom in out.geometry:
            if geom is None or geom.is_empty:
                areas.append(0.0)
                continue
            try:
                areas.append(abs(float(geod.geometry_area_perimeter(geom)[0])))
            except Exception:
                areas.append(0.0)
        out["area_m2"] = areas
    out = out.to_crs(4326)
    out["geometry"] = out["geometry"].simplify(tolerance=float(tolerance), preserve_topology=True)
    return out


def _write_geojson(gdf: gpd.GeoDataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(gdf.to_json(), encoding="utf-8")


def _context_map(*, data_dir: Path) -> dict[Path, Path]:
    return {
        data_dir / src_name: optimized_context_path(dst_name, data_dir=data_dir)
        for dst_name, src_name in CONTEXT_FILENAMES.items()
    }


def _copy_context_artifacts(*, tasks: tuple[BuildTask, ...], progress: BuildProgress) -> None:
    for task in tasks:
        src = task.source_path
        dst = task.target_path
        if src is None or dst is None:
            continue

        def _copy_one() -> None:
            dst.parent.mkdir(parents=True, exist_ok=True)
            if src.suffix.lower() == ".geojson":
                shutil.copy2(src, dst)
            elif src.suffix.lower() == ".parquet":
                shutil.copy2(src, dst)
            else:
                df = pd.read_csv(src)
                _write_parquet(_safe_numeric_downcast(df), dst)

        _run_task(task, progress, _copy_one)


def _geometry_tasks(*, data_dir: Path, selected_levels: set[str]) -> tuple[BuildTask, ...]:
    cfg = get_paths_config()
    tasks: list[BuildTask] = []

    if "district" in selected_levels:
        adm2 = ensure_adm2_columns(gpd.read_file(cfg.districts_path).to_crs(4326))
        for state_name in sorted({str(v).strip() for v in adm2["state_name"].astype(str).tolist()}):
            tasks.append(
                BuildTask(
                    stage="geometry",
                    label=f"district geometry | {state_name}",
                    state=state_name,
                    level="district",
                    source_path=Path(cfg.districts_path),
                    target_path=optimized_geometry_path(level="district", state=state_name, data_dir=data_dir),
                )
            )

    if "block" in selected_levels:
        adm3 = ensure_adm3_columns(gpd.read_file(cfg.blocks_path).to_crs(4326))
        for state_name in sorted({str(v).strip() for v in adm3["state_name"].astype(str).tolist()}):
            tasks.append(
                BuildTask(
                    stage="geometry",
                    label=f"block geometry | {state_name}",
                    state=state_name,
                    level="block",
                    source_path=Path(cfg.blocks_path),
                    target_path=optimized_geometry_path(level="block", state=state_name, data_dir=data_dir),
                )
            )

    if "basin" in selected_levels:
        tasks.append(
            BuildTask(
                stage="geometry",
                label="basin geometry",
                level="basin",
                source_path=Path(cfg.basins_path),
                target_path=optimized_geometry_path(level="basin", data_dir=data_dir),
            )
        )

    sub: Optional[gpd.GeoDataFrame] = None
    if "sub_basin" in selected_levels or {"basin", "sub_basin"} & selected_levels:
        sub = ensure_hydro_columns(gpd.read_file(cfg.subbasins_path).to_crs(4326), level="sub_basin")

    if "sub_basin" in selected_levels and sub is not None:
        for basin_id in sorted({str(v).strip() for v in sub["basin_id"].astype(str).tolist()}):
            tasks.append(
                BuildTask(
                    stage="geometry",
                    label=f"sub-basin geometry | {basin_id}",
                    level="sub_basin",
                    source_path=Path(cfg.subbasins_path),
                    target_path=optimized_geometry_path(level="sub_basin", basin_id=basin_id, data_dir=data_dir),
                )
            )

    if "block" in selected_levels:
        tasks.append(
            BuildTask(
                stage="geometry",
                label="admin block index",
                level="admin_block_index",
                source_path=Path(cfg.blocks_path),
                target_path=optimized_context_path("admin_block_index.parquet", data_dir=data_dir),
            )
        )

    if "sub_basin" in selected_levels and sub is not None:
        tasks.append(
            BuildTask(
                stage="geometry",
                label="hydro sub-basin index",
                level="hydro_subbasin_index",
                source_path=Path(cfg.subbasins_path),
                target_path=optimized_context_path("hydro_subbasin_index.parquet", data_dir=data_dir),
            )
        )

    return tuple(tasks)


def _write_geometry_bundle(*, data_dir: Path, tasks: tuple[BuildTask, ...], progress: BuildProgress) -> None:
    cfg = get_paths_config()

    task_map = {(task.level, task.state, str(task.target_path)): task for task in tasks}

    if any(task.level == "district" for task in tasks):
        adm2 = gpd.read_file(cfg.districts_path).to_crs(4326)
        adm2 = ensure_adm2_columns(adm2)
        adm2["district_key"] = adm2["state_name"].astype(str).map(alias).str.cat(
            adm2["district_name"].astype(str).map(alias),
            sep="|",
        )
        for state_name, state_gdf in adm2.groupby(adm2["state_name"].astype(str).str.strip(), dropna=False):
            out = _simplify_geometry(
                state_gdf,
                keep_cols=["district_key", "state_name", "district_name"],
                tolerance=SIMPLIFY_TOL_ADM2,
            )
            out_path = optimized_geometry_path(level="district", state=str(state_name), data_dir=data_dir)
            task = task_map.get(("district", str(state_name), str(out_path)))
            if task is not None:
                _run_task(task, progress, lambda out=out, out_path=out_path: _write_geojson(out, out_path))

    adm3: Optional[gpd.GeoDataFrame] = None
    if any(task.level in {"block", "admin_block_index"} for task in tasks):
        adm3 = gpd.read_file(cfg.blocks_path).to_crs(4326)
        adm3 = ensure_adm3_columns(adm3)
        adm3["block_key"] = (
            adm3["state_name"].astype(str)
            .map(alias)
            .str.cat(adm3["district_name"].astype(str).map(alias), sep="|")
            .str.cat(adm3["block_name"].astype(str).map(alias), sep="|")
        )
    if any(task.level == "block" for task in tasks) and adm3 is not None:
        for state_name, state_gdf in adm3.groupby(adm3["state_name"].astype(str).str.strip(), dropna=False):
            out = _simplify_geometry(
                state_gdf,
                keep_cols=["block_key", "state_name", "district_name", "block_name"],
                tolerance=SIMPLIFY_TOL_ADM3,
            )
            out_path = optimized_geometry_path(level="block", state=str(state_name), data_dir=data_dir)
            task = task_map.get(("block", str(state_name), str(out_path)))
            if task is not None:
                _run_task(task, progress, lambda out=out, out_path=out_path: _write_geojson(out, out_path))

    if any(task.level == "admin_block_index" for task in tasks) and adm3 is not None:
        admin_block_index = (
            adm3[["state_name", "district_name", "block_name"]]
            .copy()
            .dropna(subset=["state_name", "district_name", "block_name"])
        )
        for col in ("state_name", "district_name", "block_name"):
            admin_block_index[col] = admin_block_index[col].astype("string").str.strip()
        admin_block_index = admin_block_index[
            (admin_block_index["state_name"] != "")
            & (admin_block_index["district_name"] != "")
            & (admin_block_index["block_name"] != "")
        ].drop_duplicates().sort_values(["state_name", "district_name", "block_name"]).reset_index(drop=True)
        admin_block_index_path = optimized_context_path("admin_block_index.parquet", data_dir=data_dir)
        admin_block_index_task = task_map.get(("admin_block_index", None, str(admin_block_index_path)))
        if admin_block_index_task is not None:
            _run_task(
                admin_block_index_task,
                progress,
                lambda: _write_parquet(admin_block_index, admin_block_index_path),
            )

    if any(task.level == "basin" for task in tasks):
        basin = gpd.read_file(cfg.basins_path).to_crs(4326)
        basin = ensure_hydro_columns(basin, level="basin")
        basin_out = _simplify_geometry(
            basin,
            keep_cols=["basin_id", "basin_name", "hydro_level"],
            tolerance=SIMPLIFY_TOL_BASIN_RENDER,
        )
        basin_path = optimized_geometry_path(level="basin", data_dir=data_dir)
        basin_task = task_map.get(("basin", None, str(basin_path)))
        if basin_task is not None:
            _run_task(basin_task, progress, lambda: _write_geojson(basin_out, basin_path))

    sub: Optional[gpd.GeoDataFrame] = None
    if any(task.level in {"sub_basin", "hydro_subbasin_index"} for task in tasks):
        sub = gpd.read_file(cfg.subbasins_path).to_crs(4326)
        sub = ensure_hydro_columns(sub, level="sub_basin")

    if any(task.level == "sub_basin" for task in tasks) and sub is not None:
        sub_out = _simplify_geometry(
            sub,
            keep_cols=["subbasin_id", "subbasin_code", "subbasin_name", "basin_id", "basin_name", "hydro_level"],
            tolerance=SIMPLIFY_TOL_SUBBASIN_RENDER,
        )
        for basin_id, basin_gdf in sub_out.groupby("basin_id", dropna=False):
            out_path = optimized_geometry_path(level="sub_basin", basin_id=str(basin_id), data_dir=data_dir)
            task = task_map.get(("sub_basin", None, str(out_path)))
            if task is not None:
                _run_task(task, progress, lambda basin_gdf=basin_gdf, out_path=out_path: _write_geojson(basin_gdf, out_path))

    if any(task.level == "hydro_subbasin_index" for task in tasks) and sub is not None:
        hydro_subbasin_index = (
            sub[["basin_id", "basin_name", "subbasin_id", "subbasin_name"]]
            .copy()
            .dropna(subset=["basin_id", "basin_name", "subbasin_id", "subbasin_name"])
        )
        for col in ("basin_id", "basin_name", "subbasin_id", "subbasin_name"):
            hydro_subbasin_index[col] = hydro_subbasin_index[col].astype("string").str.strip()
        hydro_subbasin_index = hydro_subbasin_index[
            (hydro_subbasin_index["basin_id"] != "")
            & (hydro_subbasin_index["basin_name"] != "")
            & (hydro_subbasin_index["subbasin_id"] != "")
            & (hydro_subbasin_index["subbasin_name"] != "")
        ].drop_duplicates().sort_values(["basin_name", "subbasin_name"]).reset_index(drop=True)
        hydro_subbasin_index_path = optimized_context_path("hydro_subbasin_index.parquet", data_dir=data_dir)
        hydro_subbasin_index_task = task_map.get(("hydro_subbasin_index", None, str(hydro_subbasin_index_path)))
        if hydro_subbasin_index_task is not None:
            _run_task(
                hydro_subbasin_index_task,
                progress,
                lambda: _write_parquet(hydro_subbasin_index, hydro_subbasin_index_path),
            )


def _parity_report_path(*, data_dir: Path) -> Path:
    return resolve_optimized_bundle_root(data_dir=data_dir) / PARITY_REPORT_FILENAME


def _dir_has_parquet(path: Path) -> bool:
    return path.exists() and any(path.rglob("*.parquet"))


def _bundle_inventory_summaries(*, data_dir: Path) -> list[dict[str, object]]:
    bundle_root = resolve_optimized_bundle_root(data_dir=data_dir)
    metrics_root = bundle_root / "metrics"
    if not metrics_root.exists():
        return []

    summaries: list[dict[str, object]] = []
    for metric_root in sorted(metrics_root.iterdir()):
        if not metric_root.is_dir():
            continue
        slug = metric_root.name
        has_masters = _dir_has_parquet(metric_root / "masters")
        has_yearly_ensemble = _dir_has_parquet(metric_root / "yearly_ensemble")
        has_yearly_models = _dir_has_parquet(metric_root / "yearly_models")
        if not (has_masters or has_yearly_ensemble or has_yearly_models):
            continue
        varcfg = VARIABLES.get(slug, {})
        summaries.append(
            {
                "slug": slug,
                "source_type": str(varcfg.get("source_type") or "unknown"),
                "has_masters": has_masters,
                "has_yearly_ensemble": has_yearly_ensemble,
                "has_yearly_models": has_yearly_models,
            }
        )
    return summaries


def _write_manifest(
    *,
    data_dir: Path,
    progress: BuildProgress,
    task: BuildTask,
) -> None:
    manifest = {
        "bundle_dirname": OPTIMIZED_DIRNAME,
        "artifact_version": MANIFEST_ARTIFACT_VERSION,
        "summary_semantics": "bundle_inventory",
        "stats_contract": {
            "climate": ["mean", "median"],
            "static_snapshot": ["mean"],
            "removed": ["std", "p05", "p95", "n_models", "values_per_model", "models"],
        },
        "summaries": _bundle_inventory_summaries(data_dir=data_dir),
    }
    path = bundle_manifest_path(data_dir=data_dir)

    def _write_one() -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    _run_task(task, progress, _write_one)


def _selected_slugs(metrics: Optional[list[str]]) -> list[str]:
    available = sorted(VARIABLES.keys())
    if not metrics:
        return available
    wanted = [str(v).strip() for v in metrics if str(v).strip()]
    unknown = sorted({slug for slug in wanted if slug not in set(available)})
    if unknown:
        raise ValueError(f"Unsupported optimized metric selection: {', '.join(unknown)}")
    return [slug for slug in available if slug in wanted]


def _selected_levels(levels: Optional[list[str]]) -> tuple[str, ...]:
    if not levels:
        return LEVEL_SELECTIONS["all"]

    resolved: list[str] = []
    for value in levels:
        key = str(value).strip().lower()
        if not key:
            continue
        if key not in LEVEL_SELECTIONS:
            raise ValueError(f"Unsupported optimized level selection: {value!r}")
        for level in LEVEL_SELECTIONS[key]:
            if level not in resolved:
                resolved.append(level)
    return tuple(resolved or LEVEL_SELECTIONS["all"])


def _build_execution_plan(
    *,
    data_dir: Path,
    metrics: Optional[list[str]] = None,
    levels: Optional[list[str]] = None,
    include_geometry: bool = True,
    include_context: bool = True,
) -> BuildPlan:
    summaries_seed: list[MetricBundleSummary] = []
    master_tasks: list[BuildTask] = []
    yearly_model_jobs: list[YearlyModelsJob] = []
    yearly_ensemble_jobs: list[YearlyEnsembleJob] = []
    selected_levels = set(_selected_levels(levels))

    for slug in _selected_slugs(metrics):
        legacy_root = resolve_processed_root(slug, data_dir=data_dir, mode="portfolio")
        if not legacy_root.exists():
            continue

        varcfg = VARIABLES.get(slug, {})
        summaries_seed.append(
            MetricBundleSummary(
                slug=slug,
                source_type=str(varcfg.get("source_type") or "pipeline"),
                wrote_masters=False,
                wrote_yearly_ensemble=False,
                wrote_yearly_models=False,
            )
        )

        for state_root in _iter_state_dirs(legacy_root):
            for level in ("district", "block"):
                if level not in selected_levels:
                    continue
                source = _legacy_master_source(state_root / LEGACY_MASTER_FILENAMES[level])
                if source is not None:
                    master_tasks.append(
                        BuildTask(
                            stage="masters",
                            label=f"{slug} | {state_root.name} | {level}",
                            slug=slug,
                            state=state_root.name,
                            level=level,
                            source_path=source,
                            target_path=optimized_master_path(
                                slug,
                                level=level,
                                state=state_root.name,
                                data_dir=data_dir,
                            ),
                        )
                    )

            if bool(varcfg.get("supports_yearly_trend", True)) and "district" in selected_levels:
                district_model_paths = _iter_yearly_csv_paths(state_root, level="district")
                if district_model_paths:
                    yearly_model_jobs.append(
                        YearlyModelsJob(
                            slug=slug,
                            state=state_root.name,
                            level="district",
                            csv_paths=district_model_paths,
                            models_path=optimized_yearly_models_path(
                                slug,
                                level="district",
                                state=state_root.name,
                                data_dir=data_dir,
                            ),
                        )
                    )
                district_sources = tuple(
                    YearlyEnsembleSource(scenario=scenario, csv_path=csv_path, name_1=district_name)
                    for district_name, scenario, csv_path in iter_district_yearly_ensemble_files(
                        ts_root=legacy_root,
                        state_dir=state_root.name,
                    )
                )
                if district_sources:
                    yearly_ensemble_jobs.append(
                        YearlyEnsembleJob(
                            slug=slug,
                            level="district",
                            state=state_root.name,
                            target_path=optimized_yearly_ensemble_path(
                                slug,
                                level="district",
                                state=state_root.name,
                                data_dir=data_dir,
                            ),
                            sources=district_sources,
                        )
                    )

            if bool(varcfg.get("supports_yearly_trend", True)) and "block" in selected_levels:
                block_model_paths = _iter_yearly_csv_paths(state_root, level="block")
                if block_model_paths:
                    yearly_model_jobs.append(
                        YearlyModelsJob(
                            slug=slug,
                            state=state_root.name,
                            level="block",
                            csv_paths=block_model_paths,
                            models_path=optimized_yearly_models_path(
                                slug,
                                level="block",
                                state=state_root.name,
                                data_dir=data_dir,
                            ),
                        )
                    )
                block_sources = tuple(
                    YearlyEnsembleSource(
                        scenario=scenario,
                        csv_path=csv_path,
                        name_1=district_name,
                        name_2=block_name,
                    )
                    for district_name, block_name, scenario, csv_path in iter_block_yearly_ensemble_files(
                        ts_root=legacy_root,
                        state_dir=state_root.name,
                    )
                )
                if block_sources:
                    yearly_ensemble_jobs.append(
                        YearlyEnsembleJob(
                            slug=slug,
                            level="block",
                            state=state_root.name,
                            target_path=optimized_yearly_ensemble_path(
                                slug,
                                level="block",
                                state=state_root.name,
                                data_dir=data_dir,
                            ),
                            sources=block_sources,
                        )
                    )

        hydro_root = legacy_root / "hydro"
        if hydro_root.exists():
            for level in ("basin", "sub_basin"):
                if level not in selected_levels:
                    continue
                source = _legacy_master_source(hydro_root / LEGACY_MASTER_FILENAMES[level])
                if source is None:
                    continue
                master_tasks.append(
                    BuildTask(
                        stage="masters",
                        label=f"{slug} | hydro | {level}",
                        slug=slug,
                        level=level,
                        source_path=source,
                        target_path=optimized_master_path(slug, level=level, data_dir=data_dir),
                    )
                )
                if bool(varcfg.get("supports_yearly_trend", True)):
                    hydro_sources = tuple(
                        YearlyEnsembleSource(
                            scenario=scenario,
                            csv_path=csv_path,
                            name_1=basin_name,
                            name_2=subbasin_name,
                        )
                        for basin_name, subbasin_name, scenario, csv_path in iter_hydro_yearly_ensemble_files(
                            ts_root=legacy_root,
                            level=level,
                        )
                    )
                    if hydro_sources:
                        yearly_ensemble_jobs.append(
                            YearlyEnsembleJob(
                                slug=slug,
                                level=level,
                                target_path=optimized_yearly_ensemble_path(
                                    slug,
                                    level=level,
                                    data_dir=data_dir,
                                ),
                                source_mode="legacy_ensemble",
                                sources=hydro_sources,
                            )
                        )
                    else:
                        hydro_model_sources = tuple(
                            YearlyEnsembleSource(
                                scenario=scenario,
                                csv_path=csv_path,
                                name_1=basin_name,
                                name_2=subbasin_name,
                                model=model_name,
                            )
                            for basin_name, subbasin_name, model_name, scenario, csv_path in iter_hydro_yearly_model_files(
                                ts_root=legacy_root,
                                level=level,
                            )
                        )
                        if hydro_model_sources:
                            yearly_ensemble_jobs.append(
                                YearlyEnsembleJob(
                                    slug=slug,
                                    level=level,
                                    target_path=optimized_yearly_ensemble_path(
                                        slug,
                                        level=level,
                                        data_dir=data_dir,
                                    ),
                                    source_mode="hydro_model_fallback",
                                    sources=hydro_model_sources,
                                )
                            )

    context_tasks: list[BuildTask] = []
    if include_context:
        for src, dst in _context_map(data_dir=data_dir).items():
            if not src.exists():
                continue
            context_tasks.append(
                BuildTask(
                    stage="context",
                    label=f"context | {dst.name}",
                    source_path=src,
                    target_path=dst,
                )
            )

    geometry_tasks = (
        _geometry_tasks(data_dir=data_dir, selected_levels=selected_levels)
        if include_geometry
        else tuple()
    )
    manifest_task = BuildTask(
        stage="manifest",
        label="bundle manifest",
        target_path=bundle_manifest_path(data_dir=data_dir),
    )

    return BuildPlan(
        summaries_seed=tuple(summaries_seed),
        master_tasks=tuple(master_tasks),
        yearly_model_jobs=tuple(yearly_model_jobs),
        yearly_ensemble_jobs=tuple(yearly_ensemble_jobs),
        context_tasks=tuple(context_tasks),
        geometry_tasks=tuple(geometry_tasks),
        manifest_task=manifest_task,
    )


def _progress_enabled(show_progress: Optional[bool]) -> bool:
    if show_progress is not None:
        return bool(show_progress)
    return bool(sys.stderr.isatty())


def _metric_task_count(plan: BuildPlan) -> int:
    return len(plan.master_tasks) + len(plan.yearly_model_jobs) + len(plan.yearly_ensemble_jobs)


def _validate_build_request(
    *,
    data_dir: Path,
    plan: BuildPlan,
    metrics: Optional[list[str]],
    levels: Optional[list[str]],
    overwrite: bool,
    prune_scope: bool,
    full_rebuild: bool,
    include_geometry: bool,
    include_context: bool,
) -> None:
    if prune_scope and not overwrite:
        raise ValueError("--prune-scope requires --overwrite.")
    if full_rebuild and overwrite:
        raise ValueError("--full-rebuild cannot be combined with --overwrite.")
    if full_rebuild and prune_scope:
        raise ValueError("--full-rebuild cannot be combined with --prune-scope.")
    if full_rebuild and metrics:
        raise ValueError("--full-rebuild only supports an unfiltered whole-bundle rebuild.")
    if full_rebuild and levels:
        raise ValueError("--full-rebuild only supports an unfiltered whole-bundle rebuild.")
    if full_rebuild and not include_geometry:
        raise ValueError("--full-rebuild cannot be combined with --skip-geometry.")
    if full_rebuild and not include_context:
        raise ValueError("--full-rebuild cannot be combined with --skip-context.")

    explicit_selection = bool(metrics) or bool(levels)
    metric_task_count = _metric_task_count(plan)
    if explicit_selection and metric_task_count == 0:
        levels_msg = ", ".join(_selected_levels(levels))
        metrics_msg = ", ".join(metrics or [])
        raise ValueError(
            "No buildable legacy processed sources found for the requested selection "
            f"(metrics={metrics_msg or 'ALL'}, levels={levels_msg or 'ALL'})."
        )

    if not explicit_selection and metric_task_count == 0 and not plan.context_tasks and not plan.geometry_tasks:
        print(
            "NO OPTIMIZED OUTPUTS "
            f"(data_dir={Path(data_dir).resolve()}, bundle_root={resolve_optimized_bundle_root(data_dir=data_dir)})"
        )


def _iter_unique_paths(paths: Iterable[Path]) -> tuple[Path, ...]:
    ordered: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        resolved = str(Path(path))
        if resolved in seen:
            continue
        seen.add(resolved)
        ordered.append(Path(path))
    return tuple(ordered)


def _owned_scope_paths(
    *,
    data_dir: Path,
    slugs: Iterable[str],
    levels: Iterable[str],
    include_geometry: bool,
    include_context: bool,
) -> tuple[Path, ...]:
    bundle_root = resolve_optimized_bundle_root(data_dir=data_dir)
    metrics_root = bundle_root / "metrics"
    owned: list[Path] = []

    for slug in slugs:
        metric_root = metrics_root / slug
        if "district" in levels:
            owned.extend(
                [
                    metric_root / "masters" / "admin" / "district",
                    metric_root / "yearly_models" / "admin" / "district",
                    metric_root / "yearly_ensemble" / "admin" / "district",
                ]
            )
        if "block" in levels:
            owned.extend(
                [
                    metric_root / "masters" / "admin" / "block",
                    metric_root / "yearly_models" / "admin" / "block",
                    metric_root / "yearly_ensemble" / "admin" / "block",
                ]
            )
        if "basin" in levels:
            owned.extend(
                [
                    metric_root / "masters" / "hydro" / "basin",
                    metric_root / "yearly_ensemble" / "hydro" / "basin",
                ]
            )
        if "sub_basin" in levels:
            owned.extend(
                [
                    metric_root / "masters" / "hydro" / "sub_basin",
                    metric_root / "yearly_ensemble" / "hydro" / "sub_basin",
                ]
            )

    if include_geometry:
        geometry_root = bundle_root / "geometry"
        if "district" in levels:
            owned.append(geometry_root / "admin" / "district")
        if "block" in levels:
            owned.append(geometry_root / "admin" / "block")
        if "basin" in levels:
            owned.append(geometry_root / "hydro" / "basin.geojson")
        if "sub_basin" in levels:
            owned.append(geometry_root / "hydro" / "sub_basin")

    if include_context:
        if "block" in levels:
            owned.append(optimized_context_path("admin_block_index.parquet", data_dir=data_dir))
        if "sub_basin" in levels:
            owned.append(optimized_context_path("hydro_subbasin_index.parquet", data_dir=data_dir))

    return _iter_unique_paths(owned)


def _delete_owned_paths(paths: Iterable[Path]) -> None:
    for path in paths:
        if path.exists() and path.is_dir():
            remove_tree(path)
            continue
        unlink_file(path)


def _invalidate_bundle_metadata(*, data_dir: Path) -> None:
    unlink_file(bundle_manifest_path(data_dir=data_dir))
    unlink_file(_parity_report_path(data_dir=data_dir))


def _validate_full_rebuild_root(*, bundle_root: Path, data_dir: Path) -> None:
    resolved_root = Path(bundle_root).resolve()
    default_root = (Path(data_dir).resolve() / OPTIMIZED_DIRNAME).resolve()
    suspicious_targets = {
        Path(data_dir).resolve(),
        get_paths_config().repo_root.resolve(),
        Path.home().resolve(),
    }
    if resolved_root == resolved_root.parent:
        raise ValueError(f"Refusing to delete filesystem root: {resolved_root}")
    if resolved_root in suspicious_targets:
        raise ValueError(f"Refusing to delete suspicious optimized bundle root: {resolved_root}")
    if resolved_root == default_root:
        return
    if resolved_root.name != OPTIMIZED_DIRNAME:
        raise ValueError(
            "Refusing to delete custom optimized bundle root unless it is explicitly named "
            f"`{OPTIMIZED_DIRNAME}`: {resolved_root}"
        )
    if not resolved_root.exists():
        return
    if not resolved_root.is_dir():
        raise ValueError(f"Refusing to delete non-directory optimized bundle root: {resolved_root}")
    child_names = {child.name for child in resolved_root.iterdir()}
    allowed_markers = {"metrics", "geometry", "context", "bundle_manifest.json", PARITY_REPORT_FILENAME}
    if child_names and not (child_names & allowed_markers):
        raise ValueError(f"Refusing to delete custom root that does not look like an optimized bundle: {resolved_root}")


def _collect_write_targets(*, plan: BuildPlan, data_dir: Path, run_audit: bool) -> tuple[Path, ...]:
    targets: list[Path] = []
    for task in plan.master_tasks:
        if task.target_path is not None:
            targets.append(task.target_path)
    for job in plan.yearly_model_jobs:
        targets.append(job.models_path)
    for job in plan.yearly_ensemble_jobs:
        targets.append(job.target_path)
    for task in plan.context_tasks:
        if task.target_path is not None:
            targets.append(task.target_path)
    for task in plan.geometry_tasks:
        if task.target_path is not None:
            targets.append(task.target_path)
    if plan.manifest_task.target_path is not None:
        targets.append(plan.manifest_task.target_path)
    if run_audit:
        targets.append(_parity_report_path(data_dir=data_dir))
    return _iter_unique_paths(targets)


def _print_dry_run(
    *,
    data_dir: Path,
    bundle_root: Path,
    plan: BuildPlan,
    overwrite: bool,
    prune_scope: bool,
    full_rebuild: bool,
    include_geometry: bool,
    include_context: bool,
    run_audit: bool,
    levels: Optional[list[str]],
) -> None:
    selected_slugs = [seed.slug for seed in plan.summaries_seed]
    selected_levels = _selected_levels(levels)
    print("PROCESSED OPTIMISED DRY RUN")
    print(f"data_dir: {Path(data_dir).resolve()}")
    print(f"bundle_root: {bundle_root}")
    if full_rebuild:
        print(f"full_rebuild_delete_root: {bundle_root}")
    print(f"metadata_invalidated: {bundle_manifest_path(data_dir=data_dir)}")
    print(f"metadata_invalidated: {_parity_report_path(data_dir=data_dir)}")
    if overwrite and prune_scope:
        for path in _owned_scope_paths(
            data_dir=data_dir,
            slugs=selected_slugs,
            levels=selected_levels,
            include_geometry=include_geometry,
            include_context=include_context,
        ):
            print(f"scope_prune: {path}")
    for path in _collect_write_targets(plan=plan, data_dir=data_dir, run_audit=run_audit):
        print(f"write_target: {path}")


def _required_columns_for_master(level: str) -> set[str]:
    level_norm = str(level).strip().lower()
    if level_norm == "district":
        return {"state", "district", "district_key"}
    if level_norm == "block":
        return {"state", "district", "block", "block_key"}
    if level_norm == "basin":
        return {"basin_id", "basin_name"}
    return {"basin_id", "basin_name", "subbasin_id", "subbasin_name"}


def _required_columns_for_yearly_models(level: str) -> set[str]:
    key_col = "district_key" if str(level).strip().lower() == "district" else "block_key"
    return {key_col, "scenario", "model", "year", "value"}


def _required_columns_for_yearly_ensemble(level: str) -> set[str]:
    level_norm = str(level).strip().lower()
    if level_norm == "district":
        return {"district_key", "scenario", "year", "mean"}
    if level_norm == "block":
        return {"block_key", "scenario", "year", "mean"}
    if level_norm == "basin":
        return {"basin_name", "scenario", "year", "mean"}
    return {"basin_name", "subbasin_name", "scenario", "year", "mean"}


def _table_has_required_columns(path: Path, required_columns: set[str]) -> tuple[bool, list[str]]:
    if not path.exists():
        return False, sorted(required_columns)
    try:
        cols = set(read_table(path).columns)
    except Exception:
        return False, sorted(required_columns)
    missing = sorted(required_columns - cols)
    return not missing, missing


def audit_processed_optimised_parity(
    *,
    data_dir: Path,
    metrics: Optional[list[str]] = None,
    levels: Optional[list[str]] = None,
    include_geometry: bool = True,
    include_context: bool = True,
    write_report: bool = True,
) -> dict:
    """Validate that optimized artifacts exist for every dashboard-visible legacy source."""
    plan = _build_execution_plan(
        data_dir=data_dir,
        metrics=metrics,
        levels=levels,
        include_geometry=include_geometry,
        include_context=include_context,
    )
    bundle_root = resolve_optimized_bundle_root(data_dir=data_dir)
    issues: list[dict[str, str | list[str]]] = []

    for task in plan.master_tasks:
        target = task.target_path
        if target is None:
            continue
        ok, missing_cols = _table_has_required_columns(target, _required_columns_for_master(str(task.level)))
        if not ok:
            issues.append(
                {
                    "stage": "masters",
                    "slug": str(task.slug or ""),
                    "level": str(task.level or ""),
                    "target": str(target),
                    "missing_columns": missing_cols,
                }
            )

    for job in plan.yearly_model_jobs:
        ok, missing_cols = _table_has_required_columns(job.models_path, _required_columns_for_yearly_models(job.level))
        if not ok:
            issues.append(
                {
                    "stage": "yearly-models",
                    "slug": job.slug,
                    "level": job.level,
                    "target": str(job.models_path),
                    "missing_columns": missing_cols,
                }
            )

    for job in plan.yearly_ensemble_jobs:
        ok, missing_cols = _table_has_required_columns(job.target_path, _required_columns_for_yearly_ensemble(job.level))
        if not ok:
            issues.append(
                {
                    "stage": "yearly-ensemble",
                    "slug": job.slug,
                    "level": job.level,
                    "target": str(job.target_path),
                    "missing_columns": missing_cols,
                }
            )

    for task in plan.context_tasks:
        if task.target_path is not None and not task.target_path.exists():
            issues.append(
                {
                    "stage": "context",
                    "slug": "",
                    "level": "",
                    "target": str(task.target_path),
                    "missing_columns": [],
                }
            )

    for task in plan.geometry_tasks:
        if task.target_path is not None and not task.target_path.exists():
            issues.append(
                {
                    "stage": "geometry",
                    "slug": "",
                    "level": str(task.level or ""),
                    "target": str(task.target_path),
                    "missing_columns": [],
                }
            )

    if not plan.manifest_task.target_path or not plan.manifest_task.target_path.exists():
        issues.append(
            {
                "stage": "manifest",
                "slug": "",
                "level": "",
                "target": str(plan.manifest_task.target_path or bundle_manifest_path(data_dir=data_dir)),
                "missing_columns": [],
            }
        )

    report = {
        "bundle_root": str(bundle_root),
        "metrics_considered": len(plan.summaries_seed),
        "expected_master_outputs": len(plan.master_tasks),
        "expected_yearly_model_outputs": len(plan.yearly_model_jobs),
        "expected_yearly_ensemble_outputs": len(plan.yearly_ensemble_jobs),
        "expected_context_outputs": len(plan.context_tasks),
        "expected_geometry_outputs": len(plan.geometry_tasks),
        "issue_count": len(issues),
        "issues": issues,
    }

    if write_report:
        report_path = bundle_root / "parity_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    return report


def build_processed_optimised_bundle(
    *,
    data_dir: Path,
    metrics: Optional[list[str]] = None,
    levels: Optional[list[str]] = None,
    workers: Optional[int] = None,
    overwrite: bool = False,
    prune_scope: bool = False,
    full_rebuild: bool = False,
    dry_run: bool = False,
    include_geometry: bool = True,
    include_context: bool = True,
    show_progress: Optional[bool] = None,
    run_audit: bool = True,
) -> list[MetricBundleSummary]:
    """
    Build the optimized runtime bundle from the current legacy processed tree.
    """
    plan = _build_execution_plan(
        data_dir=data_dir,
        metrics=metrics,
        levels=levels,
        include_geometry=include_geometry,
        include_context=include_context,
    )
    _validate_build_request(
        data_dir=data_dir,
        plan=plan,
        metrics=metrics,
        levels=levels,
        overwrite=overwrite,
        prune_scope=prune_scope,
        full_rebuild=full_rebuild,
        include_geometry=include_geometry,
        include_context=include_context,
    )

    bundle_root = resolve_optimized_bundle_root(data_dir=data_dir)
    summaries_map = {
        seed.slug: {
            "slug": seed.slug,
            "source_type": seed.source_type,
            "wrote_masters": False,
            "wrote_yearly_ensemble": False,
            "wrote_yearly_models": False,
        }
        for seed in plan.summaries_seed
    }
    metric_task_count = _metric_task_count(plan)
    if metric_task_count == 0 and not plan.context_tasks and not plan.geometry_tasks:
        return []

    if dry_run:
        if full_rebuild:
            _validate_full_rebuild_root(bundle_root=bundle_root, data_dir=data_dir)
        _print_dry_run(
            data_dir=data_dir,
            bundle_root=bundle_root,
            plan=plan,
            overwrite=overwrite,
            prune_scope=prune_scope,
            full_rebuild=full_rebuild,
            include_geometry=include_geometry,
            include_context=include_context,
            run_audit=run_audit,
            levels=levels,
        )
        return [MetricBundleSummary(**payload) for payload in summaries_map.values()]

    if full_rebuild:
        _validate_full_rebuild_root(bundle_root=bundle_root, data_dir=data_dir)
        remove_tree(bundle_root)
    else:
        _invalidate_bundle_metadata(data_dir=data_dir)
        if overwrite and prune_scope:
            _delete_owned_paths(
                _owned_scope_paths(
                    data_dir=data_dir,
                    slugs=[seed.slug for seed in plan.summaries_seed],
                    levels=_selected_levels(levels),
                    include_geometry=include_geometry,
                    include_context=include_context,
                )
            )

    progress = BuildProgress(plan, enabled=_progress_enabled(show_progress))
    progress.print_plan_summary()
    resolved_workers = resolve_build_workers(workers)

    try:
        for task in plan.master_tasks:
            slug = task.slug or ""
            varcfg = VARIABLES.get(slug, {})
            supported_stats = list(varcfg.get("supported_statistics") or ("mean", "median"))

            def _write_master() -> None:
                source = task.source_path
                target = task.target_path
                if source is None or target is None:
                    return
                df = _read_legacy_master(source)
                if df.empty:
                    return
                out = _select_master_columns(df, level=str(task.level), supported_stats=supported_stats)
                if out.empty:
                    return
                _write_parquet(out, target)
                summaries_map[slug]["wrote_masters"] = True

            _run_task(task, progress, _write_master)

        for job in plan.yearly_model_jobs:
            model_df = _load_legacy_admin_yearly_models(
                slug=job.slug,
                state_name=job.state,
                level=job.level,
                csv_paths=job.csv_paths,
                progress=progress,
                workers=resolved_workers,
            )

            model_task = BuildTask(
                stage="yearly-models",
                label=f"{job.slug} | {job.state} | {job.level} | models parquet",
                slug=job.slug,
                state=job.state,
                level=job.level,
                target_path=job.models_path,
            )

            def _write_models() -> None:
                if model_df.empty:
                    return
                _write_parquet(model_df, job.models_path)
                summaries_map[job.slug]["wrote_yearly_models"] = True

            _run_task(model_task, progress, _write_models)

        for job in plan.yearly_ensemble_jobs:
            label_prefix = f"{job.slug} | {job.state or 'hydro'} | {job.level}"
            basin_map: dict[str, tuple[str, str]] = {}
            subbasin_map: dict[str, tuple[str, str, str, str]] = {}
            if job.level in {"basin", "sub_basin"}:
                basin_map, subbasin_map = _hydro_name_maps(
                    data_dir=data_dir,
                    slug=job.slug,
                    level=job.level,
                )

            if job.source_mode == "hydro_model_fallback":
                hydro_model_df = _load_legacy_hydro_yearly_models(
                    level=job.level,
                    sources=job.sources,
                    basin_map=basin_map,
                    subbasin_map=subbasin_map,
                    progress=progress,
                    label_prefix=label_prefix,
                    workers=resolved_workers,
                )
                ensemble_df = pd.DataFrame()
            else:
                ensemble_df = _load_legacy_yearly_ensemble(
                    level=job.level,
                    state_name=job.state,
                    sources=job.sources,
                    basin_map=basin_map,
                    subbasin_map=subbasin_map,
                    progress=progress,
                    label_prefix=label_prefix,
                    workers=resolved_workers,
                )
                hydro_model_df = pd.DataFrame()

            ensemble_task = BuildTask(
                stage="yearly-ensemble",
                label=f"{job.slug} | {job.state or 'hydro'} | {job.level} | ensemble parquet",
                slug=job.slug,
                state=job.state,
                level=job.level,
                target_path=job.target_path,
            )

            def _write_ensemble() -> None:
                if job.source_mode == "hydro_model_fallback":
                    derived_df = _build_hydro_yearly_ensemble_from_models(hydro_model_df, level=job.level)
                    if derived_df.empty:
                        return
                    _write_parquet(derived_df, job.target_path)
                    summaries_map[job.slug]["wrote_yearly_ensemble"] = True
                    return
                if ensemble_df.empty:
                    return
                _write_parquet(_safe_numeric_downcast(ensemble_df), job.target_path)
                summaries_map[job.slug]["wrote_yearly_ensemble"] = True

            _run_task(ensemble_task, progress, _write_ensemble)

        if include_context:
            _copy_context_artifacts(tasks=plan.context_tasks, progress=progress)
        if include_geometry:
            _write_geometry_bundle(data_dir=data_dir, tasks=plan.geometry_tasks, progress=progress)

        summaries = [MetricBundleSummary(**payload) for payload in summaries_map.values()]
        _write_manifest(
            data_dir=data_dir,
            progress=progress,
            task=plan.manifest_task,
        )
        progress.close()
        if run_audit:
            parity = audit_processed_optimised_parity(
                data_dir=data_dir,
                metrics=metrics,
                levels=levels,
                include_geometry=include_geometry,
                include_context=include_context,
                write_report=True,
            )
            print(
                "PARITY AUDIT "
                f"(metrics={parity['metrics_considered']}, issues={parity['issue_count']}, "
                f"report={resolve_optimized_bundle_root(data_dir=data_dir) / 'parity_report.json'})"
            )
        return summaries
    except Exception:
        progress.close()
        print(progress.failure_summary(), file=sys.stderr)
        raise


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the processed_optimised runtime bundle.")
    parser.add_argument("--metric", action="append", dest="metrics", help="One metric slug to include. Repeatable.")
    parser.add_argument(
        "--level",
        action="append",
        dest="levels",
        choices=sorted(LEVEL_SELECTIONS.keys()),
        help="Restrict the build to one or more level groups or concrete levels.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Rewrite only the selected optimized outputs in place without deleting the bundle root.",
    )
    parser.add_argument(
        "--prune-scope",
        action="store_true",
        help="With --overwrite, delete stale files only inside the selected metric/level ownership roots before rewriting.",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Delete and rebuild the entire processed_optimised bundle. This is destructive.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resolved write/delete plan without mutating processed_optimised.",
    )
    parser.add_argument("--skip-geometry", action="store_true", help="Skip optimized geometry generation.")
    parser.add_argument("--skip-context", action="store_true", help="Skip optimized context artifacts.")
    parser.add_argument("--skip-audit", action="store_true", help="Skip the post-build parity audit.")
    parser.add_argument(
        "--workers",
        type=int,
        help="Worker count for parallel yearly processing. Defaults to 80%% of logical CPUs; use 1 for serial execution.",
    )
    parser.add_argument("--no-progress", action="store_true", help="Disable tqdm progress bars.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    data_dir = get_paths_config().data_dir
    summaries = build_processed_optimised_bundle(
        data_dir=data_dir,
        metrics=args.metrics,
        levels=args.levels,
        workers=args.workers,
        overwrite=bool(args.overwrite),
        prune_scope=bool(args.prune_scope),
        full_rebuild=bool(args.full_rebuild),
        dry_run=bool(args.dry_run),
        include_geometry=not bool(args.skip_geometry),
        include_context=not bool(args.skip_context),
        show_progress=False if bool(args.no_progress) else None,
        run_audit=not bool(args.skip_audit),
    )

    print("PROCESSED OPTIMISED BUNDLE")
    print(f"data_dir: {data_dir}")
    print(f"bundle_root: {resolve_optimized_bundle_root(data_dir=data_dir)}")
    print(f"mode: {'dry-run' if bool(args.dry_run) else 'build'}")
    print(f"metrics_written: {len(summaries)}")
    wrote_yearly = sum(1 for s in summaries if s.wrote_yearly_ensemble or s.wrote_yearly_models)
    print(f"metrics_with_yearly: {wrote_yearly}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

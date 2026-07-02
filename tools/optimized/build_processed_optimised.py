"""
Build the compact `processed_optimised` runtime bundle from existing legacy data.

This tool is intentionally non-destructive: it reads from the current
`IRT_DATA_DIR/processed` tree and writes a new optimized bundle under
`IRT_DATA_DIR/processed_optimised`.
"""

from __future__ import annotations

import argparse
import math
import os
import json
import shutil
import sys
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional, TypeVar

from pyproj import datadir


def _configure_pyproj_data_dir() -> None:
    """Point pyproj/GDAL at a usable PROJ database before GeoPandas imports."""
    candidates = [
        os.environ.get("PROJ_DATA"),
        os.environ.get("PROJ_LIB"),
    ]
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix:
        candidates.append(str(Path(conda_prefix) / "Library" / "share" / "proj"))
    candidates.append(str(Path(sys.prefix) / "Library" / "share" / "proj"))

    for candidate in candidates:
        if not candidate:
            continue
        proj_db = Path(candidate) / "proj.db"
        if proj_db.exists():
            os.environ.setdefault("PROJ_DATA", str(proj_db.parent))
            os.environ.setdefault("PROJ_LIB", str(proj_db.parent))
            datadir.set_data_dir(str(proj_db.parent))
            return


_configure_pyproj_data_dir()

import geopandas as gpd
import pandas as pd
from tqdm.auto import tqdm

from india_resilience_tool.config.constants import (
    SIMPLIFY_TOL_ADM1,
    SIMPLIFY_TOL_ADM2,
    SIMPLIFY_TOL_ADM3,
)
from india_resilience_tool.compute.glance_view_model import (
    GLANCE_FILENAMES,
    GLANCE_REQUIRED_COLUMNS,
    build_glance_view_models,
    glance_manifest_payload,
)
from india_resilience_tool.config.dashboard_bundles import (
    get_dashboard_bundle_spec_by_slug,
    get_dashboard_bundle_specs,
)
from india_resilience_tool.config.paths import get_paths_config, resolve_processed_root
from india_resilience_tool.config.proposal_bundles import (
    get_proposal_bundle_spec_by_slug,
    proposal_available_rule_count_column,
    proposal_available_rule_weight_fraction_column,
    proposal_rule_abs_score_column,
    proposal_rule_chg_score_column,
    proposal_rule_imp_score_column,
    proposal_rule_score_column,
)
from india_resilience_tool.config.variables import VARIABLES
from india_resilience_tool.data.adm2_loader import ensure_adm2_columns
from india_resilience_tool.data.adm3_loader import ensure_adm3_columns
from india_resilience_tool.data.discovery import (
    iter_block_yearly_ensemble_files,
    iter_district_yearly_ensemble_files,
)
from india_resilience_tool.data.optimized_bundle import (
    OPTIMIZED_DIRNAME,
    bundle_manifest_path,
    optimized_adm1_path,
    optimized_glance_root,
    optimized_context_path,
    optimized_geometry_path,
    optimized_master_path,
    optimized_master_sources_from_metric_root,
    optimized_state_values_path,
    optimized_yearly_ensemble_path,
    optimized_yearly_models_path,
    resolve_optimized_metric_root,
    resolve_optimized_bundle_root,
)
from india_resilience_tool.utils.naming import alias, normalize_name
from india_resilience_tool.utils.processed_io import read_table, remove_tree, unlink_file


LEGACY_MASTER_FILENAMES = {
    "district": "master_metrics_by_district.csv",
    "block": "master_metrics_by_block.csv",
}

ADMIN_ID_COLS = {
    "district": ["district", "state"],
    "block": ["block", "district", "state"],
}

CONTEXT_FILENAMES = {
    "district_subbasin.parquet": "district_subbasin_crosswalk.csv",
    "block_subbasin.parquet": "block_subbasin_crosswalk.csv",
    "district_basin.parquet": "district_basin_crosswalk.csv",
    "block_basin.parquet": "block_basin_crosswalk.csv",
    "river_reaches.parquet": "river_reaches.parquet",
    "river_network_display.geojson": "river_network_display.geojson",
    "jrc_flood_depth/overlay/rp100_depth_overlay.png": "jrc_flood_depth/overlay/rp100_depth_overlay.png",
    "jrc_flood_depth/overlay/rp100_depth_overlay_meta.json": "jrc_flood_depth/overlay/rp100_depth_overlay_meta.json",
    "population/overlay/population_exposure_2025_overlay.png": "population/overlay/population_exposure_2025_overlay.png",
    "population/overlay/population_exposure_2025_overlay_meta.json": "population/overlay/population_exposure_2025_overlay_meta.json",
    "built_up_area/overlay/built_up_area_current_overlay.png": "built_up_area/overlay/built_up_area_current_overlay.png",
    "built_up_area/overlay/built_up_area_current_overlay_meta.json": "built_up_area/overlay/built_up_area_current_overlay_meta.json",
    "lulc/overlay/lulc_agri_current_overlay.png": "lulc/overlay/lulc_agri_current_overlay.png",
    "lulc/overlay/lulc_agri_current_overlay_meta.json": "lulc/overlay/lulc_agri_current_overlay_meta.json",
    "rural_facilities/overlay/rural_facilities_density_total_overlay.png": "rural_facilities/overlay/rural_facilities_density_total_overlay.png",
    "rural_facilities/overlay/rural_facilities_density_total_overlay_meta.json": "rural_facilities/overlay/rural_facilities_density_total_overlay_meta.json",
    "rural_facilities/overlay/rural_facilities_density_agro_overlay.png": "rural_facilities/overlay/rural_facilities_density_agro_overlay.png",
    "rural_facilities/overlay/rural_facilities_density_agro_overlay_meta.json": "rural_facilities/overlay/rural_facilities_density_agro_overlay_meta.json",
    "rural_facilities/overlay/rural_facilities_density_education_overlay.png": "rural_facilities/overlay/rural_facilities_density_education_overlay.png",
    "rural_facilities/overlay/rural_facilities_density_education_overlay_meta.json": "rural_facilities/overlay/rural_facilities_density_education_overlay_meta.json",
    "rural_facilities/overlay/rural_facilities_density_health_overlay.png": "rural_facilities/overlay/rural_facilities_density_health_overlay.png",
    "rural_facilities/overlay/rural_facilities_density_health_overlay_meta.json": "rural_facilities/overlay/rural_facilities_density_health_overlay_meta.json",
    "rural_facilities/overlay/rural_facilities_density_service_overlay.png": "rural_facilities/overlay/rural_facilities_density_service_overlay.png",
    "rural_facilities/overlay/rural_facilities_density_service_overlay_meta.json": "rural_facilities/overlay/rural_facilities_density_service_overlay_meta.json",
}

LEVEL_SELECTIONS = {
    "all": ("district", "block"),
    "admin": ("district", "block"),
    "district": ("district",),
    "block": ("block",),
}

YEARLY_PARALLEL_CHUNK_SIZE = 64
MANIFEST_ARTIFACT_VERSION = 3
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
    glance_slugs: tuple[str, ...] = ()

    def stage_totals(self) -> dict[str, int]:
        return {
            "masters": len(self.master_tasks),
            "yearly-models": sum(len(job.csv_paths) + 1 for job in self.yearly_model_jobs),
            "yearly-ensemble": sum(len(job.sources) + 1 for job in self.yearly_ensemble_jobs),
            "context": len(self.context_tasks),
            "geometry": len(self.geometry_tasks),
            "glance": len(self.glance_slugs),
            "manifest": 1 if self.manifest_task.target_path is not None else 0,
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
        # Per-stage wall-clock accounting (independent of progress-bar enablement).
        # Stages run serially, so each stage owns the interval between its first
        # touch and the next stage's first touch; close() seals the final stage.
        self._stage_elapsed: dict[str, float] = {stage: 0.0 for stage in self._stage_totals}
        self._timing_stage: Optional[str] = None
        self._timing_start: Optional[float] = None

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

    def _touch_stage_timer(self, stage: str) -> None:
        """Attribute wall-clock to the stage we are leaving and arm the new stage.

        Runs regardless of progress-bar enablement so timing is available under a
        profiler that disables bars. Idempotent within a stage.
        """
        if stage == self._timing_stage:
            return
        now = time.perf_counter()
        if self._timing_stage is not None and self._timing_start is not None:
            self._stage_elapsed[self._timing_stage] = (
                self._stage_elapsed.get(self._timing_stage, 0.0) + (now - self._timing_start)
            )
        self._timing_stage = stage
        self._timing_start = now

    def start_task(self, task: BuildTask) -> None:
        self._current_task = task
        self._touch_stage_timer(task.stage)
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
        self._touch_stage_timer(stage)
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

    def _seal_stage_timer(self) -> None:
        """Fold the currently-open stage interval into its accumulator (idempotent)."""
        if self._timing_stage is not None and self._timing_start is not None:
            now = time.perf_counter()
            self._stage_elapsed[self._timing_stage] = (
                self._stage_elapsed.get(self._timing_stage, 0.0) + (now - self._timing_start)
            )
        self._timing_start = None

    def print_stage_timing(self) -> None:
        """Emit the per-stage wall-clock split of the build (audit is timed separately)."""
        total = sum(self._stage_elapsed.values())
        if total <= 0:
            return
        parts = [
            f"{stage}={self._stage_elapsed.get(stage, 0.0):.1f}s "
            f"({(self._stage_elapsed.get(stage, 0.0) / total * 100.0):.1f}%)"
            for stage in self._stage_totals
        ]
        print(f"STAGE TIMING total={total:.1f}s " + ", ".join(parts))

    def close(self) -> None:
        self._seal_stage_timer()
        if self._stage_bar is not None:
            self._stage_bar.close()
            self._stage_bar = None
        if self._overall_bar is not None:
            self._overall_bar.close()
            self._overall_bar = None
        self.print_stage_timing()


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


def _yearly_executor_kind() -> str:
    """Resolve the executor backend for the yearly-loader chunk pool.

    ``'process'`` (default) preserves today's ``ProcessPoolExecutor`` behavior;
    ``'thread'`` (opt-in via ``IRT_YEARLY_EXECUTOR=thread``) uses a thread pool —
    safe because the yearly workers read CSVs only (no geospatial/pyproj calls)
    and pay no spawn/pickle tax. Unknown values fall back to ``'process'``.
    """
    kind = os.environ.get("IRT_YEARLY_EXECUTOR", "process").strip().lower()
    return kind if kind in {"process", "thread"} else "process"


def _yearly_chunk_size(n_items: int, worker_count: int, kind: str) -> int:
    """Chunk size for the yearly-loader parallel branch.

    Processes want few large chunks (spawn is costly) -> keep the fixed default.
    Threads want many small chunks (cheap fan-out) -> split per worker so small
    single-state jobs actually parallelize. Note this yields ~``ceil(n/workers)``
    chunks, not ``worker_count`` chunks; callers must not assume the latter.
    """
    if kind == "thread" and worker_count > 1:
        return max(1, math.ceil(n_items / worker_count))
    return YEARLY_PARALLEL_CHUNK_SIZE


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


def _proposal_retained_admin_master_cols(
    df: pd.DataFrame,
    *,
    slug: str,
    level: str,
) -> list[str]:
    """Return proposal-only admin master columns retained in the optimized bundle."""
    level_norm = str(level).strip().lower()
    if level_norm not in {"district", "block"}:
        return []

    bundle_spec = get_proposal_bundle_spec_by_slug(slug)
    if bundle_spec is None:
        return []

    keep_cols: list[str] = []
    available_cols = set(df.columns)
    for scenario in ("ssp245", "ssp585"):
        for period in ("2020-2040", "2040-2060", "2060-2080"):
            available_count_col = proposal_available_rule_count_column(bundle_spec.composite_slug, scenario, period)
            if available_count_col in available_cols:
                keep_cols.append(available_count_col)
            available_weight_col = proposal_available_rule_weight_fraction_column(
                bundle_spec.composite_slug,
                scenario,
                period,
            )
            if available_weight_col in available_cols:
                keep_cols.append(available_weight_col)
            for rule in bundle_spec.rules:
                score_col = proposal_rule_score_column(rule.rule_slug, scenario, period)
                if score_col in available_cols:
                    keep_cols.append(score_col)
                for lens_col_builder in (
                    proposal_rule_abs_score_column,
                    proposal_rule_chg_score_column,
                    proposal_rule_imp_score_column,
                ):
                    lens_col = lens_col_builder(rule.rule_slug, scenario, period)
                    if lens_col in available_cols:
                        keep_cols.append(lens_col)
    return keep_cols


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


class CanonicalRosterError(RuntimeError):
    """Raised when published masters carry district/block keys absent from the current canonical boundary roster."""


@lru_cache(maxsize=None)
def _canonical_admin_keys(level: str, source_path: str) -> frozenset:
    """Return the alias-normalized admin keys for ``level`` from the live boundary source.

    District keys: ``alias(state_name)|alias(district_name)``.
    Block keys:    ``alias(state_name)|alias(district_name)|alias(block_name)``.
    Loads the full national boundary layer once (cached per (level, source_path)); the
    build process is short-lived, so this read happens at most twice per run.
    """
    gdf = gpd.read_file(source_path)
    if level == "district":
        gdf = ensure_adm2_columns(gdf)
        keys = (
            gdf["state_name"].astype(str).map(alias)
            .str.cat(gdf["district_name"].astype(str).map(alias), sep="|")
        )
    else:
        gdf = ensure_adm3_columns(gdf)
        keys = (
            gdf["state_name"].astype(str).map(alias)
            .str.cat(gdf["district_name"].astype(str).map(alias), sep="|")
            .str.cat(gdf["block_name"].astype(str).map(alias), sep="|")
        )
    return frozenset(str(k) for k in keys.tolist())


def _roster_gate_mode() -> str:
    """Resolve the gate mode from ``IRT_ROSTER_GATE``.

    Default ``warn`` during the boundary-migration transition; the intended end-state
    is ``strict`` (flip the default in a follow-up once the roster audit is 100% clean).
    Unknown values fall back to ``strict`` (fail-safe) with a warning.
    """
    mode = os.environ.get("IRT_ROSTER_GATE", "warn").strip().lower()
    if mode not in {"strict", "warn", "off"}:
        warnings.warn(f"Unknown IRT_ROSTER_GATE={mode!r}; falling back to 'strict'.", stacklevel=2)
        return "strict"
    return mode


def _roster_offender_summary(slug: str, level: str, n_rows: int, offenders: list) -> str:
    shown = ", ".join(sorted(offenders)[:12]) + (" …" if len(offenders) > 12 else "")
    return (
        f"[roster-gate] {slug} ({level}): {n_rows} row(s) across {len(offenders)} admin "
        f"unit(s) absent from the current canonical boundary roster — likely stale/renamed "
        f"names: {shown}"
    )


def _check_canonical_roster(out: pd.DataFrame, *, slug: str, level: str):
    """Validate a master's admin rows against the live canonical boundary roster.

    Returns ``(frame_to_write_or_None, offenders)`` and never raises:
      * ``off`` / non-admin level / no offenders -> ``(out, [])``.
      * ``warn`` -> ``(out_without_offenders, offenders)``: caller writes the cleaned
        frame and logs. NOTE: this only *annotates* — a renamed unit's sole row is
        dropped, so it stays blank on the map. ``warn`` does not heal.
      * ``strict`` -> ``(None, offenders)``: caller skips writing this master (leaving the
        last-good copy untouched); the run raises once, after the loop, with the full list.
    """
    mode = _roster_gate_mode()
    if mode == "off" or level not in {"district", "block"}:
        return out, []
    key_col = "district_key" if level == "district" else "block_key"
    if key_col not in out.columns:
        return out, []
    cfg = get_paths_config()
    source = str(cfg.districts_path if level == "district" else cfg.blocks_path)
    canonical = _canonical_admin_keys(level, source)
    mask_bad = ~out[key_col].astype(str).isin(canonical)
    if not mask_bad.any():
        return out, []
    id_cols = [c for c in ("state", "district", "block") if c in out.columns]
    offenders = (
        out.loc[mask_bad, id_cols].drop_duplicates().astype(str).agg(" | ".join, axis=1).tolist()
    )
    if mode == "warn":
        warnings.warn(
            _roster_offender_summary(slug, level, int(mask_bad.sum()), offenders)
            + " — dropping rows (IRT_ROSTER_GATE=warn); these units stay blank, not healed.",
            stacklevel=2,
        )
        return out.loc[~mask_bad].copy(), offenders
    return None, offenders


def _roster_violations_report(violations: dict) -> str:
    lines = [
        "Canonical-roster gate (IRT_ROSTER_GATE=strict) blocked the publish.",
        f"{len(violations)} master(s) carried admin units absent from the current boundary "
        f"roster; their stale copies were left untouched and NOT republished:",
    ]
    for slug in sorted(violations):
        units = sorted(set(violations[slug]))
        shown = ", ".join(units[:12]) + (" …" if len(units) > 12 else "")
        lines.append(f"  - {slug}: {len(units)} unit(s) -> {shown}")
    lines.append(
        "Rebuild these masters on the current boundaries, or set IRT_ROSTER_GATE=warn to "
        "publish the clean bundles and drop the stale rows."
    )
    return "\n".join(lines)


def _select_master_columns(
    df: pd.DataFrame,
    *,
    slug: str,
    level: str,
    supported_stats: Iterable[str],
) -> pd.DataFrame:
    id_cols = list(ADMIN_ID_COLS[level])
    keep_cols = [c for c in id_cols if c in df.columns]
    keep_cols.extend(_metric_value_cols(df, supported_stats=supported_stats))
    keep_cols.extend(_proposal_retained_admin_master_cols(df, slug=slug, level=level))
    keep_cols = list(dict.fromkeys(keep_cols))
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


def _run_chunks_serial(
    *,
    progress: BuildProgress,
    stage: str,
    label_prefix: str,
    chunks: tuple[tuple[str, ...], ...],
    worker_fn,
    worker_kwargs: dict[str, object],
) -> list[tuple[int, pd.DataFrame]]:
    """Run chunk workers in-process, mirroring the parallel path exactly.

    Used when ``max_workers <= 1`` so a single-chunk job does not pay the cost of
    spawning a one-worker process pool (pure overhead on Windows). Reproduces all
    parallel-path invariants: per-chunk ``advance_stage``, deterministic
    chunk-ordered output via the post-sort, and unswallowed worker exceptions
    (matching ``future.result()`` semantics).
    """
    results: list[tuple[int, pd.DataFrame]] = []
    for chunk_index, chunk in enumerate(chunks):
        chunk_result = worker_fn(chunk_index, chunk, **worker_kwargs)
        progress.advance_stage(
            stage=stage,
            label=f"{label_prefix} | chunk {chunk_index + 1}/{len(chunks)}",
            count=len(chunk),
        )
        results.append(chunk_result)
    results.sort(key=lambda item: item[0])
    return results


def _execute_parallel_chunks(
    *,
    progress: BuildProgress,
    stage: str,
    label_prefix: str,
    chunks: tuple[tuple[str, ...], ...],
    worker_count: int,
    worker_fn,
    worker_kwargs: dict[str, object],
    kind: str = "process",
) -> list[tuple[int, pd.DataFrame]]:
    """Execute chunked worker tasks and preserve deterministic chunk ordering.

    ``kind`` selects the parallel backend (``'process'`` or ``'thread'``) and is
    resolved once by the caller; this function never re-reads the environment.
    A ``max_workers <= 1`` plan runs serially in-process (no pool spawn).
    """
    if not chunks:
        return []

    max_workers = max(1, min(int(worker_count), len(chunks)))
    if max_workers <= 1:
        return _run_chunks_serial(
            progress=progress,
            stage=stage,
            label_prefix=label_prefix,
            chunks=chunks,
            worker_fn=worker_fn,
            worker_kwargs=worker_kwargs,
        )

    executor_cls = ThreadPoolExecutor if kind == "thread" else ProcessPoolExecutor
    results: list[tuple[int, pd.DataFrame]] = []
    with executor_cls(max_workers=max_workers) as executor:
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
    kind = _yearly_executor_kind()
    path_strings = tuple(str(path) for path in csv_paths)
    path_chunks = _chunk_tuple(
        path_strings, chunk_size=_yearly_chunk_size(len(path_strings), workers, kind)
    )
    chunk_results = _execute_parallel_chunks(
        progress=progress,
        stage="yearly-models",
        label_prefix=f"{slug} | {state_name} | {level}",
        chunks=path_chunks,
        worker_count=workers,
        worker_fn=_load_legacy_admin_yearly_models_chunk,
        worker_kwargs={"state_name": state_name, "level": level},
        kind=kind,
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


def _normalized_key(*parts: str) -> str:
    return "|".join(alias(part) for part in parts)


def _load_one_legacy_yearly_ensemble_source(
    *,
    source: YearlyEnsembleSource,
    level: str,
    state_name: Optional[str],
) -> pd.DataFrame:
    """Load one optimized yearly-ensemble frame from a legacy yearly source."""
    df = _normalize_legacy_ensemble_df(_read_yearly_csv(source.csv_path))
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["scenario"] = str(source.scenario).strip().lower()
    if level == "district":
        df["district_key"] = _normalized_key(str(state_name or ""), source.name_1)
    else:
        df["block_key"] = _normalized_key(str(state_name or ""), source.name_1, str(source.name_2 or ""))
    return df


def _load_legacy_yearly_ensemble_chunk(
    chunk_index: int,
    sources: tuple[YearlyEnsembleSource, ...],
    *,
    level: str,
    state_name: Optional[str],
) -> tuple[int, pd.DataFrame]:
    """Load one chunk of legacy yearly-ensemble sources in stable order."""
    rows: list[pd.DataFrame] = []
    for source in sources:
        row_df = _load_one_legacy_yearly_ensemble_source(
            source=source,
            level=level,
            state_name=state_name,
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
    progress: BuildProgress,
    label_prefix: str = "ensemble",
    workers: int = 1,
) -> pd.DataFrame:
    """Load optimized yearly-ensemble rows directly from legacy ensemble CSVs."""
    if workers <= 1 or len(sources) <= 1:
        rows: list[pd.DataFrame] = []
        for source in sources:
            row_df = _load_one_legacy_yearly_ensemble_source(
                source=source,
                level=level,
                state_name=state_name,
            )
            if not row_df.empty:
                rows.append(row_df)
        if not rows:
            return pd.DataFrame()
        return _safe_numeric_downcast(pd.concat(rows, ignore_index=True, sort=False))

    progress.start_task(BuildTask(stage="yearly-ensemble", label=f"{label_prefix} | parallel ensemble reads"))
    kind = _yearly_executor_kind()
    source_chunks = _chunk_tuple(sources, chunk_size=_yearly_chunk_size(len(sources), workers, kind))
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
        },
        kind=kind,
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
            if src.suffix.lower() in {".geojson", ".parquet", ".png", ".json"}:
                shutil.copy2(src, dst)
            else:
                df = pd.read_csv(src)
                _write_parquet(_safe_numeric_downcast(df), dst)

        _run_task(task, progress, _copy_one)


def _geometry_tasks(
    *,
    data_dir: Path,
    selected_levels: set[str],
    selected_admin_states: tuple[str, ...] = (),
    include_shared_admin_artifacts: bool = True,
) -> tuple[BuildTask, ...]:
    cfg = get_paths_config()
    tasks: list[BuildTask] = []
    selected_admin_state_set = {str(state).strip() for state in selected_admin_states if str(state).strip()}

    if "district" in selected_levels:
        adm2 = ensure_adm2_columns(gpd.read_file(cfg.districts_path).to_crs(4326))
        district_states = sorted({str(v).strip() for v in adm2["state_name"].astype(str).tolist()})
        if selected_admin_state_set:
            district_states = [state_name for state_name in district_states if state_name in selected_admin_state_set]
        for state_name in district_states:
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
        if include_shared_admin_artifacts:
            tasks.append(
                BuildTask(
                    stage="geometry",
                    label="adm1 state polygons",
                    level="adm1",
                    source_path=Path(cfg.districts_path),
                    target_path=optimized_adm1_path(data_dir=data_dir),
                )
            )

    if "block" in selected_levels:
        adm3 = ensure_adm3_columns(gpd.read_file(cfg.blocks_path).to_crs(4326))
        block_states = sorted({str(v).strip() for v in adm3["state_name"].astype(str).tolist()})
        if selected_admin_state_set:
            block_states = [state_name for state_name in block_states if state_name in selected_admin_state_set]
        for state_name in block_states:
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

    if "block" in selected_levels:
        if include_shared_admin_artifacts:
            tasks.append(
                BuildTask(
                    stage="geometry",
                    label="admin block index",
                    level="admin_block_index",
                    source_path=Path(cfg.blocks_path),
                    target_path=optimized_context_path("admin_block_index.parquet", data_dir=data_dir),
                )
            )

    return tuple(tasks)


def _write_geometry_bundle(*, data_dir: Path, tasks: tuple[BuildTask, ...], progress: BuildProgress) -> None:
    cfg = get_paths_config()

    task_map = {(task.level, task.state, str(task.target_path)): task for task in tasks}

    adm2_for_adm1: Optional[gpd.GeoDataFrame] = None
    if any(task.level == "district" for task in tasks):
        adm2 = gpd.read_file(cfg.districts_path).to_crs(4326)
        adm2 = ensure_adm2_columns(adm2)
        adm2["district_key"] = adm2["state_name"].astype(str).map(alias).str.cat(
            adm2["district_name"].astype(str).map(alias),
            sep="|",
        )
        adm2_for_adm1 = adm2
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

    if any(task.level == "adm1" for task in tasks) and adm2_for_adm1 is not None:
        from india_resilience_tool.data.adm2_loader import build_adm1_from_adm2

        adm1 = build_adm1_from_adm2(adm2_for_adm1, state_col="state_name")
        adm1["geometry"] = adm1["geometry"].simplify(
            tolerance=float(SIMPLIFY_TOL_ADM1), preserve_topology=True
        )
        adm1_keep = [c for c in ("state_name", "shapeName") if c in adm1.columns]
        adm1_out = adm1[[*adm1_keep, "geometry"]].reset_index(drop=True)
        adm1_path = optimized_adm1_path(data_dir=data_dir)
        adm1_task = task_map.get(("adm1", None, str(adm1_path)))
        if adm1_task is not None:
            _run_task(adm1_task, progress, lambda: _write_geojson(adm1_out, adm1_path))

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
            "proposal_bundle": [
                "mean",
                "score",
                "abs_score",
                "chg_score",
                "imp_score",
                "available_rule_count",
                "available_rule_weight_fraction",
            ],
            "static_snapshot": ["mean"],
            "removed": ["std", "p05", "p95", "n_models", "values_per_model", "models"],
        },
        "summaries": _bundle_inventory_summaries(data_dir=data_dir),
        "glance_view_model": glance_manifest_payload(data_dir=data_dir),
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


def _effective_levels(levels: Optional[list[str]], *, states: Optional[list[str]]) -> Optional[list[str]]:
    """Resolve the requested level filter, defaulting scoped state runs to admin."""
    if states and not levels:
        return ["admin"]
    return levels


def _normalized_state_key(value: str) -> str:
    """Return the canonical comparison token for one admin state name."""
    return alias(normalize_name(str(value or "").strip()))


def _resolve_requested_state_names(
    discovered_states: Iterable[str],
    requested_states: Optional[list[str]],
) -> tuple[str, ...]:
    """Resolve user-requested states to discovered legacy state root names."""
    discovered = [str(state).strip() for state in discovered_states if str(state).strip()]
    if not requested_states:
        return tuple(discovered)

    normalized_map: dict[str, list[str]] = {}
    for state in discovered:
        normalized_map.setdefault(_normalized_state_key(state), []).append(state)

    resolved: list[str] = []
    for requested in requested_states:
        token = _normalized_state_key(requested)
        matches = sorted(normalized_map.get(token, []))
        if not matches:
            raise ValueError(
                f"Requested state {requested!r} was not found in discovered legacy roots: {', '.join(sorted(discovered))}"
            )
        if len(matches) > 1:
            raise ValueError(
                f"Requested state {requested!r} is ambiguous across discovered legacy roots: {', '.join(matches)}"
            )
        if matches[0] not in resolved:
            resolved.append(matches[0])
    return tuple(resolved)


def _state_scoped_admin_run(*, states: Optional[list[str]], levels: Optional[list[str]]) -> bool:
    """Return True when the request is scoped to admin state-owned artifacts."""
    if not states:
        return False
    return bool({"district", "block"} & set(_selected_levels(_effective_levels(levels, states=states))))


def _admin_sources_for_glance_slug(slug: str, *, data_dir: Path) -> bool:
    """Return whether a dashboard composite has district master inputs for Glance."""
    optimized_root = resolve_optimized_metric_root(slug, data_dir=data_dir)
    if optimized_root.exists():
        if any(
            path.exists()
            for path in optimized_master_sources_from_metric_root(
                optimized_root,
                level="district",
                selected_state="All",
            )
        ):
            return True

    legacy_root = resolve_processed_root(slug, data_dir=data_dir, mode="portfolio")
    if not legacy_root.exists():
        return False
    master_name = LEGACY_MASTER_FILENAMES["district"]
    return any(_legacy_master_source(state_root / master_name) is not None for state_root in _iter_state_dirs(legacy_root))


def _build_execution_plan(
    *,
    data_dir: Path,
    metrics: Optional[list[str]] = None,
    levels: Optional[list[str]] = None,
    states: Optional[list[str]] = None,
    include_geometry: bool = True,
    include_context: bool = True,
    include_shared_admin_artifacts: bool = True,
) -> BuildPlan:
    summaries_seed: list[MetricBundleSummary] = []
    master_tasks: list[BuildTask] = []
    yearly_model_jobs: list[YearlyModelsJob] = []
    yearly_ensemble_jobs: list[YearlyEnsembleJob] = []
    selected_levels = set(_selected_levels(_effective_levels(levels, states=states)))
    selected_admin_state_names_union: set[str] = set()

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

        discovered_state_roots = tuple(_iter_state_dirs(legacy_root))
        selected_state_names = _resolve_requested_state_names(
            (state_root.name for state_root in discovered_state_roots),
            states,
        )
        selected_admin_state_names_union.update(selected_state_names)
        for state_root in discovered_state_roots:
            if selected_state_names and state_root.name not in set(selected_state_names):
                continue
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

    context_tasks: list[BuildTask] = []
    scoped_admin_run = _state_scoped_admin_run(states=states, levels=levels)
    if include_context and (not scoped_admin_run or include_shared_admin_artifacts):
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
        _geometry_tasks(
            data_dir=data_dir,
            selected_levels=selected_levels,
            selected_admin_states=tuple(sorted(selected_admin_state_names_union)),
            include_shared_admin_artifacts=(not scoped_admin_run or include_shared_admin_artifacts),
        )
        if include_geometry
        else tuple()
    )
    glance_slugs: tuple[str, ...] = ()
    if include_context and "district" in selected_levels and (not scoped_admin_run or include_shared_admin_artifacts):
        selected_metric_set = {str(slug).strip() for slug in metrics or [] if str(slug).strip()}
        dashboard_specs = [
            spec
            for spec in get_dashboard_bundle_specs()
            if spec.show_in_landing
            and (not selected_metric_set or spec.composite_slug in selected_metric_set)
        ]
        glance_slugs = tuple(
            spec.composite_slug
            for spec in dashboard_specs
            if _admin_sources_for_glance_slug(spec.composite_slug, data_dir=data_dir)
        )
    manifest_task = BuildTask(
        stage="manifest",
        label="bundle manifest",
        target_path=None if scoped_admin_run else bundle_manifest_path(data_dir=data_dir),
    )

    return BuildPlan(
        summaries_seed=tuple(summaries_seed),
        master_tasks=tuple(master_tasks),
        yearly_model_jobs=tuple(yearly_model_jobs),
        yearly_ensemble_jobs=tuple(yearly_ensemble_jobs),
        context_tasks=tuple(context_tasks),
        geometry_tasks=tuple(geometry_tasks),
        glance_slugs=glance_slugs,
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
    states: Optional[list[str]],
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
    if full_rebuild and states:
        raise ValueError("--full-rebuild cannot be combined with --state.")

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


def _state_owned_plan_paths(plan: BuildPlan) -> tuple[Path, ...]:
    """Return exact state-owned target paths represented by the execution plan."""
    owned: list[Path] = []
    for task in plan.master_tasks:
        if task.target_path is not None:
            owned.append(task.target_path)
    for job in plan.yearly_model_jobs:
        owned.append(job.models_path)
    for job in plan.yearly_ensemble_jobs:
        owned.append(job.target_path)
    for task in plan.geometry_tasks:
        if task.target_path is None:
            continue
        if task.level in {"district", "block"}:
            owned.append(task.target_path)
    return _iter_unique_paths(owned)


def _delete_owned_paths(paths: Iterable[Path]) -> None:
    for path in paths:
        unlink_file(path)
        parent = path.parent
        while parent.exists() and parent.is_dir() and parent != parent.parent:
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent


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


def _collect_write_targets(
    *,
    plan: BuildPlan,
    data_dir: Path,
    run_audit: bool,
    report_path: Optional[Path] = None,
) -> tuple[Path, ...]:
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
    for slug in plan.glance_slugs:
        for result in build_glance_view_models(
            data_dir=data_dir,
            composite_slugs=[slug],
            overwrite=True,
            dry_run=True,
        ):
            for filename in GLANCE_FILENAMES:
                targets.append(result.output_root / filename)
    if plan.manifest_task.target_path is not None:
        targets.append(plan.manifest_task.target_path)
    if run_audit and report_path is not None:
        targets.append(report_path)
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
    report_path: Optional[Path],
    levels: Optional[list[str]],
) -> None:
    print("PROCESSED OPTIMISED DRY RUN")
    print(f"data_dir: {Path(data_dir).resolve()}")
    print(f"bundle_root: {bundle_root}")
    if full_rebuild:
        print(f"full_rebuild_delete_root: {bundle_root}")
    if plan.manifest_task.target_path is not None:
        print(f"metadata_invalidated: {bundle_manifest_path(data_dir=data_dir)}")
    if report_path is not None:
        print(f"metadata_invalidated: {report_path}")
    if overwrite and prune_scope:
        for path in _state_owned_plan_paths(plan):
            print(f"scope_prune: {path}")
    for path in _collect_write_targets(plan=plan, data_dir=data_dir, run_audit=run_audit, report_path=report_path):
        print(f"write_target: {path}")


def _required_columns_for_master(level: str) -> set[str]:
    level_norm = str(level).strip().lower()
    if level_norm == "district":
        return {"state", "district", "district_key"}
    return {"state", "district", "block", "block_key"}


def _required_columns_for_yearly_models(level: str) -> set[str]:
    key_col = "district_key" if str(level).strip().lower() == "district" else "block_key"
    return {key_col, "scenario", "model", "year", "value"}


def _required_columns_for_yearly_ensemble(level: str) -> set[str]:
    level_norm = str(level).strip().lower()
    if level_norm == "district":
        return {"district_key", "scenario", "year", "mean"}
    return {"block_key", "scenario", "year", "mean"}


def _table_has_required_columns(path: Path, required_columns: set[str]) -> tuple[bool, list[str]]:
    if not path.exists():
        return False, sorted(required_columns)
    try:
        cols = set(read_table(path).columns)
    except Exception:
        return False, sorted(required_columns)
    missing = sorted(required_columns - cols)
    return not missing, missing



def _issue(
    *,
    stage: str,
    slug: str,
    level: str,
    target: Path | str,
    missing_columns: list[str] | None = None,
    severity: str = "error",
    reason: str = "",
) -> dict[str, str | list[str]]:
    """Build one parity issue payload with a stable severity field."""
    payload: dict[str, str | list[str]] = {
        "stage": stage,
        "slug": slug,
        "level": level,
        "target": str(target),
        "missing_columns": missing_columns or [],
        "severity": severity,
    }
    if reason:
        payload["reason"] = reason
    return payload


def _state_names_for_block_yearly_model_audit(
    *,
    data_dir: Path,
    slug: str,
    requested_states: Optional[list[str]],
) -> tuple[str, ...]:
    if requested_states:
        return tuple(str(state).strip() for state in requested_states if str(state).strip())
    level_dir = resolve_optimized_metric_root(slug, data_dir=data_dir) / "yearly_ensemble" / "admin" / "block"
    return tuple(sorted(path.stem.removeprefix("state=") for path in level_dir.glob("state=*.parquet")))

def audit_processed_optimised_parity(
    *,
    data_dir: Path,
    metrics: Optional[list[str]] = None,
    levels: Optional[list[str]] = None,
    states: Optional[list[str]] = None,
    include_geometry: bool = True,
    include_context: bool = True,
    include_shared_admin_artifacts: bool = True,
    write_report: bool = True,
    report_path: Optional[Path] = None,
    require_block_yearly_models: bool = False,
) -> dict:
    """Validate that optimized artifacts exist for every dashboard-visible legacy source."""
    effective_levels = _effective_levels(levels, states=states)
    scoped_admin_run = _state_scoped_admin_run(states=states, levels=effective_levels)
    plan = _build_execution_plan(
        data_dir=data_dir,
        metrics=metrics,
        levels=effective_levels,
        states=states,
        include_geometry=include_geometry,
        include_context=include_context,
        include_shared_admin_artifacts=include_shared_admin_artifacts,
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

    for slug in plan.glance_slugs:
        for result in build_glance_view_models(
            data_dir=data_dir,
            composite_slugs=[slug],
            overwrite=True,
            dry_run=True,
        ):
            for filename in GLANCE_FILENAMES:
                path = result.output_root / filename
                ok, missing_cols = _table_has_required_columns(path, GLANCE_REQUIRED_COLUMNS[filename])
                if not ok:
                    issues.append(
                        {
                            "stage": "glance",
                            "slug": slug,
                            "level": "district",
                            "target": str(path),
                            "missing_columns": missing_cols,
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

    if plan.manifest_task.target_path is not None and not plan.manifest_task.target_path.exists():
        issues.append(
            {
                "stage": "manifest",
                "slug": "",
                "level": "",
                "target": str(plan.manifest_task.target_path or bundle_manifest_path(data_dir=data_dir)),
                "missing_columns": [],
            }
        )


    # Non-fatal presence check: the precomputed area-weighted state-values table
    # is an optional read-path accelerator (the app falls back to live
    # computation), so its absence is a warning, not a publish-blocking error.
    _state_values_seen: set[tuple[str, str]] = set()
    for task in plan.master_tasks:
        slug = str(task.slug or "").strip()
        level = str(task.level or "").strip().lower()
        if not slug or level not in {"district", "block"}:
            continue
        if (slug, level) in _state_values_seen:
            continue
        _state_values_seen.add((slug, level))
        state_values_target = optimized_state_values_path(slug, level=level, data_dir=data_dir)
        if not state_values_target.exists():
            issues.append(
                _issue(
                    stage="state-values",
                    slug=slug,
                    level=level,
                    target=state_values_target,
                    severity="warning",
                    reason="precomputed_state_values_missing",
                )
            )

    if require_block_yearly_models and "block" in set(_selected_levels(effective_levels)):
        for slug in _selected_slugs(metrics):
            for state_name in _state_names_for_block_yearly_model_audit(
                data_dir=data_dir,
                slug=slug,
                requested_states=states,
            ):
                ensemble_path = optimized_yearly_ensemble_path(
                    slug,
                    level="block",
                    state=state_name,
                    data_dir=data_dir,
                )
                models_path = optimized_yearly_models_path(
                    slug,
                    level="block",
                    state=state_name,
                    data_dir=data_dir,
                )
                if ensemble_path.exists() and not models_path.exists():
                    issues.append(
                        _issue(
                            stage="yearly-models",
                            slug=slug,
                            level="block",
                            target=models_path,
                            severity="error",
                            reason="block_yearly_ensemble_without_yearly_models",
                        )
                    )

    for issue in issues:
        issue.setdefault("severity", "error")

    report = {
        "bundle_root": str(bundle_root),
        "metrics_considered": len(plan.summaries_seed),
        "expected_master_outputs": len(plan.master_tasks),
        "expected_yearly_model_outputs": len(plan.yearly_model_jobs),
        "expected_yearly_ensemble_outputs": len(plan.yearly_ensemble_jobs),
        "expected_context_outputs": len(plan.context_tasks),
        "expected_geometry_outputs": len(plan.geometry_tasks),
        "expected_glance_outputs": len(plan.glance_slugs) * len(GLANCE_FILENAMES),
        "issue_count": len(issues),
        "issues": issues,
    }

    report_target = report_path
    if report_target is None and write_report and not scoped_admin_run:
        report_target = bundle_root / "parity_report.json"
    if report_target is not None:
        report_target.parent.mkdir(parents=True, exist_ok=True)
        report_target.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    return report


def build_processed_optimised_bundle(
    *,
    data_dir: Path,
    metrics: Optional[list[str]] = None,
    levels: Optional[list[str]] = None,
    states: Optional[list[str]] = None,
    workers: Optional[int] = None,
    overwrite: bool = False,
    prune_scope: bool = False,
    full_rebuild: bool = False,
    dry_run: bool = False,
    include_geometry: bool = True,
    include_context: bool = True,
    include_shared_admin_artifacts: bool = True,
    show_progress: Optional[bool] = None,
    run_audit: bool = True,
    report_path: Optional[Path] = None,
) -> list[MetricBundleSummary]:
    """
    Build the optimized runtime bundle from the current legacy processed tree.
    """
    effective_levels = _effective_levels(levels, states=states)
    scoped_admin_run = _state_scoped_admin_run(states=states, levels=effective_levels)
    plan = _build_execution_plan(
        data_dir=data_dir,
        metrics=metrics,
        levels=effective_levels,
        states=states,
        include_geometry=include_geometry,
        include_context=include_context,
        include_shared_admin_artifacts=include_shared_admin_artifacts,
    )
    _validate_build_request(
        data_dir=data_dir,
        plan=plan,
        metrics=metrics,
        levels=effective_levels,
        states=states,
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
            report_path=report_path,
            levels=effective_levels,
        )
        return [MetricBundleSummary(**payload) for payload in summaries_map.values()]

    if full_rebuild:
        _validate_full_rebuild_root(bundle_root=bundle_root, data_dir=data_dir)
        remove_tree(bundle_root)
    elif not scoped_admin_run:
        _invalidate_bundle_metadata(data_dir=data_dir)
        if overwrite and prune_scope:
            _delete_owned_paths(_state_owned_plan_paths(plan))
    elif overwrite and prune_scope:
        _delete_owned_paths(_state_owned_plan_paths(plan))

    progress = BuildProgress(plan, enabled=_progress_enabled(show_progress))
    progress.print_plan_summary()
    resolved_workers = resolve_build_workers(workers)

    try:
        roster_violations: dict = {}
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
                out = _select_master_columns(df, slug=slug, level=str(task.level), supported_stats=supported_stats)
                if out.empty:
                    return
                frame, offenders = _check_canonical_roster(out, slug=slug, level=str(task.level))
                if offenders:
                    roster_violations.setdefault(slug, []).extend(offenders)
                if frame is None or frame.empty:
                    return
                _write_parquet(frame, target)
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
            label_prefix = f"{job.slug} | {job.state} | {job.level}"
            ensemble_df = _load_legacy_yearly_ensemble(
                level=job.level,
                state_name=job.state,
                sources=job.sources,
                progress=progress,
                label_prefix=label_prefix,
                workers=resolved_workers,
            )

            ensemble_task = BuildTask(
                stage="yearly-ensemble",
                label=f"{job.slug} | {job.state} | {job.level} | ensemble parquet",
                slug=job.slug,
                state=job.state,
                level=job.level,
                target_path=job.target_path,
            )

            def _write_ensemble() -> None:
                if ensemble_df.empty:
                    return
                _write_parquet(_safe_numeric_downcast(ensemble_df), job.target_path)
                summaries_map[job.slug]["wrote_yearly_ensemble"] = True

            _run_task(ensemble_task, progress, _write_ensemble)

        if include_context:
            _copy_context_artifacts(tasks=plan.context_tasks, progress=progress)
            for slug in plan.glance_slugs:
                task = BuildTask(stage="glance", label=f"glance | {slug}", slug=slug)

                def _write_glance(slug=slug) -> None:
                    build_glance_view_models(
                        data_dir=data_dir,
                        composite_slugs=[slug],
                        overwrite=overwrite,
                        dry_run=False,
                    )

                _run_task(task, progress, _write_glance)
        if include_geometry:
            _write_geometry_bundle(data_dir=data_dir, tasks=plan.geometry_tasks, progress=progress)

        summaries = [MetricBundleSummary(**payload) for payload in summaries_map.values()]
        if plan.manifest_task.target_path is not None:
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
                levels=effective_levels,
                states=states,
                include_geometry=include_geometry,
                include_context=include_context,
                include_shared_admin_artifacts=include_shared_admin_artifacts,
                write_report=not scoped_admin_run and report_path is None,
                report_path=report_path,
                require_block_yearly_models=False,
            )
            print(
                "PARITY AUDIT "
                f"(metrics={parity['metrics_considered']}, issues={parity['issue_count']}, "
                f"report={report_path or (resolve_optimized_bundle_root(data_dir=data_dir) / 'parity_report.json' if not scoped_admin_run else 'not-written')})"
            )
        if roster_violations and _roster_gate_mode() == "strict":
            raise CanonicalRosterError(_roster_violations_report(roster_violations))
        return summaries
    except Exception:
        progress.close()
        print(progress.failure_summary(), file=sys.stderr)
        raise


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the processed_optimised runtime bundle.")
    parser.add_argument("--metric", action="append", dest="metrics", help="One metric slug to include. Repeatable.")
    parser.add_argument("--state", action="append", dest="states", help="One admin state to include. Repeatable.")
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
    parser.add_argument(
        "--include-shared-admin-artifacts",
        action="store_true",
        help="With --state, also rebuild shared-global admin artifacts such as adm1, admin_block_index, and Glance.",
    )
    parser.add_argument("--skip-audit", action="store_true", help="Skip the post-build parity audit.")
    parser.add_argument(
        "--report-path",
        type=Path,
        help="Explicit parity report output path. Scoped --state runs leave the global report untouched unless this is provided.",
    )
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
        states=args.states,
        workers=args.workers,
        overwrite=bool(args.overwrite),
        prune_scope=bool(args.prune_scope),
        full_rebuild=bool(args.full_rebuild),
        dry_run=bool(args.dry_run),
        include_geometry=not bool(args.skip_geometry),
        include_context=not bool(args.skip_context),
        include_shared_admin_artifacts=bool(args.include_shared_admin_artifacts),
        show_progress=False if bool(args.no_progress) else None,
        run_audit=not bool(args.skip_audit),
        report_path=args.report_path.expanduser().resolve() if args.report_path else None,
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

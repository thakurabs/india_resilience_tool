#!/usr/bin/env python3
"""
Canonical dashboard-ready workflow runner for IRT data prep.

This is the primary operator entrypoint. It orchestrates the lower-level
pipeline and geodata tools so users can prepare one bundle, one metric, or the
full dashboard without memorizing internal commands.

The runner is non-destructive by default:
- existing outputs are not forcibly deleted unless `--overwrite` is supplied
- climate runs default to `--level all`
- climate runs resolve live metrics per requested level (`admin` vs `hydro`)
- climate compute uses validated completion markers and `--skip-existing` by
  default unless `--overwrite` is supplied
- climate, Aqueduct, population, and groundwater flows can refresh
  `processed_optimised` and then audit parity/readiness
- climate `--audit-only` and normal execution both return non-zero when the
  requested readiness state is still incomplete

Examples:
    python -m tools.runs.prepare_dashboard --help
    python -m tools.runs.prepare_dashboard climate-hazards
    python -m tools.runs.prepare_dashboard climate-hazards --level hydro
    python -m tools.runs.prepare_dashboard climate-hazards --metrics tas_annual_mean
    python -m tools.runs.prepare_dashboard climate-hazards --level hydro --metrics r95ptot_contribution_pct --models CanESM5 --scenarios historical
    python -m tools.runs.prepare_dashboard climate-hazards --plan-only
    python -m tools.runs.prepare_dashboard climate-hazards --audit-only
    python -m tools.runs.prepare_dashboard climate-hazards --overwrite
    python -m tools.runs.prepare_dashboard climate-hazards --skip-optimised
    python -m tools.runs.prepare_dashboard aqueduct
    python -m tools.runs.prepare_dashboard dashboard-package --include-pytest
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence


DEFAULT_ADMIN_STATE = "Telangana"
DEFAULT_VALIDATION_TESTS = [
    "tests/test_build_blocks_geojson.py",
    "tests/test_prepare_aqueduct_baseline.py",
    "tests/test_aqueduct_admin_transfer.py",
    "tests/test_aqueduct_hydro_transfer.py",
    "tests/test_groundwater_district_masters.py",
    "tests/test_population_admin_masters.py",
    "tests/test_validate_aqueduct_workflow.py",
    "tests/test_metrics_registry.py",
    "tests/test_config.py",
    "tests/test_available_states.py",
    "tests/test_crosswalk_generator.py",
]
CLIMATE_PILLAR = "Climate Hazards"
AQUEDUCT_DOMAIN = "Aqueduct Water Risk"
POPULATION_DOMAIN = "Population Exposure"
GROUNDWATER_DOMAIN = "Groundwater Status & Availability"
LEVEL_GROUPS = {
    "all": ["district", "block", "basin", "sub_basin"],
    "admin": ["district", "block"],
    "hydro": ["basin", "sub_basin"],
    "district": ["district"],
    "block": ["block"],
    "basin": ["basin"],
    "sub_basin": ["sub_basin"],
}
LEVEL_TO_FAMILY = {
    "district": "admin",
    "block": "admin",
    "basin": "hydro",
    "sub_basin": "hydro",
}
LEGACY_MASTER_FILENAMES = {
    "district": "master_metrics_by_district.csv",
    "block": "master_metrics_by_block.csv",
    "basin": "master_metrics_by_basin.csv",
    "sub_basin": "master_metrics_by_sub_basin.csv",
}
MASTER_REQUIRED_COLUMNS = {
    "district": {"state", "district"},
    "block": {"state", "district", "block"},
    "basin": {"basin_id", "basin_name"},
    "sub_basin": {"basin_id", "basin_name", "subbasin_id", "subbasin_name"},
}


@dataclass(frozen=True)
class PlannedCommand:
    """One concrete command that the runner can execute."""

    label: str
    argv: list[str]


@dataclass(frozen=True)
class BundleRuntimeScope:
    """Resolved metric scope and readiness state for a bundle plan."""

    selected_metrics: list[str]
    pending_metrics: list[str]
    has_global_issues: bool

    @property
    def runtime_needed(self) -> bool:
        return bool(self.pending_metrics or self.has_global_issues)


@dataclass(frozen=True)
class ClimateLevelReadiness:
    """Readiness state for one climate level across requested scopes."""

    level: str
    selected_metrics: tuple[str, ...]
    runnable_metrics: tuple[str, ...]
    compute_pending_metrics: tuple[str, ...]
    masters_pending_metrics: tuple[str, ...]
    optimized_pending_metrics: tuple[str, ...]
    complete_metrics: tuple[str, ...]
    unrunnable_metrics: tuple[str, ...]
    unrunnable_reasons_by_metric: dict[str, tuple[str, ...]]
    unsupported_requested_metrics: tuple[str, ...] = ()

    @property
    def pending_metrics(self) -> tuple[str, ...]:
        return tuple(
            _dedupe_keep_order(
                list(self.compute_pending_metrics)
                + list(self.masters_pending_metrics)
                + list(self.optimized_pending_metrics)
                + list(self.unrunnable_metrics)
            )
        )


@dataclass(frozen=True)
class ClimateRuntimeScope:
    """Stage-aware climate readiness grouped by requested levels."""

    levels: tuple[str, ...]
    by_level: dict[str, ClimateLevelReadiness]
    global_issues: tuple[dict[str, Any], ...] = ()

    @property
    def selected_metrics(self) -> list[str]:
        metrics: list[str] = []
        for level in self.levels:
            readiness = self.by_level.get(level)
            if readiness is not None:
                metrics.extend(readiness.selected_metrics)
        return _dedupe_keep_order(metrics)

    @property
    def pending_metrics(self) -> list[str]:
        metrics: list[str] = []
        for level in self.levels:
            readiness = self.by_level.get(level)
            if readiness is not None:
                metrics.extend(readiness.pending_metrics)
        return _dedupe_keep_order(metrics)

    @property
    def has_global_issues(self) -> bool:
        return bool(self.global_issues)

    @property
    def runtime_needed(self) -> bool:
        return bool(self.pending_metrics or self.global_issues)


@dataclass(frozen=True)
class ClimatePostRunStatus:
    """Post-run blocking vs informational readiness for the executed stage set."""

    blocking: bool
    informational_pending: bool
    informational_messages: tuple[str, ...] = ()


def _py_module_cmd(module: str) -> list[str]:
    return [sys.executable, "-m", module]


def _dedupe_keep_order(items: Sequence[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _split_csv_values(values: Sequence[str] | None) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    for raw in values:
        for part in str(raw).split(","):
            val = part.strip()
            if val:
                out.append(val)
    return _dedupe_keep_order(out)


def _append_flag(argv: list[str], flag: str, enabled: bool) -> None:
    if enabled:
        argv.append(flag)


def _append_multi(argv: list[str], flag: str, values: Sequence[str] | None) -> None:
    vals = _split_csv_values(values)
    if vals:
        argv.extend([flag, *vals])


def _append_repeat(argv: list[str], flag: str, values: Sequence[str] | None) -> None:
    for value in _split_csv_values(values):
        argv.extend([flag, value])


def _resolve_levels(level: str) -> list[str]:
    try:
        return list(LEVEL_GROUPS[level])
    except KeyError as exc:
        raise SystemExit(f"Unsupported level selection: {level}") from exc


def _resolve_admin_states(state_args: Sequence[str] | None) -> list[str]:
    states = _split_csv_values(state_args)
    return states or [DEFAULT_ADMIN_STATE]


def _should_prune_completed_metrics(args: argparse.Namespace) -> bool:
    return not (
        bool(getattr(args, "overwrite", False))
        or bool(getattr(args, "audit_only", False))
        or bool(_split_csv_values(getattr(args, "models", None)))
        or bool(_split_csv_values(getattr(args, "scenarios", None)))
    )


def _metrics_for_domain(domain: str) -> list[str]:
    from india_resilience_tool.config.metrics_registry import get_metrics_for_domain

    return list(get_metrics_for_domain(domain))


def _scope_names_for_level(level: str, admin_states: Sequence[str]) -> tuple[str, ...]:
    if level in {"district", "block"}:
        return tuple(admin_states)
    return ("hydro",)


def _resolve_climate_metrics_for_level(
    args: argparse.Namespace,
    *,
    level: str,
) -> tuple[list[str], list[str]]:
    from india_resilience_tool.config.metrics_registry import (
        get_domains_for_pillar,
        get_metrics_for_domain,
    )

    family = LEVEL_TO_FAMILY[level]
    metrics: list[str] = []
    for domain in get_domains_for_pillar(CLIMATE_PILLAR, spatial_family=family, level=level):
        metrics.extend(get_metrics_for_domain(domain, spatial_family=family, level=level))
    live_metrics = _dedupe_keep_order(metrics)

    explicit = _split_csv_values(getattr(args, "metrics", None))
    if not explicit:
        return live_metrics, []

    selected = [metric for metric in live_metrics if metric in set(explicit)]
    unsupported = [metric for metric in explicit if metric not in set(live_metrics)]
    return _dedupe_keep_order(selected), _dedupe_keep_order(unsupported)


def _resolve_climate_bundle_metrics(args: argparse.Namespace) -> list[str]:
    metrics: list[str] = []
    for level in _resolve_levels(str(getattr(args, "level", "all"))):
        level_metrics, _ = _resolve_climate_metrics_for_level(args, level=level)
        metrics.extend(level_metrics)
    return _dedupe_keep_order(metrics)


def _resolve_bundle_metrics(bundle: str, args: argparse.Namespace) -> list[str]:
    if bundle == "climate-hazards":
        return _resolve_climate_bundle_metrics(args)
    if bundle == "aqueduct":
        explicit = _split_csv_values(getattr(args, "metric_slug", None))
        return explicit or _metrics_for_domain(AQUEDUCT_DOMAIN)
    if bundle == "population-exposure":
        return _metrics_for_domain(POPULATION_DOMAIN)
    if bundle == "groundwater":
        return _metrics_for_domain(GROUNDWATER_DOMAIN)
    if bundle == "dashboard-package":
        metrics: list[str] = []
        metrics.extend(_resolve_climate_bundle_metrics(args))
        metrics.extend(_split_csv_values(getattr(args, "metric_slug", None)) or _metrics_for_domain(AQUEDUCT_DOMAIN))
        metrics.extend(_metrics_for_domain(POPULATION_DOMAIN))
        metrics.extend(_metrics_for_domain(GROUNDWATER_DOMAIN))
        return _dedupe_keep_order(metrics)
    return []


def _issue_relevant_to_levels(issue: dict[str, Any], levels: Sequence[str] | None) -> bool:
    if not levels:
        return True
    issue_level = str(issue.get("level") or "").strip()
    if not issue_level:
        return True
    return issue_level in set(levels)


def _legacy_master_path(*, slug: str, level: str, scope_name: str, data_dir: Path) -> Path:
    from india_resilience_tool.config.paths import resolve_processed_root

    root = resolve_processed_root(slug, data_dir=data_dir, mode="portfolio")
    if level in {"district", "block"}:
        return root / scope_name / LEGACY_MASTER_FILENAMES[level]
    return root / "hydro" / LEGACY_MASTER_FILENAMES[level]


def _legacy_master_ready(*, slug: str, level: str, scope_name: str, data_dir: Path) -> bool:
    from india_resilience_tool.utils.processed_io import read_table

    path = _legacy_master_path(slug=slug, level=level, scope_name=scope_name, data_dir=data_dir)
    if not path.exists():
        return False
    try:
        df = read_table(path)
    except Exception:
        return False
    required = MASTER_REQUIRED_COLUMNS[level]
    return (not df.empty) and required.issubset(set(df.columns))


def _resolve_climate_runtime_scope(
    args: argparse.Namespace,
    *,
    levels: Sequence[str],
) -> ClimateRuntimeScope:
    from india_resilience_tool.config.paths import get_paths_config
    from tools.optimized.build_processed_optimised import audit_processed_optimised_parity
    from tools.pipeline.compute_indices_multiprocess import (
        build_processing_task_plan,
        ensemble_completion_marker_valid,
        task_completion_marker_valid,
    )

    data_dir = get_paths_config().data_dir
    admin_states = _resolve_admin_states(getattr(args, "state", None))
    selected_by_level: dict[str, list[str]] = {}
    unsupported_by_level: dict[str, list[str]] = {}
    all_selected: list[str] = []
    for level in levels:
        selected, unsupported = _resolve_climate_metrics_for_level(args, level=level)
        selected_by_level[level] = selected
        unsupported_by_level[level] = unsupported
        all_selected.extend(selected)

    union_selected = _dedupe_keep_order(all_selected)
    parity = audit_processed_optimised_parity(
        data_dir=data_dir,
        metrics=union_selected or None,
        levels=list(levels),
        include_geometry=True,
        include_context=True,
        write_report=False,
    )
    parity_issues = list(parity.get("issues", []))
    global_issues = tuple(issue for issue in parity_issues if not str(issue.get("slug") or "").strip())

    by_level: dict[str, ClimateLevelReadiness] = {}
    for level in levels:
        selected_metrics = selected_by_level[level]
        if not selected_metrics and not unsupported_by_level[level]:
            by_level[level] = ClimateLevelReadiness(
                level=level,
                selected_metrics=(),
                runnable_metrics=(),
                compute_pending_metrics=(),
                masters_pending_metrics=(),
                optimized_pending_metrics=(),
                complete_metrics=(),
                unrunnable_metrics=(),
                unrunnable_reasons_by_metric={},
            )
            continue

        scope_names = _scope_names_for_level(level, admin_states)
        scope_presence: dict[str, dict[str, bool]] = {metric: {} for metric in selected_metrics}
        scope_compute_pending: dict[str, dict[str, bool]] = {metric: {} for metric in selected_metrics}
        reason_map: dict[str, set[str]] = {metric: set() for metric in selected_metrics}

        for scope_name in scope_names:
            task_plan = build_processing_task_plan(
                metrics_filter=selected_metrics,
                models_filter=_split_csv_values(getattr(args, "models", None)) or None,
                scenarios_filter=_split_csv_values(getattr(args, "scenarios", None)) or None,
                level=level,
                state=scope_name,
            )
            tasks_by_metric: dict[str, list[Any]] = {}
            for task in task_plan.tasks:
                tasks_by_metric.setdefault(task.slug, []).append(task)

            for metric in selected_metrics:
                metric_tasks = tasks_by_metric.get(metric, [])
                has_tasks = bool(metric_tasks)
                scope_presence[metric][scope_name] = has_tasks
                if not has_tasks:
                    reasons = task_plan.skipped_reasons_by_metric.get(metric, ("no_tasks_after_filters",))
                    for reason in reasons:
                        if len(scope_names) == 1:
                            reason_map[metric].add(str(reason))
                        else:
                            reason_map[metric].add(f"{scope_name}:{reason}")
                    continue

                task_pending = any(not task_completion_marker_valid(task) for task in metric_tasks)
                ensemble_pending = not ensemble_completion_marker_valid(
                    slug=metric,
                    level=level,
                    scope_name=scope_name,
                    allowed_models=_split_csv_values(getattr(args, "models", None)) or None,
                    allowed_scenarios=_split_csv_values(getattr(args, "scenarios", None)) or None,
                )
                scope_compute_pending[metric][scope_name] = task_pending or ensemble_pending

        parity_metric_issues = {
            str(issue.get("slug")).strip()
            for issue in parity_issues
            if str(issue.get("slug") or "").strip() and str(issue.get("level") or "").strip() == level
        }

        runnable_metrics: list[str] = []
        compute_pending_metrics: list[str] = []
        masters_pending_metrics: list[str] = []
        optimized_pending_metrics: list[str] = []
        complete_metrics: list[str] = []
        unrunnable_metrics: list[str] = list(unsupported_by_level[level])
        for metric in unsupported_by_level[level]:
            reason_map.setdefault(metric, set()).add("unsupported_for_level")

        for metric in selected_metrics:
            if any(not scope_presence[metric].get(scope_name, False) for scope_name in scope_names):
                unrunnable_metrics.append(metric)
                continue

            runnable_metrics.append(metric)
            compute_pending = any(scope_compute_pending[metric].get(scope_name, False) for scope_name in scope_names)
            if compute_pending:
                compute_pending_metrics.append(metric)

            master_ready = all(
                _legacy_master_ready(
                    slug=metric,
                    level=level,
                    scope_name=scope_name,
                    data_dir=data_dir,
                )
                for scope_name in scope_names
            )
            master_pending = compute_pending or not master_ready
            if master_pending:
                masters_pending_metrics.append(metric)

            optimized_pending = master_pending or metric in parity_metric_issues
            if optimized_pending:
                optimized_pending_metrics.append(metric)

            if not compute_pending and not master_pending and not optimized_pending:
                complete_metrics.append(metric)

        by_level[level] = ClimateLevelReadiness(
            level=level,
            selected_metrics=tuple(_dedupe_keep_order(selected_metrics)),
            runnable_metrics=tuple(_dedupe_keep_order(runnable_metrics)),
            compute_pending_metrics=tuple(_dedupe_keep_order(compute_pending_metrics)),
            masters_pending_metrics=tuple(_dedupe_keep_order(masters_pending_metrics)),
            optimized_pending_metrics=tuple(_dedupe_keep_order(optimized_pending_metrics)),
            complete_metrics=tuple(_dedupe_keep_order(complete_metrics)),
            unrunnable_metrics=tuple(_dedupe_keep_order(unrunnable_metrics)),
            unrunnable_reasons_by_metric={
                metric: tuple(sorted(reasons))
                for metric, reasons in sorted(reason_map.items())
                if reasons
            },
            unsupported_requested_metrics=tuple(_dedupe_keep_order(unsupported_by_level[level])),
        )

    return ClimateRuntimeScope(levels=tuple(levels), by_level=by_level, global_issues=global_issues)


def _print_climate_readiness(scope: ClimateRuntimeScope) -> None:
    print("CLIMATE READINESS")
    for level in scope.levels:
        readiness = scope.by_level[level]
        print(
            f"- {level}: selected={len(readiness.selected_metrics)} "
            f"runnable={len(readiness.runnable_metrics)} "
            f"compute_pending={len(readiness.compute_pending_metrics)} "
            f"masters_pending={len(readiness.masters_pending_metrics)} "
            f"optimized_pending={len(readiness.optimized_pending_metrics)} "
            f"complete={len(readiness.complete_metrics)} "
            f"unrunnable={len(readiness.unrunnable_metrics)}"
        )
        if readiness.unrunnable_reasons_by_metric:
            for metric, reasons in sorted(readiness.unrunnable_reasons_by_metric.items()):
                print(f"  unrunnable {metric}: {', '.join(reasons)}")
    if scope.global_issues:
        print(f"- global_issues={len(scope.global_issues)}")


def _climate_scope_is_ready(scope: ClimateRuntimeScope) -> bool:
    if scope.global_issues:
        return False
    for readiness in scope.by_level.values():
        if (
            readiness.compute_pending_metrics
            or readiness.masters_pending_metrics
            or readiness.optimized_pending_metrics
            or readiness.unrunnable_metrics
        ):
            return False
    return True


def _evaluate_climate_post_run_status(
    scope: ClimateRuntimeScope,
    *,
    require_compute: bool,
    require_masters: bool,
    require_optimized: bool,
    require_audit: bool,
) -> ClimatePostRunStatus:
    """Return blocking vs informational pending state for one climate run."""
    informational: list[str] = []
    blocking = False

    if scope.global_issues:
        if require_audit:
            blocking = True
        else:
            informational.append(
                f"informational global_issues={len(scope.global_issues)} (audit stage was skipped)"
            )

    for level in scope.levels:
        readiness = scope.by_level.get(level)
        if readiness is None:
            continue

        if readiness.compute_pending_metrics or readiness.unrunnable_metrics:
            if require_compute:
                blocking = True
            else:
                informational.append(
                    f"informational {level}: compute_pending={len(readiness.compute_pending_metrics)} "
                    f"unrunnable={len(readiness.unrunnable_metrics)} (compute stage was skipped)"
                )

        if readiness.masters_pending_metrics and not require_masters:
            informational.append(
                f"informational {level}: masters_pending={len(readiness.masters_pending_metrics)} "
                f"(--skip-masters)"
            )
        elif readiness.masters_pending_metrics:
            blocking = True

        if readiness.optimized_pending_metrics and not require_optimized:
            informational.append(
                f"informational {level}: optimized_pending={len(readiness.optimized_pending_metrics)} "
                f"(--skip-optimised)"
            )
        elif readiness.optimized_pending_metrics:
            blocking = True

    deduped = tuple(_dedupe_keep_order(informational))
    return ClimatePostRunStatus(
        blocking=blocking,
        informational_pending=bool(deduped),
        informational_messages=deduped,
    )


def _resolve_runtime_scope(
    bundle: str,
    args: argparse.Namespace,
    *,
    levels: Sequence[str] | None = None,
) -> BundleRuntimeScope:
    selected_metrics = _resolve_bundle_metrics(bundle, args)
    if not selected_metrics:
        return BundleRuntimeScope(selected_metrics=[], pending_metrics=[], has_global_issues=False)

    if not _should_prune_completed_metrics(args):
        return BundleRuntimeScope(
            selected_metrics=selected_metrics,
            pending_metrics=selected_metrics,
            has_global_issues=False,
        )

    try:
        from india_resilience_tool.config.paths import get_paths_config
        from tools.optimized.build_processed_optimised import audit_processed_optimised_parity
    except Exception:
        return BundleRuntimeScope(
            selected_metrics=selected_metrics,
            pending_metrics=selected_metrics,
            has_global_issues=False,
        )

    report = audit_processed_optimised_parity(
        data_dir=get_paths_config().data_dir,
        metrics=selected_metrics,
        include_geometry=True,
        include_context=True,
        write_report=False,
    )
    relevant_issues = [
        issue
        for issue in report.get("issues", [])
        if _issue_relevant_to_levels(issue, levels)
    ]
    pending_metrics = _dedupe_keep_order(
        [str(issue.get("slug") or "").strip() for issue in relevant_issues if str(issue.get("slug") or "").strip()]
    )
    has_global_issues = any(not str(issue.get("slug") or "").strip() for issue in relevant_issues)
    return BundleRuntimeScope(
        selected_metrics=selected_metrics,
        pending_metrics=pending_metrics,
        has_global_issues=has_global_issues,
    )


def _select_metrics_for_execution(scope: BundleRuntimeScope) -> list[str]:
    if scope.pending_metrics:
        return scope.pending_metrics
    if scope.has_global_issues:
        return scope.selected_metrics
    return []


def _build_optimised_step(
    args: argparse.Namespace,
    metrics: Sequence[str] | None,
    *,
    levels: Sequence[str] | None = None,
    label: str = "processed-optimised-build",
) -> PlannedCommand:
    argv = _py_module_cmd("tools.optimized.build_processed_optimised")
    _append_repeat(argv, "--metric", metrics)
    _append_repeat(argv, "--level", levels)
    _append_flag(argv, "--overwrite", bool(args.overwrite))
    argv.append("--skip-audit")
    return PlannedCommand(label=label, argv=argv)


def _build_audit_step(
    args: argparse.Namespace,
    metrics: Sequence[str] | None,
    *,
    levels: Sequence[str] | None = None,
    label: str = "processed-optimised-audit",
) -> PlannedCommand:
    argv = _py_module_cmd("tools.optimized.audit_processed_optimised_parity")
    _append_repeat(argv, "--metric", metrics)
    _append_repeat(argv, "--level", levels)
    return PlannedCommand(label=label, argv=argv)


def _build_runtime_plan(
    args: argparse.Namespace,
    *,
    scope: BundleRuntimeScope,
    allow_optimised: bool = True,
) -> list[PlannedCommand]:
    if bool(getattr(args, "audit_only", False)):
        return [] if bool(getattr(args, "skip_audit", False)) else [_build_audit_step(args, scope.selected_metrics)]

    plan: list[PlannedCommand] = []
    if allow_optimised and not bool(getattr(args, "skip_optimised", False)) and scope.runtime_needed:
        plan.append(_build_optimised_step(args, _select_metrics_for_execution(scope)))
    if not bool(getattr(args, "skip_audit", False)):
        plan.append(_build_audit_step(args, scope.selected_metrics))
    return plan


def _build_prepare_aqueduct_baseline_step(args: argparse.Namespace) -> PlannedCommand:
    if not args.source_gdb or not args.baseline_csv:
        raise SystemExit(
            "Aqueduct baseline preparation requires both --source-gdb and --baseline-csv."
        )
    argv = _py_module_cmd("tools.geodata.prepare_aqueduct_baseline")
    argv.extend(["--source-gdb", str(args.source_gdb), "--baseline-csv", str(args.baseline_csv)])
    _append_flag(argv, "--overwrite", bool(args.overwrite))
    return PlannedCommand(label="aqueduct-baseline", argv=argv)


def _build_aqueduct_metric_args(args: argparse.Namespace) -> list[str]:
    metric_slugs = _split_csv_values(getattr(args, "metric_slug", None))
    argv: list[str] = []
    for slug in metric_slugs:
        argv.extend(["--metric-slug", slug])
    return argv


def build_blocks_geojson_plan(args: argparse.Namespace) -> list[PlannedCommand]:
    """Build the canonical block-boundary refresh step."""
    argv = _py_module_cmd("tools.geodata.build_blocks_geojson")
    _append_flag(argv, "--overwrite", bool(args.overwrite))
    return [PlannedCommand(label="blocks-geojson", argv=argv)]


def build_aqueduct_plan(
    args: argparse.Namespace,
    *,
    include_blocks_geojson: bool = True,
    include_runtime: bool = True,
    runtime_scope: Optional[BundleRuntimeScope] = None,
) -> list[PlannedCommand]:
    scope = runtime_scope or _resolve_runtime_scope("aqueduct", args)
    plan: list[PlannedCommand] = []

    if not bool(getattr(args, "audit_only", False)) and (
        bool(args.overwrite) or scope.runtime_needed or not include_runtime
    ):
        if include_blocks_geojson:
            plan.extend(build_blocks_geojson_plan(args))
        if bool(getattr(args, "prepare_baseline", False)):
            plan.append(_build_prepare_aqueduct_baseline_step(args))

        for label, module in [
            ("aqueduct-admin-crosswalk", "tools.geodata.build_aqueduct_admin_crosswalk"),
            ("aqueduct-block-crosswalk", "tools.geodata.build_aqueduct_block_crosswalk"),
            ("aqueduct-hydro-crosswalk", "tools.geodata.build_aqueduct_hydro_crosswalk"),
        ]:
            argv = _py_module_cmd(module)
            _append_flag(argv, "--overwrite", bool(args.overwrite))
            plan.append(PlannedCommand(label=label, argv=argv))

        admin_argv = _py_module_cmd("tools.geodata.build_aqueduct_admin_masters")
        _append_flag(admin_argv, "--overwrite", bool(args.overwrite))
        admin_argv.extend(_build_aqueduct_metric_args(args))
        plan.append(PlannedCommand(label="aqueduct-admin-masters", argv=admin_argv))

        hydro_argv = _py_module_cmd("tools.geodata.build_aqueduct_hydro_masters")
        _append_flag(hydro_argv, "--overwrite", bool(args.overwrite))
        hydro_argv.extend(_build_aqueduct_metric_args(args))
        plan.append(PlannedCommand(label="aqueduct-hydro-masters", argv=hydro_argv))

        if not bool(getattr(args, "skip_validation", False)):
            validate_argv = _py_module_cmd("tools.geodata.validate_aqueduct_workflow")
            _append_flag(validate_argv, "--overwrite", bool(args.overwrite))
            validate_argv.extend(_build_aqueduct_metric_args(args))
            plan.append(PlannedCommand(label="aqueduct-validate", argv=validate_argv))

    if include_runtime:
        plan.extend(_build_runtime_plan(args, scope=scope))
    return plan


def build_population_plan(
    args: argparse.Namespace,
    *,
    include_blocks_geojson: bool = True,
    include_runtime: bool = True,
    runtime_scope: Optional[BundleRuntimeScope] = None,
) -> list[PlannedCommand]:
    """Build the population exposure prep plan."""
    scope = runtime_scope or _resolve_runtime_scope("population-exposure", args)
    plan: list[PlannedCommand] = []
    if not bool(getattr(args, "audit_only", False)) and (
        bool(args.overwrite) or scope.runtime_needed or not include_runtime
    ):
        if include_blocks_geojson:
            plan.extend(build_blocks_geojson_plan(args))
        argv = _py_module_cmd("tools.geodata.build_population_admin_masters")
        _append_flag(argv, "--overwrite", bool(args.overwrite))
        if getattr(args, "population_raster", None):
            argv.extend(["--raster", str(args.population_raster)])
        plan.append(PlannedCommand(label="population-admin-masters", argv=argv))
    if include_runtime:
        plan.extend(_build_runtime_plan(args, scope=scope))
    return plan


def build_groundwater_plan(
    args: argparse.Namespace,
    *,
    include_runtime: bool = True,
    runtime_scope: Optional[BundleRuntimeScope] = None,
) -> list[PlannedCommand]:
    """Build the groundwater district prep plan."""
    scope = runtime_scope or _resolve_runtime_scope("groundwater", args)
    plan: list[PlannedCommand] = []
    if not bool(getattr(args, "audit_only", False)) and (
        bool(args.overwrite) or scope.runtime_needed or not include_runtime
    ):
        argv = _py_module_cmd("tools.geodata.build_groundwater_district_masters")
        _append_flag(argv, "--overwrite", bool(args.overwrite))
        if getattr(args, "groundwater_workbook", None):
            argv.extend(["--workbook", str(args.groundwater_workbook)])
        if getattr(args, "groundwater_alias_csv", None):
            argv.extend(["--district-alias-csv", str(args.groundwater_alias_csv)])
        plan.append(PlannedCommand(label="groundwater-district-masters", argv=argv))
    if include_runtime:
        plan.extend(_build_runtime_plan(args, scope=scope))
    return plan


def _build_climate_compute_steps(
    args: argparse.Namespace,
    *,
    levels: Sequence[str],
    metrics_by_level: dict[str, Sequence[str]],
    admin_states: Sequence[str],
) -> list[PlannedCommand]:
    plan: list[PlannedCommand] = []
    for level in levels:
        metrics = list(metrics_by_level.get(level, ()))
        if not metrics:
            continue
        states_for_level = admin_states if level in {"district", "block"} else [""]
        for state_name in states_for_level:
            argv = _py_module_cmd("tools.pipeline.compute_indices_multiprocess")
            argv.extend(["--level", level])
            if state_name:
                argv.extend(["--state", state_name])
            _append_multi(argv, "--metrics", metrics)
            _append_multi(argv, "--models", getattr(args, "models", None))
            _append_multi(argv, "--scenarios", getattr(args, "scenarios", None))
            if getattr(args, "workers", None) is not None:
                argv.extend(["--workers", str(args.workers)])
            _append_flag(argv, "--verbose", bool(getattr(args, "verbose", False)))
            _append_flag(argv, "--spi-legacy", bool(getattr(args, "spi_legacy", False)))
            if getattr(args, "spi_distribution", None):
                argv.extend(["--spi-distribution", str(args.spi_distribution)])
            _append_flag(argv, "--overwrite", bool(getattr(args, "overwrite", False)))
            if not bool(getattr(args, "overwrite", False)):
                argv.append("--skip-existing")
            label = f"climate-compute:{level}"
            if state_name:
                label = f"{label}:{state_name}"
            plan.append(PlannedCommand(label=label, argv=argv))
    return plan


def _build_climate_master_steps(
    args: argparse.Namespace,
    *,
    levels: Sequence[str],
    metrics_by_level: dict[str, Sequence[str]],
    admin_states: Sequence[str],
) -> list[PlannedCommand]:
    plan: list[PlannedCommand] = []
    for level in levels:
        metrics = list(metrics_by_level.get(level, ()))
        if not metrics:
            continue
        argv = _py_module_cmd("tools.pipeline.build_master_metrics")
        argv.extend(["--level", level])
        if level in {"district", "block"}:
            argv.extend(["--state", ",".join(admin_states)])
        _append_multi(argv, "--metrics", metrics)
        if getattr(args, "workers", None) is not None:
            argv.extend(["--workers", str(args.workers)])
        if not bool(getattr(args, "overwrite", False)):
            argv.append("--skip-existing")
        if not bool(getattr(args, "verbose", False)):
            argv.append("--quiet")
        plan.append(PlannedCommand(label=f"climate-masters:{level}", argv=argv))
    return plan


def _build_climate_runtime_plan(
    args: argparse.Namespace,
    *,
    scope: ClimateRuntimeScope,
) -> list[PlannedCommand]:
    if bool(getattr(args, "audit_only", False)):
        return [] if bool(getattr(args, "skip_audit", False)) else [
            _build_audit_step(args, scope.selected_metrics, levels=scope.levels)
        ]

    plan: list[PlannedCommand] = []
    if not bool(getattr(args, "skip_optimised", False)):
        grouped: dict[tuple[tuple[str, ...], tuple[str, ...]], list[str]] = {}
        for level in scope.levels:
            readiness = scope.by_level[level]
            pending_metrics = tuple(readiness.optimized_pending_metrics)
            has_level_global_issues = any(
                str(issue.get("level") or "").strip() in {"", level}
                for issue in scope.global_issues
            )
            if not pending_metrics and not has_level_global_issues:
                continue
            key = (pending_metrics, tuple())
            grouped.setdefault(key, []).append(level)

        for (pending_metrics, _unused), grouped_levels in grouped.items():
            label_levels = "+".join(grouped_levels)
            plan.append(
                _build_optimised_step(
                    args,
                    pending_metrics,
                    levels=grouped_levels,
                    label=f"processed-optimised-build:{label_levels}",
                )
            )

    if not bool(getattr(args, "skip_audit", False)):
        plan.append(
            _build_audit_step(
                args,
                scope.selected_metrics,
                levels=scope.levels,
            )
        )
    return plan


def build_climate_hazards_plan(
    args: argparse.Namespace,
    *,
    include_runtime: bool = True,
    runtime_scope: Optional[ClimateRuntimeScope] = None,
) -> list[PlannedCommand]:
    levels = _resolve_levels(str(args.level))
    admin_states = _resolve_admin_states(getattr(args, "state", None))
    scope = runtime_scope or _resolve_climate_runtime_scope(args, levels=levels)

    compute_metrics_by_level = {
        level: list(scope.by_level[level].compute_pending_metrics)
        for level in levels
        if level in scope.by_level
    }
    master_metrics_by_level = {
        level: list(scope.by_level[level].masters_pending_metrics)
        for level in levels
        if level in scope.by_level
    }

    plan: list[PlannedCommand] = []
    if not bool(getattr(args, "audit_only", False)):
        if not bool(getattr(args, "skip_compute", False)):
            plan.extend(
                _build_climate_compute_steps(
                    args,
                    levels=levels,
                    metrics_by_level=compute_metrics_by_level,
                    admin_states=admin_states,
                )
            )
        if not bool(getattr(args, "skip_masters", False)):
            plan.extend(
                _build_climate_master_steps(
                    args,
                    levels=levels,
                    metrics_by_level=master_metrics_by_level,
                    admin_states=admin_states,
                )
            )

    if include_runtime:
        plan.extend(_build_climate_runtime_plan(args, scope=scope))
    return plan


def build_validation_plan(args: argparse.Namespace) -> list[PlannedCommand]:
    plan: list[PlannedCommand] = []
    validate_argv = _py_module_cmd("tools.geodata.validate_aqueduct_workflow")
    _append_flag(validate_argv, "--overwrite", bool(args.overwrite))
    validate_argv.extend(_build_aqueduct_metric_args(args))
    plan.append(PlannedCommand(label="aqueduct-validate", argv=validate_argv))

    if bool(getattr(args, "include_pytest", False)):
        pytest_argv = [sys.executable, "-m", "pytest", "-q", *DEFAULT_VALIDATION_TESTS]
        plan.append(PlannedCommand(label="pytest-validation", argv=pytest_argv))

    return plan


def build_dashboard_package_plan(args: argparse.Namespace) -> list[PlannedCommand]:
    climate_levels = _resolve_levels(str(args.level))
    climate_scope = _resolve_climate_runtime_scope(
        args,
        levels=climate_levels,
    )
    aqueduct_scope = _resolve_runtime_scope("aqueduct", args)
    population_scope = _resolve_runtime_scope("population-exposure", args)
    groundwater_scope = _resolve_runtime_scope("groundwater", args)

    package_scope = BundleRuntimeScope(
        selected_metrics=_resolve_bundle_metrics("dashboard-package", args),
        pending_metrics=_dedupe_keep_order(
            climate_scope.pending_metrics
            + aqueduct_scope.pending_metrics
            + population_scope.pending_metrics
            + groundwater_scope.pending_metrics
        ),
        has_global_issues=(
            climate_scope.has_global_issues
            or aqueduct_scope.has_global_issues
            or population_scope.has_global_issues
            or groundwater_scope.has_global_issues
        ),
    )

    if bool(getattr(args, "audit_only", False)):
        return _build_runtime_plan(args, scope=package_scope)

    climate_plan = build_climate_hazards_plan(args, include_runtime=False, runtime_scope=climate_scope)
    aqueduct_plan = build_aqueduct_plan(args, include_blocks_geojson=False, include_runtime=False, runtime_scope=aqueduct_scope)
    population_plan = build_population_plan(args, include_blocks_geojson=False, include_runtime=False, runtime_scope=population_scope)
    groundwater_plan = build_groundwater_plan(args, include_runtime=False, runtime_scope=groundwater_scope)

    plan: list[PlannedCommand] = []
    if aqueduct_plan or population_plan:
        plan.extend(build_blocks_geojson_plan(args))
    plan.extend(climate_plan)
    plan.extend(aqueduct_plan)
    plan.extend(population_plan)
    plan.extend(groundwater_plan)
    plan.extend(_build_runtime_plan(args, scope=package_scope))

    if bool(getattr(args, "include_pytest", False)) and not bool(getattr(args, "audit_only", False)):
        plan.append(
            PlannedCommand(
                label="pytest-validation",
                argv=[sys.executable, "-m", "pytest", "-q", *DEFAULT_VALIDATION_TESTS],
            )
        )
    return plan


def build_step_plan(args: argparse.Namespace) -> list[PlannedCommand]:
    step = str(args.command)
    if step == "aqueduct-baseline":
        return [_build_prepare_aqueduct_baseline_step(args)]

    module_map = {
        "blocks-geojson": "tools.geodata.build_blocks_geojson",
        "aqueduct-admin-crosswalk": "tools.geodata.build_aqueduct_admin_crosswalk",
        "aqueduct-block-crosswalk": "tools.geodata.build_aqueduct_block_crosswalk",
        "aqueduct-admin-masters": "tools.geodata.build_aqueduct_admin_masters",
        "aqueduct-hydro-crosswalk": "tools.geodata.build_aqueduct_hydro_crosswalk",
        "aqueduct-hydro-masters": "tools.geodata.build_aqueduct_hydro_masters",
        "aqueduct-validate": "tools.geodata.validate_aqueduct_workflow",
        "population-admin-masters": "tools.geodata.build_population_admin_masters",
        "groundwater-district-masters": "tools.geodata.build_groundwater_district_masters",
    }
    if step in module_map:
        argv = _py_module_cmd(module_map[step])
        _append_flag(argv, "--overwrite", bool(args.overwrite))
        if step.startswith("aqueduct-"):
            argv.extend(_build_aqueduct_metric_args(args))
        if step == "population-admin-masters" and getattr(args, "population_raster", None):
            argv.extend(["--raster", str(args.population_raster)])
        if step == "groundwater-district-masters":
            if getattr(args, "groundwater_workbook", None):
                argv.extend(["--workbook", str(args.groundwater_workbook)])
            if getattr(args, "groundwater_alias_csv", None):
                argv.extend(["--district-alias-csv", str(args.groundwater_alias_csv)])
        return [PlannedCommand(label=step, argv=argv)]

    if step == "climate-compute":
        return build_climate_hazards_plan(
            argparse.Namespace(
                level=args.level,
                state=args.state,
                metrics=args.metrics,
                models=args.models,
                scenarios=args.scenarios,
                workers=args.workers,
                verbose=args.verbose,
                spi_legacy=args.spi_legacy,
                spi_distribution=args.spi_distribution,
                skip_compute=False,
                skip_masters=True,
                overwrite=args.overwrite,
                audit_only=False,
                skip_optimised=True,
                skip_audit=True,
            ),
            include_runtime=False,
            runtime_scope=_resolve_climate_runtime_scope(args, levels=_resolve_levels(str(args.level))),
        )

    if step == "climate-masters":
        return build_climate_hazards_plan(
            argparse.Namespace(
                level=args.level,
                state=args.state,
                metrics=args.metrics,
                models=None,
                scenarios=None,
                workers=args.workers,
                verbose=args.verbose,
                spi_legacy=False,
                spi_distribution=None,
                skip_compute=True,
                skip_masters=False,
                overwrite=args.overwrite,
                audit_only=False,
                skip_optimised=True,
                skip_audit=True,
            ),
            include_runtime=False,
            runtime_scope=_resolve_climate_runtime_scope(args, levels=_resolve_levels(str(args.level))),
        )

    if step == "pytest-validation":
        return [
            PlannedCommand(
                label="pytest-validation",
                argv=[sys.executable, "-m", "pytest", "-q", *DEFAULT_VALIDATION_TESTS],
            )
        ]

    raise SystemExit(f"Unsupported command: {step}")


def build_command_plan(args: argparse.Namespace) -> list[PlannedCommand]:
    command = str(args.command)
    if bool(getattr(args, "audit_only", False)) and bool(getattr(args, "skip_audit", False)):
        raise SystemExit("--audit-only cannot be combined with --skip-audit.")
    if command == "list":
        return []
    if command == "aqueduct":
        return build_aqueduct_plan(args, include_blocks_geojson=True, include_runtime=True)
    if command == "climate-hazards":
        return build_climate_hazards_plan(args, include_runtime=True)
    if command == "population-exposure":
        return build_population_plan(args, include_blocks_geojson=True, include_runtime=True)
    if command == "groundwater":
        return build_groundwater_plan(args, include_runtime=True)
    if command == "dashboard-package":
        return build_dashboard_package_plan(args)
    if command == "validate":
        return build_validation_plan(args)
    return build_step_plan(args)


def _print_available_commands() -> None:
    print("Available workflow bundles:")
    print("  aqueduct")
    print("  climate-hazards")
    print("  population-exposure")
    print("  groundwater")
    print("  dashboard-package")
    print("  validate")
    print("")
    print("Available step commands:")
    for step in [
        "blocks-geojson",
        "aqueduct-baseline",
        "aqueduct-admin-crosswalk",
        "aqueduct-block-crosswalk",
        "aqueduct-admin-masters",
        "aqueduct-hydro-crosswalk",
        "aqueduct-hydro-masters",
        "aqueduct-validate",
        "population-admin-masters",
        "groundwater-district-masters",
        "climate-compute",
        "climate-masters",
        "pytest-validation",
    ]:
        print(f"  {step}")


def execute_plan(plan: Sequence[PlannedCommand], *, dry_run: bool, plan_only: bool) -> int:
    prefix = "PLAN" if plan_only else "DRY-RUN" if dry_run else "RUN"
    print("PREPARE DASHBOARD RUN")
    print(f"steps: {len(plan)}")
    if not plan:
        print("  Nothing to do.")
        return 0
    for idx, step in enumerate(plan, start=1):
        rendered = shlex.join(step.argv)
        print(f"[{idx}/{len(plan)}] {prefix} {step.label}")
        print(f"  {rendered}")
        if dry_run or plan_only:
            continue
        try:
            subprocess.run(step.argv, check=True)
        except subprocess.CalledProcessError as exc:
            print(f"STEP FAILED [{idx}/{len(plan)}] {step.label} (exit={exc.returncode})")
            return int(exc.returncode or 1)
    return 0


def _add_common_runner_flags(parser: argparse.ArgumentParser, *, include_runtime_controls: bool = False) -> None:
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")
    parser.add_argument("--plan-only", action="store_true", help="Render the final plan without executing it.")
    parser.add_argument("--overwrite", action="store_true", help="Force rebuilds instead of preserving current outputs.")
    if include_runtime_controls:
        parser.add_argument("--skip-optimised", action="store_true", help="Skip the processed_optimised rebuild stage.")
        parser.add_argument("--skip-audit", action="store_true", help="Skip the final processed_optimised audit stage.")
        parser.add_argument("--audit-only", action="store_true", help="Run only the processed_optimised audit stage for the selected scope.")


def _add_aqueduct_flags(parser: argparse.ArgumentParser, *, bundle: bool) -> None:
    parser.add_argument(
        "--metric-slug",
        action="append",
        default=None,
        help="Restrict Aqueduct steps to one or more onboarded metric slugs.",
    )
    if bundle:
        parser.add_argument(
            "--prepare-baseline",
            action="store_true",
            help="Include the raw Aqueduct baseline cleanup step before crosswalks and masters.",
        )
        parser.add_argument("--source-gdb", default=None, help="Aqueduct file geodatabase path for baseline cleanup.")
        parser.add_argument("--baseline-csv", default=None, help="Aqueduct baseline CSV path for baseline cleanup.")
        parser.add_argument("--skip-validation", action="store_true", help="Skip Aqueduct validation at the end.")


def _add_population_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--population-raster",
        default=None,
        help="Optional override path to the 2025 population raster used by population exposure prep.",
    )


def _add_groundwater_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--groundwater-workbook",
        default=None,
        help="Optional override path to the 2024-2025 groundwater assessment workbook.",
    )
    parser.add_argument(
        "--groundwater-alias-csv",
        default=None,
        help="Optional override path to the groundwater district alias CSV.",
    )


def _add_climate_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--level",
        choices=sorted(LEVEL_GROUPS.keys()),
        default="all",
        help="Level group to process (default: all).",
    )
    parser.add_argument(
        "--state",
        action="append",
        default=None,
        help=(
            "Admin state to process. Repeat or pass comma-separated values. "
            f"Defaults to {DEFAULT_ADMIN_STATE} for admin levels."
        ),
    )
    parser.add_argument("--metrics", nargs="+", default=None, help="Restrict climate compute/master steps to metric slugs.")
    parser.add_argument("--models", nargs="+", default=None, help="Restrict climate compute to model names.")
    parser.add_argument("--scenarios", nargs="+", default=None, help="Restrict climate compute to scenarios.")
    parser.add_argument("--workers", type=int, default=None, help="Worker count to pass through to compute/master steps.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose compute output.")
    parser.add_argument("--spi-legacy", action="store_true", help="Pass through the legacy SPI flag to climate compute.")
    parser.add_argument(
        "--spi-distribution",
        choices=["gamma", "pearson"],
        default=None,
        help="SPI distribution to pass through to climate compute.",
    )
    parser.add_argument("--skip-compute", action="store_true", help="Skip the climate compute stage.")
    parser.add_argument("--skip-masters", action="store_true", help="Skip the climate master build stage.")


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run canonical IRT dashboard-prep workflows from one operator-facing command."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_list = subparsers.add_parser("list", help="List available workflow bundles and step commands.")
    _add_common_runner_flags(p_list)

    p_aq = subparsers.add_parser("aqueduct", help="Prepare the Aqueduct dashboard bundle.")
    _add_common_runner_flags(p_aq, include_runtime_controls=True)
    _add_aqueduct_flags(p_aq, bundle=True)

    p_climate = subparsers.add_parser("climate-hazards", help="Prepare the climate hazards dashboard bundle.")
    _add_common_runner_flags(p_climate, include_runtime_controls=True)
    _add_climate_flags(p_climate)

    p_population = subparsers.add_parser("population-exposure", help="Prepare the population exposure dashboard bundle.")
    _add_common_runner_flags(p_population, include_runtime_controls=True)
    _add_population_flags(p_population)

    p_groundwater = subparsers.add_parser("groundwater", help="Prepare the groundwater dashboard bundle.")
    _add_common_runner_flags(p_groundwater, include_runtime_controls=True)
    _add_groundwater_flags(p_groundwater)

    p_pkg = subparsers.add_parser("dashboard-package", help="Prepare all dashboard bundles end to end.")
    _add_common_runner_flags(p_pkg, include_runtime_controls=True)
    _add_climate_flags(p_pkg)
    _add_aqueduct_flags(p_pkg, bundle=True)
    _add_population_flags(p_pkg)
    _add_groundwater_flags(p_pkg)
    p_pkg.add_argument("--include-pytest", action="store_true", help="Run the default validation pytest set at the end.")

    p_validate = subparsers.add_parser("validate", help="Run Aqueduct validation and optional targeted pytest checks.")
    _add_common_runner_flags(p_validate)
    _add_aqueduct_flags(p_validate, bundle=False)
    p_validate.add_argument("--include-pytest", action="store_true", help="Run the default validation pytest set after the validator.")

    for name in [
        "blocks-geojson",
        "aqueduct-baseline",
        "aqueduct-admin-crosswalk",
        "aqueduct-block-crosswalk",
        "aqueduct-admin-masters",
        "aqueduct-hydro-crosswalk",
        "aqueduct-hydro-masters",
        "aqueduct-validate",
        "population-admin-masters",
        "groundwater-district-masters",
    ]:
        sub = subparsers.add_parser(name, help=f"Run the `{name}` step only.")
        _add_common_runner_flags(sub)
        if name == "population-admin-masters":
            _add_population_flags(sub)
        elif name == "groundwater-district-masters":
            _add_groundwater_flags(sub)
        elif name != "blocks-geojson":
            _add_aqueduct_flags(sub, bundle=(name == "aqueduct-baseline"))

    p_compute = subparsers.add_parser("climate-compute", help="Run climate compute only.")
    _add_common_runner_flags(p_compute)
    _add_climate_flags(p_compute)

    p_masters = subparsers.add_parser("climate-masters", help="Run climate master builds only.")
    _add_common_runner_flags(p_masters)
    _add_climate_flags(p_masters)

    p_pytest = subparsers.add_parser("pytest-validation", help="Run the default validation pytest target set.")
    _add_common_runner_flags(p_pytest)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)
    if str(args.command) == "list":
        _print_available_commands()
        return 0
    if str(args.command) == "climate-hazards":
        climate_scope = _resolve_climate_runtime_scope(args, levels=_resolve_levels(str(args.level)))
        _print_climate_readiness(climate_scope)
        plan = build_climate_hazards_plan(args, include_runtime=True, runtime_scope=climate_scope)
        rc = execute_plan(plan, dry_run=bool(args.dry_run), plan_only=bool(args.plan_only))
        if bool(args.dry_run) or bool(args.plan_only):
            return rc
        if rc != 0:
            return rc
        post_scope = _resolve_climate_runtime_scope(args, levels=_resolve_levels(str(args.level)))
        post_status = _evaluate_climate_post_run_status(
            post_scope,
            require_compute=not bool(getattr(args, "audit_only", False)) and not bool(getattr(args, "skip_compute", False)),
            require_masters=not bool(getattr(args, "audit_only", False)) and not bool(getattr(args, "skip_masters", False)),
            require_optimized=not bool(getattr(args, "audit_only", False)) and not bool(getattr(args, "skip_optimised", False)),
            require_audit=not bool(getattr(args, "skip_audit", False)),
        )
        if post_status.blocking or post_status.informational_pending:
            print("POST-RUN CLIMATE READINESS")
            _print_climate_readiness(post_scope)
            for message in post_status.informational_messages:
                print(f"- {message}")
        if post_status.blocking:
            return 1
        return rc
    plan = build_command_plan(args)
    return execute_plan(plan, dry_run=bool(args.dry_run), plan_only=bool(args.plan_only))


if __name__ == "__main__":
    raise SystemExit(main())

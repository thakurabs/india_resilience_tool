#!/usr/bin/env python3
"""
Canonical dashboard-ready workflow runner for IRT data prep.

This is the primary operator entrypoint. It orchestrates the lower-level
pipeline and geodata tools so users can prepare one bundle, one metric, or the
full dashboard without memorizing internal commands.

The runner is non-destructive by default:
- existing outputs are not forcibly deleted unless `--overwrite` is supplied
- climate runs default to `--level all`
- climate runs resolve live metrics per requested admin level (district/block)
- climate compute uses validated completion markers and `--skip-existing` by
  default unless `--overwrite` is supplied
- climate, Aqueduct, population, and groundwater flows can refresh
  `processed_optimised` and then audit parity/readiness
- climate `--audit-only` and normal execution both return non-zero when the
  requested readiness state is still incomplete

Examples:
    python -m tools.runs.prepare_dashboard --help
    python -m tools.runs.prepare_dashboard climate-hazards
    python -m tools.runs.prepare_dashboard climate-hazards --level district
    python -m tools.runs.prepare_dashboard climate-hazards --metrics tas_annual_mean
    python -m tools.runs.prepare_dashboard climate-hazards --level block --metrics r95ptot_contribution_pct --models CanESM5 --scenarios historical
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
    "tests/test_groundwater_district_masters.py",
    "tests/test_jrc_flood_depth_admin_masters.py",
    "tests/test_population_admin_masters.py",
    "tests/test_built_up_area_admin_masters.py",
    "tests/test_lulc_admin_masters.py",
    "tests/test_rural_facilities_admin_masters.py",
    "tests/test_validate_aqueduct_workflow.py",
    "tests/test_metrics_registry.py",
    "tests/test_config.py",
    "tests/test_available_states.py",
    "tests/test_crosswalk_generator.py",
    "tests/test_prepare_dashboard_runner.py",
]
CLIMATE_PILLAR = "Climate Hazards"
AQUEDUCT_DOMAIN = "Aqueduct Water Risk"
POPULATION_DOMAIN = "Population Exposure"
RURAL_FACILITIES_DOMAIN = "Rural Facilities Exposure"
BUILT_UP_AREA_DOMAIN = "Built-up Area Exposure"
LULC_DOMAIN = "Agricultural LULC Exposure"
GROUNDWATER_DOMAIN = "Groundwater Status & Availability"
JRC_DOMAIN = "Riverine Flood"
LEVEL_GROUPS = {
    "all": ["district", "block"],
    "admin": ["district", "block"],
    "district": ["district"],
    "block": ["block"],
}
LEVEL_TO_FAMILY = {
    "district": "admin",
    "block": "admin",
}
LEGACY_MASTER_FILENAMES = {
    "district": "master_metrics_by_district.csv",
    "block": "master_metrics_by_block.csv",
}
MASTER_REQUIRED_COLUMNS = {
    "district": {"state", "district", "district_key"},
    "block": {"state", "district", "block", "block_key"},
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


def _append_repeat_literal(argv: list[str], flag: str, values: Sequence[str] | None) -> None:
    """Append ``flag VALUE`` per item WITHOUT comma-splitting.

    For already-resolved names that may legitimately contain commas — e.g. the UT
    ``Dadra, Nagar Haveli, Daman & Diu`` — unlike :func:`_append_repeat`, which re-splits
    each value on ``,`` and would fracture such a name into phantom states.
    """
    if not values:
        return
    for value in _dedupe_keep_order([str(v).strip() for v in values if str(v).strip()]):
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
    return tuple(admin_states)


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
    if bundle == "rural-facilities":
        return _metrics_for_domain(RURAL_FACILITIES_DOMAIN)
    if bundle == "built-up-area":
        return _metrics_for_domain(BUILT_UP_AREA_DOMAIN)
    if bundle == "lulc":
        return _metrics_for_domain(LULC_DOMAIN)
    if bundle == "groundwater":
        return _metrics_for_domain(GROUNDWATER_DOMAIN)
    if bundle == "jrc-flood-depth":
        return _metrics_for_domain(JRC_DOMAIN)
    if bundle == "dashboard-package":
        metrics: list[str] = []
        metrics.extend(_resolve_climate_bundle_metrics(args))
        metrics.extend(_split_csv_values(getattr(args, "metric_slug", None)) or _metrics_for_domain(AQUEDUCT_DOMAIN))
        metrics.extend(_metrics_for_domain(POPULATION_DOMAIN))
        metrics.extend(_metrics_for_domain(BUILT_UP_AREA_DOMAIN))
        metrics.extend(_metrics_for_domain(LULC_DOMAIN))
        if bool(getattr(args, "include_rural_facilities", False)):
            metrics.extend(_metrics_for_domain(RURAL_FACILITIES_DOMAIN))
        metrics.extend(_metrics_for_domain(GROUNDWATER_DOMAIN))
        if bool(getattr(args, "include_jrc_flood_depth", False)):
            metrics.extend(_metrics_for_domain(JRC_DOMAIN))
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
    return root / scope_name / LEGACY_MASTER_FILENAMES[level]


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


def _resolve_composite_metric_slugs() -> list[str]:
    from india_resilience_tool.config.composite_metrics import get_visible_glance_composite_slugs

    return list(get_visible_glance_composite_slugs())


def _resolve_sector_wise_bundle_slugs() -> list[str]:
    from india_resilience_tool.config.dashboard_bundles import SECTOR_WISE_DASHBOARD_BUNDLES

    return [spec.composite_slug for spec in SECTOR_WISE_DASHBOARD_BUNDLES]


def _resolve_composite_runtime_scope(
    *,
    levels: Sequence[str],
    admin_states: Sequence[str],
    data_dir: Path,
    overwrite: bool,
) -> BundleRuntimeScope:
    composite_levels = [level for level in levels if level in {"district", "block"}]
    selected_metrics = _resolve_composite_metric_slugs() if composite_levels else []
    pending_metrics: list[str] = []
    if overwrite:
        pending_metrics = list(selected_metrics)
    else:
        for slug in selected_metrics:
            for level in composite_levels:
                if not all(
                    _legacy_master_ready(
                        slug=slug,
                        level=level,
                        scope_name=state_name,
                        data_dir=data_dir,
                    )
                    for state_name in admin_states
                ):
                    pending_metrics.append(slug)
                    break
    return BundleRuntimeScope(
        selected_metrics=selected_metrics,
        pending_metrics=_dedupe_keep_order(pending_metrics),
        has_global_issues=False,
    )


def _resolve_proposal_runtime_scope(
    *,
    levels: Sequence[str],
    admin_states: Sequence[str],
    data_dir: Path,
    overwrite: bool,
) -> BundleRuntimeScope:
    proposal_levels = [level for level in levels if level in {"district", "block"}]
    selected_metrics = _resolve_sector_wise_bundle_slugs() if proposal_levels else []
    pending_metrics: list[str] = []
    if overwrite:
        pending_metrics = list(selected_metrics)
    else:
        for slug in selected_metrics:
            for level in proposal_levels:
                if not all(
                    _legacy_master_ready(
                        slug=slug,
                        level=level,
                        scope_name=state_name,
                        data_dir=data_dir,
                    )
                    for state_name in admin_states
                ):
                    pending_metrics.append(slug)
                    break
    return BundleRuntimeScope(
        selected_metrics=selected_metrics,
        pending_metrics=_dedupe_keep_order(pending_metrics),
        has_global_issues=False,
    )


def _build_composite_master_steps(
    args: argparse.Namespace,
    *,
    levels: Sequence[str],
    admin_states: Sequence[str],
    scope: BundleRuntimeScope,
    metrics: Optional[Sequence[str]] = None,
    force_overwrite: bool = False,
) -> list[PlannedCommand]:
    """Build composite-master steps for the requested levels.

    By default the metric list is derived from ``scope`` (pending-aware). Callers may
    pass an explicit ``metrics`` override to force-build a specific set regardless of
    the scope's pending state (e.g. the JRC plan must rebuild the Riverine Flood
    composite from its just-built component masters even when the pre-build audit
    reported nothing pending). ``force_overwrite`` always emits ``--overwrite`` for
    that set; otherwise it follows ``args.overwrite``.
    """
    plan: list[PlannedCommand] = []
    if metrics is None and not scope.selected_metrics:
        return plan
    resolved = (
        list(metrics)
        if metrics is not None
        else (_select_metrics_for_execution(scope) or scope.selected_metrics)
    )
    if not resolved:
        return plan
    for level in levels:
        if level not in {"district", "block"}:
            continue
        argv = _py_module_cmd("tools.pipeline.build_composite_metrics")
        argv.extend(["--level", level])
        _append_repeat_literal(argv, "--state", admin_states)
        _append_repeat(argv, "--metric", resolved)
        _append_flag(argv, "--overwrite", force_overwrite or bool(getattr(args, "overwrite", False)))
        if not bool(getattr(args, "verbose", False)):
            argv.append("--quiet")
        plan.append(PlannedCommand(label=f"composite-masters:{level}", argv=argv))
    return plan


def _build_proposal_bundle_steps(
    args: argparse.Namespace,
    *,
    levels: Sequence[str],
    admin_states: Sequence[str],
    scope: BundleRuntimeScope,
) -> list[PlannedCommand]:
    plan: list[PlannedCommand] = []
    if not scope.selected_metrics:
        return plan
    metrics = _select_metrics_for_execution(scope) or scope.selected_metrics
    for level in levels:
        if level not in {"district", "block"}:
            continue
        argv = _py_module_cmd("tools.pipeline.build_proposal_bundles")
        argv.extend(["--level", level])
        _append_repeat(argv, "--state", admin_states)
        _append_repeat(argv, "--bundle", metrics)
        _append_flag(argv, "--overwrite", bool(getattr(args, "overwrite", False)))
        if not bool(getattr(args, "verbose", False)):
            argv.append("--quiet")
        plan.append(PlannedCommand(label=f"proposal-bundles:{level}", argv=argv))
    return plan


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
    # Only error-severity issues block readiness; warnings (e.g. an optional,
    # live-fallback-backed precomputed artifact being absent) must not force a
    # rebuild or mark metrics pending.
    blocking_issues = [
        issue
        for issue in parity_issues
        if str(issue.get("severity") or "error").strip().lower() != "warning"
    ]
    global_issues = tuple(issue for issue in blocking_issues if not str(issue.get("slug") or "").strip())

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
            for issue in blocking_issues
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


def _collect_climate_failure_diagnostics(
    args: argparse.Namespace,
    scope: ClimateRuntimeScope,
) -> tuple[str, ...]:
    """Return one concrete post-run failure explanation per metric/level."""
    from tools.pipeline.compute_indices_multiprocess import (
        build_processing_task_plan,
        ensemble_completion_marker_status,
        task_completion_marker_status,
    )

    admin_states = _resolve_admin_states(getattr(args, "state", None))
    allowed_models = _split_csv_values(getattr(args, "models", None)) or None
    allowed_scenarios = _split_csv_values(getattr(args, "scenarios", None)) or None
    diagnostics: list[str] = []

    for level in scope.levels:
        readiness = scope.by_level.get(level)
        if readiness is None:
            continue

        metrics_needing_explanation = _dedupe_keep_order(
            list(readiness.compute_pending_metrics)
            + list(readiness.masters_pending_metrics)
            + list(readiness.optimized_pending_metrics)
            + list(readiness.unrunnable_metrics)
        )
        if not metrics_needing_explanation:
            continue

        scope_names = _scope_names_for_level(level, admin_states)
        for scope_name in scope_names:
            task_plan = build_processing_task_plan(
                metrics_filter=metrics_needing_explanation,
                models_filter=allowed_models,
                scenarios_filter=allowed_scenarios,
                level=level,
                state=scope_name,
            )
            tasks_by_metric: dict[str, list[Any]] = {}
            for task in task_plan.tasks:
                tasks_by_metric.setdefault(task.slug, []).append(task)

            for metric in metrics_needing_explanation:
                prefix = f"{level}/{metric}/{scope_name}"

                if metric in readiness.unrunnable_metrics:
                    reasons = readiness.unrunnable_reasons_by_metric.get(metric, ())
                    diagnostics.append(
                        f"{prefix}: unrunnable ({', '.join(reasons) or 'unknown_reason'})"
                    )
                    continue

                for task in tasks_by_metric.get(metric, []):
                    status = task_completion_marker_status(task)
                    if not status.valid:
                        detail = f" [{status.detail}]" if status.detail else ""
                        diagnostics.append(
                            f"{prefix}: compute marker invalid for {task.model}/{task.scenario}: "
                            f"{status.reason}{detail}"
                        )
                        break
                else:
                    ensemble_status = ensemble_completion_marker_status(
                        slug=metric,
                        level=level,
                        scope_name=scope_name,
                        allowed_models=allowed_models,
                        allowed_scenarios=allowed_scenarios,
                    )
                    if not ensemble_status.valid:
                        detail = f" [{ensemble_status.detail}]" if ensemble_status.detail else ""
                        diagnostics.append(
                            f"{prefix}: ensemble marker invalid: {ensemble_status.reason}{detail}"
                        )
                        continue

                    if metric in readiness.masters_pending_metrics:
                        diagnostics.append(f"{prefix}: legacy master not ready")
                        continue

                    if metric in readiness.optimized_pending_metrics:
                        diagnostics.append(f"{prefix}: optimized parity issue remains")

    return tuple(_dedupe_keep_order(diagnostics))


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
        and str(issue.get("severity") or "error").strip().lower() != "warning"
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
    overwrite: Optional[bool] = None,
) -> PlannedCommand:
    argv = _py_module_cmd("tools.optimized.build_processed_optimised")
    _append_repeat(argv, "--metric", metrics)
    _append_repeat(argv, "--level", levels)
    _append_flag(argv, "--overwrite", bool(args.overwrite) if overwrite is None else bool(overwrite))
    argv.append("--skip-audit")
    return PlannedCommand(label=label, argv=argv)


def _build_state_values_step(
    args: argparse.Namespace,
    metrics: Sequence[str] | None,
    *,
    label: str = "processed-optimised-state-values",
) -> PlannedCommand:
    """Precompute area-weighted state headline values over the fresh bundle.

    Admin-only by construction (the tool defaults to district+block and skips
    levels without master shards), so no ``--level`` is forwarded. Runs without
    ``--strict`` so coverage-cliff/join warnings never block publish; the parity
    audit's presence check (non-fatal) flags a missing artifact.
    """
    argv = _py_module_cmd("tools.optimized.build_state_values")
    _append_repeat(argv, "--metric", metrics)
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
    overwrite_optimised: Optional[bool] = None,
) -> list[PlannedCommand]:
    if bool(getattr(args, "audit_only", False)):
        return [] if bool(getattr(args, "skip_audit", False)) else [_build_audit_step(args, scope.selected_metrics)]

    plan: list[PlannedCommand] = []
    if allow_optimised and not bool(getattr(args, "skip_optimised", False)) and scope.runtime_needed:
        execution_metrics = _select_metrics_for_execution(scope)
        plan.append(
            _build_optimised_step(
                args,
                execution_metrics,
                overwrite=overwrite_optimised,
            )
        )
        # Precompute state headline values over the freshly built bundle, before
        # the parity audit reports on them.
        plan.append(_build_state_values_step(args, execution_metrics))
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


def _add_jrc_flags(parser: argparse.ArgumentParser, *, prefixed: bool = False) -> None:
    if prefixed:
        parser.add_argument("--include-jrc-flood-depth", action="store_true", help="Include the JRC flood-depth bundle.")
        parser.add_argument("--jrc-state", default=None, help="Admin state for JRC flood-depth masters (default: Telangana).")
        parser.add_argument("--jrc-source-manifest", default=None, help="Strict RP-100 source_manifest.json from prepare_jrc_rp100_source.")
        parser.add_argument("--jrc-rp100-only", action="store_true", help="Build only strict RP-100 JRC outputs from --jrc-source-manifest.")
        parser.add_argument("--jrc-source-dir", default=None, help="Directory containing the required JRC flood-depth rasters.")
        parser.add_argument("--jrc-assume-units", default=None, help="Attested JRC flood-depth units; must be 'm' when provided.")
        parser.add_argument("--jrc-districts-path", default=None, help="Optional override path to canonical district boundaries.")
        parser.add_argument("--jrc-blocks-path", default=None, help="Optional override path to canonical block boundaries.")
        parser.add_argument("--jrc-qa-dir", default=None, help="Optional override directory for JRC QA outputs.")
        parser.add_argument("--jrc-overlay-dir", default=None, help="Optional override directory for the shared RP-100 overlay.")
        return
    parser.add_argument("--state", default="Telangana", help="Admin state for JRC flood-depth masters (default: Telangana).")
    parser.add_argument("--source-manifest", default=None, help="Strict RP-100 source_manifest.json from prepare_jrc_rp100_source.")
    parser.add_argument("--rp100-only", action="store_true", help="Build only strict RP-100 JRC outputs from --source-manifest.")
    parser.add_argument("--source-dir", default=None, help="Directory containing the required JRC flood-depth rasters.")
    parser.add_argument("--assume-units", default=None, help="Attested JRC flood-depth units; must be 'm'.")
    parser.add_argument("--districts-path", default=None, help="Optional override path to canonical district boundaries.")
    parser.add_argument("--blocks-path", default=None, help="Optional override path to canonical block boundaries.")
    parser.add_argument("--qa-dir", default=None, help="Optional override directory for JRC QA outputs.")
    parser.add_argument("--overlay-dir", default=None, help="Optional override directory for the shared RP-100 overlay.")


def _validate_jrc_inputs(
    args: argparse.Namespace,
    *,
    prefixed: bool = False,
    require_source: bool,
) -> None:
    source_dir_attr = "jrc_source_dir" if prefixed else "source_dir"
    source_manifest_attr = "jrc_source_manifest" if prefixed else "source_manifest"
    rp100_only_attr = "jrc_rp100_only" if prefixed else "rp100_only"
    assume_units_attr = "jrc_assume_units" if prefixed else "assume_units"
    source_dir = getattr(args, source_dir_attr, None)
    source_manifest = getattr(args, source_manifest_attr, None)
    assume_units = getattr(args, assume_units_attr, None)
    if not require_source:
        return
    if source_manifest:
        if source_dir:
            prefix = "--jrc-" if prefixed else "--"
            raise SystemExit(
                f"JRC flood-depth planning accepts either {prefix}source-manifest or {prefix}source-dir, not both."
            )
        if not bool(getattr(args, rp100_only_attr, False)):
            prefix = "--jrc-" if prefixed else "--"
            raise SystemExit(f"JRC flood-depth planning requires {prefix}rp100-only with {prefix}source-manifest.")
        return
    if not source_dir or not assume_units:
        prefix = "--jrc-" if prefixed else "--"
        raise SystemExit(
            f"JRC flood-depth planning requires {prefix}source-dir and {prefix}assume-units m unless --audit-only is set."
        )
    if str(assume_units).strip().lower() != "m":
        prefix = "--jrc-" if prefixed else "--"
        raise SystemExit(f"JRC flood-depth planning requires {prefix}assume-units m.")


def _build_jrc_builder_args(
    args: argparse.Namespace,
    *,
    prefixed: bool = False,
) -> list[str]:
    argv: list[str] = []
    state_attr = "jrc_state" if prefixed else "state"
    source_dir_attr = "jrc_source_dir" if prefixed else "source_dir"
    source_manifest_attr = "jrc_source_manifest" if prefixed else "source_manifest"
    rp100_only_attr = "jrc_rp100_only" if prefixed else "rp100_only"
    assume_units_attr = "jrc_assume_units" if prefixed else "assume_units"
    districts_attr = "jrc_districts_path" if prefixed else "districts_path"
    blocks_attr = "jrc_blocks_path" if prefixed else "blocks_path"
    qa_attr = "jrc_qa_dir" if prefixed else "qa_dir"
    overlay_attr = "jrc_overlay_dir" if prefixed else "overlay_dir"
    if getattr(args, state_attr, None):
        argv.extend(["--state", str(getattr(args, state_attr))])
    if getattr(args, source_manifest_attr, None):
        argv.extend(["--source-manifest", str(getattr(args, source_manifest_attr))])
    if bool(getattr(args, rp100_only_attr, False)):
        argv.append("--rp100-only")
    if getattr(args, source_dir_attr, None):
        argv.extend(["--source-dir", str(getattr(args, source_dir_attr))])
        argv.append("--allow-unversioned-source")
    if getattr(args, assume_units_attr, None):
        argv.extend(["--assume-units", str(getattr(args, assume_units_attr))])
    if getattr(args, districts_attr, None):
        argv.extend(["--districts-path", str(getattr(args, districts_attr))])
    if getattr(args, blocks_attr, None):
        argv.extend(["--blocks-path", str(getattr(args, blocks_attr))])
    if getattr(args, qa_attr, None):
        argv.extend(["--qa-dir", str(getattr(args, qa_attr))])
    if getattr(args, overlay_attr, None):
        argv.extend(["--overlay-dir", str(getattr(args, overlay_attr))])
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
        ]:
            argv = _py_module_cmd(module)
            _append_flag(argv, "--overwrite", bool(args.overwrite))
            plan.append(PlannedCommand(label=label, argv=argv))

        admin_argv = _py_module_cmd("tools.geodata.build_aqueduct_admin_masters")
        _append_flag(admin_argv, "--overwrite", bool(args.overwrite))
        admin_argv.extend(_build_aqueduct_metric_args(args))
        plan.append(PlannedCommand(label="aqueduct-admin-masters", argv=admin_argv))

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


def build_rural_facilities_plan(
    args: argparse.Namespace,
    *,
    include_blocks_geojson: bool = True,
    include_runtime: bool = True,
    runtime_scope: Optional[BundleRuntimeScope] = None,
) -> list[PlannedCommand]:
    """Build the rural facilities exposure prep plan."""
    scope = runtime_scope or _resolve_runtime_scope("rural-facilities", args)
    plan: list[PlannedCommand] = []
    if not bool(getattr(args, "audit_only", False)) and (
        bool(args.overwrite) or scope.runtime_needed or not include_runtime
    ):
        if include_blocks_geojson:
            plan.extend(build_blocks_geojson_plan(args))
        argv = _py_module_cmd("tools.geodata.build_rural_facilities_admin_masters")
        _append_flag(argv, "--overwrite", bool(args.overwrite))
        if getattr(args, "rural_facilities_source_dir", None):
            argv.extend(["--source-dir", str(args.rural_facilities_source_dir)])
        if getattr(args, "rural_facilities_qa_dir", None):
            argv.extend(["--qa-dir", str(args.rural_facilities_qa_dir)])
        if getattr(args, "rural_facilities_overlay_dir", None):
            argv.extend(["--overlay-dir", str(args.rural_facilities_overlay_dir)])
        plan.append(PlannedCommand(label="rural-facilities-admin-masters", argv=argv))
        summary_argv = _py_module_cmd("tools.pipeline.build_admin_exposure_summary")
        from india_resilience_tool.config.paths import get_paths_config

        summary_argv.extend(["--data-dir", str(get_paths_config().data_dir)])
        plan.append(PlannedCommand(label="admin-exposure-summary", argv=summary_argv))
    if include_runtime:
        plan.extend(_build_runtime_plan(args, scope=scope))
    return plan


def build_built_up_area_plan(
    args: argparse.Namespace,
    *,
    include_blocks_geojson: bool = True,
    include_runtime: bool = True,
    runtime_scope: Optional[BundleRuntimeScope] = None,
) -> list[PlannedCommand]:
    """Build the built-up area exposure prep plan."""
    scope = runtime_scope or _resolve_runtime_scope("built-up-area", args)
    plan: list[PlannedCommand] = []
    if bool(getattr(args, "audit_only", False)):
        return [] if bool(getattr(args, "skip_audit", False)) else [_build_audit_step(args, scope.selected_metrics)]

    explicit_builder_input = any(
        getattr(args, attr, None)
        for attr in ("built_up_raster", "built_up_qa_dir", "built_up_overlay_dir")
    )
    if not bool(getattr(args, "audit_only", False)) and (
        bool(args.overwrite) or scope.runtime_needed or explicit_builder_input or not include_runtime
    ):
        if include_blocks_geojson:
            plan.extend(build_blocks_geojson_plan(args))
        argv = _py_module_cmd("tools.geodata.build_built_up_area_admin_masters")
        _append_flag(argv, "--overwrite", bool(args.overwrite))
        if getattr(args, "built_up_raster", None):
            argv.extend(["--raster", str(args.built_up_raster)])
        if getattr(args, "built_up_qa_dir", None):
            argv.extend(["--qa-dir", str(args.built_up_qa_dir)])
        if getattr(args, "built_up_overlay_dir", None):
            argv.extend(["--overlay-dir", str(args.built_up_overlay_dir)])
        plan.append(PlannedCommand(label="built-up-area-admin-masters", argv=argv))
        if include_runtime and not bool(getattr(args, "skip_optimised", False)):
            plan.append(_build_optimised_step(args, _select_metrics_for_execution(scope)))
        summary_argv = _py_module_cmd("tools.pipeline.build_admin_exposure_summary")
        from india_resilience_tool.config.paths import get_paths_config

        summary_argv.extend(["--data-dir", str(get_paths_config().data_dir)])
        plan.append(PlannedCommand(label="admin-exposure-summary", argv=summary_argv))
        if include_runtime and not bool(getattr(args, "skip_audit", False)):
            plan.append(_build_audit_step(args, scope.selected_metrics))
    elif include_runtime:
        plan.extend(_build_runtime_plan(args, scope=scope))
    return plan


def build_lulc_plan(
    args: argparse.Namespace,
    *,
    include_blocks_geojson: bool = True,
    include_runtime: bool = True,
    runtime_scope: Optional[BundleRuntimeScope] = None,
) -> list[PlannedCommand]:
    """Build the agricultural LULC exposure prep plan."""
    scope = runtime_scope or _resolve_runtime_scope("lulc", args)
    plan: list[PlannedCommand] = []
    if bool(getattr(args, "audit_only", False)):
        return [] if bool(getattr(args, "skip_audit", False)) else [_build_audit_step(args, scope.selected_metrics)]

    explicit_builder_input = any(
        getattr(args, attr, None)
        for attr in ("lulc_raster", "lulc_qa_dir", "lulc_overlay_dir")
    ) or any(
        bool(getattr(args, attr, False))
        for attr in ("lulc_allow_total_outlier", "lulc_allow_unexpected_values", "lulc_allow_share_outlier")
    )
    if bool(args.overwrite) or scope.runtime_needed or explicit_builder_input or not include_runtime:
        if include_blocks_geojson:
            plan.extend(build_blocks_geojson_plan(args))
        argv = _py_module_cmd("tools.geodata.build_lulc_admin_masters")
        _append_flag(argv, "--overwrite", bool(args.overwrite))
        if getattr(args, "lulc_raster", None):
            argv.extend(["--raster", str(args.lulc_raster)])
        if getattr(args, "lulc_qa_dir", None):
            argv.extend(["--qa-dir", str(args.lulc_qa_dir)])
        if getattr(args, "lulc_overlay_dir", None):
            argv.extend(["--overlay-dir", str(args.lulc_overlay_dir)])
        _append_flag(argv, "--allow-total-outlier", bool(getattr(args, "lulc_allow_total_outlier", False)))
        _append_flag(argv, "--allow-unexpected-values", bool(getattr(args, "lulc_allow_unexpected_values", False)))
        _append_flag(argv, "--allow-share-outlier", bool(getattr(args, "lulc_allow_share_outlier", False)))
        plan.append(PlannedCommand(label="lulc-admin-masters", argv=argv))
        if include_runtime and not bool(getattr(args, "skip_optimised", False)):
            plan.append(_build_optimised_step(args, _select_metrics_for_execution(scope)))
        summary_argv = _py_module_cmd("tools.pipeline.build_admin_exposure_summary")
        from india_resilience_tool.config.paths import get_paths_config

        summary_argv.extend(["--data-dir", str(get_paths_config().data_dir)])
        plan.append(PlannedCommand(label="admin-exposure-summary", argv=summary_argv))
        if include_runtime and not bool(getattr(args, "skip_audit", False)):
            plan.append(_build_audit_step(args, scope.selected_metrics))
    elif include_runtime:
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


def build_jrc_flood_depth_plan(
    args: argparse.Namespace,
    *,
    include_blocks_geojson: bool = True,
    include_runtime: bool = True,
    runtime_scope: Optional[BundleRuntimeScope] = None,
) -> list[PlannedCommand]:
    """Build the JRC flood-depth admin prep plan for the selected state."""
    require_source = not bool(getattr(args, "audit_only", False))
    _validate_jrc_inputs(args, require_source=require_source)
    scope = runtime_scope or _resolve_runtime_scope("jrc-flood-depth", args, levels=("district", "block"))
    plan: list[PlannedCommand] = []
    if bool(getattr(args, "audit_only", False)):
        return [] if bool(getattr(args, "skip_audit", False)) else [_build_audit_step(args, scope.selected_metrics)]

    # A non-default --state, an explicit source/QA/overlay/boundary override, or
    # --overwrite means the operator is requesting a (re)build for a specific
    # state. In that case the parity audit (computed pre-build, often against the
    # Telangana pilot) may report nothing pending, so we must force both the
    # builder *and* a same-run optimized publish + audit over the full JRC metric
    # set -- otherwise new state masters would be built but never published.
    explicit_builder_input = any(
        getattr(args, attr, None)
        for attr in ("source_dir", "qa_dir", "overlay_dir", "districts_path", "blocks_path")
    ) or (str(getattr(args, "state", "") or "").strip().casefold() not in ("", "telangana"))

    if bool(args.overwrite) or scope.runtime_needed or explicit_builder_input or not include_runtime:
        # CHG-0065 / CHG-0093: the JRC builder only *reads* the canonical blocks
        # GeoJSON. Rebuilding it here regenerates the shared, pipeline-wide block
        # boundary via the superseded build_blocks_geojson (legacy GHS-WUP source,
        # pre-LGD spellings) and would clobber the LGD-aligned boundary as a side
        # effect of a JRC --overwrite. Schedule blocks-geojson only when the
        # canonical file is genuinely missing; never force it from --overwrite.
        from india_resilience_tool.config.paths import get_paths_config
        blocks_for_build = getattr(args, "blocks_path", None) or str(get_paths_config().blocks_path)
        if include_blocks_geojson and not Path(blocks_for_build).exists():
            plan.extend(build_blocks_geojson_plan(args))
        argv = _py_module_cmd("tools.geodata.build_jrc_flood_depth_admin_masters")
        argv.extend(_build_jrc_builder_args(args))
        _append_flag(argv, "--overwrite", bool(args.overwrite))
        plan.append(PlannedCommand(label="jrc-flood-depth-admin-masters", argv=argv))
        # Build the Riverine Flood composite master(s) from the JRC metric masters built
        # immediately above, BEFORE the optimized publish + audit. The composite has no
        # compute stage of its own; without this the optimized build packages and the audit
        # checks a stale/empty composite_flood_jrc_depth master and the run fails. Emitted
        # within this build branch independent of include_runtime so the merged
        # dashboard-package runtime pass also consumes a valid composite master. Forced
        # overwrite: derived data must always track its just-built component masters.
        from india_resilience_tool.config.composite_metrics import is_composite_metric

        composite_slugs = [m for m in scope.selected_metrics if is_composite_metric(m)]
        if composite_slugs:
            # The JRC subcommand's --state is a single canonical name that may itself
            # contain commas (the UT "Dadra, Nagar Haveli, Daman & Diu"). Pass it whole;
            # _append_repeat_literal emits it without the CSV comma-splitting that would
            # otherwise fracture it into phantom states with no source masters (failing
            # the parity audit). (… or "").strip() or DEFAULT_ADMIN_STATE reproduces the
            # old empty->[DEFAULT_ADMIN_STATE] fallback.
            jrc_state = (getattr(args, "state", None) or "").strip() or DEFAULT_ADMIN_STATE
            plan.extend(
                _build_composite_master_steps(
                    args,
                    levels=("district", "block"),
                    admin_states=[jrc_state],
                    scope=scope,
                    metrics=composite_slugs,
                    force_overwrite=True,
                )
            )
        # Publish the full JRC metric set (not the pre-build pending subset), so a
        # newly built state is always reflected in the optimized runtime + audit.
        publish_metrics = scope.selected_metrics
        if include_runtime and not bool(getattr(args, "skip_optimised", False)):
            plan.append(_build_optimised_step(args, publish_metrics, overwrite=False))
        if include_runtime and not bool(getattr(args, "skip_audit", False)):
            plan.append(_build_audit_step(args, publish_metrics))
    elif include_runtime:
        plan.extend(_build_runtime_plan(args, scope=scope, overwrite_optimised=False))
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
    extra_metrics: Sequence[str] | None = None,
    extra_runtime_needed: bool = False,
) -> list[PlannedCommand]:
    selected_metrics = _dedupe_keep_order(list(scope.selected_metrics) + list(extra_metrics or ()))
    if bool(getattr(args, "audit_only", False)):
        return [] if bool(getattr(args, "skip_audit", False)) else [
            _build_audit_step(args, selected_metrics, levels=scope.levels)
        ]

    plan: list[PlannedCommand] = []
    if not bool(getattr(args, "skip_optimised", False)):
        if bool(getattr(args, "overwrite", False)):
            if scope.selected_metrics or scope.has_global_issues:
                label_levels = "+".join(scope.levels)
                plan.append(
                    _build_optimised_step(
                        args,
                        selected_metrics,
                        levels=scope.levels,
                        label=f"processed-optimised-build:{label_levels}",
                    )
                )
        else:
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
                        _dedupe_keep_order(list(pending_metrics) + list(extra_metrics or ())),
                        levels=grouped_levels,
                        label=f"processed-optimised-build:{label_levels}",
                    )
                )
        if extra_runtime_needed and not plan and extra_metrics:
            admin_levels = [level for level in scope.levels if level in {"district", "block"}]
            if admin_levels:
                label_levels = "+".join(admin_levels)
                plan.append(
                    _build_optimised_step(
                        args,
                        extra_metrics,
                        levels=admin_levels,
                        label=f"processed-optimised-build:{label_levels}",
                    )
                )

    if not bool(getattr(args, "skip_audit", False)):
        plan.append(
            _build_audit_step(
                args,
                selected_metrics,
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
    from india_resilience_tool.config.paths import get_paths_config

    levels = _resolve_levels(str(args.level))
    admin_states = _resolve_admin_states(getattr(args, "state", None))
    scope = runtime_scope or _resolve_climate_runtime_scope(args, levels=levels)
    composite_scope = _resolve_composite_runtime_scope(
        levels=levels,
        admin_states=admin_states,
        data_dir=get_paths_config().data_dir,
        overwrite=bool(getattr(args, "overwrite", False)),
    )

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
            plan.extend(
                _build_composite_master_steps(
                    args,
                    levels=levels,
                    admin_states=admin_states,
                    scope=composite_scope,
                )
            )

    if include_runtime:
        plan.extend(
            _build_climate_runtime_plan(
                args,
                scope=scope,
                extra_metrics=composite_scope.selected_metrics if composite_scope.selected_metrics else (),
                extra_runtime_needed=composite_scope.runtime_needed,
            )
        )
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
    from india_resilience_tool.config.paths import get_paths_config

    if bool(getattr(args, "include_jrc_flood_depth", False)):
        _validate_jrc_inputs(
            args,
            prefixed=True,
            require_source=not bool(getattr(args, "audit_only", False)),
        )
    climate_levels = _resolve_levels(str(args.level))
    climate_scope = _resolve_climate_runtime_scope(
        args,
        levels=climate_levels,
    )
    composite_scope = _resolve_composite_runtime_scope(
        levels=climate_levels,
        admin_states=_resolve_admin_states(getattr(args, "state", None)),
        data_dir=get_paths_config().data_dir,
        overwrite=bool(getattr(args, "overwrite", False)),
    )
    proposal_scope = _resolve_proposal_runtime_scope(
        levels=climate_levels,
        admin_states=_resolve_admin_states(getattr(args, "state", None)),
        data_dir=get_paths_config().data_dir,
        overwrite=bool(getattr(args, "overwrite", False)),
    )
    aqueduct_scope = _resolve_runtime_scope("aqueduct", args)
    population_scope = _resolve_runtime_scope("population-exposure", args)
    built_up_scope = _resolve_runtime_scope("built-up-area", args)
    lulc_scope = _resolve_runtime_scope("lulc", args)
    rural_facilities_scope = (
        _resolve_runtime_scope("rural-facilities", args)
        if bool(getattr(args, "include_rural_facilities", False))
        else BundleRuntimeScope(selected_metrics=[], pending_metrics=[], has_global_issues=False)
    )
    groundwater_scope = _resolve_runtime_scope("groundwater", args)
    jrc_scope = (
        _resolve_runtime_scope("jrc-flood-depth", args, levels=("district", "block"))
        if bool(getattr(args, "include_jrc_flood_depth", False))
        else BundleRuntimeScope(selected_metrics=[], pending_metrics=[], has_global_issues=False)
    )

    package_scope = BundleRuntimeScope(
        selected_metrics=_dedupe_keep_order(
            _resolve_bundle_metrics("dashboard-package", args)
            + composite_scope.selected_metrics
            + proposal_scope.selected_metrics
        ),
        pending_metrics=_dedupe_keep_order(
            climate_scope.pending_metrics
            + composite_scope.pending_metrics
            + proposal_scope.pending_metrics
            + aqueduct_scope.pending_metrics
            + population_scope.pending_metrics
            + built_up_scope.pending_metrics
            + lulc_scope.pending_metrics
            + rural_facilities_scope.pending_metrics
            + groundwater_scope.pending_metrics
            + jrc_scope.pending_metrics
        ),
        has_global_issues=(
            climate_scope.has_global_issues
            or proposal_scope.has_global_issues
            or aqueduct_scope.has_global_issues
            or population_scope.has_global_issues
            or built_up_scope.has_global_issues
            or lulc_scope.has_global_issues
            or rural_facilities_scope.has_global_issues
            or groundwater_scope.has_global_issues
            or jrc_scope.has_global_issues
        ),
    )

    if bool(getattr(args, "audit_only", False)):
        return _build_runtime_plan(args, scope=package_scope)

    climate_plan = build_climate_hazards_plan(args, include_runtime=False, runtime_scope=climate_scope)
    proposal_plan = _build_proposal_bundle_steps(
        args,
        levels=climate_levels,
        admin_states=_resolve_admin_states(getattr(args, "state", None)),
        scope=proposal_scope,
    )
    aqueduct_plan = build_aqueduct_plan(args, include_blocks_geojson=False, include_runtime=False, runtime_scope=aqueduct_scope)
    population_plan = build_population_plan(args, include_blocks_geojson=False, include_runtime=False, runtime_scope=population_scope)
    built_up_plan = build_built_up_area_plan(args, include_blocks_geojson=False, include_runtime=False, runtime_scope=built_up_scope)
    lulc_plan = build_lulc_plan(args, include_blocks_geojson=False, include_runtime=False, runtime_scope=lulc_scope)
    rural_facilities_plan = (
        build_rural_facilities_plan(args, include_blocks_geojson=False, include_runtime=False, runtime_scope=rural_facilities_scope)
        if bool(getattr(args, "include_rural_facilities", False))
        else []
    )
    groundwater_plan = build_groundwater_plan(args, include_runtime=False, runtime_scope=groundwater_scope)
    # vars(args) already carries `state` (the climate admin state, possibly a
    # comma-list), so the JRC overrides must be applied via dict-update to avoid a
    # duplicate-keyword TypeError. JRC takes a single state; fall back to Telangana
    # (the historical pilot) when --jrc-state is not given, rather than reusing the
    # climate --state.
    jrc_ns_kwargs = dict(vars(args))
    jrc_ns_kwargs.update(
        state=getattr(args, "jrc_state", None) or "Telangana",
        source_manifest=getattr(args, "jrc_source_manifest", None),
        rp100_only=bool(getattr(args, "jrc_rp100_only", False)),
        source_dir=getattr(args, "jrc_source_dir", None),
        assume_units=getattr(args, "jrc_assume_units", None),
        districts_path=getattr(args, "jrc_districts_path", None),
        blocks_path=getattr(args, "jrc_blocks_path", None),
        qa_dir=getattr(args, "jrc_qa_dir", None),
        overlay_dir=getattr(args, "jrc_overlay_dir", None),
    )
    jrc_plan = (
        build_jrc_flood_depth_plan(
            argparse.Namespace(**jrc_ns_kwargs),
            include_blocks_geojson=False,
            include_runtime=False,
            runtime_scope=jrc_scope,
        )
        if bool(getattr(args, "include_jrc_flood_depth", False))
        else []
    )

    plan: list[PlannedCommand] = []
    if aqueduct_plan or population_plan or built_up_plan or lulc_plan or rural_facilities_plan or jrc_plan:
        plan.extend(build_blocks_geojson_plan(args))
    plan.extend(climate_plan)
    plan.extend(proposal_plan)
    plan.extend(aqueduct_plan)
    plan.extend(population_plan)
    plan.extend(built_up_plan)
    plan.extend(lulc_plan)
    plan.extend(rural_facilities_plan)
    plan.extend(groundwater_plan)
    plan.extend(jrc_plan)
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
        "aqueduct-validate": "tools.geodata.validate_aqueduct_workflow",
        "population-admin-masters": "tools.geodata.build_population_admin_masters",
        "rural-facilities-admin-masters": "tools.geodata.build_rural_facilities_admin_masters",
        "built-up-area-admin-masters": "tools.geodata.build_built_up_area_admin_masters",
        "lulc-admin-masters": "tools.geodata.build_lulc_admin_masters",
        "groundwater-district-masters": "tools.geodata.build_groundwater_district_masters",
        "jrc-flood-depth-admin-masters": "tools.geodata.build_jrc_flood_depth_admin_masters",
    }
    if step in module_map:
        argv = _py_module_cmd(module_map[step])
        _append_flag(argv, "--overwrite", bool(args.overwrite))
        if step.startswith("aqueduct-"):
            argv.extend(_build_aqueduct_metric_args(args))
        if step == "population-admin-masters" and getattr(args, "population_raster", None):
            argv.extend(["--raster", str(args.population_raster)])
        if step == "rural-facilities-admin-masters":
            if getattr(args, "rural_facilities_source_dir", None):
                argv.extend(["--source-dir", str(args.rural_facilities_source_dir)])
            if getattr(args, "rural_facilities_qa_dir", None):
                argv.extend(["--qa-dir", str(args.rural_facilities_qa_dir)])
            if getattr(args, "rural_facilities_overlay_dir", None):
                argv.extend(["--overlay-dir", str(args.rural_facilities_overlay_dir)])
        if step == "built-up-area-admin-masters":
            if getattr(args, "built_up_raster", None):
                argv.extend(["--raster", str(args.built_up_raster)])
            if getattr(args, "built_up_qa_dir", None):
                argv.extend(["--qa-dir", str(args.built_up_qa_dir)])
            if getattr(args, "built_up_overlay_dir", None):
                argv.extend(["--overlay-dir", str(args.built_up_overlay_dir)])
        if step == "lulc-admin-masters":
            if getattr(args, "lulc_raster", None):
                argv.extend(["--raster", str(args.lulc_raster)])
            if getattr(args, "lulc_qa_dir", None):
                argv.extend(["--qa-dir", str(args.lulc_qa_dir)])
            if getattr(args, "lulc_overlay_dir", None):
                argv.extend(["--overlay-dir", str(args.lulc_overlay_dir)])
            _append_flag(argv, "--allow-total-outlier", bool(getattr(args, "lulc_allow_total_outlier", False)))
            _append_flag(argv, "--allow-unexpected-values", bool(getattr(args, "lulc_allow_unexpected_values", False)))
            _append_flag(argv, "--allow-share-outlier", bool(getattr(args, "lulc_allow_share_outlier", False)))
        if step == "groundwater-district-masters":
            if getattr(args, "groundwater_workbook", None):
                argv.extend(["--workbook", str(args.groundwater_workbook)])
            if getattr(args, "groundwater_alias_csv", None):
                argv.extend(["--district-alias-csv", str(args.groundwater_alias_csv)])
        if step == "jrc-flood-depth-admin-masters":
            _validate_jrc_inputs(args, require_source=True)
            argv.extend(_build_jrc_builder_args(args))
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
    if command == "rural-facilities":
        return build_rural_facilities_plan(args, include_blocks_geojson=True, include_runtime=True)
    if command == "built-up-area":
        return build_built_up_area_plan(args, include_blocks_geojson=True, include_runtime=True)
    if command == "lulc":
        return build_lulc_plan(args, include_blocks_geojson=True, include_runtime=True)
    if command == "groundwater":
        return build_groundwater_plan(args, include_runtime=True)
    if command == "jrc-flood-depth":
        return build_jrc_flood_depth_plan(args, include_runtime=True)
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
    print("  rural-facilities")
    print("  built-up-area")
    print("  lulc")
    print("  groundwater")
    print("  jrc-flood-depth")
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
        "aqueduct-validate",
        "population-admin-masters",
        "rural-facilities-admin-masters",
        "built-up-area-admin-masters",
        "lulc-admin-masters",
        "groundwater-district-masters",
        "jrc-flood-depth-admin-masters",
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


def _add_rural_facilities_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--rural-facilities-source-dir",
        default=None,
        help="Optional override path to the rural facilities shapefile directory.",
    )
    parser.add_argument(
        "--rural-facilities-qa-dir",
        default=None,
        help="Optional override directory for rural facilities QA outputs.",
    )
    parser.add_argument(
        "--rural-facilities-overlay-dir",
        default=None,
        help="Optional override directory for rural facilities overlay artifacts.",
    )


def _add_built_up_area_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--built-up-raster",
        default=None,
        help="Optional override path to Cleaned_India_Built_Surface_WGS84.tif.",
    )
    parser.add_argument(
        "--built-up-qa-dir",
        default=None,
        help="Optional override directory for built-up area QA outputs.",
    )
    parser.add_argument(
        "--built-up-overlay-dir",
        default=None,
        help="Optional override directory for built-up area overlay artifacts.",
    )


def _add_lulc_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--lulc-raster",
        default=None,
        help="Optional override path to LULC_2_Agri.tif.",
    )
    parser.add_argument(
        "--lulc-qa-dir",
        default=None,
        help="Optional override directory for agricultural LULC QA outputs.",
    )
    parser.add_argument(
        "--lulc-overlay-dir",
        default=None,
        help="Optional override directory for agricultural LULC overlay artifacts.",
    )
    parser.add_argument(
        "--lulc-allow-total-outlier",
        action="store_true",
        help="Allow agricultural LULC national total outside the default guardrail range.",
    )
    parser.add_argument(
        "--lulc-allow-unexpected-values",
        action="store_true",
        help="Allow LULC raster values outside {0, 1}.",
    )
    parser.add_argument(
        "--lulc-allow-share-outlier",
        action="store_true",
        help="Allow district/block agricultural LULC shares above 100.01%%.",
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
    parser.add_argument(
        "--spi-legacy",
        action="store_true",
        help="Accepted for compatibility but rejected because legacy SPI is non-conformant.",
    )
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

    p_rural = subparsers.add_parser("rural-facilities", help="Prepare the rural facilities exposure dashboard bundle.")
    _add_common_runner_flags(p_rural, include_runtime_controls=True)
    _add_rural_facilities_flags(p_rural)

    p_built = subparsers.add_parser("built-up-area", help="Prepare the built-up area exposure dashboard bundle.")
    _add_common_runner_flags(p_built, include_runtime_controls=True)
    _add_built_up_area_flags(p_built)

    p_lulc = subparsers.add_parser("lulc", help="Prepare the agricultural LULC exposure dashboard bundle.")
    _add_common_runner_flags(p_lulc, include_runtime_controls=True)
    _add_lulc_flags(p_lulc)

    p_groundwater = subparsers.add_parser("groundwater", help="Prepare the groundwater dashboard bundle.")
    _add_common_runner_flags(p_groundwater, include_runtime_controls=True)
    _add_groundwater_flags(p_groundwater)

    p_jrc = subparsers.add_parser(
        "jrc-flood-depth",
        help=(
            "Prepare the JRC flood-depth dashboard bundle for --state (default Telangana). "
            "Note: blocks-geojson is rebuilt only when the canonical blocks file is missing "
            "or --overwrite is passed; with --overwrite it regenerates the pipeline-wide "
            "canonical blocks GeoJSON as a side-effect (see tools/README.md)."
        ),
    )
    _add_common_runner_flags(p_jrc, include_runtime_controls=True)
    _add_jrc_flags(p_jrc, prefixed=False)

    p_pkg = subparsers.add_parser("dashboard-package", help="Prepare all dashboard bundles end to end.")
    _add_common_runner_flags(p_pkg, include_runtime_controls=True)
    _add_climate_flags(p_pkg)
    _add_aqueduct_flags(p_pkg, bundle=True)
    _add_population_flags(p_pkg)
    _add_built_up_area_flags(p_pkg)
    _add_lulc_flags(p_pkg)
    _add_rural_facilities_flags(p_pkg)
    _add_groundwater_flags(p_pkg)
    _add_jrc_flags(p_pkg, prefixed=True)
    p_pkg.add_argument("--include-rural-facilities", action="store_true", help="Include the rural facilities exposure bundle.")
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
        "aqueduct-validate",
        "population-admin-masters",
        "rural-facilities-admin-masters",
        "built-up-area-admin-masters",
        "lulc-admin-masters",
        "groundwater-district-masters",
        "jrc-flood-depth-admin-masters",
    ]:
        sub = subparsers.add_parser(name, help=f"Run the `{name}` step only.")
        _add_common_runner_flags(sub)
        if name == "population-admin-masters":
            _add_population_flags(sub)
        elif name == "rural-facilities-admin-masters":
            _add_rural_facilities_flags(sub)
        elif name == "built-up-area-admin-masters":
            _add_built_up_area_flags(sub)
        elif name == "lulc-admin-masters":
            _add_lulc_flags(sub)
        elif name == "groundwater-district-masters":
            _add_groundwater_flags(sub)
        elif name == "jrc-flood-depth-admin-masters":
            _add_jrc_flags(sub, prefixed=False)
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
    if bool(getattr(args, "spi_legacy", False)):
        print(
            "Legacy SPI z-score is non-conformant with WMO SPI methodology; rerun without --spi-legacy.",
            file=sys.stderr,
        )
        return 2
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
            diagnostics = _collect_climate_failure_diagnostics(args, post_scope)
            if diagnostics:
                print("POST-RUN CLIMATE FAILURE DETAILS")
                for message in diagnostics:
                    print(f"- {message}")
            return 1
        return rc
    plan = build_command_plan(args)
    return execute_plan(plan, dry_run=bool(args.dry_run), plan_only=bool(args.plan_only))


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Canonical workflow runner for common IRT dashboard-prep tasks.

This CLI is orchestration-only: it shells out to the existing `python -m ...`
tools so operators do not need to memorize long command sequences.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from dataclasses import dataclass
from typing import Sequence


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
LEVEL_GROUPS = {
    "all": ["district", "block", "basin", "sub_basin"],
    "admin": ["district", "block"],
    "hydro": ["basin", "sub_basin"],
    "district": ["district"],
    "block": ["block"],
    "basin": ["basin"],
    "sub_basin": ["sub_basin"],
}


@dataclass(frozen=True)
class PlannedCommand:
    """One concrete command that the runner can execute."""

    label: str
    argv: list[str]


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


def _resolve_levels(level: str) -> list[str]:
    try:
        return list(LEVEL_GROUPS[level])
    except KeyError as exc:
        raise SystemExit(f"Unsupported level selection: {level}") from exc


def _resolve_admin_states(state_args: Sequence[str] | None) -> list[str]:
    states = _split_csv_values(state_args)
    return states or [DEFAULT_ADMIN_STATE]


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


def build_aqueduct_plan(args: argparse.Namespace, *, include_blocks_geojson: bool = True) -> list[PlannedCommand]:
    plan: list[PlannedCommand] = []
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

    return plan


def build_population_plan(args: argparse.Namespace, *, include_blocks_geojson: bool = True) -> list[PlannedCommand]:
    """Build the population exposure prep plan."""
    plan: list[PlannedCommand] = []
    if include_blocks_geojson:
        plan.extend(build_blocks_geojson_plan(args))
    argv = _py_module_cmd("tools.geodata.build_population_admin_masters")
    _append_flag(argv, "--overwrite", bool(args.overwrite))
    if getattr(args, "population_raster", None):
        argv.extend(["--raster", str(args.population_raster)])
    plan.append(PlannedCommand(label="population-admin-masters", argv=argv))
    return plan


def build_groundwater_plan(args: argparse.Namespace) -> list[PlannedCommand]:
    """Build the groundwater district prep plan."""
    argv = _py_module_cmd("tools.geodata.build_groundwater_district_masters")
    _append_flag(argv, "--overwrite", bool(args.overwrite))
    if getattr(args, "groundwater_workbook", None):
        argv.extend(["--workbook", str(args.groundwater_workbook)])
    if getattr(args, "groundwater_alias_csv", None):
        argv.extend(["--district-alias-csv", str(args.groundwater_alias_csv)])
    return [PlannedCommand(label="groundwater-district-masters", argv=argv)]


def build_climate_hazards_plan(args: argparse.Namespace) -> list[PlannedCommand]:
    plan: list[PlannedCommand] = []
    levels = _resolve_levels(str(args.level))
    admin_states = _resolve_admin_states(getattr(args, "state", None))

    for level in levels:
        states_for_level = admin_states if level in {"district", "block"} else [""]
        for state_name in states_for_level:
            argv = _py_module_cmd("tools.pipeline.compute_indices_multiprocess")
            argv.extend(["--level", level])
            if state_name:
                argv.extend(["--state", state_name])
            _append_multi(argv, "--metrics", getattr(args, "metrics", None))
            _append_multi(argv, "--models", getattr(args, "models", None))
            _append_multi(argv, "--scenarios", getattr(args, "scenarios", None))
            if getattr(args, "workers", None) is not None:
                argv.extend(["--workers", str(args.workers)])
            _append_flag(argv, "--verbose", bool(args.verbose))
            _append_flag(argv, "--spi-legacy", bool(args.spi_legacy))
            if getattr(args, "spi_distribution", None):
                argv.extend(["--spi-distribution", str(args.spi_distribution)])
            label = f"climate-compute:{level}"
            if state_name:
                label = f"{label}:{state_name}"
            plan.append(PlannedCommand(label=label, argv=argv))

    if not bool(getattr(args, "skip_masters", False)):
        for level in levels:
            argv = _py_module_cmd("tools.pipeline.build_master_metrics")
            argv.extend(["--level", level])
            if level in {"district", "block"}:
                argv.extend(["--state", ",".join(admin_states)])
            _append_multi(argv, "--metrics", getattr(args, "metrics", None))
            if getattr(args, "workers", None) is not None:
                argv.extend(["--workers", str(args.workers)])
            if not bool(args.verbose):
                argv.append("--quiet")
            plan.append(PlannedCommand(label=f"climate-masters:{level}", argv=argv))

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
                skip_masters=True,
            )
        )

    if step == "climate-masters":
        levels = _resolve_levels(str(args.level))
        return build_climate_hazards_plan(
            argparse.Namespace(
                level=args.level,
                state=args.state,
                metrics=args.metrics,
                workers=args.workers,
                verbose=args.verbose,
                skip_masters=False,
                models=None,
                scenarios=None,
                spi_legacy=False,
                spi_distribution=None,
            )
        )[-len(levels):]

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
    if command == "list":
        return []
    if command == "aqueduct":
        return build_aqueduct_plan(args, include_blocks_geojson=True)
    if command == "climate-hazards":
        return build_climate_hazards_plan(args)
    if command == "population-exposure":
        return build_population_plan(args, include_blocks_geojson=True)
    if command == "groundwater":
        return build_groundwater_plan(args)
    if command == "dashboard-package":
        plan = build_blocks_geojson_plan(args)
        plan.extend(build_climate_hazards_plan(args))
        plan.extend(build_aqueduct_plan(args, include_blocks_geojson=False))
        plan.extend(build_population_plan(args, include_blocks_geojson=False))
        plan.extend(build_groundwater_plan(args))
        if bool(getattr(args, "include_pytest", False)):
            plan.append(
                PlannedCommand(
                    label="pytest-validation",
                    argv=[sys.executable, "-m", "pytest", "-q", *DEFAULT_VALIDATION_TESTS],
                )
            )
        return plan
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


def execute_plan(plan: Sequence[PlannedCommand], *, dry_run: bool) -> int:
    if not plan:
        return 0
    print("PREPARE DASHBOARD RUN")
    print(f"steps: {len(plan)}")
    for idx, step in enumerate(plan, start=1):
        rendered = shlex.join(step.argv)
        prefix = "DRY-RUN" if dry_run else "RUN"
        print(f"[{idx}/{len(plan)}] {prefix} {step.label}")
        print(f"  {rendered}")
        if dry_run:
            continue
        subprocess.run(step.argv, check=True)
    return 0


def _add_common_runner_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")
    parser.add_argument("--overwrite", action="store_true", help="Pass through --overwrite where supported.")


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
    parser.add_argument("--skip-masters", action="store_true", help="Skip the climate master build step.")


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run common IRT dashboard-prep workflows without memorizing long command sequences."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_list = subparsers.add_parser("list", help="List available workflow bundles and step commands.")
    _add_common_runner_flags(p_list)

    p_aq = subparsers.add_parser("aqueduct", help="Run the full Aqueduct onboarding workflow.")
    _add_common_runner_flags(p_aq)
    _add_aqueduct_flags(p_aq, bundle=True)

    p_climate = subparsers.add_parser("climate-hazards", help="Run climate hazard compute + master workflows.")
    _add_common_runner_flags(p_climate)
    _add_climate_flags(p_climate)

    p_population = subparsers.add_parser("population-exposure", help="Build district/block population exposure masters.")
    _add_common_runner_flags(p_population)
    _add_population_flags(p_population)

    p_groundwater = subparsers.add_parser("groundwater", help="Build district groundwater assessment masters.")
    _add_common_runner_flags(p_groundwater)
    _add_groundwater_flags(p_groundwater)

    p_pkg = subparsers.add_parser("dashboard-package", help="Run climate hazards, Aqueduct, population exposure, and groundwater prep for the dashboard.")
    _add_common_runner_flags(p_pkg)
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
    p_compute.set_defaults(skip_masters=True)

    p_masters = subparsers.add_parser("climate-masters", help="Run climate master builds only.")
    _add_common_runner_flags(p_masters)
    _add_climate_flags(p_masters)
    p_masters.set_defaults(skip_masters=False)

    p_pytest = subparsers.add_parser("pytest-validation", help="Run the default validation pytest target set.")
    _add_common_runner_flags(p_pytest)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)
    if str(args.command) == "list":
        _print_available_commands()
        return 0
    plan = build_command_plan(args)
    return execute_plan(plan, dry_run=bool(args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())

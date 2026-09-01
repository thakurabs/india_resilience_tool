"""Shared lightweight CLI helpers for the climate compute bootstrap/runtime."""

from __future__ import annotations

import argparse
from multiprocessing import cpu_count
from pathlib import Path
from typing import Optional, Sequence

from india_resilience_tool.config.paths import get_paths_config


LIGHTWEIGHT_SCENARIOS = {
    "historical": {"subdir": "historical/tas"},
    "ssp245": {"subdir": "ssp245/tas"},
    "ssp585": {"subdir": "ssp585/tas"},
}
YEARLY_CLEANUP_POLICIES = ("default", "preserve", "delete_after_ensemble")


def default_workers_75pct() -> int:
    """Return the historical default worker count used by the compute CLI."""
    return max(1, int(cpu_count() * 0.75))


def build_parser(*, default_workers: Optional[int] = None) -> argparse.ArgumentParser:
    """Build the shared compute parser without importing the heavy runtime stack."""
    workers_default = int(default_workers or default_workers_75pct())
    parser = argparse.ArgumentParser(description="IRT Climate Index Pipeline (Multiprocess)")
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=workers_default,
        help=f"Number of worker processes (default: {workers_default})",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose/debug logging")
    parser.add_argument(
        "-l",
        "--level",
        choices=["district", "block", "both"],
        default="both",
        help="Spatial level for aggregation (default: both = district + block)",
    )
    parser.add_argument("-s", "--state", default="Telangana", help="State to process (default: Telangana)")
    parser.add_argument("--metrics", nargs="+", help="Filter to specific metric slugs")
    parser.add_argument("--models", nargs="+", help="Filter to specific models")
    parser.add_argument("--scenarios", nargs="+", help="Filter to specific scenarios")
    parser.add_argument(
        "--yearly-cleanup-policy",
        choices=YEARLY_CLEANUP_POLICIES,
        default="default",
        help=(
            "Per-model yearly CSV cleanup policy. default preserves district yearly CSVs "
            "and deletes block yearly CSVs after ensembles; preserve keeps yearly CSVs for "
            "all levels; delete_after_ensemble is valid only for block runs."
        ),
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip compute tasks with validated completion markers and intact outputs.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete the selected compute outputs and markers before rebuilding.",
    )
    parser.add_argument("--list-metrics", action="store_true", help="List available metrics and exit")
    parser.add_argument("--list-models", action="store_true", help="List discovered models and exit")
    parser.add_argument(
        "--spi-legacy",
        action="store_true",
        help="Accepted for compatibility but rejected because legacy SPI is non-conformant.",
    )
    parser.add_argument(
        "--spi-distribution",
        choices=["gamma", "pearson"],
        default="gamma",
        help="Distribution for SPI fitting when using climate-indices package (default: gamma)",
    )
    return parser


def levels_for_cli_value(level: str) -> tuple[str, ...]:
    """Return concrete compute levels represented by one CLI --level value."""
    return ("district", "block") if str(level).strip() == "both" else (str(level).strip(),)


def effective_yearly_cleanup_policy(level: str, requested: str = "default") -> str:
    """Resolve the yearly cleanup policy for one concrete compute level."""
    requested_norm = str(requested or "default").strip()
    level_norm = str(level).strip()
    if requested_norm == "default":
        return "delete_after_ensemble" if level_norm == "block" else "preserve"
    return requested_norm


def effective_yearly_cleanup_policies(level: str, requested: str = "default") -> dict[str, str]:
    """Return effective cleanup policies keyed by concrete level for a CLI request."""
    return {
        concrete_level: effective_yearly_cleanup_policy(concrete_level, requested)
        for concrete_level in levels_for_cli_value(level)
    }


def validate_yearly_cleanup_policy_args(
    args: argparse.Namespace,
    *,
    parser: argparse.ArgumentParser,
) -> None:
    """Reject cleanup-policy combinations that cannot be honored safely."""
    requested = str(getattr(args, "yearly_cleanup_policy", "default") or "default").strip()
    levels = levels_for_cli_value(str(getattr(args, "level", "")).strip())
    if requested == "delete_after_ensemble" and any(level != "block" for level in levels):
        parser.error("--yearly-cleanup-policy delete_after_ensemble is valid only with --level block")


def parse_args(argv: Optional[Sequence[str]] = None, *, default_workers: Optional[int] = None) -> argparse.Namespace:
    """Parse CLI args using the shared parser."""
    parser = build_parser(default_workers=default_workers)
    args = parser.parse_args(argv)
    validate_yearly_cleanup_policy_args(args, parser=parser)
    return args


def format_effective_yearly_cleanup_policy(args: argparse.Namespace) -> str:
    """Return a compact display string for the effective cleanup policy."""
    policies = effective_yearly_cleanup_policies(args.level, args.yearly_cleanup_policy)
    if len(policies) == 1:
        return next(iter(policies.values()))
    return ",".join(f"{level}:{policy}" for level, policy in policies.items())


def emit_startup_banner(args: argparse.Namespace, *, stream) -> None:
    """Emit an immediate banner before the heavy runtime module is imported."""
    metrics = ",".join(args.metrics or ["ALL"])
    models = ",".join(args.models or ["ALL"])
    scenarios = ",".join(args.scenarios or ["ALL"])
    stream.write("IRT Climate Index Pipeline bootstrap\n")
    stream.write(f"  level={args.level}\n")
    stream.write(f"  state={args.state}\n")
    stream.write(f"  metrics={metrics}\n")
    stream.write(f"  models={models}\n")
    stream.write(f"  scenarios={scenarios}\n")
    stream.write(f"  workers={args.workers}\n")
    stream.write(f"  yearly_cleanup_policy_raw={args.yearly_cleanup_policy}\n")
    stream.write(f"  yearly_cleanup_policy_effective={format_effective_yearly_cleanup_policy(args)}\n")
    stream.write(f"  skip_existing={bool(args.skip_existing)} overwrite={bool(args.overwrite)}\n")
    stream.write("  source_inventory_cache=enabled\n")
    stream.flush()


def discover_models_lightweight(
    *,
    data_root: Optional[Path] = None,
    scenarios: Optional[dict[str, dict[str, str]]] = None,
    variables: Optional[Sequence[str]] = None,
) -> list[str]:
    """Discover available climate models without importing the heavy runtime."""
    paths = get_paths_config()
    root = Path(data_root or paths.data_root).expanduser().resolve()
    scenarios_map = scenarios or LIGHTWEIGHT_SCENARIOS
    variables_to_check = tuple(variables or ("tas", "tasmax", "tasmin", "pr"))
    models: set[str] = set()
    for scenario_conf in scenarios_map.values():
        base_parts = Path(str(scenario_conf["subdir"])).parts
        for varname in variables_to_check:
            model_base = root / base_parts[0] / varname
            if not model_base.exists():
                continue
            for entry in model_base.iterdir():
                if entry.is_dir():
                    models.add(entry.name)
    return sorted(models)

"""List optimized metrics that have yearly artifacts for a selected level/state."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

from india_resilience_tool.config.paths import get_paths_config
from india_resilience_tool.data.optimized_bundle import resolve_optimized_bundle_root


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="List optimized yearly metrics.")
    parser.add_argument("--state", help="Admin state to inspect, e.g. Telangana.")
    parser.add_argument(
        "--level",
        required=True,
        choices=["district", "block", "basin", "sub_basin"],
        help="Spatial level to inspect.",
    )
    parser.add_argument(
        "--format",
        choices=["lines", "args", "json"],
        default="lines",
        help="Output format. args emits repeated --metric flags.",
    )
    return parser


def list_metrics(*, data_dir: Path, level: str, state: Optional[str]) -> list[str]:
    bundle_root = resolve_optimized_bundle_root(data_dir=data_dir)
    metrics_root = bundle_root / "metrics"
    if not metrics_root.exists():
        return []

    family = "hydro" if level in {"basin", "sub_basin"} else "admin"
    if family == "admin" and not state:
        raise ValueError("--state is required for admin levels")
    target_name = "master.parquet" if family == "hydro" else f"state={state}.parquet"

    slugs: list[str] = []
    for metric_dir in sorted(path for path in metrics_root.iterdir() if path.is_dir()):
        ensemble_path = metric_dir / "yearly_ensemble" / family / level / target_name
        models_path = metric_dir / "yearly_models" / family / level / target_name
        if ensemble_path.exists() or models_path.exists():
            slugs.append(metric_dir.name)
    return slugs


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        metrics = list_metrics(data_dir=get_paths_config().data_dir, level=args.level, state=args.state)
    except ValueError as exc:
        parser.error(str(exc))

    if args.format == "json":
        print(json.dumps(metrics, indent=2))
    elif args.format == "args":
        print(" ".join(f"--metric {slug}" for slug in metrics))
    else:
        for slug in metrics:
            print(slug)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

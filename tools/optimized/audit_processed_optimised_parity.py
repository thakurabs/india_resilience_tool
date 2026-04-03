"""Audit parity of the processed_optimised runtime bundle against legacy processed inputs."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from india_resilience_tool.config.paths import get_paths_config
from tools.optimized.build_processed_optimised import audit_processed_optimised_parity


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit processed_optimised parity against legacy processed inputs.")
    parser.add_argument("--metric", action="append", dest="metrics", help="One metric slug to include. Repeatable.")
    parser.add_argument(
        "--level",
        action="append",
        dest="levels",
        choices=["all", "admin", "hydro", "district", "block", "basin", "sub_basin"],
        help="Restrict the audit to one or more level groups or concrete levels.",
    )
    parser.add_argument("--skip-geometry", action="store_true", help="Skip optimized geometry validation.")
    parser.add_argument("--skip-context", action="store_true", help="Skip optimized context validation.")
    parser.add_argument("--no-report", action="store_true", help="Do not write parity_report.json.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    data_dir: Path = get_paths_config().data_dir
    report = audit_processed_optimised_parity(
        data_dir=data_dir,
        metrics=args.metrics,
        levels=args.levels,
        include_geometry=not bool(args.skip_geometry),
        include_context=not bool(args.skip_context),
        write_report=not bool(args.no_report),
    )
    print("PROCESSED OPTIMISED PARITY AUDIT")
    print(f"bundle_root: {report['bundle_root']}")
    print(f"metrics_considered: {report['metrics_considered']}")
    print(f"issue_count: {report['issue_count']}")
    if report["issue_count"]:
        for issue in report["issues"][:50]:
            print(
                f"- {issue['stage']} | {issue['slug']} | {issue['level']} | {issue['target']} | "
                f"missing={','.join(issue.get('missing_columns') or [])}"
            )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

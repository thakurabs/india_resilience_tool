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
    parser.add_argument("--state", action="append", dest="states", help="One admin state to include. Repeatable.")
    parser.add_argument(
        "--level",
        action="append",
        dest="levels",
        choices=["all", "admin", "district", "block"],
        help="Restrict the audit to one or more level groups or concrete levels.",
    )
    parser.add_argument("--skip-geometry", action="store_true", help="Skip optimized geometry validation.")
    parser.add_argument("--skip-context", action="store_true", help="Skip optimized context validation.")
    parser.add_argument(
        "--include-shared-admin-artifacts",
        action="store_true",
        help="With --state, also audit shared-global admin artifacts.",
    )
    parser.add_argument("--no-report", action="store_true", help="Do not write parity_report.json.")
    parser.add_argument(
        "--report-path",
        type=Path,
        help="Explicit parity report output path. Scoped --state runs leave the global report untouched unless this is provided.",
    )
    parser.add_argument(
        "--require-block-yearly-models",
        action="store_true",
        help="Require block yearly_models Parquet whenever selected block yearly_ensemble Parquet exists.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when error-severity audit issues are present.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    data_dir: Path = get_paths_config().data_dir
    report = audit_processed_optimised_parity(
        data_dir=data_dir,
        metrics=args.metrics,
        levels=args.levels,
        states=args.states,
        include_geometry=not bool(args.skip_geometry),
        include_context=not bool(args.skip_context),
        include_shared_admin_artifacts=bool(args.include_shared_admin_artifacts),
        write_report=not bool(args.no_report) and args.report_path is None,
        report_path=args.report_path.expanduser().resolve() if args.report_path else None,
        require_block_yearly_models=bool(args.require_block_yearly_models),
    )
    print("PROCESSED OPTIMISED PARITY AUDIT")
    print(f"bundle_root: {report['bundle_root']}")
    print(f"metrics_considered: {report['metrics_considered']}")
    print(f"issue_count: {report['issue_count']}")
    if report["issue_count"]:
        for issue in report["issues"][:50]:
            print(
                f"- {issue['stage']} | {issue['slug']} | {issue['level']} | {issue['target']} | "
                f"severity={issue.get('severity', 'error')} | "
                f"missing={','.join(issue.get('missing_columns') or [])}"
            )
        has_error = any(
            str(issue.get("severity", "error")).strip().lower() == "error"
            for issue in report["issues"]
        )
        has_warning = any(
            str(issue.get("severity", "error")).strip().lower() == "warning"
            for issue in report["issues"]
        )
        # Error-severity issues always fail the audit. Warning-only issues (e.g.
        # an optional, live-fallback-backed precomputed artifact being absent)
        # are non-fatal unless --strict is requested.
        if has_error:
            return 1
        if has_warning and bool(args.strict):
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

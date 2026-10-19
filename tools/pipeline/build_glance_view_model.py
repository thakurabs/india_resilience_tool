"""Build persisted Glance view-model artifacts for landing runtime."""

from __future__ import annotations

import argparse
from typing import Optional

from india_resilience_tool.compute.glance_view_model import build_glance_view_models
from india_resilience_tool.config.paths import get_paths_config


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build persisted Glance view-model artifacts.")
    parser.add_argument("--metric", action="append", dest="metrics", help="Dashboard composite slug to include. Repeatable.")
    parser.add_argument("--overwrite", action="store_true", help="Rewrite selected Glance artifacts.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned Glance output paths without writing files.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    data_dir = get_paths_config().data_dir
    results = build_glance_view_models(
        data_dir=data_dir,
        composite_slugs=args.metrics,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
    )
    print("GLANCE VIEW MODEL")
    print(f"data_dir: {data_dir}")
    print(f"mode: {'dry-run' if bool(args.dry_run) else 'build'}")
    print(f"artifact_sets: {len(results)}")
    for result in results:
        print(f"write_target: {result.output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

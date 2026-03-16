#!/usr/bin/env python3
"""
Promote Parquet build artifacts into the published processed_parquet contract.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from paths import MIGRATED_BASE_OUTPUT_ROOT
from india_resilience_tool.utils.processed_layout import (
    metric_archive_root,
    metric_build_root,
    metric_contract_root,
    metric_published_root,
)

IGNORED_ROOTS = {"archive", "published", "build", "__pycache__"}


def _iter_metric_roots(processed_root: Path, metrics: Optional[Iterable[str]] = None) -> list[Path]:
    root = Path(processed_root)
    if (root / "build").exists() or (root / "published").exists():
        return [metric_contract_root(root)]

    metric_dirs = sorted([p for p in root.iterdir() if p.is_dir() and p.name not in IGNORED_ROOTS])
    if metrics:
        wanted = {str(m).strip() for m in metrics if str(m).strip()}
        metric_dirs = [p for p in metric_dirs if p.name in wanted]
    return metric_dirs


def _copy_tree(source_root: Path, target_root: Path, *, dry_run: bool) -> int:
    copied = 0
    for source in source_root.rglob("*"):
        if not source.is_file():
            continue
        rel = source.relative_to(source_root)
        target = target_root / rel
        copied += 1
        if dry_run:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return copied


def _archive_existing_published(metric_root: Path, *, timestamp: str, dry_run: bool) -> int:
    published_root = metric_published_root(metric_root)
    if not published_root.exists():
        return 0

    archive_root = metric_archive_root(metric_root, timestamp)
    moved = 0
    for child in published_root.iterdir():
        moved += 1
        if dry_run:
            continue
        archive_root.mkdir(parents=True, exist_ok=True)
        shutil.move(str(child), str(archive_root / child.name))
    return moved


def _prune_old_archives(metric_root: Path, *, keep_archives: int, dry_run: bool) -> int:
    archive_parent = metric_contract_root(metric_root) / "archive"
    if not archive_parent.exists() or keep_archives < 0:
        return 0

    snapshots = sorted([p for p in archive_parent.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True)
    stale = snapshots[max(int(keep_archives), 0):]
    if dry_run:
        return len(stale)

    removed = 0
    for snap in stale:
        shutil.rmtree(snap, ignore_errors=True)
        removed += 1
    return removed


def publish_metric_root(
    metric_root: Path,
    *,
    timestamp: str,
    dry_run: bool = False,
    keep_archives: int = 2,
) -> dict[str, int]:
    """Promote one metric build tree into published/ with archive-on-replace."""
    metric_root = metric_contract_root(Path(metric_root))
    build_root = metric_build_root(metric_root)
    published_root = metric_published_root(metric_root)

    if not build_root.exists():
        raise FileNotFoundError(f"Build root does not exist: {build_root}")

    archived_targets = _archive_existing_published(metric_root, timestamp=timestamp, dry_run=dry_run)
    if not dry_run:
        published_root.mkdir(parents=True, exist_ok=True)
    copied_files = _copy_tree(build_root, published_root, dry_run=dry_run)
    pruned_archives = _prune_old_archives(metric_root, keep_archives=keep_archives, dry_run=dry_run)

    return {
        "archived_targets": archived_targets,
        "copied_files": copied_files,
        "pruned_archives": pruned_archives,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote Parquet build outputs into published/archive.")
    parser.add_argument(
        "--processed-root",
        default=str(MIGRATED_BASE_OUTPUT_ROOT),
        help="processed_parquet base root or a single metric root. Default: paths.MIGRATED_BASE_OUTPUT_ROOT",
    )
    parser.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Optional metric slug filter when --processed-root points at the processed_parquet base.",
    )
    parser.add_argument(
        "--timestamp",
        default=None,
        help="Archive timestamp override (default: UTC now as YYYYMMDDTHHMMSSZ).",
    )
    parser.add_argument(
        "--keep-archives",
        type=int,
        default=2,
        help="Number of archive snapshots to retain per metric (default: 2).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be published without writing.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    processed_root = Path(args.processed_root).expanduser().resolve()
    timestamp = (
        str(args.timestamp).strip()
        if args.timestamp
        else pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")
    )
    metric_roots = _iter_metric_roots(processed_root, metrics=args.metrics)
    if not metric_roots:
        raise SystemExit(f"No metric roots found under {processed_root}")

    for metric_root in metric_roots:
        stats = publish_metric_root(
            metric_root,
            timestamp=timestamp,
            dry_run=bool(args.dry_run),
            keep_archives=int(args.keep_archives),
        )
        print(
            f"{metric_root.name}: "
            f"archived_targets={stats['archived_targets']} "
            f"copied_files={stats['copied_files']} "
            f"pruned_archives={stats['pruned_archives']}"
        )


if __name__ == "__main__":
    main()

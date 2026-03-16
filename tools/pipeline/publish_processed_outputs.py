#!/usr/bin/env python3
"""
Publish processed outputs into a published/archive layout with Parquet serving.
"""

from __future__ import annotations

import argparse
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from india_resilience_tool.utils.processed_io import read_table, write_partitioned_dataset, write_table
from paths import BASE_OUTPUT_ROOT, MIGRATED_BASE_OUTPUT_ROOT

IGNORED_ROOTS = {"archive", "published", "build", "__pycache__"}
SUMMARY_PREFIXES = (
    "master_metrics_by_",
    "state_model_averages_",
    "state_ensemble_stats_",
    "state_yearly_model_averages_",
    "state_yearly_ensemble_stats_",
)


def _iter_metric_roots(processed_root: Path, metrics: Optional[Iterable[str]] = None) -> list[Path]:
    root = Path(processed_root)

    def _looks_like_metric_root(path: Path) -> bool:
        if not path.exists() or not path.is_dir():
            return False
        if any((path / name).is_dir() for name in ("build", "hydro", "published")):
            return True
        try:
            for child in path.iterdir():
                if not child.is_dir() or child.name in IGNORED_ROOTS:
                    continue
                if any((child / level_dir).exists() for level_dir in ("districts", "blocks")):
                    return True
                if any(child.glob("master_metrics_by_*.csv")) or any(child.glob("master_metrics_by_*.parquet")):
                    return True
        except Exception:
            return False
        return False

    if _looks_like_metric_root(root):
        return [root]

    metric_dirs = sorted([p for p in root.iterdir() if p.is_dir() and p.name not in IGNORED_ROOTS])
    if metrics:
        wanted = {str(m).strip() for m in metrics if str(m).strip()}
        metric_dirs = [p for p in metric_dirs if p.name in wanted]
    return metric_dirs


def _source_root(metric_root: Path) -> Path:
    build_root = metric_root / "build"
    return build_root if build_root.exists() and build_root.is_dir() else metric_root


def _iter_source_csvs(source_root: Path) -> Iterable[Path]:
    for path in source_root.rglob("*.csv"):
        rel = path.relative_to(source_root)
        if rel.parts and rel.parts[0] in IGNORED_ROOTS:
            continue
        yield path


def _is_summary_csv(path: Path) -> bool:
    return any(path.name.startswith(prefix) for prefix in SUMMARY_PREFIXES)


def _archive_target(
    target: Path,
    *,
    published_root: Path,
    archive_root: Path,
    archived: set[Path],
    dry_run: bool,
) -> None:
    target = Path(target)
    if target in archived or not target.exists():
        return

    rel = target.relative_to(published_root)
    archive_path = archive_root / rel
    if dry_run:
        archived.add(target)
        return

    archive_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(target), str(archive_path))
    archived.add(target)


def _copy_csv_mirrors(
    source_root: Path,
    published_root: Path,
    archive_root: Path,
    *,
    dry_run: bool,
) -> list[Path]:
    archived: set[Path] = set()
    copied: list[Path] = []
    for source in _iter_source_csvs(source_root):
        rel = source.relative_to(source_root)
        target = published_root / rel
        _archive_target(target, published_root=published_root, archive_root=archive_root, archived=archived, dry_run=dry_run)
        copied.append(target)
        if dry_run:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return copied


def _write_summary_parquet_companions(
    copied_csvs: Iterable[Path],
    *,
    published_root: Path,
    archive_root: Path,
    dry_run: bool,
) -> list[Path]:
    archived: set[Path] = set()
    written: list[Path] = []
    for csv_path in copied_csvs:
        if not _is_summary_csv(csv_path):
            continue
        parquet_path = csv_path.with_suffix(".parquet")
        _archive_target(parquet_path, published_root=published_root, archive_root=archive_root, archived=archived, dry_run=dry_run)
        written.append(parquet_path)
        if dry_run:
            continue
        df = read_table(csv_path)
        write_table(df, parquet_path)
    return written


def _load_csv_with_required_columns(csv_path: Path, inferred: dict[str, str]) -> pd.DataFrame:
    df = read_table(csv_path)
    if df.empty:
        return df
    out = df.copy()
    for column, value in inferred.items():
        if column not in out.columns:
            out[column] = value
    return out


def _build_admin_yearly_datasets(
    published_root: Path,
    archive_root: Path,
    *,
    level: str,
    dry_run: bool,
) -> list[Path]:
    created: list[Path] = []
    archived: set[Path] = set()
    level_dir = "blocks" if level == "block" else "districts"
    grouped: dict[Path, list[pd.DataFrame]] = defaultdict(list)

    if level == "district":
        pattern = f"*/{level_dir}/ensembles/*/*/*_yearly_ensemble.csv"
    else:
        pattern = f"*/{level_dir}/ensembles/*/*/*/*_yearly_ensemble.csv"

    for csv_path in published_root.glob(pattern):
        rel = csv_path.relative_to(published_root)
        state = rel.parts[0]
        if level == "district":
            district, scenario = rel.parts[3], rel.parts[4]
            inferred = {"state": state, "district": district, "scenario": scenario}
        else:
            district, block, scenario = rel.parts[3], rel.parts[4], rel.parts[5]
            inferred = {
                "state": state,
                "district": district,
                "block": block,
                "scenario": scenario,
            }
        df = _load_csv_with_required_columns(csv_path, inferred)
        if not df.empty:
            grouped[published_root / state / level_dir / "ensembles" / "yearly"].append(df)

    for dataset_root, frames in grouped.items():
        created.append(dataset_root)
        _archive_target(dataset_root, published_root=published_root, archive_root=archive_root, archived=archived, dry_run=dry_run)
        if dry_run:
            continue
        combined = pd.concat(frames, ignore_index=True)
        write_partitioned_dataset(combined, dataset_root, partition_cols=["scenario"])

    return created


def _build_hydro_yearly_datasets(
    published_root: Path,
    archive_root: Path,
    *,
    level: str,
    dry_run: bool,
) -> list[Path]:
    created: list[Path] = []
    archived: set[Path] = set()
    level_dir = "sub_basins" if level == "sub_basin" else "basins"
    grouped: dict[Path, list[pd.DataFrame]] = defaultdict(list)
    pattern = f"hydro/{level_dir}/ensembles/*/*/*_yearly_ensemble.csv"
    if level == "sub_basin":
        pattern = f"hydro/{level_dir}/ensembles/*/*/*/*_yearly_ensemble.csv"

    for csv_path in published_root.glob(pattern):
        rel = csv_path.relative_to(published_root)
        if level == "basin":
            basin, scenario = rel.parts[3], rel.parts[4]
            inferred = {"basin": basin, "scenario": scenario}
        else:
            basin, sub_basin, scenario = rel.parts[3], rel.parts[4], rel.parts[5]
            inferred = {"basin": basin, "sub_basin": sub_basin, "scenario": scenario}
        df = _load_csv_with_required_columns(csv_path, inferred)
        if not df.empty:
            grouped[published_root / "hydro" / level_dir / "ensembles" / "yearly"].append(df)

    for dataset_root, frames in grouped.items():
        created.append(dataset_root)
        _archive_target(dataset_root, published_root=published_root, archive_root=archive_root, archived=archived, dry_run=dry_run)
        if dry_run:
            continue
        combined = pd.concat(frames, ignore_index=True)
        write_partitioned_dataset(combined, dataset_root, partition_cols=["scenario"])

    return created


def publish_metric_root(
    source_metric_root: Path,
    dest_metric_root: Path,
    *,
    timestamp: str,
    dry_run: bool = False,
) -> dict[str, int]:
    """Publish one metric tree into a separate destination root."""
    source_metric_root = Path(source_metric_root)
    dest_metric_root = Path(dest_metric_root)
    source_root = _source_root(source_metric_root)
    published_root = dest_metric_root / "published"
    archive_root = dest_metric_root / "archive" / timestamp

    copied_csvs = _copy_csv_mirrors(source_root, published_root, archive_root, dry_run=dry_run)
    summary_parquet = _write_summary_parquet_companions(
        copied_csvs,
        published_root=published_root,
        archive_root=archive_root,
        dry_run=dry_run,
    )
    yearly_roots = []
    yearly_roots.extend(_build_admin_yearly_datasets(published_root, archive_root, level="district", dry_run=dry_run))
    yearly_roots.extend(_build_admin_yearly_datasets(published_root, archive_root, level="block", dry_run=dry_run))
    yearly_roots.extend(_build_hydro_yearly_datasets(published_root, archive_root, level="basin", dry_run=dry_run))
    yearly_roots.extend(_build_hydro_yearly_datasets(published_root, archive_root, level="sub_basin", dry_run=dry_run))

    return {
        "csv_mirrors": len(copied_csvs),
        "summary_parquet": len(summary_parquet),
        "yearly_datasets": len(yearly_roots),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish processed outputs into published/archive layout.")
    parser.add_argument(
        "--source-root",
        default=str(BASE_OUTPUT_ROOT),
        help="Legacy processed root or single legacy metric root. Default: paths.BASE_OUTPUT_ROOT",
    )
    parser.add_argument(
        "--dest-root",
        default=str(MIGRATED_BASE_OUTPUT_ROOT),
        help="Migrated processed root or single migrated metric root. Default: paths.MIGRATED_BASE_OUTPUT_ROOT",
    )
    parser.add_argument(
        "--metrics",
        nargs="+",
        default=None,
        help="Optional metric slug filter when --source-root points at the processed base.",
    )
    parser.add_argument(
        "--timestamp",
        default=None,
        help="Archive timestamp override (default: UTC now as YYYYMMDDTHHMMSSZ).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be published without writing.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_root = Path(args.source_root).expanduser().resolve()
    dest_root = Path(args.dest_root).expanduser().resolve()
    timestamp = str(args.timestamp).strip() if args.timestamp else pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ")
    source_metric_roots = _iter_metric_roots(source_root, metrics=args.metrics)
    if not source_metric_roots:
        raise SystemExit(f"No metric roots found under {source_root}")

    dest_root_is_metric = dest_root.name not in {"processed_parquet", "processed", "build", "published", "archive"}

    for source_metric_root in source_metric_roots:
        dest_metric_root = dest_root if dest_root_is_metric else (dest_root / source_metric_root.name)
        stats = publish_metric_root(
            source_metric_root,
            dest_metric_root,
            timestamp=timestamp,
            dry_run=bool(args.dry_run),
        )
        print(
            f"{source_metric_root.name}: "
            f"csv_mirrors={stats['csv_mirrors']} "
            f"summary_parquet={stats['summary_parquet']} "
            f"yearly_datasets={stats['yearly_datasets']}"
        )


if __name__ == "__main__":
    main()

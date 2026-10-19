"""Prepare the backed-up identity record for published unsigned shade files.

The default command plans only. ``--run`` hashes all four optimised shade trees,
checks deployed source markers, saves a backup, then installs one atomic sidecar
on the production volume. It never rewrites a shade data file.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

import pandas as pd

from india_resilience_tool.data.wbgt_contract import SHADE_SLUGS

METHOD = "legacy-original-stull-gridfirst-v2"
SOURCE_COMMIT = "269be913150e65de14b55d119cacacb708e63d20"
SOURCE_PATH = "india_resilience_tool/compute/heat_stress_gridfirst.py"
SOURCE_SHA256 = "0a56ebfc6d5529f9c32f3927e891a8814a357bfd257af6a804d94b2a0385650a"
SIDECAR = "shade_method_identity.json"


def sha256(path: Path) -> str:
    """Hash one regular file in bounded memory."""
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def verify_source_code(repo: Path) -> dict:
    """Verify the historical implementation that built deployed legacy shade."""
    result = subprocess.run(["git", "show", f"{SOURCE_COMMIT}:{SOURCE_PATH}"],
                            cwd=repo, capture_output=True, check=True)
    actual = hashlib.sha256(result.stdout).hexdigest()
    if actual != SOURCE_SHA256:
        raise ValueError("Legacy source implementation hash changed")
    source = result.stdout.decode("utf-8")
    if ("0.7 * twb_c + 0.3 * tas_c" not in source or
        "(field >= threshold).fillna(False).sum(dim=\"time\")" not in source or
        "aggregate_cell_values(grid_ds[value_col], weights" not in source):
        raise ValueError("Legacy formula or cell-first threshold order differs")
    return {"commit": SOURCE_COMMIT, "path": SOURCE_PATH, "sha256": actual,
            "formula": "0.7*Stull(tas,hurs)+0.3*tas",
            "threshold_order": "daily_cell_threshold_then_annual_cell_count_then_area_weight"}


def source_marker_evidence(processed: Path) -> dict[str, dict]:
    """Check one complete published yearly file for each of the four slugs."""
    markers = {}
    for slug in sorted(SHADE_SLUGS):
        path = (processed / slug / "Kerala" / "districts" / "Wayanad" /
                "ACCESS-CM2" / "historical" / "Wayanad_yearly.csv")
        frame = pd.read_csv(path)
        required = {"source_file", "method_version", "aggregation_method", "year", "value"}
        if not required.issubset(frame) or frame.empty:
            raise ValueError(f"Missing deployed shade source markers: {path}")
        if "shade_method_signature" in frame:
            raise ValueError(f"Expected unsigned deployed shade, got signed: {path}")
        if not frame.method_version.eq("heat-stress-v2-gridfirst-2").all():
            raise ValueError(f"Mixed legacy method version: {path}")
        if not frame.aggregation_method.eq("gridfirst_area_weighted_mean").all():
            raise ValueError(f"Mixed legacy aggregation marker: {path}")
        sources = frame.source_file.map(json.loads)
        if not sources.map(lambda paths: len(paths) == 2 and
                           any("\\tas\\" in p for p in paths) and
                           any("\\hurs\\" in p for p in paths) and
                           not any("\\tasmax\\" in p for p in paths)).all():
            raise ValueError(f"Legacy source variables differ from tas/hurs: {path}")
        markers[slug] = {"path": str(path.resolve()), "sha256": sha256(path),
                         "rows_checked": len(frame),
                         "method_version": "heat-stress-v2-gridfirst-2",
                         "aggregation_method": "gridfirst_area_weighted_mean",
                         "source_variables": ["tas", "hurs"]}
    return markers


def hash_tree(root: Path, slug: str) -> dict[str, str]:
    """Inventory every regular file in one optimised shade tree."""
    metric = root / "metrics" / slug
    if not metric.is_dir():
        raise ValueError(f"Missing optimized shade tree: {metric}")
    files = sorted(metric.rglob("*"))
    if any(path.is_symlink() for path in files):
        raise ValueError(f"Symlink in optimized shade tree: {metric}")
    hashes = {path.relative_to(root).as_posix(): sha256(path)
              for path in files if path.is_file()}
    if not hashes or not any("/masters/" in name for name in hashes) or not any(
            "/yearly_models/" in name for name in hashes):
        raise ValueError(f"Incomplete optimized shade inventory: {metric}")
    return hashes


def build_record(repo: Path, processed: Path, bundle: Path) -> dict:
    """Assemble source evidence and per-file hashes without writing files."""
    code = verify_source_code(repo)
    markers = source_marker_evidence(processed)
    optimized = {}
    for slug in sorted(SHADE_SLUGS):
        optimized.update(hash_tree(bundle, slug))
    processed_masters = {}
    for slug in sorted(SHADE_SLUGS):
        for path in sorted((processed / slug).glob("*/master_metrics_by_*.csv")):
            processed_masters[path.relative_to(processed).as_posix()] = sha256(path)
    if not processed_masters:
        raise ValueError("No processed shade masters to identify")
    return {"schema": 1, "method": METHOD, "unsigned": True,
            "formula_source": code, "source_markers": markers,
            "optimized_root": str(bundle.resolve()), "processed_root": str(processed.resolve()),
            "optimized_files": optimized, "processed_master_files": processed_masters,
            "optimized_file_count": len(optimized),
            "processed_master_file_count": len(processed_masters)}


def install(record: dict, bundle: Path, backup_dir: Path) -> Path:
    """Back up existing identity state and atomically install the new record."""
    target = bundle / SIDECAR
    if target.exists():
        raise ValueError(f"Existing identity sidecar requires coordinated cutover: {target}")
    backup_dir.mkdir(parents=True, exist_ok=False)
    payload = json.dumps(record, indent=2, sort_keys=True).encode("utf-8")
    backup = backup_dir / SIDECAR
    backup.write_bytes(payload)
    if hashlib.sha256(backup.read_bytes()).hexdigest() != hashlib.sha256(payload).hexdigest():
        raise ValueError("Identity backup did not round-trip")
    temporary = bundle / f"{SIDECAR}.part"
    temporary.write_bytes(payload)
    temporary.replace(target)
    if sha256(target) != sha256(backup):
        raise ValueError("Installed identity sidecar differs from backup")
    return target


def main(argv: list[str] | None = None) -> int:
    """Plan or install a backed-up unsigned legacy identity record."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--processed", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--backup-dir", type=Path, required=True)
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args(argv)
    print(json.dumps({"processed": str(args.processed.resolve()),
                      "bundle": str(args.bundle.resolve()),
                      "backup_dir": str(args.backup_dir.resolve()), "writes": args.run}, indent=2),
          flush=True)
    if not args.run:
        return 0
    if args.backup_dir.resolve().is_relative_to(args.bundle.resolve()):
        parser.error("Backup must be outside the optimised bundle")
    record = build_record(args.repo, args.processed, args.bundle)
    target = install(record, args.bundle, args.backup_dir)
    print(json.dumps({"sidecar": str(target), "sha256": sha256(target),
                      "optimized_files": record["optimized_file_count"],
                      "processed_masters": record["processed_master_file_count"]}, indent=2),
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

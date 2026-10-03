#!/usr/bin/env python3
"""Remove the 8 Lakshadweep-only orphan SPI metrics resurrected by the
`--level both` Lakshadweep regen (F2.5).

These 8 slugs did not exist in the bundle before the 2026-07-27 regen (verified
against _backup_20260727_135017: absent in both processed/ and
processed_optimised/metrics/). The full-registry compute recreated them for
Lakshadweep only, which:
  * left the SPI `yearly_models` stage unemitted -> 8 audit errors, and
  * made every non-Lakshadweep block read as "partial coverage" (parity_verify
    D-block jumped 3 -> 7127; 7124 of those are these 8 metrics).

This script restores the pre-regen state:
  1. deletes each slug's dir from processed/ (source) and
     processed_optimised/metrics/ (bundle),
  2. removes each slug's single entry from `summaries` in bundle_manifest.json
     (backed up first).

SAFETY: refuses to delete a source dir that contains any admin-state subdir
other than Lakshadweep (i.e. only ever removes Lakshadweep-only orphans).
There is no manifest hash/checksum, so the summaries edit is self-consistent.

Usage (Windows, conda activate irt), from repo root:

    python scratch\\cleanup_spi_orphans.py            # DRY RUN (default) - prints plan
    python scratch\\cleanup_spi_orphans.py --apply    # actually delete + patch
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import shutil
import sys
from pathlib import Path

DATA_DIR = Path(r"D:\projects\irt_data")

ORPHAN_SLUGS = (
    "spi12_count_months_lt_minus1",
    "spi12_count_months_lt_minus2",
    "spi12_drought_index",
    "spi3_count_months_lt_minus2",
    "spi3_drought_index",
    "spi6_count_months_lt_minus1",
    "spi6_count_months_lt_minus2",
    "spi6_drought_index",
)

# names inside a source metric dir that are not admin-state subdirs
_NON_STATE = {".markers", "_internal"}


def _source_states(metric_dir: Path) -> list[str]:
    """Return admin-state subdir names under a source metric dir."""
    if not metric_dir.is_dir():
        return []
    return sorted(
        p.name
        for p in metric_dir.iterdir()
        if p.is_dir() and p.name not in _NON_STATE
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=DATA_DIR)
    ap.add_argument("--apply", action="store_true",
                    help="perform the deletions + manifest patch (default: dry run)")
    args = ap.parse_args(argv)

    data: Path = args.data_dir
    processed = data / "processed"
    opt_metrics = data / "processed_optimised" / "metrics"
    manifest_path = data / "processed_optimised" / "bundle_manifest.json"

    if not manifest_path.is_file():
        print(f"ERROR: manifest not found: {manifest_path}", file=sys.stderr)
        return 2

    mode = "APPLY" if args.apply else "DRY RUN"
    print("=" * 70)
    print(f"SPI orphan cleanup  [{mode}]   data-dir = {data}")
    print("=" * 70)

    # ---- safety pre-check: every slug must be Lakshadweep-only in source ----
    unsafe: list[tuple[str, list[str]]] = []
    for slug in ORPHAN_SLUGS:
        states = _source_states(processed / slug)
        others = [s for s in states if s != "Lakshadweep"]
        if others:
            unsafe.append((slug, others))
    if unsafe:
        print("\nABORT - refusing to delete: these slugs contain non-Lakshadweep states:")
        for slug, others in unsafe:
            print(f"  {slug}: {others}")
        return 3
    print("\nsafety check OK - all 8 slugs are Lakshadweep-only (or already absent) in source\n")

    # ---- 1 + 2. delete dirs from both trees --------------------------------
    for slug in ORPHAN_SLUGS:
        for label, root in (("source", processed / slug), ("bundle", opt_metrics / slug)):
            if root.is_dir():
                print(f"  delete [{label}] {root}")
                if args.apply:
                    shutil.rmtree(root)
            else:
                print(f"  skip   [{label}] {root}  (already absent)")

    # ---- 3. patch manifest summaries ---------------------------------------
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    summaries = manifest.get("summaries")
    if not isinstance(summaries, list):
        print("ERROR: manifest has no list-typed 'summaries'", file=sys.stderr)
        return 4

    before = len(summaries)
    kept = [e for e in summaries if e.get("slug") not in ORPHAN_SLUGS]
    removed = [e.get("slug") for e in summaries if e.get("slug") in ORPHAN_SLUGS]
    print(f"\nmanifest summaries: {before} -> {len(kept)}  (removing {len(removed)}: {removed})")

    if args.apply:
        stamp = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = manifest_path.with_name(f"{manifest_path.name}.bak-preSPIcleanup-{stamp}")
        shutil.copy2(manifest_path, backup)
        print(f"  backed up manifest -> {backup}")
        manifest["summaries"] = kept
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        print(f"  wrote patched manifest ({len(kept)} summaries)")

    print("\n" + "=" * 70)
    if args.apply:
        print("DONE. Next: rerun build_state_values (national), then the parity audit.")
    else:
        print("DRY RUN complete - nothing changed. Re-run with --apply to execute.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Enrich river_network_display.geojson in place with a district_names_clean column.

Performs a spatial intersection between the cleaned river display artifact and
the canonical districts boundary file, then rewrites the same GeoJSON augmented
with a comma-joined `district_names_clean` property per feature. The original
file is preserved as `<input>.bak` on first run.

Run:
    python -m tools.pipeline.enrich_river_network_districts --dry-run
    python -m tools.pipeline.enrich_river_network_districts
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import Iterable

import geopandas as gpd
import pandas as pd

from paths import (
    DISTRICTS_PATH,
    RIVER_NETWORK_DISPLAY_PATH,
)
from india_resilience_tool.data.adm2_loader import load_local_adm2

# Equal-area CRS for India (used for robust line/polygon intersection).
_METRIC_CRS = "EPSG:7755"


def _normalize_district_name(value: object) -> str:
    text = "" if value is None else str(value)
    return text.strip()


def _join_unique(names: Iterable[str]) -> str:
    cleaned = sorted({n for n in (str(x).strip() for x in names) if n})
    return ",".join(cleaned)


def enrich(
    *,
    rivers_path: Path,
    districts_path: Path,
    output_path: Path,
    dry_run: bool,
) -> None:
    if not rivers_path.exists():
        raise FileNotFoundError(f"River display GeoJSON not found: {rivers_path}")
    if not districts_path.exists():
        raise FileNotFoundError(f"Districts GeoJSON not found: {districts_path}")

    print(f"Reading rivers: {rivers_path}")
    rivers = gpd.read_file(str(rivers_path))
    if rivers.crs is None:
        rivers = rivers.set_crs("EPSG:4326")
    else:
        rivers = rivers.to_crs("EPSG:4326")
    if "river_feature_id" not in rivers.columns:
        raise ValueError("Input rivers layer is missing river_feature_id column.")

    print(f"Reading districts: {districts_path}")
    # Use the canonical loader so DISTRICT/STATE_UT raw columns get normalized
    # to district_name/state_name. tolerance=0 keeps full polygon resolution.
    districts = load_local_adm2(
        districts_path,
        tolerance=0.0,
        bbox=None,
        min_area=0.0,
    )
    if "district_name" not in districts.columns:
        raise ValueError("Districts layer is missing district_name column after normalization.")

    rivers_metric = rivers[["river_feature_id", "geometry"]].to_crs(_METRIC_CRS)
    districts_metric = districts[["district_name", "geometry"]].to_crs(_METRIC_CRS)

    print("Performing sjoin (intersects)...")
    joined = gpd.sjoin(
        rivers_metric,
        districts_metric,
        how="left",
        predicate="intersects",
    )

    print("Aggregating district names per river feature...")
    joined["district_name"] = joined["district_name"].map(_normalize_district_name)
    grouped = (
        joined.groupby("river_feature_id")["district_name"]
        .apply(_join_unique)
        .rename("district_names_clean")
        .reset_index()
    )

    if "district_names_clean" in rivers.columns:
        rivers = rivers.drop(columns=["district_names_clean"])
    out = rivers.merge(grouped, on="river_feature_id", how="left")
    out["district_names_clean"] = out["district_names_clean"].fillna("")

    total = len(out)
    empty = int((out["district_names_clean"] == "").sum())
    print(f"Features: {total}; with no district match: {empty} ({empty / max(total, 1):.2%})")

    if dry_run:
        print("Dry run — sample (first 5 rows):")
        sample_cols = [c for c in (
            "river_feature_id", "river_name_clean", "basin_name_clean",
            "state_names_clean", "district_names_clean",
        ) if c in out.columns]
        with pd.option_context("display.max_colwidth", 80):
            print(out[sample_cols].head().to_string(index=False))
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        backup = output_path.with_suffix(output_path.suffix + ".bak")
        if not backup.exists():
            print(f"Backing up existing artifact: {backup}")
            shutil.copy2(output_path, backup)
        else:
            print(f"Backup already present, leaving as-is: {backup}")
    print(f"Writing: {output_path}")
    out.to_file(str(output_path), driver="GeoJSON")
    print("Done.")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=RIVER_NETWORK_DISPLAY_PATH)
    parser.add_argument("--districts", type=Path, default=DISTRICTS_PATH)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Override write path. Defaults to --input (in-place enrichment).",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    output_path = args.output if args.output is not None else args.input
    enrich(
        rivers_path=args.input,
        districts_path=args.districts,
        output_path=output_path,
        dry_run=bool(args.dry_run),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

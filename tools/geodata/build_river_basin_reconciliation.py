#!/usr/bin/env python3
"""
Build the canonical hydro-basin to river-basin reconciliation CSV for IRT.

This tool compares canonical hydro basin polygons against the cleaned river
display artifact and emits one row per hydro basin:
- obvious exact normalized matches as `matched`
- unresolved basins as `review_required`

It does not attempt fuzzy matching for coastal/drainage/composite basins.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd

from india_resilience_tool.data.hydro_loader import ensure_epsg4326, ensure_hydro_columns
from india_resilience_tool.data.river_loader import (
    canonicalize_river_hydro_name,
    ensure_river_basin_reconciliation,
    load_local_river_display,
)
from paths import BASINS_PATH, RIVER_BASIN_RECONCILIATION_PATH, RIVER_NETWORK_DISPLAY_PATH


def build_river_basin_reconciliation_df(
    basin_gdf: gpd.GeoDataFrame,
    river_display_gdf: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Build one reconciliation row per hydro basin."""
    basins = ensure_hydro_columns(ensure_epsg4326(basin_gdf), level="basin")
    rivers = river_display_gdf.copy()
    rivers["__river_basin_canonical"] = (
        rivers["basin_name_clean"].astype(str).map(canonicalize_river_hydro_name)
    )
    river_counts = (
        rivers.groupby("basin_name_clean", dropna=False)
        .size()
        .astype(int)
        .to_dict()
    )

    rows: list[dict[str, Any]] = []
    basin_rows = (
        basins[["basin_id", "basin_name"]]
        .drop_duplicates()
        .sort_values(by=["basin_name", "basin_id"])
        .reset_index(drop=True)
    )
    distinct_river_names = sorted(
        rivers["basin_name_clean"].astype(str).str.strip().dropna().unique().tolist()
    )

    for _, row in basin_rows.iterrows():
        hydro_basin_name = str(row["basin_name"]).strip()
        hydro_basin_id = str(row["basin_id"]).strip()
        canonical_name = canonicalize_river_hydro_name(hydro_basin_name)
        candidates = sorted(
            {
                str(v).strip()
                for v in distinct_river_names
                if canonicalize_river_hydro_name(v) == canonical_name
            }
        )
        if len(candidates) == 1:
            river_basin_name = candidates[0]
            match_status = "matched"
            notes = (
                f"Auto-matched by normalized exact basin-name equality; "
                f"{river_counts.get(river_basin_name, 0)} river features."
            )
        elif len(candidates) > 1:
            river_basin_name = ""
            match_status = "review_required"
            notes = (
                "Multiple river basin-name candidates matched after normalization: "
                + ", ".join(candidates)
            )
        else:
            river_basin_name = ""
            match_status = "review_required"
            notes = "No exact normalized river basin-name match found in river_network_display.geojson."

        rows.append(
            {
                "hydro_basin_name": hydro_basin_name,
                "hydro_basin_id": hydro_basin_id,
                "river_basin_name": river_basin_name,
                "match_status": match_status,
                "notes": notes,
            }
        )

    return ensure_river_basin_reconciliation(pd.DataFrame(rows))


def _print_summary(df: pd.DataFrame) -> None:
    matched = int(df["match_status"].eq("matched").sum())
    unresolved = int(df["match_status"].eq("review_required").sum())
    no_source = int(df["match_status"].eq("no_source_rivers").sum())
    print("RIVER BASIN RECONCILIATION")
    print(f"Rows: {len(df)}")
    print(f"Matched: {matched}")
    print(f"Review required: {unresolved}")
    print(f"No source rivers: {no_source}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the hydro-basin to river-basin reconciliation CSV."
    )
    parser.add_argument(
        "--basins",
        type=str,
        default=str(BASINS_PATH),
        help="Path to canonical basins.geojson. Default: IRT_DATA_DIR/basins.geojson",
    )
    parser.add_argument(
        "--river-display",
        type=str,
        default=str(RIVER_NETWORK_DISPLAY_PATH),
        help="Path to cleaned river_network_display.geojson. Default: IRT_DATA_DIR/river_network_display.geojson",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=str(RIVER_BASIN_RECONCILIATION_PATH),
        help="Output reconciliation CSV path. Default: IRT_DATA_DIR/river_basin_name_reconciliation.csv",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output CSV if it exists.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and summarize without writing the CSV.")
    args = parser.parse_args(argv)

    basins_path = Path(args.basins).expanduser().resolve()
    river_display_path = Path(args.river_display).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    if not basins_path.exists():
        raise FileNotFoundError(f"Basins GeoJSON not found: {basins_path}")
    if not river_display_path.exists():
        raise FileNotFoundError(f"River display GeoJSON not found: {river_display_path}")
    if out_path.exists() and not args.overwrite and not args.dry_run:
        raise FileExistsError(f"Output already exists: {out_path}. Re-run with --overwrite.")

    basin_gdf = gpd.read_file(basins_path)
    river_display_gdf = load_local_river_display(river_display_path)
    reconciliation = build_river_basin_reconciliation_df(basin_gdf, river_display_gdf)
    _print_summary(reconciliation)

    if args.dry_run:
        print("Dry run complete. No files written.")
        return 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    reconciliation.to_csv(out_path, index=False)
    print(f"Wrote reconciliation CSV: {out_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        raise

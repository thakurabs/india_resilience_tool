#!/usr/bin/env python3
"""
Build a diagnostics CSV comparing canonical hydro sub-basins with cleaned river sub-basin names.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd

from india_resilience_tool.data.hydro_loader import ensure_epsg4326, ensure_hydro_columns
from india_resilience_tool.data.river_loader import load_local_river_display
from paths import RIVER_NETWORK_DISPLAY_PATH, RIVER_SUBBASIN_DIAGNOSTICS_PATH, SUBBASINS_PATH


def build_river_subbasin_diagnostics_df(
    subbasin_gdf: gpd.GeoDataFrame,
    river_display_gdf: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Compare hydro sub-basins against river display sub-basin names."""
    hydro = ensure_hydro_columns(ensure_epsg4326(subbasin_gdf), level="sub_basin")
    rivers = river_display_gdf.copy()
    rivers["__subbasin_key"] = rivers["subbasin_name_clean"].fillna("").astype(str).str.strip().str.lower()
    rivers["__is_placeholder"] = (
        rivers["subbasin_name_clean"].fillna("").astype(str).str.strip().str.upper().eq("MAJOR RIVER")
    )
    feature_counts = rivers.groupby("__subbasin_key", dropna=False).size().astype(int).to_dict()
    placeholder_counts = (
        rivers.groupby("__subbasin_key", dropna=False)["__is_placeholder"].sum().astype(int).to_dict()
    )

    rows: list[dict[str, object]] = []
    hydro_rows = (
        hydro[["basin_id", "basin_name", "subbasin_id", "subbasin_name"]]
        .drop_duplicates()
        .sort_values(by=["basin_name", "subbasin_name", "subbasin_id"])
        .reset_index(drop=True)
    )
    for _, row in hydro_rows.iterrows():
        key = str(row["subbasin_name"]).strip().lower()
        count = int(feature_counts.get(key, 0))
        placeholder = int(placeholder_counts.get(key, 0))
        match_status = "matched" if count > 0 else "review_required"
        notes = (
            "Exact normalized sub-basin name match found."
            if count > 0
            else "No exact normalized river sub-basin name match found."
        )
        rows.append(
            {
                "basin_id": str(row["basin_id"]).strip(),
                "basin_name": str(row["basin_name"]).strip(),
                "subbasin_id": str(row["subbasin_id"]).strip(),
                "subbasin_name": str(row["subbasin_name"]).strip(),
                "matched_river_feature_count": count,
                "placeholder_river_feature_count": placeholder,
                "match_status": match_status,
                "notes": notes,
            }
        )
    return pd.DataFrame(rows)


def _print_summary(df: pd.DataFrame) -> None:
    matched = int(df["match_status"].eq("matched").sum())
    unresolved = int(df["match_status"].eq("review_required").sum())
    print("RIVER SUB-BASIN DIAGNOSTICS")
    print(f"Rows: {len(df)}")
    print(f"Matched: {matched}")
    print(f"Review required: {unresolved}")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build hydro sub-basin vs river-name diagnostics CSV.")
    parser.add_argument(
        "--subbasins",
        type=str,
        default=str(SUBBASINS_PATH),
        help="Path to canonical subbasins.geojson. Default: IRT_DATA_DIR/subbasins.geojson",
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
        default=str(RIVER_SUBBASIN_DIAGNOSTICS_PATH),
        help="Output diagnostics CSV path. Default: IRT_DATA_DIR/river_subbasin_diagnostics.csv",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output CSV if it exists.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and summarize without writing the CSV.")
    args = parser.parse_args(argv)

    subbasins_path = Path(args.subbasins).expanduser().resolve()
    river_display_path = Path(args.river_display).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    if not subbasins_path.exists():
        raise FileNotFoundError(f"Sub-basins GeoJSON not found: {subbasins_path}")
    if not river_display_path.exists():
        raise FileNotFoundError(f"River display GeoJSON not found: {river_display_path}")
    if out_path.exists() and not args.overwrite and not args.dry_run:
        raise FileExistsError(f"Output already exists: {out_path}. Re-run with --overwrite.")

    subbasins = gpd.read_file(str(subbasins_path))
    river_display = load_local_river_display(str(river_display_path))
    diagnostics = build_river_subbasin_diagnostics_df(subbasins, river_display)
    _print_summary(diagnostics)

    if args.dry_run:
        print("Dry run complete. No files written.")
        return 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    diagnostics.to_csv(out_path, index=False)
    print(f"Wrote sub-basin diagnostics CSV: {out_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        raise

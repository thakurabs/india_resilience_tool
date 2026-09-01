#!/usr/bin/env python3
"""
Build the compact ADM1 (state polygons) GeoJSON artifact for IRT.

This tool produces ``processed_optimised/geometry/admin/adm1.geojson`` from the
canonical ``districts_4326.geojson`` source. The artifact is consumed at
dashboard boot to render the state selector without cold-loading the full ADM2
monolith.

Output contract:
    - EPSG:4326
    - One feature per `state_name`
    - Columns: ``state_name``, ``shapeName`` (mirror of state_name), ``geometry``
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd

from india_resilience_tool.config.constants import (
    INDIA_BBOX,
    SIMPLIFY_TOL_ADM1,
    SIMPLIFY_TOL_ADM2,
)
from india_resilience_tool.data.adm2_loader import (
    build_adm1_from_adm2,
    crop_to_bbox,
    simplify_and_filter,
)
from india_resilience_tool.data.optimized_bundle import optimized_adm1_path
from paths import DISTRICTS_PATH


def prepare_adm1_geojson(
    districts_path: Path,
    *,
    adm2_tolerance: float = SIMPLIFY_TOL_ADM2,
    adm1_tolerance: float = SIMPLIFY_TOL_ADM1,
    min_area: float = 0.0003,
) -> gpd.GeoDataFrame:
    """Read ADM2 and dissolve to ADM1 state polygons, simplified for fast render."""
    adm2 = gpd.read_file(str(districts_path))
    adm2 = simplify_and_filter(adm2, tolerance=adm2_tolerance, min_area=min_area)
    adm2 = crop_to_bbox(adm2, INDIA_BBOX).reset_index(drop=True)

    adm1 = build_adm1_from_adm2(adm2, state_col="state_name")
    adm1["geometry"] = adm1["geometry"].simplify(
        tolerance=float(adm1_tolerance), preserve_topology=True
    )

    keep_cols = [c for c in ("state_name", "shapeName") if c in adm1.columns]
    return adm1[[*keep_cols, "geometry"]].reset_index(drop=True)


def _write_geojson(gdf: gpd.GeoDataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing file without --overwrite: {path}"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        gdf.to_file(path, driver="GeoJSON", encoding="utf-8")
    except TypeError:
        gdf.to_file(path, driver="GeoJSON")


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the compact ADM1 state polygons GeoJSON artifact."
    )
    parser.add_argument(
        "--districts",
        type=str,
        default=str(DISTRICTS_PATH),
        help="Path to the source districts GeoJSON (ADM2).",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=str(optimized_adm1_path()),
        help="Output path for the ADM1 artifact.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and print summary without writing the output.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    districts_path = Path(args.districts).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    if not districts_path.exists():
        raise FileNotFoundError(f"Source districts GeoJSON not found: {districts_path}")

    gdf = prepare_adm1_geojson(districts_path)

    print("ADM1 GEOJSON")
    print(f"source_districts: {districts_path}")
    print(f"feature_count: {len(gdf)}")
    print(f"unique_state_count: {gdf['state_name'].astype(str).nunique()}")
    print(f"out: {out_path}")

    if args.dry_run:
        print("dry-run: not writing output")
        return 0

    _write_geojson(gdf, out_path, overwrite=bool(args.overwrite))
    size_bytes = out_path.stat().st_size if out_path.exists() else 0
    print(f"wrote: {out_path} ({size_bytes / 1024:.1f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

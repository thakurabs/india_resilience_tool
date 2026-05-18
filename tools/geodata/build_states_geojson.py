#!/usr/bin/env python3
"""
Build a full-fidelity ``states_4326.geojson`` by dissolving the canonical
``districts_4326.geojson``.

The output is a shareable companion to the district boundaries: state polygons
whose outer edges are built from the exact same vertex chains as the district
file, so overlaying the two produces no slivers or gaps.

This artifact is intentionally **unsimplified** and **not bbox-cropped** — it is
a source-of-truth handoff file, not a runtime asset. The dashboard continues to
use ``processed_optimised/geometry/admin/adm1.geojson`` (built by
``build_adm1_geojson.py``) for fast boot.

Output contract:
    - EPSG:4326
    - One feature per ``state_name``
    - Columns: ``state_name``, ``shapeName`` (mirror of state_name), ``geometry``
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
from shapely.validation import make_valid

from india_resilience_tool.data.adm2_loader import (
    build_adm1_from_adm2,
    ensure_adm2_columns,
    ensure_epsg4326,
)
from paths import DATA_DIR, DISTRICTS_PATH


DEFAULT_OUT = DATA_DIR / "states_4326.geojson"


def prepare_states_geojson(districts_path: Path) -> gpd.GeoDataFrame:
    """Read ADM2 and dissolve to full-fidelity ADM1 state polygons.

    No simplification, no bbox crop. Invalid input geometries are repaired via
    ``shapely.validation.make_valid`` before dissolve so slivers do not survive
    into the merged state polygons.
    """
    adm2 = gpd.read_file(str(districts_path))
    adm2 = ensure_epsg4326(adm2)
    adm2 = ensure_adm2_columns(adm2)

    adm2["geometry"] = adm2.geometry.apply(make_valid)

    adm1 = build_adm1_from_adm2(adm2, state_col="state_name")

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
        description=(
            "Build a full-fidelity states_4326.geojson by dissolving "
            "districts_4326.geojson (no simplification, no bbox crop)."
        )
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
        default=str(DEFAULT_OUT),
        help="Output path for the states GeoJSON.",
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

    gdf = prepare_states_geojson(districts_path)

    print("STATES GEOJSON (full fidelity)")
    print(f"source_districts: {districts_path}")
    print(f"feature_count: {len(gdf)}")
    print(f"unique_state_count: {gdf['state_name'].astype(str).nunique()}")
    print(f"crs: {gdf.crs}")
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

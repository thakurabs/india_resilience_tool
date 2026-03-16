#!/usr/bin/env python3
"""
Build Aqueduct HydroSHEDS-to-admin overlap crosswalks.

This tool intersects the clean India Aqueduct ``pfaf_id`` polygons directly
with canonical district boundaries, then writes a durable area-weighted overlap
table for later admin-boundary metric transfer.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd

from paths import DISTRICTS_PATH, get_paths_config
from tools.geodata.build_aqueduct_hydro_crosswalk import load_aqueduct_boundaries
from tools.geodata.build_district_subbasin_crosswalk import load_district_boundaries


def _default_aqueduct_dir() -> Path:
    return get_paths_config().data_dir / "aqueduct"


def build_aqueduct_district_crosswalk(
    aqueduct_gdf: gpd.GeoDataFrame,
    district_gdf: gpd.GeoDataFrame,
    *,
    area_epsg: int = 6933,
) -> pd.DataFrame:
    """Build a durable Aqueduct ``pfaf_id`` -> district overlap table."""
    aqueduct_proj = aqueduct_gdf.to_crs(epsg=area_epsg).copy()
    district_proj = district_gdf.to_crs(epsg=area_epsg).copy()

    aqueduct_proj["pfaf_area_km2"] = aqueduct_proj.geometry.area / 1_000_000.0
    district_proj["district_area_km2"] = district_proj.geometry.area / 1_000_000.0

    intersections = gpd.overlay(
        aqueduct_proj[["pfaf_id", "pfaf_area_km2", "geometry"]].copy(),
        district_proj[["state_name", "district_name", "district_key", "district_area_km2", "geometry"]].copy(),
        how="intersection",
    )
    if intersections.empty:
        return pd.DataFrame(
            columns=[
                "pfaf_id",
                "pfaf_area_km2",
                "state_name",
                "district_name",
                "district_key",
                "district_area_km2",
                "intersection_area_km2",
                "pfaf_area_fraction_in_district",
                "district_area_fraction_in_pfaf",
            ]
        )

    intersections["intersection_area_km2"] = intersections.geometry.area / 1_000_000.0
    intersections = intersections.loc[intersections["intersection_area_km2"] > 0].copy()
    if intersections.empty:
        raise ValueError("Aqueduct and district layers intersect only on zero-area boundaries.")

    grouped = (
        intersections.groupby(
            [
                "pfaf_id",
                "pfaf_area_km2",
                "state_name",
                "district_name",
                "district_key",
                "district_area_km2",
            ],
            dropna=False,
            as_index=False,
        )["intersection_area_km2"]
        .sum()
        .sort_values(["state_name", "district_name", "pfaf_id"])
        .reset_index(drop=True)
    )
    grouped["pfaf_area_fraction_in_district"] = grouped["intersection_area_km2"] / grouped["pfaf_area_km2"]
    grouped["district_area_fraction_in_pfaf"] = grouped["intersection_area_km2"] / grouped["district_area_km2"]
    return grouped


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Output already exists (pass --overwrite): {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Aqueduct HydroSHEDS-to-district crosswalk CSV.")
    parser.add_argument(
        "--aqueduct",
        type=str,
        default=str(_default_aqueduct_dir() / "baseline_clean_india.geojson"),
        help="Path to the clean India Aqueduct polygon GeoJSON.",
    )
    parser.add_argument(
        "--districts",
        type=str,
        default=str(DISTRICTS_PATH),
        help="Path to canonical district boundaries GeoJSON.",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=str(_default_aqueduct_dir() / "aqueduct_district_crosswalk.csv"),
        help="Path to the Aqueduct-to-district crosswalk CSV.",
    )
    parser.add_argument("--area-epsg", type=int, default=6933, help="Equal-area EPSG used for overlap calculations.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output CSV.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    aqueduct_path = Path(args.aqueduct).expanduser().resolve()
    districts_path = Path(args.districts).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    if not aqueduct_path.exists():
        raise FileNotFoundError(f"Aqueduct GeoJSON not found: {aqueduct_path}")
    if not districts_path.exists():
        raise FileNotFoundError(f"District boundaries GeoJSON not found: {districts_path}")

    aqueduct_gdf = load_aqueduct_boundaries(aqueduct_path)
    district_gdf = load_district_boundaries(districts_path)

    crosswalk_df = build_aqueduct_district_crosswalk(
        aqueduct_gdf,
        district_gdf,
        area_epsg=int(args.area_epsg),
    )
    _write_csv(crosswalk_df, out_path, overwrite=bool(args.overwrite))

    print("AQUEDUCT DISTRICT CROSSWALK")
    print(f"aqueduct_pfaf_count: {len(aqueduct_gdf)}")
    print(f"district_count: {len(district_gdf)}")
    print(f"crosswalk_rows: {len(crosswalk_df)}")
    print(f"out: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

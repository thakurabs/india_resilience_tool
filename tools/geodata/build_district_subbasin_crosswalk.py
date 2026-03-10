#!/usr/bin/env python3
"""
Build a canonical district ↔ sub-basin crosswalk CSV for IRT.

The output is a read-optimized intersection table used by the dashboard to
connect administrative and hydro storylines. Geometry is not written to the
artifact; the script computes overlap areas in a projected CRS and exports a
flat CSV.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd

from india_resilience_tool.data.adm2_loader import ensure_adm2_columns, ensure_epsg4326
from india_resilience_tool.data.crosswalks import ensure_district_subbasin_crosswalk
from india_resilience_tool.data.hydro_loader import ensure_hydro_columns
from paths import DISTRICT_SUBBASIN_CROSSWALK_PATH, DISTRICTS_PATH, SUBBASINS_PATH


def _assert_areal_geometries(gdf: gpd.GeoDataFrame, *, label: str) -> None:
    """Raise on null, empty, or non-areal geometries."""
    if gdf.geometry.isna().any():
        raise ValueError(f"{label} contains null geometries.")
    if gdf.geometry.is_empty.any():
        raise ValueError(f"{label} contains empty geometries.")
    bad = ~gdf.geom_type.isin(["Polygon", "MultiPolygon"])
    if bad.any():
        bad_types = sorted(gdf.loc[bad].geom_type.astype(str).unique().tolist())
        raise ValueError(f"{label} contains non-areal geometries: {bad_types}.")


def _district_key(state_name: object, district_name: object) -> str:
    """Build a stable district key from canonical district fields."""
    return f"{str(state_name).strip()}::{str(district_name).strip()}"


def load_district_boundaries(path: Path) -> gpd.GeoDataFrame:
    """Load canonical district boundaries for crosswalk generation."""
    gdf = gpd.read_file(path)
    gdf = ensure_epsg4326(gdf)
    gdf = ensure_adm2_columns(gdf)
    _assert_areal_geometries(gdf, label="District boundaries")

    out = gdf[["state_name", "district_name", "geometry"]].copy()
    out["state_name"] = out["state_name"].astype(str).str.strip()
    out["district_name"] = out["district_name"].astype(str).str.strip()
    out["district_key"] = [
        _district_key(state_name, district_name)
        for state_name, district_name in zip(out["state_name"], out["district_name"])
    ]
    # Some district sources contain multiple polygon features for the same
    # district (islands/fragments). Dissolve them so the crosswalk has one
    # canonical district geometry per district key.
    out = out.dissolve(
        by="district_key",
        as_index=False,
        aggfunc={
            "state_name": "first",
            "district_name": "first",
        },
    )
    out["district_key"] = out["district_key"].astype(str).str.strip()
    out["state_name"] = out["state_name"].astype(str).str.strip()
    out["district_name"] = out["district_name"].astype(str).str.strip()
    return out[["state_name", "district_name", "district_key", "geometry"]].reset_index(drop=True)


def load_subbasin_boundaries(path: Path) -> gpd.GeoDataFrame:
    """Load canonical sub-basin boundaries for crosswalk generation."""
    gdf = gpd.read_file(path)
    gdf = ensure_epsg4326(gdf)
    gdf = ensure_hydro_columns(gdf, level="sub_basin")
    _assert_areal_geometries(gdf, label="Sub-basin boundaries")

    keep_cols = [
        "basin_id",
        "basin_name",
        "subbasin_id",
        "subbasin_code",
        "subbasin_name",
        "geometry",
    ]
    return gdf[keep_cols].copy().reset_index(drop=True)


def build_district_subbasin_crosswalk(
    districts: gpd.GeoDataFrame,
    subbasins: gpd.GeoDataFrame,
    *,
    area_epsg: int = 6933,
) -> pd.DataFrame:
    """
    Build the canonical district ↔ sub-basin overlap table.

    Fractions use the current dashboard contract:
    - district_area_fraction_in_subbasin = intersection_area / district_area
    - subbasin_area_fraction_in_district = intersection_area / subbasin_area
    """
    districts_proj = districts.to_crs(epsg=area_epsg).copy()
    subbasins_proj = subbasins.to_crs(epsg=area_epsg).copy()

    districts_proj["district_area_km2"] = districts_proj.geometry.area / 1_000_000.0
    subbasins_proj["subbasin_area_km2"] = subbasins_proj.geometry.area / 1_000_000.0

    districts_keep = districts_proj[
        ["state_name", "district_name", "district_key", "district_area_km2", "geometry"]
    ].copy()
    subbasins_keep = subbasins_proj[
        [
            "basin_id",
            "basin_name",
            "subbasin_id",
            "subbasin_code",
            "subbasin_name",
            "subbasin_area_km2",
            "geometry",
        ]
    ].copy()

    intersections = gpd.overlay(districts_keep, subbasins_keep, how="intersection")
    if intersections.empty:
        return pd.DataFrame(
            columns=[
                "district_key",
                "district_name",
                "state_name",
                "basin_id",
                "basin_name",
                "subbasin_id",
                "subbasin_code",
                "subbasin_name",
                "district_area_km2",
                "subbasin_area_km2",
                "intersection_area_km2",
                "district_area_fraction_in_subbasin",
                "subbasin_area_fraction_in_district",
            ]
        )

    intersections["intersection_area_km2"] = intersections.geometry.area / 1_000_000.0
    intersections = intersections[intersections["intersection_area_km2"] > 0].copy()

    if intersections.empty:
        raise ValueError("District and sub-basin layers intersect only on zero-area boundaries.")

    grouped = (
        intersections.groupby(
            [
                "district_key",
                "district_name",
                "state_name",
                "basin_id",
                "basin_name",
                "subbasin_id",
                "subbasin_code",
                "subbasin_name",
                "district_area_km2",
                "subbasin_area_km2",
            ],
            dropna=False,
            as_index=False,
        )["intersection_area_km2"]
        .sum()
    )

    grouped["district_area_fraction_in_subbasin"] = (
        grouped["intersection_area_km2"] / grouped["district_area_km2"]
    )
    grouped["subbasin_area_fraction_in_district"] = (
        grouped["intersection_area_km2"] / grouped["subbasin_area_km2"]
    )

    grouped = grouped.sort_values(
        by=["state_name", "district_name", "basin_name", "subbasin_name"],
        ascending=[True, True, True, True],
    ).reset_index(drop=True)

    return grouped[
        [
            "district_key",
            "district_name",
            "state_name",
            "basin_id",
            "basin_name",
            "subbasin_id",
            "subbasin_code",
            "subbasin_name",
            "district_area_km2",
            "subbasin_area_km2",
            "intersection_area_km2",
            "district_area_fraction_in_subbasin",
            "subbasin_area_fraction_in_district",
        ]
    ].copy()


def _print_summary(df: pd.DataFrame) -> None:
    """Print a compact artifact summary."""
    print(f"Rows: {len(df)}")
    print(f"Districts covered: {df['district_key'].nunique(dropna=True)}")
    print(f"Sub-basins covered: {df['subbasin_id'].nunique(dropna=True)}")
    print(f"Basins covered: {df['basin_id'].nunique(dropna=True)}")


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Build the canonical district-subbasin crosswalk CSV for IRT."
    )
    parser.add_argument(
        "--districts",
        type=str,
        default=str(DISTRICTS_PATH),
        help="Path to district boundaries GeoJSON. Defaults to the canonical DATA_DIR path.",
    )
    parser.add_argument(
        "--subbasins",
        type=str,
        default=str(SUBBASINS_PATH),
        help="Path to sub-basin GeoJSON. Defaults to the canonical DATA_DIR path.",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=str(DISTRICT_SUBBASIN_CROSSWALK_PATH),
        help="Output CSV path. Defaults to DATA_DIR/district_subbasin_crosswalk.csv.",
    )
    parser.add_argument(
        "--area-epsg",
        type=int,
        default=6933,
        help="Projected EPSG used for area calculations. Default: 6933.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the output CSV if it already exists.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and validate the crosswalk, but do not write the CSV.",
    )
    args = parser.parse_args(argv)

    districts_path = Path(args.districts).expanduser().resolve()
    subbasins_path = Path(args.subbasins).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    if not districts_path.exists():
        raise FileNotFoundError(f"District boundaries not found: {districts_path}")
    if not subbasins_path.exists():
        raise FileNotFoundError(f"Sub-basin boundaries not found: {subbasins_path}")
    if out_path.exists() and not args.overwrite and not args.dry_run:
        raise FileExistsError(
            f"Output already exists: {out_path}. Re-run with --overwrite to replace it."
        )

    districts = load_district_boundaries(districts_path)
    subbasins = load_subbasin_boundaries(subbasins_path)
    crosswalk = build_district_subbasin_crosswalk(
        districts,
        subbasins,
        area_epsg=int(args.area_epsg),
    )
    crosswalk = ensure_district_subbasin_crosswalk(crosswalk)

    print("DISTRICT ↔ SUB-BASIN CROSSWALK")
    print(f"Districts: {districts_path}")
    print(f"Sub-basins: {subbasins_path}")
    print(f"Area EPSG: {args.area_epsg}")
    _print_summary(crosswalk)

    if args.dry_run:
        print("Dry run complete. No file written.")
        return 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    crosswalk.to_csv(out_path, index=False)
    print(f"Wrote crosswalk CSV: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

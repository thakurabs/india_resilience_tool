#!/usr/bin/env python3
"""
Shared polygon crosswalk builders for IRT.

This module keeps the district ↔ sub-basin CLI entrypoint, but also exposes the
shared loaders and builders used by the broader polygon crosswalk phase:
- district ↔ sub-basin
- block ↔ sub-basin
- district ↔ basin
- block ↔ basin
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Literal, Optional

import geopandas as gpd
import pandas as pd

from india_resilience_tool.data.adm2_loader import ensure_adm2_columns, ensure_epsg4326
from india_resilience_tool.data.adm3_loader import ensure_adm3_columns
from india_resilience_tool.data.crosswalks import (
    ensure_block_basin_crosswalk,
    ensure_block_subbasin_crosswalk,
    ensure_district_basin_crosswalk,
    ensure_district_subbasin_crosswalk,
)
from india_resilience_tool.data.hydro_loader import ensure_hydro_columns
from paths import (
    BASINS_PATH,
    BLOCKS_PATH,
    BLOCK_BASIN_CROSSWALK_PATH,
    BLOCK_SUBBASIN_CROSSWALK_PATH,
    DISTRICTS_PATH,
    DISTRICT_BASIN_CROSSWALK_PATH,
    DISTRICT_SUBBASIN_CROSSWALK_PATH,
    SUBBASINS_PATH,
)


AdminLevel = Literal["district", "block"]
HydroLevel = Literal["basin", "sub_basin"]


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
    return f"{str(state_name).strip()}::{str(district_name).strip()}"


def _block_key(state_name: object, district_name: object, block_name: object) -> str:
    return (
        f"{str(state_name).strip()}::"
        f"{str(district_name).strip()}::"
        f"{str(block_name).strip()}"
    )


def load_district_boundaries(path: Path) -> gpd.GeoDataFrame:
    """Load canonical district boundaries and dissolve district fragments."""
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
    out = out.dissolve(
        by="district_key",
        as_index=False,
        aggfunc={"state_name": "first", "district_name": "first"},
    )
    return out[["state_name", "district_name", "district_key", "geometry"]].reset_index(drop=True)


def load_block_boundaries(path: Path) -> gpd.GeoDataFrame:
    """Load canonical block boundaries and dissolve block fragments."""
    gdf = gpd.read_file(path)
    gdf = ensure_epsg4326(gdf)
    gdf = ensure_adm3_columns(gdf)
    _assert_areal_geometries(gdf, label="Block boundaries")

    out = gdf[["state_name", "district_name", "block_name", "geometry"]].copy()
    for col in ("state_name", "district_name", "block_name"):
        out[col] = out[col].astype(str).str.strip()
    out["block_key"] = [
        _block_key(state_name, district_name, block_name)
        for state_name, district_name, block_name in zip(
            out["state_name"], out["district_name"], out["block_name"]
        )
    ]
    out = out.dissolve(
        by="block_key",
        as_index=False,
        aggfunc={"state_name": "first", "district_name": "first", "block_name": "first"},
    )
    return out[["state_name", "district_name", "block_name", "block_key", "geometry"]].reset_index(drop=True)


def load_hydro_boundaries(path: Path, *, level: HydroLevel) -> gpd.GeoDataFrame:
    """Load canonical basin or sub-basin boundaries."""
    gdf = gpd.read_file(path)
    gdf = ensure_epsg4326(gdf)
    gdf = ensure_hydro_columns(gdf, level=level)
    _assert_areal_geometries(gdf, label=f"{'Sub-basin' if level == 'sub_basin' else 'Basin'} boundaries")
    if level == "sub_basin":
        keep_cols = ["basin_id", "basin_name", "subbasin_id", "subbasin_code", "subbasin_name", "geometry"]
    else:
        keep_cols = ["basin_id", "basin_name", "geometry"]
    return gdf[keep_cols].copy().reset_index(drop=True)


def build_admin_hydro_crosswalk(
    admin_gdf: gpd.GeoDataFrame,
    hydro_gdf: gpd.GeoDataFrame,
    *,
    admin_level: AdminLevel,
    hydro_level: HydroLevel,
    area_epsg: int = 6933,
) -> pd.DataFrame:
    """
    Build a canonical overlap table between one admin level and one hydro level.

    Fractions follow the naming contract used by the dashboard:
    - `<admin>_area_fraction_in_<hydro>` = intersection / admin area
    - `<hydro>_area_fraction_in_<admin>` = intersection / hydro area
    """
    admin_area_col = f"{admin_level}_area_km2"
    hydro_area_col = "subbasin_area_km2" if hydro_level == "sub_basin" else "basin_area_km2"
    admin_fraction_col = f"{admin_level}_area_fraction_in_{'subbasin' if hydro_level == 'sub_basin' else 'basin'}"
    hydro_fraction_col = f"{'subbasin' if hydro_level == 'sub_basin' else 'basin'}_area_fraction_in_{admin_level}"

    if admin_level == "district" and "district_key" in admin_gdf.columns:
        admin_gdf = admin_gdf.dissolve(
            by="district_key",
            as_index=False,
            aggfunc={"state_name": "first", "district_name": "first"},
        )
    elif admin_level == "block" and "block_key" in admin_gdf.columns:
        admin_gdf = admin_gdf.dissolve(
            by="block_key",
            as_index=False,
            aggfunc={"state_name": "first", "district_name": "first", "block_name": "first"},
        )

    if hydro_level == "sub_basin" and "subbasin_id" in hydro_gdf.columns:
        hydro_gdf = hydro_gdf.dissolve(
            by="subbasin_id",
            as_index=False,
            aggfunc={
                "basin_id": "first",
                "basin_name": "first",
                "subbasin_code": "first",
                "subbasin_name": "first",
            },
        )
    elif hydro_level == "basin" and "basin_id" in hydro_gdf.columns:
        hydro_gdf = hydro_gdf.dissolve(
            by="basin_id",
            as_index=False,
            aggfunc={"basin_name": "first"},
        )

    admin_proj = admin_gdf.to_crs(epsg=area_epsg).copy()
    hydro_proj = hydro_gdf.to_crs(epsg=area_epsg).copy()

    admin_proj[admin_area_col] = admin_proj.geometry.area / 1_000_000.0
    hydro_proj[hydro_area_col] = hydro_proj.geometry.area / 1_000_000.0

    if admin_level == "district":
        admin_cols = ["state_name", "district_name", "district_key", admin_area_col, "geometry"]
        group_cols = ["state_name", "district_name", "district_key", admin_area_col]
    else:
        admin_cols = ["state_name", "district_name", "block_name", "block_key", admin_area_col, "geometry"]
        group_cols = ["state_name", "district_name", "block_name", "block_key", admin_area_col]

    if hydro_level == "sub_basin":
        hydro_cols = [
            "basin_id",
            "basin_name",
            "subbasin_id",
            "subbasin_code",
            "subbasin_name",
            hydro_area_col,
            "geometry",
        ]
        hydro_group_cols = ["basin_id", "basin_name", "subbasin_id", "subbasin_code", "subbasin_name", hydro_area_col]
    else:
        hydro_cols = ["basin_id", "basin_name", hydro_area_col, "geometry"]
        hydro_group_cols = ["basin_id", "basin_name", hydro_area_col]

    intersections = gpd.overlay(
        admin_proj[admin_cols].copy(),
        hydro_proj[hydro_cols].copy(),
        how="intersection",
    )
    if intersections.empty:
        return pd.DataFrame(columns=group_cols + hydro_group_cols + ["intersection_area_km2", admin_fraction_col, hydro_fraction_col])

    intersections["intersection_area_km2"] = intersections.geometry.area / 1_000_000.0
    intersections = intersections[intersections["intersection_area_km2"] > 0].copy()
    if intersections.empty:
        raise ValueError(f"{admin_level.title()} and {hydro_level.replace('_', '-') } layers intersect only on zero-area boundaries.")

    grouped = (
        intersections.groupby(group_cols + hydro_group_cols, dropna=False, as_index=False)["intersection_area_km2"]
        .sum()
    )
    grouped[admin_fraction_col] = grouped["intersection_area_km2"] / grouped[admin_area_col]
    grouped[hydro_fraction_col] = grouped["intersection_area_km2"] / grouped[hydro_area_col]

    sort_cols = ["state_name", "district_name"]
    if admin_level == "block":
        sort_cols.append("block_name")
    sort_cols.extend(["basin_name"])
    if hydro_level == "sub_basin":
        sort_cols.append("subbasin_name")
    grouped = grouped.sort_values(by=sort_cols, ascending=[True] * len(sort_cols)).reset_index(drop=True)
    return grouped


def build_district_subbasin_crosswalk(
    districts: gpd.GeoDataFrame,
    subbasins: gpd.GeoDataFrame,
    *,
    area_epsg: int = 6933,
) -> pd.DataFrame:
    """Build the canonical district ↔ sub-basin overlap table."""
    return build_admin_hydro_crosswalk(
        districts,
        subbasins,
        admin_level="district",
        hydro_level="sub_basin",
        area_epsg=area_epsg,
    )


def build_block_subbasin_crosswalk(
    blocks: gpd.GeoDataFrame,
    subbasins: gpd.GeoDataFrame,
    *,
    area_epsg: int = 6933,
) -> pd.DataFrame:
    """Build the canonical block ↔ sub-basin overlap table."""
    return build_admin_hydro_crosswalk(
        blocks,
        subbasins,
        admin_level="block",
        hydro_level="sub_basin",
        area_epsg=area_epsg,
    )


def build_district_basin_crosswalk(
    districts: gpd.GeoDataFrame,
    basins: gpd.GeoDataFrame,
    *,
    area_epsg: int = 6933,
) -> pd.DataFrame:
    """Build the canonical district ↔ basin overlap table."""
    return build_admin_hydro_crosswalk(
        districts,
        basins,
        admin_level="district",
        hydro_level="basin",
        area_epsg=area_epsg,
    )


def build_block_basin_crosswalk(
    blocks: gpd.GeoDataFrame,
    basins: gpd.GeoDataFrame,
    *,
    area_epsg: int = 6933,
) -> pd.DataFrame:
    """Build the canonical block ↔ basin overlap table."""
    return build_admin_hydro_crosswalk(
        blocks,
        basins,
        admin_level="block",
        hydro_level="basin",
        area_epsg=area_epsg,
    )


def _print_summary(df: pd.DataFrame, *, admin_level: AdminLevel, hydro_level: HydroLevel) -> None:
    """Print a compact artifact summary."""
    print(f"Rows: {len(df)}")
    admin_key_col = "district_key" if admin_level == "district" else "block_key"
    print(f"{admin_level.title()}s covered: {df[admin_key_col].nunique(dropna=True)}")
    if hydro_level == "sub_basin":
        print(f"Sub-basins covered: {df['subbasin_id'].nunique(dropna=True)}")
    print(f"Basins covered: {df['basin_id'].nunique(dropna=True)}")


def _run_build(
    *,
    title: str,
    admin_path: Path,
    hydro_path: Path,
    out_path: Path,
    area_epsg: int,
    overwrite: bool,
    dry_run: bool,
    load_admin_fn,
    load_hydro_fn,
    build_fn,
    ensure_fn,
    admin_level: AdminLevel,
    hydro_level: HydroLevel,
) -> int:
    if not admin_path.exists():
        raise FileNotFoundError(f"{admin_level.title()} boundaries not found: {admin_path}")
    if not hydro_path.exists():
        raise FileNotFoundError(f"{'Sub-basin' if hydro_level == 'sub_basin' else 'Basin'} boundaries not found: {hydro_path}")
    if out_path.exists() and not overwrite and not dry_run:
        raise FileExistsError(f"Output already exists: {out_path}. Re-run with --overwrite to replace it.")

    admin = load_admin_fn(admin_path)
    hydro = load_hydro_fn(hydro_path)
    crosswalk = build_fn(admin, hydro, area_epsg=area_epsg)
    crosswalk = ensure_fn(crosswalk)

    print(title)
    print(f"{admin_level.title()}s: {admin_path}")
    print(f"{'Sub-basins' if hydro_level == 'sub_basin' else 'Basins'}: {hydro_path}")
    print(f"Area EPSG: {area_epsg}")
    _print_summary(crosswalk, admin_level=admin_level, hydro_level=hydro_level)

    if dry_run:
        print("Dry run complete. No file written.")
        return 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    crosswalk.to_csv(out_path, index=False)
    print(f"Wrote crosswalk CSV: {out_path}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point for the district ↔ sub-basin artifact."""
    parser = argparse.ArgumentParser(
        description="Build the canonical district-subbasin crosswalk CSV for IRT."
    )
    parser.add_argument("--districts", type=str, default=str(DISTRICTS_PATH))
    parser.add_argument("--subbasins", type=str, default=str(SUBBASINS_PATH))
    parser.add_argument("--out", type=str, default=str(DISTRICT_SUBBASIN_CROSSWALK_PATH))
    parser.add_argument("--area-epsg", type=int, default=6933)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    return _run_build(
        title="DISTRICT ↔ SUB-BASIN CROSSWALK",
        admin_path=Path(args.districts).expanduser().resolve(),
        hydro_path=Path(args.subbasins).expanduser().resolve(),
        out_path=Path(args.out).expanduser().resolve(),
        area_epsg=int(args.area_epsg),
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        load_admin_fn=load_district_boundaries,
        load_hydro_fn=lambda path: load_hydro_boundaries(path, level="sub_basin"),
        build_fn=build_district_subbasin_crosswalk,
        ensure_fn=ensure_district_subbasin_crosswalk,
        admin_level="district",
        hydro_level="sub_basin",
    )


if __name__ == "__main__":
    raise SystemExit(main())

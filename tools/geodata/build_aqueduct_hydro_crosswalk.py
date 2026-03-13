#!/usr/bin/env python3
"""
Build Aqueduct HydroSHEDS-to-SOI hydro overlap crosswalks.

This tool intersects the India-only Aqueduct HydroSHEDS Level 6 polygons with
the canonical Survey of India basin and sub-basin polygons, then writes durable
area-weighted overlap tables for later metric transfer.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd

from india_resilience_tool.data.hydro_loader import ensure_epsg4326, ensure_hydro_columns
from paths import BASINS_PATH, SUBBASINS_PATH, get_paths_config


HydroLevel = Literal["basin", "sub_basin"]


def _default_aqueduct_dir() -> Path:
    return get_paths_config().data_dir / "aqueduct"


def _assert_areal_geometries(gdf: gpd.GeoDataFrame, *, label: str) -> None:
    if gdf.geometry.isna().any():
        raise ValueError(f"{label} contains null geometries.")
    if gdf.geometry.is_empty.any():
        raise ValueError(f"{label} contains empty geometries.")
    bad = ~gdf.geom_type.isin(["Polygon", "MultiPolygon"])
    if bad.any():
        bad_types = sorted(gdf.loc[bad].geom_type.astype(str).unique().tolist())
        raise ValueError(f"{label} contains non-areal geometries: {bad_types}.")


def _normalize_pfaf_id_series(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().fillna("")


def load_aqueduct_boundaries(path: Path) -> gpd.GeoDataFrame:
    """Load the clean India Aqueduct polygons keyed by ``pfaf_id``."""
    gdf = gpd.read_file(path)
    if "pfaf_id" not in gdf.columns:
        raise ValueError(f"Aqueduct layer is missing required column 'pfaf_id': {path}")
    gdf = ensure_epsg4326(gdf)
    _assert_areal_geometries(gdf, label="Aqueduct boundaries")

    out = gdf[["pfaf_id", "geometry"]].copy()
    out["pfaf_id"] = _normalize_pfaf_id_series(out["pfaf_id"])
    if out["pfaf_id"].eq("").any():
        raise ValueError("Aqueduct boundaries contain blank pfaf_id values.")
    if out["pfaf_id"].duplicated().any():
        dupes = out.loc[out["pfaf_id"].duplicated(keep=False), "pfaf_id"].tolist()
        raise ValueError(f"Aqueduct boundaries contain duplicate pfaf_id values: {dupes[:10]}")
    return out.reset_index(drop=True)


def load_soi_hydro_boundaries(path: Path, *, level: HydroLevel) -> gpd.GeoDataFrame:
    """Load canonical SOI basin or sub-basin boundaries."""
    gdf = gpd.read_file(path)
    gdf = ensure_epsg4326(gdf)
    gdf = ensure_hydro_columns(gdf, level=level)
    _assert_areal_geometries(gdf, label=f"SOI {level} boundaries")

    if level == "sub_basin":
        keep_cols = ["basin_id", "basin_name", "subbasin_id", "subbasin_code", "subbasin_name", "geometry"]
    else:
        keep_cols = ["basin_id", "basin_name", "geometry"]
    return gdf[keep_cols].copy().reset_index(drop=True)


def build_aqueduct_hydro_crosswalk(
    aqueduct_gdf: gpd.GeoDataFrame,
    hydro_gdf: gpd.GeoDataFrame,
    *,
    hydro_level: HydroLevel,
    area_epsg: int = 6933,
) -> pd.DataFrame:
    """Build a durable Aqueduct-to-SOI overlap table."""
    aqueduct_proj = aqueduct_gdf.to_crs(epsg=area_epsg).copy()
    hydro_proj = hydro_gdf.to_crs(epsg=area_epsg).copy()

    target_area_col = "subbasin_area_km2" if hydro_level == "sub_basin" else "basin_area_km2"
    pfaf_fraction_col = "pfaf_area_fraction_in_subbasin" if hydro_level == "sub_basin" else "pfaf_area_fraction_in_basin"
    target_fraction_col = "subbasin_area_fraction_in_pfaf" if hydro_level == "sub_basin" else "basin_area_fraction_in_pfaf"

    aqueduct_proj["pfaf_area_km2"] = aqueduct_proj.geometry.area / 1_000_000.0
    hydro_proj[target_area_col] = hydro_proj.geometry.area / 1_000_000.0

    if hydro_level == "sub_basin":
        hydro_cols = [
            "basin_id",
            "basin_name",
            "subbasin_id",
            "subbasin_code",
            "subbasin_name",
            target_area_col,
            "geometry",
        ]
        group_cols = [
            "pfaf_id",
            "pfaf_area_km2",
            "basin_id",
            "basin_name",
            "subbasin_id",
            "subbasin_code",
            "subbasin_name",
            target_area_col,
        ]
        sort_cols = ["basin_name", "subbasin_name", "pfaf_id"]
    else:
        hydro_cols = ["basin_id", "basin_name", target_area_col, "geometry"]
        group_cols = ["pfaf_id", "pfaf_area_km2", "basin_id", "basin_name", target_area_col]
        sort_cols = ["basin_name", "pfaf_id"]

    intersections = gpd.overlay(
        aqueduct_proj[["pfaf_id", "pfaf_area_km2", "geometry"]].copy(),
        hydro_proj[hydro_cols].copy(),
        how="intersection",
    )

    if intersections.empty:
        return pd.DataFrame(columns=group_cols + ["intersection_area_km2", pfaf_fraction_col, target_fraction_col])

    intersections["intersection_area_km2"] = intersections.geometry.area / 1_000_000.0
    intersections = intersections.loc[intersections["intersection_area_km2"] > 0].copy()
    if intersections.empty:
        raise ValueError("Aqueduct and SOI hydro layers intersect only on zero-area boundaries.")

    grouped = (
        intersections.groupby(group_cols, dropna=False, as_index=False)["intersection_area_km2"]
        .sum()
    )
    grouped[pfaf_fraction_col] = grouped["intersection_area_km2"] / grouped["pfaf_area_km2"]
    grouped[target_fraction_col] = grouped["intersection_area_km2"] / grouped[target_area_col]
    return grouped.sort_values(sort_cols).reset_index(drop=True)


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Output already exists (pass --overwrite): {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Aqueduct HydroSHEDS-to-SOI hydro crosswalk CSVs.")
    parser.add_argument(
        "--aqueduct",
        type=str,
        default=str(_default_aqueduct_dir() / "baseline_clean_india.geojson"),
        help="Path to the clean India Aqueduct polygon GeoJSON.",
    )
    parser.add_argument("--basins", type=str, default=str(BASINS_PATH), help="Path to canonical SOI basins GeoJSON.")
    parser.add_argument(
        "--subbasins",
        type=str,
        default=str(SUBBASINS_PATH),
        help="Path to canonical SOI sub-basins GeoJSON.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(_default_aqueduct_dir()),
        help="Directory for the Aqueduct hydro crosswalk CSV outputs.",
    )
    parser.add_argument("--area-epsg", type=int, default=6933, help="Equal-area EPSG used for overlap calculations.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output CSVs.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    aqueduct_path = Path(args.aqueduct).expanduser().resolve()
    basins_path = Path(args.basins).expanduser().resolve()
    subbasins_path = Path(args.subbasins).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if not aqueduct_path.exists():
        raise FileNotFoundError(f"Aqueduct GeoJSON not found: {aqueduct_path}")
    if not basins_path.exists():
        raise FileNotFoundError(f"Basins GeoJSON not found: {basins_path}")
    if not subbasins_path.exists():
        raise FileNotFoundError(f"Sub-basins GeoJSON not found: {subbasins_path}")

    aqueduct_gdf = load_aqueduct_boundaries(aqueduct_path)
    basin_gdf = load_soi_hydro_boundaries(basins_path, level="basin")
    subbasin_gdf = load_soi_hydro_boundaries(subbasins_path, level="sub_basin")

    basin_df = build_aqueduct_hydro_crosswalk(
        aqueduct_gdf,
        basin_gdf,
        hydro_level="basin",
        area_epsg=int(args.area_epsg),
    )
    subbasin_df = build_aqueduct_hydro_crosswalk(
        aqueduct_gdf,
        subbasin_gdf,
        hydro_level="sub_basin",
        area_epsg=int(args.area_epsg),
    )

    basin_out = output_dir / "aqueduct_basin_crosswalk.csv"
    subbasin_out = output_dir / "aqueduct_subbasin_crosswalk.csv"
    _write_csv(basin_df, basin_out, overwrite=bool(args.overwrite))
    _write_csv(subbasin_df, subbasin_out, overwrite=bool(args.overwrite))

    print("AQUEDUCT HYDRO CROSSWALKS")
    print(f"aqueduct_pfaf_count: {len(aqueduct_gdf)}")
    print(f"basin_rows: {len(basin_df)}")
    print(f"subbasin_rows: {len(subbasin_df)}")
    print(f"basin_out: {basin_out}")
    print(f"subbasin_out: {subbasin_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

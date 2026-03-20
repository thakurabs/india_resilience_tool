#!/usr/bin/env python3
"""
Convert a block-level shapefile (.shp) to a standardized GeoJSON (EPSG:4326)
matching the IRT-style schema used for districts.

Output schema (properties):
- state: str
- district: str
- block: str
- __key: str  (normalized stable key: STATE|DISTRICT|BLOCK)
- geometry: Polygon/MultiPolygon in EPSG:4326

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Optional

import geopandas as gpd


def _norm_token(text: object) -> str:
    """
    Normalize a name token to a stable, comparable form.

    Rules:
    - stringify
    - strip
    - collapse whitespace
    - remove most punctuation
    - uppercase
    """
    if text is None:
        return ""
    s = str(text).strip()
    s = re.sub(r"\s+", " ", s)
    # keep letters, numbers, spaces, and a few separators; drop the rest
    s = re.sub(r"[^0-9A-Za-z \-_/().]", "", s)
    s = s.upper()
    return s


def _pick_first_matching_column(columns: list[str], patterns: list[str]) -> Optional[str]:
    cols_lower = {c.lower(): c for c in columns}
    for c_lower, original in cols_lower.items():
        for pat in patterns:
            if pat in c_lower:
                return original
    return None


def _fix_invalid_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Attempt to fix invalid geometries in a lightweight, dependency-safe way.
    """
    # Some environments have shapely.make_valid, some don't; buffer(0) is a common fallback.
    def _fix_one(geom):
        if geom is None or geom.is_empty:
            return geom
        try:
            if geom.is_valid:
                return geom
        except Exception:
            pass
        try:
            return geom.buffer(0)
        except Exception:
            return geom

    gdf = gdf.copy()
    gdf["geometry"] = gdf["geometry"].apply(_fix_one)
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()
    return gdf


def convert_blocks_shp_to_geojson(
    shp_path: Path,
    out_path: Path,
    *,
    state_col: Optional[str],
    district_col: Optional[str],
    block_col: Optional[str],
    simplify_m: float,
    assume_epsg: Optional[int],
) -> Path:
    """
    Convert block shapefile to standardized GeoJSON in EPSG:4326.

    Args:
        shp_path: Path to .shp
        out_path: GeoJSON output path
        state_col: Column name for state (optional; auto-detected if None)
        district_col: Column name for district (optional; auto-detected if None)
        block_col: Column name for block (optional; auto-detected if None)
        simplify_m: Simplification tolerance in meters (0 disables simplification)
        assume_epsg: EPSG code to assume if CRS is missing (None = error)

    Returns:
        Path to output GeoJSON
    """
    if not shp_path.exists():
        raise FileNotFoundError(f"Shapefile not found: {shp_path}")

    gdf = gpd.read_file(shp_path)

    if gdf.empty:
        raise ValueError("Input shapefile has 0 features.")

    if "geometry" not in gdf.columns:
        raise ValueError("Input file has no geometry column.")

    # CRS handling
    if gdf.crs is None:
        if assume_epsg is None:
            raise ValueError(
                "Input shapefile has no CRS. Re-run with --assume-epsg <code> "
                "(ONLY if you are sure of the source CRS)."
            )
        gdf = gdf.set_crs(epsg=int(assume_epsg))

    # Auto-detect hierarchy columns if not provided
    cols = [c for c in gdf.columns if c != "geometry"]

    if state_col is None:
        state_col = _pick_first_matching_column(
            cols, patterns=["state", "st_nm", "stname", "state_ut", "adm1", "stname"]
        )
    if district_col is None:
        district_col = _pick_first_matching_column(
            cols, patterns=["district", "dist", "adm2", "dtname", "distname"]
        )
    if block_col is None:
        block_col = _pick_first_matching_column(
            cols,
            patterns=[
                "block",
                "mandal",
                "tehsil",
                "taluk",
                "taluka",
                "subdistrict",
                "sub_district",
                "adm3",
                "sdname",
            ],
        )

    missing = [name for name, col in [("state", state_col), ("district", district_col), ("block", block_col)] if col is None]
    if missing:
        raise ValueError(
            "Could not auto-detect required columns: "
            + ", ".join(missing)
            + "\nRe-run specifying them explicitly, e.g. "
            + "--state-col ST_NM --district-col DIST_NM --block-col BLOCK_NM"
        )

    # Keep only what we want (reduces size + enforces schema)
    keep_cols = [state_col, district_col, block_col, "geometry"]
    gdf = gdf[keep_cols].copy()
    gdf = gdf.rename(columns={state_col: "state", district_col: "district", block_col: "block"})

    # Fix invalid geometries before simplification
    gdf = _fix_invalid_geometries(gdf)

    # Explode multipart geometries into individual features (keeps properties)
    try:
        gdf = gdf.explode(index_parts=False, ignore_index=True)
    except TypeError:
        # Older geopandas
        gdf = gdf.explode()

    # Reproject to EPSG:4326 (dashboard expects this)
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Simplify in meters by temporarily projecting to Web Mercator
    if simplify_m and simplify_m > 0:
        gdf_3857 = gdf.to_crs(epsg=3857)
        gdf_3857["geometry"] = gdf_3857["geometry"].simplify(float(simplify_m), preserve_topology=True)
        gdf = gdf_3857.to_crs(epsg=4326)
        gdf = _fix_invalid_geometries(gdf)

    # Standardize strings and build __key
    gdf["state"] = gdf["state"].astype(str).str.strip()
    gdf["district"] = gdf["district"].astype(str).str.strip()
    gdf["block"] = gdf["block"].astype(str).str.strip()

    gdf["__key"] = (
        gdf["state"].map(_norm_token)
        + "|"
        + gdf["district"].map(_norm_token)
        + "|"
        + gdf["block"].map(_norm_token)
    )

    # Reorder columns
    gdf = gdf[["state", "district", "block", "__key", "geometry"]].copy()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_path, driver="GeoJSON")

    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert block shapefile to standardized EPSG:4326 GeoJSON.")
    parser.add_argument("shp", type=str, help="Path to blocks shapefile (.shp)")
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output GeoJSON path (default: <shp_dir>/blocks_4326.geojson)",
    )
    parser.add_argument("--state-col", type=str, default=None, help="Column name for state")
    parser.add_argument("--district-col", type=str, default=None, help="Column name for district")
    parser.add_argument("--block-col", type=str, default=None, help="Column name for block/mandal/tehsil")
    parser.add_argument(
        "--simplify-m",
        type=float,
        default=100.0,
        help="Geometry simplification tolerance in meters (0 disables). Default: 100m",
    )
    parser.add_argument(
        "--assume-epsg",
        type=int,
        default=None,
        help="If input CRS is missing, assume this EPSG code (use with care).",
    )

    args = parser.parse_args()
    shp_path = Path(args.shp).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve() if args.out else (shp_path.parent / "blocks_4326.geojson")

    try:
        out = convert_blocks_shp_to_geojson(
            shp_path=shp_path,
            out_path=out_path,
            state_col=args.state_col,
            district_col=args.district_col,
            block_col=args.block_col,
            simplify_m=args.simplify_m,
            assume_epsg=args.assume_epsg,
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    size_mb = out.stat().st_size / (1024 * 1024)
    print(f"OK: Wrote: {out}")
    print(f"   Size: {size_mb:.2f} MB")
    print("   Schema: state, district, block, __key, geometry (EPSG:4326)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

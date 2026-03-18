#!/usr/bin/env python3
"""
Build the canonical block boundaries GeoJSON for IRT.

This tool rebuilds ``IRT_DATA_DIR/blocks_4326.geojson`` from the source block
shapefile using canonical ADM3 columns and UTF-8-aware label handling.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd

from india_resilience_tool.data.adm3_loader import (
    collect_adm3_label_anomalies,
    ensure_adm3_columns,
    ensure_epsg4326,
)
from paths import BLOCKS_PATH, get_paths_config


CANONICAL_BLOCK_COLUMNS = ["state_name", "district_name", "block_name", "block_type", "block_lgd_code"]
_INVALID_ADMIN_VALUES = {"", "nan", "none", "null", "nat"}


def _default_source_shapefile() -> Path:
    data_dir = get_paths_config().data_dir
    return (
        data_dir
        / "Block_GH_WUP_POP R2025A _GHS_WUP"
        / "Block_GH_WUP_POP R2025A _GHS_WUP.shp"
    )


def _default_summary_path() -> Path:
    return get_paths_config().data_dir / "block_boundary_repair_summary.csv"


def _default_anomalies_path() -> Path:
    return get_paths_config().data_dir / "block_boundary_label_anomalies.csv"


def _invalid_identity_mask(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.strip().fillna("")
    return normalized.str.lower().isin(_INVALID_ADMIN_VALUES)


def _block_key(state_name: object, district_name: object, block_name: object) -> str:
    return f"{str(state_name).strip()}::{str(district_name).strip()}::{str(block_name).strip()}"


def _fix_invalid_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Apply a lightweight validity repair suitable for canonical boundary rebuilds."""

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

    out = gdf.copy()
    out["geometry"] = out["geometry"].apply(_fix_one)
    return out.loc[out.geometry.notna() & ~out.geometry.is_empty].copy()


def _read_source_blocks(path: Path, *, encoding: str, assume_epsg: int | None) -> gpd.GeoDataFrame:
    try:
        gdf = gpd.read_file(path, encoding=encoding)
    except TypeError:
        gdf = gpd.read_file(path)

    if gdf.crs is None:
        if assume_epsg is None:
            raise ValueError(
                "Source block shapefile has no CRS. Re-run with --assume-epsg if you know the source CRS."
            )
        gdf = gdf.set_crs(epsg=int(assume_epsg))
    return gdf


def prepare_blocks_geojson(
    shp_path: Path,
    *,
    encoding: str = "utf-8",
    assume_epsg: int | None = None,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Return the canonical block boundary GeoDataFrame plus QA summary/anomalies.
    """
    raw = _read_source_blocks(shp_path, encoding=encoding, assume_epsg=assume_epsg)
    raw_feature_count = int(len(raw))

    gdf = _fix_invalid_geometries(raw)
    gdf = ensure_epsg4326(gdf)
    gdf = ensure_adm3_columns(gdf)

    keep_cols = [col for col in CANONICAL_BLOCK_COLUMNS if col in gdf.columns]
    gdf = gdf[[*keep_cols, "geometry"]].copy()

    invalid_mask = (
        _invalid_identity_mask(gdf["state_name"])
        | _invalid_identity_mask(gdf["district_name"])
        | _invalid_identity_mask(gdf["block_name"])
    )
    dropped_invalid_rows = int(invalid_mask.sum())
    gdf = gdf.loc[~invalid_mask].copy().reset_index(drop=True)
    if gdf.empty:
        raise ValueError("No valid block rows remain after canonical identity filtering.")

    gdf["block_key"] = [
        _block_key(state_name, district_name, block_name)
        for state_name, district_name, block_name in zip(
            gdf["state_name"], gdf["district_name"], gdf["block_name"]
        )
    ]
    duplicate_rows_before_dissolve = int(gdf["block_key"].duplicated().sum())

    aggfunc = {
        "state_name": "first",
        "district_name": "first",
        "block_name": "first",
    }
    if "block_type" in gdf.columns:
        aggfunc["block_type"] = "first"
    if "block_lgd_code" in gdf.columns:
        aggfunc["block_lgd_code"] = "first"

    gdf = gdf.dissolve(by="block_key", as_index=False, aggfunc=aggfunc).reset_index(drop=True)

    anomalies_df = collect_adm3_label_anomalies(gdf)
    summary_df = pd.DataFrame(
        [
            {
                "source_shapefile": str(shp_path),
                "raw_feature_count": raw_feature_count,
                "canonical_feature_count": int(len(gdf)),
                "unique_state_count": int(gdf["state_name"].astype(str).nunique()),
                "dropped_invalid_identity_rows": dropped_invalid_rows,
                "duplicate_rows_before_dissolve": duplicate_rows_before_dissolve,
                "suspicious_label_rows": int(len(anomalies_df)),
            }
        ]
    )
    return gdf, summary_df, anomalies_df


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file without --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_geojson(gdf: gpd.GeoDataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file without --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        gdf.to_file(path, driver="GeoJSON", encoding="utf-8")
    except TypeError:
        gdf.to_file(path, driver="GeoJSON")


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the canonical IRT blocks_4326.geojson from the source block shapefile.")
    parser.add_argument("--shp", type=str, default=str(_default_source_shapefile()), help="Path to the source block shapefile.")
    parser.add_argument("--out", type=str, default=str(BLOCKS_PATH), help="Path to the canonical block GeoJSON.")
    parser.add_argument("--qa-summary-out", type=str, default=str(_default_summary_path()), help="CSV path for the block-boundary summary QA output.")
    parser.add_argument("--qa-anomalies-out", type=str, default=str(_default_anomalies_path()), help="CSV path for suspicious repaired-label rows.")
    parser.add_argument("--encoding", type=str, default="utf-8", help="Encoding hint for the source shapefile attributes.")
    parser.add_argument("--assume-epsg", type=int, default=None, help="EPSG code to assume if the source shapefile is missing CRS metadata.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    shp_path = Path(args.shp).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()
    qa_summary_out = Path(args.qa_summary_out).expanduser().resolve()
    qa_anomalies_out = Path(args.qa_anomalies_out).expanduser().resolve()

    if not shp_path.exists():
        raise FileNotFoundError(f"Source block shapefile not found: {shp_path}")

    gdf, summary_df, anomalies_df = prepare_blocks_geojson(
        shp_path,
        encoding=str(args.encoding),
        assume_epsg=args.assume_epsg,
    )

    _write_csv(summary_df, qa_summary_out, overwrite=bool(args.overwrite))
    _write_csv(anomalies_df, qa_anomalies_out, overwrite=bool(args.overwrite))
    if not anomalies_df.empty:
        sample = anomalies_df[["field", "state_name", "district_name", "block_name"]].head(10).to_dict(orient="records")
        raise ValueError(
            "Canonical block rebuild detected suspicious admin labels after repair. "
            f"Inspect {qa_anomalies_out}. Sample: {sample}"
        )

    output_cols = [col for col in ["state_name", "district_name", "block_name", "block_type", "block_lgd_code", "block_key", "geometry"] if col in gdf.columns]
    _write_geojson(gdf[output_cols].copy(), out_path, overwrite=bool(args.overwrite))

    print("BLOCKS GEOJSON")
    print(f"source_shapefile: {shp_path}")
    print(f"canonical_feature_count: {len(gdf)}")
    print(f"unique_state_count: {summary_df['unique_state_count'].iloc[0]}")
    print(f"qa_summary: {qa_summary_out}")
    print(f"qa_anomalies: {qa_anomalies_out}")
    print(f"out: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

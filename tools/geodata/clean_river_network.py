#!/usr/bin/env python3
"""
Clean the Survey of India river network into canonical IRT artifacts.

Outputs:
- Canonical GeoParquet for processing and future topology work
- Simplified GeoJSON for inspection/display
- Row-level QA CSV for flagged records
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path
from typing import Any, Optional

import geopandas as gpd
import pandas as pd
from shapely.errors import ShapelyError
from shapely.geometry import LineString, MultiLineString
from shapely.geometry.base import BaseGeometry

from paths import get_paths_config


_PLACEHOLDER_SUBBASIN_VALUES = {"MAJOR RIVER", "MAJOR RIVERS"}
_DISPLAY_COLUMNS = [
    "river_feature_id",
    "source_uid_river",
    "source_uid_is_duplicate",
    "river_name_clean",
    "basin_name_clean",
    "subbasin_name_clean",
    "state_names_clean",
    "length_km_source",
    "geometry",
]
_QA_COLUMNS = [
    "river_feature_id",
    "source_uid_river",
    "source_uid_is_duplicate",
    "rivname",
    "ba_name",
    "sub_basin",
    "state_al",
    "issue_duplicate_uid",
    "issue_missing_river_name",
    "issue_missing_basin_name",
    "issue_missing_subbasin_name",
    "issue_placeholder_subbasin",
    "issue_missing_state_names",
    "issue_missing_length_source",
    "issue_length_diff_gt_threshold",
    "issue_multipart",
]
_DEFAULT_LENGTH_COMPARE_EPSG = 6933


def _normalize_optional_text(value: object) -> Optional[str]:
    """Normalize text while preserving aliases/slashes and raw meaning."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    text = re.sub(r"\s+", " ", text)
    return text


def _validate_line_geometries(gdf: gpd.GeoDataFrame, *, label: str) -> None:
    """Raise on null, empty, or non-linear geometries."""
    if "geometry" not in gdf.columns:
        raise ValueError(f"{label} has no geometry column.")
    if gdf.geometry.isna().any():
        raise ValueError(f"{label} contains null geometries.")
    if gdf.geometry.is_empty.any():
        raise ValueError(f"{label} contains empty geometries.")
    bad = ~gdf.geom_type.isin(["LineString", "MultiLineString"])
    if bad.any():
        bad_types = sorted(gdf.loc[bad].geom_type.astype(str).unique().tolist())
        raise ValueError(f"{label} contains non-line geometries: {bad_types}.")


def _write_parquet(gdf: gpd.GeoDataFrame, path: Path) -> None:
    """Write GeoParquet with a clear error if parquet dependencies are missing."""
    try:
        gdf.to_parquet(path, index=False)
    except ImportError as exc:
        raise RuntimeError(
            "Writing GeoParquet requires pyarrow or fastparquet in the active environment."
        ) from exc


def _safe_issue_flag(series: pd.Series) -> pd.Series:
    """Return a nullable boolean issue flag with NA coerced to False."""
    return series.fillna(False).astype(bool)


def _part_count(geom: BaseGeometry) -> int:
    """Return the number of parts in a line geometry."""
    if isinstance(geom, MultiLineString):
        return len(list(geom.geoms))
    if isinstance(geom, LineString):
        return 1
    return 0


def _vertex_count(geom: BaseGeometry) -> int:
    """Return the total vertex count across all parts."""
    if isinstance(geom, LineString):
        return len(geom.coords)
    if isinstance(geom, MultiLineString):
        return sum(len(part.coords) for part in geom.geoms)
    return 0


def _build_base_id(row: pd.Series) -> str:
    """Build a deterministic base identifier from source attrs + geometry."""
    geom = row.geometry
    geom_token = geom.wkb_hex if geom is not None else ""
    length_val = row.get("length_km_source")
    length_token = ""
    try:
        if pd.notna(length_val):
            length_token = f"{float(length_val):.6f}"
    except Exception:
        length_token = ""

    tokens = [
        str(row.get("source_uid_river", "") or ""),
        str(row.get("river_name_clean", "") or ""),
        str(row.get("basin_name_clean", "") or ""),
        str(row.get("subbasin_name_clean", "") or ""),
        str(row.get("state_names_clean", "") or ""),
        length_token,
        geom_token,
    ]
    digest = hashlib.sha1("||".join(tokens).encode("utf-8")).hexdigest()[:16]
    return f"riv_{digest}"


def _assign_feature_ids(gdf: gpd.GeoDataFrame) -> pd.Series:
    """Assign a deterministic, unique feature id."""
    base_ids = gdf.apply(_build_base_id, axis=1)
    counts: dict[str, int] = {}
    feature_ids: list[str] = []
    for base_id in base_ids:
        counts[base_id] = counts.get(base_id, 0) + 1
        feature_ids.append(base_id if counts[base_id] == 1 else f"{base_id}_{counts[base_id]}")
    return pd.Series(feature_ids, index=gdf.index, dtype="string")


def _choose_length_compare_gdf(
    gdf: gpd.GeoDataFrame,
    *,
    fallback_epsg: int = _DEFAULT_LENGTH_COMPARE_EPSG,
) -> gpd.GeoDataFrame:
    """Choose a projected GeoDataFrame for length comparison."""
    if gdf.crs is not None and getattr(gdf.crs, "is_projected", False):
        return gdf
    return gdf.to_crs(epsg=fallback_epsg)


def clean_river_network_gdf(
    gdf: gpd.GeoDataFrame,
    *,
    source_path: Path,
    length_diff_threshold_pct: float = 10.0,
    length_compare_epsg: int = _DEFAULT_LENGTH_COMPARE_EPSG,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame, dict[str, Any]]:
    """
    Clean a raw river-network GeoDataFrame into canonical IRT artifacts.

    Returns:
      (cleaned_gdf_epsg4326, qa_rows_df, summary_dict)
    """
    if gdf.empty:
        raise ValueError("River network source has 0 features.")
    if gdf.crs is None:
        raise ValueError("River network source has no CRS.")
    if length_diff_threshold_pct < 0:
        raise ValueError("length_diff_threshold_pct must be >= 0.")

    _validate_line_geometries(gdf, label="River network source")

    source_crs_wkt = gdf.crs.to_wkt() if gdf.crs is not None else ""
    compare_gdf = _choose_length_compare_gdf(gdf, fallback_epsg=length_compare_epsg).copy()
    compare_gdf["length_km_geometry"] = compare_gdf.geometry.length / 1000.0

    out = gdf.to_crs(epsg=4326).copy()
    _validate_line_geometries(out, label="River network source reprojected to EPSG:4326")

    out["source_uid_river"] = out.get("UID_River", pd.Series(index=out.index, dtype="object")).map(_normalize_optional_text)
    out["source_uid_is_duplicate"] = out["source_uid_river"].notna() & out["source_uid_river"].duplicated(keep=False)

    out["river_name_clean"] = out.get("rivname", pd.Series(index=out.index, dtype="object")).map(_normalize_optional_text)
    out["basin_name_clean"] = out.get("ba_name", pd.Series(index=out.index, dtype="object")).map(_normalize_optional_text)
    out["subbasin_name_clean"] = out.get("sub_basin", pd.Series(index=out.index, dtype="object")).map(_normalize_optional_text)
    out["state_names_clean"] = out.get("state_al", pd.Series(index=out.index, dtype="object")).map(_normalize_optional_text)
    out["origin_clean"] = out.get("origin", pd.Series(index=out.index, dtype="object")).map(_normalize_optional_text)
    out["major_trib_clean"] = out.get("major_trib", pd.Series(index=out.index, dtype="object")).map(_normalize_optional_text)
    out["confluence_clean"] = out.get("Confluence", pd.Series(index=out.index, dtype="object")).map(_normalize_optional_text)
    out["remark_clean"] = out.get("remark", pd.Series(index=out.index, dtype="object")).map(_normalize_optional_text)

    out["is_multipart"] = out.geometry.geom_type.eq("MultiLineString")
    out["part_count"] = out.geometry.apply(_part_count).astype("int64")
    out["vertex_count"] = out.geometry.apply(_vertex_count).astype("int64")
    out["length_km_source"] = pd.to_numeric(out.get("length_km"), errors="coerce")
    out["length_km_geometry"] = compare_gdf["length_km_geometry"].values
    out["length_diff_km"] = (out["length_km_geometry"] - out["length_km_source"]).abs()
    out["length_diff_pct"] = out["length_diff_km"] / out["length_km_source"].where(out["length_km_source"] > 0)
    out["crs_source_wkt"] = source_crs_wkt
    out["geometry_type_clean"] = out.geom_type.astype(str)

    out["issue_duplicate_uid"] = _safe_issue_flag(out["source_uid_is_duplicate"])
    out["issue_missing_river_name"] = _safe_issue_flag(out["river_name_clean"].isna())
    out["issue_missing_basin_name"] = _safe_issue_flag(out["basin_name_clean"].isna())
    out["issue_missing_subbasin_name"] = _safe_issue_flag(out["subbasin_name_clean"].isna())
    out["issue_placeholder_subbasin"] = _safe_issue_flag(
        out["subbasin_name_clean"].fillna("").str.upper().isin(_PLACEHOLDER_SUBBASIN_VALUES)
    )
    out["issue_missing_state_names"] = _safe_issue_flag(out["state_names_clean"].isna())
    out["issue_missing_length_source"] = _safe_issue_flag(
        out["length_km_source"].isna() | (out["length_km_source"] <= 0)
    )
    out["issue_length_diff_gt_threshold"] = _safe_issue_flag(
        out["length_diff_pct"].fillna(0).gt(float(length_diff_threshold_pct) / 100.0)
    )
    out["issue_multipart"] = _safe_issue_flag(out["is_multipart"])

    out["river_feature_id"] = _assign_feature_ids(out)
    if out["river_feature_id"].duplicated().any():
        raise ValueError("Failed to generate unique river_feature_id values.")

    issue_cols = [col for col in _QA_COLUMNS if col.startswith("issue_")]
    issue_mask = out[issue_cols].any(axis=1)
    qa_df = out.loc[issue_mask, [col for col in _QA_COLUMNS if col in out.columns]].copy()

    geom_counts = out.geom_type.astype(str).value_counts(dropna=False).to_dict()
    summary = {
        "source_path": str(source_path),
        "source_crs_wkt": source_crs_wkt,
        "feature_count": int(len(out)),
        "geometry_type_counts": geom_counts,
        "multipart_count": int(out["is_multipart"].sum()),
        "duplicate_uid_count": int(out["source_uid_is_duplicate"].sum()),
        "placeholder_subbasin_count": int(out["issue_placeholder_subbasin"].sum()),
        "null_counts": {
            "rivname": int(out.get("rivname", pd.Series(dtype="object")).isna().sum()),
            "ba_name": int(out.get("ba_name", pd.Series(dtype="object")).isna().sum()),
            "sub_basin": int(out.get("sub_basin", pd.Series(dtype="object")).isna().sum()),
            "state_al": int(out.get("state_al", pd.Series(dtype="object")).isna().sum()),
            "origin": int(out.get("origin", pd.Series(dtype="object")).isna().sum()),
            "major_trib": int(out.get("major_trib", pd.Series(dtype="object")).isna().sum()),
            "remark": int(out.get("remark", pd.Series(dtype="object")).isna().sum()),
            "length_km": int(out["length_km_source"].isna().sum()),
        },
        "length_compare_crs": compare_gdf.crs.to_string() if compare_gdf.crs is not None else "",
        "length_diff_km_mean": float(out["length_diff_km"].dropna().mean()) if out["length_diff_km"].notna().any() else None,
        "length_diff_pct_mean": float(out["length_diff_pct"].dropna().mean()) if out["length_diff_pct"].notna().any() else None,
        "length_diff_pct_max": float(out["length_diff_pct"].dropna().max()) if out["length_diff_pct"].notna().any() else None,
        "qa_row_count": int(len(qa_df)),
    }
    return out.reset_index(drop=True), qa_df.reset_index(drop=True), summary


def build_river_network_display_gdf(
    cleaned_gdf: gpd.GeoDataFrame,
    *,
    display_tolerance: float = 250.0,
) -> gpd.GeoDataFrame:
    """Build a simplified display GeoJSON view from the cleaned dataset."""
    if cleaned_gdf.empty:
        return cleaned_gdf[_DISPLAY_COLUMNS].copy()

    display = cleaned_gdf[_DISPLAY_COLUMNS].copy()
    tol = float(display_tolerance)
    if tol < 0.0:
        raise ValueError("display_tolerance must be >= 0.")
    if tol > 0.0:
        display_3857 = display.to_crs(epsg=3857)
        try:
            display_3857["geometry"] = display_3857.geometry.simplify(
                tol,
                preserve_topology=True,
            )
        except ShapelyError as exc:
            raise ValueError(f"Failed to simplify river display geometry: {exc}") from exc
        display = display_3857.to_crs(epsg=4326)
    _validate_line_geometries(display, label="River network display artifact")
    return display.reset_index(drop=True)


def _print_summary(summary: dict[str, Any]) -> None:
    """Print a compact run summary."""
    print("RIVER NETWORK CLEANING")
    print(f"Source: {summary['source_path']}")
    print(f"Source CRS: {summary['source_crs_wkt'][:120]}{'...' if len(summary['source_crs_wkt']) > 120 else ''}")
    print(f"Feature count: {summary['feature_count']}")
    print(f"Geometry counts: {summary['geometry_type_counts']}")
    print(f"Multipart features: {summary['multipart_count']}")
    print(f"Duplicate UID_River features: {summary['duplicate_uid_count']}")
    print(f"Placeholder sub-basin names: {summary['placeholder_subbasin_count']}")
    print(f"Length compare CRS: {summary['length_compare_crs']}")
    if summary["length_diff_km_mean"] is not None:
        print(f"Mean abs length diff (km): {summary['length_diff_km_mean']:.3f}")
    if summary["length_diff_pct_mean"] is not None:
        print(f"Mean abs length diff (%): {summary['length_diff_pct_mean'] * 100.0:.2f}")
    if summary["length_diff_pct_max"] is not None:
        print(f"Max abs length diff (%): {summary['length_diff_pct_max'] * 100.0:.2f}")
    print(f"QA rows: {summary['qa_row_count']}")


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point."""
    data_dir = get_paths_config().data_dir

    parser = argparse.ArgumentParser(
        description="Clean the Survey of India river network into canonical IRT artifacts."
    )
    parser.add_argument("--src", type=str, required=True, help="Path to river_network_goi.shp")
    parser.add_argument(
        "--out-parquet",
        type=str,
        default=str(data_dir / "river_network.parquet"),
        help="Canonical GeoParquet output path. Default: IRT_DATA_DIR/river_network.parquet",
    )
    parser.add_argument(
        "--out-display-geojson",
        type=str,
        default=str(data_dir / "river_network_display.geojson"),
        help="Simplified display GeoJSON output path. Default: IRT_DATA_DIR/river_network_display.geojson",
    )
    parser.add_argument(
        "--out-qa",
        type=str,
        default=str(data_dir / "river_network_qa.csv"),
        help="QA CSV output path. Default: IRT_DATA_DIR/river_network_qa.csv",
    )
    parser.add_argument(
        "--display-tolerance",
        type=float,
        default=250.0,
        help="Line simplification tolerance for display GeoJSON, in meters. Default: 250",
    )
    parser.add_argument(
        "--length-diff-threshold-pct",
        type=float,
        default=10.0,
        help="Flag rows whose recomputed length differs from source length_km by more than this percent. Default: 10",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output files if they already exist.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and summarize without writing outputs.")
    args = parser.parse_args(argv)

    src_path = Path(args.src).expanduser().resolve()
    out_parquet = Path(args.out_parquet).expanduser().resolve()
    out_display_geojson = Path(args.out_display_geojson).expanduser().resolve()
    out_qa = Path(args.out_qa).expanduser().resolve()

    if not src_path.exists():
        raise FileNotFoundError(f"River network source not found: {src_path}")
    if args.display_tolerance < 0:
        raise ValueError("--display-tolerance must be >= 0.")
    if args.length_diff_threshold_pct < 0:
        raise ValueError("--length-diff-threshold-pct must be >= 0.")

    if not args.dry_run:
        for output_path in (out_parquet, out_display_geojson, out_qa):
            if output_path.exists() and not args.overwrite:
                raise FileExistsError(
                    f"Output already exists: {output_path}. Re-run with --overwrite to replace it."
                )

    gdf = gpd.read_file(src_path)
    cleaned_gdf, qa_df, summary = clean_river_network_gdf(
        gdf,
        source_path=src_path,
        length_diff_threshold_pct=float(args.length_diff_threshold_pct),
    )
    display_gdf = build_river_network_display_gdf(
        cleaned_gdf,
        display_tolerance=float(args.display_tolerance),
    )

    _print_summary(summary)

    if args.dry_run:
        print("Dry run complete. No files written.")
        return 0

    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    out_display_geojson.parent.mkdir(parents=True, exist_ok=True)
    out_qa.parent.mkdir(parents=True, exist_ok=True)

    _write_parquet(cleaned_gdf, out_parquet)
    display_gdf.to_file(out_display_geojson, driver="GeoJSON")
    qa_df.to_csv(out_qa, index=False)

    print(f"Wrote canonical parquet: {out_parquet}")
    print(f"Wrote display GeoJSON: {out_display_geojson}")
    print(f"Wrote QA CSV: {out_qa}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        raise

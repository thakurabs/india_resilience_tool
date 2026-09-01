#!/usr/bin/env python3
"""Build district/block built-up area exposure masters and overlay artifacts.

The source raster is treated as built-up area in ``m2/source cell``. Tabulation
uses the raster's native CRS: admin polygons are reprojected to the raster CRS
and cells are included with ``all_touched=False`` (centroid-inclusion edge rule).
The canonical share metric uses the full polygon area in EPSG:6933 as its
denominator; supported-cell denominator fields are emitted only as QA.
"""

from __future__ import annotations

import argparse
import json
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

from affine import Affine
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from PIL import Image
from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window
from rasterio.warp import Resampling, calculate_default_transform, reproject, transform_bounds
from shapely.geometry import mapping

from paths import get_master_csv_filename, get_paths_config, resolve_processed_root
from tools.geodata.build_district_subbasin_crosswalk import (
    load_block_boundaries,
    load_district_boundaries,
)


AdminLevel = Literal["district", "block"]

SNAPSHOT_SCENARIO = "snapshot"
SNAPSHOT_PERIOD = "Current"
AREA_EPSG = 6933
DEFAULT_RASTER_NAME = "Cleaned_India_Built_Surface_WGS84.tif"
INVALID_VALUE = 65535.0
NATIONAL_TOTAL_KM2_MIN = 43_000.0
NATIONAL_TOTAL_KM2_MAX = 220_000.0
OVERLAY_MAX_DIMENSION = 4096
IMAGE_CRS = "EPSG:3857"
BUILT_UP_AREA_OVERLAY_ID = "built_up_area_current_raster"

BUILT_UP_AREA_KM2_COL = "built_up_area_km2__snapshot__Current__mean"
BUILT_UP_AREA_SHARE_PCT_COL = "built_up_area_share_pct__snapshot__Current__mean"

BUILT_UP_BIN_EDGES_M2_PER_CELL: tuple[float, ...] = (0.0, 100.0, 500.0, 1000.0, 2500.0, 5000.0)
BUILT_UP_BIN_COLORS_HEX: tuple[Optional[str], ...] = (
    None,
    "#edf8fb",
    "#b2e2e2",
    "#66c2a4",
    "#2ca25f",
    "#006d2c",
    "#00441b",
)


@dataclass(frozen=True)
class RasterValidationSummary:
    """Raster contract fields checked before outputs are written."""

    path: Path
    crs: str
    transform: str
    dtype: str
    declared_nodata: Optional[float]
    bounds: tuple[float, float, float, float]
    min_value: float
    max_value: float
    national_built_up_area_km2: float


@dataclass(frozen=True)
class GeometryBuiltUpStats:
    """One polygon's built-up summary using the shared centroid-inclusion rule."""

    built_up_m2: float
    raster_supported_cell_count: int
    raster_supported_area_km2: float


def _default_raster_path() -> Path:
    return get_paths_config().data_dir / "built_up_area" / DEFAULT_RASTER_NAME


def _default_qa_dir() -> Path:
    return get_paths_config().data_dir / "built_up_area"


def _default_overlay_dir() -> Path:
    return get_paths_config().data_dir / "built_up_area" / "overlay"


def built_up_area_overlay_paths(overlay_dir: Path) -> tuple[Path, Path]:
    """Return canonical built-up overlay PNG and metadata paths."""
    return (
        overlay_dir / "built_up_area_current_overlay.png",
        overlay_dir / "built_up_area_current_overlay_meta.json",
    )


def _metric_col(slug: str) -> str:
    if slug == "built_up_area_km2":
        return BUILT_UP_AREA_KM2_COL
    if slug == "built_up_area_share_pct":
        return BUILT_UP_AREA_SHARE_PCT_COL
    raise ValueError(f"Unsupported built-up metric slug: {slug}")


def _identity_cols(level: AdminLevel) -> list[str]:
    if level == "block":
        return ["state_name", "district_name", "block_name", "block_key"]
    return ["state_name", "district_name", "district_key"]


def _output_key_cols(level: AdminLevel) -> list[str]:
    if level == "block":
        return ["state", "district", "block", "block_key"]
    return ["state", "district", "district_key"]


def _area_col(level: AdminLevel) -> str:
    return "block_area_km2" if level == "block" else "district_area_km2"


def _safe_pixel_area_m2(src: rasterio.io.DatasetReader) -> float:
    """Return source-cell area for projected rasters, or NaN for geographic CRS."""
    if src.crs is not None and getattr(src.crs, "is_projected", False):
        return float(abs(src.transform.a * src.transform.e))
    return float("nan")


def _valid_data_mask(data: np.ma.MaskedArray | np.ndarray) -> np.ndarray:
    arr = np.asarray(data, dtype=float)
    mask = np.ma.getmaskarray(data)
    return (~mask) & np.isfinite(arr) & (~np.isclose(arr, INVALID_VALUE))


def _scan_raster_summary(raster_path: Path) -> RasterValidationSummary:
    """Scan the raster block-by-block for min/max and national built-up total."""
    with rasterio.open(raster_path) as src:
        if src.count != 1:
            raise ValueError(f"Built-up raster must be single-band: {raster_path}")
        if src.crs is None:
            raise ValueError(f"Built-up raster has no CRS: {raster_path}")
        dtype = np.dtype(src.dtypes[0])
        if not np.issubdtype(dtype, np.number):
            raise ValueError(f"Built-up raster must be numeric: {raster_path}")

        valid_min = float("inf")
        valid_max = float("-inf")
        total_m2 = 0.0
        any_valid = False
        for _block_index, window in src.block_windows(1):
            data = src.read(1, window=window, masked=True)
            valid = _valid_data_mask(data)
            if not np.any(valid):
                continue
            vals = np.asarray(data, dtype=float)[valid]
            total_m2 += float(vals.sum())
            valid_min = min(valid_min, float(vals.min()))
            valid_max = max(valid_max, float(vals.max()))
            any_valid = True

        if not any_valid:
            valid_min = float("nan")
            valid_max = float("nan")

        return RasterValidationSummary(
            path=raster_path,
            crs=src.crs.to_string(),
            transform=str(src.transform),
            dtype=str(dtype),
            declared_nodata=None if src.nodata is None else float(src.nodata),
            bounds=tuple(float(v) for v in src.bounds),
            min_value=valid_min,
            max_value=valid_max,
            national_built_up_area_km2=total_m2 / 1_000_000.0,
        )


def validate_built_up_raster_contract(
    raster_path: Path,
    *,
    allow_total_outlier: bool = False,
) -> RasterValidationSummary:
    """Validate raster metadata and national total guardrails before writing outputs."""
    summary = _scan_raster_summary(raster_path)
    total = summary.national_built_up_area_km2
    if not allow_total_outlier and not (NATIONAL_TOTAL_KM2_MIN <= total <= NATIONAL_TOTAL_KM2_MAX):
        raise ValueError(
            "Built-up national total is outside the guardrail range "
            f"{NATIONAL_TOTAL_KM2_MIN:,.0f}-{NATIONAL_TOTAL_KM2_MAX:,.0f} km2: "
            f"{total:,.2f} km2. Recheck source units or rerun with --allow-total-outlier."
        )
    return summary


def _zonal_built_up_for_geometry(
    src: rasterio.io.DatasetReader,
    geom,
) -> GeometryBuiltUpStats:
    if geom is None or geom.is_empty:
        return GeometryBuiltUpStats(0.0, 0, float("nan"))
    try:
        window = geometry_window(src, [mapping(geom)])
    except WindowError:
        return GeometryBuiltUpStats(0.0, 0, float("nan"))

    data = src.read(1, window=window, masked=True)
    if data.size == 0:
        return GeometryBuiltUpStats(0.0, 0, float("nan"))
    geom_mask = geometry_mask(
        [mapping(geom)],
        out_shape=data.shape,
        transform=src.window_transform(window),
        invert=True,
        all_touched=False,
    )
    valid = _valid_data_mask(data) & geom_mask
    if not np.any(valid):
        pixel_area_m2 = _safe_pixel_area_m2(src)
        return GeometryBuiltUpStats(0.0, 0, 0.0 if np.isfinite(pixel_area_m2) else float("nan"))
    built_up_m2 = float(np.asarray(data, dtype=float)[valid].sum())
    supported_count = int(np.count_nonzero(valid))
    pixel_area_m2 = _safe_pixel_area_m2(src)
    supported_area_km2 = (
        supported_count * pixel_area_m2 / 1_000_000.0
        if np.isfinite(pixel_area_m2)
        else float("nan")
    )
    return GeometryBuiltUpStats(built_up_m2, supported_count, supported_area_km2)


def aggregate_built_up_area_to_admin_units(
    admin_gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
    raster_path: Path,
    area_epsg: int = AREA_EPSG,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate built-up area from one native raster onto admin polygons."""
    if admin_gdf.empty:
        raise ValueError(f"No {level} boundaries were provided.")
    missing = [col for col in _identity_cols(level) + ["geometry"] if col not in admin_gdf.columns]
    if missing:
        raise ValueError(f"{level.title()} boundaries are missing required columns: {missing}")

    with rasterio.open(raster_path) as src:
        if src.crs is None:
            raise ValueError(f"Built-up raster has no CRS: {raster_path}")
        admin_for_raster = admin_gdf.to_crs(src.crs).copy()
        stats = [_zonal_built_up_for_geometry(src, geom) for geom in admin_for_raster.geometry]

    area_df = admin_gdf.to_crs(epsg=area_epsg).copy()
    out = admin_gdf[_identity_cols(level)].copy()
    out["polygon_area_km2"] = pd.to_numeric(area_df.geometry.area / 1_000_000.0, errors="coerce")
    out[_area_col(level)] = out["polygon_area_km2"]
    out["built_up_area_m2"] = [item.built_up_m2 for item in stats]
    out[BUILT_UP_AREA_KM2_COL] = out["built_up_area_m2"] / 1_000_000.0
    out[BUILT_UP_AREA_SHARE_PCT_COL] = np.where(
        pd.to_numeric(out["polygon_area_km2"], errors="coerce").gt(0),
        out[BUILT_UP_AREA_KM2_COL] / out["polygon_area_km2"] * 100.0,
        np.nan,
    )
    out["raster_supported_cell_count"] = [item.raster_supported_cell_count for item in stats]
    out["raster_supported_area_km2"] = [item.raster_supported_area_km2 for item in stats]
    out["built_up_share_supported_pct"] = np.where(
        pd.to_numeric(out["raster_supported_area_km2"], errors="coerce").gt(0),
        out[BUILT_UP_AREA_KM2_COL] / out["raster_supported_area_km2"] * 100.0,
        np.nan,
    )
    out["support_area_pct_of_polygon"] = np.where(
        pd.to_numeric(out["polygon_area_km2"], errors="coerce").gt(0),
        out["raster_supported_area_km2"] / out["polygon_area_km2"] * 100.0,
        np.nan,
    )
    out["low_support_coverage"] = pd.to_numeric(out["support_area_pct_of_polygon"], errors="coerce").lt(95.0)

    master_df = out.rename(columns={"state_name": "state", "district_name": "district", "block_name": "block"})
    master_df = master_df.sort_values(_output_key_cols(level)).reset_index(drop=True)
    qa_df = master_df.copy()
    qa_df["source_raster"] = str(raster_path)
    return master_df, qa_df


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file without --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_master_table(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    parquet_path = path.with_suffix(".parquet")
    if not overwrite:
        existing = [str(p) for p in (path, parquet_path) if p.exists()]
        if existing:
            raise FileExistsError(f"Refusing to overwrite existing file without --overwrite: {', '.join(existing)}")
    _write_csv(df, path, overwrite=True)
    df.to_parquet(parquet_path, index=False)


def _hex_to_rgba(color_hex: str, alpha: int = 255) -> tuple[int, int, int, int]:
    value = color_hex.lstrip("#")
    return (int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16), int(alpha))


def _built_up_rgba(data: np.ndarray) -> np.ndarray:
    rgba = np.zeros((data.shape[0], data.shape[1], 4), dtype=np.uint8)
    finite = np.isfinite(data) & (~np.isclose(data, INVALID_VALUE))
    bins = (
        (finite & (data > 0.0) & (data <= 100.0), "#edf8fb"),
        (finite & (data > 100.0) & (data <= 500.0), "#b2e2e2"),
        (finite & (data > 500.0) & (data <= 1000.0), "#66c2a4"),
        (finite & (data > 1000.0) & (data <= 2500.0), "#2ca25f"),
        (finite & (data > 2500.0) & (data <= 5000.0), "#006d2c"),
        (finite & (data > 5000.0), "#00441b"),
    )
    for mask, color_hex in bins:
        rgba[mask] = _hex_to_rgba(color_hex)
    return rgba


def export_built_up_area_overlay(
    *,
    raster_path: Path,
    overlay_dir: Path,
    overwrite: bool,
    dry_run: bool,
) -> dict[str, object]:
    """Export the display-only built-up PNG/metadata overlay in EPSG:3857."""
    png_path, meta_path = built_up_area_overlay_paths(overlay_dir)
    if not dry_run and not overwrite:
        existing = [str(path) for path in (png_path, meta_path) if path.exists()]
        if existing:
            raise FileExistsError(
                f"Refusing to overwrite existing built-up overlay artifact without --overwrite: {', '.join(existing)}"
            )

    with rasterio.open(raster_path) as src:
        if src.crs is None:
            raise ValueError(f"Built-up raster has no CRS: {raster_path}")
        dst_transform, dst_width, dst_height = calculate_default_transform(
            src.crs,
            IMAGE_CRS,
            src.width,
            src.height,
            *src.bounds,
        )
        max_dim = max(int(dst_width), int(dst_height))
        if max_dim > OVERLAY_MAX_DIMENSION:
            scale = max_dim / float(OVERLAY_MAX_DIMENSION)
            target_width = max(1, int(round(dst_width / scale)))
            target_height = max(1, int(round(dst_height / scale)))
            dst_transform = dst_transform * Affine.scale(
                dst_width / float(target_width),
                dst_height / float(target_height),
            )
            dst_width, dst_height = target_width, target_height

        dst = np.full((int(dst_height), int(dst_width)), np.nan, dtype=np.float32)
        reproject(
            source=rasterio.band(src, 1),
            destination=dst,
            src_transform=src.transform,
            src_crs=src.crs,
            src_nodata=src.nodata,
            dst_transform=dst_transform,
            dst_crs=IMAGE_CRS,
            dst_nodata=np.nan,
            resampling=Resampling.average,
        )
        dst[np.isclose(dst, INVALID_VALUE)] = np.nan

        merc_west = float(dst_transform.c)
        merc_east = float(dst_transform.c + dst_transform.a * int(dst_width))
        merc_north = float(dst_transform.f)
        merc_south = float(dst_transform.f + dst_transform.e * int(dst_height))
        wgs84_left, wgs84_bottom, wgs84_right, wgs84_top = transform_bounds(
            IMAGE_CRS,
            "EPSG:4326",
            merc_west,
            merc_south,
            merc_east,
            merc_north,
        )
        valid_positive = np.isfinite(dst) & (dst > 0.0)
        source_positive_max = float(np.nanmax(dst[valid_positive])) if np.any(valid_positive) else 0.0
        metadata = {
            "overlay_id": BUILT_UP_AREA_OVERLAY_ID,
            "source_raster_name": DEFAULT_RASTER_NAME,
            "source_crs": src.crs.to_string(),
            "image_crs": IMAGE_CRS,
            "bounds_latlon": [
                [round(float(wgs84_bottom), 6), round(float(wgs84_left), 6)],
                [round(float(wgs84_top), 6), round(float(wgs84_right), 6)],
            ],
            "snapshot_period": SNAPSHOT_PERIOD,
            "display_units": "m2/source cell",
            "display_transform": "binned_m2_per_source_cell",
            "invalid_value": int(INVALID_VALUE),
            "bin_edges_m2_per_cell": [float(v) for v in BUILT_UP_BIN_EDGES_M2_PER_CELL],
            "bin_colors_hex": list(BUILT_UP_BIN_COLORS_HEX),
            "width_px": int(dst_width),
            "height_px": int(dst_height),
            "source_positive_max_built_up_m2_per_cell": source_positive_max,
            "clipped_above_display_max": bool(source_positive_max > BUILT_UP_BIN_EDGES_M2_PER_CELL[-1]),
        }

        if not dry_run:
            overlay_dir.mkdir(parents=True, exist_ok=True)
            Image.fromarray(_built_up_rgba(dst), mode="RGBA").save(png_path)
            meta_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"png_path": png_path, "meta_path": meta_path, "metadata": metadata}


def _metric_specific_master(master_df: pd.DataFrame, *, level: AdminLevel, metric_slug: str) -> pd.DataFrame:
    base_cols = _output_key_cols(level) + [_area_col(level), "polygon_area_km2"]
    if metric_slug == "built_up_area_km2":
        return master_df[base_cols + [BUILT_UP_AREA_KM2_COL]].copy()
    if metric_slug == "built_up_area_share_pct":
        return master_df[base_cols + [BUILT_UP_AREA_SHARE_PCT_COL]].copy()
    raise ValueError(f"Unsupported built-up metric slug: {metric_slug}")


def _write_state_slices(
    master_df: pd.DataFrame,
    *,
    metric_slug: str,
    level: AdminLevel,
    overwrite: bool,
) -> dict[str, int]:
    processed_root = resolve_processed_root(metric_slug, data_dir=get_paths_config().data_dir, mode="portfolio")
    out_name = get_master_csv_filename(level)
    counts: dict[str, int] = {}
    for state_name, state_df in master_df.groupby("state", dropna=False, as_index=False):
        state_label = str(state_name or "").strip()
        if not state_label:
            raise ValueError(f"Built-up {level} master contains an empty state value.")
        out_path = processed_root / state_label / out_name
        _write_master_table(state_df.reset_index(drop=True), out_path, overwrite=overwrite)
        counts[state_label] = int(state_df.shape[0])
    return counts


def _planned_metric_paths(master_df: pd.DataFrame, *, metric_slug: str, level: AdminLevel) -> list[Path]:
    processed_root = resolve_processed_root(metric_slug, data_dir=get_paths_config().data_dir, mode="portfolio")
    filename = get_master_csv_filename(level)
    paths: list[Path] = []
    for state_name in sorted(str(v).strip() for v in master_df["state"].dropna().unique()):
        if state_name:
            csv_path = processed_root / state_name / filename
            paths.extend([csv_path, csv_path.with_suffix(".parquet")])
    return paths


def _assert_can_write(paths: list[Path], *, overwrite: bool) -> None:
    if overwrite:
        return
    existing = [str(path) for path in paths if path.exists()]
    if existing:
        raise FileExistsError("Refusing to overwrite existing built-up outputs without --overwrite: " + ", ".join(existing))


def _national_summary_df(summary: RasterValidationSummary, district_master_df: pd.DataFrame, block_master_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    row = {
        "source_raster": str(summary.path),
        "source_crs": summary.crs,
        "dtype": summary.dtype,
        "declared_nodata": summary.declared_nodata,
        "invalid_value": int(INVALID_VALUE),
        "raster_min_valid_value": summary.min_value,
        "raster_max_valid_value": summary.max_value,
        "national_built_up_area_km2_raster": summary.national_built_up_area_km2,
        "district_built_up_area_km2_sum": float(pd.to_numeric(district_master_df[BUILT_UP_AREA_KM2_COL], errors="coerce").fillna(0).sum()),
        "block_built_up_area_km2_sum": np.nan,
        "centroid_inclusion_rule": "all_touched=False",
        "share_denominator": "polygon_area_epsg_6933",
    }
    if block_master_df is not None and not block_master_df.empty:
        row["block_built_up_area_km2_sum"] = float(pd.to_numeric(block_master_df[BUILT_UP_AREA_KM2_COL], errors="coerce").fillna(0).sum())
    return pd.DataFrame([row])


def build_built_up_area_admin_outputs(
    *,
    raster_path: Path,
    districts_path: Path,
    blocks_path: Optional[Path],
    qa_dir: Path,
    overlay_dir: Optional[Path],
    overwrite: bool,
    dry_run: bool,
    allow_total_outlier: bool,
) -> dict[str, object]:
    """Build built-up district/block masters, QA CSVs, and display overlay artifacts."""
    if not districts_path.exists():
        raise FileNotFoundError(f"District boundaries not found: {districts_path}")
    if blocks_path is not None and not blocks_path.exists():
        warnings.warn(f"Block boundaries not found; district outputs will still be built: {blocks_path}", RuntimeWarning)
        blocks_path = None

    raster_summary = validate_built_up_raster_contract(raster_path, allow_total_outlier=allow_total_outlier)
    district_gdf = load_district_boundaries(districts_path)
    district_master_df, district_qa_df = aggregate_built_up_area_to_admin_units(
        district_gdf,
        level="district",
        raster_path=raster_path,
    )

    block_master_df: Optional[pd.DataFrame] = None
    block_qa_df: Optional[pd.DataFrame] = None
    if blocks_path is not None:
        block_gdf = load_block_boundaries(blocks_path)
        block_master_df, block_qa_df = aggregate_built_up_area_to_admin_units(
            block_gdf,
            level="block",
            raster_path=raster_path,
        )

    resolved_overlay_dir = overlay_dir if overlay_dir is not None else _default_overlay_dir()
    overlay_output = export_built_up_area_overlay(
        raster_path=raster_path,
        overlay_dir=resolved_overlay_dir,
        overwrite=overwrite,
        dry_run=True if dry_run else False,
    )

    national_summary = _national_summary_df(raster_summary, district_master_df, block_master_df)
    planned_paths: list[Path] = []
    for slug in ("built_up_area_km2", "built_up_area_share_pct"):
        planned_paths.extend(_planned_metric_paths(district_master_df, metric_slug=slug, level="district"))
        if block_master_df is not None:
            planned_paths.extend(_planned_metric_paths(block_master_df, metric_slug=slug, level="block"))
    planned_paths.extend(
        [
            qa_dir / "built_up_area_district_master_qa.csv",
            qa_dir / "built_up_area_national_summary.csv",
            Path(overlay_output["png_path"]),
            Path(overlay_output["meta_path"]),
        ]
    )
    if block_qa_df is not None:
        planned_paths.append(qa_dir / "built_up_area_block_master_qa.csv")
    if not dry_run:
        _assert_can_write(planned_paths, overwrite=overwrite)

        for slug in ("built_up_area_km2", "built_up_area_share_pct"):
            _write_state_slices(
                _metric_specific_master(district_master_df, level="district", metric_slug=slug),
                metric_slug=slug,
                level="district",
                overwrite=overwrite,
            )
            if block_master_df is not None:
                _write_state_slices(
                    _metric_specific_master(block_master_df, level="block", metric_slug=slug),
                    metric_slug=slug,
                    level="block",
                    overwrite=overwrite,
                )
        _write_csv(district_qa_df, qa_dir / "built_up_area_district_master_qa.csv", overwrite=overwrite)
        if block_qa_df is not None:
            _write_csv(block_qa_df, qa_dir / "built_up_area_block_master_qa.csv", overwrite=overwrite)
        _write_csv(national_summary, qa_dir / "built_up_area_national_summary.csv", overwrite=overwrite)
        overlay_output = export_built_up_area_overlay(
            raster_path=raster_path,
            overlay_dir=resolved_overlay_dir,
            overwrite=True,
            dry_run=False,
        )

    return {
        "raster_summary": raster_summary,
        "district_master_df": district_master_df,
        "block_master_df": block_master_df,
        "district_qa_df": district_qa_df,
        "block_qa_df": block_qa_df,
        "national_summary_df": national_summary,
        "overlay": overlay_output,
        "planned_paths": tuple(planned_paths),
    }


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build built-up area exposure masters from the cleaned India built-surface raster.")
    parser.add_argument("--raster", default=str(_default_raster_path()), help="Path to Cleaned_India_Built_Surface_WGS84.tif.")
    parser.add_argument("--districts", default=str(get_paths_config().districts_path), help="Canonical district boundaries.")
    parser.add_argument("--blocks", default=str(get_paths_config().blocks_path), help="Canonical block boundaries. Missing blocks warn and skip block outputs.")
    parser.add_argument("--qa-dir", default=str(_default_qa_dir()), help="Directory for QA CSV outputs.")
    parser.add_argument("--overlay-dir", default=str(_default_overlay_dir()), help="Directory for PNG/metadata overlay artifacts.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print planned outputs without writing.")
    parser.add_argument("--allow-total-outlier", action="store_true", help="Allow national total outside the built-up guardrail range.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_cli().parse_args(argv)
    raster_path = Path(args.raster).expanduser().resolve()
    districts_path = Path(args.districts).expanduser().resolve()
    blocks_path = Path(args.blocks).expanduser().resolve() if args.blocks else None
    qa_dir = Path(args.qa_dir).expanduser().resolve()
    overlay_dir = Path(args.overlay_dir).expanduser().resolve() if args.overlay_dir else None

    if not raster_path.exists():
        raise FileNotFoundError(
            f"Built-up raster not found: {raster_path}. Place {DEFAULT_RASTER_NAME} under "
            f"{get_paths_config().data_dir / 'built_up_area'} or pass --raster."
        )

    outputs = build_built_up_area_admin_outputs(
        raster_path=raster_path,
        districts_path=districts_path,
        blocks_path=blocks_path,
        qa_dir=qa_dir,
        overlay_dir=overlay_dir,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        allow_total_outlier=bool(args.allow_total_outlier),
    )
    summary: RasterValidationSummary = outputs["raster_summary"]  # type: ignore[assignment]
    print("BUILT-UP AREA ADMIN MASTERS")
    print(f"raster: {raster_path}")
    print(f"source_crs: {summary.crs}")
    print(f"dtype: {summary.dtype}")
    print(f"declared_nodata: {summary.declared_nodata}")
    print(f"invalid_value: {int(INVALID_VALUE)}")
    print(f"raster_valid_min: {summary.min_value}")
    print(f"raster_valid_max: {summary.max_value}")
    print(f"national_built_up_area_km2: {summary.national_built_up_area_km2:.2f}")
    print(f"district_rows: {int(outputs['district_master_df'].shape[0])}")
    block_df = outputs.get("block_master_df")
    print(f"block_rows: {0 if block_df is None else int(block_df.shape[0])}")
    if bool(args.dry_run):
        print("dry_run: True")
        print("planned_outputs:")
        for path in outputs["planned_paths"]:
            print(f"  {path}")
    else:
        overlay = outputs["overlay"]
        print(f"qa_dir: {qa_dir}")
        print(f"built_up_overlay_png: {overlay['png_path']}")
        print(f"built_up_overlay_meta: {overlay['meta_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

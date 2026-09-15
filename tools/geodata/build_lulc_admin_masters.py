#!/usr/bin/env python3
"""Build district/block agricultural LULC exposure masters and overlay artifacts.

The source raster is interpreted as a binary agricultural mask: value ``1`` is
agricultural LULC and value ``0`` is nodata/background. Metrics are tabulated in
EPSG:6933 through a nearest-neighbor WarpedVRT, using centroid inclusion
(``all_touched=False``). Share metrics use full canonical polygon area in
EPSG:6933 as the denominator.
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
from rasterio.enums import Resampling
from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window
from rasterio.vrt import WarpedVRT
from rasterio.warp import calculate_default_transform, reproject, transform_bounds
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
IMAGE_CRS = "EPSG:3857"
DEFAULT_RASTER_NAME = "LULC_2_Agri.tif"
LULC_AGRI_VALID_VALUE = 1
LULC_AGRI_NODATA = 0
NATIONAL_TOTAL_KM2_MIN = 1_200_000.0
NATIONAL_TOTAL_KM2_MAX = 2_300_000.0
SHARE_OUTLIER_MAX_PCT = 100.01
OVERLAY_MAX_DIMENSION = 4096
LULC_AGRI_OVERLAY_ID = "lulc_agri_current_raster"

LULC_AGRI_AREA_KM2_COL = "lulc_agri_area_km2__snapshot__Current__mean"
LULC_AGRI_SHARE_PCT_COL = "lulc_agri_share_pct__snapshot__Current__mean"

LULC_AGRI_COLOR_HEX = "#2ca25f"


@dataclass(frozen=True)
class RasterValidationSummary:
    """Raster contract fields checked before outputs are written."""

    path: Path
    crs: str
    transform: str
    dtype: str
    declared_nodata: Optional[float]
    bounds: tuple[float, float, float, float]
    value_counts: dict[int, int]
    unexpected_value_counts: dict[str, int]
    unexpected_value_count: int
    national_agri_area_km2: float
    agri_cell_count_equal_area: int
    equal_area_cell_area_m2: float


@dataclass(frozen=True)
class GeometryLulcStats:
    """One polygon's agricultural LULC summary using centroid inclusion."""

    agri_cell_count: int
    support_cell_count: int
    cell_area_m2: float

    @property
    def agri_area_km2(self) -> float:
        return self.agri_cell_count * self.cell_area_m2 / 1_000_000.0

    @property
    def support_area_km2(self) -> float:
        return self.support_cell_count * self.cell_area_m2 / 1_000_000.0


def _default_raster_path() -> Path:
    return get_paths_config().data_dir / "lulc" / DEFAULT_RASTER_NAME


def _default_qa_dir() -> Path:
    return get_paths_config().data_dir / "lulc"


def _default_overlay_dir() -> Path:
    return get_paths_config().data_dir / "lulc" / "overlay"


def lulc_agri_overlay_paths(overlay_dir: Path) -> tuple[Path, Path]:
    """Return canonical LULC agricultural overlay PNG and metadata paths."""
    return (
        overlay_dir / "lulc_agri_current_overlay.png",
        overlay_dir / "lulc_agri_current_overlay_meta.json",
    )


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


def _scan_source_values(raster_path: Path) -> tuple[dict[int, int], dict[str, int], int]:
    value_counts: dict[int, int] = {LULC_AGRI_NODATA: 0, LULC_AGRI_VALID_VALUE: 0}
    unexpected: dict[str, int] = {}
    unexpected_total = 0
    with rasterio.open(raster_path) as src:
        for _block_index, window in src.block_windows(1):
            data = src.read(1, window=window, masked=False)
            vals, counts = np.unique(data, return_counts=True)
            for raw_value, raw_count in zip(vals, counts):
                count = int(raw_count)
                try:
                    value = float(raw_value)
                except (TypeError, ValueError):
                    key = str(raw_value)
                    unexpected[key] = unexpected.get(key, 0) + count
                    unexpected_total += count
                    continue
                if np.isfinite(value) and float(value).is_integer() and int(value) in {0, 1}:
                    value_counts[int(value)] = value_counts.get(int(value), 0) + count
                else:
                    key = str(raw_value)
                    unexpected[key] = unexpected.get(key, 0) + count
                    unexpected_total += count
    return value_counts, unexpected, unexpected_total


def _equal_area_vrt(src: rasterio.io.DatasetReader) -> WarpedVRT:
    return WarpedVRT(
        src,
        crs=f"EPSG:{AREA_EPSG}",
        resampling=Resampling.nearest,
        src_nodata=LULC_AGRI_NODATA,
        nodata=LULC_AGRI_NODATA,
    )


def _cell_area_m2(src: rasterio.io.DatasetReader | WarpedVRT) -> float:
    cell_area = float(abs(src.transform.a * src.transform.e))
    if not np.isfinite(cell_area) or cell_area <= 0:
        raise ValueError("Equal-area raster cell area must be positive and finite.")
    return cell_area


def _scan_equal_area_agri_total(raster_path: Path) -> tuple[int, float, float]:
    agri_count = 0
    with rasterio.open(raster_path) as base:
        with _equal_area_vrt(base) as src:
            cell_area = _cell_area_m2(src)
            for _block_index, window in src.block_windows(1):
                data = src.read(1, window=window, masked=False)
                agri_count += int(np.count_nonzero(np.asarray(data) == LULC_AGRI_VALID_VALUE))
    total_km2 = agri_count * cell_area / 1_000_000.0
    return agri_count, cell_area, total_km2


def _scan_raster_summary(raster_path: Path) -> RasterValidationSummary:
    with rasterio.open(raster_path) as src:
        if src.count != 1:
            raise ValueError(f"LULC agriculture raster must be single-band: {raster_path}")
        if src.crs is None:
            raise ValueError(f"LULC agriculture raster has no CRS: {raster_path}")
        dtype = np.dtype(src.dtypes[0])
        if not np.issubdtype(dtype, np.number):
            raise ValueError(f"LULC agriculture raster must be numeric: {raster_path}")
        value_counts, unexpected, unexpected_total = _scan_source_values(raster_path)
        agri_count, cell_area, total_km2 = _scan_equal_area_agri_total(raster_path)
        return RasterValidationSummary(
            path=raster_path,
            crs=src.crs.to_string(),
            transform=str(src.transform),
            dtype=str(dtype),
            declared_nodata=None if src.nodata is None else float(src.nodata),
            bounds=tuple(float(v) for v in src.bounds),
            value_counts=value_counts,
            unexpected_value_counts=unexpected,
            unexpected_value_count=unexpected_total,
            national_agri_area_km2=total_km2,
            agri_cell_count_equal_area=agri_count,
            equal_area_cell_area_m2=cell_area,
        )


def validate_lulc_agri_raster_contract(
    raster_path: Path,
    *,
    allow_total_outlier: bool = False,
    allow_unexpected_values: bool = False,
) -> RasterValidationSummary:
    """Validate raster metadata, binary values, and national total guardrails."""
    summary = _scan_raster_summary(raster_path)
    if summary.unexpected_value_count and not allow_unexpected_values:
        raise ValueError(
            "LULC agriculture raster contains values outside {0, 1}: "
            f"{summary.unexpected_value_counts}. Recheck the source or rerun with "
            "--allow-unexpected-values."
        )
    total = summary.national_agri_area_km2
    if not allow_total_outlier and not (NATIONAL_TOTAL_KM2_MIN <= total <= NATIONAL_TOTAL_KM2_MAX):
        raise ValueError(
            "LULC agriculture national total is outside the guardrail range "
            f"{NATIONAL_TOTAL_KM2_MIN:,.0f}-{NATIONAL_TOTAL_KM2_MAX:,.0f} km2: "
            f"{total:,.2f} km2. Recheck source units or rerun with --allow-total-outlier."
        )
    return summary


def _zonal_lulc_for_geometry(src: WarpedVRT, geom) -> GeometryLulcStats:
    if geom is None or geom.is_empty:
        return GeometryLulcStats(0, 0, _cell_area_m2(src))
    try:
        window = geometry_window(src, [mapping(geom)])
    except WindowError:
        return GeometryLulcStats(0, 0, _cell_area_m2(src))

    data = src.read(1, window=window, masked=False)
    if data.size == 0:
        return GeometryLulcStats(0, 0, _cell_area_m2(src))
    geom_mask = geometry_mask(
        [mapping(geom)],
        out_shape=data.shape,
        transform=src.window_transform(window),
        invert=True,
        all_touched=False,
    )
    agri = geom_mask & (np.asarray(data) == LULC_AGRI_VALID_VALUE)
    return GeometryLulcStats(
        agri_cell_count=int(np.count_nonzero(agri)),
        support_cell_count=int(np.count_nonzero(geom_mask)),
        cell_area_m2=_cell_area_m2(src),
    )


def aggregate_lulc_agri_to_admin_units(
    admin_gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
    raster_path: Path,
    area_epsg: int = AREA_EPSG,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate agricultural LULC area from one binary raster onto admin polygons."""
    if admin_gdf.empty:
        raise ValueError(f"No {level} boundaries were provided.")
    missing = [col for col in _identity_cols(level) + ["geometry"] if col not in admin_gdf.columns]
    if missing:
        raise ValueError(f"{level.title()} boundaries are missing required columns: {missing}")

    with rasterio.open(raster_path) as base:
        if base.crs is None:
            raise ValueError(f"LULC agriculture raster has no CRS: {raster_path}")
        with _equal_area_vrt(base) as src:
            admin_for_raster = admin_gdf.to_crs(src.crs).copy()
            stats = [_zonal_lulc_for_geometry(src, geom) for geom in admin_for_raster.geometry]

    area_df = admin_gdf.to_crs(epsg=area_epsg).copy()
    out = admin_gdf[_identity_cols(level)].copy()
    out["polygon_area_km2"] = pd.to_numeric(area_df.geometry.area / 1_000_000.0, errors="coerce")
    out[_area_col(level)] = out["polygon_area_km2"]
    out["agri_cell_count"] = [item.agri_cell_count for item in stats]
    out["agri_area_km2"] = [item.agri_area_km2 for item in stats]
    out[LULC_AGRI_AREA_KM2_COL] = out["agri_area_km2"]
    out[LULC_AGRI_SHARE_PCT_COL] = np.where(
        pd.to_numeric(out["polygon_area_km2"], errors="coerce").gt(0),
        out[LULC_AGRI_AREA_KM2_COL] / out["polygon_area_km2"] * 100.0,
        np.nan,
    )
    out["lulc_agri_share_pct"] = out[LULC_AGRI_SHARE_PCT_COL]
    out["raster_extent_support_cell_count"] = [item.support_cell_count for item in stats]
    out["raster_extent_support_area_km2"] = [item.support_area_km2 for item in stats]
    out["support_area_pct_of_polygon"] = np.where(
        pd.to_numeric(out["polygon_area_km2"], errors="coerce").gt(0),
        out["raster_extent_support_area_km2"] / out["polygon_area_km2"] * 100.0,
        np.nan,
    )
    out["low_support_coverage"] = pd.to_numeric(out["support_area_pct_of_polygon"], errors="coerce").lt(95.0)
    out["share_out_of_range"] = pd.to_numeric(out[LULC_AGRI_SHARE_PCT_COL], errors="coerce").gt(SHARE_OUTLIER_MAX_PCT)

    master_df = out.rename(columns={"state_name": "state", "district_name": "district", "block_name": "block"})
    master_df = master_df.sort_values(_output_key_cols(level)).reset_index(drop=True)
    qa_df = master_df.copy()
    qa_df["source_raster"] = str(raster_path)
    qa_df["centroid_inclusion_rule"] = "all_touched=False"
    qa_df["share_denominator"] = "polygon_area_epsg_6933"
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


def _lulc_rgba(data: np.ndarray) -> np.ndarray:
    rgba = np.zeros((data.shape[0], data.shape[1], 4), dtype=np.uint8)
    rgba[np.asarray(data) == LULC_AGRI_VALID_VALUE] = _hex_to_rgba(LULC_AGRI_COLOR_HEX)
    return rgba


def export_lulc_agri_overlay(
    *,
    raster_path: Path,
    overlay_dir: Path,
    overwrite: bool,
    dry_run: bool,
) -> dict[str, object]:
    """Export the display-only agricultural LULC PNG/metadata overlay in EPSG:3857."""
    png_path, meta_path = lulc_agri_overlay_paths(overlay_dir)
    if not dry_run and not overwrite:
        existing = [str(path) for path in (png_path, meta_path) if path.exists()]
        if existing:
            raise FileExistsError(
                f"Refusing to overwrite existing LULC overlay artifact without --overwrite: {', '.join(existing)}"
            )

    with rasterio.open(raster_path) as src:
        if src.crs is None:
            raise ValueError(f"LULC agriculture raster has no CRS: {raster_path}")
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

        dst = np.zeros((int(dst_height), int(dst_width)), dtype=np.uint8)
        reproject(
            source=rasterio.band(src, 1),
            destination=dst,
            src_transform=src.transform,
            src_crs=src.crs,
            src_nodata=LULC_AGRI_NODATA,
            dst_transform=dst_transform,
            dst_crs=IMAGE_CRS,
            dst_nodata=LULC_AGRI_NODATA,
            resampling=Resampling.nearest,
        )
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
        metadata = {
            "overlay_id": LULC_AGRI_OVERLAY_ID,
            "source_raster_name": DEFAULT_RASTER_NAME,
            "source_crs": src.crs.to_string(),
            "image_crs": IMAGE_CRS,
            "bounds_latlon": [
                [round(float(wgs84_bottom), 6), round(float(wgs84_left), 6)],
                [round(float(wgs84_top), 6), round(float(wgs84_right), 6)],
            ],
            "snapshot_period": SNAPSHOT_PERIOD,
            "display_units": "agricultural LULC binary class",
            "display_transform": "nearest_binary_class",
            "valid_value": LULC_AGRI_VALID_VALUE,
            "nodata_value": LULC_AGRI_NODATA,
            "valid_color_hex": LULC_AGRI_COLOR_HEX,
            "width_px": int(dst_width),
            "height_px": int(dst_height),
            "agri_pixel_count": int(np.count_nonzero(dst == LULC_AGRI_VALID_VALUE)),
        }

        if not dry_run:
            overlay_dir.mkdir(parents=True, exist_ok=True)
            Image.fromarray(_lulc_rgba(dst), mode="RGBA").save(png_path)
            meta_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"png_path": png_path, "meta_path": meta_path, "metadata": metadata}


def _metric_specific_master(master_df: pd.DataFrame, *, level: AdminLevel, metric_slug: str) -> pd.DataFrame:
    base_cols = _output_key_cols(level) + [_area_col(level), "polygon_area_km2"]
    if metric_slug == "lulc_agri_area_km2":
        return master_df[base_cols + [LULC_AGRI_AREA_KM2_COL]].copy()
    if metric_slug == "lulc_agri_share_pct":
        return master_df[base_cols + [LULC_AGRI_SHARE_PCT_COL]].copy()
    raise ValueError(f"Unsupported LULC agriculture metric slug: {metric_slug}")


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
            raise ValueError(f"LULC agriculture {level} master contains an empty state value.")
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
        raise FileExistsError("Refusing to overwrite existing LULC agriculture outputs without --overwrite: " + ", ".join(existing))


def _assert_share_guardrail(*, district_master_df: pd.DataFrame, block_master_df: Optional[pd.DataFrame], allow_share_outlier: bool) -> None:
    if allow_share_outlier:
        return
    offenders: list[dict[str, object]] = []
    for level, df in (("district", district_master_df), ("block", block_master_df)):
        if df is None or df.empty:
            continue
        share = pd.to_numeric(df[LULC_AGRI_SHARE_PCT_COL], errors="coerce")
        bad = df.loc[share.gt(SHARE_OUTLIER_MAX_PCT)].copy()
        if not bad.empty:
            for _, row in bad.head(5).iterrows():
                offenders.append(
                    {
                        "level": level,
                        "state": row.get("state"),
                        "district": row.get("district"),
                        "block": row.get("block", ""),
                        "share_pct": row.get(LULC_AGRI_SHARE_PCT_COL),
                    }
                )
    if offenders:
        raise ValueError(
            "LULC agriculture share exceeds 100.01% for district/block rows. "
            f"Examples: {offenders}. Recheck raster/polygon alignment or rerun with --allow-share-outlier."
        )


def _national_summary_df(summary: RasterValidationSummary, district_master_df: pd.DataFrame, block_master_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    row = {
        "source_raster": str(summary.path),
        "source_crs": summary.crs,
        "dtype": summary.dtype,
        "declared_nodata": summary.declared_nodata,
        "nodata_value": LULC_AGRI_NODATA,
        "valid_value": LULC_AGRI_VALID_VALUE,
        "value_counts_json": json.dumps(summary.value_counts, sort_keys=True),
        "unexpected_value_counts_json": json.dumps(summary.unexpected_value_counts, sort_keys=True),
        "unexpected_value_count": summary.unexpected_value_count,
        "national_agri_area_km2_raster_equal_area": summary.national_agri_area_km2,
        "national_total_guardrail_min_km2": NATIONAL_TOTAL_KM2_MIN,
        "national_total_guardrail_max_km2": NATIONAL_TOTAL_KM2_MAX,
        "national_total_within_guardrail": NATIONAL_TOTAL_KM2_MIN <= summary.national_agri_area_km2 <= NATIONAL_TOTAL_KM2_MAX,
        "equal_area_epsg": AREA_EPSG,
        "equal_area_cell_area_m2": summary.equal_area_cell_area_m2,
        "district_agri_area_km2_sum": float(pd.to_numeric(district_master_df[LULC_AGRI_AREA_KM2_COL], errors="coerce").fillna(0).sum()),
        "block_agri_area_km2_sum": np.nan,
        "centroid_inclusion_rule": "all_touched=False",
        "share_denominator": "polygon_area_epsg_6933",
    }
    if block_master_df is not None and not block_master_df.empty:
        row["block_agri_area_km2_sum"] = float(pd.to_numeric(block_master_df[LULC_AGRI_AREA_KM2_COL], errors="coerce").fillna(0).sum())
    return pd.DataFrame([row])


def build_lulc_agri_admin_outputs(
    *,
    raster_path: Path,
    districts_path: Path,
    blocks_path: Optional[Path],
    qa_dir: Path,
    overlay_dir: Optional[Path],
    overwrite: bool,
    dry_run: bool,
    allow_total_outlier: bool,
    allow_unexpected_values: bool,
    allow_share_outlier: bool,
) -> dict[str, object]:
    """Build LULC agriculture district/block masters, QA CSVs, and overlay artifacts."""
    if not districts_path.exists():
        raise FileNotFoundError(f"District boundaries not found: {districts_path}")
    if blocks_path is not None and not blocks_path.exists():
        warnings.warn(f"Block boundaries not found; district outputs will still be built: {blocks_path}", RuntimeWarning)
        blocks_path = None

    raster_summary = validate_lulc_agri_raster_contract(
        raster_path,
        allow_total_outlier=allow_total_outlier,
        allow_unexpected_values=allow_unexpected_values,
    )
    district_gdf = load_district_boundaries(districts_path)
    district_master_df, district_qa_df = aggregate_lulc_agri_to_admin_units(
        district_gdf,
        level="district",
        raster_path=raster_path,
    )

    block_master_df: Optional[pd.DataFrame] = None
    block_qa_df: Optional[pd.DataFrame] = None
    if blocks_path is not None:
        block_gdf = load_block_boundaries(blocks_path)
        block_master_df, block_qa_df = aggregate_lulc_agri_to_admin_units(
            block_gdf,
            level="block",
            raster_path=raster_path,
        )

    _assert_share_guardrail(
        district_master_df=district_master_df,
        block_master_df=block_master_df,
        allow_share_outlier=allow_share_outlier,
    )

    resolved_overlay_dir = overlay_dir if overlay_dir is not None else _default_overlay_dir()
    overlay_output = export_lulc_agri_overlay(
        raster_path=raster_path,
        overlay_dir=resolved_overlay_dir,
        overwrite=overwrite,
        dry_run=True if dry_run else False,
    )

    national_summary = _national_summary_df(raster_summary, district_master_df, block_master_df)
    planned_paths: list[Path] = []
    for slug in ("lulc_agri_area_km2", "lulc_agri_share_pct"):
        planned_paths.extend(_planned_metric_paths(district_master_df, metric_slug=slug, level="district"))
        if block_master_df is not None:
            planned_paths.extend(_planned_metric_paths(block_master_df, metric_slug=slug, level="block"))
    planned_paths.extend(
        [
            qa_dir / "lulc_agri_district_master_qa.csv",
            qa_dir / "lulc_agri_national_summary.csv",
            Path(overlay_output["png_path"]),
            Path(overlay_output["meta_path"]),
        ]
    )
    if block_qa_df is not None:
        planned_paths.append(qa_dir / "lulc_agri_block_master_qa.csv")

    if not dry_run:
        _assert_can_write(planned_paths, overwrite=overwrite)
        for slug in ("lulc_agri_area_km2", "lulc_agri_share_pct"):
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
        _write_csv(district_qa_df, qa_dir / "lulc_agri_district_master_qa.csv", overwrite=overwrite)
        if block_qa_df is not None:
            _write_csv(block_qa_df, qa_dir / "lulc_agri_block_master_qa.csv", overwrite=overwrite)
        _write_csv(national_summary, qa_dir / "lulc_agri_national_summary.csv", overwrite=overwrite)
        overlay_output = export_lulc_agri_overlay(
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
    parser = argparse.ArgumentParser(description="Build agricultural LULC exposure masters from the binary India LULC raster.")
    parser.add_argument("--raster", default=str(_default_raster_path()), help="Path to LULC_2_Agri.tif.")
    parser.add_argument("--districts", default=str(get_paths_config().districts_path), help="Canonical district boundaries.")
    parser.add_argument("--blocks", default=str(get_paths_config().blocks_path), help="Canonical block boundaries. Missing blocks warn and skip block outputs.")
    parser.add_argument("--qa-dir", default=str(_default_qa_dir()), help="Directory for QA CSV outputs.")
    parser.add_argument("--overlay-dir", default=str(_default_overlay_dir()), help="Directory for PNG/metadata overlay artifacts.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print planned outputs without writing.")
    parser.add_argument("--allow-total-outlier", action="store_true", help="Allow national total outside the agricultural LULC guardrail range.")
    parser.add_argument("--allow-unexpected-values", action="store_true", help="Allow source values outside {0, 1}; such values are ignored unless equal to 1.")
    parser.add_argument("--allow-share-outlier", action="store_true", help="Allow district/block agricultural shares above 100.01%%.")
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
            f"LULC agriculture raster not found: {raster_path}. Place {DEFAULT_RASTER_NAME} under "
            f"{get_paths_config().data_dir / 'lulc'} or pass --raster."
        )

    outputs = build_lulc_agri_admin_outputs(
        raster_path=raster_path,
        districts_path=districts_path,
        blocks_path=blocks_path,
        qa_dir=qa_dir,
        overlay_dir=overlay_dir,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        allow_total_outlier=bool(args.allow_total_outlier),
        allow_unexpected_values=bool(args.allow_unexpected_values),
        allow_share_outlier=bool(args.allow_share_outlier),
    )
    summary: RasterValidationSummary = outputs["raster_summary"]  # type: ignore[assignment]
    print("LULC AGRICULTURE ADMIN MASTERS")
    print(f"raster: {raster_path}")
    print(f"source_crs: {summary.crs}")
    print(f"dtype: {summary.dtype}")
    print(f"declared_nodata: {summary.declared_nodata}")
    print(f"valid_value: {LULC_AGRI_VALID_VALUE}")
    print(f"nodata_value: {LULC_AGRI_NODATA}")
    print(f"value_counts: {summary.value_counts}")
    print(f"unexpected_value_count: {summary.unexpected_value_count}")
    print(f"national_agri_area_km2: {summary.national_agri_area_km2:.2f}")
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
        print(f"lulc_overlay_png: {overlay['png_path']}")
        print(f"lulc_overlay_meta: {overlay['meta_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

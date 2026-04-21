#!/usr/bin/env python3
"""
Build Telangana district and block JRC flood-depth masters.

This tool aggregates four fixed return-period flood-depth rasters from JRC onto
canonical Telangana admin polygons. Block values use the p95 flooded-cell depth
within each polygon, while district values use a flooded-area-weighted mean of
child block p95 depths with QA outputs. The RP-100 pass also emits derived
flood-severity-index and flood-extent products with dedicated QA.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window
from shapely.geometry import mapping

from india_resilience_tool.utils.naming import normalize_compact, normalize_name
from paths import get_master_csv_filename, get_paths_config, resolve_processed_root
from tools.geodata.build_district_subbasin_crosswalk import (
    load_block_boundaries,
    load_district_boundaries,
)


AREA_EPSG = 6933
ASSUME_UNITS = "m"
TARGET_STATE = "Telangana"
DERIVED_INDEX_METRIC_SLUG = "jrc_flood_depth_index_rp100"
DERIVED_INDEX_SOURCE_METRIC_SLUG = "jrc_flood_depth_rp100"
DERIVED_EXTENT_METRIC_SLUG = "jrc_flood_extent_rp100"
DERIVED_EXTENT_SOURCE_METRIC_SLUG = "jrc_flood_depth_rp100"
DERIVED_INDEX_LABELS: dict[int, str] = {
    1: "VeryLow/No",
    2: "Low",
    3: "Moderate",
    4: "High",
    5: "Extreme",
}
DERIVED_INDEX_MATRIX: tuple[tuple[int, ...], ...] = (
    (1, 2, 2, 3, 4),
    (2, 2, 3, 4, 4),
    (2, 3, 4, 4, 5),
    (3, 4, 4, 5, 5),
    (4, 5, 5, 5, 5),
)
JRC_FILE_MAP: dict[str, str] = {
    "jrc_flood_depth_rp10": "RP10_depth.tif",
    "jrc_flood_depth_rp50": "RP50_depth.tif",
    "jrc_flood_depth_rp100": "RP100_depth.tif",
    "jrc_flood_depth_rp500": "RP500_depth.tif",
}
DERIVED_METRIC_SLUGS: tuple[str, ...] = (
    DERIVED_INDEX_METRIC_SLUG,
    DERIVED_EXTENT_METRIC_SLUG,
)
ALL_OUTPUT_METRIC_SLUGS: tuple[str, ...] = tuple(JRC_FILE_MAP) + DERIVED_METRIC_SLUGS


@dataclass(frozen=True)
class RasterContract:
    """Resolved and validated raster inputs for one builder run."""

    source_dir: Path
    raster_paths: dict[str, Path]
    raster_crs: str
    raster_shape: str
    nodata_value: str


@dataclass(frozen=True)
class GeometryCoverageStats:
    """One polygon's raster extent/depth summary using the shared clipping rule."""

    total_in_polygon_cell_count: int
    valid_in_polygon_cell_count: int
    coverage_fraction: float
    zero_valid_cell_count: int
    positive_valid_cell_count: int
    max_valid_depth_m: float
    mean_valid_depth_m: float
    p95_positive_depth_m: float


def _derived_metric_column(metric_slug: str) -> str:
    """Return the canonical static-snapshot master column for one metric slug."""
    return f"{metric_slug}__snapshot__Current__mean"


def _is_zero_nodata(value: object) -> bool:
    try:
        return bool(np.isclose(float(value), 0.0))
    except (TypeError, ValueError):
        return False


def _safe_fraction(numerator: object, denominator: object) -> float:
    """Return a finite fraction or NaN when the denominator is not positive."""
    try:
        num = float(numerator)
        den = float(denominator)
    except (TypeError, ValueError):
        return float("nan")
    if not np.isfinite(num) or not np.isfinite(den) or den <= 0.0:
        return float("nan")
    return float(num / den)


def _quantile_linear(values: np.ndarray, q: float) -> float:
    """Return one quantile using explicit linear interpolation across NumPy versions."""
    if values.size == 0:
        return float("nan")
    try:
        return float(np.quantile(values, q, method="linear"))
    except TypeError:
        return float(np.quantile(values, q, interpolation="linear"))


@dataclass(frozen=True)
class AdminJoinValidation:
    """Preflight district/block alignment diagnostics for Telangana boundaries."""

    qa_df: pd.DataFrame
    missing_in_blocks: int
    missing_in_districts: int
    duplicate_within_source: int


def _default_qa_dir() -> Path:
    return get_paths_config().data_dir / "jrc_flood_depth" / "qa"


def _district_join_key(state_name: object, district_name: object) -> str:
    state_key = normalize_name(str(state_name or ""))
    district_key = normalize_compact(str(district_name or ""))
    return f"{state_key}::{district_key}"


def _normalize_units_token(value: object) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    compact = raw.replace("_", "").replace("-", "").replace(" ", "")
    aliases = {
        "m": "m",
        "meter": "m",
        "meters": "m",
        "metre": "m",
        "metres": "m",
    }
    return aliases.get(compact, compact)


def _attested_raster_units(src: rasterio.io.DatasetReader) -> str:
    candidates = []
    tags = src.tags()
    band_tags = src.tags(1)
    for key in ("unit", "units", "UnitType", "UNITTYPE", "units_name"):
        candidates.extend([tags.get(key, ""), band_tags.get(key, "")])
    for candidate in candidates:
        normalized = _normalize_units_token(candidate)
        if normalized:
            return normalized
    return ""


def _validate_raster_contract(source_dir: Path, *, assume_units: str) -> RasterContract:
    if not source_dir.exists() or not source_dir.is_dir():
        raise FileNotFoundError(f"JRC source directory not found: {source_dir}")
    if str(assume_units).strip().lower() != ASSUME_UNITS:
        raise ValueError(f"Only --assume-units {ASSUME_UNITS!r} is supported for JRC flood depth.")

    raster_paths: dict[str, Path] = {}
    reference: tuple[object, tuple[int, int], object, object, tuple[str, ...]] | None = None
    raster_crs = ""
    raster_shape = ""
    nodata_value = ""

    for slug, filename in JRC_FILE_MAP.items():
        matches = sorted(p.resolve() for p in source_dir.glob(filename))
        if len(matches) != 1:
            raise FileNotFoundError(
                f"Expected exactly one {filename} for {slug} under {source_dir}, found {len(matches)}."
            )
        raster_path = matches[0]
        raster_paths[slug] = raster_path
        with rasterio.open(raster_path) as src:
            if src.count != 1:
                raise ValueError(f"JRC raster must be single-band: {raster_path}")
            if src.crs is None:
                raise ValueError(f"JRC raster has no CRS: {raster_path}")
            dtype = np.dtype(src.dtypes[0])
            if not np.issubdtype(dtype, np.number):
                raise ValueError(f"JRC raster must be numeric: {raster_path}")
            attested_units = _attested_raster_units(src)
            if attested_units and attested_units != ASSUME_UNITS:
                raise ValueError(
                    f"JRC raster units {attested_units!r} do not match required {ASSUME_UNITS!r}: {raster_path}"
                )

            mask_flags = tuple(str(flag) for flag in src.mask_flag_enums[0])
            current = (src.crs.to_string(), src.shape, src.transform, src.nodata, mask_flags)
            if reference is None:
                reference = current
                raster_crs = src.crs.to_string()
                raster_shape = f"{src.height}x{src.width}"
                nodata_value = "" if src.nodata is None else str(src.nodata)
            elif current != reference:
                raise ValueError(
                    "JRC rasters must share CRS, shape, transform, nodata, and mask semantics across all return periods."
                )

    return RasterContract(
        source_dir=source_dir,
        raster_paths=raster_paths,
        raster_crs=raster_crs,
        raster_shape=raster_shape,
        nodata_value=nodata_value,
    )


def _load_telangana_admin(
    *,
    districts_path: Path,
    blocks_path: Path,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    districts = load_district_boundaries(districts_path)
    blocks = load_block_boundaries(blocks_path)
    districts = districts.loc[
        districts["state_name"].astype(str).str.strip().str.casefold() == TARGET_STATE.casefold()
    ].copy()
    blocks = blocks.loc[
        blocks["state_name"].astype(str).str.strip().str.casefold() == TARGET_STATE.casefold()
    ].copy()
    if districts.empty:
        raise ValueError(f"No Telangana district rows found in {districts_path}")
    if blocks.empty:
        raise ValueError(f"No Telangana block rows found in {blocks_path}")
    return districts.reset_index(drop=True), blocks.reset_index(drop=True)


def _collect_admin_join_validation(
    *,
    district_gdf: gpd.GeoDataFrame,
    block_gdf: gpd.GeoDataFrame,
) -> AdminJoinValidation:
    district_unique = (
        district_gdf[["state_name", "district_name"]]
        .drop_duplicates()
        .assign(source_type="district")
        .reset_index(drop=True)
    )
    block_unique = (
        block_gdf[["state_name", "district_name"]]
        .drop_duplicates()
        .assign(source_type="block")
        .reset_index(drop=True)
    )

    district_unique["district_join_key"] = [
        _district_join_key(state_name, district_name)
        for state_name, district_name in zip(district_unique["state_name"], district_unique["district_name"])
    ]
    block_unique["district_join_key"] = [
        _district_join_key(state_name, district_name)
        for state_name, district_name in zip(block_unique["state_name"], block_unique["district_name"])
    ]

    district_name_map = district_unique.groupby("district_join_key")["district_name"].agg(
        lambda values: sorted({str(value).strip() for value in values})
    )
    block_name_map = block_unique.groupby("district_join_key")["district_name"].agg(
        lambda values: sorted({str(value).strip() for value in values})
    )

    rows: list[dict[str, object]] = []
    missing_in_blocks = 0
    missing_in_districts = 0
    duplicate_within_source = 0

    for source_df, source_map, other_map, missing_status in (
        (district_unique, district_name_map, block_name_map, "missing_in_blocks"),
        (block_unique, block_name_map, district_name_map, "missing_in_districts"),
    ):
        for row in source_df.itertuples(index=False):
            source_names = source_map[row.district_join_key]
            matches = other_map.get(row.district_join_key, [])
            if len(source_names) > 1:
                status = "duplicate_within_source"
                duplicate_within_source += 1
            elif not matches:
                status = missing_status
                if missing_status == "missing_in_blocks":
                    missing_in_blocks += 1
                else:
                    missing_in_districts += 1
            else:
                status = "matched"
            rows.append(
                {
                    "source_type": row.source_type,
                    "state_name_raw": row.state_name,
                    "district_name_raw": row.district_name,
                    "district_join_key": row.district_join_key,
                    "status": status,
                    "matching_raw_district_name": "; ".join(matches),
                }
            )

    qa_df = (
        pd.DataFrame(rows)
        .sort_values(["source_type", "district_join_key", "district_name_raw"])
        .reset_index(drop=True)
    )
    return AdminJoinValidation(
        qa_df=qa_df,
        missing_in_blocks=missing_in_blocks,
        missing_in_districts=missing_in_districts,
        duplicate_within_source=duplicate_within_source,
    )


def _raise_admin_join_error(validation: AdminJoinValidation, *, qa_path: Path) -> None:
    problem_df = validation.qa_df.loc[validation.qa_df["status"] != "matched"].copy()
    samples = (
        problem_df[["source_type", "district_name_raw", "status"]]
        .head(8)
        .to_dict(orient="records")
    )
    raise ValueError(
        "Telangana district/block boundary alignment failed before raster aggregation: "
        f"missing_in_blocks={validation.missing_in_blocks}, "
        f"missing_in_districts={validation.missing_in_districts}, "
        f"duplicate_within_source={validation.duplicate_within_source}. "
        f"See {qa_path}. Sample: {samples}"
    )


def _add_area_km2(gdf: gpd.GeoDataFrame, *, area_col: str) -> gpd.GeoDataFrame:
    projected = gdf.to_crs(epsg=AREA_EPSG).copy()
    out = gdf.copy()
    out[area_col] = projected.geometry.area / 1_000_000.0
    return out


def _geometry_coverage_stats(
    dataset: rasterio.io.DatasetReader,
    geom,
) -> GeometryCoverageStats:
    if geom is None or geom.is_empty:
        return GeometryCoverageStats(0, 0, 0.0, 0, 0, np.nan, np.nan, np.nan)
    try:
        window = geometry_window(dataset, [mapping(geom)])
    except WindowError:
        return GeometryCoverageStats(0, 0, 0.0, 0, 0, np.nan, np.nan, np.nan)

    masked_data = dataset.read(1, window=window, masked=True)
    data = np.asarray(masked_data.data, dtype=float)
    if data.size == 0:
        return GeometryCoverageStats(0, 0, 0.0, 0, 0, np.nan, np.nan, np.nan)

    in_polygon_mask = geometry_mask(
        [mapping(geom)],
        out_shape=data.shape,
        transform=dataset.window_transform(window),
        invert=True,
        all_touched=False,
    )
    total_in_polygon = int(np.count_nonzero(in_polygon_mask))
    if total_in_polygon == 0:
        return GeometryCoverageStats(0, 0, 0.0, 0, 0, np.nan, np.nan, np.nan)

    raw_mask = np.ma.getmaskarray(masked_data)
    finite_mask = np.isfinite(data)
    valid_mask = np.logical_and(in_polygon_mask, finite_mask)
    if dataset.nodata is not None and not _is_zero_nodata(dataset.nodata):
        valid_mask = np.logical_and(valid_mask, ~np.isclose(data, float(dataset.nodata)))
    if raw_mask is not np.ma.nomask:
        zero_dry_override = np.logical_and(in_polygon_mask, np.isclose(data, 0.0))
        valid_mask = np.logical_and(valid_mask, ~raw_mask)
        if _is_zero_nodata(dataset.nodata):
            # Preserve JRC dry-zero semantics even when the TIFF encodes nodata as zero.
            valid_mask = np.logical_or(valid_mask, np.logical_and(zero_dry_override, finite_mask))

    valid = data[valid_mask]
    valid_count = int(valid.size)
    coverage_fraction = float(valid_count / total_in_polygon) if total_in_polygon else 0.0
    if valid_count == 0:
        return GeometryCoverageStats(total_in_polygon, 0, coverage_fraction, 0, 0, np.nan, np.nan, np.nan)
    zero_count = int(np.count_nonzero(np.isclose(valid, 0.0)))
    positive_count = int(np.count_nonzero(valid > 0.0))
    positive_valid = valid[valid > 0.0]
    return GeometryCoverageStats(
        total_in_polygon,
        valid_count,
        coverage_fraction,
        zero_count,
        positive_count,
        float(np.nanmax(valid)),
        float(np.nanmean(valid)),
        _quantile_linear(positive_valid, 0.95),
    )


def _build_block_frames(
    *,
    block_gdf: gpd.GeoDataFrame,
    dataset: rasterio.io.DatasetReader,
    metric_slug: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    col = _derived_metric_column(metric_slug)
    block_raster = block_gdf.to_crs(dataset.crs).copy()

    master_rows: list[dict[str, object]] = []
    qa_rows: list[dict[str, object]] = []
    for src_row, raster_row in zip(block_gdf.itertuples(index=False), block_raster.itertuples(index=False)):
        stats = _geometry_coverage_stats(
            dataset,
            raster_row.geometry,
        )
        coverage_pass = stats.valid_in_polygon_cell_count > 0
        valid_support_fraction = _safe_fraction(
            stats.valid_in_polygon_cell_count,
            stats.total_in_polygon_cell_count,
        )
        flooded_support_fraction = _safe_fraction(
            stats.positive_valid_cell_count,
            stats.total_in_polygon_cell_count,
        )
        valid_supported_area_km2 = float(src_row.block_area_km2) * valid_support_fraction if pd.notna(valid_support_fraction) else np.nan
        flooded_supported_area_km2 = (
            float(src_row.block_area_km2) * flooded_support_fraction if pd.notna(flooded_support_fraction) else np.nan
        )
        if not coverage_pass:
            dashboard_value = np.nan
        elif stats.positive_valid_cell_count == 0:
            dashboard_value = 0.0
        else:
            dashboard_value = stats.p95_positive_depth_m

        master_rows.append(
            {
                "state": TARGET_STATE,
                "district": src_row.district_name,
                "block": src_row.block_name,
                "block_key": src_row.block_key,
                "block_area_km2": float(src_row.block_area_km2),
                col: dashboard_value,
            }
        )
        qa_rows.append(
            {
                "state": TARGET_STATE,
                "district": src_row.district_name,
                "block": src_row.block_name,
                "block_key": src_row.block_key,
                "block_area_km2": float(src_row.block_area_km2),
                "total_in_polygon_cell_count": stats.total_in_polygon_cell_count,
                "valid_in_polygon_cell_count": stats.valid_in_polygon_cell_count,
                "coverage_fraction": stats.coverage_fraction,
                "zero_valid_cell_count": stats.zero_valid_cell_count,
                "positive_valid_cell_count": stats.positive_valid_cell_count,
                "max_valid_depth_m": stats.max_valid_depth_m,
                "mean_valid_depth_m": (0.0 if coverage_pass and stats.positive_valid_cell_count == 0 else stats.mean_valid_depth_m),
                "p95_positive_depth_m": (0.0 if coverage_pass and stats.positive_valid_cell_count == 0 else stats.p95_positive_depth_m),
                "valid_support_fraction_of_block_area": valid_support_fraction,
                "flooded_support_fraction_of_block_area": flooded_support_fraction,
                "valid_supported_area_km2": valid_supported_area_km2,
                "flooded_supported_area_km2": flooded_supported_area_km2,
                "dashboard_value_m": dashboard_value,
                "coverage_pass": bool(coverage_pass),
            }
        )

    master_df = pd.DataFrame(master_rows).sort_values(["state", "district", "block"]).reset_index(drop=True)
    qa_df = pd.DataFrame(qa_rows).sort_values(["state", "district", "block"]).reset_index(drop=True)
    return master_df, qa_df


def _build_district_frames(
    *,
    district_gdf: gpd.GeoDataFrame,
    block_master_df: pd.DataFrame,
    block_qa_df: pd.DataFrame,
    dataset: rasterio.io.DatasetReader,
    metric_slug: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    col = _derived_metric_column(metric_slug)
    district_raster = district_gdf.to_crs(dataset.crs).copy()
    district_join_keys = pd.Series(
        [
            _district_join_key(state_name, district_name)
            for state_name, district_name in zip(district_gdf["state_name"], district_gdf["district_name"])
        ],
        index=district_gdf.index,
    )
    block_join_keys = pd.Series(
        [
            _district_join_key(TARGET_STATE, district_name)
            for district_name in block_master_df["district"]
        ],
        index=block_master_df.index,
    )

    master_rows: list[dict[str, object]] = []
    qa_rows: list[dict[str, object]] = []
    for idx, (src_row, raster_row) in enumerate(
        zip(district_gdf.itertuples(index=False), district_raster.itertuples(index=False))
    ):
        district_blocks = block_master_df.loc[
            block_join_keys == district_join_keys.iloc[idx]
        ].copy()
        if district_blocks.empty:
            raise ValueError(
                f"District {src_row.district_name!r} has no Telangana child blocks after normalized matching."
            )
        district_blocks = district_blocks.merge(
            block_qa_df[
                [
                    "block_key",
                    "mean_valid_depth_m",
                    "p95_positive_depth_m",
                    "valid_supported_area_km2",
                    "flooded_supported_area_km2",
                ]
            ],
            on="block_key",
            how="left",
            validate="one_to_one",
        )
        district_blocks[col] = pd.to_numeric(district_blocks[col], errors="coerce")
        district_blocks["block_area_km2"] = pd.to_numeric(district_blocks["block_area_km2"], errors="coerce").fillna(0.0)
        district_blocks["valid_supported_area_km2"] = pd.to_numeric(
            district_blocks.get("valid_supported_area_km2"),
            errors="coerce",
        ).fillna(0.0)
        district_blocks["flooded_supported_area_km2"] = pd.to_numeric(
            district_blocks.get("flooded_supported_area_km2"),
            errors="coerce",
        ).fillna(0.0)
        district_blocks["p95_positive_depth_m"] = pd.to_numeric(
            district_blocks.get("p95_positive_depth_m"),
            errors="coerce",
        )
        district_blocks["mean_valid_depth_m"] = pd.to_numeric(
            district_blocks.get("mean_valid_depth_m"),
            errors="coerce",
        )
        district_area_km2 = float(src_row.district_area_km2)
        child_block_area_sum_km2 = float(district_blocks["block_area_km2"].sum())
        child_block_area_gap_km2 = float(district_area_km2 - child_block_area_sum_km2)
        district_valid_supported_area_km2 = float(district_blocks["valid_supported_area_km2"].sum())
        district_flooded_supported_area_km2 = float(district_blocks["flooded_supported_area_km2"].sum())
        district_valid_support_fraction = (
            float(district_valid_supported_area_km2 / district_area_km2) if district_area_km2 > 0 else 0.0
        )
        covered = district_blocks.loc[district_blocks["valid_supported_area_km2"] > 0.0].copy()
        covered_flooded = district_blocks.loc[district_blocks["flooded_supported_area_km2"] > 0.0].copy()

        if district_valid_supported_area_km2 <= 0.0:
            chosen_value = np.nan
            covered_block_min_p95 = np.nan
            covered_block_max_p95 = np.nan
        elif district_flooded_supported_area_km2 <= 0.0:
            chosen_value = 0.0
            covered_block_min_p95 = np.nan
            covered_block_max_p95 = np.nan
        else:
            if covered_flooded["p95_positive_depth_m"].isna().any():
                missing_blocks = covered_flooded.loc[
                    covered_flooded["p95_positive_depth_m"].isna(), "block"
                ].astype(str).tolist()
                raise ValueError(
                    f"{metric_slug} district {src_row.district_name!r} has flooded blocks missing p95_positive_depth_m: {missing_blocks[:8]}"
                )
            numerator = float((covered_flooded["p95_positive_depth_m"] * covered_flooded["flooded_supported_area_km2"]).sum())
            chosen_value = float(numerator / district_flooded_supported_area_km2)
            covered_block_min_p95 = float(covered_flooded["p95_positive_depth_m"].min())
            covered_block_max_p95 = float(covered_flooded["p95_positive_depth_m"].max())

        direct_stats = _geometry_coverage_stats(
            dataset,
            raster_row.geometry,
        )
        direct_p95_positive = (
            0.0
            if direct_stats.valid_in_polygon_cell_count > 0 and direct_stats.positive_valid_cell_count == 0
            else direct_stats.p95_positive_depth_m
        )
        delta_vs_direct_p95 = (
            float(chosen_value - direct_p95_positive)
            if pd.notna(chosen_value) and pd.notna(direct_p95_positive)
            else np.nan
        )
        delta_warn = bool(pd.notna(delta_vs_direct_p95) and abs(delta_vs_direct_p95) > 1.0)

        if district_flooded_supported_area_km2 < -1e-6 or district_valid_supported_area_km2 < -1e-6:
            raise ValueError(
                f"{metric_slug} district {src_row.district_name!r} produced negative supported area diagnostics."
            )
        if district_flooded_supported_area_km2 > district_valid_supported_area_km2 + 1e-6:
            raise ValueError(
                f"{metric_slug} district {src_row.district_name!r} has flooded supported area exceeding valid supported area."
            )
        if district_valid_supported_area_km2 > district_area_km2 + 1e-6:
            raise ValueError(
                f"{metric_slug} district {src_row.district_name!r} has valid supported area exceeding district polygon area."
            )
        if district_valid_supported_area_km2 > 0.0 and district_flooded_supported_area_km2 <= 0.0:
            if not np.isclose(chosen_value, 0.0):
                raise ValueError(
                    f"{metric_slug} district {src_row.district_name!r} must publish 0.0 when valid support exists but flooded area is zero."
                )
        if district_flooded_supported_area_km2 > 0.0 and pd.notna(chosen_value):
            if chosen_value < covered_block_min_p95 - 1e-6 or chosen_value > covered_block_max_p95 + 1e-6:
                raise ValueError(
                    f"{metric_slug} district {src_row.district_name!r} depth {chosen_value} falls outside covered block p95 bounds "
                    f"[{covered_block_min_p95}, {covered_block_max_p95}]."
                )
        if covered_flooded.empty and district_flooded_supported_area_km2 > 0.0:
            raise ValueError(
                f"{metric_slug} district {src_row.district_name!r} has flooded supported area but no flooded blocks."
            )

        master_rows.append(
            {
                "state": TARGET_STATE,
                "district": src_row.district_name,
                "district_key": src_row.district_key,
                "district_area_km2": district_area_km2,
                col: chosen_value,
            }
        )
        qa_rows.append(
            {
                "state": TARGET_STATE,
                "district": src_row.district_name,
                "district_key": src_row.district_key,
                "district_area_km2": district_area_km2,
                "child_block_area_sum_km2": child_block_area_sum_km2,
                "child_block_area_gap_km2": child_block_area_gap_km2,
                "district_valid_supported_area_km2": district_valid_supported_area_km2,
                "district_flooded_supported_area_km2": district_flooded_supported_area_km2,
                "district_valid_support_fraction": district_valid_support_fraction,
                "covered_area_fraction": district_valid_support_fraction,
                "covered_block_count": int(covered.shape[0]),
                "uncovered_block_count": int(district_blocks.shape[0] - covered.shape[0]),
                "covered_flooded_block_count": int(covered_flooded.shape[0]),
                "covered_nonflooded_block_count": int(covered.shape[0] - covered_flooded.shape[0]),
                "chosen_value_m": chosen_value,
                "published_depth_m": chosen_value,
                "covered_block_min_p95_positive_depth_m": covered_block_min_p95,
                "covered_block_max_p95_positive_depth_m": covered_block_max_p95,
                "direct_p95_positive_depth_m": direct_p95_positive,
                "direct_total_in_polygon_cell_count": direct_stats.total_in_polygon_cell_count,
                "direct_valid_in_polygon_cell_count": direct_stats.valid_in_polygon_cell_count,
                "direct_positive_valid_cell_count": direct_stats.positive_valid_cell_count,
                "delta_vs_direct_p95_positive_m": delta_vs_direct_p95,
                "delta_warn": delta_warn,
            }
        )

    master_df = pd.DataFrame(master_rows).sort_values(["state", "district"]).reset_index(drop=True)
    qa_df = pd.DataFrame(qa_rows).sort_values(["state", "district"]).reset_index(drop=True)
    return master_df, qa_df


def _class_label(value: object) -> str:
    """Return the shared class label for one class index."""
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return ""
    return DERIVED_INDEX_LABELS.get(numeric, "")


def _coerce_depth_m(depth_m: object) -> Optional[float]:
    """Validate one raw RP-100 depth value in meters."""
    try:
        value = float(depth_m)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(value):
        return None
    if value < 0.0:
        raise ValueError(f"RP-100 flood depth cannot be negative: {value}")
    return value


def _coerce_extent_fraction(extent_fraction: object) -> Optional[float]:
    """Validate one raw RP-100 extent value stored as a fraction in [0, 1]."""
    try:
        value = float(extent_fraction)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(value):
        return None
    if value < 0.0 or value > 1.0:
        raise ValueError(f"RP-100 flood extent fraction must be within [0, 1]: {value}")
    return value


def _classify_depth_index(depth_m: object) -> Optional[int]:
    """Map one raw RP-100 flood-depth value to the configured depth class."""
    value = _coerce_depth_m(depth_m)
    if value is None:
        return None
    if value <= 0.2:
        return 1
    if value <= 0.5:
        return 2
    if value <= 1.0:
        return 3
    if value <= 2.5:
        return 4
    return 5


def _classify_extent_index(extent_fraction: object) -> Optional[int]:
    """Map one raw RP-100 extent fraction to the configured extent class."""
    value = _coerce_extent_fraction(extent_fraction)
    if value is None:
        return None
    if value <= 0.01:
        return 1
    if value <= 0.05:
        return 2
    if value <= 0.15:
        return 3
    if value <= 0.25:
        return 4
    return 5


def _lookup_severity_index(extent_class: Optional[int], depth_class: Optional[int]) -> Optional[int]:
    """Return the final RP-100 severity class from the agreed depth × extent matrix."""
    try:
        extent_numeric = float(extent_class) if extent_class is not None else float("nan")
        depth_numeric = float(depth_class) if depth_class is not None else float("nan")
    except (TypeError, ValueError):
        return None
    if not np.isfinite(extent_numeric) or not np.isfinite(depth_numeric):
        return None
    extent_int = int(round(extent_numeric))
    depth_int = int(round(depth_numeric))
    if not np.isclose(extent_numeric, extent_int) or not np.isclose(depth_numeric, depth_int):
        raise ValueError(
            f"RP-100 severity lookup expects integer classes 1..5, got extent={extent_class}, depth={depth_class}"
        )
    if extent_int not in DERIVED_INDEX_LABELS or depth_int not in DERIVED_INDEX_LABELS:
        raise ValueError(
            f"RP-100 severity lookup expects classes 1..5, got extent={extent_class}, depth={depth_class}"
        )
    return int(DERIVED_INDEX_MATRIX[extent_int - 1][depth_int - 1])


def _build_derived_index_outputs(
    *,
    raw_depth_output: dict[str, pd.DataFrame],
    extent_output: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Build the persisted RP100 severity-index masters and QA frames."""
    raw_col = _derived_metric_column(DERIVED_INDEX_SOURCE_METRIC_SLUG)
    derived_col = _derived_metric_column(DERIVED_INDEX_METRIC_SLUG)
    extent_col = _derived_metric_column(DERIVED_EXTENT_METRIC_SLUG)

    block_master = raw_depth_output["block_master_df"].copy()
    block_master = block_master.rename(columns={raw_col: "raw_rp100_depth_m"})
    block_extent = extent_output["block_master_df"].loc[
        :, ["state", "district", "block", "block_key", extent_col]
    ].rename(columns={extent_col: "raw_rp100_extent_fraction"})
    block_master = block_master.merge(
        block_extent,
        on=["state", "district", "block", "block_key"],
        how="left",
        validate="one_to_one",
    )
    block_master["depth_class_index"] = block_master["raw_rp100_depth_m"].map(_classify_depth_index)
    block_master["extent_class_index"] = block_master["raw_rp100_extent_fraction"].map(_classify_extent_index)
    block_master[derived_col] = pd.to_numeric(
        block_master.apply(
            lambda row: _lookup_severity_index(
                row["extent_class_index"],
                row["depth_class_index"],
            ),
            axis=1,
        ),
        errors="coerce",
    )
    block_master = block_master.drop(
        columns=["raw_rp100_depth_m", "raw_rp100_extent_fraction", "depth_class_index", "extent_class_index"]
    )

    district_master = raw_depth_output["district_master_df"].copy()
    district_master = district_master.rename(columns={raw_col: "raw_rp100_depth_m"})
    district_extent = extent_output["district_master_df"].loc[
        :, ["state", "district", "district_key", extent_col]
    ].rename(columns={extent_col: "raw_rp100_extent_fraction"})
    district_master = district_master.merge(
        district_extent,
        on=["state", "district", "district_key"],
        how="left",
        validate="one_to_one",
    )
    district_master["depth_class_index"] = district_master["raw_rp100_depth_m"].map(_classify_depth_index)
    district_master["extent_class_index"] = district_master["raw_rp100_extent_fraction"].map(_classify_extent_index)
    district_master[derived_col] = pd.to_numeric(
        district_master.apply(
            lambda row: _lookup_severity_index(
                row["extent_class_index"],
                row["depth_class_index"],
            ),
            axis=1,
        ),
        errors="coerce",
    )
    district_master = district_master.drop(
        columns=["raw_rp100_depth_m", "raw_rp100_extent_fraction", "depth_class_index", "extent_class_index"]
    )

    block_qa = raw_depth_output["block_qa_df"].loc[
        :,
        ["state", "district", "block", "block_key", "dashboard_value_m", "coverage_pass"],
    ].copy()
    block_qa = block_qa.rename(columns={"dashboard_value_m": "raw_rp100_depth_m"})
    block_qa = block_qa.merge(
        block_extent,
        on=["state", "district", "block", "block_key"],
        how="left",
        validate="one_to_one",
    )
    block_qa["depth_class_index"] = block_qa["raw_rp100_depth_m"].map(_classify_depth_index)
    block_qa["depth_class_label"] = block_qa["depth_class_index"].map(_class_label)
    block_qa["extent_class_index"] = block_qa["raw_rp100_extent_fraction"].map(_classify_extent_index)
    block_qa["extent_class_label"] = block_qa["extent_class_index"].map(_class_label)
    block_qa["depth_class_index"] = pd.to_numeric(block_qa["depth_class_index"], errors="coerce")
    block_qa["extent_class_index"] = pd.to_numeric(block_qa["extent_class_index"], errors="coerce")
    block_qa["class_index"] = pd.to_numeric(
        block_qa.apply(
            lambda row: _lookup_severity_index(
                row["extent_class_index"],
                row["depth_class_index"],
            ),
            axis=1,
        ),
        errors="coerce",
    )
    block_qa["class_label"] = block_qa["class_index"].map(_class_label)

    district_qa = raw_depth_output["district_qa_df"].loc[
        :,
        ["state", "district", "district_key", "chosen_value_m", "district_valid_support_fraction"],
    ].copy()
    district_qa = district_qa.rename(columns={"chosen_value_m": "raw_rp100_depth_m"})
    district_qa = district_qa.merge(
        district_extent,
        on=["state", "district", "district_key"],
        how="left",
        validate="one_to_one",
    )
    district_qa["depth_class_index"] = district_qa["raw_rp100_depth_m"].map(_classify_depth_index)
    district_qa["depth_class_label"] = district_qa["depth_class_index"].map(_class_label)
    district_qa["extent_class_index"] = district_qa["raw_rp100_extent_fraction"].map(_classify_extent_index)
    district_qa["extent_class_label"] = district_qa["extent_class_index"].map(_class_label)
    district_qa["depth_class_index"] = pd.to_numeric(district_qa["depth_class_index"], errors="coerce")
    district_qa["extent_class_index"] = pd.to_numeric(district_qa["extent_class_index"], errors="coerce")
    district_qa["class_index"] = pd.to_numeric(
        district_qa.apply(
            lambda row: _lookup_severity_index(
                row["extent_class_index"],
                row["depth_class_index"],
            ),
            axis=1,
        ),
        errors="coerce",
    )
    district_qa["class_label"] = district_qa["class_index"].map(_class_label)

    return {
        "block_master_df": block_master,
        "district_master_df": district_master,
        "block_qa_df": block_qa,
        "district_qa_df": district_qa,
    }


def _build_derived_extent_outputs(
    *,
    raw_output: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Build the persisted RP100 flood-extent masters and QA frames."""
    derived_col = _derived_metric_column(DERIVED_EXTENT_METRIC_SLUG)

    block_qa_raw = raw_output["block_qa_df"].copy()
    district_qa_raw = raw_output["district_qa_df"].copy()
    district_master_raw = raw_output["district_master_df"].copy()

    block_qa = block_qa_raw.loc[
        :,
        [
            "state",
            "district",
            "block",
            "block_key",
            "block_area_km2",
            "total_in_polygon_cell_count",
            "valid_in_polygon_cell_count",
            "positive_valid_cell_count",
            "valid_support_fraction_of_block_area",
            "flooded_support_fraction_of_block_area",
            "valid_supported_area_km2",
            "flooded_supported_area_km2",
        ],
    ].copy()
    block_qa["district_join_key"] = block_qa["district"].map(
        lambda value: _district_join_key(TARGET_STATE, value)
    )
    block_qa["has_raster_overlap"] = block_qa["total_in_polygon_cell_count"].gt(0)
    block_qa["has_valid_support"] = block_qa["valid_in_polygon_cell_count"].gt(0)
    block_qa[derived_col] = block_qa["flooded_support_fraction_of_block_area"]
    block_qa.loc[~block_qa["has_valid_support"], derived_col] = np.nan
    block_qa["publishable"] = block_qa["has_valid_support"]

    block_master = block_qa.loc[
        :,
        ["state", "district", "block", "block_key", "block_area_km2", derived_col],
    ].copy()

    district_lookup = district_master_raw.loc[
        :,
        ["district", "district_key", "district_area_km2"],
    ].copy()
    district_lookup["district_join_key"] = district_lookup["district"].map(
        lambda value: _district_join_key(TARGET_STATE, value)
    )
    district_qa_rows: list[dict[str, object]] = []
    for district_row in district_lookup.itertuples(index=False):
        block_rows = block_qa.loc[
            block_qa["district_join_key"].astype(str) == str(district_row.district_join_key)
        ].copy()
        covered_block_count = int(block_rows["has_valid_support"].fillna(False).astype(bool).sum())
        uncovered_block_count = int(block_rows.shape[0] - covered_block_count)
        district_valid_supported_area_km2 = float(
            pd.to_numeric(block_rows["valid_supported_area_km2"], errors="coerce").fillna(0.0).sum()
        )
        district_flooded_supported_area_km2 = float(
            pd.to_numeric(block_rows["flooded_supported_area_km2"], errors="coerce").fillna(0.0).sum()
        )
        district_area_km2 = float(district_row.district_area_km2)
        covered_valid_support_fraction = _safe_fraction(district_valid_supported_area_km2, district_area_km2)
        publishable = bool(district_valid_supported_area_km2 > 0.0)
        extent_fraction = _safe_fraction(district_flooded_supported_area_km2, district_area_km2)
        if not publishable:
            extent_fraction = float("nan")

        direct_row = district_qa_raw.loc[
            district_qa_raw["district_key"].astype(str) == str(district_row.district_key)
        ]
        direct_total = direct_row["direct_total_in_polygon_cell_count"].iloc[0] if not direct_row.empty else np.nan
        direct_valid = direct_row["direct_valid_in_polygon_cell_count"].iloc[0] if not direct_row.empty else np.nan
        direct_positive = direct_row["direct_positive_valid_cell_count"].iloc[0] if not direct_row.empty else np.nan
        direct_extent_fraction = _safe_fraction(direct_positive, direct_total)
        delta_vs_direct = (
            float(extent_fraction - direct_extent_fraction)
            if pd.notna(extent_fraction) and pd.notna(direct_extent_fraction)
            else np.nan
        )
        delta_warn = bool(pd.notna(delta_vs_direct) and abs(delta_vs_direct) > 0.02)

        district_qa_rows.append(
            {
                "state": TARGET_STATE,
                "district": district_row.district,
                "district_key": district_row.district_key,
                "district_area_km2": district_area_km2,
                "district_valid_supported_area_km2": district_valid_supported_area_km2,
                "district_flooded_supported_area_km2": district_flooded_supported_area_km2,
                "covered_valid_support_fraction": covered_valid_support_fraction,
                "covered_block_count": covered_block_count,
                "uncovered_block_count": uncovered_block_count,
                derived_col: extent_fraction,
                "publishable": publishable,
                "direct_extent_fraction_from_raster": direct_extent_fraction,
                "delta_vs_direct_extent_fraction": delta_vs_direct,
                "delta_warn": delta_warn,
            }
        )

    district_qa = pd.DataFrame(district_qa_rows).sort_values(["state", "district"]).reset_index(drop=True)
    block_qa = block_qa.drop(columns=["district_join_key"])
    district_master = district_qa.loc[
        :,
        ["state", "district", "district_key", "district_area_km2", derived_col],
    ].copy()

    return {
        "block_master_df": block_master,
        "district_master_df": district_master,
        "block_qa_df": block_qa,
        "district_qa_df": district_qa,
    }


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file without --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_master(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    parquet_path = path.with_suffix(".parquet")
    if not overwrite:
        existing = [str(candidate) for candidate in (path, parquet_path) if candidate.exists()]
        if existing:
            raise FileExistsError(
                f"Refusing to overwrite existing file without --overwrite: {', '.join(existing)}"
            )
    _write_csv(df, path, overwrite=True)
    df.to_parquet(parquet_path, index=False)


def _expected_output_paths(*, metric_slug: str, qa_dir: Path) -> list[Path]:
    processed_root = resolve_processed_root(metric_slug, data_dir=get_paths_config().data_dir, mode="portfolio")
    state_root = processed_root / TARGET_STATE
    district_csv = state_root / get_master_csv_filename("district")
    block_csv = state_root / get_master_csv_filename("block")
    return [
        district_csv,
        district_csv.with_suffix(".parquet"),
        block_csv,
        block_csv.with_suffix(".parquet"),
        qa_dir / f"{metric_slug}_block_qa.csv",
        qa_dir / f"{metric_slug}_district_qa.csv",
    ]


def build_jrc_flood_depth_outputs(
    *,
    source_dir: Path,
    districts_path: Path,
    blocks_path: Path,
    qa_dir: Path,
    assume_units: str,
    overwrite: bool,
    dry_run: bool,
) -> dict[str, object]:
    """Build Telangana block and district masters plus QA for the four JRC rasters."""
    contract = _validate_raster_contract(source_dir, assume_units=assume_units)
    district_gdf, block_gdf = _load_telangana_admin(
        districts_path=districts_path,
        blocks_path=blocks_path,
    )
    district_gdf = _add_area_km2(district_gdf, area_col="district_area_km2")
    block_gdf = _add_area_km2(block_gdf, area_col="block_area_km2")

    all_target_paths: list[Path] = []
    for metric_slug in ALL_OUTPUT_METRIC_SLUGS:
        all_target_paths.extend(_expected_output_paths(metric_slug=metric_slug, qa_dir=qa_dir))
    all_target_paths.append(qa_dir / "admin_boundary_join_qa.csv")
    all_target_paths.append(qa_dir / "run_summary.csv")
    if not overwrite:
        existing = [path for path in all_target_paths if path.exists()]
        if existing:
            joined = ", ".join(str(path) for path in existing[:8])
            more = " ..." if len(existing) > 8 else ""
            raise FileExistsError(f"Refusing to overwrite existing outputs without --overwrite: {joined}{more}")

    join_validation = _collect_admin_join_validation(district_gdf=district_gdf, block_gdf=block_gdf)
    admin_join_qa_path = qa_dir / "admin_boundary_join_qa.csv"
    if not dry_run:
        _write_csv(join_validation.qa_df, admin_join_qa_path, overwrite=overwrite)
    if (
        join_validation.missing_in_blocks
        or join_validation.missing_in_districts
        or join_validation.duplicate_within_source
    ):
        _raise_admin_join_error(join_validation, qa_path=admin_join_qa_path)

    outputs: dict[str, object] = {}
    run_summary_rows: list[dict[str, object]] = []

    for metric_slug, raster_path in contract.raster_paths.items():
        with rasterio.open(raster_path) as dataset:
            block_master_df, block_qa_df = _build_block_frames(
                block_gdf=block_gdf,
                dataset=dataset,
                metric_slug=metric_slug,
            )
            district_master_df, district_qa_df = _build_district_frames(
                district_gdf=district_gdf,
                block_master_df=block_master_df,
                block_qa_df=block_qa_df,
                dataset=dataset,
                metric_slug=metric_slug,
            )

        outputs[metric_slug] = {
            "block_master_df": block_master_df,
            "district_master_df": district_master_df,
            "block_qa_df": block_qa_df,
            "district_qa_df": district_qa_df,
        }
        run_summary_rows.append(
            {
                "run_utc": pd.Timestamp.utcnow().isoformat(),
                "metric_slug": metric_slug,
                "metric_kind": "raw_raster",
                "source_metric_slug": "",
                "component_metric_slugs": "",
                "source_dir": str(contract.source_dir),
                "assume_units": assume_units,
                "raster_crs": contract.raster_crs,
                "raster_shape": contract.raster_shape,
                "nodata_value": contract.nodata_value,
                "depth_method": "block_p95_positive__district_flooded_area_weighted_v2",
                "district_area_denominator": "district_polygon_area_epsg6933_v2",
                "percentile_method": "q95_linear__positive_depth_only_v2",
                "severity_method": "",
                "blocks_total": int(block_qa_df.shape[0]),
                "blocks_covered": int(block_qa_df["coverage_pass"].fillna(False).astype(bool).sum()),
                "blocks_uncovered": int((~block_qa_df["coverage_pass"].fillna(False).astype(bool)).sum()),
                "districts_total": int(district_qa_df.shape[0]),
                "districts_covered": int(district_qa_df["chosen_value_m"].notna().sum()),
                "districts_uncovered": int(district_qa_df["chosen_value_m"].isna().sum()),
                "district_delta_warn_count": int(district_qa_df["delta_warn"].fillna(False).astype(bool).sum()),
                "boundary_join_missing_count": int(
                    join_validation.missing_in_blocks + join_validation.missing_in_districts
                ),
                "boundary_join_duplicate_count": int(join_validation.duplicate_within_source),
            }
            )

        if not dry_run:
            processed_root = resolve_processed_root(metric_slug, data_dir=get_paths_config().data_dir, mode="portfolio")
            state_root = processed_root / TARGET_STATE
            _write_master(
                district_master_df,
                state_root / get_master_csv_filename("district"),
                overwrite=overwrite,
            )
            _write_master(
                block_master_df,
                state_root / get_master_csv_filename("block"),
                overwrite=overwrite,
            )
            _write_csv(block_qa_df, qa_dir / f"{metric_slug}_block_qa.csv", overwrite=overwrite)
            _write_csv(district_qa_df, qa_dir / f"{metric_slug}_district_qa.csv", overwrite=overwrite)

    derived_extent_output = _build_derived_extent_outputs(raw_output=outputs[DERIVED_EXTENT_SOURCE_METRIC_SLUG])
    derived_outputs = {
        DERIVED_EXTENT_METRIC_SLUG: derived_extent_output,
        DERIVED_INDEX_METRIC_SLUG: _build_derived_index_outputs(
            raw_depth_output=outputs[DERIVED_INDEX_SOURCE_METRIC_SLUG],
            extent_output=derived_extent_output,
        ),
    }
    derived_sources = {
        DERIVED_INDEX_METRIC_SLUG: DERIVED_INDEX_SOURCE_METRIC_SLUG,
        DERIVED_EXTENT_METRIC_SLUG: DERIVED_EXTENT_SOURCE_METRIC_SLUG,
    }
    derived_kinds = {
        DERIVED_INDEX_METRIC_SLUG: "derived_severity_matrix",
        DERIVED_EXTENT_METRIC_SLUG: "derived_extent",
    }
    for metric_slug, derived_output in derived_outputs.items():
        outputs[metric_slug] = derived_output
        derived_block_master_df = derived_output["block_master_df"]
        derived_district_master_df = derived_output["district_master_df"]
        derived_block_qa_df = derived_output["block_qa_df"]
        derived_district_qa_df = derived_output["district_qa_df"]
        metric_col = _derived_metric_column(metric_slug)
        publishable_block_col = "publishable" if "publishable" in derived_block_qa_df.columns else metric_col
        publishable_district_col = "publishable" if "publishable" in derived_district_qa_df.columns else metric_col
        run_summary_rows.append(
            {
                "run_utc": pd.Timestamp.utcnow().isoformat(),
                "metric_slug": metric_slug,
                "metric_kind": derived_kinds[metric_slug],
                "source_metric_slug": derived_sources[metric_slug],
                "component_metric_slugs": (
                    "jrc_flood_depth_rp100;jrc_flood_extent_rp100"
                    if metric_slug == DERIVED_INDEX_METRIC_SLUG
                    else ""
                ),
                "source_dir": str(contract.source_dir),
                "assume_units": assume_units,
                "raster_crs": contract.raster_crs,
                "raster_shape": contract.raster_shape,
                "nodata_value": contract.nodata_value,
                "depth_method": "block_p95_positive__district_flooded_area_weighted_v2",
                "district_area_denominator": "district_polygon_area_epsg6933_v2",
                "percentile_method": "q95_linear__positive_depth_only_v2",
                "severity_method": (
                    "rp100_depth_extent_matrix_v1"
                    if metric_slug == DERIVED_INDEX_METRIC_SLUG
                    else ""
                ),
                "blocks_total": int(derived_block_master_df.shape[0]),
                "blocks_covered": int(
                    derived_block_qa_df[publishable_block_col].fillna(False).astype(bool).sum()
                    if publishable_block_col == "publishable"
                    else derived_block_master_df[metric_col].notna().sum()
                ),
                "blocks_uncovered": int(
                    (~derived_block_qa_df[publishable_block_col].fillna(False).astype(bool)).sum()
                    if publishable_block_col == "publishable"
                    else derived_block_master_df[metric_col].isna().sum()
                ),
                "districts_total": int(derived_district_master_df.shape[0]),
                "districts_covered": int(
                    derived_district_qa_df[publishable_district_col].fillna(False).astype(bool).sum()
                    if publishable_district_col == "publishable"
                    else derived_district_master_df[metric_col].notna().sum()
                ),
                "districts_uncovered": int(
                    (~derived_district_qa_df[publishable_district_col].fillna(False).astype(bool)).sum()
                    if publishable_district_col == "publishable"
                    else derived_district_master_df[metric_col].isna().sum()
                ),
                "district_delta_warn_count": int(derived_district_qa_df.get("delta_warn", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
                "boundary_join_missing_count": int(
                    join_validation.missing_in_blocks + join_validation.missing_in_districts
                ),
                "boundary_join_duplicate_count": int(join_validation.duplicate_within_source),
            }
        )
        if not dry_run:
            processed_root = resolve_processed_root(
                metric_slug,
                data_dir=get_paths_config().data_dir,
                mode="portfolio",
            )
            state_root = processed_root / TARGET_STATE
            _write_master(
                derived_district_master_df,
                state_root / get_master_csv_filename("district"),
                overwrite=overwrite,
            )
            _write_master(
                derived_block_master_df,
                state_root / get_master_csv_filename("block"),
                overwrite=overwrite,
            )
            _write_csv(
                derived_block_qa_df,
                qa_dir / f"{metric_slug}_block_qa.csv",
                overwrite=overwrite,
            )
            _write_csv(
                derived_district_qa_df,
                qa_dir / f"{metric_slug}_district_qa.csv",
                overwrite=overwrite,
            )

    run_summary_df = pd.DataFrame(run_summary_rows).sort_values("metric_slug").reset_index(drop=True)
    if not dry_run:
        _write_csv(run_summary_df, qa_dir / "run_summary.csv", overwrite=overwrite)
    outputs["run_summary_df"] = run_summary_df
    outputs["contract"] = contract
    outputs["admin_join_qa_df"] = join_validation.qa_df
    return outputs


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build Telangana block and district JRC flood-depth masters for RP-10, RP-50, RP-100, "
            "and RP-500, plus the derived RP-100 flood-depth-index and flood-extent outputs."
        )
    )
    parser.add_argument("--source-dir", required=True, help="Directory containing the required JRC flood-depth rasters.")
    parser.add_argument("--assume-units", choices=[ASSUME_UNITS], required=True, help="Attest the raster depth units.")
    parser.add_argument("--districts-path", default=str(get_paths_config().districts_path))
    parser.add_argument("--blocks-path", default=str(get_paths_config().blocks_path))
    parser.add_argument("--qa-dir", default=str(_default_qa_dir()))
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs and compute summaries without writing files.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)
    outputs = build_jrc_flood_depth_outputs(
        source_dir=Path(args.source_dir).expanduser().resolve(),
        districts_path=Path(args.districts_path).expanduser().resolve(),
        blocks_path=Path(args.blocks_path).expanduser().resolve(),
        qa_dir=Path(args.qa_dir).expanduser().resolve(),
        assume_units=str(args.assume_units),
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
    )
    run_summary_df = outputs["run_summary_df"]
    contract: RasterContract = outputs["contract"]
    print("JRC FLOOD DEPTH ADMIN MASTERS")
    print(f"source_dir: {contract.source_dir}")
    print(f"raster_crs: {contract.raster_crs}")
    print(f"raster_shape: {contract.raster_shape}")
    print(f"metrics: {', '.join(sorted(slug for slug in outputs if slug in ALL_OUTPUT_METRIC_SLUGS))}")
    print(f"run_summary_rows: {int(run_summary_df.shape[0])}")
    if bool(args.dry_run):
        print("dry_run: True")
    else:
        print(f"qa_dir: {Path(args.qa_dir).expanduser().resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

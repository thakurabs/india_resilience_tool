#!/usr/bin/env python3
"""Build district/block irrigated and rainfed cropland masters from LGRIP30.

Source: LGRIP30 V001 (GFSAD project, USGS/NASA LP DAAC),
DOI 10.5067/Community/LGRIP/LGRIP30.001 -- Landsat-derived, 30 m, nominal 2015.
Band 1 is a uint8 class raster::

    0 = Water (ocean and water bodies)
    1 = Non-cropland
    2 = Irrigated cropland
    3 = Rainfed cropland

Three quantities are published, each as an area and a share:

    irrigated  = class 2
    rainfed    = class 3
    cropland   = classes 2 + 3

``cropland`` is deliberately published alongside the split. It is a like-for-like
replacement candidate for ``lulc_agri_share_pct``, whose source raster has no
recoverable provenance (BACKLOG BL-0027); publishing it here lets the two be
compared directly before anything is retired.

Method mirrors ``build_lulc_admin_masters`` exactly so the cropland comparison is
apples to apples: tabulation happens in EPSG:6933 through a nearest-neighbour
WarpedVRT with centroid inclusion (``all_touched=False``), and every share divides
by the full canonical polygon area in EPSG:6933 -- never by cropland area. A share
of district area is what makes a stipple comparable between a small intensively
farmed unit and a large sparsely farmed one.

The ten India tiles sit on one exact common grid, so they are mosaicked through a
generated VRT with no resampling and no reprojection.
"""

from __future__ import annotations

import argparse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window
from rasterio.vrt import WarpedVRT
from shapely.geometry import box, mapping
from shapely.ops import unary_union

from paths import get_master_csv_filename, get_paths_config, resolve_processed_root
from tools.geodata.build_district_subbasin_crosswalk import (
    load_block_boundaries,
    load_district_boundaries,
)


AdminLevel = Literal["district", "block"]

AREA_EPSG = 6933
SNAPSHOT_SCENARIO = "snapshot"
SNAPSHOT_PERIOD = "Current"

CLASS_WATER = 0
CLASS_NON_CROPLAND = 1
CLASS_IRRIGATED = 2
CLASS_RAINFED = 3
EXPECTED_CLASSES = (CLASS_WATER, CLASS_NON_CROPLAND, CLASS_IRRIGATED, CLASS_RAINFED)

VRT_NAME = "lgrip30_india.vrt"
TILE_GLOB = "LGRIP30_2015_*.tif"

IRRIGATED_AREA_COL = "lgrip_irrigated_area_km2__snapshot__Current__mean"
IRRIGATED_SHARE_COL = "lgrip_irrigated_share_pct__snapshot__Current__mean"
RAINFED_AREA_COL = "lgrip_rainfed_area_km2__snapshot__Current__mean"
RAINFED_SHARE_COL = "lgrip_rainfed_share_pct__snapshot__Current__mean"
CROPLAND_AREA_COL = "lgrip_cropland_area_km2__snapshot__Current__mean"
CROPLAND_SHARE_COL = "lgrip_cropland_share_pct__snapshot__Current__mean"

METRIC_COLUMNS: dict[str, str] = {
    "lgrip_irrigated_area_km2": IRRIGATED_AREA_COL,
    "lgrip_irrigated_share_pct": IRRIGATED_SHARE_COL,
    "lgrip_rainfed_area_km2": RAINFED_AREA_COL,
    "lgrip_rainfed_share_pct": RAINFED_SHARE_COL,
    "lgrip_cropland_area_km2": CROPLAND_AREA_COL,
    "lgrip_cropland_share_pct": CROPLAND_SHARE_COL,
}

#: India's gross cropped area is roughly 2.0 million km2 and net sown area roughly
#: 1.4 million km2. A cropland-extent mask should land between them, wide enough
#: for a genuine product revision, tight enough to catch a class-mapping error.
NATIONAL_CROPLAND_KM2_MIN = 1_000_000.0
NATIONAL_CROPLAND_KM2_MAX = 2_400_000.0

#: Share ceiling used to catch a genuine denominator, CRS or class-mapping fault.
#: It is deliberately NOT set just above 100: counting whole cells whose centre falls
#: inside a polygon makes a fully-cropped unit land marginally above 100% by pure
#: quantisation (a 67 km2 block that is 100% cropland measures 100.01%). The invariant
#: that actually catches bugs is ``cropland_cells <= support_cells``, enforced
#: separately and without tolerance.
SHARE_OUTLIER_MAX_PCT = 101.0
#: Units whose share exceeds this are flagged in QA but do not fail the build.
SHARE_QUANTISATION_FLAG_PCT = 100.0
#: Minimum share of an admin unit that must fall inside the tile footprint union.
MIN_TILE_COVERAGE_PCT = 99.9
LOW_SUPPORT_PCT = 95.0


@dataclass(frozen=True)
class GeometryLgripStats:
    """Per-geometry class cell counts and the equal-area cell size."""

    irrigated_cells: int
    rainfed_cells: int
    support_cells: int
    cell_area_m2: float

    def _km2(self, cells: int) -> float:
        return cells * self.cell_area_m2 / 1_000_000.0

    @property
    def irrigated_area_km2(self) -> float:
        return self._km2(self.irrigated_cells)

    @property
    def rainfed_area_km2(self) -> float:
        return self._km2(self.rainfed_cells)

    @property
    def cropland_area_km2(self) -> float:
        return self._km2(self.irrigated_cells + self.rainfed_cells)

    @property
    def support_area_km2(self) -> float:
        return self._km2(self.support_cells)


def _default_tile_dir() -> Path:
    return get_paths_config().data_dir / "irrigation"


def _default_qa_dir() -> Path:
    return get_paths_config().data_dir / "irrigation"


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


# ---------------------------------------------------------------------------
# VRT mosaic
# ---------------------------------------------------------------------------

def build_tile_vrt(
    tile_dir: Path,
    vrt_path: Path,
    *,
    overwrite: bool,
    dry_run: bool,
    tile_glob: str = TILE_GLOB,
    product: str = "LGRIP30",
) -> dict[str, object]:
    """Write a VRT mosaicking co-gridded uint8 tiles, or fail if they are not.

    A VRT is used rather than a physical mosaic because the ten India tiles are
    roughly 14 GB uncompressed. Every tile is verified to share one resolution and
    to sit at an integer cell offset from a common origin, so the mosaic involves
    no resampling whatsoever; anything else is refused rather than silently warped.

    ``tile_glob`` and ``product`` exist so the same mosaicker serves any single-band
    uint8 EPSG:4326 tile set on a common grid; JRC Global Surface Water is the second
    caller. Nothing here is specific to the class semantics of either product.
    """
    tiles = sorted(tile_dir.glob(tile_glob))
    if not tiles:
        raise FileNotFoundError(f"No {product} tiles matching {tile_glob} in {tile_dir}")

    specs = []
    for path in tiles:
        with rasterio.open(path) as src:
            if src.count != 1:
                raise ValueError(f"{path.name}: expected a single band, found {src.count}")
            if src.crs is None or src.crs.to_epsg() != 4326:
                raise ValueError(f"{path.name}: expected EPSG:4326, found {src.crs}")
            if np.dtype(src.dtypes[0]) != np.uint8:
                raise ValueError(f"{path.name}: expected uint8, found {src.dtypes[0]}")
            t = src.transform
            specs.append(
                {
                    "path": path,
                    "width": int(src.width),
                    "height": int(src.height),
                    "res_x": float(t.a),
                    "res_y": float(-t.e),
                    "origin_x": float(t.c),
                    "origin_y": float(t.f),
                    "block": (int(src.block_shapes[0][1]), int(src.block_shapes[0][0])),
                }
            )

    res_x = specs[0]["res_x"]
    res_y = specs[0]["res_y"]
    for spec in specs:
        if not (np.isclose(spec["res_x"], res_x, rtol=0, atol=1e-12)
                and np.isclose(spec["res_y"], res_y, rtol=0, atol=1e-12)):
            raise ValueError(
                f"{spec['path'].name}: resolution {spec['res_x']}x{spec['res_y']} differs from "
                f"{res_x}x{res_y}. A no-resample VRT mosaic requires one common resolution."
            )

    origin_x = min(spec["origin_x"] for spec in specs)
    origin_y = max(spec["origin_y"] for spec in specs)

    for spec in specs:
        col = (spec["origin_x"] - origin_x) / res_x
        row = (origin_y - spec["origin_y"]) / res_y
        if abs(col - round(col)) > 1e-6 or abs(row - round(row)) > 1e-6:
            raise ValueError(
                f"{spec['path'].name}: sits at a fractional cell offset "
                f"(col={col}, row={row}) from the mosaic origin. Refusing to build a "
                "VRT that would need resampling."
            )
        spec["col"] = int(round(col))
        spec["row"] = int(round(row))

    mosaic_width = max(spec["col"] + spec["width"] for spec in specs)
    mosaic_height = max(spec["row"] + spec["height"] for spec in specs)

    root = ET.Element("VRTDataset", rasterXSize=str(mosaic_width), rasterYSize=str(mosaic_height))
    ET.SubElement(root, "SRS").text = "EPSG:4326"
    ET.SubElement(root, "GeoTransform").text = (
        f"{origin_x!r}, {res_x!r}, 0.0, {origin_y!r}, 0.0, {-res_y!r}"
    )
    band = ET.SubElement(root, "VRTRasterBand", dataType="Byte", band="1")
    ET.SubElement(band, "ColorInterp").text = "Gray"

    for spec in specs:
        source = ET.SubElement(band, "SimpleSource")
        filename = ET.SubElement(source, "SourceFilename", relativeToVRT="1")
        filename.text = spec["path"].name
        ET.SubElement(source, "SourceBand").text = "1"
        ET.SubElement(
            source,
            "SourceProperties",
            RasterXSize=str(spec["width"]),
            RasterYSize=str(spec["height"]),
            DataType="Byte",
            BlockXSize=str(spec["block"][0]),
            BlockYSize=str(spec["block"][1]),
        )
        ET.SubElement(source, "SrcRect", xOff="0", yOff="0",
                      xSize=str(spec["width"]), ySize=str(spec["height"]))
        ET.SubElement(source, "DstRect", xOff=str(spec["col"]), yOff=str(spec["row"]),
                      xSize=str(spec["width"]), ySize=str(spec["height"]))

    if vrt_path.exists() and not overwrite and not dry_run:
        raise FileExistsError(f"Refusing to overwrite existing VRT without --overwrite: {vrt_path}")

    if not dry_run:
        vrt_path.parent.mkdir(parents=True, exist_ok=True)
        ET.ElementTree(root).write(vrt_path, encoding="utf-8", xml_declaration=False)

    return {
        "path": str(vrt_path),
        "tile_count": len(specs),
        "tiles": [spec["path"].name for spec in specs],
        "tile_bounds": [
            (
                spec["origin_x"],
                spec["origin_y"] - spec["height"] * res_y,
                spec["origin_x"] + spec["width"] * res_x,
                spec["origin_y"],
            )
            for spec in specs
        ],
        "width": mosaic_width,
        "height": mosaic_height,
        "resolution_deg": res_x,
        "written": not dry_run,
    }


def tile_coverage_pct(
    admin_gdf: gpd.GeoDataFrame, tile_bounds: list[tuple[float, float, float, float]]
) -> pd.Series:
    """Percent of each admin polygon that falls inside the union of tile footprints.

    This exists because a VRT reports no error for an area no tile covers: the read
    simply returns 0, which in LGRIP is the legitimate class "water". A missing tile
    therefore masquerades as a district that is 100% water, and no nodata, support or
    class check can tell the two apart. Only the geometry of the tile set can.
    """
    if not tile_bounds:
        return pd.Series(np.zeros(len(admin_gdf)), index=admin_gdf.index, dtype="float64")
    footprint = unary_union([box(*bounds) for bounds in tile_bounds])
    geoms = admin_gdf.to_crs(epsg=4326).geometry
    covered = gpd.GeoSeries(
        [geom.intersection(footprint) for geom in geoms], crs="EPSG:4326", index=admin_gdf.index
    )
    total_area = geoms.to_crs(epsg=AREA_EPSG).area
    covered_area = covered.to_crs(epsg=AREA_EPSG).area
    return pd.Series(
        np.where(total_area > 0, 100.0 * covered_area / total_area, np.nan),
        index=admin_gdf.index,
        dtype="float64",
    )


# ---------------------------------------------------------------------------
# Raster validation
# ---------------------------------------------------------------------------

def _equal_area_vrt(src: rasterio.io.DatasetReader) -> WarpedVRT:
    return WarpedVRT(src, crs=f"EPSG:{AREA_EPSG}", resampling=Resampling.nearest)


def _cell_area_m2(src: rasterio.io.DatasetReader | WarpedVRT) -> float:
    cell_area = float(abs(src.transform.a * src.transform.e))
    if not np.isfinite(cell_area) or cell_area <= 0:
        raise ValueError("Equal-area raster cell area must be positive and finite.")
    return cell_area


def scan_class_histogram(raster_path: Path, *, sample_step: int = 1) -> dict[int, int]:
    """Class histogram over the native-CRS mosaic, read block by block."""
    counts: dict[int, int] = {}
    with rasterio.open(raster_path) as src:
        for _index, window in src.block_windows(1):
            if sample_step > 1 and (_index[0] % sample_step or _index[1] % sample_step):
                continue
            data = np.asarray(src.read(1, window=window, masked=False))
            values, freq = np.unique(data, return_counts=True)
            for value, count in zip(values.tolist(), freq.tolist()):
                counts[int(value)] = counts.get(int(value), 0) + int(count)
    return counts


# ---------------------------------------------------------------------------
# Zonal aggregation
# ---------------------------------------------------------------------------

def _zonal_lgrip_for_geometry(src: WarpedVRT, geom) -> GeometryLgripStats:
    cell_area = _cell_area_m2(src)
    if geom is None or geom.is_empty:
        return GeometryLgripStats(0, 0, 0, cell_area)
    try:
        window = geometry_window(src, [mapping(geom)])
    except WindowError:
        return GeometryLgripStats(0, 0, 0, cell_area)

    data = src.read(1, window=window, masked=False)
    if data.size == 0:
        return GeometryLgripStats(0, 0, 0, cell_area)

    inside = geometry_mask(
        [mapping(geom)],
        out_shape=data.shape,
        transform=src.window_transform(window),
        invert=True,
        all_touched=False,
    )
    values = np.asarray(data)
    return GeometryLgripStats(
        irrigated_cells=int(np.count_nonzero(inside & (values == CLASS_IRRIGATED))),
        rainfed_cells=int(np.count_nonzero(inside & (values == CLASS_RAINFED))),
        support_cells=int(np.count_nonzero(inside)),
        cell_area_m2=cell_area,
    )


def aggregate_lgrip_to_admin_units(
    admin_gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
    raster_path: Path,
    area_epsg: int = AREA_EPSG,
    progress_every: int = 0,
    tile_bounds: Optional[list[tuple[float, float, float, float]]] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate LGRIP irrigated/rainfed/cropland area onto canonical polygons."""
    if admin_gdf.empty:
        raise ValueError(f"No {level} boundaries were provided.")
    missing = [col for col in _identity_cols(level) + ["geometry"] if col not in admin_gdf.columns]
    if missing:
        raise ValueError(f"{level.title()} boundaries are missing required columns: {missing}")

    with rasterio.open(raster_path) as base:
        if base.crs is None:
            raise ValueError(f"LGRIP raster has no CRS: {raster_path}")
        with _equal_area_vrt(base) as src:
            admin_for_raster = admin_gdf.to_crs(src.crs).copy()
            stats: list[GeometryLgripStats] = []
            for position, geom in enumerate(admin_for_raster.geometry, start=1):
                stats.append(_zonal_lgrip_for_geometry(src, geom))
                if progress_every and position % progress_every == 0:
                    print(f"    {level}: {position}/{len(admin_for_raster)}", flush=True)

    area_df = admin_gdf.to_crs(epsg=area_epsg).copy()
    out = admin_gdf[_identity_cols(level)].copy()
    out["polygon_area_km2"] = pd.to_numeric(area_df.geometry.area / 1_000_000.0, errors="coerce")
    out[_area_col(level)] = out["polygon_area_km2"]

    out["irrigated_cell_count"] = [item.irrigated_cells for item in stats]
    out["rainfed_cell_count"] = [item.rainfed_cells for item in stats]
    out[IRRIGATED_AREA_COL] = [item.irrigated_area_km2 for item in stats]
    out[RAINFED_AREA_COL] = [item.rainfed_area_km2 for item in stats]
    out[CROPLAND_AREA_COL] = [item.cropland_area_km2 for item in stats]

    polygon_area = pd.to_numeric(out["polygon_area_km2"], errors="coerce")
    for area_col, share_col in (
        (IRRIGATED_AREA_COL, IRRIGATED_SHARE_COL),
        (RAINFED_AREA_COL, RAINFED_SHARE_COL),
        (CROPLAND_AREA_COL, CROPLAND_SHARE_COL),
    ):
        out[share_col] = np.where(
            polygon_area.gt(0), out[area_col] / polygon_area * 100.0, np.nan
        )

    out["tile_coverage_pct"] = (
        tile_coverage_pct(admin_gdf, tile_bounds).to_numpy()
        if tile_bounds is not None
        else np.full(len(out), np.nan)
    )
    out["raster_extent_support_cell_count"] = [item.support_cells for item in stats]
    out["raster_extent_support_area_km2"] = [item.support_area_km2 for item in stats]
    out["support_area_pct_of_polygon"] = np.where(
        polygon_area.gt(0), out["raster_extent_support_area_km2"] / polygon_area * 100.0, np.nan
    )
    out["low_support_coverage"] = pd.to_numeric(
        out["support_area_pct_of_polygon"], errors="coerce"
    ).lt(LOW_SUPPORT_PCT)
    # Cropland can never exceed the raster support inside the polygon; that is the
    # invariant worth failing on. Exceeding 100% of *polygon* area, by contrast, is
    # an expected discretisation effect for small or thin units, so it is recorded
    # rather than treated as an error.
    out["cropland_exceeds_support"] = (
        pd.to_numeric(out["irrigated_cell_count"], errors="coerce").fillna(0)
        + pd.to_numeric(out["rainfed_cell_count"], errors="coerce").fillna(0)
    ).gt(pd.to_numeric(out["raster_extent_support_cell_count"], errors="coerce").fillna(0))
    out["share_above_100_pct"] = pd.to_numeric(
        out[CROPLAND_SHARE_COL], errors="coerce"
    ).gt(SHARE_QUANTISATION_FLAG_PCT)
    out["share_out_of_range"] = pd.to_numeric(
        out[CROPLAND_SHARE_COL], errors="coerce"
    ).gt(SHARE_OUTLIER_MAX_PCT)

    master_df = out.rename(
        columns={"state_name": "state", "district_name": "district", "block_name": "block"}
    )
    master_df = master_df.sort_values(_output_key_cols(level)).reset_index(drop=True)

    qa_df = master_df.copy()
    qa_df["source_raster"] = str(raster_path)
    qa_df["centroid_inclusion_rule"] = "all_touched=False"
    qa_df["share_denominator"] = "polygon_area_epsg_6933"
    return master_df, qa_df


# ---------------------------------------------------------------------------
# Guardrails and QA
# ---------------------------------------------------------------------------

def build_lgrip_national_summary(
    district_master_df: pd.DataFrame,
    block_master_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    def _sum(df: pd.DataFrame, col: str) -> float:
        return float(pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum())

    row = {
        "district_irrigated_km2": _sum(district_master_df, IRRIGATED_AREA_COL),
        "district_rainfed_km2": _sum(district_master_df, RAINFED_AREA_COL),
        "district_cropland_km2": _sum(district_master_df, CROPLAND_AREA_COL),
        "district_polygon_km2": _sum(district_master_df, "polygon_area_km2"),
    }
    if block_master_df is not None and not block_master_df.empty:
        row.update(
            {
                "block_irrigated_km2": _sum(block_master_df, IRRIGATED_AREA_COL),
                "block_rainfed_km2": _sum(block_master_df, RAINFED_AREA_COL),
                "block_cropland_km2": _sum(block_master_df, CROPLAND_AREA_COL),
            }
        )
    total = row["district_cropland_km2"]
    row["national_cropland_share_pct"] = (
        100.0 * total / row["district_polygon_km2"] if row["district_polygon_km2"] > 0 else float("nan")
    )
    row["national_irrigated_pct_of_cropland"] = (
        100.0 * row["district_irrigated_km2"] / total if total > 0 else float("nan")
    )
    return pd.DataFrame([row])


def assert_lgrip_guardrails(
    *,
    district_master_df: pd.DataFrame,
    block_master_df: Optional[pd.DataFrame],
    national_summary_df: pd.DataFrame,
    class_histogram: dict[int, int],
    allow_incomplete_coverage: bool,
    allow_unexpected_values: bool,
    allow_total_outlier: bool,
    allow_share_outlier: bool,
) -> None:
    coverage_gaps: list[str] = []
    for level, frame in (
        [("district", district_master_df)]
        + ([("block", block_master_df)] if block_master_df is not None and not block_master_df.empty else [])
    ):
        coverage = pd.to_numeric(frame.get("tile_coverage_pct"), errors="coerce")
        if coverage is None or coverage.isna().all():
            continue
        bad = coverage.lt(MIN_TILE_COVERAGE_PCT)
        if bool(bad.any()):
            worst = frame.loc[bad].assign(_c=coverage[bad]).nsmallest(5, "_c")
            sample = [
                f"{r['state']}/{r['district']} {r['_c']:.1f}%" for _, r in worst.iterrows()
            ]
            coverage_gaps.append(
                f"{level}: {int(bad.sum())} unit(s) below {MIN_TILE_COVERAGE_PCT}% tile "
                f"coverage, worst: {sample}"
            )
    if coverage_gaps and not allow_incomplete_coverage:
        raise ValueError(
            "LGRIP tile coverage is incomplete for some admin units. An uncovered area "
            "reads as class 0 (water) and would be published as zero cropland:\n  - "
            + "\n  - ".join(coverage_gaps)
            + "\nDownload the missing tiles, or rerun with --allow-incomplete-coverage "
              "if the gap is genuinely outside India."
        )

    unexpected = {k: v for k, v in class_histogram.items() if k not in EXPECTED_CLASSES}
    if unexpected and not allow_unexpected_values:
        raise ValueError(
            f"LGRIP raster contains values outside {EXPECTED_CLASSES}: {unexpected}. "
            "Recheck the source or rerun with --allow-unexpected-values."
        )

    total = float(national_summary_df.iloc[0]["district_cropland_km2"])
    if not allow_total_outlier and not (NATIONAL_CROPLAND_KM2_MIN <= total <= NATIONAL_CROPLAND_KM2_MAX):
        raise ValueError(
            "LGRIP national cropland total is outside the guardrail range "
            f"{NATIONAL_CROPLAND_KM2_MIN:,.0f}-{NATIONAL_CROPLAND_KM2_MAX:,.0f} km2: "
            f"{total:,.2f} km2. Recheck the class mapping or rerun with --allow-total-outlier."
        )

    frames = [("district", district_master_df)]
    if block_master_df is not None and not block_master_df.empty:
        frames.append(("block", block_master_df))

    # Hard invariant: no tolerance, no override. Cropland cells are a subset of the
    # support cells by construction, so a violation means the tabulation is wrong.
    invariant_breaks: list[str] = []
    for level, frame in frames:
        bad = frame["cropland_exceeds_support"].fillna(False).astype(bool)
        if bool(bad.any()):
            sample = frame.loc[bad, _output_key_cols(level)].head(5).to_dict(orient="records")
            invariant_breaks.append(f"{level}: {int(bad.sum())} unit(s), e.g. {sample}")
    if invariant_breaks:
        raise ValueError(
            "LGRIP cropland area exceeds the raster support area for some units, which "
            "cannot happen if the tabulation is correct:\n  - " + "\n  - ".join(invariant_breaks)
        )

    offenders: list[str] = []
    for level, frame in frames:
        bad = pd.to_numeric(frame[CROPLAND_SHARE_COL], errors="coerce").gt(SHARE_OUTLIER_MAX_PCT)
        if bool(bad.any()):
            sample = frame.loc[bad, _output_key_cols(level)].head(5).to_dict(orient="records")
            offenders.append(f"{level}: {int(bad.sum())} unit(s) above {SHARE_OUTLIER_MAX_PCT}%, e.g. {sample}")
    if offenders and not allow_share_outlier:
        raise ValueError(
            "LGRIP cropland share guardrail failed:\n  - " + "\n  - ".join(offenders)
            + "\nRerun with --allow-share-outlier only if this is understood."
        )


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

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
            raise FileExistsError(
                f"Refusing to overwrite existing file without --overwrite: {', '.join(existing)}"
            )
    _write_csv(df, path, overwrite=True)
    df.to_parquet(parquet_path, index=False)


def metric_specific_master(
    master_df: pd.DataFrame, *, level: AdminLevel, metric_slug: str
) -> pd.DataFrame:
    if metric_slug not in METRIC_COLUMNS:
        raise ValueError(f"Unsupported LGRIP metric slug: {metric_slug}")
    identity = (
        ["state", "district", "block", "block_key"]
        if level == "block"
        else ["state", "district", "district_key"]
    )
    keep = identity + [_area_col(level), METRIC_COLUMNS[metric_slug]]
    return master_df[keep].copy()


def _write_state_slices(
    master_df: pd.DataFrame, *, metric_slug: str, level: AdminLevel, overwrite: bool
) -> dict[str, int]:
    processed_root = resolve_processed_root(
        metric_slug, data_dir=get_paths_config().data_dir, mode="portfolio"
    )
    out_name = get_master_csv_filename(level)
    counts: dict[str, int] = {}
    for state_name, state_df in master_df.groupby("state", dropna=False, as_index=False):
        state_label = str(state_name or "").strip()
        if not state_label:
            raise ValueError(f"LGRIP {level} master contains an empty state value.")
        _write_master_table(
            state_df.reset_index(drop=True), processed_root / state_label / out_name, overwrite=overwrite
        )
        counts[state_label] = int(state_df.shape[0])
    return counts


def build_lgrip_admin_outputs(
    *,
    tile_dir: Path,
    vrt_path: Path,
    districts_path: Path,
    blocks_path: Path,
    qa_dir: Path,
    overwrite: bool,
    dry_run: bool,
    skip_blocks: bool = False,
    allow_incomplete_coverage: bool = False,
    allow_unexpected_values: bool = False,
    allow_total_outlier: bool = False,
    allow_share_outlier: bool = False,
    histogram_sample_step: int = 0,
    progress_every: int = 0,
) -> dict[str, object]:
    """Build LGRIP irrigated/rainfed/cropland district and block masters."""
    vrt_info = build_tile_vrt(tile_dir, vrt_path, overwrite=overwrite, dry_run=dry_run)

    if dry_run and not vrt_path.exists():
        return {"vrt": vrt_info, "aggregated": False}

    class_histogram = (
        scan_class_histogram(vrt_path, sample_step=histogram_sample_step)
        if histogram_sample_step
        else {}
    )

    district_gdf = load_district_boundaries(districts_path)
    district_master_df, district_qa_df = aggregate_lgrip_to_admin_units(
        district_gdf, level="district", raster_path=vrt_path, progress_every=progress_every,
        tile_bounds=vrt_info["tile_bounds"]
    )

    block_master_df: Optional[pd.DataFrame] = None
    block_qa_df: Optional[pd.DataFrame] = None
    if not skip_blocks:
        block_gdf = load_block_boundaries(blocks_path)
        block_master_df, block_qa_df = aggregate_lgrip_to_admin_units(
            block_gdf, level="block", raster_path=vrt_path, progress_every=progress_every,
            tile_bounds=vrt_info["tile_bounds"]
        )

    national_summary_df = build_lgrip_national_summary(district_master_df, block_master_df)
    assert_lgrip_guardrails(
        district_master_df=district_master_df,
        block_master_df=block_master_df,
        national_summary_df=national_summary_df,
        class_histogram=class_histogram,
        allow_incomplete_coverage=allow_incomplete_coverage,
        allow_unexpected_values=allow_unexpected_values,
        allow_total_outlier=allow_total_outlier,
        allow_share_outlier=allow_share_outlier,
    )

    district_counts: dict[str, int] = {}
    block_counts: dict[str, int] = {}
    if not dry_run:
        for slug in METRIC_COLUMNS:
            district_counts = _write_state_slices(
                metric_specific_master(district_master_df, level="district", metric_slug=slug),
                metric_slug=slug, level="district", overwrite=overwrite,
            )
            if block_master_df is not None:
                block_counts = _write_state_slices(
                    metric_specific_master(block_master_df, level="block", metric_slug=slug),
                    metric_slug=slug, level="block", overwrite=overwrite,
                )
        _write_csv(district_qa_df, qa_dir / "lgrip_district_master_qa.csv", overwrite=overwrite)
        if block_qa_df is not None:
            _write_csv(block_qa_df, qa_dir / "lgrip_block_master_qa.csv", overwrite=overwrite)
        _write_csv(national_summary_df, qa_dir / "lgrip_national_summary.csv", overwrite=overwrite)

    return {
        "vrt": vrt_info,
        "class_histogram": class_histogram,
        "district_master_df": district_master_df,
        "block_master_df": block_master_df,
        "district_qa_df": district_qa_df,
        "block_qa_df": block_qa_df,
        "national_summary_df": national_summary_df,
        "district_counts": district_counts,
        "block_counts": block_counts,
        "aggregated": True,
    }


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build district and block irrigated/rainfed cropland masters from LGRIP30."
    )
    parser.add_argument("--tile-dir", type=str, default=str(_default_tile_dir()),
                        help="Directory holding the LGRIP30 GeoTIFF tiles.")
    parser.add_argument("--vrt", type=str, default="",
                        help=f"VRT mosaic path (default: <tile-dir>/{VRT_NAME}).")
    parser.add_argument("--districts", type=str, default=str(get_paths_config().districts_path))
    parser.add_argument("--blocks", type=str, default=str(get_paths_config().blocks_path))
    parser.add_argument("--qa-dir", type=str, default=str(_default_qa_dir()))
    parser.add_argument("--skip-blocks", action="store_true",
                        help="District level only. Useful for a fast first pass.")
    parser.add_argument("--histogram-sample-step", type=int, default=0,
                        help="Scan every Nth raster block for a class histogram (0 disables).")
    parser.add_argument("--progress-every", type=int, default=0,
                        help="Print progress every N units (0 disables).")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate tiles and report planned outputs without writing masters.")
    parser.add_argument("--allow-incomplete-coverage", action="store_true",
                        help="Allow admin units not fully inside the tile footprint union.")
    parser.add_argument("--allow-unexpected-values", action="store_true",
                        help="Allow class values outside {0, 1, 2, 3}.")
    parser.add_argument("--allow-total-outlier", action="store_true",
                        help="Allow a national cropland total outside the guardrail range.")
    parser.add_argument("--allow-share-outlier", action="store_true",
                        help="Allow unit cropland shares above 100.01%%.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    tile_dir = Path(args.tile_dir).expanduser().resolve()
    vrt_path = Path(args.vrt).expanduser().resolve() if args.vrt else tile_dir / VRT_NAME
    districts_path = Path(args.districts).expanduser().resolve()
    blocks_path = Path(args.blocks).expanduser().resolve()
    qa_dir = Path(args.qa_dir).expanduser().resolve()

    if not tile_dir.exists():
        parser.error(f"Tile directory not found: {tile_dir}")
    if not districts_path.exists():
        parser.error(f"District boundaries not found: {districts_path}")
    if not args.skip_blocks and not blocks_path.exists():
        parser.error(f"Block boundaries not found: {blocks_path}")

    outputs = build_lgrip_admin_outputs(
        tile_dir=tile_dir,
        vrt_path=vrt_path,
        districts_path=districts_path,
        blocks_path=blocks_path,
        qa_dir=qa_dir,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        skip_blocks=bool(args.skip_blocks),
        allow_incomplete_coverage=bool(args.allow_incomplete_coverage),
        allow_unexpected_values=bool(args.allow_unexpected_values),
        allow_total_outlier=bool(args.allow_total_outlier),
        allow_share_outlier=bool(args.allow_share_outlier),
        histogram_sample_step=int(args.histogram_sample_step),
        progress_every=int(args.progress_every),
    )

    mode = "DRY RUN" if args.dry_run else "WROTE"
    vrt_info = outputs["vrt"]
    print(f"[{mode}] LGRIP30 admin masters")
    print(f"  vrt: {vrt_info['tile_count']} tiles -> {vrt_info['path']}")
    print(f"       {vrt_info['width']} x {vrt_info['height']} px @ {vrt_info['resolution_deg']:.10f} deg")
    if not outputs.get("aggregated"):
        print("  VRT absent under --dry-run; skipped admin aggregation.")
        return 0

    if outputs.get("class_histogram"):
        total = sum(outputs["class_histogram"].values())
        print("  class histogram (sampled):")
        for value in sorted(outputs["class_histogram"]):
            count = outputs["class_histogram"][value]
            print(f"    {value}: {count:,} ({100.0 * count / total:.3f}%)")

    summary = outputs["national_summary_df"].iloc[0]
    print(f"  districts: {len(outputs['district_master_df']):,}")
    if outputs["block_master_df"] is not None:
        print(f"  blocks   : {len(outputs['block_master_df']):,}")
    print(f"  national irrigated = {summary['district_irrigated_km2']:,.0f} km2")
    print(f"  national rainfed   = {summary['district_rainfed_km2']:,.0f} km2")
    print(f"  national cropland  = {summary['district_cropland_km2']:,.0f} km2 "
          f"({summary['national_cropland_share_pct']:.2f}% of land area)")
    print(f"  irrigated share of cropland = {summary['national_irrigated_pct_of_cropland']:.2f}%")
    print("  metric slugs: " + ", ".join(sorted(METRIC_COLUMNS)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

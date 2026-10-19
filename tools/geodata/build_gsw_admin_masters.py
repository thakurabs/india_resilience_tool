#!/usr/bin/env python3
"""Build district/block inland surface-water masters from JRC Global Surface Water.

Source: JRC Global Surface Water v1.4, ``occurrence`` band (Pekel et al., 2016,
Nature), observation period 1984-03 to 2021-12, 30 m, EPSG:4326. Band 1 is uint8::

    0-100 = percent of valid observations in which the pixel was water
    255   = no valid observation  (NOT zero water)

Two quantities are published, each as an area and a share of full unit area::

    permanent = occurrence >= 75
    seasonal  = 25 <= occurrence < 75

Occurrence below 25 is discarded as ephemeral. These thresholds are a stated
methodological choice, not a product definition: GSW ships a dedicated
``seasonality`` band that would settle the split without thresholds, and only
``occurrence`` was acquired. Anything that ranks units on these numbers inherits
the choice, so it is named here, in the QA tables, and in the published provenance.

Method mirrors ``build_lgrip_admin_masters`` so that water, cropland and built-up
shares are mutually comparable: tabulation happens in EPSG:6933 through a
nearest-neighbour WarpedVRT with centroid inclusion (``all_touched=False``), and
every share divides by the full canonical polygon area in EPSG:6933.

The sea problem
---------------
GSW masks the open ocean inconsistently. Far offshore it carries 0 or 255, but a
nearshore band tens of kilometres wide is classified as genuine permanent water at
99-100% occurrence. That band falls inside coastal admin polygons, so an uncorrected
tabulation reports the sea as district surface water: the Nicobars measure 17.5%
permanent water, of which almost all is ocean. Left alone, an inland-water metric
would rank coastal districts highest for the wrong reason.

Because the offshore mask is inconsistent, a flood fill from the raster edge cannot
reach that band. The mask is therefore built by connectivity to the area outside the
national land union, on a coarse grid:

    1. read the mosaic decimated to ~550 m
    2. water = permanent occurrence
    3. open the water mask by ``--sea-opening-cells`` (default 1, ~550 m), which
       severs narrow barmouths and river mouths
    4. seed from opened water lying outside the union of State/UT polygons
    5. the connected components containing a seed are sea; dilate back and
       intersect with the original water mask

Step 3 is what protects coastal lagoons. Vembanad and Chilika are physically
connected to the sea through mouths under a kilometre wide, so a naive fill swallows
them: Alappuzha falls from 10.7% to 8.2% and Puri from 19.8% to 16.7%. One cell of
opening restores both (8.9% and 18.2%, stable at every larger opening) while still
removing roughly 90% of the marine water from island districts. Larger openings only
protect more nearshore creeks, so the smallest opening that stabilises the lagoons is
the default.

The correction is never silent: the marine area removed from each unit is published
as a QA column, so a coastal unit's residual nearshore water stays auditable.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window, rasterize
from rasterio.vrt import WarpedVRT
from scipy import ndimage
from shapely.ops import unary_union
from shapely.geometry import mapping

from paths import get_master_csv_filename, get_paths_config, resolve_processed_root
from tools.geodata.build_district_subbasin_crosswalk import (
    load_block_boundaries,
    load_district_boundaries,
)
from tools.geodata.build_lgrip_admin_masters import (
    build_tile_vrt,
    scan_class_histogram,
    tile_coverage_pct,
)


AdminLevel = Literal["district", "block"]

AREA_EPSG = 6933
SNAPSHOT_SCENARIO = "snapshot"
SNAPSHOT_PERIOD = "Current"

#: Occurrence band semantics.
NODATA_VALUE = 255
MAX_VALID_OCCURRENCE = 100
#: Occurrence at or above this is treated as permanent water.
PERMANENT_MIN_OCCURRENCE = 75
#: Occurrence at or above this, and below the permanent floor, is seasonal water.
SEASONAL_MIN_OCCURRENCE = 25

VRT_NAME = "gsw_occurrence_india.vrt"
SEA_MASK_NAME = "gsw_sea_mask.tif"
TILE_GLOB = "occurrence_*.tif"
PRODUCT_LABEL = "JRC GSW occurrence"

#: Decimation factor for the coarse grid the sea mask is built on. The native
#: mosaic is 160000 x 160000; 20x gives 8000 x 8000 at ~0.005 deg (~550 m), which
#: reads in about fifteen seconds and labels in under a second.
SEA_MASK_DECIMATION = 20
#: Cells of binary opening applied before seeding the sea fill. See the module
#: docstring: this is what keeps coastal lagoons out of the sea mask.
SEA_OPENING_CELLS = 1

PERMANENT_AREA_COL = "surface_water_permanent_area_km2__snapshot__Current__mean"
PERMANENT_SHARE_COL = "surface_water_permanent_share_pct__snapshot__Current__mean"
SEASONAL_AREA_COL = "surface_water_seasonal_area_km2__snapshot__Current__mean"
SEASONAL_SHARE_COL = "surface_water_seasonal_share_pct__snapshot__Current__mean"

METRIC_COLUMNS: dict[str, str] = {
    "surface_water_permanent_area_km2": PERMANENT_AREA_COL,
    "surface_water_permanent_share_pct": PERMANENT_SHARE_COL,
    "surface_water_seasonal_area_km2": SEASONAL_AREA_COL,
    "surface_water_seasonal_share_pct": SEASONAL_SHARE_COL,
}

#: Permanent water at a 75% occurrence floor is a strict definition: it counts only
#: what is wet in three observations out of four across 1984-2021, so it captures
#: large reservoirs, lakes and perennial river channels and excludes most tanks and
#: floodplain. India measures about 17,000 km2 on it, roughly 0.5% of land area --
#: far below the 2-3% usually quoted for "water bodies", which mixes in seasonal
#: extent. The range is set to catch the two ways this can break rather than to
#: police a revision: a sea mask that fails open pushes the total up several-fold
#: (marine water is roughly two thirds of all permanent water in the India box),
#: and a threshold or band error collapses it towards zero.
NATIONAL_PERMANENT_KM2_MIN = 8_000.0
NATIONAL_PERMANENT_KM2_MAX = 60_000.0

#: See ``build_lgrip_admin_masters``: counting whole cells whose centre falls inside
#: a polygon puts a fully-covered unit marginally above 100% by pure quantisation,
#: so the ceiling sits above 100 and the invariant that actually catches bugs is
#: ``water_cells <= support_cells``, enforced separately and without tolerance.
SHARE_OUTLIER_MAX_PCT = 101.0
SHARE_QUANTISATION_FLAG_PCT = 100.0
MIN_TILE_COVERAGE_PCT = 99.9
LOW_SUPPORT_PCT = 95.0
#: Units where the sea mask removed at least this share of the unit's own area are
#: flagged: their published figure is a residual after a large marine correction.
MARINE_CORRECTION_FLAG_PCT = 1.0
#: Units where this share of the unit carries no valid observation are flagged.
HIGH_NODATA_FLAG_PCT = 5.0


@dataclass(frozen=True)
class GeometrySurfaceWaterStats:
    """Per-geometry occurrence-class cell counts and the equal-area cell size."""

    permanent_cells: int
    seasonal_cells: int
    marine_cells: int
    nodata_cells: int
    support_cells: int
    cell_area_m2: float

    def _km2(self, cells: int) -> float:
        return cells * self.cell_area_m2 / 1_000_000.0

    @property
    def permanent_area_km2(self) -> float:
        return self._km2(self.permanent_cells)

    @property
    def seasonal_area_km2(self) -> float:
        return self._km2(self.seasonal_cells)

    @property
    def marine_area_km2(self) -> float:
        return self._km2(self.marine_cells)

    @property
    def support_area_km2(self) -> float:
        return self._km2(self.support_cells)


def _default_tile_dir() -> Path:
    return get_paths_config().data_dir / "surface_water"


def _default_qa_dir() -> Path:
    return get_paths_config().data_dir / "surface_water"


def _default_states_path() -> Path:
    """The State/UT boundary file, which ``paths.py`` does not expose as a field.

    Any polygon set whose union is the national land area works as a sea-fill seed;
    the States file is simply the cheapest one to dissolve.
    """
    return get_paths_config().data_dir / "states_4326.geojson"


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


def _cell_area_m2(src) -> float:
    cell_area = float(abs(src.transform.a * src.transform.e))
    if not np.isfinite(cell_area) or cell_area <= 0:
        raise ValueError("Equal-area raster cell area must be positive and finite.")
    return cell_area


# ---------------------------------------------------------------------------
# Sea mask
# ---------------------------------------------------------------------------

def build_sea_mask(
    raster_path: Path,
    states_path: Path,
    *,
    decimation: int = SEA_MASK_DECIMATION,
    opening_cells: int = SEA_OPENING_CELLS,
) -> tuple[np.ndarray, rasterio.Affine, dict[str, object]]:
    """Return a coarse boolean mask of marine water, its transform and statistics.

    Marine water is permanent water connected to the area outside the national land
    union, measured after a binary opening of ``opening_cells``. The opening is the
    whole point: without it the fill runs through the barmouths of Vembanad and
    Chilika and removes two of India's largest inland water bodies. See the module
    docstring for the measured effect of each opening width.
    """
    if decimation < 1:
        raise ValueError(f"Sea mask decimation must be at least 1, got {decimation}")
    if opening_cells < 0:
        raise ValueError(f"Sea mask opening must not be negative, got {opening_cells}")

    with rasterio.open(raster_path) as src:
        height = src.height // decimation
        width = src.width // decimation
        if height < 1 or width < 1:
            raise ValueError(
                f"Decimation {decimation} leaves a {width}x{height} grid; too coarse to "
                "resolve a coastline."
            )
        occurrence = src.read(
            1, out_shape=(height, width), resampling=Resampling.nearest
        )
        transform = src.transform * src.transform.scale(
            src.width / width, src.height / height
        )
        raster_crs = src.crs

    water = (occurrence >= PERMANENT_MIN_OCCURRENCE) & (occurrence <= MAX_VALID_OCCURRENCE)
    if not water.any():
        raise ValueError(
            "No permanent water found on the coarse grid. The occurrence band or the "
            "decimation is wrong; refusing to build an empty sea mask."
        )

    states = gpd.read_file(states_path)
    if states.empty:
        raise ValueError(f"State boundaries are empty: {states_path}")
    if states.crs is not None and raster_crs is not None and states.crs != raster_crs:
        states = states.to_crs(raster_crs)
    land = rasterize(
        [(unary_union(states.geometry.values), 1)],
        out_shape=water.shape,
        transform=transform,
        fill=0,
        all_touched=True,
    ).astype(bool)
    if not land.any():
        raise ValueError(
            "The national land union rasterised to nothing on the coarse grid. The "
            "sea fill would have no seed and would remove no water."
        )

    # ``border_value=1`` treats everything beyond the array as water, so eroding does
    # not detach the ocean from the array edge and quietly empty the seed set.
    core = (
        ndimage.binary_erosion(water, iterations=opening_cells, border_value=1)
        if opening_cells
        else water
    )
    labels, component_count = ndimage.label(core)
    seed_ids = np.unique(labels[core & ~land])
    seed_ids = seed_ids[seed_ids > 0]
    if seed_ids.size == 0:
        raise ValueError(
            "No permanent water lies outside the national land union, so the sea fill "
            "has no seed. Check that the State boundaries and the raster share a CRS."
        )
    sea_core = np.isin(labels, seed_ids)
    sea = (
        ndimage.binary_dilation(sea_core, iterations=opening_cells) & water
        if opening_cells
        else sea_core
    )

    water_cells = int(water.sum())
    stats: dict[str, object] = {
        "decimation": int(decimation),
        "opening_cells": int(opening_cells),
        "grid_shape": (int(water.shape[0]), int(water.shape[1])),
        "resolution_deg": float(abs(transform.a)),
        "component_count": int(component_count),
        "seed_component_count": int(seed_ids.size),
        "water_cells": water_cells,
        "sea_cells": int(sea.sum()),
        "sea_pct_of_water": 100.0 * float(sea.sum()) / water_cells if water_cells else float("nan"),
    }
    return sea, transform, stats


def write_sea_mask_raster(
    sea: np.ndarray,
    transform: rasterio.Affine,
    path: Path,
    *,
    crs: str = "EPSG:4326",
    overwrite: bool,
) -> Path:
    """Persist the coarse sea mask so the zonal pass can read it as a raster."""
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file without --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=int(sea.shape[0]),
        width=int(sea.shape[1]),
        count=1,
        dtype="uint8",
        crs=crs,
        transform=transform,
        compress="LZW",
        tiled=True,
    ) as dst:
        dst.write(sea.astype("uint8"), 1)
    return path


# ---------------------------------------------------------------------------
# Zonal aggregation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CoarseSeaMask:
    """The sea mask held in memory on its own coarse equal-area grid.

    The obvious implementation pins a ``WarpedVRT`` of the mask to the occurrence
    grid so one window indexes both rasters. It is also four times slower than the
    whole rest of the aggregation: every window read then makes GDAL warp a ~550 m
    mask up to a ~27 m grid, for 7,900 windows. Holding the coarse array and
    sampling it by index instead is exact -- both grids are axis-aligned in the
    same equal-area CRS, so mapping a fine pixel centre to a coarse cell is two
    divisions -- and costs one read.
    """

    mask: np.ndarray
    transform: rasterio.Affine

    def window_mask(self, src, window) -> np.ndarray:
        """The mask resampled, nearest-neighbour, onto one window of ``src``."""
        fine = src.window_transform(window)
        rows = int(window.height)
        cols = int(window.width)
        # centres of the fine cells, in projected units
        xs = fine.c + (np.arange(cols) + 0.5) * fine.a
        ys = fine.f + (np.arange(rows) + 0.5) * fine.e
        cols_idx = np.floor((xs - self.transform.c) / self.transform.a).astype(np.int64)
        rows_idx = np.floor((ys - self.transform.f) / self.transform.e).astype(np.int64)
        inside_x = (cols_idx >= 0) & (cols_idx < self.mask.shape[1])
        inside_y = (rows_idx >= 0) & (rows_idx < self.mask.shape[0])
        np.clip(cols_idx, 0, self.mask.shape[1] - 1, out=cols_idx)
        np.clip(rows_idx, 0, self.mask.shape[0] - 1, out=rows_idx)
        out = self.mask[np.ix_(rows_idx, cols_idx)]
        # anything outside the mask's own footprint is not known to be sea
        return out & inside_y[:, None] & inside_x[None, :]


def load_coarse_sea_mask(sea_mask_path: Path, *, area_epsg: int = AREA_EPSG) -> CoarseSeaMask:
    """Read the sea mask once, reprojected to the equal-area CRS at its own scale."""
    with rasterio.open(sea_mask_path) as base:
        with WarpedVRT(base, crs=f"EPSG:{area_epsg}", resampling=Resampling.nearest) as vrt:
            return CoarseSeaMask(mask=vrt.read(1).astype(bool), transform=vrt.transform)


def _zonal_surface_water_for_geometry(occurrence_src, sea_mask: CoarseSeaMask, geom) -> GeometrySurfaceWaterStats:
    """Class cell counts for one geometry, with marine cells split out."""
    cell_area = _cell_area_m2(occurrence_src)
    empty = GeometrySurfaceWaterStats(0, 0, 0, 0, 0, cell_area)
    if geom is None or geom.is_empty:
        return empty
    try:
        window = geometry_window(occurrence_src, [mapping(geom)])
    except WindowError:
        return empty

    occurrence = occurrence_src.read(1, window=window, masked=False)
    if occurrence.size == 0:
        return empty
    sea = sea_mask.window_mask(occurrence_src, window)

    inside = geometry_mask(
        [mapping(geom)],
        out_shape=occurrence.shape,
        transform=occurrence_src.window_transform(window),
        invert=True,
        all_touched=False,
    )
    values = np.asarray(occurrence)
    # 255 means the pixel was never validly observed. It is neither water nor dry
    # land, so it is counted and reported but never folded into either class.
    nodata = values == NODATA_VALUE
    # The mask is coarse, so a sea-flagged cell covers a ~550 m block that may hold
    # dry land. Only cells that would otherwise have been classed as water count as
    # a marine removal: that is what the published column claims, and it leaves
    # coastal land as ordinary land rather than inflating the correction.
    wet = (values >= SEASONAL_MIN_OCCURRENCE) & (values <= MAX_VALID_OCCURRENCE)
    marine = inside & sea & wet
    inland = inside & ~sea & ~nodata
    permanent = inland & (values >= PERMANENT_MIN_OCCURRENCE) & (values <= MAX_VALID_OCCURRENCE)
    seasonal = (
        inland
        & (values >= SEASONAL_MIN_OCCURRENCE)
        & (values < PERMANENT_MIN_OCCURRENCE)
    )
    return GeometrySurfaceWaterStats(
        permanent_cells=int(np.count_nonzero(permanent)),
        seasonal_cells=int(np.count_nonzero(seasonal)),
        marine_cells=int(np.count_nonzero(marine)),
        nodata_cells=int(np.count_nonzero(inside & nodata)),
        support_cells=int(np.count_nonzero(inside)),
        cell_area_m2=cell_area,
    )


def aggregate_surface_water_to_admin_units(
    admin_gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
    raster_path: Path,
    sea_mask_path: Path,
    area_epsg: int = AREA_EPSG,
    progress_every: int = 0,
    tile_bounds: Optional[list[tuple[float, float, float, float]]] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate permanent and seasonal inland water onto canonical polygons."""
    if admin_gdf.empty:
        raise ValueError(f"No {level} boundaries were provided.")
    missing = [col for col in _identity_cols(level) + ["geometry"] if col not in admin_gdf.columns]
    if missing:
        raise ValueError(f"{level.title()} boundaries are missing required columns: {missing}")

    sea_mask = load_coarse_sea_mask(sea_mask_path, area_epsg=area_epsg)
    with rasterio.open(raster_path) as occurrence_base:
        if occurrence_base.crs is None:
            raise ValueError(f"GSW raster has no CRS: {raster_path}")
        with WarpedVRT(
            occurrence_base, crs=f"EPSG:{area_epsg}", resampling=Resampling.nearest
        ) as occurrence_src:
            admin_for_raster = admin_gdf.to_crs(occurrence_src.crs).copy()
            stats: list[GeometrySurfaceWaterStats] = []
            for position, geom in enumerate(admin_for_raster.geometry, start=1):
                stats.append(
                    _zonal_surface_water_for_geometry(occurrence_src, sea_mask, geom)
                )
                if progress_every and position % progress_every == 0:
                    print(f"    {level}: {position}/{len(admin_for_raster)}", flush=True)

    area_df = admin_gdf.to_crs(epsg=area_epsg).copy()
    out = admin_gdf[_identity_cols(level)].copy()
    out["polygon_area_km2"] = pd.to_numeric(area_df.geometry.area / 1_000_000.0, errors="coerce")
    out[_area_col(level)] = out["polygon_area_km2"]

    out["permanent_cell_count"] = [item.permanent_cells for item in stats]
    out["seasonal_cell_count"] = [item.seasonal_cells for item in stats]
    out[PERMANENT_AREA_COL] = [item.permanent_area_km2 for item in stats]
    out[SEASONAL_AREA_COL] = [item.seasonal_area_km2 for item in stats]

    polygon_area = pd.to_numeric(out["polygon_area_km2"], errors="coerce")
    for area_col, share_col in (
        (PERMANENT_AREA_COL, PERMANENT_SHARE_COL),
        (SEASONAL_AREA_COL, SEASONAL_SHARE_COL),
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

    # The marine correction is published, not hidden: a coastal unit's figure is a
    # residual after this much of its area was ruled sea, and a reader deciding how
    # far to trust it needs to see the size of the subtraction.
    out["marine_removed_area_km2"] = [item.marine_area_km2 for item in stats]
    out["marine_removed_pct_of_polygon"] = np.where(
        polygon_area.gt(0), out["marine_removed_area_km2"] / polygon_area * 100.0, np.nan
    )
    out["marine_corrected"] = pd.to_numeric(
        out["marine_removed_pct_of_polygon"], errors="coerce"
    ).ge(MARINE_CORRECTION_FLAG_PCT)

    out["nodata_cell_count"] = [item.nodata_cells for item in stats]
    out["nodata_pct_of_support"] = np.where(
        out["raster_extent_support_cell_count"] > 0,
        100.0 * out["nodata_cell_count"] / out["raster_extent_support_cell_count"],
        np.nan,
    )
    out["high_nodata"] = pd.to_numeric(out["nodata_pct_of_support"], errors="coerce").ge(
        HIGH_NODATA_FLAG_PCT
    )

    # Water cells are a subset of the support cells by construction; a violation means
    # the tabulation is wrong. Exceeding 100% of *polygon* area is instead an expected
    # discretisation effect for small units, so it is recorded rather than failed on.
    out["water_exceeds_support"] = (
        pd.to_numeric(out["permanent_cell_count"], errors="coerce").fillna(0)
        + pd.to_numeric(out["seasonal_cell_count"], errors="coerce").fillna(0)
    ).gt(pd.to_numeric(out["raster_extent_support_cell_count"], errors="coerce").fillna(0))
    combined_share = pd.to_numeric(out[PERMANENT_SHARE_COL], errors="coerce").fillna(
        0.0
    ) + pd.to_numeric(out[SEASONAL_SHARE_COL], errors="coerce").fillna(0.0)
    out["share_above_100_pct"] = combined_share.gt(SHARE_QUANTISATION_FLAG_PCT)
    out["share_out_of_range"] = combined_share.gt(SHARE_OUTLIER_MAX_PCT)

    master_df = out.rename(
        columns={"state_name": "state", "district_name": "district", "block_name": "block"}
    )
    master_df = master_df.sort_values(_output_key_cols(level)).reset_index(drop=True)

    qa_df = master_df.copy()
    qa_df["source_raster"] = str(raster_path)
    qa_df["sea_mask_raster"] = str(sea_mask_path)
    qa_df["centroid_inclusion_rule"] = "all_touched=False"
    qa_df["share_denominator"] = "polygon_area_epsg_6933"
    qa_df["permanent_threshold_occurrence_pct"] = PERMANENT_MIN_OCCURRENCE
    qa_df["seasonal_threshold_occurrence_pct"] = SEASONAL_MIN_OCCURRENCE
    return master_df, qa_df


# ---------------------------------------------------------------------------
# Guardrails and QA
# ---------------------------------------------------------------------------

def build_surface_water_national_summary(
    district_master_df: pd.DataFrame,
    block_master_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    def _sum(df: pd.DataFrame, col: str) -> float:
        return float(pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum())

    row = {
        "district_permanent_km2": _sum(district_master_df, PERMANENT_AREA_COL),
        "district_seasonal_km2": _sum(district_master_df, SEASONAL_AREA_COL),
        "district_marine_removed_km2": _sum(district_master_df, "marine_removed_area_km2"),
        "district_polygon_km2": _sum(district_master_df, "polygon_area_km2"),
    }
    if block_master_df is not None and not block_master_df.empty:
        row.update(
            {
                "block_permanent_km2": _sum(block_master_df, PERMANENT_AREA_COL),
                "block_seasonal_km2": _sum(block_master_df, SEASONAL_AREA_COL),
                "block_marine_removed_km2": _sum(block_master_df, "marine_removed_area_km2"),
            }
        )
    polygon = row["district_polygon_km2"]
    row["national_permanent_share_pct"] = (
        100.0 * row["district_permanent_km2"] / polygon if polygon > 0 else float("nan")
    )
    row["national_seasonal_share_pct"] = (
        100.0 * row["district_seasonal_km2"] / polygon if polygon > 0 else float("nan")
    )
    return pd.DataFrame([row])


def assert_surface_water_guardrails(
    *,
    district_master_df: pd.DataFrame,
    block_master_df: Optional[pd.DataFrame],
    national_summary_df: pd.DataFrame,
    value_histogram: dict[int, int],
    allow_incomplete_coverage: bool,
    allow_unexpected_values: bool,
    allow_total_outlier: bool,
    allow_share_outlier: bool,
) -> None:
    frames = [("district", district_master_df)]
    if block_master_df is not None and not block_master_df.empty:
        frames.append(("block", block_master_df))

    coverage_gaps: list[str] = []
    for level, frame in frames:
        coverage = pd.to_numeric(frame.get("tile_coverage_pct"), errors="coerce")
        if coverage is None or coverage.isna().all():
            continue
        bad = coverage.lt(MIN_TILE_COVERAGE_PCT)
        if bool(bad.any()):
            worst = frame.loc[bad].assign(_c=coverage[bad]).nsmallest(5, "_c")
            sample = [f"{r['state']}/{r['district']} {r['_c']:.1f}%" for _, r in worst.iterrows()]
            coverage_gaps.append(
                f"{level}: {int(bad.sum())} unit(s) below {MIN_TILE_COVERAGE_PCT}% tile "
                f"coverage, worst: {sample}"
            )
    if coverage_gaps and not allow_incomplete_coverage:
        raise ValueError(
            "GSW tile coverage is incomplete for some admin units. An uncovered area "
            "reads as occurrence 0 and would be published as a unit with no water:\n  - "
            + "\n  - ".join(coverage_gaps)
            + "\nDownload the missing tiles, or rerun with --allow-incomplete-coverage "
              "if the gap is genuinely outside India."
        )

    unexpected = {
        value: count
        for value, count in value_histogram.items()
        if not (0 <= value <= MAX_VALID_OCCURRENCE or value == NODATA_VALUE)
    }
    if unexpected and not allow_unexpected_values:
        raise ValueError(
            f"GSW occurrence contains values outside 0-{MAX_VALID_OCCURRENCE} and "
            f"{NODATA_VALUE}: {unexpected}. Recheck the band or rerun with "
            "--allow-unexpected-values."
        )

    total = float(national_summary_df.iloc[0]["district_permanent_km2"])
    if not allow_total_outlier and not (
        NATIONAL_PERMANENT_KM2_MIN <= total <= NATIONAL_PERMANENT_KM2_MAX
    ):
        raise ValueError(
            "GSW national permanent water total is outside the guardrail range "
            f"{NATIONAL_PERMANENT_KM2_MIN:,.0f}-{NATIONAL_PERMANENT_KM2_MAX:,.0f} km2: "
            f"{total:,.2f} km2. A total far above the range usually means the sea mask "
            "failed open. Recheck it, or rerun with --allow-total-outlier."
        )

    # Hard invariant: no tolerance, no override.
    invariant_breaks: list[str] = []
    for level, frame in frames:
        bad = frame["water_exceeds_support"].fillna(False).astype(bool)
        if bool(bad.any()):
            sample = frame.loc[bad, _output_key_cols(level)].head(5).to_dict(orient="records")
            invariant_breaks.append(f"{level}: {int(bad.sum())} unit(s), e.g. {sample}")
    if invariant_breaks:
        raise ValueError(
            "GSW water area exceeds the raster support area for some units, which "
            "cannot happen if the tabulation is correct:\n  - " + "\n  - ".join(invariant_breaks)
        )

    offenders: list[str] = []
    for level, frame in frames:
        bad = frame["share_out_of_range"].fillna(False).astype(bool)
        if bool(bad.any()):
            sample = frame.loc[bad, _output_key_cols(level)].head(5).to_dict(orient="records")
            offenders.append(
                f"{level}: {int(bad.sum())} unit(s) above {SHARE_OUTLIER_MAX_PCT}%, e.g. {sample}"
            )
    if offenders and not allow_share_outlier:
        raise ValueError(
            "GSW combined water share guardrail failed:\n  - " + "\n  - ".join(offenders)
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
        raise ValueError(f"Unsupported GSW metric slug: {metric_slug}")
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
            raise ValueError(f"GSW {level} master contains an empty state value.")
        _write_master_table(
            state_df.reset_index(drop=True),
            processed_root / state_label / out_name,
            overwrite=overwrite,
        )
        counts[state_label] = int(state_df.shape[0])
    return counts


def build_surface_water_admin_outputs(
    *,
    tile_dir: Path,
    vrt_path: Path,
    sea_mask_path: Path,
    states_path: Path,
    districts_path: Path,
    blocks_path: Path,
    qa_dir: Path,
    overwrite: bool,
    dry_run: bool,
    skip_blocks: bool = False,
    sea_mask_decimation: int = SEA_MASK_DECIMATION,
    sea_opening_cells: int = SEA_OPENING_CELLS,
    allow_incomplete_coverage: bool = False,
    allow_unexpected_values: bool = False,
    allow_total_outlier: bool = False,
    allow_share_outlier: bool = False,
    histogram_sample_step: int = 0,
    progress_every: int = 0,
) -> dict[str, object]:
    """Build GSW permanent and seasonal surface-water district and block masters."""
    vrt_info = build_tile_vrt(
        tile_dir,
        vrt_path,
        overwrite=overwrite,
        dry_run=dry_run,
        tile_glob=TILE_GLOB,
        product=PRODUCT_LABEL,
    )

    if dry_run and not vrt_path.exists():
        return {"vrt": vrt_info, "aggregated": False}

    sea, sea_transform, sea_stats = build_sea_mask(
        vrt_path,
        states_path,
        decimation=sea_mask_decimation,
        opening_cells=sea_opening_cells,
    )
    # The sea mask is a derived intermediate, not an output: it must always match the
    # tiles it was computed from, so a real run recomputes and rewrites it (about forty
    # seconds). A dry run writes it only when it is absent, because the validation pass
    # below cannot aggregate without it.
    if not dry_run or not sea_mask_path.exists():
        write_sea_mask_raster(sea, sea_transform, sea_mask_path, overwrite=True)

    value_histogram = (
        scan_class_histogram(vrt_path, sample_step=histogram_sample_step)
        if histogram_sample_step
        else {}
    )

    district_gdf = load_district_boundaries(districts_path)
    district_master_df, district_qa_df = aggregate_surface_water_to_admin_units(
        district_gdf,
        level="district",
        raster_path=vrt_path,
        sea_mask_path=sea_mask_path,
        progress_every=progress_every,
        tile_bounds=vrt_info["tile_bounds"],
    )

    block_master_df: Optional[pd.DataFrame] = None
    block_qa_df: Optional[pd.DataFrame] = None
    if not skip_blocks:
        block_gdf = load_block_boundaries(blocks_path)
        block_master_df, block_qa_df = aggregate_surface_water_to_admin_units(
            block_gdf,
            level="block",
            raster_path=vrt_path,
            sea_mask_path=sea_mask_path,
            progress_every=progress_every,
            tile_bounds=vrt_info["tile_bounds"],
        )

    national_summary_df = build_surface_water_national_summary(district_master_df, block_master_df)
    assert_surface_water_guardrails(
        district_master_df=district_master_df,
        block_master_df=block_master_df,
        national_summary_df=national_summary_df,
        value_histogram=value_histogram,
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
                metric_slug=slug,
                level="district",
                overwrite=overwrite,
            )
            if block_master_df is not None:
                block_counts = _write_state_slices(
                    metric_specific_master(block_master_df, level="block", metric_slug=slug),
                    metric_slug=slug,
                    level="block",
                    overwrite=overwrite,
                )
        _write_csv(district_qa_df, qa_dir / "gsw_district_master_qa.csv", overwrite=overwrite)
        if block_qa_df is not None:
            _write_csv(block_qa_df, qa_dir / "gsw_block_master_qa.csv", overwrite=overwrite)
        _write_csv(national_summary_df, qa_dir / "gsw_national_summary.csv", overwrite=overwrite)

    return {
        "vrt": vrt_info,
        "sea_mask": sea_stats | {"path": str(sea_mask_path)},
        "value_histogram": value_histogram,
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
        description="Build district and block inland surface-water masters from JRC GSW."
    )
    parser.add_argument("--tile-dir", type=str, default=str(_default_tile_dir()),
                        help="Directory holding the GSW occurrence GeoTIFF tiles.")
    parser.add_argument("--vrt", type=str, default="",
                        help=f"VRT mosaic path (default: <tile-dir>/{VRT_NAME}).")
    parser.add_argument("--sea-mask", type=str, default="",
                        help=f"Sea mask raster path (default: <tile-dir>/{SEA_MASK_NAME}).")
    parser.add_argument("--states", type=str, default=str(_default_states_path()),
                        help="State/UT boundaries used to seed the sea fill.")
    parser.add_argument("--districts", type=str, default=str(get_paths_config().districts_path))
    parser.add_argument("--blocks", type=str, default=str(get_paths_config().blocks_path))
    parser.add_argument("--qa-dir", type=str, default=str(_default_qa_dir()))
    parser.add_argument("--skip-blocks", action="store_true",
                        help="District level only. Useful for a fast first pass.")
    parser.add_argument("--sea-mask-decimation", type=int, default=SEA_MASK_DECIMATION,
                        help="Decimation factor for the coarse grid the sea mask is built on.")
    parser.add_argument("--sea-opening-cells", type=int, default=SEA_OPENING_CELLS,
                        help="Cells of binary opening before the sea fill is seeded. "
                             "0 swallows coastal lagoons; see the module docstring.")
    parser.add_argument("--histogram-sample-step", type=int, default=0,
                        help="Scan every Nth raster block for a value histogram (0 disables).")
    parser.add_argument("--progress-every", type=int, default=0,
                        help="Print progress every N units (0 disables).")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate tiles and report planned outputs without writing masters.")
    parser.add_argument("--allow-incomplete-coverage", action="store_true",
                        help="Allow admin units not fully inside the tile footprint union.")
    parser.add_argument("--allow-unexpected-values", action="store_true",
                        help="Allow occurrence values outside 0-100 and 255.")
    parser.add_argument("--allow-total-outlier", action="store_true",
                        help="Allow a national permanent-water total outside the guardrail range.")
    parser.add_argument("--allow-share-outlier", action="store_true",
                        help="Allow unit water shares above 101%%.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    tile_dir = Path(args.tile_dir).expanduser().resolve()
    vrt_path = Path(args.vrt).expanduser().resolve() if args.vrt else tile_dir / VRT_NAME
    sea_mask_path = (
        Path(args.sea_mask).expanduser().resolve() if args.sea_mask else tile_dir / SEA_MASK_NAME
    )
    states_path = Path(args.states).expanduser().resolve()
    districts_path = Path(args.districts).expanduser().resolve()
    blocks_path = Path(args.blocks).expanduser().resolve()
    qa_dir = Path(args.qa_dir).expanduser().resolve()

    if not tile_dir.exists():
        parser.error(f"Tile directory not found: {tile_dir}")
    if not states_path.exists():
        parser.error(f"State boundaries not found: {states_path}")
    if not districts_path.exists():
        parser.error(f"District boundaries not found: {districts_path}")
    if not args.skip_blocks and not blocks_path.exists():
        parser.error(f"Block boundaries not found: {blocks_path}")

    outputs = build_surface_water_admin_outputs(
        tile_dir=tile_dir,
        vrt_path=vrt_path,
        sea_mask_path=sea_mask_path,
        states_path=states_path,
        districts_path=districts_path,
        blocks_path=blocks_path,
        qa_dir=qa_dir,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        skip_blocks=bool(args.skip_blocks),
        sea_mask_decimation=int(args.sea_mask_decimation),
        sea_opening_cells=int(args.sea_opening_cells),
        allow_incomplete_coverage=bool(args.allow_incomplete_coverage),
        allow_unexpected_values=bool(args.allow_unexpected_values),
        allow_total_outlier=bool(args.allow_total_outlier),
        allow_share_outlier=bool(args.allow_share_outlier),
        histogram_sample_step=int(args.histogram_sample_step),
        progress_every=int(args.progress_every),
    )

    mode = "DRY RUN" if args.dry_run else "WROTE"
    vrt_info = outputs["vrt"]
    print(f"[{mode}] JRC Global Surface Water admin masters")
    print(f"  vrt: {vrt_info['tile_count']} tiles -> {vrt_info['path']}")
    print(f"       {vrt_info['width']} x {vrt_info['height']} px @ {vrt_info['resolution_deg']:.10f} deg")
    if not outputs.get("aggregated"):
        print("  VRT absent under --dry-run; skipped admin aggregation.")
        return 0

    sea = outputs["sea_mask"]
    print(f"  sea mask: {sea['grid_shape'][0]} x {sea['grid_shape'][1]} @ "
          f"{sea['resolution_deg']:.5f} deg, opening {sea['opening_cells']} cell(s)")
    print(f"            {sea['seed_component_count']:,} seeded of {sea['component_count']:,} "
          f"components; {sea['sea_pct_of_water']:.1f}% of permanent water ruled marine")

    if outputs.get("value_histogram"):
        histogram = outputs["value_histogram"]
        total = sum(histogram.values())
        water = sum(c for v, c in histogram.items() if PERMANENT_MIN_OCCURRENCE <= v <= MAX_VALID_OCCURRENCE)
        nodata = histogram.get(NODATA_VALUE, 0)
        print("  value histogram (sampled):")
        print(f"    occurrence >= {PERMANENT_MIN_OCCURRENCE}: {water:,} ({100.0 * water / total:.3f}%)")
        print(f"    no observation ({NODATA_VALUE}): {nodata:,} ({100.0 * nodata / total:.3f}%)")

    summary = outputs["national_summary_df"].iloc[0]
    print(f"  districts: {len(outputs['district_master_df']):,}")
    if outputs["block_master_df"] is not None:
        print(f"  blocks   : {len(outputs['block_master_df']):,}")
    print(f"  national permanent water = {summary['district_permanent_km2']:,.0f} km2 "
          f"({summary['national_permanent_share_pct']:.2f}% of land area)")
    print(f"  national seasonal water  = {summary['district_seasonal_km2']:,.0f} km2 "
          f"({summary['national_seasonal_share_pct']:.2f}% of land area)")
    print(f"  marine water removed     = {summary['district_marine_removed_km2']:,.0f} km2")
    print("  metric slugs: " + ", ".join(sorted(METRIC_COLUMNS)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

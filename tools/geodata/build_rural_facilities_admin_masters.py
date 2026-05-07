#!/usr/bin/env python3
"""Build rural facilities exposure masters and density overlay artifacts."""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from affine import Affine
from PIL import Image
from rasterio.features import MergeAlg, rasterize
from rasterio.transform import from_bounds
from rasterio.warp import Resampling, calculate_default_transform, reproject, transform_bounds
from shapely.geometry import Point, box, mapping
from shapely.ops import unary_union

from paths import get_master_csv_filename, get_paths_config, resolve_processed_root
from india_resilience_tool.utils.naming import normalize_compact
from tools.geodata.build_district_subbasin_crosswalk import (
    load_block_boundaries,
    load_district_boundaries,
)


AdminLevel = Literal["district", "block"]

SNAPSHOT_SCENARIO = "snapshot"
SNAPSHOT_PERIOD = "2019-2021"
POPULATION_DENOMINATOR_COL = "population_total__snapshot__2025__mean"
GRID_CRS = "EPSG:6933"
IMAGE_CRS = "EPSG:3857"
GRID_CELL_SIZE_M = 10_000
OVERLAY_MAX_DIMENSION = 4096
RURAL_FACILITIES_OVERLAY_ID = "rural_facilities_density"
SOURCE_BASENAMES: dict[str, str] = {
    "Agroinfrastructure.shp": "agro",
    "Educationinfrastructure.shp": "education",
    "Healthinfrastructure.shp": "health",
    "Serviceinfrastructure.shp": "service",
}
REQUIRED_SOURCE_COLUMNS = {
    "facilityna",
    "facilityca",
    "facilitysu",
    "habitation",
    "habitati_1",
    "address",
    "block",
    "district",
    "state",
    "fileupload",
    "lattitude",
    "longitude",
}
FACILITY_CATEGORIES: tuple[str, ...] = ("total", "agro", "education", "health", "service")
FACILITY_OVERLAY_CATEGORIES: tuple[str, ...] = ("agro", "education", "health", "service")
COUNT_METRIC_SLUGS: dict[str, str] = {
    "total": "rural_facilities_total_count",
    "agro": "rural_facilities_agro_count",
    "education": "rural_facilities_education_count",
    "health": "rural_facilities_health_count",
    "service": "rural_facilities_service_count",
}
RATE_METRIC_SLUGS: dict[str, str] = {
    category: f"{slug}_per_100k" for category, slug in COUNT_METRIC_SLUGS.items()
}
RURAL_FACILITIES_METRIC_SLUGS: tuple[str, ...] = tuple(COUNT_METRIC_SLUGS.values()) + tuple(RATE_METRIC_SLUGS.values())
def _build_ramp(hexes: tuple[str, str, str, str, str, str, str, str]) -> list[dict[str, object]]:
    """Build the canonical 9-row density ramp from 8 single-hue stops (light to dark)."""
    edges: list[tuple[Optional[float], Optional[float]]] = [
        (0.0, 1.0),
        (1.0, 5.0),
        (5.0, 10.0),
        (10.0, 25.0),
        (25.0, 50.0),
        (50.0, 100.0),
        (100.0, 250.0),
        (250.0, None),
    ]
    ramp: list[dict[str, object]] = [
        {"min_value_exclusive": None, "max_value_inclusive": 0.0, "color_hex": None, "transparent": True}
    ]
    for (lo, hi), color in zip(edges, hexes):
        ramp.append(
            {
                "min_value_exclusive": lo,
                "max_value_inclusive": hi,
                "color_hex": color,
                "transparent": False,
            }
        )
    return ramp


RURAL_FACILITIES_COLOR_RAMPS: dict[str, list[dict[str, object]]] = {
    "agro": _build_ramp(
        ("#f0fdf4", "#dcfce7", "#bbf7d0", "#86efac", "#4ade80", "#22c55e", "#16a34a", "#14532d")
    ),
    "education": _build_ramp(
        ("#eff6ff", "#dbeafe", "#bfdbfe", "#93c5fd", "#60a5fa", "#3b82f6", "#1d4ed8", "#1e3a8a")
    ),
    "health": _build_ramp(
        ("#fef2f2", "#fee2e2", "#fecaca", "#fca5a5", "#f87171", "#ef4444", "#dc2626", "#7f1d1d")
    ),
    "service": _build_ramp(
        ("#fff7ed", "#ffedd5", "#fed7aa", "#fdba74", "#fb923c", "#f97316", "#ea580c", "#7c2d12")
    ),
}


@dataclass(frozen=True)
class RuralFacilitiesOutputs:
    """Resolved rural facilities outputs from one build invocation."""

    normalized_points: gpd.GeoDataFrame
    assigned_points: gpd.GeoDataFrame
    district_master: pd.DataFrame
    block_master: pd.DataFrame
    planned_paths: tuple[Path, ...]


def _default_source_dir() -> Path:
    return (
        get_paths_config().data_dir
        / "Ruralfacilties_4files-20260423T052127Z-3-001"
        / "Ruralfacilties_4files"
    )


def _default_qa_dir() -> Path:
    return get_paths_config().data_dir / "rural_facilities"


def _default_overlay_dir() -> Path:
    return get_paths_config().data_dir / "rural_facilities" / "overlay"


def _metric_col(slug: str) -> str:
    return f"{slug}__{SNAPSHOT_SCENARIO}__{SNAPSHOT_PERIOD}__mean"


def rural_facilities_overlay_paths(overlay_dir: Path, category: str) -> tuple[Path, Path]:
    """Return canonical PNG and metadata paths for one rural facilities category."""
    category_key = _validate_category(category)
    return (
        overlay_dir / f"rural_facilities_density_{category_key}_overlay.png",
        overlay_dir / f"rural_facilities_density_{category_key}_overlay_meta.json",
    )


def _validate_category(category: str) -> str:
    value = str(category or "").strip().lower()
    if value not in FACILITY_OVERLAY_CATEGORIES:
        raise ValueError(
            f"Unsupported rural facilities overlay category {category!r}; "
            f"expected one of {FACILITY_OVERLAY_CATEGORIES}."
        )
    return value


def _normalize_source_columns(gdf: gpd.GeoDataFrame, *, source_name: str) -> gpd.GeoDataFrame:
    out = gdf.copy()
    out.columns = [str(col).strip().lower() for col in out.columns]
    missing = sorted(REQUIRED_SOURCE_COLUMNS.difference(set(out.columns)))
    if missing:
        raise ValueError(f"{source_name} is missing required rural facilities fields: {missing}")
    return out


def load_rural_facilities_sources(source_dir: Path) -> gpd.GeoDataFrame:
    """Load and validate the four rural facilities source shapefiles."""
    shp_paths = sorted(source_dir.glob("*.shp"))
    found = {path.name for path in shp_paths}
    expected = set(SOURCE_BASENAMES)
    unknown = sorted(found.difference(expected))
    missing = sorted(expected.difference(found))
    if unknown:
        raise ValueError(f"Unknown rural facilities shapefile(s): {unknown}. Expected only: {sorted(expected)}")
    if missing:
        raise FileNotFoundError(f"Missing rural facilities shapefile(s): {missing}")

    frames: list[gpd.GeoDataFrame] = []
    for path in shp_paths:
        family = SOURCE_BASENAMES[path.name]
        frame = _normalize_source_columns(gpd.read_file(path), source_name=path.name)
        frame["source_shapefile_name"] = path.name
        frame["facility_family"] = family
        frame["source_row_id"] = np.arange(frame.shape[0], dtype=int)
        frames.append(frame)
    if not frames:
        raise ValueError(f"No rural facilities shapefiles found in {source_dir}")
    combined = pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry=combined.geometry if "geometry" in combined else None, crs=frames[0].crs)


def normalize_rural_facility_points(raw_gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """Parse WGS84 coordinates and split invalid coordinates into QA."""
    out = raw_gdf.copy()
    out["longitude_num"] = pd.to_numeric(out["longitude"], errors="coerce")
    out["latitude_num"] = pd.to_numeric(out["lattitude"], errors="coerce")
    out["fileupload_parsed"] = pd.to_datetime(out["fileupload"], errors="coerce")
    valid = (
        np.isfinite(out["longitude_num"])
        & np.isfinite(out["latitude_num"])
        & out["longitude_num"].between(68.0, 98.0)
        & out["latitude_num"].between(6.0, 38.0)
    )
    invalid = out.loc[~valid].drop(columns=["geometry"], errors="ignore").copy()
    points = out.loc[valid].copy()
    points["geometry"] = [Point(xy) for xy in zip(points["longitude_num"], points["latitude_num"])]
    points = gpd.GeoDataFrame(points, geometry="geometry", crs="EPSG:4326")
    return points.reset_index(drop=True), invalid.reset_index(drop=True)


def assign_points_to_blocks(
    points_gdf: gpd.GeoDataFrame,
    block_gdf: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame, pd.DataFrame]:
    """Assign points to exactly one covering canonical block; unmatched/ambiguous are QA only."""
    if points_gdf.empty:
        assigned_cols = list(points_gdf.columns) + ["state_name", "district_name", "block_name", "block_key"]
        return gpd.GeoDataFrame(columns=assigned_cols, geometry="geometry", crs="EPSG:4326"), pd.DataFrame(), pd.DataFrame()
    if str(points_gdf.crs) != "EPSG:4326":
        points_gdf = points_gdf.to_crs(4326)
    if str(block_gdf.crs) != "EPSG:4326":
        block_gdf = block_gdf.to_crs(4326)

    joined = gpd.sjoin(
        points_gdf.reset_index(names="point_index"),
        block_gdf[["state_name", "district_name", "block_name", "block_key", "geometry"]],
        how="left",
        predicate="covered_by",
    )
    hit_counts = joined.groupby("point_index", dropna=False)["block_key"].nunique(dropna=True)
    assigned_index = set(hit_counts[hit_counts == 1].index.tolist())
    ambiguous_index = set(hit_counts[hit_counts > 1].index.tolist())
    all_index = set(points_gdf.reset_index().index.tolist())
    unmatched_index = all_index.difference(assigned_index).difference(ambiguous_index)

    assigned = joined[joined["point_index"].isin(assigned_index)].copy()
    assigned = assigned.drop_duplicates(subset=["point_index"]).drop(columns=["index_right"], errors="ignore")
    ambiguous = joined[joined["point_index"].isin(ambiguous_index)].drop(columns=["geometry"], errors="ignore").copy()
    unmatched = points_gdf.reset_index(names="point_index")
    unmatched = unmatched[unmatched["point_index"].isin(unmatched_index)].drop(columns=["geometry"], errors="ignore").copy()
    return (
        gpd.GeoDataFrame(assigned, geometry="geometry", crs="EPSG:4326").reset_index(drop=True),
        unmatched.reset_index(drop=True),
        ambiguous.reset_index(drop=True),
    )


def _load_population_denominators(
    *,
    level: AdminLevel,
    states: set[str],
    data_dir: Path,
) -> pd.DataFrame:
    filename = get_master_csv_filename(level)
    root = resolve_processed_root("population_total", data_dir=data_dir, mode="portfolio")
    state_dirs = [path for path in root.iterdir() if path.is_dir()] if root.exists() else []
    dirs_by_token: dict[str, list[Path]] = {}
    for path in state_dirs:
        dirs_by_token.setdefault(normalize_compact(path.name), []).append(path)

    def _resolve_state_file(state: str) -> Path:
        direct_dir = root / state
        candidate_dirs = [direct_dir]
        token = normalize_compact(state)
        candidate_dirs.extend(dirs_by_token.get(token, []))
        candidate_dirs.extend(
            path
            for path in state_dirs
            if normalize_compact(path.name).startswith(token) or token.startswith(normalize_compact(path.name))
        )
        seen: set[Path] = set()
        for directory in candidate_dirs:
            if directory in seen:
                continue
            seen.add(directory)
            csv_path = directory / filename
            parquet_path = csv_path.with_suffix(".parquet")
            if parquet_path.exists():
                return parquet_path
            if csv_path.exists():
                return csv_path
        return direct_dir / filename

    frames: list[pd.DataFrame] = []
    missing: list[str] = []
    for state in sorted(states):
        path = _resolve_state_file(state)
        if not path.exists():
            missing.append(str(root / state / filename))
            continue
        df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
        if POPULATION_DENOMINATOR_COL not in df.columns:
            raise ValueError(f"Population denominator column {POPULATION_DENOMINATOR_COL} missing from {path}")
        frames.append(df)
    if missing:
        raise FileNotFoundError("Missing population denominator master(s): " + ", ".join(missing))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _counts_by_admin(
    assigned: pd.DataFrame,
    admin_gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
) -> pd.DataFrame:
    if level == "block":
        id_cols = ["state_name", "district_name", "block_name", "block_key"]
        group_cols = ["state_name", "district_name", "block_name", "block_key"]
    else:
        id_cols = ["state_name", "district_name", "district_key"]
        group_cols = ["state_name", "district_name"]
    out = admin_gdf[id_cols].copy()
    if assigned.empty:
        counts = pd.DataFrame(columns=group_cols + list(COUNT_METRIC_SLUGS.values()))
    else:
        counts = assigned.groupby(group_cols + ["facility_family"], dropna=False).size().unstack(fill_value=0)
        counts = counts.rename(columns={family: COUNT_METRIC_SLUGS[family] for family in SOURCE_BASENAMES.values()})
        counts[COUNT_METRIC_SLUGS["total"]] = counts.sum(axis=1)
        counts = counts.reset_index()
    out = out.merge(counts, on=group_cols, how="left")
    for slug in COUNT_METRIC_SLUGS.values():
        if slug not in out.columns:
            out[slug] = 0
        out[slug] = pd.to_numeric(out[slug], errors="coerce").fillna(0).astype(int)
    return out


def _district_counts_from_block_counts(block_counts: pd.DataFrame, district_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Roll block-level rural facility counts up to canonical district rows."""
    id_cols = ["state_name", "district_name", "district_key"]
    out = district_gdf[id_cols].copy()
    out["_state_token"] = out["state_name"].map(normalize_compact)
    out["_district_token"] = out["district_name"].map(normalize_compact)

    if block_counts.empty:
        counts = pd.DataFrame(columns=["_state_token", "_district_token", *COUNT_METRIC_SLUGS.values()])
    else:
        block_values = block_counts.copy()
        block_values["_state_token"] = block_values["state_name"].map(normalize_compact)
        block_values["_district_token"] = block_values["district_name"].map(normalize_compact)
        counts = (
            block_values.groupby(["_state_token", "_district_token"], as_index=False)[list(COUNT_METRIC_SLUGS.values())]
            .sum()
        )

    out = out.merge(counts, on=["_state_token", "_district_token"], how="left")
    out = out.drop(columns=["_state_token", "_district_token"])
    for slug in COUNT_METRIC_SLUGS.values():
        if slug not in out.columns:
            out[slug] = 0
        out[slug] = pd.to_numeric(out[slug], errors="coerce").fillna(0).astype(int)
    return out


def _add_denominator_rates(
    counts_df: pd.DataFrame,
    denominator_df: pd.DataFrame,
    *,
    level: AdminLevel,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    join_cols = ["state", "district", "block"] if level == "block" else ["state", "district"]
    out = counts_df.rename(columns={"state_name": "state", "district_name": "district", "block_name": "block"}).copy()
    denom_keep = join_cols + [POPULATION_DENOMINATOR_COL]
    merged = out.merge(denominator_df[denom_keep], on=join_cols, how="left")
    population = pd.to_numeric(merged[POPULATION_DENOMINATOR_COL], errors="coerce")
    valid_pop = np.isfinite(population) & population.gt(0)
    for category, count_slug in COUNT_METRIC_SLUGS.items():
        rate_slug = RATE_METRIC_SLUGS[category]
        counts = pd.to_numeric(merged[count_slug], errors="coerce").fillna(0.0)
        merged[rate_slug] = np.where(valid_pop, counts / population * 100_000.0, np.nan)
    issues = merged.loc[~valid_pop, join_cols + [POPULATION_DENOMINATOR_COL]].copy()
    issues["level"] = level
    issues["issue"] = "population denominator missing, non-finite, or <= 0"
    return merged, issues


def _wide_master_for_metric(df: pd.DataFrame, *, level: AdminLevel, slug: str) -> pd.DataFrame:
    id_cols = ["state", "district", "block", "block_key"] if level == "block" else ["state", "district", "district_key"]
    value_col = _metric_col(slug)
    out = df[id_cols].copy()
    out[value_col] = df[slug]
    return out


def _planned_master_paths(master_df: pd.DataFrame, *, level: AdminLevel, slug: str) -> list[Path]:
    filename = get_master_csv_filename(level)
    root = resolve_processed_root(slug, data_dir=get_paths_config().data_dir, mode="portfolio")
    paths: list[Path] = []
    for state in sorted({str(v).strip() for v in master_df["state"].dropna().tolist()}):
        csv_path = root / state / filename
        paths.extend([csv_path, csv_path.with_suffix(".parquet")])
    return paths


def _write_master_table(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    parquet_path = path.with_suffix(".parquet")
    existing = [str(p) for p in (path, parquet_path) if p.exists()]
    if existing and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing rural facilities master without --overwrite: {', '.join(existing)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    df.to_parquet(parquet_path, index=False)


def _write_state_slices(master_df: pd.DataFrame, *, level: AdminLevel, slug: str, overwrite: bool) -> None:
    filename = get_master_csv_filename(level)
    root = resolve_processed_root(slug, data_dir=get_paths_config().data_dir, mode="portfolio")
    for state, state_df in master_df.groupby("state", dropna=False):
        state_label = str(state or "").strip()
        if not state_label:
            raise ValueError(f"Rural facilities {level} master contains an empty state value.")
        _write_master_table(state_df.reset_index(drop=True), root / state_label / filename, overwrite=overwrite)


def _hex_to_rgba(color_hex: str, alpha: int = 255) -> tuple[int, int, int, int]:
    value = color_hex.lstrip("#")
    return (int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16), int(alpha))


def _density_rgba(data: np.ndarray, *, category: str) -> np.ndarray:
    """Render the density grid into RGBA using the per-category color ramp."""
    if category not in RURAL_FACILITIES_COLOR_RAMPS:
        raise ValueError(
            f"Unknown rural facilities overlay category {category!r}; "
            f"expected one of {tuple(RURAL_FACILITIES_COLOR_RAMPS.keys())}."
        )
    ramp = RURAL_FACILITIES_COLOR_RAMPS[category]
    bin_edges: list[tuple[float, float, str]] = []
    for stop in ramp:
        if stop.get("transparent"):
            continue
        lo = stop.get("min_value_exclusive")
        hi = stop.get("max_value_inclusive")
        color_hex = str(stop.get("color_hex") or "")
        if not color_hex:
            continue
        lo_value = -math.inf if lo is None else float(lo)
        hi_value = math.inf if hi is None else float(hi)
        bin_edges.append((lo_value, hi_value, color_hex))

    rgba = np.zeros((data.shape[0], data.shape[1], 4), dtype=np.uint8)
    finite = np.isfinite(data)
    for lo_value, hi_value, color_hex in bin_edges:
        if hi_value == math.inf:
            mask = finite & (data > lo_value)
        else:
            mask = finite & (data > lo_value) & (data <= hi_value)
        rgba[mask] = _hex_to_rgba(color_hex)
    return rgba


def _snapped_bounds(bounds: tuple[float, float, float, float], cell_size: int) -> tuple[float, float, float, float]:
    west, south, east, north = bounds
    return (
        math.floor(west / cell_size) * cell_size,
        math.floor(south / cell_size) * cell_size,
        math.ceil(east / cell_size) * cell_size,
        math.ceil(north / cell_size) * cell_size,
    )


def export_rural_facilities_density_overlays(
    *,
    assigned_points: gpd.GeoDataFrame,
    district_gdf: gpd.GeoDataFrame,
    overlay_dir: Path,
    source_names: list[str],
    source_summary: pd.DataFrame,
    assignment_summary: dict[str, int],
    overwrite: bool,
    dry_run: bool,
) -> list[dict[str, object]]:
    """Export all five rural facilities density PNG/metadata overlay pairs."""
    union_geom = unary_union(list(district_gdf.to_crs(GRID_CRS).geometry))
    west, south, east, north = _snapped_bounds(tuple(union_geom.bounds), GRID_CELL_SIZE_M)
    width = max(1, int(round((east - west) / GRID_CELL_SIZE_M)))
    height = max(1, int(round((north - south) / GRID_CELL_SIZE_M)))
    transform = from_bounds(west, south, east, north, width, height)
    union_raster = rasterize(
        [(mapping(union_geom), 1)],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype="uint8",
        all_touched=True,
    )
    grid_area_km2 = (GRID_CELL_SIZE_M * GRID_CELL_SIZE_M) / 1_000_000.0
    effective_area_km2 = np.where(union_raster > 0, grid_area_km2, 0.0).astype("float32")
    outputs: list[dict[str, object]] = []
    points_6933 = assigned_points.to_crs(GRID_CRS) if not assigned_points.empty else assigned_points

    for category in FACILITY_OVERLAY_CATEGORIES:
        png_path, meta_path = rural_facilities_overlay_paths(overlay_dir, category)
        if not dry_run and not overwrite:
            existing = [str(path) for path in (png_path, meta_path) if path.exists()]
            if existing:
                raise FileExistsError(f"Refusing to overwrite rural facilities overlay without --overwrite: {', '.join(existing)}")
        category_points = points_6933[points_6933["facility_family"] == category]
        shapes = [(mapping(geom), 1) for geom in category_points.geometry if geom is not None and not geom.is_empty]
        counts = rasterize(
            shapes,
            out_shape=(height, width),
            transform=transform,
            fill=0,
            dtype="uint16",
            merge_alg=MergeAlg.add,
        ) if shapes else np.zeros((height, width), dtype="uint16")
        density = np.full((height, width), np.nan, dtype=np.float32)
        density_mask = (effective_area_km2 > 0) & (counts > 0)
        np.divide(
            counts,
            effective_area_km2 / 1000.0,
            out=density,
            where=density_mask,
            casting="unsafe",
        )

        dst_transform, dst_width, dst_height = calculate_default_transform(
            GRID_CRS, IMAGE_CRS, width, height, west, south, east, north
        )
        max_dim = max(int(dst_width), int(dst_height))
        if max_dim > OVERLAY_MAX_DIMENSION:
            scale = max_dim / float(OVERLAY_MAX_DIMENSION)
            target_width = max(1, int(round(dst_width / scale)))
            target_height = max(1, int(round(dst_height / scale)))
            dst_transform = dst_transform * Affine.scale(dst_width / float(target_width), dst_height / float(target_height))
            dst_width, dst_height = target_width, target_height
        dst = np.full((int(dst_height), int(dst_width)), np.nan, dtype=np.float32)
        reproject(
            source=density,
            destination=dst,
            src_transform=transform,
            src_crs=GRID_CRS,
            src_nodata=np.nan,
            dst_transform=dst_transform,
            dst_crs=IMAGE_CRS,
            dst_nodata=np.nan,
            resampling=Resampling.nearest,
        )
        merc_west = float(dst_transform.c)
        merc_east = float(dst_transform.c + dst_transform.a * int(dst_width))
        merc_north = float(dst_transform.f)
        merc_south = float(dst_transform.f + dst_transform.e * int(dst_height))
        wgs84_left, wgs84_bottom, wgs84_right, wgs84_top = transform_bounds(
            IMAGE_CRS, "EPSG:4326", merc_west, merc_south, merc_east, merc_north
        )
        positive = np.isfinite(dst) & (dst > 0.0)
        source_positive_max = float(np.nanmax(dst[positive])) if np.any(positive) else 0.0
        metadata = {
            "overlay_id": RURAL_FACILITIES_OVERLAY_ID,
            "category": category,
            "source_shapefile_names": source_names,
            "snapshot_period": SNAPSHOT_PERIOD,
            "display_units": "facilities per 1,000 km2",
            "display_transform": "assigned_points_per_effective_area_1000km2",
            "grid_crs": GRID_CRS,
            "grid_cell_size_m": GRID_CELL_SIZE_M,
            "image_crs": IMAGE_CRS,
            "bounds_latlon": [[round(wgs84_bottom, 6), round(wgs84_left, 6)], [round(wgs84_top, 6), round(wgs84_right, 6)]],
            "width_px": int(dst_width),
            "height_px": int(dst_height),
            "color_ramp": [dict(item) for item in RURAL_FACILITIES_COLOR_RAMPS[category]],
            "source_row_counts": source_summary.to_dict(orient="records"),
            "valid_coordinate_count": int(assignment_summary.get("valid_coordinate_count", 0)),
            "assigned_count": int(assignment_summary.get("assigned_count", 0)),
            "unmatched_count": int(assignment_summary.get("unmatched_count", 0)),
            "ambiguous_count": int(assignment_summary.get("ambiguous_count", 0)),
            "source_positive_max": source_positive_max,
            "clipped_above_display_max": bool(source_positive_max > 250.0),
        }
        if not dry_run:
            overlay_dir.mkdir(parents=True, exist_ok=True)
            Image.fromarray(_density_rgba(dst, category=category), mode="RGBA").save(png_path)
            meta_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        outputs.append({"category": category, "png_path": png_path, "meta_path": meta_path, "metadata": metadata})
    return outputs


def _duplicate_candidates(points: pd.DataFrame) -> pd.DataFrame:
    cols = ["facility_family", "facilityna", "longitude_num", "latitude_num"]
    if points.empty:
        return pd.DataFrame(columns=cols + ["duplicate_count"])
    dup = points.groupby(cols, dropna=False).size().reset_index(name="duplicate_count")
    return dup[dup["duplicate_count"] > 1].reset_index(drop=True)


def _source_summary(raw: pd.DataFrame, invalid: pd.DataFrame) -> pd.DataFrame:
    total = raw.groupby(["source_shapefile_name", "facility_family"], as_index=False).size().rename(columns={"size": "source_rows"})
    bad = invalid.groupby(["source_shapefile_name", "facility_family"], as_index=False).size().rename(columns={"size": "invalid_coordinate_rows"})
    return total.merge(bad, on=["source_shapefile_name", "facility_family"], how="left").fillna({"invalid_coordinate_rows": 0})


def _district_consistency(district_master: pd.DataFrame, block_master: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for slug in COUNT_METRIC_SLUGS.values():
        d = district_master[["state", "district", slug]].rename(columns={slug: "district_value"}).copy()
        d["_state_token"] = d["state"].map(normalize_compact)
        d["_district_token"] = d["district"].map(normalize_compact)
        b = block_master.copy()
        b["_state_token"] = b["state"].map(normalize_compact)
        b["_district_token"] = b["district"].map(normalize_compact)
        b = (
            b.groupby(["_state_token", "_district_token"], as_index=False)[slug]
            .sum()
            .rename(columns={slug: "sum_block_value"})
        )
        merged = d.merge(b, on=["_state_token", "_district_token"], how="left")
        merged = merged.drop(columns=["_state_token", "_district_token"])
        merged["metric_slug"] = slug
        merged["difference"] = merged["district_value"] - merged["sum_block_value"].fillna(0)
        rows.extend(merged.to_dict(orient="records"))
    return pd.DataFrame(rows)


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing rural facilities QA without --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _qa_parquet_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return a Parquet-safe QA frame with mixed object columns coerced to strings."""
    out = df.copy()
    for col in out.select_dtypes(include=["object"]).columns:
        out[col] = out[col].astype("string")
    for col in out.select_dtypes(include=["datetimetz", "datetime64[ns]"]).columns:
        out[col] = out[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
    return out


def build_rural_facilities_admin_outputs(
    *,
    source_dir: Path,
    districts_path: Path,
    blocks_path: Path,
    qa_dir: Path,
    overlay_dir: Path,
    overwrite: bool,
    dry_run: bool,
) -> RuralFacilitiesOutputs:
    """Build rural facilities district/block masters, QA tables, and overlays."""
    raw = load_rural_facilities_sources(source_dir)
    districts = load_district_boundaries(districts_path)
    blocks = load_block_boundaries(blocks_path)
    states = set(str(v).strip() for v in blocks["state_name"].dropna().tolist())
    # Denominators are validated before any write side effects.
    block_denoms = _load_population_denominators(level="block", states=states, data_dir=get_paths_config().data_dir)
    district_denoms = _load_population_denominators(level="district", states=states, data_dir=get_paths_config().data_dir)

    points, invalid = normalize_rural_facility_points(raw)
    assigned, unmatched, ambiguous = assign_points_to_blocks(points, blocks)
    block_counts = _counts_by_admin(assigned, blocks, level="block")
    district_counts = _district_counts_from_block_counts(block_counts, districts)
    block_full, block_pop_issues = _add_denominator_rates(block_counts, block_denoms, level="block")
    district_full, district_pop_issues = _add_denominator_rates(district_counts, district_denoms, level="district")

    block_masters = {
        slug: _wide_master_for_metric(block_full, level="block", slug=slug)
        for slug in RURAL_FACILITIES_METRIC_SLUGS
    }
    district_masters = {
        slug: _wide_master_for_metric(district_full, level="district", slug=slug)
        for slug in RURAL_FACILITIES_METRIC_SLUGS
    }
    planned_paths: list[Path] = []
    for slug in RURAL_FACILITIES_METRIC_SLUGS:
        planned_paths.extend(_planned_master_paths(block_masters[slug], level="block", slug=slug))
        planned_paths.extend(_planned_master_paths(district_masters[slug], level="district", slug=slug))
    for category in FACILITY_OVERLAY_CATEGORIES:
        planned_paths.extend(rural_facilities_overlay_paths(overlay_dir, category))

    source_summary = _source_summary(raw, invalid)
    assignment_summary = {
        "valid_coordinate_count": int(points.shape[0]),
        "assigned_count": int(assigned.shape[0]),
        "unmatched_count": int(unmatched.shape[0]),
        "ambiguous_count": int(ambiguous["point_index"].nunique() if not ambiguous.empty else 0),
    }
    overlay_outputs = export_rural_facilities_density_overlays(
        assigned_points=assigned,
        district_gdf=districts,
        overlay_dir=overlay_dir,
        source_names=sorted(SOURCE_BASENAMES),
        source_summary=source_summary,
        assignment_summary=assignment_summary,
        overwrite=overwrite,
        dry_run=dry_run,
    )

    if not dry_run:
        for slug, master in block_masters.items():
            _write_state_slices(master, level="block", slug=slug, overwrite=overwrite)
        for slug, master in district_masters.items():
            _write_state_slices(master, level="district", slug=slug, overwrite=overwrite)
        qa_dir.mkdir(parents=True, exist_ok=True)
        _qa_parquet_frame(points.drop(columns=["geometry"], errors="ignore")).to_parquet(
            qa_dir / "rural_facilities_normalized_points.parquet",
            index=False,
        )
        _write_csv(invalid, qa_dir / "rural_facilities_invalid_coordinates.csv", overwrite=overwrite)
        _write_csv(unmatched, qa_dir / "rural_facilities_unmatched_points.csv", overwrite=overwrite)
        _write_csv(ambiguous, qa_dir / "rural_facilities_ambiguous_assignments.csv", overwrite=overwrite)
        _write_csv(pd.concat([district_pop_issues, block_pop_issues], ignore_index=True), qa_dir / "rural_facilities_population_denominator_issues.csv", overwrite=overwrite)
        _write_csv(_duplicate_candidates(points), qa_dir / "rural_facilities_duplicate_candidates.csv", overwrite=overwrite)
        _write_csv(source_summary, qa_dir / "rural_facilities_family_source_summary.csv", overwrite=overwrite)
        _write_csv(_district_consistency(district_full, block_full), qa_dir / "rural_facilities_district_vs_block_consistency.csv", overwrite=overwrite)
        _write_csv(pd.DataFrame([item["metadata"] for item in overlay_outputs]), qa_dir / "rural_facilities_overlay_summary.csv", overwrite=overwrite)

    return RuralFacilitiesOutputs(
        normalized_points=points,
        assigned_points=assigned,
        district_master=district_full,
        block_master=block_full,
        planned_paths=tuple(planned_paths),
    )


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build rural facilities exposure admin masters and density overlays.")
    parser.add_argument("--source-dir", default=str(_default_source_dir()))
    parser.add_argument("--districts", default=str(get_paths_config().districts_path))
    parser.add_argument("--blocks", default=str(get_paths_config().blocks_path))
    parser.add_argument("--qa-dir", default=str(_default_qa_dir()))
    parser.add_argument("--overlay-dir", default=str(_default_overlay_dir()))
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Compute summaries and planned paths without writing files.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)
    source_dir = Path(args.source_dir).expanduser().resolve()
    districts_path = Path(args.districts).expanduser().resolve()
    blocks_path = Path(args.blocks).expanduser().resolve()
    qa_dir = Path(args.qa_dir).expanduser().resolve()
    overlay_dir = Path(args.overlay_dir).expanduser().resolve()
    if not source_dir.exists():
        raise FileNotFoundError(f"Rural facilities source directory not found: {source_dir}")
    outputs = build_rural_facilities_admin_outputs(
        source_dir=source_dir,
        districts_path=districts_path,
        blocks_path=blocks_path,
        qa_dir=qa_dir,
        overlay_dir=overlay_dir,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
    )
    print("RURAL FACILITIES ADMIN MASTERS")
    print(f"source_dir: {source_dir}")
    print(f"valid_coordinate_rows: {outputs.normalized_points.shape[0]}")
    print(f"assigned_rows: {outputs.assigned_points.shape[0]}")
    print(f"district_rows: {outputs.district_master.shape[0]}")
    print(f"block_rows: {outputs.block_master.shape[0]}")
    if args.dry_run:
        print("dry_run: True")
        for path in outputs.planned_paths[:20]:
            print(f"planned: {path}")
        if len(outputs.planned_paths) > 20:
            print(f"planned_more: {len(outputs.planned_paths) - 20}")
    else:
        print(f"qa_dir: {qa_dir}")
        print(f"overlay_dir: {overlay_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

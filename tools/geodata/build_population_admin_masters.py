#!/usr/bin/env python3
"""
Build district and block population exposure masters from the 2025 population raster.

This tool treats the raster as the authoritative source and aggregates it onto the
canonical IRT admin polygons. The resulting master CSVs follow the shared wide-master
contract used by the dashboard.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Literal, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.errors import WindowError
from rasterio.features import geometry_mask, geometry_window
from shapely.geometry import mapping

from paths import get_master_csv_filename, get_paths_config, resolve_processed_root
from tools.geodata.build_district_subbasin_crosswalk import (
    load_block_boundaries,
    load_district_boundaries,
)


AdminLevel = Literal["district", "block"]

POPULATION_TOTAL_COL = "population_total__snapshot__2025__mean"
POPULATION_DENSITY_COL = "population_density__snapshot__2025__mean"
AREA_EPSG = 6933
DEFAULT_RASTER_NAME = "ind_pop_2025_CN_1km_R2025A_UA_v1.tif"


def _find_default_population_raster() -> Path:
    data_dir = get_paths_config().data_dir
    direct = data_dir / "population" / DEFAULT_RASTER_NAME
    if direct.exists():
        return direct

    preferred = (
        data_dir
        / "population-20260317T093028Z-1-001"
        / "population"
        / DEFAULT_RASTER_NAME
    )
    if preferred.exists():
        return preferred

    candidates = sorted(data_dir.glob(f"population*/population/{DEFAULT_RASTER_NAME}"))
    if candidates:
        return candidates[0]
    return preferred


def _default_population_output_dir() -> Path:
    return get_paths_config().data_dir / "population"


def _get_admin_identity_columns(level: AdminLevel) -> list[str]:
    if level == "block":
        return ["state_name", "district_name", "block_name", "block_key"]
    return ["state_name", "district_name", "district_key"]


def _area_column(level: AdminLevel) -> str:
    return "block_area_km2" if level == "block" else "district_area_km2"


def _qa_key_columns(level: AdminLevel) -> list[str]:
    if level == "block":
        return ["state", "district", "block", "block_key"]
    return ["state", "district", "district_key"]


def _zonal_sum_for_geometry(dataset: rasterio.io.DatasetReader, geom) -> tuple[float, int]:
    """Return (population_sum, contributing_cell_count) for one geometry."""
    if geom is None or geom.is_empty:
        return 0.0, 0

    try:
        window = geometry_window(dataset, [mapping(geom)])
    except WindowError:
        return 0.0, 0

    data = dataset.read(1, window=window, masked=True)
    if data.size == 0:
        return 0.0, 0

    geom_mask = geometry_mask(
        [mapping(geom)],
        out_shape=data.shape,
        transform=dataset.window_transform(window),
        invert=True,
        all_touched=False,
    )
    combined_mask = np.logical_or(np.ma.getmaskarray(data), ~geom_mask)
    clipped = np.ma.array(data, mask=combined_mask)
    if clipped.count() == 0:
        return 0.0, 0
    return float(clipped.sum()), int(clipped.count())


def aggregate_population_to_admin_units(
    admin_gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
    raster_path: Path,
    area_epsg: int = AREA_EPSG,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aggregate raster population onto canonical district or block polygons.

    Returns:
        (master_df, qa_df)
    """
    if admin_gdf.empty:
        raise ValueError(f"No {level} boundaries were provided.")

    identity_cols = _get_admin_identity_columns(level)
    missing = [col for col in identity_cols + ["geometry"] if col not in admin_gdf.columns]
    if missing:
        raise ValueError(f"{level.title()} boundaries are missing required columns: {missing}")

    with rasterio.open(raster_path) as src:
        if src.crs is None:
            raise ValueError(f"Population raster has no CRS: {raster_path}")
        admin_for_raster = admin_gdf.to_crs(src.crs).copy()

        population_sums: list[float] = []
        cell_counts: list[int] = []
        for geom in admin_for_raster.geometry:
            pop_sum, cell_count = _zonal_sum_for_geometry(src, geom)
            population_sums.append(pop_sum)
            cell_counts.append(cell_count)

        raster_band = src.read(1, masked=True)
        raster_total = float(raster_band.sum()) if raster_band.count() else 0.0

    area_df = admin_gdf.to_crs(epsg=area_epsg).copy()
    out = admin_gdf[identity_cols].copy()
    out[_area_column(level)] = area_df.geometry.area / 1_000_000.0
    out[POPULATION_TOTAL_COL] = pd.to_numeric(pd.Series(population_sums), errors="coerce").fillna(0.0).to_numpy()
    out["__cell_count"] = pd.to_numeric(pd.Series(cell_counts), errors="coerce").fillna(0).astype(int).to_numpy()

    area_vals = pd.to_numeric(out[_area_column(level)], errors="coerce")
    totals = pd.to_numeric(out[POPULATION_TOTAL_COL], errors="coerce").fillna(0.0)
    out[POPULATION_DENSITY_COL] = np.where(
        area_vals.gt(0),
        totals / area_vals,
        np.nan,
    )

    master_df = out.copy()
    master_df = master_df.rename(
        columns={
            "state_name": "state",
            "district_name": "district",
            "block_name": "block",
        }
    )
    master_df = master_df.sort_values(_qa_key_columns(level)).reset_index(drop=True)

    qa_df = master_df.copy()
    qa_df["source_raster"] = str(raster_path)
    qa_df["raster_cell_count"] = out["__cell_count"].to_numpy()
    qa_df["raster_population_total"] = raster_total
    qa_df = qa_df.sort_values(_qa_key_columns(level)).reset_index(drop=True)
    return master_df, qa_df


def build_population_consistency_qa(
    district_master_df: pd.DataFrame,
    block_master_df: pd.DataFrame,
) -> pd.DataFrame:
    """Compare district totals against the sum of child blocks."""
    district_totals = district_master_df[
        ["state", "district", POPULATION_TOTAL_COL]
    ].rename(columns={POPULATION_TOTAL_COL: "district_population_total"})

    block_totals = (
        block_master_df.groupby(["state", "district"], as_index=False)[POPULATION_TOTAL_COL]
        .sum()
        .rename(columns={POPULATION_TOTAL_COL: "sum_block_population_total"})
    )

    qa = district_totals.merge(block_totals, on=["state", "district"], how="left")
    qa["sum_block_population_total"] = pd.to_numeric(
        qa["sum_block_population_total"], errors="coerce"
    ).fillna(0.0)
    qa["difference_abs"] = (
        pd.to_numeric(qa["district_population_total"], errors="coerce").fillna(0.0)
        - qa["sum_block_population_total"]
    )
    qa["difference_pct_of_district"] = np.where(
        qa["district_population_total"].abs() > 0,
        (qa["difference_abs"] / qa["district_population_total"]) * 100.0,
        np.nan,
    )
    return qa.sort_values(["state", "district"]).reset_index(drop=True)


def build_population_national_summary(
    district_master_df: pd.DataFrame,
    block_master_df: pd.DataFrame,
    *,
    district_qa_df: pd.DataFrame,
) -> pd.DataFrame:
    """Return a one-row summary comparing raster total against admin aggregates."""
    raster_total = float(
        pd.to_numeric(district_qa_df.get("raster_population_total"), errors="coerce")
        .dropna()
        .iloc[0]
    ) if not district_qa_df.empty else 0.0
    district_total = float(pd.to_numeric(district_master_df[POPULATION_TOTAL_COL], errors="coerce").fillna(0.0).sum())
    block_total = float(pd.to_numeric(block_master_df[POPULATION_TOTAL_COL], errors="coerce").fillna(0.0).sum())
    return pd.DataFrame(
        [
            {
                "raster_population_total": raster_total,
                "district_population_total": district_total,
                "block_population_total": block_total,
                "district_minus_raster": district_total - raster_total,
                "block_minus_raster": block_total - raster_total,
            }
        ]
    )


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file without --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_master_table(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    """Write a master CSV plus a Parquet companion for faster runtime reads."""
    parquet_path = path.with_suffix(".parquet")
    if not overwrite:
        existing = [str(p) for p in (path, parquet_path) if p.exists()]
        if existing:
            raise FileExistsError(
                f"Refusing to overwrite existing file without --overwrite: {', '.join(existing)}"
            )
    _write_csv(df, path, overwrite=True)
    df.to_parquet(parquet_path, index=False)


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
            raise ValueError(f"Population {level} master contains an empty state value.")
        out_path = processed_root / state_label / out_name
        _write_master_table(state_df.reset_index(drop=True), out_path, overwrite=overwrite)
        counts[state_label] = int(state_df.shape[0])
    return counts


def _metric_specific_master(master_df: pd.DataFrame, *, level: AdminLevel, metric_slug: str) -> pd.DataFrame:
    if metric_slug == "population_total":
        if level == "block":
            keep = ["state", "district", "block", "block_key", _area_column(level), POPULATION_TOTAL_COL]
        else:
            keep = ["state", "district", "district_key", _area_column(level), POPULATION_TOTAL_COL]
        return master_df[keep].copy()
    if metric_slug == "population_density":
        if level == "block":
            keep = ["state", "district", "block", "block_key", _area_column(level), POPULATION_DENSITY_COL]
        else:
            keep = ["state", "district", "district_key", _area_column(level), POPULATION_DENSITY_COL]
        return master_df[keep].copy()
    raise ValueError(f"Unsupported population metric slug: {metric_slug}")


def build_population_admin_outputs(
    *,
    raster_path: Path,
    districts_path: Path,
    blocks_path: Path,
    qa_dir: Path,
    overwrite: bool,
    dry_run: bool,
) -> dict[str, object]:
    """Build the full district + block population outputs."""
    district_gdf = load_district_boundaries(districts_path)
    block_gdf = load_block_boundaries(blocks_path)

    district_master_df, district_qa_df = aggregate_population_to_admin_units(
        district_gdf,
        level="district",
        raster_path=raster_path,
    )
    block_master_df, block_qa_df = aggregate_population_to_admin_units(
        block_gdf,
        level="block",
        raster_path=raster_path,
    )

    consistency_qa_df = build_population_consistency_qa(district_master_df, block_master_df)
    national_summary_df = build_population_national_summary(
        district_master_df,
        block_master_df,
        district_qa_df=district_qa_df,
    )

    if not dry_run:
        district_counts = _write_state_slices(
            _metric_specific_master(district_master_df, level="district", metric_slug="population_total"),
            metric_slug="population_total",
            level="district",
            overwrite=overwrite,
        )
        _write_state_slices(
            _metric_specific_master(district_master_df, level="district", metric_slug="population_density"),
            metric_slug="population_density",
            level="district",
            overwrite=overwrite,
        )
        block_counts = _write_state_slices(
            _metric_specific_master(block_master_df, level="block", metric_slug="population_total"),
            metric_slug="population_total",
            level="block",
            overwrite=overwrite,
        )
        _write_state_slices(
            _metric_specific_master(block_master_df, level="block", metric_slug="population_density"),
            metric_slug="population_density",
            level="block",
            overwrite=overwrite,
        )

        _write_csv(district_qa_df, qa_dir / "population_district_master_qa.csv", overwrite=overwrite)
        _write_csv(block_qa_df, qa_dir / "population_block_master_qa.csv", overwrite=overwrite)
        _write_csv(consistency_qa_df, qa_dir / "population_district_vs_blocks_qa.csv", overwrite=overwrite)
        _write_csv(national_summary_df, qa_dir / "population_national_summary.csv", overwrite=overwrite)
    else:
        district_counts = district_master_df.groupby("state", as_index=False).size().set_index("state")["size"].astype(int).to_dict()
        block_counts = block_master_df.groupby("state", as_index=False).size().set_index("state")["size"].astype(int).to_dict()

    return {
        "district_master_df": district_master_df,
        "block_master_df": block_master_df,
        "district_qa_df": district_qa_df,
        "block_qa_df": block_qa_df,
        "consistency_qa_df": consistency_qa_df,
        "national_summary_df": national_summary_df,
        "district_counts": district_counts,
        "block_counts": block_counts,
    }


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build canonical district and block population masters from the 2025 population raster."
    )
    parser.add_argument("--raster", type=str, default=str(_find_default_population_raster()))
    parser.add_argument("--districts", type=str, default=str(get_paths_config().districts_path))
    parser.add_argument("--blocks", type=str, default=str(get_paths_config().blocks_path))
    parser.add_argument("--qa-dir", type=str, default=str(_default_population_output_dir()))
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Compute summaries without writing files.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    raster_path = Path(args.raster).expanduser().resolve()
    districts_path = Path(args.districts).expanduser().resolve()
    blocks_path = Path(args.blocks).expanduser().resolve()
    qa_dir = Path(args.qa_dir).expanduser().resolve()

    if not raster_path.exists():
        raise FileNotFoundError(f"Population raster not found: {raster_path}")
    if not districts_path.exists():
        raise FileNotFoundError(f"District boundaries not found: {districts_path}")
    if not blocks_path.exists():
        raise FileNotFoundError(f"Block boundaries not found: {blocks_path}")

    outputs = build_population_admin_outputs(
        raster_path=raster_path,
        districts_path=districts_path,
        blocks_path=blocks_path,
        qa_dir=qa_dir,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
    )

    district_master_df = outputs["district_master_df"]
    block_master_df = outputs["block_master_df"]
    district_counts = outputs["district_counts"]
    block_counts = outputs["block_counts"]
    national_summary_df = outputs["national_summary_df"]

    print("POPULATION ADMIN MASTERS")
    print(f"raster: {raster_path}")
    print(f"district_rows: {int(district_master_df.shape[0])}")
    print(f"block_rows: {int(block_master_df.shape[0])}")
    print(
        "district_states: "
        + ", ".join(f"{state}:{count}" for state, count in sorted(district_counts.items())[:8])
        + (" ..." if len(district_counts) > 8 else "")
    )
    print(
        "block_states: "
        + ", ".join(f"{state}:{count}" for state, count in sorted(block_counts.items())[:8])
        + (" ..." if len(block_counts) > 8 else "")
    )
    if not national_summary_df.empty:
        row = national_summary_df.iloc[0]
        print(f"raster_population_total: {float(row['raster_population_total']):.2f}")
        print(f"district_population_total: {float(row['district_population_total']):.2f}")
        print(f"block_population_total: {float(row['block_population_total']):.2f}")
    if bool(args.dry_run):
        print("dry_run: True")
    else:
        print(f"qa_dir: {qa_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

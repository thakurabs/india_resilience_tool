#!/usr/bin/env python3
"""
Build SOI hydro master CSVs for Aqueduct metrics.

This tool transfers Aqueduct HydroSHEDS-derived metrics onto Survey of India
 basin and sub-basin units using precomputed area-weighted overlap crosswalks.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd

from paths import BASINS_PATH, SUBBASINS_PATH, get_master_csv_filename, get_paths_config, resolve_processed_root
from tools.geodata.build_aqueduct_hydro_crosswalk import load_soi_hydro_boundaries


HydroLevel = Literal["basin", "sub_basin"]
_INVALID_NUMERIC_SENTINELS = {-9999.0, 9999.0}

AQ_WATER_STRESS_COLUMN_MAP: dict[str, str] = {
    "aq_water_stress__historical__1979-2019__mean": "bws_raw",
    "aq_water_stress__bau__2030__mean": "bau30_ws_x_r",
    "aq_water_stress__bau__2050__mean": "bau50_ws_x_r",
    "aq_water_stress__bau__2080__mean": "bau80_ws_x_r",
    "aq_water_stress__opt__2030__mean": "opt30_ws_x_r",
    "aq_water_stress__opt__2050__mean": "opt50_ws_x_r",
    "aq_water_stress__opt__2080__mean": "opt80_ws_x_r",
    "aq_water_stress__pes__2030__mean": "pes30_ws_x_r",
    "aq_water_stress__pes__2050__mean": "pes50_ws_x_r",
    "aq_water_stress__pes__2080__mean": "pes80_ws_x_r",
}


def _default_aqueduct_dir() -> Path:
    return get_paths_config().data_dir / "aqueduct"


def _normalize_pfaf_id_series(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().fillna("")


def _numeric_metric_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.mask(numeric.isin(_INVALID_NUMERIC_SENTINELS))


def load_metric_source_table(
    baseline_path: Path,
    future_path: Path,
    *,
    source_column_map: dict[str, str],
) -> pd.DataFrame:
    """Load and combine the required Aqueduct source columns keyed by ``pfaf_id``."""
    baseline_gdf = gpd.read_file(baseline_path)
    future_gdf = gpd.read_file(future_path)

    if "pfaf_id" not in baseline_gdf.columns:
        raise ValueError(f"Baseline Aqueduct artifact is missing 'pfaf_id': {baseline_path}")
    if "pfaf_id" not in future_gdf.columns:
        raise ValueError(f"Future Aqueduct artifact is missing 'pfaf_id': {future_path}")

    baseline_df = baseline_gdf.drop(columns="geometry", errors="ignore").copy()
    future_df = future_gdf.drop(columns="geometry", errors="ignore").copy()
    baseline_df["pfaf_id"] = _normalize_pfaf_id_series(baseline_df["pfaf_id"])
    future_df["pfaf_id"] = _normalize_pfaf_id_series(future_df["pfaf_id"])

    source_df = pd.DataFrame({"pfaf_id": sorted(set(baseline_df["pfaf_id"]).union(set(future_df["pfaf_id"])))})
    source_df = source_df[source_df["pfaf_id"].astype(str).str.strip().ne("")].reset_index(drop=True)

    for output_column, source_column in source_column_map.items():
        if source_column in baseline_df.columns:
            values = baseline_df[["pfaf_id", source_column]].copy()
        elif source_column in future_df.columns:
            values = future_df[["pfaf_id", source_column]].copy()
        else:
            raise ValueError(f"Aqueduct source column not found in baseline/future artifacts: {source_column}")

        if values["pfaf_id"].duplicated().any():
            dupes = values.loc[values["pfaf_id"].duplicated(keep=False), "pfaf_id"].tolist()
            raise ValueError(f"Aqueduct source column {source_column!r} has duplicate pfaf_id values: {dupes[:10]}")

        values = values.rename(columns={source_column: output_column})
        values[output_column] = _numeric_metric_series(values[output_column])
        source_df = source_df.merge(values, on="pfaf_id", how="left")

    return source_df.reset_index(drop=True)


def load_crosswalk(path: Path, *, hydro_level: HydroLevel) -> pd.DataFrame:
    """Load an Aqueduct-to-SOI overlap crosswalk CSV."""
    df = pd.read_csv(path)
    if "pfaf_id" not in df.columns or "intersection_area_km2" not in df.columns:
        raise ValueError(f"Crosswalk is missing required columns: {path}")

    df = df.copy()
    df["pfaf_id"] = _normalize_pfaf_id_series(df["pfaf_id"])

    if hydro_level == "sub_basin":
        required = {"basin_id", "basin_name", "subbasin_id", "subbasin_name", "subbasin_area_km2"}
    else:
        required = {"basin_id", "basin_name", "basin_area_km2"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Crosswalk missing required columns {missing}: {path}")

    return df


def aggregate_crosswalk_to_targets(
    *,
    source_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    target_gdf: gpd.GeoDataFrame,
    hydro_level: HydroLevel,
    source_column_map: dict[str, str],
    area_epsg: int = 6933,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate Aqueduct source metrics onto SOI hydro target units."""
    target_level = "subbasin" if hydro_level == "sub_basin" else "basin"
    target_id_col = "subbasin_id" if hydro_level == "sub_basin" else "basin_id"
    target_area_col = "subbasin_area_km2" if hydro_level == "sub_basin" else "basin_area_km2"
    target_keep_cols = ["basin_id", "basin_name", "subbasin_id", "subbasin_code", "subbasin_name"] if hydro_level == "sub_basin" else ["basin_id", "basin_name"]

    target_proj = target_gdf.to_crs(epsg=area_epsg).copy()
    target_proj[target_area_col] = target_proj.geometry.area / 1_000_000.0
    target_df = (
        target_proj[target_keep_cols + [target_area_col]]
        .drop_duplicates()
        .copy()
        .reset_index(drop=True)
    )
    overlaps = crosswalk_df.merge(source_df, on="pfaf_id", how="left", validate="many_to_one")

    qa = (
        crosswalk_df.groupby(target_keep_cols, dropna=False, as_index=False)
        .agg(
            source_pfaf_count=("pfaf_id", "nunique"),
            intersection_area_km2=("intersection_area_km2", "sum"),
        )
    )
    qa = target_df.rename(columns={target_area_col: "target_area_km2"}).merge(qa, on=target_keep_cols, how="left")
    qa["source_pfaf_count"] = pd.to_numeric(qa["source_pfaf_count"], errors="coerce").fillna(0).astype(int)
    qa["intersection_area_km2"] = pd.to_numeric(qa["intersection_area_km2"], errors="coerce").fillna(0.0)
    qa["target_area_km2"] = pd.to_numeric(qa["target_area_km2"], errors="coerce")
    qa[f"{target_level}_coverage_fraction"] = qa["intersection_area_km2"] / qa["target_area_km2"]

    aggregated = target_df.copy()
    for output_column in source_column_map:
        valid = overlaps.loc[
            overlaps[output_column].notna() & overlaps["intersection_area_km2"].gt(0),
            target_keep_cols + ["intersection_area_km2", output_column],
        ].copy()
        if valid.empty:
            aggregated[output_column] = pd.NA
            qa[f"{output_column}__valid_weight_km2"] = 0.0
            continue

        valid["weighted_value"] = valid[output_column] * valid["intersection_area_km2"]
        numerators = valid.groupby(target_keep_cols, dropna=False)["weighted_value"].sum()
        denominators = valid.groupby(target_keep_cols, dropna=False)["intersection_area_km2"].sum()

        rolled = (
            pd.DataFrame(
                {
                    output_column: numerators / denominators,
                    f"{output_column}__valid_weight_km2": denominators,
                }
            )
            .reset_index()
        )
        aggregated = aggregated.merge(rolled[target_keep_cols + [output_column]], on=target_keep_cols, how="left")
        qa = qa.merge(rolled[target_keep_cols + [f"{output_column}__valid_weight_km2"]], on=target_keep_cols, how="left")

    for output_column in source_column_map:
        valid_col = f"{output_column}__valid_weight_km2"
        if valid_col in qa.columns:
            qa[valid_col] = pd.to_numeric(qa[valid_col], errors="coerce").fillna(0.0)

    aggregated = aggregated.sort_values(target_keep_cols).reset_index(drop=True)
    qa = qa.sort_values(target_keep_cols).reset_index(drop=True)

    if aggregated[target_id_col].duplicated().any():
        dupes = aggregated.loc[aggregated[target_id_col].duplicated(keep=False), target_id_col].tolist()
        raise ValueError(f"Aggregated {target_level} master contains duplicate IDs: {dupes[:10]}")

    return aggregated, qa


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Output already exists (pass --overwrite): {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Aqueduct hydro master CSVs on SOI basin/sub-basin units.")
    parser.add_argument(
        "--baseline",
        type=str,
        default=str(_default_aqueduct_dir() / "baseline_clean_india.geojson"),
        help="Path to the clean India Aqueduct baseline GeoJSON.",
    )
    parser.add_argument(
        "--future",
        type=str,
        default=str(_default_aqueduct_dir() / "future_annual_india.geojson"),
        help="Path to the India-only Aqueduct future GeoJSON.",
    )
    parser.add_argument(
        "--basin-crosswalk",
        type=str,
        default=str(_default_aqueduct_dir() / "aqueduct_basin_crosswalk.csv"),
        help="Path to the Aqueduct-to-basin crosswalk CSV.",
    )
    parser.add_argument(
        "--subbasin-crosswalk",
        type=str,
        default=str(_default_aqueduct_dir() / "aqueduct_subbasin_crosswalk.csv"),
        help="Path to the Aqueduct-to-sub-basin crosswalk CSV.",
    )
    parser.add_argument("--basins", type=str, default=str(BASINS_PATH), help="Path to canonical SOI basins GeoJSON.")
    parser.add_argument(
        "--subbasins",
        type=str,
        default=str(SUBBASINS_PATH),
        help="Path to canonical SOI sub-basins GeoJSON.",
    )
    parser.add_argument(
        "--metric-slug",
        type=str,
        default="aq_water_stress",
        help="Metric slug whose processed/{slug}/hydro masters should be written.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing master CSVs and QA outputs.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    metric_slug = str(args.metric_slug).strip()
    if metric_slug != "aq_water_stress":
        raise ValueError(f"Unsupported Aqueduct hydro metric slug: {metric_slug}")

    baseline_path = Path(args.baseline).expanduser().resolve()
    future_path = Path(args.future).expanduser().resolve()
    basin_crosswalk_path = Path(args.basin_crosswalk).expanduser().resolve()
    subbasin_crosswalk_path = Path(args.subbasin_crosswalk).expanduser().resolve()
    basins_path = Path(args.basins).expanduser().resolve()
    subbasins_path = Path(args.subbasins).expanduser().resolve()

    for path in (baseline_path, future_path, basin_crosswalk_path, subbasin_crosswalk_path, basins_path, subbasins_path):
        if not path.exists():
            raise FileNotFoundError(f"Required Aqueduct hydro input not found: {path}")

    source_df = load_metric_source_table(
        baseline_path,
        future_path,
        source_column_map=AQ_WATER_STRESS_COLUMN_MAP,
    )

    basin_crosswalk_df = load_crosswalk(basin_crosswalk_path, hydro_level="basin")
    subbasin_crosswalk_df = load_crosswalk(subbasin_crosswalk_path, hydro_level="sub_basin")
    basin_targets = load_soi_hydro_boundaries(basins_path, level="basin")
    subbasin_targets = load_soi_hydro_boundaries(subbasins_path, level="sub_basin")

    basin_master_df, basin_qa_df = aggregate_crosswalk_to_targets(
        source_df=source_df,
        crosswalk_df=basin_crosswalk_df,
        target_gdf=basin_targets,
        hydro_level="basin",
        source_column_map=AQ_WATER_STRESS_COLUMN_MAP,
    )
    subbasin_master_df, subbasin_qa_df = aggregate_crosswalk_to_targets(
        source_df=source_df,
        crosswalk_df=subbasin_crosswalk_df,
        target_gdf=subbasin_targets,
        hydro_level="sub_basin",
        source_column_map=AQ_WATER_STRESS_COLUMN_MAP,
    )

    processed_root = resolve_processed_root(metric_slug, mode="portfolio")
    hydro_root = processed_root / "hydro"
    basin_master_path = hydro_root / get_master_csv_filename("basin")
    subbasin_master_path = hydro_root / get_master_csv_filename("sub_basin")
    aqueduct_dir = _default_aqueduct_dir()
    basin_qa_path = aqueduct_dir / f"{metric_slug}_basin_master_qa.csv"
    subbasin_qa_path = aqueduct_dir / f"{metric_slug}_subbasin_master_qa.csv"

    _write_csv(basin_master_df, basin_master_path, overwrite=bool(args.overwrite))
    _write_csv(subbasin_master_df, subbasin_master_path, overwrite=bool(args.overwrite))
    _write_csv(basin_qa_df, basin_qa_path, overwrite=bool(args.overwrite))
    _write_csv(subbasin_qa_df, subbasin_qa_path, overwrite=bool(args.overwrite))

    print("AQUEDUCT HYDRO MASTERS")
    print(f"metric_slug: {metric_slug}")
    print(f"basin_master_rows: {len(basin_master_df)}")
    print(f"subbasin_master_rows: {len(subbasin_master_df)}")
    print(f"basin_master: {basin_master_path}")
    print(f"subbasin_master: {subbasin_master_path}")
    print(f"basin_qa: {basin_qa_path}")
    print(f"subbasin_qa: {subbasin_qa_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

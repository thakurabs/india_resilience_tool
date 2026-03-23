#!/usr/bin/env python3
"""
Build SOI hydro master CSVs for Aqueduct metrics.

This tool transfers Aqueduct HydroSHEDS-derived metrics onto Survey of India
 basin and sub-basin units using precomputed area-weighted overlap crosswalks.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd

from paths import BASINS_PATH, SUBBASINS_PATH, get_master_csv_filename, get_paths_config, resolve_processed_root
from tools.geodata.build_aqueduct_hydro_crosswalk import load_soi_hydro_boundaries


HydroLevel = Literal["basin", "sub_basin"]
_INVALID_NUMERIC_SENTINELS = {-9999.0, 9999.0}

BASELINE_PERIOD = "1979-2019"
FUTURE_SCENARIO_PERIODS: tuple[tuple[str, str, str], ...] = (
    ("bau", "30", "2030"),
    ("bau", "50", "2050"),
    ("bau", "80", "2080"),
    ("opt", "30", "2030"),
    ("opt", "50", "2050"),
    ("opt", "80", "2080"),
    ("pes", "30", "2030"),
    ("pes", "50", "2050"),
    ("pes", "80", "2080"),
)


@dataclass(frozen=True)
class AqueductMetricSpec:
    """Declarative mapping for one onboarded Aqueduct metric family."""

    slug: str
    label: str
    description: str
    baseline_source_column: str
    baseline_indicator_name: str
    baseline_interpretation: str
    future_source_suffix: str
    future_indicator_name: str
    future_interpretation: str
    units: str = "index"
    rank_higher_is_worse: bool = True

    def output_columns(self) -> dict[str, str]:
        """Return dashboard master column -> Aqueduct source column mapping."""
        columns = {
            f"{self.slug}__historical__{BASELINE_PERIOD}__mean": self.baseline_source_column,
        }
        for scenario, horizon_token, period in FUTURE_SCENARIO_PERIODS:
            columns[f"{self.slug}__{scenario}__{period}__mean"] = f"{scenario}{horizon_token}_{self.future_source_suffix}_x_r"
        return columns

    def field_contract_rows(self) -> list[dict[str, str]]:
        """Return markdown-ready field contract rows for this metric."""
        rows = [
            {
                "metric_slug": self.slug,
                "metric_label": self.label,
                "output_column": f"{self.slug}__historical__{BASELINE_PERIOD}__mean",
                "source_column": self.baseline_source_column,
                "source_dataset": "baseline_clean_india.geojson",
                "indicator_name": self.baseline_indicator_name,
                "scenario": "historical",
                "period": BASELINE_PERIOD,
                "interpretation": self.baseline_interpretation,
                "comparability_note": (
                    f"Used as the historical reference for the current {self.label.lower()} onboarding."
                ),
            }
        ]
        for scenario, horizon_token, period in FUTURE_SCENARIO_PERIODS:
            rows.append(
                {
                    "metric_slug": self.slug,
                    "metric_label": self.label,
                    "output_column": f"{self.slug}__{scenario}__{period}__mean",
                    "source_column": f"{scenario}{horizon_token}_{self.future_source_suffix}_x_r",
                    "source_dataset": "future_annual_india.geojson",
                    "indicator_name": self.future_indicator_name,
                    "scenario": scenario,
                    "period": period,
                    "interpretation": self.future_interpretation,
                    "comparability_note": (
                        f"Projected future {self.label.lower()} under Aqueduct 4.0 {scenario} scenario."
                    ),
                }
            )
        return rows


AQUEDUCT_METRIC_SPECS: dict[str, AqueductMetricSpec] = {
    "aq_water_stress": AqueductMetricSpec(
        slug="aq_water_stress",
        label="Aqueduct Water Stress",
        description=(
            "Aqueduct 4.0 annual water stress transferred from HydroSHEDS Level 6 "
            "onto Survey of India basin and sub-basin units using area-weighted overlap."
        ),
        baseline_source_column="bws_raw",
        baseline_indicator_name="Baseline Water Stress",
        baseline_interpretation="Baseline annual water stress screening indicator",
        future_source_suffix="ws",
        future_indicator_name="Future Water Stress",
        future_interpretation="Future annual water stress screening indicator",
    ),
    "aq_interannual_variability": AqueductMetricSpec(
        slug="aq_interannual_variability",
        label="Aqueduct Interannual Variability",
        description=(
            "Aqueduct 4.0 interannual variability transferred from HydroSHEDS Level 6 "
            "onto Survey of India basin and sub-basin units using area-weighted overlap."
        ),
        baseline_source_column="iav_raw",
        baseline_indicator_name="Interannual Variability",
        baseline_interpretation="Baseline interannual variability screening indicator",
        future_source_suffix="iv",
        future_indicator_name="Future Interannual Variability",
        future_interpretation="Future interannual variability screening indicator",
    ),
    "aq_seasonal_variability": AqueductMetricSpec(
        slug="aq_seasonal_variability",
        label="Aqueduct Seasonal Variability",
        description=(
            "Aqueduct 4.0 seasonal variability transferred from HydroSHEDS Level 6 "
            "onto Survey of India basin and sub-basin units using area-weighted overlap."
        ),
        baseline_source_column="sev_raw",
        baseline_indicator_name="Seasonal Variability",
        baseline_interpretation="Baseline seasonal variability screening indicator",
        future_source_suffix="sv",
        future_indicator_name="Future Seasonal Variability",
        future_interpretation="Future seasonal variability screening indicator",
    ),
    "aq_water_depletion": AqueductMetricSpec(
        slug="aq_water_depletion",
        label="Aqueduct Water Depletion",
        description=(
            "Aqueduct 4.0 water depletion transferred from HydroSHEDS Level 6 "
            "onto Survey of India basin and sub-basin units using area-weighted overlap."
        ),
        baseline_source_column="bwd_raw",
        baseline_indicator_name="Baseline Water Depletion",
        baseline_interpretation="Baseline water depletion screening indicator",
        future_source_suffix="wd",
        future_indicator_name="Future Water Depletion",
        future_interpretation="Future water depletion screening indicator",
    ),
}

AQUEDUCT_METRIC_ORDER: tuple[str, ...] = tuple(AQUEDUCT_METRIC_SPECS.keys())
AQ_WATER_STRESS_COLUMN_MAP: dict[str, str] = AQUEDUCT_METRIC_SPECS["aq_water_stress"].output_columns()


def _default_aqueduct_dir() -> Path:
    return get_paths_config().data_dir / "aqueduct"


def get_supported_aqueduct_metric_slugs() -> tuple[str, ...]:
    """Return supported Aqueduct dashboard metric slugs in build order."""
    return AQUEDUCT_METRIC_ORDER


def get_aqueduct_metric_spec(metric_slug: str) -> AqueductMetricSpec:
    """Return the metric spec for one onboarded Aqueduct metric slug."""
    slug = str(metric_slug).strip()
    if slug not in AQUEDUCT_METRIC_SPECS:
        raise ValueError(
            f"Unsupported Aqueduct hydro metric slug: {slug}. "
            f"Expected one of: {', '.join(get_supported_aqueduct_metric_slugs())}"
        )
    return AQUEDUCT_METRIC_SPECS[slug]


def get_aqueduct_source_column_map(metric_slug: str) -> dict[str, str]:
    """Return dashboard master column -> source-column mapping for one metric."""
    return get_aqueduct_metric_spec(metric_slug).output_columns()


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


def _write_master_table(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    """Write a runtime master table as CSV plus a Parquet companion."""
    parquet_path = path.with_suffix(".parquet")
    if not overwrite:
        existing = [str(p) for p in (path, parquet_path) if p.exists()]
        if existing:
            raise FileExistsError(f"Output already exists (pass --overwrite): {', '.join(existing)}")
    _write_csv(df, path, overwrite=True)
    df.to_parquet(parquet_path, index=False)


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
        action="append",
        default=None,
        help=(
            "Aqueduct metric slug to build. Repeat for multiple metrics, or pass `all` "
            f"to build all supported Aqueduct metrics ({', '.join(get_supported_aqueduct_metric_slugs())})."
        ),
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing master CSVs and QA outputs.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    raw_metric_slugs = [str(v).strip() for v in (args.metric_slug or []) if str(v).strip()]
    if not raw_metric_slugs or any(v.lower() == "all" for v in raw_metric_slugs):
        metric_slugs = list(get_supported_aqueduct_metric_slugs())
    else:
        seen: set[str] = set()
        metric_slugs = []
        for slug in raw_metric_slugs:
            if slug not in seen:
                metric_slugs.append(get_aqueduct_metric_spec(slug).slug)
                seen.add(slug)

    baseline_path = Path(args.baseline).expanduser().resolve()
    future_path = Path(args.future).expanduser().resolve()
    basin_crosswalk_path = Path(args.basin_crosswalk).expanduser().resolve()
    subbasin_crosswalk_path = Path(args.subbasin_crosswalk).expanduser().resolve()
    basins_path = Path(args.basins).expanduser().resolve()
    subbasins_path = Path(args.subbasins).expanduser().resolve()

    for path in (baseline_path, future_path, basin_crosswalk_path, subbasin_crosswalk_path, basins_path, subbasins_path):
        if not path.exists():
            raise FileNotFoundError(f"Required Aqueduct hydro input not found: {path}")

    basin_crosswalk_df = load_crosswalk(basin_crosswalk_path, hydro_level="basin")
    subbasin_crosswalk_df = load_crosswalk(subbasin_crosswalk_path, hydro_level="sub_basin")
    basin_targets = load_soi_hydro_boundaries(basins_path, level="basin")
    subbasin_targets = load_soi_hydro_boundaries(subbasins_path, level="sub_basin")

    aqueduct_dir = _default_aqueduct_dir()
    print("AQUEDUCT HYDRO MASTERS")
    print(f"metric_slugs: {', '.join(metric_slugs)}")
    for metric_slug in metric_slugs:
        source_column_map = get_aqueduct_source_column_map(metric_slug)
        source_df = load_metric_source_table(
            baseline_path,
            future_path,
            source_column_map=source_column_map,
        )

        basin_master_df, basin_qa_df = aggregate_crosswalk_to_targets(
            source_df=source_df,
            crosswalk_df=basin_crosswalk_df,
            target_gdf=basin_targets,
            hydro_level="basin",
            source_column_map=source_column_map,
        )
        subbasin_master_df, subbasin_qa_df = aggregate_crosswalk_to_targets(
            source_df=source_df,
            crosswalk_df=subbasin_crosswalk_df,
            target_gdf=subbasin_targets,
            hydro_level="sub_basin",
            source_column_map=source_column_map,
        )

        processed_root = resolve_processed_root(metric_slug, mode="portfolio")
        hydro_root = processed_root / "hydro"
        basin_master_path = hydro_root / get_master_csv_filename("basin")
        subbasin_master_path = hydro_root / get_master_csv_filename("sub_basin")
        basin_qa_path = aqueduct_dir / f"{metric_slug}_basin_master_qa.csv"
        subbasin_qa_path = aqueduct_dir / f"{metric_slug}_subbasin_master_qa.csv"

        _write_master_table(basin_master_df, basin_master_path, overwrite=bool(args.overwrite))
        _write_master_table(subbasin_master_df, subbasin_master_path, overwrite=bool(args.overwrite))
        _write_csv(basin_qa_df, basin_qa_path, overwrite=bool(args.overwrite))
        _write_csv(subbasin_qa_df, subbasin_qa_path, overwrite=bool(args.overwrite))

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

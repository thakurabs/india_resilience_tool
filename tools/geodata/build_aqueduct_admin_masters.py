#!/usr/bin/env python3
"""
Build admin master CSVs for Aqueduct metrics.

This tool transfers clean Aqueduct ``pfaf_id`` metrics directly onto canonical
district polygons using a precomputed Aqueduct-to-district overlap crosswalk.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from paths import get_master_csv_filename, get_paths_config, resolve_processed_root
from tools.geodata.build_aqueduct_hydro_masters import (
    get_aqueduct_metric_spec,
    get_aqueduct_source_column_map,
    get_supported_aqueduct_metric_slugs,
    load_metric_source_table,
)


def _default_aqueduct_dir() -> Path:
    return get_paths_config().data_dir / "aqueduct"


def _normalize_pfaf_id_series(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().fillna("")


def _normalize_text_series(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().fillna("")


def _invalid_identity_mask(series: pd.Series) -> pd.Series:
    normalized = _normalize_text_series(series)
    lowered = normalized.str.lower()
    return lowered.isin({"", "nan", "none", "null", "nat"})


def _validate_district_identity_columns(df: pd.DataFrame, *, label: str) -> None:
    required = ("state_name", "district_name", "district_key")
    for column in required:
        invalid = _invalid_identity_mask(df[column])
        if invalid.any():
            bad_values = sorted({str(v) for v in df.loc[invalid, column].astype("string").fillna("<NA>").tolist()})
            raise ValueError(
                f"{label} contains invalid {column} values: {bad_values[:5]}"
            )


def load_district_crosswalk(path: Path) -> pd.DataFrame:
    """Load an Aqueduct-to-district overlap crosswalk CSV."""
    df = pd.read_csv(path)
    required = {
        "pfaf_id",
        "state_name",
        "district_name",
        "district_key",
        "district_area_km2",
        "intersection_area_km2",
        "pfaf_area_fraction_in_district",
        "district_area_fraction_in_pfaf",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"District crosswalk missing required columns {missing}: {path}")
    df = df.copy()
    df["pfaf_id"] = _normalize_pfaf_id_series(df["pfaf_id"])
    df["state_name"] = _normalize_text_series(df["state_name"])
    df["district_name"] = _normalize_text_series(df["district_name"])
    df["district_key"] = _normalize_text_series(df["district_key"])
    _validate_district_identity_columns(df, label=f"District crosswalk {path}")
    return df


def aggregate_crosswalk_to_districts(
    *,
    source_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    source_column_map: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate Aqueduct source metrics onto district target units."""
    target_keep_cols = ["state_name", "district_name", "district_key"]
    target_df = (
        crosswalk_df[target_keep_cols + ["district_area_km2"]]
        .drop_duplicates()
        .copy()
        .reset_index(drop=True)
    )
    overlaps = crosswalk_df.merge(source_df, on="pfaf_id", how="left", validate="many_to_one")

    qa_df = (
        crosswalk_df.groupby(target_keep_cols, dropna=False, as_index=False)
        .agg(
            source_pfaf_count=("pfaf_id", "nunique"),
            intersection_area_km2=("intersection_area_km2", "sum"),
            district_area_km2=("district_area_km2", "first"),
        )
        .reset_index(drop=True)
    )
    qa_df["source_pfaf_count"] = pd.to_numeric(qa_df["source_pfaf_count"], errors="coerce").fillna(0).astype(int)
    qa_df["intersection_area_km2"] = pd.to_numeric(qa_df["intersection_area_km2"], errors="coerce").fillna(0.0)
    qa_df["district_area_km2"] = pd.to_numeric(qa_df["district_area_km2"], errors="coerce")
    qa_df["district_coverage_fraction"] = qa_df["intersection_area_km2"] / qa_df["district_area_km2"]

    aggregated = target_df.copy()
    for output_column in source_column_map:
        valid = overlaps.loc[
            overlaps[output_column].notna() & overlaps["intersection_area_km2"].gt(0),
            target_keep_cols + ["intersection_area_km2", output_column],
        ].copy()
        if valid.empty:
            aggregated[output_column] = pd.NA
            qa_df[f"{output_column}__valid_weight_km2"] = 0.0
            continue

        valid["weighted_value"] = pd.to_numeric(valid[output_column], errors="coerce") * valid["intersection_area_km2"]
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
        qa_df = qa_df.merge(rolled[target_keep_cols + [f"{output_column}__valid_weight_km2"]], on=target_keep_cols, how="left")

    for output_column in source_column_map:
        valid_col = f"{output_column}__valid_weight_km2"
        if valid_col in qa_df.columns:
            qa_df[valid_col] = pd.to_numeric(qa_df[valid_col], errors="coerce").fillna(0.0)

    aggregated = aggregated.sort_values(["state_name", "district_name"]).reset_index(drop=True)
    qa_df = qa_df.sort_values(["state_name", "district_name"]).reset_index(drop=True)
    _validate_district_identity_columns(aggregated, label="Aggregated Aqueduct district masters")
    _validate_district_identity_columns(qa_df, label="Aqueduct district QA")
    aggregated = aggregated.rename(columns={"state_name": "state", "district_name": "district"})
    return aggregated, qa_df


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Output already exists (pass --overwrite): {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Aqueduct district master CSVs on canonical admin units.")
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
        "--district-crosswalk",
        type=str,
        default=str(_default_aqueduct_dir() / "aqueduct_district_crosswalk.csv"),
        help="Path to the Aqueduct-to-district crosswalk CSV.",
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
            canonical = get_aqueduct_metric_spec(slug).slug
            if canonical not in seen:
                metric_slugs.append(canonical)
                seen.add(canonical)

    baseline_path = Path(args.baseline).expanduser().resolve()
    future_path = Path(args.future).expanduser().resolve()
    district_crosswalk_path = Path(args.district_crosswalk).expanduser().resolve()
    for path in (baseline_path, future_path, district_crosswalk_path):
        if not path.exists():
            raise FileNotFoundError(f"Required Aqueduct admin input not found: {path}")

    district_crosswalk_df = load_district_crosswalk(district_crosswalk_path)
    aqueduct_dir = _default_aqueduct_dir()

    print("AQUEDUCT ADMIN MASTERS")
    print(f"metric_slugs: {', '.join(metric_slugs)}")
    for metric_slug in metric_slugs:
        source_column_map = get_aqueduct_source_column_map(metric_slug)
        source_df = load_metric_source_table(
            baseline_path,
            future_path,
            source_column_map=source_column_map,
        )
        district_master_df, district_qa_df = aggregate_crosswalk_to_districts(
            source_df=source_df,
            crosswalk_df=district_crosswalk_df,
            source_column_map=source_column_map,
        )

        processed_root = resolve_processed_root(metric_slug, mode="portfolio")
        master_name = get_master_csv_filename("district")
        state_counts: list[str] = []
        for state_name, state_df in district_master_df.groupby("state", dropna=False):
            state_dir = processed_root / str(state_name)
            out_path = state_dir / master_name
            _write_csv(state_df.reset_index(drop=True), out_path, overwrite=bool(args.overwrite))
            state_counts.append(f"{state_name}:{len(state_df)}")

        district_qa_path = aqueduct_dir / f"{metric_slug}_district_master_qa.csv"
        _write_csv(district_qa_df, district_qa_path, overwrite=bool(args.overwrite))

        print(f"metric_slug: {metric_slug}")
        print(f"district_master_rows: {len(district_master_df)}")
        print(f"state_slices: {', '.join(state_counts[:8])}{' ...' if len(state_counts) > 8 else ''}")
        print(f"district_qa: {district_qa_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

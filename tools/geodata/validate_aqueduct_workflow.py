#!/usr/bin/env python3
"""
Validate the Aqueduct district + SOI hydro workflow.

This tool audits:
- source-field semantics for the currently onboarded Aqueduct metrics
- cleaned ``pfaf_id`` integrity
- crosswalk area conservation
- master coverage reliability
- projection / transfer sensitivity
- sampled-unit spot checks against the written masters
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Literal, Sequence

import geopandas as gpd
import pandas as pd

from paths import BASINS_PATH, SUBBASINS_PATH, get_paths_config
from tools.geodata.build_aqueduct_admin_masters import (
    aggregate_crosswalk_to_districts,
    load_district_crosswalk,
)
from tools.geodata.build_aqueduct_hydro_crosswalk import (
    load_aqueduct_boundaries,
    load_soi_hydro_boundaries,
)
from tools.geodata.build_district_subbasin_crosswalk import load_district_boundaries
from tools.geodata.build_aqueduct_hydro_masters import (
    aggregate_crosswalk_to_targets,
    get_aqueduct_metric_spec,
    get_aqueduct_source_column_map,
    get_supported_aqueduct_metric_slugs,
    load_crosswalk,
    load_metric_source_table,
)

TargetLevel = Literal["district", "basin", "sub_basin"]

DEFAULT_AREA_EPSG = 6933
INDIA_ALBERS_CRS = (
    "+proj=aea +lat_1=12 +lat_2=32 +lat_0=0 +lon_0=78 "
    "+datum=WGS84 +units=m +no_defs"
)

FIELD_CONTRACT: tuple[dict[str, str], ...] = tuple(
    row
    for slug in get_supported_aqueduct_metric_slugs()
    for row in get_aqueduct_metric_spec(slug).field_contract_rows()
)

FIELD_SEMANTICS_SOURCES: tuple[dict[str, str], ...] = (
    {
        "label": "Aqueduct 4.0 Technical Note",
        "url": "https://www.wri.org/research/aqueduct-40-updated-decision-relevant-global-water-risk-indicators",
        "note": "Primary methodology reference for Aqueduct 4.0 indicators and future scenarios.",
    },
    {
        "label": "Aqueduct 4.0 Current and Future Global Maps Data",
        "url": "https://www.wri.org/data/aqueduct-global-maps-40-data",
        "note": "Dataset landing page confirming baseline indicators and future annual projections.",
    },
    {
        "label": "Aqueduct FAQ",
        "url": "https://www.wri.org/aqueduct/faq",
        "note": "Clarifies that baseline quantity indicators use 1979-2019 and that Aqueduct is a screening tool.",
    },
)


def _default_aqueduct_dir() -> Path:
    return get_paths_config().data_dir / "aqueduct"


def _normalize_pfaf_id(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().fillna("")


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Output already exists (pass --overwrite): {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_text(text: str, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Output already exists (pass --overwrite): {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _target_columns(level: TargetLevel) -> tuple[list[str], str, str, str]:
    if level == "district":
        return (
            ["state_name", "district_name", "district_key"],
            "district_key",
            "district_name",
            "district_coverage_fraction",
        )
    if level == "sub_basin":
        return (
            ["basin_id", "basin_name", "subbasin_id", "subbasin_code", "subbasin_name"],
            "subbasin_id",
            "subbasin_name",
            "subbasin_coverage_fraction",
        )
    return (
        ["basin_id", "basin_name"],
        "basin_id",
        "basin_name",
        "basin_coverage_fraction",
    )


def build_field_semantics_markdown(metric_slug: str) -> str:
    """Return a markdown summary of one Aqueduct metric field contract."""
    spec = get_aqueduct_metric_spec(metric_slug)
    contract_rows = [row for row in FIELD_CONTRACT if row["metric_slug"] == spec.slug]
    lines = [
        "# Aqueduct Field Semantics Audit",
        "",
        f"This audit documents the source fields currently used to build the `{spec.slug}` district and hydro masters.",
        "",
        "## Current field contract",
        "",
        "| Dashboard column | Aqueduct source column | Dataset | Scenario | Period | Meaning |",
        "|---|---|---|---|---|---|",
    ]
    for row in contract_rows:
        lines.append(
            f"| `{row['output_column']}` | `{row['source_column']}` | `{row['source_dataset']}` | "
            f"`{row['scenario']}` | `{row['period']}` | {row['interpretation']} |"
        )

    lines.extend(
        [
            "",
            "## Interpretation notes",
            "",
            f"- The baseline reference currently used for dashboard comparison is `{spec.baseline_source_column}`.",
            f"- Future values currently come from the Aqueduct annual future fields `*_{spec.future_source_suffix}_x_r`.",
            f"- Dashboard deltas therefore represent **Aqueduct scenario value minus Aqueduct historical baseline** for `{spec.label}`, not a pure climate-only delta.",
            "- Aqueduct remains a screening/prioritization product and should be complemented by local or regional analysis.",
            "",
            "## Official sources",
            "",
        ]
    )
    for source in FIELD_SEMANTICS_SOURCES:
        lines.append(f"- [{source['label']}]({source['url']}): {source['note']}")
    return "\n".join(lines) + "\n"


def build_pfaf_cleaning_checks(
    *,
    baseline_path: Path,
    future_path: Path,
    qa_path: Path,
    area_epsg: int = DEFAULT_AREA_EPSG,
) -> pd.DataFrame:
    """Build per-`pfaf_id` integrity checks for the cleaned Aqueduct artifacts."""
    baseline_gdf = gpd.read_file(baseline_path)
    future_gdf = gpd.read_file(future_path)
    qa_df = pd.read_csv(qa_path)

    baseline = baseline_gdf[["pfaf_id", "geometry"]].copy()
    future = future_gdf[["pfaf_id", "geometry"]].copy()
    baseline["pfaf_id"] = _normalize_pfaf_id(baseline["pfaf_id"])
    future["pfaf_id"] = _normalize_pfaf_id(future["pfaf_id"])

    baseline_proj = baseline.to_crs(epsg=area_epsg)
    future_proj = future.to_crs(epsg=area_epsg)
    baseline["baseline_geometry_area_km2"] = baseline_proj.geometry.area / 1_000_000.0
    future["future_geometry_area_km2"] = future_proj.geometry.area / 1_000_000.0
    baseline["baseline_geometry_valid"] = baseline.geometry.is_valid.fillna(False)
    future["future_geometry_valid"] = future.geometry.is_valid.fillna(False)
    baseline["baseline_has_geometry"] = (~baseline.geometry.isna()) & (~baseline.geometry.is_empty)
    future["future_has_geometry"] = (~future.geometry.isna()) & (~future.geometry.is_empty)

    merged = baseline.merge(
        future[["pfaf_id", "geometry", "future_geometry_area_km2", "future_geometry_valid", "future_has_geometry"]],
        on="pfaf_id",
        how="outer",
        suffixes=("_baseline", "_future"),
    )

    def _geom_equal(row: pd.Series) -> bool:
        left = row.get("geometry_baseline")
        right = row.get("geometry_future")
        if left is None or right is None:
            return False
        if getattr(left, "is_empty", True) or getattr(right, "is_empty", True):
            return False
        try:
            return bool(left.equals(right))
        except Exception:
            return False

    merged["geometry_equal"] = merged.apply(_geom_equal, axis=1)
    merged["area_abs_diff_km2"] = (
        pd.to_numeric(merged["baseline_geometry_area_km2"], errors="coerce")
        - pd.to_numeric(merged["future_geometry_area_km2"], errors="coerce")
    ).abs()

    qa_df = qa_df.copy()
    qa_df["pfaf_id"] = _normalize_pfaf_id(qa_df["pfaf_id"])
    keep_qa_cols = ["pfaf_id", "baseline_area_km2_sum", "baseline_segment_count", "weighted_segment_count"]
    merged = merged.merge(qa_df[keep_qa_cols], on="pfaf_id", how="left")
    merged["baseline_area_vs_future_geom_abs_diff_km2"] = (
        pd.to_numeric(merged["baseline_area_km2_sum"], errors="coerce")
        - pd.to_numeric(merged["future_geometry_area_km2"], errors="coerce")
    ).abs()
    merged["status"] = "ok"
    merged.loc[~merged["geometry_equal"], "status"] = "geometry_mismatch"
    merged.loc[~merged["baseline_has_geometry"].fillna(False), "status"] = "baseline_missing_geometry"
    merged.loc[~merged["future_has_geometry"].fillna(False), "status"] = "future_missing_geometry"
    return merged.drop(columns=["geometry_baseline", "geometry_future"], errors="ignore").sort_values("pfaf_id").reset_index(drop=True)


def build_crosswalk_conservation_summary(
    crosswalk_df: pd.DataFrame,
    *,
    hydro_level: TargetLevel,
) -> pd.DataFrame:
    """Summarize per-`pfaf_id` conservation behavior for one crosswalk."""
    if hydro_level == "district":
        pfaf_fraction_col = "pfaf_area_fraction_in_district"
    else:
        pfaf_fraction_col = "pfaf_area_fraction_in_subbasin" if hydro_level == "sub_basin" else "pfaf_area_fraction_in_basin"
    grouped = (
        crosswalk_df.groupby("pfaf_id", dropna=False, as_index=False)
        .agg(
            pfaf_area_km2=("pfaf_area_km2", "first"),
            target_count=("pfaf_id", "size"),
            summed_intersection_area_km2=("intersection_area_km2", "sum"),
            summed_pfaf_fraction=(pfaf_fraction_col, "sum"),
        )
    )
    grouped["pfaf_area_residual_km2"] = grouped["pfaf_area_km2"] - grouped["summed_intersection_area_km2"]
    grouped["pfaf_fraction_abs_diff_from_1"] = (grouped["summed_pfaf_fraction"] - 1.0).abs()
    grouped["status"] = "near_full"
    grouped.loc[grouped["summed_pfaf_fraction"] < 0.98, "status"] = "partial_target_coverage"
    grouped.loc[grouped["summed_pfaf_fraction"] > 1.02, "status"] = "over_coverage_check"
    return grouped.sort_values(["status", "pfaf_fraction_abs_diff_from_1", "pfaf_id"], ascending=[True, False, True]).reset_index(drop=True)


def classify_reliability_tiers(
    qa_df: pd.DataFrame,
    *,
    hydro_level: TargetLevel,
) -> pd.DataFrame:
    """Assign reliability tiers from the master QA coverage fractions."""
    _, target_id_col, target_name_col, coverage_col = _target_columns(hydro_level)
    out = qa_df.copy()
    out[coverage_col] = pd.to_numeric(out[coverage_col], errors="coerce").fillna(0.0)
    out["reliability_tier"] = "low"
    out.loc[out[coverage_col] >= 0.50, "reliability_tier"] = "moderate"
    out.loc[out[coverage_col] >= 0.90, "reliability_tier"] = "high"
    order = {"high": 0, "moderate": 1, "low": 2}
    out["__tier_order"] = out["reliability_tier"].map(order).fillna(9)
    return out.sort_values(["__tier_order", coverage_col, target_name_col], ascending=[True, False, True]).drop(columns="__tier_order").reset_index(drop=True)


def _build_crosswalk_with_area_crs(
    aqueduct_gdf: gpd.GeoDataFrame,
    hydro_gdf: gpd.GeoDataFrame,
    *,
    hydro_level: TargetLevel,
    area_crs: Any,
) -> pd.DataFrame:
    """Compute an overlap table with a configurable equal-area CRS."""
    if hydro_level == "district":
        return _build_district_crosswalk_with_area_crs(aqueduct_gdf, hydro_gdf, area_crs=area_crs)
    aqueduct_proj = aqueduct_gdf.to_crs(area_crs).copy()
    hydro_proj = hydro_gdf.to_crs(area_crs).copy()
    target_area_col = "subbasin_area_km2" if hydro_level == "sub_basin" else "basin_area_km2"
    if hydro_level == "sub_basin":
        hydro_cols = ["basin_id", "basin_name", "subbasin_id", "subbasin_code", "subbasin_name", "geometry"]
        group_cols = ["pfaf_id", "basin_id", "basin_name", "subbasin_id", "subbasin_code", "subbasin_name"]
    else:
        hydro_cols = ["basin_id", "basin_name", "geometry"]
        group_cols = ["pfaf_id", "basin_id", "basin_name"]

    aqueduct_proj["pfaf_area_km2"] = aqueduct_proj.geometry.area / 1_000_000.0
    hydro_proj[target_area_col] = hydro_proj.geometry.area / 1_000_000.0

    intersections = gpd.overlay(
        aqueduct_proj[["pfaf_id", "pfaf_area_km2", "geometry"]],
        hydro_proj[hydro_cols + [target_area_col]],
        how="intersection",
    )
    if intersections.empty:
        return pd.DataFrame()
    intersections["intersection_area_km2"] = intersections.geometry.area / 1_000_000.0
    intersections = intersections.loc[intersections["intersection_area_km2"] > 0].copy()
    if intersections.empty:
        return pd.DataFrame()
    return (
        intersections.groupby(group_cols + ["pfaf_area_km2", target_area_col], as_index=False, dropna=False)["intersection_area_km2"]
        .sum()
        .reset_index(drop=True)
    )


def _build_district_crosswalk_with_area_crs(
    aqueduct_gdf: gpd.GeoDataFrame,
    district_gdf: gpd.GeoDataFrame,
    *,
    area_crs: Any,
) -> pd.DataFrame:
    """Compute an Aqueduct-to-district overlap table with a configurable area CRS."""
    aqueduct_proj = aqueduct_gdf.to_crs(area_crs).copy()
    district_proj = district_gdf.to_crs(area_crs).copy()
    aqueduct_proj["pfaf_area_km2"] = aqueduct_proj.geometry.area / 1_000_000.0
    district_proj["district_area_km2"] = district_proj.geometry.area / 1_000_000.0
    intersections = gpd.overlay(
        aqueduct_proj[["pfaf_id", "pfaf_area_km2", "geometry"]],
        district_proj[["state_name", "district_name", "district_key", "district_area_km2", "geometry"]],
        how="intersection",
    )
    if intersections.empty:
        return pd.DataFrame()
    intersections["intersection_area_km2"] = intersections.geometry.area / 1_000_000.0
    intersections = intersections.loc[intersections["intersection_area_km2"] > 0].copy()
    if intersections.empty:
        return pd.DataFrame()
    grouped = (
        intersections.groupby(
            ["pfaf_id", "pfaf_area_km2", "state_name", "district_name", "district_key", "district_area_km2"],
            as_index=False,
            dropna=False,
        )["intersection_area_km2"]
        .sum()
        .reset_index(drop=True)
    )
    grouped["pfaf_area_fraction_in_district"] = grouped["intersection_area_km2"] / grouped["pfaf_area_km2"]
    grouped["district_area_fraction_in_pfaf"] = grouped["intersection_area_km2"] / grouped["district_area_km2"]
    return grouped


def build_projection_sensitivity_summary(
    *,
    source_df: pd.DataFrame,
    aqueduct_gdf: gpd.GeoDataFrame,
    hydro_gdf: gpd.GeoDataFrame,
    base_master_df: pd.DataFrame,
    hydro_level: TargetLevel,
    source_column_map: dict[str, str],
) -> pd.DataFrame:
    """Compare the reference transfer against alternate projections and a dominant-overlap rule."""
    target_keep_cols, target_id_col, target_name_col, _ = _target_columns(hydro_level)
    alt_crosswalk = _build_crosswalk_with_area_crs(
        aqueduct_gdf,
        hydro_gdf,
        hydro_level=hydro_level,
        area_crs=INDIA_ALBERS_CRS,
    )
    if hydro_level == "district":
        alt_master_df, _ = aggregate_crosswalk_to_districts(
            source_df=source_df,
            crosswalk_df=alt_crosswalk,
            source_column_map=source_column_map,
        )
        alt_master_df = alt_master_df.rename(columns={"state": "state_name", "district": "district_name"})
    else:
        alt_master_df, _ = aggregate_crosswalk_to_targets(
            source_df=source_df,
            crosswalk_df=alt_crosswalk,
            target_gdf=hydro_gdf,
            hydro_level=hydro_level,
            source_column_map=source_column_map,
            area_epsg=DEFAULT_AREA_EPSG,
        )

    records: list[dict[str, object]] = []
    value_columns = list(source_column_map)
    base_lookup = base_master_df.set_index(target_keep_cols)
    alt_lookup = alt_master_df.set_index(target_keep_cols)

    for key, row in base_lookup.iterrows():
        key_tuple = key if isinstance(key, tuple) else (key,)
        key_map = dict(zip(target_keep_cols, key_tuple))
        alt_row = alt_lookup.loc[key] if key in alt_lookup.index else pd.Series(dtype=object)

        for column in value_columns:
            base_val = pd.to_numeric(row.get(column), errors="coerce")
            alt_val = pd.to_numeric(alt_row.get(column), errors="coerce") if not alt_row.empty else pd.NA
            if pd.notna(base_val) and pd.notna(alt_val):
                abs_diff = abs(float(base_val) - float(alt_val))
                rel_diff_pct = abs_diff / abs(float(base_val)) * 100.0 if float(base_val) != 0.0 else pd.NA
            else:
                abs_diff = pd.NA
                rel_diff_pct = pd.NA
            records.append(
                {
                    **key_map,
                    "hydro_level": hydro_level,
                    "comparison": "alt_equal_area_vs_epsg6933",
                    "metric_column": column,
                    "base_value": base_val,
                    "comparison_value": alt_val,
                    "abs_diff": abs_diff,
                    "rel_diff_pct": rel_diff_pct,
                }
            )

    base_crosswalk = _build_crosswalk_with_area_crs(
        aqueduct_gdf,
        hydro_gdf,
        hydro_level=hydro_level,
        area_crs=DEFAULT_AREA_EPSG,
    )
    overlaps = base_crosswalk.merge(source_df, on="pfaf_id", how="left", validate="many_to_one")
    for key, grp in overlaps.groupby(target_keep_cols, dropna=False):
        key_tuple = key if isinstance(key, tuple) else (key,)
        key_map = dict(zip(target_keep_cols, key_tuple))
        try:
            base_row = base_lookup.loc[key]
        except KeyError:
            continue
        for column in value_columns:
            valid = grp.loc[grp[column].notna() & grp["intersection_area_km2"].gt(0)].copy()
            if valid.empty:
                dominant_val = pd.NA
                abs_diff = pd.NA
                rel_diff_pct = pd.NA
            else:
                valid = valid.sort_values(["intersection_area_km2", "pfaf_id"], ascending=[False, True])
                dominant_val = pd.to_numeric(valid.iloc[0][column], errors="coerce")
                base_val = pd.to_numeric(base_row.get(column), errors="coerce")
                if pd.notna(base_val) and pd.notna(dominant_val):
                    abs_diff = abs(float(base_val) - float(dominant_val))
                    rel_diff_pct = abs_diff / abs(float(base_val)) * 100.0 if float(base_val) != 0.0 else pd.NA
                else:
                    abs_diff = pd.NA
                    rel_diff_pct = pd.NA
            records.append(
                {
                    **key_map,
                    "hydro_level": hydro_level,
                    "comparison": "dominant_overlap_vs_weighted",
                    "metric_column": column,
                    "base_value": pd.to_numeric(base_row.get(column), errors="coerce"),
                    "comparison_value": dominant_val,
                    "abs_diff": abs_diff,
                    "rel_diff_pct": rel_diff_pct,
                }
            )

    return pd.DataFrame(records).sort_values(["hydro_level", "comparison", target_name_col, "metric_column"]).reset_index(drop=True)


def select_sample_audit_units(
    reliability_df: pd.DataFrame,
    *,
    hydro_level: TargetLevel,
    per_tier: int = 3,
) -> pd.DataFrame:
    """Select deterministic sample units across reliability tiers."""
    _, target_id_col, target_name_col, coverage_col = _target_columns(hydro_level)
    out_rows: list[pd.DataFrame] = []
    df = reliability_df.copy()
    for tier in ("high", "moderate", "low"):
        subset = df[df["reliability_tier"] == tier].copy()
        if subset.empty:
            continue
        subset = subset.sort_values([coverage_col, target_name_col], ascending=[tier != "high", True]).head(per_tier)
        subset = subset.assign(hydro_level=hydro_level, audit_reason=f"{tier}_coverage_sample")
        out_rows.append(subset[[target_id_col, target_name_col, coverage_col, "source_pfaf_count", "hydro_level", "audit_reason"]])
    if not out_rows:
        return pd.DataFrame(columns=[target_id_col, target_name_col, coverage_col, "source_pfaf_count", "hydro_level", "audit_reason"])
    return pd.concat(out_rows, ignore_index=True)


def build_master_value_spotcheck(
    *,
    sample_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    source_df: pd.DataFrame,
    master_df: pd.DataFrame,
    hydro_level: TargetLevel,
    metric_columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Recompute master values for sampled units and compare against the written masters."""
    target_keep_cols, target_id_col, target_name_col, _ = _target_columns(hydro_level)
    overlaps = crosswalk_df.merge(source_df, on="pfaf_id", how="left", validate="many_to_one")
    master_lookup = master_df.set_index(target_keep_cols)
    sample_ids = set(sample_df[target_id_col].astype(str).tolist())
    overlaps = overlaps[overlaps[target_id_col].astype(str).isin(sample_ids)].copy()
    requested_columns = list(metric_columns) if metric_columns is not None else [
        column for column in master_df.columns if column in overlaps.columns
    ]
    value_columns = [column for column in requested_columns if column in overlaps.columns and column in master_df.columns]

    records: list[dict[str, object]] = []
    for key, grp in overlaps.groupby(target_keep_cols, dropna=False):
        key_tuple = key if isinstance(key, tuple) else (key,)
        key_map = dict(zip(target_keep_cols, key_tuple))
        try:
            master_row = master_lookup.loc[key]
        except KeyError:
            continue
        contributor_ids = ",".join(sorted(grp["pfaf_id"].astype(str).unique().tolist()))
        for column in value_columns:
            valid = grp.loc[grp[column].notna() & grp["intersection_area_km2"].gt(0), ["pfaf_id", "intersection_area_km2", column]].copy()
            if valid.empty:
                recomputed = pd.NA
                dominant_pfaf = ""
                dominant_share = pd.NA
            else:
                weighted = (pd.to_numeric(valid[column], errors="coerce") * valid["intersection_area_km2"]).sum()
                weight = valid["intersection_area_km2"].sum()
                recomputed = weighted / weight if weight > 0 else pd.NA
                valid = valid.sort_values(["intersection_area_km2", "pfaf_id"], ascending=[False, True])
                dominant_pfaf = str(valid.iloc[0]["pfaf_id"])
                dominant_share = valid.iloc[0]["intersection_area_km2"] / weight if weight > 0 else pd.NA
            master_val = pd.to_numeric(master_row.get(column), errors="coerce")
            abs_diff = abs(float(master_val) - float(recomputed)) if pd.notna(master_val) and pd.notna(recomputed) else pd.NA
            records.append(
                {
                    **key_map,
                    "hydro_level": hydro_level,
                    "metric_column": column,
                    "master_value": master_val,
                    "recomputed_weighted_value": recomputed,
                    "abs_diff": abs_diff,
                    "dominant_pfaf_id": dominant_pfaf,
                    "dominant_overlap_share_of_valid_weight": dominant_share,
                    "contributing_pfaf_ids": contributor_ids,
                }
            )
    return pd.DataFrame(records).sort_values(["hydro_level", target_name_col, "metric_column"]).reset_index(drop=True)


def build_validation_summary_markdown(
    *,
    metric_slug: str,
    pfaf_checks_df: pd.DataFrame,
    district_conservation_df: pd.DataFrame,
    basin_conservation_df: pd.DataFrame,
    subbasin_conservation_df: pd.DataFrame,
    district_reliability_df: pd.DataFrame,
    basin_reliability_df: pd.DataFrame,
    subbasin_reliability_df: pd.DataFrame,
    sensitivity_df: pd.DataFrame,
    spotcheck_df: pd.DataFrame,
) -> str:
    """Summarize the validation outputs in markdown."""
    district_tiers = district_reliability_df["reliability_tier"].value_counts().to_dict() if not district_reliability_df.empty else {}
    basin_tiers = basin_reliability_df["reliability_tier"].value_counts().to_dict() if not basin_reliability_df.empty else {}
    subbasin_tiers = subbasin_reliability_df["reliability_tier"].value_counts().to_dict() if not subbasin_reliability_df.empty else {}
    district_cons = district_conservation_df.copy()
    basin_cons = basin_conservation_df.copy()
    subbasin_cons = subbasin_conservation_df.copy()
    district_cons["pfaf_fraction_abs_diff_from_1"] = pd.to_numeric(
        district_cons.get("pfaf_fraction_abs_diff_from_1"),
        errors="coerce",
    )
    basin_cons["pfaf_fraction_abs_diff_from_1"] = pd.to_numeric(
        basin_cons.get("pfaf_fraction_abs_diff_from_1"),
        errors="coerce",
    )
    subbasin_cons["pfaf_fraction_abs_diff_from_1"] = pd.to_numeric(
        subbasin_cons.get("pfaf_fraction_abs_diff_from_1"),
        errors="coerce",
    )
    worst_basin_conservation = basin_cons.dropna(subset=["pfaf_fraction_abs_diff_from_1"]).nlargest(
        5,
        "pfaf_fraction_abs_diff_from_1",
    )[["pfaf_id", "pfaf_fraction_abs_diff_from_1"]]
    worst_district_conservation = district_cons.dropna(subset=["pfaf_fraction_abs_diff_from_1"]).nlargest(
        5,
        "pfaf_fraction_abs_diff_from_1",
    )[["pfaf_id", "pfaf_fraction_abs_diff_from_1"]]
    worst_subbasin_conservation = subbasin_cons.dropna(subset=["pfaf_fraction_abs_diff_from_1"]).nlargest(
        5,
        "pfaf_fraction_abs_diff_from_1",
    )[["pfaf_id", "pfaf_fraction_abs_diff_from_1"]]

    sensitivity_num = sensitivity_df.copy()
    if not sensitivity_num.empty:
        sensitivity_num["abs_diff"] = pd.to_numeric(sensitivity_num.get("abs_diff"), errors="coerce")
        worst_sensitivity = sensitivity_num.dropna(subset=["abs_diff"]).nlargest(10, "abs_diff")
    else:
        worst_sensitivity = pd.DataFrame()

    max_spotcheck = (
        float(pd.to_numeric(spotcheck_df["abs_diff"], errors="coerce").max()) if not spotcheck_df.empty else 0.0
    )
    geometry_mismatches = int((pfaf_checks_df["status"] != "ok").sum()) if not pfaf_checks_df.empty else 0

    lines = [
        "# Aqueduct Workflow Validation Summary",
        "",
        "## Headline checks",
        "",
        f"- Metric slug: `{metric_slug}`",
        f"- `pfaf_id` rows checked: {len(pfaf_checks_df)}",
        f"- `pfaf_id` rows with non-OK status: {geometry_mismatches}",
        f"- District reliability tiers: {district_tiers}",
        f"- Basin reliability tiers: {basin_tiers}",
        f"- Sub-basin reliability tiers: {subbasin_tiers}",
        f"- Maximum sampled-unit master recomputation difference: {max_spotcheck:.12f}",
        "",
        "## Largest conservation deviations",
        "",
        "### District crosswalk",
        "",
    ]
    if worst_district_conservation.empty:
        lines.append("- No district conservation deviations found.")
    else:
        for _, row in worst_district_conservation.iterrows():
            lines.append(f"- `{row['pfaf_id']}`: fraction diff from 1 = {row['pfaf_fraction_abs_diff_from_1']:.6f}")
    lines.extend(
        [
            "",
        "### Basin crosswalk",
        "",
        ]
    )
    if worst_basin_conservation.empty:
        lines.append("- No basin conservation deviations found.")
    else:
        for _, row in worst_basin_conservation.iterrows():
            lines.append(f"- `{row['pfaf_id']}`: fraction diff from 1 = {row['pfaf_fraction_abs_diff_from_1']:.6f}")
    lines.extend(["", "### Sub-basin crosswalk", ""])
    if worst_subbasin_conservation.empty:
        lines.append("- No sub-basin conservation deviations found.")
    else:
        for _, row in worst_subbasin_conservation.iterrows():
            lines.append(f"- `{row['pfaf_id']}`: fraction diff from 1 = {row['pfaf_fraction_abs_diff_from_1']:.6f}")

    lines.extend(["", "## Largest sensitivity differences", ""])
    if worst_sensitivity.empty:
        lines.append("- No sensitivity deltas were computed.")
    else:
        for _, row in worst_sensitivity.iterrows():
            location_name = row.get("district_name", row.get("basin_name", row.get("subbasin_name", "unknown")))
            lines.append(
                f"- `{row['hydro_level']}` `{row.get('metric_column', '')}` `{row.get('comparison', '')}` "
                f"at `{location_name}`: abs diff {float(row['abs_diff']):.6f}"
            )

    lines.extend(
        [
            "",
            "## Interpretation note",
            "",
            "- This validation package checks internal consistency, transfer robustness, and coverage behavior.",
            "- It does not by itself validate Aqueduct as an observational truth surface; Aqueduct remains a screening tool and should be paired with local review for high-stakes decisions.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate the Aqueduct district + SOI hydro workflow.")
    aqueduct_dir = _default_aqueduct_dir()
    parser.add_argument("--baseline", type=str, default=str(aqueduct_dir / "baseline_clean_india.geojson"))
    parser.add_argument("--future", type=str, default=str(aqueduct_dir / "future_annual_india.geojson"))
    parser.add_argument("--baseline-qa", type=str, default=str(aqueduct_dir / "baseline_clean_india_qa.csv"))
    parser.add_argument("--district-crosswalk", type=str, default=str(aqueduct_dir / "aqueduct_district_crosswalk.csv"))
    parser.add_argument("--basin-crosswalk", type=str, default=str(aqueduct_dir / "aqueduct_basin_crosswalk.csv"))
    parser.add_argument("--subbasin-crosswalk", type=str, default=str(aqueduct_dir / "aqueduct_subbasin_crosswalk.csv"))
    parser.add_argument("--districts", type=str, default=str(get_paths_config().districts_path))
    parser.add_argument("--basins", type=str, default=str(BASINS_PATH))
    parser.add_argument("--subbasins", type=str, default=str(SUBBASINS_PATH))
    parser.add_argument(
        "--metric-slug",
        action="append",
        default=None,
        help=(
            "Aqueduct metric slug to validate. Repeat for multiple metrics, or pass `all` "
            f"to validate all supported Aqueduct metrics ({', '.join(get_supported_aqueduct_metric_slugs())})."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(aqueduct_dir / "validation"),
        help="Directory for validation outputs.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite validation outputs.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    baseline_path = Path(args.baseline).expanduser().resolve()
    future_path = Path(args.future).expanduser().resolve()
    baseline_qa_path = Path(args.baseline_qa).expanduser().resolve()
    district_crosswalk_path = Path(args.district_crosswalk).expanduser().resolve()
    basin_crosswalk_path = Path(args.basin_crosswalk).expanduser().resolve()
    subbasin_crosswalk_path = Path(args.subbasin_crosswalk).expanduser().resolve()
    districts_path = Path(args.districts).expanduser().resolve()
    basins_path = Path(args.basins).expanduser().resolve()
    subbasins_path = Path(args.subbasins).expanduser().resolve()
    output_root = Path(args.output_dir).expanduser().resolve()
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

    required = [
        baseline_path,
        future_path,
        baseline_qa_path,
        district_crosswalk_path,
        basin_crosswalk_path,
        subbasin_crosswalk_path,
        districts_path,
        basins_path,
        subbasins_path,
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(f"Required validation input not found: {path}")

    pfaf_checks_df = build_pfaf_cleaning_checks(
        baseline_path=baseline_path,
        future_path=future_path,
        qa_path=baseline_qa_path,
    )

    district_crosswalk_df = load_district_crosswalk(district_crosswalk_path)
    basin_crosswalk_df = load_crosswalk(basin_crosswalk_path, hydro_level="basin")
    subbasin_crosswalk_df = load_crosswalk(subbasin_crosswalk_path, hydro_level="sub_basin")
    district_conservation_df = build_crosswalk_conservation_summary(district_crosswalk_df, hydro_level="district")
    basin_conservation_df = build_crosswalk_conservation_summary(basin_crosswalk_df, hydro_level="basin")
    subbasin_conservation_df = build_crosswalk_conservation_summary(subbasin_crosswalk_df, hydro_level="sub_basin")

    aqueduct_gdf = load_aqueduct_boundaries(baseline_path)
    district_gdf = load_district_boundaries(districts_path)
    basin_gdf = load_soi_hydro_boundaries(basins_path, level="basin")
    subbasin_gdf = load_soi_hydro_boundaries(subbasins_path, level="sub_basin")
    print("AQUEDUCT VALIDATION")
    print(f"output_root: {output_root}")
    print(f"metric_slugs: {', '.join(metric_slugs)}")
    for metric_slug in metric_slugs:
        source_column_map = get_aqueduct_source_column_map(metric_slug)
        district_master_qa_path = _default_aqueduct_dir() / f"{metric_slug}_district_master_qa.csv"
        basin_master_qa_path = _default_aqueduct_dir() / f"{metric_slug}_basin_master_qa.csv"
        subbasin_master_qa_path = _default_aqueduct_dir() / f"{metric_slug}_subbasin_master_qa.csv"
        if not district_master_qa_path.exists():
            raise FileNotFoundError(f"Required district QA for validation not found: {district_master_qa_path}")
        if not basin_master_qa_path.exists():
            raise FileNotFoundError(f"Required basin QA for validation not found: {basin_master_qa_path}")
        if not subbasin_master_qa_path.exists():
            raise FileNotFoundError(f"Required sub-basin QA for validation not found: {subbasin_master_qa_path}")

        source_df = load_metric_source_table(
            baseline_path=baseline_path,
            future_path=future_path,
            source_column_map=source_column_map,
        )
        district_master_qa_df = pd.read_csv(district_master_qa_path)
        basin_master_qa_df = pd.read_csv(basin_master_qa_path)
        subbasin_master_qa_df = pd.read_csv(subbasin_master_qa_path)
        district_reliability_df = classify_reliability_tiers(district_master_qa_df, hydro_level="district")
        basin_reliability_df = classify_reliability_tiers(basin_master_qa_df, hydro_level="basin")
        subbasin_reliability_df = classify_reliability_tiers(subbasin_master_qa_df, hydro_level="sub_basin")
        district_samples = select_sample_audit_units(district_reliability_df, hydro_level="district")
        basin_samples = select_sample_audit_units(basin_reliability_df, hydro_level="basin")
        subbasin_samples = select_sample_audit_units(subbasin_reliability_df, hydro_level="sub_basin")
        sample_df = pd.concat([district_samples, basin_samples, subbasin_samples], ignore_index=True)
        district_master_root = get_paths_config().data_dir / "processed" / metric_slug
        district_master_paths = sorted(district_master_root.glob("*/master_metrics_by_district.csv"))
        if not district_master_paths:
            raise FileNotFoundError(f"Required district masters for validation not found under: {district_master_root}")
        district_master_frames = [pd.read_csv(path) for path in district_master_paths]
        district_master_df = pd.concat(district_master_frames, ignore_index=True)
        district_master_df = district_master_df.rename(columns={"state": "state_name", "district": "district_name"})
        if "district_key" not in district_master_df.columns:
            district_master_df["district_key"] = (
                district_master_df["state_name"].astype(str).str.strip()
                + "::"
                + district_master_df["district_name"].astype(str).str.strip()
            )
        basin_master_path = get_paths_config().data_dir / "processed" / metric_slug / "hydro" / "master_metrics_by_basin.csv"
        subbasin_master_path = get_paths_config().data_dir / "processed" / metric_slug / "hydro" / "master_metrics_by_sub_basin.csv"
        if not basin_master_path.exists():
            raise FileNotFoundError(f"Required basin master for validation not found: {basin_master_path}")
        if not subbasin_master_path.exists():
            raise FileNotFoundError(f"Required sub-basin master for validation not found: {subbasin_master_path}")

        basin_master_df = pd.read_csv(basin_master_path)
        subbasin_master_df = pd.read_csv(subbasin_master_path)

        district_sensitivity_df = build_projection_sensitivity_summary(
            source_df=source_df,
            aqueduct_gdf=aqueduct_gdf,
            hydro_gdf=district_gdf,
            base_master_df=district_master_df,
            hydro_level="district",
            source_column_map=source_column_map,
        )
        basin_sensitivity_df = build_projection_sensitivity_summary(
            source_df=source_df,
            aqueduct_gdf=aqueduct_gdf,
            hydro_gdf=basin_gdf,
            base_master_df=basin_master_df,
            hydro_level="basin",
            source_column_map=source_column_map,
        )
        subbasin_sensitivity_df = build_projection_sensitivity_summary(
            source_df=source_df,
            aqueduct_gdf=aqueduct_gdf,
            hydro_gdf=subbasin_gdf,
            base_master_df=subbasin_master_df,
            hydro_level="sub_basin",
            source_column_map=source_column_map,
        )
        sensitivity_df = pd.concat([district_sensitivity_df, basin_sensitivity_df, subbasin_sensitivity_df], ignore_index=True)

        district_spotcheck = build_master_value_spotcheck(
            sample_df=district_samples,
            crosswalk_df=district_crosswalk_df,
            source_df=source_df,
            master_df=district_master_df,
            hydro_level="district",
            metric_columns=list(source_column_map),
        )
        basin_spotcheck = build_master_value_spotcheck(
            sample_df=basin_samples,
            crosswalk_df=basin_crosswalk_df,
            source_df=source_df,
            master_df=basin_master_df,
            hydro_level="basin",
            metric_columns=list(source_column_map),
        )
        subbasin_spotcheck = build_master_value_spotcheck(
            sample_df=subbasin_samples,
            crosswalk_df=subbasin_crosswalk_df,
            source_df=source_df,
            master_df=subbasin_master_df,
            hydro_level="sub_basin",
            metric_columns=list(source_column_map),
        )
        spotcheck_df = pd.concat([district_spotcheck, basin_spotcheck, subbasin_spotcheck], ignore_index=True)

        summary_md = build_validation_summary_markdown(
            metric_slug=metric_slug,
            pfaf_checks_df=pfaf_checks_df,
            district_conservation_df=district_conservation_df,
            basin_conservation_df=basin_conservation_df,
            subbasin_conservation_df=subbasin_conservation_df,
            district_reliability_df=district_reliability_df,
            basin_reliability_df=basin_reliability_df,
            subbasin_reliability_df=subbasin_reliability_df,
            sensitivity_df=sensitivity_df,
            spotcheck_df=spotcheck_df,
        )

        metric_output_dir = output_root / metric_slug
        _write_text(build_field_semantics_markdown(metric_slug), metric_output_dir / "field_semantics_audit.md", overwrite=bool(args.overwrite))
        _write_csv(pfaf_checks_df, metric_output_dir / "pfaf_cleaning_checks.csv", overwrite=bool(args.overwrite))
        _write_csv(district_conservation_df, metric_output_dir / "crosswalk_conservation_district.csv", overwrite=bool(args.overwrite))
        _write_csv(basin_conservation_df, metric_output_dir / "crosswalk_conservation_basin.csv", overwrite=bool(args.overwrite))
        _write_csv(subbasin_conservation_df, metric_output_dir / "crosswalk_conservation_subbasin.csv", overwrite=bool(args.overwrite))
        _write_csv(district_reliability_df, metric_output_dir / "coverage_reliability_district.csv", overwrite=bool(args.overwrite))
        _write_csv(basin_reliability_df, metric_output_dir / "coverage_reliability_basin.csv", overwrite=bool(args.overwrite))
        _write_csv(subbasin_reliability_df, metric_output_dir / "coverage_reliability_subbasin.csv", overwrite=bool(args.overwrite))
        _write_csv(sensitivity_df, metric_output_dir / "projection_sensitivity_summary.csv", overwrite=bool(args.overwrite))
        _write_csv(sample_df, metric_output_dir / "sample_audit_units.csv", overwrite=bool(args.overwrite))
        _write_csv(district_samples, metric_output_dir / "district_sample_audit_units.csv", overwrite=bool(args.overwrite))
        _write_csv(spotcheck_df, metric_output_dir / "master_value_spotcheck.csv", overwrite=bool(args.overwrite))
        _write_csv(district_spotcheck, metric_output_dir / "district_master_value_spotcheck.csv", overwrite=bool(args.overwrite))
        _write_text(summary_md, metric_output_dir / "validation_summary.md", overwrite=bool(args.overwrite))

        print(f"metric_slug: {metric_slug}")
        print(f"output_dir: {metric_output_dir}")
        print(f"pfaf_rows: {len(pfaf_checks_df)}")
        print(f"district_samples: {len(district_samples)}")
        print(f"basin_samples: {len(basin_samples)}")
        print(f"subbasin_samples: {len(subbasin_samples)}")
        print(f"sensitivity_rows: {len(sensitivity_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

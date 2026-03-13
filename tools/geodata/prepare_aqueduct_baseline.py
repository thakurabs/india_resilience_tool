#!/usr/bin/env python3
"""
Build a clean Aqueduct baseline layer using future geometry as the spatial base.

This tool intentionally ignores the geometry in Aqueduct's ``baseline_annual``
product. Instead, it:

1. Loads the clean ``future_annual`` HydroBASINS geometry keyed by ``pfaf_id``.
2. Reads baseline attributes from the CSV export.
3. Filters to the requested scope (India by default).
4. Aggregates segmented baseline rows to one row per ``pfaf_id`` using
   ``area_km2`` weights.
5. Writes a canonical clean GeoJSON and a QA CSV.

The resulting artifact is designed as the canonical Aqueduct baseline source
for later SOI basin/sub-basin crosswalking.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, Optional

import geopandas as gpd
import pandas as pd
import shapely
from shapely.geometry import MultiPolygon, Polygon

from paths import get_paths_config


Scope = str
_SCOPE_INDIA = "india"
_SCOPE_GLOBAL = "global"
_VALID_SCOPES = (_SCOPE_INDIA, _SCOPE_GLOBAL)
_INVALID_NUMERIC_SENTINELS = {-9999.0, 9999.0}
_DROP_BASELINE_COLUMNS = {
    "string_id",
    "aq30_id",
    "gid_1",
    "aqid",
    "gid_0",
    "name_0",
    "name_1",
}


def _default_output_dir() -> Path:
    """Return the default Aqueduct artifact directory under ``IRT_DATA_DIR``."""
    return get_paths_config().data_dir / "aqueduct"


def _normalize_string_series(series: pd.Series) -> pd.Series:
    """Normalize a text-like series into trimmed pandas string dtype."""
    return series.astype("string").str.strip()


def _canonicalize_pfaf_value(value: object) -> str:
    """Normalize a pfaf identifier so CSV and GDB values join deterministically."""
    if value is None:
        return ""
    if pd.isna(value):
        return ""

    text = str(value).strip()
    if not text:
        return ""

    if re.fullmatch(r"-?\d+\.0+", text):
        return text.split(".", 1)[0]

    return text


def _normalize_pfaf_id_series(series: pd.Series) -> pd.Series:
    """Return trimmed ``pfaf_id`` values with nulls preserved."""
    return series.map(_canonicalize_pfaf_value).astype("string").fillna("")


def _ensure_epsg4326(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ensure a GeoDataFrame is in EPSG:4326."""
    if gdf.crs is None:
        raise ValueError("Aqueduct geometry source has no CRS.")
    return gdf.to_crs(epsg=4326)


def _validate_areal_geometries(gdf: gpd.GeoDataFrame, *, label: str) -> None:
    """Raise on null, empty, or non-areal geometries."""
    if "geometry" not in gdf.columns:
        raise ValueError(f"{label} has no geometry column.")
    if gdf.geometry.isna().any():
        raise ValueError(f"{label} contains null geometries.")
    if gdf.geometry.is_empty.any():
        raise ValueError(f"{label} contains empty geometries.")
    bad = ~gdf.geom_type.isin(["Polygon", "MultiPolygon"])
    if bad.any():
        bad_types = sorted(gdf.loc[bad].geom_type.astype(str).unique().tolist())
        raise ValueError(f"{label} contains non-areal geometries: {bad_types}.")
    invalid = ~gdf.geometry.is_valid
    if invalid.any():
        raise ValueError(
            f"{label} contains invalid geometries for pfaf_id values: "
            f"{gdf.loc[invalid, 'pfaf_id'].astype(str).tolist()[:10]}"
        )


def _coerce_to_areal_geometry(geometry: object) -> Polygon | MultiPolygon:
    """Return a polygonal geometry after repair."""
    if geometry is None:
        raise ValueError("Encountered null geometry during repair.")

    geom_type = getattr(geometry, "geom_type", None)
    if geom_type == "Polygon":
        return geometry
    if geom_type == "MultiPolygon":
        return geometry
    if geom_type != "GeometryCollection":
        raise ValueError(f"Repair produced non-areal geometry type: {geom_type}.")

    polygons: list[Polygon] = []
    for part in geometry.geoms:
        if isinstance(part, Polygon):
            polygons.append(part)
        elif isinstance(part, MultiPolygon):
            polygons.extend(list(part.geoms))

    if not polygons:
        raise ValueError(
            "Repair produced a GeometryCollection without polygonal parts."
        )

    if len(polygons) == 1:
        return polygons[0]
    return MultiPolygon(polygons)


def _repair_invalid_areal_geometries(
    gdf: gpd.GeoDataFrame,
    *,
    label: str,
) -> gpd.GeoDataFrame:
    """Repair invalid polygon geometries using shapely.make_valid."""
    invalid_mask = ~gdf.geometry.is_valid
    if not invalid_mask.any():
        return gdf.copy()

    repaired = gdf.copy()
    repaired_values = shapely.make_valid(
        repaired.loc[invalid_mask, "geometry"].values
    )
    repaired.loc[invalid_mask, "geometry"] = [
        _coerce_to_areal_geometry(geometry) for geometry in repaired_values
    ]
    _validate_areal_geometries(repaired, label=label)
    return repaired


def _numeric_metric_series(series: pd.Series) -> pd.Series:
    """
    Convert a source metric series to numeric values with Aqueduct sentinels masked.
    """
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.mask(numeric.isin(_INVALID_NUMERIC_SENTINELS))


def load_future_geometry(
    source_gdb: Path,
    *,
    layer: str = "future_annual",
) -> gpd.GeoDataFrame:
    """Load the future layer keyed by ``pfaf_id`` while preserving source attributes."""
    gdf = gpd.read_file(str(source_gdb), layer=layer)
    if "pfaf_id" not in gdf.columns:
        raise ValueError(f"Layer {layer!r} is missing required column 'pfaf_id'.")

    out = gdf.copy()
    out["pfaf_id"] = _normalize_pfaf_id_series(out["pfaf_id"])
    out = _ensure_epsg4326(out)

    if out["pfaf_id"].eq("").any():
        raise ValueError(f"Layer {layer!r} contains blank pfaf_id values.")
    if out["pfaf_id"].duplicated().any():
        dupes = out.loc[out["pfaf_id"].duplicated(keep=False), "pfaf_id"].tolist()
        raise ValueError(
            f"Layer {layer!r} contains duplicate pfaf_id values: {dupes[:10]}"
        )

    return out.reset_index(drop=True)


def load_baseline_csv(path: Path) -> pd.DataFrame:
    """Load the baseline annual CSV with stable string normalization."""
    df = pd.read_csv(path, low_memory=False)
    if "pfaf_id" not in df.columns:
        raise ValueError("Baseline CSV is missing required column 'pfaf_id'.")
    if "area_km2" not in df.columns:
        raise ValueError("Baseline CSV is missing required column 'area_km2'.")
    df = df.copy()
    df["pfaf_id"] = _normalize_pfaf_id_series(df["pfaf_id"])
    if "gid_0" in df.columns:
        df["gid_0"] = _normalize_string_series(df["gid_0"]).fillna("")
    return df


def collect_scope_pfaf_ids(
    baseline_df: pd.DataFrame,
    *,
    scope: Scope,
) -> tuple[str, ...]:
    """Collect the canonical ``pfaf_id`` set for the requested scope."""
    if scope not in _VALID_SCOPES:
        raise ValueError(f"Unsupported scope {scope!r}; expected one of {_VALID_SCOPES}.")

    pfaf_series = _normalize_pfaf_id_series(baseline_df["pfaf_id"])
    mask = pfaf_series.ne("") & pfaf_series.ne("-9999")
    if scope == _SCOPE_INDIA:
        if "gid_0" not in baseline_df.columns:
            raise ValueError("Baseline CSV is missing required column 'gid_0' for India scope.")
        mask &= _normalize_string_series(baseline_df["gid_0"]).fillna("").eq("IND")

    values = pd.unique(pfaf_series.loc[mask])
    return tuple(sorted(str(value) for value in values if str(value)))


def _metric_columns_from_baseline(columns: Iterable[str]) -> list[str]:
    """Return the baseline numeric metric columns to aggregate."""
    metric_columns: list[str] = []
    for column in columns:
        if column == "pfaf_id" or column == "area_km2":
            continue
        if column in _DROP_BASELINE_COLUMNS:
            continue
        if column.endswith("_cat") or column.endswith("_label"):
            continue
        metric_columns.append(column)
    return metric_columns


def aggregate_baseline_by_pfaf(
    baseline_df: pd.DataFrame,
    *,
    scope_pfaf_ids: Iterable[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aggregate segmented baseline rows to one row per ``pfaf_id``.

    Returns:
        ``(aggregated_df, qa_df)``
    """
    scope_ids = tuple(scope_pfaf_ids)
    if not scope_ids:
        raise ValueError("No pfaf_id values were selected for the requested scope.")

    metric_columns = _metric_columns_from_baseline(baseline_df.columns)
    filtered = baseline_df.loc[baseline_df["pfaf_id"].isin(scope_ids)].copy()
    if filtered.empty:
        raise ValueError("Baseline CSV has no rows for the requested scope.")

    filtered["area_km2_numeric"] = _numeric_metric_series(filtered["area_km2"])
    filtered["weight_km2"] = filtered["area_km2_numeric"].where(
        filtered["area_km2_numeric"] > 0
    )

    grouped = (
        filtered.groupby("pfaf_id", dropna=False, as_index=False)
        .agg(
            area_km2=("area_km2_numeric", "sum"),
            segment_count=("pfaf_id", "size"),
            weighted_segment_count=("weight_km2", lambda s: int(s.notna().sum())),
        )
        .reset_index(drop=True)
    )

    aggregated = grouped.copy()
    qa_df = grouped.rename(
        columns={
            "area_km2": "baseline_area_km2_sum",
            "segment_count": "baseline_segment_count",
        }
    ).copy()
    aggregated_metric_columns: dict[str, pd.Series] = {}
    qa_metric_columns: dict[str, pd.Series] = {}

    for column in metric_columns:
        values = _numeric_metric_series(filtered[column])
        valid = values.notna() & filtered["weight_km2"].notna()
        weighted_sum = (
            (values.where(valid, 0.0) * filtered["weight_km2"].where(valid, 0.0))
            .groupby(filtered["pfaf_id"])
            .sum()
        )
        weight_sum = (
            filtered["weight_km2"].where(valid, 0.0).groupby(filtered["pfaf_id"]).sum()
        )
        aggregated_metric_columns[column] = aggregated["pfaf_id"].map(
            weighted_sum / weight_sum.where(weight_sum > 0)
        )
        qa_metric_columns[f"{column}__valid_weight_km2"] = qa_df["pfaf_id"].map(
            weight_sum
        ).fillna(
            0.0
        )

    if aggregated_metric_columns:
        aggregated = pd.concat(
            [aggregated, pd.DataFrame(aggregated_metric_columns, index=aggregated.index)],
            axis=1,
        )
    if qa_metric_columns:
        qa_df = pd.concat(
            [qa_df, pd.DataFrame(qa_metric_columns, index=qa_df.index)],
            axis=1,
        )

    return aggregated.reset_index(drop=True), qa_df.reset_index(drop=True)


def build_clean_baseline_gdf(
    future_gdf: gpd.GeoDataFrame,
    aggregated_baseline_df: pd.DataFrame,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """Join clean future geometry with aggregated baseline metrics."""
    if future_gdf["pfaf_id"].duplicated().any():
        raise ValueError("Future geometry has duplicate pfaf_id values.")
    if aggregated_baseline_df["pfaf_id"].duplicated().any():
        raise ValueError("Aggregated baseline has duplicate pfaf_id values.")

    joined = future_gdf.merge(aggregated_baseline_df, on="pfaf_id", how="inner")
    if joined.empty:
        raise ValueError("No features remained after joining future geometry and baseline metrics.")

    missing_future = sorted(
        set(aggregated_baseline_df["pfaf_id"]) - set(future_gdf["pfaf_id"])
    )
    if missing_future:
        raise ValueError(
            "Future geometry is missing pfaf_id values required by the baseline scope: "
            f"{missing_future[:10]}"
        )

    ordered_columns = [
        "pfaf_id",
        "area_km2",
        *[
            column
            for column in aggregated_baseline_df.columns
            if column
            not in {"pfaf_id", "area_km2", "segment_count", "weighted_segment_count"}
        ],
        "geometry",
    ]
    clean_gdf = gpd.GeoDataFrame(
        joined[ordered_columns].copy(),
        geometry="geometry",
        crs=future_gdf.crs,
    )
    _validate_areal_geometries(clean_gdf, label="Clean Aqueduct baseline")

    qa_df = aggregated_baseline_df[["pfaf_id"]].copy()
    qa_df["future_geometry_present"] = qa_df["pfaf_id"].isin(future_gdf["pfaf_id"])
    qa_df["join_status"] = qa_df["future_geometry_present"].map(
        lambda present: "joined" if bool(present) else "missing_future_geometry"
    )
    return clean_gdf.reset_index(drop=True), qa_df.reset_index(drop=True)


def build_scope_future_gdf(
    future_gdf: gpd.GeoDataFrame,
    *,
    scope_pfaf_ids: Iterable[str],
    layer_label: str,
    keep_all_columns: bool = True,
) -> gpd.GeoDataFrame:
    """Return the repaired future geometry subset for the requested scope."""
    scope_ids = tuple(scope_pfaf_ids)
    if keep_all_columns:
        scoped = future_gdf.loc[future_gdf["pfaf_id"].isin(scope_ids)].copy()
    else:
        scoped = future_gdf.loc[
            future_gdf["pfaf_id"].isin(scope_ids), ["pfaf_id", "geometry"]
        ].copy()
    if scoped.empty:
        raise ValueError(
            f"No {layer_label} features remained after filtering to the requested scope."
        )
    scoped = _repair_invalid_areal_geometries(scoped, label=layer_label)
    _validate_areal_geometries(scoped, label=layer_label)
    return scoped.reset_index(drop=True)


def prepare_aqueduct_baseline(
    *,
    source_gdb: Path,
    baseline_csv: Path,
    future_layer: str = "future_annual",
    scope: Scope = _SCOPE_INDIA,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame, gpd.GeoDataFrame, dict[str, int | str]]:
    """Build the clean Aqueduct baseline GeoDataFrame and QA diagnostics."""
    baseline_df = load_baseline_csv(baseline_csv)
    scope_pfaf_ids = collect_scope_pfaf_ids(baseline_df, scope=scope)
    future_gdf = load_future_geometry(source_gdb, layer=future_layer)
    future_scope_geometry_gdf = build_scope_future_gdf(
        future_gdf,
        scope_pfaf_ids=scope_pfaf_ids,
        layer_label=f"Aqueduct {future_layer} geometry",
        keep_all_columns=False,
    )
    future_scope_gdf = build_scope_future_gdf(
        future_gdf,
        scope_pfaf_ids=scope_pfaf_ids,
        layer_label=f"Aqueduct {future_layer} geometry",
        keep_all_columns=True,
    )
    aggregated_df, aggregation_qa_df = aggregate_baseline_by_pfaf(
        baseline_df,
        scope_pfaf_ids=scope_pfaf_ids,
    )
    clean_gdf, join_qa_df = build_clean_baseline_gdf(
        future_scope_geometry_gdf,
        aggregated_df,
    )

    qa_df = aggregation_qa_df.merge(join_qa_df, on="pfaf_id", how="left")
    qa_df = qa_df.sort_values("pfaf_id").reset_index(drop=True)

    summary: dict[str, int | str] = {
        "scope": scope,
        "scope_pfaf_count": len(scope_pfaf_ids),
        "future_geometry_count": len(future_scope_gdf),
        "clean_feature_count": len(clean_gdf),
        "qa_row_count": len(qa_df),
    }
    return clean_gdf, qa_df, future_scope_gdf, summary


def _write_geojson(gdf: gpd.GeoDataFrame, path: Path) -> None:
    """Write a GeoJSON file with the parent directory created if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(path, driver="GeoJSON")


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point."""
    default_dir = _default_output_dir()
    parser = argparse.ArgumentParser(
        description=(
            "Build a clean Aqueduct baseline layer using future_annual geometry "
            "as the canonical HydroBASINS base."
        )
    )
    parser.add_argument(
        "--source-gdb",
        type=str,
        required=True,
        help="Path to the Aqueduct file geodatabase.",
    )
    parser.add_argument(
        "--baseline-csv",
        type=str,
        required=True,
        help="Path to Aqueduct40_baseline_annual CSV export.",
    )
    parser.add_argument(
        "--future-layer",
        type=str,
        default="future_annual",
        help="Future geometry layer name in the geodatabase. Default: future_annual",
    )
    parser.add_argument(
        "--scope",
        type=str,
        default=_SCOPE_INDIA,
        choices=list(_VALID_SCOPES),
        help="Spatial scope to build. Default: india",
    )
    parser.add_argument(
        "--out-geojson",
        type=str,
        default=str(default_dir / "baseline_clean_india.geojson"),
        help="Output clean GeoJSON path.",
    )
    parser.add_argument(
        "--out-qa",
        type=str,
        default=str(default_dir / "baseline_clean_india_qa.csv"),
        help="Output QA CSV path.",
    )
    parser.add_argument(
        "--out-future-geojson",
        type=str,
        default=str(default_dir / "future_annual_india.geojson"),
        help="Output India-only future_annual GeoJSON path.",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    source_gdb = Path(args.source_gdb).expanduser().resolve()
    baseline_csv = Path(args.baseline_csv).expanduser().resolve()
    out_geojson = Path(args.out_geojson).expanduser().resolve()
    out_qa = Path(args.out_qa).expanduser().resolve()
    out_future_geojson = Path(args.out_future_geojson).expanduser().resolve()

    if not source_gdb.exists():
        raise FileNotFoundError(f"Aqueduct geodatabase does not exist: {source_gdb}")
    if not baseline_csv.exists():
        raise FileNotFoundError(f"Baseline CSV does not exist: {baseline_csv}")
    if not args.overwrite:
        for path in (out_geojson, out_qa, out_future_geojson):
            if path.exists():
                raise FileExistsError(
                    f"Output already exists: {path}. Pass --overwrite to replace it."
                )

    clean_gdf, qa_df, future_scope_gdf, summary = prepare_aqueduct_baseline(
        source_gdb=source_gdb,
        baseline_csv=baseline_csv,
        future_layer=args.future_layer,
        scope=args.scope,
    )

    print("AQUEDUCT CLEAN BASELINE")
    for key, value in summary.items():
        print(f"{key}: {value}")
    print(f"out_geojson: {out_geojson}")
    print(f"out_qa: {out_qa}")
    print(f"out_future_geojson: {out_future_geojson}")

    if args.dry_run:
        return 0

    _write_geojson(clean_gdf, out_geojson)
    _write_geojson(future_scope_gdf, out_future_geojson)
    out_qa.parent.mkdir(parents=True, exist_ok=True)
    qa_df.to_csv(out_qa, index=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

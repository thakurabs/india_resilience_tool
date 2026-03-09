#!/usr/bin/env python3
"""
Inspect and normalize the waterbasin_goi shapefile for basin and sub-basin
analytics.

This script:
1. Reads the source shapefile.
2. Prints schema, CRS, geometry, and hierarchy diagnostics.
3. Optionally repairs invalid geometries with ``shapely.make_valid``.
4. Exports a canonical sub-basin GeoJSON.
5. Derives a basin GeoJSON by dissolving sub-basins.

Behavior with missing/invalid data:
- Raises an error if required fields are missing.
- Raises an error if null or empty geometries are present.
- Raises an error on invalid geometries unless ``--repair-invalid`` is used.
- Raises an error if repair produces non-areal geometry.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import geopandas as gpd
import pandas as pd
import shapely
from shapely.geometry import MultiPolygon, Polygon

REQUIRED_FIELDS = [
    "bacode",
    "ba_name",
    "sbconc",
    "sbcode",
    "sub_basin",
]


def _print_header(title: str) -> None:
    """Print a section header."""
    print(f"\n{'=' * 80}\n{title}\n{'=' * 80}")


def _assert_required_fields(columns: Iterable[str]) -> None:
    """Raise an error if required fields are missing."""
    missing = [field for field in REQUIRED_FIELDS if field not in columns]
    if missing:
        raise ValueError(
            f"Missing required fields: {missing}. "
            "Please inspect the shapefile schema before continuing."
        )


def _print_basic_info(gdf: gpd.GeoDataFrame) -> None:
    """Print basic schema and geometry information."""
    _print_header("BASIC LAYER INFO")
    print(f"Feature count: {len(gdf)}")
    print(f"CRS: {gdf.crs}")
    print(f"Geometry types: {gdf.geom_type.value_counts(dropna=False).to_dict()}")
    print("\nColumns and dtypes:")
    print(gdf.dtypes.astype(str).to_string())


def _print_null_summary(gdf: gpd.GeoDataFrame) -> None:
    """Print null counts for all columns."""
    _print_header("NULL COUNTS")
    null_counts = gdf.isna().sum().sort_values(ascending=False)
    print(null_counts.to_string())


def _print_hierarchy_checks(gdf: gpd.GeoDataFrame) -> None:
    """Print checks on basin/sub-basin relationships."""
    _print_header("HIERARCHY CHECKS")

    print(f"Unique basins by bacode: {gdf['bacode'].nunique(dropna=True)}")
    print(f"Unique basin names: {gdf['ba_name'].nunique(dropna=True)}")
    print(f"Unique sub-basin IDs (sbconc): {gdf['sbconc'].nunique(dropna=True)}")
    print(f"Unique sub-basin codes (sbcode): {gdf['sbcode'].nunique(dropna=True)}")
    print(f"Unique sub-basin names: {gdf['sub_basin'].nunique(dropna=True)}")

    basin_name_per_bacode = (
        gdf.groupby("bacode", dropna=False)["ba_name"].nunique(dropna=False)
    )
    inconsistent_basin_name = basin_name_per_bacode[basin_name_per_bacode > 1]

    subbasin_per_sbconc = (
        gdf.groupby("sbconc", dropna=False)["sub_basin"].nunique(dropna=False)
    )
    inconsistent_subbasin_name = subbasin_per_sbconc[subbasin_per_sbconc > 1]

    basin_per_sbconc = (
        gdf.groupby("sbconc", dropna=False)["bacode"].nunique(dropna=False)
    )
    inconsistent_subbasin_parent = basin_per_sbconc[basin_per_sbconc > 1]

    print("\nInconsistent basin names per bacode:")
    print(
        inconsistent_basin_name.to_string()
        if not inconsistent_basin_name.empty
        else "None"
    )

    print("\nInconsistent sub-basin names per sbconc:")
    print(
        inconsistent_subbasin_name.to_string()
        if not inconsistent_subbasin_name.empty
        else "None"
    )

    print("\nSub-basin IDs attached to multiple basin codes:")
    print(
        inconsistent_subbasin_parent.to_string()
        if not inconsistent_subbasin_parent.empty
        else "None"
    )


def _invalid_rows(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Return invalid rows with GEOS validity reasons."""
    invalid_mask = ~gdf.geometry.is_valid
    if not invalid_mask.any():
        return pd.DataFrame(
            columns=["bacode", "ba_name", "sbconc", "sub_basin", "invalid_reason"]
        )

    invalid = gdf.loc[
        invalid_mask,
        ["bacode", "ba_name", "sbconc", "sub_basin", "geometry"],
    ].copy()
    invalid["invalid_reason"] = shapely.is_valid_reason(invalid.geometry.values)
    return invalid.drop(columns=["geometry"])


def _print_geometry_checks(gdf: gpd.GeoDataFrame) -> None:
    """Print geometry validity diagnostics."""
    _print_header("GEOMETRY CHECKS")

    null_geometry = gdf.geometry.isna().sum()
    empty_geometry = gdf.geometry.is_empty.sum()
    invalid_geometry = (~gdf.geometry.is_valid).sum()

    print(f"Null geometry count: {null_geometry}")
    print(f"Empty geometry count: {empty_geometry}")
    print(f"Invalid geometry count: {invalid_geometry}")

    invalid_rows = _invalid_rows(gdf)
    if not invalid_rows.empty:
        print("\nInvalid geometry rows:")
        print(invalid_rows.to_string(index=False))


def _coerce_to_areal_geometry(geometry: object) -> Polygon | MultiPolygon:
    """Return a polygonal geometry after repair.

    Raises:
        ValueError: If the geometry cannot be represented as Polygon or
            MultiPolygon after repair.
    """
    if geometry is None:
        raise ValueError("Encountered null geometry during repair.")

    geom_type = getattr(geometry, "geom_type", None)
    if geom_type == "Polygon":
        return geometry
    if geom_type == "MultiPolygon":
        return geometry
    if geom_type != "GeometryCollection":
        raise ValueError(
            f"Repair produced non-areal geometry type: {geom_type}."
        )

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


def _repair_invalid_geometries(
    gdf: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    """Repair invalid geometries and return QA diagnostics.

    Returns:
        A tuple of:
        - repaired GeoDataFrame
        - QA dataframe with pre/post area comparison for repaired features

    Behavior with missing/invalid data:
        Repairs only invalid geometries with ``shapely.make_valid``.
        Raises an error if repaired features remain invalid or become
        non-polygonal.
    """
    invalid_mask = ~gdf.geometry.is_valid
    if not invalid_mask.any():
        return gdf.copy(), pd.DataFrame(
            columns=[
                "sbconc",
                "sub_basin",
                "shape_Area",
                "recomputed_area",
                "relative_diff",
                "geom_type_after",
            ]
        )

    repaired = gdf.copy()
    repaired_values = shapely.make_valid(
        repaired.loc[invalid_mask, "geometry"].values
    )
    repaired.loc[invalid_mask, "geometry"] = [
        _coerce_to_areal_geometry(geometry) for geometry in repaired_values
    ]

    remaining_invalid = repaired.loc[~repaired.geometry.is_valid]
    if not remaining_invalid.empty:
        invalid_rows = _invalid_rows(repaired)
        raise ValueError(
            "Geometry repair completed, but some invalid features remain:\n"
            f"{invalid_rows.to_string(index=False)}"
        )

    qa = repaired.loc[invalid_mask, ["sbconc", "sub_basin"]].copy()
    if "shape_Area" in repaired.columns:
        qa["shape_Area"] = gdf.loc[invalid_mask, "shape_Area"].values
    else:
        qa["shape_Area"] = gdf.loc[invalid_mask].geometry.area.values
    qa["recomputed_area"] = repaired.loc[invalid_mask].geometry.area.values
    qa["relative_diff"] = (
        (qa["recomputed_area"] - qa["shape_Area"]).abs() / qa["shape_Area"]
    )
    qa["geom_type_after"] = repaired.loc[invalid_mask].geom_type.values

    return repaired, qa


def _print_repair_summary(qa: pd.DataFrame) -> None:
    """Print QA diagnostics for repaired geometries."""
    _print_header("REPAIR QA")
    if qa.empty:
        print("No invalid geometries found. No repair needed.")
        return
    print(qa.to_string(index=False))


def _prepare_subbasins(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Build the canonical sub-basin layer.

    Behavior with missing/invalid data:
    - Raises an error if null, empty, or invalid geometries are present.
    - Raises an error if the input CRS is undefined.
    """
    if gdf.geometry.isna().any():
        raise ValueError("Null geometries detected. Repair them before export.")
    if gdf.geometry.is_empty.any():
        raise ValueError("Empty geometries detected. Repair them before export.")
    if (~gdf.geometry.is_valid).any():
        raise ValueError(
            "Invalid geometries detected. Re-run with --repair-invalid or repair "
            "the source data before exporting GeoJSON."
        )
    if gdf.crs is None:
        raise ValueError(
            "Input layer has no CRS. Define the CRS before exporting to GeoJSON."
        )

    subbasins = gdf[
        ["bacode", "ba_name", "sbconc", "sbcode", "sub_basin", "geometry"]
    ].copy()

    subbasins = subbasins.rename(
        columns={
            "bacode": "basin_id",
            "ba_name": "basin_name",
            "sbconc": "subbasin_id",
            "sbcode": "subbasin_code",
            "sub_basin": "subbasin_name",
        }
    )

    subbasins["hydro_level"] = "sub_basin"
    subbasins["area_km2"] = subbasins.geometry.area / 1_000_000
    subbasins["perimeter_km"] = subbasins.geometry.length / 1_000

    for column in [
        "basin_id",
        "basin_name",
        "subbasin_id",
        "subbasin_code",
        "subbasin_name",
    ]:
        subbasins[column] = subbasins[column].astype(str).str.strip()

    return subbasins[
        [
            "basin_id",
            "basin_name",
            "subbasin_id",
            "subbasin_code",
            "subbasin_name",
            "hydro_level",
            "area_km2",
            "perimeter_km",
            "geometry",
        ]
    ].copy()


def _prepare_basins(subbasins: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Create basin polygons by dissolving sub-basins."""
    basin_name_lookup = (
        subbasins[["basin_id", "basin_name"]]
        .drop_duplicates(subset=["basin_id"])
        .copy()
    )

    basins = subbasins[["basin_id", "geometry"]].dissolve(
        by="basin_id", as_index=False
    )
    basins = basins.merge(basin_name_lookup, on="basin_id", how="left")
    basins["hydro_level"] = "basin"
    basins["area_km2"] = basins.geometry.area / 1_000_000
    basins["perimeter_km"] = basins.geometry.length / 1_000

    return basins[
        [
            "basin_id",
            "basin_name",
            "hydro_level",
            "area_km2",
            "perimeter_km",
            "geometry",
        ]
    ].copy()


def main() -> None:
    """Run the inspection and export workflow."""
    parser = argparse.ArgumentParser(
        description=(
            "Inspect waterbasin_goi shapefile, optionally repair invalid "
            "geometries, and export basin/sub-basin GeoJSON."
        )
    )
    parser.add_argument(
        "input_path",
        type=str,
        help="Path to the source shapefile (.shp).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="hydro_boundaries",
        help="Directory to write GeoJSON outputs.",
    )
    parser.add_argument(
        "--repair-invalid",
        action="store_true",
        help=(
            "Repair invalid geometries with shapely.make_valid before export. "
            "Without this flag, the script fails fast on invalid geometry."
        ),
    )
    args = parser.parse_args()

    input_path = Path(args.input_path)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    gdf = gpd.read_file(input_path)

    _assert_required_fields(gdf.columns)
    _print_basic_info(gdf)
    _print_null_summary(gdf)
    _print_hierarchy_checks(gdf)
    _print_geometry_checks(gdf)

    if (~gdf.geometry.is_valid).any():
        if not args.repair_invalid:
            raise ValueError(
                "Invalid geometries detected. Re-run with --repair-invalid or "
                "repair the source layer before exporting GeoJSON."
            )
        gdf, repair_qa = _repair_invalid_geometries(gdf)
        _print_repair_summary(repair_qa)
        _print_header("POST-REPAIR GEOMETRY CHECKS")
        print(f"Invalid geometry count: {(~gdf.geometry.is_valid).sum()}")
        print(f"Geometry types after repair: {gdf.geom_type.value_counts().to_dict()}")

    subbasins = _prepare_subbasins(gdf)
    basins = _prepare_basins(subbasins)

    subbasin_path = output_dir / "subbasins.geojson"
    basin_path = output_dir / "basins.geojson"

    subbasins.to_crs(epsg=4326).to_file(subbasin_path, driver="GeoJSON")
    basins.to_crs(epsg=4326).to_file(basin_path, driver="GeoJSON")

    _print_header("EXPORT COMPLETE")
    print(f"Sub-basin GeoJSON: {subbasin_path}")
    print(f"Basin GeoJSON:     {basin_path}")
    print(f"Sub-basin count:   {len(subbasins)}")
    print(f"Basin count:       {len(basins)}")


if __name__ == "__main__":
    main()
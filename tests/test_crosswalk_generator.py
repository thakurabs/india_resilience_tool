from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from india_resilience_tool.data.crosswalks import (
    ensure_block_subbasin_crosswalk,
    ensure_district_basin_crosswalk,
    ensure_district_subbasin_crosswalk,
)
from tools.geodata.build_district_subbasin_crosswalk import (
    build_block_subbasin_crosswalk,
    build_district_basin_crosswalk,
    build_district_subbasin_crosswalk,
    load_block_boundaries,
    load_district_boundaries,
)


def _districts_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Nizamabad", "Nizamabad"],
            "district_key": ["Telangana::Nizamabad", "Telangana::Nizamabad"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1)]),
        ],
        crs="EPSG:4326",
    )


def _blocks_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Nizamabad", "Nizamabad"],
            "block_name": ["Armur", "Armur"],
            "block_key": ["Telangana::Nizamabad::Armur", "Telangana::Nizamabad::Armur"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1)]),
        ],
        crs="EPSG:4326",
    )


def _subbasins_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "basin_id": ["B01", "B01"],
            "basin_name": ["Godavari", "Godavari"],
            "subbasin_id": ["SB01", "SB02"],
            "subbasin_code": ["GOD_1", "GOD_2"],
            "subbasin_name": ["Godavari West", "Godavari East"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1)]),
        ],
        crs="EPSG:4326",
    )


def _basins_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "basin_id": ["B01", "B02"],
            "basin_name": ["Godavari", "Krishna"],
        },
        geometry=[
            Polygon([(0, 0), (1.5, 0), (1.5, 1.5)]),
            Polygon([(1.5, 0), (3.0, 0), (3.0, 1.5)]),
        ],
        crs="EPSG:4326",
    )


def test_build_district_subbasin_crosswalk_returns_expected_rows() -> None:
    df = build_district_subbasin_crosswalk(_districts_gdf(), _subbasins_gdf(), area_epsg=6933)
    validated = ensure_district_subbasin_crosswalk(df)
    assert validated["subbasin_name"].tolist() == ["Godavari East", "Godavari West"]
    assert pytest.approx(validated["district_area_fraction_in_subbasin"].sum(), rel=1e-6) == 1.0


def test_build_block_subbasin_crosswalk_keeps_expected_columns() -> None:
    df = build_block_subbasin_crosswalk(_blocks_gdf(), _subbasins_gdf(), area_epsg=6933)
    validated = ensure_block_subbasin_crosswalk(df)
    assert list(validated.columns) == [
        "state_name",
        "district_name",
        "block_name",
        "block_key",
        "block_area_km2",
        "basin_id",
        "basin_name",
        "subbasin_id",
        "subbasin_code",
        "subbasin_name",
        "subbasin_area_km2",
        "intersection_area_km2",
        "block_area_fraction_in_subbasin",
        "subbasin_area_fraction_in_block",
    ]


def test_build_district_basin_crosswalk_aggregates_to_one_row_per_pair() -> None:
    df = build_district_basin_crosswalk(_districts_gdf(), _basins_gdf(), area_epsg=6933)
    validated = ensure_district_basin_crosswalk(df)
    assert validated["basin_name"].tolist() == ["Godavari", "Krishna"]
    assert validated["district_name"].nunique() == 1


def test_load_district_boundaries_dissolves_same_district_fragments(tmp_path) -> None:
    in_path = tmp_path / "districts.geojson"
    _districts_gdf().to_file(in_path, driver="GeoJSON")
    dissolved = load_district_boundaries(in_path)
    assert len(dissolved) == 1
    assert dissolved["district_key"].tolist() == ["Telangana::Nizamabad"]


def test_load_block_boundaries_dissolves_same_block_fragments(tmp_path) -> None:
    in_path = tmp_path / "blocks.geojson"
    _blocks_gdf().to_file(in_path, driver="GeoJSON")
    dissolved = load_block_boundaries(in_path)
    assert len(dissolved) == 1
    assert dissolved["block_key"].tolist() == ["Telangana::Nizamabad::Armur"]

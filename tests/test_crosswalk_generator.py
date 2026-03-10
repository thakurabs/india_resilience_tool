from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from india_resilience_tool.data.crosswalks import ensure_district_subbasin_crosswalk
from tools.geodata.build_district_subbasin_crosswalk import (
    build_district_subbasin_crosswalk,
)


def _districts_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Nizamabad", "Nizamabad"],
            "district_key": ["Telangana::Nizamabad", "Telangana::Nizamabad"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
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
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
        ],
        crs="EPSG:4326",
    )


def test_build_district_subbasin_crosswalk_returns_expected_rows() -> None:
    df = build_district_subbasin_crosswalk(
        _districts_gdf(),
        _subbasins_gdf(),
        area_epsg=6933,
    )

    validated = ensure_district_subbasin_crosswalk(df)
    assert validated["subbasin_name"].tolist() == ["Godavari East", "Godavari West"]
    assert pytest.approx(validated["district_area_fraction_in_subbasin"].sum(), rel=1e-6) == 1.0
    assert all(pytest.approx(v, rel=1e-6) == 0.5 for v in validated["district_area_fraction_in_subbasin"])


def test_build_district_subbasin_crosswalk_keeps_expected_columns() -> None:
    df = build_district_subbasin_crosswalk(
        _districts_gdf(),
        _subbasins_gdf(),
        area_epsg=6933,
    )

    assert list(df.columns) == [
        "district_key",
        "district_name",
        "state_name",
        "basin_id",
        "basin_name",
        "subbasin_id",
        "subbasin_code",
        "subbasin_name",
        "district_area_km2",
        "subbasin_area_km2",
        "intersection_area_km2",
        "district_area_fraction_in_subbasin",
        "subbasin_area_fraction_in_district",
    ]


def test_load_district_boundaries_dissolves_same_district_fragments(tmp_path) -> None:
    in_path = tmp_path / "districts.geojson"
    _districts_gdf().to_file(in_path, driver="GeoJSON")

    from tools.geodata.build_district_subbasin_crosswalk import load_district_boundaries

    dissolved = load_district_boundaries(in_path)
    assert len(dissolved) == 1
    assert dissolved["district_key"].tolist() == ["Telangana::Nizamabad"]

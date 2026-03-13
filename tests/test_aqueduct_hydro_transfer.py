from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from tools.geodata.build_aqueduct_hydro_crosswalk import build_aqueduct_hydro_crosswalk
from tools.geodata.build_aqueduct_hydro_masters import aggregate_crosswalk_to_targets


def _aqueduct_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "pfaf_id": ["P1", "P2"],
        },
        geometry=[
            Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            Polygon([(2, 0), (4, 0), (4, 2), (2, 2)]),
        ],
        crs="EPSG:4326",
    )


def _basin_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "basin_id": ["B1"],
            "basin_name": ["Test Basin"],
            "hydro_level": ["basin"],
        },
        geometry=[Polygon([(0, 0), (4, 0), (4, 2), (0, 2)])],
        crs="EPSG:4326",
    )


def _subbasin_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "basin_id": ["B1", "B1"],
            "basin_name": ["Test Basin", "Test Basin"],
            "subbasin_id": ["SB1", "SB2"],
            "subbasin_code": ["SB1", "SB2"],
            "subbasin_name": ["West", "East"],
            "hydro_level": ["sub_basin", "sub_basin"],
        },
        geometry=[
            Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            Polygon([(2, 0), (4, 0), (4, 2), (2, 2)]),
        ],
        crs="EPSG:4326",
    )


def test_build_aqueduct_basin_crosswalk_covers_basin_area() -> None:
    df = build_aqueduct_hydro_crosswalk(
        _aqueduct_gdf(),
        _basin_gdf()[["basin_id", "basin_name", "geometry"]],
        hydro_level="basin",
        area_epsg=6933,
    )
    assert df["basin_id"].tolist() == ["B1", "B1"]
    assert pytest.approx(df["pfaf_area_fraction_in_basin"].sum(), rel=1e-6) == 2.0
    assert pytest.approx(df["basin_area_fraction_in_pfaf"].sum(), rel=1e-6) == 1.0


def test_build_aqueduct_subbasin_crosswalk_keeps_expected_ids() -> None:
    df = build_aqueduct_hydro_crosswalk(
        _aqueduct_gdf(),
        _subbasin_gdf()[["basin_id", "basin_name", "subbasin_id", "subbasin_code", "subbasin_name", "geometry"]],
        hydro_level="sub_basin",
        area_epsg=6933,
    )
    assert df["subbasin_id"].tolist() == ["SB2", "SB1"]
    assert set(df["pfaf_id"]) == {"P1", "P2"}


def test_aggregate_crosswalk_to_targets_builds_expected_hydro_masters() -> None:
    source_df = pd.DataFrame(
        {
            "pfaf_id": ["P1", "P2"],
            "aq_water_stress__historical__1979-2019__mean": [1.0, 3.0],
            "aq_water_stress__bau__2030__mean": [2.0, 4.0],
        }
    )
    basin_crosswalk = build_aqueduct_hydro_crosswalk(
        _aqueduct_gdf(),
        _basin_gdf()[["basin_id", "basin_name", "geometry"]],
        hydro_level="basin",
        area_epsg=6933,
    )
    master_df, qa_df = aggregate_crosswalk_to_targets(
        source_df=source_df,
        crosswalk_df=basin_crosswalk,
        target_gdf=_basin_gdf(),
        hydro_level="basin",
        source_column_map={
            "aq_water_stress__historical__1979-2019__mean": "unused",
            "aq_water_stress__bau__2030__mean": "unused",
        },
    )

    assert master_df["basin_id"].tolist() == ["B1"]
    assert master_df["basin_name"].tolist() == ["Test Basin"]
    assert pytest.approx(
        float(master_df["aq_water_stress__historical__1979-2019__mean"].iloc[0]),
        rel=1e-6,
    ) == 2.0
    assert pytest.approx(
        float(master_df["aq_water_stress__bau__2030__mean"].iloc[0]),
        rel=1e-6,
    ) == 3.0
    assert pytest.approx(float(qa_df["basin_coverage_fraction"].iloc[0]), rel=1e-6) == 1.0

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from tools.geodata.build_aqueduct_admin_crosswalk import build_aqueduct_district_crosswalk
from tools.geodata.build_aqueduct_admin_masters import aggregate_crosswalk_to_districts


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


def _district_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Demo District"],
            "district_key": ["Telangana::Demo District"],
        },
        geometry=[Polygon([(0, 0), (4, 0), (4, 2), (0, 2)])],
        crs="EPSG:4326",
    )


def test_build_aqueduct_district_crosswalk_covers_expected_district() -> None:
    df = build_aqueduct_district_crosswalk(_aqueduct_gdf(), _district_gdf(), area_epsg=6933)
    assert df["district_key"].tolist() == ["Telangana::Demo District", "Telangana::Demo District"]
    assert set(df["pfaf_id"]) == {"P1", "P2"}
    assert pytest.approx(df["district_area_fraction_in_pfaf"].sum(), rel=1e-6) == 1.0


def test_aggregate_crosswalk_to_districts_builds_expected_district_master() -> None:
    source_df = pd.DataFrame(
        {
            "pfaf_id": ["P1", "P2"],
            "aq_water_stress__historical__1979-2019__mean": [1.0, 3.0],
            "aq_water_stress__bau__2030__mean": [2.0, 4.0],
        }
    )
    crosswalk_df = build_aqueduct_district_crosswalk(_aqueduct_gdf(), _district_gdf(), area_epsg=6933)
    master_df, qa_df = aggregate_crosswalk_to_districts(
        source_df=source_df,
        crosswalk_df=crosswalk_df,
        source_column_map={
            "aq_water_stress__historical__1979-2019__mean": "unused",
            "aq_water_stress__bau__2030__mean": "unused",
        },
    )

    assert master_df["state"].tolist() == ["Telangana"]
    assert master_df["district"].tolist() == ["Demo District"]
    assert master_df["district_key"].tolist() == ["Telangana::Demo District"]
    assert pytest.approx(float(master_df["aq_water_stress__historical__1979-2019__mean"].iloc[0]), rel=1e-6) == 2.0
    assert pytest.approx(float(master_df["aq_water_stress__bau__2030__mean"].iloc[0]), rel=1e-6) == 3.0
    assert pytest.approx(float(qa_df["district_coverage_fraction"].iloc[0]), rel=1e-6) == 1.0

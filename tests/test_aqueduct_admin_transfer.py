from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from tools.geodata.build_aqueduct_admin_crosswalk import build_aqueduct_district_crosswalk
from tools.geodata.build_aqueduct_block_crosswalk import build_aqueduct_block_crosswalk
from tools.geodata.build_aqueduct_admin_masters import (
    aggregate_crosswalk_to_blocks,
    aggregate_crosswalk_to_districts,
    load_block_crosswalk,
    load_district_crosswalk,
)


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


def _block_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Demo District", "Demo District"],
            "block_name": ["North Block", "South Block"],
            "block_key": [
                "Telangana::Demo District::North Block",
                "Telangana::Demo District::South Block",
            ],
        },
        geometry=[
            Polygon([(0, 0), (4, 0), (4, 1), (0, 1)]),
            Polygon([(0, 1), (4, 1), (4, 2), (0, 2)]),
        ],
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


def test_build_aqueduct_block_crosswalk_covers_expected_blocks() -> None:
    df = build_aqueduct_block_crosswalk(_aqueduct_gdf(), _block_gdf(), area_epsg=6933)
    assert set(df["block_key"]) == {
        "Telangana::Demo District::North Block",
        "Telangana::Demo District::South Block",
    }
    assert set(df["pfaf_id"]) == {"P1", "P2"}
    assert pytest.approx(df["block_area_fraction_in_pfaf"].sum(), rel=1e-6) == 2.0


def test_aggregate_crosswalk_to_blocks_builds_expected_block_master() -> None:
    source_df = pd.DataFrame(
        {
            "pfaf_id": ["P1", "P2"],
            "aq_water_stress__historical__1979-2019__mean": [1.0, 3.0],
            "aq_water_stress__bau__2030__mean": [2.0, 4.0],
        }
    )
    crosswalk_df = build_aqueduct_block_crosswalk(_aqueduct_gdf(), _block_gdf(), area_epsg=6933)
    master_df, qa_df = aggregate_crosswalk_to_blocks(
        source_df=source_df,
        crosswalk_df=crosswalk_df,
        source_column_map={
            "aq_water_stress__historical__1979-2019__mean": "unused",
            "aq_water_stress__bau__2030__mean": "unused",
        },
    )

    assert master_df["state"].tolist() == ["Telangana", "Telangana"]
    assert master_df["district"].tolist() == ["Demo District", "Demo District"]
    assert master_df["block"].tolist() == ["North Block", "South Block"]
    assert pytest.approx(float(master_df["aq_water_stress__historical__1979-2019__mean"].iloc[0]), rel=1e-6) == 2.0
    assert pytest.approx(float(master_df["aq_water_stress__bau__2030__mean"].iloc[1]), rel=1e-6) == 3.0
    assert qa_df["block_coverage_fraction"].between(0.99, 1.01).all()


def test_load_district_crosswalk_rejects_invalid_state_name(tmp_path) -> None:
    csv_path = tmp_path / "district_crosswalk.csv"
    pd.DataFrame(
        {
            "pfaf_id": ["P1"],
            "state_name": [pd.NA],
            "district_name": ["Demo District"],
            "district_key": ["Telangana::Demo District"],
            "district_area_km2": [10.0],
            "intersection_area_km2": [5.0],
            "pfaf_area_fraction_in_district": [0.5],
            "district_area_fraction_in_pfaf": [0.5],
        }
    ).to_csv(csv_path, index=False)

    with pytest.raises(ValueError, match="invalid state_name"):
        load_district_crosswalk(csv_path)


def test_load_block_crosswalk_rejects_invalid_block_name(tmp_path) -> None:
    csv_path = tmp_path / "block_crosswalk.csv"
    pd.DataFrame(
        {
            "pfaf_id": ["P1"],
            "state_name": ["Telangana"],
            "district_name": ["Demo District"],
            "block_name": [pd.NA],
            "block_key": ["Telangana::Demo District::North Block"],
            "block_area_km2": [10.0],
            "intersection_area_km2": [5.0],
            "pfaf_area_fraction_in_block": [0.5],
            "block_area_fraction_in_pfaf": [0.5],
        }
    ).to_csv(csv_path, index=False)

    with pytest.raises(ValueError, match="invalid block_name"):
        load_block_crosswalk(csv_path)

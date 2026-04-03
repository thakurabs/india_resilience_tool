"""Tests for selector-scoped geo cache helpers."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from india_resilience_tool.app import geo_cache
from india_resilience_tool.utils.naming import alias


def _blocks_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana", "Telangana"],
            "district_name": ["Adilabad", "Adilabad", "Nirmal"],
            "block_name": ["Adilabad Rural", "Bela", "Laxmanchanda"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
        ],
        crs="EPSG:4326",
    )


def test_build_adm3_geojson_by_district_returns_district_scoped_featurecollections(
    monkeypatch,
) -> None:
    geo_cache.build_adm3_geojson_by_district.clear()
    monkeypatch.setattr(geo_cache, "load_local_adm3", lambda path, tolerance: _blocks_gdf())

    out = geo_cache.build_adm3_geojson_by_district(
        path="ignored.geojson",
        tolerance=0.01,
        mtime=1.0,
    )

    selector_key = f"{alias('Telangana')}|{alias('Adilabad')}"
    assert selector_key in out
    assert len(out[selector_key]["features"]) == 2
    assert {feat["properties"]["district_name"] for feat in out[selector_key]["features"]} == {"Adilabad"}
    assert len(out["all"]["features"]) == 3


def test_load_admin_block_selector_index_builds_state_district_mapping(tmp_path) -> None:
    path = tmp_path / "admin_block_index.parquet"
    pd.DataFrame(
        {
            "state_name": ["Telangana", "Telangana", "Telangana"],
            "district_name": ["Adilabad", "Adilabad", "Nirmal"],
            "block_name": ["Bela", "Gudihathnoor", "Laxmanchanda"],
        }
    ).to_parquet(path, index=False)

    out = geo_cache.load_admin_block_selector_index(str(path))

    assert out["blocks_by_selector"][f"{alias('Telangana')}|{alias('Adilabad')}"] == ["Bela", "Gudihathnoor"]


def test_load_hydro_subbasin_selector_index_groups_by_basin(tmp_path) -> None:
    path = tmp_path / "hydro_subbasin_index.parquet"
    pd.DataFrame(
        {
            "basin_id": ["B01", "B01", "B02"],
            "basin_name": ["Godavari Basin", "Godavari Basin", "Krishna Basin"],
            "subbasin_id": ["SB01", "SB02", "SB03"],
            "subbasin_name": ["Pranhita", "Indravati", "Tungabhadra"],
        }
    ).to_parquet(path, index=False)

    out = geo_cache.load_hydro_subbasin_selector_index(str(path))

    assert out["basin_names"] == ["Godavari Basin", "Krishna Basin"]
    assert out["basin_ids_by_name"]["Godavari Basin"] == "B01"
    assert out["subbasins_by_basin"]["Godavari Basin"] == ["Indravati", "Pranhita"]

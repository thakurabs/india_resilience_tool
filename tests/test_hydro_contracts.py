from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from india_resilience_tool.data.hydro_loader import (
    ensure_hydro_columns,
    ensure_hydro_key_column,
    filter_subbasins_for_basin,
)
from india_resilience_tool.data.merge import (
    get_unit_name_column,
    merge_basin_with_master,
    merge_subbasin_with_master,
)
from paths import get_boundary_path, get_master_csv_filename


def _subbasin_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "basin_id": ["B01", "B01"],
            "basin_name": ["Godavari", "Godavari"],
            "subbasin_id": ["SB01", "SB02"],
            "subbasin_code": ["GOD_1", "GOD_2"],
            "subbasin_name": ["Upper Godavari", "Lower Godavari"],
            "hydro_level": ["sub_basin", "sub_basin"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
        ],
        crs="EPSG:4326",
    )


def _basin_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "basin_id": ["B01"],
            "basin_name": ["Godavari"],
            "hydro_level": ["basin"],
        },
        geometry=[Polygon([(0, 0), (2, 0), (2, 1), (0, 1)])],
        crs="EPSG:4326",
    )


def test_paths_support_hydro_levels() -> None:
    assert get_boundary_path("basin").name == "basins.geojson"
    assert get_boundary_path("sub_basin").name == "subbasins.geojson"
    assert get_master_csv_filename("basin") == "master_metrics_by_basin.csv"
    assert get_master_csv_filename("sub_basin") == "master_metrics_by_sub_basin.csv"
    assert get_unit_name_column("basin") == "basin_name"
    assert get_unit_name_column("sub_basin") == "subbasin_name"


def test_hydro_loader_requires_canonical_columns() -> None:
    gdf = _subbasin_gdf().drop(columns=["subbasin_code"])
    with pytest.raises(ValueError):
        ensure_hydro_columns(gdf, level="sub_basin")


def test_hydro_loader_adds_deterministic_keys() -> None:
    gdf = ensure_hydro_columns(_subbasin_gdf(), level="sub_basin")
    keyed = ensure_hydro_key_column(gdf, level="sub_basin", alias_fn=lambda s: str(s).lower())
    assert keyed["__key"].tolist() == ["sb01", "sb02"]


def test_filter_subbasins_for_basin_filters_by_basin_id() -> None:
    gdf = ensure_hydro_columns(_subbasin_gdf(), level="sub_basin")
    filtered = filter_subbasins_for_basin(gdf, "B01", alias_fn=lambda s: str(s).lower())
    assert filtered["subbasin_name"].tolist() == ["Upper Godavari", "Lower Godavari"]


def test_merge_basin_with_master_joins_on_basin_id() -> None:
    basin_gdf = _basin_gdf()
    master_df = pd.DataFrame(
        {
            "basin_id": ["B01"],
            "tas__ssp245__2020-2040__mean": [42.0],
        }
    )
    merged = merge_basin_with_master(
        basin_gdf,
        master_df,
        alias_fn=lambda s: str(s).lower(),
    )
    assert float(merged["tas__ssp245__2020-2040__mean"].iloc[0]) == 42.0


def test_merge_subbasin_with_master_joins_on_subbasin_id() -> None:
    subbasin_gdf = _subbasin_gdf()
    master_df = pd.DataFrame(
        {
            "subbasin_id": ["SB02"],
            "tas__ssp245__2020-2040__mean": [7.5],
        }
    )
    merged = merge_subbasin_with_master(
        subbasin_gdf,
        master_df,
        alias_fn=lambda s: str(s).lower(),
    )
    val = merged.loc[merged["subbasin_id"] == "SB02", "tas__ssp245__2020-2040__mean"].iloc[0]
    assert float(val) == 7.5

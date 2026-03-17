from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import Polygon

from tools.geodata.build_population_admin_masters import (
    POPULATION_DENSITY_COL,
    POPULATION_TOTAL_COL,
    aggregate_population_to_admin_units,
    build_population_consistency_qa,
    build_population_national_summary,
)


def _write_test_raster(path: Path) -> Path:
    data = np.array([[10.0, 20.0], [30.0, 40.0]], dtype="float32")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=2,
        width=2,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 2, 1, 1),
        nodata=-9999.0,
    ) as dst:
        dst.write(data, 1)
    return path


def _districts_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Demo District"],
            "district_key": ["Telangana::Demo District"],
        },
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )


def _blocks_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Demo District", "Demo District"],
            "block_name": ["Top Block", "Bottom Block"],
            "block_key": [
                "Telangana::Demo District::Top Block",
                "Telangana::Demo District::Bottom Block",
            ],
        },
        geometry=[
            Polygon([(0, 1), (2, 1), (2, 2), (0, 2)]),
            Polygon([(0, 0), (2, 0), (2, 1), (0, 1)]),
        ],
        crs="EPSG:4326",
    )


def test_aggregate_population_to_districts_and_blocks(tmp_path: Path) -> None:
    raster_path = _write_test_raster(tmp_path / "population.tif")

    district_master_df, district_qa_df = aggregate_population_to_admin_units(
        _districts_gdf(),
        level="district",
        raster_path=raster_path,
    )
    block_master_df, block_qa_df = aggregate_population_to_admin_units(
        _blocks_gdf(),
        level="block",
        raster_path=raster_path,
    )

    assert district_master_df["state"].tolist() == ["Telangana"]
    assert district_master_df["district"].tolist() == ["Demo District"]
    assert float(district_master_df[POPULATION_TOTAL_COL].iloc[0]) == 100.0
    assert district_qa_df["raster_cell_count"].tolist() == [4]

    assert block_master_df["block"].tolist() == ["Bottom Block", "Top Block"]
    totals = {
        row["block"]: float(row[POPULATION_TOTAL_COL])
        for _, row in block_master_df.iterrows()
    }
    assert totals == {"Top Block": 30.0, "Bottom Block": 70.0}
    assert block_qa_df["raster_cell_count"].tolist() == [2, 2]

    for _, row in district_master_df.iterrows():
        assert np.isclose(
            float(row[POPULATION_DENSITY_COL]),
            float(row[POPULATION_TOTAL_COL]) / float(row["district_area_km2"]),
        )
    for _, row in block_master_df.iterrows():
        assert np.isclose(
            float(row[POPULATION_DENSITY_COL]),
            float(row[POPULATION_TOTAL_COL]) / float(row["block_area_km2"]),
        )


def test_population_consistency_and_national_summary(tmp_path: Path) -> None:
    raster_path = _write_test_raster(tmp_path / "population.tif")
    district_master_df, district_qa_df = aggregate_population_to_admin_units(
        _districts_gdf(),
        level="district",
        raster_path=raster_path,
    )
    block_master_df, _ = aggregate_population_to_admin_units(
        _blocks_gdf(),
        level="block",
        raster_path=raster_path,
    )

    consistency_df = build_population_consistency_qa(district_master_df, block_master_df)
    assert consistency_df["difference_abs"].tolist() == [0.0]

    national_summary_df = build_population_national_summary(
        district_master_df,
        block_master_df,
        district_qa_df=district_qa_df,
    )
    row = national_summary_df.iloc[0]
    assert float(row["raster_population_total"]) == 100.0
    assert float(row["district_population_total"]) == 100.0
    assert float(row["block_population_total"]) == 100.0

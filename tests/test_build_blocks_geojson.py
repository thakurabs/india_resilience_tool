from __future__ import annotations

from pathlib import Path

import geopandas as gpd
from shapely.geometry import Polygon

from tools.geodata.build_blocks_geojson import prepare_blocks_geojson


def test_prepare_blocks_geojson_dissolves_duplicate_block_fragments(tmp_path: Path) -> None:
    shp_path = tmp_path / "blocks.shp"
    gdf = gpd.GeoDataFrame(
        {
            "STATE_UT": ["Karnataka", "Karnataka"],
            "District": ["Belagavi", "Belagavi"],
            "Sub_dist": ["Athani", "Athani"],
            "Subdis_Typ": ["Tehsil", "Tehsil"],
            "Subdis_LGD": ["100", "100"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1)]),
        ],
        crs="EPSG:4326",
    )
    gdf.to_file(shp_path)

    out_gdf, summary_df, anomalies_df = prepare_blocks_geojson(shp_path)

    assert len(out_gdf) == 1
    assert out_gdf["state_name"].tolist() == ["Karnataka"]
    assert out_gdf["district_name"].tolist() == ["Belagavi"]
    assert out_gdf["block_name"].tolist() == ["Athani"]
    assert out_gdf["block_key"].tolist() == ["Karnataka::Belagavi::Athani"]
    assert int(summary_df["duplicate_rows_before_dissolve"].iloc[0]) == 1
    assert anomalies_df.empty


def test_prepare_blocks_geojson_repairs_known_label_corruption(tmp_path: Path) -> None:
    shp_path = tmp_path / "blocks_bad.shp"
    gdf = gpd.GeoDataFrame(
        {
            "STATE_UT": ["Karn<taka"],
            "District": ["H>ORA"],
            "Sub_dist": ["B<d<mi"],
            "Subdis_Typ": ["Tehsil"],
            "Subdis_LGD": ["100"],
        },
        geometry=[Polygon([(0, 0), (1, 0), (1, 1)])],
        crs="EPSG:4326",
    )
    gdf.to_file(shp_path)

    out_gdf, summary_df, anomalies_df = prepare_blocks_geojson(shp_path)

    assert out_gdf["state_name"].tolist() == ["Karnataka"]
    assert out_gdf["district_name"].tolist() == ["Haora"]
    assert out_gdf["block_name"].tolist() == ["Badami"]
    assert int(summary_df["suspicious_label_rows"].iloc[0]) == 0
    assert anomalies_df.empty


def test_prepare_blocks_geojson_reconciles_lgd_district_renames(tmp_path: Path) -> None:
    """A renamed Telangana district resolves to its canonical LGD spelling;
    a non-renamed district passes through unchanged (CHG-0092)."""
    shp_path = tmp_path / "blocks_tg.shp"
    gdf = gpd.GeoDataFrame(
        {
            "STATE_UT": ["TELANGANA", "TELANGANA"],
            "District": ["Jagtial", "Adilabad"],
            "Sub_dist": ["Jagtial", "Adilabad Rural"],
            "Subdis_Typ": ["Mandal", "Mandal"],
            "Subdis_LGD": ["201", "202"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1)]),
        ],
        crs="EPSG:4326",
    )
    gdf.to_file(shp_path)

    out_gdf, summary_df, _ = prepare_blocks_geojson(shp_path)

    districts = dict(zip(out_gdf["block_name"], out_gdf["district_name"]))
    assert districts["Jagtial"] == "Jagitial"          # renamed
    assert districts["Adilabad Rural"] == "Adilabad"    # unchanged
    assert "TELANGANA::Jagitial::Jagtial" in set(out_gdf["block_key"])
    assert int(summary_df["renamed_district_rows"].iloc[0]) == 1

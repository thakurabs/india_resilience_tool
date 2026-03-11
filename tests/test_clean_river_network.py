from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import LineString, MultiLineString, Polygon

from tools.geodata.clean_river_network import (
    _DISPLAY_COLUMNS,
    build_river_network_display_gdf,
    clean_river_network_gdf,
)


def _river_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "UID_River": ["R-001", "R-001"],
            "rivname": ["  Bhogdoi/Desai  ", " Chulu Ganpa /  Phutang Chu "],
            "ba_name": ["Godavari", "Godavari"],
            "sub_basin": ["Major River", "Pranhita and others"],
            "state_al": ["Telangana / Maharashtra", "Telangana"],
            "origin": [None, " Himalaya "],
            "major_trib": [None, " Major  tributary "],
            "Confluence": ["  Main stem", " Tributary "],
            "remark": [None, "  Needs review "],
            "length_km": [2.0, 1.5],
        },
        geometry=[
            LineString([(0, 0), (1000, 0), (2000, 0)]),
            MultiLineString(
                [
                    [(0, 0), (0, 1000)],
                    [(0, 1000), (1000, 1000), (2000, 1000)],
                ]
            ),
        ],
        crs="EPSG:3857",
    )


def test_clean_river_network_flags_duplicate_uid_and_keeps_feature_ids_unique() -> None:
    cleaned, qa_df, summary = clean_river_network_gdf(
        _river_gdf(),
        source_path=Path("/tmp/river_network_goi.shp"),
        length_diff_threshold_pct=10.0,
    )

    assert cleaned["river_feature_id"].is_unique
    assert cleaned["source_uid_is_duplicate"].tolist() == [True, True]
    assert summary["duplicate_uid_count"] == 2
    assert qa_df["issue_duplicate_uid"].all()


def test_clean_river_network_normalizes_text_and_preserves_aliases() -> None:
    cleaned, _, _ = clean_river_network_gdf(
        _river_gdf(),
        source_path=Path("/tmp/river_network_goi.shp"),
    )

    assert cleaned.loc[0, "river_name_clean"] == "Bhogdoi/Desai"
    assert cleaned.loc[1, "river_name_clean"] == "Chulu Ganpa / Phutang Chu"
    assert cleaned.loc[0, "subbasin_name_clean"] == "Major River"
    assert cleaned.loc[1, "major_trib_clean"] == "Major tributary"


def test_clean_river_network_sets_multipart_counts_and_issue_flags() -> None:
    cleaned, qa_df, _ = clean_river_network_gdf(
        _river_gdf(),
        source_path=Path("/tmp/river_network_goi.shp"),
    )

    assert bool(cleaned.loc[0, "is_multipart"]) is False
    assert cleaned.loc[0, "part_count"] == 1
    assert cleaned.loc[0, "vertex_count"] == 3
    assert bool(cleaned.loc[1, "is_multipart"]) is True
    assert cleaned.loc[1, "part_count"] == 2
    assert cleaned.loc[1, "vertex_count"] == 5
    assert bool(cleaned.loc[0, "issue_placeholder_subbasin"]) is True
    assert bool(cleaned.loc[1, "issue_multipart"]) is True
    assert cleaned.loc[0, "river_feature_id"].startswith("riv_")
    assert qa_df["river_feature_id"].isin(cleaned["river_feature_id"]).all()


def test_build_river_network_display_gdf_keeps_expected_columns() -> None:
    cleaned, _, _ = clean_river_network_gdf(
        _river_gdf(),
        source_path=Path("/tmp/river_network_goi.shp"),
    )

    display = build_river_network_display_gdf(cleaned, display_tolerance=0.0)
    assert list(display.columns) == _DISPLAY_COLUMNS
    assert display.crs.to_epsg() == 4326


def test_clean_river_network_rejects_non_line_geometry() -> None:
    bad = gpd.GeoDataFrame(
        {
            "UID_River": ["R-100"],
            "rivname": ["Bad geometry"],
            "ba_name": ["Godavari"],
            "sub_basin": ["Pranhita and others"],
            "state_al": ["Telangana"],
            "length_km": [1.0],
        },
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )

    with pytest.raises(ValueError, match="non-line geometries"):
        clean_river_network_gdf(bad, source_path=Path("/tmp/river_network_goi.shp"))


def test_build_river_network_display_gdf_rejects_negative_tolerance() -> None:
    cleaned, _, _ = clean_river_network_gdf(
        _river_gdf(),
        source_path=Path("/tmp/river_network_goi.shp"),
    )

    with pytest.raises(ValueError, match="display_tolerance must be >= 0"):
        build_river_network_display_gdf(cleaned, display_tolerance=-1.0)

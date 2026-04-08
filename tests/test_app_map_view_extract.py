"""
Tests for map click extraction helper.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import geopandas as gpd
from shapely.geometry import Polygon

from india_resilience_tool.app.views.map_view import (
    extract_click_coordinates,
    extract_clicked_district_state,
    extract_clicked_state,
    find_state_at_coordinates,
)


def test_extract_clicked_district_state_from_properties() -> None:
    ret = {
        "last_object_clicked": {
            "properties": {
                "district_name": "Alpha",
                "state_name": "Telangana",
            }
        }
    }
    d, s = extract_clicked_district_state(ret)
    assert d == "Alpha"
    assert s == "Telangana"


def test_extract_clicked_district_state_none_safe() -> None:
    d, s = extract_clicked_district_state(None)
    assert d is None
    assert s is None


def test_extract_clicked_state_from_shape_name_payload() -> None:
    ret = {
        "last_object_clicked": {
            "properties": {
                "shapeName": "Telangana",
            }
        }
    }

    assert extract_clicked_state(ret) == "Telangana"


def test_extract_clicked_state_prefers_explicit_state_name() -> None:
    ret = {
        "last_object_clicked": {
            "properties": {
                "state_name": "Telangana",
                "shapeName": "Nalgonda",
            }
        }
    }

    assert extract_clicked_state(ret) == "Telangana"


def test_extract_clicked_state_none_safe() -> None:
    assert extract_clicked_state(None) is None


def test_extract_click_coordinates_prefers_last_clicked() -> None:
    ret = {
        "last_clicked": {"lat": 17.3, "lng": 78.4},
    }

    lat, lon = extract_click_coordinates(ret)

    assert lat == 17.3
    assert lon == 78.4


def test_find_state_at_coordinates_matches_containing_polygon() -> None:
    adm1 = gpd.GeoDataFrame(
        {
            "shapeName": ["Alpha", "Beta"],
            "geometry": [
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                Polygon([(3, 0), (5, 0), (5, 2), (3, 2)]),
            ],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    assert find_state_at_coordinates(adm1, 1.0, 1.0) == "Alpha"


def test_find_state_at_coordinates_falls_back_to_nearest_centroid() -> None:
    adm1 = gpd.GeoDataFrame(
        {
            "shapeName": ["Alpha", "Beta"],
            "geometry": [
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                Polygon([(5, 0), (7, 0), (7, 2), (5, 2)]),
            ],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )

    assert find_state_at_coordinates(adm1, 1.0, 3.2) == "Alpha"

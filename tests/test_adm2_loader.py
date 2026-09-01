from __future__ import annotations

import geopandas as gpd
from shapely.geometry import Polygon

from india_resilience_tool.data.adm2_loader import (
    enrich_adm2_with_state_names,
    ensure_adm2_columns,
    ensure_key_column,
)


def test_ensure_adm2_columns_fills_state_name_from_source_columns() -> None:
    adm2 = gpd.GeoDataFrame(
        {"DISTRICT": ["Adilabad"], "STATE_UT": ["Telangana"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )

    out = ensure_adm2_columns(adm2)

    assert out.loc[0, "district_name"] == "Adilabad"
    assert out.loc[0, "state_name"] == "Telangana"


def test_enrich_adm2_with_state_names_preserves_existing_state_names() -> None:
    adm2 = gpd.GeoDataFrame(
        {
            "district_name": ["Adilabad"],
            "state_name": ["Telangana"],
        },
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    adm1 = gpd.GeoDataFrame(
        {"shapeName": ["Maharashtra"]},
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )

    out = enrich_adm2_with_state_names(adm2, adm1)

    assert out.loc[0, "state_name"] == "Telangana"


def test_enrich_adm2_with_state_names_recovers_unknown_with_representative_point() -> None:
    adm2 = gpd.GeoDataFrame(
        {
            "district_name": ["Nalgonda"],
            "state_name": ["Unknown"],
        },
        geometry=[Polygon([(0, 0), (3, 0), (3, 3), (0, 3)])],
        crs="EPSG:4326",
    )
    adm1 = gpd.GeoDataFrame(
        {"shapeName": ["Telangana"]},
        geometry=[Polygon([(0, 0), (4, 0), (4, 4), (0, 4)])],
        crs="EPSG:4326",
    )

    out = enrich_adm2_with_state_names(adm2, adm1)

    assert out.loc[0, "state_name"] == "Telangana"


def test_enrich_adm2_with_state_names_logs_residual_unknowns(caplog) -> None:
    adm2 = gpd.GeoDataFrame(
        {
            "district_name": ["Unknown District"],
            "state_name": ["Unknown"],
        },
        geometry=[Polygon([(10, 10), (11, 10), (11, 11), (10, 11)])],
        crs="EPSG:4326",
    )
    adm1 = gpd.GeoDataFrame(
        {"shapeName": ["Telangana"]},
        geometry=[Polygon([(0, 0), (4, 0), (4, 4), (0, 4)])],
        crs="EPSG:4326",
    )

    out = enrich_adm2_with_state_names(adm2, adm1)

    assert out.loc[0, "state_name"] == "Unknown"
    assert "left 1/1 rows" in caplog.text


def test_ensure_key_column_uses_state_and_district() -> None:
    adm2 = gpd.GeoDataFrame(
        {
            "district_name": ["Raigarh", "Raigarh"],
            "state_name": ["Chhattisgarh", "Maharashtra"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
        ],
        crs="EPSG:4326",
    )

    keyed = ensure_key_column(
        adm2,
        state_col="state_name",
        district_col="district_name",
        alias_fn=lambda s: str(s).strip().lower(),
    )

    assert keyed["__key"].tolist() == [
        "chhattisgarh|raigarh",
        "maharashtra|raigarh",
    ]

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from tools.geodata.prepare_aqueduct_baseline import (
    aggregate_baseline_by_pfaf,
    build_clean_baseline_gdf,
    build_scope_future_gdf,
    collect_scope_pfaf_ids,
    _repair_invalid_areal_geometries,
)


def test_collect_scope_pfaf_ids_filters_india_and_invalid_ids() -> None:
    df = pd.DataFrame(
        {
            "pfaf_id": ["1001", "1002", "-9999", "2001"],
            "gid_0": ["IND", "IND", "IND", "NPL"],
            "area_km2": [1.0, 2.0, 3.0, 4.0],
        }
    )

    assert collect_scope_pfaf_ids(df, scope="india") == ("1001", "1002")
    assert collect_scope_pfaf_ids(df, scope="global") == ("1001", "1002", "2001")


def test_aggregate_baseline_by_pfaf_area_weights_numeric_metrics() -> None:
    baseline_df = pd.DataFrame(
        {
            "pfaf_id": ["1001", "1001", "1002"],
            "gid_0": ["IND", "IND", "IND"],
            "string_id": ["s1", "s2", "s3"],
            "aq30_id": [1, 2, 3],
            "aqid": [10, 11, 12],
            "name_0": ["India", "India", "India"],
            "name_1": ["State A", "State A", "State B"],
            "area_km2": [2.0, 1.0, 4.0],
            "bws_raw": [0.5, 1.1, 2.0],
            "bws_score": [1.0, 3.0, 5.0],
            "bws_cat": [0, 2, 4],
            "bws_label": ["Low", "High", "Extreme"],
            "sev_raw": [9999.0, 0.4, -9999.0],
        }
    )

    aggregated_df, qa_df = aggregate_baseline_by_pfaf(
        baseline_df,
        scope_pfaf_ids=("1001", "1002"),
    )

    assert "bws_cat" not in aggregated_df.columns
    assert "bws_label" not in aggregated_df.columns
    assert "aq30_id" not in aggregated_df.columns
    assert aggregated_df.loc[
        aggregated_df["pfaf_id"] == "1001", "area_km2"
    ].item() == pytest.approx(3.0)
    assert aggregated_df.loc[
        aggregated_df["pfaf_id"] == "1001", "bws_raw"
    ].item() == pytest.approx((2.0 * 0.5 + 1.0 * 1.1) / 3.0)
    assert aggregated_df.loc[
        aggregated_df["pfaf_id"] == "1001", "bws_score"
    ].item() == pytest.approx((2.0 * 1.0 + 1.0 * 3.0) / 3.0)
    assert aggregated_df.loc[
        aggregated_df["pfaf_id"] == "1001", "sev_raw"
    ].item() == pytest.approx(0.4)
    assert qa_df.loc[
        qa_df["pfaf_id"] == "1001", "baseline_segment_count"
    ].item() == 2


def test_build_clean_baseline_gdf_joins_future_geometry() -> None:
    future_gdf = gpd.GeoDataFrame(
        {
            "pfaf_id": ["1001", "1002"],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
        ],
        crs="EPSG:4326",
    )
    aggregated_df = pd.DataFrame(
        {
            "pfaf_id": ["1001", "1002"],
            "area_km2": [3.0, 4.0],
            "bws_raw": [0.7, 2.0],
        }
    )

    clean_gdf, qa_df = build_clean_baseline_gdf(future_gdf, aggregated_df)

    assert list(clean_gdf.columns) == ["pfaf_id", "area_km2", "bws_raw", "geometry"]
    assert clean_gdf.crs.to_epsg() == 4326
    assert clean_gdf["pfaf_id"].tolist() == ["1001", "1002"]
    assert clean_gdf.geometry.equals(future_gdf.geometry)
    assert qa_df["join_status"].tolist() == ["joined", "joined"]


def test_build_scope_future_gdf_filters_to_requested_pfaf_ids() -> None:
    future_gdf = gpd.GeoDataFrame(
        {
            "pfaf_id": ["1001", "1002", "1003"],
            "bau50_ba_x_r": [1.0, 2.0, 3.0],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
        ],
        crs="EPSG:4326",
    )

    scoped = build_scope_future_gdf(
        future_gdf,
        scope_pfaf_ids=("1001", "1003"),
        layer_label="Aqueduct future_annual geometry",
    )

    assert list(scoped.columns) == ["pfaf_id", "bau50_ba_x_r", "geometry"]
    assert scoped["pfaf_id"].tolist() == ["1001", "1003"]
    assert scoped["bau50_ba_x_r"].tolist() == [1.0, 3.0]


def test_repair_invalid_areal_geometries_fixes_bowtie_polygon() -> None:
    invalid_gdf = gpd.GeoDataFrame(
        {"pfaf_id": ["1001"]},
        geometry=[Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])],
        crs="EPSG:4326",
    )

    repaired = _repair_invalid_areal_geometries(
        invalid_gdf,
        label="Aqueduct future geometry",
    )

    assert repaired.geometry.is_valid.all()

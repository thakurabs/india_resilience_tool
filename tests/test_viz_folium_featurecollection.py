"""Tests for lightweight Folium FeatureCollection patch helpers."""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.utils.naming import alias
from india_resilience_tool.viz.folium_featurecollection import (
    build_geojson_tooltip,
    build_props_map_from_gdf,
    clone_featurecollection_for_patch,
    patch_fc_properties,
    props_map_signature,
)


def test_clone_featurecollection_for_patch_preserves_source_properties() -> None:
    geometry = {"type": "Polygon", "coordinates": (((0, 0), (1, 0), (1, 1), (0, 0)),)}
    source = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"district_name": "Adilabad", "fillColor": "#cccccc"},
                "geometry": geometry,
            }
        ],
    }

    cloned = clone_featurecollection_for_patch(source)
    cloned["features"][0]["properties"]["fillColor"] = "#ff0000"

    assert source["features"][0]["properties"]["fillColor"] == "#cccccc"
    assert cloned["features"][0]["geometry"] is geometry


def test_build_props_map_from_gdf_vectorizes_block_keys_and_patch_preserves_geometry() -> None:
    prop_df = pd.DataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Adilabad"],
            "block_name": ["Adilabad Rural"],
            "tas_annual_mean": [28.26],
            "fillColor": ["#ff0000"],
            "_tooltip_value": ["28.26 °C"],
            "_risk_class": ["High"],
        }
    )
    props_map, value_cols, text_cols = build_props_map_from_gdf(
        prop_df,
        level="block",
        alias_fn=alias,
        feature_key_col="__bkey",
        metric_col="tas_annual_mean",
        map_value_col="tas_annual_mean",
    )

    feature_key = f"{alias('Telangana')}|{alias('Adilabad')}|{alias('Adilabad Rural')}"
    assert feature_key in props_map
    assert props_map[feature_key]["fillColor"] == "#ff0000"
    assert "tas_annual_mean" in value_cols
    assert "_tooltip_value" in text_cols
    assert props_map_signature(props_map) == props_map_signature(props_map)

    geometry = {"type": "Polygon", "coordinates": (((0, 0), (1, 0), (1, 1), (0, 0)),)}
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "state_name": "Telangana",
                    "district_name": "Adilabad",
                    "block_name": "Adilabad Rural",
                },
                "geometry": geometry,
            }
        ],
    }

    patched = patch_fc_properties(
        clone_featurecollection_for_patch(fc),
        level="block",
        alias_fn=alias,
        feature_key_col="__bkey",
        props_map=props_map,
    )

    assert patched["features"][0]["geometry"] is geometry
    assert patched["features"][0]["properties"]["fillColor"] == "#ff0000"
    assert fc["features"][0]["properties"].get("fillColor") is None


def test_build_geojson_tooltip_omits_percentile_risk_class_for_jrc_flood_severity() -> None:
    tooltip = build_geojson_tooltip(
        level="block",
        map_mode="Absolute value",
        has_baseline=False,
        rank_scope_label="district",
        metric_slug="jrc_flood_depth_index_rp100",
    )

    assert tooltip.fields == ["block_name", "district_name", "state_name", "_tooltip_value", "_tooltip_rank"]
    assert tooltip.aliases == ["Block", "District", "State", "Value", "Rank in district"]


def test_build_geojson_tooltip_keeps_percentile_risk_class_for_standard_metrics() -> None:
    tooltip = build_geojson_tooltip(
        level="district",
        map_mode="Absolute value",
        has_baseline=False,
        rank_scope_label="state",
        metric_slug="tas_annual_mean",
    )

    assert tooltip.fields == ["district_name", "state_name", "_tooltip_value", "_risk_class", "_tooltip_rank"]
    assert tooltip.aliases == ["District", "State", "Value", "Risk class", "Rank in state"]

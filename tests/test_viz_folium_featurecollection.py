"""Tests for lightweight Folium FeatureCollection patch helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from india_resilience_tool.data.admin_coverage import compute_coverage_diagnostics
from india_resilience_tool.utils.naming import alias
from india_resilience_tool.viz.folium_featurecollection import (
    build_geojson_tooltip,
    build_props_map_from_gdf,
    clone_featurecollection_for_patch,
    patch_fc_properties,
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


def test_build_props_map_from_gdf_uses_state_aware_district_keys() -> None:
    prop_df = pd.DataFrame(
        {
            "state_name": ["Chhattisgarh", "Maharashtra"],
            "district_name": ["Raigarh", "Raigarh"],
            "tas_annual_mean": [30.0, 20.0],
            "fillColor": ["#aa0000", "#00aa00"],
            "_tooltip_value": ["30.0", "20.0"],
        }
    )

    props_map, _value_cols, _text_cols = build_props_map_from_gdf(
        prop_df,
        level="district",
        alias_fn=alias,
        feature_key_col="__key",
        metric_col="tas_annual_mean",
        map_value_col="tas_annual_mean",
    )

    assert "chhattisgarh|raigarh" in props_map
    assert "maharashtra|raigarh" in props_map
    assert props_map["chhattisgarh|raigarh"]["state_name"] == "Chhattisgarh"
    assert props_map["maharashtra|raigarh"]["state_name"] == "Maharashtra"


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


def test_compute_coverage_diagnostics_uses_actual_props_map_hit_rate() -> None:
    feature_collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"state_name": "Telangana", "district_name": "Adilabad", "__key": "telangana|adilabad"},
                "geometry": None,
            },
            {
                "type": "Feature",
                "properties": {"state_name": "Odisha", "district_name": "Cuttack", "__key": "odisha|cuttack"},
                "geometry": None,
            },
            {
                "type": "Feature",
                "properties": {"state_name": "Punjab", "district_name": "Amritsar", "__key": "punjab|amritsar"},
                "geometry": None,
            },
        ],
    }
    props_map = {
        "telangana|adilabad": {"fillColor": "#ff0000"},
    }
    master_df = pd.DataFrame(
        {
            "state": ["Telangana", "Odisha"],
            "district": ["Adilabad", "Cuttack"],
            "tas_annual_mean": [28.26, pd.NA],
        }
    )

    diagnostics = compute_coverage_diagnostics(
        feature_collection=feature_collection,
        level="district",
        alias_fn=alias,
        feature_key_col="__key",
        props_map=props_map,
        master_df=master_df,
        map_value_col="tas_annual_mean",
        metric_col="tas_annual_mean",
        baseline_col=None,
    )

    assert diagnostics.total_feature_keys == 3
    assert diagnostics.matched_feature_keys == 1
    assert diagnostics.coverage_pct == pytest.approx(100.0 / 3.0)
    assert diagnostics.missing_master_row_keys == ("punjab|amritsar",)
    assert diagnostics.null_value_keys == ("odisha|cuttack",)
    assert diagnostics.broken_join_keys == ()


def test_compute_coverage_diagnostics_flags_broken_joins_from_master_value_keys() -> None:
    feature_collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"state_name": "Telangana", "district_name": "Adilabad", "__key": "telangana|adilabad"},
                "geometry": None,
            }
        ],
    }
    master_df = pd.DataFrame(
        {
            "state": ["Telangana"],
            "district": ["Adilabad"],
            "tas_annual_mean": [28.26],
        }
    )

    diagnostics = compute_coverage_diagnostics(
        feature_collection=feature_collection,
        level="district",
        alias_fn=alias,
        feature_key_col="__key",
        props_map={},
        master_df=master_df,
        map_value_col="tas_annual_mean",
        metric_col="tas_annual_mean",
        baseline_col=None,
    )

    assert diagnostics.matched_feature_keys == 0
    assert diagnostics.missing_master_row_keys == ()
    assert diagnostics.null_value_keys == ()
    assert diagnostics.broken_join_keys == ("telangana|adilabad",)

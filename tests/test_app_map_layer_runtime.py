"""Tests for map-layer runtime render-path optimizations."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from india_resilience_tool.app.map_layer_runtime import build_folium_map_for_selection
from india_resilience_tool.utils.naming import alias


def _touch(path: Path) -> Path:
    path.write_text("{}", encoding="utf-8")
    return path


def _block_feature(*, block_name: str, district_name: str = "Adilabad", state_name: str = "Telangana") -> dict:
    return {
        "type": "Feature",
        "properties": {
            "block_name": block_name,
            "district_name": district_name,
            "state_name": state_name,
            "__bkey": f"{alias(state_name)}|{alias(district_name)}|{alias(block_name)}",
        },
        "geometry": {"type": "Polygon", "coordinates": (((0, 0), (1, 0), (1, 1), (0, 0)),)},
    }


def _district_feature(*, district_name: str = "Adilabad", state_name: str = "Telangana") -> dict:
    return {
        "type": "Feature",
        "properties": {
            "district_name": district_name,
            "state_name": state_name,
            "__key": alias(district_name),
        },
        "geometry": {"type": "Polygon", "coordinates": (((0, 0), (1, 0), (1, 1), (0, 0)),)},
    }


def _common_kwargs(tmp_path: Path) -> dict:
    return {
        "adm2_geojson_path": _touch(tmp_path / "adm2.geojson"),
        "adm3_geojson_path": _touch(tmp_path / "adm3.geojson"),
        "basin_geojson_path": _touch(tmp_path / "basin.geojson"),
        "subbasin_geojson_path": _touch(tmp_path / "subbasin.geojson"),
        "river_display_geojson_path": _touch(tmp_path / "river.geojson"),
        "simplify_tolerance_adm2": 0.01,
        "simplify_tolerance_adm3": 0.01,
        "adm1": None,
        "map_center": [0.0, 0.0],
        "map_zoom": 6.0,
        "bounds_latlon": [[0.0, 0.0], [1.0, 1.0]],
        "map_mode": "Absolute value",
        "baseline_col": None,
        "rank_scope_label": "state",
        "alias_fn": alias,
        "normalize_state_fn": alias,
        "crosswalk_overlay": None,
        "show_river_network": False,
        "resolved_river_basin_name": None,
        "hover_enabled": False,
    }


def test_build_folium_map_for_selection_uses_district_scoped_block_shard(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import india_resilience_tool.app.geo_cache as geo_cache
    import india_resilience_tool.app.views.map_view as map_view

    calls = {"district": 0, "state": 0}
    district_fc = {
        "type": "FeatureCollection",
        "features": [_block_feature(block_name="Adilabad Rural")],
    }

    def _district_builder(**kwargs):
        calls["district"] += 1
        selector = f"{alias('Telangana')}|{alias('Adilabad')}"
        return {selector: district_fc, "all": district_fc}

    def _state_builder(**kwargs):
        calls["state"] += 1
        return {"telangana": district_fc, "all": district_fc}

    monkeypatch.setattr(geo_cache, "build_adm3_geojson_by_district", _district_builder)
    monkeypatch.setattr(geo_cache, "build_adm3_geojson_by_state", _state_builder)
    monkeypatch.setattr(map_view, "build_choropleth_map_with_geojson_layer", lambda **kwargs: kwargs["fc"])

    merged = pd.DataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Adilabad"],
            "block_name": ["Adilabad Rural"],
            "tas_annual_mean": [28.26],
            "fillColor": ["#ff0000"],
        }
    )

    out = build_folium_map_for_selection(
        level="block",
        merged=merged,
        display_gdf=merged,
        session_state={},
        render_signature=("block", "render"),
        selected_state="Telangana",
        selected_district="Adilabad",
        selected_basin="All",
        selected_subbasin="All",
        metric_col="tas_annual_mean",
        map_value_col="tas_annual_mean",
        **_common_kwargs(tmp_path),
    )

    assert calls["district"] == 1
    assert calls["state"] == 0
    assert len(out["features"]) == 1
    assert out["features"][0]["properties"]["block_name"] == "Adilabad Rural"


def test_build_folium_map_for_selection_reuses_patched_featurecollection_cache(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import india_resilience_tool.app.geo_cache as geo_cache
    import india_resilience_tool.app.views.map_view as map_view
    import india_resilience_tool.viz.folium_featurecollection as fc_helpers

    source_fc = {
        "type": "FeatureCollection",
        "features": [_district_feature()],
    }
    monkeypatch.setattr(
        geo_cache,
        "build_adm2_geojson_by_state",
        lambda **kwargs: {"telangana": source_fc, "all": source_fc},
    )
    monkeypatch.setattr(map_view, "build_choropleth_map_with_geojson_layer", lambda **kwargs: kwargs["fc"])

    patch_calls = {"count": 0}
    original_patch = fc_helpers.patch_fc_properties

    def _counting_patch(*args, **kwargs):
        patch_calls["count"] += 1
        return original_patch(*args, **kwargs)

    monkeypatch.setattr(fc_helpers, "patch_fc_properties", _counting_patch)

    merged = pd.DataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Adilabad"],
            "tas_annual_mean": [28.26],
            "fillColor": ["#ff0000"],
        }
    )
    session_state: dict = {}
    kwargs = dict(
        level="district",
        merged=merged,
        display_gdf=merged,
        session_state=session_state,
        render_signature=("district", "render"),
        selected_state="Telangana",
        selected_district="All",
        selected_basin="All",
        selected_subbasin="All",
        metric_col="tas_annual_mean",
        map_value_col="tas_annual_mean",
        **_common_kwargs(tmp_path),
    )

    first = build_folium_map_for_selection(**kwargs)
    second = build_folium_map_for_selection(**kwargs)

    assert patch_calls["count"] == 1
    assert first["features"][0]["properties"]["fillColor"] == "#ff0000"
    assert second["features"][0]["properties"]["fillColor"] == "#ff0000"

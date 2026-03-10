from __future__ import annotations

from india_resilience_tool.app.crosswalk_runtime import (
    clear_crosswalk_overlay,
    navigate_from_crosswalk_overlap,
    overlay_matches_context,
    set_crosswalk_overlay_from_context,
)
from india_resilience_tool.data.crosswalks import CrosswalkContext, CrosswalkOverlap
from india_resilience_tool.viz.folium_featurecollection import filter_fc_by_feature_keys


def _district_context() -> CrosswalkContext:
    return CrosswalkContext(
        direction="district_to_subbasin",
        selected_name="Nizamabad",
        section_title="Hydrology context",
        overlap_count=2,
        classification="dominant_subbasin",
        dominant_counterpart_id="SB01",
        dominant_counterpart_name="Godavari Upper",
        dominant_counterpart_fraction=0.8,
        primary_basin_id="B01",
        primary_basin_name="Godavari",
        all_counterpart_ids=("SB01", "SB02"),
        overlaps=(
            CrosswalkOverlap(
                counterpart_id="SB01",
                counterpart_name="Godavari Upper",
                counterpart_state_name=None,
                basin_id="B01",
                basin_name="Godavari",
                intersection_area_km2=120.0,
                selected_fraction=0.8,
                counterpart_fraction=0.5,
            ),
        ),
        explanation="Most of Nizamabad lies in Godavari Upper.",
        coordination_note="This district intersects multiple sub-basins.",
    )


def _subbasin_context() -> CrosswalkContext:
    return CrosswalkContext(
        direction="subbasin_to_district",
        selected_name="Godavari Middle",
        section_title="Administrative context",
        overlap_count=2,
        classification="distributed_across_districts",
        dominant_counterpart_id="Telangana::Karimnagar",
        dominant_counterpart_name="Karimnagar",
        dominant_counterpart_fraction=0.6,
        primary_basin_id="B01",
        primary_basin_name="Godavari",
        all_counterpart_ids=("Telangana::Karimnagar", "Telangana::Nizamabad"),
        overlaps=(
            CrosswalkOverlap(
                counterpart_id="Telangana::Karimnagar",
                counterpart_name="Karimnagar",
                counterpart_state_name="Telangana",
                basin_id="B01",
                basin_name="Godavari",
                intersection_area_km2=80.0,
                selected_fraction=0.6,
                counterpart_fraction=0.75,
            ),
        ),
        explanation="This sub-basin spans multiple districts.",
        coordination_note="Action is distributed across jurisdictions.",
    )


def test_set_crosswalk_overlay_from_context_sets_expected_overlay() -> None:
    session_state: dict[str, object] = {}

    set_crosswalk_overlay_from_context(
        session_state,
        context=_district_context(),
        feature_keys=("SB01", "SB02"),
    )

    assert session_state["crosswalk_overlay"] == {
        "level": "sub_basin",
        "feature_keys": ["SB01", "SB02"],
        "label": "Related sub-basins",
        "source_direction": "district_to_subbasin",
        "selected_name": "Nizamabad",
    }


def test_navigate_from_crosswalk_overlap_opens_subbasin_view() -> None:
    session_state: dict[str, object] = {"crosswalk_overlay": {"level": "sub_basin"}}

    navigate_from_crosswalk_overlap(
        session_state,
        context=_district_context(),
        overlap={
            "counterpart_name": "Godavari Upper",
            "basin_name": "Godavari",
        },
    )

    pending = session_state["_pending_crosswalk_navigation"]
    assert isinstance(pending, dict)
    assert pending["spatial_family"] == "hydro"
    assert pending["admin_level"] == "sub_basin"
    assert pending["analysis_mode"] == "Single sub-basin focus"
    assert pending["selected_basin"] == "Godavari"
    assert pending["selected_subbasin"] == "Godavari Upper"
    assert session_state["crosswalk_overlay"] is None
    assert session_state["jump_to_map"] is True


def test_navigate_from_crosswalk_overlap_opens_district_view() -> None:
    session_state: dict[str, object] = {}

    navigate_from_crosswalk_overlap(
        session_state,
        context=_subbasin_context(),
        overlap={
            "counterpart_name": "Karimnagar",
            "counterpart_state_name": "Telangana",
            "basin_name": "Godavari",
        },
    )

    pending = session_state["_pending_crosswalk_navigation"]
    assert isinstance(pending, dict)
    assert pending["spatial_family"] == "admin"
    assert pending["admin_level"] == "district"
    assert pending["analysis_mode"] == "Single district focus"
    assert pending["selected_state"] == "Telangana"
    assert pending["selected_district"] == "Karimnagar"
    assert pending["selected_basin"] == "All"
    assert pending["selected_subbasin"] == "All"
    assert session_state["jump_to_map"] is True


def test_overlay_matches_context_checks_direction_level_and_name() -> None:
    context = _subbasin_context()
    overlay = {
        "level": "district",
        "feature_keys": ["Telangana::Karimnagar"],
        "label": "Related districts",
        "source_direction": "subbasin_to_district",
        "selected_name": "Godavari Middle",
    }

    assert overlay_matches_context(overlay, context=context) is True

    clear_crosswalk_overlay(overlay)
    assert overlay_matches_context(overlay.get("crosswalk_overlay"), context=context) is False


def test_filter_fc_by_feature_keys_filters_district_overlay_by_state_and_name() -> None:
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"district_name": "Karimnagar", "state_name": "Telangana"},
                "geometry": None,
            },
            {
                "type": "Feature",
                "properties": {"district_name": "Karimnagar", "state_name": "Maharashtra"},
                "geometry": None,
            },
        ],
    }

    filtered = filter_fc_by_feature_keys(
        fc,
        feature_keys=("Telangana::Karimnagar",),
        level="district",
        alias_fn=lambda s: str(s).strip().lower(),
    )

    assert len(filtered["features"]) == 1
    assert filtered["features"][0]["properties"]["state_name"] == "Telangana"

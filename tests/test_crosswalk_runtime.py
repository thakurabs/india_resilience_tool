from __future__ import annotations

from india_resilience_tool.app.crosswalk_runtime import (
    clear_crosswalk_overlay,
    navigate_from_crosswalk_overlap,
    overlay_matches_context,
    set_crosswalk_overlay_from_context,
)
from india_resilience_tool.data.crosswalks import CrosswalkContext, CrosswalkOverlap
from india_resilience_tool.viz.folium_featurecollection import filter_fc_by_feature_keys


def _district_to_subbasin_context() -> CrosswalkContext:
    return CrosswalkContext(
        direction="district_to_sub_basin",
        selected_level="district",
        counterpart_level="sub_basin",
        selected_name="Nizamabad",
        section_title="Hydrology context",
        overlap_count=2,
        classification="dominant_subbasin",
        dominant_counterpart_id="SB01",
        dominant_counterpart_name="Godavari Upper",
        dominant_counterpart_fraction=0.8,
        dominant_label="Dominant sub-basin",
        primary_basin_id="B01",
        primary_basin_name="Godavari",
        all_counterpart_ids=("SB01", "SB02"),
        overlaps=(
            CrosswalkOverlap(
                counterpart_id="SB01",
                counterpart_name="Godavari Upper",
                counterpart_level="sub_basin",
                counterpart_state_name=None,
                counterpart_parent_name="Godavari",
                basin_id="B01",
                basin_name="Godavari",
                intersection_area_km2=120.0,
                selected_fraction=0.8,
                counterpart_fraction=0.5,
            ),
        ),
        explanation="Most of Nizamabad lies in Godavari Upper.",
        coordination_note="This district intersects multiple sub-basins.",
        highlight_action_label="Highlight related sub-basins",
        open_action_label="Open sub-basin",
        selected_fraction_label="District share",
        counterpart_fraction_label="Sub-basin share",
    )


def _subbasin_to_district_context() -> CrosswalkContext:
    return CrosswalkContext(
        direction="sub_basin_to_district",
        selected_level="sub_basin",
        counterpart_level="district",
        selected_name="Godavari Middle",
        section_title="Administrative context",
        overlap_count=2,
        classification="distributed_across_districts",
        dominant_counterpart_id="Telangana::Karimnagar",
        dominant_counterpart_name="Karimnagar",
        dominant_counterpart_fraction=0.6,
        dominant_label="District covering the largest share of this sub-basin",
        primary_basin_id="B01",
        primary_basin_name="Godavari",
        all_counterpart_ids=("Telangana::Karimnagar", "Telangana::Nizamabad"),
        overlaps=(
            CrosswalkOverlap(
                counterpart_id="Telangana::Karimnagar",
                counterpart_name="Karimnagar",
                counterpart_level="district",
                counterpart_state_name="Telangana",
                counterpart_parent_name=None,
                basin_id="B01",
                basin_name="Godavari",
                intersection_area_km2=80.0,
                selected_fraction=0.6,
                counterpart_fraction=0.75,
            ),
        ),
        explanation="This sub-basin spans multiple districts.",
        coordination_note="Action is distributed across jurisdictions.",
        highlight_action_label="Highlight related districts",
        open_action_label="Open district",
        selected_fraction_label="Share of sub-basin",
        counterpart_fraction_label="Share of district in sub-basin",
    )


def _block_to_basin_context() -> CrosswalkContext:
    return CrosswalkContext(
        direction="block_to_basin",
        selected_level="block",
        counterpart_level="basin",
        selected_name="Armur",
        section_title="Basin context",
        overlap_count=2,
        classification="dominant_basin",
        dominant_counterpart_id="B01",
        dominant_counterpart_name="Godavari",
        dominant_counterpart_fraction=0.9,
        dominant_label="Dominant basin",
        primary_basin_id="B01",
        primary_basin_name="Godavari",
        all_counterpart_ids=("B01", "B02"),
        overlaps=(
            CrosswalkOverlap(
                counterpart_id="B01",
                counterpart_name="Godavari",
                counterpart_level="basin",
                counterpart_state_name=None,
                counterpart_parent_name=None,
                basin_id="B01",
                basin_name="Godavari",
                intersection_area_km2=45.0,
                selected_fraction=0.9,
                counterpart_fraction=0.08,
            ),
        ),
        explanation="Most of Armur lies in Godavari.",
        coordination_note=None,
        highlight_action_label="Highlight related basins",
        open_action_label="Open basin",
        selected_fraction_label="Block share",
        counterpart_fraction_label="Basin share",
    )


def _basin_to_block_context() -> CrosswalkContext:
    return CrosswalkContext(
        direction="basin_to_block",
        selected_level="basin",
        counterpart_level="block",
        selected_name="Krishna",
        section_title="Administrative context",
        overlap_count=2,
        classification="distributed_across_blocks",
        dominant_counterpart_id="Telangana::Nizamabad::Bheemgal",
        dominant_counterpart_name="Bheemgal",
        dominant_counterpart_fraction=0.1,
        dominant_label="Block covering the largest share of this basin",
        primary_basin_id="B02",
        primary_basin_name="Krishna",
        all_counterpart_ids=("Telangana::Nizamabad::Armur", "Telangana::Nizamabad::Bheemgal"),
        overlaps=(
            CrosswalkOverlap(
                counterpart_id="Telangana::Nizamabad::Bheemgal",
                counterpart_name="Bheemgal",
                counterpart_level="block",
                counterpart_state_name="Telangana",
                counterpart_parent_name="Nizamabad",
                basin_id="B02",
                basin_name="Krishna",
                intersection_area_km2=20.0,
                selected_fraction=0.1,
                counterpart_fraction=0.8,
            ),
        ),
        explanation="This basin spans multiple blocks.",
        coordination_note="Action is distributed across jurisdictions.",
        highlight_action_label="Highlight related blocks",
        open_action_label="Open block",
        selected_fraction_label="Share of basin",
        counterpart_fraction_label="Share of block in basin",
    )


def test_set_crosswalk_overlay_from_context_sets_expected_overlay() -> None:
    session_state: dict[str, object] = {}
    set_crosswalk_overlay_from_context(
        session_state,
        context=_district_to_subbasin_context(),
        feature_keys=("SB01", "SB02"),
    )
    assert session_state["crosswalk_overlay"] == {
        "level": "sub_basin",
        "feature_keys": ["SB01", "SB02"],
        "label": "related sub-basins",
        "source_direction": "district_to_sub_basin",
        "selected_name": "Nizamabad",
    }


def test_navigate_from_crosswalk_overlap_opens_subbasin_view() -> None:
    session_state: dict[str, object] = {"crosswalk_overlay": {"level": "sub_basin"}}
    navigate_from_crosswalk_overlap(
        session_state,
        context=_district_to_subbasin_context(),
        overlap={"counterpart_name": "Godavari Upper", "basin_name": "Godavari"},
    )
    pending = session_state["_pending_crosswalk_navigation"]
    assert isinstance(pending, dict)
    assert pending["spatial_family"] == "hydro"
    assert pending["admin_level"] == "sub_basin"
    assert pending["selected_basin"] == "Godavari"
    assert pending["selected_subbasin"] == "Godavari Upper"
    assert session_state["crosswalk_overlay"] is None


def test_navigate_from_crosswalk_overlap_opens_district_view() -> None:
    session_state: dict[str, object] = {}
    navigate_from_crosswalk_overlap(
        session_state,
        context=_subbasin_to_district_context(),
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
    assert pending["selected_state"] == "Telangana"
    assert pending["selected_district"] == "Karimnagar"


def test_navigate_from_crosswalk_overlap_opens_basin_view() -> None:
    session_state: dict[str, object] = {}
    navigate_from_crosswalk_overlap(
        session_state,
        context=_block_to_basin_context(),
        overlap={"counterpart_name": "Godavari"},
    )
    pending = session_state["_pending_crosswalk_navigation"]
    assert isinstance(pending, dict)
    assert pending["spatial_family"] == "hydro"
    assert pending["admin_level"] == "basin"
    assert pending["selected_basin"] == "Godavari"
    assert pending["selected_subbasin"] == "All"


def test_navigate_from_crosswalk_overlap_opens_block_view() -> None:
    session_state: dict[str, object] = {}
    navigate_from_crosswalk_overlap(
        session_state,
        context=_basin_to_block_context(),
        overlap={
            "counterpart_name": "Bheemgal",
            "counterpart_state_name": "Telangana",
            "counterpart_parent_name": "Nizamabad",
        },
    )
    pending = session_state["_pending_crosswalk_navigation"]
    assert isinstance(pending, dict)
    assert pending["spatial_family"] == "admin"
    assert pending["admin_level"] == "block"
    assert pending["selected_state"] == "Telangana"
    assert pending["selected_district"] == "Nizamabad"
    assert pending["selected_block"] == "Bheemgal"


def test_overlay_matches_context_checks_direction_level_and_name() -> None:
    context = _subbasin_to_district_context()
    overlay = {
        "level": "district",
        "feature_keys": ["Telangana::Karimnagar"],
        "label": "related districts",
        "source_direction": "sub_basin_to_district",
        "selected_name": "Godavari Middle",
    }
    assert overlay_matches_context(overlay, context=context) is True
    clear_crosswalk_overlay(overlay)
    assert overlay_matches_context(overlay.get("crosswalk_overlay"), context=context) is False


def test_filter_fc_by_feature_keys_filters_district_overlay_by_state_and_name() -> None:
    fc = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {"district_name": "Karimnagar", "state_name": "Telangana"}, "geometry": None},
            {"type": "Feature", "properties": {"district_name": "Karimnagar", "state_name": "Maharashtra"}, "geometry": None},
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


def test_filter_fc_by_feature_keys_filters_block_overlay_by_triplet() -> None:
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"block_name": "Bheemgal", "district_name": "Nizamabad", "state_name": "Telangana"},
                "geometry": None,
            },
            {
                "type": "Feature",
                "properties": {"block_name": "Bheemgal", "district_name": "Adilabad", "state_name": "Telangana"},
                "geometry": None,
            },
        ],
    }
    filtered = filter_fc_by_feature_keys(
        fc,
        feature_keys=("Telangana::Nizamabad::Bheemgal",),
        level="block",
        alias_fn=lambda s: str(s).strip().lower(),
        key_col="__bkey",
    )
    assert len(filtered["features"]) == 1
    assert filtered["features"][0]["properties"]["district_name"] == "Nizamabad"

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

import india_resilience_tool.app.landing_runtime as landing_runtime
from india_resilience_tool.analysis.bundle_scores import BundleMetricSpec
from india_resilience_tool.app.landing_runtime import (
    LandingMetricContext,
    _apply_landing_map_click,
    _assemble_bundle_context,
    _build_landing_search_options,
    _intersect_bundle_scenario_period_pairs,
    apply_landing_back,
    build_deep_dive_handoff,
    ensure_landing_state,
    set_landing_focus_district,
    set_landing_focus_india,
    set_landing_focus_state,
    sync_landing_widget_state,
)


def _metric_context(
    slug: str,
    *,
    pairs: tuple[tuple[str, str], ...],
) -> LandingMetricContext:
    return LandingMetricContext(
        spec=BundleMetricSpec(slug=slug, label=slug, column=slug, higher_is_worse=True),
        source_signature=(),
        source_paths=(),
        available_pairs=pairs,
    )


def _adm1_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "shapeName": ["Telangana", "Maharashtra"],
            "geometry": [
                Polygon([(0, 0), (4, 0), (4, 4), (0, 4)]),
                Polygon([(5, 0), (9, 0), (9, 4), (5, 4)]),
            ],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )


def _adm2_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Telangana", "Maharashtra"],
            "district_name": ["Nalgonda", "Khammam", "Pune"],
            "geometry": [
                Polygon([(0, 0), (2, 0), (2, 4), (0, 4)]),
                Polygon([(2, 0), (4, 0), (4, 4), (2, 4)]),
                Polygon([(5, 0), (9, 0), (9, 4), (5, 4)]),
            ],
        },
        geometry="geometry",
        crs="EPSG:4326",
    )


def test_ensure_landing_state_sets_frozen_defaults() -> None:
    session_state: dict[str, object] = {}

    ensure_landing_state(session_state)

    assert session_state["landing_active"] is True
    assert session_state["landing_bundle"] == "Heat Risk"
    assert session_state["landing_scenario"] == "ssp585"
    assert session_state["landing_period"] == "2040-2060"
    assert session_state["landing_focus_level"] == "india"
    assert session_state["landing_selected_state"] is None
    assert session_state["landing_selected_district"] is None
    assert session_state["landing_tab"] == "Rankings"
    assert session_state["landing_search_selection"] is None
    assert session_state["landing_search_last_applied"] is None
    assert session_state["landing_search_reset_pending"] is False


def test_sync_landing_widget_state_updates_scenario_period_pair() -> None:
    session_state: dict[str, object] = {
        "landing_scenario": "ssp585",
        "landing_period": "2040-2060",
        "landing_context_pair": ("ssp245", "2020-2040"),
    }

    sync_landing_widget_state(session_state)

    assert session_state["landing_scenario"] == "ssp245"
    assert session_state["landing_period"] == "2020-2040"


def test_landing_focus_transitions_cover_india_state_district_back_reset() -> None:
    session_state: dict[str, object] = {}
    ensure_landing_state(session_state)

    set_landing_focus_state(session_state, "Telangana")
    assert session_state["landing_focus_level"] == "state"
    assert session_state["landing_selected_state"] == "Telangana"
    assert session_state["landing_selected_district"] is None

    set_landing_focus_district(session_state, "Telangana", "Nalgonda")
    assert session_state["landing_focus_level"] == "district"
    assert session_state["landing_selected_state"] == "Telangana"
    assert session_state["landing_selected_district"] == "Nalgonda"

    apply_landing_back(session_state)
    assert session_state["landing_focus_level"] == "state"
    assert session_state["landing_selected_state"] == "Telangana"
    assert session_state["landing_selected_district"] is None

    apply_landing_back(session_state)
    assert session_state["landing_focus_level"] == "india"
    assert session_state["landing_selected_state"] is None
    assert session_state["landing_selected_district"] is None

    set_landing_focus_district(session_state, "Telangana", "Nalgonda")
    set_landing_focus_india(session_state)
    assert session_state["landing_focus_level"] == "india"
    assert session_state["landing_selected_state"] is None
    assert session_state["landing_selected_district"] is None


def test_build_deep_dive_handoff_preserves_bundle_and_geography_context() -> None:
    landing_state = {
        "landing_bundle": "Heat Risk",
        "landing_scenario": "ssp585",
        "landing_period": "2040-2060",
        "landing_focus_level": "district",
        "landing_selected_state": "Telangana",
        "landing_selected_district": "Nalgonda",
    }

    handoff = build_deep_dive_handoff(
        landing_state,
        bundle_domain="Heat Risk",
        metric_slug="tas_annual_mean",
    )

    assert handoff["landing_active"] is False
    assert handoff["spatial_family"] == "admin"
    assert handoff["admin_level"] == "district"
    assert handoff["selected_pillar"] == "Climate Hazards"
    assert handoff["selected_bundle"] == "Heat Risk"
    assert handoff["selected_var"] == "tas_annual_mean"
    assert handoff["sel_scenario"] == "ssp585"
    assert handoff["sel_period"] == "2040-2060"
    assert handoff["selected_state"] == "Telangana"
    assert handoff["selected_district"] == "Nalgonda"
    assert handoff["map_mode"] == "Absolute value"


def test_build_deep_dive_handoff_requires_non_empty_metric_slug() -> None:
    with pytest.raises(ValueError, match="metric_slug"):
        build_deep_dive_handoff(
            {"landing_focus_level": "india"},
            bundle_domain="Heat Risk",
            metric_slug="",
        )


def test_intersect_bundle_scenario_period_pairs_uses_required_metric_intersection() -> None:
    contexts = [
        _metric_context(
            "metric_a",
            pairs=(("ssp245", "2020-2040"), ("ssp585", "2040-2060")),
        ),
        _metric_context(
            "metric_b",
            pairs=(("ssp245", "2020-2040"), ("ssp585", "2040-2060")),
        ),
        _metric_context(
            "metric_c",
            pairs=(("ssp245", "2020-2040"),),
        ),
    ]

    assert _intersect_bundle_scenario_period_pairs(contexts) == [("ssp245", "2020-2040")]


def test_assemble_bundle_context_builds_ranked_outputs_deterministically() -> None:
    merged_frame = pd.DataFrame(
        {
            "state_name": ["A", "A", "B"],
            "district_name": ["One", "Two", "Three"],
            "metric_a": [10.0, 20.0, 30.0],
            "metric_b": [10.0, 20.0, 30.0],
        }
    )
    metric_specs = [
        BundleMetricSpec(slug="metric_a", label="Metric A", column="metric_a", higher_is_worse=True),
        BundleMetricSpec(slug="metric_b", label="Metric B", column="metric_b", higher_is_worse=True),
    ]

    district_scores, state_scores, returned_specs = _assemble_bundle_context(
        merged_frame,
        metric_specs=metric_specs,
    )

    assert [spec.slug for spec in returned_specs] == ["metric_a", "metric_b"]
    assert dict(zip(state_scores["state_name"], state_scores["bundle_score"])) == {"A": 25.0, "B": 100.0}

    district_by_name = district_scores.set_index("district_name")
    assert district_by_name.loc["Two", "district_rank"] == 1.0
    assert district_by_name.loc["One", "district_rank"] == 2.0
    assert district_by_name.loc["Two", "district_count"] == 2


def test_resolve_first_valid_landing_metric_skips_invalid_first_metric(monkeypatch: pytest.MonkeyPatch) -> None:
    contexts = [
        _metric_context("metric_a", pairs=(("ssp585", "2040-2060"),)),
        _metric_context("metric_b", pairs=(("ssp585", "2040-2060"),)),
    ]

    def fake_loader(
        metric_slug: str,
        scenario: str,
        period: str,
        stat: str,
        source_signature: tuple[tuple[str, float | None], ...],
        source_paths: tuple[str, ...],
    ) -> pd.DataFrame:
        _ = (scenario, period, stat, source_signature, source_paths)
        if metric_slug == "metric_a":
            return pd.DataFrame({"state_name": ["A"], "district_name": ["One"], "raw_metric_value": [float("nan")]})
        return pd.DataFrame({"state_name": ["A"], "district_name": ["One"], "raw_metric_value": [42.0]})

    monkeypatch.setattr(landing_runtime, "_load_metric_district_values_cached", fake_loader)

    metric_slug = landing_runtime._resolve_first_valid_landing_metric(
        "Heat Risk",
        scenario="ssp585",
        period="2040-2060",
        stat="mean",
        data_dir=Path("."),
        metric_contexts=contexts,
    )

    assert metric_slug == "metric_b"


def test_resolve_first_valid_landing_metric_returns_none_when_no_metric_has_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contexts = [_metric_context("metric_a", pairs=(("ssp585", "2040-2060"),))]

    def fake_loader(
        metric_slug: str,
        scenario: str,
        period: str,
        stat: str,
        source_signature: tuple[tuple[str, float | None], ...],
        source_paths: tuple[str, ...],
    ) -> pd.DataFrame:
        _ = (metric_slug, scenario, period, stat, source_signature, source_paths)
        return pd.DataFrame({"state_name": ["A"], "district_name": ["One"], "raw_metric_value": [float("nan")]})

    monkeypatch.setattr(landing_runtime, "_load_metric_district_values_cached", fake_loader)

    metric_slug = landing_runtime._resolve_first_valid_landing_metric(
        "Heat Risk",
        scenario="ssp585",
        period="2040-2060",
        stat="mean",
        data_dir=Path("."),
        metric_contexts=contexts,
    )

    assert metric_slug is None


def test_build_landing_search_options_includes_state_and_district_labels() -> None:
    state_scores = pd.DataFrame({"state_name": ["Telangana", "Maharashtra"]})
    district_scores = pd.DataFrame(
        {
            "district_name": ["Nalgonda", "Jayashankar Bhupalpalli"],
            "state_name": ["Telangana", "Telangana"],
        }
    )

    options = _build_landing_search_options(state_scores, district_scores)

    assert options["State: Telangana"] == ("state", "Telangana", None)
    assert options["District: Nalgonda, Telangana"] == ("district", "Telangana", "Nalgonda")
    assert options["District: Jayashankar Bhupalpalli, Telangana"] == (
        "district",
        "Telangana",
        "Jayashankar Bhupalpalli",
    )


def test_build_landing_search_options_orders_states_before_districts() -> None:
    state_scores = pd.DataFrame({"state_name": ["Telangana", "Andhra Pradesh"]})
    district_scores = pd.DataFrame(
        {
            "district_name": ["Nalgonda", "Visakhapatnam"],
            "state_name": ["Telangana", "Andhra Pradesh"],
        }
    )

    options = _build_landing_search_options(state_scores, district_scores)
    labels = list(options.keys())

    assert labels == [
        "State: Andhra Pradesh",
        "State: Telangana",
        "District: Visakhapatnam, Andhra Pradesh",
        "District: Nalgonda, Telangana",
    ]


def test_apply_landing_map_click_enters_state_focus_from_india() -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()

    action = _apply_landing_map_click(
        focus_level="india",
        returned={
            "last_object_clicked": {
                "properties": {"state_name": "Telangana", "__state_key": "telangana"}
            }
        },
        clicked_state="Telangana",
        clicked_district=None,
        selected_state=None,
        selected_district=None,
        adm1=adm1,
        adm2=adm2,
    )

    assert action == ("focus_state", "Telangana", None)


def test_apply_landing_map_click_enters_state_focus_from_coordinates_only() -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()

    action = _apply_landing_map_click(
        focus_level="india",
        returned={"last_clicked": {"lat": 1.5, "lng": 1.5}},
        clicked_state=None,
        clicked_district=None,
        selected_state=None,
        selected_district=None,
        adm1=adm1,
        adm2=adm2,
    )

    assert action == ("focus_state", "Telangana", None)


def test_apply_landing_map_click_noops_on_invalid_india_click() -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()

    action = _apply_landing_map_click(
        focus_level="india",
        returned={},
        clicked_state="Unknown",
        clicked_district=None,
        selected_state=None,
        selected_district=None,
        adm1=adm1,
        adm2=adm2,
    )

    assert action == ("noop", None, None)


def test_apply_landing_map_click_enters_district_focus_from_state() -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()
    visible_districts = adm2[adm2["state_name"] == "Telangana"].copy()

    action = _apply_landing_map_click(
        focus_level="state",
        returned={
            "last_object_clicked": {
                "properties": {
                    "district_name": "Nalgonda",
                    "state_name": "Telangana",
                    "__district_key": "telangana|nalgonda",
                }
            }
        },
        clicked_state=None,
        clicked_district="Nalgonda",
        selected_state="Telangana",
        selected_district=None,
        adm1=adm1,
        adm2=adm2,
        visible_districts=visible_districts,
    )

    assert action == ("focus_district", "Telangana", "Nalgonda")


def test_apply_landing_map_click_switches_district_within_state_focus() -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()
    visible_districts = adm2[adm2["state_name"] == "Telangana"].copy()

    action = _apply_landing_map_click(
        focus_level="district",
        returned={
            "last_object_clicked": {
                "properties": {
                    "district_name": "Khammam",
                    "state_name": "Telangana",
                    "__district_key": "telangana|khammam",
                }
            }
        },
        clicked_state="Telangana",
        clicked_district="Khammam",
        selected_state="Telangana",
        selected_district="Nalgonda",
        adm1=adm1,
        adm2=adm2,
        visible_districts=visible_districts,
    )

    assert action == ("focus_district", "Telangana", "Khammam")


def test_apply_landing_map_click_noops_on_same_district_selection() -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()
    visible_districts = adm2[adm2["state_name"] == "Telangana"].copy()

    action = _apply_landing_map_click(
        focus_level="district",
        returned={
            "last_object_clicked": {
                "properties": {
                    "district_name": "Nalgonda",
                    "state_name": "Telangana",
                    "__district_key": "telangana|nalgonda",
                }
            }
        },
        clicked_state="Telangana",
        clicked_district="Nalgonda",
        selected_state="Telangana",
        selected_district="Nalgonda",
        adm1=adm1,
        adm2=adm2,
        visible_districts=visible_districts,
    )

    assert action == ("noop", None, None)


def test_apply_landing_map_click_noops_on_invalid_district_payload() -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()
    visible_districts = adm2[adm2["state_name"] == "Telangana"].copy()

    action = _apply_landing_map_click(
        focus_level="state",
        returned={},
        clicked_state=None,
        clicked_district="Unknown",
        selected_state="Telangana",
        selected_district=None,
        adm1=adm1,
        adm2=adm2,
        visible_districts=visible_districts,
    )

    assert action == ("noop", None, None)


def test_apply_landing_map_click_enters_district_focus_from_coordinates_only() -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()
    visible_districts = adm2[adm2["state_name"] == "Telangana"].copy()

    action = _apply_landing_map_click(
        focus_level="state",
        returned={"last_clicked": {"lat": 1.5, "lng": 1.0}},
        clicked_state=None,
        clicked_district=None,
        selected_state="Telangana",
        selected_district=None,
        adm1=adm1,
        adm2=adm2,
        visible_districts=visible_districts,
    )

    assert action == ("focus_district", "Telangana", "Nalgonda")

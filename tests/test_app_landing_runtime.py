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
    _bundle_metric_specs,
    _build_landing_search_options,
    _landing_bundle_domains,
    _sanitize_landing_context,
    _intersect_bundle_scenario_period_pairs,
    apply_landing_back,
    build_deep_dive_handoff,
    build_glance_handoff_from_deep_dive,
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
    weight: float = 1.0,
    higher_is_worse: bool = True,
    column: str | None = None,
    label: str | None = None,
    source_signature: tuple[tuple[str, float | None], ...] = (),
    source_paths: tuple[str, ...] = (),
) -> LandingMetricContext:
    return LandingMetricContext(
        spec=BundleMetricSpec(
            slug=slug,
            label=label or slug,
            column=column or slug,
            weight=weight,
            higher_is_worse=higher_is_worse,
        ),
        source_signature=source_signature,
        source_paths=source_paths,
        available_pairs=pairs,
    )


@pytest.fixture(autouse=True)
def _clear_landing_runtime_caches() -> None:
    landing_runtime._prepare_bundle_context_cached.clear()
    yield
    landing_runtime._prepare_bundle_context_cached.clear()


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


class _DummyContext:
    def __enter__(self) -> "_DummyContext":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _DummyRerun(RuntimeError):
    pass


class _DummyStreamlit:
    def __init__(self, session_state: dict[str, object]) -> None:
        self.session_state = session_state
        self.rerun_calls = 0

    def columns(self, spec) -> list[_DummyContext]:
        return [_DummyContext() for _ in spec]

    def expander(self, *args, **kwargs) -> _DummyContext:
        return _DummyContext()

    def selectbox(self, _label, options, index=None, key=None, **kwargs):
        if key is None:
            return options[index] if index is not None and options else None
        if key not in self.session_state:
            if index is None or not options:
                self.session_state[key] = None
            else:
                self.session_state[key] = options[index]
        return self.session_state.get(key)

    def radio(self, _label, options, key=None, **kwargs):
        if key is not None and key not in self.session_state:
            self.session_state[key] = options[0] if options else None
        return self.session_state.get(key) if key is not None else (options[0] if options else None)

    def button(self, *args, **kwargs) -> bool:
        return False

    def text_input(self, *args, value=None, **kwargs):
        return value

    def rerun(self) -> None:
        self.rerun_calls += 1
        raise _DummyRerun()

    def title(self, *args, **kwargs) -> None:
        return None

    def markdown(self, *args, **kwargs) -> None:
        return None

    def caption(self, *args, **kwargs) -> None:
        return None

    def info(self, *args, **kwargs) -> None:
        return None

    def error(self, *args, **kwargs) -> None:
        return None

    def write(self, *args, **kwargs) -> None:
        return None

    def json(self, *args, **kwargs) -> None:
        return None


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
    assert session_state[landing_runtime.LANDING_MAP_CONTEXT_KEY] is None
    assert session_state[landing_runtime.LANDING_MAP_INPUT_ARMED_KEY] is False


def test_landing_bundle_domains_hide_non_glance_bundles() -> None:
    assert _landing_bundle_domains() == [
        "Heat Risk",
        "Drought Risk",
        "Flood Inundation Depth (JRC)",
        "Flood & Extreme Rainfall Risk",
        "Heat Stress",
        "Cold Risk",
        "Agriculture & Growing Conditions",
    ]


def test_sanitize_landing_context_falls_back_from_hidden_bundle(monkeypatch, tmp_path: Path) -> None:
    session_state: dict[str, object] = {
        "landing_bundle": "Temperature Variability",
        "landing_scenario": "ssp585",
        "landing_period": "2040-2060",
    }

    monkeypatch.setattr(
        landing_runtime,
        "_bundle_scenario_period_options",
        lambda bundle_domain, *, data_dir: [("ssp585", "2040-2060")],
    )

    _sanitize_landing_context(session_state, data_dir=tmp_path)

    assert session_state["landing_bundle"] == "Heat Risk"
    assert session_state["landing_scenario"] == "ssp585"
    assert session_state["landing_period"] == "2040-2060"


def test_bundle_metric_specs_use_custom_heat_risk_weights() -> None:
    specs = _bundle_metric_specs("Heat Risk")
    by_slug = {spec.slug: spec for spec in specs}

    assert by_slug["tasmin_tropical_nights_gt25"].weight == 0.2 / 3.0
    assert by_slug["hwfi_tmean_90p"].weight == 0.15 / 2.0


def test_bundle_metric_specs_use_custom_heat_stress_weights() -> None:
    specs = _bundle_metric_specs("Heat Stress")
    by_slug = {spec.slug: spec for spec in specs}

    assert len(specs) == 11
    assert by_slug["twb_summer_mean"].weight == 0.20 / 2.0
    assert by_slug["wbd_gt3_le6"].weight == 0.15 / 2.0
    assert by_slug["twb_days_ge_28"].weight == 0.25 / 3.0
    assert "wbd_le_6" not in by_slug


def test_bundle_metric_specs_use_custom_cold_risk_weights() -> None:
    specs = _bundle_metric_specs("Cold Risk")
    by_slug = {spec.slug: spec for spec in specs}

    assert len(specs) == 11
    assert by_slug["tasmin_winter_min"].weight == 0.20 / 2.0
    assert by_slug["tnle10_cold_nights"].weight == 0.25 / 3.0
    assert by_slug["tnle10_consecutive_cold_nights"].weight == 0.20 / 2.0
    assert "fd_frost_days" not in by_slug
    assert "tnlt2_cold_nights" not in by_slug


def test_bundle_metric_specs_use_custom_drought_risk_weights() -> None:
    specs = _bundle_metric_specs("Drought Risk")
    by_slug = {spec.slug: spec for spec in specs}

    assert len(specs) == 3
    assert by_slug["spi3_count_events_lt_minus1"].weight == 0.20
    assert by_slug["spi6_count_events_lt_minus1"].weight == 0.30
    assert by_slug["spi12_count_events_lt_minus1"].weight == 0.50


def test_bundle_metric_specs_use_custom_jrc_flood_weights_only() -> None:
    specs = _bundle_metric_specs("Flood Inundation Depth (JRC)")

    assert [spec.slug for spec in specs] == ["jrc_flood_depth_index_rp100"]
    assert specs[0].weight == 1.0


def test_bundle_metric_specs_use_custom_flood_weights() -> None:
    specs = _bundle_metric_specs("Flood & Extreme Rainfall Risk")
    by_slug = {spec.slug: spec for spec in specs}

    assert len(specs) == 6
    assert by_slug["pr_max_1day_precip"].weight == 0.25 / 2.0
    assert by_slug["r20mm_very_heavy_precip_days"].weight == 0.25
    assert by_slug["cwd_consecutive_wet_days"].weight == 0.25


def test_bundle_metric_specs_use_custom_agriculture_weights() -> None:
    specs = _bundle_metric_specs("Agriculture & Growing Conditions")
    by_slug = {spec.slug: spec for spec in specs}

    assert len(specs) == 9
    assert by_slug["gsl_growing_season"].weight == 0.20
    assert by_slug["prcptot_annual_total"].weight == 0.20 / 2.0
    assert by_slug["dtr_daily_temp_range"].weight == 0.20


def test_bundle_metric_specs_default_to_equal_weights_without_custom_config() -> None:
    specs = _bundle_metric_specs("Rainfall Totals & Typical Wetness")

    assert specs
    assert all(spec.weight == 1.0 for spec in specs)


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


def test_apply_landing_search_selection_updates_focus_without_bundle_notice_dependency() -> None:
    session_state: dict[str, object] = {
        "landing_search_last_applied": None,
        "landing_search_reset_pending": False,
        "landing_focus_level": "india",
        landing_runtime.LANDING_MAP_CLICK_TOKEN_KEY: ("focus_district", "telangana", "hyderabad"),
        landing_runtime.LANDING_PENDING_MAP_TRANSITION_KEY: ("district", "telangana", "hyderabad"),
    }

    rerun_needed = landing_runtime._apply_landing_search_selection(
        session_state,
        search_selection="State: Telangana",
        search_options={"State: Telangana": ("state", "Telangana", None)},
    )

    assert rerun_needed is True
    assert session_state["landing_search_last_applied"] == "State: Telangana"
    assert session_state["landing_search_reset_pending"] is True
    assert session_state["landing_focus_level"] == "state"
    assert session_state["landing_selected_state"] == "Telangana"
    assert session_state["landing_selected_district"] is None
    assert landing_runtime.LANDING_MAP_CLICK_TOKEN_KEY not in session_state
    assert landing_runtime.LANDING_PENDING_MAP_TRANSITION_KEY not in session_state


def test_queue_and_consume_pending_landing_map_transition_for_state_focus() -> None:
    session_state: dict[str, object] = {}

    first = landing_runtime._queue_landing_map_transition(
        session_state,
        action="focus_state",
        state_name="Telangana",
        district_name=None,
    )
    consumed = landing_runtime._consume_pending_landing_map_transition(
        session_state,
        focus_level="state",
        selected_state="Telangana",
        selected_district=None,
    )

    assert first is True
    assert consumed is True
    assert session_state["landing_focus_level"] == "state"
    assert session_state["landing_selected_state"] == "Telangana"
    assert session_state["landing_selected_district"] is None
    assert landing_runtime.LANDING_PENDING_MAP_TRANSITION_KEY not in session_state


def test_queue_landing_map_transition_rejects_noop_without_mutating_session_state() -> None:
    session_state: dict[str, object] = {
        "landing_focus_level": "india",
        "landing_selected_state": None,
        "landing_selected_district": None,
    }

    queued = landing_runtime._queue_landing_map_transition(
        session_state,
        action="noop",
        state_name="Telangana",
        district_name=None,
    )

    assert queued is False
    assert session_state["landing_focus_level"] == "india"
    assert session_state["landing_selected_state"] is None
    assert session_state["landing_selected_district"] is None
    assert landing_runtime.LANDING_PENDING_MAP_TRANSITION_KEY not in session_state


def test_queue_landing_map_transition_preserves_valid_focus_actions() -> None:
    state_session: dict[str, object] = {}
    district_session: dict[str, object] = {}

    queued_state = landing_runtime._queue_landing_map_transition(
        state_session,
        action="focus_state",
        state_name="Telangana",
        district_name=None,
    )
    queued_district = landing_runtime._queue_landing_map_transition(
        district_session,
        action="focus_district",
        state_name="Telangana",
        district_name="Hyderabad",
    )

    assert queued_state is True
    assert state_session["landing_focus_level"] == "state"
    assert state_session["landing_selected_state"] == "Telangana"
    assert state_session["landing_selected_district"] is None
    assert state_session[landing_runtime.LANDING_PENDING_MAP_TRANSITION_KEY] == (
        "state",
        "telangana",
        "",
    )

    assert queued_district is True
    assert district_session["landing_focus_level"] == "district"
    assert district_session["landing_selected_state"] == "Telangana"
    assert district_session["landing_selected_district"] == "Hyderabad"
    assert district_session[landing_runtime.LANDING_PENDING_MAP_TRANSITION_KEY] == (
        "district",
        "telangana",
        "hyderabad",
    )


def test_identical_landing_click_is_allowed_again_after_pending_transition_is_consumed() -> None:
    session_state: dict[str, object] = {}

    first = landing_runtime._queue_landing_map_transition(
        session_state,
        action="focus_district",
        state_name="Telangana",
        district_name="Hyderabad",
    )
    consumed = landing_runtime._consume_pending_landing_map_transition(
        session_state,
        focus_level="district",
        selected_state="Telangana",
        selected_district="Hyderabad",
    )
    second = landing_runtime._queue_landing_map_transition(
        session_state,
        action="focus_state",
        state_name="Telangana",
        district_name=None,
    )

    assert first is True
    assert consumed is True
    assert second is True
    assert session_state["landing_focus_level"] == "state"
    assert session_state["landing_selected_state"] == "Telangana"
    assert session_state["landing_selected_district"] is None


def test_replayed_india_click_noop_does_not_recreate_pending_transition() -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()
    visible_districts = adm2[adm2["state_name"] == "Telangana"].copy()
    session_state: dict[str, object] = {}
    replayed_payload = {
        "last_object_clicked": {
            "properties": {"state_name": "Telangana", "__state_key": "telangana"}
        }
    }

    queued = landing_runtime._queue_landing_map_transition(
        session_state,
        action="focus_state",
        state_name="Telangana",
        district_name=None,
    )
    consumed = landing_runtime._consume_pending_landing_map_transition(
        session_state,
        focus_level="state",
        selected_state="Telangana",
        selected_district=None,
    )
    click_action, next_state, next_district = _apply_landing_map_click(
        focus_level="state",
        returned=replayed_payload,
        clicked_state="Telangana",
        clicked_district=None,
        selected_state="Telangana",
        selected_district=None,
        adm1=adm1,
        adm2=adm2,
        visible_districts=visible_districts,
    )
    replay_queued = landing_runtime._queue_landing_map_transition(
        session_state,
        action=click_action,
        state_name=next_state,
        district_name=next_district,
    )

    assert queued is True
    assert consumed is True
    assert click_action == "noop"
    assert next_state is None
    assert next_district is None
    assert replay_queued is False
    assert landing_runtime.LANDING_PENDING_MAP_TRANSITION_KEY not in session_state


def test_sync_landing_map_input_gate_disarms_until_context_reports_empty_payload() -> None:
    session_state: dict[str, object] = {}
    context = landing_runtime._landing_map_context_token(
        bundle_domain="Flood Inundation Depth (JRC)",
        scenario="snapshot",
        period="Current",
        focus_level="state",
        selected_state="Telangana",
        selected_district=None,
    )

    input_armed, context_changed = landing_runtime._sync_landing_map_input_gate(
        session_state,
        context_token=context,
        payload_is_empty=False,
    )

    assert input_armed is False
    assert context_changed is True
    assert session_state[landing_runtime.LANDING_MAP_CONTEXT_KEY] == context
    assert session_state[landing_runtime.LANDING_MAP_INPUT_ARMED_KEY] is False

    input_armed, context_changed = landing_runtime._sync_landing_map_input_gate(
        session_state,
        context_token=context,
        payload_is_empty=True,
    )

    assert input_armed is True
    assert context_changed is False
    assert session_state[landing_runtime.LANDING_MAP_INPUT_ARMED_KEY] is True


def test_sync_landing_map_input_gate_arms_immediately_for_empty_new_context() -> None:
    session_state: dict[str, object] = {}
    context = landing_runtime._landing_map_context_token(
        bundle_domain="Flood Inundation Depth (JRC)",
        scenario="snapshot",
        period="Current",
        focus_level="india",
        selected_state=None,
        selected_district=None,
    )

    input_armed, context_changed = landing_runtime._sync_landing_map_input_gate(
        session_state,
        context_token=context,
        payload_is_empty=True,
    )

    assert input_armed is True
    assert context_changed is True
    assert session_state[landing_runtime.LANDING_MAP_CONTEXT_KEY] == context
    assert session_state[landing_runtime.LANDING_MAP_INPUT_ARMED_KEY] is True


def test_clear_landing_pending_map_transition_clears_map_input_gate() -> None:
    session_state: dict[str, object] = {
        landing_runtime.LANDING_PENDING_MAP_TRANSITION_KEY: ("state", "telangana", ""),
        landing_runtime.LANDING_MAP_CONTEXT_KEY: ("flood", "snapshot", "Current", "state", "telangana", "", "district"),
        landing_runtime.LANDING_MAP_INPUT_ARMED_KEY: True,
        landing_runtime.LANDING_MAP_REPLAY_GUARD_KEY: ("legacy",),
        landing_runtime.LANDING_MAP_CLICK_TOKEN_KEY: ("focus_state", "telangana", ""),
    }

    landing_runtime._clear_landing_pending_map_transition(session_state)

    assert landing_runtime.LANDING_PENDING_MAP_TRANSITION_KEY not in session_state
    assert landing_runtime.LANDING_MAP_CONTEXT_KEY not in session_state
    assert landing_runtime.LANDING_MAP_INPUT_ARMED_KEY not in session_state
    assert landing_runtime.LANDING_MAP_REPLAY_GUARD_KEY not in session_state
    assert landing_runtime.LANDING_MAP_CLICK_TOKEN_KEY not in session_state


def test_render_landing_page_ignores_stale_payloads_until_first_empty_then_accepts_real_click(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()
    visible_districts = adm2[adm2["state_name"] == "Telangana"].copy()
    visible_districts["bundle_score"] = [72.0, 64.0]

    session_state: dict[str, object] = {
        "landing_bundle": "Flood Inundation Depth (JRC)",
        "landing_scenario": "snapshot",
        "landing_period": "Current",
        "landing_focus_level": "india",
        "landing_selected_state": None,
        "landing_selected_district": None,
    }
    stub_st = _DummyStreamlit(session_state)
    replayed_state_payload = {
        "last_object_clicked": {
            "properties": {
                "state_name": "Telangana",
                "shapeName": "Telangana",
                "__state_key": "telangana",
            }
        },
        "last_clicked": {"lat": 1.5, "lng": 1.0},
    }
    render_returns = iter(
        [
            ({}, None, None),
            (
                {
                    "last_object_clicked": {
                        "properties": {
                            "state_name": "Telangana",
                            "shapeName": "Telangana",
                            "__state_key": "telangana",
                        }
                    },
                    "last_clicked": {"lat": 1.5, "lng": 1.0},
                },
                None,
                "Telangana",
            ),
            (replayed_state_payload, "Telangana", "Telangana"),
            ({}, None, None),
            (
                {
                    "last_object_clicked": {
                        "properties": {
                            "district_name": "Nalgonda",
                            "state_name": "Telangana",
                            "__district_key": "telangana|nalgonda",
                        }
                    },
                    "last_clicked": {"lat": 1.5, "lng": 1.0},
                },
                "Nalgonda",
                "Telangana",
            ),
        ]
    )

    def fake_render_map_view(**kwargs):
        return next(render_returns)

    def fake_build_map_artifacts(**kwargs):
        focus_level = kwargs["focus_level"]
        return object(), None, "Map", visible_districts if focus_level in {"state", "district"} else adm1

    monkeypatch.setattr(landing_runtime, "st", stub_st)
    monkeypatch.setattr(landing_runtime, "_sanitize_landing_context", lambda *args, **kwargs: None)
    monkeypatch.setattr(landing_runtime, "_collect_bundle_metric_contexts", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        landing_runtime,
        "_intersect_bundle_scenario_period_pairs",
        lambda metric_contexts: [("snapshot", "Current")],
    )
    monkeypatch.setattr(
        landing_runtime,
        "_prepare_bundle_context",
        lambda *args, **kwargs: (
            pd.DataFrame(
                {
                    "state_name": ["Telangana", "Maharashtra"],
                    "__state_key": ["telangana", "maharashtra"],
                    "bundle_score": [75.0, 55.0],
                }
            ),
            pd.DataFrame(
                {
                    "state_name": ["Telangana", "Maharashtra"],
                    "__state_key": ["telangana", "maharashtra"],
                    "bundle_score": [75.0, 55.0],
                }
            ),
            [BundleMetricSpec(slug="jrc_flood_depth_index_rp100", label="Flood", column="jrc_flood_depth_index_rp100")],
        ),
    )
    monkeypatch.setattr(landing_runtime, "_build_landing_search_options", lambda *args, **kwargs: {})
    monkeypatch.setattr(landing_runtime, "_build_landing_map_artifacts", fake_build_map_artifacts)
    monkeypatch.setattr(landing_runtime, "render_map_view", fake_render_map_view)
    monkeypatch.setattr(landing_runtime, "_render_national_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(landing_runtime, "_render_state_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(landing_runtime, "_render_district_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(landing_runtime, "_render_landing_compare", lambda *args, **kwargs: None)
    monkeypatch.setattr(landing_runtime, "_render_landing_rankings", lambda *args, **kwargs: None)

    landing_runtime.render_landing_page(adm1=adm1, adm2=adm2, data_dir=Path("."))
    assert session_state["landing_focus_level"] == "india"
    assert session_state[landing_runtime.LANDING_MAP_INPUT_ARMED_KEY] is True

    with pytest.raises(_DummyRerun):
        landing_runtime.render_landing_page(adm1=adm1, adm2=adm2, data_dir=Path("."))

    assert session_state["landing_focus_level"] == "state"
    assert session_state["landing_selected_state"] == "Telangana"
    assert session_state["landing_selected_district"] is None

    landing_runtime.render_landing_page(adm1=adm1, adm2=adm2, data_dir=Path("."))
    assert session_state["landing_focus_level"] == "state"
    assert session_state["landing_selected_state"] == "Telangana"
    assert session_state["landing_selected_district"] is None
    assert session_state[landing_runtime.LANDING_MAP_INPUT_ARMED_KEY] is False

    landing_runtime.render_landing_page(adm1=adm1, adm2=adm2, data_dir=Path("."))
    assert session_state["landing_focus_level"] == "state"
    assert session_state["landing_selected_state"] == "Telangana"
    assert session_state["landing_selected_district"] is None
    assert session_state[landing_runtime.LANDING_MAP_INPUT_ARMED_KEY] is True

    with pytest.raises(_DummyRerun):
        landing_runtime.render_landing_page(adm1=adm1, adm2=adm2, data_dir=Path("."))

    assert session_state["landing_focus_level"] == "district"
    assert session_state["landing_selected_state"] == "Telangana"
    assert session_state["landing_selected_district"] == "Nalgonda"


def test_consume_pending_landing_map_transition_ignores_non_matching_focus() -> None:
    session_state: dict[str, object] = {
        landing_runtime.LANDING_PENDING_MAP_TRANSITION_KEY: ("state", "telangana", ""),
    }

    consumed = landing_runtime._consume_pending_landing_map_transition(
        session_state,
        focus_level="india",
        selected_state=None,
        selected_district=None,
    )

    assert consumed is False
    assert session_state[landing_runtime.LANDING_PENDING_MAP_TRANSITION_KEY] == ("state", "telangana", "")


def test_ensure_landing_state_clears_legacy_sticky_map_click_token() -> None:
    session_state: dict[str, object] = {
        landing_runtime.LANDING_MAP_CLICK_TOKEN_KEY: ("focus_state", "telangana", ""),
    }

    landing_runtime.ensure_landing_state(session_state)

    assert landing_runtime.LANDING_MAP_CLICK_TOKEN_KEY not in session_state


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


def test_build_deep_dive_handoff_for_flood_uses_biophysical_pillar_and_telangana_default() -> None:
    landing_state = {
        "landing_bundle": "Flood Inundation Depth (JRC)",
        "landing_scenario": "snapshot",
        "landing_period": "Current",
        "landing_focus_level": "india",
        "landing_selected_state": None,
        "landing_selected_district": None,
    }

    handoff = build_deep_dive_handoff(
        landing_state,
        bundle_domain="Flood Inundation Depth (JRC)",
        metric_slug="jrc_flood_depth_index_rp100",
    )

    assert handoff["selected_pillar"] == "Bio-physical Hazards"
    assert handoff["selected_bundle"] == "Flood Inundation Depth (JRC)"
    assert handoff["selected_var"] == "jrc_flood_depth_index_rp100"
    assert handoff["sel_scenario"] == "snapshot"
    assert handoff["sel_period"] == "Current"
    assert handoff["selected_state"] == "Telangana"
    assert handoff["selected_district"] == "All"


def test_build_deep_dive_handoff_requires_non_empty_metric_slug() -> None:
    with pytest.raises(ValueError, match="metric_slug"):
        build_deep_dive_handoff(
            {"landing_focus_level": "india"},
            bundle_domain="Heat Risk",
            metric_slug="",
        )


def test_build_glance_handoff_from_deep_dive_maps_compatible_district_context() -> None:
    detailed_state = {
        "spatial_family": "admin",
        "admin_level": "district",
        "selected_pillar": "Climate Hazards",
        "selected_bundle": "Heat Risk",
        "sel_scenario": "ssp585",
        "sel_period": "2040-2060",
        "selected_state": "Telangana",
        "selected_district": "Nalgonda",
    }

    handoff = build_glance_handoff_from_deep_dive(detailed_state)

    assert handoff["landing_active"] is True
    assert handoff["landing_bundle"] == "Heat Risk"
    assert handoff["landing_scenario"] == "ssp585"
    assert handoff["landing_period"] == "2040-2060"
    assert handoff["landing_context_pair"] == ("ssp585", "2040-2060")
    assert handoff["landing_focus_level"] == "district"
    assert handoff["landing_selected_state"] == "Telangana"
    assert handoff["landing_selected_district"] == "Nalgonda"
    assert handoff["landing_search_selection"] is None
    assert handoff["landing_search_last_applied"] is None
    assert handoff["landing_search_reset_pending"] is True


def test_build_glance_handoff_from_deep_dive_maps_compatible_state_context() -> None:
    detailed_state = {
        "spatial_family": "admin",
        "admin_level": "district",
        "selected_pillar": "Climate Hazards",
        "selected_bundle": "Heat Risk",
        "sel_scenario": "ssp585",
        "sel_period": "2040-2060",
        "selected_state": "Telangana",
        "selected_district": "All",
    }

    handoff = build_glance_handoff_from_deep_dive(detailed_state)

    assert handoff["landing_focus_level"] == "state"
    assert handoff["landing_selected_state"] == "Telangana"
    assert handoff["landing_selected_district"] is None


def test_build_glance_handoff_from_deep_dive_maps_compatible_india_context() -> None:
    detailed_state = {
        "spatial_family": "admin",
        "admin_level": "district",
        "selected_pillar": "Climate Hazards",
        "selected_bundle": "Heat Risk",
        "sel_scenario": "ssp585",
        "sel_period": "2040-2060",
        "selected_state": "All",
        "selected_district": "All",
    }

    handoff = build_glance_handoff_from_deep_dive(detailed_state)

    assert handoff["landing_focus_level"] == "india"
    assert handoff["landing_selected_state"] is None
    assert handoff["landing_selected_district"] is None


def test_build_glance_handoff_from_deep_dive_maps_compatible_flood_context() -> None:
    detailed_state = {
        "spatial_family": "admin",
        "admin_level": "district",
        "selected_pillar": "Bio-physical Hazards",
        "selected_bundle": "Flood Inundation Depth (JRC)",
        "sel_scenario": "snapshot",
        "sel_period": "Current",
        "selected_state": "Telangana",
        "selected_district": "All",
    }

    handoff = build_glance_handoff_from_deep_dive(detailed_state)

    assert handoff["landing_bundle"] == "Flood Inundation Depth (JRC)"
    assert handoff["landing_scenario"] == "snapshot"
    assert handoff["landing_period"] == "Current"
    assert handoff["landing_focus_level"] == "state"
    assert handoff["landing_selected_state"] == "Telangana"
    assert handoff["landing_selected_district"] is None


def test_build_glance_handoff_from_deep_dive_preserves_prior_landing_state_for_hydro() -> None:
    detailed_state = {
        "spatial_family": "hydro",
        "admin_level": "basin",
        "selected_pillar": "Climate Hazards",
        "selected_bundle": "Heat Risk",
        "sel_scenario": "ssp585",
        "sel_period": "2040-2060",
        "landing_bundle": "Drought Risk",
        "landing_scenario": "ssp245",
        "landing_period": "2020-2040",
        "landing_focus_level": "state",
        "landing_selected_state": "Maharashtra",
        "landing_selected_district": None,
    }

    handoff = build_glance_handoff_from_deep_dive(detailed_state)

    assert handoff == {
        "landing_active": True,
        "landing_search_selection": None,
        "landing_search_last_applied": None,
        "landing_search_reset_pending": True,
    }


def test_build_glance_handoff_from_deep_dive_preserves_landing_ui_state_when_incompatible() -> None:
    detailed_state = {
        "spatial_family": "admin",
        "admin_level": "district",
        "selected_pillar": "Exposure",
        "selected_bundle": "Population",
        "sel_scenario": "snapshot",
        "sel_period": "2025",
        "landing_tab": "Compare",
        "landing_compare_selection": ["Telangana"],
    }

    handoff = build_glance_handoff_from_deep_dive(detailed_state)

    assert "landing_tab" not in handoff
    assert "landing_compare_selection" not in handoff
    assert handoff["landing_active"] is True
    assert handoff["landing_search_reset_pending"] is True


def test_sanitize_landing_context_resets_unsupported_state_for_flood(monkeypatch, tmp_path: Path) -> None:
    session_state: dict[str, object] = {
        "landing_bundle": "Flood Inundation Depth (JRC)",
        "landing_scenario": "ssp585",
        "landing_period": "2040-2060",
        "landing_focus_level": "state",
        "landing_selected_state": "Maharashtra",
        "landing_selected_district": None,
    }
    monkeypatch.setattr(
        landing_runtime,
        "_bundle_scenario_period_options",
        lambda bundle_domain, *, data_dir: [("snapshot", "Current")],
    )

    _sanitize_landing_context(session_state, data_dir=tmp_path)

    assert session_state["landing_scenario"] == "snapshot"
    assert session_state["landing_period"] == "Current"
    assert session_state["landing_focus_level"] == "india"
    assert session_state["landing_selected_state"] is None
    assert session_state["landing_selected_district"] is None


def test_sanitize_landing_context_downgrades_invalid_telangana_district_for_flood(
    monkeypatch,
    tmp_path: Path,
) -> None:
    session_state: dict[str, object] = {
        "landing_bundle": "Flood Inundation Depth (JRC)",
        "landing_scenario": "snapshot",
        "landing_period": "Current",
        "landing_focus_level": "district",
        "landing_selected_state": "Telangana",
        "landing_selected_district": "Khammam",
    }
    monkeypatch.setattr(
        landing_runtime,
        "_bundle_scenario_period_options",
        lambda bundle_domain, *, data_dir: [("snapshot", "Current")],
    )
    monkeypatch.setattr(
        landing_runtime,
        "_prepare_bundle_context",
        lambda *args, **kwargs: (
            pd.DataFrame(
                {
                    "state_name": ["Telangana"],
                    "district_name": ["Hyderabad"],
                    "bundle_score": [75.0],
                }
            ),
            pd.DataFrame({"state_name": ["Telangana"], "bundle_score": [75.0]}),
            [BundleMetricSpec(slug="jrc_flood_depth_index_rp100", label="Flood", column="jrc_flood_depth_index_rp100")],
        ),
    )

    _sanitize_landing_context(session_state, data_dir=tmp_path)

    assert session_state["landing_focus_level"] == "state"
    assert session_state["landing_selected_state"] == "Telangana"
    assert session_state["landing_selected_district"] is None


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


def test_context_key_round_trip_preserves_metric_specs() -> None:
    contexts = [
        _metric_context(
            "metric_a",
            pairs=(("snapshot", "Current"),),
            weight=2.5,
            higher_is_worse=False,
            column="metric_a_col",
            label="Metric A",
            source_signature=(("source_a.csv", 1.0),),
            source_paths=("a.csv",),
        ),
        _metric_context(
            "metric_b",
            pairs=(("snapshot", "Current"), ("ssp585", "2040-2060")),
            weight=0.75,
            higher_is_worse=True,
            column="metric_b_col",
            label="Metric B",
            source_signature=(("source_b.csv", 2.0),),
            source_paths=("b.csv",),
        ),
    ]

    context_key = landing_runtime._bundle_context_cache_key(contexts)
    specs = landing_runtime._metric_specs_from_context_key(context_key)

    assert [(spec.slug, spec.column, spec.weight, spec.higher_is_worse) for spec in specs] == [
        ("metric_a", "metric_a_col", 2.5, False),
        ("metric_b", "metric_b_col", 0.75, True),
    ]


def test_prepare_bundle_context_cached_uses_available_pairs_not_source_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contexts = [
        _metric_context(
            "jrc_flood_depth_index_rp100",
            pairs=(("snapshot", "Current"),),
            source_signature=(("synthetic.csv", 1.0),),
            source_paths=("D:/processed/jrc_flood_depth_index_rp100.csv",),
        )
    ]
    context_key = landing_runtime._bundle_context_cache_key(contexts)

    def fake_loader(
        metric_slug: str,
        scenario: str,
        period: str,
        stat: str,
        source_signature: tuple[tuple[str, float | None], ...],
        source_paths: tuple[str, ...],
    ) -> pd.DataFrame:
        _ = (metric_slug, scenario, period, stat, source_signature, source_paths)
        return pd.DataFrame(
            {
                "state_name": ["Telangana", "Telangana"],
                "district_name": ["Adilabad", "Hyderabad"],
                "raw_metric_value": [5.0, 4.0],
            }
        )

    monkeypatch.setattr(landing_runtime, "_load_metric_district_values_cached", fake_loader)

    district_scores, state_scores, metric_specs = landing_runtime._prepare_bundle_context_cached(
        "Flood Inundation Depth (JRC)",
        "snapshot",
        "Current",
        "mean",
        context_key,
    )

    assert [spec.slug for spec in metric_specs] == ["jrc_flood_depth_index_rp100"]
    assert not district_scores.empty
    assert not state_scores.empty
    assert district_scores["bundle_score"].notna().all()
    assert state_scores["bundle_score"].notna().all()


def test_prepare_bundle_context_cached_preserves_weighted_scoring_round_trip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contexts = [
        _metric_context(
            "metric_a",
            pairs=(("snapshot", "Current"),),
            weight=2.0,
            higher_is_worse=True,
            source_signature=(("metric_a.csv", 1.0),),
            source_paths=("metric_a.csv",),
        ),
        _metric_context(
            "metric_b",
            pairs=(("snapshot", "Current"),),
            weight=1.0,
            higher_is_worse=False,
            source_signature=(("metric_b.csv", 1.0),),
            source_paths=("metric_b.csv",),
        ),
    ]
    context_key = landing_runtime._bundle_context_cache_key(contexts)

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
            return pd.DataFrame(
                {
                    "state_name": ["Telangana", "Telangana"],
                    "district_name": ["Adilabad", "Hyderabad"],
                    "raw_metric_value": [10.0, 20.0],
                }
            )
        return pd.DataFrame(
            {
                "state_name": ["Telangana", "Telangana"],
                "district_name": ["Adilabad", "Hyderabad"],
                "raw_metric_value": [1.0, 5.0],
            }
        )

    monkeypatch.setattr(landing_runtime, "_load_metric_district_values_cached", fake_loader)

    district_scores, state_scores, metric_specs = landing_runtime._prepare_bundle_context_cached(
        "Synthetic Weighted Bundle",
        "snapshot",
        "Current",
        "mean",
        context_key,
    )

    assert [(spec.slug, spec.weight, spec.higher_is_worse) for spec in metric_specs] == [
        ("metric_a", 2.0, True),
        ("metric_b", 1.0, False),
    ]
    district_by_name = district_scores.set_index("district_name")
    assert district_by_name.loc["Adilabad", "bundle_score"] == pytest.approx(100.0 / 3.0)
    assert district_by_name.loc["Hyderabad", "bundle_score"] == pytest.approx(200.0 / 3.0)
    assert state_scores.set_index("state_name").loc["Telangana", "bundle_score"] == pytest.approx(50.0)


def test_prepare_bundle_context_cached_returns_empty_for_unsupported_pair() -> None:
    contexts = [
        _metric_context(
            "metric_a",
            pairs=(("ssp585", "2040-2060"),),
            source_signature=(("metric_a.csv", 1.0),),
            source_paths=("metric_a.csv",),
        )
    ]
    context_key = landing_runtime._bundle_context_cache_key(contexts)

    district_scores, state_scores, metric_specs = landing_runtime._prepare_bundle_context_cached(
        "Unsupported Pair Bundle",
        "snapshot",
        "Current",
        "mean",
        context_key,
    )

    assert [spec.slug for spec in metric_specs] == ["metric_a"]
    assert district_scores.empty
    assert state_scores.empty


@pytest.mark.parametrize("bad_entry", [("a", "b", "c", 1.0, True, (), ()), ("a", "b", "c", 1.0, True, (), (), (), ())])
def test_decode_context_key_entry_rejects_malformed_schema(bad_entry: tuple[object, ...]) -> None:
    with pytest.raises(ValueError, match="expected 8 fields"):
        landing_runtime._decode_context_key_entry(bad_entry)


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


def test_build_district_map_frame_sorts_deterministically_for_feature_serialization() -> None:
    adm2 = _adm2_gdf().iloc[[1, 0, 2]].reset_index(drop=True)
    district_scores = pd.DataFrame(
        {
            "__district_key": ["telangana|nalgonda", "telangana|khammam"],
            "state_name": ["Telangana", "Telangana"],
            "district_name": ["Nalgonda", "Khammam"],
            "bundle_score": [72.0, 64.0],
        }
    )

    frame = landing_runtime._build_district_map_frame(
        adm2,
        district_scores,
        selected_state="Telangana",
    )
    feature_collection = landing_runtime._selection_to_feature_collection(
        frame,
        property_columns=("__district_key", "district_name"),
    )

    assert list(frame["__district_key"]) == ["telangana|khammam", "telangana|nalgonda"]
    assert [
        feature["properties"]["__district_key"]
        for feature in feature_collection["features"]
    ] == ["telangana|khammam", "telangana|nalgonda"]


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
    visible_districts["bundle_score"] = [72.0, 64.0]

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
    visible_districts["bundle_score"] = [72.0, 64.0]

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
    visible_districts["bundle_score"] = [72.0, 64.0]

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
    visible_districts["bundle_score"] = [72.0, 64.0]

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


def test_apply_landing_map_click_noops_on_replayed_state_payload_before_coords_fallback() -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()
    visible_districts = adm2[adm2["state_name"] == "Telangana"].copy()
    visible_districts["bundle_score"] = [72.0, 64.0]

    action = _apply_landing_map_click(
        focus_level="state",
        returned={
            "last_object_clicked": {
                "properties": {
                    "state_name": "Telangana",
                    "shapeName": "Telangana",
                    "__state_key": "telangana",
                }
            },
            "last_clicked": {"lat": 1.5, "lng": 1.0},
        },
        clicked_state="Telangana",
        clicked_district="Telangana",
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
    visible_districts["bundle_score"] = [72.0, 64.0]

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


def test_apply_landing_map_click_noops_when_replayed_coordinates_resolve_to_no_score_district() -> None:
    adm1 = _adm1_gdf()
    adm2 = _adm2_gdf()
    visible_districts = adm2[adm2["state_name"] == "Telangana"].copy()
    visible_districts["bundle_score"] = [float("nan"), 81.0]

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

    assert action == ("noop", None, None)

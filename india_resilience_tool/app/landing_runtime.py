"""
Landing / discovery surface runtime for the India Resilience Tool (IRT).

This module implements the climate-hazard-first landing experience:
India -> State -> District -> Deep Dive.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Optional, Sequence

import numpy as np
import pandas as pd
import streamlit as st

from india_resilience_tool.app.dashboard_bundle_runtime import dashboard_bundle_display
from india_resilience_tool.app.glance_exports import (
    build_glance_answer_pack_xlsx,
    build_glance_answer_text,
    build_glance_csv_bytes,
    build_glance_export_frame,
    glance_export_filename,
)
from india_resilience_tool.config.bundle_weights import get_bundle_weights
from india_resilience_tool.config.dashboard_bundles import (
    dashboard_bundle_names,
    get_dashboard_bundle_spec,
)
from india_resilience_tool.app.views.map_view import (
    build_choropleth_map_with_geojson_layer,
    extract_click_coordinates,
    find_block_at_coordinates,
    find_district_at_coordinates,
    find_state_at_coordinates,
    render_map_view,
)
from india_resilience_tool.config.constants import MAX_LAT, MAX_LON, MIN_LAT, MIN_LON
from india_resilience_tool.config.variables import (
    VARIABLES,
    get_pillar_for_domain,
)
from india_resilience_tool.data.optimized_bundle import (
    optimized_glance_root,
)
from india_resilience_tool.utils.naming import alias, normalize_name
from india_resilience_tool.viz.charts import (
    SCENARIO_DISPLAY,
    canonical_period_label,
    ordered_period_keys,
    ordered_scenario_keys,
    period_display_label,
)
from india_resilience_tool.viz.colors import (
    apply_fillcolor_binned,
    build_vertical_binned_legend_block_html,
)

LANDING_DEFAULT_BUNDLE = "Heat Risk"
LANDING_DEFAULT_SCENARIO = "ssp585"
LANDING_DEFAULT_PERIOD = "2040-2060"
LANDING_DEFAULT_TAB = "Rankings"
LANDING_SCORE_STAT = "mean"
LANDING_SEARCH_PLACEHOLDER = "Search geography..."
LANDING_COMPARE_KEY = "landing_compare_selection"
LANDING_SCENARIO_PAIR_KEY = "landing_context_pair"
LANDING_MAP_CLICK_TOKEN_KEY = "landing_last_map_click_token"
LANDING_PENDING_MAP_TRANSITION_KEY = "landing_pending_map_transition"
LANDING_MAP_REPLAY_GUARD_KEY = "landing_map_replay_guard"
LANDING_MAP_CONTEXT_KEY = "landing_map_context"
LANDING_MAP_INPUT_ARMED_KEY = "landing_map_input_armed"
LANDING_TABS = ("Rankings", "Compare")
LANDING_BAND_FILTER_KEY = "landing_band_filter"
LANDING_BAND_DISPLAY_ORDER = ("Very High", "High", "Moderate", "Low")

LANDING_PERIOD_SHORT_LABELS: dict[str, str] = {
    "Current": "Current",
    "2020-2040": "Early century",
    "2040-2060": "Mid-century",
    "2060-2080": "End century",
}


@dataclass(frozen=True)
class LandingDriverContext:
    """Best-effort component-metric context used only for Glance driver display."""

    district_scores: pd.DataFrame
    metric_specs: list[object]
    available: bool
    reason: Optional[str] = None


@dataclass(frozen=True)
class GlancePairContext:
    """Persisted Glance view-model tables for one bundle/scenario/period."""

    district: pd.DataFrame
    state: pd.DataFrame
    drivers: pd.DataFrame
    attributes: pd.DataFrame
    distributions: pd.DataFrame
    block: Optional[pd.DataFrame] = None


def _clear_landing_map_click_token(session_state: MutableMapping[str, object]) -> None:
    """Drop any stored landing map-click debounce token."""
    session_state.pop(LANDING_MAP_CLICK_TOKEN_KEY, None)


def _clear_landing_map_replay_guard(session_state: MutableMapping[str, object]) -> None:
    """Drop the short-lived landing replay fingerprint guard."""
    session_state.pop(LANDING_MAP_REPLAY_GUARD_KEY, None)


def _clear_landing_pending_transition_token(session_state: MutableMapping[str, object]) -> None:
    """Drop only the pending landing transition token."""
    session_state.pop(LANDING_PENDING_MAP_TRANSITION_KEY, None)


def _clear_landing_map_input_gate(session_state: MutableMapping[str, object]) -> None:
    """Drop the current landing map-context settle gate state."""
    session_state.pop(LANDING_MAP_CONTEXT_KEY, None)
    session_state.pop(LANDING_MAP_INPUT_ARMED_KEY, None)


def _clear_landing_pending_map_transition(session_state: MutableMapping[str, object]) -> None:
    """Drop pending landing map transitions and all short-lived map interaction state."""
    _clear_landing_pending_transition_token(session_state)
    _clear_landing_map_replay_guard(session_state)
    _clear_landing_map_click_token(session_state)
    _clear_landing_map_input_gate(session_state)


def _landing_defaults() -> dict[str, object]:
    """Return the landing-mode session defaults."""
    return {
        "landing_active": True,
        "landing_bundle": LANDING_DEFAULT_BUNDLE,
        "landing_scenario": LANDING_DEFAULT_SCENARIO,
        "landing_period": LANDING_DEFAULT_PERIOD,
        "landing_focus_level": "india",
        "landing_selected_state": None,
        "landing_selected_district": None,
        "landing_selected_block": None,
        "landing_tab": LANDING_DEFAULT_TAB,
        "landing_search_selection": None,
        "landing_search_last_applied": None,
        "landing_search_reset_pending": False,
        LANDING_MAP_CONTEXT_KEY: None,
        LANDING_MAP_INPUT_ARMED_KEY: False,
    }


def sync_landing_widget_state(session_state: MutableMapping[str, object]) -> None:
    """Synchronize widget-backed landing keys into the canonical landing state."""
    pair = session_state.get(LANDING_SCENARIO_PAIR_KEY)
    if isinstance(pair, (tuple, list)) and len(pair) == 2:
        session_state["landing_scenario"] = str(pair[0]).strip()
        session_state["landing_period"] = canonical_period_label(str(pair[1]).strip())


def ensure_landing_state(session_state: MutableMapping[str, object]) -> None:
    """Ensure landing-specific session keys exist without clobbering user state."""
    # Clear legacy replay/debounce state if it lingers from earlier buggy sessions.
    _clear_landing_map_click_token(session_state)
    _clear_landing_map_replay_guard(session_state)
    for key, value in _landing_defaults().items():
        if key not in session_state:
            session_state[key] = value


def set_landing_focus_india(session_state: MutableMapping[str, object]) -> None:
    """Reset the landing geography to the India overview."""
    session_state["landing_focus_level"] = "india"
    session_state["landing_selected_state"] = None
    session_state["landing_selected_district"] = None
    session_state["landing_selected_block"] = None


def set_landing_focus_state(
    session_state: MutableMapping[str, object],
    state_name: str,
) -> None:
    """Move the landing view into state focus."""
    session_state["landing_focus_level"] = "state"
    session_state["landing_selected_state"] = str(state_name).strip() or None
    session_state["landing_selected_district"] = None
    session_state["landing_selected_block"] = None


def set_landing_focus_district(
    session_state: MutableMapping[str, object],
    state_name: str,
    district_name: str,
) -> None:
    """Move the landing view into district focus while preserving state context."""
    session_state["landing_focus_level"] = "district"
    session_state["landing_selected_state"] = str(state_name).strip() or None
    session_state["landing_selected_district"] = str(district_name).strip() or None
    session_state["landing_selected_block"] = None


def set_landing_focus_block(
    session_state: MutableMapping[str, object],
    state_name: str,
    district_name: str,
    block_name: Optional[str] = None,
) -> None:
    """Move the landing view into block focus while preserving district context."""
    session_state["landing_focus_level"] = "block"
    session_state["landing_selected_state"] = str(state_name).strip() or None
    session_state["landing_selected_district"] = str(district_name).strip() or None
    session_state["landing_selected_block"] = str(block_name or "").strip() or None


def apply_landing_back(session_state: MutableMapping[str, object]) -> None:
    """Reverse the landing drill-down hierarchy by one step."""
    focus_level = str(session_state.get("landing_focus_level", "india")).strip().lower()
    if focus_level == "block":
        session_state["landing_focus_level"] = "district"
        session_state["landing_selected_block"] = None
        return
    if focus_level == "district":
        session_state["landing_focus_level"] = "state"
        session_state["landing_selected_district"] = None
        session_state["landing_selected_block"] = None
        return
    set_landing_focus_india(session_state)


def _apply_landing_search_selection(
    session_state: MutableMapping[str, object],
    *,
    search_selection: Optional[str],
    search_options: Mapping[str, tuple[str, Optional[str], Optional[str]]],
) -> bool:
    """Apply a landing search selection once and report whether a rerun is needed."""
    selection = str(search_selection or "").strip()
    if not selection or selection == str(session_state.get("landing_search_last_applied") or "").strip():
        return False

    resolved = search_options.get(selection)
    if resolved is None:
        return False

    search_kind, state_name, district_name = resolved
    session_state["landing_search_last_applied"] = selection
    session_state["landing_search_reset_pending"] = True
    _clear_landing_pending_map_transition(session_state)

    if search_kind == "state" and state_name:
        set_landing_focus_state(session_state, state_name)
        return True
    if search_kind == "district" and state_name and district_name:
        set_landing_focus_district(session_state, state_name, district_name)
        return True
    return False


def _landing_pending_map_transition(
    *,
    focus_level: str,
    state_name: Optional[str],
    district_name: Optional[str],
    block_name: Optional[str] = None,
) -> Optional[tuple[str, str, str, str]]:
    """Return a stable token for one landing focus transition target."""
    focus_value = str(focus_level or "").strip().lower()
    if focus_value not in {"state", "district", "block"}:
        return None
    return (
        focus_value,
        alias(str(state_name or "").strip()),
        alias(str(district_name or "").strip()),
        alias(str(block_name or "").strip()),
    )


def _queue_landing_map_transition(
    session_state: MutableMapping[str, object],
    *,
    action: str,
    state_name: Optional[str],
    district_name: Optional[str],
    block_name: Optional[str] = None,
) -> bool:
    """Apply one landing map click and mark the resulting rerun as pending."""
    action_value = str(action or "").strip().lower()
    if action_value not in {"focus_state", "focus_district", "focus_block"}:
        return False

    if action_value == "focus_state":
        focus_level = "state"
    elif action_value == "focus_district":
        focus_level = "district"
    else:
        focus_level = "block"
    if focus_level == "state" and not state_name:
        return False
    if focus_level == "district" and (not state_name or not district_name):
        return False
    if focus_level == "block" and (not state_name or not district_name or not block_name):
        return False

    token = _landing_pending_map_transition(
        focus_level=focus_level,
        state_name=state_name,
        district_name=district_name,
        block_name=block_name,
    )
    if token is None:
        return False

    session_state[LANDING_PENDING_MAP_TRANSITION_KEY] = token
    if focus_level == "state" and state_name:
        set_landing_focus_state(session_state, state_name)
        return True
    if focus_level == "district" and state_name and district_name:
        set_landing_focus_district(session_state, state_name, district_name)
        return True
    if focus_level == "block" and state_name and district_name and block_name:
        set_landing_focus_block(session_state, state_name, district_name, block_name)
        return True
    return False


def _consume_pending_landing_map_transition(
    session_state: MutableMapping[str, object],
    *,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str],
    selected_block: Optional[str] = None,
) -> bool:
    """Suppress one replayed map payload after a successful landing transition rerun."""
    pending = session_state.get(LANDING_PENDING_MAP_TRANSITION_KEY)
    if not isinstance(pending, (tuple, list)) or len(pending) not in {3, 4}:
        return False

    expected = _landing_pending_map_transition(
        focus_level=focus_level,
        state_name=selected_state,
        district_name=selected_district,
        block_name=selected_block,
    )
    pending_token = (
        str(pending[0]).strip().lower(),
        alias(str(pending[1]).strip()),
        alias(str(pending[2]).strip()),
        alias(str(pending[3]).strip()) if len(pending) == 4 else "",
    )
    if expected != pending_token:
        return False

    _clear_landing_pending_transition_token(session_state)
    return True


def build_deep_dive_handoff(
    landing_state: Mapping[str, object],
    *,
    bundle_domain: str,
    metric_slug: str,
) -> dict[str, object]:
    """
    Build the detailed-flow session-state handoff from the landing context.

    The landing flow always hands off into the admin deep-dive workflow for the
    selected landing bundle/domain.
    """
    metric_slug = str(metric_slug).strip()
    if not metric_slug:
        raise ValueError("metric_slug is required for Deep Dive handoff.")

    focus_level = str(landing_state.get("landing_focus_level", "india")).strip().lower()
    selected_state = str(landing_state.get("landing_selected_state") or "").strip()
    selected_district = str(landing_state.get("landing_selected_district") or "").strip()
    selected_block = str(landing_state.get("landing_selected_block") or "").strip()
    selected_pillar = get_pillar_for_domain(bundle_domain) or "Climate Hazards"
    is_block_handoff = focus_level == "block" and bool(selected_block)
    pending_state = selected_state if focus_level in {"state", "district", "block"} and selected_state else "All"
    pending_district = selected_district if focus_level in {"district", "block"} and selected_district else "All"
    return {
        "landing_active": False,
        "spatial_family": "admin",
        "admin_level": "block" if is_block_handoff else "district",
        "analysis_mode": "Single block focus" if is_block_handoff else "Single district focus",
        "active_view": "Map view",
        "main_view_selector": "Map view",
        "selected_pillar": selected_pillar,
        "selected_bundle": bundle_domain,
        "selected_var": metric_slug,
        "registry_metric": str(VARIABLES.get(metric_slug, {}).get("periods_metric_col") or metric_slug),
        "sel_scenario": str(landing_state.get("landing_scenario") or LANDING_DEFAULT_SCENARIO),
        "sel_period": canonical_period_label(str(landing_state.get("landing_period") or LANDING_DEFAULT_PERIOD)),
        "sel_stat": LANDING_SCORE_STAT,
        "map_mode": "Absolute value",
        "selected_state": pending_state,
        "selected_district": pending_district,
        "selected_block": selected_block if is_block_handoff else "All",
        "selected_basin": "All",
        "selected_subbasin": "All",
    }


def build_glance_handoff_from_deep_dive(
    detailed_state: Mapping[str, object],
) -> dict[str, object]:
    """
    Build the reverse handoff from deep dive back to the landing/glance view.

    Contract:
    - Always re-enable landing mode.
    - Reset landing search widget state so a stale selection is not reapplied.
    - If the current deep-dive context is compatible with landing, mirror that
      context back into landing session state.
    - Otherwise preserve the last known landing context already stored in
      session state.
    """
    updates: dict[str, object] = {
        "landing_active": True,
        "landing_search_selection": None,
        "landing_search_last_applied": None,
        "landing_search_reset_pending": True,
    }

    spatial_family = str(detailed_state.get("spatial_family") or "").strip().lower()
    admin_level = str(detailed_state.get("admin_level") or "").strip().lower()
    selected_pillar = str(detailed_state.get("selected_pillar") or "").strip()
    selected_bundle = str(detailed_state.get("selected_bundle") or "").strip()
    sel_scenario = str(detailed_state.get("sel_scenario") or "").strip()
    sel_period = str(detailed_state.get("sel_period") or "").strip()
    bundle_pillar = get_pillar_for_domain(selected_bundle)
    visible_bundles = set(dashboard_bundle_names(level="district", landing_only=True))

    if not (
        spatial_family == "admin"
        and admin_level in {"district", "block"}
        and selected_bundle in visible_bundles
        and bundle_pillar
        and selected_pillar == bundle_pillar
        and selected_bundle
        and sel_scenario
        and sel_period
    ):
        return updates

    selected_state = str(detailed_state.get("selected_state") or "").strip()
    selected_district = str(detailed_state.get("selected_district") or "").strip()
    selected_block = str(detailed_state.get("selected_block") or "").strip()
    landing_period = canonical_period_label(sel_period)

    updates.update(
        {
            "landing_bundle": selected_bundle,
            "landing_scenario": sel_scenario,
            "landing_period": landing_period,
            "landing_context_pair": (sel_scenario, landing_period),
        }
    )

    if selected_state == "All" or not selected_state:
        updates["landing_focus_level"] = "india"
        updates["landing_selected_state"] = None
        updates["landing_selected_district"] = None
        updates["landing_selected_block"] = None
        return updates

    if selected_district == "All" or not selected_district:
        updates["landing_focus_level"] = "state"
        updates["landing_selected_state"] = selected_state
        updates["landing_selected_district"] = None
        updates["landing_selected_block"] = None
        return updates

    if admin_level == "block" and selected_block and selected_block != "All":
        updates["landing_focus_level"] = "block"
        updates["landing_selected_state"] = selected_state
        updates["landing_selected_district"] = selected_district
        updates["landing_selected_block"] = selected_block
        return updates

    updates["landing_focus_level"] = "district"
    updates["landing_selected_state"] = selected_state
    updates["landing_selected_district"] = selected_district
    updates["landing_selected_block"] = None
    return updates


def _landing_bundle_domains(*, data_dir: Path) -> list[str]:
    """Return the supported landing bundles in a stable UX order."""
    return _available_glance_bundle_names(data_dir=data_dir)


def _glance_artifact_path(
    bundle_domain: str,
    artifact_name: str,
    *,
    scenario: str,
    period: str,
    data_dir: Path,
) -> Path:
    """Return one persisted Glance artifact path for a dashboard bundle selection."""
    spec = get_dashboard_bundle_spec(bundle_domain)
    slug = spec.composite_slug if spec is not None else str(bundle_domain).strip()
    return optimized_glance_root(
        slug,
        scenario=str(scenario).strip().lower(),
        period=canonical_period_label(str(period).strip()),
        data_dir=data_dir,
    ) / artifact_name


@st.cache_data(show_spinner=False)
def _load_glance_artifact_cached(
    path: str,
    mtime: Optional[float],
) -> pd.DataFrame:
    """Load one persisted Glance Parquet artifact."""
    _ = mtime
    artifact_path = Path(path)
    if not artifact_path.exists():
        return pd.DataFrame()
    return pd.read_parquet(artifact_path)


def _load_glance_artifact(
    bundle_domain: str,
    artifact_name: str,
    *,
    scenario: str,
    period: str,
    data_dir: Path,
) -> pd.DataFrame:
    path = _glance_artifact_path(
        bundle_domain,
        artifact_name,
        scenario=scenario,
        period=period,
        data_dir=data_dir,
    )
    return _load_glance_artifact_cached(str(path), path.stat().st_mtime if path.exists() else None)


def _load_glance_pair_context(
    bundle_domain: str,
    *,
    scenario: str,
    period: str,
    data_dir: Path,
) -> GlancePairContext:
    """Load all persisted Glance tables for one bundle/scenario/period."""
    block_path = _glance_artifact_path(
        bundle_domain,
        "block.parquet",
        scenario=scenario,
        period=period,
        data_dir=data_dir,
    )
    return GlancePairContext(
        district=_load_glance_artifact(bundle_domain, "district.parquet", scenario=scenario, period=period, data_dir=data_dir),
        state=_load_glance_artifact(bundle_domain, "state.parquet", scenario=scenario, period=period, data_dir=data_dir),
        drivers=_load_glance_artifact(bundle_domain, "drivers.parquet", scenario=scenario, period=period, data_dir=data_dir),
        attributes=_load_glance_artifact(bundle_domain, "attributes.parquet", scenario=scenario, period=period, data_dir=data_dir),
        distributions=_load_glance_artifact(
            bundle_domain,
            "distributions.parquet",
            scenario=scenario,
            period=period,
            data_dir=data_dir,
        ),
        block=(
            _load_glance_artifact_cached(str(block_path), block_path.stat().st_mtime)
            if block_path.exists()
            else None
        ),
    )


def _glance_scenario_period_options(
    bundle_domain: str,
    *,
    data_dir: Path,
) -> tuple[tuple[str, str], ...]:
    """Return scenario-period pairs with complete persisted Glance artifacts."""
    spec = get_dashboard_bundle_spec(bundle_domain)
    if spec is None:
        return ()
    root = optimized_glance_root(spec.composite_slug, data_dir=data_dir)
    if not root.exists():
        return ()
    pairs: set[tuple[str, str]] = set()
    required = {"district.parquet", "state.parquet", "drivers.parquet", "attributes.parquet", "distributions.parquet"}
    for scenario_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        for period_dir in sorted(path for path in scenario_dir.iterdir() if path.is_dir()):
            if all((period_dir / name).exists() for name in required):
                pairs.add((scenario_dir.name, canonical_period_label(period_dir.name)))
    return tuple(_ordered_scenario_period_pairs(pairs))


def _available_glance_bundle_names(*, data_dir: Path) -> list[str]:
    """Return landing bundle names gated by persisted Glance artifact presence."""
    out: list[str] = []
    for bundle_name in dashboard_bundle_names(level="district", landing_only=True):
        if _glance_scenario_period_options(bundle_name, data_dir=data_dir):
            out.append(bundle_name)
    return out


def _landing_bundle_display(bundle_domain: str) -> str:
    """Return the user-facing landing label for a bundle/domain."""
    return dashboard_bundle_display(bundle_domain)


def _landing_context_chip(scenario: str, period: str) -> str:
    """Return the visible scenario-period chip label used on landing."""
    scenario_label = SCENARIO_DISPLAY.get(str(scenario).strip().lower(), str(scenario).strip())
    period_key = canonical_period_label(period)
    period_label = LANDING_PERIOD_SHORT_LABELS.get(period_key, period_display_label(period_key))
    return f"{scenario_label} • {period_label}"


def _landing_map_label(
    *,
    bundle_domain: str,
    scenario: str,
    period: str,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str] = None,
) -> str:
    """Build the trust-critical map label for the current landing context."""
    focus = str(focus_level or "india").strip().lower()
    if focus == "india":
        level_label = "State-level"
    elif focus == "block":
        level_label = "Block-level"
    else:
        level_label = "District-level"
    bundle_label = _landing_bundle_display(bundle_domain)
    chip = _landing_context_chip(scenario, period)
    if focus == "india" or not selected_state:
        return f"{level_label} {bundle_label} Bundle Score • {chip}"
    if focus == "block" and selected_district:
        return f"{level_label} {bundle_label} Bundle Score • {selected_state} / {selected_district} • {chip}"
    return f"{level_label} {bundle_label} Bundle Score • {selected_state} • {chip}"


def _format_score(score: object) -> str:
    """Return a compact, user-facing score string."""
    try:
        value = float(score)
    except (TypeError, ValueError):
        return "Insufficient data"
    if not np.isfinite(value):
        return "Insufficient data"
    return f"{value:.1f}"


def _landing_driver_heading(bundle_domain: str) -> str:
    """Return the appropriate Glance driver heading for one bundle."""
    dashboard_spec = get_dashboard_bundle_spec(bundle_domain)
    if dashboard_spec is not None and dashboard_spec.group_key == "sector_wise":
        return "Top Rule Signals"
    return "Metric Drivers"


def _ordered_scenario_period_pairs(
    pairs: Sequence[tuple[str, str]] | set[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Return scenario-period pairs in the canonical dashboard order."""
    ordered: list[tuple[str, str]] = []
    by_scenario: dict[str, list[str]] = {}
    for scenario, period in pairs:
        by_scenario.setdefault(str(scenario), []).append(canonical_period_label(str(period)))

    for scenario in ordered_scenario_keys(list(by_scenario.keys())):
        for period in ordered_period_keys(by_scenario.get(scenario, [])):
            ordered.append((scenario, period))
    return ordered


def _bundle_scenario_period_options(
    bundle_domain: str,
    *,
    data_dir: Path,
) -> list[tuple[str, str]]:
    """Return available scenario-period pairs for one persisted landing composite."""
    return list(_glance_scenario_period_options(bundle_domain, data_dir=data_dir))


def _sanitize_landing_context(session_state: MutableMapping[str, object], *, data_dir: Path) -> None:
    """Ensure landing bundle and scenario-period choices remain valid."""
    bundle_domains = _landing_bundle_domains(data_dir=data_dir)
    if not bundle_domains:
        return

    current_bundle = str(session_state.get("landing_bundle") or "").strip()
    if current_bundle not in bundle_domains:
        session_state["landing_bundle"] = (
            LANDING_DEFAULT_BUNDLE if LANDING_DEFAULT_BUNDLE in bundle_domains else bundle_domains[0]
        )

    current_options = _bundle_scenario_period_options(
        str(session_state.get("landing_bundle") or LANDING_DEFAULT_BUNDLE),
        data_dir=data_dir,
    )
    current_pair = (
        str(session_state.get("landing_scenario") or LANDING_DEFAULT_SCENARIO).strip(),
        canonical_period_label(str(session_state.get("landing_period") or LANDING_DEFAULT_PERIOD).strip()),
    )
    if current_options and current_pair not in current_options:
        if (LANDING_DEFAULT_SCENARIO, LANDING_DEFAULT_PERIOD) in current_options:
            current_pair = (LANDING_DEFAULT_SCENARIO, LANDING_DEFAULT_PERIOD)
        else:
            current_pair = current_options[0]

    session_state["landing_scenario"] = current_pair[0]
    session_state["landing_period"] = current_pair[1]
    session_state[LANDING_SCENARIO_PAIR_KEY] = current_pair

    focus_level = str(session_state.get("landing_focus_level", "india")).strip().lower()
    selected_state = str(session_state.get("landing_selected_state") or "").strip()
    selected_district = str(session_state.get("landing_selected_district") or "").strip()
    if focus_level not in {"india", "state", "district", "block"}:
        set_landing_focus_india(session_state)
    elif focus_level == "india":
        session_state["landing_selected_state"] = None
        session_state["landing_selected_district"] = None
        session_state["landing_selected_block"] = None
    elif focus_level == "state" and not selected_state:
        set_landing_focus_india(session_state)
    elif focus_level == "state":
        session_state["landing_selected_district"] = None
        session_state["landing_selected_block"] = None
    elif focus_level == "district" and (not selected_state or not selected_district):
        if selected_state:
            set_landing_focus_state(session_state, selected_state)
        else:
            set_landing_focus_india(session_state)
    elif focus_level == "district":
        session_state["landing_selected_block"] = None
    elif focus_level == "block" and (not selected_state or not selected_district):
        if selected_state:
            set_landing_focus_state(session_state, selected_state)
        else:
            set_landing_focus_india(session_state)

def _prepare_bundle_context(
    bundle_domain: str,
    *,
    scenario: str,
    period: str,
    stat: str,
    data_dir: Path,
    metric_contexts: Optional[Sequence[object]] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load persisted Glance district/state score tables."""
    _ = metric_contexts
    def _empty_context() -> tuple[pd.DataFrame, pd.DataFrame]:
        empty = pd.DataFrame(
            columns=[
                "state_name",
                "district_name",
                "bundle_score",
                "bundle_score_display",
                "score_band",
                "district_rank",
                "district_count",
                "state_bundle_score",
                "state_rank",
                "state_count",
            ]
        )
        state_empty = pd.DataFrame(
            columns=[
                "state_name",
                "bundle_score",
                "__state_key",
                "score_band",
                "bundle_score_display",
                "state_rank",
                "state_count",
            ]
        )
        return empty, state_empty

    _ = stat
    context = _load_glance_pair_context(
        bundle_domain,
        scenario=scenario,
        period=period,
        data_dir=data_dir,
    )
    if context.district.empty or context.state.empty:
        return _empty_context()
    return context.district.copy(), context.state.copy()


def _prepare_driver_context(
    bundle_domain: str,
    *,
    scenario: str,
    period: str,
    stat: str,
    data_dir: Path,
) -> LandingDriverContext:
    """Load pre-ranked persisted Glance driver rows."""
    _ = stat
    drivers = _load_glance_artifact(
        bundle_domain,
        "drivers.parquet",
        scenario=scenario,
        period=period,
        data_dir=data_dir,
    )
    return LandingDriverContext(
        district_scores=drivers,
        metric_specs=[],
        available=not drivers.empty,
        reason=None if not drivers.empty else "empty_driver_artifact",
    )


def _build_landing_search_options(
    state_scores: pd.DataFrame,
    district_scores: pd.DataFrame,
) -> dict[str, tuple[str, str, Optional[str]]]:
    """Build searchable landing geography suggestions in a stable order."""
    options: dict[str, tuple[str, str, Optional[str]]] = {}

    if "state_name" in state_scores.columns:
        state_names = sorted(
            {
                str(value).strip()
                for value in state_scores["state_name"].dropna().tolist()
                if str(value).strip()
            },
            key=lambda value: (normalize_name(value), value),
        )
        for state_name in state_names:
            options[f"State: {state_name}"] = ("state", state_name, None)

    if {"district_name", "state_name"}.issubset(set(district_scores.columns)):
        district_pairs = (
            district_scores[["district_name", "state_name"]]
            .dropna()
            .drop_duplicates()
            .sort_values(["state_name", "district_name"], kind="stable")
        )
        for row in district_pairs.itertuples(index=False):
            district_name = str(row.district_name).strip()
            state_name = str(row.state_name).strip()
            if district_name and state_name:
                options[f"District: {district_name}, {state_name}"] = (
                    "district",
                    state_name,
                    district_name,
                )

    return options


def _selection_to_feature_collection(
    gdf: Any,
    *,
    property_columns: Sequence[str],
) -> dict[str, Any]:
    """Serialize a GeoDataFrame subset into a lightweight FeatureCollection."""
    features: list[dict[str, Any]] = []
    if gdf is None:
        return {"type": "FeatureCollection", "features": features}

    for _, row in gdf.iterrows():
        geometry = row.get("geometry")
        if geometry is None or getattr(geometry, "is_empty", False):
            continue

        props: dict[str, Any] = {}
        for column in property_columns:
            value = row.get(column)
            if pd.isna(value):
                value = None
            elif hasattr(value, "item"):
                value = value.item()
            props[column] = value

        features.append(
            {
                "type": "Feature",
                "properties": props,
                "geometry": geometry.__geo_interface__,
            }
        )

    return {"type": "FeatureCollection", "features": features}


def _sort_landing_map_frame(gdf: pd.DataFrame) -> pd.DataFrame:
    """Return a stably sorted landing map frame for deterministic FeatureCollection output."""
    if gdf is None or gdf.empty:
        return gdf

    sort_columns = [
        column
        for column in ("__state_key", "state_name", "__district_key", "district_name", "__block_key", "block_name", "shapeName")
        if column in gdf.columns
    ]
    if not sort_columns:
        return gdf
    return gdf.sort_values(sort_columns, kind="stable").reset_index(drop=True)


def _build_state_map_frame(adm1: Any, state_scores: pd.DataFrame) -> pd.DataFrame:
    """Merge state-level landing scores onto ADM1 geometry."""
    gdf = adm1.copy()
    if "state_name" not in gdf.columns and "shapeName" in gdf.columns:
        gdf["state_name"] = gdf["shapeName"].astype(str).str.strip()
    if "shapeName" not in gdf.columns and "state_name" in gdf.columns:
        gdf["shapeName"] = gdf["state_name"].astype(str).str.strip()

    gdf["__state_key"] = gdf["state_name"].astype(str).map(alias)
    merged = gdf.merge(
        state_scores,
        on="__state_key",
        how="left",
        suffixes=("", "_score"),
    )
    merged["state_name"] = merged["state_name"].fillna(merged["shapeName"])
    return _sort_landing_map_frame(merged)


def _build_district_map_frame(
    adm2: Any,
    district_scores: pd.DataFrame,
    *,
    selected_state: str,
) -> pd.DataFrame:
    """Merge district-level landing scores onto ADM2 geometry for one state."""
    gdf = adm2.copy()
    gdf["__state_key"] = gdf["state_name"].astype(str).map(alias)
    gdf["__district_key"] = (
        gdf["state_name"].astype(str).map(alias)
        + "|"
        + gdf["district_name"].astype(str).map(alias)
    )
    state_key = alias(selected_state)
    gdf = gdf[gdf["__state_key"] == state_key].copy()
    merged = gdf.merge(
        district_scores,
        on="__district_key",
        how="left",
        suffixes=("", "_score"),
    )
    merged["state_name"] = merged["state_name"].fillna(selected_state)
    return _sort_landing_map_frame(merged)


def _build_block_map_frame(
    adm3_by_district: dict[str, dict],
    block_scores: pd.DataFrame,
    *,
    selected_state: str,
    selected_district: str,
) -> pd.DataFrame:
    """Merge block-level landing scores onto ADM3 geometry for one district."""
    import geopandas as gpd

    district_sel_key = alias(selected_state) + "|" + alias(selected_district)
    fc = adm3_by_district.get(district_sel_key)
    if not fc or not fc.get("features"):
        return pd.DataFrame()

    gdf = gpd.GeoDataFrame.from_features(fc["features"])
    if gdf.empty:
        return pd.DataFrame()
    gdf["__state_key"] = gdf["state_name"].astype(str).map(alias)
    gdf["__district_key"] = gdf["__state_key"] + "|" + gdf["district_name"].astype(str).map(alias)
    gdf["__block_key"] = gdf["__district_key"] + "|" + gdf["block_name"].astype(str).map(alias)
    merged = gdf.merge(
        block_scores,
        on="__block_key",
        how="left",
        suffixes=("", "_score"),
    )
    merged["state_name"] = merged["state_name"].fillna(selected_state)
    merged["district_name"] = merged["district_name"].fillna(selected_district)
    return _sort_landing_map_frame(merged)


def _build_landing_map_artifacts(
    *,
    adm1: Any,
    adm2: Any,
    adm3_by_district: Optional[dict] = None,
    state_scores: pd.DataFrame,
    district_scores: pd.DataFrame,
    block_scores: Optional[pd.DataFrame] = None,
    bundle_domain: str,
    scenario: str,
    period: str,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str],
    selected_block: Optional[str] = None,
) -> tuple[Any, Optional[str], str, pd.DataFrame]:
    """Build the landing Folium map, legend, and map label."""
    import folium

    map_label = _landing_map_label(
        bundle_domain=bundle_domain,
        scenario=scenario,
        period=period,
        focus_level=focus_level,
        selected_state=selected_state,
        selected_district=selected_district,
    )

    if focus_level == "india":
        display_gdf = _build_state_map_frame(adm1, state_scores)
        tooltip = folium.features.GeoJsonTooltip(
            fields=["state_name", "bundle_score_display", "score_band"],
            aliases=["State", "Bundle score", "Risk band"],
            localize=True,
            sticky=True,
        )
        fc = _selection_to_feature_collection(
            display_gdf,
            property_columns=(
                "__state_key",
                "state_name",
                "shapeName",
                "bundle_score_display",
                "score_band",
                "fillColor",
            ),
        )
        selected_state_for_fit = "All"
        selected_district_for_fit = "All"
        reference_fc = None
        layer_name = "States"
        map_center = [22.0, 82.5]
        map_zoom = 4.8
        reference_level = None
        reference_layer_name = None
    elif focus_level == "block" and adm3_by_district and block_scores is not None:
        display_gdf = _build_block_map_frame(
            adm3_by_district,
            block_scores,
            selected_state=str(selected_state or ""),
            selected_district=str(selected_district or ""),
        )
        tooltip = folium.features.GeoJsonTooltip(
            fields=["block_name", "district_name", "bundle_score_display", "score_band"],
            aliases=["Block", "District", "Bundle score", "Risk band"],
            localize=True,
            sticky=True,
        )
        fc = _selection_to_feature_collection(
            display_gdf,
            property_columns=(
                "__state_key",
                "__district_key",
                "__block_key",
                "__bkey",
                "block_name",
                "district_name",
                "state_name",
                "bundle_score_display",
                "score_band",
                "fillColor",
            ),
        )
        selected_state_for_fit = str(selected_state or "All")
        selected_district_for_fit = str(selected_district or "All")
        layer_name = "Blocks"
        reference_fc = None
        reference_level = None
        reference_layer_name = None
        if not display_gdf.empty and "geometry" in display_gdf.columns:
            try:
                bounds = display_gdf.geometry.total_bounds
                map_center = [float((bounds[1] + bounds[3]) / 2), float((bounds[0] + bounds[2]) / 2)]
            except Exception:
                map_center = [22.0, 82.5]
        else:
            map_center = [22.0, 82.5]
        map_zoom = 9.0

        if selected_block and not display_gdf.empty:
            selected_row = display_gdf[
                display_gdf["block_name"].astype(str).str.strip().map(alias) == alias(selected_block)
            ]
            if not selected_row.empty:
                reference_fc = _selection_to_feature_collection(
                    selected_row,
                    property_columns=("block_name", "district_name", "state_name"),
                )
                reference_level = "block"
                reference_layer_name = "Selected block"
    else:
        display_gdf = _build_district_map_frame(
            adm2,
            district_scores,
            selected_state=str(selected_state or ""),
        )
        tooltip = folium.features.GeoJsonTooltip(
            fields=["district_name", "state_name", "bundle_score_display", "score_band"],
            aliases=["District", "State", "Bundle score", "Risk band"],
            localize=True,
            sticky=True,
        )
        fc = _selection_to_feature_collection(
            display_gdf,
            property_columns=(
                "__state_key",
                "__district_key",
                "district_name",
                "state_name",
                "bundle_score_display",
                "score_band",
                "fillColor",
            ),
        )
        selected_state_for_fit = str(selected_state or "All")
        selected_district_for_fit = "All"
        layer_name = "Districts"
        reference_fc = None
        reference_level = None
        reference_layer_name = None
        state_row = adm1[adm1["shapeName"].astype(str).str.strip().map(alias) == alias(selected_state or "")]
        if not state_row.empty:
            bounds = state_row.iloc[0].geometry.bounds
            map_center = [float((bounds[1] + bounds[3]) / 2), float((bounds[0] + bounds[2]) / 2)]
        else:
            map_center = [22.0, 82.5]
        map_zoom = 7.0

        if focus_level == "district" and selected_district:
            selected_row = display_gdf[
                display_gdf["district_name"].astype(str).str.strip().map(alias) == alias(selected_district)
            ]
            if not selected_row.empty:
                reference_fc = _selection_to_feature_collection(
                    selected_row,
                    property_columns=("district_name", "state_name"),
                )
                reference_level = "district"
                reference_layer_name = "Selected district"

    display_gdf = display_gdf.copy()
    display_gdf["bundle_score_numeric"] = pd.to_numeric(display_gdf.get("bundle_score"), errors="coerce")
    display_gdf = apply_fillcolor_binned(
        display_gdf,
        "bundle_score_numeric",
        0.0,
        100.0,
        cmap_name="YlOrRd",
        nlevels=15,
    )
    fc = _selection_to_feature_collection(
        display_gdf,
        property_columns=(
            "__state_key",
            "__district_key",
            "__block_key",
            "__bkey",
            "state_name",
            "shapeName",
            "district_name",
            "block_name",
            "bundle_score_display",
            "score_band",
            "fillColor",
        ),
    )

    finite_scores = pd.to_numeric(display_gdf.get("bundle_score_numeric"), errors="coerce").dropna()
    legend_html: Optional[str]
    if finite_scores.empty:
        legend_html = None
    else:
        legend_html = build_vertical_binned_legend_block_html(
            legend_title="Bundle score",
            vmin=0.0,
            vmax=100.0,
            cmap_name="YlOrRd",
            nlevels=15,
            nticks=5,
            include_zero_tick=True,
            map_height=520,
        )

    m = build_choropleth_map_with_geojson_layer(
        fc=fc,
        map_center=map_center,
        map_zoom=map_zoom,
        bounds_latlon=[[MIN_LAT, MIN_LON], [MAX_LAT, MAX_LON]],
        adm1=adm1,
        selected_state=selected_state_for_fit,
        selected_district=selected_district_for_fit,
        layer_name=layer_name,
        tooltip=tooltip,
        reference_fc=reference_fc,
        reference_level=reference_level if reference_fc is not None else None,
        reference_layer_name=reference_layer_name if reference_fc is not None else None,
    )
    return m, legend_html, map_label, display_gdf


def _state_exists(adm2: pd.DataFrame, state_name: Optional[str]) -> bool:
    """Return whether the given state exists in the current ADM2 table."""
    state_value = str(state_name or "").strip()
    if not state_value or "state_name" not in adm2.columns:
        return False
    return bool((adm2["state_name"].astype(str).map(alias) == alias(state_value)).any())


def _district_exists(districts: pd.DataFrame, state_name: Optional[str], district_name: Optional[str]) -> bool:
    """Return whether the given `(state, district)` pair exists in the given district table."""
    state_value = str(state_name or "").strip()
    district_value = str(district_name or "").strip()
    if not state_value or not district_value:
        return False
    required_columns = {"state_name", "district_name"}
    if not required_columns.issubset(set(districts.columns)):
        return False
    matches = districts[
        (districts["state_name"].astype(str).map(alias) == alias(state_value))
        & (districts["district_name"].astype(str).map(alias) == alias(district_value))
    ]
    return not matches.empty


def _district_row_has_landing_score(row: Optional[pd.Series]) -> bool:
    """Return whether a resolved district row has a usable landing bundle score."""
    if row is None:
        return False
    if "bundle_score" not in row.index:
        return True
    score = pd.to_numeric(pd.Series([row.get("bundle_score")]), errors="coerce").iloc[0]
    return bool(np.isfinite(score))


def _block_row_has_landing_score(row: Optional[pd.Series]) -> bool:
    """Return whether a resolved block row has a usable landing bundle score."""
    if row is None:
        return False
    if "bundle_score" not in row.index:
        return True
    score = pd.to_numeric(pd.Series([row.get("bundle_score")]), errors="coerce").iloc[0]
    return bool(np.isfinite(score))


def _landing_click_payloads(returned: Optional[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Collect candidate click payload property dictionaries from the raw map return payload."""
    if not returned:
        return []
    payloads: list[dict[str, Any]] = []
    for key in ("last_object_clicked", "clicked_feature", "last_active_drawing", "last_object"):
        feature = returned.get(key)
        if not isinstance(feature, dict):
            continue
        props = feature.get("properties") if isinstance(feature.get("properties"), dict) else feature
        if isinstance(props, dict):
            payloads.append(props)
    return payloads


def _landing_rendered_map_level(focus_level: str) -> str:
    """Return the rendered landing map level for the current focus."""
    focus = str(focus_level or "india").strip().lower()
    if focus == "india":
        return "state"
    if focus == "block":
        return "block"
    return "district"


def _landing_map_context_token(
    *,
    bundle_domain: str,
    scenario: str,
    period: str,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str],
    selected_block: Optional[str] = None,
) -> tuple[str, str, str, str, str, str, str, str]:
    """Return the canonical landing map context token for one rendered landing map."""
    return (
        alias(str(bundle_domain or "").strip()),
        str(scenario or "").strip().lower(),
        canonical_period_label(str(period or "").strip()),
        str(focus_level or "india").strip().lower(),
        alias(str(selected_state or "").strip()),
        alias(str(selected_district or "").strip()),
        alias(str(selected_block or "").strip()),
        _landing_rendered_map_level(focus_level),
    )


def _landing_map_payload_is_empty(returned: Optional[Mapping[str, Any]]) -> bool:
    """Return whether the raw landing map payload contains no actionable click state."""
    if _landing_click_payloads(returned):
        return False
    lat, lon = extract_click_coordinates(returned)
    return lat is None and lon is None


def _sync_landing_map_input_gate(
    session_state: MutableMapping[str, object],
    *,
    context_token: tuple[str, str, str, str, str, str, str, str],
    payload_is_empty: bool,
) -> tuple[bool, bool]:
    """
    Synchronize the landing map settle gate for the current rendered context.

    Returns:
        `(input_armed, context_changed)` for the current render pass.
    """
    stored_context = session_state.get(LANDING_MAP_CONTEXT_KEY)
    normalized_stored: Optional[tuple[str, str, str, str, str, str, str, str]]
    if isinstance(stored_context, tuple) and len(stored_context) == 8:
        normalized_stored = tuple(str(part) for part in stored_context)  # type: ignore[assignment]
    else:
        normalized_stored = None

    if normalized_stored != context_token:
        session_state[LANDING_MAP_CONTEXT_KEY] = context_token
        session_state[LANDING_MAP_INPUT_ARMED_KEY] = bool(payload_is_empty)
        return bool(payload_is_empty), True

    input_armed = bool(session_state.get(LANDING_MAP_INPUT_ARMED_KEY, False))
    if not input_armed and payload_is_empty:
        session_state[LANDING_MAP_INPUT_ARMED_KEY] = True
        return True, False
    return input_armed, False


def _canonical_state_name(adm1: pd.DataFrame, state_name: Optional[str] = None, state_key: Optional[str] = None) -> Optional[str]:
    """Return the canonical ADM1 display name for a state name or internal state key."""
    if adm1 is None or adm1.empty:
        return None
    state_frame = adm1.copy()
    if "state_name" not in state_frame.columns and "shapeName" in state_frame.columns:
        state_frame["state_name"] = state_frame["shapeName"].astype(str).str.strip()
    if "__state_key" not in state_frame.columns:
        state_frame["__state_key"] = state_frame["state_name"].astype(str).map(alias)

    if state_key:
        matches = state_frame[state_frame["__state_key"].astype(str) == str(state_key).strip()]
        if not matches.empty:
            row = matches.iloc[0]
            return str(row.get("shapeName") or row.get("state_name") or "").strip() or None

    state_value = str(state_name or "").strip()
    if state_value:
        matches = state_frame[state_frame["state_name"].astype(str).map(alias) == alias(state_value)]
        if matches.empty and "shapeName" in state_frame.columns:
            matches = state_frame[state_frame["shapeName"].astype(str).map(alias) == alias(state_value)]
        if not matches.empty:
            row = matches.iloc[0]
            return str(row.get("shapeName") or row.get("state_name") or "").strip() or None

    return None


def _resolve_district_row(
    districts: pd.DataFrame,
    *,
    district_key: Optional[str] = None,
    state_name: Optional[str] = None,
    district_name: Optional[str] = None,
) -> Optional[pd.Series]:
    """Return the canonical visible-district row for a stable key or `(state, district)` pair."""
    if districts is None or districts.empty:
        return None
    district_frame = districts.copy()
    if "__state_key" not in district_frame.columns:
        district_frame["__state_key"] = district_frame["state_name"].astype(str).map(alias)
    if "__district_key" not in district_frame.columns:
        district_frame["__district_key"] = (
            district_frame["state_name"].astype(str).map(alias)
            + "|"
            + district_frame["district_name"].astype(str).map(alias)
        )

    if district_key:
        matches = district_frame[district_frame["__district_key"].astype(str) == str(district_key).strip()]
        if not matches.empty:
            return matches.iloc[0]

    state_value = str(state_name or "").strip()
    district_value = str(district_name or "").strip()
    if state_value and district_value:
        matches = district_frame[
            (district_frame["state_name"].astype(str).map(alias) == alias(state_value))
            & (district_frame["district_name"].astype(str).map(alias) == alias(district_value))
        ]
        if not matches.empty:
            return matches.iloc[0]

    return None


def _resolve_block_row(
    blocks: pd.DataFrame,
    *,
    block_key: Optional[str] = None,
    state_name: Optional[str] = None,
    district_name: Optional[str] = None,
    block_name: Optional[str] = None,
) -> Optional[pd.Series]:
    """Return the canonical visible-block row for a stable key or `(state, district, block)`."""
    if blocks is None or blocks.empty:
        return None
    block_frame = blocks.copy()
    if "__state_key" not in block_frame.columns:
        block_frame["__state_key"] = block_frame["state_name"].astype(str).map(alias)
    if "__district_key" not in block_frame.columns:
        block_frame["__district_key"] = (
            block_frame["state_name"].astype(str).map(alias)
            + "|"
            + block_frame["district_name"].astype(str).map(alias)
        )
    if "__block_key" not in block_frame.columns:
        block_frame["__block_key"] = block_frame["__district_key"].astype(str) + "|" + block_frame["block_name"].astype(str).map(alias)

    if block_key:
        matches = block_frame[block_frame["__block_key"].astype(str) == str(block_key).strip()]
        if matches.empty and "__bkey" in block_frame.columns:
            matches = block_frame[block_frame["__bkey"].astype(str) == str(block_key).strip()]
        if not matches.empty:
            return matches.iloc[0]

    state_value = str(state_name or "").strip()
    district_value = str(district_name or "").strip()
    block_value = str(block_name or "").strip()
    if state_value and district_value and block_value:
        matches = block_frame[
            (block_frame["state_name"].astype(str).map(alias) == alias(state_value))
            & (block_frame["district_name"].astype(str).map(alias) == alias(district_value))
            & (block_frame["block_name"].astype(str).map(alias) == alias(block_value))
        ]
        if not matches.empty:
            return matches.iloc[0]

    return None


def _apply_landing_map_click(
    *,
    focus_level: str,
    returned: Optional[Mapping[str, Any]],
    clicked_state: Optional[str],
    clicked_district: Optional[str],
    clicked_block: Optional[str] = None,
    selected_state: Optional[str],
    selected_district: Optional[str],
    selected_block: Optional[str] = None,
    adm1: pd.DataFrame,
    adm2: pd.DataFrame,
    visible_districts: Optional[pd.DataFrame] = None,
    visible_blocks: Optional[pd.DataFrame] = None,
) -> tuple[str, Optional[str], Optional[str], Optional[str]]:
    """
    Resolve a landing map click into a geography navigation action.

    Returns:
        A tuple of `(action, state_name, district_name, block_name)` where
        `action` is one of `noop`, `focus_state`, `focus_district`, or
        `focus_block`.
    """
    focus = str(focus_level or "india").strip().lower()
    current_state = str(selected_state or "").strip() or None
    current_district = str(selected_district or "").strip() or None
    current_block = str(selected_block or "").strip() or None
    payloads = _landing_click_payloads(returned)

    if focus == "india":
        resolved_state: Optional[str] = None
        for props in payloads:
            state_key = props.get("__state_key")
            state_label = props.get("state_name") or props.get("shapeName") or props.get("name")
            resolved_state = _canonical_state_name(
                adm1,
                state_name=str(state_label).strip() if state_label else None,
                state_key=str(state_key).strip() if state_key else None,
            )
            if resolved_state:
                break

        if not resolved_state and clicked_state:
            resolved_state = _canonical_state_name(adm1, state_name=clicked_state)
        if not resolved_state:
            lat, lon = extract_click_coordinates(returned)
            if lat is not None and lon is not None:
                resolved_state = find_state_at_coordinates(adm1, lat, lon)
                resolved_state = _canonical_state_name(adm1, state_name=resolved_state)

        if resolved_state and _state_exists(adm2, resolved_state):
            return "focus_state", resolved_state, None, None
        return "noop", None, None, None

    if focus == "block":
        block_frame = visible_blocks if visible_blocks is not None else pd.DataFrame()
        resolved_block_row: Optional[pd.Series] = None
        had_payloads = bool(payloads)

        for props in payloads:
            block_key = props.get("__block_key") or props.get("__bkey")
            state_label = props.get("state_name") or props.get("shapeName_0") or props.get("state")
            district_label = props.get("district_name") or props.get("shapeName_1") or props.get("shapeName_2") or props.get("district")
            block_label = props.get("block_name") or props.get("subdistrict_name") or props.get("adm3_name") or props.get("name")
            resolved_block_row = _resolve_block_row(
                block_frame,
                block_key=str(block_key).strip() if block_key else None,
                state_name=str(state_label).strip() if state_label else (current_state or None),
                district_name=str(district_label).strip() if district_label else (current_district or None),
                block_name=str(block_label).strip() if block_label else None,
            )
            if resolved_block_row is not None:
                break

        if resolved_block_row is None and had_payloads:
            return "noop", None, None, None

        block_click_value = str(clicked_block or "").strip()
        if resolved_block_row is None and block_click_value:
            resolved_block_row = _resolve_block_row(
                block_frame,
                state_name=clicked_state or current_state,
                district_name=clicked_district or current_district,
                block_name=block_click_value,
            )

        if resolved_block_row is None:
            lat, lon = extract_click_coordinates(returned)
            if lat is not None and lon is not None:
                block_name, district_name, state_name = find_block_at_coordinates(block_frame, lat, lon)
                resolved_block_row = _resolve_block_row(
                    block_frame,
                    state_name=state_name or current_state,
                    district_name=district_name or current_district,
                    block_name=block_name,
                )

        if resolved_block_row is None or not _block_row_has_landing_score(resolved_block_row):
            return "noop", None, None, None
        resolved_state = str(resolved_block_row.get("state_name") or "").strip() or None
        district_name = str(resolved_block_row.get("district_name") or "").strip() or None
        block_name = str(resolved_block_row.get("block_name") or "").strip() or None
        if not resolved_state or not district_name or not block_name:
            return "noop", None, None, None
        if (
            current_state
            and current_district
            and current_block
            and alias(resolved_state) == alias(current_state)
            and alias(district_name) == alias(current_district)
            and alias(block_name) == alias(current_block)
        ):
            return "noop", None, None, None
        return "focus_block", resolved_state, district_name, block_name

    if focus not in {"state", "district"}:
        return "noop", None, None, None

    district_frame = visible_districts if visible_districts is not None else adm2
    resolved_row: Optional[pd.Series] = None
    had_payloads = bool(payloads)

    for props in payloads:
        district_key = props.get("__district_key")
        state_label = props.get("state_name") or props.get("shapeName_0") or props.get("state")
        district_label = props.get("district_name") or props.get("shapeName") or props.get("name")
        resolved_row = _resolve_district_row(
            district_frame,
            district_key=str(district_key).strip() if district_key else None,
            state_name=str(state_label).strip() if state_label else (current_state or None),
            district_name=str(district_label).strip() if district_label else None,
        )
        if resolved_row is not None:
            break

    # If the map returned a feature payload but we could not resolve it as a
    # district row in the current district map, treat it as stale/incompatible
    # rather than falling through to coordinate lookup. This prevents replayed
    # India-state payloads from being reinterpreted as fresh district clicks
    # after the map key changes on state drill-down.
    if resolved_row is None and had_payloads:
        return "noop", None, None, None

    if resolved_row is None and clicked_district:
        resolved_row = _resolve_district_row(
            district_frame,
            state_name=clicked_state or current_state,
            district_name=clicked_district,
        )

    if resolved_row is None:
        lat, lon = extract_click_coordinates(returned)
        if lat is not None and lon is not None:
            district_name, state_name = find_district_at_coordinates(district_frame, lat, lon)
            resolved_row = _resolve_district_row(
                district_frame,
                state_name=state_name or current_state,
                district_name=district_name,
            )

    if resolved_row is None:
        return "noop", None, None, None
    if not _district_row_has_landing_score(resolved_row):
        return "noop", None, None, None
    resolved_state = str(resolved_row.get("state_name") or "").strip() or None
    district_name = str(resolved_row.get("district_name") or "").strip() or None
    if not resolved_state or not district_name:
        return "noop", None, None, None
    if not _district_exists(district_frame, resolved_state, district_name):
        return "noop", None, None, None
    if (
        focus == "district"
        and current_state
        and current_district
        and alias(resolved_state) == alias(current_state)
        and alias(district_name) == alias(current_district)
    ):
        return "noop", None, None, None
    return "focus_district", resolved_state, district_name, None


def _render_driver_table(driver_df: pd.DataFrame, *, top_n: int = 5) -> None:
    """Render a compact driver table for the landing drawer."""
    if driver_df.empty:
        st.caption("No driver detail is available for this scope.")
        return

    if "driver_score" in driver_df.columns:
        driver_df = driver_df[pd.to_numeric(driver_df["driver_score"], errors="coerce").notna()].copy()
        if driver_df.empty:
            st.caption("No driver detail is available for this scope.")
            return

    sort_cols = [col for col in ("driver_rank", "driver_score") if col in driver_df.columns]
    if sort_cols:
        display_df = driver_df.sort_values(sort_cols, ascending=[True, False][: len(sort_cols)], kind="stable").head(top_n).copy()
    else:
        display_df = driver_df.head(top_n).copy()
    if "driver_score_display" not in display_df.columns:
        display_df["driver_score_display"] = pd.to_numeric(display_df.get("driver_score"), errors="coerce").map(_format_score)
    display_df = display_df.rename(
        columns={
            "driver_label": "Metric driver",
            "driver_score_display": "Normalized score",
        }
    )
    st.dataframe(
        display_df[["Metric driver", "Normalized score"]],
        hide_index=True,
        use_container_width=True,
    )


def _set_landing_band_filter(
    session_state: MutableMapping[str, object],
    *,
    band: str,
    scope: str,
    bundle: str,
    scenario: str,
    period: str,
    state_name: Optional[str] = None,
    district_name: Optional[str] = None,
) -> None:
    """Store a Glance band filter and route the user to the Rankings tab."""
    session_state[LANDING_BAND_FILTER_KEY] = {
        "band": str(band),
        "scope": str(scope),
        "bundle": str(bundle),
        "scenario": str(scenario),
        "period": str(period),
        "state_name": state_name,
        "district_name": district_name,
    }
    session_state["landing_tab"] = "Rankings"


def _get_landing_band_filter(session_state: Mapping[str, object]) -> Optional[Mapping[str, object]]:
    """Return the stored Glance band filter if present and well-formed, else None."""
    raw = session_state.get(LANDING_BAND_FILTER_KEY)
    if isinstance(raw, Mapping) and raw.get("band") and raw.get("scope"):
        return raw
    return None


def _clear_stale_landing_band_filter(
    session_state: MutableMapping[str, object],
    *,
    bundle: str,
    scenario: str,
    period: str,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str],
) -> None:
    """Drop the band filter when the active Glance context no longer matches it."""
    band_filter = _get_landing_band_filter(session_state)
    if band_filter is None:
        return
    scope = str(band_filter.get("scope"))
    if str(band_filter.get("bundle")) != bundle:
        session_state.pop(LANDING_BAND_FILTER_KEY, None)
        return
    if str(band_filter.get("scenario")) != scenario or str(band_filter.get("period")) != period:
        session_state.pop(LANDING_BAND_FILTER_KEY, None)
        return
    if scope == "national":
        valid_focus = focus_level == "india"
    elif scope == "state":
        valid_focus = focus_level in ("state", "district")
    elif scope == "block":
        valid_focus = focus_level == "block"
    else:
        valid_focus = False
    if not valid_focus:
        session_state.pop(LANDING_BAND_FILTER_KEY, None)
        return
    if scope in ("state", "block"):
        if alias(str(band_filter.get("state_name") or "")) != alias(str(selected_state or "")):
            session_state.pop(LANDING_BAND_FILTER_KEY, None)
            return
    if scope == "block":
        if alias(str(band_filter.get("district_name") or "")) != alias(str(selected_district or "")):
            session_state.pop(LANDING_BAND_FILTER_KEY, None)


def _apply_landing_band_filter(
    df: pd.DataFrame,
    band_filter: Optional[Mapping[str, object]],
    *,
    expected_scope: str,
    state_name: Optional[str] = None,
    district_name: Optional[str] = None,
) -> tuple[pd.DataFrame, Optional[str]]:
    """Filter df by the stored band when the stored scope matches expected_scope.

    Returns (filtered_df, applied_band). applied_band is None when no filter
    was applied (no filter, scope mismatch, name mismatch, or missing column).
    """
    if not band_filter:
        return df, None
    if str(band_filter.get("scope")) != expected_scope:
        return df, None
    if expected_scope in ("state", "block"):
        if alias(str(band_filter.get("state_name") or "")) != alias(str(state_name or "")):
            return df, None
    if expected_scope == "block":
        if alias(str(band_filter.get("district_name") or "")) != alias(str(district_name or "")):
            return df, None
    band = str(band_filter.get("band") or "").strip()
    if not band or "score_band" not in df.columns:
        return df, None
    filtered = df[df["score_band"].astype(str) == band].copy()
    return filtered, band


def _current_landing_glance_context() -> tuple[str, str, str]:
    """Read the active Glance bundle/scenario/period triple from session state."""
    bundle = str(st.session_state.get("landing_bundle") or LANDING_DEFAULT_BUNDLE).strip()
    scenario = str(st.session_state.get("landing_scenario") or LANDING_DEFAULT_SCENARIO).strip()
    period = canonical_period_label(
        str(st.session_state.get("landing_period") or LANDING_DEFAULT_PERIOD).strip()
    )
    return bundle, scenario, period


def _render_band_filter_buttons(
    dist_df: pd.DataFrame,
    *,
    scope: str,
    key_prefix: str,
    state_name: Optional[str] = None,
    district_name: Optional[str] = None,
) -> None:
    """Render one button per band present in dist_df, below a distribution chart."""
    if dist_df is None or dist_df.empty or "band" not in dist_df.columns:
        return
    present = set(dist_df["band"].astype(str).tolist())
    bands = [b for b in LANDING_BAND_DISPLAY_ORDER if b in present]
    if not bands:
        return
    bundle, scenario, period = _current_landing_glance_context()
    st.caption("Click a band to filter the Rankings table.")
    cols = st.columns(len(bands))
    state_token = alias(str(state_name or "NA"))
    district_token = alias(str(district_name or "NA"))
    for col, band in zip(cols, bands):
        button_key = f"{key_prefix}_{state_token}_{district_token}_{band}"
        if col.button(band, key=button_key, use_container_width=True):
            _set_landing_band_filter(
                st.session_state,
                band=band,
                scope=scope,
                bundle=bundle,
                scenario=scenario,
                period=period,
                state_name=state_name,
                district_name=district_name,
            )
            if scope == "block" and state_name and district_name:
                set_landing_focus_block(
                    st.session_state,
                    state_name=state_name,
                    district_name=district_name,
                    block_name=None,
                )
            st.rerun()


def _build_block_band_distribution(block_scope_df: pd.DataFrame) -> pd.DataFrame:
    """Return a per-band count frame for a single district's blocks, ordered VH→Low."""
    if block_scope_df is None or block_scope_df.empty or "score_band" not in block_scope_df.columns:
        return pd.DataFrame(columns=["band", "count"])
    counts = (
        block_scope_df["score_band"].astype(str).value_counts(dropna=False).to_dict()
    )
    rows = [
        {"band": band, "count": int(counts.get(band, 0))}
        for band in LANDING_BAND_DISPLAY_ORDER
        if int(counts.get(band, 0)) > 0
    ]
    return pd.DataFrame(rows, columns=["band", "count"])


def _render_national_summary(
    *,
    state_scores: pd.DataFrame,
    bundle_domain: str,
    distributions: Optional[pd.DataFrame] = None,
) -> None:
    """Render the compact national drawer for the India overview."""
    finite_scores = state_scores[pd.to_numeric(state_scores.get("bundle_score"), errors="coerce").notna()].copy()

    with st.container(border=True):
        st.markdown("#### Overview")
        st.caption(
            f"{_landing_bundle_display(bundle_domain)} is shown as a bundle-level hazard summary only. "
            "It does not include exposure, vulnerability, or resilience."
        )

        if finite_scores.empty:
            st.info("Insufficient data is available for this bundle and scenario-period.")
            return

        top_states = finite_scores.sort_values("bundle_score", ascending=False, kind="stable").head(3)
        st.markdown("**Top Hotspot States**")
        for index, row in enumerate(top_states.itertuples(index=False), start=1):
            st.write(f"{index}. {row.state_name}")

        st.markdown("**Score Distribution**")
        distributions = distributions if distributions is not None else pd.DataFrame()
        dist_df = distributions[distributions.get("scope_level", pd.Series(dtype=str)).astype(str) == "national"].copy()
        if not dist_df.empty:
            st.bar_chart(dist_df.rename(columns={"band": "Band", "count": "Count"}).set_index("Band")["Count"])
            _render_band_filter_buttons(
                dist_df,
                scope="national",
                key_prefix="landing_band_btn_national",
            )


def _render_state_summary(
    *,
    bundle_domain: str,
    state_name: str,
    district_scores: pd.DataFrame,
    state_scores: pd.DataFrame,
    driver_context: LandingDriverContext,
    distributions: Optional[pd.DataFrame] = None,
    deep_dive_disabled: bool = False,
) -> None:
    """Render the expanded drawer for state focus."""
    state_row = state_scores[state_scores["state_name"].astype(str).map(alias) == alias(state_name)]
    state_scope = district_scores[district_scores["state_name"].astype(str).map(alias) == alias(state_name)].copy()

    with st.container(border=True):
        st.markdown(f"#### {state_name} Summary")
        if state_row.empty:
            st.info("State-level landing data is not available for this selection.")
            return

        row = state_row.iloc[0]
        st.metric(
            label=f"{_landing_bundle_display(str(st.session_state.get('landing_bundle') or LANDING_DEFAULT_BUNDLE))} bundle score",
            value=_format_score(row.get("bundle_score")),
        )

        rank_value = row.get("state_rank")
        count_value = row.get("state_count")
        if pd.notna(rank_value) and pd.notna(count_value):
            st.caption(f"State rank: {int(rank_value)} / {int(count_value)} across India")
        st.caption(f"Risk band: {row.get('score_band') or 'Insufficient data'}")

        hotspot_df = state_scope.sort_values("bundle_score", ascending=False, kind="stable").head(5)
        st.markdown("**Top Hotspot Districts**")
        for index, hotspot_row in enumerate(hotspot_df.itertuples(index=False), start=1):
            hotspot_district_name = str(hotspot_row.district_name)
            button_label = f"{index}. {hotspot_district_name}"
            button_key = f"landing_hotspot_district_{state_name}_{index}_{hotspot_district_name}"
            if st.button(button_label, key=button_key, use_container_width=True):
                set_landing_focus_district(
                    st.session_state,
                    state_name=state_name,
                    district_name=hotspot_district_name,
                )
                st.rerun()

        st.markdown("**District Score Distribution**")
        distributions = distributions if distributions is not None else pd.DataFrame()
        dist_df = distributions[
            (distributions.get("scope_level", pd.Series(dtype=str)).astype(str) == "state")
            & (distributions.get("__state_key", pd.Series(dtype=str)).astype(str) == alias(state_name))
        ].copy()
        if not dist_df.empty:
            st.bar_chart(dist_df.rename(columns={"band": "Band", "count": "Count"}).set_index("Band")["Count"])
            _render_band_filter_buttons(
                dist_df,
                scope="state",
                key_prefix="landing_band_btn_state",
                state_name=state_name,
            )
        st.markdown(f"**{_landing_driver_heading(bundle_domain)}**")
        driver_scope = pd.DataFrame()
        if driver_context.available and not driver_context.district_scores.empty:
            driver_scope = driver_context.district_scores[
                (driver_context.district_scores["scope_level"].astype(str) == "state")
                & (driver_context.district_scores["__state_key"].astype(str) == alias(state_name))
            ].copy()
        _render_driver_table(driver_scope, top_n=5)

        if st.button(
            "Deep Dive",
            key="landing_deep_dive_state",
            use_container_width=True,
            disabled=deep_dive_disabled,
        ):
            _enter_deep_dive(st.session_state)


def _render_district_summary(
    *,
    bundle_domain: str,
    state_name: str,
    district_name: str,
    district_scores: pd.DataFrame,
    driver_context: LandingDriverContext,
    attributes: Optional[pd.DataFrame] = None,
    deep_dive_disabled: bool = False,
    block_scores_available: bool = False,
) -> None:
    """Render the district-focus drawer with peer and driver context."""
    state_scope = district_scores[district_scores["state_name"].astype(str).map(alias) == alias(state_name)].copy()
    district_row = state_scope[state_scope["district_name"].astype(str).map(alias) == alias(district_name)]

    with st.container(border=True):
        st.markdown(f"#### {district_name} Overview")
        if district_row.empty:
            st.info("District-level landing data is not available for this selection.")
            return

        row = district_row.iloc[0]

        # Primary card: use raw ordinal class + label when available.
        non_attr = [e for e in get_bundle_weights(bundle_domain) if not e.is_attribute]
        primary_slug = non_attr[0].metric_slug if len(non_attr) == 1 else None
        primary_class_labels: dict = (
            VARIABLES.get(primary_slug or "", {}).get("class_labels") or {}
        )
        raw_primary = None if not primary_slug else pd.to_numeric(
            pd.Series([row.get(primary_slug)]), errors="coerce"
        ).iloc[0]

        if primary_class_labels and raw_primary is not None and np.isfinite(raw_primary):
            cls = int(round(raw_primary))
            class_label = primary_class_labels.get(cls, str(cls))
            primary_label = str(VARIABLES.get(primary_slug, {}).get("label") or primary_slug)
            st.metric(label=primary_label, value=f"{cls} — {class_label}")
        else:
            st.metric(
                label=f"{_landing_bundle_display(str(st.session_state.get('landing_bundle') or LANDING_DEFAULT_BUNDLE))} bundle score",
                value=_format_score(row.get("bundle_score")),
            )

        st.caption(f"Risk band: {row.get('score_band') or 'Insufficient data'}")

        rank_value = row.get("district_rank")
        count_value = row.get("district_count")
        if pd.notna(rank_value) and pd.notna(count_value):
            st.caption(f"Rank within {state_name}: {int(rank_value)} / {int(count_value)}")

        state_mean = pd.to_numeric(pd.Series([row.get("state_mean_score")]), errors="coerce").iloc[0]
        delta_display = row.get("delta_vs_state_mean_display")
        if np.isfinite(state_mean) and delta_display:
            st.caption(
                f"Compared with the {state_name} average: {delta_display} points "
                f"(state average {state_mean:.1f})"
            )

        # Inline attribute captions (e.g. raw depth and extent for JRC flood).
        district_key = f"{alias(state_name)}|{alias(district_name)}"
        attributes = attributes if attributes is not None else pd.DataFrame()
        attr_scope = attributes[attributes.get("__district_key", pd.Series(dtype=str)).astype(str) == district_key].copy()
        if not attr_scope.empty and "sort_order" in attr_scope.columns:
            attr_scope = attr_scope.sort_values("sort_order", kind="stable")
        for attr_row in attr_scope.itertuples(index=False):
            label = str(getattr(attr_row, "attribute_label", "") or getattr(attr_row, "attribute_slug", ""))
            display = str(getattr(attr_row, "attribute_display", "") or "")
            if label and display:
                st.caption(f"{label}: {display}")

        st.markdown(f"**{_landing_driver_heading(bundle_domain)}**")
        driver_scope = pd.DataFrame()
        if driver_context.available and not driver_context.district_scores.empty:
            driver_scope = driver_context.district_scores[
                (driver_context.district_scores["scope_level"].astype(str) == "district")
                & (driver_context.district_scores["__district_key"].astype(str) == district_key)
            ].copy()
        if driver_scope.empty:
            st.caption("No driver detail is available for this district.")
        else:
            _render_driver_table(driver_scope, top_n=5)

        if st.button(
            "Deep Dive",
            key="landing_deep_dive_district",
            use_container_width=True,
            disabled=deep_dive_disabled,
        ):
            _enter_deep_dive(st.session_state)

        if block_scores_available:
            if st.button("View Blocks", key="landing_view_blocks_district", use_container_width=True):
                set_landing_focus_block(
                    st.session_state,
                    state_name=state_name,
                    district_name=district_name,
                    block_name=None,
                )
                st.rerun()


def _render_block_summary(
    *,
    bundle_domain: str,
    state_name: str,
    district_name: str,
    block_name: Optional[str],
    block_scores: pd.DataFrame,
    driver_context: LandingDriverContext,
    deep_dive_disabled: bool = False,
) -> None:
    """Render the block-focus drawer."""
    district_key = f"{alias(state_name)}|{alias(district_name)}"
    district_scope = block_scores[
        (block_scores["state_name"].astype(str).map(alias) == alias(state_name))
        & (block_scores["district_name"].astype(str).map(alias) == alias(district_name))
    ].copy()
    selected_block = str(block_name or "").strip()

    with st.container(border=True):
        if not selected_block:
            st.markdown(f"#### {district_name} Blocks")
            if district_scope.empty:
                st.info("Block-level landing data is not available for this district.")
                return
            count_value = district_scope["block_name"].dropna().astype(str).nunique()
            st.metric(label="Blocks with landing scores", value=str(count_value))
            dist_score_display = district_scope["district_bundle_score_display"].dropna().astype(str).head(1)
            if not dist_score_display.empty:
                st.caption(f"Parent district ({district_name}) score: {dist_score_display.iloc[0]}")
            hotspot_df = district_scope.sort_values("bundle_score", ascending=False, kind="stable").head(5)
            st.markdown("**Top Hotspot Blocks**")
            for index, hotspot_row in enumerate(hotspot_df.itertuples(index=False), start=1):
                hotspot_block_name = str(hotspot_row.block_name)
                button_label = f"{index}. {hotspot_block_name}"
                button_key = (
                    f"landing_hotspot_block_{state_name}_{district_name}_{index}_{hotspot_block_name}"
                )
                if st.button(button_label, key=button_key, use_container_width=True):
                    set_landing_focus_block(
                        st.session_state,
                        state_name=state_name,
                        district_name=district_name,
                        block_name=hotspot_block_name,
                    )
                    st.rerun()

            block_dist = _build_block_band_distribution(district_scope)
            if not block_dist.empty:
                st.markdown("**Block Score Distribution**")
                st.bar_chart(
                    block_dist.rename(columns={"band": "Band", "count": "Count"}).set_index("Band")["Count"]
                )
                _render_band_filter_buttons(
                    block_dist,
                    scope="block",
                    key_prefix="landing_band_btn_block",
                    state_name=state_name,
                    district_name=district_name,
                )
            return

        block_key = district_key + "|" + alias(selected_block)
        block_row = block_scores[block_scores["__block_key"].astype(str) == block_key]
        st.markdown(f"#### {selected_block} Block")
        if block_row.empty:
            st.info("Block-level landing data is not available.")
            return

        row = block_row.iloc[0]
        st.metric(
            label=f"{_landing_bundle_display(str(st.session_state.get('landing_bundle') or LANDING_DEFAULT_BUNDLE))} bundle score",
            value=_format_score(row.get("bundle_score")),
        )
        st.caption(f"Risk band: {row.get('score_band') or 'Insufficient data'}")

        rank_d = row.get("block_rank_within_district")
        count_d = row.get("block_count_within_district")
        if pd.notna(rank_d) and pd.notna(count_d):
            st.caption(f"Rank within {district_name}: {int(rank_d)} / {int(count_d)}")
        rank_s = row.get("block_rank_within_state")
        count_s = row.get("block_count_within_state")
        if pd.notna(rank_s) and pd.notna(count_s):
            st.caption(f"Rank within {state_name}: {int(rank_s)} / {int(count_s)}")
        dist_score_display = row.get("district_bundle_score_display")
        if dist_score_display:
            st.caption(f"Parent district ({district_name}) score: {dist_score_display}")

        driver_scope = pd.DataFrame()
        driver_heading = _landing_driver_heading(bundle_domain)
        if driver_context.available and not driver_context.district_scores.empty:
            drivers = driver_context.district_scores
            if "__block_key" in drivers.columns and "scope_level" in drivers.columns:
                driver_scope = drivers[
                    (drivers["scope_level"].astype(str) == "block")
                    & (drivers["__block_key"].astype(str) == block_key)
                ].copy()
            if driver_scope.empty and "scope_level" in drivers.columns:
                driver_scope = drivers[
                    (drivers["scope_level"].astype(str) == "district")
                    & (drivers["__district_key"].astype(str) == district_key)
                ].copy()
                if not driver_scope.empty:
                    driver_heading = f"Parent District {_landing_driver_heading(bundle_domain)}"
        st.markdown(f"**{driver_heading}**")
        _render_driver_table(driver_scope, top_n=5)

        if st.button(
            "Deep Dive",
            key="landing_deep_dive_block",
            use_container_width=True,
            disabled=deep_dive_disabled,
        ):
            _enter_deep_dive(st.session_state)


def _render_landing_rankings(
    *,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str],
    state_scores: pd.DataFrame,
    district_scores: pd.DataFrame,
    block_scores: Optional[pd.DataFrame] = None,
    selected_block: Optional[str] = None,
) -> pd.DataFrame:
    """Render context-sensitive landing rankings."""
    visible_rows = _compute_visible_ranking_rows(
        focus_level=focus_level,
        selected_state=selected_state,
        selected_district=selected_district,
        selected_block=selected_block,
        state_scores=state_scores,
        district_scores=district_scores,
        block_scores=block_scores,
        band_filter=_get_landing_band_filter(st.session_state),
    )
    applied_band = (
        str(visible_rows["active_band_filter"].dropna().head(1).iloc[0])
        if not visible_rows.empty and visible_rows["active_band_filter"].dropna().any()
        else None
    )
    unit_scope = (
        str(visible_rows["unit_scope"].dropna().head(1).iloc[0])
        if not visible_rows.empty and "unit_scope" in visible_rows.columns
        else ("state" if focus_level == "india" else ("block" if focus_level == "block" else "district"))
    )
    _render_band_filter_status(applied_band, len(visible_rows), level_noun=unit_scope)
    if visible_rows.empty:
        st.dataframe(pd.DataFrame(), hide_index=True, use_container_width=True)
        return visible_rows
    _render_selected_focus_summary(visible_rows)
    display_df = visible_rows.copy()
    display_df["Current focus"] = display_df["is_current_focus"].map(lambda value: "Selected" if bool(value) else "")
    if unit_scope == "state":
        display_df = display_df.rename(
            columns={
                "rank": "Rank",
                "unit_name": "State",
                "bundle_score_display": "Bundle score",
                "score_band": "Risk band",
            }
        )
        st.dataframe(
            display_df[["Rank", "State", "Bundle score", "Risk band"]],
            hide_index=True,
            use_container_width=True,
        )
        return visible_rows
    if unit_scope == "block":
        display_df = display_df.rename(
            columns={
                "rank": "Rank",
                "unit_name": "Block",
                "bundle_score_display": "Bundle score",
                "score_band": "Risk band",
            }
        )
        st.dataframe(
            display_df[["Rank", "Block", "Bundle score", "Risk band", "Current focus"]],
            hide_index=True,
            use_container_width=True,
        )
        return visible_rows
    display_df = display_df.rename(
        columns={
            "rank": "Rank",
            "unit_name": "District",
            "bundle_score_display": "Bundle score",
            "score_band": "Risk band",
        }
    )
    st.dataframe(
        display_df[["Rank", "District", "Bundle score", "Risk band", "Current focus"]],
        hide_index=True,
        use_container_width=True,
    )
    return visible_rows


def _ranking_scalar(value: object, *, fallback: str = "") -> str:
    if pd.isna(value):
        return fallback
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value).strip() or fallback
    return str(int(numeric)) if numeric.is_integer() else f"{numeric:g}"


def _ranking_text(value: object, *, fallback: str = "") -> str:
    if pd.isna(value):
        return fallback
    text = str(value).strip()
    return text or fallback


def _render_selected_focus_summary(visible_rows: pd.DataFrame) -> None:
    """Render a compact selected-row summary without changing table order."""
    if visible_rows.empty or "is_current_focus" not in visible_rows.columns:
        return
    focus_mask = visible_rows["is_current_focus"].fillna(False).astype(bool)
    focus_rows = visible_rows[focus_mask].sort_values("rank", kind="stable")
    if focus_rows.empty:
        return
    focus = focus_rows.iloc[0]
    unit_scope = _ranking_text(focus.get("unit_scope"), fallback="unit").lower()
    unit_label = {"district": "district", "block": "block", "state": "state"}.get(unit_scope, "unit")
    unit_name = _ranking_text(focus.get("unit_name"))
    rank = _ranking_scalar(focus.get("rank"), fallback="unranked")
    comparison_count = _ranking_scalar(focus.get("comparison_count"), fallback=str(len(visible_rows)))
    score = _ranking_text(focus.get("bundle_score_display")) or _ranking_text(focus.get("bundle_score"))
    band = _ranking_text(focus.get("score_band"))
    score_text = f", score {score}" if score else ""
    band_text = f", {band} risk band" if band else ""
    st.caption(
        f"Selected {unit_label}: {unit_name.upper()} - rank {rank} / {comparison_count}"
        f"{score_text}{band_text}"
    )


def _compute_visible_ranking_rows(
    *,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str],
    selected_block: Optional[str],
    state_scores: pd.DataFrame,
    district_scores: pd.DataFrame,
    block_scores: Optional[pd.DataFrame] = None,
    band_filter: Optional[Mapping[str, object]] = None,
) -> pd.DataFrame:
    """Return the exact Glance ranking rows visible for the active scope."""
    focus = str(focus_level or "india").strip().lower()
    rank_warning = ""
    if focus == "india":
        scope_df = state_scores.sort_values("bundle_score", ascending=False, kind="stable").copy()
        scope_df, applied_band = _apply_landing_band_filter(scope_df, band_filter, expected_scope="national")
        unit_scope = "state"
        rank_col = "state_rank"
        count_col = "state_count"
        unit_col = "state_name"
        parent_state = ""
        parent_district = ""
        comparison_group = "India"
    elif focus == "block" and block_scores is not None:
        scope_df = block_scores[
            (block_scores["state_name"].astype(str).map(alias) == alias(selected_state or ""))
            & (block_scores["district_name"].astype(str).map(alias) == alias(selected_district or ""))
        ].sort_values("bundle_score", ascending=False, kind="stable").copy()
        scope_df, applied_band = _apply_landing_band_filter(
            scope_df,
            band_filter,
            expected_scope="block",
            state_name=selected_state,
            district_name=selected_district,
        )
        unit_scope = "block"
        rank_col = "block_rank_within_district"
        count_col = "block_count_within_district"
        unit_col = "block_name"
        parent_state = selected_state or ""
        parent_district = selected_district or ""
        comparison_group = selected_district or ""
    else:
        scope_df = district_scores[
            district_scores["state_name"].astype(str).map(alias) == alias(selected_state or "")
        ].sort_values("bundle_score", ascending=False, kind="stable").copy()
        scope_df, applied_band = _apply_landing_band_filter(
            scope_df,
            band_filter,
            expected_scope="state",
            state_name=selected_state,
        )
        unit_scope = "district"
        rank_col = "district_rank"
        count_col = "district_count"
        unit_col = "district_name"
        parent_state = selected_state or ""
        parent_district = ""
        comparison_group = selected_state or ""
    if scope_df.empty:
        return pd.DataFrame(
            columns=[
                "unit_scope",
                "rank",
                "unit_name",
                "unit_type",
                "parent_state",
                "parent_district",
                "is_current_focus",
                "comparison_group",
                "comparison_count",
                "active_band_filter",
                "rank_warning",
            ]
        )
    if rank_col in scope_df.columns:
        rank_values = pd.to_numeric(scope_df[rank_col], errors="coerce")
    else:
        rank_values = pd.Series([np.nan] * len(scope_df), index=scope_df.index)
    if rank_values.isna().any():
        rank_warning = f"{rank_col} was missing for at least one row; rank fallback used visible score order."
        fallback = scope_df["bundle_score"].rank(method="min", ascending=False, na_option="bottom")
        rank_values = rank_values.fillna(fallback)
    scope_df["unit_scope"] = unit_scope
    scope_df["rank"] = rank_values.astype("Int64")
    scope_df["unit_name"] = scope_df[unit_col].astype(str)
    scope_df["unit_type"] = unit_scope
    scope_df["parent_state"] = parent_state if parent_state else scope_df.get("state_name", "")
    scope_df["parent_district"] = parent_district if parent_district else scope_df.get("district_name", "")
    if unit_scope == "block":
        scope_df["is_current_focus"] = scope_df[unit_col].map(
            lambda value: bool(selected_block and alias(str(value)) == alias(selected_block))
        )
    elif unit_scope == "district":
        scope_df["is_current_focus"] = scope_df[unit_col].map(
            lambda value: bool(selected_district and alias(str(value)) == alias(selected_district))
        )
    else:
        scope_df["is_current_focus"] = False
    scope_df["comparison_group"] = comparison_group
    if count_col in scope_df.columns:
        scope_df["comparison_count"] = pd.to_numeric(scope_df[count_col], errors="coerce").astype("Int64")
    else:
        scope_df["comparison_count"] = len(scope_df)
    scope_df["active_band_filter"] = applied_band or ""
    scope_df["rank_warning"] = rank_warning
    return scope_df.sort_values(["rank", "unit_name"], kind="stable").reset_index(drop=True)


def _render_glance_answer_export_panel(
    *,
    visible_rows: pd.DataFrame,
    drivers: pd.DataFrame,
    bundle_domain: str,
    scenario: str,
    period: str,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str],
) -> None:
    """Render Glance answer and export controls from the visible rankings frame."""
    st.markdown("#### Answer & Export")
    if visible_rows.empty:
        st.caption("No visible ranking rows are available to export for the current selection.")
        st.button("Generate copyable answer", key="landing_glance_answer_disabled", disabled=True)
        st.download_button("Download ranking CSV", data=b"", file_name="irt_glance_empty.csv", disabled=True, key="landing_glance_csv_disabled")
        st.download_button("Download answer pack", data=b"", file_name="irt_glance_empty.xlsx", disabled=True, key="landing_glance_xlsx_disabled")
        return
    export_frame, driver_note = build_glance_export_frame(visible_rows, drivers)
    unit_scope = str(visible_rows["unit_scope"].iloc[0])
    active_band = str(visible_rows["active_band_filter"].iloc[0] or "")
    geography = selected_district if unit_scope == "block" else selected_state if unit_scope == "district" else "India"
    geography = geography or "India"
    bundle_label = _landing_bundle_display(bundle_domain)
    scenario_label = SCENARIO_DISPLAY.get(str(scenario).strip().lower(), str(scenario))
    period_label = period_display_label(canonical_period_label(period))
    answer_text = build_glance_answer_text(
        export_frame,
        bundle_label=bundle_label,
        scenario_label=scenario_label,
        period_label=period_label,
        geography_label=geography,
        is_projection=str(scenario).strip().lower() != "snapshot",
        driver_note=driver_note,
    )
    current_focus_token = ""
    if "is_current_focus" in export_frame.columns:
        focus_rows = export_frame[export_frame["is_current_focus"].fillna(False).astype(bool)]
        if not focus_rows.empty:
            focus = focus_rows.sort_values("rank", kind="stable").head(1).iloc[0]
            focus_parts = [
                focus.get("unit_name", ""),
                focus.get("rank", ""),
                focus.get("comparison_count", ""),
                focus.get("bundle_score_display", ""),
                focus.get("score_band", ""),
                focus.get("top_driver_1", ""),
                focus.get("top_driver_2", ""),
                focus.get("top_driver_3", ""),
            ]
            current_focus_token = "|".join(alias(part) for part in focus_parts)
    answer_context_token = "|".join(
        [
            alias(bundle_domain),
            alias(scenario),
            alias(period),
            alias(focus_level),
            alias(unit_scope),
            alias(geography),
            alias(active_band),
            str(len(export_frame)),
            str(export_frame["unit_name"].astype(str).tolist()[:3]),
            current_focus_token,
        ]
    )
    if st.session_state.get("landing_glance_answer_context_token") != answer_context_token:
        st.session_state["landing_glance_answer_context_token"] = answer_context_token
        st.session_state["landing_glance_answer_text"] = answer_text
        st.session_state["landing_glance_answer_text_area"] = answer_text
    if st.button("Generate copyable answer", key="landing_glance_generate_answer", use_container_width=True):
        st.session_state["landing_glance_answer_text"] = answer_text
        st.session_state["landing_glance_answer_text_area"] = answer_text
    st.text_area(
        "Copyable answer",
        value=str(st.session_state.get("landing_glance_answer_text_area") or answer_text),
        key="landing_glance_answer_text_area",
        height=120,
    )
    metadata = {
        "bundle": bundle_domain,
        "scenario": scenario,
        "period": period,
        "geography": geography,
        "focus_level": focus_level,
        "unit_scope": unit_scope,
        "active_band_filter": active_band,
        "score_direction": "Higher bundle score indicates higher hazard signal.",
        "missing_data_rule": "Missing values remain blank; persisted ranks are used when available.",
        "source_artifacts": "state.parquet, district.parquet, block.parquet when available, drivers.parquet",
        "driver_source_artifact": "drivers.parquet",
        "rank_warning": str(visible_rows["rank_warning"].dropna().head(1).iloc[0] or ""),
    }
    csv_name = glance_export_filename(
        kind="csv",
        bundle_slug=bundle_domain,
        unit_scope=unit_scope,
        scenario=scenario,
        period=period,
        geography=geography,
        band_filter=active_band or None,
    )
    xlsx_name = glance_export_filename(
        kind="xlsx",
        bundle_slug=bundle_domain,
        unit_scope=unit_scope,
        scenario=scenario,
        period=period,
        geography=geography,
        band_filter=active_band or None,
    )
    export_cols = st.columns(2)
    with export_cols[0]:
        st.download_button(
            "Download ranking CSV",
            data=build_glance_csv_bytes(export_frame),
            file_name=csv_name,
            mime="text/csv",
            key="landing_glance_csv_download",
            use_container_width=True,
        )
    answer_pack_bytes: bytes | None = None
    answer_pack_unavailable = False
    try:
        answer_pack_bytes = build_glance_answer_pack_xlsx(
            answer_text=answer_text,
            export_frame=export_frame,
            metadata=metadata,
            driver_note=driver_note,
        )
    except ModuleNotFoundError as exc:
        if str(getattr(exc, "name", "")).strip() != "openpyxl":
            raise
        answer_pack_unavailable = True
    with export_cols[1]:
        if answer_pack_unavailable:
            st.download_button(
                "Download answer pack",
                data=b"",
                file_name=xlsx_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="landing_glance_xlsx_download",
                use_container_width=True,
                disabled=True,
            )
        else:
            st.download_button(
                "Download answer pack",
                data=answer_pack_bytes or b"",
                file_name=xlsx_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="landing_glance_xlsx_download",
                use_container_width=True,
            )


def _render_band_filter_status(applied_band: Optional[str], row_count: int, *, level_noun: str) -> None:
    """Show the active band filter, row count, and a Clear button."""
    if not applied_band:
        return
    plural = f"{level_noun}s"
    if row_count == 0:
        st.caption(
            f"Filtered to **{applied_band}** risk band — no {plural} fall in this band for the current selection."
        )
    else:
        st.caption(f"Filtered to **{applied_band}** risk band — {row_count} {plural if row_count != 1 else level_noun}.")
    if st.button("Clear filter", key="landing_band_filter_clear"):
        st.session_state.pop(LANDING_BAND_FILTER_KEY, None)
        st.rerun()


def _sanitize_compare_selection(
    session_state: MutableMapping[str, object],
    *,
    options: Sequence[str],
    defaults: Sequence[str],
) -> list[str]:
    """Keep landing compare selections valid for the active geography scope."""
    option_set = {str(option) for option in options}
    current = session_state.get(LANDING_COMPARE_KEY)
    if isinstance(current, (list, tuple)):
        sanitized = [str(value) for value in current if str(value) in option_set]
    else:
        sanitized = []

    if not sanitized:
        sanitized = [str(value) for value in defaults if str(value) in option_set]

    session_state[LANDING_COMPARE_KEY] = sanitized
    return sanitized


def _render_landing_compare(
    *,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str],
    state_scores: pd.DataFrame,
    district_scores: pd.DataFrame,
    block_scores: Optional[pd.DataFrame] = None,
    selected_block: Optional[str] = None,
) -> None:
    """Render the lightweight landing compare view for the current geography scope."""
    if focus_level == "india":
        scope_df = state_scores.sort_values("bundle_score", ascending=False, kind="stable").copy()
        unit_column = "state_name"
        unit_label = "states"
        defaults = scope_df["state_name"].head(3).tolist()
        context_mean = pd.to_numeric(scope_df["bundle_score"], errors="coerce").dropna().mean()
    elif focus_level == "block" and block_scores is not None:
        scope_df = block_scores[
            (block_scores["state_name"].astype(str).map(alias) == alias(selected_state or ""))
            & (block_scores["district_name"].astype(str).map(alias) == alias(selected_district or ""))
        ].sort_values("bundle_score", ascending=False, kind="stable").copy()
        unit_column = "block_name"
        unit_label = "blocks"
        defaults = scope_df["block_name"].head(3).tolist()
        if selected_block and selected_block not in defaults:
            defaults = [selected_block] + defaults[:2]
        context_mean = pd.to_numeric(scope_df["bundle_score"], errors="coerce").dropna().mean()
    else:
        scope_df = district_scores[
            district_scores["state_name"].astype(str).map(alias) == alias(selected_state or "")
        ].sort_values("bundle_score", ascending=False, kind="stable").copy()
        unit_column = "district_name"
        unit_label = "districts"
        defaults = scope_df["district_name"].head(3).tolist()
        if selected_district and selected_district not in defaults:
            defaults = [selected_district] + defaults[:2]
        context_mean = pd.to_numeric(scope_df["bundle_score"], errors="coerce").dropna().mean()

    options = scope_df[unit_column].astype(str).tolist()
    defaults = _sanitize_compare_selection(
        st.session_state,
        options=options,
        defaults=defaults,
    )

    selected_units = st.multiselect(
        f"Compare {unit_label}",
        options=options,
        default=defaults,
        key=LANDING_COMPARE_KEY,
    )

    compare_df = scope_df[scope_df[unit_column].astype(str).isin(selected_units)].copy()
    if compare_df.empty:
        st.info("Select at least one geography to compare.")
        return

    compare_df["delta_vs_scope_mean"] = pd.to_numeric(compare_df["bundle_score"], errors="coerce") - float(context_mean)
    compare_df["delta_vs_scope_mean"] = compare_df["delta_vs_scope_mean"].map(
        lambda value: f"{float(value):+.1f}" if np.isfinite(value) else "n/a"
    )

    if focus_level == "india":
        display_df = compare_df.rename(
            columns={
                "state_name": "State",
                "bundle_score_display": "Bundle score",
                "score_band": "Risk band",
                "delta_vs_scope_mean": "vs India mean",
            }
        )
        st.dataframe(
            display_df[["State", "Bundle score", "Risk band", "vs India mean"]],
            hide_index=True,
            use_container_width=True,
        )
    elif focus_level == "block" and block_scores is not None:
        compare_df["Current focus"] = compare_df["block_name"].map(
            lambda value: "Selected" if selected_block and alias(str(value)) == alias(selected_block) else ""
        )
        display_df = compare_df.rename(
            columns={
                "block_name": "Block",
                "bundle_score_display": "Bundle score",
                "score_band": "Risk band",
                "delta_vs_scope_mean": f"vs {selected_district} mean",
            }
        )
        st.dataframe(
            display_df[["Block", "Bundle score", "Risk band", f"vs {selected_district} mean", "Current focus"]],
            hide_index=True,
            use_container_width=True,
        )
    else:
        compare_df["Current focus"] = compare_df["district_name"].map(
            lambda value: "Selected" if selected_district and alias(str(value)) == alias(selected_district) else ""
        )
        display_df = compare_df.rename(
            columns={
                "district_name": "District",
                "bundle_score_display": "Bundle score",
                "score_band": "Risk band",
                "delta_vs_scope_mean": f"vs {selected_state} mean",
            }
        )
        st.dataframe(
            display_df[["District", "Bundle score", "Risk band", f"vs {selected_state} mean", "Current focus"]],
            hide_index=True,
            use_container_width=True,
        )


def _enter_deep_dive(
    session_state: MutableMapping[str, object],
) -> None:
    """Apply the landing -> detailed workflow handoff and rerun the app."""
    bundle_domain = str(session_state.get("landing_bundle") or LANDING_DEFAULT_BUNDLE).strip()
    dashboard_spec = get_dashboard_bundle_spec(bundle_domain)
    if dashboard_spec is None:
        st.warning("Deep Dive is unavailable because this Glance bundle has no configured composite metric.")
        return

    handoff = build_deep_dive_handoff(
        session_state,
        bundle_domain=bundle_domain,
        metric_slug=dashboard_spec.composite_slug,
    )
    for key, value in handoff.items():
        session_state[key] = value
    st.rerun()


def render_landing_page(
    *,
    adm1: Any,
    adm2: Any,
    adm3_by_district: Optional[dict] = None,
    data_dir: Path,
) -> None:
    """Render the climate-hazard landing / discovery surface."""
    ensure_landing_state(st.session_state)
    sync_landing_widget_state(st.session_state)
    _sanitize_landing_context(st.session_state, data_dir=data_dir)

    bundle_domain = str(st.session_state.get("landing_bundle") or LANDING_DEFAULT_BUNDLE).strip()
    scenario = str(st.session_state.get("landing_scenario") or LANDING_DEFAULT_SCENARIO).strip()
    period = canonical_period_label(str(st.session_state.get("landing_period") or LANDING_DEFAULT_PERIOD).strip())
    focus_level = str(st.session_state.get("landing_focus_level", "india")).strip().lower()
    selected_state = str(st.session_state.get("landing_selected_state") or "").strip() or None
    selected_district = str(st.session_state.get("landing_selected_district") or "").strip() or None
    selected_block = str(st.session_state.get("landing_selected_block") or "").strip() or None
    bundle_options = _landing_bundle_domains(data_dir=data_dir)
    if not bundle_options:
        st.error("No Glance bundles are available for the landing experience.")
        return

    scenario_options = _bundle_scenario_period_options(bundle_domain, data_dir=data_dir)
    district_scores, state_scores = _prepare_bundle_context(
        bundle_domain,
        scenario=scenario,
        period=period,
        stat=LANDING_SCORE_STAT,
        data_dir=data_dir,
    )
    driver_context = _prepare_driver_context(
        bundle_domain,
        scenario=scenario,
        period=period,
        stat=LANDING_SCORE_STAT,
        data_dir=data_dir,
    )
    glance_context = _load_glance_pair_context(
        bundle_domain,
        scenario=scenario,
        period=period,
        data_dir=data_dir,
    )
    block_scores = glance_context.block
    block_available = block_scores is not None and not block_scores.empty
    search_options = _build_landing_search_options(state_scores, district_scores)
    if str(st.session_state.get("landing_tab") or LANDING_DEFAULT_TAB) not in LANDING_TABS:
        st.session_state["landing_tab"] = LANDING_DEFAULT_TAB
    _clear_stale_landing_band_filter(
        st.session_state,
        bundle=bundle_domain,
        scenario=scenario,
        period=period,
        focus_level=focus_level,
        selected_state=selected_state,
        selected_district=selected_district,
    )
    if bool(st.session_state.get("landing_search_reset_pending", False)):
        st.session_state["landing_search_selection"] = None
        st.session_state["landing_search_last_applied"] = None
        st.session_state["landing_search_reset_pending"] = False
    if st.session_state.get("landing_search_selection") not in search_options:
        st.session_state["landing_search_selection"] = None

    st.title("India Resilience Tool")

    control_cols = st.columns([2.4, 1.4, 1.6, 0.9])
    with control_cols[0]:
        search_selection = st.selectbox(
            "Search geography",
            options=list(search_options.keys()),
            index=None,
            key="landing_search_selection",
            placeholder=LANDING_SEARCH_PLACEHOLDER,
            label_visibility="visible",
        )
    with control_cols[1]:
        st.selectbox(
            "Bundle",
            options=bundle_options,
            index=bundle_options.index(bundle_domain),
            key="landing_bundle",
            label_visibility="visible",
            format_func=_landing_bundle_display,
        )
    with control_cols[2]:
        if scenario_options:
            selected_pair = st.selectbox(
                "Scenario-period",
                options=scenario_options,
                index=scenario_options.index((scenario, period)),
                key=LANDING_SCENARIO_PAIR_KEY,
                label_visibility="visible",
                format_func=lambda pair: _landing_context_chip(pair[0], pair[1]),
            )
            st.session_state["landing_scenario"] = selected_pair[0]
            st.session_state["landing_period"] = selected_pair[1]
            scenario, period = selected_pair
        else:
            st.text_input(
                "Scenario-period",
                value="No full-coverage scenario-period available",
                disabled=True,
                label_visibility="visible",
            )
    with control_cols[3]:
        st.write("")
        if st.button(
            "Deep Dive",
            key="landing_deep_dive_top",
            use_container_width=True,
            disabled=not scenario_options,
        ):
            _enter_deep_dive(
                st.session_state,
            )
    if _apply_landing_search_selection(
        st.session_state,
        search_selection=search_selection,
        search_options=search_options,
    ):
        st.rerun()

    if not scenario_options:
        st.info(
            "No scenario-period currently has full required metric coverage for this bundle. "
            "Choose another bundle or return later when coverage is available."
        )

    if focus_level == "state" and selected_state and not (
        adm1["shapeName"].astype(str).map(alias) == alias(selected_state)
    ).any():
        _clear_landing_pending_map_transition(st.session_state)
        set_landing_focus_india(st.session_state)
        st.rerun()

    if focus_level == "district" and selected_state and selected_district:
        district_exists = adm2[
            (adm2["state_name"].astype(str).map(alias) == alias(selected_state))
            & (adm2["district_name"].astype(str).map(alias) == alias(selected_district))
        ]
        if district_exists.empty:
            _clear_landing_pending_map_transition(st.session_state)
            set_landing_focus_state(st.session_state, selected_state)
            st.rerun()

    if focus_level == "block" and selected_state and selected_district:
        district_exists = adm2[
            (adm2["state_name"].astype(str).map(alias) == alias(selected_state))
            & (adm2["district_name"].astype(str).map(alias) == alias(selected_district))
        ]
        if district_exists.empty:
            _clear_landing_pending_map_transition(st.session_state)
            set_landing_focus_state(st.session_state, selected_state)
            st.rerun()
        if not block_available or adm3_by_district is None:
            _clear_landing_pending_map_transition(st.session_state)
            set_landing_focus_district(st.session_state, selected_state, selected_district)
            st.rerun()
        district_block_scope = block_scores[
            (block_scores["state_name"].astype(str).map(alias) == alias(selected_state))
            & (block_scores["district_name"].astype(str).map(alias) == alias(selected_district))
        ]
        if district_block_scope.empty:
            _clear_landing_pending_map_transition(st.session_state)
            set_landing_focus_district(st.session_state, selected_state, selected_district)
            st.rerun()
        if selected_block and not (
            district_block_scope["block_name"].astype(str).map(alias) == alias(selected_block)
        ).any():
            st.session_state["landing_selected_block"] = None
            selected_block = None

    map_col, drawer_col = st.columns([4.2, 1.8])
    with map_col:
        action_cols = st.columns([0.9, 1.0, 4.6])
        with action_cols[0]:
            if st.button(
                "Back",
                key="landing_back",
                disabled=focus_level == "india",
                use_container_width=True,
            ):
                _clear_landing_pending_map_transition(st.session_state)
                apply_landing_back(st.session_state)
                st.rerun()
        with action_cols[1]:
            if st.button("Reset to India", key="landing_reset", use_container_width=True):
                _clear_landing_pending_map_transition(st.session_state)
                set_landing_focus_india(st.session_state)
                st.rerun()
        with action_cols[2]:
            st.markdown(
                f"**{_landing_map_label(bundle_domain=bundle_domain, scenario=scenario, period=period, focus_level=focus_level, selected_state=selected_state, selected_district=selected_district)}**"
            )

        landing_map, legend_html, _map_label, visible_map_gdf = _build_landing_map_artifacts(
            adm1=adm1,
            adm2=adm2,
            adm3_by_district=adm3_by_district if focus_level == "block" else None,
            state_scores=state_scores,
            district_scores=district_scores,
            block_scores=block_scores if focus_level == "block" else None,
            bundle_domain=bundle_domain,
            scenario=scenario,
            period=period,
            focus_level=focus_level,
            selected_state=selected_state,
            selected_district=selected_district,
            selected_block=selected_block,
        )

        map_level = "state" if focus_level == "india" else ("block" if focus_level == "block" else "district")
        returned, clicked_district, clicked_state = render_map_view(
            m=landing_map,
            variable_slug=f"landing_{alias(bundle_domain)}",
            map_mode="Bundle score",
            sel_scenario=scenario,
            sel_period=period,
            sel_stat=LANDING_SCORE_STAT,
            selected_state=selected_state or "All",
            selected_district=selected_district or "All",
            selected_block=selected_block or "All",
            selected_basin="All",
            selected_subbasin="All",
            map_width=780,
            map_height=520,
            legend_block_html=legend_html,
            level=map_level,
            perf_section=None,
        )
        clicked_block = str(st.session_state.get("clicked_block") or "").strip() or None
        raw_returned = returned
        raw_clicked_district = clicked_district
        raw_clicked_state = clicked_state
        raw_clicked_block = clicked_block
        raw_payload_is_empty = _landing_map_payload_is_empty(raw_returned)
        map_context_token = _landing_map_context_token(
            bundle_domain=bundle_domain,
            scenario=scenario,
            period=period,
            focus_level=focus_level,
            selected_state=selected_state,
            selected_district=selected_district,
            selected_block=selected_block,
        )
        map_input_armed, map_context_changed = _sync_landing_map_input_gate(
            st.session_state,
            context_token=map_context_token,
            payload_is_empty=raw_payload_is_empty,
        )
        rerun_reason: Optional[str] = None
        if _consume_pending_landing_map_transition(
            st.session_state,
            focus_level=focus_level,
            selected_state=selected_state,
            selected_district=selected_district,
            selected_block=selected_block,
        ):
            returned = {}
            clicked_district = None
            clicked_state = None
            clicked_block = None
        if not map_input_armed:
            returned = {}
            clicked_district = None
            clicked_state = None
            clicked_block = None
        click_action, next_state, next_district, next_block = _apply_landing_map_click(
            focus_level=focus_level,
            returned=returned,
            clicked_state=clicked_state,
            clicked_district=clicked_district,
            clicked_block=clicked_block,
            selected_state=selected_state,
            selected_district=selected_district,
            selected_block=selected_block,
            adm1=adm1,
            adm2=adm2,
            visible_districts=visible_map_gdf if focus_level in {"state", "district"} else None,
            visible_blocks=visible_map_gdf if focus_level == "block" else None,
        )
        rerun_reason = (
            "landing_map_click_transition"
            if click_action in {"focus_state", "focus_district", "focus_block"}
            else None
        )
        if bool(st.session_state.get("perf_enabled", False)):
            with st.expander("Landing click debug", expanded=False):
                st.json(
                    {
                        "focus_level": focus_level,
                        "returned": returned,
                        "raw_returned": raw_returned,
                        "clicked_state": clicked_state,
                        "clicked_district": clicked_district,
                        "clicked_block": clicked_block,
                        "raw_clicked_state": raw_clicked_state,
                        "raw_clicked_district": raw_clicked_district,
                        "raw_clicked_block": raw_clicked_block,
                        "click_action": click_action,
                        "next_state": next_state,
                        "next_district": next_district,
                        "next_block": next_block,
                        "pending_transition": st.session_state.get(LANDING_PENDING_MAP_TRANSITION_KEY),
                        "map_context_token": map_context_token,
                        "map_context_changed": map_context_changed,
                        "map_input_armed": st.session_state.get(LANDING_MAP_INPUT_ARMED_KEY, False),
                        "raw_payload_is_empty": raw_payload_is_empty,
                        "rerun_reason": rerun_reason,
                    }
                )
        if _queue_landing_map_transition(
            st.session_state,
            action=click_action,
            state_name=next_state,
            district_name=next_district,
            block_name=next_block,
        ):
            st.rerun()
        if click_action == "noop" and raw_payload_is_empty:
            _clear_landing_pending_transition_token(st.session_state)

    with drawer_col:
        if focus_level == "india":
            _render_national_summary(
                state_scores=state_scores,
                bundle_domain=bundle_domain,
                distributions=glance_context.distributions,
            )
        elif focus_level == "state" and selected_state:
                _render_state_summary(
                    bundle_domain=bundle_domain,
                    state_name=selected_state,
                    district_scores=district_scores,
                    state_scores=state_scores,
                    driver_context=driver_context,
                    distributions=glance_context.distributions,
                    deep_dive_disabled=not scenario_options,
                )
        elif focus_level == "district" and selected_state and selected_district:
                _render_district_summary(
                    bundle_domain=bundle_domain,
                    state_name=selected_state,
                    district_name=selected_district,
                    district_scores=district_scores,
                    driver_context=driver_context,
                    attributes=glance_context.attributes,
                    deep_dive_disabled=not scenario_options,
                    block_scores_available=(
                        block_available
                        and adm3_by_district is not None
                        and bool(
                            (
                                (block_scores["state_name"].astype(str).map(alias) == alias(selected_state))
                                & (block_scores["district_name"].astype(str).map(alias) == alias(selected_district))
                            ).any()
                        )
                    ),
                )
        elif focus_level == "block" and selected_state and selected_district and block_scores is not None:
                _render_block_summary(
                    bundle_domain=bundle_domain,
                    state_name=selected_state,
                    district_name=selected_district,
                    block_name=selected_block,
                    block_scores=block_scores,
                    driver_context=driver_context,
                    deep_dive_disabled=not scenario_options,
                )

    st.write("")
    st.radio(
        "Landing tab",
        options=list(LANDING_TABS),
        horizontal=True,
        key="landing_tab",
        label_visibility="collapsed",
    )

    if str(st.session_state.get("landing_tab") or LANDING_DEFAULT_TAB) == "Compare":
        _render_landing_compare(
            focus_level=focus_level,
            selected_state=selected_state,
            selected_district=selected_district,
            selected_block=selected_block,
            state_scores=state_scores,
            district_scores=district_scores,
            block_scores=block_scores,
        )
    else:
        visible_ranking_rows = _render_landing_rankings(
            focus_level=focus_level,
            selected_state=selected_state,
            selected_district=selected_district,
            selected_block=selected_block,
            state_scores=state_scores,
            district_scores=district_scores,
            block_scores=block_scores,
        )
        if visible_ranking_rows is not None:
            _render_glance_answer_export_panel(
                visible_rows=visible_ranking_rows,
                drivers=glance_context.drivers,
                bundle_domain=bundle_domain,
                scenario=scenario,
                period=period,
                focus_level=focus_level,
                selected_state=selected_state,
                selected_district=selected_district,
            )

    method_note = (
        "Method note: landing bundle scores are weighted averages of normalized hazard metrics "
        "using approved bundle definitions. "
        "Only scenario-periods with full required bundle-metric coverage are shown. "
        "They are hazard summaries only, not resilience scores."
    )
    st.caption(method_note)

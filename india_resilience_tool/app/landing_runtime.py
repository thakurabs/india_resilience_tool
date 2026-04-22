"""
Landing / discovery surface runtime for the India Resilience Tool (IRT).

This module implements the climate-hazard-first landing experience:
India -> State -> District -> Deep Dive.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, MutableMapping, NamedTuple, Optional, Sequence

import numpy as np
import pandas as pd
import streamlit as st

from india_resilience_tool.analysis.bundle_scores import (
    BundleMetricSpec,
    aggregate_state_bundle_scores,
    compute_bundle_score_frame,
    normalized_metric_column,
)
from india_resilience_tool.config.bundle_weights import get_bundle_weights
from india_resilience_tool.config.composite_metrics import get_composite_metric_for_bundle
from india_resilience_tool.app.geography import list_available_states_from_processed_root
from india_resilience_tool.app.views.map_view import (
    build_choropleth_map_with_geojson_layer,
    extract_click_coordinates,
    find_district_at_coordinates,
    find_state_at_coordinates,
    render_map_view,
)
from india_resilience_tool.config.constants import MAX_LAT, MAX_LON, MIN_LAT, MIN_LON
from india_resilience_tool.config.variables import (
    VARIABLES,
    get_metrics_for_bundle,
    get_pillar_for_domain,
)
from india_resilience_tool.data.master_columns import resolve_metric_column
from india_resilience_tool.data.master_loader import (
    load_master_csvs,
    master_source_signature,
    normalize_master_columns,
    parse_master_schema,
    resolve_preferred_master_path,
)
from india_resilience_tool.data.optimized_bundle import (
    optimized_master_sources_from_metric_root,
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
from paths import resolve_processed_optimised_root, resolve_processed_root


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

LANDING_DOMAIN_DISPLAY: dict[str, str] = {
    "Heat Risk": "Heat",
    "Heat Stress": "Heat Stress",
    "Cold Risk": "Cold",
    "Agriculture & Growing Conditions": "Agriculture",
    "Flood & Extreme Rainfall Risk": "Extreme Rainfall",
    "Rainfall Totals & Typical Wetness": "Rainfall",
    "Drought Risk": "Drought",
    "Temperature Variability": "Temperature Variability",
}

LANDING_DOMAIN_ORDER: tuple[str, ...] = (
    "Heat Risk",
    "Drought Risk",
    "Flood & Extreme Rainfall Risk",
    "Heat Stress",
    "Cold Risk",
    "Agriculture & Growing Conditions",
)
LANDING_VISIBLE_DOMAINS: tuple[str, ...] = LANDING_DOMAIN_ORDER

LANDING_PERIOD_SHORT_LABELS: dict[str, str] = {
    "Current": "Current",
    "2020-2040": "Early century",
    "2040-2060": "Mid-century",
    "2060-2080": "End century",
}


@dataclass(frozen=True)
class LandingMetricContext:
    """Resolved bundle-metric context used by the landing selectors and score prep."""

    spec: BundleMetricSpec
    source_signature: tuple[tuple[str, Optional[float]], ...]
    source_paths: tuple[str, ...]
    available_pairs: tuple[tuple[str, str], ...]


class _ContextKeyEntry(NamedTuple):
    """Decoded landing bundle-context cache entry."""

    slug: str
    label: str
    column: str
    weight: float
    higher_is_worse: bool
    source_signature: tuple[tuple[str, Optional[float]], ...]
    source_paths: tuple[str, ...]
    available_pairs: tuple[tuple[str, str], ...]


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


def set_landing_focus_state(
    session_state: MutableMapping[str, object],
    state_name: str,
) -> None:
    """Move the landing view into state focus."""
    session_state["landing_focus_level"] = "state"
    session_state["landing_selected_state"] = str(state_name).strip() or None
    session_state["landing_selected_district"] = None


def set_landing_focus_district(
    session_state: MutableMapping[str, object],
    state_name: str,
    district_name: str,
) -> None:
    """Move the landing view into district focus while preserving state context."""
    session_state["landing_focus_level"] = "district"
    session_state["landing_selected_state"] = str(state_name).strip() or None
    session_state["landing_selected_district"] = str(district_name).strip() or None


def apply_landing_back(session_state: MutableMapping[str, object]) -> None:
    """Reverse the landing drill-down hierarchy by one step."""
    focus_level = str(session_state.get("landing_focus_level", "india")).strip().lower()
    if focus_level == "district":
        session_state["landing_focus_level"] = "state"
        session_state["landing_selected_district"] = None
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
) -> Optional[tuple[str, str, str]]:
    """Return a stable token for one landing focus transition target."""
    focus_value = str(focus_level or "").strip().lower()
    if focus_value not in {"state", "district"}:
        return None
    return (
        focus_value,
        alias(str(state_name or "").strip()),
        alias(str(district_name or "").strip()),
    )


def _queue_landing_map_transition(
    session_state: MutableMapping[str, object],
    *,
    action: str,
    state_name: Optional[str],
    district_name: Optional[str],
) -> bool:
    """Apply one landing map click and mark the resulting rerun as pending."""
    action_value = str(action or "").strip().lower()
    if action_value not in {"focus_state", "focus_district"}:
        return False

    focus_level = "state" if action_value == "focus_state" else "district"
    if focus_level == "state" and not state_name:
        return False
    if focus_level == "district" and (not state_name or not district_name):
        return False

    token = _landing_pending_map_transition(
        focus_level=focus_level,
        state_name=state_name,
        district_name=district_name,
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
    return False


def _consume_pending_landing_map_transition(
    session_state: MutableMapping[str, object],
    *,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str],
) -> bool:
    """Suppress one replayed map payload after a successful landing transition rerun."""
    pending = session_state.get(LANDING_PENDING_MAP_TRANSITION_KEY)
    if not isinstance(pending, (tuple, list)) or len(pending) != 3:
        return False

    expected = _landing_pending_map_transition(
        focus_level=focus_level,
        state_name=selected_state,
        district_name=selected_district,
    )
    pending_token = (
        str(pending[0]).strip().lower(),
        alias(str(pending[1]).strip()),
        alias(str(pending[2]).strip()),
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
    selected_pillar = get_pillar_for_domain(bundle_domain) or "Climate Hazards"
    pending_state = selected_state if focus_level in {"state", "district"} and selected_state else "All"
    pending_district = selected_district if focus_level == "district" and selected_district else "All"
    return {
        "landing_active": False,
        "spatial_family": "admin",
        "admin_level": "district",
        "analysis_mode": "Single district focus",
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
        "selected_block": "All",
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
    visible_bundles = set(_landing_bundle_domains())

    if not (
        spatial_family == "admin"
        and admin_level == "district"
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
        return updates

    if selected_district == "All" or not selected_district:
        updates["landing_focus_level"] = "state"
        updates["landing_selected_state"] = selected_state
        updates["landing_selected_district"] = None
        return updates

    updates["landing_focus_level"] = "district"
    updates["landing_selected_state"] = selected_state
    updates["landing_selected_district"] = selected_district
    return updates


def _landing_bundle_domains() -> list[str]:
    """Return the supported landing bundles in a stable UX order."""
    visible_domains = [
        domain
        for domain in LANDING_VISIBLE_DOMAINS
        if get_metrics_for_bundle(domain, spatial_family="admin", level="district")
    ]
    ordered = [domain for domain in LANDING_DOMAIN_ORDER if domain in set(visible_domains)]
    if ordered:
        return ordered
    return sorted(visible_domains)


def _landing_bundle_display(bundle_domain: str) -> str:
    """Return the user-facing landing label for a bundle/domain."""
    return LANDING_DOMAIN_DISPLAY.get(str(bundle_domain).strip(), str(bundle_domain).strip())


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
) -> str:
    """Build the trust-critical map label for the current landing context."""
    level_label = "State-level" if focus_level == "india" else "District-level"
    bundle_label = _landing_bundle_display(bundle_domain)
    chip = _landing_context_chip(scenario, period)
    if focus_level == "india" or not selected_state:
        return f"{level_label} {bundle_label} Bundle Score • {chip}"
    return f"{level_label} {bundle_label} Bundle Score • {selected_state} • {chip}"


def _score_band(score: object) -> str:
    """Return a simple qualitative score band for the landing score."""
    try:
        value = float(score)
    except (TypeError, ValueError):
        return "Insufficient data"

    if not np.isfinite(value):
        return "Insufficient data"
    if value < 25.0:
        return "Low"
    if value < 50.0:
        return "Moderate"
    if value < 75.0:
        return "High"
    return "Very High"


def _format_score(score: object) -> str:
    """Return a compact, user-facing score string."""
    try:
        value = float(score)
    except (TypeError, ValueError):
        return "Insufficient data"
    if not np.isfinite(value):
        return "Insufficient data"
    return f"{value:.1f}"


def _standardize_admin_district_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize admin district master columns to stable landing names."""
    out = df.copy()

    rename_map: dict[str, str] = {}
    if "state_name" not in out.columns:
        for candidate in ("state", "STATE_UT", "shapeName_0"):
            if candidate in out.columns:
                rename_map[candidate] = "state_name"
                break
    if "district_name" not in out.columns:
        for candidate in ("district", "DISTRICT", "shapeName", "shapeName_2"):
            if candidate in out.columns:
                rename_map[candidate] = "district_name"
                break
    if rename_map:
        out = out.rename(columns=rename_map)

    for required in ("state_name", "district_name"):
        if required not in out.columns:
            out[required] = ""

    out["state_name"] = out["state_name"].astype("string").fillna("").str.strip()
    out["district_name"] = out["district_name"].astype("string").fillna("").str.strip()
    out = out[(out["state_name"] != "") & (out["district_name"] != "")]
    return out.reset_index(drop=True)


def _path_exists(path: Path) -> bool:
    """Return True when a CSV path or its preferred Parquet companion exists."""
    return resolve_preferred_master_path(path).exists()


def _resolve_metric_master_sources(
    metric_slug: str,
    *,
    data_dir: Path,
) -> tuple[Path, ...]:
    """Resolve district-level admin master sources for one metric slug."""
    optimized_root = resolve_processed_optimised_root(
        metric_slug,
        data_dir=data_dir,
        mode="portfolio",
    )
    optimized_sources: tuple[Path, ...] = ()
    if optimized_root.exists():
        optimized_sources = tuple(
            path
            for path in optimized_master_sources_from_metric_root(
                optimized_root,
                level="district",
                selected_state="All",
            )
            if path.exists()
        )
    if optimized_sources:
        return optimized_sources

    legacy_root = resolve_processed_root(
        metric_slug,
        data_dir=data_dir,
        mode="portfolio",
    )
    states = list_available_states_from_processed_root(str(legacy_root.resolve()))
    legacy_sources = tuple(
        legacy_root / state_name / "master_metrics_by_district.csv"
        for state_name in states
        if _path_exists(legacy_root / state_name / "master_metrics_by_district.csv")
    )
    return legacy_sources


@st.cache_data(show_spinner=False)
def _load_metric_scenario_period_pairs_cached(
    metric_slug: str,
    source_signature: tuple[tuple[str, Optional[float]], ...],
    source_paths: tuple[str, ...],
) -> tuple[tuple[str, str], ...]:
    """Read one metric master and list supported future scenario-period pairs."""
    _ = source_signature
    if not source_paths:
        return tuple()

    df = normalize_master_columns(load_master_csvs(source_paths))
    schema_items, _metrics, by_metric = parse_master_schema(df.columns)

    metric_base = str(VARIABLES.get(metric_slug, {}).get("periods_metric_col") or metric_slug).strip()
    items = by_metric.get(metric_base, []) or schema_items

    allowed_scenarios = {"ssp245", "ssp585", "snapshot"}
    pairs = {
        (str(item["scenario"]).strip().lower(), canonical_period_label(str(item["period"]).strip()))
        for item in items
        if str(item["scenario"]).strip().lower() in allowed_scenarios
    }
    ordered: list[tuple[str, str]] = []
    by_scenario: dict[str, list[str]] = {}
    for scenario, period in pairs:
        by_scenario.setdefault(scenario, []).append(period)

    for scenario in ordered_scenario_keys(list(by_scenario.keys())):
        for period in ordered_period_keys(by_scenario.get(scenario, [])):
            ordered.append((scenario, period))
    return tuple(ordered)


@st.cache_data(show_spinner=False)
def _load_metric_district_values_cached(
    metric_slug: str,
    scenario: str,
    period: str,
    stat: str,
    source_signature: tuple[tuple[str, Optional[float]], ...],
    source_paths: tuple[str, ...],
) -> pd.DataFrame:
    """Load one metric's district-level values for the selected scenario/period."""
    _ = source_signature
    if not source_paths:
        return pd.DataFrame(columns=["state_name", "district_name", "raw_metric_value"])

    df = normalize_master_columns(load_master_csvs(source_paths))
    df = _standardize_admin_district_frame(df)

    metric_base = str(VARIABLES.get(metric_slug, {}).get("periods_metric_col") or metric_slug).strip()
    metric_col = resolve_metric_column(
        df,
        metric_base,
        scenario,
        canonical_period_label(period),
        stat,
    )

    out = df.loc[:, ["state_name", "district_name"]].copy()
    if metric_col and metric_col in df.columns:
        out["raw_metric_value"] = pd.to_numeric(df[metric_col], errors="coerce")
    else:
        out["raw_metric_value"] = np.nan

    grouped = (
        out.groupby(["state_name", "district_name"], as_index=False, dropna=False)["raw_metric_value"]
        .mean()
        .reset_index(drop=True)
    )
    return grouped


def _landing_metric_slugs(bundle_domain: str) -> list[str]:
    """Return the ordered metric slugs that define one landing bundle."""
    configured_weights = tuple(get_bundle_weights(bundle_domain))
    available_metrics = set(
        get_metrics_for_bundle(bundle_domain, spatial_family="admin", level="district")
    )
    if configured_weights:
        ordered: list[str] = []
        seen: set[str] = set()
        for entry in configured_weights:
            slug = str(entry.metric_slug).strip()
            if slug in seen:
                raise ValueError(f"Bundle {bundle_domain!r} repeats weighted metric slug {slug!r}.")
            if slug not in VARIABLES:
                raise ValueError(f"Bundle {bundle_domain!r} references unknown weighted metric slug {slug!r}.")
            if slug not in available_metrics:
                raise ValueError(
                    f"Bundle {bundle_domain!r} references weighted metric slug {slug!r} "
                    "that is not available for admin/district landing."
                )
            seen.add(slug)
            ordered.append(slug)
        return ordered
    return get_metrics_for_bundle(bundle_domain, spatial_family="admin", level="district")


def _bundle_metric_specs(bundle_domain: str) -> list[BundleMetricSpec]:
    """Return normalized bundle metric specs for the landing score."""
    configured_weights = {entry.metric_slug: entry for entry in get_bundle_weights(bundle_domain)}
    specs: list[BundleMetricSpec] = []
    for metric_slug in _landing_metric_slugs(bundle_domain):
        varcfg = VARIABLES.get(metric_slug, {})
        weight_entry = configured_weights.get(metric_slug)
        specs.append(
            BundleMetricSpec(
                slug=metric_slug,
                label=str(varcfg.get("label") or metric_slug),
                column=metric_slug,
                weight=float(weight_entry.weight) if weight_entry is not None else 1.0,
                higher_is_worse=bool(varcfg.get("rank_higher_is_worse", True)),
            )
        )
    return specs


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


def _collect_bundle_metric_contexts(
    bundle_domain: str,
    *,
    data_dir: Path,
) -> list[LandingMetricContext]:
    """Resolve landing metric contexts for one bundle in stable registry order."""
    contexts: list[LandingMetricContext] = []
    for spec in _bundle_metric_specs(bundle_domain):
        sources = _resolve_metric_master_sources(spec.slug, data_dir=data_dir)
        if sources:
            source_signature = master_source_signature(sources)
            source_paths = tuple(str(path) for path in sources)
            available_pairs = _load_metric_scenario_period_pairs_cached(
                spec.slug,
                source_signature,
                source_paths,
            )
        else:
            source_signature = ()
            source_paths = ()
            available_pairs = ()
        contexts.append(
            LandingMetricContext(
                spec=spec,
                source_signature=source_signature,
                source_paths=source_paths,
                available_pairs=available_pairs,
            )
        )
    return contexts


def _intersect_bundle_scenario_period_pairs(
    metric_contexts: Sequence[LandingMetricContext],
) -> list[tuple[str, str]]:
    """
    Return scenario-period options with full required bundle-metric coverage.

    V1 contract:
    - all bundle metrics currently resolved for the landing bundle are required
    - only scenario-period pairs present for every required metric are selectable
    """
    if not metric_contexts:
        return []

    common_pairs: Optional[set[tuple[str, str]]] = None
    for ctx in metric_contexts:
        ctx_pairs = set(ctx.available_pairs)
        common_pairs = ctx_pairs if common_pairs is None else (common_pairs & ctx_pairs)

    return _ordered_scenario_period_pairs(common_pairs or set())


def _bundle_context_cache_key(
    metric_contexts: Sequence[LandingMetricContext],
) -> tuple[tuple[object, ...], ...]:
    """Return a stable, hashable cache key for one bundle's metric contexts."""
    return tuple(
        (
            ctx.spec.slug,
            ctx.spec.label,
            ctx.spec.column,
            float(ctx.spec.weight),
            bool(ctx.spec.higher_is_worse),
            ctx.source_signature,
            ctx.source_paths,
            ctx.available_pairs,
        )
        for ctx in metric_contexts
    )


def _decode_context_key_entry(entry: tuple[object, ...]) -> _ContextKeyEntry:
    """Decode and validate one serialized landing metric-context cache entry."""
    if len(entry) != 8:
        raise ValueError(
            f"Malformed landing context-key entry: expected 8 fields, got {len(entry)}"
        )

    (
        slug,
        label,
        column,
        weight,
        higher_is_worse,
        source_signature,
        source_paths,
        available_pairs,
    ) = entry
    return _ContextKeyEntry(
        slug=str(slug),
        label=str(label),
        column=str(column),
        weight=float(weight),
        higher_is_worse=bool(higher_is_worse),
        source_signature=tuple(source_signature),  # type: ignore[arg-type]
        source_paths=tuple(str(path) for path in source_paths),  # type: ignore[arg-type]
        available_pairs=tuple(
            (str(pair[0]).strip(), canonical_period_label(str(pair[1]).strip()))
            for pair in available_pairs  # type: ignore[arg-type]
        ),
    )


def _metric_specs_from_context_key(
    context_key: tuple[tuple[object, ...], ...],
) -> list[BundleMetricSpec]:
    """Reconstruct bundle metric specs from the serialized context cache key."""
    specs: list[BundleMetricSpec] = []
    for entry in context_key:
        parsed = _decode_context_key_entry(entry)
        specs.append(
            BundleMetricSpec(
                slug=parsed.slug,
                label=parsed.label,
                column=parsed.column,
                weight=parsed.weight,
                higher_is_worse=parsed.higher_is_worse,
            )
        )
    return specs


def _build_empty_bundle_context(
    metric_specs: Sequence[BundleMetricSpec],
) -> tuple[pd.DataFrame, pd.DataFrame, list[BundleMetricSpec]]:
    """Return empty district/state landing score tables for a bundle."""
    district_columns = [
        "state_name",
        "district_name",
        "bundle_score",
        "available_metric_count",
        "__state_key",
        "__district_key",
        "score_band",
        "bundle_score_display",
        "district_rank",
        "district_count",
        "state_bundle_score",
        "state_rank",
        "state_count",
    ] + [normalized_metric_column(spec.slug) for spec in metric_specs]
    state_columns = [
        "state_name",
        "bundle_score",
        "__state_key",
        "score_band",
        "bundle_score_display",
        "state_rank",
        "state_count",
    ]
    return (
        pd.DataFrame(columns=district_columns),
        pd.DataFrame(columns=state_columns),
        list(metric_specs),
    )


def _bundle_scenario_period_options(
    bundle_domain: str,
    *,
    data_dir: Path,
) -> list[tuple[str, str]]:
    """Return available scenario-period pairs for one persisted landing composite."""
    composite_spec = get_composite_metric_for_bundle(bundle_domain)
    if composite_spec is None:
        return []

    source_paths = _resolve_metric_master_sources(composite_spec.composite_slug, data_dir=data_dir)
    if not source_paths:
        return []

    source_signature = master_source_signature(source_paths)
    return list(
        _load_metric_scenario_period_pairs_cached(
            composite_spec.composite_slug,
            source_signature,
            tuple(str(path) for path in source_paths),
        )
    )


def _sanitize_landing_context(session_state: MutableMapping[str, object], *, data_dir: Path) -> None:
    """Ensure landing bundle and scenario-period choices remain valid."""
    bundle_domains = _landing_bundle_domains()
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
    if focus_level not in {"india", "state", "district"}:
        set_landing_focus_india(session_state)
    elif focus_level == "india":
        session_state["landing_selected_state"] = None
        session_state["landing_selected_district"] = None
    elif focus_level == "state" and not selected_state:
        set_landing_focus_india(session_state)
    elif focus_level == "district" and (not selected_state or not selected_district):
        if selected_state:
            set_landing_focus_state(session_state, selected_state)
        else:
            set_landing_focus_india(session_state)



def _assemble_bundle_context(
    merged_frame: Optional[pd.DataFrame],
    *,
    metric_specs: Sequence[BundleMetricSpec],
) -> tuple[pd.DataFrame, pd.DataFrame, list[BundleMetricSpec]]:
    """Assemble district/state landing score tables from a merged metric frame."""
    if merged_frame is None or merged_frame.empty:
        return _build_empty_bundle_context(metric_specs)

    district_scores = compute_bundle_score_frame(
        merged_frame,
        metric_specs=metric_specs,
        id_columns=("state_name", "district_name"),
    )
    district_scores["__state_key"] = district_scores["state_name"].astype(str).map(alias)
    district_scores["__district_key"] = (
        district_scores["state_name"].astype(str).map(alias)
        + "|"
        + district_scores["district_name"].astype(str).map(alias)
    )
    district_scores["score_band"] = district_scores["bundle_score"].map(_score_band)
    district_scores["bundle_score_display"] = district_scores["bundle_score"].map(_format_score)

    state_scores = aggregate_state_bundle_scores(
        district_scores,
        state_col="state_name",
        score_col="bundle_score",
    )
    state_scores["__state_key"] = state_scores["state_name"].astype(str).map(alias)
    state_scores["score_band"] = state_scores["bundle_score"].map(_score_band)
    state_scores["bundle_score_display"] = state_scores["bundle_score"].map(_format_score)

    state_scores["state_rank"] = (
        state_scores["bundle_score"]
        .rank(method="min", ascending=False, na_option="bottom")
        .where(state_scores["bundle_score"].notna())
    )
    n_states = int(pd.to_numeric(state_scores["bundle_score"], errors="coerce").notna().sum())
    state_scores["state_count"] = n_states

    district_scores["district_rank"] = (
        district_scores.groupby("state_name", dropna=False)["bundle_score"]
        .rank(method="min", ascending=False, na_option="bottom")
        .where(district_scores["bundle_score"].notna())
    )
    district_counts = (
        district_scores.groupby("state_name", dropna=False)["bundle_score"]
        .transform(lambda series: int(pd.to_numeric(series, errors="coerce").notna().sum()))
    )
    district_scores["district_count"] = district_counts

    state_lookup = state_scores[["state_name", "bundle_score", "state_rank", "state_count"]].rename(
        columns={
            "bundle_score": "state_bundle_score",
            "state_rank": "state_rank",
            "state_count": "state_count",
        }
    )
    district_scores = district_scores.merge(state_lookup, on="state_name", how="left")
    return district_scores, state_scores, list(metric_specs)


@st.cache_data(show_spinner=False)
def _prepare_bundle_context_cached(
    bundle_domain: str,
    scenario: str,
    period: str,
    stat: str,
    context_key: tuple[tuple[object, ...], ...],
) -> tuple[pd.DataFrame, pd.DataFrame, list[BundleMetricSpec]]:
    """Load, merge, and rank one validated landing bundle context."""
    _ = bundle_domain
    metric_specs = _metric_specs_from_context_key(context_key)
    if not metric_specs:
        return _build_empty_bundle_context(metric_specs)

    selected_pair = (str(scenario).strip(), canonical_period_label(str(period).strip()))
    parsed_entries = [_decode_context_key_entry(entry) for entry in context_key]
    if any(selected_pair not in set(entry.available_pairs) for entry in parsed_entries):
        return _build_empty_bundle_context(metric_specs)

    merged_frame: Optional[pd.DataFrame] = None
    for entry in parsed_entries:
        metric_frame = _load_metric_district_values_cached(
            entry.slug,
            scenario,
            period,
            stat,
            entry.source_signature,
            entry.source_paths,
        )
        if metric_frame.empty:
            continue

        metric_frame = metric_frame.rename(columns={"raw_metric_value": entry.slug})
        metric_frame = metric_frame[["state_name", "district_name", entry.slug]].copy()

        if merged_frame is None:
            merged_frame = metric_frame
        else:
            merged_frame = merged_frame.merge(
                metric_frame,
                on=["state_name", "district_name"],
                how="outer",
            )

    return _assemble_bundle_context(merged_frame, metric_specs=metric_specs)


def _prepare_bundle_context(
    bundle_domain: str,
    *,
    scenario: str,
    period: str,
    stat: str,
    data_dir: Path,
    metric_contexts: Optional[Sequence[LandingMetricContext]] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load persisted composite metric values and assemble landing score tables."""
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

    composite_spec = get_composite_metric_for_bundle(bundle_domain)
    if composite_spec is None:
        return _empty_context()

    source_paths = _resolve_metric_master_sources(composite_spec.composite_slug, data_dir=data_dir)
    if not source_paths:
        return _empty_context()

    source_signature = master_source_signature(source_paths)
    available_pairs = set(
        _load_metric_scenario_period_pairs_cached(
            composite_spec.composite_slug,
            source_signature,
            tuple(str(path) for path in source_paths),
        )
    )
    selected_pair = (str(scenario).strip(), canonical_period_label(str(period).strip()))
    if selected_pair not in available_pairs:
        return _empty_context()

    metric_frame = _load_metric_district_values_cached(
        composite_spec.composite_slug,
        scenario,
        period,
        stat,
        source_signature,
        tuple(str(path) for path in source_paths),
    )
    if metric_frame.empty:
        return _empty_context()

    district_scores = metric_frame.rename(columns={"raw_metric_value": "bundle_score"}).copy()
    district_scores["__state_key"] = district_scores["state_name"].astype(str).map(alias)
    district_scores["__district_key"] = (
        district_scores["state_name"].astype(str).map(alias)
        + "|"
        + district_scores["district_name"].astype(str).map(alias)
    )
    district_scores["score_band"] = district_scores["bundle_score"].map(_score_band)
    district_scores["bundle_score_display"] = district_scores["bundle_score"].map(_format_score)

    state_scores = aggregate_state_bundle_scores(district_scores)
    state_scores["__state_key"] = state_scores["state_name"].astype(str).map(alias)
    state_scores["score_band"] = state_scores["bundle_score"].map(_score_band)
    state_scores["bundle_score_display"] = state_scores["bundle_score"].map(_format_score)
    state_scores["state_rank"] = (
        state_scores["bundle_score"].rank(method="min", ascending=False, na_option="bottom")
        .where(state_scores["bundle_score"].notna())
    )
    state_count = int(pd.to_numeric(state_scores["bundle_score"], errors="coerce").notna().sum())
    state_scores["state_count"] = state_count

    district_scores["district_rank"] = (
        district_scores.groupby("state_name", dropna=False)["bundle_score"]
        .rank(method="min", ascending=False, na_option="bottom")
        .where(district_scores["bundle_score"].notna())
    )
    district_scores["district_count"] = (
        district_scores.groupby("state_name", dropna=False)["bundle_score"]
        .transform(lambda series: int(pd.to_numeric(series, errors="coerce").notna().sum()))
    )
    district_scores = district_scores.merge(
        state_scores[["state_name", "bundle_score", "state_rank", "state_count"]].rename(
            columns={"bundle_score": "state_bundle_score"}
        ),
        on="state_name",
        how="left",
    )
    return district_scores, state_scores


def _build_distribution_frame(score_series: pd.Series) -> pd.DataFrame:
    """Return a stable score-band distribution table for small summary charts."""
    categories = ["Low", "Moderate", "High", "Very High"]
    counts = {category: 0 for category in categories}
    for value in score_series.dropna():
        counts[_score_band(value)] = counts.get(_score_band(value), 0) + 1
    return pd.DataFrame(
        {
            "Band": categories,
            "Count": [counts.get(category, 0) for category in categories],
        }
    )


def _resolve_first_valid_landing_metric(
    bundle_domain: str,
    *,
    scenario: str,
    period: str,
    stat: str,
    data_dir: Path,
    metric_contexts: Optional[Sequence[LandingMetricContext]] = None,
) -> Optional[str]:
    """Return the first bundle metric with usable data for the current landing context."""
    contexts = list(metric_contexts) if metric_contexts is not None else _collect_bundle_metric_contexts(
        bundle_domain,
        data_dir=data_dir,
    )
    selected_pair = (str(scenario).strip(), canonical_period_label(str(period).strip()))

    for ctx in contexts:
        if selected_pair not in set(ctx.available_pairs):
            continue

        metric_frame = _load_metric_district_values_cached(
            ctx.spec.slug,
            scenario,
            period,
            stat,
            ctx.source_signature,
            ctx.source_paths,
        )
        raw_values = pd.to_numeric(
            metric_frame.get("raw_metric_value", pd.Series(dtype=float)),
            errors="coerce",
        )
        if raw_values.notna().any():
            return ctx.spec.slug

    return None


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
        for column in ("__state_key", "state_name", "__district_key", "district_name", "shapeName")
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


def _build_landing_map_artifacts(
    *,
    adm1: Any,
    adm2: Any,
    state_scores: pd.DataFrame,
    district_scores: pd.DataFrame,
    bundle_domain: str,
    scenario: str,
    period: str,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str],
) -> tuple[Any, Optional[str], str, pd.DataFrame]:
    """Build the landing Folium map, legend, and map label."""
    import folium

    map_label = _landing_map_label(
        bundle_domain=bundle_domain,
        scenario=scenario,
        period=period,
        focus_level=focus_level,
        selected_state=selected_state,
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
            "state_name",
            "shapeName",
            "district_name",
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
        reference_level="district" if reference_fc is not None else None,
        reference_layer_name="Selected district" if reference_fc is not None else None,
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
    return "state" if str(focus_level or "india").strip().lower() == "india" else "district"


def _landing_map_context_token(
    *,
    bundle_domain: str,
    scenario: str,
    period: str,
    focus_level: str,
    selected_state: Optional[str],
    selected_district: Optional[str],
) -> tuple[str, str, str, str, str, str, str]:
    """Return the canonical landing map context token for one rendered landing map."""
    return (
        alias(str(bundle_domain or "").strip()),
        str(scenario or "").strip().lower(),
        canonical_period_label(str(period or "").strip()),
        str(focus_level or "india").strip().lower(),
        alias(str(selected_state or "").strip()),
        alias(str(selected_district or "").strip()),
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
    context_token: tuple[str, str, str, str, str, str, str],
    payload_is_empty: bool,
) -> tuple[bool, bool]:
    """
    Synchronize the landing map settle gate for the current rendered context.

    Returns:
        `(input_armed, context_changed)` for the current render pass.
    """
    stored_context = session_state.get(LANDING_MAP_CONTEXT_KEY)
    normalized_stored: Optional[tuple[str, str, str, str, str, str, str]]
    if isinstance(stored_context, tuple) and len(stored_context) == 7:
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


def _apply_landing_map_click(
    *,
    focus_level: str,
    returned: Optional[Mapping[str, Any]],
    clicked_state: Optional[str],
    clicked_district: Optional[str],
    selected_state: Optional[str],
    selected_district: Optional[str],
    adm1: pd.DataFrame,
    adm2: pd.DataFrame,
    visible_districts: Optional[pd.DataFrame] = None,
) -> tuple[str, Optional[str], Optional[str]]:
    """
    Resolve a landing map click into a geography navigation action.

    Returns:
        A tuple of `(action, state_name, district_name)` where `action` is one of:
        `noop`, `focus_state`, or `focus_district`.
    """
    focus = str(focus_level or "india").strip().lower()
    current_state = str(selected_state or "").strip() or None
    current_district = str(selected_district or "").strip() or None
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
            return "focus_state", resolved_state, None
        return "noop", None, None

    if focus not in {"state", "district"}:
        return "noop", None, None

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
        return "noop", None, None

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
        return "noop", None, None
    if not _district_row_has_landing_score(resolved_row):
        return "noop", None, None
    resolved_state = str(resolved_row.get("state_name") or "").strip() or None
    district_name = str(resolved_row.get("district_name") or "").strip() or None
    if not resolved_state or not district_name:
        return "noop", None, None
    if not _district_exists(district_frame, resolved_state, district_name):
        return "noop", None, None
    if (
        focus == "district"
        and current_state
        and current_district
        and alias(resolved_state) == alias(current_state)
        and alias(district_name) == alias(current_district)
    ):
        return "noop", None, None
    return "focus_district", resolved_state, district_name


def _render_driver_table(driver_df: pd.DataFrame, *, top_n: int = 5) -> None:
    """Render a compact driver table for the landing drawer."""
    if driver_df.empty:
        st.caption("No driver detail is available for this scope.")
        return

    display_df = driver_df.head(top_n).copy()
    display_df["normalized_score"] = display_df["normalized_score"].map(lambda value: f"{float(value):.1f}")
    display_df = display_df.rename(
        columns={
            "metric_label": "Metric driver",
            "normalized_score": "Normalized score",
        }
    )
    st.dataframe(
        display_df[["Metric driver", "Normalized score"]],
        hide_index=True,
        use_container_width=True,
    )


def _render_national_summary(
    *,
    state_scores: pd.DataFrame,
    bundle_domain: str,
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
        dist_df = _build_distribution_frame(finite_scores["bundle_score"])
        st.bar_chart(dist_df.set_index("Band"))


def _render_state_summary(
    *,
    state_name: str,
    district_scores: pd.DataFrame,
    state_scores: pd.DataFrame,
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
        st.caption(f"Risk band: {_score_band(row.get('bundle_score'))}")

        hotspot_df = state_scope.sort_values("bundle_score", ascending=False, kind="stable").head(5)
        st.markdown("**Top Hotspot Districts**")
        for index, hotspot_row in enumerate(hotspot_df.itertuples(index=False), start=1):
            st.write(f"{index}. {hotspot_row.district_name}")

        st.markdown("**District Score Distribution**")
        st.bar_chart(_build_distribution_frame(state_scope["bundle_score"]).set_index("Band"))

        if st.button(
            "Deep Dive",
            key="landing_deep_dive_state",
            use_container_width=True,
            disabled=deep_dive_disabled,
        ):
            _enter_deep_dive(st.session_state)


def _render_district_summary(
    *,
    state_name: str,
    district_name: str,
    district_scores: pd.DataFrame,
    deep_dive_disabled: bool = False,
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
        st.metric(
            label=f"{_landing_bundle_display(str(st.session_state.get('landing_bundle') or LANDING_DEFAULT_BUNDLE))} bundle score",
            value=_format_score(row.get("bundle_score")),
        )
        st.caption(f"Risk band: {_score_band(row.get('bundle_score'))}")

        rank_value = row.get("district_rank")
        count_value = row.get("district_count")
        if pd.notna(rank_value) and pd.notna(count_value):
            st.caption(f"Rank within {state_name}: {int(rank_value)} / {int(count_value)}")

        state_mean = pd.to_numeric(state_scope["bundle_score"], errors="coerce").dropna().mean()
        if np.isfinite(state_mean):
            district_score = pd.to_numeric(pd.Series([row.get("bundle_score")]), errors="coerce").iloc[0]
            if np.isfinite(district_score):
                delta = float(district_score) - float(state_mean)
                st.caption(
                    f"Compared with the {state_name} average: {delta:+.1f} points "
                    f"(state average {state_mean:.1f})"
                )

        if st.button(
            "Deep Dive",
            key="landing_deep_dive_district",
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
) -> None:
    """Render context-sensitive landing rankings."""
    if focus_level == "india":
        scope_df = state_scores.sort_values("bundle_score", ascending=False, kind="stable").copy()
        scope_df["Rank"] = scope_df["bundle_score"].rank(method="min", ascending=False, na_option="bottom")
        display_df = scope_df.rename(
            columns={
                "state_name": "State",
                "bundle_score_display": "Bundle score",
                "score_band": "Risk band",
            }
        )
        st.dataframe(
            display_df[["Rank", "State", "Bundle score", "Risk band"]],
            hide_index=True,
            use_container_width=True,
        )
        return

    scope_df = district_scores[
        district_scores["state_name"].astype(str).map(alias) == alias(selected_state or "")
    ].sort_values("bundle_score", ascending=False, kind="stable")
    scope_df = scope_df.copy()
    scope_df["Current focus"] = scope_df["district_name"].map(
        lambda value: "Selected" if selected_district and alias(str(value)) == alias(selected_district) else ""
    )
    display_df = scope_df.rename(
        columns={
            "district_rank": "Rank",
            "district_name": "District",
            "bundle_score_display": "Bundle score",
            "score_band": "Risk band",
        }
    )
    st.dataframe(
        display_df[["Rank", "District", "Bundle score", "Risk band", "Current focus"]],
        hide_index=True,
        use_container_width=True,
    )


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
) -> None:
    """Render the lightweight landing compare view for the current geography scope."""
    if focus_level == "india":
        scope_df = state_scores.sort_values("bundle_score", ascending=False, kind="stable").copy()
        unit_column = "state_name"
        unit_label = "states"
        defaults = scope_df["state_name"].head(3).tolist()
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
    composite_spec = get_composite_metric_for_bundle(bundle_domain)
    if composite_spec is None:
        st.warning("Deep Dive is unavailable because this Glance bundle has no configured composite metric.")
        return

    handoff = build_deep_dive_handoff(
        session_state,
        bundle_domain=bundle_domain,
        metric_slug=composite_spec.composite_slug,
    )
    for key, value in handoff.items():
        session_state[key] = value
    st.rerun()


def render_landing_page(
    *,
    adm1: Any,
    adm2: Any,
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
    bundle_options = _landing_bundle_domains()
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
    search_options = _build_landing_search_options(state_scores, district_scores)
    if str(st.session_state.get("landing_tab") or LANDING_DEFAULT_TAB) not in LANDING_TABS:
        st.session_state["landing_tab"] = LANDING_DEFAULT_TAB
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
                f"**{_landing_map_label(bundle_domain=bundle_domain, scenario=scenario, period=period, focus_level=focus_level, selected_state=selected_state)}**"
            )

        landing_map, legend_html, _map_label, visible_map_gdf = _build_landing_map_artifacts(
            adm1=adm1,
            adm2=adm2,
            state_scores=state_scores,
            district_scores=district_scores,
            bundle_domain=bundle_domain,
            scenario=scenario,
            period=period,
            focus_level=focus_level,
            selected_state=selected_state,
            selected_district=selected_district,
        )

        returned, clicked_district, clicked_state = render_map_view(
            m=landing_map,
            variable_slug=f"landing_{alias(bundle_domain)}",
            map_mode="Bundle score",
            sel_scenario=scenario,
            sel_period=period,
            sel_stat=LANDING_SCORE_STAT,
            selected_state=selected_state or "All",
            selected_district=selected_district or "All",
            selected_block="All",
            selected_basin="All",
            selected_subbasin="All",
            map_width=780,
            map_height=520,
            legend_block_html=legend_html,
            level="state" if focus_level == "india" else "district",
            perf_section=None,
        )
        raw_returned = returned
        raw_clicked_district = clicked_district
        raw_clicked_state = clicked_state
        raw_payload_is_empty = _landing_map_payload_is_empty(raw_returned)
        map_context_token = _landing_map_context_token(
            bundle_domain=bundle_domain,
            scenario=scenario,
            period=period,
            focus_level=focus_level,
            selected_state=selected_state,
            selected_district=selected_district,
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
        ):
            returned = {}
            clicked_district = None
            clicked_state = None
        if not map_input_armed:
            returned = {}
            clicked_district = None
            clicked_state = None
        click_action, next_state, next_district = _apply_landing_map_click(
            focus_level=focus_level,
            returned=returned,
            clicked_state=clicked_state,
            clicked_district=clicked_district,
            selected_state=selected_state,
            selected_district=selected_district,
            adm1=adm1,
            adm2=adm2,
            visible_districts=visible_map_gdf if focus_level in {"state", "district"} else None,
        )
        rerun_reason = (
            "landing_map_click_transition"
            if click_action in {"focus_state", "focus_district"}
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
                        "raw_clicked_state": raw_clicked_state,
                        "raw_clicked_district": raw_clicked_district,
                        "click_action": click_action,
                        "next_state": next_state,
                        "next_district": next_district,
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
        ):
            st.rerun()
        if click_action == "noop" and raw_payload_is_empty:
            _clear_landing_pending_transition_token(st.session_state)

    with drawer_col:
        if focus_level == "india":
            _render_national_summary(
                state_scores=state_scores,
                bundle_domain=bundle_domain,
            )
        elif focus_level == "state" and selected_state:
                _render_state_summary(
                    state_name=selected_state,
                    district_scores=district_scores,
                    state_scores=state_scores,
                    deep_dive_disabled=not scenario_options,
                )
        elif focus_level == "district" and selected_state and selected_district:
                _render_district_summary(
                    state_name=selected_state,
                    district_name=selected_district,
                    district_scores=district_scores,
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
            state_scores=state_scores,
            district_scores=district_scores,
        )
    else:
        _render_landing_rankings(
            focus_level=focus_level,
            selected_state=selected_state,
            selected_district=selected_district,
            state_scores=state_scores,
            district_scores=district_scores,
        )

    method_note = (
        "Method note: landing bundle scores are weighted averages of normalized hazard metrics "
        "using approved bundle definitions. "
        "Only scenario-periods with full required bundle-metric coverage are shown. "
        "They are hazard summaries only, not resilience scores."
    )
    st.caption(method_note)

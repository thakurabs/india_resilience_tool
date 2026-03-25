"""
Crosswalk action and overlay helpers for the Streamlit app layer.

These helpers mutate session-state-like mappings but avoid importing Streamlit,
which keeps them easy to test.
"""

from __future__ import annotations

from typing import Any, Mapping, MutableMapping, Sequence

from india_resilience_tool.data.crosswalks import CrosswalkContext


def clear_crosswalk_overlay(session_state: MutableMapping[str, Any]) -> None:
    """Clear any active related-units overlay state."""
    session_state["crosswalk_overlay"] = None


def set_crosswalk_overlay_from_context(
    session_state: MutableMapping[str, Any],
    *,
    context: CrosswalkContext,
    feature_keys: Sequence[str],
) -> None:
    """Activate a related-units overlay for the current crosswalk context."""
    scope_dimension: str | None = None
    scope_values: tuple[str, ...] = ()
    if context.counterpart_level in {"basin", "sub_basin"}:
        basin_names = tuple(
            sorted(
                {
                    str(overlap.basin_name).strip()
                    for overlap in context.overlaps
                    if str(overlap.basin_name).strip()
                }
            )
        )
        if basin_names:
            scope_dimension = "basin_name"
            scope_values = basin_names
    elif context.counterpart_level in {"district", "block"}:
        state_names = tuple(
            sorted(
                {
                    str(overlap.counterpart_state_name).strip()
                    for overlap in context.overlaps
                    if str(overlap.counterpart_state_name).strip()
                }
            )
        )
        if state_names:
            scope_dimension = "state_name"
            scope_values = state_names

    session_state["crosswalk_overlay"] = {
        "level": context.counterpart_level,
        "feature_keys": [str(v) for v in feature_keys if str(v).strip()],
        "label": context.highlight_action_label.replace("Highlight ", ""),
        "source_direction": context.direction,
        "selected_name": context.selected_name,
        "scope_dimension": scope_dimension,
        "scope_values": list(scope_values),
    }


def navigate_from_crosswalk_overlap(
    session_state: MutableMapping[str, Any],
    *,
    context: CrosswalkContext,
    overlap: Mapping[str, Any],
) -> None:
    """Queue a counterpart navigation to be applied before widgets are created."""
    clear_crosswalk_overlay(session_state)

    counterpart_level = str(context.counterpart_level).strip().lower()
    counterpart_name = str(overlap.get("counterpart_name", "")).strip() or "All"
    counterpart_state = str(overlap.get("counterpart_state_name", "")).strip() or "All"
    counterpart_parent = str(overlap.get("counterpart_parent_name", "")).strip() or "All"
    basin_name = str(overlap.get("basin_name", "")).strip() or "All"

    if counterpart_level == "sub_basin":
        pending = {
            "spatial_family": "hydro",
            "admin_level": "sub_basin",
            "analysis_mode": "Single sub-basin focus",
            "selected_state": "All",
            "selected_district": "All",
            "selected_block": "All",
            "selected_basin": basin_name,
            "selected_subbasin": counterpart_name,
        }
    elif counterpart_level == "basin":
        pending = {
            "spatial_family": "hydro",
            "admin_level": "basin",
            "analysis_mode": "Single basin focus",
            "selected_state": "All",
            "selected_district": "All",
            "selected_block": "All",
            "selected_basin": counterpart_name,
            "selected_subbasin": "All",
        }
    elif counterpart_level == "block":
        pending = {
            "spatial_family": "admin",
            "admin_level": "block",
            "analysis_mode": "Single block focus",
            "selected_basin": "All",
            "selected_subbasin": "All",
            "selected_state": counterpart_state,
            "selected_district": counterpart_parent,
            "selected_block": counterpart_name,
        }
    else:
        pending = {
            "spatial_family": "admin",
            "admin_level": "district",
            "analysis_mode": "Single district focus",
            "selected_basin": "All",
            "selected_subbasin": "All",
            "selected_block": "All",
            "selected_state": counterpart_state,
            "selected_district": counterpart_name,
        }

    session_state["_pending_crosswalk_navigation"] = pending
    session_state["jump_to_map"] = True
    session_state["jump_to_rankings"] = False


def overlay_matches_context(
    overlay_spec: Mapping[str, Any] | None,
    *,
    context: CrosswalkContext,
) -> bool:
    """Return True when the active overlay corresponds to the visible crosswalk context."""
    if not overlay_spec:
        return False
    return (
        str(overlay_spec.get("level", "")).strip().lower() == str(context.counterpart_level).strip().lower()
        and str(overlay_spec.get("source_direction", "")).strip() == context.direction
        and str(overlay_spec.get("selected_name", "")).strip() == context.selected_name
    )

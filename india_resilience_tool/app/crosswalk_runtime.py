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
    overlay_level = "sub_basin" if context.direction == "district_to_subbasin" else "district"
    overlay_label = "Related sub-basins" if overlay_level == "sub_basin" else "Related districts"
    session_state["crosswalk_overlay"] = {
        "level": overlay_level,
        "feature_keys": [str(v) for v in feature_keys if str(v).strip()],
        "label": overlay_label,
        "source_direction": context.direction,
        "selected_name": context.selected_name,
    }


def navigate_from_crosswalk_overlap(
    session_state: MutableMapping[str, Any],
    *,
    context: CrosswalkContext,
    overlap: Mapping[str, Any],
) -> None:
    """Queue a counterpart navigation to be applied before widgets are created."""
    clear_crosswalk_overlay(session_state)

    if context.direction == "district_to_subbasin":
        session_state["_pending_crosswalk_navigation"] = {
            "spatial_family": "hydro",
            "admin_level": "sub_basin",
            "analysis_mode": "Single sub-basin focus",
            "selected_state": "All",
            "selected_district": "All",
            "selected_block": "All",
            "selected_basin": str(overlap.get("basin_name", "")).strip() or "All",
            "selected_subbasin": str(overlap.get("counterpart_name", "")).strip() or "All",
        }
    else:
        session_state["_pending_crosswalk_navigation"] = {
            "spatial_family": "admin",
            "admin_level": "district",
            "analysis_mode": "Single district focus",
            "selected_basin": "All",
            "selected_subbasin": "All",
            "selected_block": "All",
            "selected_state": str(overlap.get("counterpart_state_name", "")).strip() or "All",
            "selected_district": str(overlap.get("counterpart_name", "")).strip() or "All",
        }

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
    expected_level = "sub_basin" if context.direction == "district_to_subbasin" else "district"
    return (
        str(overlay_spec.get("level", "")).strip().lower() == expected_level
        and str(overlay_spec.get("source_direction", "")).strip() == context.direction
        and str(overlay_spec.get("selected_name", "")).strip() == context.selected_name
    )

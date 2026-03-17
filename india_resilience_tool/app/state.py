"""
Streamlit session state defaults and key registry for IRT.

Contract:
- Do NOT rename keys (widget keys are API).
- Do NOT override existing values; only set defaults when missing.
- Keep defaults aligned with the legacy dashboard.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Any, Literal, MutableMapping, Optional


VIEW_MAP = "Map view"
VIEW_RANKINGS = "Rankings table"

ANALYSIS_MODE_SINGLE = "Single district focus"
ANALYSIS_MODE_PORTFOLIO = "Multi-district portfolio"

# Spatial family constants
SPATIAL_FAMILY_ADMIN = "admin"
SPATIAL_FAMILY_HYDRO = "hydro"

# Administrative / spatial level constants
ADMIN_LEVEL_DISTRICT = "district"
ADMIN_LEVEL_BLOCK = "block"
ADMIN_LEVEL_BASIN = "basin"
ADMIN_LEVEL_SUB_BASIN = "sub_basin"

SpatialFamily = Literal["admin", "hydro"]
AdminLevel = Literal["district", "block", "basin", "sub_basin"]

SESSION_DEFAULTS: dict[str, Any] = {
    # Core mode/router keys
    "analysis_mode": ANALYSIS_MODE_SINGLE,
    "portfolio_districts": [],
    "portfolio_blocks": [],
    "portfolio_basins": [],
    "portfolio_subbasins": [],
    "portfolio_build_route": None,
    "jump_to_rankings": False,
    "jump_to_map": False,
    "active_view": VIEW_MAP,
    "main_view_selector": VIEW_MAP,

    # Main layout (right panel)
    "right_panel_collapsed": False,

    # Administrative level (NEW)
    "spatial_family": SPATIAL_FAMILY_ADMIN,
    "admin_level": ADMIN_LEVEL_DISTRICT,
    "selected_block": "All",  # For block-level selection
    "selected_basin": "All",
    "selected_subbasin": "All",
    "hydro_admin_context_level": "district",
    "show_river_network": False,

    # Other stable keys (widget keys / caches)
    # NOTE: Do NOT pre-seed unified metric selection keys here. The legacy dashboard
    # sets them dynamically based on VARIABLES; pre-seeding can block that defaulting.
    # Keys managed dynamically: 
    #   - selected_pillar, selected_bundle, selected_var, selected_index_group (legacy) - sidebar
    #   - portfolio_bundle_selection, portfolio_manual_refinement - portfolio panel
    "hover_enabled": True,
    "portfolio_multiindex_selection": [],

    # Perf timing
    "perf_enabled": False,
    "_perf_records": [],

    # Mtime caches
    "_master_cache": {},
    "_merged_cache": {},
    "_portfolio_master_cache": {},
    "_adm3_cache": None,  # NEW: block boundary cache
    "crosswalk_overlay": None,
    "_pending_crosswalk_navigation": None,
}


def ensure_session_state(
    session_state: Optional[MutableMapping[str, Any]] = None,
    *,
    perf_default: Optional[bool] = None,
) -> None:
    """
    Ensure all known IRT session_state keys exist with correct defaults.

    Args:
        session_state: Mapping to populate; if None, uses st.session_state.
        perf_default: Optional override for perf_enabled default. If None, keeps SESSION_DEFAULTS value.
    """
    if session_state is None:
        import streamlit as st

        session_state = st.session_state

    for k, v in SESSION_DEFAULTS.items():
        if k not in session_state:
            # Avoid sharing mutable defaults across sessions
            if isinstance(v, list):
                session_state[k] = list(v)
            elif isinstance(v, dict):
                session_state[k] = dict(v)
            else:
                session_state[k] = v

    # If these keys already exist but are empty, remove them so the legacy dashboard
    # can set its own deterministic defaults (prevents VARIABLES[None] KeyError).
    for k in ("selected_var", "selected_index_group", "registry_metric"):
        if k in session_state:
            val = session_state.get(k)
            if val is None or (isinstance(val, str) and val.strip() == ""):
                session_state.pop(k, None)

    # Allow perf_default override without clobbering user choice
    if perf_default is not None and "perf_enabled" not in session_state:
        session_state["perf_enabled"] = bool(perf_default)


# -----------------------------------------------------------------------------
# Level-aware helpers (NEW)
# -----------------------------------------------------------------------------

def get_current_level(session_state: Optional[MutableMapping[str, Any]] = None) -> AdminLevel:
    """Get the current administrative level from session state."""
    if session_state is None:
        import streamlit as st
        session_state = st.session_state
    return session_state.get("admin_level", ADMIN_LEVEL_DISTRICT)


def set_level(
    session_state: Optional[MutableMapping[str, Any]] = None,
    level: AdminLevel = ADMIN_LEVEL_DISTRICT,
) -> None:
    """
    Set the administrative level and reset dependent state.
    
    When switching levels, we need to reset:
    - Selected district/block
    - Portfolio (since items are level-specific)
    - Cached merged data
    """
    if session_state is None:
        import streamlit as st
        session_state = st.session_state
    
    old_level = session_state.get("admin_level", ADMIN_LEVEL_DISTRICT)
    
    if old_level != level:
        session_state["admin_level"] = level
        session_state["selected_district"] = "All"
        session_state["selected_block"] = "All"
        session_state["selected_basin"] = "All"
        session_state["selected_subbasin"] = "All"
        session_state["hydro_admin_context_level"] = "district"
        
        # Clear portfolio when switching levels (level-specific lists)
        session_state["portfolio_districts"] = []
        session_state["portfolio_blocks"] = []
        session_state["portfolio_basins"] = []
        session_state["portfolio_subbasins"] = []
        session_state["portfolio_multiindex_df"] = None
        session_state["portfolio_multiindex_context"] = None
        
        # Clear merged cache
        session_state["_merged_cache"] = {}


def get_unit_selection_key(level: AdminLevel) -> str:
    """Get the session state key for unit selection based on level."""
    if level == "sub_basin":
        return "selected_subbasin"
    if level == "basin":
        return "selected_basin"
    return "selected_block" if level == "block" else "selected_district"


def get_level_display_name(level: AdminLevel) -> str:
    """Get display name for a level."""
    if level == "sub_basin":
        return "Sub-basin"
    if level == "basin":
        return "Basin"
    return "Block" if level == "block" else "District"


def get_level_display_name_plural(level: AdminLevel) -> str:
    """Get plural display name for a level."""
    if level == "sub_basin":
        return "Sub-basins"
    if level == "basin":
        return "Basins"
    return "Blocks" if level == "block" else "Districts"

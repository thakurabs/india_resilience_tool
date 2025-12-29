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

from typing import Any, MutableMapping, Optional


VIEW_MAP = "🗺 Map view"
VIEW_RANKINGS = "📊 Rankings table"

ANALYSIS_MODE_SINGLE = "Single district focus"
ANALYSIS_MODE_PORTFOLIO = "Multi-district portfolio"

SESSION_DEFAULTS: dict[str, Any] = {
    # Core mode/router keys
    "analysis_mode": ANALYSIS_MODE_SINGLE,
    "portfolio_districts": [],
    "portfolio_build_route": None,
    "jump_to_rankings": False,
    "jump_to_map": False,
    "active_view": VIEW_MAP,
    "main_view_selector": VIEW_MAP,

    # Other stable keys (widget keys / caches)
    # NOTE: Do NOT pre-seed unified metric selection keys here. The legacy dashboard
    # sets them dynamically based on VARIABLES; pre-seeding can block that defaulting.
    "hover_enabled": True,
    "portfolio_multiindex_selection": [],

    # Perf timing
    "perf_enabled": False,
    "_perf_records": [],

    # Mtime caches
    "_master_cache": {},
    "_merged_cache": {},
    "_portfolio_master_cache": {},
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
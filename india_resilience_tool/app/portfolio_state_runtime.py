"""
Portfolio state helpers (app-layer; Streamlit session_state).

This module keeps the dashboard runtime (`app/runtime.py`) smaller by
centralizing the small wrapper functions that adapt the Streamlit session_state
to the Streamlit-free portfolio helpers in `india_resilience_tool.analysis.portfolio`.
"""

from __future__ import annotations

from typing import Optional

import streamlit as st

from india_resilience_tool.analysis.portfolio import (
    portfolio_add as _portfolio_add_impl,
    portfolio_clear as _portfolio_clear_impl,
    portfolio_contains as _portfolio_contains_impl,
    portfolio_normalize as _portfolio_normalize_impl,
    portfolio_remove as _portfolio_remove_impl,
)
from india_resilience_tool.utils.naming import alias


def _portfolio_normalize(text: str) -> str:
    """
    Normalize a state/district/block name for robust comparison across sources.

    Delegates to `analysis.portfolio.portfolio_normalize` to keep logic centralized.
    """
    return _portfolio_normalize_impl(text, alias_fn=alias)


def _portfolio_state_key() -> str:
    """Return the active portfolio storage key based on the current admin level."""
    return "portfolio_blocks" if st.session_state.get("admin_level", "district") == "block" else "portfolio_districts"


def _portfolio_key(state_name: str, district_name: str, block_name: Optional[str] = None) -> tuple:
    """Return a normalized key tuple for district/block portfolio items."""
    if st.session_state.get("admin_level", "district") == "block":
        return (
            _portfolio_normalize(state_name),
            _portfolio_normalize(district_name),
            _portfolio_normalize(block_name or ""),
        )
    return (_portfolio_normalize(state_name), _portfolio_normalize(district_name))


def _portfolio_add(state_name: str, district_name: str, block_name: Optional[str] = None) -> None:
    """Add a unit (district or block) to the active portfolio."""
    level = st.session_state.get("admin_level", "district")
    state_key = _portfolio_state_key()
    _portfolio_add_impl(
        st.session_state,
        state_name,
        district_name,
        normalize_fn=_portfolio_normalize,
        block_name=block_name,
        level=level,
        state_key=state_key,
    )


def _portfolio_remove(state_name: str, district_name: str, block_name: Optional[str] = None) -> None:
    """Remove a unit (district or block) from the active portfolio."""
    level = st.session_state.get("admin_level", "district")
    state_key = _portfolio_state_key()
    _portfolio_remove_impl(
        st.session_state,
        state_name,
        district_name,
        normalize_fn=_portfolio_normalize,
        block_name=block_name,
        level=level,
        state_key=state_key,
    )


def _portfolio_contains(state_name: str, district_name: str, block_name: Optional[str] = None) -> bool:
    """Return True if the unit is already present in the active portfolio."""
    level = st.session_state.get("admin_level", "district")
    state_key = _portfolio_state_key()
    return bool(
        _portfolio_contains_impl(
            st.session_state,
            state_name,
            district_name,
            normalize_fn=_portfolio_normalize,
            block_name=block_name,
            level=level,
            state_key=state_key,
        )
    )


def _portfolio_clear() -> None:
    """Clear all units from the active portfolio (districts or blocks)."""
    level = st.session_state.get("admin_level", "district")
    state_key = _portfolio_state_key()
    _portfolio_clear_impl(
        st.session_state,
        level=level,
        state_key=state_key,
    )


def _portfolio_set_flash(message: str, level: str = "success") -> None:
    """Store a one-shot UI message to be rendered at the top of the right panel."""
    st.session_state["_portfolio_flash"] = {
        "message": str(message),
        "level": str(level or "success"),
    }

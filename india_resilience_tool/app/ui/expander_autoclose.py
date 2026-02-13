"""Helpers for Streamlit expander auto-collapse behavior."""

from __future__ import annotations

from collections.abc import Iterable
from numbers import Number
from typing import Any

import streamlit as st


def ensure_expander_state(expander_key: str, default_open: bool = True) -> None:
    """Initialize ``{expander_key}_open`` in session state when missing."""
    state_key = f"{expander_key}_open"
    if state_key not in st.session_state:
        st.session_state[state_key] = bool(default_open)


def register_widget_options(widget_key: str, options: Iterable[Any]) -> None:
    """Persist currently valid options for widgets with dynamic option lists."""
    st.session_state[f"_valid_options_{widget_key}"] = tuple(options)


def mark_expander_open(expander_key: str) -> None:
    """Keep an expander open when upstream selections invalidate downstream choices."""
    st.session_state[f"{expander_key}_open"] = True


def maybe_collapse_expander(expander_key: str, required_keys: list[str]) -> None:
    """Collapse expander when auto-collapse is enabled and all required keys are valid."""
    if not st.session_state.get("ui_auto_collapse_expanders", True):
        return

    if all(_is_value_valid_for_key(k, st.session_state.get(k)) for k in required_keys):
        st.session_state[f"{expander_key}_open"] = False


def _is_value_valid_for_key(key: str, value: Any) -> bool:
    if isinstance(value, str):
        if not value.strip():
            return False
    elif isinstance(value, (list, set, tuple, dict)):
        if len(value) == 0:
            return False
    elif isinstance(value, Number):
        # 0 is valid; only None is invalid (handled below)
        pass
    elif value is None:
        return False

    options_key = f"_valid_options_{key}"
    if options_key in st.session_state:
        options = st.session_state[options_key]
        if value not in options:
            return False

    return True

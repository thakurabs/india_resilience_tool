"""Reusable helpers for expander auto-collapse behavior in Streamlit."""

from __future__ import annotations

from collections.abc import Callable
from numbers import Number
from typing import Any

import streamlit as st


def ensure_expander_open_state(expander_open_key: str, default_open: bool = True) -> None:
    """Ensure an expander open-state key exists in session state."""
    if expander_open_key not in st.session_state:
        st.session_state[expander_open_key] = bool(default_open)


def ensure_touched_flag(flag_key: str) -> None:
    """Ensure a touched flag exists and defaults to ``False``."""
    if flag_key not in st.session_state:
        st.session_state[flag_key] = False


def is_value_valid(value: object) -> bool:
    """Return ``True`` when a value is considered complete for required controls."""
    if value is None:
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    if isinstance(value, Number):
        return True
    return True


def all_valid(required_value_keys: list[str]) -> bool:
    """Check that all required session-state keys contain valid values."""
    return all(is_value_valid(st.session_state.get(key)) for key in required_value_keys)


def all_touched(required_touched_keys: list[str]) -> bool:
    """Check that all required touched flags are ``True``."""
    return all(bool(st.session_state.get(key, False)) for key in required_touched_keys)


def maybe_collapse(
    expander_open_key: str,
    required_value_keys: list[str],
    required_touched_keys: list[str],
) -> None:
    """Collapse an expander only when auto-collapse is enabled and completion is met."""
    if not st.session_state.get("ui_auto_collapse_expanders", True):
        return
    if not all_valid(required_value_keys):
        return
    if not all_touched(required_touched_keys):
        return
    st.session_state[expander_open_key] = False


def wrap_on_change(
    *,
    touched_flag: str | None,
    callback: Callable[[], None] | None,
) -> Callable[[], None]:
    """Build an ``on_change`` callback that marks a touched flag before running callback."""

    def _wrapped() -> None:
        if touched_flag:
            st.session_state[touched_flag] = True
        if callback is not None:
            callback()

    return _wrapped


# Backward-compatible aliases used by in-flight branches.
ensure_expander_state = ensure_expander_open_state
maybe_collapse_expander = maybe_collapse


def mark_expander_open(expander_key: str) -> None:
    """Keep compatibility with existing call sites expecting expander key prefix."""
    st.session_state[f"{expander_key}_open"] = True

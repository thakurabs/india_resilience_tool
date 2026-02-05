"""Performance timing helpers for Streamlit dashboard flows.

Extracted from legacy dashboard implementation to support modular refactoring
while preserving existing session keys and UI behavior.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Optional
import time

import pandas as pd
import streamlit as st


def _perf_is_enabled() -> bool:
    """Return True if perf timing is enabled for this session."""
    return bool(st.session_state.get("perf_enabled", False))


def perf_reset() -> None:
    """Clear per-rerun performance records (call once near app start)."""
    if _perf_is_enabled():
        st.session_state["_perf_records"] = []


def perf_start(section: str) -> Optional[float]:
    """Start timing and return a token (start time)."""
    if not _perf_is_enabled():
        return None
    return time.perf_counter()


def perf_end(section: str, start: Optional[float]) -> None:
    """Stop timing for `section` using the token from perf_start()."""
    if start is None or not _perf_is_enabled():
        return
    elapsed = time.perf_counter() - start
    st.session_state.setdefault("_perf_records", []).append(
        {"section": section, "seconds": float(elapsed)}
    )


@contextmanager
def perf_section(section: str):
    """Context manager wrapper around perf_start/perf_end."""
    start = perf_start(section)
    try:
        yield
    finally:
        perf_end(section, start)


def render_perf_panel(container) -> None:
    """Render the timing table into a Streamlit container/placeholder."""
    if not _perf_is_enabled():
        return

    records = st.session_state.get("_perf_records", [])
    with container:
        with st.expander("⏱ Performance timings", expanded=False):
            if not records:
                st.caption("No timings recorded for this rerun yet.")
                return

            df_perf = pd.DataFrame(records)
            df_perf["ms"] = (df_perf["seconds"] * 1000.0).round(1)
            df_perf = df_perf.drop(columns=["seconds"])
            st.dataframe(df_perf, hide_index=True, use_container_width=True)
            st.caption(f"Total: {df_perf['ms'].sum():.1f} ms")


def render_perf_panel_safe() -> None:
    """Best-effort performance panel render.

    This makes the perf panel resilient to early `st.stop()` branches by
    rendering into a sidebar placeholder if available.
    """
    if not _perf_is_enabled():
        return

    placeholder = globals().get("perf_panel_placeholder")
    if placeholder is None:
        # Prefer the sidebar so the UI matches the developer control location.
        try:
            placeholder = st.sidebar.empty()
        except Exception:
            placeholder = st.empty()
        globals()["perf_panel_placeholder"] = placeholder

    render_perf_panel(placeholder)

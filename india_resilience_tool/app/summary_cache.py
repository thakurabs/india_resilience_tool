"""Streamlit cache wrappers for optional dashboard context summaries.

Keep Streamlit imports in the app layer only; data loaders remain Streamlit-free.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from india_resilience_tool.data.exposure_summary import load_admin_exposure_summary
from india_resilience_tool.data.hydro_summary import load_admin_hydro_summary


@st.cache_data(show_spinner=False)
def load_exposure_summary_cached(path_str: str, mtime: float) -> pd.DataFrame:
    """Cached app-layer wrapper for exposure summary parquet."""
    _ = mtime  # cache buster
    return load_admin_exposure_summary(path_str)


@st.cache_data(show_spinner=False)
def load_hydro_summary_cached(path_str: str, mtime: float) -> pd.DataFrame:
    """Cached app-layer wrapper for hydro summary parquet."""
    _ = mtime  # cache buster
    return load_admin_hydro_summary(path_str)

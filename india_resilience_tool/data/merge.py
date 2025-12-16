"""
Deterministic ADM2 ↔ master merge and cache helpers for IRT.

This module is Streamlit-free. Pass `session_state=st.session_state` from the app layer
to preserve caching semantics without importing streamlit here.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, MutableMapping, Optional, Union

import pandas as pd


PathLike = Union[str, Path]


def get_master_mtime(master_path: PathLike) -> Optional[float]:
    """
    Return master CSV mtime, or None if unavailable.
    """
    try:
        return Path(master_path).stat().st_mtime
    except Exception:
        return None


def restrict_adm2_to_master_states(
    adm2_df: pd.DataFrame,
    master_df: pd.DataFrame,
    *,
    adm2_state_col: str,
    master_state_col: str,
    alias_fn: Callable[[str], str],
) -> pd.DataFrame:
    """
    Restrict ADM2 to states that occur in the master CSV (after alias normalization).
    """
    if adm2_state_col not in adm2_df.columns or master_state_col not in master_df.columns:
        return adm2_df

    state_keys = master_df[master_state_col].astype(str).map(alias_fn)
    valid_states = set(state_keys.dropna().tolist())
    if not valid_states:
        return adm2_df

    mask = adm2_df[adm2_state_col].astype(str).map(alias_fn).isin(valid_states)
    return adm2_df[mask].copy()


def merge_adm2_with_master(
    adm2_df: pd.DataFrame,
    master_df: pd.DataFrame,
    *,
    alias_fn: Callable[[str], str],
    adm2_district_col: str = "district_name",
    master_district_col: str = "district",
    key_col: str = "__key",
    suffixes: tuple[str, str] = ("", "_csv"),
) -> pd.DataFrame:
    """
    Deterministic left merge using alias-normalized join keys.

    Notes:
      - Does not mutate inputs.
      - Drops join key column after merge (matches existing dashboard behavior).
    """
    adm2c = adm2_df.copy()
    dfc = master_df.copy()

    if key_col not in adm2c.columns:
        adm2c[key_col] = adm2c[adm2_district_col].astype(str).map(alias_fn)
    dfc[key_col] = dfc[master_district_col].astype(str).map(alias_fn)

    merged = adm2c.merge(dfc, on=key_col, how="left", suffixes=suffixes).drop(columns=[key_col])
    return merged


def get_or_build_merged_for_index_cached(
    adm2_df: pd.DataFrame,
    master_df: pd.DataFrame,
    *,
    slug: str,
    master_path: PathLike,
    session_state: MutableMapping,
    alias_fn: Callable[[str], str],
    adm2_state_col: str = "state_name",
    master_state_col: str = "state",
) -> pd.DataFrame:
    """
    Cache merged result by master mtime in session_state["_merged_cache"][slug].

    Contract:
      - Cache key is (slug, master_mtime)
      - Stored under session_state["_merged_cache"]
      - Restricts ADM2 to states present in master (if columns exist)
      - Deterministic join using alias-normalized district keys
    """
    merged_cache = session_state.setdefault("_merged_cache", {})

    mtime = get_master_mtime(master_path)
    cache_entry = merged_cache.get(slug)

    if cache_entry is not None and cache_entry.get("mtime") == mtime:
        return cache_entry["gdf"]

    adm2c = restrict_adm2_to_master_states(
        adm2_df,
        master_df,
        adm2_state_col=adm2_state_col,
        master_state_col=master_state_col,
        alias_fn=alias_fn,
    )

    merged = merge_adm2_with_master(adm2c, master_df, alias_fn=alias_fn)

    merged_cache[slug] = {"mtime": mtime, "gdf": merged}
    return merged

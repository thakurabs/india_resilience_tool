"""
Deterministic boundary ↔ master merge and cache helpers for IRT.

This module supports both district (ADM2) and block (ADM3) level merging.
It is Streamlit-free. Pass `session_state=st.session_state` from the app layer
to preserve caching semantics without importing streamlit here.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Callable, Literal, MutableMapping, Optional

import pandas as pd

from india_resilience_tool.data.master_loader import MasterSourceLike, master_source_signature

AdminLevel = Literal["district", "block", "basin", "sub_basin"]


def get_master_mtime(master_path: MasterSourceLike) -> Optional[float]:
    """
    Return the latest master CSV mtime, or None if unavailable.
    """
    mtimes = [mtime for _path, mtime in master_source_signature(master_path) if mtime is not None]
    return max(mtimes) if mtimes else None


def restrict_boundaries_to_master_states(
    boundary_df: pd.DataFrame,
    master_df: pd.DataFrame,
    *,
    boundary_state_col: str,
    master_state_col: str,
    alias_fn: Callable[[str], str],
) -> pd.DataFrame:
    """
    Restrict boundary GeoDataFrame to states that occur in the master CSV 
    (after alias normalization).
    
    Works for both district (ADM2) and block (ADM3) boundaries.
    """
    if boundary_state_col not in boundary_df.columns or master_state_col not in master_df.columns:
        return boundary_df

    state_keys = master_df[master_state_col].astype(str).map(alias_fn)
    valid_states = set(state_keys.dropna().tolist())
    if not valid_states:
        return boundary_df

    mask = boundary_df[boundary_state_col].astype(str).map(alias_fn).isin(valid_states)
    return boundary_df[mask].copy()


# Backward-compatible alias
restrict_adm2_to_master_states = restrict_boundaries_to_master_states


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
    Deterministic left merge for districts using alias-normalized join keys.

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


def merge_adm3_with_master(
    adm3_df: pd.DataFrame,
    master_df: pd.DataFrame,
    *,
    alias_fn: Callable[[str], str],
    adm3_block_col: str = "block_name",
    adm3_district_col: str = "district_name",
    master_block_col: str = "block",
    master_district_col: str = "district",
    key_col: str = "__key",
    suffixes: tuple[str, str] = ("", "_csv"),
) -> pd.DataFrame:
    """
    Deterministic left merge for blocks using composite alias-normalized join keys.
    
    The join key is a composite of district|block to ensure uniqueness,
    since block names can repeat across different districts.

    Notes:
      - Does not mutate inputs.
      - Drops join key column after merge.
    """
    adm3c = adm3_df.copy()
    dfc = master_df.copy()

    # Build composite key: district|block (both normalized)
    if key_col not in adm3c.columns:
        adm3c[key_col] = (
            adm3c[adm3_district_col].astype(str).map(alias_fn) + 
            "|" + 
            adm3c[adm3_block_col].astype(str).map(alias_fn)
        )
    
    dfc[key_col] = (
        dfc[master_district_col].astype(str).map(alias_fn) + 
        "|" + 
        dfc[master_block_col].astype(str).map(alias_fn)
    )

    merged = adm3c.merge(dfc, on=key_col, how="left", suffixes=suffixes).drop(columns=[key_col])
    return merged


def merge_basin_with_master(
    basin_df: pd.DataFrame,
    master_df: pd.DataFrame,
    *,
    alias_fn: Callable[[str], str],
    basin_id_col: str = "basin_id",
    master_basin_id_col: str = "basin_id",
    key_col: str = "__key",
    suffixes: tuple[str, str] = ("", "_csv"),
) -> pd.DataFrame:
    """Deterministic left merge for basins using normalized basin IDs."""
    basinc = basin_df.copy()
    dfc = master_df.copy()

    if key_col not in basinc.columns:
        basinc[key_col] = basinc[basin_id_col].astype(str).map(alias_fn)
    dfc[key_col] = dfc[master_basin_id_col].astype(str).map(alias_fn)

    merged = basinc.merge(dfc, on=key_col, how="left", suffixes=suffixes).drop(columns=[key_col])
    return merged


def merge_subbasin_with_master(
    subbasin_df: pd.DataFrame,
    master_df: pd.DataFrame,
    *,
    alias_fn: Callable[[str], str],
    subbasin_id_col: str = "subbasin_id",
    master_subbasin_id_col: str = "subbasin_id",
    key_col: str = "__key",
    suffixes: tuple[str, str] = ("", "_csv"),
) -> pd.DataFrame:
    """Deterministic left merge for sub-basins using normalized sub-basin IDs."""
    subc = subbasin_df.copy()
    dfc = master_df.copy()

    if key_col not in subc.columns:
        subc[key_col] = subc[subbasin_id_col].astype(str).map(alias_fn)
    dfc[key_col] = dfc[master_subbasin_id_col].astype(str).map(alias_fn)

    merged = subc.merge(dfc, on=key_col, how="left", suffixes=suffixes).drop(columns=[key_col])
    return merged


def get_or_build_merged_for_index_cached(
    adm2_df: pd.DataFrame,
    master_df: pd.DataFrame,
    *,
    slug: str,
    master_path: MasterSourceLike,
    session_state: MutableMapping,
    alias_fn: Callable[[str], str],
    level: AdminLevel = "district",
    adm2_state_col: str = "state_name",
    master_state_col: str = "state",
) -> pd.DataFrame:
    """
    Cache merged result by master mtime in session_state["_merged_cache"][cache_key].

    Contract:
      - Cache key is (slug, level, master_mtime)
      - Stored under session_state["_merged_cache"]
      - Restricts boundaries to states present in master (if columns exist)
      - Deterministic join using alias-normalized keys
      - For blocks, uses composite district|block key
      - For hydro levels, uses canonical basin/sub-basin IDs
      
    Args:
        adm2_df: GeoDataFrame with boundary geometries (ADM2 or ADM3)
        master_df: Master metrics DataFrame
        slug: Index slug for cache key
        master_path: Path to master CSV (for mtime checking)
        session_state: Streamlit session state or dict-like
        alias_fn: Normalization function for names
        level: "district" or "block"
        adm2_state_col: State column name in boundary_df (kept for backward compat)
        master_state_col: State column name in master_df
        
    Returns:
        Merged GeoDataFrame
    """
    merged_cache = session_state.setdefault("_merged_cache", {})

    source_signature = master_source_signature(master_path)
    cache_key = (slug, level, tuple(path for path, _ in source_signature))
    cache_entry = merged_cache.get(cache_key)

    if cache_entry is not None and cache_entry.get("source_signature") == source_signature:
        return cache_entry["gdf"]

    # Restrict to states present in master
    boundary_c = restrict_boundaries_to_master_states(
        adm2_df,
        master_df,
        boundary_state_col=adm2_state_col,
        master_state_col=master_state_col,
        alias_fn=alias_fn,
    )

    # Merge based on level
    if level == "sub_basin":
        merged = merge_subbasin_with_master(boundary_c, master_df, alias_fn=alias_fn)
    elif level == "basin":
        merged = merge_basin_with_master(boundary_c, master_df, alias_fn=alias_fn)
    elif level == "block":
        merged = merge_adm3_with_master(boundary_c, master_df, alias_fn=alias_fn)
    else:
        merged = merge_adm2_with_master(boundary_c, master_df, alias_fn=alias_fn)

    merged_cache[cache_key] = {"source_signature": source_signature, "gdf": merged}
    return merged


def get_unit_name_column(level: AdminLevel) -> str:
    """Get the primary unit name column for a given level."""
    if level == "sub_basin":
        return "subbasin_name"
    if level == "basin":
        return "basin_name"
    return "block_name" if level == "block" else "district_name"

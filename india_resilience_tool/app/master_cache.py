"""
Master CSV + schema caching helpers for the Streamlit app.

These helpers cache expensive CSV reads/normalization/parsing in Streamlit
session_state keyed by metric slug and file mtime.
"""

from __future__ import annotations

from typing import Callable

import pandas as pd
import streamlit as st

from india_resilience_tool.data.master_loader import (
    MasterSourceLike,
    load_master_csvs,
    master_source_signature,
    normalize_master_columns,
    parse_master_schema,
)


def make_load_master_and_schema_fn(
    *,
    perf_section: Callable[..., object],
) -> Callable[[MasterSourceLike, str], tuple[pd.DataFrame, list[dict], list[str], dict]]:
    """
    Return a `(master_path, slug) -> (df, schema_items, metrics, by_metric)` callable.

    The returned callable uses `st.session_state["_master_cache"]` for caching.
    """

    def _load_master_and_schema(master_path: MasterSourceLike, slug: str):
        cache = st.session_state.setdefault("_master_cache", {})
        signature = master_source_signature(master_path)
        cache_key = (slug, tuple(path for path, _ in signature))

        entry = cache.get(cache_key)
        if entry is not None and entry.get("signature") == signature:
            return (
                entry["df"],
                entry["schema_items"],
                entry["metrics"],
                entry["by_metric"],
            )

        with perf_section("master: read csv"):
            with st.spinner("Loading master CSV..."):
                df_local = load_master_csvs(master_path)

        with perf_section("master: normalize columns"):
            df_local = normalize_master_columns(df_local)

        with perf_section("master: parse schema"):
            schema_items_local, metrics_local, by_metric_local = parse_master_schema(df_local.columns)

        cache[cache_key] = {
            "df": df_local,
            "schema_items": schema_items_local,
            "metrics": metrics_local,
            "by_metric": by_metric_local,
            "signature": signature,
        }
        return df_local, schema_items_local, metrics_local, by_metric_local

    return _load_master_and_schema

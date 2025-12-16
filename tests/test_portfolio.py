"""
Unit tests for analysis.portfolio helpers.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from india_resilience_tool.analysis.portfolio import (
    build_portfolio_multiindex_df,
    portfolio_add,
    portfolio_clear,
    portfolio_contains,
    portfolio_normalize,
    portfolio_remove,
)


def _alias(s: str) -> str:
    return str(s).strip().lower()


def _norm(s: str) -> str:
    return portfolio_normalize(s, alias_fn=_alias)


def test_portfolio_add_remove_contains_clear() -> None:
    ss: dict[str, Any] = {}

    assert not portfolio_contains(ss, "Telangana", "Mancherial", normalize_fn=_norm)

    portfolio_add(ss, "Telangana", "Mancherial", normalize_fn=_norm)
    assert portfolio_contains(ss, "Telangana", "Mancherial", normalize_fn=_norm)

    # De-dupe with spacing differences
    portfolio_add(ss, "Telangana", "Man che rial", normalize_fn=_norm)
    assert len(ss["portfolio_districts"]) == 1

    portfolio_remove(ss, "Telangana", "Mancherial", normalize_fn=_norm)
    assert not portfolio_contains(ss, "Telangana", "Mancherial", normalize_fn=_norm)

    portfolio_add(ss, "Telangana", "Mancherial", normalize_fn=_norm)
    portfolio_clear(ss)
    assert ss["portfolio_districts"] == []


def test_build_portfolio_multiindex_df_smoke() -> None:
    # Dummy 1-row master with "current" and "baseline"
    df_local = pd.DataFrame(
        {
            "state": ["Telangana"],
            "district": ["Alpha"],
            "m__ssp585__2020-2040__mean": [20.0],
            "m__historical__1985-2014__mean": [10.0],
        }
    )

    def _load(slug: str):
        return df_local, None, ["m"], None

    def _resolve(df: pd.DataFrame, metric: str, scenario: str, period: str, stat: str) -> Optional[str]:
        if metric == "m" and scenario == "ssp585":
            return "m__ssp585__2020-2040__mean"
        return None

    def _baseline(cols, metric: str, stat: str) -> Optional[str]:
        return "m__historical__1985-2014__mean"

    def _match(df: pd.DataFrame, st: str, dist: str) -> Optional[int]:
        return 0

    def _rankpct(**kwargs):
        return 1, 90.0

    def _risk(p: float) -> str:
        return "Very High" if p >= 80 else "Other"

    variables = {"slug1": {"label": "Metric One", "group": "temperature", "periods_metric_col": "m"}}
    group_labels = {"temperature": "Temperature"}

    out = build_portfolio_multiindex_df(
        portfolio=[{"state": "Telangana", "district": "Alpha"}],
        selected_slugs=["slug1"],
        variables=variables,
        index_group_labels=group_labels,
        sel_scenario="ssp585",
        sel_period="2020-2040",
        sel_stat="mean",
        load_master_and_schema_for_slug=_load,
        resolve_metric_column=_resolve,
        find_baseline_column_for_stat=_baseline,
        match_row_idx=_match,
        compute_rank_and_percentile=_rankpct,
        risk_class_from_percentile=_risk,
        normalize_fn=_norm,
    )

    assert out.shape[0] == 1
    assert out.loc[0, "Current value"] == 20.0
    assert out.loc[0, "Baseline"] == 10.0
    assert out.loc[0, "Δ"] == 10.0
    assert out.loc[0, "Risk class"] == "Very High"

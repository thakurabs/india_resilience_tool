"""
Tests for the precomputed state-values read path and the build tool's
value-column parsing.

The value/baseline rows are parity with the live area-weighted mean; the
all-states table additionally enables a real Position-in-India rank. These tests
cover the pure resolution/parse logic (no optimized bundle required).

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import math

import pandas as pd

from india_resilience_tool.app.views.state_summary_view import (
    _precomputed_state_value_map,
)
from tools.optimized.build_state_values import _parse_value_columns


def _values_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            # selected metric/scenario/period/stat across three states
            {"state": "Telangana", "metric": "hot_days", "scenario": "ssp245",
             "period": "2021-2040", "stat": "mean", "value": 30.0, "n_units": 33},
            {"state": "Kerala", "metric": "hot_days", "scenario": "ssp245",
             "period": "2021-2040", "stat": "mean", "value": 10.0, "n_units": 14},
            # present-but-null (coverage cliff)
            {"state": "Rajasthan", "metric": "hot_days", "scenario": "ssp245",
             "period": "2021-2040", "stat": "mean", "value": None, "n_units": 0},
        ]
    )


def test_resolver_returns_value_and_matched_states() -> None:
    df = _values_frame()
    value_map, matched = _precomputed_state_value_map(
        df, base_metric="hot_days", scenario="ssp245", period="2021-2040", stat="mean"
    )
    assert value_map == {"telangana": 30.0, "kerala": 10.0}
    # Rajasthan matched the key even though its value is null.
    assert matched == {"telangana", "kerala", "rajasthan"}


def test_resolver_present_but_null_is_matched_but_absent_from_map() -> None:
    df = _values_frame()
    value_map, matched = _precomputed_state_value_map(
        df, base_metric="hot_days", scenario="ssp245", period="2021-2040", stat="mean"
    )
    assert "rajasthan" in matched
    assert "rajasthan" not in value_map  # so caller renders N/A, not a live fallback


def test_resolver_absent_state_not_in_matched() -> None:
    df = _values_frame()
    _value_map, matched = _precomputed_state_value_map(
        df, base_metric="hot_days", scenario="ssp245", period="2021-2040", stat="mean"
    )
    assert "gujarat" not in matched  # caller falls back to the live value


def test_resolver_period_format_tolerant() -> None:
    df = _values_frame()
    value_map, matched = _precomputed_state_value_map(
        df, base_metric="hot_days", scenario="ssp245", period="2021_2040", stat="mean"
    )
    assert value_map["telangana"] == 30.0
    assert "telangana" in matched


def test_resolver_no_match_returns_empty() -> None:
    df = _values_frame()
    value_map, matched = _precomputed_state_value_map(
        df, base_metric="hot_days", scenario="ssp585", period="2021-2040", stat="mean"
    )
    assert value_map == {}
    assert matched == set()


def test_resolver_missing_columns_returns_empty() -> None:
    df = pd.DataFrame({"state": ["A"], "value": [1.0]})
    assert _precomputed_state_value_map(
        df, base_metric="hot_days", scenario="ssp245", period="2021-2040", stat="mean"
    ) == ({}, set())


def test_parse_value_columns_accepts_four_part_value_stats() -> None:
    cols = [
        "state",
        "district",
        "hot_days__ssp245__2021-2040__mean",
        "hot_days__historical__1990-2010__mean",
        "spi12__ssp585__2041-2060__p95",
        # composite/proposal score stats must be covered too (not excluded)
        "composite_heat__ssp245__2021-2040__score",
    ]
    parsed = dict(_parse_value_columns(cols))
    assert "hot_days__ssp245__2021-2040__mean" in parsed
    assert parsed["spi12__ssp585__2041-2060__p95"] == ("spi12", "ssp585", "2041-2060", "p95")
    assert parsed["composite_heat__ssp245__2021-2040__score"] == (
        "composite_heat",
        "ssp245",
        "2021-2040",
        "score",
    )
    # id columns (no "__") are skipped.
    assert "state" not in parsed
    assert "district" not in parsed


def test_parse_value_columns_skips_non_value_stats_and_malformed() -> None:
    cols = [
        "hot_days__ssp245__2021-2040__models",          # list companion -> skip
        "hot_days__ssp245__2021-2040__values_per_model",  # skip
        "hot_days__ssp245__2021-2040",                    # only 3 parts -> skip
        "hot_days____2021-2040__mean",                    # empty scenario part -> skip
    ]
    assert _parse_value_columns(cols) == []

"""
Tier-3: multi-state helpers for portfolio comparison.

These tests avoid importing Streamlit.
"""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.app.portfolio_multistate import (
    compute_portfolio_summary_stats,
    extract_states_in_portfolio,
)


def test_extract_states_in_portfolio_dedupes_and_uses_fallback() -> None:
    portfolio = [
        {"state": "Telangana", "district": "A"},
        {"state": "telangana", "district": "B"},
        ("Odisha", "C"),
        {"state": "All", "district": "X"},
        {"state": "", "district": "Y"},
    ]
    states = extract_states_in_portfolio(portfolio, fallback_state="Fallback")
    assert states == ["Telangana", "Odisha"]

    states2 = extract_states_in_portfolio([], fallback_state="Telangana")
    assert states2 == ["Telangana"]


def test_compute_portfolio_summary_stats_district() -> None:
    df = pd.DataFrame(
        {
            "State": ["S1", "S1", "S2"],
            "District": ["D1", "D2", "D3"],
            "Index": ["I1", "I1", "I2"],
            "Risk class": ["High", "Low", None],
        }
    )
    s = compute_portfolio_summary_stats(df, level="district")
    assert s["units_count"] == 3
    assert s["states_count"] == 2
    assert s["metrics_count"] == 2
    assert s["risk_counts"]["High"] == 1
    assert s["risk_counts"]["Low"] == 1
    assert s["risk_counts"]["Unknown"] == 1


def test_compute_portfolio_summary_stats_block() -> None:
    df = pd.DataFrame(
        {
            "State": ["S1", "S1", "S1"],
            "District": ["D1", "D1", "D1"],
            "Block": ["B1", "B2", "B2"],
            "Index": ["I1", "I1", "I2"],
            "Risk class": ["Medium", "Medium", "High"],
        }
    )
    s = compute_portfolio_summary_stats(df, level="block")
    assert s["units_count"] == 2  # (S1,D1,B1) and (S1,D1,B2)
    assert s["states_count"] == 1
    assert s["metrics_count"] == 2


def test_compute_portfolio_summary_stats_basin() -> None:
    df = pd.DataFrame(
        {
            "Basin": ["Godavari", "Krishna"],
            "Index": ["I1", "I2"],
            "Risk class": ["High", "Low"],
        }
    )
    s = compute_portfolio_summary_stats(df, level="basin")
    assert s["units_count"] == 2
    assert s["states_count"] == 2
    assert s["metrics_count"] == 2


def test_compute_portfolio_summary_stats_subbasin() -> None:
    df = pd.DataFrame(
        {
            "Basin": ["Godavari", "Godavari", "Krishna"],
            "Sub-basin": ["Pranhita", "Wardha", "Bhima"],
            "Index": ["I1", "I1", "I2"],
            "Risk class": ["High", "Medium", "Low"],
        }
    )
    s = compute_portfolio_summary_stats(df, level="sub_basin")
    assert s["units_count"] == 3
    assert s["states_count"] == 2
    assert s["metrics_count"] == 2

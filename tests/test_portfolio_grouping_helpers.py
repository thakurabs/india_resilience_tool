"""
Tests for portfolio grouping helpers used by the Streamlit UI.

These tests are intentionally Streamlit-free and operate on small synthetic inputs.
"""

from __future__ import annotations

from india_resilience_tool.app.portfolio_ui import (
    _group_portfolio_items_by_state,
    _group_portfolio_items_by_state_and_district,
)


def _norm(text: str) -> str:
    return str(text or "").strip().lower().replace(" ", "")


def test_group_portfolio_items_by_state_sorts_states_and_districts() -> None:
    items = [
        {"state": "B State", "district": "Zulu", "block": ""},
        {"state": "A State", "district": "beta", "block": ""},
        {"state": "A State", "district": "Alpha", "block": ""},
    ]

    grouped = _group_portfolio_items_by_state(items, normalize_fn=_norm)

    assert list(grouped.keys()) == ["A State", "B State"]
    assert [it["district"] for it in grouped["A State"]] == ["Alpha", "beta"]


def test_group_portfolio_items_by_state_and_district_sorts_nested() -> None:
    items = [
        {"state": "Telangana", "district": "Hanumakonda", "block": "KAMALAPUR"},
        {"state": "Telangana", "district": "Hanumakonda", "block": "ELKATHURTHI"},
        {"state": "Telangana", "district": "Nalgonda", "block": "ANUMULA"},
        {"state": "Andhra Pradesh", "district": "Vizianagaram", "block": "CHEEPURUPALLI"},
    ]

    grouped = _group_portfolio_items_by_state_and_district(items, normalize_fn=_norm)

    assert list(grouped.keys()) == ["Andhra Pradesh", "Telangana"]
    assert list(grouped["Telangana"].keys()) == ["Hanumakonda", "Nalgonda"]
    assert [it["block"] for it in grouped["Telangana"]["Hanumakonda"]] == ["ELKATHURTHI", "KAMALAPUR"]


def test_group_portfolio_items_by_state_and_district_empty() -> None:
    assert _group_portfolio_items_by_state_and_district([], normalize_fn=_norm) == {}


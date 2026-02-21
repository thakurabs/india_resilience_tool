"""
Tier-2: manage-portfolio list helpers (grouping and filtering).

These tests avoid importing Streamlit and exercise pure helper functions.
"""

from __future__ import annotations

from india_resilience_tool.app.portfolio_ui import (
    _filter_portfolio_items,
    _group_portfolio_items_by_state,
    _portfolio_items_to_dicts,
)


def _norm(s: str) -> str:
    return str(s or "").strip().lower().replace(" ", "")


def test_portfolio_items_to_dicts_filters_invalid_and_requires_block_in_block_mode() -> None:
    portfolio = [
        {"state": "Telangana", "district": "A"},
        {"state": "Telangana", "district": "A", "block": "X"},
        ("Telangana", "B", "Y"),
        ("", "C"),
    ]
    items_district = _portfolio_items_to_dicts(portfolio, is_block=False)
    assert any(it["district"] == "A" for it in items_district)

    items_block = _portfolio_items_to_dicts(portfolio, is_block=True)
    assert all(it.get("block") for it in items_block)


def test_filter_portfolio_items_matches_state_district_or_block() -> None:
    items = [
        {"state": "Telangana", "district": "A", "block": ""},
        {"state": "Odisha", "district": "B", "block": ""},
        {"state": "Telangana", "district": "A", "block": "X"},
    ]
    out = _filter_portfolio_items(items, query="odisha", normalize_fn=_norm)
    assert len(out) == 1
    assert out[0]["state"] == "Odisha"

    out2 = _filter_portfolio_items(items, query="x", normalize_fn=_norm)
    assert len(out2) == 1
    assert out2[0]["block"] == "X"


def test_group_portfolio_items_by_state_sorts_and_groups() -> None:
    items = [
        {"state": "BState", "district": "Z", "block": ""},
        {"state": "AState", "district": "B", "block": ""},
        {"state": "AState", "district": "A", "block": ""},
    ]
    grouped = _group_portfolio_items_by_state(items, normalize_fn=_norm)
    assert list(grouped.keys()) == ["AState", "BState"]
    assert [it["district"] for it in grouped["AState"]] == ["A", "B"]


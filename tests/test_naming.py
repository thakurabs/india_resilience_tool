"""
Unit tests for utils.naming.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.utils.naming import alias, normalize_compact, normalize_name


def test_normalize_name_basic() -> None:
    assert normalize_name(" Jayashankar-Bhupalpally ") == "jayashankar bhupalpally"
    assert normalize_name("Sanga_Reddy") == "sanga reddy"


def test_alias_applies_default_aliases() -> None:
    # from default mapping in naming.py
    assert alias("Hanamkonda") == "hanumakonda"


def test_normalize_compact_removes_spaces() -> None:
    assert normalize_compact("Sanga Reddy") == "sangareddy"


def test_maharashtra_block_and_district_spellings_reconcile() -> None:
    # CHG-0066: block-boundary vs district-boundary spellings for 6 Maharashtra
    # districts must compact to the same key so block<->district joins succeed.
    pairs = [
        ("Ahamadnagar", "AHMEDNAGAR"),
        ("Amaravati", "AMRAVATI"),
        ("Bid", "BEED"),
        ("Mumbai City", "MUMBAI"),
        ("Sub Urban Mumbai", "MUMBAI SUBURBAN"),
        ("Raygad", "RAIGARH"),
    ]
    for block_name, district_name in pairs:
        assert normalize_compact(block_name) == normalize_compact(district_name), (
            f"{block_name!r} did not reconcile with {district_name!r}"
        )

    # Distinct Mumbai districts must NOT collapse into each other.
    assert normalize_compact("Mumbai City") != normalize_compact("Sub Urban Mumbai")

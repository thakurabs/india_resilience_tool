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

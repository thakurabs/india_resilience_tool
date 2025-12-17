"""
Tests for map click extraction helper.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app.views.map_view import extract_clicked_district_state


def test_extract_clicked_district_state_from_properties() -> None:
    ret = {
        "last_object_clicked": {
            "properties": {
                "district_name": "Alpha",
                "state_name": "Telangana",
            }
        }
    }
    d, s = extract_clicked_district_state(ret)
    assert d == "Alpha"
    assert s == "Telangana"


def test_extract_clicked_district_state_none_safe() -> None:
    d, s = extract_clicked_district_state(None)
    assert d is None
    assert s is None

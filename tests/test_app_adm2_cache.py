"""
Tests for app.adm2_cache helpers.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from india_resilience_tool.app.adm2_cache import ensure_all_featurecollection


def test_ensure_all_featurecollection_adds_all() -> None:
    by_state = {
        "telangana": {"type": "FeatureCollection", "features": [{"type": "Feature"}]},
        "karnataka": {"type": "FeatureCollection", "features": [{"type": "Feature"}]},
    }
    out = ensure_all_featurecollection(by_state)
    assert "all" in out
    assert out["all"]["type"] == "FeatureCollection"
    assert len(out["all"]["features"]) == 2


def test_ensure_all_featurecollection_preserves_existing_all() -> None:
    by_state = {"all": {"type": "FeatureCollection", "features": [{"type": "Feature"}]}}
    out = ensure_all_featurecollection(by_state)
    assert out is by_state

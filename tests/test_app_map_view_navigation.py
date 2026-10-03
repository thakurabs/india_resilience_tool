"""
Tests for resolve_clicked_state_for_navigation (CHG-0279 §5a).

Regression guard for the click-navigation gap: a nationwide click payload that
carries only a duplicated district name must resolve its state from the click
coordinates, never from first-match ordering.
"""

from __future__ import annotations

import pytest

gpd = pytest.importorskip("geopandas")
shapely_geometry = pytest.importorskip("shapely.geometry")

from india_resilience_tool.app.views.map_view import resolve_clicked_state_for_navigation


def _norm(value: str) -> str:
    return str(value).strip().lower()


@pytest.fixture()
def merged_two_hamirpurs():
    """Two same-named districts in different states with disjoint polygons."""
    box = shapely_geometry.box
    return gpd.GeoDataFrame(
        {
            "district_name": ["Hamirpur", "Hamirpur"],
            "state_name": ["Uttar Pradesh", "Himachal Pradesh"],
        },
        geometry=[box(79.0, 25.0, 80.5, 26.5), box(76.0, 31.5, 77.0, 32.2)],
        crs="EPSG:4326",
    )


def _payload(lat: float, lon: float) -> dict:
    return {"last_clicked": {"lat": lat, "lng": lon}}


def test_click_inside_up_polygon_resolves_up(merged_two_hamirpurs) -> None:
    state = resolve_clicked_state_for_navigation(
        returned=_payload(25.8, 79.8),
        merged=merged_two_hamirpurs,
        level="district",
        clicked_district="Hamirpur",
        clicked_state=None,
        normalize_fn=_norm,
    )
    assert state == "Uttar Pradesh"


def test_click_inside_hp_polygon_resolves_hp(merged_two_hamirpurs) -> None:
    state = resolve_clicked_state_for_navigation(
        returned=_payload(31.8, 76.5),
        merged=merged_two_hamirpurs,
        level="district",
        clicked_district="Hamirpur",
        clicked_state="All",
        normalize_fn=_norm,
    )
    assert state == "Himachal Pradesh"


def test_payload_without_coordinates_returns_none(merged_two_hamirpurs) -> None:
    state = resolve_clicked_state_for_navigation(
        returned={"last_clicked": None},
        merged=merged_two_hamirpurs,
        level="district",
        clicked_district="Hamirpur",
        clicked_state=None,
        normalize_fn=_norm,
    )
    assert state is None


def test_already_set_state_returned_unchanged_without_geometry() -> None:
    # merged=None would break any geometry path; a concrete state short-circuits.
    state = resolve_clicked_state_for_navigation(
        returned=_payload(25.8, 79.8),
        merged=None,
        level="district",
        clicked_district="Hamirpur",
        clicked_state="Bihar",
        normalize_fn=_norm,
    )
    assert state == "Bihar"


def test_district_mismatch_under_normalize_returns_none(merged_two_hamirpurs) -> None:
    state = resolve_clicked_state_for_navigation(
        returned=_payload(25.8, 79.8),
        merged=merged_two_hamirpurs,
        level="district",
        clicked_district="Aurangabad",
        clicked_state=None,
        normalize_fn=_norm,
    )
    assert state is None

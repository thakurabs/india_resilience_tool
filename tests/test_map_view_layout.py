"""Tests for responsive map layout helpers."""

from india_resilience_tool.app.views.map_view import clamp_map_height


def test_clamp_map_height_enforces_minimum() -> None:
    """Available height below the minimum clamps upward."""
    assert clamp_map_height(300) == 420


def test_clamp_map_height_preserves_in_range_values() -> None:
    """Available height inside the supported range passes through."""
    assert clamp_map_height(560) == 560


def test_clamp_map_height_enforces_maximum() -> None:
    """Available height above the maximum clamps downward."""
    assert clamp_map_height(900) == 700

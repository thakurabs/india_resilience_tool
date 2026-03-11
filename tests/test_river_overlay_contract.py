from __future__ import annotations

from india_resilience_tool.data.river_loader import canonicalize_river_hydro_name


def test_basin_selector_normalizes_for_river_overlay_lookup() -> None:
    selected_basin = "Godavari Basin"
    selector_key = canonicalize_river_hydro_name(selected_basin).strip().lower()
    assert selector_key == "godavari"

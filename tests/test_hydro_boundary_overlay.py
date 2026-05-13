from __future__ import annotations

from pathlib import Path

import geopandas as gpd
from shapely.geometry import Polygon

from india_resilience_tool.app.hydro_boundary_overlay import (
    _candidate_boundary_paths,
    _read_boundary,
    _read_boundary_cached,
)
from india_resilience_tool.data.optimized_bundle import optimized_geometry_path


def test_candidate_boundary_paths_prefer_optimized_basin_path(tmp_path: Path) -> None:
    optimized_path = optimized_geometry_path(level="basin", data_dir=tmp_path)

    candidates = _candidate_boundary_paths(tmp_path, "basin")

    assert candidates[0] == optimized_path


def test_read_boundary_uses_cache_for_repeated_basin_lookup(tmp_path: Path) -> None:
    _read_boundary_cached.cache_clear()
    basin_path = optimized_geometry_path(level="basin", data_dir=tmp_path)
    basin_path.parent.mkdir(parents=True)
    basin_gdf = gpd.GeoDataFrame(
        {"basin_id": ["GODAVARI"], "basin_name": ["Godavari Basin"]},
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )
    basin_gdf.to_file(basin_path, driver="GeoJSON")
    active_overlay = {
        "hydro_level": "basin",
        "basin_id": "GODAVARI",
        "hydro_name": "Godavari Basin",
    }

    first = _read_boundary(tmp_path, active_overlay)
    second = _read_boundary(tmp_path, active_overlay)
    cache_info = _read_boundary_cached.cache_info()

    assert first is not None
    assert second is not None
    assert cache_info.misses == 1
    assert cache_info.hits == 1

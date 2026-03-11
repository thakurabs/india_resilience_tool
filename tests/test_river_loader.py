from __future__ import annotations

import geopandas as gpd
import pytest
from shapely.geometry import LineString, Polygon

from india_resilience_tool.data.river_loader import (
    canonicalize_river_hydro_name,
    ensure_river_display_columns,
    ensure_river_key_column,
    filter_rivers_for_basin,
    filter_rivers_for_subbasin,
)


def _river_gdf() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "river_feature_id": ["riv_001", "riv_002"],
            "source_uid_river": ["101", "102"],
            "river_name_clean": ["Pranhita", "Wardha"],
            "basin_name_clean": ["Godavari", "Godavari"],
            "subbasin_name_clean": ["Pranhita and others", "Wardha"],
            "state_names_clean": ["Telangana", "Maharashtra"],
            "length_km_source": [100.0, 50.0],
        },
        geometry=[
            LineString([(0, 0), (1, 1)]),
            LineString([(1, 1), (2, 2)]),
        ],
        crs="EPSG:4326",
    )


def test_river_loader_requires_canonical_display_columns() -> None:
    gdf = _river_gdf().drop(columns=["river_name_clean"])
    with pytest.raises(ValueError):
        ensure_river_display_columns(gdf)


def test_river_loader_rejects_non_line_geometry() -> None:
    gdf = _river_gdf()
    gdf.loc[0, "geometry"] = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    with pytest.raises(ValueError, match="non-line geometries"):
        ensure_river_display_columns(gdf)


def test_river_loader_adds_deterministic_keys() -> None:
    keyed = ensure_river_key_column(
        ensure_river_display_columns(_river_gdf()),
        alias_fn=lambda s: str(s).lower(),
    )
    assert keyed["__key"].tolist() == ["riv_001", "riv_002"]


def test_filter_rivers_for_basin_and_subbasin() -> None:
    gdf = ensure_river_display_columns(_river_gdf())
    basin_filtered = filter_rivers_for_basin(gdf, "Godavari", alias_fn=lambda s: str(s).lower())
    subbasin_filtered = filter_rivers_for_subbasin(
        gdf,
        "Wardha",
        alias_fn=lambda s: str(s).lower(),
    )
    assert basin_filtered["river_name_clean"].tolist() == ["Pranhita", "Wardha"]
    assert subbasin_filtered["river_name_clean"].tolist() == ["Wardha"]


def test_canonicalize_river_hydro_name_trims_basin_suffix() -> None:
    assert canonicalize_river_hydro_name("Godavari Basin") == "Godavari"
    assert canonicalize_river_hydro_name("Godavari") == "Godavari"

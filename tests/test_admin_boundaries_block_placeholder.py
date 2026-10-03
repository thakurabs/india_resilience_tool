"""Regression guard for CHG-0119.

A district whose only source block carries a blank ``block_name`` (e.g. the
Jammu & Kashmir / Mirpur PoK block) must survive into the district and state
layers via a synthesized block_name, instead of being dropped wholesale by the
invalid-identity filter. A blank *district* name must still be dropped.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
from shapely.geometry import Polygon

from tools.geodata.build_admin_boundaries_from_lgd import prepare_admin_boundaries


def _source(tmp_path: Path, rows: list[dict], geoms: list[Polygon]) -> Path:
    shp_path = tmp_path / "lgd_blocks.shp"
    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs="EPSG:4326")
    gdf.to_file(shp_path)
    return shp_path


def test_blank_block_name_is_recovered_via_placeholder(tmp_path: Path) -> None:
    # Two valid Jammu blocks + one single-block district (Mirpur) whose only
    # block has an empty block_name.
    rows = [
        {"state": "JAMMU & KASHMIR", "district": "Jammu", "block_name": "Jammu North", "dist_lgd": 5},
        {"state": "JAMMU & KASHMIR", "district": "Jammu", "block_name": "Jammu South", "dist_lgd": 5},
        {"state": "JAMMU & KASHMIR", "district": "Mirpur", "block_name": "", "dist_lgd": 0},
    ]
    geoms = [
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
        Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
        Polygon([(3, 0), (4, 0), (4, 1), (3, 1)]),
    ]
    blocks, districts, states, qa = prepare_admin_boundaries(_source(tmp_path, rows, geoms))

    assert qa["synthesized_block_names"] == 1

    # Mirpur survives as a district and as a (placeholder-named) block.
    assert "Mirpur" in set(districts["district_name"])
    mirpur_blocks = blocks.loc[blocks["district_name"] == "Mirpur"]
    assert len(mirpur_blocks) == 1
    assert mirpur_blocks["block_name"].iloc[0] == "Mirpur"  # placeholder == district name
    assert not mirpur_blocks.geometry.iloc[0].is_empty

    # The state dissolve includes Mirpur's geometry (it is no longer dropped).
    jk_state = states.loc[states["state_name"] == "Jammu & Kashmir"].geometry.iloc[0]
    assert jk_state.bounds[2] >= 4.0  # extends to Mirpur's eastern edge (x=4)


def test_blank_district_name_is_still_dropped(tmp_path: Path) -> None:
    # A valid state + blank district must NOT be rescued: synthesis only fires
    # when state_name and district_name are both valid.
    rows = [
        {"state": "JAMMU & KASHMIR", "district": "Jammu", "block_name": "Jammu North", "dist_lgd": 5},
        {"state": "JAMMU & KASHMIR", "district": "", "block_name": "", "dist_lgd": 0},
    ]
    geoms = [
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
        Polygon([(3, 0), (4, 0), (4, 1), (3, 1)]),
    ]
    blocks, districts, states, qa = prepare_admin_boundaries(_source(tmp_path, rows, geoms))

    assert qa["synthesized_block_names"] == 0
    assert set(districts["district_name"]) == {"Jammu"}

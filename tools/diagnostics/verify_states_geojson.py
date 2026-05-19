# tools/diagnostics/verify_states_geojson.py
from __future__ import annotations

import os
import sys
from pathlib import Path

from pyproj import datadir


def _configure_pyproj_data_dir() -> None:
    """Point pyproj at conda's PROJ database when its bundled path is unusable."""
    candidates = [os.environ.get("PROJ_DATA"), os.environ.get("PROJ_LIB")]
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix:
        candidates.append(str(Path(conda_prefix) / "Library" / "share" / "proj"))
        candidates.append(str(Path(conda_prefix) / "share" / "proj"))
    candidates.append(str(Path(sys.prefix) / "Library" / "share" / "proj"))
    candidates.append(str(Path(sys.prefix) / "share" / "proj"))

    for candidate in candidates:
        if not candidate:
            continue
        proj_db = Path(candidate) / "proj.db"
        if proj_db.exists():
            datadir.set_data_dir(str(proj_db.parent))
            return


_configure_pyproj_data_dir()


import geopandas as gpd  # noqa: E402  (must import after pyproj data-dir is set)
from shapely.ops import unary_union  # noqa: E402

from india_resilience_tool.data.adm2_loader import (  # noqa: E402
    ensure_adm2_columns,
    ensure_epsg4326,
)
from paths import DATA_DIR, DISTRICTS_PATH  # noqa: E402

STATES_PATH = DATA_DIR / "states_4326.geojson"

# Use an equal-area CRS for honest area math (EPSG:6933 is what the grid-first compute uses).
EQUAL_AREA_CRS = "EPSG:6933"


def main() -> int:
    states = gpd.read_file(STATES_PATH)
    districts = gpd.read_file(DISTRICTS_PATH)
    # Districts on disk use source column names; normalize to canonical state_name
    # (same normalization the build script applied before dissolving).
    districts = ensure_epsg4326(districts)
    districts = ensure_adm2_columns(districts)

    print(f"states:    {len(states)} features, crs={states.crs}")
    print(f"districts: {len(districts)} features, crs={districts.crs}")

    # --- Schema ---
    assert str(states.crs).upper().endswith("4326"), f"CRS not EPSG:4326: {states.crs}"
    for col in ("state_name", "shapeName", "geometry"):
        assert col in states.columns, f"missing column: {col}"
    dup = states["state_name"].duplicated().sum()
    assert dup == 0, f"duplicate state_name rows: {dup}"
    invalid = (~states.geometry.is_valid).sum()
    print(f"invalid geometries in states: {invalid}")

    # --- Name coverage parity ---
    state_names_in_states = set(states["state_name"].astype(str))
    state_names_in_districts = set(districts["state_name"].astype(str))
    missing_in_states = state_names_in_districts - state_names_in_states
    extra_in_states = state_names_in_states - state_names_in_districts
    print(f"states in districts but not in states file: {sorted(missing_in_states)}")
    print(f"states in states file but not in districts: {sorted(extra_in_states)}")
    assert not missing_in_states and not extra_in_states, "state-name set mismatch"

    # --- Total-area parity (equal-area CRS) ---
    s_ea = states.to_crs(EQUAL_AREA_CRS)
    d_ea = districts.to_crs(EQUAL_AREA_CRS)
    total_states_km2 = s_ea.geometry.area.sum() / 1e6
    total_districts_km2 = d_ea.geometry.area.sum() / 1e6
    diff_km2 = abs(total_states_km2 - total_districts_km2)
    print(f"total area states:    {total_states_km2:,.2f} km^2")
    print(f"total area districts: {total_districts_km2:,.2f} km^2")
    print(f"abs diff:             {diff_km2:,.4f} km^2")
    # Allow a few km^2 of float noise across ~3.3M km^2.
    assert diff_km2 < 10.0, "total-area mismatch — geometry lost or duplicated in dissolve"

    # --- Per-state boundary parity ---
    # Union of each state's districts must equal the state polygon.
    worst = []
    d_by_state = d_ea.groupby("state_name")
    s_by_state = s_ea.set_index("state_name")
    for name, group in d_by_state:
        union = unary_union(group.geometry.values)
        state_poly = s_by_state.loc[name, "geometry"]
        symdiff_km2 = union.symmetric_difference(state_poly).area / 1e6
        worst.append((symdiff_km2, name))
    worst.sort(reverse=True)
    print("\nWorst 5 per-state symmetric-difference areas (km^2):")
    for area_km2, name in worst[:5]:
        print(f"  {name:30s}  {area_km2:.6f}")
    # Each per-state sym-diff should be effectively zero (just float noise).
    worst_area, worst_name = worst[0]
    assert worst_area < 1.0, f"per-state boundary mismatch in {worst_name}: {worst_area:.4f} km^2"

    print("\nOK — states_4326.geojson is consistent with districts_4326.geojson.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
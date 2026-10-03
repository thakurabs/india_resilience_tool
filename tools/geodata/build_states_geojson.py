#!/usr/bin/env python3
"""
Build a full-fidelity ``states_4326.geojson`` by dissolving the canonical
``districts_4326.geojson``.

The output is a shareable companion to the district boundaries: state polygons
whose outer edges are built from the exact same vertex chains as the district
file, so overlaying the two produces no slivers or gaps.

This artifact is intentionally **unsimplified** and **not bbox-cropped** — it is
a source-of-truth handoff file, not a runtime asset. The dashboard continues to
use ``processed_optimised/geometry/admin/adm1.geojson`` (built by
``build_adm1_geojson.py``) for fast boot.

Output contract:
    - EPSG:4326
    - One feature per ``state_name``
    - Columns: ``state_name``, ``shapeName`` (mirror of state_name), ``geometry``
"""

from __future__ import annotations

import argparse
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
import shapely  # noqa: E402
from shapely.validation import make_valid  # noqa: E402

from india_resilience_tool.data.adm2_loader import (  # noqa: E402
    ensure_adm2_columns,
    ensure_epsg4326,
)
from paths import DATA_DIR, DISTRICTS_PATH  # noqa: E402


DEFAULT_OUT = DATA_DIR / "states_4326.geojson"

# Snap-rounding tolerance for the per-state dissolve. 1e-6 degrees ~ 10 cm at the
# equator — tight enough to preserve real boundary detail, loose enough to close
# digitisation slivers that otherwise survive as long thin interior holes in the
# merged state polygons. Pass --grid-size 0 to disable and reproduce the legacy
# (no-snap) behaviour.
DEFAULT_GRID_SIZE: float = 1e-6


def _filter_disputed_rows(adm2: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Split rows tagged as DISPUTED (or with no state_name) from real districts.

    The canonical districts file ships ~28 contested border slivers with
    ``state_name``/``district_name`` set to null and ``REMARKS`` like
    "DISPUTED (X & Y)". Keeping them would (a) create a phantom "None" state in
    the dissolve and (b) overlap real districts. They are filtered by default.
    """
    state_null = adm2["state_name"].isna() | (
        adm2["state_name"].astype(str).str.strip().str.lower().isin(
            {"none", "nan", "null", "unknown", ""}
        )
    )
    remarks_disputed = (
        adm2["REMARKS"].astype(str).str.strip().str.upper().str.startswith("DISPUTED")
        if "REMARKS" in adm2.columns
        else False
    )
    drop_mask = state_null | remarks_disputed
    return adm2.loc[~drop_mask].reset_index(drop=True), adm2.loc[drop_mask].reset_index(drop=True)


def _dissolve_with_grid_snap(
    adm2: gpd.GeoDataFrame, *, state_col: str, grid_size: float
) -> gpd.GeoDataFrame:
    """Per-state ``unary_union`` with snap-rounding to eliminate digitisation slivers."""
    kwargs = {"grid_size": grid_size} if grid_size and grid_size > 0 else {}
    records = []
    for name, group in adm2.groupby(state_col, sort=True):
        merged = shapely.unary_union(list(group.geometry.values), **kwargs)
        records.append({state_col: name, "geometry": merged})
    out = gpd.GeoDataFrame(records, geometry="geometry", crs=adm2.crs)
    out["shapeName"] = out[state_col]
    return out


def prepare_states_geojson(
    districts_path: Path,
    *,
    grid_size: float = DEFAULT_GRID_SIZE,
    keep_disputed: bool = False,
) -> gpd.GeoDataFrame:
    """Read ADM2 and dissolve to full-fidelity ADM1 state polygons.

    No simplification, no bbox crop. Invalid input geometries are repaired via
    ``shapely.validation.make_valid``, disputed-border slivers are filtered
    (unless ``keep_disputed`` is set), and the dissolve uses
    ``shapely.unary_union`` with ``grid_size`` snap-rounding so sub-millimetre
    misalignments between adjacent districts collapse into single edges instead
    of surviving as sliver-shaped interior holes in the merged state polygons.
    """
    adm2 = gpd.read_file(str(districts_path))
    adm2 = ensure_epsg4326(adm2)
    adm2 = ensure_adm2_columns(adm2)

    # Reassign vendor's catch-all "GUJARAT and DNH & DD ISLANDS" row to GUJARAT.
    # Investigation 2026-05-19 (see notebooks/inspect_states_geojson.ipynb):
    # this row is a 93-part MultiPolygon of Gujarati offshore islands (Bet
    # Dwarka, Khadir-Bela, Piram, Aliabet, etc.) tagged with STATE_LGD = 24
    # (Gujarat's LGD code). DD&DNH already has its own Diu, Daman, and Dadra &
    # Nagar Haveli districts, so no part of this bundle duplicates DD&DNH. Three
    # of the 93 parts are sub-hectare slivers ~1.5 km from Diu (total area
    # 0.006 km^2); they are kept with the rest under GUJARAT for rule
    # simplicity, since the 300 m distance margin is well below cartographic
    # precision.
    _island_label = "GUJARAT and DNH & DD ISLANDS"
    _island_mask = adm2["state_name"].eq(_island_label)
    if _island_mask.any():
        print(f"reassigning {int(_island_mask.sum())} '{_island_label}' row(s) to GUJARAT")
        adm2.loc[_island_mask, "state_name"] = "GUJARAT"
        adm2.loc[_island_mask, "district_name"] = "GUJARAT OFFSHORE ISLANDS"

    if keep_disputed:
        dropped = adm2.iloc[0:0]
    else:
        adm2, dropped = _filter_disputed_rows(adm2)
    if len(dropped):
        print(f"dropped {len(dropped)} disputed/orphan rows before dissolve")

    adm2["geometry"] = adm2.geometry.apply(make_valid)

    adm1 = _dissolve_with_grid_snap(adm2, state_col="state_name", grid_size=grid_size)

    keep_cols = [c for c in ("state_name", "shapeName") if c in adm1.columns]
    return adm1[[*keep_cols, "geometry"]].reset_index(drop=True)


def _write_geojson(gdf: gpd.GeoDataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing file without --overwrite: {path}"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        gdf.to_file(path, driver="GeoJSON", encoding="utf-8")
    except TypeError:
        gdf.to_file(path, driver="GeoJSON")


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a full-fidelity states_4326.geojson by dissolving "
            "districts_4326.geojson (no simplification, no bbox crop)."
        )
    )
    parser.add_argument(
        "--districts",
        type=str,
        default=str(DISTRICTS_PATH),
        help="Path to the source districts GeoJSON (ADM2).",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=str(DEFAULT_OUT),
        help="Output path for the states GeoJSON.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and print summary without writing the output.",
    )
    parser.add_argument(
        "--grid-size",
        type=float,
        default=DEFAULT_GRID_SIZE,
        help=(
            "Snap-rounding tolerance (degrees) for the per-state dissolve. "
            "Defaults to 1e-6 (~10 cm at the equator); pass 0 to disable and "
            "reproduce the legacy no-snap behaviour."
        ),
    )
    parser.add_argument(
        "--keep-disputed",
        action="store_true",
        help=(
            "Retain rows with null state_name or REMARKS starting with "
            "'DISPUTED' (default: drop them, since they create a phantom "
            "'None' state in the dissolved output)."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    districts_path = Path(args.districts).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    if not districts_path.exists():
        raise FileNotFoundError(f"Source districts GeoJSON not found: {districts_path}")

    gdf = prepare_states_geojson(
        districts_path,
        grid_size=float(args.grid_size),
        keep_disputed=bool(args.keep_disputed),
    )

    print("STATES GEOJSON (full fidelity)")
    print(f"source_districts: {districts_path}")
    print(f"feature_count: {len(gdf)}")
    print(f"unique_state_count: {gdf['state_name'].astype(str).nunique()}")
    print(f"crs: {gdf.crs}")
    print(f"out: {out_path}")

    if args.dry_run:
        print("dry-run: not writing output")
        return 0

    _write_geojson(gdf, out_path, overwrite=bool(args.overwrite))
    size_bytes = out_path.stat().st_size if out_path.exists() else 0
    print(f"wrote: {out_path} ({size_bytes / 1024:.1f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

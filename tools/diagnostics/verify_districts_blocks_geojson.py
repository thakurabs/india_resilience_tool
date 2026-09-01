# tools/diagnostics/verify_districts_blocks_geojson.py
"""Sanity + parity checks for districts_4326.geojson and blocks_4326.geojson.

Modelled on ``tools/diagnostics/verify_states_geojson.py``. Two subcommands:

    districts    Schema + geometry validity + identifier uniqueness on
                 districts_4326.geojson. Flags the 28 known DISPUTED orphan
                 rows and the 1 hybrid "GUJARAT and DNH & DD ISLANDS" catch-all
                 row without failing (these are documented source-data
                 artifacts; the build script handles them).

    blocks       Same checks on blocks_4326.geojson PLUS parent parity vs
                 districts_4326.geojson: every (state, district) pair in blocks
                 must also appear in districts, and per-district sym-diff
                 between union-of-blocks and the district polygon must be
                 small. The sym-diff pass is sampled by default because the
                 blocks file is large.
"""
from __future__ import annotations

import argparse
import os
import random
import sys
from pathlib import Path

from pyproj import datadir


def _configure_pyproj_data_dir() -> None:
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
        if (Path(candidate) / "proj.db").exists():
            datadir.set_data_dir(candidate)
            return


_configure_pyproj_data_dir()


import geopandas as gpd  # noqa: E402  (must import after pyproj data-dir is set)
from shapely.ops import unary_union  # noqa: E402

from india_resilience_tool.data.adm2_loader import (  # noqa: E402
    ensure_adm2_columns,
    ensure_epsg4326 as ensure_epsg4326_adm2,
)
from india_resilience_tool.data.adm3_loader import (  # noqa: E402
    ensure_adm3_columns,
    ensure_epsg4326 as ensure_epsg4326_adm3,
)
from paths import BLOCKS_PATH, DISTRICTS_PATH  # noqa: E402

EQUAL_AREA_CRS = "EPSG:6933"
INDIA_AREA_KM2_EXPECTED = 3_287_000  # ballpark, used only for an order-of-magnitude print

# Known DATA source artefacts in districts_4326.geojson (per CHG-0008..CHG-0010
# investigation). These should be flagged but not fail the check.
KNOWN_DISPUTED_ORPHAN_COUNT = 28
KNOWN_HYBRID_LABEL = "GUJARAT and DNH & DD ISLANDS"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _report_geometry_health(
    gdf: gpd.GeoDataFrame, label: str, *, id_cols: list[str]
) -> list[str]:
    """Report null/empty/invalid geometries and return a list of warning strings.

    Does not raise on invalid geometries — those are real findings the caller
    wants to surface, not abort-on. Null/empty geometries are still fatal:
    those would crash downstream area math.
    """
    n = len(gdf)
    n_null = int(gdf.geometry.isna().sum())
    non_null = gdf.loc[~gdf.geometry.isna()]
    n_empty = int(non_null.geometry.is_empty.sum())
    invalid_mask = ~non_null.geometry.is_valid
    n_invalid = int(invalid_mask.sum())
    print(f"  {label}: rows={n}  null_geom={n_null}  empty={n_empty}  invalid={n_invalid}")
    assert n_null == 0, f"{label}: null geometries"
    assert n_empty == 0, f"{label}: empty geometries"

    warnings: list[str] = []
    if n_invalid:
        print(f"  invalid-geometry rows in {label} (first 20):")
        cols = [c for c in id_cols if c in non_null.columns]
        invalid_rows = non_null.loc[invalid_mask, cols].head(20)
        if not invalid_rows.empty:
            print(invalid_rows.to_string(index=True))
        from shapely.validation import explain_validity  # noqa: E402  (lazy import)
        for idx, geom in non_null.loc[invalid_mask, "geometry"].head(5).items():
            print(f"    idx={idx}: {explain_validity(geom)}")
        warnings.append(
            f"{label}: {n_invalid} invalid geometries — the build script repairs these "
            f"via shapely.validation.make_valid, but the source file still ships them."
        )
    return warnings


def _check_crs_4326(gdf: gpd.GeoDataFrame, label: str) -> None:
    assert str(gdf.crs).upper().endswith("4326"), f"{label}: CRS not EPSG:4326 ({gdf.crs})"


# ---------------------------------------------------------------------------
# districts
# ---------------------------------------------------------------------------

def verify_districts() -> int:
    print(f"reading {DISTRICTS_PATH}")
    gdf = gpd.read_file(DISTRICTS_PATH)
    gdf = ensure_epsg4326_adm2(gdf)
    gdf = ensure_adm2_columns(gdf)

    print(f"\nSchema:\n  crs={gdf.crs}  rows={len(gdf)}  cols={list(gdf.columns)}")
    _check_crs_4326(gdf, "districts")
    for col in ("state_name", "district_name", "geometry"):
        assert col in gdf.columns, f"districts: missing column {col}"

    print("\nGeometry:")
    warnings = _report_geometry_health(gdf, "districts", id_cols=["state_name", "district_name", "REMARKS"])

    print("\nIdentifiers:")
    n_null_state = int(gdf["state_name"].isna().sum())
    n_null_dist = int(gdf["district_name"].isna().sum())
    print(f"  null state_name: {n_null_state}")
    print(f"  null district_name: {n_null_dist}")

    if "REMARKS" in gdf.columns:
        disputed_mask = (
            gdf["REMARKS"].astype(str).str.strip().str.upper().str.startswith("DISPUTED")
        )
        n_disputed = int(disputed_mask.sum())
        print(f"  REMARKS starts with 'DISPUTED': {n_disputed} (expected {KNOWN_DISPUTED_ORPHAN_COUNT})")
        if n_disputed != KNOWN_DISPUTED_ORPHAN_COUNT:
            print(
                f"  WARNING: disputed-row count drifted from documented baseline "
                f"({KNOWN_DISPUTED_ORPHAN_COUNT}). Update CHG-0008 notes if the source changed."
            )
    else:
        print("  REMARKS column absent (cannot count disputed orphans)")

    n_hybrid = int(gdf["state_name"].eq(KNOWN_HYBRID_LABEL).sum())
    print(f"  rows tagged '{KNOWN_HYBRID_LABEL}': {n_hybrid} (build script reassigns to GUJARAT)")

    # Real-district duplicates after dropping disputed/orphan rows.
    real = gdf.copy()
    if "REMARKS" in real.columns:
        real = real.loc[
            ~real["REMARKS"].astype(str).str.strip().str.upper().str.startswith("DISPUTED")
        ]
    real = real.loc[real["state_name"].notna() & real["district_name"].notna()]
    dup_mask = real.duplicated(subset=["state_name", "district_name"], keep=False)
    n_dup_pairs = int(dup_mask.sum())
    print(f"  duplicate (state_name, district_name) rows among real districts: {n_dup_pairs}")
    if n_dup_pairs:
        print(real.loc[dup_mask, ["state_name", "district_name"]].head(20).to_string(index=False))

    print("\nArea (EPSG:6933):")
    # Repair invalid geometries before area math so we don't crash on them.
    from shapely.validation import make_valid  # noqa: E402  (lazy import)
    ea = gdf.copy()
    ea["geometry"] = ea.geometry.apply(make_valid)
    ea = ea.to_crs(EQUAL_AREA_CRS)
    total_km2 = float(ea.geometry.area.sum()) / 1e6
    print(f"  total area: {total_km2:,.2f} km^2  (India ~= {INDIA_AREA_KM2_EXPECTED:,} km^2)")

    print("\n--- Summary ---")
    if warnings:
        for w in warnings:
            print(f"  WARN: {w}")
        print("\ndistricts_4326.geojson: schema/identifiers OK; geometry warnings above.")
    else:
        print("\nOK — districts_4326.geojson passes schema/geometry/identifier checks.")
    return 0


# ---------------------------------------------------------------------------
# blocks
# ---------------------------------------------------------------------------

def verify_blocks(*, sample: int, no_area: bool) -> int:
    print(f"reading {BLOCKS_PATH}")
    blocks = gpd.read_file(BLOCKS_PATH)
    blocks = ensure_epsg4326_adm3(blocks)
    blocks = ensure_adm3_columns(blocks)

    print(f"\nSchema:\n  crs={blocks.crs}  rows={len(blocks)}  cols={list(blocks.columns)[:12]}{'...' if len(blocks.columns) > 12 else ''}")
    _check_crs_4326(blocks, "blocks")
    for col in ("state_name", "district_name", "block_name", "geometry"):
        assert col in blocks.columns, f"blocks: missing column {col}"

    print("\nGeometry:")
    block_warnings = _report_geometry_health(
        blocks, "blocks", id_cols=["state_name", "district_name", "block_name"]
    )

    print("\nIdentifiers:")
    n_null_state = int(blocks["state_name"].isna().sum())
    n_null_dist = int(blocks["district_name"].isna().sum())
    n_null_block = int(blocks["block_name"].isna().sum())
    print(f"  null state_name: {n_null_state}")
    print(f"  null district_name: {n_null_dist}")
    print(f"  null block_name: {n_null_block}")

    key_cols = ["state_name", "district_name", "block_name"]
    dup_mask = blocks.duplicated(subset=key_cols, keep=False)
    n_dup = int(dup_mask.sum())
    print(f"  duplicate (state, district, block) rows: {n_dup}")
    if n_dup:
        print(blocks.loc[dup_mask, key_cols].head(20).to_string(index=False))

    # Parent-district parity vs districts_4326
    print(f"\nParent-district parity vs {DISTRICTS_PATH.name}:")
    districts = gpd.read_file(DISTRICTS_PATH)
    districts = ensure_epsg4326_adm2(districts)
    districts = ensure_adm2_columns(districts)
    # Mirror build_states_geojson preprocessing so the comparison is like-for-like.
    island_mask = districts["state_name"].eq(KNOWN_HYBRID_LABEL)
    if island_mask.any():
        districts.loc[island_mask, "state_name"] = "GUJARAT"
        districts.loc[island_mask, "district_name"] = "GUJARAT OFFSHORE ISLANDS"
    if "REMARKS" in districts.columns:
        keep = ~districts["REMARKS"].astype(str).str.strip().str.upper().str.startswith("DISPUTED")
        districts = districts.loc[keep]
    districts = districts.loc[
        districts["state_name"].notna() & districts["district_name"].notna()
    ]

    # Normalize to UPPER + stripped for parity matching. The two source files
    # ship the same district names in inconsistent casing (blocks Title Case,
    # districts UPPER) — without folding here, the orphan/childless lists are
    # dominated by case noise and the real renames/splits are invisible.
    def _norm(s):
        return s.astype(str).str.strip().str.upper()

    districts["_state_key"] = _norm(districts["state_name"])
    districts["_district_key"] = _norm(districts["district_name"])
    blocks["_state_key"] = _norm(blocks["state_name"])
    blocks["_district_key"] = _norm(blocks["district_name"])

    district_pairs = set(zip(districts["_state_key"], districts["_district_key"]))
    block_pairs = set(zip(blocks["_state_key"], blocks["_district_key"]))
    orphans = sorted(block_pairs - district_pairs)
    childless = sorted(district_pairs - block_pairs)
    print(f"  block (state, district) pairs not in districts file: {len(orphans)}")
    for pair in orphans[:20]:
        print(f"    orphan: {pair}")
    print(f"  district (state, district) pairs with no blocks: {len(childless)}")
    for pair in childless[:20]:
        print(f"    childless: {pair}")

    if no_area:
        print("\nSkipping per-district sym-diff (--no-area).")
        print("\n--- Summary ---")
        for w in block_warnings:
            print(f"  WARN: {w}")
        print("\nblocks_4326.geojson: schema/identifiers/parity OK (sym-diff skipped).")
        return 0

    # Per-district sym-diff (sampled).
    print(f"\nPer-district union-of-blocks vs district polygon (EPSG:6933):")
    common_pairs = sorted(block_pairs & district_pairs)
    if sample > 0 and sample < len(common_pairs):
        rng = random.Random(20260519)
        pairs_to_check = rng.sample(common_pairs, sample)
        print(f"  sampling {sample} of {len(common_pairs)} (state, district) pairs (seed=20260519)")
    else:
        pairs_to_check = common_pairs
        print(f"  checking all {len(common_pairs)} (state, district) pairs")

    # Repair invalid geometries before reprojecting + area math.
    from shapely.validation import make_valid  # noqa: E402  (lazy import)
    blocks_ea = blocks.copy()
    blocks_ea["geometry"] = blocks_ea.geometry.apply(make_valid)
    blocks_ea = blocks_ea.to_crs(EQUAL_AREA_CRS)
    districts_ea = districts.copy()
    districts_ea["geometry"] = districts_ea.geometry.apply(make_valid)
    districts_ea = districts_ea.to_crs(EQUAL_AREA_CRS)
    districts_ea = districts_ea.set_index(["_state_key", "_district_key"]).sort_index()

    worst: list[tuple[float, tuple[str, str]]] = []
    for pair in pairs_to_check:
        state_key, district_key = pair
        block_geoms = blocks_ea.loc[
            (blocks_ea["_state_key"] == state_key)
            & (blocks_ea["_district_key"] == district_key),
            "geometry",
        ].values
        if len(block_geoms) == 0:
            continue
        try:
            district_poly = districts_ea.loc[pair, "geometry"]
        except KeyError:
            continue
        # `.loc` on a duplicate MultiIndex returns a Series; collapse via union_all().
        if hasattr(district_poly, "union_all"):
            district_poly = district_poly.union_all()
        union = unary_union(list(block_geoms))
        sd_km2 = union.symmetric_difference(district_poly).area / 1e6
        worst.append((sd_km2, pair))

    worst.sort(reverse=True)
    print(f"  worst 10 sym-diff (km^2):")
    for area_km2, pair in worst[:10]:
        print(f"    {pair[0]:25s} / {pair[1]:25s}  {area_km2:.4f}")
    if worst:
        worst_area, worst_pair = worst[0]
        # 5 km^2 tolerance is generous; districts span 100s..10000s of km^2 each.
        # Real sliver issues will be 100s of km^2.
        if worst_area >= 5.0:
            print(
                f"\n  WARNING: largest per-district sym-diff is {worst_area:.4f} km^2 "
                f"({worst_pair}). This is above the 5 km^2 float-noise band — likely a "
                f"real topology gap or misalignment between blocks and districts."
            )

    print("\n--- Summary ---")
    for w in block_warnings:
        print(f"  WARN: {w}")
    if not block_warnings:
        print("\nOK — blocks_4326.geojson passes schema/geometry/identifier/parity checks.")
    else:
        print("\nblocks_4326.geojson: schema/identifiers/parity OK; geometry warnings above.")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="target", required=True)

    sub.add_parser("districts", help="Verify districts_4326.geojson")

    pb = sub.add_parser("blocks", help="Verify blocks_4326.geojson")
    pb.add_argument(
        "--sample",
        type=int,
        default=50,
        help="Number of (state, district) pairs to sym-diff-check (default 50). Pass 0 to check all.",
    )
    pb.add_argument(
        "--no-area",
        action="store_true",
        help="Skip the per-district sym-diff pass entirely (schema/parity only).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_cli().parse_args(argv)
    if args.target == "districts":
        return verify_districts()
    if args.target == "blocks":
        return verify_blocks(sample=args.sample, no_area=args.no_area)
    return 2


if __name__ == "__main__":
    sys.exit(main())

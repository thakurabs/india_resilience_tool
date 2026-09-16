#!/usr/bin/env python3
"""
Build consistent state/district/block boundary GeoJSONs from one LGD source.

This tool is the single source of truth for the IRT admin hierarchy. It reads
the bharatlas ``LGD_Blocks`` block shapefile (the only atomic layer) and derives
all three canonical boundary files so that they nest *exactly* by construction:

    blocks_4326.geojson    -- atomic blocks (one row per block)
    districts_4326.geojson -- dissolve of blocks by (state_name, district_name)
    states_4326.geojson    -- dissolve of districts by state_name

Because districts and states are unions of the same blocks, every block lies
inside its parent district and every district inside its parent state -- there
is no cross-layer name/roster/geometry mismatch to reconcile, and no alias or
crosswalk table is needed.

Key design choices
------------------
* District identity is keyed on the *name* (``state_name``, ``district_name``),
  not on ``dist_lgd``. Several ``dist_lgd`` codes map to multiple modern district
  names (e.g. the 2023 Rajasthan splits, where new districts still carry the
  parent's LGD code). Name keying preserves the current roster; the LGD code is
  retained only as a reference attribute (modal value per district).
* State names are canonicalized to the project's Title-Case convention via an
  explicit, exhaustive map. An unmapped source state is a hard error so the build
  can never silently emit a non-canonical name.
* District and block labels are run through the *same* repair the ADM3 loader
  applies at runtime (:func:`repair_adm3_identity_columns`, Title-Case with
  ``" And " -> " and "``). The ADM2 loader only strips, so storing the already
  title-cased form here keeps the district file and the block-derived district
  references identical at load time.

Source limitation
-----------------
The bharatlas file includes Arunachal Pradesh with real blocks, so unlike the
Survey-of-India subdistrict layer there is no district-only gap; all 36 states/UTs
carry blocks.

Usage
-----
    # Inspect rosters + QA without writing anything:
    python -m tools.geodata.build_admin_boundaries_from_lgd --dry-run

    # Write the three canonical files (refuses to clobber without --overwrite):
    python -m tools.geodata.build_admin_boundaries_from_lgd --overwrite
"""

from __future__ import annotations

import argparse
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd

from india_resilience_tool.data.adm3_loader import (
    collect_adm3_label_anomalies,
    ensure_epsg4326,
    repair_adm3_identity_columns,
)
from paths import BLOCKS_PATH, DISTRICTS_PATH, get_paths_config


# Equal-area CRS used project-wide for km^2 computations (matches the JRC and
# crosswalk builders' AREA_EPSG = 6933).
AREA_EPSG = 6933

_INVALID_ADMIN_VALUES = {"", "nan", "none", "null", "nat", "not available"}

# Source state spelling (bharatlas LGD_Blocks `state` field) -> project canonical
# Title-Case name. Exhaustive for the 36 states/UTs present in the source; an
# unmapped value raises so the build never emits a non-canonical name.
_RAW_STATE_TO_CANONICAL: dict[str, str] = {
    "ANDAMAN & NICOBAR": "Andaman & Nicobar Islands",
    "ANDHRA PRADESH": "Andhra Pradesh",
    "ARUNACHAL PRADESH": "Arunachal Pradesh",
    "ASSAM": "Assam",
    "BIHAR": "Bihar",
    "CHANDIGARH": "Chandigarh",
    "CHHATTISGARH": "Chhattisgarh",
    "DADRA,NAGAR HAVELI,DAMAN & DIU": "Dadra, Nagar Haveli, Daman & Diu",
    "DELHI": "Delhi",
    "GOA": "Goa",
    "GUJARAT": "Gujarat",
    "HARYANA": "Haryana",
    "HIMACHAL PRADESH": "Himachal Pradesh",
    "JAMMU & KASHMIR": "Jammu & Kashmir",
    "JHARKHAND": "Jharkhand",
    "KARNATAKA": "Karnataka",
    "KERALA": "Kerala",
    "LADAKH": "Ladakh",
    "LAKSHADWEEP": "Lakshadweep",
    "MADHYA PRADESH": "Madhya Pradesh",
    "MAHARASHTRA": "Maharashtra",
    "MANIPUR": "Manipur",
    "MEGHALAYA": "Meghalaya",
    "MIZORAM": "Mizoram",
    "NAGALAND": "Nagaland",
    "ODISHA": "Odisha",
    "PUDUCHERRY": "Puducherry",
    "PUNJAB": "Punjab",
    "RAJASTHAN": "Rajasthan",
    "SIKKIM": "Sikkim",
    "TAMIL NADU": "Tamil Nadu",
    "TELANGANA": "Telangana",
    "TRIPURA": "Tripura",
    "UTTAR PRADESH": "Uttar Pradesh",
    "UTTARAKHAND": "Uttarakhand",
    "WEST BENGAL": "West Bengal",
}


def _norm_state_key(value: object) -> str:
    """Normalize a raw state label to a punctuation/space/case-insensitive key."""
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


_NORM_STATE_TO_CANONICAL: dict[str, str] = {
    _norm_state_key(raw): canonical for raw, canonical in _RAW_STATE_TO_CANONICAL.items()
}


def _default_source_shapefile() -> Path:
    """Resolve the bharatlas LGD_Blocks shapefile from a few known locations."""
    data_dir = get_paths_config().data_dir
    candidates = [
        data_dir / "LGD_Blocks" / "LGD_Blocks.shp",
        data_dir / "_tmp_lgd_blocks" / "LGD_Blocks.shp",
        data_dir / "LGD_Blocks.shp",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _states_path() -> Path:
    return get_paths_config().data_dir / "states_4326.geojson"


def _invalid_identity_mask(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.strip().fillna("")
    return normalized.str.lower().isin(_INVALID_ADMIN_VALUES)


def _strip_district_suffix(series: pd.Series) -> pd.Series:
    """Drop a redundant trailing ' District' token (e.g. 'Lakshadweep District')."""
    return series.astype("string").str.replace(r"\s+District\s*$", "", regex=True, case=False).str.strip()


def _canonicalize_states(series: pd.Series) -> tuple[pd.Series, list[str]]:
    """Map raw source states to canonical names; return (mapped, unmapped_raw)."""
    keys = series.map(_norm_state_key)
    mapped = keys.map(_NORM_STATE_TO_CANONICAL)
    unmapped_mask = mapped.isna()
    unmapped = sorted({str(v) for v in series[unmapped_mask].unique()})
    return mapped, unmapped


def _fix_invalid_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """buffer(0) repair for invalid polygons; drop null/empty geometry rows."""

    def _fix_one(geom):
        if geom is None or geom.is_empty:
            return geom
        try:
            if geom.is_valid:
                return geom
        except Exception:  # noqa: BLE001
            pass
        try:
            return geom.buffer(0)
        except Exception:  # noqa: BLE001
            return geom

    out = gdf.copy()
    out["geometry"] = out["geometry"].apply(_fix_one)
    return out.loc[out.geometry.notna() & ~out.geometry.is_empty].copy()


def _area_km2(gdf: gpd.GeoDataFrame) -> pd.Series:
    projected = gdf.to_crs(epsg=AREA_EPSG)
    return projected.geometry.area / 1_000_000.0


def _modal(series: pd.Series) -> object:
    """Return the most frequent non-null value, or None when empty."""
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    counts = cleaned.value_counts()
    return counts.index[0]


def prepare_admin_boundaries(
    shp_path: Path,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, dict[str, object]]:
    """
    Build the canonical blocks/districts/states GeoDataFrames from one LGD source.

    Returns ``(blocks, districts, states, qa)`` where ``qa`` is a dict of QA
    tables/metrics for reporting. Raises on unmapped state names or suspicious
    admin labels so a bad build fails loudly.
    """
    raw = gpd.read_file(shp_path)
    raw_feature_count = int(len(raw))

    gdf = _fix_invalid_geometries(raw)
    gdf = ensure_epsg4326(gdf)

    # Canonical state names (hard error on anything unmapped).
    state_mapped, unmapped = _canonicalize_states(gdf["state"])
    if unmapped:
        raise ValueError(
            "Source contains state name(s) with no canonical mapping: "
            f"{unmapped}. Add them to _RAW_STATE_TO_CANONICAL."
        )
    gdf["state_name"] = state_mapped.astype(str)
    gdf["district_name"] = _strip_district_suffix(gdf["district"])
    gdf["block_name"] = gdf["block_name"].astype("string").str.strip()

    # Apply the SAME identity repair the ADM3 loader uses at runtime so the stored
    # names match exactly what the dashboard reads back (Title-Case d/b labels).
    gdf = repair_adm3_identity_columns(gdf)

    invalid = (
        _invalid_identity_mask(gdf["state_name"])
        | _invalid_identity_mask(gdf["district_name"])
        | _invalid_identity_mask(gdf["block_name"])
    )
    dropped_invalid = int(invalid.sum())
    gdf = gdf.loc[~invalid].copy().reset_index(drop=True)
    if gdf.empty:
        raise ValueError("No valid block rows remain after canonical identity filtering.")

    # Fail the build on the same suspicious labels the block loader rejects.
    anomalies = collect_adm3_label_anomalies(gdf)
    if not anomalies.empty:
        sample = anomalies[["field", "state_name", "district_name", "block_name"]].head(10).to_dict(orient="records")
        raise ValueError(f"Suspicious admin labels detected after repair. Sample: {sample}")

    # Reference identity/code columns carried onto blocks for future joins.
    gdf["block_lgd_code"] = gdf.get("block_lgd")
    gdf["dist_lgd_code"] = gdf.get("dist_lgd")
    gdf["state_lgd_code"] = gdf.get("state_lgd")
    gdf["census_2011_code"] = gdf.get("code2011")

    gdf["block_key"] = (
        gdf["state_name"].astype(str).str.strip()
        + "::"
        + gdf["district_name"].astype(str).str.strip()
        + "::"
        + gdf["block_name"].astype(str).str.strip()
    )
    gdf["district_key"] = (
        gdf["state_name"].astype(str).str.strip()
        + "::"
        + gdf["district_name"].astype(str).str.strip()
    )
    duplicate_block_keys = int(gdf["block_key"].duplicated().sum())

    # ---- Blocks: dissolve multi-part fragments by block_key -----------------
    block_cols = [
        "state_name",
        "district_name",
        "block_name",
        "block_lgd_code",
        "dist_lgd_code",
        "state_lgd_code",
        "census_2011_code",
        "block_key",
        "geometry",
    ]
    block_aggfunc = {c: "first" for c in block_cols if c not in ("block_key", "geometry")}
    blocks = gdf[block_cols].dissolve(by="block_key", as_index=False, aggfunc=block_aggfunc)
    blocks = blocks[block_cols].reset_index(drop=True)

    # ---- Districts: dissolve blocks by (state_name, district_name) ----------
    districts = gdf[["district_key", "state_name", "district_name", "geometry"]].dissolve(
        by="district_key",
        as_index=False,
        aggfunc={"state_name": "first", "district_name": "first"},
    )
    modal_dist_lgd = gdf.groupby("district_key")["dist_lgd_code"].agg(_modal)
    n_codes_per_district = gdf.groupby("district_key")["dist_lgd_code"].nunique(dropna=True)
    districts["district_lgd_code"] = districts["district_key"].map(modal_dist_lgd)
    districts["area_km2"] = _area_km2(districts)
    districts = districts[
        ["state_name", "district_name", "district_lgd_code", "area_km2", "geometry"]
    ].reset_index(drop=True)

    # Districts whose blocks carry >1 distinct dist_lgd (e.g. 2023 splits sharing
    # a parent code) -- informational, not an error.
    ambiguous = (
        gdf[["district_key", "state_name", "district_name"]]
        .drop_duplicates()
        .set_index("district_key")
        .join(n_codes_per_district.rename("distinct_dist_lgd"))
        .reset_index(drop=True)
    )
    ambiguous = ambiguous.loc[ambiguous["distinct_dist_lgd"] > 1].reset_index(drop=True)

    # ---- States: dissolve districts by state_name ---------------------------
    states = districts[["state_name", "geometry"]].dissolve(by="state_name", as_index=False)
    modal_state_lgd = gdf.groupby("state_name")["state_lgd_code"].agg(_modal)
    states["state_lgd_code"] = states["state_name"].map(modal_state_lgd)
    states["area_km2"] = _area_km2(states)
    states = states[["state_name", "state_lgd_code", "area_km2", "geometry"]].reset_index(drop=True)

    per_state = (
        blocks.groupby("state_name")
        .agg(blocks=("block_name", "size"))
        .join(districts.groupby("state_name").agg(districts=("district_name", "size")))
        .reset_index()
        .sort_values("state_name")
        .reset_index(drop=True)
    )

    qa: dict[str, object] = {
        "raw_feature_count": raw_feature_count,
        "dropped_invalid_identity_rows": dropped_invalid,
        "duplicate_block_keys_dissolved": duplicate_block_keys,
        "n_blocks": int(len(blocks)),
        "n_districts": int(len(districts)),
        "n_states": int(len(states)),
        "per_state": per_state,
        "ambiguous_dist_lgd": ambiguous,
    }
    return blocks, districts, states, qa


def _write_geojson(gdf: gpd.GeoDataFrame, path: Path, *, overwrite: bool, backup: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite without --overwrite: {path}")
    if path.exists() and backup:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        bak = path.with_suffix(path.suffix + f".bak-{stamp}")
        shutil.copy2(path, bak)
        print(f"  backed up existing {path.name} -> {bak.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        gdf.to_file(path, driver="GeoJSON", encoding="utf-8")
    except TypeError:
        gdf.to_file(path, driver="GeoJSON")


def _print_report(blocks: gpd.GeoDataFrame, districts: gpd.GeoDataFrame, states: gpd.GeoDataFrame, qa: dict[str, object]) -> None:
    print("ADMIN BOUNDARIES (from LGD_Blocks)")
    print(f"  raw_feature_count:            {qa['raw_feature_count']}")
    print(f"  dropped_invalid_identity:     {qa['dropped_invalid_identity_rows']}")
    print(f"  duplicate_block_keys_merged:  {qa['duplicate_block_keys_dissolved']}")
    print(f"  blocks:    {qa['n_blocks']}")
    print(f"  districts: {qa['n_districts']}")
    print(f"  states:    {qa['n_states']}")
    per_state = qa["per_state"]
    assert isinstance(per_state, pd.DataFrame)
    print("\n  per-state (state | districts | blocks):")
    for row in per_state.itertuples(index=False):
        print(f"    {row.state_name:<34} {int(row.districts):>4}  {int(row.blocks):>5}")
    ambiguous = qa["ambiguous_dist_lgd"]
    assert isinstance(ambiguous, pd.DataFrame)
    print(f"\n  districts whose blocks carry >1 dist_lgd (e.g. 2023 splits): {len(ambiguous)}")
    for row in ambiguous.head(20).itertuples(index=False):
        print(f"    {row.state_name} / {row.district_name}  (distinct_dist_lgd={int(row.distinct_dist_lgd)})")


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build consistent state/district/block GeoJSONs from the LGD_Blocks shapefile."
    )
    parser.add_argument("--source", type=str, default=str(_default_source_shapefile()), help="Path to LGD_Blocks.shp.")
    parser.add_argument("--blocks-out", type=str, default=str(BLOCKS_PATH), help="Output blocks_4326.geojson path.")
    parser.add_argument("--districts-out", type=str, default=str(DISTRICTS_PATH), help="Output districts_4326.geojson path.")
    parser.add_argument("--states-out", type=str, default=str(_states_path()), help="Output states_4326.geojson path.")
    parser.add_argument("--qa-out", type=str, default="", help="Optional CSV path for the per-state QA table.")
    parser.add_argument("--dry-run", action="store_true", help="Compute and report only; write nothing.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output GeoJSONs.")
    parser.add_argument("--no-backup", action="store_true", help="Do not back up existing outputs before overwrite.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    shp_path = Path(args.source).expanduser().resolve()
    if not shp_path.exists():
        raise FileNotFoundError(f"Source shapefile not found: {shp_path}")

    print(f"source: {shp_path}")
    blocks, districts, states, qa = prepare_admin_boundaries(shp_path)
    _print_report(blocks, districts, states, qa)

    if args.qa_out:
        qa_path = Path(args.qa_out).expanduser().resolve()
        qa_path.parent.mkdir(parents=True, exist_ok=True)
        assert isinstance(qa["per_state"], pd.DataFrame)
        qa["per_state"].to_csv(qa_path, index=False)
        print(f"\nwrote QA table: {qa_path}")

    if args.dry_run:
        print("\n[dry-run] no GeoJSON written.")
        return 0

    backup = not args.no_backup
    _write_geojson(blocks, Path(args.blocks_out).expanduser().resolve(), overwrite=args.overwrite, backup=backup)
    _write_geojson(districts, Path(args.districts_out).expanduser().resolve(), overwrite=args.overwrite, backup=backup)
    _write_geojson(states, Path(args.states_out).expanduser().resolve(), overwrite=args.overwrite, backup=backup)
    print("\nwrote:")
    print(f"  {args.blocks_out}")
    print(f"  {args.districts_out}")
    print(f"  {args.states_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Build admin_hydro_summary.parquet from existing crosswalk parquets.

Reads from (all already produced by the crosswalk pipeline):
  processed_optimised/context/district_basin.parquet
  processed_optimised/context/district_subbasin.parquet
  processed_optimised/context/block_basin.parquet      (optional)
  processed_optimised/context/block_subbasin.parquet   (optional)

Writes:
  processed_optimised/context/admin_hydro_summary.parquet

Usage:
  python -m tools.pipeline.build_admin_hydro_summary --data-dir D:\\projects\\irt_data
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from india_resilience_tool.data.optimized_bundle import (
    optimized_context_path,
    resolve_optimized_bundle_root,
)
from india_resilience_tool.utils.naming import alias


# ---------------------------------------------------------------------------
# Key construction
# ---------------------------------------------------------------------------

def _admin_key(state: str, district: str, block: str = "") -> str:
    parts = [alias(state), alias(district)]
    if block:
        parts.append(alias(block))
    return "|".join(parts)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _also_intersects_json(
    grp: pd.DataFrame,
    frac_col: str,
    id_col: str,
    name_col: str,
    dominant_id: str,
    *,
    min_frac: float = 0.05,
    max_items: int = 2,
) -> str:
    others = grp[grp[id_col].astype(str) != str(dominant_id)].copy()
    others = others.sort_values(frac_col, ascending=False)
    result = []
    for _, r in others.iterrows():
        frac = float(r[frac_col]) if pd.notna(r.get(frac_col)) else None
        if frac is None or frac < min_frac:
            continue
        result.append({
            "basin_id": str(r[id_col]),
            "basin_name": str(r[name_col]),
            "basin_frac": round(frac, 4),
        })
        if len(result) >= max_items:
            break
    return json.dumps(result)


def _hydro_type(basin_frac: float | None, subbasin_frac: float | None) -> str:
    if basin_frac is None:
        return "Hydro context unavailable"
    if basin_frac >= 0.70:
        if subbasin_frac is not None and subbasin_frac < 0.50:
            return "Single-basin, mixed sub-basin"
        return "Mostly single-basin"
    if 0.50 <= basin_frac < 0.70:
        return "Moderately mixed basin"
    return "Mixed-basin"


def _dominant_subbasin(
    subbasin_df: pd.DataFrame,
    *,
    state: str,
    district: str,
    block: str = "",
    frac_col: str,
) -> tuple[str, str, float | None]:
    """Return (subbasin_id, subbasin_name, frac) for the dominant sub-basin."""
    if subbasin_df.empty or frac_col not in subbasin_df.columns:
        return "", "", None

    mask = (
        subbasin_df["state_name"].astype(str).eq(state)
        & subbasin_df["district_name"].astype(str).eq(district)
    )
    if block:
        mask &= subbasin_df["block_name"].astype(str).eq(block)
    sub_grp = subbasin_df.loc[mask]
    if sub_grp.empty:
        return "", "", None

    dom = sub_grp.loc[sub_grp[frac_col].idxmax()]
    frac = float(dom[frac_col]) if pd.notna(dom[frac_col]) else None
    return str(dom["subbasin_id"]), str(dom["subbasin_name"]), frac


# ---------------------------------------------------------------------------
# District rows
# ---------------------------------------------------------------------------

def _build_district_rows(context_dir: Path) -> pd.DataFrame:
    basin_path = context_dir / "district_basin.parquet"
    subbasin_path = context_dir / "district_subbasin.parquet"

    if not basin_path.exists():
        print(f"  WARNING: {basin_path} not found — skipping districts")
        return pd.DataFrame()

    basin_df = pd.read_parquet(basin_path)
    subbasin_df = pd.read_parquet(subbasin_path) if subbasin_path.exists() else pd.DataFrame()
    frac_col = "district_area_fraction_in_basin"

    if frac_col not in basin_df.columns:
        print(f"  WARNING: column '{frac_col}' missing in district_basin.parquet — skipping districts")
        return pd.DataFrame()

    rows = []
    for (state_name, district_name), grp in basin_df.groupby(
        ["state_name", "district_name"], sort=False
    ):
        dom = grp.loc[grp[frac_col].idxmax()]
        basin_id = str(dom["basin_id"])
        basin_name = str(dom["basin_name"])
        basin_frac = float(dom[frac_col]) if pd.notna(dom[frac_col]) else None
        also = _also_intersects_json(grp, frac_col, "basin_id", "basin_name", basin_id)

        sub_id, sub_name, sub_frac = _dominant_subbasin(
            subbasin_df,
            state=str(state_name),
            district=str(district_name),
            frac_col="subbasin_area_fraction_in_district",
        )

        rows.append({
            "admin_key": _admin_key(str(state_name), str(district_name)),
            "admin_level": "district",
            "state_name": str(state_name),
            "district_name": str(district_name),
            "block_name": "",
            "basin_id": basin_id,
            "basin_name": basin_name,
            "basin_frac": basin_frac if basin_frac is not None else float("nan"),
            "subbasin_id": sub_id,
            "subbasin_name": sub_name,
            "subbasin_frac": sub_frac if sub_frac is not None else float("nan"),
            "also_intersects_basin_json": also,
            "drainage_area_km2": float("nan"),
            "primary_river": "",
            "runoff_coeff": float("nan"),
            "hydro_type": _hydro_type(basin_frac, sub_frac),
            "hydro_summary_status": "available",
        })

    print(f"  Districts: {len(rows)} rows")
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Block rows
# ---------------------------------------------------------------------------

def _build_block_rows(context_dir: Path) -> pd.DataFrame:
    basin_path = context_dir / "block_basin.parquet"
    subbasin_path = context_dir / "block_subbasin.parquet"

    if not basin_path.exists():
        print(f"  INFO: {basin_path} not found — skipping blocks")
        return pd.DataFrame()

    basin_df = pd.read_parquet(basin_path)
    subbasin_df = pd.read_parquet(subbasin_path) if subbasin_path.exists() else pd.DataFrame()
    frac_col = "block_area_fraction_in_basin"

    if frac_col not in basin_df.columns:
        print(f"  WARNING: column '{frac_col}' missing in block_basin.parquet — skipping blocks")
        return pd.DataFrame()

    rows = []
    for (state_name, district_name, block_name), grp in basin_df.groupby(
        ["state_name", "district_name", "block_name"], sort=False
    ):
        dom = grp.loc[grp[frac_col].idxmax()]
        basin_id = str(dom["basin_id"])
        basin_name = str(dom["basin_name"])
        basin_frac = float(dom[frac_col]) if pd.notna(dom[frac_col]) else None
        also = _also_intersects_json(grp, frac_col, "basin_id", "basin_name", basin_id)

        sub_id, sub_name, sub_frac = _dominant_subbasin(
            subbasin_df,
            state=str(state_name),
            district=str(district_name),
            block=str(block_name),
            frac_col="subbasin_area_fraction_in_block",
        )

        rows.append({
            "admin_key": _admin_key(str(state_name), str(district_name), str(block_name)),
            "admin_level": "block",
            "state_name": str(state_name),
            "district_name": str(district_name),
            "block_name": str(block_name),
            "basin_id": basin_id,
            "basin_name": basin_name,
            "basin_frac": basin_frac if basin_frac is not None else float("nan"),
            "subbasin_id": sub_id,
            "subbasin_name": sub_name,
            "subbasin_frac": sub_frac if sub_frac is not None else float("nan"),
            "also_intersects_basin_json": also,
            "drainage_area_km2": float("nan"),
            "primary_river": "",
            "runoff_coeff": float("nan"),
            "hydro_type": _hydro_type(basin_frac, sub_frac),
            "hydro_summary_status": "available",
        })

    print(f"  Blocks: {len(rows)} rows")
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def build(data_dir: Path) -> Path:
    context_dir = resolve_optimized_bundle_root(data_dir=data_dir) / "context"
    print(f"Reading crosswalk parquets from: {context_dir}")

    dist_rows = _build_district_rows(context_dir)
    block_rows = _build_block_rows(context_dir)

    all_rows = pd.concat(
        [df for df in [dist_rows, block_rows] if not df.empty],
        ignore_index=True,
    )

    if all_rows.empty:
        raise ValueError(
            "No rows produced — ensure district_basin.parquet exists under context/"
        )

    dupe_mask = all_rows.duplicated(["admin_level", "admin_key"], keep=False)
    if dupe_mask.any():
        dupes = all_rows.loc[dupe_mask, ["admin_level", "admin_key"]].head(5).to_dict("records")
        raise ValueError(f"Duplicate admin_key per level: {dupes}")

    out = optimized_context_path("admin_hydro_summary.parquet", data_dir=data_dir)
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    all_rows.to_parquet(out, index=False)
    print(f"Written: {out}  ({len(all_rows)} rows)")
    return Path(out)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", required=True, help="IRT_DATA_DIR root (e.g. D:\\projects\\irt_data)")
    args = ap.parse_args()
    build(Path(args.data_dir))


if __name__ == "__main__":
    main()

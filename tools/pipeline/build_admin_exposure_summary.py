"""Build admin_exposure_summary.parquet from population master CSVs.

Reads from (produced by tools/geodata/build_population_admin_masters.py):
  <data-dir>/population/population_district_master_qa.csv
  <data-dir>/population/population_block_master_qa.csv  (optional)

Writes:
  processed_optimised/context/admin_exposure_summary.parquet

Usage:
  python -m tools.pipeline.build_admin_exposure_summary --data-dir D:\\projects\\irt_data
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from paths import get_master_csv_filename
from india_resilience_tool.data.optimized_bundle import optimized_context_path
from india_resilience_tool.utils.naming import alias

# The master CSVs use this metric column for total population.
_POP_COL = "population_total__snapshot__2025__mean"
_RURAL_METRICS = {
    "rural_facilities_total_count": "rural_facilities_total_count__snapshot__2019-2021__mean",
    "rural_facilities_agro_count": "rural_facilities_agro_count__snapshot__2019-2021__mean",
    "rural_facilities_education_count": "rural_facilities_education_count__snapshot__2019-2021__mean",
    "rural_facilities_health_count": "rural_facilities_health_count__snapshot__2019-2021__mean",
    "rural_facilities_service_count": "rural_facilities_service_count__snapshot__2019-2021__mean",
    "rural_facilities_total_count_per_100k": "rural_facilities_total_count_per_100k__snapshot__2019-2021__mean",
}


# ---------------------------------------------------------------------------
# Key construction
# ---------------------------------------------------------------------------

def _admin_key(state: str, district: str, block: str = "") -> str:
    parts = [alias(state), alias(district)]
    if block:
        parts.append(alias(block))
    return "|".join(parts)


def _read_state_master(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _iter_metric_master_paths(data_dir: Path, *, slug: str, level: str) -> list[Path]:
    root = data_dir / "processed" / slug
    filename = get_master_csv_filename(level)
    if not root.exists():
        return []
    paths: list[Path] = []
    for state_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        csv_path = state_dir / filename
        parquet_path = csv_path.with_suffix(".parquet")
        if parquet_path.exists():
            paths.append(parquet_path)
        elif csv_path.exists():
            paths.append(csv_path)
    return paths


def _load_rural_metric(data_dir: Path, *, slug: str, source_col: str, level: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in _iter_metric_master_paths(data_dir, slug=slug, level=level):
        df = _read_state_master(path)
        if source_col not in df.columns:
            continue
        if level == "block":
            required = {"state", "district", "block"}
            if not required.issubset(df.columns):
                continue
            key = [
                _admin_key(row["state"], row["district"], row["block"])
                for _, row in df.iterrows()
            ]
        else:
            required = {"state", "district"}
            if not required.issubset(df.columns):
                continue
            key = [_admin_key(row["state"], row["district"]) for _, row in df.iterrows()]
        frames.append(
            pd.DataFrame(
                {
                    "admin_key": key,
                    "admin_level": level,
                    slug: pd.to_numeric(df[source_col], errors="coerce"),
                }
            )
        )
    if not frames:
        return pd.DataFrame(columns=["admin_key", "admin_level", slug])
    out = pd.concat(frames, ignore_index=True)
    return out.drop_duplicates(["admin_level", "admin_key"], keep="first")


def _merge_rural_facilities(data_dir: Path, rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows
    out = rows.copy()
    for slug, source_col in _RURAL_METRICS.items():
        frames = [
            _load_rural_metric(data_dir, slug=slug, source_col=source_col, level=level)
            for level in ("district", "block")
        ]
        metric_df = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)
        if metric_df.empty:
            continue
        out = out.merge(metric_df, on=["admin_key", "admin_level"], how="left")
    count_cols = [slug for slug in _RURAL_METRICS if slug.endswith("_count")]
    for col in count_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    for col in _RURAL_METRICS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


# ---------------------------------------------------------------------------
# District rows
# ---------------------------------------------------------------------------

def _build_district_rows(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "population" / "population_district_master_qa.csv"
    if not path.exists():
        print(f"  WARNING: {path} not found — skipping districts")
        return pd.DataFrame()

    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()

    if _POP_COL not in df.columns:
        print(f"  WARNING: column '{_POP_COL}' not found in district CSV — skipping districts")
        print(f"  Available: {list(df.columns)}")
        return pd.DataFrame()

    df["pop_2020"] = pd.to_numeric(df[_POP_COL], errors="coerce")
    df["state_name"] = df["state"].astype(str).str.strip()
    df["district_name"] = df["district"].astype(str).str.strip()

    # State-level totals as the parent population for districts
    state_totals = df.groupby("state_name")["pop_2020"].sum().rename("parent_pop_2020")
    df = df.join(state_totals, on="state_name")
    df["population_share_parent_pct"] = (
        df["pop_2020"] / df["parent_pop_2020"] * 100.0
    ).where(df["parent_pop_2020"] > 0)

    rows = []
    for _, r in df.iterrows():
        rows.append({
            "admin_key": _admin_key(r["state_name"], r["district_name"]),
            "admin_level": "district",
            "state_name": r["state_name"],
            "district_name": r["district_name"],
            "block_name": "",
            "pop_2020": r["pop_2020"],
            "parent_pop_2020": r.get("parent_pop_2020", float("nan")),
            "parent_level": "state",
            "parent_name": r["state_name"],
            "population_share_parent_pct": r.get("population_share_parent_pct", float("nan")),
        })

    print(f"  Districts: {len(rows)} rows")
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Block rows
# ---------------------------------------------------------------------------

def _build_block_rows(data_dir: Path) -> pd.DataFrame:
    path = data_dir / "population" / "population_block_master_qa.csv"
    if not path.exists():
        print(f"  INFO: {path} not found — skipping blocks")
        return pd.DataFrame()

    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()

    if _POP_COL not in df.columns:
        print(f"  WARNING: column '{_POP_COL}' not found in block CSV — skipping blocks")
        return pd.DataFrame()

    df["pop_2020"] = pd.to_numeric(df[_POP_COL], errors="coerce")
    df["state_name"] = df["state"].astype(str).str.strip()
    df["district_name"] = df["district"].astype(str).str.strip()
    df["block_name"] = df["block"].astype(str).str.strip()

    # District-level totals as the parent population for blocks
    dist_totals = df.groupby(["state_name", "district_name"])["pop_2020"].sum().rename("parent_pop_2020")
    df = df.join(dist_totals, on=["state_name", "district_name"])
    df["population_share_parent_pct"] = (
        df["pop_2020"] / df["parent_pop_2020"] * 100.0
    ).where(df["parent_pop_2020"] > 0)

    rows = []
    for _, r in df.iterrows():
        rows.append({
            "admin_key": _admin_key(r["state_name"], r["district_name"], r["block_name"]),
            "admin_level": "block",
            "state_name": r["state_name"],
            "district_name": r["district_name"],
            "block_name": r["block_name"],
            "pop_2020": r["pop_2020"],
            "parent_pop_2020": r.get("parent_pop_2020", float("nan")),
            "parent_level": "district",
            "parent_name": r["district_name"],
            "population_share_parent_pct": r.get("population_share_parent_pct", float("nan")),
        })

    print(f"  Blocks: {len(rows)} rows")
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def build(data_dir: Path) -> Path:
    print(f"Reading population CSVs from: {data_dir / 'population'}")

    dist_rows = _build_district_rows(data_dir)
    block_rows = _build_block_rows(data_dir)

    all_rows = pd.concat(
        [df for df in [dist_rows, block_rows] if not df.empty],
        ignore_index=True,
    )
    all_rows = _merge_rural_facilities(data_dir, all_rows)

    if all_rows.empty:
        raise ValueError(
            "No rows produced — check population_district_master_qa.csv exists under <data-dir>/population/"
        )

    dupe_mask = all_rows.duplicated(["admin_level", "admin_key"], keep=False)
    if dupe_mask.any():
        dupes = all_rows.loc[dupe_mask, ["admin_level", "admin_key"]].head(5).to_dict("records")
        raise ValueError(f"Duplicate admin_key per level: {dupes}")

    out = optimized_context_path("admin_exposure_summary.parquet", data_dir=data_dir)
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

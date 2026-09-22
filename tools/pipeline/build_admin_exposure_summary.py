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
_BUILT_UP_METRICS = {
    "built_up_area_km2": "built_up_area_km2__snapshot__Current__mean",
    "built_up_area_share_pct": "built_up_area_share_pct__snapshot__Current__mean",
}
_LULC_METRICS = {
    "lulc_agri_area_km2": "lulc_agri_area_km2__snapshot__Current__mean",
    "lulc_agri_share_pct": "lulc_agri_share_pct__snapshot__Current__mean",
}
# LGRIP30 cropland extent (classes 2+3). Attributable replacement candidate for the
# unprovenanced LULC_2_Agri raster behind BL-0027; both are carried so the dashboard
# can switch source without a rebuild. The irrigated/rainfed split from the same
# product is deliberately NOT carried: it misreads monsoon paddy as irrigated across
# eastern India (Assam measures 78.8% irrigated against a Census figure near 12%),
# so it is held out of the runtime until validated against Agricultural Census data.
_LGRIP_METRICS = {
    "lgrip_cropland_area_km2": "lgrip_cropland_area_km2__snapshot__Current__mean",
    "lgrip_cropland_share_pct": "lgrip_cropland_share_pct__snapshot__Current__mean",
}
# WorldPop 2025 age structure. These are State-level proportions applied to the
# gridded population, so they carry no sub-state variation (99.8% of district-level
# variance is explained by State alone). They are published as context-card text and
# must never drive a district or block map fill.
_AGE_METRICS = {
    "population_age_65plus_count": "population_age_65plus_count__snapshot__2025__mean",
    "population_age_65plus_share_pct": "population_age_65plus_share_pct__snapshot__2025__mean",
    "population_age_under5_count": "population_age_under5_count__snapshot__2025__mean",
    "population_age_under5_share_pct": "population_age_under5_share_pct__snapshot__2025__mean",
}
# JRC Global Surface Water v1.4 occurrence. Permanent is occurrence >= 75%, seasonal
# 25-74%; both are thresholds this repo chose, not product definitions. Marine water
# is removed before aggregation, because GSW classifies a nearshore ocean band as
# permanent water and coastal polygons extend into it (the Nicobars measure 17.5%
# permanent water uncorrected, almost all of it sea). The correction is recorded per
# unit in the QA tables, not folded away silently.
_GSW_METRICS = {
    "surface_water_permanent_area_km2": "surface_water_permanent_area_km2__snapshot__Current__mean",
    "surface_water_permanent_share_pct": "surface_water_permanent_share_pct__snapshot__Current__mean",
    "surface_water_seasonal_area_km2": "surface_water_seasonal_area_km2__snapshot__Current__mean",
    "surface_water_seasonal_share_pct": "surface_water_seasonal_share_pct__snapshot__Current__mean",
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


def _load_built_up_metric(data_dir: Path, *, slug: str, source_col: str, level: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in _iter_metric_master_paths(data_dir, slug=slug, level=level):
        df = _read_state_master(path)
        if source_col not in df.columns:
            continue
        if level == "block":
            required = {"state", "district", "block"}
            if not required.issubset(df.columns):
                continue
            key = [_admin_key(row["state"], row["district"], row["block"]) for _, row in df.iterrows()]
            block_name = df["block"].astype(str).str.strip()
        else:
            required = {"state", "district"}
            if not required.issubset(df.columns):
                continue
            key = [_admin_key(row["state"], row["district"]) for _, row in df.iterrows()]
            block_name = pd.Series([""] * len(df), index=df.index)
        frames.append(
            pd.DataFrame(
                {
                    "admin_key": key,
                    "admin_level": level,
                    "state_name": df["state"].astype(str).str.strip(),
                    "district_name": df["district"].astype(str).str.strip(),
                    "block_name": block_name,
                    slug: pd.to_numeric(df[source_col], errors="coerce"),
                }
            )
        )
    if not frames:
        return pd.DataFrame(columns=["admin_key", "admin_level", "state_name", "district_name", "block_name", slug])
    out = pd.concat(frames, ignore_index=True)
    return out.drop_duplicates(["admin_level", "admin_key"], keep="first")


def _load_admin_metric(data_dir: Path, *, slug: str, source_col: str, level: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in _iter_metric_master_paths(data_dir, slug=slug, level=level):
        df = _read_state_master(path)
        if source_col not in df.columns:
            continue
        if level == "block":
            required = {"state", "district", "block"}
            if not required.issubset(df.columns):
                continue
            key = [_admin_key(row["state"], row["district"], row["block"]) for _, row in df.iterrows()]
            block_name = df["block"].astype(str).str.strip()
        else:
            required = {"state", "district"}
            if not required.issubset(df.columns):
                continue
            key = [_admin_key(row["state"], row["district"]) for _, row in df.iterrows()]
            block_name = pd.Series([""] * len(df), index=df.index)
        frames.append(
            pd.DataFrame(
                {
                    "admin_key": key,
                    "admin_level": level,
                    "state_name": df["state"].astype(str).str.strip(),
                    "district_name": df["district"].astype(str).str.strip(),
                    "block_name": block_name,
                    slug: pd.to_numeric(df[source_col], errors="coerce"),
                }
            )
        )
    if not frames:
        return pd.DataFrame(columns=["admin_key", "admin_level", "state_name", "district_name", "block_name", slug])
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


def _merge_built_up_area(data_dir: Path, rows: pd.DataFrame) -> pd.DataFrame:
    metric_frames: list[pd.DataFrame] = []
    for slug, source_col in _BUILT_UP_METRICS.items():
        frames = [
            _load_built_up_metric(data_dir, slug=slug, source_col=source_col, level=level)
            for level in ("district", "block")
        ]
        non_empty = [frame for frame in frames if not frame.empty]
        metric_df = pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()
        if metric_df.empty:
            continue
        metric_frames.append(metric_df)

    if not metric_frames:
        return rows

    built_up_rows = metric_frames[0]
    for metric_df in metric_frames[1:]:
        built_up_rows = built_up_rows.merge(
            metric_df,
            on=["admin_key", "admin_level", "state_name", "district_name", "block_name"],
            how="outer",
        )
    for col in _BUILT_UP_METRICS:
        if col in built_up_rows.columns:
            built_up_rows[col] = pd.to_numeric(built_up_rows[col], errors="coerce")

    if rows.empty:
        out = built_up_rows.copy()
        out["pop_2020"] = pd.NA
        out["parent_pop_2020"] = pd.NA
        out["parent_level"] = out["admin_level"].map({"block": "district", "district": "state"}).fillna("")
        out["parent_name"] = out.apply(
            lambda r: r["district_name"] if r["admin_level"] == "block" else r["state_name"],
            axis=1,
        )
        out["population_share_parent_pct"] = pd.NA
        return out

    identity = ["admin_key", "admin_level", "state_name", "district_name", "block_name"]
    metric_cols = [col for col in _BUILT_UP_METRICS if col in built_up_rows.columns]
    out = rows.merge(
        built_up_rows[identity + metric_cols],
        on=identity,
        how="outer",
    )
    out["parent_level"] = out["parent_level"].fillna(
        out["admin_level"].map({"block": "district", "district": "state"})
    )
    out["parent_name"] = out["parent_name"].fillna(
        out.apply(lambda r: r["district_name"] if r["admin_level"] == "block" else r["state_name"], axis=1)
    )
    return out


def _merge_metric_group(
    data_dir: Path, rows: pd.DataFrame, metrics: dict[str, str]
) -> pd.DataFrame:
    """Merge one family of admin metric masters onto the exposure rows.

    Every family is optional: a metric whose masters are absent is skipped rather
    than raised on, so a partial data directory still yields a usable summary.
    """
    metric_frames: list[pd.DataFrame] = []
    for slug, source_col in metrics.items():
        frames = [
            _load_admin_metric(data_dir, slug=slug, source_col=source_col, level=level)
            for level in ("district", "block")
        ]
        non_empty = [frame for frame in frames if not frame.empty]
        metric_df = pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()
        if metric_df.empty:
            continue
        metric_frames.append(metric_df)

    if not metric_frames:
        return rows

    lulc_rows = metric_frames[0]
    for metric_df in metric_frames[1:]:
        lulc_rows = lulc_rows.merge(
            metric_df,
            on=["admin_key", "admin_level", "state_name", "district_name", "block_name"],
            how="outer",
        )
    for col in metrics:
        if col in lulc_rows.columns:
            lulc_rows[col] = pd.to_numeric(lulc_rows[col], errors="coerce")

    if rows.empty:
        out = lulc_rows.copy()
        out["pop_2020"] = pd.NA
        out["parent_pop_2020"] = pd.NA
        out["parent_level"] = out["admin_level"].map({"block": "district", "district": "state"}).fillna("")
        out["parent_name"] = out.apply(
            lambda r: r["district_name"] if r["admin_level"] == "block" else r["state_name"],
            axis=1,
        )
        out["population_share_parent_pct"] = pd.NA
        return out

    identity = ["admin_key", "admin_level", "state_name", "district_name", "block_name"]
    metric_cols = [col for col in metrics if col in lulc_rows.columns]
    out = rows.merge(
        lulc_rows[identity + metric_cols],
        on=identity,
        how="outer",
    )
    out["parent_level"] = out["parent_level"].fillna(
        out["admin_level"].map({"block": "district", "district": "state"})
    )
    out["parent_name"] = out["parent_name"].fillna(
        out.apply(lambda r: r["district_name"] if r["admin_level"] == "block" else r["state_name"], axis=1)
    )
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

    base_frames = [df for df in [dist_rows, block_rows] if not df.empty]
    all_rows = pd.concat(base_frames, ignore_index=True) if base_frames else pd.DataFrame()
    all_rows = _merge_rural_facilities(data_dir, all_rows)
    all_rows = _merge_built_up_area(data_dir, all_rows)
    all_rows = _merge_metric_group(data_dir, all_rows, _LULC_METRICS)
    all_rows = _merge_metric_group(data_dir, all_rows, _LGRIP_METRICS)
    all_rows = _merge_metric_group(data_dir, all_rows, _AGE_METRICS)
    all_rows = _merge_metric_group(data_dir, all_rows, _GSW_METRICS)

    if all_rows.empty:
        raise ValueError(
            "No rows produced — check population QA, built-up processed masters, or LULC processed masters exist under <data-dir>/"
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

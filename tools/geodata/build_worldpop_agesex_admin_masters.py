#!/usr/bin/env python3
"""Build district and block age-structure exposure masters from WorldPop age-sex bands.

Two age groups are published, deliberately kept separate rather than merged into a
single "dependent population" figure: their geographies are close to anti-correlated
across India, and merging them would cancel the signal the layer exists to show.

  * ``65+``      -- WorldPop age groups 65, 70, 75, 80 for both sexes (8 bands)
  * ``under 5``  -- WorldPop age groups 00 (age 0) and 01 (ages 1-4), both sexes (4 bands)

Counts are zonal sums over the canonical admin polygons, using the identical method
and helper as the population master (``_zonal_sum_for_geometry``) so that the share
denominator is method-consistent with the published ``population_total``. Shares are
percentages of that same recomputed unit population, which keeps the dashboard's
circle size (population) and circle fill (age share) sourced from one arithmetic.

The age-sex bands share the population raster's grid exactly (same CRS, bounds,
dimensions and transform), so no resampling or warping occurs anywhere in this tool.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Literal, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio

from paths import get_master_csv_filename, get_paths_config, resolve_processed_root
from tools.geodata.build_district_subbasin_crosswalk import (
    load_block_boundaries,
    load_district_boundaries,
)
from tools.geodata.build_population_admin_masters import (
    _find_default_population_raster,
    _zonal_sum_for_geometry,
)


AdminLevel = Literal["district", "block"]

AREA_EPSG = 6933
SNAPSHOT_SCENARIO = "snapshot"
SNAPSHOT_PERIOD = "2025"

POPULATION_TOTAL_COL = "population_total__snapshot__2025__mean"
AGE_65PLUS_COUNT_COL = "population_age_65plus_count__snapshot__2025__mean"
AGE_65PLUS_SHARE_COL = "population_age_65plus_share_pct__snapshot__2025__mean"
AGE_UNDER5_COUNT_COL = "population_age_under5_count__snapshot__2025__mean"
AGE_UNDER5_SHARE_COL = "population_age_under5_share_pct__snapshot__2025__mean"

#: Metric slug -> master column. These four slugs are what the registry and the
#: exposure summary consume; nothing downstream reads the derived rasters.
METRIC_COLUMNS: dict[str, str] = {
    "population_age_65plus_count": AGE_65PLUS_COUNT_COL,
    "population_age_65plus_share_pct": AGE_65PLUS_SHARE_COL,
    "population_age_under5_count": AGE_UNDER5_COUNT_COL,
    "population_age_under5_share_pct": AGE_UNDER5_SHARE_COL,
}

#: WorldPop age-group tokens. ``00`` is age 0 and ``01`` is ages 1-4, so the two
#: together are the under-5 population; 65/70/75/80 are five-year groups with 80
#: open-ended.
UNDER5_GROUPS: tuple[str, ...] = ("00", "01")
AGE_65PLUS_GROUPS: tuple[str, ...] = ("65", "70", "75", "80")
SEXES: tuple[str, ...] = ("f", "m")

BAND_TEMPLATE = "ind_{sex}_{group}_2025_CN_1km_R2025A_UA_v1.tif"

DERIVED_65PLUS_NAME = "ind_age65plus_2025_CN_1km_R2025A_UA_v1.tif"
DERIVED_UNDER5_NAME = "ind_ageunder5_2025_CN_1km_R2025A_UA_v1.tif"

#: National share guardrails. India's 2025 age structure puts roughly 7% of people
#: at 65 or over and roughly 8% under 5. These bounds are wide enough to absorb a
#: genuine revision and tight enough to catch a wrong denominator or a dropped band.
NATIONAL_SHARE_BOUNDS: dict[str, tuple[float, float]] = {
    "population_age_65plus_share_pct": (4.0, 12.0),
    "population_age_under5_share_pct": (4.0, 14.0),
}

#: A unit's age count may not exceed its total population. A hair of tolerance
#: absorbs float32 accumulation noise over several thousand cells.
COUNT_TOLERANCE_RATIO = 1.0001


def _default_band_dir() -> Path:
    return get_paths_config().data_dir / "worldpop_agesex" / "bands"


def _default_derived_dir() -> Path:
    return get_paths_config().data_dir / "worldpop_agesex" / "derived"


def _default_qa_dir() -> Path:
    return get_paths_config().data_dir / "worldpop_agesex"


def _identity_cols(level: AdminLevel) -> list[str]:
    if level == "block":
        return ["state_name", "district_name", "block_name", "block_key"]
    return ["state_name", "district_name", "district_key"]


def _area_col(level: AdminLevel) -> str:
    return "block_area_km2" if level == "block" else "district_area_km2"


def _qa_key_cols(level: AdminLevel) -> list[str]:
    if level == "block":
        return ["state", "district", "block", "block_key"]
    return ["state", "district", "district_key"]


def expected_band_paths(band_dir: Path) -> dict[str, list[Path]]:
    """Return the expected band file paths for each age group."""
    return {
        "population_age_65plus": [
            band_dir / BAND_TEMPLATE.format(sex=sex, group=group)
            for group in AGE_65PLUS_GROUPS
            for sex in SEXES
        ],
        "population_age_under5": [
            band_dir / BAND_TEMPLATE.format(sex=sex, group=group)
            for group in UNDER5_GROUPS
            for sex in SEXES
        ],
    }


def _assert_grid_match(reference: rasterio.DatasetReader, other: rasterio.DatasetReader, label: str) -> None:
    """Fail loudly if a band does not sit on the reference grid.

    Summing bands cell-by-cell is only valid on an identical grid. Rather than
    silently resampling -- which would smear an age count across neighbours -- this
    refuses to proceed.
    """
    if (other.width, other.height) != (reference.width, reference.height):
        raise ValueError(
            f"{label}: raster size {other.width}x{other.height} does not match the "
            f"reference grid {reference.width}x{reference.height}."
        )
    if other.crs != reference.crs:
        raise ValueError(f"{label}: CRS {other.crs} does not match reference CRS {reference.crs}.")
    if not np.allclose(np.array(other.transform), np.array(reference.transform), atol=1e-9):
        raise ValueError(f"{label}: affine transform does not match the reference grid.")


def build_derived_age_raster(
    band_paths: list[Path],
    out_path: Path,
    *,
    overwrite: bool,
    dry_run: bool,
) -> dict[str, object]:
    """Sum a set of WorldPop age-sex bands into one raster and write it.

    Summing once and zonal-aggregating the result is both faster and easier to audit
    than aggregating twelve bands separately: the derived raster is an inspectable
    artifact that a later run, or a reviewer, can open directly.
    """
    missing = [str(p) for p in band_paths if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing WorldPop age-sex band(s): {', '.join(missing)}")

    if out_path.exists() and not overwrite and not dry_run:
        raise FileExistsError(
            f"Refusing to overwrite existing derived raster without --overwrite: {out_path}"
        )

    accumulator: Optional[np.ndarray] = None
    valid_any: Optional[np.ndarray] = None
    profile: Optional[dict] = None

    with rasterio.open(band_paths[0]) as reference:
        profile = reference.profile.copy()
        for path in band_paths:
            with rasterio.open(path) as src:
                _assert_grid_match(reference, src, label=path.name)
                data = src.read(1, masked=True)
            filled = data.filled(0.0).astype("float64")
            valid = ~np.ma.getmaskarray(data)
            accumulator = filled if accumulator is None else accumulator + filled
            valid_any = valid if valid_any is None else (valid_any | valid)

    assert accumulator is not None and valid_any is not None and profile is not None

    nodata = -99999.0
    out = np.where(valid_any, accumulator, nodata).astype("float32")
    total = float(accumulator[valid_any].sum())

    profile.update(
        dtype="float32",
        count=1,
        nodata=nodata,
        compress="deflate",
        predictor=2,
        tiled=True,
        blockxsize=256,
        blockysize=256,
    )

    if not dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(out, 1)

    return {
        "path": str(out_path),
        "band_count": len(band_paths),
        "bands": [p.name for p in band_paths],
        "national_total": total,
        "valid_cells": int(valid_any.sum()),
        "written": not dry_run,
    }


def aggregate_age_structure_to_admin_units(
    admin_gdf: gpd.GeoDataFrame,
    *,
    level: AdminLevel,
    population_raster: Path,
    raster_65plus: Path,
    raster_under5: Path,
    area_epsg: int = AREA_EPSG,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aggregate population and both age-group rasters onto canonical polygons.

    All three rasters are aggregated in one pass over the geometries with the same
    zonal helper the population master uses, so a unit's age share is exactly its
    age count divided by its population under identical cell inclusion rules.

    Returns:
        (master_df, qa_df)
    """
    if admin_gdf.empty:
        raise ValueError(f"No {level} boundaries were provided.")

    identity_cols = _identity_cols(level)
    missing = [col for col in identity_cols + ["geometry"] if col not in admin_gdf.columns]
    if missing:
        raise ValueError(f"{level.title()} boundaries are missing required columns: {missing}")

    with rasterio.open(population_raster) as pop_src, \
            rasterio.open(raster_65plus) as src_65, \
            rasterio.open(raster_under5) as src_u5:
        if pop_src.crs is None:
            raise ValueError(f"Population raster has no CRS: {population_raster}")
        _assert_grid_match(pop_src, src_65, label=Path(raster_65plus).name)
        _assert_grid_match(pop_src, src_u5, label=Path(raster_under5).name)

        admin_for_raster = admin_gdf.to_crs(pop_src.crs).copy()

        pop_sums: list[float] = []
        sums_65: list[float] = []
        sums_u5: list[float] = []
        cell_counts: list[int] = []
        for geom in admin_for_raster.geometry:
            pop_value, cells = _zonal_sum_for_geometry(pop_src, geom)
            value_65, _ = _zonal_sum_for_geometry(src_65, geom)
            value_u5, _ = _zonal_sum_for_geometry(src_u5, geom)
            pop_sums.append(pop_value)
            sums_65.append(value_65)
            sums_u5.append(value_u5)
            cell_counts.append(cells)

    area_df = admin_gdf.to_crs(epsg=area_epsg).copy()
    out = admin_gdf[identity_cols].copy()
    out[_area_col(level)] = area_df.geometry.area / 1_000_000.0
    out[POPULATION_TOTAL_COL] = np.asarray(pop_sums, dtype="float64")
    out[AGE_65PLUS_COUNT_COL] = np.asarray(sums_65, dtype="float64")
    out[AGE_UNDER5_COUNT_COL] = np.asarray(sums_u5, dtype="float64")
    out["__cell_count"] = np.asarray(cell_counts, dtype="int64")

    population = pd.to_numeric(out[POPULATION_TOTAL_COL], errors="coerce").fillna(0.0)
    for count_col, share_col in (
        (AGE_65PLUS_COUNT_COL, AGE_65PLUS_SHARE_COL),
        (AGE_UNDER5_COUNT_COL, AGE_UNDER5_SHARE_COL),
    ):
        counts = pd.to_numeric(out[count_col], errors="coerce").fillna(0.0)
        # An unpopulated unit has no age structure. NaN -- not zero -- is the honest
        # answer, and the dashboard renders absence rather than "no elderly here".
        out[share_col] = np.where(population > 0.0, 100.0 * counts / population, np.nan)

    master_df = out.rename(
        columns={"state_name": "state", "district_name": "district", "block_name": "block"}
    )
    master_df = master_df.sort_values(_qa_key_cols(level)).reset_index(drop=True)

    qa_df = master_df.copy()
    qa_df["source_population_raster"] = str(population_raster)
    qa_df["source_raster_65plus"] = str(raster_65plus)
    qa_df["source_raster_under5"] = str(raster_under5)
    qa_df["raster_cell_count"] = master_df["__cell_count"].to_numpy()
    qa_df = qa_df.sort_values(_qa_key_cols(level)).reset_index(drop=True)
    return master_df, qa_df


def build_age_consistency_qa(
    district_master_df: pd.DataFrame,
    block_master_df: pd.DataFrame,
) -> pd.DataFrame:
    """Compare district age counts against the sum of their child blocks."""
    value_cols = [POPULATION_TOTAL_COL, AGE_65PLUS_COUNT_COL, AGE_UNDER5_COUNT_COL]
    district_totals = district_master_df[["state", "district"] + value_cols].rename(
        columns={col: f"district_{col}" for col in value_cols}
    )
    block_totals = (
        block_master_df.groupby(["state", "district"], as_index=False)[value_cols]
        .sum()
        .rename(columns={col: f"blocks_{col}" for col in value_cols})
    )
    qa = district_totals.merge(block_totals, on=["state", "district"], how="left")
    for col in value_cols:
        district_col = f"district_{col}"
        block_col = f"blocks_{col}"
        qa[block_col] = pd.to_numeric(qa[block_col], errors="coerce").fillna(0.0)
        qa[f"difference_abs_{col}"] = (
            pd.to_numeric(qa[district_col], errors="coerce").fillna(0.0) - qa[block_col]
        )
        qa[f"difference_pct_{col}"] = np.where(
            qa[district_col].abs() > 0,
            100.0 * qa[f"difference_abs_{col}"] / qa[district_col],
            np.nan,
        )
    return qa.sort_values(["state", "district"]).reset_index(drop=True)


def build_age_national_summary(
    district_master_df: pd.DataFrame,
    block_master_df: pd.DataFrame,
    *,
    derived_totals: dict[str, float],
) -> pd.DataFrame:
    """One row comparing raster totals against district and block aggregates."""

    def _sum(df: pd.DataFrame, col: str) -> float:
        return float(pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum())

    district_pop = _sum(district_master_df, POPULATION_TOTAL_COL)
    row = {
        "raster_65plus_total": derived_totals.get("population_age_65plus", float("nan")),
        "raster_under5_total": derived_totals.get("population_age_under5", float("nan")),
        "district_population_total": district_pop,
        "district_65plus_total": _sum(district_master_df, AGE_65PLUS_COUNT_COL),
        "district_under5_total": _sum(district_master_df, AGE_UNDER5_COUNT_COL),
        "block_population_total": _sum(block_master_df, POPULATION_TOTAL_COL),
        "block_65plus_total": _sum(block_master_df, AGE_65PLUS_COUNT_COL),
        "block_under5_total": _sum(block_master_df, AGE_UNDER5_COUNT_COL),
    }
    row["national_65plus_share_pct"] = (
        100.0 * row["district_65plus_total"] / district_pop if district_pop > 0 else float("nan")
    )
    row["national_under5_share_pct"] = (
        100.0 * row["district_under5_total"] / district_pop if district_pop > 0 else float("nan")
    )
    return pd.DataFrame([row])


def assert_age_guardrails(
    *,
    district_master_df: pd.DataFrame,
    block_master_df: Optional[pd.DataFrame],
    national_summary_df: pd.DataFrame,
    allow_count_outlier: bool,
    allow_share_outlier: bool,
) -> None:
    """Fail on an age count exceeding its population, or an implausible national share."""
    frames = [("district", district_master_df)]
    if block_master_df is not None:
        frames.append(("block", block_master_df))

    offenders: list[str] = []
    for level, frame in frames:
        population = pd.to_numeric(frame[POPULATION_TOTAL_COL], errors="coerce").fillna(0.0)
        for label, col in (("65+", AGE_65PLUS_COUNT_COL), ("under-5", AGE_UNDER5_COUNT_COL)):
            counts = pd.to_numeric(frame[col], errors="coerce").fillna(0.0)
            bad = counts > (population * COUNT_TOLERANCE_RATIO)
            if bool(bad.any()):
                sample = frame.loc[bad, _qa_key_cols(level)].head(5).to_dict(orient="records")
                offenders.append(
                    f"{level}/{label}: {int(bad.sum())} unit(s) with an age count above "
                    f"total population, e.g. {sample}"
                )
            negative = counts < 0.0
            if bool(negative.any()):
                offenders.append(f"{level}/{label}: {int(negative.sum())} unit(s) with a negative count")

    if offenders and not allow_count_outlier:
        raise ValueError(
            "Age-structure count guardrail failed:\n  - " + "\n  - ".join(offenders)
            + "\nRe-run with --allow-count-outlier only if this is understood and intended."
        )

    share_problems: list[str] = []
    for slug, (low, high) in NATIONAL_SHARE_BOUNDS.items():
        column = "national_65plus_share_pct" if "65plus" in slug else "national_under5_share_pct"
        value = float(national_summary_df.iloc[0][column])
        if not (low <= value <= high):
            share_problems.append(f"{slug}: national share {value:.3f}% outside [{low}, {high}]")
    if share_problems and not allow_share_outlier:
        raise ValueError(
            "National age-share guardrail failed:\n  - " + "\n  - ".join(share_problems)
            + "\nA share outside these bounds usually means a wrong denominator or a "
              "dropped band. Re-run with --allow-share-outlier only if verified."
        )


def _write_csv(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file without --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_master_table(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    """Write a master CSV plus a Parquet companion for faster runtime reads."""
    parquet_path = path.with_suffix(".parquet")
    if not overwrite:
        existing = [str(p) for p in (path, parquet_path) if p.exists()]
        if existing:
            raise FileExistsError(
                f"Refusing to overwrite existing file without --overwrite: {', '.join(existing)}"
            )
    _write_csv(df, path, overwrite=True)
    df.to_parquet(parquet_path, index=False)


def metric_specific_master(
    master_df: pd.DataFrame, *, level: AdminLevel, metric_slug: str
) -> pd.DataFrame:
    """Slice the wide master down to one metric's published columns."""
    if metric_slug not in METRIC_COLUMNS:
        raise ValueError(f"Unsupported age-structure metric slug: {metric_slug}")
    identity = (
        ["state", "district", "block", "block_key"]
        if level == "block"
        else ["state", "district", "district_key"]
    )
    keep = identity + [_area_col(level), METRIC_COLUMNS[metric_slug]]
    return master_df[keep].copy()


def _write_state_slices(
    master_df: pd.DataFrame,
    *,
    metric_slug: str,
    level: AdminLevel,
    overwrite: bool,
) -> dict[str, int]:
    processed_root = resolve_processed_root(
        metric_slug, data_dir=get_paths_config().data_dir, mode="portfolio"
    )
    out_name = get_master_csv_filename(level)
    counts: dict[str, int] = {}
    for state_name, state_df in master_df.groupby("state", dropna=False, as_index=False):
        state_label = str(state_name or "").strip()
        if not state_label:
            raise ValueError(f"Age-structure {level} master contains an empty state value.")
        out_path = processed_root / state_label / out_name
        _write_master_table(state_df.reset_index(drop=True), out_path, overwrite=overwrite)
        counts[state_label] = int(state_df.shape[0])
    return counts


def build_age_structure_admin_outputs(
    *,
    band_dir: Path,
    derived_dir: Path,
    population_raster: Path,
    districts_path: Path,
    blocks_path: Path,
    qa_dir: Path,
    overwrite: bool,
    dry_run: bool,
    allow_count_outlier: bool = False,
    allow_share_outlier: bool = False,
) -> dict[str, object]:
    """Build the full district + block age-structure outputs."""
    band_paths = expected_band_paths(band_dir)
    derived = {
        "population_age_65plus": build_derived_age_raster(
            band_paths["population_age_65plus"],
            derived_dir / DERIVED_65PLUS_NAME,
            overwrite=overwrite,
            dry_run=dry_run,
        ),
        "population_age_under5": build_derived_age_raster(
            band_paths["population_age_under5"],
            derived_dir / DERIVED_UNDER5_NAME,
            overwrite=overwrite,
            dry_run=dry_run,
        ),
    }

    raster_65plus = derived_dir / DERIVED_65PLUS_NAME
    raster_under5 = derived_dir / DERIVED_UNDER5_NAME
    if dry_run and not (raster_65plus.exists() and raster_under5.exists()):
        # Nothing to aggregate against yet; report what the run would produce.
        return {
            "derived": derived,
            "district_master_df": pd.DataFrame(),
            "block_master_df": pd.DataFrame(),
            "consistency_qa_df": pd.DataFrame(),
            "national_summary_df": pd.DataFrame(),
            "district_counts": {},
            "block_counts": {},
            "aggregated": False,
        }

    district_gdf = load_district_boundaries(districts_path)
    block_gdf = load_block_boundaries(blocks_path)

    district_master_df, district_qa_df = aggregate_age_structure_to_admin_units(
        district_gdf,
        level="district",
        population_raster=population_raster,
        raster_65plus=raster_65plus,
        raster_under5=raster_under5,
    )
    block_master_df, block_qa_df = aggregate_age_structure_to_admin_units(
        block_gdf,
        level="block",
        population_raster=population_raster,
        raster_65plus=raster_65plus,
        raster_under5=raster_under5,
    )

    consistency_qa_df = build_age_consistency_qa(district_master_df, block_master_df)
    national_summary_df = build_age_national_summary(
        district_master_df,
        block_master_df,
        derived_totals={k: float(v["national_total"]) for k, v in derived.items()},
    )

    assert_age_guardrails(
        district_master_df=district_master_df,
        block_master_df=block_master_df,
        national_summary_df=national_summary_df,
        allow_count_outlier=allow_count_outlier,
        allow_share_outlier=allow_share_outlier,
    )

    if not dry_run:
        district_counts: dict[str, int] = {}
        block_counts: dict[str, int] = {}
        for slug in METRIC_COLUMNS:
            district_counts = _write_state_slices(
                metric_specific_master(district_master_df, level="district", metric_slug=slug),
                metric_slug=slug,
                level="district",
                overwrite=overwrite,
            )
            block_counts = _write_state_slices(
                metric_specific_master(block_master_df, level="block", metric_slug=slug),
                metric_slug=slug,
                level="block",
                overwrite=overwrite,
            )
        _write_csv(district_qa_df, qa_dir / "agesex_district_master_qa.csv", overwrite=overwrite)
        _write_csv(block_qa_df, qa_dir / "agesex_block_master_qa.csv", overwrite=overwrite)
        _write_csv(consistency_qa_df, qa_dir / "agesex_district_vs_blocks_qa.csv", overwrite=overwrite)
        _write_csv(national_summary_df, qa_dir / "agesex_national_summary.csv", overwrite=overwrite)
    else:
        district_counts = (
            district_master_df.groupby("state", as_index=False).size().set_index("state")["size"].astype(int).to_dict()
        )
        block_counts = (
            block_master_df.groupby("state", as_index=False).size().set_index("state")["size"].astype(int).to_dict()
        )

    return {
        "derived": derived,
        "district_master_df": district_master_df,
        "block_master_df": block_master_df,
        "district_qa_df": district_qa_df,
        "block_qa_df": block_qa_df,
        "consistency_qa_df": consistency_qa_df,
        "national_summary_df": national_summary_df,
        "district_counts": district_counts,
        "block_counts": block_counts,
        "aggregated": True,
    }


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build canonical district and block age-structure masters (65+ and under-5) "
            "from the WorldPop 2025 age-sex bands."
        )
    )
    parser.add_argument("--band-dir", type=str, default=str(_default_band_dir()),
                        help="Directory holding the extracted WorldPop age-sex band GeoTIFFs.")
    parser.add_argument("--derived-dir", type=str, default=str(_default_derived_dir()),
                        help="Directory for the summed 65+ / under-5 rasters.")
    parser.add_argument("--population-raster", type=str, default=str(_find_default_population_raster()),
                        help="Population raster used as the share denominator.")
    parser.add_argument("--districts", type=str, default=str(get_paths_config().districts_path))
    parser.add_argument("--blocks", type=str, default=str(get_paths_config().blocks_path))
    parser.add_argument("--qa-dir", type=str, default=str(_default_qa_dir()))
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate inputs and report planned outputs without writing masters.")
    parser.add_argument("--allow-count-outlier", action="store_true",
                        help="Allow an age count above a unit's total population.")
    parser.add_argument("--allow-share-outlier", action="store_true",
                        help="Allow a national age share outside the plausibility bounds.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_cli()
    args = parser.parse_args(argv)

    band_dir = Path(args.band_dir).expanduser().resolve()
    derived_dir = Path(args.derived_dir).expanduser().resolve()
    population_raster = Path(args.population_raster).expanduser().resolve()
    districts_path = Path(args.districts).expanduser().resolve()
    blocks_path = Path(args.blocks).expanduser().resolve()
    qa_dir = Path(args.qa_dir).expanduser().resolve()

    if not band_dir.exists():
        parser.error(f"Band directory not found: {band_dir}")
    if not population_raster.exists():
        parser.error(f"Population raster not found: {population_raster}")
    if not districts_path.exists():
        parser.error(f"District boundaries not found: {districts_path}")
    if not blocks_path.exists():
        parser.error(f"Block boundaries not found: {blocks_path}")

    outputs = build_age_structure_admin_outputs(
        band_dir=band_dir,
        derived_dir=derived_dir,
        population_raster=population_raster,
        districts_path=districts_path,
        blocks_path=blocks_path,
        qa_dir=qa_dir,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        allow_count_outlier=bool(args.allow_count_outlier),
        allow_share_outlier=bool(args.allow_share_outlier),
    )

    mode = "DRY RUN" if args.dry_run else "WROTE"
    print(f"[{mode}] age-structure admin masters")
    for group, info in outputs["derived"].items():
        print(f"  derived {group}: {info['band_count']} bands -> {info['path']}")
        print(f"    national total = {info['national_total']:,.0f}")

    if not outputs.get("aggregated"):
        print("  derived rasters absent under --dry-run; skipped admin aggregation.")
        return 0

    summary = outputs["national_summary_df"].iloc[0]
    print(f"  districts: {len(outputs['district_master_df']):,}   blocks: {len(outputs['block_master_df']):,}")
    print(f"  national 65+    share = {summary['national_65plus_share_pct']:.3f}%")
    print(f"  national under5 share = {summary['national_under5_share_pct']:.3f}%")
    print(f"  district population total = {summary['district_population_total']:,.0f}")
    print(f"  block    population total = {summary['block_population_total']:,.0f}")
    print("  metric slugs: " + ", ".join(sorted(METRIC_COLUMNS)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

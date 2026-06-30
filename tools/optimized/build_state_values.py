"""
Precompute area-weighted state headline values into the optimized bundle.

For each metric slug × admin level, this tool reproduces the dashboard's state
headline KPI (the area-weighted mean shown when a state is selected but no
district/block is) and writes it to disk as a tidy long table:

    metrics/<slug>/state_values/admin/<level>/all_states.parquet
      columns: state, metric, scenario, period, stat, value, n_units

Parity is guaranteed *by construction*: the merged frame is built via the single
canonical path ``data.merge.get_or_build_merged_for_index_cached`` (the same
helper the app uses), fed by the Streamlit-free boundary loaders with the same
bbox + ``min_area`` constants, and aggregated by the shared
``analysis.area_weighting.weighted_state_mean``. The value/baseline rows are
therefore methodology-neutral; the all-states table additionally enables a real
"Position in India" rank where the single-state live path renders N/A.

This module is Streamlit-free. It must never import ``app/`` modules (several of
them import Streamlit eagerly).

Usage:
    python -m tools.optimized.build_state_values --help
    python -m tools.optimized.build_state_values --dry-run
    python -m tools.optimized.build_state_values --metric tmax_days_gt_35c --level district

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

from india_resilience_tool.analysis.area_weighting import (
    weighted_state_mean,
    with_area_weights,
)
from india_resilience_tool.config.constants import (
    ADM2_MIN_AREA,
    ADM3_MIN_AREA,
    MAX_LAT,
    MAX_LON,
    MIN_LAT,
    MIN_LON,
    SIMPLIFY_TOL_ADM2,
    SIMPLIFY_TOL_ADM3,
)
from india_resilience_tool.data.adm2_loader import load_local_adm2
from india_resilience_tool.data.adm3_loader import load_local_adm3
from india_resilience_tool.data.master_loader import load_master_csvs
from india_resilience_tool.data.merge import get_or_build_merged_for_index_cached
from india_resilience_tool.data.optimized_bundle import (
    list_optimized_states_for_metric_root,
    optimized_geometry_path,
    optimized_master_path_from_metric_root,
    optimized_state_values_path_from_metric_root,
    resolve_optimized_bundle_root,
    resolve_optimized_metric_root,
)
from india_resilience_tool.utils.naming import alias

LOGGER = logging.getLogger("build_state_values")

ADMIN_LEVELS: tuple[str, ...] = ("district", "block")

# Non-numeric list/string companion stats that parse into 4 parts but are never
# an aggregatable headline value. Everything else (mean/median/std/p05/p95 and
# composite/proposal score stats like score/abs_score/chg_score/imp_score) is
# attempted; weighted_state_mean coerces to numeric and yields (None, 0) for any
# column that is not numeric, which is harmless (the app only ever reads the
# exact stat it renders).
_SKIP_STATS: frozenset[str] = frozenset({"models", "values_per_model"})

_BBOX = (MIN_LON, MIN_LAT, MAX_LON, MAX_LAT)

_OUTPUT_COLUMNS = ["state", "metric", "scenario", "period", "stat", "value", "n_units"]


def _parse_value_columns(columns) -> list[tuple[str, tuple[str, str, str, str]]]:
    """Return ``(column, (metric, scenario, period, stat))`` for value columns.

    A column qualifies only when it splits into exactly four non-empty
    ``__``-delimited parts and its stat is not a known non-numeric list companion.
    """
    out: list[tuple[str, tuple[str, str, str, str]]] = []
    for col in columns:
        parts = str(col).split("__")
        if len(parts) != 4:
            continue
        if not all(part.strip() for part in parts):
            continue
        if parts[3].strip().lower() in _SKIP_STATS:
            continue
        out.append((str(col), (parts[0], parts[1], parts[2], parts[3])))
    return out


def _boundary_signature(geojson_path: Path, tolerance: float, n_rows: int) -> tuple:
    """Build a well-formed boundary signature (mtime is best-effort)."""
    try:
        mtime: Optional[float] = geojson_path.stat().st_mtime
    except OSError:
        mtime = None
    return (str(geojson_path), mtime, float(tolerance), int(n_rows))


def _load_state_boundary(level: str, geojson_path: Path):
    """Load a single-state boundary shard with the app's bbox + min_area."""
    if level == "district":
        gdf = load_local_adm2(
            geojson_path,
            tolerance=SIMPLIFY_TOL_ADM2,
            bbox=_BBOX,
            min_area=ADM2_MIN_AREA,
        )
        return gdf, SIMPLIFY_TOL_ADM2
    gdf = load_local_adm3(
        geojson_path,
        tolerance=SIMPLIFY_TOL_ADM3,
        bbox=_BBOX,
        min_area=ADM3_MIN_AREA,
    )
    return gdf, SIMPLIFY_TOL_ADM3


def _join_miss_count(boundary_gdf, master_df: pd.DataFrame, level: str) -> int:
    """Count boundary units whose join key is absent from the master.

    This is a *true* join miss (id present in geometry, absent in master), which
    is distinct from a coverage cliff (key present, values legitimately NaN).
    Using the key set rather than a null-after-join mask avoids false alarms on
    arid all-NaN states (e.g. Banas_Kantha).
    """
    def _key(frame, cols: tuple[str, str]) -> pd.Series:
        left = frame[cols[0]].astype(str).map(alias)
        right = frame[cols[1]].astype(str).map(alias)
        return left.str.cat(right, sep="|")

    if level == "district":
        b_key = _key(boundary_gdf, ("state_name", "district_name"))
        m_key = _key(master_df, ("state", "district"))
    else:
        b_key = _key(boundary_gdf, ("district_name", "block_name"))
        m_key = _key(master_df, ("district", "block"))

    master_keys = set(m_key.tolist())
    return int((~b_key.isin(master_keys)).sum())


def _assert_area_present(merged: pd.DataFrame, *, label: str, strict: bool) -> None:
    """Fail (not silently geodesic-fallback) when area weights are missing.

    The optimized geometry shards carry an authoritative geodesic ``area_m2``;
    its absence means the bundle is malformed and the precompute must not paper
    over it with a recomputed area.
    """
    if "area_m2" not in merged.columns:
        raise ValueError(
            f"{label}: merged frame has no 'area_m2' column; the optimized "
            f"geometry shard must carry precomputed geodesic areas."
        )
    area = pd.to_numeric(merged["area_m2"], errors="coerce")
    bad = int((area.isna() | (area <= 0)).sum())
    if bad:
        msg = f"{label}: {bad}/{len(merged)} units have missing/non-positive area_m2."
        if strict:
            raise ValueError(msg)
        LOGGER.warning("%s", msg)


def _build_state_rows(
    *,
    slug: str,
    level: str,
    state: str,
    metric_root: Path,
    data_dir: Optional[Path],
    strict: bool,
) -> list[dict[str, object]]:
    """Compute all (metric, scenario, period, stat) value rows for one state."""
    geojson_path = optimized_geometry_path(level=level, state=state, data_dir=data_dir)
    if not geojson_path.exists():
        msg = f"[{slug}/{level}] geometry shard missing for state={state!r}: {geojson_path}"
        if strict:
            raise FileNotFoundError(msg)
        LOGGER.warning("%s — skipping state", msg)
        return []

    master_path = optimized_master_path_from_metric_root(metric_root, level=level, state=state)
    if not master_path.exists():
        msg = f"[{slug}/{level}] master shard missing for state={state!r}: {master_path}"
        if strict:
            raise FileNotFoundError(msg)
        LOGGER.warning("%s — skipping state", msg)
        return []

    master_df = load_master_csvs(master_path)
    if master_df.empty:
        LOGGER.warning("[%s/%s] empty master for state=%r — skipping", slug, level, state)
        return []

    boundary_gdf, tolerance = _load_state_boundary(level, geojson_path)
    if boundary_gdf is None or boundary_gdf.empty:
        LOGGER.warning("[%s/%s] empty boundary for state=%r — skipping", slug, level, state)
        return []

    boundary_sig = _boundary_signature(geojson_path, tolerance, len(boundary_gdf))

    merged = get_or_build_merged_for_index_cached(
        boundary_gdf,
        master_df,
        slug=slug,
        master_path=master_path,
        boundary_signature=boundary_sig,
        session_state={},
        alias_fn=alias,
        level=level,
        adm2_state_col="state_name",
        master_state_col="state",
    )

    _assert_area_present(merged, label=f"[{slug}/{level}] state={state!r}", strict=strict)

    missed = _join_miss_count(boundary_gdf, master_df, level)
    if missed:
        msg = f"[{slug}/{level}] state={state!r}: {missed} boundary unit(s) had no master join."
        if strict:
            raise ValueError(msg)
        LOGGER.warning("%s", msg)

    weighted = with_area_weights(merged)
    # The shard is single-state by construction; filter defensively so a stray
    # row can never leak into another state's aggregate.
    state_key = alias(state)
    if "state_name" in weighted.columns:
        keep = weighted["state_name"].astype(str).map(alias) == state_key
        weighted = weighted[keep].copy()

    rows: list[dict[str, object]] = []
    for col, (metric, scenario, period, stat) in _parse_value_columns(merged.columns):
        value, n_units = weighted_state_mean(weighted, col)
        rows.append(
            {
                "state": state,
                "metric": metric,
                "scenario": scenario,
                "period": period,
                "stat": stat,
                "value": value,
                "n_units": int(n_units),
            }
        )
    return rows


def build_state_values_for_metric(
    *,
    slug: str,
    level: str,
    data_dir: Optional[Path],
    states: Optional[list[str]],
    strict: bool,
    dry_run: bool,
) -> Optional[Path]:
    """Build and write the all-states value table for one slug/level.

    Returns the written path, or ``None`` when nothing was produced / dry-run.
    """
    metric_root = resolve_optimized_metric_root(slug, data_dir=data_dir)
    available = list_optimized_states_for_metric_root(metric_root, level=level)
    if not available:
        LOGGER.info("[%s/%s] no master state shards — nothing to do", slug, level)
        return None

    target_states = available if not states else [s for s in available if s in set(states)]
    if not target_states:
        LOGGER.info("[%s/%s] none of the requested states are present", slug, level)
        return None

    all_rows: list[dict[str, object]] = []
    for state in target_states:
        all_rows.extend(
            _build_state_rows(
                slug=slug,
                level=level,
                state=state,
                metric_root=metric_root,
                data_dir=data_dir,
                strict=strict,
            )
        )

    if not all_rows:
        LOGGER.warning("[%s/%s] produced no value rows", slug, level)
        return None

    out_df = pd.DataFrame(all_rows, columns=_OUTPUT_COLUMNS)
    out_path = optimized_state_values_path_from_metric_root(metric_root, level=level)

    if dry_run:
        LOGGER.info(
            "[%s/%s] DRY-RUN: would write %d rows across %d states -> %s",
            slug,
            level,
            len(out_df),
            out_df["state"].nunique(),
            out_path,
        )
        return None

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_path, index=False)
    LOGGER.info(
        "[%s/%s] wrote %d rows across %d states -> %s",
        slug,
        level,
        len(out_df),
        out_df["state"].nunique(),
        out_path,
    )
    return out_path


def _discover_slugs(data_dir: Optional[Path], metric_filter: Optional[list[str]]) -> list[str]:
    """List metric slugs present in the optimized bundle (optionally filtered)."""
    metrics_dir = resolve_optimized_bundle_root(data_dir=data_dir) / "metrics"
    if not metrics_dir.is_dir():
        return []
    slugs = sorted(p.name for p in metrics_dir.iterdir() if p.is_dir())
    if metric_filter:
        wanted = set(metric_filter)
        slugs = [s for s in slugs if s in wanted]
    return slugs


def _resolve_levels(level_arg: str) -> list[str]:
    level_norm = str(level_arg).strip().lower()
    if level_norm == "all":
        return list(ADMIN_LEVELS)
    if level_norm not in ADMIN_LEVELS:
        raise SystemExit(f"--level must be one of district|block|all, got {level_arg!r}")
    return [level_norm]


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.optimized.build_state_values",
        description="Precompute area-weighted state headline values into the optimized bundle.",
    )
    parser.add_argument(
        "--metric",
        action="append",
        default=None,
        help="Metric slug to process (repeatable). Default: all slugs in the bundle.",
    )
    parser.add_argument(
        "--level",
        default="all",
        help="Admin level: district | block | all (default: all).",
    )
    parser.add_argument(
        "--state",
        action="append",
        default=None,
        help="Restrict to one or more states (repeatable). Default: all available states.",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Override IRT data dir (where processed_optimised lives).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report only; write nothing.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail on missing shards, missing areas, or join misses.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    data_dir = Path(args.data_dir).expanduser().resolve() if args.data_dir else None
    levels = _resolve_levels(args.level)
    slugs = _discover_slugs(data_dir, args.metric)
    if not slugs:
        LOGGER.error("No metric slugs found in the optimized bundle.")
        return 1

    written: list[Path] = []
    for slug in slugs:
        for level in levels:
            out = build_state_values_for_metric(
                slug=slug,
                level=level,
                data_dir=data_dir,
                states=args.state,
                strict=args.strict,
                dry_run=args.dry_run,
            )
            if out is not None:
                written.append(out)

    LOGGER.info(
        "Done: %d slug(s) × %d level(s); %d file(s) %s.",
        len(slugs),
        len(levels),
        len(written),
        "would be written (dry-run)" if args.dry_run else "written",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

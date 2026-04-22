"""Offline builders for persisted visible-Glance composite metrics."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd

from india_resilience_tool.analysis.bundle_scores import BundleMetricSpec, compute_bundle_score_frame
from india_resilience_tool.app.geography import list_available_states_from_processed_root
from india_resilience_tool.config.composite_metrics import (
    COMPOSITES_BY_SLUG,
    VISIBLE_GLANCE_COMPOSITES,
    CompositeMetricSpec,
)
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG
from india_resilience_tool.config.paths import get_paths_config, resolve_processed_root
from india_resilience_tool.data.master_columns import resolve_metric_column
from india_resilience_tool.data.master_loader import (
    load_master_csv,
    normalize_master_columns,
    resolve_preferred_master_path,
)
from india_resilience_tool.utils.naming import alias


LEGACY_MASTER_FILENAMES = {
    "district": "master_metrics_by_district.csv",
    "block": "master_metrics_by_block.csv",
}
ID_COLUMNS_BY_LEVEL = {
    "district": ("state", "district", "district_key"),
    "block": ("state", "district", "block", "block_key"),
}
SUPPORTED_SCENARIOS = ("ssp245", "ssp585")
SUPPORTED_PERIODS = ("2020-2040", "2040-2060", "2060-2080")
SUPPORTED_STAT = "mean"


def _normalize_level(level: str) -> str:
    value = str(level or "").strip().lower()
    aliases = {"admin": "admin", "all": "admin"}
    if value in aliases:
        return aliases[value]
    if value not in {"district", "block"}:
        raise ValueError(f"Unsupported composite level selection: {level!r}")
    return value


def _level_selection(level: str) -> tuple[str, ...]:
    normalized = _normalize_level(level)
    if normalized == "admin":
        return ("district", "block")
    return (normalized,)


def _normalize_frame_identifiers(df: pd.DataFrame, *, level: str) -> pd.DataFrame:
    """Normalize master identifiers to the canonical admin contract."""
    out = df.copy()
    rename_map: dict[str, str] = {}
    if "state" not in out.columns and "state_name" in out.columns:
        rename_map["state_name"] = "state"
    if "district" not in out.columns and "district_name" in out.columns:
        rename_map["district_name"] = "district"
    if level == "block" and "block" not in out.columns and "block_name" in out.columns:
        rename_map["block_name"] = "block"
    if rename_map:
        out = out.rename(columns=rename_map)
    return out


def _ensure_required_id_columns(df: pd.DataFrame, *, level: str) -> pd.DataFrame:
    """Ensure canonical admin identifier columns exist, deriving keys when needed."""
    out = _normalize_frame_identifiers(df, level=level)
    if "state" in out.columns:
        out["state"] = out["state"].astype("string").fillna("").str.strip()
    if "district" in out.columns:
        out["district"] = out["district"].astype("string").fillna("").str.strip()
    if level == "block" and "block" in out.columns:
        out["block"] = out["block"].astype("string").fillna("").str.strip()

    if "district_key" not in out.columns and {"state", "district"}.issubset(out.columns):
        out["district_key"] = (
            out["state"].map(alias).astype("string").str.cat(out["district"].map(alias).astype("string"), sep="|")
        )
    if level == "block" and "block_key" not in out.columns and {"state", "district", "block"}.issubset(out.columns):
        out["block_key"] = (
            out["state"]
            .map(alias)
            .astype("string")
            .str.cat(out["district"].map(alias).astype("string"), sep="|")
            .str.cat(out["block"].map(alias).astype("string"), sep="|")
        )
    return out


def _required_id_columns(level: str) -> tuple[str, ...]:
    if level not in ID_COLUMNS_BY_LEVEL:
        raise ValueError(f"Unsupported composite level: {level!r}")
    return ID_COLUMNS_BY_LEVEL[level]


def _discover_states_for_spec(spec: CompositeMetricSpec, *, level: str, data_dir: Path) -> list[str]:
    """Discover admin states with available processed outputs for one composite spec."""
    states: list[str] = []
    seen: set[str] = set()
    for component_slug in spec.component_metric_slugs:
        processed_root = resolve_processed_root(component_slug, data_dir=data_dir, mode="portfolio")
        for state_name in list_available_states_from_processed_root(str(processed_root)):
            if state_name not in seen:
                seen.add(state_name)
                states.append(state_name)
    return states


def _resolve_state_paths(
    metric_slug: str,
    *,
    level: str,
    state_name: str,
    data_dir: Path,
) -> tuple[Path, Path]:
    """Return (source, target) master paths for one metric state partition."""
    metric_root = resolve_processed_root(metric_slug, data_dir=data_dir, mode="portfolio")
    source_path = metric_root / state_name / LEGACY_MASTER_FILENAMES[level]
    return source_path, source_path


def _load_component_master(
    metric_slug: str,
    *,
    level: str,
    state_name: str,
    data_dir: Path,
) -> Optional[pd.DataFrame]:
    source_path, _ = _resolve_state_paths(metric_slug, level=level, state_name=state_name, data_dir=data_dir)
    preferred = resolve_preferred_master_path(source_path)
    if not preferred.exists():
        return None
    frame = normalize_master_columns(load_master_csv(preferred))
    frame = _ensure_required_id_columns(frame, level=level)
    required = set(_required_id_columns(level))
    if not required.issubset(frame.columns):
        return None
    return frame


def _available_pairs_for_frame(df: pd.DataFrame, *, metric_slug: str) -> set[tuple[str, str]]:
    """Return supported scenario-period pairs for one master frame."""
    metric_base = METRICS_BY_SLUG[metric_slug].periods_metric_col or METRICS_BY_SLUG[metric_slug].value_col or metric_slug
    available: set[tuple[str, str]] = set()
    for scenario in SUPPORTED_SCENARIOS:
        for period in SUPPORTED_PERIODS:
            if resolve_metric_column(df, metric_base, scenario, period, SUPPORTED_STAT):
                available.add((scenario, period))
    return available


def _intersect_available_pairs(component_frames: dict[str, pd.DataFrame]) -> list[tuple[str, str]]:
    """Return schema-level scenario-period intersections across all component frames."""
    pair_sets: list[set[tuple[str, str]]] = []
    for metric_slug, frame in component_frames.items():
        pair_sets.append(_available_pairs_for_frame(frame, metric_slug=metric_slug))
    if not pair_sets:
        return []
    available = set.intersection(*pair_sets)
    return [
        (scenario, period)
        for scenario in SUPPORTED_SCENARIOS
        for period in SUPPORTED_PERIODS
        if (scenario, period) in available
    ]


def _bundle_metric_specs(spec: CompositeMetricSpec) -> list[BundleMetricSpec]:
    """Return weighted component specs for composite computation."""
    from india_resilience_tool.config.bundle_weights import get_bundle_weights

    weights = {entry.metric_slug: float(entry.weight) for entry in get_bundle_weights(spec.bundle_domain)}
    return [
        BundleMetricSpec(
            slug=metric_slug,
            label=METRICS_BY_SLUG[metric_slug].label,
            column=metric_slug,
            weight=weights[metric_slug],
            higher_is_worse=bool(METRICS_BY_SLUG[metric_slug].rank_higher_is_worse),
        )
        for metric_slug in spec.component_metric_slugs
    ]


def _build_wide_component_frame(
    component_frames: dict[str, pd.DataFrame],
    *,
    level: str,
    scenario: str,
    period: str,
) -> pd.DataFrame:
    """Merge component metric values into one wide frame for one pair."""
    id_columns = list(_required_id_columns(level))
    merged: Optional[pd.DataFrame] = None
    for metric_slug, frame in component_frames.items():
        metric_base = (
            METRICS_BY_SLUG[metric_slug].periods_metric_col
            or METRICS_BY_SLUG[metric_slug].value_col
            or metric_slug
        )
        metric_column = resolve_metric_column(frame, metric_base, scenario, period, SUPPORTED_STAT)
        metric_frame = frame.loc[:, id_columns].copy()
        metric_frame[metric_slug] = (
            pd.to_numeric(frame[metric_column], errors="coerce") if metric_column in frame.columns else pd.NA
        )
        if merged is None:
            merged = metric_frame
        else:
            merged = merged.merge(metric_frame, on=id_columns, how="outer")
    return merged if merged is not None else pd.DataFrame(columns=id_columns)


def compute_composite_master_frame(
    spec: CompositeMetricSpec,
    *,
    level: str,
    state_name: str,
    data_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """Compute one persisted composite master frame for a bundle/level/state.

    Methodology:
    - Scenario-period availability is based on schema intersection across all component masters.
    - Row-level partial values are allowed.
    - Weights are renormalized across available values per row.
    - Rows with all component values missing remain NaN for the composite column.
    """
    if data_dir is None:
        data_dir = get_paths_config().data_dir
    level_norm = _normalize_level(level)
    if level_norm == "admin":
        raise ValueError("compute_composite_master_frame requires a concrete level, not 'admin'.")

    component_frames: dict[str, pd.DataFrame] = {}
    for metric_slug in spec.component_metric_slugs:
        frame = _load_component_master(metric_slug, level=level_norm, state_name=state_name, data_dir=data_dir)
        if frame is None or frame.empty:
            return pd.DataFrame(columns=list(_required_id_columns(level_norm)))
        component_frames[metric_slug] = frame

    available_pairs = _intersect_available_pairs(component_frames)
    id_columns = list(_required_id_columns(level_norm))
    if not available_pairs:
        return next(iter(component_frames.values()))[id_columns].drop_duplicates().reset_index(drop=True)

    output = next(iter(component_frames.values()))[id_columns].drop_duplicates().reset_index(drop=True)
    bundle_metric_specs = _bundle_metric_specs(spec)
    for scenario, period in available_pairs:
        wide = _build_wide_component_frame(component_frames, level=level_norm, scenario=scenario, period=period)
        score_frame = compute_bundle_score_frame(
            wide,
            metric_specs=bundle_metric_specs,
            id_columns=id_columns,
        )
        score_column = f"{spec.composite_slug}__{scenario}__{period}__{SUPPORTED_STAT}"
        pair_frame = score_frame[id_columns + ["bundle_score"]].rename(columns={"bundle_score": score_column})
        output = output.merge(pair_frame, on=id_columns, how="left")

    return output


def _write_composite_master_frame(
    df: pd.DataFrame,
    *,
    spec: CompositeMetricSpec,
    level: str,
    state_name: str,
    data_dir: Path,
    overwrite: bool,
    dry_run: bool,
) -> Optional[Path]:
    """Write one composite master frame to CSV and Parquet companion."""
    target_root = resolve_processed_root(spec.composite_slug, data_dir=data_dir, mode="portfolio")
    target_path = target_root / state_name / LEGACY_MASTER_FILENAMES[level]
    if dry_run:
        return target_path
    if target_path.exists() and not overwrite:
        return None
    target_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target_path, index=False)
    df.to_parquet(target_path.with_suffix(".parquet"), index=False)
    return target_path


def build_composite_metrics(
    *,
    levels: Sequence[str],
    states: Optional[Sequence[str]] = None,
    composite_slugs: Optional[Sequence[str]] = None,
    data_dir: Optional[Path] = None,
    overwrite: bool = False,
    dry_run: bool = False,
    quiet: bool = False,
) -> list[Path]:
    """Build persisted composite metric masters for visible Glance bundles."""
    if data_dir is None:
        data_dir = get_paths_config().data_dir

    requested_levels: list[str] = []
    for level in (levels or ("admin",)):
        requested_levels.extend(_level_selection(level))
    levels_resolved = tuple(dict.fromkeys(requested_levels))

    if composite_slugs:
        specs = []
        for slug in composite_slugs:
            spec = COMPOSITES_BY_SLUG.get(str(slug).strip())
            if spec is None:
                raise ValueError(f"Unsupported composite metric selection: {slug!r}")
            specs.append(spec)
    else:
        specs = list(VISIBLE_GLANCE_COMPOSITES)

    written: list[Path] = []
    for spec in specs:
        requested_states = [str(state).strip() for state in states or () if str(state).strip()]
        if not requested_states:
            requested_states = _discover_states_for_spec(spec, level=levels_resolved[0], data_dir=data_dir)
        for level in levels_resolved:
            for state_name in requested_states:
                frame = compute_composite_master_frame(
                    spec,
                    level=level,
                    state_name=state_name,
                    data_dir=data_dir,
                )
                target = _write_composite_master_frame(
                    frame,
                    spec=spec,
                    level=level,
                    state_name=state_name,
                    data_dir=data_dir,
                    overwrite=overwrite,
                    dry_run=dry_run,
                )
                if target is not None:
                    written.append(target)
                    if not quiet:
                        print(f"[composite] wrote {target}")
    return written


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI flags for the composite metric builder."""
    parser = argparse.ArgumentParser(description="Build persisted visible-Glance composite metric masters.")
    parser.add_argument(
        "--level",
        action="append",
        default=None,
        help="Composite output level: district, block, admin, or all. Default: admin.",
    )
    parser.add_argument(
        "--state",
        action="append",
        default=None,
        help="Optional repeatable admin state filter.",
    )
    parser.add_argument(
        "--metric",
        action="append",
        default=None,
        help="Optional repeatable composite metric slug filter.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Rewrite existing composite outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned composite outputs without writing.")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-file success logging.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entrypoint for persisted composite metric building."""
    args = parse_args(argv)
    build_composite_metrics(
        levels=args.level,
        states=args.state,
        composite_slugs=args.metric,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        quiet=bool(args.quiet),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

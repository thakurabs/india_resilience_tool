"""Offline builders for persisted visible-Glance composite metrics."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
import numpy as np

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
SUPPORTED_SCENARIOS = ("ssp245", "ssp585", "snapshot")
SUPPORTED_PERIODS = ("1990-2010", "2020-2040", "2040-2060", "2060-2080", "Current")
SUPPORTED_STAT = "mean"
RETIRED_COMPOSITE_SLUGS = frozenset({"composite_agriculture_growing_conditions"})


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
    available: set[tuple[str, str]] = set()
    for scenario in SUPPORTED_SCENARIOS:
        for period in SUPPORTED_PERIODS:
            col = _resolve_component_metric_column(
                df,
                metric_slug=metric_slug,
                scenario=scenario,
                period=period,
            )
            # Guard against fuzzy-fallback false positives: verify the resolved
            # column actually encodes the expected period token.
            if col and f"__{period.lower()}__" in col.lower():
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


def _resolve_component_metric_column(
    frame: pd.DataFrame,
    *,
    metric_slug: str,
    scenario: str,
    period: str,
) -> str | None:
    """Resolve one component metric column with legacy slug fallback.

    Persisted masters normally use the registry `periods_metric_col` / `value_col`
    base. Some older fixtures and legacy outputs still expose the raw metric slug
    as the left-most token, so we accept that as a compatibility fallback.
    """
    registry_spec = METRICS_BY_SLUG[metric_slug]
    candidates: list[str] = []
    for candidate in (
        registry_spec.periods_metric_col,
        registry_spec.value_col,
        metric_slug,
    ):
        value = str(candidate or "").strip()
        if value and value not in candidates:
            candidates.append(value)
    for candidate in candidates:
        resolved = resolve_metric_column(frame, candidate, scenario, period, SUPPORTED_STAT)
        if resolved:
            return resolved
    return None


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
        metric_column = _resolve_component_metric_column(
            frame,
            metric_slug=metric_slug,
            scenario=scenario,
            period=period,
        )
        metric_frame = frame.loc[:, id_columns].copy()
        metric_frame[metric_slug] = (
            pd.to_numeric(frame[metric_column], errors="coerce") if metric_column in frame.columns else pd.NA
        )
        if merged is None:
            merged = metric_frame
        else:
            merged = merged.merge(metric_frame, on=id_columns, how="outer")
    return merged if merged is not None else pd.DataFrame(columns=id_columns)


def _normalize_with_anchor(values: pd.Series, anchor_values: pd.Series, *, higher_is_worse: bool) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    anchor = pd.to_numeric(anchor_values, errors="coerce")
    finite_anchor = anchor[np.isfinite(anchor)]
    out = pd.Series(np.nan, index=numeric.index, dtype=float)
    if finite_anchor.empty:
        return out
    lo = float(finite_anchor.min())
    hi = float(finite_anchor.max())
    finite = numeric[np.isfinite(numeric)]
    if finite.empty:
        return out
    if hi == lo:
        out.loc[finite.index] = 50.0
        return out
    scaled = (finite - lo) / (hi - lo)
    if not higher_is_worse:
        scaled = 1.0 - scaled
    out.loc[finite.index] = (scaled * 100.0).clip(0.0, 100.0)
    return out


def _compute_baseline_anchored_score_frame(
    wide: pd.DataFrame,
    *,
    anchor_wide: pd.DataFrame,
    metric_specs: list[BundleMetricSpec],
    id_columns: list[str],
    min_components: int,
) -> pd.DataFrame:
    out = wide.loc[:, [col for col in id_columns if col in wide.columns]].copy()
    normalized_cols: list[str] = []
    weights: list[float] = []
    for spec in metric_specs:
        if spec.slug not in wide.columns or spec.slug not in anchor_wide.columns:
            continue
        norm_col = f"{spec.slug}__landing_norm"
        out[norm_col] = _normalize_with_anchor(
            wide[spec.slug],
            anchor_wide[spec.slug],
            higher_is_worse=bool(spec.higher_is_worse),
        )
        normalized_cols.append(norm_col)
        weights.append(float(spec.weight))
    if not normalized_cols:
        out["bundle_score"] = np.nan
        out["available_metric_count"] = 0
        return out
    norm_frame = out[normalized_cols]
    weight_series = pd.Series(weights, index=normalized_cols, dtype=float)
    available_weights = norm_frame.notna().mul(weight_series, axis=1).sum(axis=1)
    weighted_sum = norm_frame.mul(weight_series, axis=1).sum(axis=1, skipna=True)
    out["available_metric_count"] = norm_frame.notna().sum(axis=1).astype(int)
    out["bundle_score"] = weighted_sum.div(available_weights.where(available_weights > 0.0))
    out.loc[out["available_metric_count"] < int(min_components), "bundle_score"] = np.nan
    return out


def _ordinal_class_bounds(metric_slug: str) -> tuple[int, int]:
    """Return (min_code, max_code) for a scored ordinal component, or raise.

    The ``pre_scaled_ordinal`` composite mode maps integer class codes to an
    absolute 0-100 score. It requires every scored (weight>0) component to carry a
    contiguous integer ``class_labels`` mapping in the registry (e.g. ``{1: ...,
    2: ..., 3: ..., 4: ...}``). This blocks a future continuous component from
    silently riding the ordinal mapping; such metrics must use a different mode.
    """
    spec = METRICS_BY_SLUG.get(metric_slug)
    labels = getattr(spec, "class_labels", None) if spec is not None else None
    if not labels:
        raise ValueError(
            "pre_scaled_ordinal composite requires integer class_labels for scored "
            f"component {metric_slug!r}, but none are defined in the registry."
        )
    try:
        codes = sorted(int(key) for key in labels.keys())
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"pre_scaled_ordinal composite component {metric_slug!r} has non-integer "
            f"class_labels keys: {list(labels.keys())!r}."
        ) from exc
    if len(codes) < 2 or codes != list(range(codes[0], codes[0] + len(codes))):
        raise ValueError(
            f"pre_scaled_ordinal composite component {metric_slug!r} must have contiguous "
            f"integer class_labels; got {codes!r}."
        )
    return codes[0], codes[-1]


def _pre_scaled_ordinal_series(
    values: pd.Series,
    *,
    min_code: int,
    max_code: int,
    higher_is_worse: bool,
) -> pd.Series:
    """Map ordinal class codes onto an absolute 0-100 scale (min->0, max->100).

    Unlike the default per-period min-max normalization, this mapping is data
    independent, so the same class always yields the same score in every state.
    Missing/non-numeric values become NaN.
    """
    numeric = pd.to_numeric(values, errors="coerce")
    out = pd.Series(np.nan, index=numeric.index, dtype=float)
    finite = numeric[np.isfinite(numeric)]
    if finite.empty or max_code == min_code:
        return out
    scaled = (finite - float(min_code)) / (float(max_code) - float(min_code))
    if not higher_is_worse:
        scaled = 1.0 - scaled
    out.loc[finite.index] = (scaled * 100.0).clip(0.0, 100.0)
    return out


def _compute_pre_scaled_ordinal_score_frame(
    wide: pd.DataFrame,
    *,
    metric_specs: list[BundleMetricSpec],
    id_columns: list[str],
) -> pd.DataFrame:
    """Score a composite from fixed ordinal classes (absolute pre-scaled mapping).

    Every scored (weight>0) component is validated to carry contiguous integer
    class_labels (raising otherwise), then mapped 1..k -> 0..100 before the
    existing per-row weighted mean.
    """
    out = wide.loc[:, [col for col in id_columns if col in wide.columns]].copy()
    normalized_cols: list[str] = []
    weights: list[float] = []
    for spec in metric_specs:
        if float(spec.weight) <= 0.0:
            continue  # attributes are excluded upstream; guard the scored contract anyway
        min_code, max_code = _ordinal_class_bounds(spec.slug)
        if spec.column not in wide.columns:
            continue
        norm_col = f"{spec.slug}__landing_norm"
        out[norm_col] = _pre_scaled_ordinal_series(
            wide[spec.column],
            min_code=min_code,
            max_code=max_code,
            higher_is_worse=bool(spec.higher_is_worse),
        )
        normalized_cols.append(norm_col)
        weights.append(float(spec.weight))
    if not normalized_cols:
        out["bundle_score"] = np.nan
        out["available_metric_count"] = 0
        return out
    norm_frame = out[normalized_cols]
    weight_series = pd.Series(weights, index=normalized_cols, dtype=float)
    available_weights = norm_frame.notna().mul(weight_series, axis=1).sum(axis=1)
    weighted_sum = norm_frame.mul(weight_series, axis=1).sum(axis=1, skipna=True)
    out["bundle_score"] = weighted_sum.div(available_weights.where(available_weights > 0.0))
    out["available_metric_count"] = norm_frame.notna().sum(axis=1).astype(int)
    out.loc[out["available_metric_count"] == 0, "bundle_score"] = np.nan
    return out


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
    normalization_mode = getattr(spec, "normalization", "per_period")
    anchor_wide = None
    if normalization_mode == "baseline_anchored":
        anchor_wide = _build_wide_component_frame(
            component_frames,
            level=level_norm,
            scenario=spec.anchor_scenario,
            period=spec.anchor_period,
        )
    for scenario, period in available_pairs:
        wide = _build_wide_component_frame(component_frames, level=level_norm, scenario=scenario, period=period)
        if normalization_mode == "pre_scaled_ordinal":
            score_frame = _compute_pre_scaled_ordinal_score_frame(
                wide,
                metric_specs=bundle_metric_specs,
                id_columns=id_columns,
            )
        elif anchor_wide is not None:
            score_frame = _compute_baseline_anchored_score_frame(
                wide,
                anchor_wide=anchor_wide,
                metric_specs=bundle_metric_specs,
                id_columns=id_columns,
                min_components=spec.min_anchored_components,
            )
        else:
            score_frame = compute_bundle_score_frame(
                wide,
                metric_specs=bundle_metric_specs,
                id_columns=id_columns,
            )
        score_column = f"{spec.composite_slug}__{scenario}__{period}__{SUPPORTED_STAT}"
        pair_frame = score_frame[id_columns + ["bundle_score"]].rename(columns={"bundle_score": score_column})
        output = output.merge(pair_frame, on=id_columns, how="left")

    # Sub-cell climate-fill provenance. A composite blends its component metrics, so
    # a unit whose composite draws on any idw-filled component must not read
    # "native". _build_wide_component_frame slices to id+value and drops the flag, so
    # read it here where the full component masters (component_frames) are still in
    # scope. The composite is idw iff ANY component is idw for that unit, else native.
    # Only fires when at least one component master carried the column (post-regen),
    # so composites over all-native components stay byte-identical.
    idw_units: Optional[pd.DataFrame] = None
    for frame in component_frames.values():
        if "climate_fill_method" not in frame.columns or not set(id_columns).issubset(frame.columns):
            continue
        is_idw = frame["climate_fill_method"].astype("string").str.lower().eq("idw").fillna(False)
        comp_idw = frame.loc[is_idw.to_numpy(), id_columns].drop_duplicates()
        idw_units = comp_idw if idw_units is None else pd.concat([idw_units, comp_idw], ignore_index=True)
    if idw_units is not None:
        idw_units = idw_units.drop_duplicates()
        idw_units["climate_fill_method"] = "idw"
        output = output.merge(idw_units, on=id_columns, how="left")
        output["climate_fill_method"] = output["climate_fill_method"].fillna("native")

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


def _prune_retired_composite_artifacts(*, data_dir: Path, dry_run: bool, quiet: bool) -> list[Path]:
    """Delete explicitly retired composite roots only when requested."""
    pruned: list[Path] = []
    for slug in sorted(RETIRED_COMPOSITE_SLUGS):
        root = resolve_processed_root(slug, data_dir=data_dir, mode="portfolio")
        if not root.exists():
            continue
        pruned.append(root)
        if not quiet:
            prefix = "[composite-prune-dry-run]" if dry_run else "[composite-prune]"
            print(f"{prefix} retired root {root}")
        if not dry_run:
            shutil.rmtree(root)
    return pruned


def build_composite_metrics(
    *,
    levels: Sequence[str],
    states: Optional[Sequence[str]] = None,
    composite_slugs: Optional[Sequence[str]] = None,
    data_dir: Optional[Path] = None,
    overwrite: bool = False,
    dry_run: bool = False,
    prune_retired: bool = False,
    quiet: bool = False,
) -> list[Path]:
    """Build persisted composite metric masters for visible Glance bundles."""
    if data_dir is None:
        data_dir = get_paths_config().data_dir

    pruned = _prune_retired_composite_artifacts(data_dir=data_dir, dry_run=dry_run, quiet=quiet) if prune_retired else []

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

    written: list[Path] = list(pruned)
    for spec in specs:
        requested_states = [str(state).strip() for state in states or () if str(state).strip()]
        if not requested_states:
            requested_states = _discover_states_for_spec(spec, level=levels_resolved[0], data_dir=data_dir)
        for level in levels_resolved:
            if level not in spec.supported_levels:
                # District-only composites (e.g. composite_water_risk) must never
                # await or build block masters.
                continue
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
    parser.add_argument(
        "--prune-retired",
        action="store_true",
        help="Delete retired composite processed roots such as composite_agriculture_growing_conditions. Honors --dry-run.",
    )
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
        prune_retired=bool(args.prune_retired),
        quiet=bool(args.quiet),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

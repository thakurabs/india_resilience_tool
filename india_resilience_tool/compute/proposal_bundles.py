"""Offline builders for proposal climate-risk bundles."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from india_resilience_tool.analysis.timeseries import load_block_yearly, load_district_yearly
from india_resilience_tool.app.geography import list_available_states_from_processed_root
from india_resilience_tool.config.metrics_registry import METRICS_BY_SLUG
from india_resilience_tool.config.paths import get_paths_config, resolve_processed_root
from india_resilience_tool.config.proposal_bundles import (
    PROPOSAL_BUNDLES,
    PROPOSAL_BUNDLES_BY_SLUG,
    ProposalBundleSpec,
    ProposalRuleSpec,
)
from india_resilience_tool.data.master_columns import find_baseline_column_for_metric, resolve_metric_column
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
PERIOD_YEAR_WINDOWS = {
    "2020-2040": (2020, 2040),
    "2040-2060": (2040, 2060),
    "2060-2080": (2060, 2080),
}
BASELINE_TOKENS = ("1995-2014", "1995_2014", "1985-2014")
HELPER_METRIC_SLUG = "r95p_interannual_variability"
HELPER_SOURCE_METRIC_SLUG = "r95p_very_wet_precip"


@dataclass(frozen=True)
class BuildWarning:
    """One non-fatal build warning."""

    bundle_slug: str
    level: str
    state_name: str
    message: str


class TargetBuildError(RuntimeError):
    """Raised when one target cannot be built safely."""


def _normalize_level(level: str) -> str:
    value = str(level or "").strip().lower()
    aliases = {"admin": "admin", "all": "admin"}
    if value in aliases:
        return aliases[value]
    if value not in {"district", "block"}:
        raise ValueError(f"Unsupported proposal bundle level selection: {level!r}")
    return value


def _level_selection(level: str) -> tuple[str, ...]:
    normalized = _normalize_level(level)
    if normalized == "admin":
        return ("district", "block")
    return (normalized,)


def _required_id_columns(level: str) -> tuple[str, ...]:
    if level not in ID_COLUMNS_BY_LEVEL:
        raise ValueError(f"Unsupported proposal bundle level: {level!r}")
    return ID_COLUMNS_BY_LEVEL[level]


def _normalize_frame_identifiers(df: pd.DataFrame, *, level: str) -> pd.DataFrame:
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

    required = set(_required_id_columns(level))
    if not required.issubset(out.columns):
        missing = sorted(required.difference(out.columns))
        raise TargetBuildError(f"Missing canonical ID columns for level={level!r}: {missing}")
    return out


def _state_roots_for_bundle(bundle: ProposalBundleSpec, *, data_dir: Path) -> list[Path]:
    state_roots: list[Path] = []
    seen: set[str] = set()
    for rule in bundle.rules:
        metric_slug = HELPER_SOURCE_METRIC_SLUG if rule.metric_slug == HELPER_METRIC_SLUG else rule.metric_slug
        if metric_slug in seen:
            continue
        seen.add(metric_slug)
        state_roots.append(resolve_processed_root(metric_slug, data_dir=data_dir, mode="portfolio"))
    return state_roots


def _discover_states_for_bundle(bundle: ProposalBundleSpec, *, data_dir: Path) -> list[str]:
    per_root_states: list[set[str]] = []
    for root in _state_roots_for_bundle(bundle, data_dir=data_dir):
        per_root_states.append(set(list_available_states_from_processed_root(str(root))))
    if not per_root_states:
        return []
    return sorted(set.intersection(*per_root_states))


def _load_metric_master(metric_slug: str, *, level: str, state_name: str, data_dir: Path) -> pd.DataFrame:
    source_path = resolve_processed_root(metric_slug, data_dir=data_dir, mode="portfolio") / state_name / LEGACY_MASTER_FILENAMES[level]
    preferred = resolve_preferred_master_path(source_path)
    if not preferred.exists():
        raise TargetBuildError(
            f"Missing mandatory master for metric={metric_slug!r}, level={level!r}, state={state_name!r}: {source_path}"
        )
    frame = normalize_master_columns(load_master_csv(preferred))
    return _ensure_required_id_columns(frame, level=level)


def _stable_key_frame(component_frames: dict[str, pd.DataFrame], *, level: str) -> pd.DataFrame:
    id_columns = list(_required_id_columns(level))
    merged: Optional[pd.DataFrame] = None
    for frame in component_frames.values():
        key_frame = frame.loc[:, id_columns].drop_duplicates().copy()
        if merged is None:
            merged = key_frame
        else:
            merged = merged.merge(key_frame, on=id_columns, how="outer")
    if merged is None:
        return pd.DataFrame(columns=id_columns)
    return merged


def _resolve_baseline_column(df: pd.DataFrame, metric_slug: str) -> Optional[str]:
    metric_base = METRICS_BY_SLUG[metric_slug].periods_metric_col or METRICS_BY_SLUG[metric_slug].value_col or metric_slug
    return find_baseline_column_for_metric(
        list(df.columns),
        base_metric=metric_base,
        preferred_period_tokens=BASELINE_TOKENS,
    )


def _metric_column(frame: pd.DataFrame, metric_slug: str, scenario: str, period: str) -> Optional[str]:
    metric_base = METRICS_BY_SLUG[metric_slug].periods_metric_col or METRICS_BY_SLUG[metric_slug].value_col or metric_slug
    return resolve_metric_column(frame, metric_base, scenario, period, SUPPORTED_STAT)


def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype(float)


def _series_for_rule(
    key_frame: pd.DataFrame,
    source_frame: pd.DataFrame,
    *,
    level: str,
    metric_slug: str,
    scenario: str,
    period: str,
) -> pd.Series:
    id_columns = list(_required_id_columns(level))
    metric_column = _metric_column(source_frame, metric_slug, scenario, period)
    merged = key_frame.merge(
        source_frame.loc[:, id_columns + ([metric_column] if metric_column else [])],
        on=id_columns,
        how="left",
    )
    if metric_column is None:
        return pd.Series(np.nan, index=merged.index, dtype=float)
    return _coerce_numeric(merged[metric_column])


def _build_threshold_rule(
    key_frame: pd.DataFrame,
    source_frame: pd.DataFrame,
    *,
    level: str,
    rule: ProposalRuleSpec,
    scenario: str,
    period: str,
) -> pd.Series:
    values = _series_for_rule(key_frame, source_frame, level=level, metric_slug=rule.metric_slug, scenario=scenario, period=period)
    score = pd.Series(np.nan, index=values.index, dtype=float)
    mask = values.notna()
    score.loc[mask] = np.where(values.loc[mask] >= float(rule.threshold), 100.0, 0.0)
    return score


def _build_change_rule(
    key_frame: pd.DataFrame,
    source_frame: pd.DataFrame,
    *,
    level: str,
    metric_slug: str,
    scenario: str,
    period: str,
    warnings: list[BuildWarning],
    bundle_slug: str,
    state_name: str,
) -> pd.Series:
    current_values = _series_for_rule(
        key_frame,
        source_frame,
        level=level,
        metric_slug=metric_slug,
        scenario=scenario,
        period=period,
    )
    baseline_column = _resolve_baseline_column(source_frame, metric_slug)
    if not baseline_column:
        warnings.append(
            BuildWarning(
                bundle_slug=bundle_slug,
                level=level,
                state_name=state_name,
                message=(
                    f"Missing historical baseline mean column for metric={metric_slug!r}; "
                    "change-vs-baseline rule scored as NaN."
                ),
            )
        )
        return pd.Series(np.nan, index=current_values.index, dtype=float)

    id_columns = list(_required_id_columns(level))
    merged = key_frame.merge(source_frame.loc[:, id_columns + [baseline_column]], on=id_columns, how="left")
    baseline_values = _coerce_numeric(merged[baseline_column])
    score = pd.Series(np.nan, index=current_values.index, dtype=float)
    valid = current_values.notna() & baseline_values.notna() & (baseline_values.abs() >= 1e-6)
    pct_change = ((current_values.loc[valid] - baseline_values.loc[valid]) / baseline_values.loc[valid].abs()) * 100.0
    score.loc[valid] = np.where(pct_change > 20.0, 100.0, 0.0)
    return score


def _empty_varcfg() -> dict[str, tuple[str, ...]]:
    return {"district_yearly_candidates": (), "block_yearly_candidates": ()}


def _load_legacy_yearly_series(
    *,
    metric_slug: str,
    level: str,
    state_name: str,
    district_name: str,
    block_name: str | None,
    scenario: str,
    data_dir: Path,
) -> pd.DataFrame:
    ts_root = resolve_processed_root(metric_slug, data_dir=data_dir, mode="portfolio")
    if level == "district":
        return load_district_yearly(
            ts_root=ts_root,
            state_dir=state_name,
            district_display=district_name,
            scenario_name=scenario,
            varcfg=_empty_varcfg(),
            normalize_fn=alias,
        )
    return load_block_yearly(
        ts_root=ts_root,
        state_dir=state_name,
        district_display=district_name,
        block_display=str(block_name or ""),
        scenario_name=scenario,
        varcfg=_empty_varcfg(),
        normalize_fn=alias,
    )


def _prepare_period_yearly(df: pd.DataFrame, *, period: str) -> pd.DataFrame:
    start_year, end_year = PERIOD_YEAR_WINDOWS[period]
    out = df.copy()
    out["year"] = pd.to_numeric(out.get("year"), errors="coerce")
    out["mean"] = pd.to_numeric(out.get("mean"), errors="coerce")
    out = out.dropna(subset=["year", "mean"])
    out["year"] = out["year"].astype(int)
    out = out[(out["year"] >= start_year) & (out["year"] <= end_year)]
    if out.empty:
        return pd.DataFrame(columns=["year", "mean"])
    out = out.groupby("year", as_index=False)["mean"].mean().sort_values("year").reset_index(drop=True)
    return out


def _row_labels(row: pd.Series, *, level: str) -> str:
    if level == "block":
        return f"{row.get('state', '')}/{row.get('district', '')}/{row.get('block', '')}"
    return f"{row.get('state', '')}/{row.get('district', '')}"


def _build_trend_rule(
    key_frame: pd.DataFrame,
    *,
    level: str,
    metric_slug: str,
    scenario: str,
    period: str,
    data_dir: Path,
    bundle_slug: str,
    state_name: str,
) -> pd.Series:
    scores: list[float] = []
    for _, row in key_frame.iterrows():
        yearly = _load_legacy_yearly_series(
            metric_slug=metric_slug,
            level=level,
            state_name=str(row["state"]),
            district_name=str(row["district"]),
            block_name=str(row["block"]) if level == "block" and "block" in row else None,
            scenario=scenario,
            data_dir=data_dir,
        )
        if yearly.empty:
            raise TargetBuildError(
                f"Missing mandatory yearly ensemble series for metric={metric_slug!r}, bundle={bundle_slug!r}, "
                f"level={level!r}, state={state_name!r}, unit={_row_labels(row, level=level)!r}."
            )
        prepared = _prepare_period_yearly(yearly, period=period)
        if len(prepared) < 10:
            scores.append(np.nan)
            continue
        slope = float(np.polyfit(prepared["year"].to_numpy(dtype=float), prepared["mean"].to_numpy(dtype=float), 1)[0])
        scores.append(100.0 if slope > 0.0 else 0.0)
    return pd.Series(scores, index=key_frame.index, dtype=float)


def _build_spi_proxy_rule(
    key_frame: pd.DataFrame,
    source_frame: pd.DataFrame,
    *,
    level: str,
    scenario: str,
    period: str,
) -> pd.Series:
    values = _series_for_rule(
        key_frame,
        source_frame,
        level=level,
        metric_slug="spi3_count_months_lt_minus1",
        scenario=scenario,
        period=period,
    )
    score = pd.Series(np.nan, index=values.index, dtype=float)
    valid = values.notna()
    score.loc[valid] = np.clip(values.loc[valid] / 12.0, 0.0, 1.0) * 100.0
    return score


def _compute_r95p_interannual_variability_from_yearly(yearly: pd.DataFrame, *, period: str) -> float:
    prepared = _prepare_period_yearly(yearly, period=period)
    if len(prepared) < 2:
        return float("nan")
    mean_value = float(prepared["mean"].mean())
    std_value = float(prepared["mean"].std(ddof=0))
    if abs(mean_value) < 1e-6:
        return std_value
    return std_value / abs(mean_value)


def compute_r95p_interannual_variability_master_frame(
    *,
    level: str,
    state_name: str,
    data_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """Build one state/level helper frame for R95p interannual variability."""
    if data_dir is None:
        data_dir = get_paths_config().data_dir
    source_frame = _load_metric_master(HELPER_SOURCE_METRIC_SLUG, level=level, state_name=state_name, data_dir=data_dir)
    id_columns = list(_required_id_columns(level))
    output = source_frame.loc[:, id_columns].drop_duplicates().reset_index(drop=True)
    for scenario in SUPPORTED_SCENARIOS:
        for period in SUPPORTED_PERIODS:
            values: list[float] = []
            for _, row in output.iterrows():
                yearly = _load_legacy_yearly_series(
                    metric_slug=HELPER_SOURCE_METRIC_SLUG,
                    level=level,
                    state_name=str(row["state"]),
                    district_name=str(row["district"]),
                    block_name=str(row["block"]) if level == "block" and "block" in row else None,
                    scenario=scenario,
                    data_dir=data_dir,
                )
                if yearly.empty:
                    raise TargetBuildError(
                        f"Missing mandatory yearly ensemble series for helper metric={HELPER_SOURCE_METRIC_SLUG!r}, "
                        f"level={level!r}, state={state_name!r}, unit={_row_labels(row, level=level)!r}."
                    )
                values.append(_compute_r95p_interannual_variability_from_yearly(yearly, period=period))
            output[f"{HELPER_METRIC_SLUG}__{scenario}__{period}__{SUPPORTED_STAT}"] = values
    return output


def _write_helper_master_frame(
    df: pd.DataFrame,
    *,
    level: str,
    state_name: str,
    data_dir: Path,
    overwrite: bool,
    dry_run: bool,
) -> Optional[Path]:
    target_root = resolve_processed_root(HELPER_METRIC_SLUG, data_dir=data_dir, mode="portfolio")
    target_path = target_root / state_name / LEGACY_MASTER_FILENAMES[level]
    if dry_run:
        return target_path
    if target_path.exists() and not overwrite:
        return target_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target_path, index=False)
    df.to_parquet(target_path.with_suffix(".parquet"), index=False)
    return target_path


def _build_variability_proxy_rule(
    helper_frame: pd.DataFrame,
    *,
    level: str,
    scenario: str,
    period: str,
) -> pd.Series:
    metric_column = f"{HELPER_METRIC_SLUG}__{scenario}__{period}__{SUPPORTED_STAT}"
    values = _coerce_numeric(helper_frame[metric_column]) if metric_column in helper_frame.columns else pd.Series(
        np.nan, index=helper_frame.index, dtype=float
    )
    score = pd.Series(np.nan, index=values.index, dtype=float)
    finite = values[np.isfinite(values)]
    if finite.empty:
        return score
    lo = float(finite.min())
    hi = float(finite.max())
    if hi == lo:
        score.loc[finite.index] = 50.0
        return score
    score.loc[finite.index] = ((finite - lo) / (hi - lo)) * 100.0
    return score


def _target_sort_columns(level: str) -> list[str]:
    return ["state", "district", "district_key"] if level == "district" else ["state", "district", "block", "block_key"]


def compute_proposal_bundle_master_frame(
    bundle: ProposalBundleSpec,
    *,
    level: str,
    state_name: str,
    data_dir: Optional[Path] = None,
    warnings: Optional[list[BuildWarning]] = None,
    helper_frame: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Compute one proposal bundle output frame for one state/level."""
    if data_dir is None:
        data_dir = get_paths_config().data_dir
    if warnings is None:
        warnings = []

    metric_frames: dict[str, pd.DataFrame] = {}
    for rule in bundle.rules:
        if rule.metric_slug == HELPER_METRIC_SLUG:
            continue
        if rule.metric_slug not in metric_frames:
            metric_frames[rule.metric_slug] = _load_metric_master(
                rule.metric_slug,
                level=level,
                state_name=state_name,
                data_dir=data_dir,
            )

    key_frame = _stable_key_frame(metric_frames, level=level)
    if key_frame.empty:
        raise TargetBuildError(
            f"No canonical IDs available for bundle={bundle.composite_slug!r}, level={level!r}, state={state_name!r}."
        )

    output = key_frame.copy()
    ordered_columns = list(_required_id_columns(level))
    for scenario in SUPPORTED_SCENARIOS:
        for period in SUPPORTED_PERIODS:
            rule_columns: list[str] = []
            for rule in bundle.rules:
                score_column = f"{rule.rule_slug}__{scenario}__{period}__score"
                if rule.rule_type == "threshold":
                    score = _build_threshold_rule(
                        key_frame,
                        metric_frames[rule.metric_slug],
                        level=level,
                        rule=rule,
                        scenario=scenario,
                        period=period,
                    )
                elif rule.rule_type == "change_vs_baseline":
                    score = _build_change_rule(
                        key_frame,
                        metric_frames[rule.metric_slug],
                        level=level,
                        metric_slug=rule.metric_slug,
                        scenario=scenario,
                        period=period,
                        warnings=warnings,
                        bundle_slug=bundle.composite_slug,
                        state_name=state_name,
                    )
                elif rule.rule_type == "trend":
                    score = _build_trend_rule(
                        key_frame,
                        level=level,
                        metric_slug=rule.metric_slug,
                        scenario=scenario,
                        period=period,
                        data_dir=data_dir,
                        bundle_slug=bundle.composite_slug,
                        state_name=state_name,
                    )
                elif rule.rule_slug == "spi3_low_flow_proxy_norm":
                    score = _build_spi_proxy_rule(
                        key_frame,
                        metric_frames["spi3_count_months_lt_minus1"],
                        level=level,
                        scenario=scenario,
                        period=period,
                    )
                elif rule.rule_slug == "r95p_interannual_variability_norm":
                    if helper_frame is None:
                        raise TargetBuildError("Hydropower bundle requires a precomputed R95p variability helper frame.")
                    score = _build_variability_proxy_rule(helper_frame, level=level, scenario=scenario, period=period)
                else:
                    raise TargetBuildError(f"Unsupported proposal rule implementation: {rule.rule_slug!r}")
                output[score_column] = score
                rule_columns.append(score_column)
                ordered_columns.append(score_column)

            bundle_score_column = f"{bundle.composite_slug}__{scenario}__{period}__{SUPPORTED_STAT}"
            available_count_column = f"{bundle.composite_slug}__{scenario}__{period}__available_rule_count"
            output[available_count_column] = output[rule_columns].notna().sum(axis=1).astype(int)
            output[bundle_score_column] = output[rule_columns].mean(axis=1, skipna=True)
            output.loc[output[available_count_column] == 0, bundle_score_column] = np.nan
            ordered_columns.extend([bundle_score_column, available_count_column])

    output = output.loc[:, ordered_columns]
    return output.sort_values(_target_sort_columns(level), kind="stable").reset_index(drop=True)


def _write_bundle_master_frame(
    df: pd.DataFrame,
    *,
    bundle: ProposalBundleSpec,
    level: str,
    state_name: str,
    data_dir: Path,
    overwrite: bool,
    dry_run: bool,
) -> Optional[Path]:
    target_root = resolve_processed_root(bundle.composite_slug, data_dir=data_dir, mode="portfolio")
    target_path = target_root / state_name / LEGACY_MASTER_FILENAMES[level]
    if dry_run:
        return target_path
    if target_path.exists() and not overwrite:
        return None
    target_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target_path, index=False)
    df.to_parquet(target_path.with_suffix(".parquet"), index=False)
    return target_path


def build_proposal_bundles(
    *,
    levels: Sequence[str],
    states: Optional[Sequence[str]] = None,
    bundle_slugs: Optional[Sequence[str]] = None,
    data_dir: Optional[Path] = None,
    overwrite: bool = False,
    dry_run: bool = False,
    quiet: bool = False,
) -> tuple[list[Path], list[BuildWarning], list[str]]:
    """Build persisted proposal climate-risk bundle masters."""
    if data_dir is None:
        data_dir = get_paths_config().data_dir

    requested_levels: list[str] = []
    for level in (levels or ("admin",)):
        requested_levels.extend(_level_selection(level))
    levels_resolved = tuple(dict.fromkeys(requested_levels))

    if bundle_slugs:
        bundles = []
        for slug in bundle_slugs:
            spec = PROPOSAL_BUNDLES_BY_SLUG.get(str(slug).strip())
            if spec is None:
                raise ValueError(f"Unsupported proposal bundle selection: {slug!r}")
            bundles.append(spec)
    else:
        bundles = list(PROPOSAL_BUNDLES)

    written: list[Path] = []
    warnings: list[BuildWarning] = []
    failures: list[str] = []
    helper_cache: dict[tuple[str, str], pd.DataFrame] = {}

    for bundle in bundles:
        requested_states = [str(state).strip() for state in states or () if str(state).strip()]
        if not requested_states:
            requested_states = _discover_states_for_bundle(bundle, data_dir=data_dir)
        for level in levels_resolved:
            for state_name in requested_states:
                try:
                    helper_frame: Optional[pd.DataFrame] = None
                    if any(rule.metric_slug == HELPER_METRIC_SLUG for rule in bundle.rules):
                        cache_key = (level, state_name)
                        if cache_key not in helper_cache:
                            helper_cache[cache_key] = compute_r95p_interannual_variability_master_frame(
                                level=level,
                                state_name=state_name,
                                data_dir=data_dir,
                            )
                        helper_frame = helper_cache[cache_key]
                        helper_target = _write_helper_master_frame(
                            helper_frame,
                            level=level,
                            state_name=state_name,
                            data_dir=data_dir,
                            overwrite=overwrite,
                            dry_run=dry_run,
                        )
                        if helper_target is not None and helper_target not in written:
                            written.append(helper_target)
                            if not quiet:
                                print(f"[proposal-helper] wrote {helper_target}")

                    frame = compute_proposal_bundle_master_frame(
                        bundle,
                        level=level,
                        state_name=state_name,
                        data_dir=data_dir,
                        warnings=warnings,
                        helper_frame=helper_frame,
                    )
                    target = _write_bundle_master_frame(
                        frame,
                        bundle=bundle,
                        level=level,
                        state_name=state_name,
                        data_dir=data_dir,
                        overwrite=overwrite,
                        dry_run=dry_run,
                    )
                    if target is not None:
                        written.append(target)
                        if not quiet:
                            print(f"[proposal-bundle] wrote {target}")
                except TargetBuildError as exc:
                    failures.append(str(exc))
                    if not quiet:
                        print(f"[proposal-error] {exc}")
    return written, warnings, failures


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI flags for the proposal bundle builder."""
    parser = argparse.ArgumentParser(description="Build persisted proposal climate-risk bundle masters.")
    parser.add_argument(
        "--level",
        action="append",
        default=None,
        help="Proposal bundle output level: district, block, admin, or all. Default: admin.",
    )
    parser.add_argument(
        "--state",
        action="append",
        default=None,
        help="Optional repeatable admin state filter.",
    )
    parser.add_argument(
        "--bundle",
        action="append",
        default=None,
        help="Optional repeatable proposal composite slug filter.",
    )
    parser.add_argument(
        "--metric",
        action="append",
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--overwrite", action="store_true", help="Rewrite existing proposal bundle outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned proposal bundle outputs without writing.")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-file success logging.")
    args = parser.parse_args(argv)
    if args.bundle and args.metric:
        parser.error("Use either --bundle or deprecated --metric, not both.")
    if args.metric and not args.bundle:
        args.bundle = list(args.metric)
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entrypoint for proposal bundle building."""
    args = parse_args(argv)
    written, warnings, failures = build_proposal_bundles(
        levels=args.level or ("admin",),
        states=args.state,
        bundle_slugs=args.bundle,
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        quiet=bool(args.quiet),
    )
    if args.dry_run and not args.quiet:
        for path in written:
            print(f"[proposal-dry-run] {path}")
    if not args.quiet:
        for warning in warnings:
            print(f"[proposal-warning] {warning.bundle_slug}:{warning.level}:{warning.state_name}: {warning.message}")
    return 1 if failures else 0

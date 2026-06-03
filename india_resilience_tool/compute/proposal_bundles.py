"""Offline builders for proposal climate-risk bundles.

The builder persists Phase-1 sector climate hazard-pressure scores. These
outputs are not full sectoral risk scores because exposure, vulnerability, and
adaptive capacity are not included in this compute path.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

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
    proposal_available_rule_count_column,
    proposal_available_rule_weight_fraction_column,
    proposal_bundle_mean_column,
    proposal_rule_abs_score_column,
    proposal_rule_chg_score_column,
    proposal_rule_imp_score_column,
    proposal_rule_score_column,
)
from india_resilience_tool.data.master_columns import find_baseline_column_for_metric, resolve_metric_column
from india_resilience_tool.data.adm2_loader import load_local_adm2
from india_resilience_tool.data.adm3_loader import load_local_adm3
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
BASELINE_YEAR_WINDOWS = {
    "1995-2014": (1995, 2014),
    "1985-2014": (1985, 2014),
}
HELPER_METRIC_SLUG = "r95p_interannual_variability"
HELPER_SOURCE_METRIC_SLUG = "r95p_very_wet_precip"
HELPER_BASELINE_SOURCE_METRIC_SLUGS = ("pr_max_5day_precip", "pr_consecutive_dry_days_lt1mm")


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


def _canonical_key_column(level: str) -> str:
    return "district_key" if level == "district" else "block_key"


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


def _derived_join_key_series(df: pd.DataFrame, *, level: str) -> pd.Series:
    """Return normalized proposal-bundle join keys from canonical admin labels."""
    if level == "district":
        return df["state"].map(alias).astype("string").str.cat(df["district"].map(alias).astype("string"), sep="|")
    return (
        df["state"]
        .map(alias)
        .astype("string")
        .str.cat(df["district"].map(alias).astype("string"), sep="|")
        .str.cat(df["block"].map(alias).astype("string"), sep="|")
    )


def _sample_identifier_records(df: pd.DataFrame, *, level: str, limit: int = 5) -> list[dict[str, str]]:
    cols = ["state", "district", "district_key"] if level == "district" else ["state", "district", "block", "block_key"]
    available = [col for col in cols if col in df.columns]
    if not available:
        return []
    sample = df.loc[:, available].head(limit).fillna("")
    return [{col: str(row[col]) for col in available} for _, row in sample.iterrows()]


def _validate_unique_canonical_keys(
    df: pd.DataFrame,
    *,
    level: str,
    context: str,
) -> pd.DataFrame:
    key_col = _canonical_key_column(level)
    if key_col not in df.columns:
        raise TargetBuildError(f"{context} is missing canonical key column {key_col!r}.")

    dup_mask = df[key_col].astype("string").fillna("").duplicated(keep=False)
    if dup_mask.any():
        duplicate_rows = df.loc[dup_mask].copy()
        duplicate_count = int(len(duplicate_rows))
        duplicate_keys = int(duplicate_rows[key_col].nunique(dropna=False))
        sample = _sample_identifier_records(duplicate_rows, level=level)
        raise TargetBuildError(
            f"{context} contains duplicate canonical keys on {key_col!r}: "
            f"duplicate_rows={duplicate_count}, duplicate_keys={duplicate_keys}, sample={sample}"
        )
    return df


def _load_canonical_unit_frame(
    *,
    level: str,
    state_name: str,
    data_dir: Path,
) -> pd.DataFrame:
    if level == "district":
        gdf = load_local_adm2(data_dir / "districts_4326.geojson", tolerance=0.0, bbox=None, min_area=0.0)
        source = gdf.rename(columns={"state_name": "state", "district_name": "district"})
    else:
        gdf = load_local_adm3(data_dir / "blocks_4326.geojson", tolerance=0.0, bbox=None, min_area=0.0)
        source = gdf.rename(columns={"state_name": "state", "district_name": "district", "block_name": "block"})

    filtered = source.loc[source["state"].map(alias) == alias(state_name)].copy()
    if filtered.empty:
        return pd.DataFrame(columns=list(_required_id_columns(level)))

    filtered = _ensure_required_id_columns(filtered, level=level)
    if level == "block":
        filtered["block_key"] = _derived_join_key_series(filtered, level=level)
    filtered = filtered.loc[:, list(_required_id_columns(level))].drop_duplicates().reset_index(drop=True)
    filtered = _validate_unique_canonical_keys(
        filtered,
        level=level,
        context=f"Canonical boundary units for level={level!r}, state={state_name!r}",
    )
    return filtered.sort_values(_target_sort_columns(level), kind="stable").reset_index(drop=True)


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
    frame = _ensure_required_id_columns(frame, level=level)
    return _validate_unique_canonical_keys(
        frame,
        level=level,
        context=f"Source master metric={metric_slug!r}, level={level!r}, state={state_name!r}",
    )


def _resolve_baseline_column(df: pd.DataFrame, metric_slug: str) -> Optional[str]:
    metric_base = METRICS_BY_SLUG[metric_slug].periods_metric_col or METRICS_BY_SLUG[metric_slug].value_col or metric_slug
    return find_baseline_column_for_metric(
        list(df.columns),
        base_metric=metric_base,
        preferred_period_tokens=BASELINE_TOKENS,
    )


def _metric_column(frame: pd.DataFrame, metric_slug: str, scenario: str, period: str) -> Optional[str]:
    metric_base = METRICS_BY_SLUG[metric_slug].periods_metric_col or METRICS_BY_SLUG[metric_slug].value_col or metric_slug
    return resolve_metric_column(frame, metric_base, scenario, period, SUPPORTED_STAT, strict=True)


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
    key_col = _canonical_key_column(level)
    metric_column = _metric_column(source_frame, metric_slug, scenario, period)
    merged = key_frame.merge(
        source_frame.loc[:, [key_col] + ([metric_column] if metric_column else [])],
        on=key_col,
        how="left",
    )
    if metric_column is None:
        return pd.Series(np.nan, index=merged.index, dtype=float)
    return _coerce_numeric(merged[metric_column])


def _normalize_direction(direction: str) -> str:
    """Return a supported score direction for climate hazard-pressure rules."""
    normalized = str(direction or "higher_worse").strip().lower()
    if normalized not in {"higher_worse", "lower_worse"}:
        raise TargetBuildError(f"Unsupported proposal rule direction: {direction!r}")
    return normalized


def _valid_numeric(values: pd.Series) -> pd.Series:
    """Return finite numeric values while preserving the original index."""
    numeric = _coerce_numeric(values)
    return numeric.loc[np.isfinite(numeric)]


def _score_by_reference_distribution(
    values: pd.Series,
    *,
    direction: str = "higher_worse",
    lower_quantile: float = 0.10,
    upper_quantile: float = 0.90,
    flat_score: float = 50.0,
) -> pd.Series:
    """Convert a metric vector to a robust 0-100 relative pressure score.

    Missing or invalid values return NaN. Valid values are scaled between the
    reference distribution's p10 and p90 to reduce outlier sensitivity. If all
    valid values are identical, valid rows receive ``flat_score`` and quality
    diagnostics can warn on the downstream rule or bundle column.
    """
    direction = _normalize_direction(direction)
    numeric = _coerce_numeric(values)
    score = pd.Series(np.nan, index=numeric.index, dtype=float)
    finite = _valid_numeric(numeric)
    if finite.empty:
        return score

    lo = float(finite.quantile(lower_quantile))
    hi = float(finite.quantile(upper_quantile))
    if not np.isfinite(lo) or not np.isfinite(hi):
        return score
    if np.isclose(hi, lo):
        score.loc[finite.index] = float(flat_score)
        return score

    raw = (finite - lo) / (hi - lo)
    if direction == "lower_worse":
        raw = 1.0 - raw
    score.loc[finite.index] = np.clip(raw.to_numpy(dtype=float), 0.0, 1.0) * 100.0
    return score


def _score_impact_threshold(
    values: pd.Series,
    *,
    impact_low: float | None,
    impact_high: float | None,
    direction: str = "higher_worse",
) -> pd.Series:
    """Convert defensible low/high impact thresholds to a continuous score.

    Missing or incomplete thresholds return NaN so the impact component is
    ignored row-wise. For higher-worse metrics, ``impact_low`` is the onset of
    concern and ``impact_high`` is the severe threshold. For lower-worse metrics,
    the two values may be reversed.
    """
    direction = _normalize_direction(direction)
    numeric = _coerce_numeric(values)
    score = pd.Series(np.nan, index=numeric.index, dtype=float)
    if impact_low is None or impact_high is None:
        return score
    low = float(impact_low)
    high = float(impact_high)
    if np.isclose(low, high):
        return score

    finite = _valid_numeric(numeric)
    if finite.empty:
        return score
    if direction == "higher_worse":
        raw = (finite - low) / (high - low)
    else:
        raw = (low - finite) / (low - high)
    score.loc[finite.index] = np.clip(raw.to_numpy(dtype=float), 0.0, 1.0) * 100.0
    return score


def _is_temperature_metric(metric_slug: str) -> bool:
    """Return whether a metric slug should default to absolute Celsius change."""
    normalized = str(metric_slug or "").strip().lower()
    return normalized.startswith(("tas", "tx", "tn")) or "temperature" in normalized or "heat" in normalized


def _change_values(
    current_values: pd.Series,
    baseline_values: pd.Series,
    *,
    metric_slug: str,
    change_mode: str,
) -> pd.Series:
    """Return future-minus-baseline values for continuous change scoring.

    Invalid source values return NaN. ``relative_pct`` protects against tiny
    denominators by returning NaN for those rows rather than exploding the score.
    """
    current = _coerce_numeric(current_values)
    baseline = _coerce_numeric(baseline_values)
    mode = str(change_mode or "auto").strip().lower()
    if mode == "auto":
        mode = "absolute_delta" if _is_temperature_metric(metric_slug) else "relative_pct"
    if mode not in {"absolute_delta", "relative_pct"}:
        raise TargetBuildError(f"Unsupported change mode for metric={metric_slug!r}: {change_mode!r}")

    change = pd.Series(np.nan, index=current.index, dtype=float)
    valid = current.notna() & baseline.notna()
    if mode == "absolute_delta":
        change.loc[valid] = current.loc[valid] - baseline.loc[valid]
        return change

    safe = valid & (baseline.abs() >= 1e-6)
    change.loc[safe] = ((current.loc[safe] - baseline.loc[safe]) / baseline.loc[safe].abs()) * 100.0
    return change


def _baseline_values_for_rule(
    key_frame: pd.DataFrame,
    source_frame: pd.DataFrame,
    *,
    level: str,
    rule: ProposalRuleSpec,
    warnings: list[BuildWarning],
    bundle_slug: str,
    state_name: str,
) -> pd.Series:
    """Return baseline values aligned to ``key_frame`` or NaN with a warning.

    Missing baseline columns return NaN. The build continues so other score
    components and other rules can still produce partial, explainable outputs.
    """
    baseline_column = _resolve_baseline_column(source_frame, rule.metric_slug)
    if not baseline_column:
        warnings.append(
            BuildWarning(
                bundle_slug=bundle_slug,
                level=level,
                state_name=state_name,
                message=(
                    f"Missing historical baseline mean column for rule={rule.rule_slug!r}, "
                    f"metric={rule.metric_slug!r}; change component scored as NaN."
                ),
            )
        )
        return pd.Series(np.nan, index=key_frame.index, dtype=float)

    key_col = _canonical_key_column(level)
    merged = key_frame.merge(source_frame.loc[:, [key_col, baseline_column]], on=key_col, how="left")
    return _coerce_numeric(merged[baseline_column])


def _weighted_component_score(component_scores: list[pd.Series], component_weights: list[float]) -> pd.Series:
    """Return a row-wise weighted mean across available component scores."""
    if not component_scores:
        return pd.Series(dtype=float)
    aligned_scores = pd.concat(component_scores, axis=1)
    weights = np.asarray(component_weights, dtype=float)
    valid = aligned_scores.notna()
    weighted = aligned_scores.multiply(weights, axis=1).where(valid)
    denominator = valid.multiply(weights, axis=1).sum(axis=1)
    score = weighted.sum(axis=1, skipna=True) / denominator.replace(0.0, np.nan)
    return score.astype(float)


def _rule_weights_for_bundle(bundle: ProposalBundleSpec) -> np.ndarray:
    """Return bundle-level rule weights in rule order."""
    if bundle.weight_mode == "explicit_normalized":
        return np.asarray([float(rule.rule_weight) for rule in bundle.rules], dtype=float)
    if not bundle.rules:
        return np.asarray([], dtype=float)
    return np.full(len(bundle.rules), 1.0 / float(len(bundle.rules)), dtype=float)


def _weighted_bundle_score(
    rule_scores: pd.DataFrame,
    rule_weights: np.ndarray,
) -> tuple[pd.Series, pd.Series]:
    """Return row-wise available-weight fraction and normalized weighted score."""
    if rule_scores.empty:
        empty = pd.Series(np.nan, index=rule_scores.index, dtype=float)
        return empty, empty
    valid = rule_scores.notna()
    weighted = rule_scores.multiply(rule_weights, axis=1).where(valid)
    available_weight = valid.multiply(rule_weights, axis=1).sum(axis=1).astype(float)
    score = weighted.sum(axis=1, skipna=True) / available_weight.replace(0.0, np.nan)
    return available_weight.astype(float), score.astype(float)


@dataclass(frozen=True)
class BlendedRuleScores:
    """Persisted score decomposition for one proposal rule selection.

    ``blended`` is the weighted-mean rule score (the existing
    ``{rule_slug}__{scenario}__{period}__score`` column).

    ``components`` carries the per-lens component scores in [0, 100] for the
    lenses that are active on this rule (i.e., the lens weight is > 0). Keys
    are a subset of {"absolute", "change", "impact"}; only active lenses are
    persisted. Rules whose config activates only the absolute lens emit only
    ``{"absolute": blended}``; rules with absolute+change and no impact lens
    emit the two active lens columns.
    """

    blended: pd.Series
    components: dict[str, pd.Series]


def _absolute_only_scores(blended_score: pd.Series) -> BlendedRuleScores:
    """Return a BlendedRuleScores for a rule whose only active lens is the absolute lens."""
    return BlendedRuleScores(blended=blended_score, components={"absolute": blended_score})


def _build_blended_rule(
    key_frame: pd.DataFrame,
    source_frame: pd.DataFrame,
    *,
    level: str,
    rule: ProposalRuleSpec,
    scenario: str,
    period: str,
    warnings: list[BuildWarning],
    bundle_slug: str,
    state_name: str,
) -> BlendedRuleScores:
    """Build a continuous sector climate hazard-pressure rule.

    Missing current or baseline data returns NaN for the affected component.
    The final rule score is the weighted mean of available components, allowing
    partial but transparent results when one component is unavailable. The
    return value also carries the per-lens [0, 100] component scores for the
    active lenses, so the orchestrator can persist them alongside the blend.
    """
    current_values = _series_for_rule(
        key_frame,
        source_frame,
        level=level,
        metric_slug=rule.metric_slug,
        scenario=scenario,
        period=period,
    )
    component_scores: list[pd.Series] = []
    component_weights: list[float] = []
    components: dict[str, pd.Series] = {}

    if rule.absolute_weight > 0.0:
        abs_score = _score_by_reference_distribution(current_values, direction=rule.direction)
        component_scores.append(abs_score)
        component_weights.append(float(rule.absolute_weight))
        components["absolute"] = abs_score

    if rule.change_weight > 0.0:
        baseline_values = _baseline_values_for_rule(
            key_frame,
            source_frame,
            level=level,
            rule=rule,
            warnings=warnings,
            bundle_slug=bundle_slug,
            state_name=state_name,
        )
        changes = _change_values(
            current_values,
            baseline_values,
            metric_slug=rule.metric_slug,
            change_mode=rule.change_mode,
        )
        chg_score = _score_by_reference_distribution(changes, direction=rule.direction)
        component_scores.append(chg_score)
        component_weights.append(float(rule.change_weight))
        components["change"] = chg_score

    if rule.impact_weight > 0.0:
        imp_score = _score_impact_threshold(
            current_values,
            impact_low=rule.impact_low,
            impact_high=rule.impact_high,
            direction=rule.direction,
        )
        component_scores.append(imp_score)
        component_weights.append(float(rule.impact_weight))
        components["impact"] = imp_score

    score = _weighted_component_score(component_scores, component_weights)
    if score.empty:
        blended = pd.Series(np.nan, index=current_values.index, dtype=float)
    else:
        blended = score.reindex(current_values.index).astype(float)
    return BlendedRuleScores(blended=blended, components=components)


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


def _prepare_yearly_window(df: pd.DataFrame, *, start_year: int, end_year: int) -> pd.DataFrame:
    """Return the yearly mean series filtered to an inclusive ``[start, end]`` window.

    Shared slicing/grouping for both future periods and historical baseline
    windows so the same logic does not flow through the future-only
    ``PERIOD_YEAR_WINDOWS`` map for historical baselines.
    """
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


def _prepare_period_yearly(df: pd.DataFrame, *, period: str) -> pd.DataFrame:
    start_year, end_year = PERIOD_YEAR_WINDOWS[period]
    return _prepare_yearly_window(df, start_year=start_year, end_year=end_year)


def _row_labels(row: pd.Series, *, level: str) -> str:
    if level == "block":
        return f"{row.get('state', '')}/{row.get('district', '')}/{row.get('block', '')}"
    return f"{row.get('state', '')}/{row.get('district', '')}"


def _build_trend_rule(
    key_frame: pd.DataFrame,
    *,
    level: str,
    rule: ProposalRuleSpec,
    scenario: str,
    period: str,
    data_dir: Path,
    bundle_slug: str,
    state_name: str,
) -> pd.Series:
    """Build a continuous adverse-trend pressure score from yearly series.

    Missing yearly data raises ``TargetBuildError`` because trend rules cannot be
    reconstructed from period means. Units with fewer than 10 period years return
    NaN. Non-adverse slopes score zero; positive adverse slopes are scaled within
    the state/level/scenario/period reference distribution.
    """
    adverse_slopes: list[float] = []
    direction = _normalize_direction(rule.direction)
    for _, row in key_frame.iterrows():
        yearly = _load_legacy_yearly_series(
            metric_slug=rule.metric_slug,
            level=level,
            state_name=str(row["state"]),
            district_name=str(row["district"]),
            block_name=str(row["block"]) if level == "block" and "block" in row else None,
            scenario=scenario,
            data_dir=data_dir,
        )
        if yearly.empty:
            raise TargetBuildError(
                f"Missing mandatory yearly ensemble series for metric={rule.metric_slug!r}, bundle={bundle_slug!r}, "
                f"level={level!r}, state={state_name!r}, unit={_row_labels(row, level=level)!r}."
            )
        prepared = _prepare_period_yearly(yearly, period=period)
        if len(prepared) < 10:
            adverse_slopes.append(np.nan)
            continue
        slope = float(np.polyfit(prepared["year"].to_numpy(dtype=float), prepared["mean"].to_numpy(dtype=float), 1)[0])
        adverse_slopes.append(max(slope, 0.0) if direction == "higher_worse" else max(-slope, 0.0))

    slope_values = pd.Series(adverse_slopes, index=key_frame.index, dtype=float)
    finite = _valid_numeric(slope_values)
    if finite.empty:
        return pd.Series(np.nan, index=key_frame.index, dtype=float)
    if finite.max() <= 0.0:
        score = pd.Series(np.nan, index=key_frame.index, dtype=float)
        score.loc[finite.index] = 0.0
        return score
    return _score_by_reference_distribution(slope_values, direction="higher_worse")


def _compute_r95p_interannual_variability_from_yearly(
    yearly: pd.DataFrame, *, start_year: int, end_year: int
) -> float:
    prepared = _prepare_yearly_window(yearly, start_year=start_year, end_year=end_year)
    if len(prepared) < 2:
        return float("nan")
    mean_value = float(prepared["mean"].mean())
    std_value = float(prepared["mean"].std(ddof=0))
    if abs(mean_value) < 1e-6:
        return std_value
    return std_value / abs(mean_value)


def _resolve_hydropower_baseline_token(*, level: str, state_name: str, data_dir: Path) -> str:
    """Return the shared historical baseline token for the Hydropower helper.

    The R95p variability helper must use the same historical epoch as the
    Rx5day and CDD source masters so all three Hydropower change lenses are
    comparable. Both source masters must resolve a historical baseline column,
    agree on the period token, and that token must be a supported baseline
    window. Raises ``TargetBuildError`` otherwise so a cosmetic change lens is
    never landed.
    """
    tokens: dict[str, str] = {}
    for metric_slug in HELPER_BASELINE_SOURCE_METRIC_SLUGS:
        frame = _load_metric_master(metric_slug, level=level, state_name=state_name, data_dir=data_dir)
        baseline_column = _resolve_baseline_column(frame, metric_slug)
        if not baseline_column:
            raise TargetBuildError(
                f"Hydropower helper baseline token resolution failed: no historical baseline "
                f"column for metric={metric_slug!r}, level={level!r}, state={state_name!r}."
            )
        tokens[metric_slug] = str(baseline_column).split("__")[-2].replace("_", "-")
    distinct = set(tokens.values())
    if len(distinct) != 1:
        raise TargetBuildError(
            f"Hydropower helper baseline token mismatch across source masters for "
            f"level={level!r}, state={state_name!r}: {tokens}."
        )
    token = distinct.pop()
    if token not in BASELINE_YEAR_WINDOWS:
        raise TargetBuildError(
            f"Hydropower helper baseline token {token!r} is not a supported baseline window "
            f"{tuple(BASELINE_YEAR_WINDOWS)} for level={level!r}, state={state_name!r}."
        )
    return token


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

    baseline_token = _resolve_hydropower_baseline_token(level=level, state_name=state_name, data_dir=data_dir)
    hist_start, hist_end = BASELINE_YEAR_WINDOWS[baseline_token]

    for scenario in SUPPORTED_SCENARIOS:
        for period in SUPPORTED_PERIODS:
            start_year, end_year = PERIOD_YEAR_WINDOWS[period]
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
                values.append(
                    _compute_r95p_interannual_variability_from_yearly(
                        yearly, start_year=start_year, end_year=end_year
                    )
                )
            output[f"{HELPER_METRIC_SLUG}__{scenario}__{period}__{SUPPORTED_STAT}"] = values

    hist_values: list[float] = []
    for _, row in output.iterrows():
        yearly = _load_legacy_yearly_series(
            metric_slug=HELPER_SOURCE_METRIC_SLUG,
            level=level,
            state_name=str(row["state"]),
            district_name=str(row["district"]),
            block_name=str(row["block"]) if level == "block" and "block" in row else None,
            scenario="historical",
            data_dir=data_dir,
        )
        if yearly.empty:
            raise TargetBuildError(
                f"Missing mandatory historical yearly ensemble series for helper metric="
                f"{HELPER_SOURCE_METRIC_SLUG!r}, level={level!r}, state={state_name!r}, "
                f"unit={_row_labels(row, level=level)!r}."
            )
        hist_values.append(
            _compute_r95p_interannual_variability_from_yearly(yearly, start_year=hist_start, end_year=hist_end)
        )
    output[f"{HELPER_METRIC_SLUG}__historical__{baseline_token}__{SUPPORTED_STAT}"] = hist_values

    if not _resolve_baseline_column(output, HELPER_METRIC_SLUG):
        raise TargetBuildError(
            f"Hydropower helper frame is missing a resolvable historical baseline column for "
            f"level={level!r}, state={state_name!r}; expected token {baseline_token!r}."
        )
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


def _append_score_quality_warnings(
    values: pd.Series,
    *,
    warnings: list[BuildWarning],
    bundle_slug: str,
    level: str,
    state_name: str,
    column_name: str,
    label: str,
) -> None:
    """Append non-fatal flatness and saturation diagnostics for score columns."""
    finite = _valid_numeric(values)
    valid_count = int(len(finite))
    if valid_count < 2:
        return
    unique_count = int(finite.nunique(dropna=True))
    if unique_count <= 1:
        warnings.append(
            BuildWarning(
                bundle_slug=bundle_slug,
                level=level,
                state_name=state_name,
                message=(
                    f"Flat proposal {label} score column {column_name!r}: "
                    f"valid_count={valid_count}, unique_count={unique_count}."
                ),
            )
        )
    if valid_count < 5:
        return
    low_share = float((finite <= 1.0).mean())
    high_share = float((finite >= 99.0).mean())
    if low_share >= 0.95 or high_share >= 0.95:
        warnings.append(
            BuildWarning(
                bundle_slug=bundle_slug,
                level=level,
                state_name=state_name,
                message=(
                    f"Saturated proposal {label} score column {column_name!r}: "
                    f"valid_count={valid_count}, share_le_1={low_share:.3f}, share_ge_99={high_share:.3f}."
                ),
            )
        )


def _target_sort_columns(level: str) -> list[str]:
    return ["state", "district", "district_key"] if level == "district" else ["state", "district", "block", "block_key"]


def _dispatch_rule_scores(
    *,
    rule: ProposalRuleSpec,
    key_frame: pd.DataFrame,
    metric_frames: dict[str, pd.DataFrame],
    helper_frame: Optional[pd.DataFrame],
    level: str,
    scenario: str,
    period: str,
    data_dir: Path,
    warnings: list[BuildWarning],
    bundle_slug: str,
    state_name: str,
) -> BlendedRuleScores:
    """Return the blended-and-per-lens score decomposition for one rule selection.

    Centralizes the rule_type / helper dispatch so the orchestrator stays
    free of special cases. Trend builders emit only the absolute lens;
    ``_build_blended_rule`` emits whichever lenses are active on the rule.
    """
    if rule.rule_type == "blended":
        if rule.metric_slug == HELPER_METRIC_SLUG:
            if helper_frame is None:
                raise TargetBuildError(
                    "Hydropower bundle requires a precomputed R95p variability helper frame."
                )
            return _build_blended_rule(
                key_frame,
                helper_frame,
                level=level,
                rule=rule,
                scenario=scenario,
                period=period,
                warnings=warnings,
                bundle_slug=bundle_slug,
                state_name=state_name,
            )
        return _build_blended_rule(
            key_frame,
            metric_frames[rule.metric_slug],
            level=level,
            rule=rule,
            scenario=scenario,
            period=period,
            warnings=warnings,
            bundle_slug=bundle_slug,
            state_name=state_name,
        )
    if rule.rule_type == "trend":
        blended = _build_trend_rule(
            key_frame,
            level=level,
            rule=rule,
            scenario=scenario,
            period=period,
            data_dir=data_dir,
            bundle_slug=bundle_slug,
            state_name=state_name,
        )
        return _absolute_only_scores(blended)
    raise TargetBuildError(f"Unsupported proposal rule implementation: {rule.rule_slug!r}")


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

    key_frame = _load_canonical_unit_frame(level=level, state_name=state_name, data_dir=data_dir)
    if key_frame.empty:
        raise TargetBuildError(
            f"No canonical IDs available for bundle={bundle.composite_slug!r}, level={level!r}, state={state_name!r}."
        )

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

    output = key_frame.copy()
    ordered_columns = list(_required_id_columns(level))
    rule_weights = _rule_weights_for_bundle(bundle)
    lens_column_builders = {
        "absolute": proposal_rule_abs_score_column,
        "change": proposal_rule_chg_score_column,
        "impact": proposal_rule_imp_score_column,
    }
    for scenario in SUPPORTED_SCENARIOS:
        for period in SUPPORTED_PERIODS:
            rule_columns: list[str] = []
            for rule in bundle.rules:
                scores = _dispatch_rule_scores(
                    rule=rule,
                    key_frame=key_frame,
                    metric_frames=metric_frames,
                    helper_frame=helper_frame,
                    level=level,
                    scenario=scenario,
                    period=period,
                    data_dir=data_dir,
                    warnings=warnings,
                    bundle_slug=bundle.composite_slug,
                    state_name=state_name,
                )
                score_column = proposal_rule_score_column(rule.rule_slug, scenario, period)
                output[score_column] = scores.blended
                _append_score_quality_warnings(
                    scores.blended,
                    warnings=warnings,
                    bundle_slug=bundle.composite_slug,
                    level=level,
                    state_name=state_name,
                    column_name=score_column,
                    label=f"rule {rule.rule_slug}",
                )
                rule_columns.append(score_column)
                ordered_columns.append(score_column)
                for lens_key, lens_series in scores.components.items():
                    lens_column = lens_column_builders[lens_key](rule.rule_slug, scenario, period)
                    output[lens_column] = lens_series
                    ordered_columns.append(lens_column)

            bundle_score_column = proposal_bundle_mean_column(bundle.composite_slug, scenario, period)
            available_count_column = proposal_available_rule_count_column(bundle.composite_slug, scenario, period)
            available_weight_column = proposal_available_rule_weight_fraction_column(bundle.composite_slug, scenario, period)
            output[available_count_column] = output[rule_columns].notna().sum(axis=1).astype(int)
            available_weight_fraction, bundle_score = _weighted_bundle_score(output[rule_columns], rule_weights)
            output[available_weight_column] = available_weight_fraction
            output[bundle_score_column] = bundle_score
            output.loc[
                output[available_weight_column] < float(bundle.min_available_rule_weight_fraction),
                bundle_score_column,
            ] = np.nan
            output.loc[output[available_count_column] == 0, bundle_score_column] = np.nan
            _append_score_quality_warnings(
                output[bundle_score_column],
                warnings=warnings,
                bundle_slug=bundle.composite_slug,
                level=level,
                state_name=state_name,
                column_name=bundle_score_column,
                label="bundle",
            )
            ordered_columns.extend([bundle_score_column, available_count_column, available_weight_column])

    output = output.loc[:, ordered_columns]
    output = output.sort_values(_target_sort_columns(level), kind="stable").reset_index(drop=True)
    output = _validate_unique_canonical_keys(
        output,
        level=level,
        context=f"Proposal bundle output bundle={bundle.composite_slug!r}, level={level!r}, state={state_name!r}",
    )
    if len(output) != len(key_frame):
        raise TargetBuildError(
            f"Proposal bundle output row-count mismatch for bundle={bundle.composite_slug!r}, "
            f"level={level!r}, state={state_name!r}: expected_rows={len(key_frame)}, actual_rows={len(output)}"
        )
    return output


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

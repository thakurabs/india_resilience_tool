"""Heat Risk national-ruler pilot (CHG-0346) — read-only diagnostic.

Purpose
-------
Score the thematic **Heat Risk** composite for every Indian district against two
candidate *frozen national rulers*, instead of the production per-state min-max,
and emit the evidence needed to decide which ruler (if either) to freeze.

The two rulers, both fitted on the same pooled full-span sample:

- ``linear``  — clip to the pooled p1..p99 range and scale linearly. Preserves
  magnitude; a district twice as far above p1 scores twice as high.
- ``cdf``     — map through the **exact** pooled empirical CDF: one knot per
  distinct pooled value, scored at its tie mid-rank ``100*(b + c/2)/n``.
  Preserves rank only; uniformizes. The 21-knot quantile grid is retained only
  as a *measured approximation* of this exact ruler — its maximum and mean score
  error against the pooled sample are reported per metric (P-05).

The pilot is **district-level only**: block geometry and block aggregation are a
separate initiative, and the ruler question is answered at district level.

The pool for every metric is all districts x all 7 scenario/period slices
(``historical/1990-2010`` + {ssp245, ssp585} x {2020-2040, 2040-2060,
2060-2080}), so one ruler serves every state and every slice.

Chosen as the pilot bundle because thematic Heat Risk is a plain weighted mean of
14 metric scores with **no change lens**, so it needs only a level ruler and has
no delta-zero spike to handle — while still exercising the acclimatization
question through its internal 0.367 baseline-referenced / 0.633
absolute-threshold weight split.

Outputs (all written under ``--out-dir``; nothing under ``IRT_DATA_DIR`` is
touched, no config is changed, no composite master is rewritten):

- ``district_scores.csv``    one row per district x slice x ruler: composite,
  the two sub-composites, weight coverage, and the coverage-gated composite
- ``state_scores.csv``       area-weighted vs unweighted state means (P-12)
- ``roster_reconciliation.csv`` districts in the roster with no master row, and
  master rows whose ``district_key`` is not in the roster (P-09, P-10)
- ``slice_summary.csv``      realized composite range / IQR, saturation and
  clamping counts per slice x ruler (P-02, P-03, P-06, P-07)
- ``metric_ruler_spec.csv``  per metric: p1/p99, pooled min/max, modal value and
  its mass, tied-observation count, and — for ``cdf`` — the 21-knot grid's max and
  mean score error against the exact ruler (P-05, P-06)
- ``cdf_support.csv``        long-form exact CDF support: one row per (metric,
  distinct pooled value) with its tie count and mid-rank score. Kept out of
  ``metric_ruler_spec.csv`` so the spec stays one readable row per metric (P-05)
- ``metric_fit_report.csv``  per metric: configured weight, whether the column was
  present, whether a ruler was fitted, and why not (P-08)
- ``coverage_report.csv``    finite values per state x metric x slice against the
  **canonical district roster**, with national totals derived from those same
  rows (P-09)
- ``maps/*.png``             national district choropleths, one 2x4 panel per
  ruler x score field (composite, baseline-referenced, absolute-threshold)
- ``summary.md``             the headline numbers, pitfall-tagged

Pilot-grade caveat: national coverage is still in flux (AP republish, Lakshadweep
sub-cell fill, groundwater rewire). Treat every number here as indicative of
ruler *behaviour*, not as a publishable score.

Usage
-----
    python -m tools.diagnostics.heat_risk_national_ruler_pilot --help
    python -m tools.diagnostics.heat_risk_national_ruler_pilot \\
        --out-dir docs/diagnostics/heat_risk_pilot

The canonical district roster and district areas are read from the geometry
GeoJSON **property tables** with the stdlib ``json`` module, so every table —
including the area-weighted state means — is produced without geopandas. A
missing or area-less roster is a hard error by default (pass
``--allow-missing-geometry`` to degrade to unweighted state means). geopandas and
matplotlib are needed only to *render* the choropleths; ``--no-maps`` skips that
step and changes no table.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from india_resilience_tool.analysis.bundle_scores import BundleMetricSpec
from india_resilience_tool.app.geography import list_available_states_from_processed_root
from india_resilience_tool.compute.composite_metrics import (
    _bundle_metric_specs,
    _build_wide_component_frame,
    _load_component_master,
    _required_id_columns,
)
from india_resilience_tool.config.composite_metrics import get_composite_metric_for_bundle
from india_resilience_tool.config.paths import get_paths_config, resolve_processed_root

BUNDLE_DOMAIN = "Heat Risk"

#: The pilot is district-level only (CHG-0352). Block identifiers use ``block_key``
#: rather than ``district_key``, and neither the geometry roster nor the state
#: aggregation below is defined for them, so the level is a constant, not a flag.
LEVEL = "district"

#: The frozen slice grid the rulers are fitted on. Changing this changes every
#: score (pitfall P-04), so it is written into the outputs verbatim.
SLICES: tuple[tuple[str, str], ...] = (
    ("historical", "1990-2010"),
    ("ssp245", "2020-2040"),
    ("ssp245", "2040-2060"),
    ("ssp245", "2060-2080"),
    ("ssp585", "2020-2040"),
    ("ssp585", "2040-2060"),
    ("ssp585", "2060-2080"),
)

#: Heat Risk metrics whose value is defined *relative to the district's own
#: baseline distribution* (ETCCDI percentile and percentile-spell indices). The
#: complement is scored against absolute physical thresholds or absolute levels.
#: Verified against config/bundle_weights.py: 0.367 vs 0.633 of bundle weight.
BASELINE_REFERENCED_SLUGS: frozenset[str] = frozenset(
    {
        "tn90p_warm_nights_pct",
        "tx90p_hot_days_pct",
        "wsdi_warm_spell_days",
        "hwfi_tmean_90p",
        "hwfi_events_tmean_90p",
    }
)

#: The 21-point quantile grid. No longer the ``cdf`` ruler itself — it is fitted
#: alongside the exact mid-rank ruler purely to measure what re-quantizing to a
#: compact knot set would cost (CHG-0353, P-05/P-11).
CDF_QUANTILES: np.ndarray = np.linspace(0.0, 1.0, 21)
LINEAR_LOW_Q = 0.01
LINEAR_HIGH_Q = 0.99
DEFAULT_COVERAGE_GATE = 0.70


# ---------------------------------------------------------------------------
# Rulers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MetricRuler:
    """One frozen national ruler for a single metric, on a 0-100 higher-worse scale."""

    metric_slug: str
    kind: str  # "linear" | "cdf"
    higher_is_worse: bool
    knot_values: np.ndarray  # strictly increasing
    knot_scores: np.ndarray  # 0..100, same length as knot_values
    pooled_min: float
    pooled_max: float
    pooled_n: int
    duplicate_knots: int
    modal_value: float
    modal_mass: float
    #: Observation count behind each knot. Only the exact ``cdf`` ruler has one.
    knot_counts: Optional[np.ndarray] = None
    #: Max/mean |score| deviation of the 21-knot grid from this exact ruler,
    #: evaluated on the pooled sample. NaN for ``linear`` (CHG-0353).
    approx_max_score_error: float = float("nan")
    approx_mean_score_error: float = float("nan")

    def apply(self, values: pd.Series) -> pd.Series:
        """Score a value series against this ruler, clamping outside the knot range."""
        numeric = pd.to_numeric(values, errors="coerce")
        out = pd.Series(np.nan, index=numeric.index, dtype=float)
        finite_mask = np.isfinite(numeric.to_numpy(dtype=float, na_value=np.nan))
        if not finite_mask.any():
            return out
        finite = numeric.to_numpy(dtype=float, na_value=np.nan)[finite_mask]
        if self.knot_values.size == 1:
            scored = np.full(finite.shape, 50.0)
        else:
            scored = np.interp(finite, self.knot_values, self.knot_scores)
        if not self.higher_is_worse:
            scored = 100.0 - scored
        out.iloc[np.flatnonzero(finite_mask)] = np.clip(scored, 0.0, 100.0)
        return out

    def clamped_mask(self, values: pd.Series) -> pd.Series:
        """True where a finite value falls outside the ruler's knot range."""
        numeric = pd.to_numeric(values, errors="coerce")
        arr = numeric.to_numpy(dtype=float, na_value=np.nan)
        finite = np.isfinite(arr)
        low = self.knot_values[0]
        high = self.knot_values[-1]
        return pd.Series(finite & ((arr < low) | (arr > high)), index=numeric.index)


def _collapse_duplicate_knots(
    values: np.ndarray, scores: np.ndarray
) -> tuple[np.ndarray, np.ndarray, int]:
    """Collapse tied knot values to a single strictly increasing knot at mid-rank score.

    Zero-inflated metrics (day counts, spell days) produce several identical
    quantile knots. Interpolating over a zero-width segment is undefined, so ties
    are merged and given the mean of the tied scores, i.e. the mid-rank score for
    that tied block (pitfall P-05).
    """
    frame = pd.DataFrame({"value": values, "score": scores})
    grouped = frame.groupby("value", as_index=False, sort=True)["score"].mean()
    duplicates = int(len(frame) - len(grouped))
    return (
        grouped["value"].to_numpy(dtype=float),
        grouped["score"].to_numpy(dtype=float),
        duplicates,
    )


def _exact_midrank_cdf(pool: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Exact empirical mid-rank CDF of a pooled sample (CHG-0353, pitfall P-05).

    A value observed ``c`` times with ``b`` strictly smaller observations scores
    ``100 * (b + c / 2) / n``. This is the exact mid-rank of the tied block; a
    fixed quantile grid reproduces it only when tie boundaries happen to fall on
    grid points, which is precisely what P-05 warns about.

    Note the range is open: the lowest score is ``100 * (c / 2) / n > 0`` and the
    highest is below 100, so no district is scored a hard 0 or 100 by rank alone.

    Returns ``(distinct_values, midrank_scores, tie_counts)``.
    """
    values, counts = np.unique(pool, return_counts=True)
    n = float(pool.size)
    below = np.concatenate(([0.0], np.cumsum(counts, dtype=float)[:-1]))
    scores = 100.0 * (below + counts / 2.0) / n
    return values.astype(float), scores.astype(float), counts.astype(np.int64)


def _grid_approximation_error(
    pool: np.ndarray, exact_values: np.ndarray, exact_scores: np.ndarray
) -> tuple[float, float]:
    """Score error of the 21-knot grid against the exact ruler, on the pooled sample.

    This is the number that says whether a compact frozen artifact can replace the
    exact support without changing anyone's score materially (P-05, P-11).
    """
    if pool.size == 0:
        return (float("nan"), float("nan"))
    raw_values = np.quantile(pool, CDF_QUANTILES)
    knot_values, knot_scores, _ = _collapse_duplicate_knots(raw_values, CDF_QUANTILES * 100.0)
    if knot_values.size < 2:
        approx = np.full(pool.shape, 50.0)
    else:
        approx = np.interp(pool, knot_values, knot_scores)
    if exact_values.size < 2:
        exact = np.full(pool.shape, 50.0)
    else:
        exact = np.interp(pool, exact_values, exact_scores)
    deviation = np.abs(approx - exact)
    return (float(deviation.max()), float(deviation.mean()))


def _pool_stats(pool: np.ndarray) -> tuple[float, float]:
    """Return (modal_value, modal_mass) for a pooled sample."""
    if pool.size == 0:
        return (float("nan"), float("nan"))
    counts = pd.Series(pool).value_counts()
    return (float(counts.index[0]), float(counts.iloc[0]) / float(pool.size))


def build_ruler(
    metric_slug: str,
    pool: np.ndarray,
    *,
    kind: str,
    higher_is_worse: bool,
) -> Optional[MetricRuler]:
    """Fit one frozen national ruler from a pooled full-span sample."""
    pool = pool[np.isfinite(pool)]
    if pool.size == 0:
        return None
    modal_value, modal_mass = _pool_stats(pool)
    pooled_min = float(pool.min())
    pooled_max = float(pool.max())

    knot_counts: Optional[np.ndarray] = None
    approx_max = float("nan")
    approx_mean = float("nan")

    if kind == "linear":
        low = float(np.quantile(pool, LINEAR_LOW_Q))
        high = float(np.quantile(pool, LINEAR_HIGH_Q))
        if not np.isfinite(low) or not np.isfinite(high) or high <= low:
            knot_values = np.array([pooled_min], dtype=float)
            knot_scores = np.array([50.0], dtype=float)
            duplicates = 0
        else:
            knot_values = np.array([low, high], dtype=float)
            knot_scores = np.array([0.0, 100.0], dtype=float)
            duplicates = 0
    elif kind == "cdf":
        knot_values, knot_scores, knot_counts = _exact_midrank_cdf(pool)
        approx_max, approx_mean = _grid_approximation_error(pool, knot_values, knot_scores)
        # Every observation beyond the first in each tied block: the tie mass the
        # 21-knot grid used to approximate away (P-05).
        duplicates = int(pool.size - knot_values.size)
    else:
        raise ValueError(f"Unknown ruler kind: {kind!r}")

    return MetricRuler(
        metric_slug=metric_slug,
        kind=kind,
        higher_is_worse=higher_is_worse,
        knot_values=knot_values,
        knot_scores=knot_scores,
        pooled_min=pooled_min,
        pooled_max=pooled_max,
        pooled_n=int(pool.size),
        duplicate_knots=duplicates,
        modal_value=modal_value,
        modal_mass=modal_mass,
        knot_counts=knot_counts,
        approx_max_score_error=approx_max,
        approx_mean_score_error=approx_mean,
    )


# ---------------------------------------------------------------------------
# Data assembly
# ---------------------------------------------------------------------------


def district_geometry_root(data_dir: Path) -> Path:
    """Directory holding the canonical per-state district geometry files."""
    return Path(data_dir) / "processed_optimised" / "geometry" / "admin" / "district"


def load_district_roster(
    data_dir: Path, states: Optional[Sequence[str]] = None, *, verbose: bool = True
) -> pd.DataFrame:
    """Canonical district universe and areas, from the geometry property tables.

    Parsed with the stdlib ``json`` module rather than geopandas: the roster is a
    table concern (it defines the expected universe for coverage, P-09, and the
    weights for area aggregation, P-12) and must not be contingent on the
    plotting stack being importable (CHG-0349/0354).

    Returns one row per ``district_key`` with ``state``, ``district`` and
    ``area_m2``. Raises ``FileNotFoundError`` when no geometry is present at all.
    """
    root = district_geometry_root(data_dir)
    if states:
        wanted = [root / f"state={name}.geojson" for name in states]
        absent = [path.name for path in wanted if not path.exists()]
        if absent and verbose:
            print(f"  [warn] no district geometry for: {', '.join(sorted(absent))}", file=sys.stderr)
        files = [path for path in wanted if path.exists()]
    else:
        files = sorted(root.glob("state=*.geojson"))
    if not files:
        raise FileNotFoundError(f"No district geometry found under {root}")

    rows: list[dict[str, object]] = []
    for path in files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for feature in payload.get("features") or []:
            props = feature.get("properties") or {}
            rows.append(
                {
                    "district_key": props.get("district_key"),
                    "state": props.get("state_name"),
                    "district": props.get("district_name"),
                    "area_m2": pd.to_numeric(props.get("area_m2"), errors="coerce"),
                }
            )
    roster = pd.DataFrame(rows)
    if roster.empty:
        raise FileNotFoundError(f"District geometry under {root} carries no features")
    roster = roster.loc[roster["district_key"].notna()].drop_duplicates(subset=["district_key"])
    return roster.reset_index(drop=True)


def expand_to_roster(
    long_frame: pd.DataFrame, roster: pd.DataFrame, metric_slugs: Sequence[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reindex the assembled frame onto the full roster x slice grid (P-09).

    A district present in the roster but absent from every master must appear as
    an all-NaN row rather than vanish, otherwise coverage is measured against
    "what exists" and can never detect absence (CHG-0354). Master rows whose
    ``district_key`` is not in the roster are dropped from scoring — the roster is
    the universe — but are reported in the returned reconciliation frame.

    Returns ``(expanded_frame, reconciliation)``.
    """
    slices = pd.DataFrame(list(SLICES), columns=["scenario", "period"])
    grid = roster.loc[:, ["district_key", "state", "district"]].merge(slices, how="cross")

    value_columns = [slug for slug in metric_slugs if slug in long_frame.columns]
    payload = long_frame.loc[:, ["district_key", "scenario", "period"] + value_columns]
    expanded = grid.merge(payload, on=["district_key", "scenario", "period"], how="left")
    for slug in metric_slugs:
        if slug not in expanded.columns:
            expanded[slug] = np.nan
        expanded[slug] = pd.to_numeric(expanded[slug], errors="coerce")

    finite = _finite_mask_frame(expanded, metric_slugs)
    present = (
        finite.any(axis=1)
        .groupby(expanded["district_key"], sort=False)
        .sum()
        .rename("n_slices_with_any_metric")
    )
    reconciliation = roster.loc[:, ["district_key", "state", "district"]].merge(
        present, left_on="district_key", right_index=True, how="left"
    )
    reconciliation["n_slices_with_any_metric"] = (
        reconciliation["n_slices_with_any_metric"].fillna(0).astype(int)
    )
    reconciliation["status"] = np.where(
        reconciliation["n_slices_with_any_metric"] > 0, "roster_and_master", "roster_no_master"
    )

    orphan_keys = sorted(set(long_frame["district_key"].dropna()) - set(roster["district_key"]))
    if orphan_keys:
        orphan_states = (
            long_frame.loc[long_frame["district_key"].isin(orphan_keys)]
            .drop_duplicates(subset=["district_key"])
            .set_index("district_key")
        )
        orphans = pd.DataFrame(
            {
                "district_key": orphan_keys,
                "state": [orphan_states.at[key, "state"] for key in orphan_keys],
                "district": [orphan_states.at[key, "district"] for key in orphan_keys],
                "n_slices_with_any_metric": np.nan,
                "status": "master_not_in_roster",
            }
        )
        reconciliation = pd.concat([reconciliation, orphans], ignore_index=True)

    return expanded, reconciliation


def discover_states(metric_slugs: Sequence[str], *, data_dir: Path) -> list[str]:
    """Union of states with processed output for any component metric."""
    states: list[str] = []
    seen: set[str] = set()
    for slug in metric_slugs:
        root = resolve_processed_root(slug, data_dir=data_dir, mode="portfolio")
        for state_name in list_available_states_from_processed_root(str(root)):
            if state_name not in seen:
                seen.add(state_name)
                states.append(state_name)
    return sorted(states)


def load_national_long_frame(
    metric_slugs: Sequence[str],
    *,
    level: str,
    states: Sequence[str],
    data_dir: Path,
    verbose: bool = True,
) -> pd.DataFrame:
    """Assemble one national long frame: id columns + scenario/period + one column per metric.

    Unlike the production path, availability is **not** intersected across
    component metrics: a metric missing for a state is left NaN so that coverage
    can be measured rather than silently dropping the state (pitfall P-08/P-09).
    """
    id_columns = list(_required_id_columns(level))
    rows: list[pd.DataFrame] = []
    for state_name in states:
        component_frames: dict[str, pd.DataFrame] = {}
        for slug in metric_slugs:
            frame = _load_component_master(slug, level=level, state_name=state_name, data_dir=data_dir)
            if frame is not None and not frame.empty:
                component_frames[slug] = frame
        if not component_frames:
            if verbose:
                print(f"  [skip] {state_name}: no component masters", file=sys.stderr)
            continue
        for scenario, period in SLICES:
            wide = _build_wide_component_frame(
                component_frames, level=level, scenario=scenario, period=period
            )
            if wide is None or wide.empty:
                continue
            wide = wide.copy()
            for slug in metric_slugs:
                if slug not in wide.columns:
                    wide[slug] = np.nan
            wide["scenario"] = scenario
            wide["period"] = period
            rows.append(wide.loc[:, id_columns + ["scenario", "period"] + list(metric_slugs)])
        if verbose:
            print(f"  [ok] {state_name}: {len(component_frames)}/{len(metric_slugs)} metrics", file=sys.stderr)
    if not rows:
        return pd.DataFrame(columns=id_columns + ["scenario", "period"] + list(metric_slugs))
    out = pd.concat(rows, ignore_index=True)
    for slug in metric_slugs:
        out[slug] = pd.to_numeric(out[slug], errors="coerce")
    return out


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def _finite_mask_frame(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """Boolean frame: True where a column holds a finite value.

    `MetricRuler.apply` scores only finite values, so every count of "available"
    data — coverage report included — must use this mask rather than `.notna()`,
    which admits +/-inf and would report coverage a district never receives
    (CHG-0351).
    """
    present = [column for column in columns if column in frame.columns]
    numeric = frame.loc[:, present].apply(pd.to_numeric, errors="coerce")
    return pd.DataFrame(
        np.isfinite(numeric.to_numpy(dtype=float, na_value=np.nan)),
        index=frame.index,
        columns=present,
    )


def _weighted_row_score(
    score_frame: pd.DataFrame,
    weights: pd.Series,
    *,
    total_weight: Optional[float] = None,
) -> tuple[pd.Series, pd.Series]:
    """Weighted mean over available columns, renormalized per row.

    Returns (score, available_weight_fraction). Mirrors
    `analysis/bundle_scores.compute_bundle_score_frame` so the pilot's composite
    is comparable to the production one apart from the normalization step.

    `total_weight` is the coverage **denominator** and must be the total
    *configured* bundle weight. Defaulting it to `weights.sum()` would silently
    drop a metric that is absent from the whole pool out of both numerator and
    denominator, letting such rows report full coverage against a smaller
    universe than the gate claims (CHG-0350).
    """
    denominator = float(weights.sum()) if total_weight is None else float(total_weight)
    available = _finite_mask_frame(score_frame, list(score_frame.columns)).mul(
        weights, axis=1
    ).sum(axis=1)
    weighted = score_frame.mul(weights, axis=1).sum(axis=1, skipna=True)
    score = weighted.div(available.where(available > 0.0))
    coverage = available / denominator if denominator > 0 else available * np.nan
    return score, coverage


def score_national_frame(
    long_frame: pd.DataFrame,
    *,
    metric_specs: Sequence[BundleMetricSpec],
    rulers: dict[str, MetricRuler],
    id_columns: Sequence[str],
    coverage_gate: float,
) -> pd.DataFrame:
    """Score one national long frame against one set of metric rulers."""
    out = long_frame.loc[:, list(id_columns) + ["scenario", "period"]].copy()
    scored: dict[str, pd.Series] = {}
    clamped = pd.Series(0, index=long_frame.index, dtype=int)
    for spec in metric_specs:
        ruler = rulers.get(spec.slug)
        if ruler is None or spec.slug not in long_frame.columns:
            continue
        scored[spec.slug] = ruler.apply(long_frame[spec.slug])
        clamped = clamped.add(ruler.clamped_mask(long_frame[spec.slug]).astype(int), fill_value=0)

    if not scored:
        raise RuntimeError("No metric could be scored; check processed data availability.")

    score_frame = pd.DataFrame(scored, index=long_frame.index)
    weights = pd.Series(
        {spec.slug: float(spec.weight) for spec in metric_specs if spec.slug in score_frame.columns},
        dtype=float,
    )

    # Denominators come from the configured bundle, not from what happened to fit.
    configured_total = sum(float(spec.weight) for spec in metric_specs)
    configured_baseline = sum(
        float(spec.weight) for spec in metric_specs if spec.slug in BASELINE_REFERENCED_SLUGS
    )
    configured_absolute = configured_total - configured_baseline

    composite, coverage = _weighted_row_score(
        score_frame, weights, total_weight=configured_total
    )
    out["composite"] = composite
    out["weight_coverage"] = coverage
    out["composite_gated"] = composite.where(coverage >= coverage_gate)
    out["clamped_metric_count"] = clamped.astype(int)

    baseline_cols = [c for c in score_frame.columns if c in BASELINE_REFERENCED_SLUGS]
    absolute_cols = [c for c in score_frame.columns if c not in BASELINE_REFERENCED_SLUGS]
    if baseline_cols:
        sub, sub_coverage = _weighted_row_score(
            score_frame[baseline_cols],
            weights[baseline_cols],
            total_weight=configured_baseline,
        )
        out["composite_baseline_referenced"] = sub
        out["coverage_baseline_referenced"] = sub_coverage
    if absolute_cols:
        sub, sub_coverage = _weighted_row_score(
            score_frame[absolute_cols],
            weights[absolute_cols],
            total_weight=configured_absolute,
        )
        out["composite_absolute_threshold"] = sub
        out["coverage_absolute_threshold"] = sub_coverage

    for slug, series in scored.items():
        out[f"score__{slug}"] = series
    return out


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def slice_summary(scored: pd.DataFrame, *, ruler_kind: str) -> pd.DataFrame:
    """Realized composite spread, saturation and clamping per slice (P-02/03/06/07)."""
    rows: list[dict[str, object]] = []
    for (scenario, period), group in scored.groupby(["scenario", "period"], sort=False):
        values = pd.to_numeric(group["composite"], errors="coerce").dropna()
        rows.append(
            {
                "ruler": ruler_kind,
                "scenario": scenario,
                "period": period,
                "n_districts": int(len(group)),
                "n_scored": int(values.size),
                "composite_min": float(values.min()) if values.size else np.nan,
                "composite_p25": float(values.quantile(0.25)) if values.size else np.nan,
                "composite_median": float(values.median()) if values.size else np.nan,
                "composite_p75": float(values.quantile(0.75)) if values.size else np.nan,
                "composite_max": float(values.max()) if values.size else np.nan,
                "composite_iqr": float(values.quantile(0.75) - values.quantile(0.25)) if values.size else np.nan,
                "composite_range": float(values.max() - values.min()) if values.size else np.nan,
                "pct_score_ge_99": float((values >= 99.0).mean() * 100.0) if values.size else np.nan,
                "pct_score_le_1": float((values <= 1.0).mean() * 100.0) if values.size else np.nan,
                "mean_clamped_metrics": float(group["clamped_metric_count"].mean()),
                "pct_below_coverage_gate": float(group["composite_gated"].isna().mean() * 100.0),
            }
        )
    return pd.DataFrame(rows)


def ruler_spec_frame(rulers: dict[str, MetricRuler]) -> pd.DataFrame:
    """Serialize the fitted rulers for audit (P-05, P-06)."""
    rows: list[dict[str, object]] = []
    for slug, ruler in rulers.items():
        rows.append(
            {
                "metric_slug": slug,
                "ruler": ruler.kind,
                "higher_is_worse": ruler.higher_is_worse,
                "pooled_n": ruler.pooled_n,
                "pooled_min": ruler.pooled_min,
                "pooled_max": ruler.pooled_max,
                "knot_low": float(ruler.knot_values[0]),
                "knot_high": float(ruler.knot_values[-1]),
                "n_knots": int(ruler.knot_values.size),
                "tied_observations": ruler.duplicate_knots,
                "tied_fraction": (
                    ruler.duplicate_knots / ruler.pooled_n if ruler.pooled_n else np.nan
                ),
                "modal_value": ruler.modal_value,
                "modal_mass_fraction": ruler.modal_mass,
                "grid21_max_score_error": ruler.approx_max_score_error,
                "grid21_mean_score_error": ruler.approx_mean_score_error,
                # The exact CDF has one knot per distinct pooled value, which does
                # not belong in a spreadsheet cell — it is written to
                # ``cdf_support.csv`` instead (CHG-0353).
                "knots_json": (
                    json.dumps(
                        [
                            [float(v), float(sc)]
                            for v, sc in zip(
                                ruler.knot_values.tolist(), ruler.knot_scores.tolist()
                            )
                        ]
                    )
                    if ruler.knot_values.size <= CDF_QUANTILES.size
                    else "see cdf_support.csv"
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(["ruler", "metric_slug"]).reset_index(drop=True)


def metric_fit_report(
    metric_specs: Sequence[BundleMetricSpec],
    rulers: dict[str, MetricRuler],
    long_frame: pd.DataFrame,
    *,
    ruler_kind: str,
) -> pd.DataFrame:
    """Per configured metric: is it present, did a ruler fit, and what weight rides on it.

    A metric absent from the entire pool contributes weight to the coverage
    denominator but can never contribute score. That gap is invisible in the
    scored frame, so it is reported explicitly here (CHG-0350).
    """
    configured_total = sum(float(spec.weight) for spec in metric_specs)
    rows: list[dict[str, object]] = []
    for spec in metric_specs:
        present = spec.slug in long_frame.columns
        n_finite = (
            int(_finite_mask_frame(long_frame, [spec.slug])[spec.slug].sum()) if present else 0
        )
        ruler = rulers.get(spec.slug)
        if ruler is not None:
            reason = ""
        elif not present:
            reason = "column absent from assembled frame"
        elif n_finite == 0:
            reason = "no finite value anywhere in the pool"
        else:
            reason = "ruler fit failed"
        rows.append(
            {
                "ruler": ruler_kind,
                "metric_slug": spec.slug,
                "weight": float(spec.weight),
                "weight_fraction": (
                    float(spec.weight) / configured_total if configured_total > 0 else np.nan
                ),
                "present_in_frame": present,
                "n_finite": n_finite,
                "ruler_fitted": ruler is not None,
                "reason_not_fitted": reason,
            }
        )
    return pd.DataFrame(rows)


def cdf_support_frame(rulers: dict[str, MetricRuler], *, ruler_kind: str) -> pd.DataFrame:
    """Long-form exact CDF support: one row per (metric, distinct pooled value)."""
    rows: list[pd.DataFrame] = []
    for slug, ruler in rulers.items():
        if ruler.knot_counts is None:
            continue
        rows.append(
            pd.DataFrame(
                {
                    "ruler": ruler_kind,
                    "metric_slug": slug,
                    "knot_value": ruler.knot_values,
                    "midrank_score": ruler.knot_scores,
                    "tie_count": ruler.knot_counts,
                }
            )
        )
    if not rows:
        return pd.DataFrame(
            columns=["ruler", "metric_slug", "knot_value", "midrank_score", "tie_count"]
        )
    return pd.concat(rows, ignore_index=True)


def coverage_report(long_frame: pd.DataFrame, metric_slugs: Sequence[str]) -> pd.DataFrame:
    """Finite values per state x metric x slice, with national totals (P-09).

    The input must already be expanded onto the canonical roster grid, so
    ``n_rows`` is the *expected* district count for that state and slice rather
    than however many rows a master happened to contain. National rows are summed
    from the same state rows, so the two levels cannot disagree (CHG-0354).
    """
    finite = _finite_mask_frame(long_frame, metric_slugs)
    keys = long_frame.loc[:, ["state", "scenario", "period"]]

    rows: list[dict[str, object]] = []
    for (state, scenario, period), index in keys.groupby(
        ["state", "scenario", "period"], sort=False
    ).groups.items():
        block = finite.loc[index]
        for slug in metric_slugs:
            n_finite = int(block[slug].sum()) if slug in block.columns else 0
            rows.append(
                {
                    "scope": "state",
                    "state": state,
                    "scenario": scenario,
                    "period": period,
                    "metric_slug": slug,
                    "n_rows": int(len(index)),
                    "n_finite": n_finite,
                    "coverage_pct": round(100.0 * n_finite / max(len(index), 1), 2),
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    national = (
        frame.groupby(["scenario", "period", "metric_slug"], as_index=False, sort=False)[
            ["n_rows", "n_finite"]
        ]
        .sum()
        .assign(scope="national", state="__ALL_STATES__")
    )
    national["coverage_pct"] = (
        100.0 * national["n_finite"] / national["n_rows"].where(national["n_rows"] > 0)
    ).round(2)
    return pd.concat([frame, national.loc[:, frame.columns]], ignore_index=True)


def state_scores(
    scored: pd.DataFrame, *, areas: Optional[pd.DataFrame], ruler_kind: str
) -> pd.DataFrame:
    """Area-weighted vs unweighted state means (P-12)."""
    frame = scored.loc[:, ["state", "district_key", "scenario", "period", "composite"]].copy()
    if areas is not None:
        frame = frame.merge(areas, on="district_key", how="left")
    else:
        frame["area_m2"] = np.nan

    rows: list[dict[str, object]] = []
    for (state, scenario, period), group in frame.groupby(
        ["state", "scenario", "period"], dropna=False, sort=False
    ):
        values = pd.to_numeric(group["composite"], errors="coerce")
        weights = pd.to_numeric(group["area_m2"], errors="coerce")
        mask = values.notna()
        unweighted = float(values[mask].mean()) if mask.any() else np.nan
        wmask = mask & weights.notna() & (weights > 0)
        weighted = (
            float(np.average(values[wmask], weights=weights[wmask])) if wmask.any() else np.nan
        )
        rows.append(
            {
                "state": state,
                "scenario": scenario,
                "period": period,
                "ruler": ruler_kind,
                "unweighted_mean": unweighted,
                "area_weighted_mean": weighted,
                "delta_area_minus_unweighted": weighted - unweighted,
                "n_districts_scored": int(mask.sum()),
                "n_districts_with_area": int(wmask.sum()),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Geometry / maps
# ---------------------------------------------------------------------------


def load_district_geometry(data_dir: Path, states: Optional[Sequence[str]] = None):
    """Load district geometry for rendering, optionally restricted to some states.

    Rendering is the only step that needs geopandas; the roster and areas that
    feed the tables are read from the same files with `json` (CHG-0349).
    """
    import geopandas as gpd  # local import: tables-only runs must not require geopandas

    root = district_geometry_root(data_dir)
    if states:
        wanted = [root / f"state={name}.geojson" for name in states]
        files = [path for path in wanted if path.exists()]
    else:
        files = sorted(root.glob("state=*.geojson"))
    if not files:
        raise FileNotFoundError(f"No district geometry found under {root}")
    frames = [gpd.read_file(path) for path in files]
    gdf = pd.concat(frames, ignore_index=True)
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=frames[0].crs)
    keep = [c for c in ("district_key", "state_name", "district_name", "area_m2") if c in gdf.columns]
    return gdf.loc[:, keep + ["geometry"]]


def render_map_panel(
    gdf,
    scored: pd.DataFrame,
    *,
    value_col: str,
    title: str,
    out_path: Path,
    fixed_domain: bool,
) -> None:
    """Render one 2x4 panel of national district choropleths, one per slice."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    values = pd.to_numeric(scored[value_col], errors="coerce").dropna()
    if fixed_domain:
        vmin, vmax = 0.0, 100.0
        domain_note = "fixed 0-100"
    else:
        vmin, vmax = (float(values.min()), float(values.max())) if values.size else (0.0, 100.0)
        domain_note = f"auto {vmin:.1f}-{vmax:.1f}"

    fig, axes = plt.subplots(2, 4, figsize=(18, 12))
    axes = axes.ravel()
    for ax, (scenario, period) in zip(axes, SLICES):
        subset = scored[(scored["scenario"] == scenario) & (scored["period"] == period)]
        merged = gdf.merge(
            subset.loc[:, ["district_key", value_col]], on="district_key", how="left"
        )
        merged.plot(
            column=value_col,
            ax=ax,
            cmap="YlOrRd",
            vmin=vmin,
            vmax=vmax,
            linewidth=0.05,
            edgecolor="#999999",
            missing_kwds={"color": "#eeeeee", "edgecolor": "#cccccc", "hatch": "//"},
        )
        ax.set_title(f"{scenario} {period}", fontsize=10)
        ax.set_axis_off()
    for ax in axes[len(SLICES):]:
        ax.set_axis_off()

    sm = plt.cm.ScalarMappable(cmap="YlOrRd", norm=plt.Normalize(vmin=vmin, vmax=vmax))
    cbar = fig.colorbar(sm, ax=axes.tolist(), fraction=0.02, pad=0.02)
    cbar.set_label(f"{value_col} ({domain_note})")
    fig.suptitle(title, fontsize=14)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=130, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _as_table(frame: pd.DataFrame) -> str:
    """Markdown table when `tabulate` is installed, fixed-width text otherwise."""
    try:
        return frame.to_markdown(index=False, floatfmt=".3f")
    except ImportError:
        return "```\n" + frame.to_string(index=False, float_format=lambda v: f"{v:.3f}") + "\n```"


def _write_summary(
    out_dir: Path,
    *,
    level: str,
    states: Sequence[str],
    metric_specs: Sequence[BundleMetricSpec],
    summaries: pd.DataFrame,
    specs: pd.DataFrame,
    coverage_gate: float,
    scored_by_ruler: dict[str, pd.DataFrame],
    fit_frame: pd.DataFrame,
    reconciliation: pd.DataFrame,
) -> None:
    lines: list[str] = []
    lines.append("# Heat Risk national-ruler pilot — summary\n")
    lines.append("**Pilot-grade. Nothing here is frozen, published, or committed to config.**\n")
    lines.append(
        f"Level: `{level}` · states: {len(states)} · metrics: {len(metric_specs)} · "
        f"slices: {len(SLICES)} · coverage gate: {coverage_gate:.0%}\n"
    )

    baseline_weight = sum(
        float(s.weight) for s in metric_specs if s.slug in BASELINE_REFERENCED_SLUGS
    )
    absolute_weight = sum(
        float(s.weight) for s in metric_specs if s.slug not in BASELINE_REFERENCED_SLUGS
    )
    lines.append(
        f"Weight split — baseline-referenced {baseline_weight:.3f} / "
        f"absolute-threshold {absolute_weight:.3f}\n"
    )

    lines.append("\n## Realized composite spread per slice (P-02, P-03)\n")
    cols = [
        "ruler", "scenario", "period", "n_scored",
        "composite_min", "composite_median", "composite_max",
        "composite_iqr", "pct_score_ge_99", "pct_score_le_1",
    ]
    lines.append(_as_table(summaries.loc[:, cols]))

    lines.append("\n\n## Roster reconciliation (P-09, P-10)\n")
    if reconciliation.empty:
        lines.append("- No canonical roster was available; coverage below is not against a fixed universe.")
    else:
        counts = reconciliation["status"].value_counts()
        for status, label in (
            ("roster_and_master", "roster districts with at least one master row"),
            ("roster_no_master", "roster districts with **no** master row in any slice"),
            ("master_not_in_roster", "master district_keys absent from the roster (dropped)"),
        ):
            lines.append(f"- {int(counts.get(status, 0))} {label}")

    lines.append("\n\n## Metric fit (P-08)\n")
    unfitted = fit_frame.loc[~fit_frame["ruler_fitted"]]
    if unfitted.empty:
        lines.append(
            "- Every configured metric fitted a ruler for every ruler kind. "
            "The coverage denominator is the full configured bundle weight."
        )
    else:
        lines.append(
            _as_table(
                unfitted.loc[
                    :, ["ruler", "metric_slug", "weight_fraction", "n_finite", "reason_not_fitted"]
                ]
            )
        )
        lines.append(
            "\nThis weight remains in the coverage denominator and can never be scored, "
            "so affected rows are correctly reported as short of full coverage (CHG-0350)."
        )

    lines.append("\n\n## Ruler fit (P-05, P-06)\n")
    lines.append(
        _as_table(
            specs.loc[
                :,
                ["ruler", "metric_slug", "pooled_n", "knot_low", "knot_high", "n_knots",
                 "tied_fraction", "modal_mass_fraction",
                 "grid21_max_score_error", "grid21_mean_score_error"],
            ]
        )
    )
    lines.append(
        "\n`grid21_*_score_error` is how far the compact 21-knot quantile grid departs "
        "from the exact mid-rank CDF on the pooled sample. Small values mean a frozen "
        "artifact can carry the grid instead of the full support (P-05, P-11)."
    )

    lines.append("\n\n## Coverage gate effect (P-08)\n")
    for kind, scored in scored_by_ruler.items():
        dropped = float(scored["composite_gated"].isna().mean() * 100.0)
        low = scored.loc[scored["weight_coverage"] < coverage_gate]
        lines.append(
            f"- `{kind}`: {dropped:.2f}% of district-slices fall below the "
            f"{coverage_gate:.0%} weight-coverage gate "
            f"({low['state'].nunique()} states affected)."
        )

    lines.append("\n\n## How to read this\n")
    lines.append(
        "- If `linear` and `cdf` disagree about *which* districts are worst, the ruler "
        "choice is methodologically load-bearing and must be argued, not defaulted (P-01).\n"
        "- If `composite_iqr` is small under `cdf` but the map still looks differentiated, "
        "contrast is being manufactured by uniformization (P-01/P-02).\n"
        "- If `pct_score_ge_99` climbs steeply between 2040-2060 and 2060-2080, the ruler is "
        "saturating and late-century differences are being lost (P-07).\n"
        "- If the baseline slice is near-uniform, that is expected under a full-span pooled "
        "ruler and is a product decision, not a defect (P-03).\n"
        "- The exact mid-rank `cdf` ruler cannot return a hard 0 or 100 by construction, so "
        "`pct_score_ge_99` / `pct_score_le_1` read lower for it than for `linear` at equal "
        "saturation. Compare each ruler against itself across slices, not across rulers.\n"
        "- `roster_no_master` districts are scored NaN, not 0. They lower coverage, they do "
        "not lower the composite (P-09).\n"
    )
    (out_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Heat Risk national-ruler pilot: linear p1-p99 vs empirical CDF (read-only)."
    )
    parser.add_argument(
        "--out-dir",
        default="docs/diagnostics/heat_risk_pilot",
        help="Directory for pilot outputs (created if absent). Default: %(default)s",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Override IRT data dir; defaults to the resolved paths config.",
    )
    parser.add_argument(
        "--states",
        default=None,
        help="Comma-separated state subset for a fast smoke run. Default: all discovered states.",
    )
    parser.add_argument(
        "--rulers",
        default="linear,cdf",
        help="Comma-separated ruler kinds to fit. Default: %(default)s",
    )
    parser.add_argument(
        "--coverage-gate",
        type=float,
        default=DEFAULT_COVERAGE_GATE,
        help="Minimum fraction of bundle weight required for a gated score. Default: %(default)s",
    )
    parser.add_argument("--no-maps", action="store_true", help="Skip choropleths; tables only.")
    parser.add_argument(
        "--allow-missing-geometry",
        action="store_true",
        help=(
            "Continue when the canonical district roster or its areas are missing. "
            "Off by default: without the roster there is no expected universe for "
            "coverage (P-09) and no weights for area aggregation (P-12), so the run "
            "would emit blank diagnostics that read as passes."
        ),
    )
    parser.add_argument(
        "--map-domain",
        default="fixed",
        choices=("fixed", "auto"),
        help="Colour domain for the maps: fixed 0-100, or auto-scaled to the data. Default: %(default)s",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress per-state progress lines.")
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir) if args.data_dir else get_paths_config().data_dir
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    level = LEVEL
    verbose = not args.quiet
    ruler_kinds = [k.strip() for k in str(args.rulers).split(",") if k.strip()]

    spec = get_composite_metric_for_bundle(BUNDLE_DOMAIN)
    if spec is None:
        print(f"No composite spec for bundle {BUNDLE_DOMAIN!r}", file=sys.stderr)
        return 2
    metric_specs = _bundle_metric_specs(spec)
    metric_slugs = [s.slug for s in metric_specs]
    id_columns = list(_required_id_columns(level))

    requested_states = (
        [name.strip() for name in args.states.split(",") if name.strip()] if args.states else None
    )

    # The canonical roster is the expected universe for coverage and the weight
    # source for area aggregation, so it is loaded before any master (CHG-0354).
    roster: Optional[pd.DataFrame] = None
    try:
        roster = load_district_roster(data_dir, states=requested_states, verbose=verbose)
    except Exception as exc:
        if not args.allow_missing_geometry:
            print(
                f"[error] canonical district roster unavailable ({exc}). "
                "Coverage and area weighting cannot be computed. "
                "Re-run with --allow-missing-geometry to proceed without them.",
                file=sys.stderr,
            )
            return 2
        print(f"[warn] roster unavailable ({exc}); continuing degraded.", file=sys.stderr)

    areas: Optional[pd.DataFrame] = None
    if roster is not None:
        if roster["area_m2"].notna().any():
            areas = roster.loc[:, ["district_key", "area_m2"]].copy()
        elif not args.allow_missing_geometry:
            print(
                "[error] district roster carries no area_m2; area-weighted state means "
                "(P-12) would be blank. Re-run with --allow-missing-geometry to proceed.",
                file=sys.stderr,
            )
            return 2
        else:
            print("[warn] roster carries no area_m2; state means will be unweighted.", file=sys.stderr)

    if requested_states:
        states = requested_states
    elif roster is not None:
        states = sorted(roster["state"].dropna().unique().tolist())
    else:
        states = discover_states(metric_slugs, data_dir=data_dir)
    print(f"Heat Risk pilot: {len(metric_slugs)} metrics, {len(states)} states, level={level}", file=sys.stderr)

    long_frame = load_national_long_frame(
        metric_slugs, level=level, states=states, data_dir=data_dir, verbose=verbose
    )
    if long_frame.empty and roster is None:
        print("No data assembled; nothing to do.", file=sys.stderr)
        return 1
    print(f"Assembled {len(long_frame):,} district-slice rows from masters.", file=sys.stderr)

    if roster is not None:
        long_frame, reconciliation = expand_to_roster(long_frame, roster, metric_slugs)
        reconciliation.to_csv(out_dir / "roster_reconciliation.csv", index=False)
        n_no_master = int((reconciliation["status"] == "roster_no_master").sum())
        n_orphan = int((reconciliation["status"] == "master_not_in_roster").sum())
        print(
            f"Roster grid: {len(long_frame):,} district-slice rows "
            f"({n_no_master} roster districts with no master row, "
            f"{n_orphan} master keys not in the roster).",
            file=sys.stderr,
        )
    else:
        reconciliation = pd.DataFrame(columns=["district_key", "state", "district", "status"])

    coverage_report(long_frame, metric_slugs).to_csv(out_dir / "coverage_report.csv", index=False)

    gdf = None
    if not args.no_maps:
        try:
            gdf = load_district_geometry(data_dir, states=states)
        except Exception as exc:  # rendering is the only geopandas-dependent step
            print(f"[warn] geometry not renderable ({exc}); continuing without maps.", file=sys.stderr)
            gdf = None

    all_specs: list[pd.DataFrame] = []
    all_summaries: list[pd.DataFrame] = []
    all_scores: list[pd.DataFrame] = []
    all_states: list[pd.DataFrame] = []
    all_fits: list[pd.DataFrame] = []
    all_support: list[pd.DataFrame] = []
    scored_by_ruler: dict[str, pd.DataFrame] = {}

    for kind in ruler_kinds:
        rulers: dict[str, MetricRuler] = {}
        for metric_spec in metric_specs:
            if metric_spec.slug not in long_frame.columns:
                continue
            pool = pd.to_numeric(long_frame[metric_spec.slug], errors="coerce").to_numpy(
                dtype=float, na_value=np.nan
            )
            ruler = build_ruler(
                metric_spec.slug,
                pool,
                kind=kind,
                higher_is_worse=bool(metric_spec.higher_is_worse),
            )
            if ruler is not None:
                rulers[metric_spec.slug] = ruler
        fit_report = metric_fit_report(metric_specs, rulers, long_frame, ruler_kind=kind)
        unfitted = fit_report.loc[~fit_report["ruler_fitted"]]
        print(f"[{kind}] fitted {len(rulers)}/{len(metric_specs)} metric rulers.", file=sys.stderr)
        if not unfitted.empty:
            # This weight stays in the coverage denominator and can never be
            # scored, so it is never silent (CHG-0350).
            print(
                f"[{kind}] {len(unfitted)} metric(s) unfitted, "
                f"{unfitted['weight_fraction'].sum():.1%} of bundle weight: "
                + ", ".join(unfitted["metric_slug"].tolist()),
                file=sys.stderr,
            )

        scored = score_national_frame(
            long_frame,
            metric_specs=metric_specs,
            rulers=rulers,
            id_columns=id_columns,
            coverage_gate=float(args.coverage_gate),
        )
        scored.insert(0, "ruler", kind)
        scored_by_ruler[kind] = scored

        all_fits.append(fit_report)
        all_support.append(cdf_support_frame(rulers, ruler_kind=kind))
        all_specs.append(ruler_spec_frame(rulers))
        all_summaries.append(slice_summary(scored, ruler_kind=kind))
        all_scores.append(scored)
        all_states.append(state_scores(scored, areas=areas, ruler_kind=kind))

        if gdf is not None:
            for value_col, label in (
                ("composite", "Heat Risk composite"),
                ("composite_baseline_referenced", "Heat Risk — baseline-referenced metrics only"),
                ("composite_absolute_threshold", "Heat Risk — absolute-threshold metrics only"),
            ):
                if value_col not in scored.columns:
                    continue
                render_map_panel(
                    gdf,
                    scored,
                    value_col=value_col,
                    title=f"{label} — {kind} national ruler (pilot-grade)",
                    out_path=out_dir / "maps" / f"{kind}__{value_col}.png",
                    fixed_domain=(args.map_domain == "fixed"),
                )
            print(f"[{kind}] maps written to {out_dir / 'maps'}", file=sys.stderr)

    specs_frame = pd.concat(all_specs, ignore_index=True)
    summary_frame = pd.concat(all_summaries, ignore_index=True)
    fit_frame = pd.concat(all_fits, ignore_index=True)
    support_frame = pd.concat(all_support, ignore_index=True)
    fit_frame.to_csv(out_dir / "metric_fit_report.csv", index=False)
    support_frame.to_csv(out_dir / "cdf_support.csv", index=False)
    specs_frame.to_csv(out_dir / "metric_ruler_spec.csv", index=False)
    summary_frame.to_csv(out_dir / "slice_summary.csv", index=False)
    pd.concat(all_scores, ignore_index=True).to_csv(out_dir / "district_scores.csv", index=False)
    pd.concat(all_states, ignore_index=True).to_csv(out_dir / "state_scores.csv", index=False)

    _write_summary(
        out_dir,
        level=level,
        states=states,
        metric_specs=metric_specs,
        summaries=summary_frame,
        specs=specs_frame,
        coverage_gate=float(args.coverage_gate),
        scored_by_ruler=scored_by_ruler,
        fit_frame=fit_frame,
        reconciliation=reconciliation,
    )
    print(f"Done. Outputs under {out_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

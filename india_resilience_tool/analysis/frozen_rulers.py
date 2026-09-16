"""Frozen national rulers for composite scoring (CHG-0367a).

A *frozen ruler* maps a metric's raw physical value onto a 0-100 higher-is-worse
score through a fixed, persisted transfer function. It replaces per-``(state,
level, scenario, period)`` min-max normalization, which forces a 0 and a 100
inside every state and every slice and so produces scores comparable neither
across states nor across time.

The ruler shipped here is the **exact pooled empirical mid-rank CDF**: one knot
per distinct pooled value, scored at that tied block's mid-rank
``100 * (below + count / 2) / n``. It is fitted once, over the national district
pool across every declared scenario/period slice, and then applied unchanged to
every unit at every level -- so a district and a block holding the same physical
value receive the same score.

Because the ruler is non-linear, the area-weighted mean of a district's block
scores does not equal that district's own score. Both levels are scored from
their own physical values and neither is derived from the other; that is the
declared contract, not a defect.

The fitted artifact lives in the repository under
``india_resilience_tool/config/frozen_rulers/<composite_slug>/<version>/`` and is
read back with :func:`load_ruler_set`, which *reconstructs* rulers from the
persisted support rather than refitting. A refit over a shifted pool would
silently produce different scores under an unchanged column name.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence

import numpy as np
import pandas as pd

#: Ruler kind persisted in the artifact. Only the exact mid-rank CDF is a
#: production ruler; the pilot's ``linear`` comparison ruler is a decided
#: question and is not promoted here.
CDF_KIND = "cdf"

CDF_SUPPORT_COLUMNS: tuple[str, ...] = (
    "metric_slug",
    "support_value",
    "midrank_score",
    "tie_count",
    "observations_below",
    "pooled_n",
)

RULER_SPEC_COLUMNS: tuple[str, ...] = (
    "metric_slug",
    "higher_is_worse",
    "weight",
    "headline_weight",
    "pooled_min",
    "pooled_max",
    "pooled_n",
    "n_knots",
    "modal_value",
    "modal_mass_fraction",
)

SUPPORT_FILENAME = "cdf_support.parquet"
SPEC_FILENAME = "ruler_spec.parquet"
META_FILENAME = "ruler.json"


# ---------------------------------------------------------------------------
# Ruler
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MetricRuler:
    """One frozen national ruler for a single metric, on a 0-100 higher-worse scale."""

    metric_slug: str
    kind: str  # "cdf" in production; the pilot also fits a "linear" comparison ruler
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
    #: Max/mean |score| deviation of a coarse quantile grid from this exact ruler,
    #: evaluated on the pooled sample. Measured by the pilot only (CHG-0353).
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


# ---------------------------------------------------------------------------
# Fitting
# ---------------------------------------------------------------------------


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


def exact_midrank_cdf(pool: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Exact empirical mid-rank CDF of a pooled sample (CHG-0353, pitfall P-05).

    A value observed ``c`` times with ``b`` strictly smaller observations scores
    ``100 * (b + c / 2) / n``. This is the exact mid-rank of the tied block; a
    fixed quantile grid reproduces it only when tie boundaries happen to fall on
    grid points, which is precisely what P-05 warns about.

    Note the range is open: the lowest score is ``100 * (c / 2) / n > 0`` and the
    highest is below 100, so no unit is scored a hard 0 or 100 by rank alone.

    Returns ``(distinct_values, midrank_scores, tie_counts)``.
    """
    values, counts = np.unique(pool, return_counts=True)
    n = float(pool.size)
    below = np.concatenate(([0.0], np.cumsum(counts, dtype=float)[:-1]))
    scores = 100.0 * (below + counts / 2.0) / n
    return values.astype(float), scores.astype(float), counts.astype(np.int64)


def pool_stats(pool: np.ndarray) -> tuple[float, float]:
    """Return ``(modal_value, modal_mass)`` for a pooled sample."""
    if pool.size == 0:
        return (float("nan"), float("nan"))
    counts = pd.Series(pool).value_counts()
    return (float(counts.index[0]), float(counts.iloc[0]) / float(pool.size))


def build_cdf_ruler(
    metric_slug: str,
    pool: np.ndarray,
    *,
    higher_is_worse: bool,
) -> Optional[MetricRuler]:
    """Fit one frozen national ruler from a pooled full-span sample.

    Returns ``None`` when the pool holds no finite value, which is the caller's
    signal that the metric cannot contribute score (and must still contribute
    weight to the coverage denominator -- see :func:`weighted_row_score`).
    """
    pool = np.asarray(pool, dtype=float)
    pool = pool[np.isfinite(pool)]
    if pool.size == 0:
        return None
    modal_value, modal_mass = pool_stats(pool)
    knot_values, knot_scores, knot_counts = exact_midrank_cdf(pool)
    return MetricRuler(
        metric_slug=str(metric_slug),
        kind=CDF_KIND,
        higher_is_worse=bool(higher_is_worse),
        knot_values=knot_values,
        knot_scores=knot_scores,
        pooled_min=float(pool.min()),
        pooled_max=float(pool.max()),
        pooled_n=int(pool.size),
        # Every observation beyond the first in each tied block: the tie mass a
        # coarse quantile grid would approximate away (P-05).
        duplicate_knots=int(pool.size - knot_values.size),
        modal_value=modal_value,
        modal_mass=modal_mass,
        knot_counts=knot_counts,
    )


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------


def finite_mask_frame(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """Boolean frame: True where a column holds a finite value.

    :meth:`MetricRuler.apply` scores only finite values, so every count of
    "available" data must use this mask rather than ``.notna()``, which admits
    +/-inf and would report coverage a unit never receives (CHG-0351).
    """
    present = [column for column in columns if column in frame.columns]
    numeric = frame.loc[:, present].apply(pd.to_numeric, errors="coerce")
    return pd.DataFrame(
        np.isfinite(numeric.to_numpy(dtype=float, na_value=np.nan)),
        index=frame.index,
        columns=present,
    )


def weighted_row_score(
    score_frame: pd.DataFrame,
    weights: pd.Series,
    *,
    total_weight: Optional[float] = None,
) -> tuple[pd.Series, pd.Series]:
    """Weighted mean over available columns, renormalized per row.

    Returns ``(score, available_weight_fraction)``. Mirrors
    ``analysis.bundle_scores.compute_bundle_score_frame`` so a frozen-ruler
    composite is comparable to the min-max one apart from the normalization step.

    ``total_weight`` is the coverage **denominator** and must be the total
    *configured* bundle weight. Defaulting it to ``weights.sum()`` would silently
    drop a metric that is absent from the whole pool out of both numerator and
    denominator, letting such rows report full coverage against a smaller
    universe than the gate claims (CHG-0350).
    """
    denominator = float(weights.sum()) if total_weight is None else float(total_weight)
    available = (
        finite_mask_frame(score_frame, list(score_frame.columns))
        .mul(weights, axis=1)
        .sum(axis=1)
    )
    weighted = score_frame.mul(weights, axis=1).sum(axis=1, skipna=True)
    score = weighted.div(available.where(available > 0.0))
    coverage = available / denominator if denominator > 0 else available * np.nan
    return score, coverage


# ---------------------------------------------------------------------------
# The frozen artifact
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FrozenRulerSet:
    """One persisted, immutable set of metric rulers plus the metadata that dates it."""

    ruler_id: str
    composite_slug: str
    bundle_domain: str
    level_fitted: str
    rulers: dict[str, MetricRuler]
    #: ``(scenario, period)`` pairs the ruler was fitted over. Scoring a pair
    #: outside this grid is refused (P-04).
    slices: tuple[tuple[str, str], ...]
    #: Configured headline weight per metric slug, before renormalization.
    weights: dict[str, float]
    #: Sum of the configured headline weights over the whole bundle, i.e. the
    #: coverage denominator. Not ``sum(weights.values())`` of what happened to fit.
    configured_weight: float
    coverage_gate: float
    ruler_sha256: str = ""
    data_snapshot_hash: str = ""
    fitted_utc: str = ""
    meta: dict[str, object] = field(default_factory=dict)

    def metric_slugs(self) -> tuple[str, ...]:
        return tuple(sorted(self.rulers))

    def validate_slice(self, scenario: str, period: str) -> None:
        """Raise if ``(scenario, period)`` was not part of the fitted grid (P-04)."""
        if (str(scenario), str(period)) not in set(self.slices):
            raise ValueError(
                f"Ruler {self.ruler_id!r} was not fitted over slice "
                f"({scenario!r}, {period!r}); fitted slices are {sorted(self.slices)!r}. "
                "Refusing to score against a ruler that never saw this slice."
            )


def cdf_support_frame(rulers: Mapping[str, MetricRuler]) -> pd.DataFrame:
    """Long-form exact CDF support: one row per (metric, distinct pooled value).

    Row order is fixed by ``(metric_slug, support_value)`` so the file bytes --
    and therefore ``ruler_sha256`` -- reproduce for an identical fit.
    """
    rows: list[pd.DataFrame] = []
    for slug in sorted(rulers):
        ruler = rulers[slug]
        counts = (
            np.asarray(ruler.knot_counts, dtype=np.int64)
            if ruler.knot_counts is not None
            else np.ones(ruler.knot_values.size, dtype=np.int64)
        )
        below = np.concatenate(([0], np.cumsum(counts)[:-1])).astype(np.int64)
        rows.append(
            pd.DataFrame(
                {
                    "metric_slug": str(slug),
                    "support_value": ruler.knot_values.astype(float),
                    "midrank_score": ruler.knot_scores.astype(float),
                    "tie_count": counts,
                    "observations_below": below,
                    "pooled_n": np.int64(ruler.pooled_n),
                }
            )
        )
    if not rows:
        return pd.DataFrame(columns=list(CDF_SUPPORT_COLUMNS))
    out = pd.concat(rows, ignore_index=True)
    return (
        out.sort_values(["metric_slug", "support_value"], kind="mergesort")
        .reset_index(drop=True)
        .loc[:, list(CDF_SUPPORT_COLUMNS)]
    )


def ruler_spec_frame(
    rulers: Mapping[str, MetricRuler],
    *,
    weights: Mapping[str, float],
    headline_weights: Optional[Mapping[str, float]] = None,
) -> pd.DataFrame:
    """One row per metric: orientation, weights and pool shape.

    Orientation lives here and nowhere else -- :class:`MetricRuler` cannot be
    rebuilt from the support table alone.
    """
    headline = dict(headline_weights or {})
    rows: list[dict[str, object]] = []
    for slug in sorted(rulers):
        ruler = rulers[slug]
        rows.append(
            {
                "metric_slug": str(slug),
                "higher_is_worse": bool(ruler.higher_is_worse),
                "weight": float(weights.get(slug, float("nan"))),
                "headline_weight": float(headline.get(slug, float("nan"))),
                "pooled_min": float(ruler.pooled_min),
                "pooled_max": float(ruler.pooled_max),
                "pooled_n": int(ruler.pooled_n),
                "n_knots": int(ruler.knot_values.size),
                "modal_value": float(ruler.modal_value),
                "modal_mass_fraction": float(ruler.modal_mass),
            }
        )
    if not rows:
        return pd.DataFrame(columns=list(RULER_SPEC_COLUMNS))
    return pd.DataFrame(rows).loc[:, list(RULER_SPEC_COLUMNS)]


def _write_frozen_parquet(frame: pd.DataFrame, path: Path) -> None:
    """Write with pinned settings so identical content reproduces identical bytes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(
        path,
        index=False,
        engine="pyarrow",
        compression="zstd",
        version="2.6",
    )


def sha256_file(path: Path) -> str:
    """SHA-256 of a file's bytes."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def snapshot_hash(paths: Iterable[Path], *, root: Optional[Path] = None) -> str:
    """Hash the source files a fit read: sorted ``(relative_path, size, sha256)``.

    This is what dates a ruler to a data snapshot. It is deliberately simple --
    a canonical binary encoding of the pool itself is a separate initiative.
    """
    entries: list[str] = []
    for path in sorted({Path(p) for p in paths}):
        try:
            rel = str(path.relative_to(root)) if root is not None else str(path)
        except ValueError:
            rel = str(path)
        entries.append(f"{rel.replace(chr(92), '/')}|{path.stat().st_size}|{sha256_file(path)}")
    digest = hashlib.sha256()
    for entry in sorted(entries):
        digest.update(entry.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def save_ruler_set(
    ruler_set: FrozenRulerSet,
    out_dir: Path,
    *,
    overwrite: bool = False,
) -> dict[str, Path]:
    """Persist a fitted ruler set as ``cdf_support.parquet`` + ``ruler_spec.parquet`` + ``ruler.json``.

    Refuses an occupied directory unless ``overwrite`` is set. Re-fitting a
    published version in place is how a frozen ruler silently changes.
    """
    out_dir = Path(out_dir)
    support_path = out_dir / SUPPORT_FILENAME
    spec_path = out_dir / SPEC_FILENAME
    meta_path = out_dir / META_FILENAME
    if not overwrite and any(p.exists() for p in (support_path, spec_path, meta_path)):
        raise FileExistsError(
            f"{out_dir} already holds a frozen ruler. Fit a new version directory "
            "instead of overwriting a published one."
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    _write_frozen_parquet(cdf_support_frame(ruler_set.rulers), support_path)
    _write_frozen_parquet(
        ruler_spec_frame(
            ruler_set.rulers,
            weights=ruler_set.weights,
            headline_weights=ruler_set.weights,
        ),
        spec_path,
    )

    meta = {
        "ruler_id": ruler_set.ruler_id,
        "ruler_kind": CDF_KIND,
        "composite_slug": ruler_set.composite_slug,
        "bundle_domain": ruler_set.bundle_domain,
        "level_fitted": ruler_set.level_fitted,
        "slices": [list(pair) for pair in ruler_set.slices],
        "weights": {str(k): float(v) for k, v in sorted(ruler_set.weights.items())},
        "configured_weight": float(ruler_set.configured_weight),
        "coverage_gate": float(ruler_set.coverage_gate),
        "headline_metric_count": len(ruler_set.rulers),
        "pooled_n": int(
            max((r.pooled_n for r in ruler_set.rulers.values()), default=0)
        ),
        "ruler_sha256": sha256_file(support_path),
        "data_snapshot_hash": ruler_set.data_snapshot_hash,
        "fitted_utc": ruler_set.fitted_utc
        or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    meta.update({k: v for k, v in ruler_set.meta.items() if k not in meta})
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"support": support_path, "spec": spec_path, "meta": meta_path}


def load_ruler_set(ruler_dir: Path) -> FrozenRulerSet:
    """Rebuild a :class:`FrozenRulerSet` from a persisted directory.

    Reconstructs; never refits. The support table *is* the ruler: one row per
    distinct pooled value carrying that value's mid-rank score. Validation here
    is deliberately strict -- a ruler that fails to round-trip must fail loudly
    rather than score a published map slightly differently.
    """
    ruler_dir = Path(ruler_dir)
    support_path = ruler_dir / SUPPORT_FILENAME
    spec_path = ruler_dir / SPEC_FILENAME
    meta_path = ruler_dir / META_FILENAME
    for path in (support_path, spec_path, meta_path):
        if not path.exists():
            raise FileNotFoundError(f"Frozen ruler directory {ruler_dir} is missing {path.name}")

    support = pd.read_parquet(support_path)
    for column in ("metric_slug", "support_value", "midrank_score"):
        if column not in support.columns:
            raise ValueError(f"{support_path} has no {column!r} column; not a CDF support table")
    spec = pd.read_parquet(spec_path)
    if "metric_slug" not in spec.columns or "higher_is_worse" not in spec.columns:
        raise ValueError(f"{spec_path} carries no orientation rows")
    spec = spec.set_index("metric_slug")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    rulers: dict[str, MetricRuler] = {}
    for slug, group in support.groupby("metric_slug", sort=True):
        group = group.sort_values("support_value", kind="mergesort")
        values = group["support_value"].to_numpy(dtype=float)
        scores = group["midrank_score"].to_numpy(dtype=float)
        if values.size < 2:
            raise ValueError(f"ruler {slug!r} has fewer than two knots; refusing to score")
        if not np.all(np.diff(values) > 0):
            raise ValueError(f"ruler {slug!r} knots are not strictly increasing")
        if slug not in spec.index:
            raise ValueError(f"{spec_path} has no orientation row for metric {slug!r}")
        row = spec.loc[slug]
        rulers[str(slug)] = MetricRuler(
            metric_slug=str(slug),
            kind=CDF_KIND,
            higher_is_worse=bool(row["higher_is_worse"]),
            knot_values=values,
            knot_scores=scores,
            pooled_min=float(row.get("pooled_min", values[0])),
            pooled_max=float(row.get("pooled_max", values[-1])),
            pooled_n=int(row.get("pooled_n", values.size)),
            duplicate_knots=0,
            modal_value=float(row.get("modal_value", float("nan"))),
            modal_mass=float(row.get("modal_mass_fraction", float("nan"))),
            knot_counts=(
                group["tie_count"].to_numpy(dtype=np.int64)
                if "tie_count" in group.columns
                else None
            ),
        )

    return FrozenRulerSet(
        ruler_id=str(meta.get("ruler_id", ruler_dir.name)),
        composite_slug=str(meta.get("composite_slug", "")),
        bundle_domain=str(meta.get("bundle_domain", "")),
        level_fitted=str(meta.get("level_fitted", "district")),
        rulers=rulers,
        slices=tuple((str(s), str(p)) for s, p in meta.get("slices", ())),
        weights={str(k): float(v) for k, v in dict(meta.get("weights", {})).items()},
        configured_weight=float(meta.get("configured_weight", float("nan"))),
        coverage_gate=float(meta.get("coverage_gate", float("nan"))),
        ruler_sha256=str(meta.get("ruler_sha256", "")),
        data_snapshot_hash=str(meta.get("data_snapshot_hash", "")),
        fitted_utc=str(meta.get("fitted_utc", "")),
        meta=meta,
    )


# ---------------------------------------------------------------------------
# Artifact location
# ---------------------------------------------------------------------------


def frozen_ruler_root() -> Path:
    """Repository directory holding committed frozen ruler artifacts.

    Deliberately inside the package, not under ``IRT_DATA_DIR``: the ruler is the
    scientific contract, and a pipeline regen must not be able to replace it.
    """
    return Path(__file__).resolve().parents[1] / "config" / "frozen_rulers"


def frozen_ruler_dir(composite_slug: str, version: str) -> Path:
    """Directory for one composite's frozen ruler at one version."""
    return frozen_ruler_root() / str(composite_slug) / str(version)

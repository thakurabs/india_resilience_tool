"""Frozen annual holdout decision for the CarbonPlan shade replacement.

Inputs are matched 2000–2010 annual unit metrics. The released full-fit QDM
historical series must never be supplied as ``holdout`` here.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

METRICS = ("mean_c", "days_ge_28", "days_ge_30", "days_ge_32")
YEARS = tuple(range(2000, 2011))
N_DRAWS = 1000
SEED = 20261010


@dataclass(frozen=True)
class ScoreResult:
    """Per-unit comparisons and state/national acceptance cuts."""

    units: pd.DataFrame
    cuts: pd.DataFrame


def bootstrap_weights(n_draws: int = N_DRAWS, seed: int = SEED) -> tuple[np.ndarray, np.ndarray]:
    """Draw whole model years once, independently of reference years."""
    rng = np.random.default_rng(seed)
    method_draws = rng.integers(0, len(YEARS), size=(n_draws, len(YEARS)))
    reference_draws = rng.integers(0, len(YEARS), size=(n_draws, len(YEARS)))
    method_weights = np.eye(len(YEARS))[method_draws].mean(axis=1)
    reference_weights = np.eye(len(YEARS))[reference_draws].mean(axis=1)
    return method_weights, reference_weights


def _metric_band(metric: str) -> float:
    return .05 if metric == "mean_c" else .5


def _support_class(metric: str, reference: np.ndarray) -> np.ndarray:
    if metric == "mean_c":
        return np.full(reference.shape[1], "not_applicable", dtype=object)
    annual = np.nansum(reference, axis=0)
    return np.where(annual == 0, "zero", np.where(annual < len(YEARS), "sparse", "supported"))


def score_model(model: str, unit_table: pd.DataFrame,
                holdout: dict[str, np.ndarray], deployed: dict[str, np.ndarray],
                reference: dict[str, np.ndarray],
                draws: tuple[np.ndarray, np.ndarray] | None = None) -> ScoreResult:
    """Score common units with 11 complete years and return blocking cuts.

    Every metric array is years × units in the exact ``YEARS`` and unit-table
    order. NaN anywhere in the three methods removes that unit for the metric;
    this attrition is recorded, never imputed. The same method-year bootstrap
    weights apply to incumbent and holdout, and reference weights are independent.
    """
    if not {"unit_index", "state_name"}.issubset(unit_table.columns):
        raise ValueError("Unit table needs unit_index and state_name")
    if unit_table.unit_index.duplicated().any():
        raise ValueError("Duplicate unit_index")
    weights, ref_weights = draws or bootstrap_weights()
    n_units = len(unit_table)
    unit_rows: list[pd.DataFrame] = []
    cuts: list[dict] = []
    state_values = unit_table.state_name.astype(str).to_numpy()
    for metric in METRICS:
        h, d, r = (np.asarray(source[metric], dtype=float)
                   for source in (holdout, deployed, reference))
        if any(a.shape != (len(YEARS), n_units) for a in (h, d, r)):
            raise ValueError(f"{metric}: expected 11 years x {n_units} units")
        common = np.isfinite(h).all(axis=0) & np.isfinite(d).all(axis=0) & np.isfinite(r).all(axis=0)
        hmean, dmean, rmean = h.mean(axis=0), d.mean(axis=0), r.mean(axis=0)
        herror, derror = hmean - rmean, dmean - rmean
        delta = np.abs(herror) - np.abs(derror)
        band = _metric_band(metric)
        label = np.where(delta > band, "worsened", np.where(delta < -band, "improved", "unchanged"))
        label = np.where(common, label, "missing")
        support = _support_class(metric, r)
        unit_rows.append(pd.DataFrame({
            "model": model, "metric": metric, "unit_index": unit_table.unit_index.to_numpy(),
            "state_name": state_values, "reference_support": np.where(common, support, "missing"),
            "holdout_mean": np.where(common, hmean, np.nan),
            "deployed_mean": np.where(common, dmean, np.nan),
            "reference_mean": np.where(common, rmean, np.nan),
            "error_holdout": np.where(common, herror, np.nan),
            "error_deployed": np.where(common, derror, np.nan),
            "delta_abs_error": np.where(common, delta, np.nan),
            "classification": label,
        }))
        boot_h = weights @ np.where(np.isfinite(h), h, 0.)
        boot_d = weights @ np.where(np.isfinite(d), d, 0.)
        boot_r = ref_weights @ np.where(np.isfinite(r), r, 0.)
        boot_abs_h = np.abs(boot_h - boot_r)
        boot_abs_d = np.abs(boot_d - boot_r)
        for state in ("national", *sorted(set(state_values))):
            selected = common & ((state_values == state) if state != "national" else True)
            n = int(selected.sum())
            n_worse = int((label[selected] == "worsened").sum())
            n_improved = int((label[selected] == "improved").sum())
            n_unchanged = int((label[selected] == "unchanged").sum())
            if n:
                samples = np.median(boot_abs_h[:, selected], axis=1) - np.median(
                    boot_abs_d[:, selected], axis=1)
                lo, hi = np.quantile(samples, [.05, .95])
                median_change = float(np.median(np.abs(herror[selected])) -
                                      np.median(np.abs(derror[selected])))
                share_worse = n_worse / n
                verdict = "worsened" if share_worse >= 2 / 3 and lo > 0 else "not_worsened"
            else:
                lo = hi = median_change = share_worse = np.nan
                verdict = "insufficient_support"
            cuts.append({"model": model, "state_name": state, "metric": metric,
                         "units_total": int((state_values == state).sum()) if state != "national" else n_units,
                         "units_classified": n, "units_missing": (int((state_values == state).sum())
                                                                if state != "national" else n_units) - n,
                         "worsened_units": n_worse, "improved_units": n_improved,
                         "unchanged_units": n_unchanged, "share_worsened": share_worse,
                         "reference_zero_units": int((support[selected] == "zero").sum()),
                         "reference_sparse_units": int((support[selected] == "sparse").sum()),
                         "median_abs_error_change": median_change,
                         "bootstrap_p05": float(lo), "bootstrap_p95": float(hi),
                         "verdict": verdict, "draws": len(weights), "seed": SEED})
    return ScoreResult(pd.concat(unit_rows, ignore_index=True), pd.DataFrame(cuts))

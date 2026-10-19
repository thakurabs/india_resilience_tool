"""Pure validation logic for the CarbonPlan shade-WBGT held-out validation (CHG-0646).

Contract: ``docs/diagnostics/wbgt_shade_carbonplan_validation/SPEC.md``. This module
holds only the decisions the SPEC freezes (train/test separation, application
windows, the IRT reporting calendar, inclusive counts, scoring, change
classification, threshold-flip attribution and count-first spatial transfer), so
they can be tested without xclim, NetCDF or the network. The QDM itself is
CarbonPlan's notebook-06 code, passed in as callables by the runner scripts.

Diagnostic only: nothing here is imported by production code.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

import numpy as np
import pandas as pd

THRESHOLDS: tuple[int, ...] = (28, 30, 32)
TAIL_QUANTILES: tuple[float, ...] = (0.90, 0.95, 0.99)
TEMPERATURE_BAND_C = 0.05
COUNT_BAND_DAYS = 0.5
SEASONS: dict[str, tuple[int, ...]] = {"JF": (1, 2), "MAM": (3, 4, 5), "JJAS": (6, 7, 8, 9),
                                       "OND": (10, 11, 12)}
INPUT_VERSION_BAND_C = 1e-3
TIE_RAW_C = 1e-4
TIE_DOY = 15
TIE_RESIDUAL_C = 1e-3


# --------------------------------------------------------------------------- windows

@dataclass(frozen=True)
class Window:
    """Inclusive calendar-year window."""

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError(f"Empty window {self.start}-{self.end}")

    @property
    def label(self) -> str:
        return f"{self.start}-{self.end}"

    def contains(self, times: pd.DatetimeIndex) -> np.ndarray:
        return (times.year >= self.start) & (times.year <= self.end)


def _times(series) -> pd.DatetimeIndex:
    """Datetime index of an xarray DataArray (``time`` dim) or a pandas object."""
    if hasattr(series, "indexes") and "time" in getattr(series, "indexes", {}):
        index = series.indexes["time"]
        if not isinstance(index, pd.DatetimeIndex):
            index = pd.DatetimeIndex(index.to_datetimeindex())
        return index
    return pd.DatetimeIndex(series.index)


def select_window(series, window: Window):
    """Return exactly the dates of ``series`` inside ``window``; fail if any year is absent."""
    times = _times(series)
    keep = window.contains(times)
    years = set(times[keep].year)
    missing = sorted(set(range(window.start, window.end + 1)) - years)
    if missing:
        raise ValueError(f"Window {window.label} lacks years {missing}")
    if hasattr(series, "isel"):
        return series.isel(time=np.flatnonzero(keep))
    return series.iloc[np.flatnonzero(keep)]


def assert_within(series, window: Window, what: str) -> None:
    """Refuse a series that has any date outside ``window`` (train/test leakage guard)."""
    times = _times(series)
    if len(times) == 0:
        raise ValueError(f"{what}: empty series")
    if times.year.min() < window.start or times.year.max() > window.end:
        raise ValueError(f"{what}: dates {times.min().date()}..{times.max().date()} "
                         f"outside {window.label}")


def fit_and_apply(train_fn: Callable, apply_fn: Callable, reference, raw, train: Window,
                  windows: Sequence[Window]) -> dict[str, object]:
    """Train on ``train`` years only, then adjust each application window in its own call.

    ``train_fn(reference_train, raw_train)`` returns a trained model and
    ``apply_fn(model, raw_window)`` returns the adjusted window. Reference data
    outside ``train`` are never passed to either callable. A window that overlaps
    the training window is refused, so a held-out score cannot be in-sample by
    accident; in-sample context runs must call ``train_fn`` directly.
    """
    ref_train = select_window(reference, train)
    raw_train = select_window(raw, train)
    assert_within(ref_train, train, "reference training data")
    assert_within(raw_train, train, "model training data")
    model = train_fn(ref_train, raw_train)
    out = {}
    for w in windows:
        if not (w.end < train.start or w.start > train.end):
            raise ValueError(f"Application window {w.label} overlaps training {train.label}")
        part = select_window(raw, w)
        assert_within(part, w, f"application window {w.label}")
        out[w.label] = apply_fn(model, part)
    return out


# --------------------------------------------------------------------------- calendar

def irt_calendar(daily: pd.DataFrame, window: Window) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply IRT's reporting calendar: drop Feb 29, keep ``window``, flag incomplete unit-years.

    Returns the trimmed frame (values unchanged, NaN preserved) and a boolean
    frame (years x units) that is True only where the unit-year has all 365
    days present and finite. Missing data never become zeros here.
    """
    if not isinstance(daily.index, pd.DatetimeIndex):
        raise TypeError("daily must have a DatetimeIndex")
    if daily.index.has_duplicates:
        raise ValueError("Duplicate timestamps")
    d = daily[~((daily.index.month == 2) & (daily.index.day == 29))]
    d = d[window.contains(d.index)]
    years = pd.Index(range(window.start, window.end + 1), name="year")
    finite = np.isfinite(d.to_numpy(dtype=float))
    by_year = pd.DataFrame(finite, index=d.index.year, columns=d.columns)
    finite_days = by_year.groupby(level=0).sum().reindex(years, fill_value=0)
    present = pd.Series(d.index.year).value_counts().reindex(years, fill_value=0)
    valid = (finite_days == 365) & (present.to_numpy()[:, None] == 365)
    return d, valid


def annual_counts(daily: pd.DataFrame, valid: pd.DataFrame,
                  thresholds: Iterable[float] = THRESHOLDS) -> dict[float, pd.DataFrame]:
    """Inclusive (>=) day counts per year and unit; NaN wherever the unit-year is invalid.

    Values are compared unrounded. An invalid unit-year is NaN, never zero.
    """
    out = {}
    years = valid.index
    for t in thresholds:
        hits = (daily >= t) & daily.notna()
        counts = hits.groupby(daily.index.year).sum().reindex(years).astype(float)
        out[t] = counts.where(valid)
    return out


def ineligible_units(valid: pd.DataFrame) -> list[dict]:
    """Every invalid unit-year, for the exclusion table."""
    rows = []
    for unit in valid.columns:
        for year in valid.index[~valid[unit].to_numpy()]:
            rows.append({"unit": unit, "year": int(year), "reason": "incomplete or non-finite unit-year"})
    return rows


# --------------------------------------------------------------------------- scoring

def support_class(reference_days: np.ndarray, n_years: int) -> np.ndarray:
    """'zero' (no day), 'sparse' (<1 d/yr) or 'supported' (>=1 d/yr) from total reference days."""
    total = np.asarray(reference_days, dtype=float)
    out = np.where(total >= n_years, "supported", np.where(total > 0, "sparse", "zero"))
    return np.where(np.isnan(total), "missing", out)


def primary_errors(x: np.ndarray, ref: np.ndarray, x_months: np.ndarray, ref_months: np.ndarray,
                   n_years: int) -> dict[str, np.ndarray]:
    """Signed per-unit errors of every primary metric (arrays are days x units, all finite)."""
    if x.ndim != 2 or ref.ndim != 2 or x.shape[1] != ref.shape[1]:
        raise ValueError("x and ref must be days x units with equal units")
    if not (np.isfinite(x).all() and np.isfinite(ref).all()):
        raise ValueError("primary_errors requires finite arrays; apply eligibility first")
    out: dict[str, np.ndarray] = {}
    monthly = []
    for m in range(1, 13):
        xm, rm = x[x_months == m], ref[ref_months == m]
        if not len(xm) or not len(rm):
            raise ValueError(f"Month {m} absent")
        monthly.append(xm.mean(0) - rm.mean(0))
    monthly = np.vstack(monthly)
    out["seasonal_mean_abs_monthly_bias"] = np.abs(monthly).mean(0)
    for q in TAIL_QUANTILES:
        out[f"p{round(q * 100)}_bias"] = np.quantile(x, q, axis=0) - np.quantile(ref, q, axis=0)
    for t in THRESHOLDS:
        out[f"days_ge_{t}_error"] = ((x >= t).sum(0) - (ref >= t).sum(0)) / n_years
    return out


def unit_scores(x: pd.DataFrame, ref: pd.DataFrame, n_years: int) -> pd.DataFrame:
    """Primary and secondary scores per unit for one candidate series against the reference.

    Both frames are IRT-calendar daily series (dates x units) restricted to
    eligible units. The seasonal primary is reported as a non-negative score, so
    its "error" is itself; all other errors are signed.
    """
    if list(x.columns) != list(ref.columns):
        raise ValueError("Unit columns differ")
    xv, rv = x.to_numpy(float), ref.to_numpy(float)
    xm, rm = x.index.month.to_numpy(), ref.index.month.to_numpy()
    prim = primary_errors(xv, rv, xm, rm, n_years)
    frame = pd.DataFrame(prim, index=x.columns)
    frame["annual_mean_bias"] = xv.mean(0) - rv.mean(0)
    for m in range(1, 13):
        frame[f"bias_m{m:02d}"] = xv[xm == m].mean(0) - rv[rm == m].mean(0)
    for name, months in SEASONS.items():
        xs, rs = xv[np.isin(xm, months)], rv[np.isin(rm, months)]
        for q in (0.5, 0.9):
            frame[f"{name}_p{round(q * 100)}_bias"] = np.quantile(xs, q, 0) - np.quantile(rs, q, 0)
    xy, ry = x.index.year.to_numpy(), ref.index.year.to_numpy()
    for t in THRESHOLDS:
        xc = pd.DataFrame(xv >= t).groupby(xy).sum().to_numpy(float)
        rc = pd.DataFrame(rv >= t).groupby(ry).sum().to_numpy(float)
        frame[f"days_ge_{t}"] = xc.mean(0)
        frame[f"ref_days_ge_{t}"] = rc.mean(0)
        frame[f"ref_total_days_ge_{t}"] = rc.sum(0)
        frame[f"support_{t}"] = support_class(rc.sum(0), n_years)
        frame[f"annual_count_sd_{t}"] = xc.std(0, ddof=1) if len(xc) > 1 else np.nan
        frame[f"ref_annual_count_sd_{t}"] = rc.std(0, ddof=1) if len(rc) > 1 else np.nan
        frame[f"annual_count_w1_{t}"] = (np.abs(np.sort(xc, 0) - np.sort(rc, 0)).mean(0)
                                         if xc.shape == rc.shape else np.nan)
    d = xv - rv
    frame["daily_rmse_diagnostic"] = np.sqrt((d * d).mean(0))
    with np.errstate(invalid="ignore", divide="ignore"):
        xa, ra = xv - xv.mean(0), rv - rv.mean(0)
        frame["daily_corr_diagnostic"] = (xa * ra).sum(0) / np.sqrt((xa * xa).sum(0) * (ra * ra).sum(0))
    frame["n_days"] = len(x)
    return frame


PRIMARY_METRICS: tuple[str, ...] = (("seasonal_mean_abs_monthly_bias",)
                                    + tuple(f"p{round(q * 100)}_bias" for q in TAIL_QUANTILES)
                                    + tuple(f"days_ge_{t}_error" for t in THRESHOLDS))


def metric_band(metric: str) -> float:
    """Indifference band for a metric: 0.5 d/yr for counts, 0.05 degC otherwise."""
    return COUNT_BAND_DAYS if metric.startswith("days_ge_") else TEMPERATURE_BAND_C


def classify_change(err_raw: np.ndarray, err_qdm: np.ndarray, band: float,
                    zero_zero: np.ndarray | None = None) -> np.ndarray:
    """'improved' / 'worsened' / 'unchanged' from |qdm| - |raw|; 'zero/zero' where flagged.

    NaN errors give 'missing' so absent data cannot be scored as unchanged.
    """
    delta = np.abs(np.asarray(err_qdm, float)) - np.abs(np.asarray(err_raw, float))
    out = np.where(delta < -band, "improved", np.where(delta > band, "worsened", "unchanged"))
    out = np.where(np.isnan(delta), "missing", out)
    if zero_zero is not None:
        out = np.where(np.asarray(zero_zero, bool), "zero/zero", out)
    return out


def zero_zero_mask(scores_raw: pd.DataFrame, scores_qdm: pd.DataFrame, threshold: int) -> np.ndarray:
    """Units where the reference, raw and QDM all have zero days at ``threshold``."""
    col = f"days_ge_{threshold}"
    return ((scores_raw[f"ref_total_days_ge_{threshold}"] == 0) & (scores_raw[col] == 0)
            & (scores_qdm[col] == 0)).to_numpy()


def screening_verdict(abs_raw: np.ndarray, abs_qdm: np.ndarray, labels: np.ndarray,
                      ci: tuple[float, float]) -> str:
    """SPEC section 9 screening: 'improves', 'worsens' or 'mixed/inconclusive'."""
    classified = np.isin(labels, ("improved", "worsened", "unchanged"))
    if not classified.any():
        return "mixed/inconclusive"
    keep = classified
    med_change = float(np.median(np.abs(abs_qdm[keep])) - np.median(np.abs(abs_raw[keep])))
    share_improved = float((labels[keep] == "improved").mean())
    share_worsened = float((labels[keep] == "worsened").mean())
    lo, hi = ci
    if med_change < 0 and share_improved >= 2 / 3 and hi < 0:
        return "improves"
    if med_change > 0 and share_worsened >= 2 / 3 and lo > 0:
        return "worsens"
    return "mixed/inconclusive"


def year_resample_indices(years: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """Day indices for one whole-year bootstrap draw (years drawn with replacement)."""
    unique = np.unique(years)
    drawn = rng.choice(unique, size=len(unique), replace=True)
    return np.concatenate([np.flatnonzero(years == y) for y in drawn])


def bootstrap_median_change(raw: np.ndarray, qdm: np.ndarray, ref: np.ndarray,
                            model_years: np.ndarray, ref_years: np.ndarray,
                            model_months: np.ndarray, ref_months: np.ndarray,
                            unit_masks: dict[str, np.ndarray], n_boot: int = 1000,
                            seed: int = 20261003, level: float = 0.90) -> dict[str, tuple[float, float]]:
    """Whole-year bootstrap of the change in across-unit median |error| (qdm - raw).

    Model-side years (shared by raw and qdm) and reference years are drawn
    independently, because model years are not paired with observed weather.
    One draw applies to all units, preserving spatial dependence. The fitted
    correction is held fixed, so the interval omits refitting uncertainty.
    ``unit_masks`` maps a metric -- or a ``(subset, metric)`` tuple, so several
    subsets share one set of draws -- to the units it is classified over.
    """
    rng = np.random.default_rng(seed)
    n_years = len(np.unique(model_years))
    draws = {k: [] for k in unit_masks}
    for _ in range(n_boot):
        mi = year_resample_indices(model_years, rng)
        ri = year_resample_indices(ref_years, rng)
        er = primary_errors(raw[mi], ref[ri], model_months[mi], ref_months[ri], n_years)
        eq = primary_errors(qdm[mi], ref[ri], model_months[mi], ref_months[ri], n_years)
        for k, mask in unit_masks.items():
            metric = k[-1] if isinstance(k, tuple) else k
            if mask.any():
                draws[k].append(np.median(np.abs(eq[metric][mask])) - np.median(np.abs(er[metric][mask])))
    alpha = (1 - level) / 2
    return {k: ((float(np.quantile(v, alpha)), float(np.quantile(v, 1 - alpha))) if v else (np.nan, np.nan))
            for k, v in draws.items()}


# --------------------------------------------------------------------------- fidelity

def _doy_distance(a: pd.Timestamp, b: pd.Timestamp) -> int:
    d = abs(a.dayofyear - b.dayofyear)
    return min(d, 365 - d)


def tie_pair_days(residual: pd.Series, raw: pd.Series, floor: float = INPUT_VERSION_BAND_C,
                  raw_tol: float = TIE_RAW_C, doy: int = TIE_DOY,
                  resid_tol: float = TIE_RESIDUAL_C) -> dict[pd.Timestamp, pd.Timestamp]:
    """Days whose residual is explained by a QDM tie pair (SPEC G2 rule b).

    A day with |residual| > ``floor`` is paired with another day of the same
    unit whose raw value differs by <= ``raw_tol``, lies within +-``doy``
    day-of-year, and whose residual is equal and opposite within ``resid_tol``.
    """
    large = residual.index[residual.abs() > floor]
    pairs: dict[pd.Timestamp, pd.Timestamp] = {}
    r = raw.reindex(residual.index)
    for day in large:
        close = r.index[(r - r[day]).abs() <= raw_tol]
        for other in close:
            if other == day or _doy_distance(day, other) > doy:
                continue
            if abs(residual[day] + residual[other]) <= resid_tol:
                pairs[day] = other
                break
    return pairs


def threshold_flips(mine: pd.Series, published: pd.Series, raw: pd.Series,
                    thresholds: Iterable[float] = THRESHOLDS,
                    band: float = INPUT_VERSION_BAND_C) -> pd.DataFrame:
    """Every day where reproduced and published values fall on opposite sides of a threshold.

    Counts stay inclusive and unrounded; ``band`` is used only to attribute a
    flip to input-version noise ('threshold_proximity'). Tie-pair days are
    attributed 'tie_pair'; anything else is 'unexplained'.
    """
    both = pd.concat({"mine": mine, "published": published}, axis=1).dropna()
    residual = both.mine - both.published
    ties = tie_pair_days(residual, raw)
    rows = []
    for t in thresholds:
        flip = (both.mine >= t) != (both.published >= t)
        for day in both.index[flip.to_numpy()]:
            m, p = float(both.mine[day]), float(both.published[day])
            if abs(m - t) <= band and abs(p - t) <= band:
                why = "threshold_proximity"
            elif day in ties:
                why = "tie_pair"
            else:
                why = "unexplained"
            rows.append({"date": day, "threshold": t, "reproduced": m, "published": p,
                         "reproduced_minus_threshold": m - t, "published_minus_threshold": p - t,
                         "direction": int(m >= t) - int(p >= t), "explanation": why,
                         "tie_partner": ties.get(day)})
    return pd.DataFrame(rows, columns=["date", "threshold", "reproduced", "published",
                                       "reproduced_minus_threshold", "published_minus_threshold",
                                       "direction", "explanation", "tie_partner"])


# --------------------------------------------------------------------------- transfer

OVERLAP_TOLERANCE = 1e-6       # overlap / admin area treated as vertex noise (CHG-0652)
OVERLAP_EXCLUSION = "unresolved_source_polygon_overlap"


def union_intersection_area(admin_geom, source_geoms: Sequence) -> tuple[float, float]:
    """(sum of intersections, area of their union) of one admin polygon with source polygons.

    Geometries must share an equal-area CRS. The difference between the two is
    the admin area claimed by more than one source polygon. Shapely is imported
    here so the rest of this module stays importable without it.
    """
    from shapely.ops import unary_union

    parts = [p for p in (admin_geom.intersection(g) for g in source_geoms) if p.area > 0]
    if not parts:
        return 0.0, 0.0
    return float(sum(p.area for p in parts)), float(unary_union(parts).area)


def crosswalk_weights(crosswalk: pd.DataFrame, keys: Sequence[str]) -> pd.DataFrame:
    """Normalise intersection areas; coverage is the UNION of intersections over the admin area.

    ``crosswalk`` has one row per admin unit x source unit with ``processing_id``
    (-1 for none), ``intersection_m2``, ``unit_area_m2`` and ``union_m2`` (the
    area of the union of that admin unit's intersections, repeated per row; see
    ``union_intersection_area``). Summing intersections double-counts land that
    overlapping source polygons both claim (CHG-0652), so ``union_m2`` is
    required. ``overlap_m2``/``overlap_fraction`` report that double-claimed
    area; weights are NOT adjusted for it -- ``overlap_status`` excludes such
    admin units instead. Zero-coverage units keep a row with NaN weight and
    reason 'no coverage'.
    """
    if "union_m2" not in crosswalk:
        raise ValueError("crosswalk needs union_m2: summed intersections over-count overlapping sources")
    cw = crosswalk.copy()
    if (cw.intersection_m2 < 0).any() or (cw.unit_area_m2 <= 0).any():
        raise ValueError("Negative intersection or non-positive admin area")
    total = cw.groupby(list(keys)).intersection_m2.transform("sum")
    union = cw.groupby(list(keys)).union_m2.transform("first")
    if (union > total * (1 + 1e-9) + 1e-6).any():
        raise ValueError("union_m2 exceeds the summed intersections")
    covered = cw[cw.processing_id >= 0].groupby(list(keys)).intersection_m2.transform("sum")
    cw.loc[cw.processing_id >= 0, "w"] = cw.loc[cw.processing_id >= 0, "intersection_m2"] / covered
    cw["coverage"] = union / cw.unit_area_m2
    cw["overlap_m2"] = (total - union).clip(lower=0.0)
    cw["overlap_fraction"] = cw.overlap_m2 / cw.unit_area_m2
    cw["reason"] = np.where(total > 0, "", "no coverage")
    return cw


def overlap_status(weights: pd.DataFrame, keys: Sequence[str],
                   tolerance: float = OVERLAP_TOLERANCE) -> pd.DataFrame:
    """One row per admin unit: union coverage, overlap and whether it is excluded.

    An admin unit whose source polygons overlap by more than ``tolerance`` of its
    area is excluded: no ownership rule for the double-claimed land has been
    chosen, so its value would silently depend on one.
    """
    g = weights.groupby(list(keys))
    out = g[["coverage", "overlap_m2", "overlap_fraction", "unit_area_m2"]].first()
    out["source_units"] = g.processing_id.apply(lambda s: int((s >= 0).sum()))
    out["excluded"] = out.overlap_fraction > tolerance
    out["exclusion_reason"] = np.where(out.excluded, OVERLAP_EXCLUSION, "")
    return out.reset_index()


def count_first_transfer(unit_values: pd.DataFrame, weights: pd.DataFrame,
                         keys: Sequence[str], excluded: pd.DataFrame | None = None) -> pd.DataFrame:
    """Area-weight per-unit (already counted) values onto admin units, per slug and year.

    ``unit_values`` columns: processing_id, slug, year, value (NaN when the unit-year
    is ineligible). A source unit with NaN contributes nothing, and its area
    counts as uncovered: weights are renormalised over the contributing area,
    and ``series_coverage`` is reported against the original admin area.
    ``excluded`` (keys + ``exclusion_reason``, e.g. from ``overlap_status``) sets
    those admin units' values to NaN and carries the reason, so an excluded unit
    is visibly missing rather than absent or silently computed.
    """
    w = weights[weights.processing_id >= 0][list(keys) + ["processing_id", "intersection_m2",
                                                           "unit_area_m2"]]
    m = w.merge(unit_values, on="processing_id", how="left")
    m = m[m.slug.notna()]
    m["ok"] = m.value.notna()
    m["a"] = np.where(m.ok, m.intersection_m2, 0.0)
    m["av"] = np.where(m.ok, m.intersection_m2 * m.value.fillna(0.0), 0.0)
    g = m.groupby(list(keys) + ["slug", "year"], dropna=False)
    out = g[["a", "av"]].sum()
    out["unit_area_m2"] = g.unit_area_m2.first()
    out["value"] = np.where(out.a > 0, out.av / out.a.where(out.a > 0), np.nan)
    out["series_coverage"] = out.a / out.unit_area_m2
    out["contributing_units"] = g.ok.sum()
    out = out.drop(columns=["a", "av"]).reset_index()
    out["exclusion_reason"] = ""
    if excluded is not None and len(excluded):
        bad = excluded[excluded.exclusion_reason != ""][list(keys) + ["exclusion_reason"]]
        hit = out[list(keys)].merge(bad, on=list(keys), how="left").exclusion_reason
        mask = hit.notna().to_numpy()
        out.loc[mask, "value"] = np.nan
        out.loc[mask, "exclusion_reason"] = hit[mask].to_numpy()
    return out

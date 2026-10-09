"""Numerical contracts for the diagnostic Route B paired-aggregation pilot.

No production imports this module. QDM and physical functions remain the pinned
CarbonPlan implementation; these helpers define support and reporting semantics.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import sparse


def overlap_lengths(source_lo: np.ndarray, source_hi: np.ndarray,
                    target_lo: float, target_hi: float) -> np.ndarray:
    """Exact positive interval overlap in a common projected coordinate system."""
    lo, hi = np.asarray(source_lo, dtype=float), np.asarray(source_hi, dtype=float)
    if lo.shape != hi.shape or lo.ndim != 1 or not len(lo):
        raise ValueError("Require matching nonempty one-dimensional bounds")
    if not np.isfinite(lo).all() or not np.isfinite(hi).all() or np.any(hi <= lo):
        raise ValueError("Invalid source bounds")
    if not np.isfinite([target_lo, target_hi]).all() or target_hi <= target_lo:
        raise ValueError("Invalid target bounds")
    return np.maximum(0, np.minimum(hi, target_hi) - np.maximum(lo, target_lo))


def conservative_matrix(lat: np.ndarray, lon: np.ndarray, source_spacing: float,
                        cell_lat: np.ndarray, cell_lon: np.ndarray,
                        target_spacing: float = .25) -> sparse.csr_matrix:
    """Build native-reference to target-cell overlap fractions in EPSG:6933.

    Fractions use the full target-cell denominator, including missing/outside
    reference support. Coordinates are not rounded or snapped to nominal edges.
    """
    from pyproj import Transformer

    lat, lon, cell_lat, cell_lon = map(np.asarray, (lat, lon, cell_lat, cell_lon))
    if not len(lat) or not len(lon) or cell_lat.shape != cell_lon.shape or not len(cell_lat):
        raise ValueError("Empty/mismatched grid coordinates")
    if any(a.ndim != 1 or not np.isfinite(a).all() for a in (lat, lon, cell_lat, cell_lon)):
        raise ValueError("Require finite one-dimensional coordinates")
    if not np.isfinite([source_spacing, target_spacing]).all() or min(source_spacing, target_spacing) <= 0:
        raise ValueError("Invalid grid spacing")
    for axis in (lat, lon):
        if len(axis) > 1 and not (np.all(np.diff(axis) > 0) or np.all(np.diff(axis) < 0)):
            raise ValueError("Source coordinates must be strictly monotonic")
        if len(axis) > 1 and not np.allclose(np.abs(np.diff(axis)), source_spacing, rtol=0, atol=1e-9):
            raise ValueError("Source spacing does not match coordinates")
    project = Transformer.from_crs("EPSG:4326", "EPSG:6933", always_xy=True)
    xlo = np.asarray(project.transform((lon - source_spacing / 2).tolist(), [0.] * len(lon))[0])
    xhi = np.asarray(project.transform((lon + source_spacing / 2).tolist(), [0.] * len(lon))[0])
    ylo = np.asarray(project.transform([0.] * len(lat), (lat - source_spacing / 2).tolist())[1])
    yhi = np.asarray(project.transform([0.] * len(lat), (lat + source_spacing / 2).tolist())[1])
    rows, cols, data = [], [], []
    for i, (y, x) in enumerate(zip(cell_lat, cell_lon)):
        tx0, ty0 = project.transform(x - target_spacing / 2, y - target_spacing / 2)
        tx1, ty1 = project.transform(x + target_spacing / 2, y + target_spacing / 2)
        dx = overlap_lengths(xlo, xhi, tx0, tx1)
        dy = overlap_lengths(ylo, yhi, ty0, ty1)
        iy, ix = np.flatnonzero(dy), np.flatnonzero(dx)
        yy, xx = np.meshgrid(iy, ix, indexing="ij")
        fraction = dy[yy.ravel()] * dx[xx.ravel()] / ((tx1 - tx0) * (ty1 - ty0))
        rows.extend([i] * len(fraction))
        cols.extend((yy.ravel() * len(lon) + xx.ravel()).tolist())
        data.extend(fraction.tolist())
    matrix = sparse.csr_matrix((data, (rows, cols)), shape=(len(cell_lat), len(lat) * len(lon)))
    if np.any(np.asarray(matrix.sum(axis=1)).ravel() > 1 + 1e-8):
        raise ValueError("Overlapping reference support exceeds target-cell area")
    return matrix


def regrid_reference(values: np.ndarray, matrix: sparse.csr_matrix) -> tuple[np.ndarray, np.ndarray]:
    """Area-average finite native reference cells; return explicit valid fractions."""
    values = np.asarray(values, dtype=float)
    if values.size != matrix.shape[1] or values.size == 0:
        raise ValueError("Reference window/mapping size mismatch")
    finite = np.isfinite(values.ravel())
    support = np.asarray(matrix @ finite.astype(float)).ravel()
    sums = np.asarray(matrix @ np.where(finite, values.ravel(), 0.)).ravel()
    result = np.divide(sums, support, out=np.full_like(sums, np.nan), where=support > 0)
    return result, support


def fixed_aggregate(daily: np.ndarray, weights: np.ndarray) -> np.ndarray:
    """Apply fixed normalized weights; any missing contributing cell yields NaN.

    Zero-weight missing cells do not poison unrelated units. No daily
    renormalization is allowed, so weather-dependent support cannot change weights.
    """
    daily, weights = np.asarray(daily, dtype=float), np.asarray(weights, dtype=float)
    if daily.ndim != 2 or weights.ndim != 2 or daily.shape[1] != weights.shape[1]:
        raise ValueError("Expected time x cells and units x cells arrays")
    if not daily.shape[0] or not daily.shape[1] or not len(weights):
        raise ValueError("Empty aggregation")
    if not np.isfinite(weights).all() or np.any(weights < 0):
        raise ValueError("Invalid weights")
    totals = weights.sum(axis=1)
    if not np.allclose(totals, 1., atol=1e-10, rtol=0):
        raise ValueError("Each unit must have fixed weights summing to one")
    finite = np.isfinite(daily)
    result = np.where(finite, daily, 0.) @ weights.T
    missing = (~finite).astype(float) @ (weights > 0).astype(float).T
    result[missing > 0] = np.nan
    return result


def annual_metrics(dates, daily: np.ndarray) -> dict[str, np.ndarray]:
    """Reduce complete 365-day cell/unit years, excluding Feb 29 explicitly.

    Missing dates, duplicated dates or incomplete values never become zero
    exceedances. Input units are degrees Celsius and threshold comparisons >=.
    """
    dates = pd.DatetimeIndex(dates).normalize()
    daily = np.asarray(daily, dtype=float)
    if daily.ndim == 1:
        daily = daily[:, None]
    if daily.ndim != 2 or len(dates) != len(daily) or not len(dates):
        raise ValueError("Dates/daily shape mismatch or empty series")
    if dates.has_duplicates or not dates.is_monotonic_increasing:
        raise ValueError("Dates must be unique and ordered")
    keep = ~((dates.month == 2) & (dates.day == 29))
    dates, daily = dates[keep], daily[keep]
    years = np.unique(dates.year)
    result = {"years": years}
    for name in ("mean_c", "max_c", "days_ge_28", "days_ge_30", "days_ge_32"):
        result[name] = np.full((len(years), daily.shape[1]), np.nan)
    for i, year in enumerate(years):
        expected = pd.date_range(f"{year}-01-01", f"{year}-12-31")
        expected = expected[~((expected.month == 2) & (expected.day == 29))]
        subset = dates.year == year
        if not dates[subset].equals(expected):
            continue
        values = daily[subset]
        good = np.isfinite(values).all(axis=0)
        result["mean_c"][i, good] = values[:, good].mean(axis=0)
        result["max_c"][i, good] = values[:, good].max(axis=0)
        for threshold in (28, 30, 32):
            result[f"days_ge_{threshold}"][i, good] = (values[:, good] >= threshold).sum(axis=0)
    return result


def grid_first_outputs(dates, cell_sun: np.ndarray,
                       weights: np.ndarray) -> dict[str, dict[str, np.ndarray]]:
    """Grid-first estimands, which need corrected cell series and area weights alone.

    Split out of ``paired_outputs`` so a grid-first-only run never has to fit, hold in
    memory or aggregate the regional-series path it does not report.
    """
    cells = annual_metrics(dates, cell_sun)
    grid_daily = annual_metrics(dates, fixed_aggregate(cell_sun, weights))
    cell_first = {"years": cells["years"]}
    for metric, values in cells.items():
        if metric != "years":
            cell_first[metric] = fixed_aggregate(values, weights)
    return {"grid_first_series": grid_daily, "grid_first_cell_metrics": cell_first}


def paired_outputs(dates, cell_sun: np.ndarray, region_sun: np.ndarray,
                   weights: np.ndarray) -> dict[str, dict[str, np.ndarray]]:
    """Keep regional-series and cell-count-first estimands explicitly separate."""
    return {"region_first_series": annual_metrics(dates, region_sun),
            **grid_first_outputs(dates, cell_sun, weights)}

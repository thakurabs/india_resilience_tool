"""
Area-weighted state aggregation helpers (Streamlit-free).

The dashboard's state headline KPI is an area-weighted mean over the selected
state's units:

    value = Σ(unit_value · unit_area) / Σ(unit_area)

dropping units with a NaN value, a NaN area, or a non-positive area. These
helpers are the single definition of that computation so the live view
(``app/views/state_summary_view.py``) and the offline precompute tool
(``tools/optimized/build_state_values.py``) cannot drift apart.

The ``pyproj`` import is kept lazy (see CLAUDE.md §13) so importing this module
never pulls the geo native stack unless a geodesic fallback is actually needed.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

import pandas as pd

__all__ = ["with_area_weights", "weighted_state_mean"]

# Canonical column the helpers attach/consume for the per-unit weight.
AREA_WEIGHT_COL = "__area_m2"


def with_area_weights(gdf: Any) -> pd.DataFrame:
    """
    Return a copy of ``gdf`` with per-unit area weights in ``__area_m2``.

    Resolution order:
      1. An existing ``__area_m2`` column (already-prepared frame).
      2. An ``area_m2`` column (carried by the optimized geometry bundle).
      3. Geodesic area computed via ``pyproj.Geod(WGS84)`` (lazy import).

    Non-finite or missing areas are coerced to ``0.0`` so they are dropped by
    :func:`weighted_state_mean`.
    """
    if gdf is None:
        return pd.DataFrame()

    out = gdf.copy()
    if AREA_WEIGHT_COL in out.columns:
        out[AREA_WEIGHT_COL] = pd.to_numeric(out[AREA_WEIGHT_COL], errors="coerce").fillna(0.0)
        return out
    if "area_m2" in out.columns:
        out[AREA_WEIGHT_COL] = pd.to_numeric(out["area_m2"], errors="coerce").fillna(0.0)
        return out

    from pyproj import Geod

    geod = Geod(ellps="WGS84")
    areas: list[float] = []
    geometries = out.geometry if "geometry" in out.columns else []
    for geom in geometries:
        if geom is None or getattr(geom, "is_empty", False):
            areas.append(0.0)
            continue
        try:
            a, _ = geod.geometry_area_perimeter(geom)
            areas.append(abs(float(a)))
        except Exception:
            areas.append(0.0)
    out[AREA_WEIGHT_COL] = areas if areas else 0.0
    return out


def weighted_state_mean(
    df: pd.DataFrame,
    value_col: Optional[str],
) -> Tuple[Optional[float], int]:
    """
    Area-weighted mean of ``value_col`` and the count of contributing units.

    The value and the unit count are derived from a **single** mask
    (finite value AND finite area > 0) so they can never disagree.

    Args:
        df: Frame already carrying the ``__area_m2`` weight column (e.g. from
            :func:`with_area_weights`).
        value_col: Column to aggregate. ``None``/absent yields ``(None, 0)``.

    Returns:
        ``(value, n_units)`` where ``value`` is ``None`` when no unit
        contributes, and ``n_units`` is the number of contributing units.
    """
    if df is None or getattr(df, "empty", True) or not value_col or value_col not in df.columns:
        return None, 0
    if AREA_WEIGHT_COL not in df.columns:
        return None, 0

    t = df[[value_col, AREA_WEIGHT_COL]].copy()
    t[value_col] = pd.to_numeric(t[value_col], errors="coerce")
    t[AREA_WEIGHT_COL] = pd.to_numeric(t[AREA_WEIGHT_COL], errors="coerce")
    t = t.dropna(subset=[value_col, AREA_WEIGHT_COL])
    t = t[t[AREA_WEIGHT_COL] > 0]

    n_units = int(t.shape[0])
    if n_units == 0:
        return None, 0

    weight_sum = float(t[AREA_WEIGHT_COL].sum())
    if weight_sum <= 0:
        return None, 0

    value = float((t[value_col] * t[AREA_WEIGHT_COL]).sum() / weight_sum)
    return value, n_units

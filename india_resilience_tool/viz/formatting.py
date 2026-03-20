"""
Formatting helpers for India Resilience Tool (IRT) visualizations.

This module centralizes number/label formatting used across:
- dashboard charts (Plotly / Matplotlib)
- Folium tooltips / legends
- PDF exports

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from typing import Optional, Tuple

import math

import numpy as np


def _is_na(x: object) -> bool:
    try:
        if x is None:
            return True
        if isinstance(x, float) and math.isnan(x):
            return True
        return bool(np.isnan(x))  # type: ignore[arg-type]
    except Exception:
        return False


def infer_decimals(
    *,
    units: Optional[str] = None,
    value_range: Optional[Tuple[float, float]] = None,
    default: int = 2,
) -> int:
    """Infer a reasonable number of decimals based on units and spread."""

    u = (units or "").strip()
    if u == "%":
        return 0

    if value_range is not None:
        try:
            vmin, vmax = float(value_range[0]), float(value_range[1])
            spread = abs(vmax - vmin)
            if spread >= 100:
                return 0
            if spread >= 10:
                return 1
            if spread >= 1:
                return 2
            return 3
        except Exception:
            return default

    return default


def format_number(
    x: object,
    *,
    decimals: int = 2,
    thousand_sep: bool = True,
    na: str = "—",
) -> str:
    """Format a number safely (handles NaN/None)."""

    if _is_na(x):
        return na

    try:
        xf = float(x)  # type: ignore[arg-type]
    except Exception:
        return na

    if abs(xf - round(xf)) < 1e-10 and decimals <= 0:
        return f"{int(round(xf)):,}" if thousand_sep else str(int(round(xf)))

    fmt = f"{{:,.{int(decimals)}f}}" if thousand_sep else f"{{:.{int(decimals)}f}}"
    return fmt.format(xf)


def format_value(
    x: object,
    *,
    units: Optional[str] = None,
    decimals: Optional[int] = None,
    thousand_sep: bool = True,
    na: str = "—",
) -> str:
    """Format a value with optional units."""

    u = (units or "").strip()
    d = int(decimals) if decimals is not None else infer_decimals(units=u)
    s = format_number(x, decimals=d, thousand_sep=thousand_sep, na=na)

    if u and s != na:
        if u == "%":
            return f"{s}%"
        return f"{s} {u}".rstrip()
    return s


def format_delta(
    delta: object,
    *,
    units: Optional[str] = None,
    decimals: Optional[int] = None,
    thousand_sep: bool = True,
    na: str = "—",
    show_sign: bool = True,
) -> str:
    """Format a delta value with sign and optional units."""

    if _is_na(delta):
        return na

    try:
        xf = float(delta)  # type: ignore[arg-type]
    except Exception:
        return na

    u = (units or "").strip()
    d = int(decimals) if decimals is not None else infer_decimals(units=u)

    sign = "+" if (show_sign and xf >= 0) else ""
    fmt = f"{{:{',' if thousand_sep else ''}.{int(d)}f}}"
    s = f"{sign}{fmt.format(xf)}"

    if u:
        if u == "%":
            return f"{s}%"
        return f"{s} {u}".rstrip()
    return s


def format_percent(
    pct: object,
    *,
    decimals: int = 0,
    na: str = "—",
    show_sign: bool = False,
) -> str:
    """Format a percent value (expects input already in percent units)."""

    if _is_na(pct):
        return na
    try:
        xf = float(pct)  # type: ignore[arg-type]
    except Exception:
        return na
    sign = "+" if (show_sign and xf >= 0) else ""
    return f"{sign}{xf:.{int(decimals)}f}%"

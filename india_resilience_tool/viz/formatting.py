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

from india_resilience_tool.config.variables import VARIABLES


_METRIC_DISPLAY_DECIMALS: dict[str, int] = {
    "population_total": 0,
}
_CLASS_DISPLAY_TOLERANCE = 1e-6


def get_metric_display_meta(
    *,
    metric_slug: Optional[str] = None,
    units: Optional[str] = None,
) -> tuple[str, float]:
    """Return human-facing display units and scaling for one metric."""
    slug = str(metric_slug or "").strip().lower()
    cfg = VARIABLES.get(slug, {}) if slug else {}

    raw_units = str(units or cfg.get("units") or cfg.get("unit") or "").strip()
    display_units = str(cfg.get("display_units") or "").strip() or raw_units
    try:
        display_scale = float(cfg.get("display_scale", 1.0) or 1.0)
    except Exception:
        display_scale = 1.0
    return display_units, display_scale


def _get_metric_cfg(metric_slug: Optional[str]) -> dict:
    slug = str(metric_slug or "").strip().lower()
    return VARIABLES.get(slug, {}) if slug else {}


def _get_metric_class_labels(metric_slug: Optional[str]) -> dict[int, str]:
    raw = _get_metric_cfg(metric_slug).get("class_labels") or {}
    labels: dict[int, str] = {}
    for key, value in dict(raw).items():
        try:
            labels[int(key)] = str(value)
        except Exception:
            continue
    return labels


def _metric_uses_label_with_score(metric_slug: Optional[str]) -> bool:
    mode = str(_get_metric_cfg(metric_slug).get("class_display_mode") or "").strip().lower()
    return mode == "label_with_score" and bool(_get_metric_class_labels(metric_slug))


def _format_metric_class_value(
    x: object,
    *,
    metric_slug: Optional[str] = None,
    thousand_sep: bool = True,
    na: str = "—",
) -> str:
    if _is_na(x):
        return na
    try:
        xf = float(x)  # type: ignore[arg-type]
    except Exception:
        return na
    if not math.isfinite(xf):
        return na

    labels = _get_metric_class_labels(metric_slug)
    rounded = int(round(xf))
    if rounded in labels and abs(xf - rounded) <= _CLASS_DISPLAY_TOLERANCE:
        return f"{labels[rounded]} ({rounded})"
    return f"{format_number(xf, decimals=1, thousand_sep=thousand_sep, na=na)} / 5"


def get_metric_display_units(
    *,
    metric_slug: Optional[str] = None,
    units: Optional[str] = None,
) -> str:
    """Return the units string that should appear in the UI."""
    if _metric_uses_label_with_score(metric_slug):
        return ""
    display_units, _ = get_metric_display_meta(metric_slug=metric_slug, units=units)
    return display_units


def get_metric_display_value(
    x: object,
    *,
    metric_slug: Optional[str] = None,
    units: Optional[str] = None,
) -> object:
    """Return the UI display value after applying any metric-specific scaling."""
    if _is_na(x):
        return x
    try:
        xf = float(x)  # type: ignore[arg-type]
    except Exception:
        return x
    _, display_scale = get_metric_display_meta(metric_slug=metric_slug, units=units)
    return xf * display_scale


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


def infer_metric_decimals(
    *,
    metric_slug: Optional[str] = None,
    units: Optional[str] = None,
    value_range: Optional[Tuple[float, float]] = None,
    default: int = 2,
) -> int:
    """Infer decimals with optional metric-specific overrides."""
    slug = str(metric_slug or "").strip().lower()
    display_units, _ = get_metric_display_meta(metric_slug=metric_slug, units=units)
    if slug in _METRIC_DISPLAY_DECIMALS:
        return int(_METRIC_DISPLAY_DECIMALS[slug])
    return infer_decimals(units=display_units or units, value_range=value_range, default=default)


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


def format_metric_number(
    x: object,
    *,
    metric_slug: Optional[str] = None,
    units: Optional[str] = None,
    decimals: Optional[int] = None,
    thousand_sep: bool = True,
    na: str = "—",
) -> str:
    """Format a metric value as a number without appending units."""
    scaled_x = get_metric_display_value(x, metric_slug=metric_slug, units=units)
    if decimals is not None:
        d = int(decimals)
    else:
        d = infer_metric_decimals(
            metric_slug=metric_slug,
            units=units,
        )
        slug = str(metric_slug or "").strip().lower()
        if slug not in _METRIC_DISPLAY_DECIMALS:
            try:
                xf = float(scaled_x)  # type: ignore[arg-type]
            except Exception:
                xf = None
            if xf is not None and math.isfinite(xf) and abs(xf - round(xf)) < 1e-10:
                d = 0
    return format_number(scaled_x, decimals=d, thousand_sep=thousand_sep, na=na)


def format_metric_value(
    x: object,
    *,
    metric_slug: Optional[str] = None,
    units: Optional[str] = None,
    decimals: Optional[int] = None,
    thousand_sep: bool = True,
    na: str = "—",
) -> str:
    """Format a metric value with metric-aware decimals and optional units."""
    if _metric_uses_label_with_score(metric_slug):
        return _format_metric_class_value(
            x,
            metric_slug=metric_slug,
            thousand_sep=thousand_sep,
            na=na,
        )
    display_units, _ = get_metric_display_meta(metric_slug=metric_slug, units=units)
    scaled_x = get_metric_display_value(x, metric_slug=metric_slug, units=units)
    d = int(decimals) if decimals is not None else infer_metric_decimals(
        metric_slug=metric_slug,
        units=units,
    )
    return format_value(scaled_x, units=display_units, decimals=d, thousand_sep=thousand_sep, na=na)


def format_metric_compact(
    x: object,
    *,
    metric_slug: Optional[str] = None,
    units: Optional[str] = None,
    decimals: Optional[int] = None,
    thousand_sep: bool = True,
    na: str = "—",
) -> str:
    """Format a metric compactly, appending units only when needed for clarity."""
    if _metric_uses_label_with_score(metric_slug):
        return _format_metric_class_value(
            x,
            metric_slug=metric_slug,
            thousand_sep=thousand_sep,
            na=na,
        )
    display_units, _ = get_metric_display_meta(metric_slug=metric_slug, units=units)
    if display_units == "%":
        return format_metric_value(
            x,
            metric_slug=metric_slug,
            units=units,
            decimals=decimals,
            thousand_sep=thousand_sep,
            na=na,
        )
    return format_metric_number(
        x,
        metric_slug=metric_slug,
        units=units,
        decimals=decimals,
        thousand_sep=thousand_sep,
        na=na,
    )


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

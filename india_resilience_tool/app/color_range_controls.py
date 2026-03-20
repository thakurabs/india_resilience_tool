"""
Color range controls for the IRT map (app-layer).

This module keeps the dashboard runtime smaller by isolating the map color-range
default computation (robust p2–p98) and slider bounds handling.
"""

from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd

from india_resilience_tool.viz.colors import compute_robust_range


def compute_color_range_defaults(scale_vals: pd.Series) -> Tuple[float, float, float, float]:
    """
    Compute slider bounds and a robust default range for a numeric series.

    Contract (legacy):
    - Slider bounds use full min/max of visible data.
    - Default selection uses robust p2–p98 (falls back to min/max).
    - If all values are identical, pad bounds by 10% (floor 1.0).
    """
    vals = pd.to_numeric(scale_vals, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if vals.empty:
        return (0.0, 1.0, 0.0, 1.0)

    data_min, data_max = float(vals.min()), float(vals.max())
    if data_min == data_max:
        padding = max(abs(data_min) * 0.1, 1.0)
        data_min -= padding
        data_max += padding

    vmin_default, vmax_default = compute_robust_range(vals, low_pct=2.0, high_pct=98.0)
    if (not np.isfinite(vmin_default)) or (not np.isfinite(vmax_default)):
        vmin_default, vmax_default = data_min, data_max

    vmin_default = max(data_min, min(float(vmin_default), data_max))
    vmax_default = max(data_min, min(float(vmax_default), data_max))
    if vmin_default > vmax_default:
        vmin_default, vmax_default = vmax_default, vmin_default
    if vmin_default == vmax_default:
        vmin_default, vmax_default = data_min, data_max

    return (data_min, data_max, float(vmin_default), float(vmax_default))


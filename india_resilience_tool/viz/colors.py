"""
Color + legend utilities for IRT visualizations.

Extracted from the legacy Streamlit dashboard to keep:
- fillColor computation stable and fast
- legend HTML generation deterministic

Streamlit-free: the app layer can cache these calls as needed.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from functools import lru_cache

import numpy as np
import pandas as pd

import matplotlib as mpl
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt


@lru_cache(maxsize=16)
def get_cmap_hex_list(cmap_name: str, *, nsteps: int = 256) -> list[str]:
    """
    Cached colormap -> hex list (used for HTML gradients).

    Args:
        cmap_name: Matplotlib colormap name
        nsteps: Number of gradient steps

    Returns:
        List of hex colors, length nsteps
    """
    cmap = mpl.colormaps.get_cmap(cmap_name)
    if nsteps < 2:
        nsteps = 2
    return [mcolors.to_hex(cmap(i / (nsteps - 1))) for i in range(nsteps)]


def apply_fillcolor(
    merged_df: pd.DataFrame,
    metric_col: str,
    vmin: float,
    vmax: float,
    cmap_name: str = "Reds",
) -> pd.DataFrame:
    """
    Add a 'fillColor' column directly to an existing (Geo)DataFrame.

    Contract (must match legacy dashboard):
      - merged_df is modified in-place and returned
      - NaN/inf -> '#cccccc'
      - also writes '_metric_val' with numeric-coerced values

    Args:
        merged_df: DataFrame/GeoDataFrame to modify
        metric_col: column to color by
        vmin: minimum value for normalization (may be NaN/inf)
        vmax: maximum value for normalization (may be NaN/inf)
        cmap_name: Matplotlib colormap name

    Returns:
        merged_df (same object) with 'fillColor' and '_metric_val'
    """
    vals = pd.to_numeric(
        merged_df.get(metric_col, pd.Series(index=merged_df.index, dtype=float)),
        errors="coerce",
    )

    arr = vals.to_numpy(dtype=float)
    fill = np.full(arr.shape, "#cccccc", dtype=object)

    mask_valid = np.isfinite(arr)
    if np.any(mask_valid):
        vmin_eff = vmin
        vmax_eff = vmax

        if not np.isfinite(vmin_eff) or not np.isfinite(vmax_eff):
            vmin_eff = float(np.nanmin(arr[mask_valid]))
            vmax_eff = float(np.nanmax(arr[mask_valid]))

        if (not np.isfinite(vmin_eff)) or (not np.isfinite(vmax_eff)) or (vmin_eff == vmax_eff):
            t = np.full(arr.shape, 0.5, dtype=float)
        else:
            t = (arr - vmin_eff) / (vmax_eff - vmin_eff)

        t = np.clip(t, 0.0, 1.0)

        cmap = plt.get_cmap(cmap_name)
        rgba = cmap(t[mask_valid])
        hex_valid = np.array(
            [
                "#{:02x}{:02x}{:02x}".format(int(r * 255), int(g * 255), int(b * 255))
                for r, g, b, _ in rgba
            ],
            dtype=object,
        )
        fill[mask_valid] = hex_valid

    merged_df["fillColor"] = fill
    merged_df["_metric_val"] = vals
    return merged_df


def build_vertical_gradient_legend_html(
    *,
    pretty_metric_label: str,
    vmin: float,
    vmax: float,
    cmap_name: str,
    map_width: int = 780,
    map_height: int = 700,
    right_px: int = 95,
    bar_width_px: int = 28,
    label_font: str = "12px",
    bar_height_fraction: float = 0.92,
) -> str:
    """
    Build the fixed-position vertical gradient legend HTML used in the Folium map.

    Contract (must match legacy dashboard markup closely):
      - fixed position at right_px and vertical centered
      - shows vmax and vmin formatted to 1 decimal
      - vertical title (writing-mode + rotate)

    Returns:
        HTML string
    """
    _ = map_width  # kept for interface stability (map_width used by callers)
    bar_height_px = int(map_height * bar_height_fraction)

    legend_colors = get_cmap_hex_list(cmap_name)
    gradient_colors = ", ".join(legend_colors)

    return f"""
<div id="legend-fixed" style="position: fixed; right: {right_px}px; top: 50%; transform: translateY(-50%);
z-index: 9999; pointer-events: none; display: flex; align-items: center; gap: 10px; font-family: Arial, Helvetica, sans-serif;">
  <div style="position: relative; display: flex; align-items: center; height: {bar_height_px}px;">
    <div style="display: flex; flex-direction: column; justify-content: space-between; height: {bar_height_px}px; margin-right:8px; font-size:{label_font}; color:#000;">
      <div style="text-align: right;">{vmax:.1f}</div>
      <div style="text-align: right;">{vmin:.1f}</div>
    </div>
    <div id="legend-bar" style="height: {bar_height_px}px; width: {bar_width_px}px; border-radius: 6px;
         box-shadow: 0 2px 6px rgba(0,0,0,0.28); background: linear-gradient(to top, {gradient_colors}); display: block;"></div>
  </div>
  <div id="legend-title" style="writing-mode: vertical-rl; transform: rotate(180deg); font-size: {label_font}; white-space: nowrap; align-self: center; color: #000;">
    {pretty_metric_label}
  </div>
</div>
"""


def build_vertical_gradient_legend_block_html(
    *,
    pretty_metric_label: str,
    vmin: float,
    vmax: float,
    cmap_name: str,
    map_height: int = 700,
    bar_width_px: int = 22,
    label_font: str = "12px",
    bar_height_fraction: float = 0.92,
) -> str:
    """
    Build a *non-fixed* legend block HTML intended to be placed inside Streamlit layout.

    Why this exists:
        Folium "position: fixed" legends are viewport-anchored, so their horizontal placement
        appears to drift across devices/layouts. This block version is container-relative and
        therefore stable when rendered in a Streamlit column next to the map.

    Returns:
        HTML string (safe to render via st.markdown(..., unsafe_allow_html=True))
    """
    bar_height_px = int(map_height * bar_height_fraction)

    legend_colors = get_cmap_hex_list(cmap_name)
    gradient_colors = ", ".join(legend_colors)

    return f"""
<div style="display: flex; align-items: center; justify-content: flex-start; gap: 10px;
            font-family: Arial, Helvetica, sans-serif; padding-top: 8px;">
  <div style="position: relative; display: flex; align-items: center; height: {bar_height_px}px;">
    <div style="display: flex; flex-direction: column; justify-content: space-between; height: {bar_height_px}px;
                margin-right: 8px; font-size: {label_font}; color: #000;">
      <div style="text-align: right;">{vmax:.1f}</div>
      <div style="text-align: right;">{vmin:.1f}</div>
    </div>
    <div style="height: {bar_height_px}px; width: {bar_width_px}px; border-radius: 6px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.20);
                background: linear-gradient(to top, {gradient_colors}); display: block;">
    </div>
  </div>
  <div style="writing-mode: vertical-rl; transform: rotate(180deg);
              font-size: {label_font}; white-space: nowrap; align-self: center; color: #000;">
    {pretty_metric_label}
  </div>
</div>
"""
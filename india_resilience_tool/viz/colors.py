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
import hashlib

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


def compute_robust_range(
    values: pd.Series,
    *,
    low_pct: float = 2.0,
    high_pct: float = 98.0,
) -> tuple[float, float]:
    """
    Compute a robust (vmin, vmax) range using percentiles.

    Notes:
        - Drops NaN/inf values.
        - If the resulting range is degenerate, pads it slightly.

    Args:
        values: numeric-like series
        low_pct: lower percentile in [0, 100]
        high_pct: upper percentile in [0, 100]

    Returns:
        (vmin, vmax) as floats (may be NaN if values are empty)
    """
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return float("nan"), float("nan")

    try:
        vmin = float(np.nanpercentile(arr, low_pct))
        vmax = float(np.nanpercentile(arr, high_pct))
    except Exception:
        vmin = float(np.nanmin(arr))
        vmax = float(np.nanmax(arr))

    if not np.isfinite(vmin) or not np.isfinite(vmax):
        vmin = float(np.nanmin(arr))
        vmax = float(np.nanmax(arr))

    if vmin > vmax:
        vmin, vmax = vmax, vmin

    if vmin == vmax:
        padding = max(abs(vmin) * 0.1, 1.0)
        vmin -= padding
        vmax += padding

    return vmin, vmax


def format_legend_value(x: float, *, vmin: float, vmax: float) -> str:
    """
    Format legend numbers with adaptive precision based on the data range.

    Args:
        x: value to format
        vmin: scale minimum
        vmax: scale maximum

    Returns:
        Formatted string (or "—" if x is not finite)
    """
    try:
        xf = float(x)
    except Exception:
        return "—"

    if not np.isfinite(xf):
        return "—"

    span = float(abs(float(vmax) - float(vmin))) if np.isfinite(vmin) and np.isfinite(vmax) else 0.0

    if span >= 10.0:
        decimals = 1
    elif span >= 1.0:
        decimals = 1
    elif span >= 0.1:
        decimals = 2
    elif span >= 0.01:
        decimals = 3
    else:
        decimals = 4

    return f"{xf:.{decimals}f}"


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


def apply_fillcolor_binned(
    merged_df: pd.DataFrame,
    metric_col: str,
    vmin: float,
    vmax: float,
    *,
    cmap_name: str = "Reds",
    nlevels: int = 15,
) -> pd.DataFrame:
    """
    Discrete (binned) fillColor assignment for choropleth maps.

    Contract:
      - merged_df is modified in-place and returned
      - NaN/inf -> '#cccccc'
      - also writes '_metric_val' with numeric-coerced values

    Args:
        merged_df: DataFrame/GeoDataFrame to modify
        metric_col: column to color by
        vmin: minimum value for binning
        vmax: maximum value for binning
        cmap_name: Matplotlib colormap name
        nlevels: number of discrete bins/colors

    Returns:
        merged_df (same object) with 'fillColor' and '_metric_val'
    """
    if nlevels < 2:
        nlevels = 2

    vals = pd.to_numeric(
        merged_df.get(metric_col, pd.Series(index=merged_df.index, dtype=float)),
        errors="coerce",
    )

    arr = vals.to_numpy(dtype=float)
    fill = np.full(arr.shape, "#cccccc", dtype=object)

    merged_df["_metric_val"] = vals

    mask_valid = np.isfinite(arr)
    if not np.any(mask_valid):
        merged_df["fillColor"] = fill
        return merged_df

    vmin_eff = float(vmin) if np.isfinite(vmin) else float(np.nanmin(arr[mask_valid]))
    vmax_eff = float(vmax) if np.isfinite(vmax) else float(np.nanmax(arr[mask_valid]))

    if vmin_eff > vmax_eff:
        vmin_eff, vmax_eff = vmax_eff, vmin_eff

    if vmin_eff == vmax_eff:
        padding = max(abs(vmin_eff) * 0.1, 1.0)
        vmin_eff -= padding
        vmax_eff += padding

    edges = np.linspace(vmin_eff, vmax_eff, int(nlevels) + 1)
    idx = np.searchsorted(edges, arr[mask_valid], side="right") - 1
    idx = np.clip(idx, 0, int(nlevels) - 1).astype(int)

    colors = np.array(get_cmap_hex_list(cmap_name, nsteps=int(nlevels)), dtype=object)
    fill[mask_valid] = colors[idx]

    merged_df["fillColor"] = fill
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


def build_vertical_binned_legend_block_html(
    *,
    pretty_metric_label: str,
    vmin: float,
    vmax: float,
    cmap_name: str,
    nlevels: int = 15,
    map_height: int = 700,
    bar_width_px: int = 18,
    label_font: str = "12px",
    bar_height_fraction: float = 0.92,
) -> str:
    """
    Build a container-relative *binned* legend block HTML (stepped colorbar).

    Notes:
        Uses a hard-stop CSS linear-gradient so the colorbar looks discrete,
        while remaining lightweight to render in Streamlit.

    Returns:
        HTML string (safe to render via st.markdown(..., unsafe_allow_html=True))
    """
    if nlevels < 2:
        nlevels = 2

    bar_height_px = int(map_height * bar_height_fraction)

    legend_colors = get_cmap_hex_list(cmap_name, nsteps=int(nlevels))

    # Use stacked div segments instead of a CSS hard-stop gradient, which can be
    # brittle across renderers/sanitizers. This is deterministic and "truly" discrete.
    segments_html = "\n".join(
        [f'<div style="flex: 1; background: {c}; width: 100%;"></div>' for c in legend_colors]
    )

    vmax_str = format_legend_value(vmax, vmin=vmin, vmax=vmax)
    vmin_str = format_legend_value(vmin, vmin=vmin, vmax=vmax)

    zero_label_html = ""
    try:
        vmin_f = float(vmin)
        vmax_f = float(vmax)
        if np.isfinite(vmin_f) and np.isfinite(vmax_f) and (vmin_f < 0.0 < vmax_f) and (vmax_f != vmin_f):
            zero_pos = (0.0 - vmin_f) / (vmax_f - vmin_f)  # 0..1
            zero_pos = float(np.clip(zero_pos, 0.0, 1.0))
            zero_str = format_legend_value(0.0, vmin=vmin_f, vmax=vmax_f)
            zero_label_html = (
                f'<div style="position:absolute; right:0; bottom:{zero_pos * 100.0:.1f}%;'
                f' transform: translateY(50%); text-align:right; white-space: nowrap;">{zero_str}</div>'
            )
    except Exception:
        zero_label_html = ""

    return f"""
<div style="display: flex; align-items: center; justify-content: flex-start; gap: 8px;
            font-family: Arial, Helvetica, sans-serif; padding-top: 8px;
            min-width: 90px; max-width: 100%; box-sizing: border-box;">
  <div style="position: relative; display: flex; align-items: center; height: {bar_height_px}px;">
    <div style="position: relative; width: 34px; height: {bar_height_px}px; margin-right: 6px;
                font-size: {label_font}; color: #000;">
      <div style="position:absolute; top:0; right:0; text-align:right; white-space: nowrap;">{vmax_str}</div>
      {zero_label_html}
      <div style="position:absolute; bottom:0; right:0; text-align:right; white-space: nowrap;">{vmin_str}</div>
    </div>
    <div style="height: {bar_height_px}px; width: {bar_width_px}px; border-radius: 6px;
                border: 1px solid rgba(0,0,0,0.18);
                box-shadow: 0 2px 6px rgba(0,0,0,0.20);
                overflow: hidden; display: flex; flex-direction: column-reverse;">
      {segments_html}
    </div>
  </div>
  <div style="writing-mode: vertical-rl; transform: rotate(180deg);
              font-size: {label_font}; white-space: nowrap; align-self: center; color: #000;">
    {pretty_metric_label}
  </div>
</div>
"""


# -----------------------------------------------------------------------------
# Discrete palette helpers (for grouped bars / categorical legends)
# -----------------------------------------------------------------------------

DISCRETE_PALETTE_HEX: list[str] = [
    "#1f77b4",  # blue
    "#ff7f0e",  # orange
    "#2ca02c",  # green
    "#d62728",  # red
    "#9467bd",  # purple
    "#8c564b",  # brown
    "#e377c2",  # pink
    "#7f7f7f",  # gray
    "#bcbd22",  # olive
    "#17becf",  # cyan
]


def stable_color_for_key(key: str, *, palette: list[str] | None = None) -> str:
    """Return a deterministic color for a given string key.

    Notes:
        This is intentionally NOT cryptographic. It is only to keep chart colors
        stable across sessions and selection sizes.
    """
    pal = palette or DISCRETE_PALETTE_HEX
    if not pal:
        return "#777777"

    k = str(key or "").strip().lower().encode("utf-8")
    h = hashlib.md5(k).hexdigest()  # nosec - non-cryptographic use
    idx = int(h[:8], 16) % len(pal)
    return pal[idx]


def stable_color_map(
    keys: list[str] | tuple[str, ...], *, palette: list[str] | None = None
) -> dict[str, str]:
    """Return a deterministic mapping from keys to colors."""
    pal = palette or DISCRETE_PALETTE_HEX
    return {str(k): stable_color_for_key(str(k), palette=pal) for k in list(keys)}

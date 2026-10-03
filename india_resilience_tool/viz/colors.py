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
import html
import hashlib
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

import matplotlib as mpl
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt


# No-data fill for choropleth units: light neutral so missing units recede
# instead of reading as a mid-gray data class.
NO_DATA_FILL_HEX = "#E8E8E8"

FLOOD_SEVERITY_CLASS_COLORS: tuple[str, ...] = (
    "#00a651",
    "#8ccf4d",
    "#fff200",
    "#ff1a1a",
    "#b30000",
)

# Per-capita water-scarcity classes 1..4 (No Stress -> Absolute scarcity), green->red.
WATER_SCARCITY_CLASS_COLORS: tuple[str, ...] = (
    "#1a9850",
    "#fee08b",
    "#fc8d59",
    "#d73027",
)

# Water-scarcity deterioration steps 0..3 (No change -> Worsens by 3), neutral->red.
# 0-based: index 0 corresponds to class code 0 ("No change").
WATER_DETERIORATION_CLASS_COLORS: tuple[str, ...] = (
    "#e0e0e0",
    "#fdae61",
    "#f46d43",
    "#d73027",
)

# Registry of fixed per-metric ordinal palettes (keyed by lowercase slug). Metrics
# absent here fall back to a colormap-sampled categorical palette so any k-class
# ordinal renders coherently. Slugs are hard-coded to keep viz free of app imports.
_CLASS_SCALE_PALETTES: dict[str, tuple[str, ...]] = {
    "jrc_flood_depth_index_rp100": FLOOD_SEVERITY_CLASS_COLORS,
    "water_scarcity_percapita": WATER_SCARCITY_CLASS_COLORS,
    "water_scarcity_percapita_2050": WATER_SCARCITY_CLASS_COLORS,
    "water_scarcity_deterioration_2050": WATER_DETERIORATION_CLASS_COLORS,
}


def _sampled_categorical_palette(n_classes: int) -> tuple[str, ...]:
    """Sample ``n_classes`` hex colors from a sequential colormap (generic fallback)."""
    n = max(1, int(n_classes))
    cmap = mpl.colormaps.get_cmap("YlOrRd")
    if n == 1:
        return (mcolors.to_hex(cmap(0.5)),)
    return tuple(mcolors.to_hex(cmap(i / (n - 1))) for i in range(n))


def class_scale_palette(slug: str, n_classes: int) -> tuple[str, ...]:
    """Return an ``n_classes``-length ordinal palette (min class first) for a slug.

    Uses the registered fixed palette when the slug is known and long enough;
    otherwise samples a sequential colormap so an arbitrary k-class metric renders
    coherently and no lookup runs past the end of a fixed tuple.
    """
    key = str(slug or "").strip().lower()
    palette = _CLASS_SCALE_PALETTES.get(key)
    if palette is not None and int(n_classes) <= len(palette):
        return tuple(palette[: int(n_classes)])
    return _sampled_categorical_palette(n_classes)


# Diverging colormap families keep their full symmetric range when binned;
# sequential ramps are clipped at the top to avoid near-black high classes.
_DIVERGING_CMAP_BASES: frozenset[str] = frozenset(
    {"rdbu", "rdylbu", "rdylgn", "rdgy", "brbg", "puor", "prgn", "piyg", "spectral", "coolwarm", "bwr", "seismic"}
)
SEQUENTIAL_CMAP_TOP_FRAC: float = 0.9


def _is_diverging_cmap(cmap_name: str) -> bool:
    """Return whether a Matplotlib colormap name belongs to a diverging family."""
    base = str(cmap_name or "").strip().lower()
    if base.endswith("_r"):
        base = base[:-2]
    return base in _DIVERGING_CMAP_BASES


# ---------------------------------------------------------------------------
# IRT domain ramps: single-hue sequential ramps anchored on UN SDG brand
# colors, interpolated in OKLab so class lightness is monotone by construction
# (perceptual ordering) while the brand hue is hit exactly at its own
# lightness position. Referenced by "irt:<family>" pseudo colormap names.
# "irt:composite" is the one multi-hue exception, reserved for composite
# indices: matplotlib magma_r sampled away from its near-white/near-black
# ends (perceptually uniform, CVD-safe, warm-to-dark = worse).
# ---------------------------------------------------------------------------
IRT_RAMP_ANCHORS: dict[str, str] = {
    "irt:heat": "#C5192D",  # SDG 4 red — heat / health domains
    "irt:water": "#126A9F",  # SDG 16 blue — water / flood / groundwater
    "irt:agri": "#407F46",  # SDG 13 green — agriculture domains
    "irt:exposure": "#F89D2A",  # SDG 11 orange — exposure / socio-economic
    "irt:drought": "#BF8D2C",  # SDG 12 bronze — drought / aridity
    "irt:cold": "#13496B",  # SDG 17 navy — cold risk
}
IRT_COMPOSITE_CMAP = "irt:composite"

# Discrete choropleth classes (equal intervals over the active color range),
# shared by the map fill, the map legend, and the landing-page map so all three
# stay in step. 5 keeps classes decodable by eye; over a zero-anchored diverging
# domain it also puts a 20%-wide neutral band around zero. Fixed-class metrics
# are unaffected — they derive their palette from their own class labels.
DEFAULT_CHOROPLETH_NLEVELS = 5
_IRT_COMPOSITE_SPAN: tuple[float, float] = (0.04, 0.86)


def _srgb_to_oklab(hex_color: str) -> tuple[float, float, float]:
    """Convert a hex sRGB color to OKLab (Björn Ottosson's reference math)."""

    def lin(c: float) -> float:
        return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4

    r, g, b = (lin(int(hex_color.lstrip("#")[i : i + 2], 16) / 255.0) for i in (0, 2, 4))
    l = (0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b) ** (1 / 3)
    m = (0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b) ** (1 / 3)
    s = (0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b) ** (1 / 3)
    return (
        0.2104542553 * l + 0.7936177850 * m - 0.0040720468 * s,
        1.9779984951 * l - 2.4285922050 * m + 0.4505937099 * s,
        0.0259040371 * l + 0.7827717662 * m - 0.8086757660 * s,
    )


def _oklab_to_hex(L: float, a: float, b: float) -> str:
    """Convert an OKLab color to hex sRGB, clipping to gamut."""
    l = (L + 0.3963377774 * a + 0.2158037573 * b) ** 3
    m = (L - 0.1055613458 * a - 0.0638541728 * b) ** 3
    s = (L - 0.0894841775 * a - 1.2914855480 * b) ** 3
    r = 4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s
    g = -1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s
    bb = -0.0041960863 * l - 0.7034186147 * m + 1.7076147010 * s

    def srgb(c: float) -> int:
        c = max(0.0, min(1.0, c))
        return round(255 * (12.92 * c if c <= 0.0031308 else 1.055 * c ** (1 / 2.4) - 0.055))

    return "#{:02X}{:02X}{:02X}".format(srgb(r), srgb(g), srgb(bb))


def _sdg_anchored_ramp(anchor_hex: str, n: int) -> list[str]:
    """Light-tint -> anchor -> deepened-anchor ramp with fixed hue in OKLCh.

    The anchor sits at its natural lightness position so lightness steps are
    spread evenly across classes (light anchors like orange otherwise crowd
    the light end and adjacent classes become hard to tell apart).
    """
    import math

    aL, aa, ab = _srgb_to_oklab(anchor_hex)
    aC, ah = math.hypot(aa, ab), math.atan2(ab, aa)
    light_L, light_C = 0.955, min(0.035, aC * 0.25)
    dark_L, dark_C = max(0.28, aL * 0.72), aC * 0.82
    t_anchor = (light_L - aL) / (light_L - dark_L)
    t_anchor = min(0.95, max(0.05, t_anchor))

    def sample(t: float) -> str:
        if t <= t_anchor:
            u = t / t_anchor
            L, C = light_L + (aL - light_L) * u, light_C + (aC - light_C) * u
        else:
            u = (t - t_anchor) / (1 - t_anchor)
            L, C = aL + (dark_L - aL) * u, aC + (dark_C - aC) * u
        return _oklab_to_hex(L, C * math.cos(ah), C * math.sin(ah))

    if int(n) <= 1:
        return [sample(0.5)]

    out: list[str] = []
    for i in range(n):
        t = i / (n - 1)
        out.append(sample(t))
    return out


@lru_cache(maxsize=32)
def get_binned_cmap_hex_list(cmap_name: str, *, nlevels: int) -> list[str]:
    """
    Sample the discrete class colors used by binned choropleths AND their legends.

    Single source of truth so the map fill colors and the legend swatches are
    always identical. "irt:*" names resolve to the SDG-anchored domain ramps
    (or the magma_r-based composite ramp) defined above; their endpoints are
    already designed, so no clipping applies. For Matplotlib names, sequential
    ramps are sampled over [0, SEQUENTIAL_CMAP_TOP_FRAC] to drop the unreadable
    near-black top; diverging ramps keep the full range so the scale stays
    symmetric around the midpoint.

    Args:
        cmap_name: Matplotlib colormap name or an "irt:*" ramp name
        nlevels: number of discrete classes (floored at 2)

    Returns:
        List of hex colors, length nlevels
    """
    n = max(2, int(nlevels))
    name = str(cmap_name or "").strip()
    if name == IRT_COMPOSITE_CMAP:
        cmap = mpl.colormaps.get_cmap("magma_r")
        lo, hi = _IRT_COMPOSITE_SPAN
        return [mcolors.to_hex(cmap(lo + (hi - lo) * i / (n - 1))) for i in range(n)]
    anchor = IRT_RAMP_ANCHORS.get(name)
    if anchor is not None:
        return _sdg_anchored_ramp(anchor, n)
    cmap = mpl.colormaps.get_cmap(cmap_name)
    top = 1.0 if _is_diverging_cmap(cmap_name) else SEQUENTIAL_CMAP_TOP_FRAC
    return [mcolors.to_hex(cmap(top * i / (n - 1))) for i in range(n)]


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
    if nsteps < 2:
        nsteps = 2
    return get_binned_cmap_hex_list(cmap_name, nlevels=int(nsteps))


def compute_robust_range(
    values: pd.Series,
    *,
    low_pct: float = 2.0,
    high_pct: float = 98.0,
    min_pad: float = 1.0,
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
        min_pad: minimum padding for degenerate ranges

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
        padding = max(abs(vmin) * 0.1, float(min_pad))
        vmin -= padding
        vmax += padding

    return vmin, vmax


def symmetric_diverging_range(vmin: float, vmax: float) -> tuple[float, float]:
    """
    Return (-M, +M) so a diverging ramp's neutral midpoint sits exactly on zero.

    A diverging palette (e.g. RdBu_r) places its neutral colour at the midpoint of
    the domain. Over a data-driven asymmetric range such as (-2, +8) that midpoint
    lands at +3, so zero-change units render cool ("cooling") and real change near
    +3 renders neutral. Anchoring the domain symmetrically about zero makes the
    palette's sign boundary match the data's sign boundary.

    Args:
        vmin: lower bound of the data-driven range
        vmax: upper bound of the data-driven range

    Returns:
        (-M, +M) where M is the largest absolute finite bound. Falls back to
        (-1.0, 1.0) when neither bound is finite or both are zero.
    """
    finite_bounds = [float(b) for b in (vmin, vmax) if np.isfinite(b)]
    magnitude = max((abs(b) for b in finite_bounds), default=0.0)
    if magnitude == 0.0:
        magnitude = 1.0
    return -magnitude, magnitude


def format_legend_value(
    x: float,
    *,
    vmin: float,
    vmax: float,
    display_scale: float = 1.0,
) -> str:
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
        xf = float(x) * float(display_scale)
    except Exception:
        return "—"

    if not np.isfinite(xf):
        return "—"

    span = (
        float(abs(float(vmax) - float(vmin))) * float(display_scale)
        if np.isfinite(vmin) and np.isfinite(vmax)
        else 0.0
    )

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
      - NaN/inf -> NO_DATA_FILL_HEX
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
    fill = np.full(arr.shape, NO_DATA_FILL_HEX, dtype=object)

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

        if str(cmap_name or "").strip().startswith("irt:"):
            cmap = mcolors.LinearSegmentedColormap.from_list(
                str(cmap_name),
                get_cmap_hex_list(cmap_name, nsteps=256),
            )
        else:
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
      - NaN/inf -> NO_DATA_FILL_HEX
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
    fill = np.full(arr.shape, NO_DATA_FILL_HEX, dtype=object)

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

    colors = np.array(get_binned_cmap_hex_list(cmap_name, nlevels=int(nlevels)), dtype=object)
    fill[mask_valid] = colors[idx]

    merged_df["fillColor"] = fill
    return merged_df


def apply_fillcolor_classed(
    merged_df: pd.DataFrame,
    metric_col: str,
    *,
    value_to_color: Mapping[int, str],
    tolerance: float = 0.5,
) -> pd.DataFrame:
    """
    Add class-driven fill colors for ordinal metrics with fixed integer classes.

    Contract:
      - merged_df is modified in-place and returned
      - NaN/inf or non-class values -> NO_DATA_FILL_HEX
      - also writes '_metric_val' with numeric-coerced values
      - tolerance=0.5 rounds any float to nearest class (handles bottom-up
        district aggregates which are continuous in [1, 5])
    """
    vals = pd.to_numeric(
        merged_df.get(metric_col, pd.Series(index=merged_df.index, dtype=float)),
        errors="coerce",
    )
    arr = vals.to_numpy(dtype=float)
    fill = np.full(arr.shape, NO_DATA_FILL_HEX, dtype=object)

    merged_df["_metric_val"] = vals
    for idx, value in enumerate(arr):
        if not np.isfinite(value):
            continue
        rounded = int(round(float(value)))
        if abs(float(value) - rounded) <= tolerance and rounded in value_to_color:
            fill[idx] = str(value_to_color[rounded])

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


def build_vertical_binned_legend_block_html(
    *,
    legend_title: str = "",
    pretty_metric_label: str | None = None,
    vmin: float,
    vmax: float,
    cmap_name: str,
    display_scale: float = 1.0,
    nlevels: int = 15,
    nticks: int = 5,
    include_zero_tick: bool = True,
    map_height: int = 700,
    bar_width_px: int = 18,
    label_font: str = "12px",
    bar_height_fraction: float = 0.82,
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

    title_text = str(legend_title or pretty_metric_label or "").strip()
    outer_pad_top_px = 30
    outer_pad_bottom_px = 18
    available_height_px = max(int(map_height) - outer_pad_top_px - outer_pad_bottom_px, 160)
    bar_height_px = max(int(available_height_px * bar_height_fraction), 120)
    title_html = ""
    if title_text:
        title_html = (
            '<div style="writing-mode: vertical-rl; transform: rotate(180deg);'
            f" font-size: {label_font}; white-space: nowrap; align-self: center; color: #000;\">"
            f"{title_text}</div>"
        )

    legend_colors = get_binned_cmap_hex_list(cmap_name, nlevels=int(nlevels))

    # Use stacked div segments instead of a CSS hard-stop gradient, which can be
    # brittle across renderers/sanitizers. This is deterministic and "truly" discrete.
    segments_html = "\n".join(
        [f'<div style="flex: 1; background: {c}; width: 100%;"></div>' for c in legend_colors]
    )

    # Tick labels (min/max + intermediates)
    tick_labels_html = ""
    try:
        vmin_f = float(vmin)
        vmax_f = float(vmax)
        span = vmax_f - vmin_f

        if np.isfinite(vmin_f) and np.isfinite(vmax_f) and span != 0.0:
            n = int(nticks) if int(nticks) >= 2 else 2
            ticks = np.linspace(vmin_f, vmax_f, n).tolist()

            # If the scale crosses 0 (diverging), ensure 0 is included as a tick label.
            if include_zero_tick and (vmin_f < 0.0 < vmax_f):
                ticks.append(0.0)
                ticks = sorted(ticks)

                # de-dupe with a tolerance (handles float round-off and exact 0 on a tick)
                tol = max(abs(span) * 1e-9, 1e-12)
                deduped: list[float] = []
                for t in ticks:
                    if not deduped or abs(t - deduped[-1]) > tol:
                        deduped.append(t)
                ticks = deduped

                # Keep the label count stable by dropping the non-critical tick nearest to 0.
                while len(ticks) > n:
                    candidates = [
                        (i, abs(t))
                        for i, t in enumerate(ticks)
                        if (i not in (0, len(ticks) - 1)) and (abs(t) > tol)
                    ]
                    if not candidates:
                        break
                    drop_i = min(candidates, key=lambda x: x[1])[0]
                    ticks.pop(drop_i)

            # Build absolutely positioned tick label divs
            parts: list[str] = []
            tol_zero = max(abs(span) * 1e-12, 1e-12)
            for i, t in enumerate(ticks):
                t_str = format_legend_value(t, vmin=vmin_f, vmax=vmax_f, display_scale=display_scale)
                is_zero = abs(t) <= tol_zero and (vmin_f < 0.0 < vmax_f)
                weight_css = " font-weight: 700;" if is_zero else ""

                if i == 0:
                    parts.append(
                        f'<div style="position:absolute; bottom:0; right:0; text-align:right;'
                        f' white-space: nowrap;{weight_css}">{t_str}</div>'
                    )
                elif i == len(ticks) - 1:
                    parts.append(
                        f'<div style="position:absolute; top:0; right:0; text-align:right;'
                        f' white-space: nowrap;{weight_css}">{t_str}</div>'
                    )
                else:
                    pos = (t - vmin_f) / span
                    pos = float(np.clip(pos, 0.0, 1.0))
                    parts.append(
                        f'<div style="position:absolute; right:0; bottom:{pos * 100.0:.1f}%;'
                        f' transform: translateY(50%); text-align:right; white-space: nowrap;{weight_css}">'
                        f"{t_str}</div>"
                    )

            tick_labels_html = "\n".join(parts)
    except Exception:
        tick_labels_html = ""

    return f"""
<div style="height: 100%; width: 100%; display: flex; align-items: center; justify-content: center;
            box-sizing: border-box; font-family: Arial, Helvetica, sans-serif;">
  <div style="display: flex; align-items: center; justify-content: center; gap: 8px;
              padding: {outer_pad_top_px}px 0 {outer_pad_bottom_px}px 0;
              min-width: 90px; max-width: 100%; box-sizing: border-box;">
    <div style="position: relative; display: flex; align-items: center; height: {bar_height_px}px;">
      <div style="position: relative; width: 34px; height: {bar_height_px}px; margin-right: 6px;
                  font-size: {label_font}; color: #000;">
        {tick_labels_html}
      </div>
      <div style="height: {bar_height_px}px; width: {bar_width_px}px; border-radius: 6px;
                  border: 1px solid rgba(0,0,0,0.18);
                  box-shadow: 0 2px 6px rgba(0,0,0,0.20);
                  overflow: hidden; display: flex; flex-direction: column-reverse;">
        {segments_html}
      </div>
    </div>
    {title_html}
  </div>
</div>
"""


def build_vertical_categorical_legend_block_html(
    *,
    legend_title: str = "",
    labels: Sequence[str],
    colors: Sequence[str],
    map_height: int = 700,
    bar_width_px: int = 18,
    label_font: str = "12px",
    bar_height_fraction: float = 0.82,
) -> str:
    """Build a container-relative categorical legend with fixed labels and colors."""
    pairs = [(str(label), str(color)) for label, color in zip(labels, colors) if str(label).strip()]
    if not pairs:
        return build_vertical_binned_legend_block_html(
            legend_title=legend_title,
            vmin=1.0,
            vmax=5.0,
            cmap_name="Reds",
            map_height=map_height,
        )

    title_text = str(legend_title or "").strip()
    outer_pad_top_px = 30
    outer_pad_bottom_px = 18
    available_height_px = max(int(map_height) - outer_pad_top_px - outer_pad_bottom_px, 160)
    bar_height_px = max(int(available_height_px * bar_height_fraction), 120)
    title_html = ""
    if title_text:
        title_html = (
            '<div style="writing-mode: vertical-rl; transform: rotate(180deg);'
            f' font-size: {label_font}; white-space: nowrap; align-self: center; color: #000;">'
            f"{title_text}</div>"
        )

    segment_height = max(bar_height_px / len(pairs), 18.0)
    label_blocks = []
    color_blocks = []
    for label, _color in reversed(pairs):
        label_blocks.append(
            f'<div style="height:{segment_height}px; display:flex; align-items:center; justify-content:flex-end; white-space:nowrap;">{label}</div>'
        )
    for _label, color in pairs:
        color_blocks.append(
            f'<div style="height:{segment_height}px; width:100%; background:{color};"></div>'
        )

    labels_html = "\n".join(label_blocks)
    segments_html = "\n".join(color_blocks)

    return f"""
<div style="height: 100%; width: 100%; display: flex; align-items: center; justify-content: center;
            box-sizing: border-box; font-family: Arial, Helvetica, sans-serif;">
  <div style="display: flex; align-items: center; justify-content: center; gap: 8px;
              padding: {outer_pad_top_px}px 0 {outer_pad_bottom_px}px 0;
              min-width: 120px; max-width: 100%; box-sizing: border-box;">
    <div style="position: relative; display: flex; align-items: center; height: {bar_height_px}px;">
      <div style="display:flex; flex-direction:column; justify-content:space-between; height:{bar_height_px}px; margin-right: 6px;
                  font-size:{label_font}; color:#000;">
        {labels_html}
      </div>
      <div style="height: {bar_height_px}px; width: {bar_width_px}px; border-radius: 6px;
                  border: 1px solid rgba(0,0,0,0.18);
                  box-shadow: 0 2px 6px rgba(0,0,0,0.20);
                  overflow: hidden; display: flex; flex-direction: column-reverse;">
        {segments_html}
      </div>
    </div>
    {title_html}
  </div>
</div>
"""


def _legend_card_title_html(title_text: str) -> str:
    """Return the compact legend title HTML, or an empty string when titleless."""
    clean = html.escape(str(title_text or "").strip())
    if not clean:
        return ""
    return f'<div style="font-weight:700; color:#111827; margin-bottom:8px;">{clean}</div>'


def build_compact_binned_legend_card_html(
    *,
    legend_title: str = "",
    pretty_metric_label: str | None = None,
    vmin: float,
    vmax: float,
    cmap_name: str,
    display_scale: float = 1.0,
    nlevels: int = 7,
    no_data_label: str = "No data",
    no_data_color: str = NO_DATA_FILL_HEX,
) -> str:
    """Build a fixed in-map compact binned legend card for Folium maps."""
    n = max(2, int(nlevels))
    title_html = _legend_card_title_html(str(legend_title or pretty_metric_label or ""))

    try:
        vmin_f = float(vmin)
        vmax_f = float(vmax)
    except Exception:
        vmin_f, vmax_f = 0.0, 1.0
    if not np.isfinite(vmin_f) or not np.isfinite(vmax_f):
        vmin_f, vmax_f = 0.0, 1.0
    if vmin_f > vmax_f:
        vmin_f, vmax_f = vmax_f, vmin_f
    if vmin_f == vmax_f:
        padding = max(abs(vmin_f) * 0.1, 1.0)
        vmin_f -= padding
        vmax_f += padding

    colors = get_binned_cmap_hex_list(cmap_name, nlevels=n)
    edges = np.linspace(vmin_f, vmax_f, n + 1)
    rows: list[str] = []
    for idx in range(n - 1, -1, -1):
        lower = format_legend_value(float(edges[idx]), vmin=vmin_f, vmax=vmax_f, display_scale=display_scale)
        upper = format_legend_value(float(edges[idx + 1]), vmin=vmin_f, vmax=vmax_f, display_scale=display_scale)
        label = f"{lower} - {upper}"
        rows.append(
            '<div style="display:flex; align-items:center; gap:7px; min-height:16px;">'
            f'<span style="width:14px; height:10px; background:{html.escape(colors[idx])};'
            ' border:1px solid rgba(17,24,39,0.20); flex:0 0 auto;"></span>'
            f'<span style="white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{html.escape(label)}</span>'
            "</div>"
        )

    rows.append(
        '<div style="display:flex; align-items:center; gap:7px; min-height:16px; margin-top:4px;">'
        f'<span style="width:14px; height:10px; background:{html.escape(no_data_color)};'
        ' border:1px solid rgba(17,24,39,0.20); flex:0 0 auto;"></span>'
        f'<span>{html.escape(str(no_data_label))}</span>'
        "</div>"
    )

    rows_html = "\n".join(rows)
    return f"""
<div id="irt-compact-map-legend" style="width:240px; max-width:calc(100vw - 36px);
            pointer-events:none;
            font-family:Arial, Helvetica, sans-serif; font-size:11px; line-height:1.2;
            color:#111827; background:rgba(255,255,255,0.94);
            border:1px solid rgba(17,24,39,0.18); border-radius:6px;
            box-shadow:0 4px 14px rgba(17,24,39,0.22); padding:10px 11px;
            box-sizing:border-box;">
  {title_html}
  <div style="display:flex; flex-direction:column; gap:3px;">
    {rows_html}
  </div>
</div>
"""


def build_compact_categorical_legend_card_html(
    *,
    legend_title: str = "",
    labels: Sequence[str],
    colors: Sequence[str],
    no_data_label: str = "No data",
    no_data_color: str = NO_DATA_FILL_HEX,
) -> str:
    """Build a fixed in-map compact categorical legend card for Folium maps."""
    title_html = _legend_card_title_html(legend_title)
    pairs = [(str(label), str(color)) for label, color in zip(labels, colors) if str(label).strip()]
    rows = [
        '<div style="display:flex; align-items:center; gap:7px; min-height:16px;">'
        f'<span style="width:14px; height:10px; background:{html.escape(color)};'
        ' border:1px solid rgba(17,24,39,0.20); flex:0 0 auto;"></span>'
        f'<span style="white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{html.escape(label)}</span>'
        "</div>"
        for label, color in pairs
    ]
    rows.append(
        '<div style="display:flex; align-items:center; gap:7px; min-height:16px; margin-top:4px;">'
        f'<span style="width:14px; height:10px; background:{html.escape(no_data_color)};'
        ' border:1px solid rgba(17,24,39,0.20); flex:0 0 auto;"></span>'
        f'<span>{html.escape(str(no_data_label))}</span>'
        "</div>"
    )
    rows_html = "\n".join(rows)
    return f"""
<div id="irt-compact-map-legend" style="width:240px; max-width:calc(100vw - 36px);
            pointer-events:none;
            font-family:Arial, Helvetica, sans-serif; font-size:11px; line-height:1.2;
            color:#111827; background:rgba(255,255,255,0.94);
            border:1px solid rgba(17,24,39,0.18); border-radius:6px;
            box-shadow:0 4px 14px rgba(17,24,39,0.22); padding:10px 11px;
            box-sizing:border-box;">
  {title_html}
  <div style="display:flex; flex-direction:column; gap:3px;">
    {rows_html}
  </div>
</div>
"""


def build_rp100_flood_depth_legend_html(
    *,
    bins: Sequence[tuple[str, str]],
    map_height: int = 700,
) -> str:
    """Build the compact display legend for the RP-100 flood-depth overlay."""
    return build_vertical_categorical_legend_block_html(
        legend_title="RP-100 flood depth",
        labels=[label for label, _color in bins],
        colors=[color for _label, color in bins],
        map_height=map_height,
        bar_width_px=18,
    )


def build_rural_facilities_density_legend_html(
    *,
    mode: str,
    category: str | None = None,
    map_height: int = 700,
) -> str:
    """Build the rural-facilities-density overlay legend HTML.

    Args:
        mode: ``"single"`` for one category's vertical binned legend,
              ``"total"`` for a side-by-side block showing all four real categories.
        category: Required when ``mode == "single"``; one of the four real categories.
        map_height: Outer container height in pixels.

    Returns:
        Sanitizer-safe HTML string suitable for st.components.html / st.markdown.
    """
    from india_resilience_tool.app.overlays import (
        RURAL_FACILITIES_BIN_LABELS,
        RURAL_FACILITIES_COLOR_RAMPS,
        RURAL_FACILITIES_REAL_CATEGORIES,
    )

    def _ramp_hexes(cat: str) -> list[str]:
        return [str(stop["color_hex"]) for stop in RURAL_FACILITIES_COLOR_RAMPS[cat] if not stop.get("transparent")]

    bin_labels = list(RURAL_FACILITIES_BIN_LABELS)

    mode_norm = str(mode or "").strip().lower()
    if mode_norm == "single":
        cat_key = str(category or "").strip().lower()
        if cat_key not in RURAL_FACILITIES_REAL_CATEGORIES:
            raise ValueError(
                f"Rural facilities density legend: unknown category {category!r}; "
                f"expected one of {RURAL_FACILITIES_REAL_CATEGORIES}."
            )
        return build_vertical_categorical_legend_block_html(
            legend_title=f"{cat_key.title()} facilities / 1,000 km2",
            labels=bin_labels,
            colors=_ramp_hexes(cat_key),
            map_height=map_height,
            bar_width_px=18,
        )

    if mode_norm != "total":
        raise ValueError(f"Rural facilities density legend: unknown mode {mode!r}.")

    outer_pad_top_px = 30
    outer_pad_bottom_px = 18
    available_height_px = max(int(map_height) - outer_pad_top_px - outer_pad_bottom_px, 200)
    bar_height_px = max(int(available_height_px * 0.78), 160)
    segment_height = max(bar_height_px / len(bin_labels), 14.0)

    label_blocks = []
    for label in reversed(bin_labels):
        label_blocks.append(
            f'<div style="height:{segment_height}px; display:flex; align-items:center;'
            f' justify-content:flex-end; white-space:nowrap;">{label}</div>'
        )
    labels_col_html = "\n".join(label_blocks)

    category_columns_html: list[str] = []
    for cat in RURAL_FACILITIES_REAL_CATEGORIES:
        hexes = _ramp_hexes(cat)
        segments = "\n".join(
            f'<div style="height:{segment_height}px; width:100%; background:{c};"></div>'
            for c in hexes
        )
        category_columns_html.append(
            f"""
<div style="display:flex; flex-direction:column; align-items:center; gap:4px;">
  <div style="font-size:11px; color:#000; white-space:nowrap;">{cat.title()}</div>
  <div style="height:{bar_height_px}px; width:14px; border-radius:4px;
              border:1px solid rgba(0,0,0,0.18);
              box-shadow:0 1px 4px rgba(0,0,0,0.18);
              overflow:hidden; display:flex; flex-direction:column-reverse;">
    {segments}
  </div>
</div>
"""
        )

    columns_block_html = "\n".join(category_columns_html)
    return f"""
<div style="height:100%; width:100%; display:flex; align-items:center; justify-content:center;
            box-sizing:border-box; font-family:Arial, Helvetica, sans-serif;">
  <div style="display:flex; flex-direction:column; align-items:center; justify-content:center;
              gap:6px; padding:{outer_pad_top_px}px 0 {outer_pad_bottom_px}px 0;
              min-width:180px; max-width:100%; box-sizing:border-box;">
    <div style="font-size:11px; color:#000; white-space:nowrap;">facilities / 1,000 km2</div>
    <div style="display:flex; flex-direction:row; align-items:flex-end; gap:8px;">
      <div style="display:flex; flex-direction:column; justify-content:space-between;
                  height:{bar_height_px}px; font-size:11px; color:#000;
                  margin-bottom:0;">
        {labels_col_html}
      </div>
      {columns_block_html}
    </div>
  </div>
</div>
"""


def build_built_up_area_legend_html(
    *,
    bins: Sequence[tuple[str, str]],
    map_height: int = 700,
) -> str:
    """Build the compact display legend for the built-up area overlay."""
    return build_vertical_categorical_legend_block_html(
        legend_title="Built-up area",
        labels=[label for label, _color in bins],
        colors=[color for _label, color in bins],
        map_height=map_height,
        bar_width_px=18,
    )


def build_lulc_agri_legend_html(
    *,
    bins: Sequence[tuple[str, str]],
    map_height: int = 700,
) -> str:
    """Build the compact display legend for the agricultural LULC overlay."""
    return build_vertical_categorical_legend_block_html(
        legend_title="Agricultural LULC",
        labels=[label for label, _color in bins],
        colors=[color for _label, color in bins],
        map_height=map_height,
        bar_width_px=18,
    )


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
